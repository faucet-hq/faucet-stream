//! `faucet discover` — connect to a config's source, enumerate the datasets
//! living behind it (tables / collections / indices / prefixes), and emit a
//! ready-to-run config with one matrix row per dataset (#211).
//!
//! The generated document is the **raw composed** input config (so `${env:…}`
//! and secrets-manager directives are echoed verbatim, never their resolved
//! values) with the `matrix:` block replaced by one row per discovered
//! dataset. Each row deep-merges the dataset's
//! [`config_patch`](faucet_core::DatasetDescriptor::config_patch) over the
//! connection config.

use crate::cli::DiscoverArgs;
use crate::config::{ConnectorSpec, PipelineConfig, PipelineSpec};
use crate::error::{CliError, CliResult};
use faucet_core::DatasetDescriptor;
use serde_json::{Value, json};

/// Execute the `discover` subcommand.
pub async fn run(args: DiscoverArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    // Interpolated + secrets-resolved config: used to CONNECT.
    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    // Raw composed text (extends/!include/profile folded, `${…}` untouched):
    // used to ECHO the connection config without leaking resolved secrets.
    let raw_composed = crate::compose::compose(&path, args.profile.as_deref())?;

    let template = args.source.as_deref().unwrap_or("default");
    let spec = select_source_template(&cfg.pipeline, template)?;

    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let source =
        crate::registry::build_source(&spec.kind, spec.config.clone(), &auth, None).await?;
    if !source.supports_discover() {
        return Err(CliError::Config(format!(
            "source '{}' does not support dataset discovery — discovery is available for \
             catalog-backed sources (postgres, mysql, mssql, sqlite, mongodb, elasticsearch, \
             bigquery, snowflake, spanner, s3, gcs) and for `rest` sources with a `discovery:` \
             recipe (config-driven API calls) or an `odata:` block (via OData `$metadata`)",
            spec.kind
        )));
    }

    let datasets = source.discover().await.map_err(|e| {
        CliError::Config(format!(
            "discovery against source '{}' failed: {e}",
            spec.kind
        ))
    })?;
    let total = datasets.len();
    let datasets = filter_datasets(datasets, &args.include, &args.exclude);
    if datasets.is_empty() {
        return Err(CliError::Config(format!(
            "discovery found no datasets{} (source '{}' reported {total} before filtering)",
            if args.include.is_empty() && args.exclude.is_empty() {
                ""
            } else {
                " matching the --include/--exclude filters"
            },
            spec.kind
        )));
    }

    if args.json {
        let out = json!({ "source": spec.kind, "datasets": datasets });
        println!(
            "{}",
            serde_json::to_string_pretty(&out)
                .map_err(|e| CliError::Internal(format!("json render: {e}")))?
        );
        return Ok(());
    }

    let doc = render_discovered_config(&raw_composed, template, args.sink.as_deref(), &datasets)?;

    // Guard: the emitted document must itself load + expand. Configs holding
    // secrets-manager directives can't be re-verified offline — warn, don't fail.
    if let Err(e) =
        PipelineConfig::from_text(&doc, &path).and_then(|c| crate::expand::expand(&c).map(|_| ()))
    {
        tracing::warn!(error = %e, "generated config failed offline re-validation — review it before running");
    }

    match args.output {
        Some(out) => {
            if out.exists() && !args.force {
                return Err(CliError::Config(format!(
                    "output file {} already exists — pass --force to overwrite",
                    out.display()
                )));
            }
            std::fs::write(&out, &doc)?;
            eprintln!(
                "wrote {} ({} dataset{} from '{}' source)",
                out.display(),
                datasets.len(),
                if datasets.len() == 1 { "" } else { "s" },
                spec.kind
            );
        }
        None => print!("{doc}"),
    }
    Ok(())
}

/// Resolve the source template to introspect: the named entry in
/// `pipeline.sources`, or the legacy singular `pipeline.source` (which
/// registers as `default`).
fn select_source_template<'a>(
    pipeline: &'a PipelineSpec,
    name: &str,
) -> CliResult<&'a ConnectorSpec> {
    if let Some(spec) = pipeline.sources.get(name) {
        return Ok(spec);
    }
    if name == "default"
        && let Some(spec) = pipeline.source.as_ref()
    {
        return Ok(spec);
    }
    let mut available: Vec<&str> = pipeline.sources.keys().map(String::as_str).collect();
    if pipeline.source.is_some() {
        available.push("default");
    }
    available.sort_unstable();
    Err(CliError::Config(format!(
        "no source template named '{name}' — available: {}",
        if available.is_empty() {
            "none (the config has no `pipeline.source` or `pipeline.sources`)".to_string()
        } else {
            available.join(", ")
        }
    )))
}

/// Simple `*`-wildcard glob match (case-sensitive). `*` matches any run of
/// characters, including none; every other character matches literally.
fn glob_match(pattern: &str, name: &str) -> bool {
    // Dynamic-programming over the pattern segments split by '*': the name
    // must start with the first segment, end with the last, and contain the
    // middle segments in order.
    let segments: Vec<&str> = pattern.split('*').collect();
    if segments.len() == 1 {
        return pattern == name;
    }
    let mut rest = name;
    for (i, seg) in segments.iter().enumerate() {
        if seg.is_empty() {
            continue;
        }
        if i == 0 {
            match rest.strip_prefix(seg) {
                Some(r) => rest = r,
                None => return false,
            }
        } else if i == segments.len() - 1 {
            return rest.ends_with(seg) && rest.len() >= seg.len();
        } else {
            match rest.find(seg) {
                Some(pos) => rest = &rest[pos + seg.len()..],
                None => return false,
            }
        }
    }
    true
}

/// Apply `--include` / `--exclude` glob filters to the discovered datasets.
/// No `--include` patterns = include everything; any matching `--exclude`
/// pattern removes a dataset.
fn filter_datasets(
    datasets: Vec<DatasetDescriptor>,
    include: &[String],
    exclude: &[String],
) -> Vec<DatasetDescriptor> {
    datasets
        .into_iter()
        .filter(|d| {
            let included = include.is_empty() || include.iter().any(|p| glob_match(p, &d.name));
            let excluded = exclude.iter().any(|p| glob_match(p, &d.name));
            included && !excluded
        })
        .collect()
}

/// Sanitize a dataset name into a matrix row id: `[A-Za-z0-9_-]` only, other
/// runs collapsed to a single `_`. Never empty.
fn sanitize_row_id(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut last_was_sep = false;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            out.push(c);
            last_was_sep = false;
        } else if !last_was_sep && !out.is_empty() {
            out.push('_');
            last_was_sep = true;
        }
    }
    let trimmed = out.trim_matches('_').to_string();
    if trimmed.is_empty() {
        "dataset".to_string()
    } else {
        trimmed
    }
}

/// Assign unique row ids to the datasets (a `-2`, `-3`, … suffix on collision).
pub(crate) fn unique_row_ids(datasets: &[DatasetDescriptor]) -> Vec<String> {
    let mut seen = std::collections::HashMap::<String, usize>::new();
    datasets
        .iter()
        .map(|d| {
            let base = sanitize_row_id(&d.name);
            let n = seen.entry(base.clone()).or_insert(0);
            *n += 1;
            if *n == 1 { base } else { format!("{base}-{n}") }
        })
        .collect()
}

/// One-line column summary for a dataset's schema comment, e.g.
/// `id integer, note string?, total number` (`?` = nullable), capped at
/// `max_cols` columns with a `…` marker.
fn schema_summary(schema: &Value, max_cols: usize) -> Option<String> {
    let props = schema.get("properties")?.as_object()?;
    if props.is_empty() {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    for (name, fragment) in props.iter().take(max_cols) {
        let (ty, nullable) = match fragment.get("type") {
            Some(Value::String(t)) => (t.clone(), false),
            Some(Value::Array(a)) => {
                let base = a
                    .iter()
                    .filter_map(|v| v.as_str())
                    .find(|t| *t != "null")
                    .unwrap_or("any");
                (base.to_string(), a.iter().any(|v| v == "null"))
            }
            _ => ("any".to_string(), false),
        };
        parts.push(format!("{name} {ty}{}", if nullable { "?" } else { "" }));
    }
    if props.len() > max_cols {
        parts.push("…".to_string());
    }
    Some(parts.join(", "))
}

/// Render a serde value as a YAML sequence item indented under `matrix:`.
fn yaml_seq_item(value: &Value) -> CliResult<String> {
    let body = serde_yaml::to_string(value)
        .map_err(|e| CliError::Internal(format!("yaml render: {e}")))?;
    let mut out = String::new();
    for (i, line) in body.trim_end().lines().enumerate() {
        if i == 0 {
            out.push_str("  - ");
        } else {
            out.push_str("    ");
        }
        out.push_str(line);
        out.push('\n');
    }
    Ok(out)
}

/// Build the generated config document: the raw composed input with the
/// `matrix:` block replaced by one row per dataset (schema summaries as
/// comments). Pure — unit-testable without a live source.
/// Estimate a dataset's relative dispatch cost for `execution.schedule: lpt`
/// (#644): roughly the **bytes** it will move, not its row count.
///
/// Row count alone mis-orders real workloads — a 970k-row × 3-column history
/// table is lighter than a 122k-row × 100-column object — so rows are scaled by
/// a width derived from the inferred schema's column types. The absolute number
/// is meaningless; only the ordering between rows matters.
///
/// `None` when there is no row estimate: an unranked row sorts after every
/// ranked one rather than being guessed at.
fn estimate_weight(rows: Option<u64>, schema: Option<&Value>) -> Option<f64> {
    let rows = rows? as f64;
    Some(rows * row_width(schema))
}

/// Approximate bytes per row from an `infer_schema`-shaped object.
///
/// Per-type widths are deliberately coarse — this feeds a comparison, not a
/// capacity plan. With no schema, every row costs the same, which degrades to
/// ranking on row count.
fn row_width(schema: Option<&Value>) -> f64 {
    const DEFAULT_ROW_WIDTH: f64 = 32.0;
    let Some(props) = schema
        .and_then(|s| s.get("properties"))
        .and_then(Value::as_object)
    else {
        return DEFAULT_ROW_WIDTH;
    };
    if props.is_empty() {
        return DEFAULT_ROW_WIDTH;
    }
    props
        .values()
        .map(|col| match column_type(col) {
            Some("boolean") => 1.0,
            Some("integer") => 8.0,
            Some("number") => 8.0,
            // Strings dominate real tables and vary wildly; a single coarse
            // figure keeps wide text objects ranked above narrow numeric ones.
            Some("string") => 32.0,
            // A nested object/array is at least as heavy as a string.
            Some("object") | Some("array") => 64.0,
            _ => 16.0,
        })
        .sum()
}

/// A column's JSON-Schema type, tolerating the `["string", "null"]` nullable
/// form `infer_schema` emits.
fn column_type(col: &Value) -> Option<&str> {
    match col.get("type")? {
        Value::String(s) => Some(s.as_str()),
        Value::Array(types) => types
            .iter()
            .filter_map(Value::as_str)
            .find(|t| *t != "null"),
        _ => None,
    }
}

fn render_discovered_config(
    raw_composed: &str,
    template: &str,
    sink_template: Option<&str>,
    datasets: &[DatasetDescriptor],
) -> CliResult<String> {
    let mut root: serde_yaml::Value = serde_yaml::from_str(raw_composed)
        .map_err(|e| CliError::Config(format!("could not re-parse composed config: {e}")))?;
    let map = root
        .as_mapping_mut()
        .ok_or_else(|| CliError::Config("composed config is not a mapping".into()))?;
    let replaced_matrix = map
        .remove(serde_yaml::Value::String("matrix".into()))
        .is_some();

    let head = serde_yaml::to_string(&root)
        .map_err(|e| CliError::Internal(format!("yaml render: {e}")))?;

    let row_ids = unique_row_ids(datasets);
    let mut doc = String::new();
    doc.push_str(&head);
    if !head.ends_with('\n') {
        doc.push('\n');
    }
    doc.push('\n');
    doc.push_str(&format!(
        "# Generated by `faucet discover` — one row per discovered dataset ({}).\n",
        datasets.len()
    ));
    if replaced_matrix {
        doc.push_str("# NOTE: the input config's `matrix:` block was replaced.\n");
    }
    doc.push_str("matrix:\n");
    for (d, id) in datasets.iter().zip(&row_ids) {
        let est = d
            .estimated_rows
            .map(|n| format!(", ~{n} rows"))
            .unwrap_or_default();
        doc.push_str(&format!("  # {} ({}{})\n", d.name, d.kind, est));
        if let Some(summary) = d.schema.as_ref().and_then(|s| schema_summary(s, 12)) {
            doc.push_str(&format!("  #   columns: {summary}\n"));
        }
        let mut row = json!({ "id": id, "source": { "config": d.config_patch } });
        // Cost hint for `execution.schedule: lpt` (#644). Discovery is the one
        // place that knows both halves, so it is where the estimate belongs —
        // the executor just ranks by it.
        if let Some(w) = estimate_weight(d.estimated_rows, d.schema.as_ref()) {
            row["weight"] = json!(w);
        }
        if template != "default" {
            row["source"]["ref"] = json!(template);
        }
        // Per-dataset sink routing: a `--sink` template ref and/or the
        // descriptor's own `sink_patch` (e.g. `{table_id: account}` so a
        // fan-out lands one table per dataset).
        if sink_template.is_some() || d.sink_patch.is_some() {
            let mut sink = json!({});
            if let Some(s) = sink_template {
                sink["ref"] = json!(s);
            }
            if let Some(patch) = &d.sink_patch {
                sink["config"] = patch.clone();
            }
            row["sink"] = sink;
        }
        doc.push_str(&yaml_seq_item(&row)?);
    }
    Ok(doc)
}

#[cfg(test)]
mod tests {

    /// #644 — the cost estimate `faucet discover` writes into each row.
    mod weight {
        use super::*;

        fn schema(cols: &[(&str, &str)]) -> Value {
            let props: serde_json::Map<String, Value> = cols
                .iter()
                .map(|(n, t)| ((*n).to_string(), json!({ "type": t })))
                .collect();
            json!({ "type": "object", "properties": props })
        }

        #[test]
        fn a_wide_short_table_outranks_a_narrow_tall_one() {
            // The whole reason the estimate is bytes rather than rows: ranking
            // on row count alone dispatches these two in the wrong order.
            let wide_short = estimate_weight(
                Some(122_000),
                Some(&schema(
                    &(0..100)
                        .map(|i| {
                            (
                                Box::leak(format!("c{i}").into_boxed_str()) as &str,
                                "string",
                            )
                        })
                        .collect::<Vec<_>>(),
                )),
            )
            .expect("weighted");
            let narrow_tall = estimate_weight(
                Some(970_000),
                Some(&schema(&[
                    ("a", "integer"),
                    ("b", "integer"),
                    ("c", "integer"),
                ])),
            )
            .expect("weighted");
            assert!(
                wide_short > narrow_tall,
                "122k x 100 string columns ({wide_short}) must outrank 970k x 3 \
                 integer columns ({narrow_tall})"
            );
        }

        #[test]
        fn no_row_estimate_means_unranked_rather_than_guessed() {
            assert_eq!(
                estimate_weight(None, Some(&schema(&[("a", "string")]))),
                None
            );
        }

        #[test]
        fn without_a_schema_it_degrades_to_row_count() {
            let small = estimate_weight(Some(10), None).expect("weighted");
            let big = estimate_weight(Some(1_000), None).expect("weighted");
            assert!(big > small);
            assert_eq!(big / small, 100.0, "ordering is linear in rows");
        }

        #[test]
        fn a_nullable_column_is_typed_by_its_non_null_half() {
            // `infer_schema` emits `["string", "null"]` for a nullable column;
            // reading that as "unknown" would flatten every nullable table to
            // the same width.
            let nullable = json!({
                "type": "object",
                "properties": { "a": { "type": ["string", "null"] } }
            });
            assert_eq!(
                estimate_weight(Some(1), Some(&nullable)),
                estimate_weight(Some(1), Some(&schema(&[("a", "string")]))),
            );
        }

        #[test]
        fn an_empty_property_set_falls_back_to_the_default_width() {
            let empty = json!({ "type": "object", "properties": {} });
            assert_eq!(
                estimate_weight(Some(5), Some(&empty)),
                estimate_weight(Some(5), None),
            );
        }
    }
    use super::*;

    fn ds(name: &str, kind: &str, patch: Value) -> DatasetDescriptor {
        DatasetDescriptor::new(name, kind, patch)
    }

    // ── glob matching ─────────────────────────────────────────────────────────

    #[test]
    fn glob_exact_and_wildcards() {
        assert!(glob_match("orders", "orders"));
        assert!(!glob_match("orders", "orders2"));
        assert!(glob_match("public.*", "public.orders"));
        assert!(!glob_match("public.*", "sales.orders"));
        assert!(glob_match("*.orders", "public.orders"));
        assert!(glob_match("*order*", "public.orders"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a*b*c", "aXXbYYc"));
        assert!(!glob_match("a*b*c", "aXXcYYb"));
        // The trailing segment must not re-consume the head segment.
        assert!(!glob_match("ab*ab", "ab"));
    }

    #[test]
    fn filter_include_exclude_composition() {
        let all = vec![
            ds("public.orders", "table", json!({})),
            ds("public.users", "table", json!({})),
            ds("audit.log", "table", json!({})),
        ];
        let got = filter_datasets(all.clone(), &["public.*".into()], &["*.users".into()]);
        let names: Vec<&str> = got.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["public.orders"]);

        // No include patterns = everything (minus excludes).
        let got = filter_datasets(all, &[], &["audit.*".into()]);
        assert_eq!(got.len(), 2);
    }

    // ── row ids ───────────────────────────────────────────────────────────────

    #[test]
    fn row_ids_sanitized_and_unique() {
        assert_eq!(sanitize_row_id("public.orders"), "public_orders");
        assert_eq!(sanitize_row_id("raw/orders/"), "raw_orders");
        assert_eq!(sanitize_row_id("...."), "dataset");
        let ids = unique_row_ids(&[
            ds("a.b", "table", json!({})),
            ds("a/b", "table", json!({})),
            ds("a.b", "table", json!({})),
        ]);
        assert_eq!(ids, vec!["a_b", "a_b-2", "a_b-3"]);
    }

    // ── schema summary ────────────────────────────────────────────────────────

    #[test]
    fn schema_summary_marks_nullable_and_caps() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "note": {"type": ["string", "null"]},
            }
        });
        let s = schema_summary(&schema, 12).unwrap();
        assert!(s.contains("id integer"), "{s}");
        assert!(s.contains("note string?"), "{s}");

        let mut props = serde_json::Map::new();
        for i in 0..15 {
            props.insert(format!("c{i:02}"), json!({"type": "integer"}));
        }
        let big = json!({"type": "object", "properties": props});
        let s = schema_summary(&big, 12).unwrap();
        assert!(s.ends_with("…"), "capped: {s}");
    }

    #[test]
    fn schema_summary_empty_is_none() {
        assert!(schema_summary(&json!({"type": "object", "properties": {}}), 12).is_none());
        assert!(schema_summary(&json!({"type": "string"}), 12).is_none());
    }

    // ── rendering ─────────────────────────────────────────────────────────────

    const RAW: &str = r#"
version: 1
name: conn
pipeline:
  source:
    type: postgres
    config:
      connection_url: ${env:DATABASE_URL}
      query: SELECT 1
  sink:
    type: jsonl
    config:
      path: ./out.jsonl
"#;

    #[test]
    fn render_emits_matrix_rows_with_comments() {
        let datasets = vec![
            ds(
                "public.orders",
                "table",
                json!({"query": "SELECT * FROM \"public\".\"orders\""}),
            )
            .with_schema(json!({
                "type": "object",
                "properties": {"id": {"type": "integer"}}
            }))
            .with_estimated_rows(120),
            ds(
                "sales.leads",
                "table",
                json!({"query": "SELECT * FROM \"sales\".\"leads\""}),
            ),
        ];
        let doc = render_discovered_config(RAW, "default", None, &datasets).unwrap();

        // Raw `${env:…}` reference echoed, never a resolved value.
        assert!(doc.contains("${env:DATABASE_URL}"), "{doc}");
        assert!(doc.contains("# public.orders (table, ~120 rows)"), "{doc}");
        assert!(doc.contains("#   columns: id integer"), "{doc}");
        assert!(doc.contains("- id: public_orders"), "{doc}");
        assert!(doc.contains("SELECT * FROM \"public\".\"orders\""), "{doc}");

        // The generated document must parse and expand to one node per dataset.
        let cfg = crate::config::parse_with_extension(&doc, "yaml").unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].id, "public_orders");
        assert_eq!(
            nodes[0].source.config["query"], "SELECT * FROM \"public\".\"orders\"",
            "row patch deep-merged over the connection config"
        );
        assert_eq!(
            nodes[0].source.config["connection_url"], "${env:DATABASE_URL}",
            "connection settings inherited"
        );
    }

    #[test]
    fn render_replaces_existing_matrix_and_notes_it() {
        let raw = format!("{RAW}matrix:\n  - id: old\n");
        let datasets = vec![ds("t", "table", json!({"query": "SELECT * FROM t"}))];
        let doc = render_discovered_config(&raw, "default", None, &datasets).unwrap();
        assert!(doc.contains("was replaced"), "{doc}");
        assert!(!doc.contains("id: old"), "{doc}");
    }

    #[test]
    fn render_emits_sink_ref_and_sink_patch_per_row() {
        // A Salesforce-style discovery: source config_patch + a per-object sink
        // patch, targeting a named sink template via --sink.
        let raw = r#"
version: 1
pipeline:
  sources:
    default:
      type: rest
      config: { base_url: "https://x.my.salesforce.com" }
  sinks:
    bigquery:
      type: jsonl
      config: { path: ./out.jsonl }
"#;
        let datasets = vec![
            DatasetDescriptor::new(
                "Account",
                "sobject",
                json!({"async_job": {"submit": {"json": {"query": "SELECT Id FROM Account"}}}}),
            )
            .with_sink_patch(json!({ "table_id": "account" })),
        ];
        let doc = render_discovered_config(raw, "default", Some("bigquery"), &datasets).unwrap();
        assert!(doc.contains("ref: bigquery"), "{doc}");
        assert!(doc.contains("table_id: account"), "{doc}");
    }

    #[test]
    fn render_named_template_sets_source_ref() {
        let raw = r#"
version: 1
pipeline:
  sources:
    warehouse:
      type: postgres
      config: { connection_url: "postgres://x", query: "SELECT 1" }
  sinks:
    default:
      type: jsonl
      config: { path: ./out.jsonl }
"#;
        let datasets = vec![ds("t", "table", json!({"query": "SELECT * FROM t"}))];
        let doc = render_discovered_config(raw, "warehouse", None, &datasets).unwrap();
        assert!(doc.contains("ref: warehouse"), "{doc}");
        let cfg = crate::config::parse_with_extension(&doc, "yaml").unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].source.kind, "postgres");
    }

    // ── template selection ────────────────────────────────────────────────────

    #[test]
    fn select_template_falls_back_to_singular_default() {
        let cfg = crate::config::parse_with_extension(RAW, "yaml").unwrap();
        let spec = select_source_template(&cfg.pipeline, "default").unwrap();
        assert_eq!(spec.kind, "postgres");
        let err = select_source_template(&cfg.pipeline, "nope").unwrap_err();
        assert!(err.to_string().contains("available: default"), "{err}");
    }
}

#[cfg(all(test, feature = "source-sqlite", feature = "sink-jsonl"))]
mod run_tests {
    //! Command-level tests driving `run()` end-to-end against a real (file)
    //! SQLite catalog — no Docker, no network.
    use super::run;
    use crate::cli::DiscoverArgs;

    fn args(config: std::path::PathBuf) -> DiscoverArgs {
        DiscoverArgs {
            config: Some(config),
            source: None,
            sink: None,
            include: vec![],
            exclude: vec![],
            output: None,
            force: false,
            json: false,
            env_file: None,
            no_env_file: true,
            profile: None,
        }
    }

    fn write_config(dir: &std::path::Path, db: &str) -> std::path::PathBuf {
        let cfg = dir.join("conn.yaml");
        std::fs::write(
            &cfg,
            format!(
                "version: 1\nname: conn\npipeline:\n  source:\n    type: sqlite\n    config:\n      database_url: \"sqlite://{db}\"\n      query: SELECT 1\n  sink:\n    type: jsonl\n    config: {{ path: ./out.jsonl }}\n"
            ),
        )
        .unwrap();
        cfg
    }

    async fn seed_db(dir: &std::path::Path) -> String {
        let db = dir.join("cat.db").display().to_string();
        let pool = sqlx::SqlitePool::connect(&format!("sqlite://{db}?mode=rwc"))
            .await
            .expect("create db");
        sqlx::query("CREATE TABLE orders (id INTEGER PRIMARY KEY, note TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE users (id INTEGER NOT NULL, active BOOLEAN)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        db
    }

    #[tokio::test]
    async fn run_errors_on_unsupported_source_kind() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = dir.path().join("conn.yaml");
        std::fs::write(
            &cfg,
            "version: 1\npipeline:\n  source: { type: csv, config: { path: ./in.csv } }\n  sink: { type: jsonl, config: { path: ./o.jsonl } }\n",
        )
        .unwrap();
        let err = run(args(cfg)).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("does not support dataset discovery"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn run_writes_generated_config_that_expands() {
        let dir = tempfile::tempdir().unwrap();
        let db = seed_db(dir.path()).await;
        let cfg = write_config(dir.path(), &db);
        let out = dir.path().join("generated.yaml");

        let mut a = args(cfg.clone());
        a.output = Some(out.clone());
        run(a).await.expect("discover runs");

        let text = std::fs::read_to_string(&out).unwrap();
        let generated = crate::config::parse_with_extension(&text, "yaml").unwrap();
        let nodes = crate::expand::expand(&generated).unwrap();
        assert_eq!(nodes.len(), 2, "one row per table: {text}");
        assert!(text.contains("orders"), "{text}");
        assert!(text.contains("users"), "{text}");

        // Re-running without --force refuses to overwrite.
        let mut a = args(cfg.clone());
        a.output = Some(out.clone());
        let err = run(a).await.unwrap_err();
        assert!(err.to_string().contains("--force"), "{err}");

        // --include narrows to one row; --json emits the descriptor list.
        let mut a = args(cfg.clone());
        a.output = Some(dir.path().join("orders-only.yaml"));
        a.include = vec!["orders".into()];
        run(a).await.expect("filtered discover runs");
        let text = std::fs::read_to_string(dir.path().join("orders-only.yaml")).unwrap();
        assert!(
            text.contains("orders") && !text.contains("- id: users"),
            "{text}"
        );

        let mut a = args(cfg);
        a.json = true;
        run(a).await.expect("json mode runs");
    }

    #[tokio::test]
    async fn run_errors_when_filters_match_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let db = seed_db(dir.path()).await;
        let cfg = write_config(dir.path(), &db);
        let mut a = args(cfg);
        a.include = vec!["nothing_matches_*".into()];
        let err = run(a).await.unwrap_err();
        assert!(err.to_string().contains("no datasets"), "{err}");
    }
}

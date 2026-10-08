//! `faucet masking` — validate a config's `masking:` block and print which
//! rules apply to each destination sink (the destination-scoping check).
//! Offline-safe: secrets are never fetched (compiling a policy needs no
//! credentials, and the key is not required to list rules).

use crate::cli::MaskingArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use faucet_core::masking::{CompiledMasking, MaskAction, MaskRule, MaskingSpec};

/// Execute the `masking` subcommand.
pub async fn run(args: MaskingArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?;
    let spec = cfg.pipeline.masking.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `pipeline.masking:` block in this config — add one, or run \
             `faucet schema masking` to see the block's JSON Schema"
                .to_string(),
        )
    })?;
    // Compile first so a malformed policy fails before anything is printed.
    CompiledMasking::compile(spec).map_err(|e| CliError::Config(format!("masking: {e}")))?;

    let destinations = destinations(&cfg)?;
    print!("{}", render_summary(spec, &destinations));
    Ok(())
}

/// One place a run writes: a label for the report, the ids masking rules are
/// scoped by (exactly the ones the executor / topology runtime pass), and the
/// connector kind.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Destination {
    label: String,
    kind: String,
    ids: Vec<String>,
}

/// Where the config's rows (or topology sink nodes) actually write — after
/// each row's `type:` override — grouped by `(template, kind)`.
fn destinations(cfg: &PipelineConfig) -> CliResult<Vec<Destination>> {
    let mut out: Vec<Destination> = Vec::new();
    if crate::topology::is_topology(cfg) {
        for (node_id, ids) in crate::topology::sink_node_masking_ids(cfg) {
            out.push(Destination {
                label: format!("node {node_id}"),
                kind: ids.get(2).cloned().unwrap_or_default(),
                ids,
            });
        }
    } else {
        let mut grouped: std::collections::BTreeMap<(String, String), Vec<String>> =
            Default::default();
        for n in crate::expand::expand(&crate::partition::offline(cfg))? {
            grouped
                .entry((n.sink_ref.clone(), n.sink.kind.clone()))
                .or_default()
                .push(n.id.clone());
        }
        for ((sink_ref, kind), rows) in grouped {
            let label = if rows.len() == 1 && rows[0].starts_with("row-") {
                sink_ref.clone()
            } else {
                format!("{sink_ref} (rows {})", rows.join(", "))
            };
            out.push(Destination {
                label,
                ids: vec![sink_ref, kind.clone()],
                kind,
            });
        }
    }
    out.sort();
    Ok(out)
}

/// Labels of the rules that apply to a destination scoped by `ids`.
fn applied_rules(spec: &MaskingSpec, ids: &[String]) -> Vec<String> {
    spec.rules
        .iter()
        .enumerate()
        .filter(|(_, r)| rule_applies(r, ids))
        .map(|(i, r)| r.name.clone().unwrap_or_else(|| format!("rule_{i}")))
        .collect()
}

fn rule_applies(rule: &MaskRule, ids: &[String]) -> bool {
    rule.applies_to.is_empty() || rule.applies_to.iter().any(|t| ids.contains(t))
}

/// Render the human summary. Pure — returned as a string for testability.
fn render_summary(spec: &MaskingSpec, destinations: &[Destination]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let n = spec.rules.len();
    let _ = writeln!(
        out,
        "masking — valid ({n} rule{})",
        if n == 1 { "" } else { "s" }
    );
    if let Some(d) = &spec.description {
        let _ = writeln!(out, "  description: {d}");
    }
    let _ = writeln!(
        out,
        "  key: {}",
        if spec.key.is_some() {
            "configured (keyed HMAC-SHA256 for hash/tokenize)"
        } else {
            "none (unkeyed SHA-256 for hash/tokenize)"
        }
    );
    let _ = writeln!(out, "  rules:");
    for (i, r) in spec.rules.iter().enumerate() {
        let label = r.name.clone().unwrap_or_else(|| format!("rule_{i}"));
        let scope = if r.applies_to.is_empty() {
            "all sinks".to_string()
        } else {
            format!("sinks[{}]", r.applies_to.join(", "))
        };
        let _ = writeln!(
            out,
            "    - {label}: {} → {} ({scope})",
            describe_match(r),
            describe_action(&r.action),
        );
    }

    if destinations.is_empty() {
        let _ = writeln!(
            out,
            "  destinations: (none declared — every unscoped rule applies)"
        );
    } else {
        let _ = writeln!(out, "  destinations:");
        for d in destinations {
            let (name, kind) = (&d.label, &d.kind);
            let applied = applied_rules(spec, &d.ids);
            let list = if applied.is_empty() {
                "(no rules apply)".to_string()
            } else {
                applied.join(", ")
            };
            let _ = writeln!(out, "    - {name} [{kind}]: {list}");
        }
    }
    out
}

fn describe_match(rule: &MaskRule) -> String {
    let m = &rule.matcher;
    let mut parts: Vec<String> = Vec::new();
    if let Some(p) = &m.field_pattern {
        parts.push(format!("field_pattern /{p}/"));
    }
    if let Some(d) = m.value_detector {
        parts.push(format!("detector {d}"));
    }
    if !m.fields.is_empty() {
        parts.push(format!("fields[{}]", m.fields.join(", ")));
    }
    parts.join(" | ")
}

fn describe_action(action: &MaskAction) -> String {
    match action {
        MaskAction::Redact { .. } => "redact".to_string(),
        MaskAction::Hash => "hash".to_string(),
        MaskAction::Tokenize { prefix } => match prefix {
            Some(p) => format!("tokenize (prefix '{p}')"),
            None => "tokenize".to_string(),
        },
        MaskAction::Partial { keep_last, .. } => format!("partial (keep_last {keep_last})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn d(label: &str, kind: &str) -> Destination {
        Destination {
            label: label.into(),
            kind: kind.into(),
            ids: vec![label.into(), kind.into()],
        }
    }

    fn spec() -> MaskingSpec {
        serde_json::from_value(json!({
            "description": "customer PII",
            "key": "k",
            "rules": [
                { "name": "emails", "match": { "value_detector": "email" },
                  "action": { "type": "redact" } },
                { "name": "ssn", "match": { "field_pattern": "(?i)ssn" },
                  "action": { "type": "hash" }, "applies_to": ["analytics"] },
                { "match": { "fields": ["card"] },
                  "action": { "type": "partial", "keep_last": 4 } }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn summary_lists_rules_key_and_scope() {
        let dests = vec![d("default", "postgres"), d("analytics", "bigquery")];
        let out = render_summary(&spec(), &dests);
        assert!(out.contains("masking — valid (3 rules)"), "{out}");
        assert!(out.contains("description: customer PII"), "{out}");
        assert!(out.contains("keyed HMAC-SHA256"), "{out}");
        assert!(
            out.contains("emails: detector email → redact (all sinks)"),
            "{out}"
        );
        assert!(
            out.contains("ssn: field_pattern /(?i)ssn/ → hash (sinks[analytics])"),
            "{out}"
        );
        assert!(
            out.contains("rule_2: fields[card] → partial (keep_last 4)"),
            "{out}"
        );
    }

    #[test]
    fn summary_shows_applied_rules_per_destination() {
        let dests = vec![d("default", "postgres"), d("analytics", "bigquery")];
        let out = render_summary(&spec(), &dests);
        // default: unscoped rules (emails, rule_2) apply; ssn is analytics-only.
        assert!(
            out.contains("- default [postgres]: emails, rule_2"),
            "{out}"
        );
        // analytics: all three (ssn scoped to analytics + the two unscoped).
        assert!(
            out.contains("- analytics [bigquery]: emails, ssn, rule_2"),
            "{out}"
        );
    }

    #[test]
    fn scope_matches_connector_kind_too() {
        let s: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "match": { "fields": ["x"] }, "action": { "type": "redact" },
                        "applies_to": ["bigquery"] }]
        }))
        .unwrap();
        // A rule scoped to the `bigquery` KIND applies to a template of that kind.
        let ids = |k: &str| vec!["warehouse".to_string(), k.to_string()];
        assert_eq!(applied_rules(&s, &ids("bigquery")), vec!["rule_0"]);
        assert!(applied_rules(&s, &ids("postgres")).is_empty());
    }

    #[test]
    fn no_destinations_note() {
        let out = render_summary(&spec(), &[]);
        assert!(out.contains("none declared"), "{out}");
    }

    #[test]
    fn unkeyed_and_tokenize_without_prefix_render() {
        let s: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "name": "tok", "match": { "fields": ["id"] },
                        "action": { "type": "tokenize" } }]
        }))
        .unwrap();
        let out = render_summary(&s, &[d("default", "jsonl")]);
        assert!(out.contains("masking — valid (1 rule)"), "{out}");
        assert!(out.contains("none (unkeyed SHA-256"), "{out}");
        assert!(
            out.contains("tok: fields[id] → tokenize (all sinks)"),
            "{out}"
        );
        assert!(out.contains("- default [jsonl]: tok"), "{out}");
    }

    #[test]
    fn destinations_reads_singular_sink_and_named_sinks() {
        use crate::config::PipelineConfig;
        use std::path::Path;
        // Singular `sink:` → the `default` destination.
        let single = PipelineConfig::from_text(
            r#"version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  masking: { rules: [ { match: { fields: [x] }, action: { type: redact } } ] }
  sink: { type: jsonl, config: { path: ./out.jsonl } }
"#,
            Path::new("test.yaml"),
        )
        .unwrap();
        assert_eq!(destinations(&single).unwrap(), vec![d("default", "jsonl")]);

        // Named `sinks:` templates → one destination each, sorted.
        let named = PipelineConfig::from_text(
            r#"version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  masking: { rules: [ { match: { fields: [x] }, action: { type: redact } } ] }
  sinks:
    warehouse: { type: bigquery, config: {} }
    archive:   { type: jsonl, config: { path: ./a.jsonl } }
matrix:
  - id: a
    sink: { ref: archive }
  - id: w
    sink: { ref: warehouse }
"#,
            Path::new("test.yaml"),
        )
        .unwrap();
        let labels: Vec<String> = destinations(&named)
            .unwrap()
            .into_iter()
            .map(|d| d.label)
            .collect();
        assert_eq!(labels, ["archive (rows a)", "warehouse (rows w)"]);
    }

    /// The report scopes rules by the kind a row actually writes (after its
    /// `type:` override) and by topology node id, like the run does
    /// (#789 CLI-73).
    #[test]
    fn destinations_follow_row_kind_overrides_and_topology_nodes() {
        use crate::config::PipelineConfig;
        use std::path::Path;
        let spec: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "name": "j", "match": { "fields": ["x"] }, "action": { "type": "redact" },
                        "applies_to": ["jsonl"] },
                      { "name": "n", "match": { "fields": ["y"] }, "action": { "type": "redact" },
                        "applies_to": ["w"] }]
        }))
        .unwrap();
        let overridden = PipelineConfig::from_text(
            r#"version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sinks:
    archive: { type: jsonl, config: { path: ./a.jsonl } }
matrix:
  - id: b
    sink: { ref: archive, type: file, config: { path: ./b.jsonl } }
"#,
            Path::new("test.yaml"),
        )
        .unwrap();
        let out = render_summary(&spec, &destinations(&overridden).unwrap());
        assert!(
            out.contains("- archive (rows b) [file]: (no rules apply)"),
            "{out}"
        );

        let topo = PipelineConfig::from_text(
            r#"version: 1
pipeline:
  sources:
    a: { type: csv, config: { path: ./in.csv } }
  sinks:
    o: { type: jsonl, config: { path: ./o.jsonl } }
  nodes:
    s: { kind: source, ref: a }
    w: { kind: sink, ref: o }
  edges:
    - { from: s, to: w }
"#,
            Path::new("test.yaml"),
        )
        .unwrap();
        let out = render_summary(&spec, &destinations(&topo).unwrap());
        assert!(out.contains("- node w [jsonl]: j, n"), "{out}");
    }
}

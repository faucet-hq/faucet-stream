//! `faucet validate` — parse + expand a pipeline config without running.
//!
//! Surfaces every per-row error with the row id, so a config with multiple
//! issues reports them together instead of failing at the first one.

use crate::cli::ValidateArgs;
use crate::config::{PipelineConfig, SourceStatus};
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use crate::registry::{sink_schema, source_schema};
use crate::select::RunSelection;
use crate::state::available_state_kinds;
use crate::transforms::available_transforms;
use std::collections::HashSet;

/// Execute the `validate` subcommand.
pub async fn run(args: ValidateArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    // Template Hub (#571): validate a composed pairing offline. Placeholder
    // binding unless `--param` is given, exactly like a file. A single
    // `report` await at the end keeps the future small (see `run`).
    let cfg = if let (Some(source), Some(sink)) = (&args.source, &args.sink) {
        let sides = crate::hub::resolve_sides(
            &args.hub,
            args.source_hub.as_deref(),
            args.sink_hub.as_deref(),
            args.overlay_hub.as_deref(),
        )
        .await?;
        let composition =
            crate::hub::compose_across(source, sink, args.overlay.as_deref(), &sides).await?;
        if args.show_composed {
            print!("{}", composition.to_yaml()?);
            return Ok(());
        }
        let inputs = crate::config::RunInputs {
            params: crate::params::collect_cli_params(&args.param)?,
            env: crate::params::collect_env_overrides(&args.param_env)?
                .into_iter()
                .collect(),
            mode: if args.param.is_empty() {
                crate::params::BindMode::Placeholder
            } else {
                crate::params::BindMode::Strict
            },
        };
        let cfg = crate::hub::load_composed(&composition, &inputs)?;
        if !args.json {
            println!(
                "hub: composed source-template '{}' × sink-template '{}' ({}) — {} stream(s)",
                composition.source,
                composition.sink,
                composition.sink_kind,
                composition.streams.len()
            );
            for (what, hub) in [
                ("source", &composition.source_hub),
                ("sink", &composition.sink_hub),
                ("overlay", &composition.overlay_hub),
            ] {
                if let Some(hub) = hub {
                    println!("  {what} from {hub}");
                }
            }
            for p in &composition.streams {
                println!("  {:<32} write_mode: {}", p.stream, p.describe());
            }
            if let Some(o) = &composition.overlay {
                println!(
                    "  overlay '{o}' sets: {}",
                    composition.overlay_contributes.join(", ")
                );
            }
            for w in &composition.warnings {
                println!("  warning: {w}");
            }
        }
        cfg
    } else {
        let path = match args.config.clone() {
            Some(p) => p,
            None => {
                crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?
            }
        };
        if let Some(msg) = crate::hub::misplaced_document(&path, "validate") {
            return Err(CliError::Config(msg));
        }

        if args.show_composed {
            let composed = crate::compose::compose(&path, args.profile.as_deref())?;
            // Normalize to exactly one trailing newline: the YAML serializer appends
            // one but `serde_json::to_string_pretty` (JSON-format configs) does not,
            // and the fast path echoes the file verbatim. A single `\n` keeps
            // `faucet validate … --show-composed > out.{yaml,json}` well-formed.
            println!("{}", composed.trim_end_matches('\n'));
            return Ok(());
        }

        // Typed run params (#444). With no `--param`, required params bind to
        // type-shaped placeholders so a parameterized config still validates in CI;
        // supplying any `--param` opts into strict binding, which is how you check a
        // concrete invocation.
        let inputs = crate::config::RunInputs {
            params: crate::params::collect_cli_params(&args.param)?,
            env: crate::params::collect_env_overrides(&args.param_env)?
                .into_iter()
                .collect(),
            mode: if args.param.is_empty() {
                crate::params::BindMode::Placeholder
            } else {
                crate::params::BindMode::Strict
            },
        };

        if args.no_secrets {
            // Grammar / structure only — never touch the network.
            PipelineConfig::from_path_tolerating_secrets_with(
                &path,
                args.profile.as_deref(),
                &inputs,
            )?
        } else {
            // Real preflight: report each secret reference, then resolve.
            let refs =
                crate::secrets::scan_path_refs_with(&path, args.profile.as_deref(), &inputs)?;
            let cfg = PipelineConfig::from_path_async_with(&path, args.profile.as_deref(), &inputs)
                .await?;
            if !args.json {
                for (scheme, reference) in &refs {
                    println!("secret: {scheme}:{reference} → resolved");
                }
            }
            cfg
        }
    };
    report(cfg, args).await
}

/// Everything `validate` prints once a config is loaded — shared by the
/// file path and the hub-composed path so both report identically.
async fn report(cfg: PipelineConfig, args: ValidateArgs) -> CliResult<()> {
    if !cfg.params.is_empty() && !args.json {
        let required: Vec<&str> = cfg
            .params
            .iter()
            .filter(|(_, p)| p.required)
            .map(|(n, _)| n.as_str())
            .collect();
        println!(
            "params: {} declared ({}){}",
            cfg.params.len(),
            if required.is_empty() {
                String::from("all optional")
            } else {
                format!("required: {}", required.join(", "))
            },
            if args.param.is_empty() && !required.is_empty() {
                " — validated against placeholders; pass --param NAME=VALUE to bind for real"
            } else {
                ""
            }
        );
    }
    // Topology mode (#71/#72): build + validate the node graph instead of the
    // matrix. `build_topology` runs the core structural validator (arity,
    // fan-out, join edges, cycle, reachability).
    if crate::topology::is_topology(&cfg) {
        let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
        let topo = crate::topology::build_topology(&cfg, &auth).await?;
        let inert: Vec<(&str, &str)> = crate::topology::inert_blocks(&cfg);
        if args.json {
            let out = serde_json::json!({
                "valid": true,
                "mode": "topology",
                "name": cfg.name.as_deref().unwrap_or("unnamed"),
                "node_count": topo.nodes().len(),
                "edge_count": topo.edges().len(),
                "nodes": topo.nodes().iter()
                    .map(|n| serde_json::json!({ "id": n.id, "kind": n.kind.kind_str() }))
                    .collect::<Vec<_>>(),
                "warnings": inert.iter()
                    .map(|(block, consequence)| serde_json::json!({
                        "block": block, "consequence": consequence,
                    }))
                    .collect::<Vec<_>>(),
            });
            println!(
                "{}",
                serde_json::to_string_pretty(&out).unwrap_or_else(|_| out.to_string())
            );
            return Ok(());
        }
        println!(
            "topology '{}': {} node(s), {} edge(s) — valid",
            cfg.name.as_deref().unwrap_or("unnamed"),
            topo.nodes().len(),
            topo.edges().len()
        );
        for n in topo.nodes() {
            println!("  - {} ({})", n.id, n.kind.kind_str());
        }
        // Say what topology mode will *not* do. Printing "valid" while silently
        // ignoring a declared block is how an operator ends up believing a policy
        // is enforced when it is not (#456 M2).
        for (block, consequence) in &inert {
            println!("  WARNING: `{block}:` is ignored in topology mode — {consequence}");
        }
        return Ok(());
    }

    // `validate` is offline by design, so a discoverable bound is not probed
    // here. Report it rather than silently validating a plan we could not build.
    let unprobed: Vec<String> = std::iter::once(("<pipeline>", cfg.partition.as_ref()))
        .chain(
            cfg.matrix
                .iter()
                .map(|r| (r.id.as_deref().unwrap_or("<row>"), r.partition.as_ref())),
        )
        .filter_map(|(id, p)| {
            p.filter(|s| crate::partition::needs_probe(s))
                .map(|_| id.to_string())
        })
        .collect();

    let nodes = expand(&cfg)?;

    if !unprobed.is_empty() && !args.json {
        println!(
            "partition: {} row(s) discover their bound at run time ({}) — the chunk count \
             cannot be planned offline, so it is not validated here",
            unprobed.len(),
            unprobed.join(", ")
        );
    }

    // Validate the replication block (snapshot source / CDC source / state) so
    // `faucet validate` catches misconfiguration without running.
    if let Some(spec) = &cfg.replication {
        crate::replication::compiled::CompiledReplication::compile(spec, &cfg)?;
        if !args.json {
            println!("replication: mode={:?} — valid", spec.mode);
        }
    }

    // Validate the backfill defaults block (window / concurrency / timezone)
    // and the window-scoping requirement: a `backfill:` block on a pipeline
    // whose sources reference no `${backfill.*}` / `${now.*}` token would
    // replay identical data into every window (#282). Offline-safe.
    if let Some(spec) = &cfg.backfill {
        let source_configs: Vec<String> = nodes
            .iter()
            .filter(|n| matches!(n.role, crate::expand::NodeRole::Root))
            .map(|n| n.source.config.to_string())
            .collect();
        spec.validate(&source_configs)?;
        if !args.json {
            println!("backfill: defaults valid");
        }
    }

    // Validate the schedule block (cron / timezone / bounds) so `faucet validate`
    // catches schedule misconfiguration in CI without running. Offline-safe.
    #[cfg(feature = "schedule")]
    if let Some(spec) = &cfg.schedule {
        crate::schedule::compiled::CompiledSchedule::compile(spec)?;
        if !args.json {
            println!(
                "schedule: cron '{}' tz '{}' — valid",
                spec.cron, spec.timezone
            );
        }
    }

    // Validate the notifications block (unique names, non-empty channel fields)
    // so `faucet validate` catches misconfiguration without running. Offline.
    #[cfg(feature = "notify")]
    if !cfg.notifications.is_empty() {
        crate::notify::validate_all(&cfg.notifications)?;
        if !args.json {
            println!("notifications: {} rule(s) — valid", cfg.notifications.len());
        }
    }

    // Lineage transport reachability — best-effort. A failure here is only a
    // warning: lineage emission never blocks a pipeline run.
    #[cfg(feature = "lineage")]
    if let Some(lc) = cfg.lineage.as_ref()
        && !args.json
    {
        match crate::lineage_glue::check_transport(lc).await {
            Ok(msg) => println!("lineage: {msg}"),
            Err(msg) => println!("lineage: WARNING — {msg} (lineage never blocks a run)"),
        }
    }

    for node in &nodes {
        // Verifying the schema lookup also catches unknown connector kinds.
        source_schema(&node.source.kind)?;
        // A discovery row (#501) enumerates a value-set and has no sink — its
        // `sink` field is a never-built placeholder, so skip the sink check.
        if !matches!(node.role, NodeRole::Discovery { .. }) {
            sink_schema(&node.sink.kind)?;
        }
        for t in &node.transforms {
            if !available_transforms().contains(&t.kind.as_str()) {
                return Err(CliError::UnknownTransform {
                    name: format!("{} (row '{}')", t.kind, node.id),
                    available: available_transforms().join(", "),
                });
            }
        }
        if let Some(state) = &node.state
            && !available_state_kinds().contains(&state.kind.as_str())
        {
            return Err(CliError::UnknownStateStore {
                name: format!("{} (row '{}')", state.kind, node.id),
                available: available_state_kinds().join(", "),
            });
        }
    }

    // Transform *kinds* are checked above; this compiles each chain so a bad
    // transform *config* fails validation too.
    check_transforms(&nodes)?;
    check_connector_configs(&nodes)?;

    // "children" = per-parent-record fan-out rows; discovery / product rows run
    // independently (no parent), so they count as top-level like roots.
    let children = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Child { .. }))
        .count();
    let roots = nodes.len() - children;

    // Runtime row-selection (#370/#371/#376/#377). The selection is computed the
    // same way `faucet run` computes it, so the run/skip decision here matches
    // what a run would do; a selection error (empty run set, missing ancestor,
    // unknown token) is surfaced after the report so `validate` catches it in CI
    // without a run.
    let selection = RunSelection::from_args(&args.selection, cfg.selection.as_ref())?;
    let uses_selection_model = nodes
        .iter()
        .any(|n| n.status != SourceStatus::Active || !n.tags.is_empty());
    let selection_active = selection.narrows() || uses_selection_model;
    let has_matrix = !cfg.matrix.is_empty();
    let selected = if selection_active {
        Some(crate::select::select_nodes(
            nodes.clone(),
            &selection,
            has_matrix,
        ))
    } else {
        None
    };
    let run_ids: HashSet<String> = match &selected {
        Some(Ok(sel)) => sel.iter().map(|n| n.id.clone()).collect(),
        _ => HashSet::new(),
    };
    let decision_for = |node: &crate::expand::ExpandedNode| -> Option<&'static str> {
        selection_active.then(|| {
            if run_ids.contains(&node.id) {
                "run"
            } else {
                "skip"
            }
        })
    };

    if args.json {
        let rows: Vec<serde_json::Value> = nodes
            .iter()
            .map(|node| {
                let (role, parent_id, parent_key) = match &node.role {
                    NodeRole::Root => ("root", None, None),
                    NodeRole::Child {
                        parent_id,
                        parent_key,
                    } => ("child", Some(parent_id.clone()), Some(parent_key.clone())),
                    NodeRole::Discovery { .. } => ("discovery", None, None),
                    NodeRole::Product { .. } => ("product", None, None),
                };
                serde_json::json!({
                    "id": node.id,
                    "source": node.source.kind,
                    "sink": node.sink.kind,
                    "role": role,
                    "parent_id": parent_id,
                    "parent_key": parent_key,
                    "depends_on": &node.depends_on,
                    "delivery": node.delivery_guarantee.to_string(),
                    "status": node.status.as_str(),
                    "tags": &node.tags,
                    "decision": decision_for(node),
                })
            })
            .collect();
        let out = serde_json::json!({
            "valid": true,
            "mode": "matrix",
            "name": cfg.name.as_deref().unwrap_or("(unnamed)"),
            "row_count": nodes.len(),
            "roots": roots,
            "children": children,
            "selection_active": selection_active,
            "rows": rows,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&out).unwrap_or_else(|_| out.to_string())
        );
        // Propagate any selection error after emitting the summary so the exit
        // code still reflects an invalid selection.
        if let Some(sel) = selected {
            sel?;
        }
        return Ok(());
    }

    println!(
        "ok: '{}' rows={} (roots={}, children={}) execution={}",
        cfg.name.as_deref().unwrap_or("(unnamed)"),
        nodes.len(),
        roots,
        children,
        cfg.execution
            .as_ref()
            .map(|e| format!(
                "max_concurrent={:?} on_error={:?}",
                e.max_concurrent.unwrap_or(0),
                e.on_error
            ))
            .unwrap_or_else(|| "(defaults)".to_owned()),
    );
    for node in &nodes {
        println!("{}", row_line(node));
    }

    // The selection report is only printed when the config actually uses the
    // readiness ladder / tags, or a selector was passed — so a plain config's
    // `validate` output is unchanged.
    if selection_active {
        println!(
            "run selection (include_parents={}):",
            selection.include_parents.as_str()
        );
        for node in &nodes {
            let decision = if run_ids.contains(&node.id) {
                "RUN"
            } else {
                "skip"
            };
            let tags = if node.tags.is_empty() {
                String::new()
            } else {
                format!(" tags=[{}]", node.tags.join(", "))
            };
            println!(
                "  - {} status={}{} -> {}",
                node.id,
                node.status.as_str(),
                tags,
                decision
            );
        }
        // Propagate any selection error (empty run set / missing ancestor /
        // unknown token) now that the report has been printed.
        if let Some(sel) = selected {
            sel?;
        }
    }
    Ok(())
}

/// Render one per-row report line for `faucet validate` output.
fn row_line(node: &crate::expand::ExpandedNode) -> String {
    let role = match &node.role {
        NodeRole::Root => "root".to_owned(),
        NodeRole::Child {
            parent_id,
            parent_key,
        } => {
            format!("child of '{parent_id}' (parent_key={parent_key})")
        }
        NodeRole::Discovery { as_alias, .. } => {
            format!("discovery (as={as_alias})")
        }
        NodeRole::Product { dims, .. } => {
            format!("product of [{}]", dims.join(", "))
        }
    };
    let deps = if node.depends_on.is_empty() {
        String::new()
    } else {
        format!(" depends_on=[{}]", node.depends_on.join(", "))
    };
    format!(
        "  - {} [{}] source={} sink={}{} delivery={}",
        node.id, role, node.source.kind, node.sink.kind, deps, node.delivery_guarantee
    )
}

/// Deserialize every row's connector `config` into its typed struct.
///
/// `expand` leaves `config` an opaque `Value`, so a structurally wrong
/// connector block — the wrong nesting under a `#[serde(flatten)]` section, an
/// unknown field under `deny_unknown_fields`, a wrong scalar type, a typo'd
/// name — used to pass `faucet validate` and fail on the first real run
/// (#609). Offline: no credentials are resolved, no pool is built, no
/// connection is opened. Live reachability remains `faucet doctor`'s job.
fn check_connector_configs(nodes: &[crate::expand::ExpandedNode]) -> CliResult<()> {
    for n in nodes {
        crate::registry::validate_source_config(&n.source.kind, &n.id, n.source.config.clone())
            .map_err(|e| CliError::Config(format!("row '{}' source: {e}", n.id)))?;
        // A discovery row has no sink (`NodeRole::Discovery` — it runs its
        // source, projects `select`, and publishes a value set), so the sink
        // slot holds a placeholder. Validating it would reject a perfectly good
        // config for a connector the row never writes to.
        if !matches!(n.role, crate::expand::NodeRole::Discovery { .. }) {
            crate::registry::validate_sink_config(&n.sink.kind, &n.id, n.sink.config.clone())
                .map_err(|e| CliError::Config(format!("row '{}' sink: {e}", n.id)))?;
        }
    }
    Ok(())
}

/// Compile every row's transform chain.
///
/// `expand` only checks the *shape* of a `transforms:` entry, so a misspelled
/// field (`set: { fields: … }` instead of `values:`) or an invalid SQL/WASM stage
/// used to pass validation and then fail on the first page of a real run. Pure and
/// offline — no connector is built.
fn check_transforms(nodes: &[crate::expand::ExpandedNode]) -> CliResult<()> {
    for n in nodes {
        if n.transforms.is_empty() {
            continue;
        }
        crate::transforms::compile_transforms(&n.transforms)
            .map_err(|e| CliError::Config(format!("row '{}': {e}", n.id)))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{check_connector_configs, check_transforms, row_line};
    use crate::expand::expand;

    #[test]
    fn row_line_renders_role_and_depends_on() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
  - id: posts
    parent: dims
    parent_key: id
  - id: facts
    depends_on: [dims]
"#,
            "yaml",
        )
        .unwrap();
        let nodes = expand(&cfg).unwrap();
        let line_for = |id: &str| row_line(nodes.iter().find(|n| n.id == id).unwrap());
        assert_eq!(
            line_for("dims"),
            "  - dims [root] source=rest sink=jsonl delivery=at-least-once"
        );
        assert_eq!(
            line_for("posts"),
            "  - posts [child of 'dims' (parent_key=id)] source=rest sink=jsonl \
             delivery=at-least-once"
        );
        assert_eq!(
            line_for("facts"),
            "  - facts [root] source=rest sink=jsonl depends_on=[dims] delivery=at-least-once"
        );
    }

    #[test]
    fn row_line_reports_derived_effectively_once_guarantees() {
        // Keyed upsert is reported even when the user did not request
        // `delivery: exactly_once` (truthful derived guarantee, #292)…
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#,
            "yaml",
        )
        .unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(
            row_line(&nodes[0]).ends_with("delivery=effectively-once (keyed upsert)"),
            "got: {}",
            row_line(&nodes[0])
        );

        // …and the atomic-watermark mechanism is reported for a CDC → SQL
        // exactly_once topology.
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
delivery: exactly_once
pipeline:
  source:
    type: postgres-cdc
    config: { connection_url: "postgres://localhost/db", slot: s, publication: p }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
  state: { type: file, config: { path: ./state } }
"#,
            "yaml",
        )
        .unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(
            row_line(&nodes[0]).ends_with("delivery=effectively-once (atomic watermark)"),
            "got: {}",
            row_line(&nodes[0])
        );
    }

    #[test]
    fn transform_chains_are_compiled_not_just_shape_checked() {
        // `set` takes `values:`; `fields:` is a plausible-looking typo that used to
        // validate cleanly and then fail on the first page of a real run.
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  transforms:
    - type: set
      config: { fields: { a: 1 } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: rowA
"#,
            "yaml",
        )
        .unwrap();
        let err = check_transforms(&expand(&cfg).unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("rowA"), "names the row: {err}");
        assert!(err.contains("values"), "names the missing field: {err}");

        // A well-formed chain compiles.
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  transforms:
    - type: set
      config: { values: { a: 1 } }
  sink:   { type: jsonl, config: { path: ./o } }
"#,
            "yaml",
        )
        .unwrap();
        check_transforms(&expand(&cfg).unwrap()).unwrap();
    }

    /// #609 — the exact reported shape: `MssqlConnectionConfig` is
    /// `#[serde(flatten)]`, so nesting it under a `connection:` key is wrong.
    /// This used to register, launch, and only fail on the first triggered run
    /// with "MSSQL config requires either connection_url or connection_string".
    #[cfg(feature = "source-mssql")]
    #[test]
    fn a_wrongly_nested_connector_config_is_rejected_at_validate_time() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source:
    type: mssql
    config:
      connection:
        connection_string: "Server=tcp:h,1433;Database=d;User Id=u;Password=p;"
      query: "SELECT 1"
  sink: { type: jsonl, config: { path: ./o } }
matrix:
  - id: rowA
"#,
            "yaml",
        )
        .expect("the document parses — only the connector block is wrong");

        let err = check_connector_configs(&expand(&cfg).expect("expand"))
            .expect_err("a wrongly-nested connector config must be rejected")
            .to_string();
        assert!(err.contains("rowA"), "names the row: {err}");
        assert!(err.contains("source"), "names the side: {err}");
    }

    #[test]
    fn a_well_formed_config_passes() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: rowA
"#,
            "yaml",
        )
        .unwrap();
        check_connector_configs(&expand(&cfg).unwrap()).expect("a valid config must pass");
    }

    #[test]
    fn a_wrong_scalar_type_is_rejected() {
        // Serde catches this one on its own — `concurrency` is a number.
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: s3, config: { bucket: b, concurrency: "ten" } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: rowA
"#,
            "yaml",
        )
        .unwrap();
        let err = check_connector_configs(&expand(&cfg).unwrap())
            .expect_err("a string where a number belongs must be rejected")
            .to_string();
        assert!(err.contains("rowA"), "names the row: {err}");
    }

    /// A key the connector does not declare is a config that reads as doing
    /// something and does nothing — the worst failure shape, because the run
    /// is green. Rejected at validate time since #654 H9, naming the row.
    #[test]
    fn an_unknown_connector_field_fails_validation() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv, no_such_field: 1 } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: rowA
"#,
            "yaml",
        )
        .unwrap();
        let err = check_connector_configs(&expand(&cfg).unwrap())
            .expect_err("an undeclared connector key must not pass validate")
            .to_string();
        assert!(err.contains("rowA"), "names the row: {err}");
        assert!(err.contains("no_such_field"), "names the key: {err}");
    }

    /// The counterpart: a config using only declared keys still passes. Without
    /// this, a bug that rejected everything would leave the test above green.
    #[test]
    fn a_config_using_only_declared_keys_passes() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv, has_headers: true } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: rowA
"#,
            "yaml",
        )
        .unwrap();
        check_connector_configs(&expand(&cfg).unwrap()).expect("a declared-key config must pass");
    }
}

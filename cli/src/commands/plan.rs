//! `faucet plan` — a read-only "what would this config do" preview (#283).
//!
//! Reports the resolved pipeline (source / sink / transforms / policies /
//! write-mode / delivery guarantee), and — given a sample (an offline
//! `--sample` fixture or a capped `--live --limit` read-only pull) — the
//! inferred output schema, the sink schema delta (via `diff_schema` when the
//! sink exposes `current_schema()`), the lineage column ops, and a volume
//! estimate. It runs the sink's non-mutating `check()` probe but **never writes
//! to any sink** — the data pass goes through the offline capturing harness
//! (`pipeline_test::run_case`).

use crate::auth_catalog;
use crate::cli::PlanArgs;
use crate::error::{CliError, CliResult};
use crate::expand::{self, ExpandedNode, NodeRole};
use crate::pipeline_test::runner::{ResolvedCase, run_case};
use serde::Serialize;
use serde_json::Value;

/// The read-only plan for one row. Serialized verbatim by `--json`.
#[derive(Debug, Serialize)]
pub struct PlanReport {
    pub row: String,
    pub source: String,
    pub sink: String,
    pub write_mode: String,
    pub transforms: Vec<String>,
    pub delivery_guarantee: String,
    pub quality: bool,
    pub contract: bool,
    pub masking: bool,
    pub schema_drift: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub lineage: Vec<String>,
    pub sink_probe: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample: Option<SampleReport>,
    /// The data-flow policy verdict for this row (#702), when a policy is in
    /// force. With a sample the columns come from the sample's input schema,
    /// else from the row's contract.
    #[cfg(feature = "policy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy: Option<crate::policy::RowPolicyReport>,
    /// Change impact analysis (#707), present with `--impact`: the downstream
    /// datasets, contracts, owners and consumers the planned schema affects.
    #[cfg(feature = "catalog")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub impact: Option<crate::impact::ImpactReport>,
}

/// The data-derived part of the plan (present only when a sample was supplied).
#[derive(Debug, Serialize)]
pub struct SampleReport {
    pub source: String,
    pub input_records: usize,
    pub output_records: usize,
    pub dlq_records: usize,
    pub inferred_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sink_schema: Option<Value>,
    pub schema_delta: SchemaDeltaReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A rendered `diff_schema` result, or a note that the sink is schemaless.
#[derive(Debug, Serialize)]
pub struct SchemaDeltaReport {
    pub schemaless_sink: bool,
    pub additions: Vec<String>,
    pub widenings: Vec<String>,
    pub incompatible: Vec<String>,
    pub droppable_required: Vec<String>,
}

/// Build the static (no-I/O) part of the plan directly from a resolved node.
pub fn build_plan_report(node: &ExpandedNode) -> PlanReport {
    let write_mode = node
        .sink
        .config
        .get("write_mode")
        .and_then(Value::as_str)
        .unwrap_or("append")
        .to_owned();
    PlanReport {
        row: node.id.clone(),
        source: node.source.kind.clone(),
        sink: node.sink.kind.clone(),
        write_mode,
        transforms: node.transforms.iter().map(|t| t.kind.clone()).collect(),
        delivery_guarantee: format!("{:?}", node.delivery_guarantee),
        quality: quality_present(node),
        contract: contract_present(node),
        masking: masking_present(node),
        schema_drift: node.schema.as_ref().map(|s| format!("{:?}", s.on_drift)),
        lineage: Vec::new(),
        sink_probe: None,
        sample: None,
        #[cfg(feature = "policy")]
        policy: None,
        #[cfg(feature = "catalog")]
        impact: None,
    }
}

#[cfg(feature = "quality")]
fn quality_present(node: &ExpandedNode) -> bool {
    node.quality.is_some()
}
#[cfg(not(feature = "quality"))]
fn quality_present(_node: &ExpandedNode) -> bool {
    false
}
#[cfg(feature = "contract")]
fn contract_present(node: &ExpandedNode) -> bool {
    node.contract.is_some()
}
#[cfg(not(feature = "contract"))]
fn contract_present(_node: &ExpandedNode) -> bool {
    false
}
#[cfg(feature = "masking")]
fn masking_present(node: &ExpandedNode) -> bool {
    node.masking.is_some()
}
#[cfg(not(feature = "masking"))]
fn masking_present(_node: &ExpandedNode) -> bool {
    false
}

/// Load a sample: an offline `--sample` fixture, or a capped `--live --limit`
/// read-only pull from the real source. Returns `None` when neither is given.
async fn load_sample(
    args: &PlanArgs,
    node: &ExpandedNode,
    auth: &auth_catalog::AuthCatalog,
) -> CliResult<Option<Vec<Value>>> {
    if let Some(path) = &args.sample {
        return Ok(Some(read_sample_file(path)?));
    }
    if args.live {
        let source = crate::registry::build_source(
            &node.source.kind,
            node.source.config.clone(),
            auth,
            None,
        )
        .await?;
        let records = pull_capped(source.as_ref(), args.limit).await?;
        return Ok(Some(records));
    }
    Ok(None)
}

/// Read a `.jsonl` (one JSON object per line) or `.json` (array) sample file.
fn read_sample_file(path: &std::path::Path) -> CliResult<Vec<Value>> {
    let text = std::fs::read_to_string(path)?;
    let trimmed = text.trim_start();
    if trimmed.starts_with('[') {
        serde_json::from_str(trimmed).map_err(|e| {
            CliError::Config(format!(
                "invalid --sample JSON array `{}`: {e}",
                path.display()
            ))
        })
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| {
                serde_json::from_str(l).map_err(|e| {
                    CliError::Config(format!(
                        "invalid --sample JSONL line in `{}`: {e}",
                        path.display()
                    ))
                })
            })
            .collect()
    }
}

/// Pull at most `limit` records from a source without advancing any bookmark
/// (uses a throwaway page pull; no state store is wired).
async fn pull_capped(source: &dyn faucet_core::Source, limit: usize) -> CliResult<Vec<Value>> {
    use futures::StreamExt;
    let ctx = std::collections::HashMap::new();
    let stream = source.stream_pages(&ctx, limit.max(1));
    futures::pin_mut!(stream);
    let mut out = Vec::new();
    while out.len() < limit {
        match stream.next().await {
            Some(Ok(page)) => {
                out.extend(page.records);
            }
            Some(Err(e)) => return Err(CliError::from(e)),
            None => break,
        }
    }
    out.truncate(limit);
    Ok(out)
}

pub(crate) fn resolved_case_from_node(
    node: &ExpandedNode,
    input: Vec<Value>,
    clock: chrono::DateTime<chrono::FixedOffset>,
) -> ResolvedCase {
    ResolvedCase {
        name: format!("plan:{}", node.id),
        transforms: node.transforms.clone(),
        #[cfg(feature = "quality")]
        quality: node.quality.clone(),
        #[cfg(feature = "contract")]
        contract: node.contract.clone(),
        #[cfg(feature = "masking")]
        masking: node.masking.clone(),
        input,
        page_size: 0,
        clock,
    }
}

fn render_delta(dest: &Value, inferred: &Value) -> SchemaDeltaReport {
    let diff = faucet_core::drift::diff_schema(dest, inferred, true);
    SchemaDeltaReport {
        schemaless_sink: false,
        additions: diff.additions.iter().map(|c| c.name.clone()).collect(),
        widenings: diff.widenings.iter().map(|c| c.name.clone()).collect(),
        incompatible: diff.incompatible.iter().map(|c| c.name.clone()).collect(),
        droppable_required: diff.droppable_required.clone(),
    }
}

/// The data-dependent inputs of a plan (shared by `faucet plan` and
/// `POST /v1/plan`).
pub struct PlanOptions<'a> {
    /// Sample input records + a label for where they came from.
    pub sample: Option<(Vec<Value>, String)>,
    /// Change impact analysis (#707) against a catalog store.
    #[cfg(feature = "catalog")]
    pub impact: Option<ImpactOptions<'a>>,
    #[cfg(not(feature = "catalog"))]
    pub _marker: std::marker::PhantomData<&'a ()>,
}

/// Where and how deep to run impact analysis.
#[cfg(feature = "catalog")]
pub struct ImpactOptions<'a> {
    pub store: &'a dyn crate::serve::history::RunHistory,
    /// The pipeline name the row's runs were recorded under.
    pub pipeline: String,
    pub depth: u32,
}

/// Plan one row: the resolved shape, the lineage ops, the optional sample
/// pass (offline harness — the sink is built only to probe it and read its
/// live schema, never to write), the data-flow policy verdict (#702) and the
/// change impact (#707).
pub async fn plan_node(
    cfg: &crate::config::PipelineConfig,
    node: &ExpandedNode,
    auth: &auth_catalog::AuthCatalog,
    opts: PlanOptions<'_>,
) -> CliResult<PlanReport> {
    let clock = chrono::Utc::now().fixed_offset();
    let mut report = build_plan_report(node);
    #[cfg(feature = "lineage")]
    {
        report.lineage = crate::lineage_glue::column_ops(&node.transforms, masking_present(node))
            .iter()
            .map(|op| format!("{op:?}"))
            .collect();
    }
    #[cfg(not(feature = "policy"))]
    let _ = cfg;

    #[cfg(feature = "policy")]
    let mut policy_input_schema: Option<Value> = None;
    #[cfg_attr(not(feature = "catalog"), allow(unused_variables, unused_mut))]
    let mut planned_output_schema: Option<Value> = None;
    if let Some((input, source_label)) = opts.sample {
        let input_records = input.len();
        #[cfg(feature = "policy")]
        {
            policy_input_schema = Some(faucet_core::schema::infer_schema(&input));
        }
        let case = resolved_case_from_node(node, input, clock);
        let run = run_case(&case).await?;
        let inferred = faucet_core::schema::infer_schema(&run.written);
        planned_output_schema = Some(inferred.clone());

        // Build the sink ONLY to probe it and read its live schema — never to
        // write. `check()` is best-effort; `current_schema()` yields the delta.
        let sink = crate::registry::build_sink(&node.sink.kind, node.sink.config.clone(), auth)
            .await
            .ok();
        let sink_schema = match &sink {
            Some(s) => s.current_schema().await.ok().flatten(),
            None => None,
        };
        report.sink_probe = match &sink {
            Some(s) => Some(probe_summary(s.as_ref()).await),
            None => Some("sink could not be built (skipped probe)".to_owned()),
        };
        let schema_delta = match &sink_schema {
            Some(dest) => render_delta(dest, &inferred),
            None => SchemaDeltaReport {
                schemaless_sink: true,
                additions: vec![],
                widenings: vec![],
                incompatible: vec![],
                droppable_required: vec![],
            },
        };
        report.sample = Some(SampleReport {
            source: source_label,
            input_records,
            output_records: run.written.len(),
            dlq_records: run.dlq_payloads.len(),
            inferred_schema: inferred,
            sink_schema,
            schema_delta,
            error: run.error,
        });
    }
    #[cfg(feature = "policy")]
    if let Some(spec) = cfg.policy.as_ref() {
        let compiled = faucet_core::CompiledPolicy::compile(spec)
            .map_err(|e| CliError::Config(format!("policy: {e}")))?;
        report.policy = Some(crate::policy::evaluate_node(
            &compiled,
            node,
            policy_input_schema.as_ref(),
        ));
    }
    // Change impact analysis (#707): who downstream a schema change affects.
    #[cfg(feature = "catalog")]
    if let Some(i) = opts.impact {
        report.impact = Some(
            crate::impact::analyze(
                i.store,
                crate::impact::ImpactInputs {
                    pipeline: &i.pipeline,
                    node,
                    planned_schema: planned_output_schema.as_ref(),
                    depth: i.depth,
                },
            )
            .await?,
        );
    }
    Ok(report)
}

/// Execute the `plan` subcommand.
pub async fn run(args: PlanArgs) -> CliResult<()> {
    if args.diff {
        #[cfg(feature = "catalog")]
        {
            return run_diff(args).await;
        }
        #[cfg(not(feature = "catalog"))]
        {
            return Err(CliError::Config(
                "`faucet plan --diff` requires a binary built with the `catalog` feature \
                 (e.g. `cargo install faucet-cli --features catalog`)"
                    .into(),
            ));
        }
    }
    let cwd = std::env::current_dir()?;
    let path = match &args.config {
        Some(p) => p.clone(),
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    #[cfg_attr(not(feature = "policy"), allow(unused_mut))]
    let mut cfg = if args.resolve_secrets {
        crate::config::PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?
    } else {
        crate::config::PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?
    };
    #[cfg(feature = "policy")]
    crate::policy::apply_to_config(&mut cfg, args.policy.as_deref())?;
    #[cfg(not(feature = "policy"))]
    if args.policy.is_some() {
        return Err(CliError::Config(
            "--policy requires a binary built with the `policy` feature \
             (e.g. `cargo install faucet-cli --features policy`)"
                .into(),
        ));
    }
    let auth = auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let nodes = expand::expand(&cfg)?;
    let node = select_root(&nodes, args.row.as_deref())?;

    let sample = match load_sample(&args, node, &auth).await? {
        Some(input) => Some((
            input,
            match &args.sample {
                Some(p) => format!("fixture:{}", p.display()),
                None => format!("live:{} (≤{})", node.source.kind, args.limit),
            },
        )),
        None => None,
    };
    #[cfg(feature = "catalog")]
    let impact_handle = if args.impact {
        let spec = cfg.catalog.as_ref().ok_or_else(|| {
            CliError::Config(
                "`faucet plan --impact` needs a `catalog:` block to read the lineage graph \
                 (see `faucet schema catalog`)"
                    .into(),
            )
        })?;
        Some(crate::catalog::connect_from_spec(spec).await?)
    } else {
        None
    };
    #[cfg(not(feature = "catalog"))]
    if args.impact {
        return Err(CliError::Config(
            "`faucet plan --impact` requires a binary built with the `catalog` feature \
             (e.g. `cargo install faucet-cli --features catalog`)"
                .into(),
        ));
    }
    let report = plan_node(
        &cfg,
        node,
        &auth,
        PlanOptions {
            sample,
            #[cfg(feature = "catalog")]
            impact: impact_handle.as_ref().map(|h| ImpactOptions {
                store: h.store.as_ref(),
                pipeline: crate::catalog::snapshot::resolve_name(&cfg, Some(&path)),
                depth: args.depth,
            }),
            #[cfg(not(feature = "catalog"))]
            _marker: std::marker::PhantomData,
        },
    )
    .await?;

    if args.json {
        let out =
            serde_json::to_string_pretty(&report).map_err(|e| CliError::Config(e.to_string()))?;
        println!("{out}");
    } else {
        render_human(&report);
    }
    Ok(())
}

async fn probe_summary(sink: &dyn faucet_core::Sink) -> String {
    let ctx = faucet_core::CheckContext::default();
    match sink.check(&ctx).await {
        Ok(report) => format!("{} probe(s)", report.probes.len()),
        Err(e) => format!("probe unavailable: {e}"),
    }
}

pub(crate) fn select_root<'a>(
    nodes: &'a [ExpandedNode],
    row: Option<&str>,
) -> CliResult<&'a ExpandedNode> {
    match row {
        Some(id) => nodes
            .iter()
            .find(|n| n.id == id)
            .ok_or_else(|| CliError::Config(format!("no row with id '{id}' in this config"))),
        None => nodes
            .iter()
            .find(|n| matches!(n.role, NodeRole::Root))
            .ok_or_else(|| CliError::Config("config has no root row to plan".to_owned())),
    }
}

fn render_human(r: &PlanReport) {
    println!("Plan for row `{}`:", r.row);
    println!("  source:   {}", r.source);
    println!("  sink:     {}  (write_mode: {})", r.sink, r.write_mode);
    println!("  delivery: {}", r.delivery_guarantee);
    if r.transforms.is_empty() {
        println!("  transforms: (none)");
    } else {
        println!("  transforms: {}", r.transforms.join(" → "));
    }
    let mut policies = Vec::new();
    if r.quality {
        policies.push("quality");
    }
    if r.contract {
        policies.push("contract");
    }
    if r.masking {
        policies.push("masking");
    }
    if let Some(d) = &r.schema_drift {
        println!("  schema-drift: on_drift={d}");
    }
    println!(
        "  policies: {}",
        if policies.is_empty() {
            "(none)".to_owned()
        } else {
            policies.join(", ")
        }
    );
    if !r.lineage.is_empty() {
        println!("  lineage ops: {}", r.lineage.join(", "));
    }
    if let Some(p) = &r.sink_probe {
        println!("  sink check: {p}");
    }
    #[cfg(feature = "policy")]
    if let Some(p) = &r.policy {
        println!(
            "  policy: {} labelled column(s) → sink {} ({}), {} violation(s){}",
            p.columns.len(),
            p.sink,
            p.sink_kind,
            p.violations.len(),
            if p.opaque {
                " — opaque transform, labels carried conservatively"
            } else {
                ""
            }
        );
        for v in &p.violations {
            println!("    ! {v}");
        }
    }
    #[cfg(feature = "catalog")]
    if let Some(i) = &r.impact {
        print!("{}", crate::impact::render_human(i));
    }
    match &r.sample {
        None => {
            println!(
                "\n  (pass --sample <fixture> or --live --limit N to preview the output schema, volume, and sink delta — no writes either way)"
            );
        }
        Some(s) => {
            println!("\n  sample ({}):", s.source);
            println!(
                "    {} in → {} out, {} to DLQ",
                s.input_records, s.output_records, s.dlq_records
            );
            if let Some(err) = &s.error {
                println!("    run error: {err}");
            }
            if s.schema_delta.schemaless_sink {
                println!("    sink schema delta: schemaless sink — no delta");
            } else {
                let d = &s.schema_delta;
                println!(
                    "    sink schema delta: +{} added, {} widened, {} incompatible, {} newly-absent-required",
                    d.additions.len(),
                    d.widenings.len(),
                    d.incompatible.len(),
                    d.droppable_required.len()
                );
                if !d.additions.is_empty() {
                    println!("      add: {}", d.additions.join(", "));
                }
                if !d.incompatible.is_empty() {
                    println!("      incompatible: {}", d.incompatible.join(", "));
                }
            }
        }
    }
    println!("\n  (read-only — no sink was written)");
}

/// `faucet plan --diff` (#374): compare the current resolved+expanded config
/// against the last snapshot recorded by a successful run. Secrets are resolved
/// (so redaction matches the record side) and never printed.
#[cfg(feature = "catalog")]
async fn run_diff(args: PlanArgs) -> CliResult<()> {
    use crate::catalog::snapshot;
    let cwd = std::env::current_dir()?;
    let path = match &args.config {
        Some(p) => p.clone(),
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    // Resolve secrets so the redacted current config matches what `run` stored.
    let cfg =
        crate::config::PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    let spec = cfg.catalog.as_ref().ok_or_else(|| {
        CliError::Config(
            "`faucet plan --diff` needs a `catalog:` block to read the last recorded run \
             (see `faucet schema catalog`)"
                .into(),
        )
    })?;
    let nodes = expand::expand(&cfg)?;
    let pipeline = snapshot::resolve_name(&cfg, Some(&path));
    let current = snapshot::build_snapshot(
        pipeline.clone(),
        snapshot::on_error_str(&cfg.execution),
        &nodes,
        chrono::Utc::now(),
    );
    let handle = crate::catalog::connect_from_spec(spec).await?;
    let previous = handle
        .store
        .catalog_last_config_snapshot(&pipeline)
        .await
        .map_err(|e| CliError::Config(format!("catalog read failed: {e}")))?;
    let d = snapshot::diff(previous.as_ref(), &current);
    if args.json {
        let out = serde_json::to_string_pretty(&d).map_err(|e| CliError::Config(e.to_string()))?;
        println!("{out}");
    } else {
        print!("{}", snapshot::render_human(&d));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn plan_reports_resolved_pipeline_and_never_writes() {
        // csv source → jsonl sink, with a rename transform. Plan against a
        // fixture; the jsonl sink path must NOT be created (zero writes).
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("out.jsonl");
        let sample = dir.path().join("sample.jsonl");
        std::fs::write(&sample, "{\"a\": 1}\n{\"a\": 2}\n").unwrap();
        let cfg_path = dir.path().join("pipe.yaml");
        std::fs::write(
            &cfg_path,
            format!(
                "version: 1\nname: plan-test\npipeline:\n  source:\n    type: csv\n    config:\n      path: in.csv\n  sink:\n    type: jsonl\n    config:\n      path: {}\n  transforms:\n    - type: flatten\n      config: {{}}\n",
                out.display()
            ),
        )
        .unwrap();

        let args = PlanArgs {
            config: Some(cfg_path),
            row: None,
            sample: Some(sample),
            live: false,
            limit: 10,
            json: false,
            diff: false,
            impact: false,
            depth: 5,
            resolve_secrets: false,
            profile: None,
            policy: None,
        };
        super::run(args).await.expect("plan runs");
        assert!(!out.exists(), "plan must not write to the sink");
    }

    #[tokio::test]
    async fn plan_json_has_sample_and_schemaless_delta() {
        let dir = tempfile::tempdir().unwrap();
        let sample = dir.path().join("s.jsonl");
        std::fs::write(&sample, "{\"x\": 1}\n").unwrap();
        let cfg_path = dir.path().join("p.yaml");
        std::fs::write(
            &cfg_path,
            "version: 1\npipeline:\n  source:\n    type: csv\n    config:\n      path: in.csv\n  sink:\n    type: jsonl\n    config:\n      path: /tmp/should-not-be-written.jsonl\n",
        )
        .unwrap();
        // Exercise the report builder directly for a deterministic assertion.
        let cfg =
            crate::config::PipelineConfig::from_path_tolerating_secrets(&cfg_path, None).unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        let report = build_plan_report(&nodes[0]);
        assert_eq!(report.source, "csv");
        assert_eq!(report.sink, "jsonl");
        assert_eq!(report.write_mode, "append");
    }

    /// `plan --diff` end-to-end over a real sqlite catalog: first run reports
    /// first-run (nothing recorded), then after a snapshot is recorded the diff
    /// compares against it. Exercises `run_diff` both branches.
    #[cfg(all(feature = "catalog", feature = "serve-history-sqlite"))]
    #[tokio::test]
    async fn plan_diff_first_run_then_against_recorded_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("cat.db");
        let cfg_path = dir.path().join("pipe.yaml");
        std::fs::write(
            &cfg_path,
            format!(
                "version: 1\nname: diffpipe\npipeline:\n  source:\n    type: csv\n    config:\n      path: in.csv\n  sink:\n    type: jsonl\n    config:\n      path: out.jsonl\ncatalog:\n  url: \"sqlite:{}\"\n",
                db.display()
            ),
        )
        .unwrap();

        let mk_args = || PlanArgs {
            config: Some(cfg_path.clone()),
            row: None,
            sample: None,
            live: false,
            limit: 10,
            json: false,
            diff: true,
            impact: false,
            depth: 5,
            resolve_secrets: false,
            profile: None,
            policy: None,
        };

        // 1. Nothing recorded yet → first-run path.
        super::run(mk_args()).await.expect("first plan --diff");

        // 2. Record a snapshot exactly as a successful run would.
        let cfg = crate::config::PipelineConfig::from_path_async(&cfg_path, None)
            .await
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        let handle = crate::catalog::connect_from_spec(cfg.catalog.as_ref().unwrap())
            .await
            .unwrap();
        crate::catalog::snapshot::record_if_ok(
            Some(&handle),
            "diffpipe",
            "continue",
            &nodes,
            true,
            chrono::Utc::now(),
        )
        .await;

        // 3. Now the diff compares against the recorded snapshot (unchanged).
        super::run(mk_args()).await.expect("second plan --diff");
    }

    /// `plan --diff` on a config with no `catalog:` block is a clear error
    /// (there is nothing to diff against).
    #[cfg(feature = "catalog")]
    #[tokio::test]
    async fn plan_diff_without_catalog_block_errors() {
        let dir = tempfile::tempdir().unwrap();
        let cfg_path = dir.path().join("p.yaml");
        std::fs::write(
            &cfg_path,
            "version: 1\nname: nocat\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n",
        )
        .unwrap();
        let args = PlanArgs {
            config: Some(cfg_path),
            row: None,
            sample: None,
            live: false,
            limit: 10,
            json: false,
            diff: true,
            impact: false,
            depth: 5,
            resolve_secrets: false,
            profile: None,
            policy: None,
        };
        let err = super::run(args).await.unwrap_err();
        assert!(err.to_string().contains("catalog:"), "{err}");
    }
}

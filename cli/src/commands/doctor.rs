//! `faucet doctor` — preflight probes for every connector in a config (#126).
//!
//! Expands the config, builds each **root** invocation's source / sink / state
//! store, and runs their non-mutating `check()` probes concurrently (bounded by
//! a semaphore, each wrapped in a per-probe timeout). Prints a green/red
//! checklist (or `--json`) and exits with the number of failed probes
//! (clamped to 255).
//!
//! Child invocations are listed but not probed: their configs depend on parent
//! records that only exist at run time (same limitation as `faucet preview`).

use crate::auth_catalog::{AuthCatalog, build_auth_catalog};
use crate::cli::DoctorArgs;
use crate::config::{ConnectorSpec, PipelineConfig, StateStoreSpec};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole, expand};
use crate::registry::{build_sink, build_source};
use crate::secrets::registry::redact;
use crate::state::build_state_store;
use faucet_core::check::{CheckContext, CheckReport, Probe, ProbeStatus};
use serde::Serialize;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

/// A single probe enriched with the role + connector it came from. This is the
/// `--json` shape for each probe.
#[derive(Debug, Serialize)]
pub struct ProbeOut {
    pub role: &'static str,
    pub connector: String,
    pub name: &'static str,
    #[serde(flatten)]
    pub status: ProbeStatus,
    pub elapsed_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl ProbeOut {
    fn from_probe(role: &'static str, connector: String, p: Probe) -> Self {
        Self {
            role,
            connector,
            name: p.name,
            status: p.status,
            elapsed_ms: p.elapsed_ms,
            hint: p.hint,
        }
    }
}

/// One expanded invocation and its probes (the `--json` per-invocation shape).
#[derive(Debug, Serialize)]
pub struct InvocationOut {
    pub id: String,
    pub probes: Vec<ProbeOut>,
    /// The derived end-to-end delivery guarantee for this invocation (#292),
    /// e.g. `"effectively-once (atomic watermark)"`. `None` for synthetic
    /// entries (lineage transport).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<String>,
    // Connector kinds for the human header; not part of the JSON contract.
    #[serde(skip)]
    pub source_kind: String,
    #[serde(skip)]
    pub sink_kind: String,
}

/// Execute the `doctor` subcommand.
pub async fn run(args: DoctorArgs) -> CliResult<()> {
    let overall = Instant::now();
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    let t_cfg = Instant::now();
    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    let cfg_ms = t_cfg.elapsed().as_millis();

    let nodes = expand(&cfg)?;

    // `--offline`: run only the static config lints — no connectors built, no
    // network, no credentials. Fast, CI-friendly, and credential-free.
    if args.offline {
        let raw = std::fs::read_to_string(&path).unwrap_or_default();
        let findings = lint_config(&cfg, &nodes, &raw);
        let errors = render_lints(&path, &findings, args.json);
        if errors > 0 {
            return Err(CliError::DoctorFailed { failed: errors });
        }
        return Ok(());
    }

    let auth = build_auth_catalog(cfg.auth.as_ref())?;
    let ctx = CheckContext {
        timeout: Duration::from_secs(args.timeout_secs),
    };
    // Same derivation as `faucet run`, so the SLA probes read the same
    // state keys the executor writes.
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    let roots: Vec<&ExpandedNode> = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root))
        .collect();
    let n_children = nodes.len() - roots.len();

    let mut invocations = probe_roots(&nodes, &auth, &ctx, cfg.sla.as_ref(), &pipeline_name).await;

    // Lineage transport reachability — one pipeline-wide probe (the `lineage:`
    // block is top-level, not per-row), rendered as its own invocation entry so
    // it isn't duplicated across roots.
    #[cfg(feature = "lineage")]
    if let Some(inv) = probe_lineage(cfg.lineage.as_ref()).await {
        invocations.push(inv);
    }

    redact_invocations(&mut invocations);
    let (_passed, failed, _skipped) = tally(&invocations);

    if args.json {
        let v = build_json(&path, overall.elapsed().as_millis(), &invocations);
        println!(
            "{}",
            serde_json::to_string_pretty(&v).expect("doctor json serializes")
        );
    } else {
        render_human(
            cfg_ms,
            roots.len(),
            n_children,
            overall.elapsed(),
            &invocations,
        );
    }

    if failed > 0 {
        return Err(CliError::DoctorFailed { failed });
    }
    Ok(())
}

/// Build the three connectors for one invocation and run their probes. When an
/// `sla:` block is configured, `sla` carries the spec plus the invocation's
/// base state key so the persisted SLA history can be probed read-only
/// (staleness vs `max_staleness_secs`, volume-baseline warm-up).
pub async fn probe_invocation(
    id: String,
    source: ConnectorSpec,
    sink: ConnectorSpec,
    state: Option<StateStoreSpec>,
    auth: &AuthCatalog,
    ctx: &CheckContext,
    sla: Option<(crate::sla::SlaSpec, String)>,
) -> InvocationOut {
    let mut probes = Vec::new();

    match build_source(&source.kind, source.config.clone(), auth, None).await {
        Ok(src) => {
            probes.extend(collect_probes("source", src.connector_name(), ctx, src.check(ctx)).await)
        }
        Err(e) => probes.push(construct_fail("source", &source.kind, &e)),
    }

    match build_sink(&sink.kind, sink.config.clone(), auth).await {
        Ok(snk) => {
            probes.extend(collect_probes("sink", snk.connector_name(), ctx, snk.check(ctx)).await)
        }
        Err(e) => probes.push(construct_fail("sink", &sink.kind, &e)),
    }

    let mut store = None;
    if let Some(spec) = state {
        match build_state_store(&spec).await {
            Ok(st) => {
                probes.extend(collect_probes("state", &spec.kind, ctx, st.check(ctx)).await);
                store = Some(st);
            }
            Err(e) => probes.push(construct_fail("state", &spec.kind, &e)),
        }
    }

    if let Some((spec, base_key)) = sla {
        let now = chrono::Utc::now().timestamp();
        let sla_probes = tokio::time::timeout(
            ctx.timeout,
            crate::sla::doctor_probes(&spec, store.as_ref(), &base_key, now),
        )
        .await
        .unwrap_or_else(|_| {
            vec![Probe::fail(
                "history",
                ctx.timeout,
                "SLA state read timed out",
            )]
        });
        probes.extend(
            sla_probes
                .into_iter()
                .map(|p| ProbeOut::from_probe("sla", "sla".to_string(), p)),
        );
    }

    InvocationOut {
        id,
        probes,
        delivery: None,
        source_kind: source.kind,
        sink_kind: sink.kind,
    }
}

/// Probe the pipeline-wide `lineage:` transport reachability, returning a
/// single-probe invocation (or `None` when no `lineage:` block is configured).
/// A failed probe is diagnostic only — lineage emission never blocks a run.
#[cfg(feature = "lineage")]
pub async fn probe_lineage(
    lineage: Option<&faucet_lineage::LineageConfig>,
) -> Option<InvocationOut> {
    let lc = lineage?;
    let start = Instant::now();
    let probe = match crate::lineage_glue::check_transport(lc).await {
        Ok(_) => Probe::pass("reachable", start.elapsed()),
        Err(reason) => Probe::fail("reachable", start.elapsed(), reason),
    };
    Some(InvocationOut {
        id: "lineage".to_string(),
        delivery: None,
        probes: vec![ProbeOut::from_probe(
            "lineage",
            "openlineage".to_string(),
            probe,
        )],
        source_kind: "—".to_string(),
        sink_kind: "—".to_string(),
    })
}

/// Probe every *root* invocation's source/sink/state concurrently (bounded).
/// Child invocations are skipped — their configs need parent records. Reused by
/// `faucet doctor` and serve's `doctor_first` preflight. `sla` /
/// `pipeline_name` come from the top-level config; when set, each root also
/// gets read-only SLA staleness/baseline probes against its state store.
pub async fn probe_roots(
    nodes: &[ExpandedNode],
    auth: &AuthCatalog,
    ctx: &CheckContext,
    sla: Option<&crate::sla::SlaSpec>,
    pipeline_name: &str,
) -> Vec<InvocationOut> {
    let sem = Arc::new(Semaphore::new(8));
    let mut handles = Vec::new();
    for node in nodes.iter().filter(|n| matches!(n.role, NodeRole::Root)) {
        let id = node.id.clone();
        let source = node.source.clone();
        let sink = node.sink.clone();
        let state = node.state.clone();
        let auth = auth.clone();
        let ctx = ctx.clone();
        let sem = sem.clone();
        let sla = node.sla.as_ref().or(sla).map(|s| {
            (
                s.clone(),
                crate::executor::build_state_key(pipeline_name, &node.id, None),
            )
        });
        let guarantee = node.delivery_guarantee.to_string();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire_owned().await.expect("semaphore not closed");
            let mut inv = probe_invocation(id, source, sink, state, &auth, &ctx, sla).await;
            inv.delivery = Some(guarantee);
            inv
        }));
    }
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(h.await.expect("doctor probe task panicked"));
    }
    out
}

/// Total number of failed probes across all invocations.
pub fn count_failures(invs: &[InvocationOut]) -> usize {
    invs.iter()
        .flat_map(|i| &i.probes)
        .filter(|p| matches!(p.status, ProbeStatus::Fail { .. }))
        .count()
}

/// Run one connector's `check()` future under the timeout, mapping the report
/// (or an outer error / timeout) into role-tagged [`ProbeOut`]s.
async fn collect_probes(
    role: &'static str,
    connector: &str,
    ctx: &CheckContext,
    fut: impl std::future::Future<Output = Result<CheckReport, faucet_core::FaucetError>>,
) -> Vec<ProbeOut> {
    let start = Instant::now();
    let report = match tokio::time::timeout(ctx.timeout, fut).await {
        Err(_) => CheckReport::single(Probe::fail("timeout", start.elapsed(), "check timed out")),
        Ok(Ok(r)) => r,
        Ok(Err(e)) => CheckReport::single(Probe::fail("check", start.elapsed(), e.to_string())),
    };
    report
        .probes
        .into_iter()
        .map(|p| ProbeOut::from_probe(role, connector.to_string(), p))
        .collect()
}

/// A `construct` failure: the connector could not even be built from its config.
/// Accepts any `Display` error (build errors are `CliError`; the unit test uses
/// `FaucetError`).
fn construct_fail(role: &'static str, kind: &str, e: impl std::fmt::Display) -> ProbeOut {
    ProbeOut::from_probe(
        role,
        kind.to_string(),
        Probe::fail("construct", Duration::ZERO, e.to_string()),
    )
}

/// Scrub resolved secrets out of every probe `reason` / `hint`.
pub fn redact_invocations(invs: &mut [InvocationOut]) {
    for inv in invs.iter_mut() {
        for p in inv.probes.iter_mut() {
            match &mut p.status {
                ProbeStatus::Fail { reason } | ProbeStatus::Skip { reason } => {
                    *reason = redact(reason).into_owned();
                }
                ProbeStatus::Pass => {}
            }
            if let Some(h) = &mut p.hint {
                *h = redact(h).into_owned();
            }
        }
    }
}

/// Count (passed, failed, skipped) probes across all invocations.
fn tally(invs: &[InvocationOut]) -> (usize, usize, usize) {
    let (mut p, mut f, mut s) = (0usize, 0usize, 0usize);
    for inv in invs {
        for pr in &inv.probes {
            match pr.status {
                ProbeStatus::Pass => p += 1,
                ProbeStatus::Fail { .. } => f += 1,
                ProbeStatus::Skip { .. } => s += 1,
            }
        }
    }
    (p, f, s)
}

/// Build the `--json` envelope.
fn build_json(config: &Path, total_ms: u128, invs: &[InvocationOut]) -> serde_json::Value {
    let (passed, failed, skipped) = tally(invs);
    serde_json::json!({
        "config": config.display().to_string(),
        "invocations": invs,
        "summary": {
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "elapsed_ms": total_ms,
        }
    })
}

/// Render the human checklist to stdout.
fn render_human(
    cfg_ms: u128,
    n_roots: usize,
    n_children: usize,
    total: Duration,
    invs: &[InvocationOut],
) {
    println!("✓ Config parses and interpolates{:>34} ms", cfg_ms);
    println!(
        "✓ Matrix expands to {} invocation{}{:>22} skipped (children)",
        n_roots,
        if n_roots == 1 { "" } else { "s" },
        n_children
    );

    for inv in invs {
        println!();
        println!(
            "▸ Invocation {}  (source={}, sink={}{})",
            inv.id,
            inv.source_kind,
            inv.sink_kind,
            inv.delivery
                .as_deref()
                .map(|d| format!(", delivery={d}"))
                .unwrap_or_default()
        );
        for p in &inv.probes {
            let (sym, extra) = match &p.status {
                ProbeStatus::Pass => ("✓", String::new()),
                ProbeStatus::Fail { reason } => ("✗", format!(" ({reason})")),
                ProbeStatus::Skip { reason } => ("•", format!(" (skip: {reason})")),
            };
            println!(
                "  {} {:6} [{}] {}{}{:>8} ms",
                sym, p.role, p.connector, p.name, extra, p.elapsed_ms
            );
            if let Some(hint) = &p.hint {
                println!("        hint: {hint}");
            }
        }
    }

    let (passed, failed, skipped) = tally(invs);
    println!();
    println!(
        "Summary: {passed} passed, {failed} failed, {skipped} skipped       total elapsed {:.1}s",
        total.as_secs_f64()
    );
}

// ── Offline config linter (#392) ─────────────────────────────────────────────

/// Severity of a static config-lint finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LintSeverity {
    /// A likely-broken config; counts toward the non-zero exit code.
    Error,
    /// A smell worth flagging; does not fail the lint.
    Warning,
}

/// One static-lint finding: a severity, a short stable `code`, a message, and a
/// fix hint.
#[derive(Debug, Clone, Serialize)]
pub struct LintFinding {
    pub severity: LintSeverity,
    pub code: &'static str,
    pub message: String,
    pub hint: String,
}

impl LintFinding {
    fn error(code: &'static str, message: String, hint: impl Into<String>) -> Self {
        Self {
            severity: LintSeverity::Error,
            code,
            message,
            hint: hint.into(),
        }
    }
    fn warning(code: &'static str, message: String, hint: impl Into<String>) -> Self {
        Self {
            severity: LintSeverity::Warning,
            code,
            message,
            hint: hint.into(),
        }
    }
}

/// File/append sinks where `batch_size` is a documented no-op (they write
/// per-record regardless).
const NO_OP_BATCH_SINKS: [&str; 3] = ["jsonl", "csv", "stdout"];

/// Run the offline static lints over a resolved config + its expanded nodes.
/// Pure: no I/O beyond the `raw` config text passed in for `${vars.*}` usage
/// scanning. Reused by `faucet doctor --offline`.
pub(crate) fn lint_config(
    cfg: &PipelineConfig,
    nodes: &[ExpandedNode],
    raw: &str,
) -> Vec<LintFinding> {
    let mut out = Vec::new();

    // Every `auth: { ref: NAME }` referenced by any node's source or sink.
    let mut referenced: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for n in nodes {
        for spec in [&n.source, &n.sink] {
            if let Some(name) = crate::auth_catalog::auth_ref(&spec.config) {
                referenced.insert(name);
            }
        }
    }
    let catalog: std::collections::BTreeSet<String> = cfg
        .auth
        .as_ref()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();

    // (1) Dangling auth ref — a connector references a provider not in `auth:`.
    for name in &referenced {
        if !catalog.contains(name) {
            out.push(LintFinding::error(
                "dangling-auth-ref",
                format!("a connector references auth provider '{name}', which is not defined in the top-level `auth:` catalog"),
                format!("add an `auth:` entry named '{name}', or fix the `auth: {{ ref: … }}` to match an existing provider"),
            ));
        }
    }

    // (2) Unreferenced auth provider — a catalog entry no connector uses.
    for name in &catalog {
        if !referenced.contains(name) {
            out.push(LintFinding::warning(
                "unreferenced-auth-provider",
                format!("auth provider '{name}' is defined in `auth:` but never referenced by any connector"),
                format!("reference it with `auth: {{ ref: {name} }}` on a connector, or remove the unused entry"),
            ));
        }
    }

    // (3) Unused vars — a `vars:` key never interpolated as `${vars.KEY}`.
    if let Some(vars) = &cfg.vars {
        for key in vars.keys() {
            let token = format!("${{vars.{key}}}");
            if !raw.contains(&token) {
                out.push(LintFinding::warning(
                    "unused-var",
                    format!("`vars.{key}` is defined but never used (no `${{vars.{key}}}` reference found)"),
                    "remove the unused variable, or reference it where intended",
                ));
            }
        }
    }

    // (4) No-op sink `batch_size: 0` on file/append sinks.
    for n in nodes {
        if NO_OP_BATCH_SINKS.contains(&n.sink.kind.as_str())
            && n.sink.config.get("batch_size").and_then(|v| v.as_u64()) == Some(0)
        {
            out.push(LintFinding::warning(
                "noop-batch-size",
                format!(
                    "row '{}': sink `{}` sets `batch_size: 0`, which has no effect (this sink writes per-record)",
                    n.id, n.sink.kind
                ),
                "drop `batch_size` from this sink — it only matters for batching sinks (databases, object stores, bulk APIs)",
            ));
        }
    }

    out
}

/// Render lint findings (human or `--json`) and return the number of *errors*
/// (warnings don't count toward the exit code).
fn render_lints(path: &Path, findings: &[LintFinding], json: bool) -> usize {
    let errors = findings
        .iter()
        .filter(|f| f.severity == LintSeverity::Error)
        .count();
    let warnings = findings.len() - errors;

    if json {
        let v = serde_json::json!({
            "config": path.display().to_string(),
            "errors": errors,
            "warnings": warnings,
            "findings": findings,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&v).expect("lint json serializes")
        );
        return errors;
    }

    println!("Config lint: {}", path.display());
    if findings.is_empty() {
        println!("  ✓ no issues found");
        return 0;
    }
    for f in findings {
        let tag = match f.severity {
            LintSeverity::Error => "error",
            LintSeverity::Warning => "warn",
        };
        println!("  [{tag}] {} — {}", f.code, f.message);
        println!("         hint: {}", f.hint);
    }
    println!();
    println!("Lint: {errors} error(s), {warnings} warning(s)");
    errors
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe_out(role: &'static str, name: &'static str, status: ProbeStatus) -> ProbeOut {
        ProbeOut {
            role,
            connector: "x".into(),
            name,
            status,
            elapsed_ms: 1,
            hint: None,
        }
    }

    fn inv(probes: Vec<ProbeOut>) -> InvocationOut {
        InvocationOut {
            id: "default".into(),
            probes,
            delivery: None,
            source_kind: "rest".into(),
            sink_kind: "stdout".into(),
        }
    }

    #[test]
    fn tally_counts_each_status() {
        let invs = vec![inv(vec![
            probe_out("source", "read", ProbeStatus::Pass),
            probe_out("sink", "auth", ProbeStatus::Fail { reason: "x".into() }),
            probe_out(
                "state",
                "sentinel",
                ProbeStatus::Skip {
                    reason: "n/a".into(),
                },
            ),
            probe_out("sink", "schema", ProbeStatus::Fail { reason: "y".into() }),
        ])];
        assert_eq!(tally(&invs), (1, 2, 1));
    }

    #[test]
    fn json_has_summary_and_invocations() {
        let invs = vec![inv(vec![probe_out("source", "read", ProbeStatus::Pass)])];
        let v = build_json(Path::new("pipeline.yaml"), 100, &invs);
        assert_eq!(v["config"], "pipeline.yaml");
        assert_eq!(v["invocations"][0]["id"], "default");
        assert_eq!(v["invocations"][0]["probes"][0]["role"], "source");
        assert_eq!(v["invocations"][0]["probes"][0]["status"], "pass");
        assert_eq!(v["summary"]["passed"], 1);
        assert_eq!(v["summary"]["failed"], 0);
        assert_eq!(v["summary"]["elapsed_ms"], 100);
    }

    #[test]
    fn redaction_scrubs_secret_in_reason_and_hint() {
        crate::secrets::registry::register("supersecretvalue");
        let mut invs = vec![inv(vec![ProbeOut {
            role: "sink",
            connector: "postgres".into(),
            name: "auth",
            status: ProbeStatus::Fail {
                reason: "login failed for supersecretvalue".into(),
            },
            elapsed_ms: 5,
            hint: Some("token supersecretvalue rejected".into()),
        }])];
        redact_invocations(&mut invs);
        if let ProbeStatus::Fail { reason } = &invs[0].probes[0].status {
            assert!(
                !reason.contains("supersecretvalue"),
                "reason not redacted: {reason}"
            );
        } else {
            panic!("expected fail");
        }
        assert!(
            !invs[0].probes[0]
                .hint
                .as_ref()
                .unwrap()
                .contains("supersecretvalue")
        );
    }

    #[test]
    fn construct_fail_is_a_fail_probe() {
        let e = faucet_core::FaucetError::Config("bad".into());
        let p = construct_fail("sink", "postgres", &e);
        assert_eq!(p.name, "construct");
        assert!(matches!(p.status, ProbeStatus::Fail { .. }));
        assert_eq!(p.connector, "postgres");
    }

    #[test]
    fn count_failures_sums_across_invocations() {
        let invs = vec![
            inv(vec![
                probe_out("source", "read", ProbeStatus::Pass),
                probe_out("sink", "auth", ProbeStatus::Fail { reason: "x".into() }),
            ]),
            inv(vec![probe_out(
                "sink",
                "auth",
                ProbeStatus::Fail { reason: "y".into() },
            )]),
        ];
        assert_eq!(count_failures(&invs), 2);
    }

    // ── Offline linter (#392) ────────────────────────────────────────────────

    /// Parse a YAML config, expand it, and run the offline lints.
    fn lint_yaml(text: &str) -> Vec<LintFinding> {
        let cfg = crate::config::parse_with_extension(text, "yaml").expect("config parses");
        let nodes = expand(&cfg).expect("config expands");
        lint_config(&cfg, &nodes, text)
    }

    fn has(findings: &[LintFinding], code: &str) -> bool {
        findings.iter().any(|f| f.code == code)
    }

    #[test]
    fn clean_config_has_no_findings() {
        let cfg = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x", auth: { ref: idp } } }
  sink: { type: jsonl, config: { path: out.jsonl } }
auth:
  idp: { type: static, config: { token: "${env:T}" } }
"#;
        assert!(lint_yaml(cfg).is_empty(), "{:?}", lint_yaml(cfg));
    }

    #[test]
    fn flags_dangling_auth_ref_as_error() {
        let cfg = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x", auth: { ref: missing } } }
  sink: { type: jsonl, config: { path: out.jsonl } }
"#;
        let f = lint_yaml(cfg);
        assert!(has(&f, "dangling-auth-ref"));
        assert!(
            f.iter()
                .any(|x| x.code == "dangling-auth-ref" && x.severity == LintSeverity::Error)
        );
    }

    #[test]
    fn flags_unreferenced_auth_provider_as_warning() {
        let cfg = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x" } }
  sink: { type: jsonl, config: { path: out.jsonl } }
auth:
  unused_idp: { type: static, config: { token: "t" } }
"#;
        let f = lint_yaml(cfg);
        let p = f.iter().find(|x| x.code == "unreferenced-auth-provider");
        assert!(p.is_some());
        assert_eq!(p.unwrap().severity, LintSeverity::Warning);
    }

    #[test]
    fn flags_unused_var() {
        let cfg = r#"
version: 1
vars:
  used: "https://x"
  never: 5
pipeline:
  source: { type: rest, config: { base_url: "${vars.used}" } }
  sink: { type: jsonl, config: { path: out.jsonl } }
"#;
        let f = lint_yaml(cfg);
        assert!(has(&f, "unused-var"));
        // Only `never` is flagged — `used` is referenced.
        assert!(
            f.iter()
                .any(|x| x.code == "unused-var" && x.message.contains("never"))
        );
        assert!(
            !f.iter()
                .any(|x| x.code == "unused-var" && x.message.contains("vars.used"))
        );
    }

    #[test]
    fn flags_noop_batch_size_on_file_sink() {
        let cfg = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x" } }
  sink: { type: jsonl, config: { path: out.jsonl, batch_size: 0 } }
"#;
        let f = lint_yaml(cfg);
        let p = f.iter().find(|x| x.code == "noop-batch-size");
        assert!(p.is_some());
        assert_eq!(p.unwrap().severity, LintSeverity::Warning);
    }

    #[test]
    fn render_lints_counts_errors_only() {
        let findings = vec![
            LintFinding::error("dangling-auth-ref", "x".into(), "h"),
            LintFinding::warning("unused-var", "y".into(), "h"),
        ];
        // JSON branch.
        assert_eq!(render_lints(Path::new("faucet.yaml"), &findings, true), 1);
        // Human branch (also exercises the empty-findings path).
        assert_eq!(render_lints(Path::new("faucet.yaml"), &findings, false), 1);
        assert_eq!(render_lints(Path::new("faucet.yaml"), &[], false), 0);
    }

    fn write_cfg(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("faucet.yaml");
        std::fs::write(&path, body).expect("write");
        (dir, path)
    }

    fn offline_args(path: std::path::PathBuf) -> DoctorArgs {
        DoctorArgs {
            config: Some(path),
            env_file: None,
            no_env_file: true,
            timeout_secs: 5,
            json: false,
            offline: true,
            profile: None,
        }
    }

    #[tokio::test]
    async fn offline_run_ok_on_clean_config() {
        let (_d, path) = write_cfg(
            "version: 1\npipeline:\n  source: { type: rest, config: { base_url: x } }\n  sink: { type: jsonl, config: { path: o } }\n",
        );
        super::run(offline_args(path)).await.expect("clean lint ok");
    }

    #[tokio::test]
    async fn offline_run_errors_on_dangling_auth_ref() {
        let (_d, path) = write_cfg(
            "version: 1\npipeline:\n  source: { type: rest, config: { base_url: x, auth: { ref: nope } } }\n  sink: { type: jsonl, config: { path: o } }\n",
        );
        let err = super::run(offline_args(path)).await;
        assert!(matches!(err, Err(CliError::DoctorFailed { failed }) if failed >= 1));
    }
}

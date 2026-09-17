//! Two-phase snapshot→CDC orchestration. Reuses `executor::run_expanded` for
//! each phase; seeds the CDC bookmark from a position captured before the
//! snapshot so the handoff has no gap (idempotent at the boundary under
//! `write_mode: upsert`).

use crate::auth_catalog::AuthCatalog;
use crate::config::{ExecutionSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::{ExpandedNode, expand};
use crate::registry::build_source;
use crate::replication::compiled::CompiledReplication;
use crate::replication::state::{
    Phase, Plan, ReplicationState, cdc_state_key, marker_key, plan_from_marker,
};
use crate::state::build_state_store;
use chrono::{DateTime, FixedOffset};
use tokio_util::sync::CancellationToken;

/// Inputs for one `faucet replicate` invocation.
pub struct ReplicationOptions {
    pub pipeline_name: String,
    pub execution: Option<ExecutionSpec>,
    pub auth: AuthCatalog,
    pub clock: DateTime<FixedOffset>,
    /// Optional resilience policy, applied to both the snapshot and CDC phases.
    pub resilience: Option<faucet_core::ResiliencePolicy>,
    /// Optional freshness/volume SLA (#202), evaluated after each phase's runs.
    pub sla: Option<crate::sla::SlaSpec>,
    /// Optional completeness reconciliation (#502), evaluated after each phase's runs.
    pub reconcile: Option<crate::reconcile::ReconcileSpec>,
    /// Optional notifier (#280), shared across both phases' runs.
    #[cfg(feature = "notify")]
    pub notifier: Option<std::sync::Arc<crate::notify::Notifier>>,
    /// Optional Data Movement Catalog store (#279), recorded into after both
    /// the snapshot and each CDC phase run.
    #[cfg(feature = "catalog")]
    pub catalog: Option<crate::catalog::CatalogHandle>,
}

/// Build the snapshot-phase node by cloning the CDC node and swapping in the
/// bulk-read source. The snapshot always runs at-least-once (the query source
/// is not exactly-once-capable; upsert makes re-runs idempotent).
///
/// The `cdc_unwrap` transform is dropped from the snapshot node: it normalizes a
/// CDC change-event envelope (`{op, before, after, …}`) and would silently drop
/// the snapshot's plain table rows (no `after`/`op` image). The snapshot source
/// already yields destination-shaped rows, so it must reach the sink directly;
/// any other (non-`cdc_unwrap`) transforms are kept so common shaping still
/// applies to both phases.
pub(crate) fn build_snapshot_node(
    cdc_node: &ExpandedNode,
    snapshot_source: crate::config::ConnectorSpec,
) -> ExpandedNode {
    let mut n = cdc_node.clone();
    n.id = "snapshot".to_string();
    n.source = snapshot_source;
    n.delivery = faucet_core::DeliveryMode::AtLeastOnce;
    // Keep the derived-guarantee report truthful for the forced-at-least-once
    // snapshot phase — unless the sink dedups by key, in which case the
    // snapshot inherits keyed-upsert effectively-once (the recommended
    // `write_mode: upsert` mirror setup).
    if n.delivery_guarantee
        != faucet_core::DeliveryGuarantee::EffectivelyOnce(
            faucet_core::EffectivelyOnceMechanism::KeyedUpsert,
        )
    {
        n.delivery_guarantee = faucet_core::DeliveryGuarantee::AtLeastOnce;
    }
    n.transforms.retain(|t| t.kind != "cdc_unwrap");
    n
}

/// Build a descriptive error from a failed phase, surfacing the first
/// underlying invocation error. The executor only emits that error via
/// `tracing::error!`, which a caller without a subscriber (e.g. a test, or a
/// `faucet validate`-style path) would otherwise lose — collapsing the failure
/// to an opaque count. Naming the phase + the real error makes replication
/// failures diagnosable.
fn phase_failure(summary: &crate::executor::RunSummary, phase: &str) -> CliError {
    let detail = summary
        .invocations
        .iter()
        .find_map(|i| i.error.clone())
        .unwrap_or_else(|| "unknown error".to_string());
    CliError::Internal(format!("replication {phase} phase failed: {detail}"))
}

/// Build a fresh `ExecuteOptions` for one phase run.
fn make_opts(opts: &ReplicationOptions, cancel: Option<CancellationToken>) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: opts.pipeline_name.clone(),
        run_id: None,
        execution: opts.execution.clone(),
        dry_run: false,
        limit: None,
        state_path_override: None,
        shard: None,
        auth: opts.auth.clone(),
        clock: opts.clock,
        cancel,
        resilience: opts.resilience.clone(),
        sla: opts.sla.clone(),
        reconcile: opts.reconcile.clone(),
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: opts.notifier.clone(),
        #[cfg(feature = "catalog")]
        catalog: opts.catalog.clone(),
    }
}

/// Spawn a task that cancels `token` on SIGTERM (Unix) or Ctrl-C. Shared
/// with the backfill orchestrator.
pub(crate) fn spawn_cancel_on_signal(token: CancellationToken) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            match signal(SignalKind::terminate()) {
                Ok(mut sigterm) => {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = sigterm.recv() => {}
                    }
                }
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        token.cancel();
    });
}

/// Run the two-phase replication. Validates, bootstraps (captures + seeds the
/// CDC position), snapshots (if not already done), then streams CDC (looping
/// until SIGTERM when `continuous`).
pub async fn run_replication(
    cfg: &PipelineConfig,
    compiled: &CompiledReplication,
    opts: ReplicationOptions,
) -> CliResult<()> {
    // expand() runs the generic gates (exactly-once, write_mode×sink). With no
    // matrix (enforced by CompiledReplication) there is exactly one node.
    let mut nodes = expand(cfg)?;
    let mut cdc_node = nodes
        .drain(..)
        .next()
        .ok_or_else(|| CliError::Internal("replication: expand produced no node".into()))?;
    cdc_node.id = "cdc".to_string();
    let snapshot_node = build_snapshot_node(&cdc_node, compiled.snapshot_source.clone());

    let state_spec = cfg
        .pipeline
        .state
        .as_ref()
        .ok_or_else(|| CliError::Config("replication requires a state store".into()))?;
    let store = build_state_store(state_spec).await?;

    let marker_k = marker_key(&opts.pipeline_name);
    let cdc_k = cdc_state_key(&opts.pipeline_name);

    let marker = match store.get(&marker_k).await? {
        Some(v) => Some(ReplicationState::from_value(v)?),
        None => None,
    };

    // ── Bootstrap: capture the CDC position before the snapshot ──────────────
    if plan_from_marker(marker.as_ref()) == Plan::Bootstrap {
        let cdc_source = build_source(
            &cdc_node.source.kind,
            cdc_node.source.config.clone(),
            &opts.auth,
            None,
        )
        .await?;
        let position = cdc_source.capture_resume_position().await?.ok_or_else(|| {
            CliError::Config(format!(
                "replication: source '{}' does not support position capture",
                cdc_node.source.kind
            ))
        })?;
        // Seed the CDC bookmark (bare value — exactly-once's unwrap_state reads a
        // bare value as seq=0, so this works for both delivery modes) and record
        // the phase marker.
        store.put(&cdc_k, &position).await?;
        store
            .put(
                &marker_k,
                &ReplicationState {
                    phase: Phase::Snapshot,
                    snapshot_done: false,
                    position: position.clone(),
                }
                .to_value()?,
            )
            .await?;
        tracing::info!(pipeline = %opts.pipeline_name, "replication bootstrap: captured CDC position, seeded bookmark");
    }

    // Re-read the marker (it now exists). Decide the remaining work.
    let marker = match store.get(&marker_k).await? {
        Some(v) => ReplicationState::from_value(v)?,
        None => {
            return Err(CliError::Internal(
                "replication: marker missing after bootstrap".into(),
            ));
        }
    };

    // Install graceful-shutdown handling UP FRONT — before the snapshot phase
    // — so a SIGTERM / Ctrl-C during a long snapshot cancels cooperatively and
    // lets the sink flush at the next page boundary, instead of hard-killing
    // the process mid-write (F40). The same token feeds both the snapshot and
    // CDC runs (`faucet schedule` installs its handler up front the same way).
    let cancel = CancellationToken::new();
    spawn_cancel_on_signal(cancel.clone());

    // ── Snapshot phase (idempotent redo on resume) ───────────────────────────
    if !marker.snapshot_done {
        tracing::info!(pipeline = %opts.pipeline_name, "replication: running snapshot phase (Ctrl-C / SIGTERM to stop)");
        let summary = run_expanded(
            vec![snapshot_node.clone()],
            make_opts(&opts, Some(cancel.clone())),
        )
        .await?;
        if summary.had_failures() {
            return Err(phase_failure(&summary, "snapshot"));
        }
        // A SIGTERM mid-snapshot flushes a *partial* result — the snapshot is
        // NOT complete. Do not mark `snapshot_done`: a restart redoes the whole
        // snapshot idempotently from the bootstrap position (F40). Marking it
        // done here would skip the un-snapshotted rows on restart (CDC only
        // replays changes after the captured position, not pre-existing rows).
        if cancel.is_cancelled() {
            tracing::warn!(
                pipeline = %opts.pipeline_name,
                "replication: snapshot interrupted by shutdown before completion; \
                 it will be redone on the next run"
            );
            return Ok(());
        }
        store
            .put(
                &marker_k,
                &ReplicationState {
                    phase: Phase::Cdc,
                    snapshot_done: true,
                    position: marker.position.clone(),
                }
                .to_value()?,
            )
            .await?;
        tracing::info!(pipeline = %opts.pipeline_name, "replication: snapshot complete; handing off to CDC");
    }

    // ── CDC phase (loop until SIGTERM when continuous) ───────────────────────
    if compiled.continuous {
        tracing::info!(pipeline = %opts.pipeline_name, "replication: streaming CDC (Ctrl-C / SIGTERM to stop)");
    }
    // In continuous mode the CDC phase is an always-on mirror: a long-lived
    // CDC connection routinely hits transient failures (network blips, server
    // restarts, slot read errors). Those must NOT crash-exit the mirror — the
    // run loops, re-running `run_expanded` which resumes from the persisted
    // bookmark (lossless: the bookmark only advances after the pipeline
    // persists, so a retry replays nothing already committed). We log, back off
    // (capped, reset on a clean cycle), and continue. A one-shot run
    // (`continuous: false`) still surfaces the error to the caller (F20).
    let mut backoff = std::time::Duration::from_secs(1);
    const MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(60);
    loop {
        let cycle: CliResult<()> = async {
            let summary = run_expanded(
                vec![cdc_node.clone()],
                make_opts(&opts, Some(cancel.clone())),
            )
            .await?;
            if summary.had_failures() {
                return Err(phase_failure(&summary, "CDC"));
            }
            Ok(())
        }
        .await;

        match cdc_loop_action(cycle.is_ok(), compiled.continuous, cancel.is_cancelled()) {
            CdcLoopAction::Break => break,
            CdcLoopAction::Continue => {
                backoff = std::time::Duration::from_secs(1); // reset on a clean cycle
            }
            CdcLoopAction::Propagate => return Err(cycle.unwrap_err()),
            CdcLoopAction::Backoff => {
                tracing::warn!(
                    pipeline = %opts.pipeline_name,
                    error = %cycle.unwrap_err(),
                    backoff_secs = backoff.as_secs(),
                    "replication: CDC cycle failed; resuming from bookmark after backoff"
                );
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => break,
                    _ = tokio::time::sleep(backoff) => {}
                }
                backoff = (backoff * 2).min(MAX_BACKOFF);
            }
        }
    }
    Ok(())
}

/// What the CDC phase loop should do after one `run_expanded` cycle (F20).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CdcLoopAction {
    /// Stop the loop (one-shot success, or cancelled).
    Break,
    /// Clean cycle in continuous mode — reset backoff and loop again.
    Continue,
    /// Surface the error to the caller (one-shot failure, or cancelled).
    Propagate,
    /// Transient failure in continuous mode — back off and resume (the mirror
    /// must not crash-exit on a routine network blip / server restart).
    Backoff,
}

/// Pure decision for the CDC phase loop. In continuous mode a cycle failure is
/// recoverable (re-running resumes from the persisted bookmark, replaying
/// nothing already committed); a one-shot run still surfaces the error.
fn cdc_loop_action(cycle_ok: bool, continuous: bool, cancelled: bool) -> CdcLoopAction {
    match (cycle_ok, continuous && !cancelled) {
        (true, false) => CdcLoopAction::Break, // one-shot or cancelled success
        (true, true) => CdcLoopAction::Continue, // keep mirroring
        (false, false) => CdcLoopAction::Propagate, // one-shot or cancelled failure
        (false, true) => CdcLoopAction::Backoff, // transient — resume after backoff
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectorSpec;
    use crate::expand::expand;

    fn cdc_node() -> ExpandedNode {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: postgres-cdc, config: { connection_url: "postgres://x", slot_name: s, publication_name: p } }
  sink:   { type: postgres, config: { connection_url: "postgres://y", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
  state:  { type: file, config: { path: ./st } }
"#,
            "yaml",
        )
        .unwrap();
        expand(&cfg).unwrap().into_iter().next().unwrap()
    }

    #[test]
    fn cdc_loop_action_continuous_resumes_on_transient_failure() {
        // F20: a continuous mirror backs off and resumes on a cycle failure
        // instead of crash-exiting; a clean cycle keeps mirroring.
        assert_eq!(cdc_loop_action(false, true, false), CdcLoopAction::Backoff);
        assert_eq!(cdc_loop_action(true, true, false), CdcLoopAction::Continue);
        // Cancellation stops the loop either way.
        assert_eq!(cdc_loop_action(true, true, true), CdcLoopAction::Break);
        assert_eq!(cdc_loop_action(false, true, true), CdcLoopAction::Propagate);
        // One-shot runs are unchanged: success stops, failure surfaces.
        assert_eq!(cdc_loop_action(true, false, false), CdcLoopAction::Break);
        assert_eq!(
            cdc_loop_action(false, false, false),
            CdcLoopAction::Propagate
        );
    }

    #[test]
    fn snapshot_node_swaps_source_and_forces_at_least_once() {
        let mut cdc = cdc_node();
        cdc.id = "cdc".into();
        cdc.delivery = faucet_core::DeliveryMode::ExactlyOnce;
        let snap_src = ConnectorSpec {
            kind: "postgres".into(),
            config: serde_json::json!({ "connection_url": "postgres://x", "query": "SELECT * FROM t" }),
            transforms: None,
            inherit_transforms: true,
            status: None,
            tags: Vec::new(),
            complete_for: None,
        };
        let node = build_snapshot_node(&cdc, snap_src);
        assert_eq!(node.id, "snapshot");
        assert_eq!(node.source.kind, "postgres");
        assert_eq!(node.sink.kind, "postgres"); // sink preserved
        assert_eq!(node.delivery, faucet_core::DeliveryMode::AtLeastOnce);
    }

    #[test]
    fn snapshot_node_strips_cdc_unwrap_but_keeps_other_transforms() {
        // The CDC pipeline normalizes envelopes with `cdc_unwrap` and then maybe
        // shapes further (e.g. `flatten`). The snapshot source yields plain table
        // rows, so `cdc_unwrap` (which would drop them) must be dropped while the
        // other transforms are preserved.
        let mut cdc = cdc_node();
        cdc.id = "cdc".into();
        cdc.transforms = vec![
            crate::config::TransformSpec {
                kind: "cdc_unwrap".into(),
                config: serde_json::json!({}),
            },
            crate::config::TransformSpec {
                kind: "flatten".into(),
                config: serde_json::json!({ "separator": "_" }),
            },
        ];
        let snap_src = ConnectorSpec {
            kind: "postgres".into(),
            config: serde_json::json!({ "connection_url": "postgres://x", "query": "SELECT * FROM t" }),
            transforms: None,
            inherit_transforms: true,
            status: None,
            tags: Vec::new(),
            complete_for: None,
        };
        let node = build_snapshot_node(&cdc, snap_src);
        let kinds: Vec<&str> = node.transforms.iter().map(|t| t.kind.as_str()).collect();
        assert_eq!(kinds, vec!["flatten"], "cdc_unwrap dropped, flatten kept");
    }

    #[test]
    fn phase_failure_surfaces_phase_and_underlying_error() {
        let summary = crate::executor::RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "snapshot".into(),
                parent_record_key: None,
                records_written: 0,
                error: Some("connection refused".into()),
                error_kind: None,
                metrics: None,
            }],
        };
        let err = phase_failure(&summary, "snapshot");
        assert!(matches!(err, CliError::Internal(_)), "{err:?}");
        let msg = format!("{err}");
        assert!(msg.contains("snapshot"), "phase named: {msg}");
        assert!(
            msg.contains("connection refused"),
            "underlying error: {msg}"
        );
    }

    #[test]
    fn phase_failure_falls_back_to_unknown_error() {
        // A failed run whose invocation carries no error string still produces a
        // descriptive error (the count-only failure case).
        let summary = crate::executor::RunSummary {
            invocations: vec![crate::executor::InvocationOutcome {
                row_id: "cdc".into(),
                parent_record_key: None,
                records_written: 0,
                error: None,
                error_kind: None,
                metrics: None,
            }],
        };
        let err = phase_failure(&summary, "CDC");
        let msg = format!("{err}");
        assert!(msg.contains("CDC"), "phase named: {msg}");
        assert!(msg.contains("unknown error"), "fallback used: {msg}");
    }
}

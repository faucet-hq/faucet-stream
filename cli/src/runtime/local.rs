//! The operations on an already-loaded config: what a CLI command runs with
//! [`Principal::local`], and what the server runs after its admission stages.
//! The engine of each lives in its own module; this is the one door to it.

use super::Principal;
use crate::auth_catalog::AuthCatalog;
use crate::commands::plan::{PlanOptions, PlanReport};
use crate::config::PipelineConfig;
use crate::error::CliResult;
use crate::expand::ExpandedNode;

/// Plan one row: its resolved connectors, transforms, guarantees and — with
/// a sample — the output it would write. Writes nothing.
pub async fn plan(
    principal: &Principal,
    cfg: &PipelineConfig,
    node: &ExpandedNode,
    auth: &AuthCatalog,
    opts: PlanOptions<'_>,
) -> CliResult<PlanReport> {
    super::under(
        principal,
        crate::commands::plan::plan_node(cfg, node, auth, opts),
    )
    .await
}

/// Compare the destination to the source by content; with `repair`, re-sync
/// the differences through the sink under the config's governance.
pub async fn verify(
    principal: &Principal,
    cfg: &PipelineConfig,
    spec: &crate::verify::VerifySpec,
    inputs: crate::verify::VerifyInputs,
) -> CliResult<crate::verify::VerifyOutcome> {
    super::under(principal, crate::verify::verify(cfg, spec, inputs)).await
}

/// Undo a run of `cfg`, found by its invocation id.
pub async fn rollback(
    principal: &Principal,
    cfg: &PipelineConfig,
    inputs: crate::rollback::RollbackInputs,
) -> CliResult<crate::rollback::RollbackReport> {
    super::under(principal, crate::rollback::rollback(cfg, inputs)).await
}

/// [`rollback`] over already-expanded nodes (a tenant's, scoped at load).
pub async fn rollback_nodes(
    principal: &Principal,
    nodes: &[ExpandedNode],
    inputs: &crate::rollback::RollbackInputs,
) -> CliResult<crate::rollback::RollbackReport> {
    super::under(principal, async {
        let (node, store, marker) = crate::rollback::locate(
            nodes,
            &inputs.pipeline_name,
            &inputs.run_id,
            inputs.row.as_deref(),
        )
        .await?;
        crate::rollback::rollback_node(&node, store, marker, inputs).await
    })
    .await
}

/// Re-run DLQ records through the referenced config's sink.
pub async fn replay_dlq(
    principal: &Principal,
    cfg: &PipelineConfig,
    location: &str,
    inputs: crate::dlq_replay::ReplayInputs<'_>,
) -> CliResult<crate::dlq_replay::ReplayOutcome> {
    super::under(principal, crate::dlq_replay::replay(cfg, location, inputs)).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(dir: &std::path::Path) -> PipelineConfig {
        let csv = dir.join("in.csv");
        std::fs::write(&csv, "id,name\n1,a\n").unwrap();
        PipelineConfig::from_text(&format!(
            "version: 1\nname: rt\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
            csv.display(),
            dir.join("out.jsonl").display()
        ), &dir.join("rt.yaml"))
        .unwrap()
    }

    #[cfg(all(feature = "source-csv", feature = "sink-jsonl"))]
    #[tokio::test]
    async fn plan_reports_the_row() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());
        let nodes = crate::expand::expand(&cfg).unwrap();
        let report = plan(
            &Principal::local(),
            &cfg,
            &nodes[0],
            &AuthCatalog::default(),
            PlanOptions {
                sample: None,
                #[cfg(feature = "catalog")]
                impact: None,
                #[cfg(not(feature = "catalog"))]
                _marker: std::marker::PhantomData,
            },
        )
        .await
        .unwrap();
        assert_eq!(report.row, nodes[0].id);
    }

    #[cfg(all(feature = "source-csv", feature = "sink-jsonl"))]
    #[tokio::test]
    async fn rollback_without_a_marker_names_the_run() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = cfg(dir.path());
        let err = rollback(
            &Principal::local(),
            &cfg,
            crate::rollback::RollbackInputs {
                run_id: "nope".into(),
                row: None,
                dry_run: true,
                force: false,
                pipeline_name: "rt".into(),
                auth: AuthCatalog::default(),
            },
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string().contains("nope") || err.to_string().contains("state"),
            "{err}"
        );
    }
}

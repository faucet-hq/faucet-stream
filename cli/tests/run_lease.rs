//! A second run of a row is refused while another run holds its live lease
//! (#789 MSG-42), unless forced.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use faucet_cli::pipeline_state::lease;
use faucet_core::{FileStateStore, StateStore};
use serde_json::json;
use std::sync::Arc;

fn opts(force: bool) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: force,
        pipeline_name: "leased".into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: Default::default(),
        shard: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        cancel: None,
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
        usage: Default::default(),
        budget: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_lease_refuses_a_second_run_until_forced() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.jsonl");
    std::fs::write(&input, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let state = dir.path().join("state");
    let doc = json!({
        "version": 1,
        "name": "leased",
        "pipeline": {
            "source": {"type": "file", "config": {"path": input}},
            "sink": {"type": "file", "config": {"path": dir.path().join("out.jsonl")}},
            "state": {"type": "file", "config": {"path": state}},
        }
    });
    let cfg =
        PipelineConfig::from_text(&doc.to_string(), std::path::Path::new("leased.json")).unwrap();
    let nodes = expand(&cfg).unwrap();
    let base = format!("leased::{}", nodes[0].id);
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(&state));
    let other = lease::acquire(Arc::clone(&store), &base, "other-run")
        .await
        .unwrap();

    let summary = run_expanded(nodes, opts(false)).await.unwrap();
    let err = summary.invocations[0].error.clone().expect("refused");
    assert!(
        err.contains("other-run") && err.contains("--force"),
        "{err}"
    );
    assert_eq!(summary.invocations[0].records_written, 0);
    assert!(
        !dir.path().join("out.jsonl").exists(),
        "nothing was written"
    );
    assert_eq!(
        lease::read(store.as_ref(), &base)
            .await
            .unwrap()
            .unwrap()
            .run_id,
        "other-run",
        "the refused run leaves the holder's lease alone"
    );

    let summary = run_expanded(expand(&cfg).unwrap(), opts(true))
        .await
        .unwrap();
    assert!(summary.invocations[0].error.is_none(), "{summary:?}");
    assert_eq!(summary.invocations[0].records_written, 2);
    other.release().await;
}

/// A source that never yields: the run it belongs to holds its lease until
/// it is aborted.
struct Stuck;

#[faucet_core::async_trait]
impl faucet_core::Source for Stuck {
    async fn fetch_with_context(
        &self,
        _ctx: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<serde_json::Value>, faucet_core::FaucetError> {
        std::future::pending().await
    }
}

/// #789: a run that crashes (its task aborted) never blocks the row: the
/// next run starts at once, without `--force`, and writes every record.
#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_run_never_blocks_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.jsonl");
    std::fs::write(&input, "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let state = dir.path().join("state");
    let out = dir.path().join("out.jsonl");
    let doc = json!({
        "version": 1,
        "name": "leased",
        "pipeline": {
            "source": {"type": "file", "config": {"path": input}},
            "sink": {"type": "file", "config": {"path": out}},
            "state": {"type": "file", "config": {"path": state}},
        }
    });
    let cfg =
        PipelineConfig::from_text(&doc.to_string(), std::path::Path::new("leased.json")).unwrap();
    let mut nodes = expand(&cfg).unwrap();
    let base = format!("leased::{}", nodes[0].id);
    nodes[0].source_override = Some(faucet_cli::dlq_replay::reader::SourceOverride::new(
        Box::new(Stuck),
    ));
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(&state));
    let crashed = tokio::spawn(run_expanded(nodes, opts(false)));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while lease::read(store.as_ref(), &base).await.unwrap().is_none() {
        assert!(
            std::time::Instant::now() < deadline,
            "the run took no lease"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    crashed.abort();
    let _ = crashed.await;

    let summary = run_expanded(expand(&cfg).unwrap(), opts(false))
        .await
        .unwrap();
    assert!(
        summary.invocations[0].error.is_none(),
        "resume is not refused: {summary:?}"
    );
    assert_eq!(summary.invocations[0].records_written, 2);
    let written = std::fs::read_to_string(&out).unwrap();
    assert_eq!(written.lines().count(), 2, "{written}");
    let lease_now = lease::read(store.as_ref(), &base).await.unwrap();
    assert!(
        lease_now.is_none_or(|l| !l.is_live(chrono::Utc::now())),
        "the finished run released the lease"
    );
}

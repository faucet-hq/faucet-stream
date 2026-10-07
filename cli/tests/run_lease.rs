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

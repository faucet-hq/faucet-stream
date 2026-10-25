//! How the executor reports invocations that did not run to completion:
//! rows a cancel kept from starting, and `--limit` previews.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, InvocationErrorKind, run_expanded};
use faucet_cli::expand::expand;
use faucet_core::CancellationToken;
use std::path::Path;

fn opts(name: &str) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: false,
        pipeline_name: name.into(),
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

fn load(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("outcomes.yaml")).unwrap()
}

/// An external cancel (serve cancel / timeout / drain) on an `on_error: stop`
/// config skips the rows it kept from starting, and says the run was
/// cancelled rather than blaming `on_error: stop`.
#[tokio::test]
async fn rows_an_external_cancel_kept_from_starting_say_so() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id\n1\n2\n").unwrap();
    let cfg = load(&format!(
        r#"version: 1
name: cancelled
execution: {{ on_error: stop }}
pipeline:
  source: {{ type: csv, config: {{ path: "{input}" }} }}
  sink: {{ type: jsonl, config: {{ path: "{dir}/out-a.jsonl" }} }}
matrix:
  - id: a
  - id: b
    sink: {{ config: {{ path: "{dir}/out-b.jsonl" }} }}
"#,
        input = input.display(),
        dir = dir.path().display(),
    ));
    let token = CancellationToken::new();
    token.cancel();
    let mut o = opts("cancelled");
    o.cancel = Some(token);
    o.execution = cfg.execution.clone();
    let summary = run_expanded(expand(&cfg).unwrap(), o).await.unwrap();
    assert_eq!(summary.invocations.len(), 2);
    for inv in &summary.invocations {
        assert_eq!(
            inv.error_kind,
            Some(InvocationErrorKind::Skipped),
            "{inv:?}"
        );
        let err = inv.error.as_deref().unwrap();
        assert!(err.contains("the run was cancelled"), "{err}");
        assert!(!err.contains("on_error"), "{err}");
    }
    assert!(!summary.had_failures());
}

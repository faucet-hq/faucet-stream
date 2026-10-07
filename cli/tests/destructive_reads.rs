//! A queue source that acks/deletes as it reads must never be read by a
//! command that does not durably write what it reads (#789 MSG-01): a
//! `--dry-run` or `--limit` run is refused before the source is polled. The
//! SQS source builds offline and the endpoint is unreachable, so a run that
//! got as far as polling would fail with a connection error instead.
#![cfg(all(feature = "source-sqs", feature = "sink-stdout"))]

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;

const CONFIG: &str = r#"
version: 1
name: queue_preview
pipeline:
  source:
    type: sqs
    config:
      queue_url: "http://127.0.0.1:1/000000000000/orders"
      region: us-east-1
      endpoint_url: "http://127.0.0.1:1"
      credentials: { type: access_key, config: { access_key_id: test, secret_access_key: test } }
      idle_timeout_secs: 1
  sink:
    type: stdout
    config: {}
"#;

fn opts(dry_run: bool, limit: Option<usize>) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        pipeline_name: "queue_preview".into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run,
        limit,
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

async fn run_error(dry_run: bool, limit: Option<usize>) -> String {
    let cfg = PipelineConfig::from_text(CONFIG, std::path::Path::new("p.yaml")).unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts(dry_run, limit)).await.unwrap();
    summary.invocations[0]
        .error
        .clone()
        .expect("the run must be refused")
}

#[tokio::test]
async fn a_dry_run_of_a_queue_source_is_refused_before_it_reads() {
    let err = run_error(true, None).await;
    assert!(err.contains("`sqs` source removes messages"), "{err}");
    assert!(err.contains("--dry-run"), "{err}");
}

#[tokio::test]
async fn a_limit_run_of_a_queue_source_is_refused_before_it_reads() {
    let err = run_error(false, Some(5)).await;
    assert!(err.contains("--limit"), "{err}");
}

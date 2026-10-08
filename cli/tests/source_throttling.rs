//! Source-side throttling reaches the usage record through the real CLI path
//! (#734): config → `expand` → `run_expanded`, with the source wrapped by the
//! executor's state-key override and a transform chain — both must forward the
//! pipeline's round-trip recorder to the REST connector.
#![cfg(all(feature = "source-rest", feature = "sink-jsonl"))]

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use serde_json::json;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

struct TwiceThrottled(Arc<AtomicUsize>);

impl Respond for TwiceThrottled {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        if self.0.fetch_add(1, Ordering::SeqCst) < 2 {
            ResponseTemplate::new(429).insert_header("Retry-After", "1")
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}, {"id": 2}]}))
        }
    }
}

fn opts() -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: false,
        pipeline_name: "throttled".into(),
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

#[tokio::test]
async fn throttling_is_on_the_usage_record_and_the_summary_line() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(TwiceThrottled(hits.clone()))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let yaml = format!(
        r#"version: 1
name: throttled
pipeline:
  source:
    type: rest
    config:
      base_url: "{uri}"
      path: /items
      records_path: "$.data[*]"
      state_key: items
  transforms:
    - type: flatten
  sink:
    type: jsonl
    config: {{ path: "{out}" }}
  state:
    type: file
    config: {{ path: "{state}" }}
"#,
        uri = server.uri(),
        out = dir.path().join("out.jsonl").display(),
        state = dir.path().join("state").display(),
    );
    let cfg = PipelineConfig::from_text(&yaml, Path::new("throttled.yaml")).unwrap();
    let summary = run_expanded(expand(&cfg).unwrap(), opts()).await.unwrap();
    let inv = &summary.invocations[0];
    assert!(inv.error.is_none(), "{inv:?}");
    assert_eq!(hits.load(Ordering::SeqCst), 3);

    let record = inv.usage.as_ref().expect("usage record");
    assert_eq!(record.usage.records_written, 2);
    assert_eq!(record.usage.throttled, 2);
    assert!(
        (1.9..4.0).contains(&record.usage.throttle_wait_secs),
        "{}",
        record.usage.throttle_wait_secs
    );
    assert_eq!(record.usage.source_retries["rate_limited"], 2);
    assert_eq!(record.usage.source_roundtrips["page"], 3);

    let line = faucet_cli::usage::summary_line(record);
    assert!(line.contains("; throttled 2× · waited 2."), "{line}");

    let report = faucet_cli::usage::model::aggregate(
        std::slice::from_ref(record),
        faucet_cli::usage::model::GroupBy::Pipeline,
        "USD",
    );
    assert_eq!(report.total.throttled, 2);
    assert!(faucet_cli::usage::render_report(&report).contains("throttled 2×"));
}

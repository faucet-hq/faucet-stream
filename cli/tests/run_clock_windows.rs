//! The run clock (`--clock`, a schedule tick, a backfill unit) bounds REST
//! datetime window slicing (#769): a run enumerates the windows up to its
//! clock, not up to the wall clock.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use faucet_core::ReplicationMethod;
use faucet_source_rest::{RestStreamConfig, WindowSpec};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn opts(clock: &str) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        pipeline_name: "clocked".into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: Default::default(),
        shard: None,
        auth: Default::default(),
        clock: chrono::DateTime::parse_from_rfc3339(clock).unwrap(),
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

fn config(uri: &str, dir: &std::path::Path) -> String {
    let window: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "query", "name": "start_date", "format": "date"},
        "upper": {"into": "query", "name": "end_date", "format": "date"},
    }))
    .unwrap();
    let rest = RestStreamConfig::new(uri, "/report")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .start_replication_value(json!("2024-01-01"))
        .window(window);
    let doc = json!({
        "version": 1,
        "name": "clocked",
        "pipeline": {
            "source": {"type": "rest", "config": serde_json::to_value(rest).unwrap()},
            "sink": {"type": "jsonl", "config": {"path": dir.join("out.jsonl"), "append": true}},
            "state": {"type": "file", "config": {"path": dir.join("state")}},
        }
    });
    serde_json::to_string(&doc).unwrap()
}

async fn start_dates(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter_map(|r| {
            r.url
                .query_pairs()
                .find(|(k, _)| k == "start_date")
                .map(|(_, v)| v.into_owned())
        })
        .collect()
}

#[tokio::test]
async fn the_run_clock_bounds_window_slicing() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/report"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1, "updated_at": "2024-01-01T12:00:00Z"}]
        })))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let cfg = PipelineConfig::from_text(
        &config(&server.uri(), dir.path()),
        &dir.path().join("p.json"),
    )
    .unwrap();
    let nodes = expand(&cfg).unwrap();

    // Clocked at 2024-01-04: exactly the three windows before it, whatever
    // the wall clock says.
    let summary = run_expanded(nodes.clone(), opts("2024-01-04T00:00:00Z"))
        .await
        .unwrap();
    assert!(!summary.had_failures(), "{:?}", summary.invocations);
    assert_eq!(
        start_dates(&server).await,
        ["2024-01-01", "2024-01-02", "2024-01-03"]
    );

    // A clock before the persisted bookmark enumerates no windows: a clean,
    // zero-record run that requests nothing.
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/report"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
        .mount(&server)
        .await;
    let summary = run_expanded(nodes.clone(), opts("2024-01-02T00:00:00Z"))
        .await
        .unwrap();
    assert!(!summary.had_failures(), "{:?}", summary.invocations);
    assert!(start_dates(&server).await.is_empty());

    // The bookmark did not move backwards: a later clock resumes at 01-04.
    let summary = run_expanded(nodes, opts("2024-01-06T00:00:00Z"))
        .await
        .unwrap();
    assert!(!summary.had_failures(), "{:?}", summary.invocations);
    assert_eq!(start_dates(&server).await, ["2024-01-04", "2024-01-05"]);
}

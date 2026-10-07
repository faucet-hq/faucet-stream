//! End-to-end resumable-replication test for `RestStream`.
//!
//! Proves the full state-store loop: a first run fetches all records and
//! persists the maximum bookmark; a second run with the same `state_key`
//! pulls the bookmark out of the store, calls `apply_start_bookmark` on the
//! REST source, and the source filters out everything at or below it —
//! emitting only the newer records and advancing the bookmark.

use async_trait::async_trait;
use faucet_core::state::{FileStateStore, StateStore};
use faucet_core::{FaucetError, Pipeline, ReplicationMethod, Sink, Source, Value};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// In-memory sink that records every batch it sees so the test can inspect
/// exactly which records made it past the source's incremental filter.
#[derive(Default)]
struct RecordingSink {
    received: Mutex<Vec<Value>>,
}

impl RecordingSink {
    fn new() -> Self {
        Self::default()
    }
    fn snapshot(&self) -> Vec<Value> {
        self.received.lock().unwrap().clone()
    }
}

#[async_trait]
impl Sink for RecordingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.received
            .lock()
            .unwrap()
            .extend(records.iter().cloned());
        Ok(records.len())
    }
}

fn rest_config(server_uri: &str) -> RestStreamConfig {
    RestStreamConfig::new(server_uri, "/api/events")
        .records_path("$.items[*]")
        .pagination(PaginationStyle::None)
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .state_key("rest_events_stream")
}

#[tokio::test]
async fn rest_source_resumes_from_file_state_store_across_runs() {
    let server = MockServer::start().await;

    // ── Run 1: server returns three records, none filtered. ─────────────────
    let initial_payload = json!({
        "items": [
            {"id": 1, "updated_at": "2026-01-01"},
            {"id": 2, "updated_at": "2026-02-01"},
            {"id": 3, "updated_at": "2026-03-01"},
        ]
    });
    let initial_mock = Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(initial_payload))
        .expect(1)
        .mount_as_scoped(&server)
        .await;

    let state_dir = TempDir::new().unwrap();
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(state_dir.path()));

    let source1 = RestStream::new(rest_config(&server.uri())).unwrap();
    let sink1 = RecordingSink::new();
    let result1 = Pipeline::new(&source1, &sink1)
        .with_state_store(Arc::clone(&store))
        .run()
        .await
        .unwrap();
    drop(initial_mock);

    assert_eq!(result1.records_written, 3, "first run sees all records");
    assert_eq!(result1.bookmark, Some(json!("2026-03-01")));
    assert_eq!(
        store
            .get("rest_events_stream")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!("2026-03-01")),
        "bookmark persisted to the file state store"
    );
    let r1 = sink1.snapshot();
    assert_eq!(r1.len(), 3);
    assert_eq!(r1[0]["id"], 1);
    assert_eq!(r1[2]["id"], 3);

    // ── Run 2: server returns all four records (the original three plus a
    // newer one). The stored bookmark should filter the original three back
    // out so only the new record reaches the sink, and the bookmark advances.
    let resumed_payload = json!({
        "items": [
            {"id": 1, "updated_at": "2026-01-01"},
            {"id": 2, "updated_at": "2026-02-01"},
            {"id": 3, "updated_at": "2026-03-01"},
            {"id": 4, "updated_at": "2026-04-01"},
        ]
    });
    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(resumed_payload))
        .expect(1)
        .mount(&server)
        .await;

    let source2 = RestStream::new(rest_config(&server.uri())).unwrap();
    let sink2 = RecordingSink::new();
    let result2 = Pipeline::new(&source2, &sink2)
        .with_state_store(Arc::clone(&store))
        .run()
        .await
        .unwrap();

    assert_eq!(
        result2.records_written, 2,
        "the new record and the one at the bookmark (inclusive filter, API-19) pass"
    );
    assert_eq!(result2.bookmark, Some(json!("2026-04-01")));
    assert_eq!(
        store
            .get("rest_events_stream")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!("2026-04-01")),
        "bookmark advances to the new max"
    );
    let r2 = sink2.snapshot();
    assert_eq!(r2.len(), 2);
    assert_eq!(r2[0]["id"], 3);
    assert_eq!(r2[1]["id"], 4);
    assert_eq!(r2[1]["updated_at"], "2026-04-01");
}

#[tokio::test]
async fn rest_source_with_state_store_but_no_stored_bookmark_emits_all_records() {
    // First-run-ever scenario: state store exists but has no entry yet. The
    // source must behave exactly like a non-incremental run and persist the
    // bookmark for next time.
    let server = MockServer::start().await;
    let payload = json!({
        "items": [
            {"id": 10, "updated_at": "2026-05-01"},
            {"id": 11, "updated_at": "2026-06-01"},
        ]
    });
    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(payload))
        .mount(&server)
        .await;

    let state_dir = TempDir::new().unwrap();
    let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(state_dir.path()));
    assert!(store.get("rest_events_stream").await.unwrap().is_none());

    let source = RestStream::new(rest_config(&server.uri())).unwrap();
    let sink = RecordingSink::new();
    let result = Pipeline::new(&source, &sink)
        .with_state_store(Arc::clone(&store))
        .run()
        .await
        .unwrap();

    assert_eq!(result.records_written, 2);
    assert_eq!(
        store
            .get("rest_events_stream")
            .await
            .unwrap()
            .map(|v| faucet_core::state_version::peel_versioned(&v)),
        Some(json!("2026-06-01"))
    );
}

#[tokio::test]
async fn rest_source_apply_start_bookmark_overrides_config_value() {
    // Direct unit test of the override semantics: if both the static config
    // value and a runtime bookmark are present, the runtime value wins.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": 1, "updated_at": "2026-01-01"},
                {"id": 2, "updated_at": "2026-02-01"},
                {"id": 3, "updated_at": "2026-03-01"},
            ]
        })))
        .mount(&server)
        .await;

    let config = rest_config(&server.uri()).start_replication_value(json!("2026-01-01"));
    let source = RestStream::new(config).unwrap();

    // With only the static config value, Jan (at the bookmark) + Feb + Mar pass.
    let (records, _) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 3);

    // After applying a stricter runtime bookmark, Feb (at it) and March pass.
    source
        .apply_start_bookmark(json!("2026-02-01"))
        .await
        .unwrap();
    let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["id"], 3);
    assert_eq!(bookmark, Some(json!("2026-03-01")));
}

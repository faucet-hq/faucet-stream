//! Integration tests for [`ElasticsearchSink::write_batch_partial`].
//!
//! Each test stands up a wiremock server, mounts a `_bulk` endpoint, and
//! drives the sink — asserting on per-row [`faucet_core::RowOutcome`]s.

use faucet_core::{FaucetError, Sink};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn errors_false_returns_all_ok() {
    let server = MockServer::start().await;
    let body = json!({
        "errors": false,
        "items": [
            { "index": { "status": 201 } },
            { "index": { "status": 201 } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| o.is_ok()));
}

#[tokio::test]
async fn item_level_errors_route_by_index() {
    let server = MockServer::start().await;
    let body = json!({
        "errors": true,
        "items": [
            { "index": { "status": 201 } },
            { "index": { "status": 400, "error": { "reason": "mapping" } } },
            { "index": { "status": 201 } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2}), json!({"a": 3})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
    assert!(outcomes[2].is_ok());
}

#[tokio::test]
async fn http_failure_bubbles_outer_err() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;
    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let result = sink.write_batch_partial(&[json!({"a": 1})]).await;
    assert!(matches!(result, Err(FaucetError::HttpStatus { .. })));
}

/// F14 regression: in upsert mode, a `_bulk` response where SOME items
/// succeeded and SOME failed must return per-row outcomes marking ONLY the
/// failed items as `Err` — the succeeded items stay `Ok`, so `OnBatchError`
/// `DlqAll` will not re-route already-applied rows to the DLQ.
#[tokio::test]
async fn upsert_item_errors_route_per_row_not_outer_err() {
    use faucet_core::{WriteMode, WriteSpec};

    let server = MockServer::start().await;
    // 3 upserts: item 0 ok, item 1 rejected, item 2 ok. Request order is the
    // upsert order (ids 1, 2, 3).
    let body = json!({
        "errors": true,
        "items": [
            { "index": { "_id": "1", "status": 200 } },
            { "index": { "_id": "2", "status": 400, "error": { "reason": "mapper_parsing_exception" } } },
            { "index": { "_id": "3", "status": 201 } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(cfg).unwrap();

    let outcomes = sink
        .write_batch_partial(&[
            json!({"id": 1, "v": "a"}),
            json!({"id": 2, "v": "b"}),
            json!({"id": 3, "v": "c"}),
        ])
        .await
        .expect("item-level errors must NOT collapse to an outer Err");

    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok(), "row 0 applied → must stay Ok");
    assert!(outcomes[1].is_err(), "row 1 rejected → must be Err");
    assert!(outcomes[2].is_ok(), "row 2 applied → must stay Ok");
    // Exactly one failure — the succeeded rows are NOT duplicated as failures.
    assert_eq!(outcomes.iter().filter(|o| o.is_err()).count(), 1);
}

/// F14: a mix of upsert and delete actions. Bulk items are ordered upserts
/// first then deletes (the body order). A failed delete must map back to its
/// original page index, leaving the succeeded upsert `Ok`.
#[tokio::test]
async fn upsert_delete_mixed_per_row_attribution() {
    use faucet_core::{DeleteMarker, WriteMode, WriteSpec};

    let server = MockServer::start().await;
    // page: [upsert id=1, delete id=2]. Body: index(1) then delete(2).
    // item 0 (upsert id=1) ok; item 1 (delete id=2) rejected.
    let body = json!({
        "errors": true,
        "items": [
            { "index": { "_id": "1", "status": 200 } },
            { "delete": { "_id": "2", "status": 500, "error": { "reason": "shard_failure" } } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: Some(DeleteMarker {
                field: "__op".to_string(),
                values: vec!["d".to_string()],
            }),
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(cfg).unwrap();

    let outcomes = sink
        .write_batch_partial(&[json!({"id": 1, "v": "a"}), json!({"id": 2, "__op": "d"})])
        .await
        .expect("item errors must not collapse to outer Err");

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_ok(), "upsert applied → Ok");
    assert!(
        outcomes[1].is_err(),
        "delete rejected → Err at original index 1"
    );
}

/// F14: last-write-wins dedup means several input rows can map to one bulk
/// action. If that action fails, every original index that fed into it must be
/// marked `Err` (the final write for that key failed).
#[tokio::test]
async fn upsert_dedup_failure_fails_all_origin_indices() {
    use faucet_core::{WriteMode, WriteSpec};

    let server = MockServer::start().await;
    // page has id=1 twice → one deduped upsert action that the server rejects.
    let body = json!({
        "errors": true,
        "items": [
            { "index": { "_id": "1", "status": 400, "error": { "reason": "bad" } } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(cfg).unwrap();

    let outcomes = sink
        .write_batch_partial(&[json!({"id": 1, "v": "old"}), json!({"id": 1, "v": "new"})])
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 2);
    assert!(
        outcomes[0].is_err(),
        "both rows feed the failed deduped action"
    );
    assert!(outcomes[1].is_err());
}

/// F14: a missing-key row stays `Err` at its original index (the realistic DLQ
/// case) while a valid upsert that the server accepts stays `Ok`.
#[tokio::test]
async fn upsert_missing_key_failed_others_ok() {
    use faucet_core::{WriteMode, WriteSpec};

    let server = MockServer::start().await;
    let body = json!({
        "errors": false,
        "items": [ { "index": { "_id": "1", "status": 200 } } ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(cfg).unwrap();

    // Row 0 valid, row 1 missing the key column entirely.
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 1, "v": "a"}), json!({"v": "no-key"})])
        .await
        .unwrap();

    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_ok());
    assert!(
        outcomes[1].is_err(),
        "missing-key row routed per-row to DLQ"
    );
}

/// F14: an HTTP-level failure (whole chunk unsendable) still surfaces as an
/// outer `Err` in upsert mode — that is a genuine "nothing landed" abort.
#[tokio::test]
async fn upsert_http_failure_bubbles_outer_err() {
    use faucet_core::{WriteMode, WriteSpec};

    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig {
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
        ..ElasticsearchSinkConfig::new(server.uri(), "idx")
    };
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let result = sink.write_batch_partial(&[json!({"id": 1})]).await;
    assert!(matches!(result, Err(FaucetError::HttpStatus { .. })));
}

#[tokio::test]
async fn truncated_response_pads_with_failures() {
    let server = MockServer::start().await;
    let body = json!({
        "errors": false,
        "items": [ { "index": { "status": 201 } } ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;
    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg).unwrap();
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
}

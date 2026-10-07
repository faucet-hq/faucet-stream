//! Integration tests for `GrpcStream` against a real tonic `EchoService`.
//!
//! Each test starts an in-process server bound to an ephemeral port and
//! drives the source through both the `fetch_all` and `stream_pages`
//! surfaces.

mod common;

use std::sync::atomic::Ordering;

use faucet_core::Source;
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, RpcKind};
use futures::StreamExt;
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_fetch_all_returns_all_items() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5 }))
    .records_path("$.items[*]");

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    assert_eq!(records.len(), 5);
    // Protobuf scalar defaults (id == 0) are omitted from the JSON output,
    // so assert on `name` which is always present.
    assert_eq!(records[0]["name"], "item-0");
    assert_eq!(records[1]["id"], 1);
    assert_eq!(records[4]["name"], "item-4");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_fetch_all_collects_all_events() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 7, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    assert_eq!(records.len(), 7);
    for (i, rec) in records.iter().enumerate() {
        assert_eq!(rec["payload"], format!("event-{i}"));
    }
    // A single attempt should be enough on the happy path.
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_stream_pages_yields_pages_of_batch_size() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 10, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .with_batch_size(3);

    let stream = GrpcStream::new(config).unwrap();
    let ctx = std::collections::HashMap::new();
    let mut pages = stream.stream_pages(&ctx, 3);
    let mut total = 0usize;
    let mut page_sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        page_sizes.push(page.records.len());
        total += page.records.len();
        // All server-streaming pages carry no bookmark — the source has no
        // native cursor and the test fixture has no resume token.
        assert!(page.bookmark.is_none());
    }
    assert_eq!(total, 10);
    // 3 + 3 + 3 + 1 trailing
    assert_eq!(page_sizes, vec![3, 3, 3, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_max_messages_caps_consumption() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 100, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .max_messages(4)
    .with_batch_size(0);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_reconnect_dedupes_replayed_prefix() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5, "fail_after": 2 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(10))
    .reconnect_max_backoff(std::time::Duration::from_millis(20))
    .reconnect_max_attempts(3)
    .reconnect_replay_from_start(true);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    // First attempt yields events 0,1 then disconnects. The fixture replays
    // from message 0 on reconnect (count = 5 → events 0..4). With the opt-in
    // `reconnect_replay_from_start = true`, the source skips the 2 already-
    // emitted messages on the reconnect, so each event is delivered exactly
    // once: 0,1,2,3,4 — no duplicates (#78/#23).
    assert_eq!(records.len(), 5);
    let payloads: Vec<&str> = records
        .iter()
        .map(|r| r["payload"].as_str().unwrap())
        .collect();
    assert_eq!(
        payloads,
        vec!["event-0", "event-1", "event-2", "event-3", "event-4"]
    );
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_reconnect_at_least_once_when_replay_disabled() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5, "fail_after": 2 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(10))
    .reconnect_max_backoff(std::time::Duration::from_millis(20))
    .reconnect_max_attempts(3)
    // Opt out of dedup: emit every received message (at-least-once).
    .reconnect_replay_from_start(false);

    let stream = GrpcStream::new(config).unwrap();
    let records = stream.fetch_all().await.unwrap();

    // events 0,1 (attempt 1) + events 0,1,2,3,4 (replayed in full) = 7.
    assert_eq!(records.len(), 7);
    assert_eq!(records[0]["payload"], "event-0");
    assert_eq!(records[1]["payload"], "event-1");
    assert_eq!(records[2]["payload"], "event-0");
    assert_eq!(records[6]["payload"], "event-4");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_terminate_on_error_propagates() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 5, "fail_after": 1 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .terminate_on_error(true);

    let stream = GrpcStream::new(config).unwrap();
    let err = stream
        .fetch_all()
        .await
        .expect_err("should propagate error");
    let msg = format!("{err}");
    assert!(
        msg.contains("server-streaming") || msg.contains("simulated disconnect"),
        "unexpected error: {msg}"
    );
    // Exactly one attempt — no reconnect when terminate_on_error is true.
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_streaming_reconnect_max_attempts_surfaces_error() {
    // Server is bound but immediately shut down so every connect fails.
    let server = common::start_server().await;
    let endpoint = server.endpoint.clone();
    drop(server);

    let config = GrpcStreamConfig::new(
        &endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 1, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(5))
    .reconnect_max_backoff(std::time::Duration::from_millis(10))
    .reconnect_max_attempts(2);

    let stream = GrpcStream::new(config).unwrap();
    let err = stream.fetch_all().await.expect_err("should give up");
    let msg = format!("{err}");
    assert!(
        msg.contains("reconnect_max_attempts"),
        "unexpected error: {msg}"
    );
}

fn tail(server: &common::ServerHandle, request: serde_json::Value) -> GrpcStreamConfig {
    GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(request)
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(5))
    .reconnect_max_backoff(std::time::Duration::from_millis(10))
}

/// #789 API-11: by default a reconnect emits everything the new stream sends;
/// nothing is skipped on the assumption that the server replays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn by_default_a_reconnect_skips_nothing() {
    let server = common::start_server().await;
    let stream = GrpcStream::new(tail(&server, json!({ "count": 5, "fail_after": 2 }))).unwrap();
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 7, "at-least-once: no message is dropped");
    assert_eq!(records[2]["payload"], "event-0");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

/// #789 API-12: a status a reconnect cannot cure ends the run on the first
/// attempt instead of retrying.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_permanent_status_is_not_retried() {
    let server = common::start_server().await;
    let config = tail(
        &server,
        json!({ "count": 5, "fail_after": 1, "fail_code": 7 }),
    )
    .unlimited_reconnects();
    let err = GrpcStream::new(config)
        .unwrap()
        .fetch_all()
        .await
        .expect_err("PERMISSION_DENIED is fatal");
    assert!(err.to_string().contains("simulated failure"), "{err}");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 1);
}

/// #789 API-12: a record-extraction error is fatal too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_extraction_error_is_not_retried() {
    let server = common::start_server().await;
    let config = tail(&server, json!({ "count": 5 }))
        .records_path("$[")
        .unlimited_reconnects();
    GrpcStream::new(config)
        .unwrap()
        .fetch_all()
        .await
        .expect_err("a bad records_path cannot be cured by reconnecting");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 1);
}

/// #789 API-12: the default reconnect budget is finite, so an endpoint that
/// never comes back fails the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_reconnect_budget_is_finite() {
    let server = common::start_server().await;
    let endpoint = server.endpoint.clone();
    drop(server);
    let config = GrpcStreamConfig::new(
        &endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .rpc_kind(RpcKind::ServerStreaming)
    .reconnect_initial_backoff(std::time::Duration::from_millis(1))
    .reconnect_max_backoff(std::time::Duration::from_millis(2));
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        GrpcStream::new(config).unwrap().fetch_all(),
    )
    .await
    .expect("the run ends")
    .expect_err("the endpoint is gone");
    assert!(
        err.to_string().contains("reconnect_max_attempts=10"),
        "{err}"
    );
}

/// #789 API-13: pages reach the consumer while the stream is still open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pages_are_yielded_before_the_stream_ends() {
    let server = common::start_server().await;
    let config = tail(&server, json!({ "count": 10, "hold_open": true })).with_batch_size(4);
    let stream = GrpcStream::new(config).unwrap();
    let ctx = std::collections::HashMap::new();
    let mut pages = stream.stream_pages(&ctx, 4);
    for _ in 0..2 {
        let page = tokio::time::timeout(std::time::Duration::from_secs(5), pages.next())
            .await
            .expect("a full page arrives while the server holds the stream open")
            .unwrap()
            .unwrap();
        assert_eq!(page.records.len(), 4);
    }
}

/// #789 API-05: a stream that goes quiet is treated as stalled and the
/// reconnect budget bounds the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stalled_stream_hits_the_idle_timeout() {
    let server = common::start_server().await;
    let config = tail(&server, json!({ "count": 0, "hold_open": true }))
        .idle_timeout(Some(std::time::Duration::from_millis(200)))
        .reconnect_max_attempts(1);
    let err = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        GrpcStream::new(config).unwrap().fetch_all(),
    )
    .await
    .expect("the idle timeout ends the run")
    .expect_err("the stream never finishes");
    assert!(err.to_string().contains("received no message"), "{err}");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

/// #789 API-05: a unary call slower than `timeout` fails instead of hanging.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_unary_call_times_out() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 1, "delay_ms": 5000 }))
    .timeout(Some(std::time::Duration::from_millis(200)));
    let err = GrpcStream::new(config)
        .unwrap()
        .fetch_all()
        .await
        .expect_err("the call outlives its timeout");
    assert!(err.to_string().contains("timed out"), "{err}");
}

#[derive(Debug, Default)]
struct RotatingProvider {
    rotated: std::sync::atomic::AtomicBool,
    invalidations: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl faucet_core::AuthProvider for RotatingProvider {
    async fn credential(&self) -> Result<faucet_core::Credential, faucet_core::FaucetError> {
        let token = if self.rotated.load(Ordering::SeqCst) {
            "fresh"
        } else {
            "stale"
        };
        Ok(faucet_core::Credential::Bearer(token.into()))
    }
    async fn invalidate(
        &self,
        _stale: &faucet_core::Credential,
    ) -> Result<faucet_core::Credential, faucet_core::FaucetError> {
        self.invalidations.fetch_add(1, Ordering::SeqCst);
        self.rotated.store(true, Ordering::SeqCst);
        self.credential().await
    }
    fn provider_name(&self) -> &'static str {
        "rotating"
    }
}

/// #789 API-06: a shared credential the server rejects is invalidated and the
/// stream reopened with the refreshed one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_shared_token_is_refreshed_on_a_stream() {
    let server = common::start_server().await;
    let provider = std::sync::Arc::new(RotatingProvider::default());
    let stream = GrpcStream::new(tail(&server, json!({ "count": 3 })))
        .unwrap()
        .with_auth_provider(provider.clone());
    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(provider.invalidations.load(Ordering::SeqCst), 1);
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

/// #789 API-06: the same for a unary call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_rejected_shared_token_is_refreshed_on_a_unary_call() {
    let server = common::start_server().await;
    let provider = std::sync::Arc::new(RotatingProvider::default());
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 2 }))
    .records_path("$.items[*]");
    let records = GrpcStream::new(config)
        .unwrap()
        .with_auth_provider(provider.clone())
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(provider.invalidations.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct AlwaysStale;

#[async_trait::async_trait]
impl faucet_core::AuthProvider for AlwaysStale {
    async fn credential(&self) -> Result<faucet_core::Credential, faucet_core::FaucetError> {
        Ok(faucet_core::Credential::Bearer("stale".into()))
    }
    fn provider_name(&self) -> &'static str {
        "always-stale"
    }
}

/// #789 API-06: a credential rejected again after the refresh is fatal, not
/// retried forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_token_rejected_after_refresh_fails_the_stream() {
    let server = common::start_server().await;
    let err = GrpcStream::new(tail(&server, json!({ "count": 3 })).unlimited_reconnects())
        .unwrap()
        .with_auth_provider(std::sync::Arc::new(AlwaysStale))
        .fetch_all()
        .await
        .expect_err("the server keeps rejecting the token");
    assert!(err.to_string().contains("token revoked"), "{err}");
    assert_eq!(server.tail_attempts.load(Ordering::SeqCst), 2);
}

/// #789 API-06: without a shared provider an UNAUTHENTICATED unary call fails
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_inline_token_rejection_is_not_retried() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 2 }))
    .auth(faucet_source_grpc::GrpcAuth::Bearer {
        token: "stale".into(),
    });
    let err = GrpcStream::new(config)
        .unwrap()
        .fetch_all()
        .await
        .expect_err("rejected");
    assert!(err.to_string().contains("token revoked"), "{err}");
}

/// #789 API-05: a stream that does not start within `timeout` is retried
/// like any transient failure, then fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_stream_start_times_out() {
    let server = common::start_server().await;
    let config = tail(&server, json!({ "count": 1, "start_delay_ms": 5000 }))
        .timeout(Some(std::time::Duration::from_millis(100)))
        .reconnect_max_attempts(0);
    let err = GrpcStream::new(config)
        .unwrap()
        .fetch_all()
        .await
        .expect_err("the stream never starts in time");
    assert!(err.to_string().contains("start timed out"), "{err}");
}

/// Context values are substituted into the call, and `null` timeouts wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_context_call_without_timeouts_streams_every_page() {
    let server = common::start_server().await;
    let config = tail(&server, json!({ "count": "{n}" }))
        .timeout(None)
        .idle_timeout(None)
        .with_batch_size(2);
    let stream = GrpcStream::new(config).unwrap();
    let ctx = std::collections::HashMap::from([("n".to_string(), json!(3))]);
    let mut pages = stream.stream_pages(&ctx, 2);
    let mut total = 0;
    while let Some(page) = pages.next().await {
        total += page.unwrap().records.len();
    }
    assert_eq!(total, 3);
}

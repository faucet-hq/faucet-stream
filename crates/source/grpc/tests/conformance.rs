//! `faucet-conformance` battery for the gRPC source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//! Check 2: `stream_pages` bounds peak memory. Only the **server-streaming**
//!          mode overrides `stream_pages` to flush a page every `batch_size`
//!          messages (unary mode uses the default buffer-then-chunk impl, which
//!          bounds sink-side memory only — so it is deliberately not used
//!          here). A local `EchoService.Tail` that streams TOTAL messages with
//!          `batch_size = 250` therefore yields pages of ≤ 250: the peak page
//!          (250) is ≤ the batch cap and strictly < the total. `fail_after = 0`
//!          disables the reconnect-injection knob so the happy path runs.

mod common;

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_grpc::{GrpcStream, GrpcStreamConfig, RpcKind};
use serde_json::json;

const BATCH: usize = 250;
const TOTAL: usize = 5000;

// ── Check 1: config schema validity (offline) ────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(GrpcStreamConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-grpc");
}

// ── Check 2: bounded-memory streaming (in-process gRPC server) ───────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bounded_memory() {
    let server = common::start_server().await;
    let config = GrpcStreamConfig::new(
        &server.endpoint,
        "faucet.test.echo.EchoService",
        "Tail",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": TOTAL, "fail_after": 0 }))
    .rpc_kind(RpcKind::ServerStreaming)
    .with_batch_size(BATCH);

    let stream = GrpcStream::new(config).unwrap();
    // Server-streaming flushes a page every BATCH messages, so the peak page is
    // 250 ≤ BATCH (250) and < TOTAL (5000).
    assert_bounded_memory(&stream, BATCH, TOTAL).await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// A source pointed at an unreachable endpoint in **unary** mode. `new()` only
/// validates config (no eager connect), so the channel connect happens at read
/// time and fails with a typed `FaucetError`. Port 1 refuses connections
/// immediately on all platforms. Unary is used deliberately — its single
/// connect attempt surfaces the error directly (ServerStreaming would first
/// spend its `reconnect_max_attempts` budget on backoff). A real
/// descriptor set is still required to construct the source, so reuse the
/// shared fixture path; the connect fails before the RPC method is invoked.
fn unreachable_source() -> GrpcStream {
    let config = GrpcStreamConfig::new(
        "http://127.0.0.1:1",
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 1 }))
    .rpc_kind(RpcKind::Unary);
    GrpcStream::new(config).expect("grpc stream builds from a valid config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_errors_not_panics() {
    assert_errors_not_panics(&unreachable_source()).await;
}

// ── Check 10: connector_name non-empty ────────────────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    assert_connector_name_nonempty(&unreachable_source());
    assert_eq!(unreachable_source().connector_name(), "grpc");
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_preflight_check_wellformed() {
    // The default `Source::check` probes the real read path; an unreachable
    // endpoint (unary) surfaces as a `Fail` probe inside `Ok(report)`, never
    // an `Err`.
    assert_preflight_check_wellformed(
        &unreachable_source(),
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

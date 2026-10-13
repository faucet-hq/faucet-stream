//! Runs the reusable `faucet-conformance` battery against the real REST source.
//!
//! - check 1 `assert_config_schema_valid`
//! - check 6 `assert_errors_not_panics` — an unreachable endpoint surfaces a
//!   typed `FaucetError` (connection refused) rather than panicking.
//! - check 9 `assert_batch_size_zero_single_page` — a single-page (`None`
//!   pagination) upstream is emitted as one `StreamPage` (REST chunks by
//!   upstream-API page boundaries, so an unpaginated response is one page).
//! - check 10 `assert_connector_name_nonempty` — the connector label is
//!   non-empty (offline).
//! - check 11 `assert_preflight_check_wellformed` — `check()` surfaces an
//!   unreachable endpoint as a `Fail` probe inside `Ok(report)`, never `Err`.
//!
//! check 2 (bounded-memory streaming) and the incremental resume path (check 3)
//! are covered in depth by this crate's dedicated `stream_test.rs`,
//! `pagination_test.rs`, and `state_resume_test.rs` against a live wiremock
//! server; the battery here adds the uniform schema + error-handling contract.

use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn unreachable_source() -> RestStream {
    // Port 1 refuses connections immediately on all platforms — a fast,
    // deterministic "unreachable endpoint" with no external dependency.
    RestStream::new(
        RestStreamConfig::new("http://127.0.0.1:1", "/items").pagination(PaginationStyle::None),
    )
    .expect("rest stream builds from a valid config")
}

#[test]
fn conformance_config_schema_valid() {
    faucet_conformance::assert_config_schema_valid(&unreachable_source());
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    faucet_conformance::assert_errors_not_panics(&unreachable_source()).await;
}

// ── Check 9: batch_size=0 emits a single page ─────────────────────────────────

#[tokio::test]
async fn conformance_batch_size_zero_single_page() {
    // `None` pagination: the upstream returns the whole result set in one
    // response, which the source emits as exactly one `StreamPage`.
    let server = MockServer::start().await;
    let data: Vec<_> = (0..50).map(|i| json!({ "id": i })).collect();
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": data })))
        .mount(&server)
        .await;

    let source = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/items")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap();

    faucet_conformance::assert_batch_size_zero_single_page(&source).await;
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    faucet_conformance::assert_connector_name_nonempty(&unreachable_source());
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // The default `Source::check` probes the real read path; an unreachable
    // endpoint surfaces as a `Fail` probe inside `Ok(report)`, never an `Err`.
    faucet_conformance::assert_preflight_check_wellformed(
        &unreachable_source(),
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

fn async_job(fetch: serde_json::Value) -> serde_json::Value {
    json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["done"], "failure": ["failed"] },
        "fetch": fetch
    })
}

#[test]
fn conformance_descriptor_matches_built_source() {
    let base = "http://127.0.0.1:1";
    let configs = [
        json!({ "base_url": base, "path": "/items" }),
        json!({
            "base_url": base, "path": "/items", "response_format": "csv",
            "async_job": async_job(json!({
                "url": "/jobs/${job_id}/result", "locator_header": "x-next", "locator_param": "loc"
            }))
        }),
        json!({
            "base_url": base, "path": "/items", "response_format": "csv",
            "async_job": async_job(json!({
                "url": "/jobs/${job_id}/result", "locator_body": "$.next", "locator_param": "loc"
            }))
        }),
        json!({
            "base_url": base, "path": "/items", "response_format": "json",
            "async_job": async_job(json!({ "url": "/jobs/${job_id}/result" }))
        }),
        json!({ "base_url": base, "path": "", "odata": { "entity": "Orders" } }),
    ];
    for v in configs {
        let cfg: RestStreamConfig = serde_json::from_value(v.clone()).unwrap();
        let built = RestStream::new(cfg).unwrap();
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_rest::DESCRIPTOR,
            "rest",
            &v,
            &built,
        );
    }
}

//! `faucet-conformance` battery for the XML source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value.
//! Check 2: `stream_pages` bounds peak memory — the event-driven parser chunks
//! a 6_000-record document into `batch_size`-sized pages (peak page ≤ the batch
//! cap and strictly < the total), never materialising the whole result set.

use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOTAL: usize = 6_000;
const BATCH: usize = 250;

// ── Check 1: config schema validity ──────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(XmlStreamConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-xml");
}

// ── Check 2: bounded-memory streaming ────────────────────────────────────────

/// Build a `<root><item id="i"><name>item-i</name></item>...</root>` document
/// with `n` items.
fn build_doc(n: usize) -> String {
    let mut s = String::with_capacity(n * 64 + 32);
    s.push_str("<root>");
    for i in 0..n {
        s.push_str(&format!(
            "<item id=\"{i}\"><name>item-{i}</name><qty>{}</qty></item>",
            i * 2
        ));
    }
    s.push_str("</root>");
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(build_doc(TOTAL)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        // Config batch_size is the authoritative knob for the overriding source.
        .with_batch_size(BATCH);
    let source = XmlStream::new(config);

    // 6_000 records at batch_size 250 stream as 24 pages of 250 — peak page =
    // 250 ≤ BATCH < TOTAL.
    assert_bounded_memory(&source, BATCH, TOTAL).await;
}

// ── Check 9: batch_size=0 emits a single page ─────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_batch_size_zero_single_page() {
    // `batch_size = 0` is the "no batching" sentinel — the event-driven parser
    // accumulates the whole document into a single `StreamPage`.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(build_doc(200)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .with_batch_size(0);
    let source = XmlStream::new(config);
    assert_batch_size_zero_single_page(&source).await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// A source pointed at an unreachable endpoint. `new()` is lazy (it builds the
/// HTTP client), so the failure surfaces on first read. Port 1 refuses
/// connections immediately on all platforms.
fn unreachable_source() -> XmlStream {
    XmlStream::new(XmlStreamConfig::new("http://127.0.0.1:1", "/feed.xml"))
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    assert_errors_not_panics(&unreachable_source()).await;
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    assert_connector_name_nonempty(&unreachable_source());
    assert_eq!(unreachable_source().connector_name(), "xml");
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // The default `Source::check` probes the real read path; an unreachable
    // endpoint surfaces as a `Fail` probe inside `Ok(report)`, never an `Err`.
    assert_preflight_check_wellformed(
        &unreachable_source(),
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[test]
fn conformance_descriptor_matches_built_source() {
    for path in ["/feed.xml", "/other.xml"] {
        let v = serde_json::to_value(XmlStreamConfig::new("http://127.0.0.1:1", path)).unwrap();
        let cfg: XmlStreamConfig = serde_json::from_value(v.clone()).unwrap();
        let built = faucet_source_xml::XmlStream::new(cfg);
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_xml::DESCRIPTOR,
            "xml",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_secs(60))
                .set_body_string("<root/>"),
        )
        .mount(&server)
        .await;
    let uri = server.uri();
    faucet_conformance::calls::assert_source_call_deadline(|| {
        XmlStream::new(XmlStreamConfig::new(uri.clone(), "/feed.xml"))
    })
    .await;
}

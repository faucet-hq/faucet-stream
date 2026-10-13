//! `faucet-conformance` Tier-1 battery for the Elasticsearch source.
//!
//! Check 1 — the connector's config JSON Schema is a valid, well-formed value.
//! Check 2 — `stream_pages` pages under a bounded batch size (every record
//! streamed; peak page ≤ batch_size and < total), i.e. memory is O(batch_size)
//! regardless of total volume.
//!
//! The bounded-memory check drives the source against a wiremock fake of the
//! Elasticsearch scroll API. Each scroll response becomes exactly one
//! `StreamPage`, so the mock returns 25 scroll pages of 250 docs (6 250 total),
//! then an empty page to signal end-of-scroll — every emitted page is ≤ the
//! configured `batch_size` of 250 while the total far exceeds it.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_conformance::{
    assert_batch_size_zero_single_page, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_elasticsearch::{ElasticsearchSource, ElasticsearchSourceConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

/// Responder that walks a fixed list of fixed-size scroll pages, returning a
/// fresh scroll-id per response and an empty-hits page once every page has
/// been served (Elasticsearch's end-of-scroll sentinel).
#[derive(Clone)]
struct PagedResponder {
    inner: Arc<PagedInner>,
}

struct PagedInner {
    /// Number of full-size pages to serve.
    pages: usize,
    /// Documents per page.
    page_size: usize,
    cursor: AtomicUsize,
}

impl PagedResponder {
    fn new(pages: usize, page_size: usize) -> Self {
        Self {
            inner: Arc::new(PagedInner {
                pages,
                page_size,
                cursor: AtomicUsize::new(0),
            }),
        }
    }
}

impl Respond for PagedResponder {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let idx = self.inner.cursor.fetch_add(1, Ordering::SeqCst);
        let body = if idx < self.inner.pages {
            let base = idx * self.inner.page_size;
            let hits: Vec<Value> = (0..self.inner.page_size)
                .map(|i| {
                    json!({
                        "_index": "test",
                        "_id": format!("{idx}-{i}"),
                        "_source": {"id": base + i},
                    })
                })
                .collect();
            json!({
                "_scroll_id": format!("scroll-{}", idx + 1),
                "hits": {
                    "total": {"value": hits.len(), "relation": "eq"},
                    "hits": hits,
                }
            })
        } else {
            json!({
                "_scroll_id": "scroll-final",
                "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}
            })
        };
        ResponseTemplate::new(200).set_body_json(body)
    }
}

async fn mount_paged_responder(server: &MockServer, responder: PagedResponder) {
    Mock::given(method("POST"))
        .and(path("/test/_search"))
        .respond_with(responder.clone())
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/_search/scroll"))
        .respond_with(responder)
        .mount(server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/_search/scroll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"succeeded": true})))
        .mount(server)
        .await;
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(ElasticsearchSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-elasticsearch");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    // 25 scroll pages × 250 docs = 6 250 total; each scroll response becomes one
    // StreamPage of 250 docs, so peak page (250) == batch_size < total.
    let server = MockServer::start().await;
    mount_paged_responder(&server, PagedResponder::new(25, 250)).await;

    // Config batch_size must equal the batch passed to the battery — this
    // overriding source treats its config batch_size as authoritative.
    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(250);
    let source = ElasticsearchSource::new(config).expect("source new");

    assert_bounded_memory(&source, 250, 6_250).await;
}

// ── Check 9: batch_size=0 emits a single page ─────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_batch_size_zero_single_page() {
    // `batch_size = 0` is the "no batching" sentinel — the source issues a
    // single non-scroll `_search` and yields the whole result set as one page.
    let server = MockServer::start().await;
    mount_paged_responder(&server, PagedResponder::new(1, 500)).await;

    let config = ElasticsearchSourceConfig::new(server.uri(), "test").with_batch_size(0);
    let source = ElasticsearchSource::new(config).expect("source new");

    assert_batch_size_zero_single_page(&source).await;
}

// ── Check 12: discovery round-trips ───────────────────────────────────────────

/// Every index `discover()` reports (`GET _cat/indices` + `GET /<idx>/_mapping`)
/// must be genuinely selectable: deep-merge its config_patch (`{"index": …}`)
/// onto the base config, rebuild the source, and read it (`POST /<idx>/_search`).
/// A realistic mock of the Elasticsearch catalog + scroll API — the integration
/// analogue of the synthetic double.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let server = MockServer::start().await;
    // Catalog: one non-system index `test`.
    Mock::given(method("GET"))
        .and(path("/_cat/indices"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([{ "index": "test", "docs.count": "3" }])),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/test/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "test": { "mappings": { "properties": { "id": { "type": "long" } } } }
        })))
        .mount(&server)
        .await;
    // Read path for the rebuilt source: one scroll page of 3 docs, then empty.
    mount_paged_responder(&server, PagedResponder::new(1, 3)).await;

    let source = ElasticsearchSource::new(ElasticsearchSourceConfig::new(server.uri(), "test"))
        .expect("source new");
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let uri = server.uri();
        async move {
            let base = serde_json::to_value(ElasticsearchSourceConfig::new(&uri, "test"))
                .expect("serialize base config");
            let merged = faucet_conformance::merge_config_patch(base, &patch);
            let cfg: ElasticsearchSourceConfig =
                serde_json::from_value(merged).expect("deserialize merged config");
            Box::new(ElasticsearchSource::new(cfg).expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// A source pointed at an unreachable endpoint. `new()` builds (lazy HTTP
/// client); the first read errors with a typed `FaucetError` (connection
/// refused). Port 1 refuses connections immediately on all platforms.
fn unreachable_source() -> ElasticsearchSource {
    ElasticsearchSource::new(ElasticsearchSourceConfig::new("http://127.0.0.1:1", "idx"))
        .expect("source new")
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    assert_errors_not_panics(&unreachable_source()).await;
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    assert_connector_name_nonempty(&unreachable_source());
    assert_eq!(unreachable_source().connector_name(), "elasticsearch");
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

/// The static descriptor answers exactly what the built source declares (RFC 0013).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    #[allow(unused_mut)]
    let configs = vec![
        r#"{"base_url": "http://127.0.0.1:1", "index": "i", "query": {"match_all": {}}, "auth": {"type": "none"}}"#,
    ];
    for raw in configs {
        let v: faucet_core::Value = faucet_core::serde_json::from_str(raw).expect("json");
        let cfg: faucet_source_elasticsearch::ElasticsearchSourceConfig =
            faucet_core::serde_json::from_value(v.clone()).expect("config");
        let built =
            faucet_source_elasticsearch::ElasticsearchSource::new(cfg).expect("offline build");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_elasticsearch::DESCRIPTOR,
            "elasticsearch",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let server = MockServer::start().await;
    Mock::given(wiremock::matchers::any())
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(60)))
        .mount(&server)
        .await;
    let uri = server.uri();
    faucet_conformance::calls::assert_source_call_deadline(|| {
        ElasticsearchSource::new(ElasticsearchSourceConfig::new(uri.clone(), "test")).unwrap()
    })
    .await;
}

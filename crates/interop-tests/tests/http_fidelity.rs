#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

//! The value-fidelity corpus (#885) over HTTP: the HTTP sink posts it, and the
//! exact bytes it sent are served to the REST and GraphQL sources and posted
//! to the webhook source. A JSON number those sources parsed through a double
//! would fail here.

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::Sink;
use faucet_sink_http::{HttpBatchMode, HttpSink, HttpSinkConfig};
use faucet_source_graphql::{GraphqlStream, GraphqlStreamConfig};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use faucet_source_webhook::{WebhookSource, WebhookSourceConfig};
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use serde_json::Value;
use std::time::Duration;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// The bytes the HTTP sink sends for the corpus (one JSON array).
async fn posted_corpus() -> Vec<u8> {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    let sink = HttpSink::new(
        HttpSinkConfig::new(format!("{}/ingest", server.uri())).batch_mode(HttpBatchMode::Array),
    );
    let sent = fidelity::corpus();
    sink.write_batch(&sent).await.expect("post the corpus");
    sink.flush().await.expect("flush");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    let landed = faucet_core::fidelity::from_slice(&requests[0].body).unwrap();
    fidelity::assert_round_trip(
        &sent,
        landed.as_array().expect("a JSON array"),
        Tolerance::exact(),
    );
    requests[0].body.clone()
}

async fn serve(m: &str, route: &str, body: Vec<u8>, content_type: &str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method(m))
        .and(path(route))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, content_type))
        .mount(&server)
        .await;
    server
}

#[tokio::test]
async fn http_sink_and_rest_source_keep_the_corpus() {
    let body = posted_corpus().await;
    let server = serve("GET", "/corpus", body, "application/json").await;
    let source = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/corpus")
            .records_path("$[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap();
    fidelity::assert_corpus_read(&source, &fidelity::corpus(), Tolerance::exact()).await;
}

#[tokio::test]
async fn graphql_source_keeps_the_corpus() {
    let body = posted_corpus().await;
    let mut doc = br#"{"data":{"rows":"#.to_vec();
    doc.extend_from_slice(&body);
    doc.extend_from_slice(b"}}");
    let server = serve("POST", "/", doc, "application/json").await;
    let source = GraphqlStream::try_new(
        GraphqlStreamConfig::new(server.uri(), "query { rows }").records_path("$.data.rows[*]"),
    )
    .expect("graphql source");
    fidelity::assert_corpus_read(&source, &fidelity::corpus(), Tolerance::exact()).await;
}

#[tokio::test]
async fn xml_source_keeps_every_rendering() {
    let opts = faucet_core::FormatOptions::default();
    let sent = vec![fidelity::flat_corpus()];
    let body = faucet_core::file_format::encode(&sent, faucet_core::FileFormat::Xml, &opts)
        .expect("encode XML");
    let element = format!("{}.{}", opts.xml.root_element, opts.xml.record_element);
    let server = serve("GET", "/feed.xml", body, "application/xml").await;
    let source = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path(element),
    );
    // XML carries text only: a number lands as the string that renders it,
    // an empty element (an empty string or a null) reads as an empty
    // element object, and a list or object is
    // the elements it encodes to (the format layer's own fidelity test,
    // crates/conformance/tests/format_fidelity.rs, covers those).
    let tol = [
        "nested_object",
        "nested_array",
        "nested_empty_object",
        "nested_empty_array",
        "booleans_and_null_null_val",
        "strings_empty",
        "strings_padded",
    ]
    .iter()
    .fold(Tolerance::exact().lenient_scalar_kind(), |t, f| {
        t.skipping(f)
    });
    fidelity::assert_corpus_read(&source, &sent, tol).await;
}

#[tokio::test]
async fn webhook_source_keeps_each_posted_payload() {
    let addr = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let sent = fidelity::corpus();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .path("/hook")
            .max_payloads(sent.len())
            .timeout_secs(30),
    );
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/hook");
    let client = reqwest::Client::new();
    for record in &sent {
        let body = serde_json::to_vec(record).unwrap();
        let mut tries = 0;
        loop {
            match client.post(&url).body(body.clone()).send().await {
                Ok(r) => {
                    assert_eq!(r.status(), 200);
                    break;
                }
                Err(_) if tries < 100 => {
                    tries += 1;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("webhook never came up: {e}"),
            }
        }
    }
    let landed: Vec<Value> = run.await.unwrap().expect("webhook run");
    fidelity::assert_round_trip(&sent, &landed, Tolerance::exact());
}

/// Serves one page of `_source` documents (their exact bytes), then the empty
/// end-of-scroll page.
struct ScrollOnce {
    first: Vec<u8>,
    served: std::sync::atomic::AtomicBool,
}

impl wiremock::Respond for ScrollOnce {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        let body = if self.served.swap(true, std::sync::atomic::Ordering::SeqCst) {
            br#"{"_scroll_id":"s","hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}"#.to_vec()
        } else {
            self.first.clone()
        };
        ResponseTemplate::new(200).set_body_raw(body, "application/json")
    }
}

#[tokio::test]
async fn elasticsearch_sink_and_source_keep_the_corpus() {
    let server = MockServer::start().await;
    let sent = fidelity::corpus();
    let items: Vec<String> = (0..sent.len())
        .map(|_| r#"{"index":{"status":201}}"#.to_string())
        .collect();
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(
            format!(r#"{{"errors":false,"items":[{}]}}"#, items.join(",")),
            "application/json",
        ))
        .mount(&server)
        .await;
    let sink = faucet_sink_elasticsearch::ElasticsearchSink::new(
        faucet_sink_elasticsearch::ElasticsearchSinkConfig::new(server.uri(), "test"),
    )
    .expect("sink");
    sink.write_batch(&sent).await.expect("bulk");
    sink.flush().await.expect("flush");
    let bulk = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.url.path() == "/_bulk")
        .expect("a _bulk request");
    let docs: Vec<&[u8]> = bulk
        .body
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .skip(1)
        .step_by(2)
        .collect();
    assert_eq!(docs.len(), sent.len());
    let mut page =
        br#"{"_scroll_id":"s","hits":{"total":{"value":1,"relation":"eq"},"hits":["#.to_vec();
    for (i, doc) in docs.iter().enumerate() {
        if i > 0 {
            page.push(b',');
        }
        page.extend_from_slice(format!(r#"{{"_index":"test","_id":"{i}","_source":"#).as_bytes());
        page.extend_from_slice(doc);
        page.push(b'}');
    }
    page.extend_from_slice(b"]}}");

    let search = MockServer::start().await;
    let responder = std::sync::Arc::new(ScrollOnce {
        first: page,
        served: std::sync::atomic::AtomicBool::new(false),
    });
    for (m, route) in [("POST", "/test/_search"), ("POST", "/_search/scroll")] {
        Mock::given(method(m))
            .and(path(route))
            .respond_with(SharedResponder(responder.clone()))
            .mount(&search)
            .await;
    }
    Mock::given(method("DELETE"))
        .and(path("/_search/scroll"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(r#"{"succeeded":true}"#, "application/json"),
        )
        .mount(&search)
        .await;
    let source = faucet_source_elasticsearch::ElasticsearchSource::new(
        faucet_source_elasticsearch::ElasticsearchSourceConfig::new(search.uri(), "test"),
    )
    .expect("source");
    fidelity::assert_corpus_read(&source, &sent, Tolerance::exact()).await;
}

struct SharedResponder(std::sync::Arc<ScrollOnce>);

impl wiremock::Respond for SharedResponder {
    fn respond(&self, req: &wiremock::Request) -> ResponseTemplate {
        self.0.respond(req)
    }
}

//! Integration tests for the HTTP-sourced reference relation (#558): a page
//! LEFT JOINs against rows fetched from a REST endpoint, and the endpoint is
//! fetched exactly once for the whole run (cached across pages).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::stage::{apply_stages_to_page, compile_stage};
use faucet_transform_sql::{
    HttpMethod, RelationSource, RelationSpec, SqlTransform, SqlTransformConfig,
};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// Counts every request, always returning the same two-row list body.
struct CountingList(Arc<AtomicUsize>);
impl Respond for CountingList {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        self.0.fetch_add(1, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                { "id": 1, "name": "alpha" },
                { "id": 2, "name": "beta" }
            ]
        }))
    }
}

fn http_relation_config(url: &str, records_path: Option<&str>) -> SqlTransformConfig {
    SqlTransformConfig {
        query: "SELECT b.list_id, r.name AS label \
                FROM batch b LEFT JOIN named_lists r ON b.list_id = r.id \
                ORDER BY b.list_id"
            .into(),
        relations: vec![RelationSpec {
            name: "named_lists".into(),
            source: RelationSource::Http {
                url: url.into(),
                method: HttpMethod::Get,
                headers: Default::default(),
                records_path: records_path.map(str::to_string),
            },
            reload_on_change: false,
        }],
        memory_limit: None,
        threads: Some(1),
    }
}

#[tokio::test]
async fn http_relation_joins_and_fetches_exactly_once_across_pages() {
    let server = MockServer::start().await;
    let counter = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/named-lists"))
        .respond_with(CountingList(counter.clone()))
        .mount(&server)
        .await;

    let cfg = http_relation_config(&format!("{}/named-lists", server.uri()), Some("$.items[*]"));

    // Compile once — this is where the single fetch happens.
    let transform = SqlTransform::compile(&cfg).unwrap();
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "endpoint fetched exactly once at compile time"
    );

    let stage = compile_stage(&transform.into_page_stage()).unwrap();

    // Page 1: two rows, both matching the cached relation.
    let page1: Vec<Value> = vec![json!({ "list_id": 1 }), json!({ "list_id": 2 })];
    let out1 = apply_stages_to_page(page1, std::slice::from_ref(&stage)).unwrap();
    assert_eq!(out1.len(), 2);
    assert_eq!(out1[0]["label"], json!("alpha"));
    assert_eq!(out1[1]["label"], json!("beta"));

    // Page 2: one matching row + one with no match (LEFT JOIN → null label).
    let page2: Vec<Value> = vec![json!({ "list_id": 1 }), json!({ "list_id": 99 })];
    let out2 = apply_stages_to_page(page2, std::slice::from_ref(&stage)).unwrap();
    assert_eq!(out2.len(), 2);
    assert_eq!(out2[0]["label"], json!("alpha"));
    assert_eq!(out2[1]["label"], json!(null), "unmatched key → null label");

    // The relation was cached: no page re-hit the endpoint.
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "endpoint fetched exactly once for the whole run"
    );
}

#[tokio::test]
async fn http_relation_whole_body_array_without_records_path() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rows"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            { "id": 1, "name": "one" },
            { "id": 2, "name": "two" }
        ])))
        .mount(&server)
        .await;

    let cfg = http_relation_config(&format!("{}/rows", server.uri()), None);
    let transform = SqlTransform::compile(&cfg).unwrap();
    let stage = compile_stage(&transform.into_page_stage()).unwrap();

    let out =
        apply_stages_to_page(vec![json!({ "list_id": 2 })], std::slice::from_ref(&stage)).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(out[0]["label"], json!("two"));
}

#[tokio::test]
async fn http_relation_non_array_body_is_config_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/one"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": 1 })))
        .mount(&server)
        .await;

    // No records_path + object body → clear config error naming records_path.
    let cfg = http_relation_config(&format!("{}/one", server.uri()), None);
    let err = SqlTransform::compile(&cfg).unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)),
        "got: {err:?}"
    );
    let msg = format!("{err}");
    assert!(msg.contains("not a JSON array"), "got: {msg}");
    assert!(msg.contains("records_path"), "got: {msg}");
}

#[tokio::test]
async fn http_relation_http_error_status_surfaces() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/boom"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let cfg = http_relation_config(&format!("{}/boom", server.uri()), Some("$.items[*]"));
    let err = SqlTransform::compile(&cfg).unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)),
        "got: {err:?}"
    );
    assert!(format!("{err}").contains("500"), "got: {err}");
}

#[tokio::test]
async fn http_relation_honours_the_scoped_reference_policy() {
    use faucet_core::resolve::ReferencePolicy;
    let server = MockServer::start().await;
    let counter = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/named-lists"))
        .respond_with(CountingList(counter.clone()))
        .mount(&server)
        .await;
    let cfg = http_relation_config(&format!("{}/named-lists", server.uri()), Some("$.items[*]"));

    let deny = Arc::new(ReferencePolicy::open().with_url_hosts(["api.example.com"]));
    let err =
        faucet_core::http::with_reference_policy(deny, async { SqlTransform::compile(&cfg).err() })
            .await
            .expect("a host outside the allow-list is refused");
    assert!(
        format!("{err}").contains("not in the allow-list"),
        "got: {err}"
    );
    assert_eq!(counter.load(Ordering::SeqCst), 0, "nothing was fetched");

    let allow = Arc::new(ReferencePolicy::open().with_url_hosts(["127.0.0.1"]));
    faucet_core::http::with_reference_policy(allow, async {
        SqlTransform::compile(&cfg).unwrap();
    })
    .await;
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_relation_never_follows_a_redirect_into_instance_metadata() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/go"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", "http://169.254.169.254/latest/meta-data/"),
        )
        .mount(&server)
        .await;
    let cfg = http_relation_config(&format!("{}/go", server.uri()), None);
    let err = SqlTransform::compile(&cfg).unwrap_err();
    assert!(format!("{err}").contains("metadata"), "got: {err}");
}

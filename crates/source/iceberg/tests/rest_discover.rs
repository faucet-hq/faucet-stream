//! Discovery against a REST catalog stand-in: how a catalog without nested
//! namespaces, and a namespace dropped mid-listing, are handled.
#![cfg(feature = "catalog-rest")]

use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use serde_json::json;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn catalog(parent_status: u16, tables_calls: u64) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"defaults": {}, "overrides": {}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(query_param_is_missing("parent"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"namespaces": [["ns"]]})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .and(query_param("parent", "ns"))
        .respond_with(ResponseTemplate::new(parent_status).set_body_json(json!({
            "error": {"message": "parent is not supported", "type": "BadRequest", "code": parent_status}
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/ns/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"identifiers": []})))
        .expect(tables_calls)
        .mount(&server)
        .await;
    server
}

async fn discover(server: &MockServer) -> Result<usize, String> {
    let cfg: IcebergSourceConfig = serde_json::from_value(json!({
        "catalog": {"type": "rest", "uri": server.uri()},
        "table": "ns.t",
    }))
    .unwrap();
    let source = IcebergSource::new(cfg).await.unwrap();
    source
        .discover()
        .await
        .map(|d| d.len())
        .map_err(|e| e.to_string())
}

/// A catalog that rejects the `parent` filter has no nested namespaces: the
/// top-level namespace's tables are still listed.
#[tokio::test(flavor = "multi_thread")]
async fn a_rejected_parent_listing_means_no_nested_namespaces() {
    for status in [400, 405, 501] {
        let server = catalog(status, 1).await;
        assert_eq!(discover(&server).await, Ok(0), "status {status}");
        server.verify().await;
    }
}

/// A namespace that disappears while it is listed is skipped, tables and all.
#[tokio::test(flavor = "multi_thread")]
async fn a_namespace_dropped_while_listed_is_skipped() {
    let server = catalog(404, 0).await;
    assert_eq!(discover(&server).await, Ok(0));
    server.verify().await;
}

/// Any other failure of the nested listing still fails discovery.
#[tokio::test(flavor = "multi_thread")]
async fn other_nested_listing_failures_fail_discovery() {
    let server = catalog(403, 0).await;
    let err = discover(&server).await.unwrap_err();
    assert!(
        err.contains("could not list the namespaces under ns"),
        "{err}"
    );
}

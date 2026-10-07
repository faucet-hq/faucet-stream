//! TLS connections verify against real roots plus an optional private CA
//! (#789 API-16).

mod common;

use faucet_source_grpc::{GrpcStream, GrpcStreamConfig};
use serde_json::json;

fn config(endpoint: &str) -> GrpcStreamConfig {
    GrpcStreamConfig::new(
        endpoint,
        "faucet.test.echo.EchoService",
        "List",
        common::descriptor_set_path(),
    )
    .request(json!({ "count": 2 }))
    .records_path("$.items[*]")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_signed_by_a_configured_ca_is_trusted() {
    let server = common::start_tls_server().await;
    let records = GrpcStream::new(config(&server.endpoint).ca_cert(common::tls_fixture("ca.pem")))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_name_overrides_the_verified_host() {
    let server = common::start_tls_server().await;
    let by_ip = server.endpoint.replace("localhost", "127.0.0.1");
    let records = GrpcStream::new(
        config(&by_ip)
            .ca_cert(common::tls_fixture("ca.pem"))
            .domain_name("localhost"),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_untrusted_server_and_a_missing_ca_file_are_errors() {
    let server = common::start_tls_server().await;
    let err = GrpcStream::new(config(&server.endpoint))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("gRPC"), "{err}");
    let err = GrpcStream::new(config(&server.endpoint).ca_cert("/nonexistent/ca.pem"))
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("cannot read ca_cert"), "{err}");
}

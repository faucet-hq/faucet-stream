//! The NATS source over TLS (#967) against a `nats:2.10` that requires TLS.
//! The connection config has no CA setting: async-nats trusts the platform
//! store, which honours `SSL_CERT_FILE`. So the test proves an unknown issuer
//! is refused, then trusts the fixture CA that way and reads a real message.
#![allow(clippy::disallowed_methods)] // the publisher waits for the subscription

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Source;
use faucet_source_nats::{NatsSource, NatsSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;

fn config(server: &str) -> NatsSourceConfig {
    let mut cfg = NatsSourceConfig::new("tls.items");
    cfg.connection.servers = vec![server.to_string()];
    cfg.connection.tls = true;
    cfg.max_messages = Some(1);
    cfg.idle_timeout_secs = Some(15);
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_tls_and_refuses_an_unknown_ca() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let nats = containers::start(|| images::nats(&tls)).await;
    let port = nats.get_host_port_ipv4(images::NATS_PORT).await.unwrap();
    let server = format!("tls://localhost:{port}");

    let untrusted = NatsSource::new(config(&server)).await.expect("source new");
    let ctx = HashMap::new();
    let first = untrusted.stream_pages(&ctx, 10).next().await;
    assert!(
        matches!(first, Some(Err(_))),
        "a server signed by an unknown CA must be refused, got {first:?}"
    );

    // SAFETY: this binary runs one test; nothing else reads the environment concurrently.
    unsafe { std::env::set_var("SSL_CERT_FILE", tls.ca_path()) };
    let source = NatsSource::new(config(&server))
        .await
        .expect("verified TLS connect");
    let publisher_server = server.clone();
    let ca = tls.ca_path();
    let publisher = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let client = async_nats::ConnectOptions::new()
            .require_tls(true)
            .add_root_certificates(ca)
            .connect(publisher_server)
            .await
            .expect("publisher connect");
        client
            .publish("tls.items", br#"{"id":1,"v":"a"}"#.to_vec().into())
            .await
            .expect("publish");
        client.flush().await.expect("flush");
    });
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 10);
    let mut records = Vec::new();
    while let Some(page) = pages.next().await {
        records.extend(page.expect("page").records);
    }
    publisher.await.unwrap();
    assert_eq!(records, vec![serde_json::json!({"id": 1, "v": "a"})]);
}

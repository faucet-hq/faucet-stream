//! The NATS source over TLS (#967, #973) against a `nats:2.10` that requires
//! TLS. `tls: true` trusts the platform store, so a server signed by the
//! fixture CA is refused; with the CA from the `tls:` block the source reads
//! a real message. Against a server that requires client certificates the
//! read needs `client_cert_path` / `client_key_path` too.
#![allow(clippy::disallowed_methods)] // the publisher waits for the subscription

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Source;
use faucet_source_nats::{NatsSource, NatsSourceConfig, NatsTls};
use futures::StreamExt;
use std::collections::HashMap;

fn config(server: &str, tls: NatsTls) -> NatsSourceConfig {
    let mut cfg = NatsSourceConfig::new("tls.items");
    cfg.connection.servers = vec![server.to_string()];
    cfg.connection.tls = true;
    cfg.connection.tls_options = tls;
    cfg.max_messages = Some(1);
    cfg.idle_timeout_secs = Some(15);
    cfg
}

fn ca(tls: &TlsFixture) -> NatsTls {
    NatsTls {
        ca_cert_path: Some(tls.ca_path()),
        ..Default::default()
    }
}

async fn wait_for_port(port: u16) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("port {port} never accepted a connection");
}

async fn first_page_fails(server: &str, tls: NatsTls) -> bool {
    match NatsSource::new(config(server, tls)).await {
        Err(_) => true,
        Ok(source) => {
            let ctx = HashMap::new();
            matches!(source.stream_pages(&ctx, 10).next().await, Some(Err(_)))
        }
    }
}

async fn read_one(server: &str, fixture: &TlsFixture, tls: NatsTls) -> Vec<serde_json::Value> {
    let source = NatsSource::new(config(server, tls))
        .await
        .expect("verified TLS connect");
    let publisher_server = server.to_string();
    let (ca, cert, key) = (
        fixture.ca_path(),
        fixture.client_cert_path(),
        fixture.client_key_path(),
    );
    let publisher = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let client = async_nats::ConnectOptions::new()
            .require_tls(true)
            .add_root_certificates(ca)
            .add_client_certificate(cert, key)
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
    records
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_with_the_configured_ca_and_refuses_an_unknown_one() {
    let tls = TlsFixture::generate();
    let nats = containers::start(|| images::nats(&tls)).await;
    let port = nats.get_host_port_ipv4(images::NATS_PORT).await.unwrap();
    let server = format!("tls://127.0.0.1:{port}");
    wait_for_port(port).await;

    assert!(
        first_page_fails(&server, NatsTls::default()).await,
        "a server signed by an unknown CA must be refused"
    );
    let records = read_one(&server, &tls, ca(&tls)).await;
    assert_eq!(records, vec![serde_json::json!({"id": 1, "v": "a"})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_reads_with_the_client_certificate() {
    let tls = TlsFixture::generate();
    let nats = containers::start(|| images::nats_mutual(&tls)).await;
    let port = nats.get_host_port_ipv4(images::NATS_PORT).await.unwrap();
    let server = format!("tls://127.0.0.1:{port}");
    wait_for_port(port).await;

    assert!(
        first_page_fails(&server, ca(&tls)).await,
        "no client certificate must be refused"
    );
    let mutual = NatsTls {
        client_cert_path: Some(tls.client_cert_path()),
        client_key_path: Some(tls.client_key_path()),
        ..ca(&tls)
    };
    let records = read_one(&server, &tls, mutual).await;
    assert_eq!(records, vec![serde_json::json!({"id": 1, "v": "a"})]);
}

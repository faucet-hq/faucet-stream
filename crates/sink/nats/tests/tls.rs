//! The NATS sink over TLS (#967, #973) against a `nats:2.10` that requires
//! TLS. `tls: true` trusts the platform store, so a server signed by the
//! fixture CA is refused; the CA from the `tls:` block (path or inline PEM)
//! publishes a record a subscriber receives, and an unrelated CA is refused.
//! Against a server that requires client certificates, publishing needs
//! `client_cert_path` / `client_key_path`.
#![allow(clippy::disallowed_methods)] // waits for the mapped port to accept connections

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Sink;
use faucet_sink_nats::{NatsSink, NatsSinkConfig, NatsTls};
use futures::StreamExt;

fn config(server: &str, tls: NatsTls) -> NatsSinkConfig {
    let mut cfg = NatsSinkConfig::new("tls.out");
    cfg.connection.servers = vec![server.to_string()];
    cfg.connection.tls = true;
    cfg.connection.tls_options = tls;
    cfg
}

fn ca(tls: &TlsFixture) -> NatsTls {
    NatsTls {
        ca_cert_path: Some(tls.ca_path()),
        ..Default::default()
    }
}

fn mutual(tls: &TlsFixture) -> NatsTls {
    NatsTls {
        client_cert_path: Some(tls.client_cert_path()),
        client_key_path: Some(tls.client_key_path()),
        ..ca(tls)
    }
}

async fn publish(server: &str, tls: NatsTls, id: u32) -> Result<usize, faucet_core::FaucetError> {
    let sink = NatsSink::new(config(server, tls)).await?;
    let written = sink.write_batch(&[serde_json::json!({ "id": id })]).await?;
    sink.flush().await?;
    Ok(written)
}

async fn subscriber(
    server: &str,
    tls: &TlsFixture,
) -> (async_nats::Client, async_nats::Subscriber) {
    let client = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(tls.ca_path())
        .add_client_certificate(tls.client_cert_path(), tls.client_key_path())
        .connect(server)
        .await
        .expect("subscriber connect");
    let messages = client.subscribe("tls.out").await.unwrap();
    client.flush().await.unwrap();
    (client, messages)
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

fn refused(result: Result<usize, faucet_core::FaucetError>, what: &str) {
    let err = result.expect_err(what).to_string();
    assert!(
        err.to_lowercase().contains("certificate") || err.contains("tls"),
        "{what}: {err}"
    );
}

async fn next_id(messages: &mut async_nats::Subscriber) -> serde_json::Value {
    let got = tokio::time::timeout(std::time::Duration::from_secs(10), messages.next())
        .await
        .expect("message within 10s")
        .expect("message");
    serde_json::from_slice(&got.payload).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_with_the_configured_ca_and_refuses_others() {
    let tls = TlsFixture::generate();
    let nats = containers::start(|| images::nats(&tls)).await;
    let port = nats.get_host_port_ipv4(images::NATS_PORT).await.unwrap();
    let server = format!("tls://127.0.0.1:{port}");

    wait_for_port(port).await;
    refused(
        publish(&server, NatsTls::default(), 0).await,
        "a server signed by an unknown CA must be refused",
    );
    let other = NatsTls {
        ca_cert_path: Some(tls.other_ca_path()),
        ..Default::default()
    };
    refused(
        publish(&server, other, 0).await,
        "a CA that did not sign the server must be refused",
    );

    let (_client, mut messages) = subscriber(&server, &tls).await;
    assert_eq!(publish(&server, ca(&tls), 7).await.expect("verified"), 1);
    assert_eq!(next_id(&mut messages).await, serde_json::json!({"id": 7}));
    let inline = NatsTls {
        ca_cert_pem: Some(tls.ca_pem().to_string()),
        ..Default::default()
    };
    assert_eq!(publish(&server, inline, 8).await.expect("inline CA"), 1);
    assert_eq!(next_id(&mut messages).await, serde_json::json!({"id": 8}));
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_needs_the_client_certificate() {
    let tls = TlsFixture::generate();
    let nats = containers::start(|| images::nats_mutual(&tls)).await;
    let port = nats.get_host_port_ipv4(images::NATS_PORT).await.unwrap();
    let server = format!("tls://127.0.0.1:{port}");

    wait_for_port(port).await;
    assert!(
        publish(&server, ca(&tls), 0).await.is_err(),
        "no client certificate must be refused"
    );
    let (_client, mut messages) = subscriber(&server, &tls).await;
    assert_eq!(publish(&server, mutual(&tls), 9).await.expect("mTLS"), 1);
    assert_eq!(next_id(&mut messages).await, serde_json::json!({"id": 9}));
}

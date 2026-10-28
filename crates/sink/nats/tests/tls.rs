//! The NATS sink over TLS (#967) against a `nats:2.10` that requires TLS.
//! The connection config has no CA setting: async-nats trusts the platform
//! store, which honours `SSL_CERT_FILE`. So the test proves an unknown issuer
//! is refused, then trusts the fixture CA that way and publishes a record a
//! subscriber receives.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Sink;
use faucet_sink_nats::{NatsSink, NatsSinkConfig};
use futures::StreamExt;

fn config(server: &str) -> NatsSinkConfig {
    let mut cfg = NatsSinkConfig::new("tls.out");
    cfg.connection.servers = vec![server.to_string()];
    cfg.connection.tls = true;
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_over_tls_and_refuses_an_unknown_ca() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let nats = containers::start(|| images::nats(&tls)).await;
    let port = nats.get_host_port_ipv4(images::NATS_PORT).await.unwrap();
    let server = format!("tls://localhost:{port}");

    let untrusted = NatsSink::new(config(&server)).await.expect("sink new");
    assert!(
        untrusted
            .write_batch(&[serde_json::json!({"id": 0})])
            .await
            .is_err(),
        "a server signed by an unknown CA must be refused"
    );

    let subscriber = async_nats::ConnectOptions::new()
        .require_tls(true)
        .add_root_certificates(tls.ca_path())
        .connect(server.as_str())
        .await
        .expect("subscriber connect");
    let mut messages = subscriber.subscribe("tls.out").await.unwrap();
    subscriber.flush().await.unwrap();

    // SAFETY: this binary runs one test; nothing else reads the environment concurrently.
    unsafe { std::env::set_var("SSL_CERT_FILE", tls.ca_path()) };
    let sink = NatsSink::new(config(&server))
        .await
        .expect("verified TLS connect");
    let written = sink
        .write_batch(&[serde_json::json!({"id": 7})])
        .await
        .expect("publish");
    assert_eq!(written, 1);
    sink.flush().await.unwrap();

    let got = tokio::time::timeout(std::time::Duration::from_secs(10), messages.next())
        .await
        .expect("message within 10s")
        .expect("message");
    let body: serde_json::Value = serde_json::from_slice(&got.payload).unwrap();
    assert_eq!(body, serde_json::json!({"id": 7}));
}

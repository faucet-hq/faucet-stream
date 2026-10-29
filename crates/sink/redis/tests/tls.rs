//! `rediss://` (#967, #972) against a `redis:7.4` that only listens for TLS.
//! Without a `tls:` block the sink trusts the bundled web PKI roots, so a
//! server signed by a private CA is refused with a typed error, as is
//! plaintext. With the fixture CA in `tls.ca_cert_path` a write lands and
//! reads back; a server that requires a client certificate accepts a write
//! only with `client_cert_path` / `client_key_path`.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Sink};
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType, RedisTls};
use serde_json::json;

async fn open(url: String, tls: RedisTls) -> Result<RedisSink, FaucetError> {
    RedisSink::new(
        RedisSinkConfig::new(
            url,
            RedisSinkType::List {
                key: "items".into(),
            },
        )
        .with_tls(tls),
    )
    .await
}

fn ca(tls: &TlsFixture) -> RedisTls {
    RedisTls {
        ca_cert_path: Some(tls.ca_path()),
        ..Default::default()
    }
}

fn mutual(tls: &TlsFixture) -> RedisTls {
    RedisTls {
        client_cert_path: Some(tls.client_cert_path()),
        client_key_path: Some(tls.client_key_path()),
        ..ca(tls)
    }
}

async fn write_and_read_back(url: String, tls: RedisTls) -> Vec<String> {
    let sink = open(url.clone(), tls.clone())
        .await
        .expect("verified connect");
    let written = sink
        .write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .expect("write over TLS");
    assert_eq!(written, 2);
    sink.flush().await.unwrap();
    let client = faucet_common_redis::build_client(&url, &tls).unwrap();
    let mut conn = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("LRANGE")
        .arg("items")
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn private_ca_round_trip_and_refusals() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();
    let url = format!("rediss://localhost:{port}");

    match open(url.clone(), RedisTls::default()).await {
        Err(FaucetError::Sink(msg)) => assert!(
            msg.contains("Redis connection failed") && msg.to_lowercase().contains("certificate"),
            "{msg}"
        ),
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(_) => panic!("an unknown CA must be refused"),
    }
    assert!(
        open(format!("redis://localhost:{port}"), RedisTls::default())
            .await
            .is_err(),
        "plaintext to a TLS-only port must fail"
    );
    let missing = RedisTls {
        ca_cert_path: Some(tls.dir().join("missing.pem")),
        ..Default::default()
    };
    assert!(matches!(
        open(url.clone(), missing).await,
        Err(FaucetError::Config(_))
    ));

    let items = write_and_read_back(url, ca(&tls)).await;
    assert_eq!(items, vec![r#"{"id":1}"#, r#"{"id":2}"#]);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_needs_the_client_certificate() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis_mutual(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();
    let url = format!("rediss://localhost:{port}");

    let without = match open(url.clone(), ca(&tls)).await {
        Err(e) => Err(e),
        Ok(sink) => sink.write_batch(&[json!({"id": 0})]).await.map(|_| ()),
    };
    assert!(without.is_err(), "no client certificate must be refused");

    let items = write_and_read_back(url, mutual(&tls)).await;
    assert_eq!(items, vec![r#"{"id":1}"#, r#"{"id":2}"#]);
}

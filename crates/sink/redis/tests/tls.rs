//! `rediss://` (#967) against a `redis:7.4` that only listens for TLS. The
//! connector trusts the bundled web PKI roots only and has no setting for a
//! private CA, so the test proves the TLS handshake runs and verifies the
//! certificate: a server signed by an unknown CA is refused with a typed error
//! (it used to panic for want of a rustls provider), as is plaintext.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::FaucetError;
use faucet_sink_redis::{RedisSink, RedisSinkConfig, RedisSinkType};

async fn open(url: String) -> Result<RedisSink, FaucetError> {
    RedisSink::new(RedisSinkConfig::new(
        url,
        RedisSinkType::List {
            key: "items".into(),
        },
    ))
    .await
}

#[tokio::test(flavor = "multi_thread")]
async fn rediss_verifies_the_server_certificate() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();

    match open(format!("rediss://localhost:{port}")).await {
        Err(FaucetError::Sink(msg)) => assert!(
            msg.contains("Redis connection failed") && msg.to_lowercase().contains("certificate"),
            "{msg}"
        ),
        Err(other) => panic!("unexpected error {other:?}"),
        Ok(_) => panic!("an unknown CA must be refused"),
    }
    assert!(
        open(format!("redis://localhost:{port}")).await.is_err(),
        "plaintext to a TLS-only port must fail"
    );
}

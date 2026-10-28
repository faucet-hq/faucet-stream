//! `rediss://` (#967) against a `redis:7.4` that only listens for TLS. The
//! connector trusts the bundled web PKI roots only and has no setting for a
//! private CA, so the test proves the TLS handshake runs and verifies the
//! certificate: a server signed by an unknown CA is refused with a typed error
//! (it used to panic for want of a rustls provider), as is plaintext.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::FaucetError;
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType};

async fn read(url: String) -> Result<Vec<serde_json::Value>, FaucetError> {
    let config = RedisSourceConfig::new(
        url,
        RedisSourceType::List {
            key: "items".into(),
        },
    );
    RedisSource::new(config)?.fetch_all().await
}

#[tokio::test(flavor = "multi_thread")]
async fn rediss_verifies_the_server_certificate() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();

    match read(format!("rediss://localhost:{port}")).await {
        Err(FaucetError::Source(msg)) => assert!(
            msg.contains("Redis connection failed") && msg.to_lowercase().contains("certificate"),
            "{msg}"
        ),
        other => panic!("an unknown CA must be refused, got {other:?}"),
    }
    assert!(
        read(format!("redis://localhost:{port}")).await.is_err(),
        "plaintext to a TLS-only port must fail"
    );
}

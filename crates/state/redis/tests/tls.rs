//! `rediss://` (#967) against a `redis:7.4` that only listens for TLS: the
//! store verifies the server certificate against the web PKI roots, so a
//! server signed by an unknown CA is refused with a typed error, not a panic.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::FaucetError;
use faucet_state_redis::RedisStateStore;

#[tokio::test(flavor = "multi_thread")]
async fn rediss_verifies_the_server_certificate() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();

    for url in [
        format!("rediss://localhost:{port}"),
        format!("redis://localhost:{port}"),
    ] {
        match RedisStateStore::connect(&url, "faucet").await {
            Err(FaucetError::State(_)) => {}
            Err(other) => panic!("{url}: unexpected error {other:?}"),
            Ok(_) => panic!("{url} must be refused"),
        }
    }
}

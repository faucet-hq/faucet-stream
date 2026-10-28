//! `rediss://` (#967, #972) against a `redis:7.4` that only listens for TLS.
//! Without a `tls:` block the store trusts the web PKI roots, so a server
//! signed by a private CA is refused with a typed error, not a panic. With
//! the fixture CA from `connect_with_tls` a bookmark round-trips; a server
//! that requires a client certificate is reached only with one.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, StateStore};
use faucet_state_redis::{RedisStateStore, RedisTls};
use serde_json::json;

fn ca(tls: &TlsFixture) -> RedisTls {
    RedisTls {
        ca_cert_path: Some(tls.ca_path()),
        ..Default::default()
    }
}

async fn round_trip(url: &str, tls: &RedisTls) {
    let store = RedisStateStore::connect_with_tls(url, "faucet", tls)
        .await
        .expect("verified connect");
    store.put("p::r", &json!({"cursor": 7})).await.unwrap();
    assert_eq!(store.get("p::r").await.unwrap(), Some(json!({"cursor": 7})));
}

#[tokio::test(flavor = "multi_thread")]
async fn private_ca_round_trip_and_refusals() {
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
    let other = RedisTls {
        ca_cert_path: Some(tls.other_ca_path()),
        ..Default::default()
    };
    let url = format!("rediss://localhost:{port}");
    assert!(matches!(
        RedisStateStore::connect_with_tls(&url, "faucet", &other).await,
        Err(FaucetError::State(_))
    ));

    round_trip(&url, &ca(&tls)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_round_trip() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis_mutual(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();
    let url = format!("rediss://localhost:{port}");

    let without = match RedisStateStore::connect_with_tls(&url, "faucet", &ca(&tls)).await {
        Err(e) => Err(e),
        Ok(store) => store.get("p::r").await.map(|_| ()),
    };
    assert!(without.is_err(), "no client certificate must be refused");

    let mutual = RedisTls {
        client_cert_path: Some(tls.client_cert_path()),
        client_key_path: Some(tls.client_key_path()),
        ..ca(&tls)
    };
    round_trip(&url, &mutual).await;
}

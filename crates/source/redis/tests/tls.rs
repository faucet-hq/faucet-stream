//! `rediss://` (#967, #972) against a `redis:7.4` that only listens for TLS.
//! Without a `tls:` block the connector trusts the bundled web PKI roots, so
//! a server signed by a private CA is refused with a typed error, as is
//! plaintext. With the fixture CA in `tls.ca_cert_path` (or inline) the read
//! round-trips; a server that requires a client certificate is reached only
//! with `client_cert_path` / `client_key_path`; a hostname the certificate
//! does not name is refused; `insecure_skip_verify` skips verification.
#![allow(clippy::disallowed_methods)] // the seeding client waits for the mapped port

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::FaucetError;
use faucet_source_redis::{RedisSource, RedisSourceConfig, RedisSourceType, RedisTls};

async fn read(url: String, tls: RedisTls) -> Result<Vec<serde_json::Value>, FaucetError> {
    let config = RedisSourceConfig::new(
        url,
        RedisSourceType::List {
            key: "items".into(),
        },
    )
    .with_tls(tls);
    RedisSource::new(config)?.fetch_all().await
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

async fn seed(url: &str, tls: &RedisTls) {
    let client = faucet_common_redis::build_client(url, tls).unwrap();
    let mut attempts = 0;
    let mut conn = loop {
        match client.get_multiplexed_async_connection().await {
            Ok(conn) => break conn,
            Err(_) if attempts < 50 => {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(e) => panic!("seed connect: {e}"),
        }
    };
    let _: i64 = redis::cmd("RPUSH")
        .arg("items")
        .arg(r#"{"id":1,"v":"a"}"#)
        .arg(r#"{"id":2,"v":"b"}"#)
        .query_async(&mut conn)
        .await
        .unwrap();
}

fn assert_cert_refusal(result: Result<Vec<serde_json::Value>, FaucetError>, what: &str) {
    match result {
        Err(FaucetError::Source(msg)) => assert!(
            msg.contains("Redis connection failed") && msg.to_lowercase().contains("certificate"),
            "{what}: {msg}"
        ),
        other => panic!("{what} must be refused, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn private_ca_round_trip_and_refusals() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let redis = containers::start(|| images::redis(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();
    let url = format!("rediss://localhost:{port}");
    seed(&url, &ca(&tls)).await;

    assert_cert_refusal(
        read(url.clone(), RedisTls::default()).await,
        "an unknown CA",
    );
    assert!(
        read(format!("redis://localhost:{port}"), RedisTls::default())
            .await
            .is_err(),
        "plaintext to a TLS-only port must fail"
    );

    let rows = read(url.clone(), ca(&tls)).await.expect("verified read");
    assert_eq!(
        rows,
        vec![
            serde_json::json!({"id": 1, "v": "a"}),
            serde_json::json!({"id": 2, "v": "b"})
        ]
    );
    let inline = RedisTls {
        ca_cert_pem: Some(tls.ca_pem().to_string()),
        ..Default::default()
    };
    assert_eq!(read(url.clone(), inline).await.unwrap().len(), 2);

    let other = RedisTls {
        ca_cert_path: Some(tls.other_ca_path()),
        ..Default::default()
    };
    assert_cert_refusal(read(url.clone(), other).await, "a server outside the CA");
    assert_cert_refusal(
        read(format!("rediss://127.0.0.1:{port}"), ca(&tls)).await,
        "a hostname the certificate does not name",
    );

    let insecure = RedisTls {
        insecure_skip_verify: true,
        ..Default::default()
    };
    let rows = read(format!("rediss://127.0.0.1:{port}"), insecure).await;
    assert_eq!(rows.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn mutual_tls_needs_the_client_certificate() {
    let tls = TlsFixture::generate();
    let redis = containers::start(|| images::redis_mutual(&tls)).await;
    let port = redis.get_host_port_ipv4(images::REDIS_PORT).await.unwrap();
    let url = format!("rediss://localhost:{port}");
    seed(&url, &mutual(&tls)).await;

    assert!(
        matches!(
            read(url.clone(), ca(&tls)).await,
            Err(FaucetError::Source(_))
        ),
        "no client certificate must be refused"
    );
    assert_eq!(read(url, mutual(&tls)).await.unwrap().len(), 2);
}

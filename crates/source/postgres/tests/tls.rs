//! The PostgreSQL source over TLS (#967) against a `postgres:16-alpine` that
//! rejects every non-TLS connection: each `sslmode` reads real rows, and the
//! verify modes refuse an unknown CA or a name the certificate does not cover.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source};
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use serde_json::{Value, json};
use std::path::Path;

const QUERY: &str = "SELECT s.ssl, t.id, t.v FROM pg_stat_ssl s, items t \
                     WHERE s.pid = pg_backend_pid() ORDER BY t.id";

fn url(host: &str, port: u16, query: &str) -> String {
    let (user, password) = images::POSTGRES_USER;
    format!("postgres://{user}:{password}@{host}:{port}/postgres?{query}")
}

fn verify(mode: &str, ca: &Path) -> String {
    format!("sslmode={mode}&sslrootcert={}", ca.display())
}

async fn read(url: String) -> Result<Vec<Value>, FaucetError> {
    let source = PostgresSource::new(PostgresSourceConfig::new(url, QUERY)).await?;
    source.fetch_all().await
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_every_sslmode_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();

    images::psql(
        &pg,
        "CREATE TABLE items (id INT PRIMARY KEY, v TEXT); INSERT INTO items VALUES (1, 'a'), (2, 'b');",
    )
    .await;

    let expected = vec![
        json!({"ssl": true, "id": 1, "v": "a"}),
        json!({"ssl": true, "id": 2, "v": "b"}),
    ];
    for (host, query) in [
        ("localhost", "sslmode=require".to_string()),
        ("localhost", "sslmode=prefer".to_string()),
        ("localhost", verify("verify-ca", &tls.ca_path())),
        ("localhost", verify("verify-full", &tls.ca_path())),
    ] {
        let rows = read(url(host, port, &query))
            .await
            .unwrap_or_else(|e| panic!("{host} {query}: {e}"));
        assert_eq!(rows, expected, "{host} {query}");
    }

    let refused = [
        ("localhost", "sslmode=disable".to_string(), "plaintext"),
        (
            "localhost",
            verify("verify-ca", &tls.other_ca_path()),
            "unknown CA",
        ),
        (
            "localhost",
            verify("verify-full", &tls.other_ca_path()),
            "unknown CA",
        ),
        (
            "127.0.0.1",
            verify("verify-full", &tls.ca_path()),
            "hostname",
        ),
    ];
    for (host, query, why) in refused {
        match read(url(host, port, &query)).await {
            Err(FaucetError::Config(msg)) => {
                assert!(msg.contains("PostgreSQL connection failed"), "{why}: {msg}")
            }
            other => panic!("{host} {query} ({why}) must be refused, got {other:?}"),
        }
    }
}

/// `sslmode=verify-ca` must trust the CA without checking the hostname. With
/// sqlx 0.8.6 and rustls 0.23.46 it checks the hostname too (#974).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "sqlx 0.8.6 verify-ca hostname bug (#974); re-enable when sqlx is fixed"]
async fn verify_ca_ignores_a_name_off_the_certificate() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
    images::psql(
        &pg,
        "CREATE TABLE items (id INT PRIMARY KEY, v TEXT); INSERT INTO items VALUES (1, 'a');",
    )
    .await;
    let rows = read(url("127.0.0.1", port, &verify("verify-ca", &tls.ca_path())))
        .await
        .expect("verify-ca by an IP the certificate does not name");
    assert_eq!(rows, vec![json!({"ssl": true, "id": 1, "v": "a"})]);
}

/// Today's behaviour, pinned until #974 is fixed upstream: `verify-ca` by a
/// name the certificate does not cover is refused like `verify-full`, with a
/// typed error.
#[tokio::test(flavor = "multi_thread")]
async fn verify_ca_and_verify_full_refuse_a_name_off_the_certificate_today() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
    for mode in ["verify-ca", "verify-full"] {
        match read(url("127.0.0.1", port, &verify(mode, &tls.ca_path()))).await {
            Err(FaucetError::Config(msg)) => assert!(
                msg.contains("PostgreSQL connection failed") && msg.contains("not valid for name"),
                "{mode}: {msg}"
            ),
            other => panic!("{mode} by 127.0.0.1 must be refused today, got {other:?}"),
        }
    }
}

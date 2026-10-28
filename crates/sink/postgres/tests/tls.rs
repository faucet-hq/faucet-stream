//! The PostgreSQL sink over TLS (#967) against a `postgres:16-alpine` that
//! rejects every non-TLS connection: each `sslmode` writes rows (by `INSERT`
//! and by `COPY`), and the verify modes refuse an unknown CA or a name the
//! certificate does not cover.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Sink};
use faucet_sink_postgres::{
    PostgresColumnMapping, PostgresSink, PostgresSinkConfig, PostgresWriteMethod,
};
use serde_json::json;
use std::path::Path;

fn url(host: &str, port: u16, query: &str) -> String {
    let (user, password) = images::POSTGRES_USER;
    format!("postgres://{user}:{password}@{host}:{port}/postgres?{query}")
}

fn verify(mode: &str, ca: &Path) -> String {
    format!("sslmode={mode}&sslrootcert={}", ca.display())
}

fn config(url: String, method: PostgresWriteMethod) -> PostgresSinkConfig {
    PostgresSinkConfig::new(url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_write_method(method)
        .with_batch_size(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_over_every_sslmode_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
    images::psql(&pg, "CREATE TABLE kv (id INT PRIMARY KEY, mode TEXT)").await;

    let modes = [
        (
            "localhost",
            "sslmode=require".to_string(),
            PostgresWriteMethod::Insert,
        ),
        (
            "localhost",
            "sslmode=prefer".to_string(),
            PostgresWriteMethod::Copy,
        ),
        (
            "localhost",
            verify("verify-ca", &tls.ca_path()),
            PostgresWriteMethod::Insert,
        ),
        (
            "localhost",
            verify("verify-full", &tls.ca_path()),
            PostgresWriteMethod::Copy,
        ),
    ];
    for (i, (host, query, method)) in modes.iter().enumerate() {
        let sink = PostgresSink::new(config(url(host, port, query), *method))
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let n = sink
            .write_batch(&[json!({"id": i, "mode": query})])
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(n, 1, "{query}");
        sink.flush().await.unwrap();
    }
    let written = images::psql(&pg, "SELECT string_agg(id::text, ',' ORDER BY id) FROM kv").await;
    assert_eq!(written.trim(), "0,1,2,3");

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
        match PostgresSink::new(config(url(host, port, &query), PostgresWriteMethod::Insert)).await
        {
            Err(FaucetError::Sink(msg)) => {
                assert!(msg.contains("PostgreSQL connection failed"), "{why}: {msg}")
            }
            Err(other) => panic!("{query} ({why}): unexpected error {other:?}"),
            Ok(_) => panic!("{query} ({why}) must be refused"),
        }
    }
}

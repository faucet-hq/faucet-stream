//! The MySQL source over TLS (#967) against a `mysql:8.1` started with
//! `require_secure_transport=ON`: each `ssl-mode` reads real rows over TLS,
//! and the verify modes refuse an unknown CA or a name the certificate does
//! not cover.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source};
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use serde_json::Value;
use std::path::Path;

const QUERY: &str = "SELECT t.id, t.v, (SELECT VARIABLE_VALUE FROM performance_schema.session_status \
                     WHERE VARIABLE_NAME = 'Ssl_version') AS tls FROM items t ORDER BY t.id";

fn url(host: &str, port: u16, query: &str) -> String {
    let password = images::MYSQL_ROOT_PASSWORD;
    let db = images::MYSQL_DATABASE;
    format!("mysql://root:{password}@{host}:{port}/{db}?{query}")
}

fn verify(mode: &str, ca: &Path) -> String {
    format!("ssl-mode={mode}&ssl-ca={}", ca.display())
}

async fn read(url: String) -> Result<Vec<Value>, FaucetError> {
    MysqlSource::new(MysqlSourceConfig::new(url, QUERY))
        .await?
        .fetch_all()
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_over_every_ssl_mode_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let db = containers::start(|| images::mysql(&tls)).await;
    let port = db.get_host_port_ipv4(images::MYSQL_PORT).await.unwrap();
    images::mysql_exec(
        &db,
        "CREATE TABLE items (id INT PRIMARY KEY, v TEXT); INSERT INTO items VALUES (1, 'a'), (2, 'b');",
    )
    .await;

    for (host, query) in [
        ("localhost", "ssl-mode=REQUIRED".to_string()),
        ("localhost", "ssl-mode=PREFERRED".to_string()),
        ("localhost", verify("VERIFY_CA", &tls.ca_path())),
        ("localhost", verify("VERIFY_IDENTITY", &tls.ca_path())),
    ] {
        let rows = read(url(host, port, &query))
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, [1, 2], "{query}: {rows:?}");
        assert_eq!(rows[0]["v"], "a");
        let version = rows[0]["tls"].as_str().unwrap_or_default();
        assert!(version.starts_with("TLSv1."), "{query}: not TLS: {rows:?}");
    }

    let refused = [
        ("localhost", "ssl-mode=DISABLED".to_string(), "plaintext"),
        (
            "localhost",
            verify("VERIFY_CA", &tls.other_ca_path()),
            "unknown CA",
        ),
        (
            "localhost",
            verify("VERIFY_IDENTITY", &tls.other_ca_path()),
            "unknown CA",
        ),
        (
            "127.0.0.1",
            verify("VERIFY_IDENTITY", &tls.ca_path()),
            "hostname",
        ),
    ];
    for (host, query, why) in refused {
        match read(url(host, port, &query)).await {
            Err(FaucetError::Config(msg)) => {
                assert!(msg.contains("MySQL connection failed"), "{why}: {msg}")
            }
            other => panic!("{query} ({why}) must be refused, got {other:?}"),
        }
    }
}

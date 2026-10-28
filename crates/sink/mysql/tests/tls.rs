//! The MySQL sink over TLS (#967) against a `mysql:8.1` started with
//! `require_secure_transport=ON`: each `ssl-mode` writes rows, and the verify
//! modes refuse an unknown CA or a name the certificate does not cover.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Sink};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use serde_json::json;
use std::path::Path;

fn url(host: &str, port: u16, query: &str) -> String {
    let password = images::MYSQL_ROOT_PASSWORD;
    let db = images::MYSQL_DATABASE;
    format!("mysql://root:{password}@{host}:{port}/{db}?{query}")
}

fn verify(mode: &str, ca: &Path) -> String {
    format!("ssl-mode={mode}&ssl-ca={}", ca.display())
}

fn config(url: String) -> MysqlSinkConfig {
    MysqlSinkConfig::new(url, "kv").column_mapping(MysqlColumnMapping::AutoMap)
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_over_every_ssl_mode_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let db = containers::start(|| images::mysql(&tls)).await;
    let port = db.get_host_port_ipv4(images::MYSQL_PORT).await.unwrap();
    images::mysql_exec(&db, "CREATE TABLE kv (id INT PRIMARY KEY, mode TEXT)").await;

    let modes = [
        "ssl-mode=REQUIRED".to_string(),
        "ssl-mode=PREFERRED".to_string(),
        verify("VERIFY_CA", &tls.ca_path()),
        verify("VERIFY_IDENTITY", &tls.ca_path()),
    ];
    for (i, query) in modes.iter().enumerate() {
        let sink = MysqlSink::new(config(url("localhost", port, query)))
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        let n = sink
            .write_batch(&[json!({"id": i, "mode": query})])
            .await
            .unwrap_or_else(|e| panic!("{query}: {e}"));
        assert_eq!(n, 1, "{query}");
        sink.flush().await.unwrap();
    }
    let written = images::mysql_exec(&db, "SELECT GROUP_CONCAT(id ORDER BY id) FROM kv").await;
    assert_eq!(written.trim(), "0,1,2,3");

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
        match MysqlSink::new(config(url(host, port, &query))).await {
            Err(FaucetError::Sink(msg)) => {
                assert!(msg.contains("MySQL connection failed"), "{why}: {msg}")
            }
            Err(other) => panic!("{query} ({why}): unexpected error {other:?}"),
            Ok(_) => panic!("{query} ({why}) must be refused"),
        }
    }
}

//! Binlog replication over TLS (#967) against a `mysql:8.1` started with
//! `require_secure_transport=ON`: each `CdcTls` mode streams a real change,
//! and the verify modes refuse an unknown CA or a name the certificate does
//! not cover.
#![allow(clippy::disallowed_methods)] // the writer waits for the stream to open

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source, Value};
use faucet_source_mysql_cdc::{CdcTls, MysqlCdcSource, MysqlCdcSourceConfig};
use serde_json::json;

fn config(host: &str, port: u16, tls: &CdcTls) -> MysqlCdcSourceConfig {
    let password = images::MYSQL_ROOT_PASSWORD;
    let db = images::MYSQL_DATABASE;
    serde_json::from_value(json!({
        "connection_url": format!("mysql://root:{password}@{host}:{port}/{db}"),
        "server_id": 1001,
        "start_position": { "type": "current" },
        "idle_timeout": 3,
        "batch_size": 0,
        "tls": tls,
    }))
    .expect("config")
}

async fn changes_after(
    db: &testcontainers::ContainerAsync<testcontainers::GenericImage>,
    cfg: MysqlCdcSourceConfig,
    insert: &str,
) -> Result<Vec<Value>, FaucetError> {
    let source = MysqlCdcSource::new(cfg).await?;
    let write = async {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        images::mysql_exec(db, insert).await;
    };
    let (records, ()) = tokio::join!(source.fetch_all(), write);
    records
}

#[tokio::test(flavor = "multi_thread")]
async fn replicates_over_every_tls_mode_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let db = containers::start(|| images::mysql(&tls)).await;
    let port = db.get_host_port_ipv4(images::MYSQL_PORT).await.unwrap();
    images::mysql_exec(
        &db,
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64))",
    )
    .await;

    let ca = Some(tls.ca_path().display().to_string());
    let modes = [
        ("require", CdcTls::Require),
        (
            "verify_ca",
            CdcTls::VerifyCa {
                ca_path: ca.clone(),
            },
        ),
        (
            "verify_full",
            CdcTls::VerifyFull {
                ca_path: ca.clone(),
            },
        ),
    ];
    for (i, (name, mode)) in modes.iter().enumerate() {
        let insert = format!("INSERT INTO users VALUES ({i}, '{name}')");
        let records = changes_after(&db, config("localhost", port, mode), &insert)
            .await
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let created: Vec<&Value> = records
            .iter()
            .filter(|r| r["op"] == "c" && r["after"]["id"] == i as i64)
            .collect();
        assert_eq!(created.len(), 1, "{name}: {records:?}");
        assert_eq!(created[0]["after"]["name"], *name);
    }

    let other = Some(tls.other_ca_path().display().to_string());
    let refused = [
        ("disable", "localhost", CdcTls::Disable),
        (
            "unknown_ca",
            "localhost",
            CdcTls::VerifyFull {
                ca_path: other.clone(),
            },
        ),
        (
            "unknown_ca_chain",
            "localhost",
            CdcTls::VerifyCa { ca_path: other },
        ),
        ("hostname", "127.0.0.1", CdcTls::VerifyFull { ca_path: ca }),
    ];
    for (name, host, mode) in refused {
        let source = MysqlCdcSource::new(config(host, port, &mode)).await;
        let outcome = match source {
            Err(e) => Err(e),
            Ok(s) => s.fetch_all().await,
        };
        assert!(outcome.is_err(), "{name} must be refused: {outcome:?}");
    }
}

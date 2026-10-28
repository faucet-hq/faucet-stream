//! Logical replication over TLS (#967) against a `postgres:16-alpine` that
//! rejects every non-TLS connection (replication included): each `CdcTls`
//! mode streams real changes, and the verify modes refuse an unknown CA or a
//! name the certificate does not cover.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::{FaucetError, Source, Value};
use faucet_source_postgres_cdc::{CdcTls, PostgresCdcSource, PostgresCdcSourceConfig, SlotType};
use std::time::Duration;

fn url(host: &str, port: u16, query: &str) -> String {
    let (user, password) = images::POSTGRES_USER;
    format!("postgres://{user}:{password}@{host}:{port}/postgres{query}")
}

fn cfg(url: String, slot: &str, tls: CdcTls) -> PostgresCdcSourceConfig {
    PostgresCdcSourceConfig {
        connection_url: url,
        slot_name: slot.into(),
        publication_name: "faucet_pub".into(),
        create_slot_if_missing: true,
        slot_type: SlotType::Permanent,
        tls,
        start_lsn: None,
        proto_version: 1,
        idle_timeout: Duration::from_secs(2),
        max_messages: None,
        max_cycle_duration: Duration::from_secs(60),
        max_staged_records: None,
        max_buffered_events: 10_000,
        max_buffered_bytes: 64 * 1024 * 1024,
        status_update_interval: Duration::from_secs(1),
        tcp_keepalive: Duration::from_secs(60),
        batch_size: faucet_core::DEFAULT_BATCH_SIZE,
        slot_acquire_retries: 3,
    }
}

async fn fetch(cfg: PostgresCdcSourceConfig) -> Result<Vec<Value>, FaucetError> {
    let source = PostgresCdcSource::new(cfg).await?;
    Ok(source.fetch_all_incremental().await?.0)
}

#[tokio::test(flavor = "multi_thread")]
async fn replicates_over_every_tls_mode_and_refuses_bad_certificates() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
    images::psql(
        &pg,
        "CREATE TABLE users (id int4 PRIMARY KEY, name text); \
         CREATE PUBLICATION faucet_pub FOR TABLE users;",
    )
    .await;

    let ca = Some(tls.ca_path().display().to_string());
    let verify_full_url = format!(
        "?sslmode=verify-full&sslrootcert={}",
        tls.ca_path().display()
    );
    let modes = [
        ("require", url("localhost", port, ""), CdcTls::Require),
        (
            "verify_ca",
            url("localhost", port, ""),
            CdcTls::VerifyCa {
                ca_path: ca.clone(),
            },
        ),
        (
            "verify_full",
            url("localhost", port, ""),
            CdcTls::VerifyFull {
                ca_path: ca.clone(),
            },
        ),
        (
            "from_url",
            url("localhost", port, &verify_full_url),
            CdcTls::FromUrl,
        ),
    ];
    for (i, (slot, url, mode)) in modes.into_iter().enumerate() {
        let config = cfg(url, slot, mode);
        let warm = fetch(config.clone())
            .await
            .unwrap_or_else(|e| panic!("{slot}: {e}"));
        assert!(warm.iter().all(|r| r["op"] != "insert"), "{slot}: {warm:?}");
        images::psql(&pg, &format!("INSERT INTO users VALUES ({i}, '{slot}')")).await;
        let records = fetch(config)
            .await
            .unwrap_or_else(|e| panic!("{slot}: {e}"));
        let inserted: Vec<&Value> = records
            .iter()
            .filter(|r| r["op"] == "insert" && r["after"]["id"] == i as i64)
            .collect();
        assert_eq!(inserted.len(), 1, "{slot}: {records:?}");
        assert_eq!(inserted[0]["after"]["name"], slot);
    }

    let other = Some(tls.other_ca_path().display().to_string());
    let refused = [
        ("disable", url("localhost", port, ""), CdcTls::Disable),
        (
            "unknown_ca",
            url("localhost", port, ""),
            CdcTls::VerifyFull {
                ca_path: other.clone(),
            },
        ),
        (
            "unknown_ca_chain",
            url("localhost", port, ""),
            CdcTls::VerifyCa { ca_path: other },
        ),
        (
            "hostname",
            url("127.0.0.1", port, ""),
            CdcTls::VerifyFull { ca_path: ca },
        ),
    ];
    for (slot, url, mode) in refused {
        assert!(
            fetch(cfg(url, slot, mode)).await.is_err(),
            "{slot} must be refused"
        );
    }
}

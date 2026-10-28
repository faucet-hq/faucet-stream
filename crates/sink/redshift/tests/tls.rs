//! The Redshift connection over every `tls_mode` (#967), against a Postgres
//! (Redshift's wire protocol) that rejects every non-TLS connection: each mode
//! that should connect writes a row, and the verify modes refuse an unknown CA
//! or a host name the certificate does not cover.

use faucet_conformance::containers;
use faucet_conformance::tls::{TlsFixture, images};
use faucet_core::Sink;
use faucet_sink_redshift::{
    RedshiftConnection, RedshiftCopyFormat, RedshiftSink, RedshiftSinkConfig, RedshiftTlsMode,
    RedshiftWriteStrategy,
};
use serde_json::json;

fn config(
    host: &str,
    port: u16,
    mode: RedshiftTlsMode,
    root: Option<String>,
) -> RedshiftSinkConfig {
    let (user, password) = images::POSTGRES_USER;
    let mut conn = RedshiftConnection::new(host, "postgres", user, password);
    conn.port = port;
    conn.tls_mode = Some(mode);
    conn.ssl_root_cert = root;
    RedshiftSinkConfig {
        connection: conn,
        table_name: "kv".into(),
        create_table: false,
        commit_rows: None,
        commit_bytes: None,
        schema: None,
        write_strategy: RedshiftWriteStrategy::Insert,
        copy: None,
        copy_format: RedshiftCopyFormat::Jsonl,
        staging_bucket: None,
        staging_prefix: String::new(),
        iam_role: None,
        region: None,
        endpoint_url: None,
        batch_size: 0,
        max_connections: 1,
    }
}

async fn write(cfg: RedshiftSinkConfig, id: usize) -> Result<(), String> {
    let sink = RedshiftSink::new(cfg).await.map_err(|e| e.to_string())?;
    sink.write_batch(&[json!({"id": id})])
        .await
        .map_err(|e| e.to_string())?;
    sink.flush().await.map_err(|e| e.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn every_tls_mode_connects_or_refuses_as_documented() {
    let tls = TlsFixture::for_names(&["localhost"]);
    let pg = containers::start(|| images::postgres(&tls)).await;
    let port = pg.get_host_port_ipv4(images::POSTGRES_PORT).await.unwrap();
    images::psql(&pg, "CREATE TABLE kv (id INT PRIMARY KEY)").await;
    let ca = || Some(tls.ca_path().display().to_string());
    let other = || Some(tls.other_ca_path().display().to_string());

    let connects = [
        (RedshiftTlsMode::Prefer, None),
        (RedshiftTlsMode::Require, None),
        (RedshiftTlsMode::VerifyCa, ca()),
        (RedshiftTlsMode::VerifyFull, ca()),
    ];
    for (i, (mode, root)) in connects.into_iter().enumerate() {
        write(config("localhost", port, mode, root), i)
            .await
            .unwrap_or_else(|e| panic!("{mode:?}: {e}"));
    }
    let written = images::psql(&pg, "SELECT string_agg(id::text, ',' ORDER BY id) FROM kv").await;
    assert_eq!(written.trim(), "0,1,2,3");

    let refused = [
        ("localhost", RedshiftTlsMode::Disable, None, "plaintext"),
        (
            "localhost",
            RedshiftTlsMode::VerifyCa,
            other(),
            "unknown CA",
        ),
        (
            "localhost",
            RedshiftTlsMode::VerifyFull,
            other(),
            "unknown CA",
        ),
        ("127.0.0.1", RedshiftTlsMode::VerifyFull, ca(), "hostname"),
    ];
    for (i, (host, mode, root, why)) in refused.into_iter().enumerate() {
        let err = write(config(host, port, mode, root), 10 + i)
            .await
            .expect_err(why);
        assert!(err.contains("redshift"), "{mode:?} ({why}): {err}");
    }
    let written = images::psql(&pg, "SELECT count(*) FROM kv").await;
    assert_eq!(written.trim(), "4", "a refused connection writes nothing");
}

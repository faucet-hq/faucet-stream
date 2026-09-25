//! Runs the reusable `faucet-conformance` battery against the real Microsoft SQL
//! Server sink.
//!
//! - check 1 `assert_config_schema_valid_value` — static, no Docker; passing it
//!   is the Tier-1 (supported) criterion.
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`) leaves no duplicates.
//! - check 5 `assert_capabilities_truthful` — the advertised idempotent /
//!   upsert / schema-evolution capabilities all hold.
//!
//! Checks 4 and 5 boot a keyed upsert sink against a real SQL Server container.
//! The SQL Server image does not boot on Apple-Silicon hosts (QEMU); CI runs
//! these tests. Locally, `--no-run` confirms they compile. Startups are
//! serialized (each container needs ~2 GB RAM).

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig, OnUnknownField};
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each — serialize startups.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_mssql::MssqlSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "mssql");
}

fn conn_cfg(port: u16) -> MssqlConnectionConfig {
    MssqlConnectionConfig {
        connection_url: Some(format!("mssql://sa:{ENCODED_PW}@127.0.0.1:{port}/master")),
        connection_string: None,
        tls: MssqlTls {
            mode: MssqlTlsMode::TrustServerCertificate,
            ca_cert_path: None,
        },
    }
}

async fn exec(pool: &MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

/// A fresh container with a keyed table `dbo.t(id PK, v)`, plus an upsert-mode
/// MSSQL sink and the connection pool used to count rows.
async fn fresh_sink() -> (ContainerAsync<MssqlServer>, MssqlPool, MssqlSink) {
    let container = MssqlServer::default()
        .with_accept_eula()
        .start()
        .await
        .expect("start mssql container");
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, v NVARCHAR(200))",
    )
    .await;

    let mut scfg = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), "dbo.t");
    scfg.connection.tls = cfg.tls.clone();
    scfg.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: OnUnknownField::Warn,
    };
    scfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: Some(DeleteMarker {
            field: "__op".to_string(),
            values: vec!["d".to_string()],
        }),
        rollback: None,
    };
    let sink = MssqlSink::new(scfg).await.expect("sink");
    (container, pool, sink)
}

async fn count_rows(pool: &MssqlPool) -> usize {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query("SELECT COUNT(*) AS c FROM dbo.t", &[])
        .await
        .expect("count query")
        .into_first_result()
        .await
        .expect("count result");
    rows[0].get::<i32, _>("c").expect("count value") as usize
}

/// Empty the data table so a following check starts from a known-clean state.
async fn reset_rows(pool: &MssqlPool) {
    exec(pool, "DELETE FROM dbo.t").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_idempotent_replay() {
    let _serial = SERIAL.lock().await;
    let (_container, pool, sink) = fresh_sink().await;
    // Check 10: connector_name is non-empty.
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    faucet_conformance::assert_idempotent_replay(&sink, || {
        let pool = pool.clone();
        async move { count_rows(&pool).await }
    })
    .await;
    // Check 8: schema evolution is effective (add-column surfaces in a fresh
    // current_schema()). Independent of row state, so it can follow the replay.
    faucet_conformance::assert_schema_evolution_effective(&sink).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let _serial = SERIAL.lock().await;
    let (_container, pool, sink) = fresh_sink().await;
    // Check 11: the sink's preflight check() returns Ok(report) with well-formed
    // probes (non-mutating).
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let pool = pool.clone();
        async move { count_rows(&pool).await }
    })
    .await;
    // Check 7: write modes are truthful (upsert converges, delete removes,
    // missing/null keys are reported failed). Needs a clean table because the
    // capabilities check above wrote rows.
    reset_rows(&pool).await;
    faucet_conformance::assert_write_modes_truthful(&sink, || {
        let pool = pool.clone();
        async move { count_rows(&pool).await }
    })
    .await;
}

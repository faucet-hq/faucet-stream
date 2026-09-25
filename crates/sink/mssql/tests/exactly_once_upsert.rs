//! Composition test (#190): exactly-once delivery + `write_mode: upsert` for
//! the Microsoft SQL Server sink, against a real SQL Server in Docker.
//!
//! Verifies that `write_batch_idempotent` routes through the upsert planner so
//! the data write (MERGE) AND the commit-token watermark MERGE commit atomically
//! in one `BEGIN TRAN`. Re-writing the same key in a later page (with a higher
//! token) must UPDATE the row in place — not duplicate it — and advance the
//! token.
//!
//! Requires Docker. The SQL Server image does not boot on Apple-Silicon hosts
//! (QEMU); CI runs this test. Locally, `--no-run` confirms it compiles.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::{Sink, WriteMode, WriteSpec, format_token};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig, OnUnknownField};
use serde_json::json;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each — serialize startups.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_mssql() -> (ContainerAsync<MssqlServer>, u16) {
    let container = MssqlServer::default()
        .with_accept_eula()
        .start()
        .await
        .expect("start mssql container");
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    (container, port)
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

async fn count(pool: &MssqlPool, table: &str) -> i32 {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(format!("SELECT COUNT(*) AS c FROM {table}"), &[])
        .await
        .expect("count query")
        .into_first_result()
        .await
        .expect("count result");
    rows[0].get::<i32, _>("c").expect("count value")
}

async fn name_of(pool: &MssqlPool, table: &str, id: i32) -> String {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(format!("SELECT name FROM {table} WHERE id = @P1"), &[&id])
        .await
        .expect("name query")
        .into_first_result()
        .await
        .expect("name result");
    rows[0]
        .get::<&str, _>("name")
        .expect("name value")
        .to_string()
}

fn upsert_sink_cfg(cfg: &MssqlConnectionConfig, write: WriteSpec) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), "dbo.t");
    s.connection.tls = cfg.tls.clone();
    s.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: OnUnknownField::Warn,
    };
    s.write = write;
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_upsert_updates_in_place_and_advances_token() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.t (id INT PRIMARY KEY, name NVARCHAR(255))",
    )
    .await;

    let scfg = upsert_sink_cfg(
        &cfg,
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
            rollback: None,
        },
    );
    let sink = MssqlSink::new(scfg).await.expect("sink");

    let scope = "dbo.t::r1";
    let t1 = format_token(1);
    let t2 = format_token(2);

    // Page 1, token ...0001: upsert id=1 -> "a" in the idempotent path.
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "a"})], scope, &t1)
        .await
        .expect("idempotent upsert 1");
    assert_eq!(written, 1, "one upsert applied");
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token read"),
        Some(t1.clone()),
        "token advanced to t1"
    );
    assert_eq!(count(&pool, "dbo.t").await, 1);
    assert_eq!(name_of(&pool, "dbo.t", 1).await, "a");

    // Page 2, token ...0002: upsert id=1 -> "b".
    let written = sink
        .write_batch_idempotent(&[json!({"id": 1, "name": "b"})], scope, &t2)
        .await
        .expect("idempotent upsert 2");
    assert_eq!(written, 1, "one upsert applied (update in place)");

    // Exactly ONE row id=1, now "b" — upserted in the idempotent path, not duplicated.
    assert_eq!(
        count(&pool, "dbo.t").await,
        1,
        "upsert in the idempotent path must update, not duplicate"
    );
    assert_eq!(
        name_of(&pool, "dbo.t", 1).await,
        "b",
        "row must reflect the latest value 'b'"
    );

    // Token advanced to t2 — committed atomically with the upsert.
    assert_eq!(
        sink.last_committed_token(scope).await.expect("token read"),
        Some(t2),
        "token advanced to t2 alongside the upsert"
    );
}

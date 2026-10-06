//! Integration tests for MSSQL sink schema introspection + evolution (#194).
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image). Run with:
//! `cargo test -p faucet-sink-mssql --test schema_evolution`.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::{ColumnChange, SchemaEvolution, Sink};
use serde_json::json;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each; serialize so parallel tests don't
// start several containers at once and exhaust the runner.
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

fn sink_cfg(cfg: &MssqlConnectionConfig, table: &str) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), table);
    s.connection.tls = cfg.tls.clone();
    s.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    s
}

use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig};

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_and_evolve_add_and_widen() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(&pool, "CREATE TABLE dbo.t (id BIGINT)").await;

    let sink = MssqlSink::new(sink_cfg(&cfg, "dbo.t")).await.expect("sink");

    // current_schema: id is integer, nullable (no NOT NULL declared).
    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("table exists");
    let id_ty = &schema["properties"]["id"]["type"];
    // BIGINT → integer; the column allows NULL so the type widens to an array.
    assert!(
        id_ty == &json!("integer") || id_ty == &json!(["integer", "null"]),
        "id should be integer (got {id_ty})"
    );

    // Evolve: add `email` (NVARCHAR(MAX) ← Text) and widen `id` → FLOAT (number).
    let evolution = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evolution).await.expect("evolve");

    // Re-query: email present (string), id now number.
    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("table exists");
    let email_ty = &schema["properties"]["email"]["type"];
    assert!(
        email_ty == &json!("string") || email_ty == &json!(["string", "null"]),
        "email should be string (got {email_ty})"
    );
    let id_ty = &schema["properties"]["id"]["type"];
    assert!(
        id_ty == &json!("number") || id_ty == &json!(["number", "null"]),
        "id should be widened to number (got {id_ty})"
    );

    // Idempotent re-run: the guarded ADD + ALTER are no-ops and must not error.
    sink.evolve_schema(&evolution)
        .await
        .expect("idempotent re-run");

    // A write that uses the new column must succeed (cache was invalidated).
    let written = sink
        .write_batch(&[json!({"id": 1, "email": "a@b.c"})])
        .await
        .expect("write after evolve");
    assert_eq!(written, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_returns_none_for_missing_table() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);

    let sink = MssqlSink::new(sink_cfg(&cfg, "dbo.does_not_exist"))
        .await
        .expect("sink");
    assert!(
        sink.current_schema()
            .await
            .expect("current_schema")
            .is_none(),
        "missing table → None"
    );
}

/// SQL-34: relaxing NOT NULL re-states the column's exact declared type —
/// precision, scale, length and collation survive.
#[tokio::test(flavor = "multi_thread")]
async fn relaxing_nullability_keeps_the_declared_type() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.money (id INT NOT NULL, amount DECIMAL(38,10) NOT NULL, \
         created DATETIME2(3) NOT NULL, code VARCHAR(20) COLLATE Latin1_General_CS_AS NOT NULL)",
    )
    .await;
    let sink = MssqlSink::new(sink_cfg(&cfg, "dbo.money"))
        .await
        .expect("sink");
    let evolution = SchemaEvolution {
        additions: vec![],
        widenings: vec![],
        relax_nullability: vec!["amount".into(), "created".into(), "code".into()],
    };
    sink.evolve_schema(&evolution).await.expect("evolve");

    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(
            "SELECT c.name, TYPE_NAME(c.system_type_id) AS t, c.precision, c.scale, \
             c.max_length, c.collation_name, c.is_nullable FROM sys.columns c \
             WHERE c.object_id = OBJECT_ID('dbo.money') ORDER BY c.column_id",
            &[],
        )
        .await
        .expect("columns")
        .into_first_result()
        .await
        .expect("columns");
    let describe = |name: &str| {
        let r = rows
            .iter()
            .find(|r| r.get::<&str, _>("name") == Some(name))
            .expect("column");
        (
            r.get::<&str, _>("t").unwrap().to_string(),
            r.get::<u8, _>("precision").unwrap(),
            r.get::<u8, _>("scale").unwrap(),
            r.get::<i16, _>("max_length").unwrap(),
            r.get::<&str, _>("collation_name").map(str::to_string),
            r.get::<bool, _>("is_nullable").unwrap(),
        )
    };
    assert_eq!(
        describe("amount"),
        ("decimal".into(), 38, 10, 17, None, true)
    );
    assert_eq!(
        describe("created"),
        ("datetime2".into(), 23, 3, 7, None, true)
    );
    assert_eq!(
        describe("code"),
        (
            "varchar".into(),
            0,
            0,
            20,
            Some("Latin1_General_CS_AS".into()),
            true
        )
    );
    // `id` was not relaxed.
    assert!(!describe("id").5);

    drop(conn);
    sink.write_batch(&[json!({
        "id": 1,
        "amount": "12345678901234567.1234567891",
        "created": null,
        "code": "Ab"
    })])
    .await
    .expect("write a NULL into a relaxed column");
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(
            "SELECT CAST(amount AS NVARCHAR(60)) AS a FROM dbo.money",
            &[],
        )
        .await
        .expect("read")
        .into_first_result()
        .await
        .expect("read");
    assert_eq!(
        rows[0].get::<&str, _>("a"),
        Some("12345678901234567.1234567891")
    );
}

/// SQL-20: a JSON-column sink stores each record whole, so it reports no
/// destination schema and a `schema:` drift policy stays inert instead of
/// treating every record field as an addition.
#[tokio::test(flavor = "multi_thread")]
async fn json_column_mode_reports_no_schema() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 1).await.expect("pool");
    exec(
        &pool,
        "CREATE TABLE dbo.json_docs (id INT IDENTITY PRIMARY KEY, data NVARCHAR(MAX))",
    )
    .await;

    let mut s = sink_cfg(&cfg, "dbo.json_docs");
    s.column_mapping = MssqlColumnMapping::JsonColumn {
        column: "data".into(),
    };
    let sink = MssqlSink::new(s).await.expect("sink");
    assert!(sink.current_schema().await.expect("schema").is_none());
}

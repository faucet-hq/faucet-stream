//! Shared Spanner-emulator bootstrap for the integration + conformance tests.
//!
//! Boots `gcr.io/cloud-spanner-emulator/emulator` via testcontainers and
//! provisions an instance + database **programmatically** through the crate's
//! own admin client; the emulator endpoint rides the connector's
//! `emulator_host` config field, so parallel tests never race on a
//! process-global env var.
//!
//! The image is pinned: change-stream DDL fixes landed in 1.5.51 / 1.5.52.
#![allow(dead_code)]

use faucet_core::{FaucetError, Source, StreamPage};
use faucet_source_spanner_cdc::{SpannerCdcSourceConfig, SpannerConnection, SpannerCredentials};
use futures::StreamExt;
use gcloud_googleapis::spanner::admin::database::v1::{
    CreateDatabaseRequest, UpdateDatabaseDdlRequest,
};
use gcloud_googleapis::spanner::admin::instance::v1::{CreateInstanceRequest, Instance};
use gcloud_spanner::client::Client;
use gcloud_spanner::statement::Statement;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use testcontainers::{ContainerAsync, GenericImage, core::IntoContainerPort, runners::AsyncRunner};

pub const PROJECT: &str = "test-project";
pub const INSTANCE: &str = "test-instance";
pub const EMULATOR_IMAGE: &str = "gcr.io/cloud-spanner-emulator/emulator";
pub const EMULATOR_TAG: &str = "1.5.55";

/// `POSTGRESQL` in the admin API's `DatabaseDialect` enum.
const DIALECT_POSTGRESQL: i32 = 2;

/// A running emulator + the `host:port` its gRPC endpoint is mapped to.
pub struct Emulator {
    pub _container: ContainerAsync<GenericImage>,
    pub host: String,
}

/// A missing test backend: a skip locally, a failure when CI requires the
/// backends (`FAUCET_REQUIRE_BACKENDS`).
fn backend_missing(why: &str) {
    if std::env::var("FAUCET_REQUIRE_BACKENDS").is_ok() {
        panic!("{why} (FAUCET_REQUIRE_BACKENDS is set)");
    }
    eprintln!("skipping: {why}");
}

/// Start the Spanner emulator. Returns `None` when Docker is unavailable so
/// tests skip cleanly on machines without a daemon.
pub async fn start_emulator() -> Option<Emulator> {
    let image = GenericImage::new(EMULATOR_IMAGE, EMULATOR_TAG).with_exposed_port(9010.tcp());
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            backend_missing(&format!("Spanner emulator: Docker not available ({e})"));
            return None;
        }
    };
    let port = container
        .get_host_port_ipv4(9010)
        .await
        .expect("emulator port");
    Some(Emulator {
        _container: container,
        host: format!("127.0.0.1:{port}"),
    })
}

/// A [`SpannerConnection`] pointed at the emulator.
pub fn connection(host: &str, database: &str) -> SpannerConnection {
    SpannerConnection {
        project_id: PROJECT.into(),
        instance: INSTANCE.into(),
        database: database.into(),
        auth: SpannerCredentials::ApplicationDefault,
        max_sessions: 20,
        emulator_host: Some(host.to_string()),
        dialect: Default::default(),
    }
}

/// Create the shared test instance (idempotent) and return the admin client.
/// The port maps before the emulator's gRPC server accepts calls, so instance
/// creation doubles as the readiness poll.
async fn ensure_instance(host: &str, database: &str) -> gcloud_spanner::admin::client::Client {
    let conn = connection(host, database);
    let mut admin = None;
    for _ in 0..120 {
        match conn.connect_admin().await {
            Ok(a) => {
                admin = Some(a);
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    let admin = admin.expect("spanner emulator admin endpoint never became reachable");

    let mut ready = false;
    for _ in 0..120 {
        let req = CreateInstanceRequest {
            parent: format!("projects/{PROJECT}"),
            instance_id: INSTANCE.into(),
            instance: Some(Instance {
                name: format!("projects/{PROJECT}/instances/{INSTANCE}"),
                config: format!("projects/{PROJECT}/instanceConfigs/emulator-config"),
                display_name: "faucet-test".into(),
                node_count: 1,
                ..Default::default()
            }),
        };
        match admin.instance().create_instance(req, None).await {
            Ok(mut op) => {
                op.wait(None).await.expect("instance LRO");
                ready = true;
                break;
            }
            Err(status) if status.code() == gcloud_gax::grpc::Code::AlreadyExists => {
                ready = true;
                break;
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    assert!(ready, "spanner emulator never became ready");
    admin
}

/// Create a GoogleSQL database with the given DDL; reports a DDL failure
/// instead of panicking (some options are not emulated).
pub async fn try_create_database(host: &str, database: &str, ddl: &[&str]) -> Result<(), String> {
    let admin = ensure_instance(host, database).await;
    let req = CreateDatabaseRequest {
        parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
        create_statement: format!("CREATE DATABASE `{database}`"),
        extra_statements: ddl.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    let mut op = admin
        .database()
        .create_database(req, None)
        .await
        .map_err(|e| e.to_string())?;
    op.wait(None).await.map(|_| ()).map_err(|e| e.to_string())
}

/// Create a GoogleSQL database with the given DDL.
pub async fn create_database(host: &str, database: &str, ddl: &[&str]) {
    try_create_database(host, database, ddl)
        .await
        .expect("create database");
}

/// Create a PostgreSQL-dialect database and apply `ddl` to it (PG databases
/// cannot take DDL in the create request).
pub async fn create_pg_database(host: &str, database: &str, ddl: &[&str]) {
    let admin = ensure_instance(host, database).await;
    let req = CreateDatabaseRequest {
        parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
        create_statement: format!("CREATE DATABASE \"{database}\""),
        database_dialect: DIALECT_POSTGRESQL,
        ..Default::default()
    };
    admin
        .database()
        .create_database(req, None)
        .await
        .expect("create pg database")
        .wait(None)
        .await
        .expect("pg database LRO");
    if ddl.is_empty() {
        return;
    }
    admin
        .database()
        .update_database_ddl(
            UpdateDatabaseDdlRequest {
                database: format!("projects/{PROJECT}/instances/{INSTANCE}/databases/{database}"),
                statements: ddl.iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("pg ddl")
        .wait(None)
        .await
        .expect("pg ddl LRO");
}

/// Raw data-plane client for seeding/inspecting test data.
pub async fn raw_client(host: &str, database: &str) -> Client {
    connection(host, database).connect().await.expect("client")
}

/// Run DML statements in one read-write transaction.
pub async fn execute_dml_txn(client: &Client, sqls: &[&str]) {
    let sqls: Vec<String> = sqls.iter().map(|s| s.to_string()).collect();
    client
        .read_write_transaction(|tx| {
            let sqls = sqls.clone();
            Box::pin(async move {
                for sql in sqls {
                    tx.update(Statement::new(sql)).await?;
                }
                Ok::<_, gcloud_spanner::client::Error>(())
            })
        })
        .await
        .expect("dml");
}

/// Run one DML statement in its own transaction.
pub async fn execute_dml(client: &Client, sql: &str) {
    execute_dml_txn(client, &[sql]).await;
}

/// The emulator's own clock, as an RFC 3339 string. Using it (rather than the
/// host clock) as a `start_from` avoids host/VM clock skew in the tests.
pub async fn emulator_now(client: &Client, pg: bool) -> String {
    let sql = if pg {
        "SELECT now() AS ts"
    } else {
        "SELECT CURRENT_TIMESTAMP() AS ts"
    };
    let mut tx = client.single().await.expect("tx");
    let mut it = tx.query(Statement::new(sql)).await.expect("query");
    let row = it.next().await.expect("row").expect("a row");
    let fields = it.columns_metadata().clone();
    faucet_common_spanner::decode::row_to_json(&row, &fields).expect("decode")["ts"]
        .as_str()
        .expect("ts string")
        .to_string()
}

/// A config for the emulator with a short heartbeat so bounded runs end fast.
pub fn cdc_config(
    host: &str,
    database: &str,
    stream: &str,
    start: Value,
) -> SpannerCdcSourceConfig {
    serde_json::from_value(json!({
        "project_id": PROJECT,
        "instance": INSTANCE,
        "database": database,
        "emulator_host": host,
        "max_sessions": 20,
        "change_stream": stream,
        "start_from": start,
        "heartbeat_ms": 1000,
        "idle_timeout": 30,
    }))
    .expect("config")
}

/// Like [`cdc_config`] for a PostgreSQL-dialect database.
pub fn pg_cdc_config(
    host: &str,
    database: &str,
    stream: &str,
    start: Value,
) -> SpannerCdcSourceConfig {
    let mut cfg = cdc_config(host, database, stream, start);
    cfg.connection.dialect = faucet_source_spanner_cdc::SpannerDialect::Postgresql;
    cfg
}

/// One drained run: its records, every page's size, and the last bookmark.
pub struct Run {
    pub records: Vec<Value>,
    pub pages: Vec<StreamPage>,
    pub bookmark: Option<Value>,
}

/// Drain one fetch cycle.
pub async fn run_once(source: &dyn Source) -> Result<Run, FaucetError> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut run = Run {
        records: Vec::new(),
        pages: Vec::new(),
        bookmark: None,
    };
    while let Some(page) = stream.next().await {
        let page = page?;
        run.records.extend(page.records.iter().cloned());
        if page.bookmark.is_some() {
            run.bookmark = page.bookmark.clone();
        }
        run.pages.push(page);
    }
    Ok(run)
}

/// Run fetch cycles, feeding each bookmark into the next, until at least `n`
/// records have been collected (or `timeout` passes). Looping is what a real
/// deployment does, and it keeps the tests independent of host/VM clock skew.
pub async fn collect(source: &dyn Source, n: usize, timeout: Duration) -> Vec<Value> {
    let deadline = Instant::now() + timeout;
    let mut all = Vec::new();
    loop {
        let run = run_once(source).await.expect("run");
        all.extend(run.records);
        if let Some(bm) = run.bookmark {
            source.apply_start_bookmark(bm).await.expect("bookmark");
        }
        if all.len() >= n || Instant::now() > deadline {
            return all;
        }
    }
}

/// `(op, id)` of every record, in delivery order.
pub fn ops_and_ids(records: &[Value]) -> Vec<(String, i64)> {
    records
        .iter()
        .map(|r| {
            (
                r["op"].as_str().unwrap_or("?").to_string(),
                r["document_key"]["id"].as_i64().unwrap_or(-1),
            )
        })
        .collect()
}

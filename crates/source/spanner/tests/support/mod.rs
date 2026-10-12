//! Shared Spanner-emulator bootstrap for the integration + conformance tests.
//!
//! Boots `gcr.io/cloud-spanner-emulator/emulator` via testcontainers and
//! provisions an instance + database **programmatically** through the crate's
//! own admin client — no `gcloud` sidecar, no `SPANNER_EMULATOR_HOST` env var
//! (the emulator endpoint rides the connector's `emulator_host` config field,
//! so parallel tests never race on process-global state).

#![allow(dead_code)] // each test binary uses a different subset of the helpers

use faucet_source_spanner::{SpannerConnection, SpannerCredentials};
use gcloud_googleapis::spanner::admin::database::v1::{
    CreateDatabaseRequest, UpdateDatabaseDdlRequest,
};
use gcloud_googleapis::spanner::admin::instance::v1::{CreateInstanceRequest, Instance};
use gcloud_spanner::client::Client;
use gcloud_spanner::statement::Statement;
use testcontainers::{ContainerAsync, GenericImage, core::IntoContainerPort};

pub const PROJECT: &str = "test-project";
pub const INSTANCE: &str = "test-instance";

/// A running emulator + the `host:port` its gRPC endpoint is mapped to.
pub struct Emulator {
    pub _container: ContainerAsync<GenericImage>,
    pub host: String,
}

/// Start the Spanner emulator. Returns `None` when Docker is unavailable so
/// tests skip cleanly on machines without a daemon.
pub async fn start_emulator() -> Option<Emulator> {
    let container = faucet_conformance::containers::start_or_skip(
        || {
            GenericImage::new("gcr.io/cloud-spanner-emulator/emulator", "latest")
                .with_exposed_port(9010.tcp())
        },
        &Default::default(),
    )
    .await?;
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

/// Create the shared test instance (idempotent: AlreadyExists is fine) and a
/// database with the given DDL. The port maps before the emulator's gRPC
/// server accepts calls, so instance creation doubles as the readiness poll.
pub async fn ensure_instance(host: &str, database: &str) -> gcloud_spanner::admin::client::Client {
    let conn = connection(host, database);
    // The mapped port exists before the emulator's gRPC server listens, so
    // even building the admin channel can hit ConnectionRefused — retry it.
    let mut admin = None;
    for _ in 0..120 {
        match conn.connect_admin().await {
            Ok(a) => {
                admin = Some(a);
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    let admin = admin.expect("spanner emulator admin endpoint never became reachable");

    let mut created = false;
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
                created = true;
                break;
            }
            Err(status) if status.code() == gcloud_gax::grpc::Code::AlreadyExists => {
                created = true;
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    assert!(created, "spanner emulator never became ready");
    admin
}

/// Create the shared instance (if needed) and a GoogleSQL database with `ddl`.
pub async fn create_database(host: &str, database: &str, ddl: &[&str]) {
    let admin = ensure_instance(host, database).await;
    let req = CreateDatabaseRequest {
        parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
        create_statement: format!("CREATE DATABASE `{database}`"),
        extra_statements: ddl.iter().map(|s| s.to_string()).collect(),
        ..Default::default()
    };
    admin
        .database()
        .create_database(req, None)
        .await
        .expect("create database")
        .wait(None)
        .await
        .expect("database LRO");
}

/// `DatabaseDialect::POSTGRESQL` on the wire.
const DIALECT_POSTGRESQL: i32 = 2;

/// A [`SpannerConnection`] for a PostgreSQL-dialect database.
pub fn pg_connection(host: &str, database: &str) -> SpannerConnection {
    let mut conn = connection(host, database);
    conn.dialect = faucet_source_spanner::SpannerDialect::Postgresql;
    conn
}

/// Create a PostgreSQL-dialect database and apply `ddl` (PG databases cannot
/// take DDL in the create request).
pub async fn create_pg_database(host: &str, database: &str, ddl: &[&str]) {
    let admin = ensure_instance(host, database).await;
    admin
        .database()
        .create_database(
            CreateDatabaseRequest {
                parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
                create_statement: format!("CREATE DATABASE \"{database}\""),
                database_dialect: DIALECT_POSTGRESQL,
                ..Default::default()
            },
            None,
        )
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

/// Run a DML statement (INSERT/UPDATE/DELETE) in a read-write transaction.
pub async fn execute_dml(client: &Client, sql: &str) {
    let sql = sql.to_string();
    client
        .read_write_transaction(|tx| {
            let sql = sql.clone();
            Box::pin(async move {
                tx.update(Statement::new(sql)).await?;
                Ok::<_, gcloud_spanner::client::Error>(())
            })
        })
        .await
        .expect("dml");
}

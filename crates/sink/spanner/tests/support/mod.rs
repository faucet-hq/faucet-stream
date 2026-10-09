//! Shared Cloud Spanner **emulator** bootstrap for integration tests.
//!
//! One emulator container is shared per test binary (via `OnceCell`); each
//! test creates its own database so tests stay isolated. The connector is
//! pointed at the emulator through the config-level `emulator_host` override
//! — no process-global `SPANNER_EMULATOR_HOST` env var, so parallel tests
//! never race.

use faucet_common_spanner::SpannerConnection;
use gcloud_googleapis::spanner::admin::database::v1::CreateDatabaseRequest;
use gcloud_googleapis::spanner::admin::instance::v1::{CreateInstanceRequest, Instance};
use gcloud_spanner::statement::Statement;
use testcontainers::core::ContainerPort;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::OnceCell;

pub const PROJECT: &str = "test-project";
pub const INSTANCE: &str = "test-instance";

static EMULATOR: OnceCell<(ContainerAsync<GenericImage>, String)> = OnceCell::const_new();

/// Start (once per test binary) and return the emulator's `host:port`.
pub async fn emulator_host() -> String {
    let (_container, host) = EMULATOR
        .get_or_init(|| async {
            let container = faucet_conformance::containers::start(|| {
                GenericImage::new("gcr.io/cloud-spanner-emulator/emulator", "latest")
                    .with_exposed_port(ContainerPort::Tcp(9010))
            })
            .await;
            let port = container
                .get_host_port_ipv4(9010)
                .await
                .expect("spanner emulator port");
            let host = format!("127.0.0.1:{port}");
            bootstrap_instance(&host).await;
            (container, host)
        })
        .await;
    host.clone()
}

/// A connection config pointing a given database at the emulator.
pub fn connection(database: &str, host: &str) -> SpannerConnection {
    SpannerConnection {
        project_id: PROJECT.into(),
        instance: INSTANCE.into(),
        database: database.into(),
        auth: Default::default(),
        max_sessions: 20,
        emulator_host: Some(host.into()),
    }
}

/// Create the shared emulator instance, polling until the emulator accepts
/// gRPC (it needs a moment after the container starts).
async fn bootstrap_instance(host: &str) {
    // The database field is irrelevant for instance-admin calls but the
    // connection block requires one.
    let conn = connection("bootstrap", host);
    for attempt in 0..120u32 {
        let admin = match conn.connect_admin().await {
            Ok(a) => a,
            Err(_) if attempt < 119 => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }
            Err(e) => panic!("spanner emulator admin connect never succeeded: {e}"),
        };
        let req = CreateInstanceRequest {
            parent: format!("projects/{PROJECT}"),
            instance_id: INSTANCE.into(),
            instance: Some(Instance {
                name: format!("projects/{PROJECT}/instances/{INSTANCE}"),
                config: format!("projects/{PROJECT}/instanceConfigs/emulator-config"),
                display_name: "conformance".into(),
                node_count: 1,
                ..Default::default()
            }),
        };
        match admin.instance().create_instance(req, None).await {
            Ok(mut op) => {
                op.wait(None).await.expect("instance create LRO");
                return;
            }
            Err(status) if status.code() == gcloud_gax::grpc::Code::AlreadyExists => return,
            Err(_) if attempt < 119 => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            Err(e) => panic!("spanner emulator create_instance failed: {e}"),
        }
    }
    panic!("spanner emulator never became ready");
}

/// Create a fresh database on the shared emulator with the given DDL and
/// return a connection config for it. Database ids: `[a-z][a-z0-9_-]{0,28}[a-z0-9]`.
pub async fn create_database(database: &str, ddl: Vec<String>) -> SpannerConnection {
    let host = emulator_host().await;
    let conn = connection(database, &host);
    let admin = conn.connect_admin().await.expect("admin connect");
    let mut op = admin
        .database()
        .create_database(
            CreateDatabaseRequest {
                parent: format!("projects/{PROJECT}/instances/{INSTANCE}"),
                create_statement: format!("CREATE DATABASE `{database}`"),
                extra_statements: ddl,
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create database");
    op.wait(None).await.expect("database create LRO");
    conn
}

/// Count rows in `table` via a raw client.
pub async fn count_rows(conn: &SpannerConnection, table: &str) -> usize {
    let client = conn.connect().await.expect("count client");
    let mut tx = client.single().await.expect("single txn");
    let mut iter = tx
        .query(Statement::new(format!(
            "SELECT COUNT(*) AS c FROM `{table}`"
        )))
        .await
        .expect("count query");
    let row = iter
        .next()
        .await
        .expect("count row")
        .expect("count row present");
    let c = row.column_by_name::<i64>("c").expect("count decode");
    c as usize
}

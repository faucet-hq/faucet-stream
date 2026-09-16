//! Integration test: `IcebergSink` with a SQLite SQL catalog writing to an
//! `s3://` warehouse, exercised against a real S3-compatible endpoint (MinIO)
//! via testcontainers.
//!
//! Requires Docker and the `catalog-sql` feature:
//!   cargo test -p faucet-sink-iceberg --test integration_s3 --features catalog-sql
//!
//! This proves the OpenDAL-backed StorageFactory selection (#181): the sink's
//! `catalog.properties` (endpoint/creds/region/path-style) reach the storage
//! layer even though `iceberg-catalog-sql` builds its FileIO with empty props.

#![cfg(feature = "catalog-sql")]

use std::collections::HashMap;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use iceberg::io::{Storage, StorageConfig, StorageFactory};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_storage_opendal::OpenDalStorageFactory;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tempfile::TempDir;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

/// MinIO's Docker Hub repository was withdrawn (September 2026): pulling
/// `minio/minio` fails with "repository does not exist / access denied". The
/// identical pinned release remains published on Quay, so only the module's
/// default image *name* is overridden — tag, cmd, and wait behavior stay
/// those of `testcontainers_modules::minio`.
const MINIO_IMAGE_NAME: &str = "quay.io/minio/minio";

const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-iceberg-tests";

/// Start a MinIO container; return the handle + `http://127.0.0.1:port` endpoint.
async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container = MinIO::default()
        .with_name(MINIO_IMAGE_NAME)
        .start()
        .await
        .expect("minio start");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");
    (container, format!("http://127.0.0.1:{port}"))
}

/// Create the test bucket via a path-style aws-sdk-s3 admin client.
async fn create_bucket(endpoint: &str) {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let s3_config = S3Config::from(&sdk_config)
        .to_builder()
        .force_path_style(true)
        .build();
    Client::from_conf(s3_config)
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");
}

/// S3 `catalog.properties` pointing OpenDAL at the MinIO endpoint, with config
/// loading disabled so the test never touches real AWS.
fn s3_props(endpoint: &str) -> serde_json::Value {
    json!({
        "s3.endpoint": endpoint,
        "s3.access-key-id": ACCESS_KEY,
        "s3.secret-access-key": SECRET_KEY,
        "s3.region": REGION,
        "s3.path-style-access": "true",
        "s3.disable-config-load": "true",
        "s3.disable-ec2-metadata": "true"
    })
}

/// Test-local prop-injecting S3 factory for the *reader* catalog. The SQL
/// catalog drops FileIO props, so the reader needs them re-supplied the same
/// way the sink does internally.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct S3PropFactory {
    props: HashMap<String, String>,
}

#[typetag::serde(name = "faucet-test-s3-prop-factory")]
impl StorageFactory for S3PropFactory {
    fn build(&self, config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        let mut merged = self.props.clone();
        merged.extend(config.props().iter().map(|(k, v)| (k.clone(), v.clone())));
        OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }
        .build(&StorageConfig::from_props(merged))
    }
}

/// Open a reader `SqlCatalog` against the same SQLite DB + s3 warehouse.
async fn open_reader_catalog(
    db_uri: &str,
    warehouse: &str,
    props: HashMap<String, String>,
) -> impl Catalog {
    let cat_props = HashMap::from([
        (SQL_CATALOG_PROP_URI.to_string(), db_uri.to_string()),
        (
            SQL_CATALOG_PROP_WAREHOUSE.to_string(),
            warehouse.to_string(),
        ),
        (
            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
            SqlBindStyle::QMark.to_string(),
        ),
    ]);
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(S3PropFactory { props }))
        .load("faucet-iceberg", cat_props)
        .await
        .expect("reader catalog open")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_catalog_s3_warehouse_commits_snapshot() {
    let (_container, endpoint) = start_minio().await;
    create_bucket(&endpoint).await;

    let dir = TempDir::new().expect("tempdir");
    let db_file = dir.path().join("catalog.db");
    let sqlite_uri = format!("sqlite:{}?mode=rwc", db_file.display());
    let warehouse = format!("s3://{BUCKET}/warehouse");

    let mut catalog = json!({
        "type": "sql",
        "uri": sqlite_uri,
        "warehouse": warehouse,
    });
    catalog["properties"] = s3_props(&endpoint);

    let cfg: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": catalog,
        "namespace": ["db"],
        "table": "events",
        "create_if_missing": true,
        "batch_size": 0
    }))
    .expect("sink config parse");

    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    let records: Vec<serde_json::Value> = (0u64..50)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    let written = sink.write_batch(&records).await.expect("write_batch");
    assert_eq!(written, 50);
    sink.flush().await.expect("flush");

    // Reopen via a reader catalog and assert exactly one snapshot landed in S3.
    let props: HashMap<String, String> =
        serde_json::from_value(s3_props(&endpoint)).expect("props map");
    let reader = open_reader_catalog(&sqlite_uri, &warehouse, props).await;
    let tid = TableIdent::new(
        NamespaceIdent::from_strs(["db"]).unwrap(),
        "events".to_string(),
    );
    let table = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush");
    let meta = table.metadata();

    assert_eq!(meta.snapshots().count(), 1, "expected exactly one snapshot");
    assert!(
        meta.current_snapshot_id().is_some(),
        "current snapshot must be set"
    );
    assert!(
        meta.location().starts_with(&warehouse),
        "table location {} must be under {warehouse}",
        meta.location()
    );
}

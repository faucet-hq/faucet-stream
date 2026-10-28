//! The Iceberg source reading tables whose files live in MinIO, through the
//! REST catalog (`apache/iceberg-rest-fixture`) and the SQL catalog (#967).
//! Each table gets two snapshots written with iceberg-rust; the source reads
//! the latest and an earlier `snapshot_id`.
//!
//! Requires Docker:
//!   cargo test -p faucet-source-iceberg --all-features --test s3_warehouse

#![cfg(all(feature = "catalog-rest", feature = "catalog-sql"))]

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use faucet_common_iceberg::{CatalogConfig, build_catalog};
use faucet_conformance::stores;
use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

const REST_IMAGE: (&str, &str) = ("apache/iceberg-rest-fixture", "1.10.0");
const BUCKET: &str = "faucet-iceberg-source";

async fn create_bucket(endpoint: &str) {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(stores::MINIO_REGION))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(
            stores::MINIO_ACCESS_KEY,
            stores::MINIO_SECRET_KEY,
            None,
            None,
            "test",
        ))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");
}

fn s3_props(endpoint: &str) -> Value {
    json!({
        "s3.endpoint": endpoint,
        "s3.access-key-id": stores::MINIO_ACCESS_KEY,
        "s3.secret-access-key": stores::MINIO_SECRET_KEY,
        "s3.region": stores::MINIO_REGION,
        "s3.path-style-access": "true",
        "s3.disable-config-load": "true",
        "s3.disable-ec2-metadata": "true"
    })
}

async fn append(catalog: &dyn Catalog, table: &Table, ids: std::ops::Range<i64>) -> Table {
    let schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(schema_to_arrow_schema(&schema).expect("arrow schema"));
    let names: Vec<String> = ids.clone().map(|i| format!("n{i}")).collect();
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(Int64Array::from(ids.collect::<Vec<_>>())),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("batch");
    let rolling = RollingFileWriterBuilder::new(
        ParquetWriterBuilder::new(WriterProperties::builder().build(), schema),
        64 * 1024 * 1024,
        table.file_io().clone(),
        DefaultLocationGenerator::new(table.metadata()).expect("location generator"),
        DefaultFileNameGenerator::new(
            "part".to_string(),
            Some(uuid::Uuid::new_v4().to_string()),
            DataFileFormat::Parquet,
        ),
    );
    let mut writer = DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .expect("writer");
    writer.write(batch).await.expect("write");
    let files = writer.close().await.expect("close");
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(files)
        .apply(tx)
        .expect("fast_append");
    tx.commit(catalog).await.expect("commit")
}

async fn read_ids(catalog: &Value, snapshot_id: Option<i64>) -> Vec<i64> {
    let cfg: IcebergSourceConfig = serde_json::from_value(json!({
        "catalog": catalog,
        "table": "db.events",
        "snapshot_id": snapshot_id,
    }))
    .expect("source config");
    let rows = IcebergSource::new(cfg)
        .await
        .expect("source")
        .fetch_all()
        .await
        .expect("read");
    let mut ids: Vec<i64> = rows
        .iter()
        .map(|r| {
            assert_eq!(r["name"], json!(format!("n{}", r["id"])), "{r}");
            r["id"].as_i64().expect("id")
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn assert_reads(catalog: Value, location_prefix: &str) {
    let cfg: CatalogConfig = serde_json::from_value(catalog.clone()).expect("catalog config");
    let writer = build_catalog(&cfg).await.expect("catalog");
    let ns = NamespaceIdent::from_strs(["db"]).unwrap();
    if !writer
        .namespace_exists(&ns)
        .await
        .expect("namespace_exists")
    {
        writer
            .create_namespace(&ns, Default::default())
            .await
            .expect("create namespace");
    }
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    let table = writer
        .create_table(
            &ns,
            TableCreation::builder()
                .name("events".to_string())
                .schema(schema)
                .build(),
        )
        .await
        .expect("create table");
    assert!(
        table.metadata().location().starts_with(location_prefix),
        "{}",
        table.metadata().location()
    );
    let table = append(writer.as_ref(), &table, 0..30).await;
    let first = table.metadata().current_snapshot_id().expect("snapshot");
    let table = append(writer.as_ref(), &table, 30..50).await;
    assert_eq!(table.metadata().snapshots().count(), 2);
    let reloaded = writer
        .load_table(&TableIdent::new(ns, "events".to_string()))
        .await
        .expect("load");
    assert_eq!(
        reloaded.metadata().current_snapshot_id(),
        table.metadata().current_snapshot_id()
    );

    assert_eq!(read_ids(&catalog, None).await, (0..50).collect::<Vec<_>>());
    assert_eq!(
        read_ids(&catalog, Some(first)).await,
        (0..30).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_catalog_table_in_s3_reads_back() {
    let suffix = std::process::id();
    let network = format!("faucet-iceberg-src-{suffix}");
    let minio_name = format!("faucet-iceberg-src-minio-{suffix}");
    let minio = stores::minio_with(|r| {
        r.with_network(network.clone())
            .with_container_name(minio_name.clone())
    })
    .await;
    create_bucket(&minio.endpoint).await;
    let rest = faucet_conformance::containers::start(|| {
        GenericImage::new(REST_IMAGE.0, REST_IMAGE.1)
            .with_exposed_port(8181.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Started"))
            .with_network(network.clone())
            .with_env_var("AWS_ACCESS_KEY_ID", stores::MINIO_ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", stores::MINIO_SECRET_KEY)
            .with_env_var("AWS_REGION", stores::MINIO_REGION)
            .with_env_var("CATALOG_WAREHOUSE", format!("s3://{BUCKET}/rest"))
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var("CATALOG_S3_ENDPOINT", format!("http://{minio_name}:9000"))
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    })
    .await;
    let port = rest.get_host_port_ipv4(8181).await.expect("rest port");
    let catalog = json!({
        "type": "rest",
        "uri": format!("http://127.0.0.1:{port}"),
        "properties": s3_props(&minio.endpoint)
    });
    assert_reads(catalog, &format!("s3://{BUCKET}/rest")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sql_catalog_table_in_s3_reads_back() {
    let minio = stores::minio().await;
    create_bucket(&minio.endpoint).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let warehouse = format!("s3://{BUCKET}/sql");
    let catalog = json!({
        "type": "sql",
        "uri": format!("sqlite:{}?mode=rwc", dir.path().join("catalog.db").display()),
        "warehouse": warehouse,
        "properties": s3_props(&minio.endpoint)
    });
    assert_reads(catalog, &warehouse).await;
}

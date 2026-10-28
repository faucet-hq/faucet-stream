//! The Iceberg source on a Hive Metastore (`apache/hive` in standalone
//! metastore mode) whose warehouse lives in MinIO, #967. The table is created
//! through faucet's HMS catalog and holds no snapshot; the sink's HMS tests
//! (`faucet-sink-iceberg/tests/integration_hms.rs`) cover reading committed
//! ones. The metastore is `faucet_conformance::stores::hive_metastore`: a
//! namespace takes its default location (the bucket), and its tables live
//! under it.
//!
//! Requires Docker and the `catalog-hms` feature:
//!   cargo test -p faucet-source-iceberg --all-features --test hms_catalog

#![cfg(feature = "catalog-hms")]

use faucet_common_iceberg::{CatalogConfig, build_catalog};
use faucet_conformance::stores;
use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use serde_json::json;
use testcontainers::ImageExt;

const BUCKET: &str = "faucet-iceberg-hms";

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hms_catalog_table_in_s3_is_discovered_and_read() {
    let suffix = std::process::id();
    let network = format!("faucet-iceberg-hms-{suffix}");
    let minio_name = format!("faucet-hms-minio-{suffix}");
    let minio = stores::minio_with(|r| {
        r.with_network(network.clone())
            .with_container_name(minio_name.clone())
    })
    .await;
    create_bucket(&minio.endpoint).await;

    let hms =
        stores::hive_metastore(&network, &minio_name, &format!("s3a://{BUCKET}/warehouse")).await;

    let catalog = json!({
        "type": "hms",
        "uri": hms.uri,
        "warehouse": format!("s3://{BUCKET}/warehouse"),
        "properties": {
            "s3.endpoint": minio.endpoint,
            "s3.access-key-id": stores::MINIO_ACCESS_KEY,
            "s3.secret-access-key": stores::MINIO_SECRET_KEY,
            "s3.region": stores::MINIO_REGION,
            "s3.path-style-access": "true",
            "s3.disable-config-load": "true",
            "s3.disable-ec2-metadata": "true"
        }
    });
    let cfg: CatalogConfig = serde_json::from_value(catalog.clone()).expect("catalog config");
    let hms_catalog = build_catalog(&cfg).await.expect("hms catalog");
    let ns = NamespaceIdent::from_strs(["db"]).unwrap();
    hms_catalog
        .create_namespace(&ns, Default::default())
        .await
        .expect("create namespace");
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("schema");
    let table = hms_catalog
        .create_table(
            &ns,
            TableCreation::builder()
                .name("events".to_string())
                .schema(schema)
                .build(),
        )
        .await
        .expect("create table through the metastore");
    let location = table.metadata().location().to_string();
    assert!(
        location.starts_with(&format!("s3a://{BUCKET}/warehouse/db.db/events")),
        "{location}"
    );

    let source_cfg: IcebergSourceConfig =
        serde_json::from_value(json!({ "catalog": catalog, "table": "db.events" }))
            .expect("source config");
    let source = IcebergSource::new(source_cfg).await.expect("source");
    let datasets = source.discover().await.expect("discover");
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
    assert_eq!(names, vec!["db.events"]);
    assert_eq!(
        datasets[0].schema.as_ref().expect("schema")["properties"]
            .as_object()
            .map(|p| {
                let mut keys: Vec<String> = p.keys().cloned().collect();
                keys.sort();
                keys
            }),
        Some(vec!["id".to_string(), "name".to_string()])
    );
    assert!(source.fetch_all().await.expect("read").is_empty());
}

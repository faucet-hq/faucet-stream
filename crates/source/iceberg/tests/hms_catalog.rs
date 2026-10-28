//! The Iceberg source on a Hive Metastore (`apache/hive` in standalone
//! metastore mode) whose warehouse lives in MinIO, #967. The table is created
//! through faucet's HMS catalog; iceberg-rust's HMS client cannot commit a
//! snapshot (the sink refuses `hms` for that reason), so it holds none.
//!
//! The metastore itself checks table locations through Hadoop's S3A client,
//! so the container gets the S3A jars on its classpath, a `core-site.xml`
//! pointing at MinIO, and a `hive-site.xml` whose warehouse dir is the bucket:
//! a namespace takes the metastore's default location, and its tables live
//! under it.
//!
//! Requires Docker and the `catalog-hms` feature:
//!   cargo test -p faucet-source-iceberg --all-features --test hms_catalog

#![cfg(feature = "catalog-hms")]

use std::time::Duration;

use faucet_common_iceberg::{CatalogConfig, build_catalog};
use faucet_conformance::containers::{ReadyProbe, StartOptions};
use faucet_conformance::stores;
use faucet_core::Source;
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};
use serde_json::json;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

// Hive 3: iceberg-catalog-hms 0.10 calls `get_table`, which Hive 4 removed.
const HIVE_IMAGE: (&str, &str) = ("apache/hive", "3.1.3");
const BUCKET: &str = "faucet-iceberg-hms";
const TOOLS: &str = "/opt/hadoop/share/hadoop/tools/lib";

fn core_site(minio_host: &str) -> String {
    let props = [
        ("fs.s3a.endpoint", format!("http://{minio_host}:9000")),
        ("fs.s3a.access.key", stores::MINIO_ACCESS_KEY.to_string()),
        ("fs.s3a.secret.key", stores::MINIO_SECRET_KEY.to_string()),
        ("fs.s3a.path.style.access", "true".to_string()),
        ("fs.s3a.connection.ssl.enabled", "false".to_string()),
        (
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider".to_string(),
        ),
        (
            "fs.s3.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem".to_string(),
        ),
    ];
    configuration(&props)
}

/// One buffered binary-protocol `get_all_databases` call: Docker's port proxy
/// accepts connections before the metastore listens, so a TCP probe passes early.
async fn thrift_answers(addr: String) -> Result<(), String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let name = b"get_all_databases";
    let mut call = 0x8001_0001u32.to_be_bytes().to_vec();
    call.extend((name.len() as u32).to_be_bytes());
    call.extend(name);
    call.extend(1i32.to_be_bytes());
    call.push(0);
    let mut stream = tokio::net::TcpStream::connect(&addr)
        .await
        .map_err(|e| e.to_string())?;
    stream.write_all(&call).await.map_err(|e| e.to_string())?;
    let mut reply = [0u8; 4];
    stream
        .read_exact(&mut reply)
        .await
        .map_err(|e| format!("no thrift reply from {addr}: {e}"))?;
    Ok(())
}

/// The metastore's warehouse dir is the bucket; no connection pool, since
/// HikariCP over embedded Derby can hang the metastore at start-up.
fn hive_site() -> String {
    let props = [
        (
            "hive.metastore.warehouse.dir",
            format!("s3a://{BUCKET}/warehouse"),
        ),
        ("datanucleus.connectionPoolingType", "None".to_string()),
    ];
    configuration(&props)
}

fn configuration(props: &[(&str, String)]) -> String {
    let body: String = props
        .iter()
        .map(|(k, v)| format!("<property><name>{k}</name><value>{v}</value></property>"))
        .collect();
    format!("<configuration>{body}</configuration>")
}

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

    let hms = faucet_conformance::containers::start_with(
        || {
            GenericImage::new(HIVE_IMAGE.0, HIVE_IMAGE.1)
                .with_exposed_port(9083.tcp())
                .with_wait_for(WaitFor::message_on_stdout("Starting Hive Metastore Server"))
                .with_network(network.clone())
                .with_env_var("SERVICE_NAME", "metastore")
                .with_env_var("HIVE_CUSTOM_CONF_DIR", "/hive_custom_conf")
                .with_env_var(
                    "HADOOP_CLASSPATH",
                    format!(
                        "{TOOLS}/hadoop-aws-3.1.0.jar:{TOOLS}/aws-java-sdk-bundle-1.11.271.jar"
                    ),
                )
                .with_copy_to(
                    "/hive_custom_conf/core-site.xml",
                    core_site(&minio_name).into_bytes(),
                )
                .with_copy_to("/hive_custom_conf/hive-site.xml", hive_site().into_bytes())
        },
        &StartOptions::default()
            .ready(ReadyProbe::custom(9083, |ep| thrift_answers(ep.addr())))
            .startup_timeout(Duration::from_secs(480))
            .ready_timeout(Duration::from_secs(480)),
    )
    .await;
    let port = hms.get_host_port_ipv4(9083).await.expect("hms port");

    let catalog = json!({
        "type": "hms",
        "uri": format!("thrift://127.0.0.1:{port}"),
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

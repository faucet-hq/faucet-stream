//! Integration test: `IcebergSink` against a real REST catalog
//! (`apache/iceberg-rest-fixture`) whose warehouse lives in MinIO (#962).
//!
//! Requires Docker; `catalog-rest` is a default feature:
//!   cargo test -p faucet-sink-iceberg --test integration_rest
//!
//! The REST catalog needs a `StorageFactory` to open table files in-process;
//! without one every `create_table` / `load_table` failed before any S3
//! property was read.

#![cfg(feature = "catalog-rest")]

use std::collections::HashMap;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_common_iceberg::{CatalogConfig, build_catalog};
use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use iceberg::{NamespaceIdent, TableIdent};
use serde_json::json;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const MINIO_IMAGE: (&str, &str) = ("cgr.dev/chainguard/minio", "latest");
const REST_IMAGE: (&str, &str) = ("apache/iceberg-rest-fixture", "1.10.0");
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-iceberg-rest";

struct Stack {
    _minio: ContainerAsync<GenericImage>,
    _rest: ContainerAsync<GenericImage>,
    s3_endpoint: String,
    rest_uri: String,
}

async fn start_stack() -> Stack {
    let suffix = std::process::id();
    let network = format!("faucet-iceberg-rest-{suffix}");
    let minio_name = format!("faucet-minio-{suffix}");

    let minio = faucet_conformance::containers::start(|| {
        GenericImage::new(MINIO_IMAGE.0, MINIO_IMAGE.1)
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_stderr("API:"))
            .with_network(network.clone())
            .with_container_name(minio_name.clone())
            .with_mount(Mount::tmpfs_mount("/data"))
            .with_env_var("MINIO_ROOT_USER", ACCESS_KEY)
            .with_env_var("MINIO_ROOT_PASSWORD", SECRET_KEY)
            .with_cmd(["server", "/data"])
    })
    .await;
    let s3_port = minio.get_host_port_ipv4(9000).await.expect("minio port");
    let s3_endpoint = format!("http://127.0.0.1:{s3_port}");
    create_bucket(&s3_endpoint).await;

    let rest = faucet_conformance::containers::start(|| {
        GenericImage::new(REST_IMAGE.0, REST_IMAGE.1)
            .with_exposed_port(8181.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Started"))
            .with_network(network.clone())
            .with_env_var("AWS_ACCESS_KEY_ID", ACCESS_KEY)
            .with_env_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY)
            .with_env_var("AWS_REGION", REGION)
            .with_env_var("CATALOG_WAREHOUSE", format!("s3://{BUCKET}/warehouse"))
            .with_env_var("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
            .with_env_var("CATALOG_S3_ENDPOINT", format!("http://{minio_name}:9000"))
            .with_env_var("CATALOG_S3_PATH__STYLE__ACCESS", "true")
    })
    .await;
    let rest_port = rest.get_host_port_ipv4(8181).await.expect("rest port");

    Stack {
        _minio: minio,
        _rest: rest,
        s3_endpoint,
        rest_uri: format!("http://127.0.0.1:{rest_port}"),
    }
}

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

fn catalog(stack: &Stack) -> serde_json::Value {
    json!({
        "type": "rest",
        "uri": stack.rest_uri,
        "properties": {
            "s3.endpoint": stack.s3_endpoint,
            "s3.access-key-id": ACCESS_KEY,
            "s3.secret-access-key": SECRET_KEY,
            "s3.region": REGION,
            "s3.path-style-access": "true",
            "s3.disable-config-load": "true",
            "s3.disable-ec2-metadata": "true"
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_catalog_creates_and_writes_a_table_in_s3() {
    let stack = start_stack().await;
    let catalog_cfg = catalog(&stack);

    let cfg: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": catalog_cfg,
        "namespace": ["db"],
        "table": "events",
        "create_if_missing": true,
        "batch_size": 0
    }))
    .expect("sink config parse");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    let records: Vec<serde_json::Value> = (0u64..25)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    assert_eq!(sink.write_batch(&records).await.expect("write_batch"), 25);
    sink.flush().await.expect("flush");

    let reader_cfg: CatalogConfig = serde_json::from_value(catalog_cfg).expect("catalog parse");
    let reader = build_catalog(&reader_cfg).await.expect("reader catalog");
    let table = reader
        .load_table(&TableIdent::new(
            NamespaceIdent::from_strs(["db"]).unwrap(),
            "events".to_string(),
        ))
        .await
        .expect("load_table after flush");
    let meta = table.metadata();
    assert!(
        meta.location()
            .starts_with(&format!("s3://{BUCKET}/warehouse")),
        "{}",
        meta.location()
    );
    let snapshot = meta.current_snapshot().expect("a committed snapshot");
    let summary: HashMap<String, String> = snapshot.summary().additional_properties.clone();
    assert_eq!(
        summary.get("added-records").map(String::as_str),
        Some("25"),
        "{summary:?}"
    );

    let manifest_list = snapshot.manifest_list();
    assert!(
        table
            .file_io()
            .exists(manifest_list)
            .await
            .expect("manifest list readable through the REST catalog's FileIO"),
        "{manifest_list}"
    );
}

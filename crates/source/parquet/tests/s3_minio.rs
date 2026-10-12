#![allow(deprecated)]
//! Reading Parquet objects from S3 (MinIO): every ranged read is pinned to the
//! object's ETag, and a prefix whose own segment starts with `_` still lists
//! the objects under it while hidden segments below it stay excluded.
//!
//! Requires Docker.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Source;
use faucet_source_parquet::{ParquetS3Config, ParquetSource, ParquetSourceConfig};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::minio::MinIO;

const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-parquet-source";

async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container: ContainerAsync<MinIO> = faucet_conformance::containers::start(|| {
        MinIO::default()
            .with_mount(testcontainers_modules::testcontainers::core::Mount::tmpfs_mount("/data"))
            .with_name(MINIO_IMAGE_NAME)
            .with_tag(MINIO_IMAGE_TAG)
            .with_mapped_port(0, testcontainers::core::IntoContainerPort::tcp(9000))
    })
    .await;
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");
    (container, format!("http://127.0.0.1:{port}"))
}

async fn admin_client(endpoint: &str) -> Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    Client::from_conf(
        S3Config::from(&sdk)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

fn parquet_bytes(first_id: i64, rows: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let ids: Vec<i64> = (first_id..first_id + rows).collect();
    let names: Vec<String> = ids.iter().map(|i| format!("row-{i}")).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut out = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    out
}

fn ids(records: &[Value]) -> Vec<i64> {
    let mut ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn s3_objects_are_read_in_ranges_under_an_underscore_prefix() {
    let (_minio, endpoint) = start_minio().await;
    let client = admin_client(&endpoint).await;
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    for (key, body) in [
        ("exports/_daily/q.parquet", parquet_bytes(1, 3)),
        ("exports/_daily/p.parquet", parquet_bytes(10, 3)),
        ("exports/_daily/_tmp/p.parquet", parquet_bytes(100, 3)),
        ("exports/_daily/.p.parquet.crc", b"crc".to_vec()),
    ] {
        client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();
    }
    // SAFETY: the only test in this binary; nothing else reads the environment.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
    }
    let s3 = ParquetS3Config::prefix(BUCKET, "exports/_daily")
        .region(REGION)
        .endpoint_url(&endpoint);
    let source = ParquetSource::new(ParquetSourceConfig::s3(s3).batch_size(2))
        .await
        .unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();

    let eager = source.fetch_with_context(&ctx).await.unwrap();
    assert_eq!(ids(&eager), vec![1, 2, 3, 10, 11, 12], "{eager:?}");
    assert!(eager.contains(&json!({"id": 10, "name": "row-10"})));

    let mut pages = source.stream_pages(&ctx, 2);
    let mut streamed = Vec::new();
    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        page_count += 1;
        streamed.extend(page.records);
    }
    assert_eq!(ids(&streamed), ids(&eager));
    assert!(page_count >= 4, "row groups stream in pages: {page_count}");
}

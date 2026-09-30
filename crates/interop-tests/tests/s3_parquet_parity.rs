//! `format: parquet` parity with the Parquet sink's S3 destination (#777),
//! against MinIO: the same records written by both read back to identical
//! records and an identical Arrow schema. Requires Docker.
#![allow(deprecated)]
#![cfg(feature = "arrow")]

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::file_format::parquet_io::read_bytes;
use faucet_core::{ParquetReadOptions, Sink};
use faucet_sink_parquet::{
    ParquetDestination, ParquetS3Destination, ParquetSink, ParquetSinkConfig,
};
use faucet_sink_s3::{S3Sink, S3SinkConfig, S3SinkFormat};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const BUCKET: &str = "faucet-parquet-parity";

async fn start() -> (ContainerAsync<MinIO>, String, Client) {
    let container = MinIO::default()
        .with_name("cgr.dev/chainguard/minio")
        .with_tag("latest")
        .with_mapped_port(0, testcontainers::core::IntoContainerPort::tcp(9000))
        .start()
        .await
        .expect("minio container start");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");
    let endpoint = format!("http://127.0.0.1:{port}");
    // SAFETY: identical constants across every test in this binary.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(&endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    let client = Client::from_conf(
        S3Config::from(&sdk)
            .to_builder()
            .force_path_style(true)
            .build(),
    );
    client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("create bucket");
    (container, endpoint, client)
}

async fn objects(client: &Client, prefix: &str) -> Vec<Vec<u8>> {
    let list = client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix(prefix)
        .send()
        .await
        .expect("list");
    let mut out = Vec::new();
    for obj in list.contents() {
        let got = client
            .get_object()
            .bucket(BUCKET)
            .key(obj.key().unwrap())
            .send()
            .await
            .expect("get");
        out.push(
            got.body
                .collect()
                .await
                .expect("body")
                .into_bytes()
                .to_vec(),
        );
    }
    out
}

fn decode(bytes: &[u8]) -> (arrow::datatypes::SchemaRef, Vec<Value>) {
    let (schema, batches) = read_bytes(
        bytes::Bytes::copy_from_slice(bytes),
        &ParquetReadOptions::default(),
        0,
        "o",
    )
    .unwrap();
    let rows = batches
        .iter()
        .flat_map(|b| faucet_core::columnar::record_batch_to_values(b).unwrap())
        .collect();
    (schema, rows)
}

fn records() -> Vec<Value> {
    (0..50)
        .map(|i| {
            json!({"id": i, "name": format!("n{i}"), "score": i as f64 / 2.0, "ok": i % 2 == 0,
                        "tags": ["a", "b"], "meta": {"k": i}})
        })
        .collect()
}

fn s3_sink(endpoint: &str, prefix: &str) -> S3SinkConfig {
    S3SinkConfig::new(BUCKET)
        .prefix(prefix)
        .file_extension(".parquet")
        .format(S3SinkFormat::Parquet)
        .endpoint_url(endpoint)
        .region(REGION)
        .with_batch_size(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn golden_parquet_sink_and_s3_sink_write_the_same_records_and_schema() {
    let (_c, endpoint, client) = start().await;
    let rows = records();

    let pq = ParquetSink::new(ParquetSinkConfig::new(ParquetDestination::S3(
        ParquetS3Destination {
            bucket: BUCKET.into(),
            prefix: "golden/pq/".into(),
            region: Some(REGION.into()),
            endpoint_url: Some(endpoint.clone()),
            allow_http: true,
        },
    )))
    .await
    .expect("parquet sink");
    pq.write_batch(&rows).await.unwrap();
    pq.flush().await.unwrap();
    drop(pq);

    let s3 = S3Sink::new(s3_sink(&endpoint, "golden/s3/"))
        .await
        .expect("s3 sink");
    s3.write_batch(&rows).await.unwrap();
    s3.flush().await.unwrap();

    let a = objects(&client, "golden/pq/").await;
    let b = objects(&client, "golden/s3/").await;
    assert_eq!((a.len(), b.len()), (1, 1), "one object each");
    let (schema_a, rows_a) = decode(&a[0]);
    let (schema_b, rows_b) = decode(&b[0]);
    assert_eq!(schema_a.fields(), schema_b.fields());
    assert_eq!(rows_a, rows_b);
    assert_eq!(rows_b.len(), 50);
}

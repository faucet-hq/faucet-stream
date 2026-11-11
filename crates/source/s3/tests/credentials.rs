//! The credentials a config names reach the S3 client, with the host's own
//! identity refused: static keys, and unsigned requests to a public bucket.

use std::collections::HashMap;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Source;
use faucet_core::resolve::with_host_credentials;
use faucet_source_s3::{S3Credentials, S3Source, S3SourceConfig};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::minio::MinIO;

const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";

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

async fn admin(endpoint: &str) -> Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test"))
        .load()
        .await;
    Client::from_conf(
        S3Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

async fn seed(client: &Client, bucket: &str) {
    client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create bucket");
    client
        .put_object()
        .bucket(bucket)
        .key("data/a.jsonl")
        .body(ByteStream::from(b"{\"id\":1}\n{\"id\":2}\n".to_vec()))
        .send()
        .await
        .expect("put object");
}

async fn read(endpoint: &str, bucket: &str, credentials: S3Credentials) -> Vec<Value> {
    let config = S3SourceConfig::new(bucket)
        .prefix("data/")
        .region(REGION)
        .endpoint_url(endpoint)
        .credentials(credentials);
    let source = S3Source::new(config).await.expect("source builds");
    source
        .fetch_with_context(&HashMap::new())
        .await
        .expect("reads")
}

#[tokio::test]
async fn static_keys_reach_the_s3_client() {
    let (_c, endpoint) = start_minio().await;
    seed(&admin(&endpoint).await, "keyed").await;
    let keys = S3Credentials::AccessKey {
        access_key_id: ACCESS_KEY.into(),
        secret_access_key: SECRET_KEY.into(),
        session_token: None,
    };
    let rows = with_host_credentials(false, read(&endpoint, "keyed", keys)).await;
    assert_eq!(rows, vec![json!({"id": 1}), json!({"id": 2})]);

    let refused = with_host_credentials(
        false,
        S3Source::new(
            S3SourceConfig::new("keyed")
                .region(REGION)
                .endpoint_url(endpoint.clone()),
        ),
    )
    .await
    .err()
    .expect("the default chain is refused");
    assert!(refused.to_string().contains("`credentials`"), "{refused}");
}

#[tokio::test]
async fn anonymous_reads_need_no_keys() {
    let (_c, endpoint) = start_minio().await;
    let client = admin(&endpoint).await;
    seed(&client, "public").await;
    let policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:GetObject", "s3:ListBucket"],
            "Resource": ["arn:aws:s3:::public", "arn:aws:s3:::public/*"]
        }]
    });
    client
        .put_bucket_policy()
        .bucket("public")
        .policy(policy.to_string())
        .send()
        .await
        .expect("public policy");
    let rows =
        with_host_credentials(false, read(&endpoint, "public", S3Credentials::Anonymous)).await;
    assert_eq!(rows, vec![json!({"id": 1}), json!({"id": 2})]);
}

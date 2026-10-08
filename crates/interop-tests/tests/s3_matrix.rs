//! The shared format × option matrix (#777) against MinIO: every writable
//! format × codec × encryption × layout × run mode, through the S3 sink.
//! Requires Docker.
#![cfg(all(
    feature = "file-formats",
    feature = "arrow",
    feature = "compression",
    feature = "encryption"
))]

mod remote_matrix;

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::{FaucetError, Sink};
use remote_matrix::{BoxFut, Remote, run_matrix};
use serde_json::Value;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

const BUCKET: &str = "faucet-matrix";

struct S3Remote {
    endpoint: String,
    client: Client,
    _c: ContainerAsync<MinIO>,
}

impl Remote for S3Remote {
    fn config(&self, prefix: &str, mut fields: Value) -> Value {
        fields["bucket"] = BUCKET.into();
        fields["prefix"] = prefix.into();
        fields["region"] = "us-east-1".into();
        fields["endpoint_url"] = self.endpoint.clone().into();
        fields
    }

    fn sink(&self, cfg: Value) -> BoxFut<'_, Result<Box<dyn Sink>, FaucetError>> {
        Box::pin(async move {
            let cfg = serde_json::from_value(cfg).map_err(FaucetError::Json)?;
            Ok(Box::new(faucet_sink_s3::S3Sink::new(cfg).await?) as Box<dyn Sink>)
        })
    }

    fn objects(&self, prefix: &str) -> BoxFut<'_, Vec<(String, Vec<u8>)>> {
        let prefix = prefix.to_string();
        Box::pin(async move {
            let list = self
                .client
                .list_objects_v2()
                .bucket(BUCKET)
                .prefix(&prefix)
                .send()
                .await
                .expect("list");
            let mut out = Vec::new();
            for o in list.contents() {
                let key = o.key().unwrap().to_string();
                let got = self
                    .client
                    .get_object()
                    .bucket(BUCKET)
                    .key(&key)
                    .send()
                    .await;
                let body = got.expect("get").body.collect().await.expect("body");
                out.push((key[prefix.len()..].to_string(), body.into_bytes().to_vec()));
            }
            out
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_writable_format_takes_every_option_on_s3() {
    let container = MinIO::default()
        // tmpfs: MinIO refuses writes when the runner disk is nearly full.
        .with_mount(testcontainers_modules::testcontainers::core::Mount::tmpfs_mount("/data"))
        .with_name("cgr.dev/chainguard/minio")
        .with_tag("latest")
        .with_mapped_port(0, testcontainers::core::IntoContainerPort::tcp(9000))
        .start()
        .await
        .expect("minio container start");
    let port = container.get_host_port_ipv4(9000).await.expect("port");
    let endpoint = format!("http://127.0.0.1:{port}");
    // SAFETY: the only test in this binary; set before any client exists.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        std::env::set_var("AWS_DEFAULT_REGION", "us-east-1");
    }
    let sdk = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .credentials_provider(Credentials::new(
            "minioadmin",
            "minioadmin",
            None,
            None,
            "t",
        ))
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
        .expect("bucket");
    run_matrix(
        S3Remote {
            endpoint,
            client,
            _c: container,
        },
        true,
    )
    .await;
}

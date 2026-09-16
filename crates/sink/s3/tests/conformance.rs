//! `faucet-conformance` battery against the real S3 sink (via MinIO).
//!
//! The S3 sink writes JSONL objects and is append-only — it advertises no
//! idempotency mechanism, so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
//!
//! Check 5 requires Docker (MinIO via testcontainers), mirroring `batching.rs`.
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_s3::{S3Sink, S3SinkConfig};
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
const TEST_BUCKET: &str = "faucet-sink-s3-conformance";
const PREFIX: &str = "conformance/";

async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container: ContainerAsync<MinIO> = MinIO::default()
        .with_name(MINIO_IMAGE_NAME)
        .start()
        .await
        .expect("minio container start");
    let port = container
        .get_host_port_ipv4(9000)
        .await
        .expect("minio port");
    (container, format!("http://127.0.0.1:{port}"))
}

async fn admin_client(endpoint: &str) -> Client {
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
}

/// Count durable records: sum the JSONL lines across every object under the
/// test prefix. Zero objects (before the first write) is zero records.
async fn count_records(client: &Client) -> usize {
    let resp = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix(PREFIX)
        .send()
        .await
        .expect("list objects");
    let keys: Vec<String> = resp
        .contents()
        .iter()
        .filter_map(|o| o.key().map(|k| k.to_string()))
        .collect();

    let mut total = 0usize;
    for key in keys {
        let obj = client
            .get_object()
            .bucket(TEST_BUCKET)
            .key(&key)
            .send()
            .await
            .expect("get object");
        let bytes = obj.body.collect().await.expect("collect body").into_bytes();
        let body = String::from_utf8(bytes.to_vec()).expect("utf-8");
        total += body.lines().filter(|l| !l.trim().is_empty()).count();
    }
    total
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(faucet_sink_s3::S3SinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "s3");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = S3SinkConfig::new("does-not-exist")
        .endpoint_url("http://127.0.0.1:1".to_string())
        .region(REGION.to_string());
    let sink = S3Sink::new(config).await.expect("sink builds lazily");
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "s3");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let (_container, endpoint) = start_minio().await;
    let admin = admin_client(&endpoint).await;
    admin
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("create bucket");

    // The sink honours the standard AWS credential chain — pass MinIO creds via
    // env vars (same approach as batching.rs).
    // SAFETY: constants; overlapping tests write the same values.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    let config = S3SinkConfig::new(TEST_BUCKET)
        .prefix(PREFIX)
        .endpoint_url(endpoint.clone())
        .region(REGION.to_string());
    let sink = S3Sink::new(config).await.expect("S3Sink::new");

    let admin_ref = &admin;
    faucet_conformance::assert_capabilities_truthful(&sink, || async move {
        // The sink writes each object synchronously within write_batch, so no
        // flush is needed for the object to be listable/readable.
        count_records(admin_ref).await
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}

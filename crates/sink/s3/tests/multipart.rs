//! The multipart upload path (#618), against a real S3-compatible endpoint.
//!
//! This is the memory bound the cross-page accumulator exists for: once the
//! open object passes `MIN_PART_BYTES` (5 MiB) the sink hands each full part
//! to `upload_part` and drops the buffer, so peak stays at O(part size)
//! rather than O(object size). Before #618 the whole object was held and
//! `put_object`'d in one go.
//!
//! Nothing else exercises it — every other test writes objects far below the
//! threshold, so the lazy `create_multipart_upload`, the part numbering, and
//! `complete_multipart_upload` were all unrun. A broken part sequence does
//! not fail loudly; it produces a corrupt or truncated object.
//!
//! Requires Docker; boots its own MinIO container.

use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::{Client, Config as S3Config};
use faucet_core::Sink;
use faucet_sink_s3::{S3Sink, S3SinkConfig};
use serde_json::{Value, json};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::minio::MinIO;

const MINIO_IMAGE_NAME: &str = "quay.io/minio/minio";
const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-sink-s3-multipart";

/// Comfortably past the 5 MiB part floor: each record is ~1 KiB of payload,
/// so 12k records is ~12 MiB and forces at least two parts plus a tail.
const RECORDS: usize = 12_000;

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

async fn admin(endpoint: &str) -> Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(REGION))
        .endpoint_url(endpoint)
        .credentials_provider(creds)
        .load()
        .await;
    Client::from_conf(
        S3Config::from(&sdk_config)
            .to_builder()
            .force_path_style(true)
            .build(),
    )
}

async fn seed(endpoint: &str) -> Client {
    let client = admin(endpoint).await;
    client
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("create bucket");
    client
}

async fn build_sink(endpoint: &str, config: S3SinkConfig) -> S3Sink {
    // SAFETY: the container is per-test and the MinIO credentials are the
    // same constants every time, so an overlapping write sets the same value.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    S3Sink::new(
        config
            .endpoint_url(endpoint.to_string())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Sink::new")
}

/// ~1 KiB per record so the object crosses the part floor without needing a
/// huge record count.
fn wide_record(i: usize) -> Value {
    json!({ "id": i, "payload": "x".repeat(1000) })
}

#[tokio::test(flavor = "multi_thread")]
async fn an_object_past_the_part_floor_uploads_as_multipart_and_reads_back_whole() {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;

    // No record cap and no byte cap: the whole run is one object, which is
    // exactly the shape that must go multipart rather than buffer.
    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix("big/")
            .file_extension(".jsonl")
            .with_batch_size(0),
    )
    .await;

    let page: Vec<Value> = (0..RECORDS).map(wide_record).collect();
    assert_eq!(sink.write_batch(&page).await.expect("write"), RECORDS);
    sink.flush().await.expect("flush completes the upload");

    let listed = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix("big/")
        .send()
        .await
        .expect("list");
    let keys: Vec<String> = listed
        .contents()
        .iter()
        .filter_map(|o| o.key().map(str::to_string))
        .collect();
    assert_eq!(keys.len(), 1, "one object, uploaded in parts: {keys:?}");

    let body = client
        .get_object()
        .bucket(TEST_BUCKET)
        .key(&keys[0])
        .send()
        .await
        .expect("get object")
        .body
        .collect()
        .await
        .expect("read body")
        .into_bytes();

    // Every record present, in order, and nothing torn at a part boundary —
    // the failure a wrong part sequence actually produces.
    let text = String::from_utf8(body.to_vec()).expect("utf-8");
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), RECORDS, "every record survived the part split");
    for (i, line) in lines.iter().enumerate() {
        let v: Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("line {i} is not whole JSON ({e}): {line:.80}"));
        assert_eq!(v["id"], json!(i), "records kept their order across parts");
    }
}

/// A byte cap below the part floor still rolls whole objects rather than
/// leaving a multipart upload dangling.
#[tokio::test(flavor = "multi_thread")]
async fn a_byte_cap_rolls_objects_without_stranding_an_upload() {
    let (_c, endpoint) = start_minio().await;
    let client = seed(&endpoint).await;
    let sink = build_sink(
        &endpoint,
        S3SinkConfig::new(TEST_BUCKET)
            .prefix("rolled/")
            .file_extension(".jsonl")
            .with_batch_size(0)
            .max_bytes_per_file(2 * 1024 * 1024),
    )
    .await;

    let page: Vec<Value> = (0..6_000).map(wide_record).collect();
    sink.write_batch(&page).await.expect("write");
    sink.flush().await.expect("flush");

    let listed = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix("rolled/")
        .send()
        .await
        .expect("list");
    assert!(
        listed.contents().len() >= 2,
        "~6 MiB at a 2 MiB cap is several objects, got {}",
        listed.contents().len()
    );

    // No multipart upload left open — an abandoned one bills storage forever
    // and is invisible in a plain object listing.
    let pending = client
        .list_multipart_uploads()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("list multipart uploads");
    assert!(
        pending.uploads().is_empty(),
        "flush must leave no dangling upload: {:?}",
        pending.uploads()
    );
}

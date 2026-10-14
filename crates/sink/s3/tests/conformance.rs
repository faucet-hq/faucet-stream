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
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::minio::MinIO;

/// MinIO's Docker Hub repository was withdrawn (September 2026): pulling
/// `minio/minio` fails with "repository does not exist / access denied". The
/// identical pinned release remains published on Quay, so only the module's
/// default image *name* is overridden — tag, cmd, and wait behavior stay
/// those of `testcontainers_modules::minio`.
/// Chainguard's maintained MinIO build: the upstream images stopped being
/// pullable (#694). Same server binary and CLI.
const MINIO_IMAGE_NAME: &str = "cgr.dev/chainguard/minio";
const MINIO_IMAGE_TAG: &str = "latest";

const ACCESS_KEY: &str = "minioadmin";
const SECRET_KEY: &str = "minioadmin";
const REGION: &str = "us-east-1";
const TEST_BUCKET: &str = "faucet-sink-s3-conformance";
const PREFIX: &str = "conformance/";

async fn start_minio() -> (ContainerAsync<MinIO>, String) {
    let container: ContainerAsync<MinIO> = faucet_conformance::containers::start(|| {
        MinIO::default()
            // tmpfs: MinIO refuses writes when the runner disk is nearly full.
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
    faucet_conformance::assert_batch_atomicity_declared(&sink);
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
    // RFC 0014: the sink runs on the object-store family driver.
    faucet_conformance::assert_driver_declarations_truthful(&sink).await;
    faucet_conformance::assert_driver_row_paths_agree(&sink).await;
}

// ── Descriptor (RFC 0013): agrees with the built sink ───────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[tokio::test]
async fn descriptor_matches_built_sink() {
    let base = serde_json::json!({"bucket": "b", "region": "us-east-1", "endpoint_url": "http://127.0.0.1:1"});
    let mut cases = vec![
        serde_json::json!({}),
        serde_json::json!({"format": "raw_text"}),
        serde_json::json!({"format": "auto", "path": "x.json"}),
        serde_json::json!({"format": "auto", "file_extension": ".txt"}),
    ];
    #[cfg(feature = "file-format-csv")]
    cases.extend([
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "warn"}}),
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "error"}}),
    ]);
    #[cfg(feature = "arrow")]
    cases.push(serde_json::json!({"format": "auto", "path": "x-{part}.parquet"}));
    for extra in cases {
        let v = descriptor_config(base.clone(), extra);
        let cfg: S3SinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = S3Sink::new(cfg).await.expect("sink builds offline");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_s3::DESCRIPTOR,
            "s3",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let endpoint = hung.http_url();
    // SAFETY: the same constants every other test here sets.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    faucet_conformance::calls::assert_sink_call_deadline(
        || {
            let config = S3SinkConfig::new("hung")
                .endpoint_url(endpoint.clone())
                .region(REGION.to_string());
            block_on(S3Sink::new(config)).expect("sink builds offline")
        },
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

// ── #878: declared capabilities against MinIO ─────────────────────────────────

/// Records in the objects under `prefix` a reader of the destination sees: the
/// overwrite swap area (`.faucet-overwrite-…/`) and its markers are not part of
/// it.
async fn visible_records(client: &Client, prefix: &str) -> usize {
    let resp = client
        .list_objects_v2()
        .bucket(TEST_BUCKET)
        .prefix(prefix)
        .send()
        .await
        .expect("list objects");
    let mut total = 0usize;
    for key in resp.contents().iter().filter_map(|o| o.key()) {
        if key.contains("/.faucet-") {
            continue;
        }
        let obj = client
            .get_object()
            .bucket(TEST_BUCKET)
            .key(key)
            .send()
            .await
            .expect("get object");
        let bytes = obj.body.collect().await.expect("collect body").into_bytes();
        total += String::from_utf8_lossy(&bytes)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count();
    }
    total
}

/// The object is uploaded when the sink flushes, so a cancelled run must
/// still flush; and `write_mode: overwrite` stages in a swap area until
/// commit, leaving the previous objects in place on abort.
///
/// The SDK's upload futures are deep, so the body runs on a large stack.
#[test]
fn conformance_declared_capabilities() {
    faucet_conformance::block_on_large_stack(declared_capabilities());
}

async fn declared_capabilities() {
    let (_container, endpoint) = start_minio().await;
    let admin = admin_client(&endpoint).await;
    admin
        .create_bucket()
        .bucket(TEST_BUCKET)
        .send()
        .await
        .expect("create bucket");
    // SAFETY: constants; overlapping tests write the same values.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", ACCESS_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", SECRET_KEY);
        std::env::set_var("AWS_DEFAULT_REGION", REGION);
    }
    let admin_ref = &admin;

    let sink = S3Sink::new(
        S3SinkConfig::new(TEST_BUCKET)
            .prefix("cancel/")
            .endpoint_url(endpoint.clone())
            .region(REGION.to_string()),
    )
    .await
    .expect("S3Sink::new");
    faucet_conformance::assert_cancellation_flushes(&sink, || async move {
        visible_records(admin_ref, "cancel/").await
    })
    .await;

    admin
        .put_object()
        .bucket(TEST_BUCKET)
        .key("ovw/out-00001.jsonl")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(
            b"{\"id\":-1}\n{\"id\":-2}\n{\"id\":-3}\n",
        ))
        .send()
        .await
        .expect("seed object");
    let mut cfg = S3SinkConfig::new(TEST_BUCKET)
        .endpoint_url(endpoint.clone())
        .region(REGION.to_string());
    cfg.path = Some("ovw/out-{part}.jsonl".into());
    cfg.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
    let overwrite = S3Sink::new(cfg).await.expect("overwrite sink");
    faucet_conformance::assert_overwrite_lifecycle(&overwrite, || async move {
        visible_records(admin_ref, "ovw/").await
    })
    .await;
}

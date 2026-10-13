//! `faucet-conformance` battery — Check 1 (config schema validity) and
//! Check 10 (connector_name non-empty). Passing this battery in CI is the
//! Tier-1 (supported) criterion.
#![allow(clippy::disallowed_methods)] // tests drive raw clients directly
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink as _;
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_gcs::GcsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "gcs");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// Building the GCS storage client with `Anonymous` creds + an endpoint
/// override performs no I/O, so this runs unconditionally (no emulator needed).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = GcsSinkConfig::new("does-not-exist")
        .auth(GcsCredentials::Anonymous)
        .storage_host("http://127.0.0.1:1");
    let sink = GcsSink::new(config).await.expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}

// ── Descriptor (RFC 0013): agrees with the built sink ───────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[tokio::test(flavor = "multi_thread")]
async fn descriptor_matches_built_sink() {
    let base = serde_json::to_value(
        GcsSinkConfig::new("b")
            .auth(GcsCredentials::Anonymous)
            .storage_host("http://127.0.0.1:1"),
    )
    .unwrap();
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
        let cfg: GcsSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = GcsSink::new(cfg).await.expect("sink builds lazily");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_gcs::DESCRIPTOR,
            "gcs",
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
    let host = hung.http_url();
    faucet_conformance::calls::assert_sink_call_deadline(
        || {
            let config = GcsSinkConfig::new("hung")
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host);
            block_on(GcsSink::new(config)).expect("sink builds offline")
        },
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

// ── #878: declared capabilities against fake-gcs-server ───────────────────────

/// Records in the objects under `prefix` a reader of the destination sees: the
/// overwrite swap area (`.faucet-overwrite-…/`) and its markers are not part of
/// it.
async fn visible_records(host: &str, bucket: &str, prefix: &str) -> usize {
    let body: serde_json::Value = reqwest::get(format!(
        "{host}/storage/v1/b/{bucket}/o?prefix={}",
        urlencoding::encode(prefix)
    ))
    .await
    .expect("list")
    .json()
    .await
    .expect("list body");
    let mut total = 0;
    for item in body["items"].as_array().cloned().unwrap_or_default() {
        let name = item["name"].as_str().unwrap_or_default();
        if name.contains("/.faucet-") {
            continue;
        }
        let text = reqwest::get(format!(
            "{host}/storage/v1/b/{bucket}/o/{}?alt=media",
            urlencoding::encode(name)
        ))
        .await
        .expect("download")
        .text()
        .await
        .expect("download body");
        total += text.lines().filter(|l| !l.trim().is_empty()).count();
    }
    total
}

/// The object is uploaded when the sink flushes, so a cancelled run must still
/// flush; and `write_mode: overwrite` stages in a swap area until commit. The
/// storage client's futures are deep, so the body runs on a large stack.
#[test]
fn conformance_declared_capabilities() {
    faucet_conformance::block_on_large_stack(declared_capabilities());
}

async fn declared_capabilities() {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::{GenericImage, ImageExt};

    let Some(container) = faucet_conformance::containers::start_or_skip(
        || {
            GenericImage::new("fsouza/fake-gcs-server", "latest")
                .with_exposed_port(4443.tcp())
                .with_wait_for(WaitFor::message_on_stderr("server started at"))
                .with_cmd(vec![
                    "-scheme=http".to_string(),
                    "-public-host=0.0.0.0:4443".to_string(),
                ])
                .with_mount(testcontainers::core::Mount::tmpfs_mount("/storage"))
        },
        &faucet_conformance::containers::StartOptions::default().ready(
            faucet_conformance::containers::ReadyProbe::http(4443, "/storage/v1/b"),
        ),
    )
    .await
    else {
        return;
    };
    let port = container.get_host_port_ipv4(4443).await.expect("port");
    let host = format!("http://127.0.0.1:{port}");
    let bucket = "conformance";
    reqwest::Client::new()
        .post(format!("{host}/storage/v1/b"))
        .json(&serde_json::json!({"name": bucket}))
        .send()
        .await
        .expect("create bucket");
    let host_ref = host.as_str();

    let sink = GcsSink::new(
        GcsSinkConfig::new(bucket)
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host)
            .prefix("cancel/"),
    )
    .await
    .expect("sink");
    faucet_conformance::assert_cancellation_flushes(&sink, || async move {
        visible_records(host_ref, bucket, "cancel/").await
    })
    .await;

    reqwest::Client::new()
        .post(format!(
            "{host}/upload/storage/v1/b/{bucket}/o?uploadType=media&name=ovw%2Fout-00001.jsonl"
        ))
        .body("{\"id\":-1}\n{\"id\":-2}\n{\"id\":-3}\n")
        .send()
        .await
        .expect("seed object");
    let mut cfg = GcsSinkConfig::new(bucket)
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    cfg.path = Some("ovw/out-{part}.jsonl".into());
    cfg.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
    let overwrite = GcsSink::new(cfg).await.expect("overwrite sink");
    faucet_conformance::assert_overwrite_lifecycle(&overwrite, || async move {
        visible_records(host_ref, bucket, "ovw/").await
    })
    .await;

    let recorded = GcsSink::new(
        GcsSinkConfig::new(bucket)
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host)
            .prefix("rt/"),
    )
    .await
    .expect("sink");
    faucet_conformance::calls::assert_sink_records_roundtrips(
        &recorded,
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

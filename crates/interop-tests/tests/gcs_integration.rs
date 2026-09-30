//! Integration tests for `faucet-sink-gcs` against `fake-gcs-server`.
//!
//! Requires Docker. Skips automatically when Docker is unavailable.

#![cfg(not(target_os = "windows"))]

use faucet_core::{Sink, Source};
use faucet_sink_gcs::{GcsCredentials, GcsSink, GcsSinkConfig};
use faucet_source_gcs::{GcsSource, GcsSourceConfig};
use serde_json::json;
use std::collections::HashMap;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

async fn spawn_fake_gcs() -> Option<(ContainerAsync<GenericImage>, String, String)> {
    let image = GenericImage::new("fsouza/fake-gcs-server", "latest")
        .with_exposed_port(4443.tcp())
        .with_wait_for(WaitFor::message_on_stderr("server started at"))
        .with_cmd(vec![
            "-scheme=http".to_string(),
            "-public-host=0.0.0.0:4443".to_string(),
        ]);
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(4443).await.ok()?;
    let host = format!("http://127.0.0.1:{port}");
    let bucket = "faucet-sink-test".to_string();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{host}/storage/v1/b"))
        .json(&json!({"name": bucket}))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::CONFLICT {
        eprintln!("Skipping: could not create bucket ({})", resp.status());
        return None;
    }
    // The handle is returned so the container lives exactly as long as the
    // test that owns it: dropping it stops and removes the container.
    // testcontainers-rs has no reaper — a forgotten handle is a leaked
    // container, one per test, forever.
    Some((container, host, bucket))
}

#[tokio::test]
async fn sink_writes_and_source_reads_them_back() {
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };

    let sink = GcsSink::new(
        GcsSinkConfig::new(&bucket)
            .prefix("rt/")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();

    let meter = std::sync::Arc::new(faucet_core::UsageMeter::new());
    sink.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Sink,
            "p",
            "r",
            "gcs",
        )
        .with_meter(meter.clone()),
    ));
    let records: Vec<_> = (0..50).map(|i| json!({"i": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 50);
    sink.flush().await.unwrap();
    assert!(
        meter
            .snapshot()
            .sink_roundtrips
            .get("put")
            .is_some_and(|n| *n >= 1)
    );

    let source = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("rt/")
            .auth(faucet_source_gcs::GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    source.set_roundtrip_recorder(std::sync::Arc::new(
        faucet_core::observability::RoundtripRecorder::new(
            faucet_core::observability::RoundtripSide::Source,
            "p",
            "r",
            "gcs",
        )
        .with_meter(meter.clone()),
    ));
    let read = source.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(read.len(), 50);
    let ops = meter.snapshot().source_roundtrips;
    assert!(
        ops.contains_key("list") && ops.contains_key("get"),
        "{ops:?}"
    );
    let mut got: Vec<i64> = read.iter().map(|r| r["i"].as_i64().unwrap()).collect();
    got.sort();
    let want: Vec<i64> = (0..50).collect();
    assert_eq!(got, want);
}

#[tokio::test]
async fn sink_rolls_files_per_max_records_per_file() {
    use futures::StreamExt;
    let Some((_gcs, host, bucket)) = spawn_fake_gcs().await else {
        return;
    };

    let sink = GcsSink::new(
        GcsSinkConfig::new(&bucket)
            .prefix("roll/")
            .auth(GcsCredentials::Anonymous)
            .max_records_per_file(10)
            .with_batch_size(0)
            .storage_host(&host),
    )
    .await
    .unwrap();
    let records: Vec<_> = (0..25).map(|i| json!({"i": i})).collect();
    sink.write_batch(&records).await.unwrap();
    sink.flush().await.unwrap();

    // Listing via the source confirms 3 files were written
    // (ceil(25 / 10) == 3).
    let source = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("roll/")
            .auth(faucet_source_gcs::GcsCredentials::Anonymous)
            .with_batch_size(0)
            .storage_host(&host),
    )
    .await
    .unwrap();
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut pages = Vec::new();
    while let Some(p) = stream.next().await {
        pages.push(p.unwrap());
    }
    assert_eq!(pages.len(), 3, "expected 3 rolled files");
}

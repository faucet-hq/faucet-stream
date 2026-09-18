//! Integration tests for `faucet-source-gcs` against `fake-gcs-server`.
//!
//! Requires Docker. Skips automatically when Docker is unavailable.

#![cfg(not(target_os = "windows"))]

use faucet_core::Source;
use faucet_source_gcs::{GcsCredentials, GcsFileFormat, GcsSource, GcsSourceConfig};
use std::collections::HashMap;
use testcontainers::{
    GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

/// Spawn `fake-gcs-server` and return `(host_url, bucket_name)`.
/// Returns `None` when Docker is unavailable so tests skip cleanly.
async fn spawn_fake_gcs() -> Option<(String, String)> {
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
    let bucket = "faucet-test".to_string();

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{host}/storage/v1/b"))
        .json(&serde_json::json!({"name": bucket}))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() && resp.status() != reqwest::StatusCode::CONFLICT {
        eprintln!("Skipping: could not create bucket ({})", resp.status());
        return None;
    }

    // Container lifetime tied to process exit (drops at the end of the
    // test binary). Cleaner than per-test setup/teardown for a smoke suite.
    std::mem::forget(container);
    Some((host, bucket))
}

/// Upload an object via fake-gcs-server's REST surface.
async fn seed_object(host: &str, bucket: &str, name: &str, body: &str, content_type: &str) {
    let client = reqwest::Client::new();
    let url = format!(
        "{host}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={}",
        urlencoding::encode(name)
    );
    client
        .post(url)
        .header("Content-Type", content_type)
        .body(body.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; fake-gcs-server only speaks REST. Run with `cargo test -- --ignored` against a live backend."]
async fn source_reads_json_lines() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "data/users.jsonl",
        "{\"id\":1,\"name\":\"Alice\"}\n{\"id\":2,\"name\":\"Bob\"}\n",
        "application/x-ndjson",
    )
    .await;

    let config = GcsSourceConfig::new(&bucket)
        .prefix("data/")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let source = GcsSource::new(config).await.unwrap();
    let records = source.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(records.len(), 2);
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(sorted, vec![1, 2]);
}

#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; see source_reads_json_lines."]
async fn source_reads_json_array() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "data/users.json",
        "[{\"id\":1},{\"id\":2},{\"id\":3}]",
        "application/json",
    )
    .await;

    let config = GcsSourceConfig::new(&bucket)
        .prefix("data/")
        .file_format(GcsFileFormat::JsonArray)
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let source = GcsSource::new(config).await.unwrap();
    let records = source.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(records.len(), 3);
}

#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; see source_reads_json_lines."]
async fn source_reads_raw_text() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(&host, &bucket, "raw/a.txt", "hello world", "text/plain").await;

    let config = GcsSourceConfig::new(&bucket)
        .prefix("raw/")
        .file_format(GcsFileFormat::RawText)
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let source = GcsSource::new(config).await.unwrap();
    let records = source.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["key"], "raw/a.txt");
    assert_eq!(records[0]["content"], "hello world");
}

#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; see source_reads_json_lines."]
async fn source_object_keys_skips_listing() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "a.jsonl",
        "{\"v\":1}\n",
        "application/x-ndjson",
    )
    .await;
    seed_object(
        &host,
        &bucket,
        "b.jsonl",
        "{\"v\":2}\n",
        "application/x-ndjson",
    )
    .await;
    seed_object(
        &host,
        &bucket,
        "c.jsonl",
        "{\"v\":3}\n",
        "application/x-ndjson",
    )
    .await;

    let config = GcsSourceConfig::new(&bucket)
        .object_keys(vec!["a.jsonl".into(), "c.jsonl".into()])
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let source = GcsSource::new(config).await.unwrap();
    let records = source.fetch_with_context(&HashMap::new()).await.unwrap();
    let mut vs: Vec<i64> = records.iter().map(|r| r["v"].as_i64().unwrap()).collect();
    vs.sort();
    assert_eq!(vs, vec![1, 3]);
}

#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; see source_reads_json_lines."]
async fn source_stream_pages_batch_size_zero_yields_one_page_per_object() {
    use futures::StreamExt;
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "p/a.jsonl",
        "{\"id\":1}\n{\"id\":2}\n",
        "application/x-ndjson",
    )
    .await;
    seed_object(
        &host,
        &bucket,
        "p/b.jsonl",
        "{\"id\":3}\n",
        "application/x-ndjson",
    )
    .await;

    let config = GcsSourceConfig::new(&bucket)
        .prefix("p/")
        .with_batch_size(0)
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let source = GcsSource::new(config).await.unwrap();
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut pages = Vec::new();
    while let Some(p) = stream.next().await {
        pages.push(p.unwrap());
    }
    assert_eq!(pages.len(), 2);
}

/// #619 — `concurrency` on the streaming path. `stream_pages` used to read
/// objects strictly one at a time, so the field was accepted and ignored. The
/// prefetch that fixed it is *ordered*, so records must still arrive in
/// listing order — an unordered look-ahead would interleave objects by
/// completion time and change the sequence a downstream sink writes.
///
/// Ignored for the same reason as the rest of this file: `fake-gcs-server`
/// speaks REST while the connector uses the gRPC data plane.
#[tokio::test]
#[ignore = "requires a real GCS-compatible gRPC backend; see source_reads_json_lines."]
async fn source_streams_objects_concurrently_in_listing_order() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    for i in 1..=12i64 {
        seed_object(
            &host,
            &bucket,
            &format!("data/part-{i:04}.jsonl"),
            &format!("{{\"id\":{i}}}\n"),
            "application/x-ndjson",
        )
        .await;
    }

    let config = GcsSourceConfig::new(&bucket)
        .prefix("data/")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host)
        .concurrency(6);
    let source = GcsSource::new(config).await.unwrap();
    let records = source.fetch_with_context(&HashMap::new()).await.unwrap();
    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, (1..=12).collect::<Vec<i64>>());
}

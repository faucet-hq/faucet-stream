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
    seed_bytes(host, bucket, name, body.as_bytes().to_vec(), content_type).await;
}

async fn seed_bytes(host: &str, bucket: &str, name: &str, body: Vec<u8>, content_type: &str) {
    let client = reqwest::Client::new();
    let url = format!(
        "{host}/upload/storage/v1/b/{bucket}/o?uploadType=media&name={}",
        urlencoding::encode(name)
    );
    client
        .post(url)
        .header("Content-Type", content_type)
        .body(body)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
}

#[tokio::test]
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
#[tokio::test]
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
    let ids: Vec<i64> = stream_all(&source)
        .await
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, (1..=12).collect::<Vec<i64>>());
}

/// A `buffered(0)` stream yields nothing forever, so `concurrency: 0` must be
/// clamped to a serial read rather than stalling.
#[tokio::test]
async fn source_concurrency_zero_reads_serially_rather_than_stalling() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    for i in 1..=3i64 {
        seed_object(
            &host,
            &bucket,
            &format!("zero/part-{i:04}.jsonl"),
            &format!("{{\"id\":{i}}}\n"),
            "application/x-ndjson",
        )
        .await;
    }
    let config = GcsSourceConfig::new(&bucket)
        .prefix("zero/")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host)
        .concurrency(0);
    let source = GcsSource::new(config).await.unwrap();
    let records = tokio::time::timeout(std::time::Duration::from_secs(60), stream_all(&source))
        .await
        .expect("concurrency 0 must not stall");
    assert_eq!(records.len(), 3);
}

async fn stream_all(source: &GcsSource) -> Vec<serde_json::Value> {
    use futures::StreamExt;
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 1000);
    let mut out = Vec::new();
    while let Some(page) = stream.next().await {
        out.extend(page.unwrap().records);
    }
    out
}

/// The preflight probe lists the bucket, so against the emulator it passes, and
/// against a bucket that does not exist it reports a failed probe, not an error.
#[tokio::test]
async fn preflight_check_passes_and_fails_against_the_emulator() {
    use faucet_core::Source as _;
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let ctx = faucet_core::check::CheckContext::default();
    for (name, want_ok) in [(bucket.as_str(), true), ("no-such-bucket", false)] {
        let c = GcsSource::new(
            GcsSourceConfig::new(name)
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host),
        )
        .await
        .unwrap();
        let report = c.check(&ctx).await.unwrap();
        assert_eq!(report.failed_count() == 0, want_ok, "{name}: {report:?}");
    }
}

/// Hash-modulo shards partition the listed objects: disjoint, and together
/// they cover every object.
#[tokio::test]
async fn shards_partition_the_listing() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    for i in 0..8i64 {
        seed_object(
            &host,
            &bucket,
            &format!("shard/part-{i}.jsonl"),
            &format!("{{\"id\":{i}}}\n"),
            "application/x-ndjson",
        )
        .await;
    }
    let config = GcsSourceConfig::new(&bucket)
        .prefix("shard/")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&host);
    let probe = GcsSource::new(config.clone()).await.unwrap();
    assert!(probe.is_shardable());
    let shards = probe.enumerate_shards(3).await.unwrap();
    assert_eq!(shards.len(), 3);
    let mut seen = Vec::new();
    for shard in &shards {
        let source = GcsSource::new(config.clone()).await.unwrap();
        source.apply_shard(shard).await.unwrap();
        seen.extend(
            stream_all(&source)
                .await
                .iter()
                .map(|r| r["id"].as_i64().unwrap()),
        );
    }
    seen.sort();
    assert_eq!(seen, (0..8).collect::<Vec<i64>>());
}

/// A key that does not exist fails the read with a typed error naming it.
#[tokio::test]
async fn missing_object_key_is_a_typed_error() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    for format in [GcsFileFormat::JsonLines, GcsFileFormat::JsonArray] {
        let source = GcsSource::new(
            GcsSourceConfig::new(&bucket)
                .object_keys(vec!["nope/missing.jsonl".into()])
                .file_format(format)
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host),
        )
        .await
        .unwrap();
        let err = source
            .fetch_with_context(&HashMap::new())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("nope/missing.jsonl"), "{err}");
    }
}

#[cfg(feature = "compression")]
#[tokio::test]
async fn gzip_objects_are_decompressed_by_extension() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let body = faucet_core::compression::compress_buf(
        b"{\"id\":1}\n{\"id\":2}\n",
        faucet_core::compression::Compression::Gzip,
    )
    .unwrap();
    seed_bytes(&host, &bucket, "gz/a.jsonl.gz", body, "application/gzip").await;
    let source = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("gz/")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    let ids: Vec<i64> = stream_all(&source)
        .await
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[cfg(feature = "file-format-csv")]
#[tokio::test]
async fn csv_objects_decode_through_the_shared_format_layer() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    seed_object(
        &host,
        &bucket,
        "csv/a.csv",
        "id,name\n1,ann\n2,bo\n",
        "text/csv",
    )
    .await;
    let source = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("csv/")
            .file_format(GcsFileFormat::Csv)
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    let rows = stream_all(&source).await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1]["name"], "bo");
}

#[cfg(feature = "arrow")]
fn parquet_bytes(rows: &[serde_json::Value], row_group: usize) -> Vec<u8> {
    let batch = faucet_core::columnar::values_to_record_batch_inferred(rows).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group))
        .build();
    let mut buf = Vec::new();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

/// Parquet is read row group by row group through ranged reads (#619), and
/// `verify_checksum` falls back to reading the whole object; both yield every
/// row, on the record path and the columnar path.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn parquet_reads_by_row_group_and_whole_object() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let rows: Vec<_> = (0..5).map(|i| serde_json::json!({"id": i})).collect();
    seed_bytes(
        &host,
        &bucket,
        "pq/a.parquet",
        parquet_bytes(&rows, 2),
        "application/vnd.apache.parquet",
    )
    .await;

    for verify_checksum in [false, true] {
        let source = GcsSource::new(
            GcsSourceConfig::new(&bucket)
                .prefix("pq/")
                .file_format(GcsFileFormat::Parquet)
                .with_batch_size(2)
                .verify_checksum(verify_checksum)
                .auth(GcsCredentials::Anonymous)
                .storage_host(&host),
        )
        .await
        .unwrap();
        let ids: Vec<i64> = stream_all(&source)
            .await
            .iter()
            .map(|r| r["id"].as_i64().unwrap())
            .collect();
        assert_eq!(
            ids,
            (0..5).collect::<Vec<i64>>(),
            "verify_checksum={verify_checksum}"
        );
        assert_eq!(
            source
                .fetch_with_context(&HashMap::new())
                .await
                .unwrap()
                .len(),
            5
        );

        assert!(source.supports_columnar());
        use futures::StreamExt;
        let ctx = HashMap::new();
        let mut batches = source.stream_batches(&ctx, 2);
        let mut total = 0;
        while let Some(page) = batches.next().await {
            total += page.unwrap().batch.num_rows();
        }
        assert_eq!(total, 5, "verify_checksum={verify_checksum}");
    }
}

/// Objects under one prefix must share a schema on the columnar path.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn parquet_schema_mismatch_across_objects_is_an_error() {
    let Some((host, bucket)) = spawn_fake_gcs().await else {
        return;
    };
    let a = parquet_bytes(&[serde_json::json!({"id": 1})], 10);
    let b = parquet_bytes(&[serde_json::json!({"name": "x"})], 10);
    seed_bytes(
        &host,
        &bucket,
        "mix/a.parquet",
        a,
        "application/vnd.apache.parquet",
    )
    .await;
    seed_bytes(
        &host,
        &bucket,
        "mix/b.parquet",
        b,
        "application/vnd.apache.parquet",
    )
    .await;
    let source = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("mix/")
            .file_format(GcsFileFormat::Parquet)
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    use futures::StreamExt;
    let ctx = HashMap::new();
    let mut batches = source.stream_batches(&ctx, 0);
    let mut err = None;
    while let Some(page) = batches.next().await {
        if let Err(e) = page {
            err = Some(e);
            break;
        }
    }
    let err = err.expect("schema mismatch must fail");
    assert!(err.to_string().contains("mix/b.parquet"), "{err}");

    let jsonl = GcsSource::new(
        GcsSourceConfig::new(&bucket)
            .prefix("mix/")
            .auth(GcsCredentials::Anonymous)
            .storage_host(&host),
    )
    .await
    .unwrap();
    assert!(!jsonl.supports_columnar());
    let mut non_parquet = jsonl.stream_batches(&ctx, 0);
    assert!(non_parquet.next().await.unwrap().is_err());
}

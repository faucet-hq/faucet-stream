//! Integration tests for the SFTP source against a real SSH server.
//!
//! The crate had none: everything under `stream.rs` — the directory listing,
//! the per-format decode, and (since #619) the bounded-concurrency prefetch —
//! was unexercised, which is a poor place to be for a *concurrent* reader
//! sharing one SSH channel. `russh-sftp` multiplexes requests over that
//! channel, so whether N reads in flight actually work is a property of the
//! library and the server, not something a unit test can assert.
//!
//! Requires Docker. Files are copied into the image rather than uploaded, so
//! the test depends on nothing but the server itself.

#![cfg(not(target_os = "windows"))]

use std::collections::HashMap;

use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::Source;
use faucet_source_sftp::{SftpFormat, SftpSource, SftpSourceConfig};
use futures::StreamExt;
use serde_json::Value;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const USER: &str = "faucet";
const PASS: &str = "secret";

/// `atmoz/sftp` takes `user:pass:::dir` and chroots the account to its home, so
/// the remote path the connector sees is `/data`.
///
/// Files are baked in with `with_copy_to`: the image creates the directory
/// itself, and copying avoids needing a working *upload* path to test the
/// *download* one.
async fn start_sftp(files: &[(String, String)]) -> Option<(ContainerAsync<GenericImage>, u16)> {
    let mut image = GenericImage::new("atmoz/sftp", "alpine")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_cmd(vec![format!("{USER}:{PASS}:::data")]);
    for (name, body) in files {
        image = image.with_copy_to(
            format!("/home/{USER}/data/{name}"),
            body.clone().into_bytes(),
        );
    }
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(22).await.ok()?;
    Some((container, port))
}

fn source(port: u16, format: SftpFormat, concurrency: usize, batch: usize) -> SftpSource {
    let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port);
    SftpSource::new(
        SftpSourceConfig::new(conn, "/data")
            .format(format)
            .with_batch_size(batch)
            .concurrency(concurrency),
    )
    .expect("config is valid")
}

async fn drain(src: &SftpSource, batch: usize) -> Vec<Value> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, batch);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page.expect("page reads").records);
    }
    out
}

/// `n` single-record JSONL files, zero-padded so lexicographic listing order is
/// numeric order.
fn jsonl_files(n: i64) -> Vec<(String, String)> {
    (1..=n)
        .map(|i| (format!("part-{i:04}.jsonl"), format!("{{\"id\":{i}}}\n")))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_reads_return_every_file_once_in_listing_order() {
    // The #619 property, and the one a shared SSH channel could plausibly
    // break: with `concurrency` reads in flight, every file must still appear
    // exactly once and in listing order.
    let Some((_c, port)) = start_sftp(&jsonl_files(12)).await else {
        return;
    };

    let ids: Vec<i64> = drain(&source(port, SftpFormat::Jsonl, 4, 1_000), 1_000)
        .await
        .iter()
        .map(|r| r["id"].as_i64().expect("id"))
        .collect();

    assert_eq!(
        ids,
        (1..=12).collect::<Vec<i64>>(),
        "a concurrent read must not drop, duplicate, or reorder files"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_serial_read_and_a_concurrent_read_agree() {
    // Concurrency must be invisible in the output. Comparing the two directly
    // is a stronger statement than checking either alone.
    let Some((_c, port)) = start_sftp(&jsonl_files(8)).await else {
        return;
    };

    let serial = drain(&source(port, SftpFormat::Jsonl, 1, 1_000), 1_000).await;
    let concurrent = drain(&source(port, SftpFormat::Jsonl, 8, 1_000), 1_000).await;
    assert_eq!(serial.len(), 8);
    assert_eq!(serial, concurrent);
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrency_zero_reads_serially_rather_than_stalling() {
    // A `buffered(0)` stream yields nothing forever, so `0` has to be clamped.
    let Some((_c, port)) = start_sftp(&jsonl_files(3)).await else {
        return;
    };

    let records = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        drain(&source(port, SftpFormat::Jsonl, 0, 1_000), 1_000),
    )
    .await
    .expect("concurrency = 0 must read serially, not hang");
    assert_eq!(records.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_text_and_json_array_survive_the_concurrent_path() {
    // The two whole-file formats: unlike JSONL these hold `concurrency` bodies
    // at once, so a prefetch that mixed up which body belonged to which path
    // would show here.
    let files = vec![
        ("a.json".to_string(), "[{\"id\":1},{\"id\":2}]".to_string()),
        ("b.json".to_string(), "[{\"id\":3}]".to_string()),
    ];
    let Some((_c, port)) = start_sftp(&files).await else {
        return;
    };

    let arr = drain(&source(port, SftpFormat::JsonArray, 2, 1_000), 1_000).await;
    let ids: Vec<i64> = arr.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3]);

    let raw = drain(&source(port, SftpFormat::RawText, 2, 1_000), 1_000).await;
    assert_eq!(raw.len(), 2, "raw_text emits one record per file");
    let paths: Vec<&str> = raw.iter().map(|r| r["path"].as_str().unwrap()).collect();
    assert!(
        paths[0].ends_with("a.json") && paths[1].ends_with("b.json"),
        "records must carry their own path, in order: {paths:?}"
    );
    assert!(raw[0]["content"].as_str().unwrap().contains("\"id\":1"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_glob_filters_the_listing() {
    let files = vec![
        ("keep-1.jsonl".to_string(), "{\"id\":1}\n".to_string()),
        ("skip.txt".to_string(), "not json".to_string()),
        ("keep-2.jsonl".to_string(), "{\"id\":2}\n".to_string()),
    ];
    let Some((_c, port)) = start_sftp(&files).await else {
        return;
    };

    let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port);
    let src = SftpSource::new(
        SftpSourceConfig::new(conn, "/data")
            .glob("*.jsonl")
            .concurrency(4),
    )
    .expect("config is valid");

    let ids: Vec<i64> = drain(&src, 1_000)
        .await
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec![1, 2],
        "the non-matching file must never be opened — decoding it would fail"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_line_is_blamed_on_its_own_file_even_when_prefetched() {
    // With a look-ahead, several files are in flight when one fails; the error
    // must still name the offending file and line.
    let files = vec![
        ("a.jsonl".to_string(), "{\"id\":1}\n".to_string()),
        ("b.jsonl".to_string(), "not json at all\n".to_string()),
        ("c.jsonl".to_string(), "{\"id\":3}\n".to_string()),
    ];
    let Some((_c, port)) = start_sftp(&files).await else {
        return;
    };

    let src = source(port, SftpFormat::Jsonl, 4, 1_000);
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 1_000);
    let mut err = None;
    while let Some(page) = pages.next().await {
        if let Err(e) = page {
            err = Some(e);
            break;
        }
    }
    let err = err.expect("the malformed file must fail the stream");
    let msg = err.to_string();
    assert!(msg.contains("b.jsonl"), "must name the file: {msg}");
    assert!(msg.contains("line 1"), "must name the line: {msg}");
}

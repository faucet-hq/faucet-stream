//! An object body that drops mid-stream is resumed from the last byte
//! received, pinned to the generation the read started on, a bounded number
//! of times (`body_resume_attempts`), and each resume is counted.
//!
//! The backend is an in-process HTTP server that announces a full body and
//! closes the connection part-way through it, which is what a dropped
//! transfer looks like to the client. No Docker.
#![allow(clippy::disallowed_methods)] // tests drive raw sockets directly
#![cfg(not(target_os = "windows"))]

use faucet_core::Source;
use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
use faucet_source_gcs::{GcsCredentials, GcsSource, GcsSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const GENERATION: &str = "7";

#[derive(Debug, Clone)]
struct Seen {
    target: String,
    range: Option<(usize, usize)>,
}

/// Serves one object. A media read answers `200` (or `206` for a `Range`)
/// announcing every byte asked for; while `cuts` remain it sends only `keep`
/// of them and closes the connection. A metadata read answers the object's
/// JSON resource.
struct CuttingServer {
    url: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl CuttingServer {
    async fn start(body: Vec<u8>, cuts: usize, keep: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new((body, Mutex::new(cuts), keep, seen.clone()));
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(serve(socket, state.clone()));
            }
        });
        Self { url, seen, task }
    }

    fn media_reads(&self) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.target.contains("alt=media"))
            .cloned()
            .collect()
    }
}

impl Drop for CuttingServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

type State = Arc<(Vec<u8>, Mutex<usize>, usize, Arc<Mutex<Vec<Seen>>>)>;

async fn serve(mut socket: TcpStream, state: State) {
    let (body, cuts, keep, seen) = &*state;
    let mut head = Vec::new();
    let mut buf = [0u8; 4096];
    while !head.windows(4).any(|w| w == b"\r\n\r\n") {
        match socket.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => head.extend_from_slice(&buf[..n]),
        }
    }
    let head = String::from_utf8_lossy(&head).into_owned();
    let target = head
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    let range = head.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if !name.eq_ignore_ascii_case("range") {
            return None;
        }
        let (start, end) = value.trim().strip_prefix("bytes=")?.split_once('-')?;
        Some((start.parse().ok()?, end.parse().ok()?))
    });
    seen.lock().unwrap().push(Seen {
        target: target.clone(),
        range,
    });

    if !target.contains("alt=media") {
        let json = format!(
            "{{\"name\":\"o\",\"bucket\":\"b\",\"size\":\"{}\",\"generation\":\"{GENERATION}\"}}",
            body.len()
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{json}",
            json.len()
        );
        let _ = socket.write_all(response.as_bytes()).await;
        let _ = socket.shutdown().await;
        return;
    }

    let (start, end) = range.unwrap_or((0, body.len() - 1));
    let wanted = &body[start..=end];
    let status = match range {
        Some(_) => format!(
            "206 Partial Content\r\ncontent-range: bytes {start}-{end}/{}",
            body.len()
        ),
        None => "200 OK".to_string(),
    };
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/octet-stream\r\n\
         content-length: {}\r\nx-goog-generation: {GENERATION}\r\n\
         x-goog-metageneration: 1\r\nx-goog-stored-content-length: {}\r\n\
         x-goog-storage-class: STANDARD\r\nconnection: close\r\n\r\n",
        wanted.len(),
        body.len()
    );
    let cut = {
        let mut left = cuts.lock().unwrap();
        let cut = *left > 0 && wanted.len() > *keep;
        if cut {
            *left -= 1;
        }
        cut
    };
    let sent = if cut { &wanted[..*keep] } else { wanted };
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.write_all(sent).await;
    let _ = socket.shutdown().await;
}

fn jsonl(n: i64) -> Vec<u8> {
    (0..n)
        .map(|i| format!("{{\"id\":{i},\"pad\":\"{}\"}}\n", "x".repeat(40)))
        .collect::<String>()
        .into_bytes()
}

async fn source(server: &CuttingServer, attempts: Option<u32>) -> GcsSource {
    let mut config = GcsSourceConfig::new("b")
        .auth(GcsCredentials::Anonymous)
        .storage_host(&server.url)
        .object_keys(vec!["a.jsonl".into()])
        .with_batch_size(50);
    if let Some(attempts) = attempts {
        config = config.body_resume_attempts(attempts);
    }
    GcsSource::new(config).await.expect("source builds offline")
}

fn metered(source: &GcsSource) -> Arc<faucet_core::UsageMeter> {
    let meter = Arc::new(faucet_core::UsageMeter::new());
    source.set_roundtrip_recorder(Arc::new(
        RoundtripRecorder::new(RoundtripSide::Source, "p", "r", "gcs").with_meter(meter.clone()),
    ));
    meter
}

async fn read_all(source: &GcsSource) -> Result<Vec<serde_json::Value>, faucet_core::FaucetError> {
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 50);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page?.records);
    }
    Ok(out)
}

#[test]
fn a_dropped_body_resumes_from_the_last_byte_on_the_same_generation() {
    faucet_conformance::block_on_large_stack(async {
        let body = jsonl(200);
        let total = body.len();
        // Mid-line, so the splice is checked byte for byte.
        let keep = 1_003;
        let server = CuttingServer::start(body, 1, keep).await;
        let source = source(&server, None).await;
        let meter = metered(&source);

        let records = read_all(&source).await.expect("the read resumes");
        let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, (0..200).collect::<Vec<i64>>());

        let reads = server.media_reads();
        assert_eq!(reads.len(), 2, "{reads:?}");
        assert!(!reads[0].target.contains("generation="), "{reads:?}");
        assert_eq!(reads[0].range, None, "{reads:?}");
        assert!(
            reads[1]
                .target
                .contains(&format!("generation={GENERATION}")),
            "the resume must pin the generation: {reads:?}"
        );
        assert_eq!(reads[1].range, Some((keep, total - 1)), "{reads:?}");

        let usage = meter.snapshot();
        assert_eq!(
            usage.source_retries.get("connection"),
            Some(&1),
            "{usage:?}"
        );
        assert_eq!(usage.source_roundtrips.get("get"), Some(&2), "{usage:?}");
    });
}

#[test]
fn a_body_that_keeps_dropping_fails_after_the_configured_resumes() {
    faucet_conformance::block_on_large_stack(async {
        for (configured, resumes) in [(Some(0), 0), (Some(2), 2), (None, 3)] {
            let server = CuttingServer::start(jsonl(200), usize::MAX, 100).await;
            let source = source(&server, configured).await;
            let meter = metered(&source);

            let outcome =
                tokio::time::timeout(std::time::Duration::from_secs(30), read_all(&source))
                    .await
                    .expect("the read must end, not loop");
            let err = outcome.expect_err("a body that never completes must fail the read");
            assert!(
                matches!(err, faucet_core::FaucetError::Source(_)),
                "{configured:?}: {err:?}"
            );

            let reads = server.media_reads();
            assert_eq!(reads.len(), 1 + resumes, "{configured:?}: {reads:?}");
            let usage = meter.snapshot();
            assert_eq!(
                usage.source_retries.get("connection").copied().unwrap_or(0),
                resumes as u64,
                "{configured:?}: {usage:?}"
            );
        }
    });
}

#[cfg(feature = "arrow")]
#[test]
fn a_dropped_parquet_range_resumes() {
    faucet_conformance::block_on_large_stack(async {
        let rows: Vec<_> = (0..500)
            .map(|i| serde_json::json!({"id": i, "pad": "x".repeat(40)}))
            .collect();
        let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows).unwrap();
        let mut body = Vec::new();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(&mut body, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let server = CuttingServer::start(body, 1, 3).await;
        let source = GcsSource::new(
            GcsSourceConfig::new("b")
                .auth(GcsCredentials::Anonymous)
                .storage_host(&server.url)
                .object_keys(vec!["a.parquet".into()])
                .file_format(faucet_source_gcs::GcsFileFormat::Parquet),
        )
        .await
        .expect("source builds offline");
        let meter = metered(&source);

        let records = read_all(&source).await.expect("the range resumes");
        let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, (0..500).collect::<Vec<i64>>());

        let reads = server.media_reads();
        let (first, resumed) = (reads[0].range.unwrap(), reads[1].range.unwrap());
        assert_eq!(resumed, (first.0 + 3, first.1), "{reads:?}");
        assert!(
            reads
                .iter()
                .all(|r| r.target.contains(&format!("generation={GENERATION}"))),
            "{reads:?}"
        );
        let usage = meter.snapshot();
        assert_eq!(
            usage.source_retries.get("connection"),
            Some(&1),
            "{usage:?}"
        );
    });
}

//! #621 — concurrent result-partition fetch.
//!
//! Snowflake splits a large result into server-chosen partitions, each its own
//! `GET …/partition={n}`. Fetching them one at a time makes the read
//! latency-bound on the round trip rather than throughput-bound on the data,
//! and the partition count is the *server's* choice — nothing `batch_size` can
//! influence.
//!
//! Overlap is measured by counting **simultaneously open requests**, not by a
//! wall-clock ratio: a timing ratio is a race on a loaded runner, which is how
//! the equivalent object-store test first failed.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_common_snowflake::SnowflakeAuth;
use faucet_core::Source as _;
use faucet_source_snowflake::{SnowflakeSource, SnowflakeSourceConfig};
use futures::StreamExt as _;
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

const PARTITIONS: usize = 6;

/// Counts concurrent in-flight partition requests and records the peak.
struct CountingResponder {
    in_flight: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

impl Respond for CountingResponder {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(now, Ordering::SeqCst);
        let in_flight = Arc::clone(&self.in_flight);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(120)).await;
            in_flight.fetch_sub(1, Ordering::SeqCst);
        });
        ResponseTemplate::new(200)
            .set_delay(std::time::Duration::from_millis(120))
            .set_body_json(json!({ "data": [["9", "row"]] }))
    }
}

fn cfg(concurrency: usize) -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id, name FROM events",
    )
    .with_batch_size(1000)
    .with_partition_concurrency(concurrency)
}

/// Mount the initial statement (reporting `PARTITIONS` partitions) and the
/// per-partition GETs.
async fn mount(server: &MockServer) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let partitions: Vec<Value> = (0..PARTITIONS).map(|_| json!({ "rowCount": 1 })).collect();
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h1",
            "resultSetMetaData": {
                "partitionInfo": partitions,
                "rowType": [
                    { "name": "ID", "type": "fixed" },
                    { "name": "NAME", "type": "text" }
                ]
            },
            "data": [["1", "first"]]
        })))
        .mount(server)
        .await;

    let in_flight = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h1"))
        .and(query_param("partition", "1"))
        .respond_with(CountingResponder {
            in_flight: Arc::clone(&in_flight),
            peak: Arc::clone(&peak),
        })
        .mount(server)
        .await;
    for n in 2..PARTITIONS {
        Mock::given(method("GET"))
            .and(path("/api/v2/statements/h1"))
            .and(query_param("partition", n.to_string()))
            .respond_with(CountingResponder {
                in_flight: Arc::clone(&in_flight),
                peak: Arc::clone(&peak),
            })
            .mount(server)
            .await;
    }
    (in_flight, peak)
}

async fn run(concurrency: usize) -> (usize, usize) {
    let server = MockServer::start().await;
    let (_in_flight, peak) = mount(&server).await;
    let source = SnowflakeSource::new(cfg(concurrency))
        .expect("source")
        .with_endpoint_base(server.uri());
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let mut rows = 0usize;
    while let Some(p) = pages.next().await {
        rows += p.expect("page").records.len();
    }
    (rows, peak.load(Ordering::SeqCst))
}

#[tokio::test]
async fn partitions_are_fetched_concurrently() {
    let (rows, peak) = run(4).await;
    // 1 row from the initial statement + 1 per remaining partition.
    assert_eq!(rows, PARTITIONS, "every partition's rows must arrive");
    assert!(
        peak > 1,
        "partition_concurrency: 4 must overlap partition fetches — peak was {peak}"
    );
    assert!(peak <= 4, "the knob is a ceiling, not a suggestion: {peak}");
}

#[tokio::test]
async fn a_concurrency_of_one_stays_sequential() {
    // The control: without this, a bug that always ran flat-out would leave
    // the test above green while ignoring the setting.
    let (rows, peak) = run(1).await;
    assert_eq!(rows, PARTITIONS);
    assert_eq!(peak, 1, "explicit 1 means one at a time, got {peak}");
}

#[tokio::test]
async fn rows_arrive_in_partition_order() {
    // Ordered look-ahead, not unordered: a result set the user wrote an
    // ORDER BY for must not be silently reordered by a throughput knob.
    let server = MockServer::start().await;
    let partitions: Vec<Value> = (0..3).map(|_| json!({ "rowCount": 1 })).collect();
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h1",
            "resultSetMetaData": {
                "partitionInfo": partitions,
                "rowType": [{ "name": "ID", "type": "fixed" }]
            },
            "data": [["0"]]
        })))
        .mount(&server)
        .await;
    // Partition 1 is slow, partition 2 fast: unordered consumption would put
    // 2 before 1.
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h1"))
        .and(query_param("partition", "1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(std::time::Duration::from_millis(200))
                .set_body_json(json!({ "data": [["1"]] })),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h1"))
        .and(query_param("partition", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [["2"]] })))
        .mount(&server)
        .await;

    let source = SnowflakeSource::new(cfg(4))
        .expect("source")
        .with_endpoint_base(server.uri());
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let mut ids = Vec::new();
    while let Some(p) = pages.next().await {
        for r in p.expect("page").records {
            ids.push(r["ID"].clone());
        }
    }
    assert_eq!(
        ids,
        vec![json!(0), json!(1), json!(2)],
        "a slow early partition must still come first"
    );
}

//! `faucet-conformance` Tier-1 battery for the Snowflake source.
//!
//! Check 1 — the connector's config JSON Schema is a valid, well-formed value.
//! Check 2 — `stream_pages` pages under a bounded batch size (every record
//! streamed; peak page ≤ batch_size and < total), i.e. memory is O(batch_size)
//! regardless of total volume.
//!
//! The bounded-memory check drives the source against a wiremock fake of the
//! Snowflake SQL REST API. The result set is split into 12 server-side
//! partitions of 500 rows (6 000 total): partition 0 rides the initial
//! `POST /api/v2/statements` response, partitions 1..12 are fetched via
//! `GET /api/v2/statements/{handle}?partition=N`. The source re-frames those
//! partitions into pages of the configured `batch_size` (250), so peak page is
//! 250 < 6 000.

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_errors_not_panics, assert_first_page_before_exhaustion,
};
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{body_string_contains, method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

const HANDLE: &str = "conformance-handle";
const NUM_PARTITIONS: usize = 12;
const ROWS_PER_PARTITION: usize = 500;

fn metadata() -> Value {
    let partition_info: Vec<Value> = (0..NUM_PARTITIONS)
        .map(|_| json!({"rowCount": ROWS_PER_PARTITION}))
        .collect();
    json!({
        "rowType": [
            {"name": "ID", "type": "fixed"}
        ],
        "partitionInfo": partition_info,
        "format": "jsonv2",
        "numRows": (NUM_PARTITIONS * ROWS_PER_PARTITION) as u64,
    })
}

/// Rows for partition `p`: `ROWS_PER_PARTITION` single-column rows, globally
/// numbered so every id across the result set is unique.
fn rows_for_partition(p: usize) -> Vec<Vec<Value>> {
    (0..ROWS_PER_PARTITION)
        .map(|i| vec![json!((p * ROWS_PER_PARTITION + i).to_string())])
        .collect()
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SnowflakeSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-snowflake");
}

/// Answers with `body` and counts the rows it has served.
struct Counted {
    body: Value,
    rows: usize,
    served: Arc<AtomicUsize>,
}

impl Counted {
    fn new(served: &Arc<AtomicUsize>, rows: usize, body: Value) -> Self {
        Self {
            body,
            rows,
            served: Arc::clone(served),
        }
    }
}

impl Respond for Counted {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        self.served.fetch_add(self.rows, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(self.body.clone())
    }
}

/// Mount the partitioned result; returns the served-row count.
async fn mount_partitions(server: &MockServer) -> Arc<AtomicUsize> {
    let served = Arc::new(AtomicUsize::new(0));
    // Initial statement submission returns partition 0 inline + the full
    // partition manifest.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(Counted::new(
            &served,
            ROWS_PER_PARTITION,
            json!({
                "code": "090001",
                "statementHandle": HANDLE,
                "resultSetMetaData": metadata(),
                "data": rows_for_partition(0),
            }),
        ))
        .mount(server)
        .await;

    // Partitions 1..NUM_PARTITIONS are fetched on demand.
    for p in 1..NUM_PARTITIONS {
        Mock::given(method("GET"))
            .and(path(format!("/api/v2/statements/{HANDLE}")))
            .and(query_param("partition", p.to_string()))
            .respond_with(Counted::new(
                &served,
                ROWS_PER_PARTITION,
                json!({
                    "code": "090001",
                    "data": rows_for_partition(p),
                }),
            ))
            .mount(server)
            .await;
    }

    served
}

fn bounded_source(server: &MockServer) -> SnowflakeSource {
    let config = SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id FROM events",
    )
    .with_batch_size(250);
    SnowflakeSource::new(config)
        .expect("source new")
        .with_endpoint_base(server.uri())
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_first_page_before_exhaustion() {
    let server = MockServer::start().await;
    let served = mount_partitions(&server).await;
    let source = bounded_source(&server);
    assert_first_page_before_exhaustion(&source, 250, NUM_PARTITIONS * ROWS_PER_PARTITION, || {
        served.load(Ordering::SeqCst)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let server = MockServer::start().await;

    mount_partitions(&server).await;

    // Config batch_size must equal the batch passed to the battery — this
    // overriding source treats its config batch_size as authoritative.
    let config = SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id FROM events",
    )
    .with_batch_size(250);
    let source = SnowflakeSource::new(config)
        .expect("source new")
        .with_endpoint_base(server.uri());

    assert_bounded_memory(&source, 250, NUM_PARTITIONS * ROWS_PER_PARTITION).await;
}

// ── Check 12: discovery round-trips ───────────────────────────────────────────

/// Every table `discover()` reports (from the `information_schema` catalog SQL)
/// must be genuinely selectable: take its config_patch (`{"query": …}`), rebuild
/// the source pointed at that query, and read it. Both the catalog SQL and the
/// data query hit `POST /api/v2/statements`, so the mock distinguishes them by
/// body — a realistic mock of the Snowflake SQL REST API.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let server = MockServer::start().await;

    // Catalog statement: the `information_schema` query returns one table
    // `PUBLIC.ORDERS` with a single column `ID`.
    let catalog_meta = json!({
        "rowType": [
            {"name": "TABLE_SCHEMA", "type": "text"},
            {"name": "TABLE_NAME", "type": "text"},
            {"name": "COLUMN_NAME", "type": "text"},
            {"name": "DATA_TYPE", "type": "text"},
            {"name": "IS_NULLABLE", "type": "text"},
            {"name": "ROW_COUNT", "type": "fixed"},
        ],
        "partitionInfo": [{"rowCount": 1}],
        "format": "jsonv2",
        "numRows": 1,
    });
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("information_schema"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": HANDLE,
            "resultSetMetaData": catalog_meta,
            "data": [["PUBLIC", "ORDERS", "ID", "NUMBER", "NO", "3"]],
        })))
        .mount(&server)
        .await;

    // Data query for the rebuilt source: `SELECT * FROM "PUBLIC"."ORDERS"`.
    let data_meta = json!({
        "rowType": [{"name": "ID", "type": "fixed"}],
        "partitionInfo": [{"rowCount": 3}],
        "format": "jsonv2",
        "numRows": 3,
    });
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("ORDERS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "data-handle",
            "resultSetMetaData": data_meta,
            "data": [["0"], ["1"], ["2"]],
        })))
        .mount(&server)
        .await;

    let base = SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT 1",
    );
    let source = SnowflakeSource::new(base)
        .expect("source new")
        .with_endpoint_base(server.uri());

    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let uri = server.uri();
        async move {
            let query = patch["query"].as_str().expect("query patch").to_string();
            let cfg = SnowflakeSourceConfig::new(
                "xy12345",
                "WH",
                "DB",
                "PUBLIC",
                SnowflakeAuth::OAuth { token: "t".into() },
                query,
            );
            Box::new(
                SnowflakeSource::new(cfg)
                    .expect("rebuilt source")
                    .with_endpoint_base(uri),
            ) as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

#[tokio::test]
async fn conformance_errors_not_panics() {
    // Unreachable endpoint: `new()` builds (lazy HTTP client), the first read
    // errors with a typed `FaucetError` (connection refused) without panicking.
    // Port 1 refuses connections immediately on all platforms.
    let config = SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id FROM events",
    );
    let source = SnowflakeSource::new(config)
        .expect("source new")
        .with_endpoint_base("http://127.0.0.1:1");
    // Check 10: connector_name() is non-empty (reuses this offline instance).
    assert_connector_name_nonempty(&source);
    assert_errors_not_panics(&source).await;
}

// ── Descriptor agreement (RFC 0013) ───────────────────────────────────────────

#[test]
fn conformance_descriptor_matches_built_source() {
    let v = json!({
        "account": "xy12345",
        "warehouse": "WH",
        "database": "DB",
        "schema": "PUBLIC",
        "auth": {"type": "oauth", "config": {"token": "t"}},
        "query": "SELECT 1",
    });
    let cfg: SnowflakeSourceConfig = serde_json::from_value(v.clone()).unwrap();
    let built = SnowflakeSource::new(cfg).expect("source new");
    faucet_conformance::assert_descriptor_matches_source(
        &faucet_source_snowflake::DESCRIPTOR,
        "snowflake",
        &v,
        &built,
    );
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

#[tokio::test]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let uri = hung.http_url();
    let make = || {
        let config = SnowflakeSourceConfig::new(
            "xy12345",
            "WH",
            "DB",
            "PUBLIC",
            SnowflakeAuth::OAuth { token: "t".into() },
            "SELECT id FROM events",
        );
        SnowflakeSource::new(config)
            .expect("source new")
            .with_endpoint_base(uri.clone())
    };
    faucet_conformance::calls::assert_source_call_deadline(make).await;
}

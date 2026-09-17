//! REST API → JSONL — full builder showcase for both connectors.
//!
//! Exercises most of the knobs on `RestStreamConfig` (auth, pagination,
//! retries, throttling, transforms, schema, replication) and the full
//! surface of `JsonlSinkConfig` (append + pretty-printing).
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_jsonl \
//!     --features "source-rest sink-jsonl transforms"
//! ```

use std::time::Duration;

use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::{
    Auth, KeyCaseMode, Labels, PaginationStyle, Pipeline, RecordTransform, ReplicationMethod,
    RestStream, RestStreamConfig, Source, TransformStage, TransformingSource, json,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let inner = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/orders")
            .name("orders")
            .method(reqwest::Method::GET)
            .auth(Auth::Bearer {
                token: std::env::var("API_TOKEN")?,
            })
            .header("X-Client", "faucet-stream")
            .query("status", "completed")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.meta.next_cursor".into(),
                param_name: "cursor".into(),
            })
            .max_pages(50)
            .request_delay(Duration::from_millis(200))
            .timeout(Duration::from_secs(30))
            .max_retries(3)
            .retry_backoff(Duration::from_secs(1))
            .tolerate_http_error(404)
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2026-01-01T00:00:00Z"))
            .primary_keys(vec!["id".into()])
            .schema_sample_size(50),
    )?;
    let source = TransformingSource::new(
        Box::new(inner) as Box<dyn Source>,
        vec![TransformStage::Map(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
            on_collision: Default::default(),
        })],
        Labels::for_named("rest"),
    )?;

    let sink = JsonlSink::new(
        JsonlSinkConfig::new("orders.jsonl")
            .append(true)
            .pretty(false),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "wrote {} orders; next-run bookmark: {:?}",
        result.records_written, result.bookmark
    );
    Ok(())
}

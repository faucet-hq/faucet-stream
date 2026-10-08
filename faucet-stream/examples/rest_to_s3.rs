//! REST API → S3 — REST + S3 sink knob showcase.
//!
//! REST uses offset pagination and a custom auth header. S3 sink shows
//! prefix, region override, custom file extension, max-records-per-file
//! sharding, and parallel-upload concurrency.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example rest_to_s3 \
//!     --features "source-rest sink-s3"
//! ```

use std::time::Duration;

use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::{Auth, PaginationStyle, Pipeline, RestStream, RestStreamConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = RestStream::new(
        RestStreamConfig::new("https://api.example.com", "/v1/events")
            .name("events")
            .auth(Auth::ApiKey {
                header: "X-Auth-Token".into(),
                value: std::env::var("AUTH_TOKEN")?,
            })
            .header("Accept", "application/json")
            .query("source", "web")
            .records_path("$.events[*]")
            .pagination(PaginationStyle::Offset {
                offset_param: "offset".into(),
                limit_param: "limit".into(),
                limit: 500,
                total_path: Some("$.meta.total".into()),
                rows_path: None,
            })
            .max_pages(usize::MAX)
            .request_delay(Duration::from_millis(50))
            .timeout(Duration::from_secs(30))
            .max_retries(3),
    )?;

    let sink = S3Sink::new(
        S3SinkConfig::new("my-data-lake")
            .prefix("events/raw/")
            .region("us-east-1")
            .file_extension(".jsonl")
            .max_records_per_file(10_000)
            .concurrency(8),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "landed {} events into s3://my-data-lake/",
        result.records_written
    );
    Ok(())
}

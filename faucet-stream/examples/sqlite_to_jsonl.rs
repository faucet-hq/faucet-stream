#![allow(deprecated)]
//! SQLite → JSONL — full builder showcase for both connectors.
//!
//! SQLite source uses a tuned pool. JSONL sink demonstrates append and
//! pretty-printing modes.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example sqlite_to_jsonl \
//!     --features "source-sqlite sink-jsonl"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::jsonl::{JsonlSink, JsonlSinkConfig};
use faucet_stream::source::sqlite::{SqliteSource, SqliteSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SqliteSource::new(
        SqliteSourceConfig::new("sqlite:./app.db", "SELECT * FROM events ORDER BY ts")
            .with_max_connections(4),
    )
    .await?;

    let sink = JsonlSink::new(
        JsonlSinkConfig::new("events.jsonl")
            .append(true)
            .pretty(false),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("dumped {} events to events.jsonl", result.records_written);
    Ok(())
}

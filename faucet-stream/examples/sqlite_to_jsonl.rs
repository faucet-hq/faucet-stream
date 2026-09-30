//! SQLite → JSONL — full builder showcase for both connectors.
//!
//! SQLite source uses a tuned pool. The file sink appends JSON Lines.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example sqlite_to_jsonl \
//!     --features "source-sqlite sink-file"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::file::{FileMode, FileSink, FileSinkConfig};
use faucet_stream::source::sqlite::{SqliteSource, SqliteSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SqliteSource::new(
        SqliteSourceConfig::new("sqlite:./app.db", "SELECT * FROM events ORDER BY ts")
            .with_max_connections(4),
    )
    .await?;

    let sink = FileSink::new(FileSinkConfig::new("events.jsonl").mode(FileMode::Append))?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("dumped {} events to events.jsonl", result.records_written);
    Ok(())
}

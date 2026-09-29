#![allow(deprecated)]
//! SQLite → CSV — full builder showcase for both connectors.
//!
//! SQLite source uses a tuned pool. CSV sink demonstrates delimiter,
//! header toggle, and append mode.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example sqlite_to_csv \
//!     --features "source-sqlite sink-csv"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::csv::{CsvSink, CsvSinkConfig};
use faucet_stream::source::sqlite::{SqliteSource, SqliteSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = SqliteSource::new(
        SqliteSourceConfig::new(
            "sqlite:local.db",
            "SELECT id, name, price FROM products ORDER BY id",
        )
        .with_max_connections(4),
    )
    .await?;

    let sink = CsvSink::new(
        CsvSinkConfig::new("products.csv")
            .delimiter(b',')
            .write_headers(true)
            .append(false),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "wrote {} product rows to products.csv",
        result.records_written
    );
    Ok(())
}

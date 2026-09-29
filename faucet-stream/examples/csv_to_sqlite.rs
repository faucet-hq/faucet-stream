#![allow(deprecated)]
//! CSV → SQLite — full builder showcase for both connectors.
//!
//! CSV source uses a TSV-like config (tab delimiter, no headers). SQLite
//! sink demonstrates the JSON column mapping plus batch and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_sqlite \
//!     --features "source-csv sink-sqlite"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use faucet_stream::source::csv::{CsvSource, CsvSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = CsvSource::new(
        CsvSourceConfig::new("inventory.tsv")
            .has_headers(false)
            .delimiter(b'\t')
            .quote(b'\''),
    );

    let sink = SqliteSink::new(
        SqliteSinkConfig::new("sqlite:./inventory.db", "inventory")
            .column_mapping(SqliteColumnMapping::Json {
                column: "row".into(),
            })
            .with_batch_size(500)
            .max_connections(4),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "imported {} inventory rows into SQLite",
        result.records_written
    );
    Ok(())
}

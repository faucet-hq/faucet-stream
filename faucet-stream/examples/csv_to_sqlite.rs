//! CSV → SQLite — full builder showcase for both connectors.
//!
//! CSV source uses a TSV-like config (tab delimiter, no headers). SQLite
//! sink demonstrates the JSON column mapping plus batch and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_sqlite \
//!     --features "source-file file-format-csv sink-sqlite"
//! ```

use faucet_stream::CsvOptions;
use faucet_stream::Pipeline;
use faucet_stream::sink::sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use faucet_stream::source::file::{FileSource, FileSourceConfig, FileSourceFormat};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = FileSourceConfig::new("inventory.tsv").format(FileSourceFormat::Csv);
    config.csv = CsvOptions {
        has_headers: false,
        delimiter: "\t".into(),
        quote: "'".into(),
        ..CsvOptions::default()
    };
    let source = FileSource::new(config)?;

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

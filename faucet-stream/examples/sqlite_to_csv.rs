//! SQLite → CSV — full builder showcase for both connectors.
//!
//! SQLite source uses a tuned pool. The file sink writes CSV with an
//! explicit delimiter and a header row.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example sqlite_to_csv \
//!     --features "source-sqlite sink-file file-format-csv"
//! ```

use faucet_stream::CsvOptions;
use faucet_stream::Pipeline;
use faucet_stream::sink::file::{FileSink, FileSinkConfig};
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

    let sink = FileSink::new(FileSinkConfig::new("products.csv").csv(CsvOptions {
        delimiter: ",".into(),
        has_headers: true,
        ..CsvOptions::default()
    }))?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "wrote {} product rows to products.csv",
        result.records_written
    );
    Ok(())
}

//! CSV → BigQuery — full builder showcase for both connectors.
//!
//! CSV source uses non-default delimiter + quote. BigQuery sink shows the
//! key-path credential variant and batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_bigquery \
//!     --features "source-file file-format-csv sink-bigquery"
//! ```

use faucet_stream::CsvOptions;
use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::file::{FileSource, FileSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = FileSourceConfig::new("transactions.csv");
    config.csv = CsvOptions {
        has_headers: true,
        delimiter: ",".into(),
        quote: "\"".into(),
        ..CsvOptions::default()
    };
    let source = FileSource::new(config)?;

    let sink = BigQuerySink::new(
        BigQuerySinkConfig::new(
            "my-gcp-project",
            "warehouse",
            "transactions",
            BigQueryCredentials::ServiceAccountKeyPath {
                path: "service-account.json".into(),
            },
        )
        .with_batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} CSV rows into BigQuery", result.records_written);
    Ok(())
}

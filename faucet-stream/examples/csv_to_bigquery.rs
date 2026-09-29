#![allow(deprecated)]
//! CSV → BigQuery — full builder showcase for both connectors.
//!
//! CSV source uses non-default delimiter + quote. BigQuery sink shows the
//! key-path credential variant and batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_bigquery \
//!     --features "source-csv sink-bigquery"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use faucet_stream::source::csv::{CsvSource, CsvSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = CsvSource::new(
        CsvSourceConfig::new("transactions.csv")
            .has_headers(true)
            .delimiter(b',')
            .quote(b'"'),
    );

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

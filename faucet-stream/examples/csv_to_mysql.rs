#![allow(deprecated)]
//! CSV → MySQL — full builder showcase for both connectors.
//!
//! CSV source uses non-default delimiter and quote characters. MySQL sink
//! demonstrates the `AutoMap` column mapping plus batch and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_mysql \
//!     --features "source-csv sink-mysql"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use faucet_stream::source::csv::{CsvSource, CsvSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = CsvSource::new(
        CsvSourceConfig::new("customers.csv")
            .has_headers(true)
            .delimiter(b',')
            .quote(b'"'),
    );

    let sink = MysqlSink::new(
        MysqlSinkConfig::new("mysql://user:pass@localhost/crm", "customers_imported")
            .column_mapping(MysqlColumnMapping::AutoMap)
            .with_batch_size(1000)
            .max_connections(10),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!("loaded {} customer rows into MySQL", result.records_written);
    Ok(())
}

//! CSV → MySQL — full builder showcase for both connectors.
//!
//! CSV source uses non-default delimiter and quote characters. MySQL sink
//! demonstrates the `AutoMap` column mapping plus batch and pool tuning.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example csv_to_mysql \
//!     --features "source-file file-format-csv sink-mysql"
//! ```

use faucet_stream::CsvOptions;
use faucet_stream::Pipeline;
use faucet_stream::sink::mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use faucet_stream::source::file::{FileSource, FileSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = FileSourceConfig::new("customers.csv");
    config.csv = CsvOptions {
        has_headers: true,
        delimiter: ",".into(),
        quote: "\"".into(),
        ..CsvOptions::default()
    };
    let source = FileSource::new(config)?;

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

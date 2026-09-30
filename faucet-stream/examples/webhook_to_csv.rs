//! Webhook receiver → CSV — full builder showcase for both connectors.
//!
//! Webhook source uses listen-addr, path, max-payloads, and timeout knobs.
//! The file sink writes CSV with a delimiter, a header row, and append mode.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_csv \
//!     --features "source-webhook sink-file file-format-csv"
//! ```

use faucet_stream::CsvOptions;
use faucet_stream::Pipeline;
use faucet_stream::sink::file::{FileMode, FileSink, FileSinkConfig};
use faucet_stream::source::webhook::{WebhookSource, WebhookSourceConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:9090")
            .path("/inbox")
            .max_payloads(5_000)
            .timeout_secs(120),
    );

    let sink = FileSink::new(
        FileSinkConfig::new("webhooks.csv")
            .mode(FileMode::Append)
            .csv(CsvOptions {
                delimiter: ";".into(),
                has_headers: true,
                ..CsvOptions::default()
            }),
    )?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "captured {} webhook payloads into webhooks.csv",
        result.records_written
    );
    Ok(())
}

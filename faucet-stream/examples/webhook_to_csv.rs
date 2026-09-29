#![allow(deprecated)]
//! Webhook receiver → CSV — full builder showcase for both connectors.
//!
//! Webhook source uses listen-addr, path, max-payloads, and timeout knobs.
//! CSV sink shows delimiter, header toggle, and append mode.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example webhook_to_csv \
//!     --features "source-webhook sink-csv"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::csv::{CsvSink, CsvSinkConfig};
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

    let sink = CsvSink::new(
        CsvSinkConfig::new("webhooks.csv")
            .delimiter(b';')
            .write_headers(true)
            .append(true),
    );

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "captured {} webhook payloads into webhooks.csv",
        result.records_written
    );
    Ok(())
}

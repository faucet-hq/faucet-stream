//! XML/SOAP → MongoDB — full builder showcase for both connectors.
//!
//! XML source uses Bearer auth and offset pagination. MongoDB sink shows
//! batch sizing.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example xml_to_mongodb \
//!     --features "source-xml sink-mongodb"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::mongodb::{MongoSink, MongoSinkConfig};
use faucet_stream::source::xml::{XmlAuth, XmlPagination, XmlStream, XmlStreamConfig};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    headers.insert("Accept", HeaderValue::from_static("application/xml"));

    let source = XmlStream::new(
        XmlStreamConfig::new("https://feeds.example.com", "/catalog")
            .method(reqwest::Method::GET)
            .auth(XmlAuth::Bearer {
                token: std::env::var("FEED_TOKEN")?,
            })
            .headers(headers)
            .query_param("format", "xml")
            .records_element_path("catalog.products.product")
            .pagination(XmlPagination::Offset {
                offset_param: "offset".into(),
                limit_param: "limit".into(),
                limit: 250,
                stop_when_short: false,
            })
            .max_pages(usize::MAX),
    );

    let sink = MongoSink::new(
        MongoSinkConfig::new("mongodb://localhost:27017", "warehouse", "catalog_items")
            .with_batch_size(1000),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "inserted {} catalog items into MongoDB",
        result.records_written
    );
    Ok(())
}

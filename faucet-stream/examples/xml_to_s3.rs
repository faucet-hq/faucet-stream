//! XML/SOAP → AWS S3 — full builder showcase for both connectors.
//!
//! XML source exercises Basic auth, custom headers, a SOAP request body,
//! a records dot-path, page-number pagination, and a query parameter. S3
//! sink shows prefix, region, endpoint override (e.g. MinIO/LocalStack),
//! file extension, sharding and parallel-upload concurrency.
//!
//! Run:
//! ```bash
//! cargo run -p faucet-stream --example xml_to_s3 \
//!     --features "source-xml sink-s3"
//! ```

use faucet_stream::Pipeline;
use faucet_stream::sink::s3::{S3Sink, S3SinkConfig};
use faucet_stream::source::xml::{XmlAuth, XmlPagination, XmlStream, XmlStreamConfig};
use reqwest::header::{HeaderMap, HeaderValue};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", HeaderValue::from_static("application/xml"));
    headers.insert(
        "SOAPAction",
        HeaderValue::from_static("\"urn:GetInventory\""),
    );

    let body = r#"<?xml version="1.0"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <GetInventory><Region>us-east</Region></GetInventory>
  </soap:Body>
</soap:Envelope>"#;

    let source = XmlStream::new(
        XmlStreamConfig::new("https://soap.example.com", "/InventoryService")
            .method(reqwest::Method::POST)
            .auth(XmlAuth::Basic {
                username: std::env::var("SOAP_USER")?,
                password: std::env::var("SOAP_PASS")?,
            })
            .headers(headers)
            .body(body)
            .query_param("region", "us-east")
            .records_element_path("soap:Envelope.soap:Body.GetInventoryResponse.Items.Item")
            .pagination(XmlPagination::PageNumber {
                param_name: "page".into(),
                start_page: 1,
                page_size: Some(500),
                page_size_param: Some("size".into()),
                stop_when_short: false,
            })
            .max_pages(50),
    );

    let sink = S3Sink::new(
        S3SinkConfig::new("my-data-lake")
            .prefix("xml/inventory/")
            .region("us-east-1")
            .endpoint_url("https://s3.us-east-1.amazonaws.com")
            .file_extension(".jsonl")
            .max_records_per_file(5_000)
            .concurrency(8),
    )
    .await?;

    let result = Pipeline::new(&source, &sink).run().await?;
    println!(
        "uploaded {} items to s3://my-data-lake/",
        result.records_written
    );
    Ok(())
}

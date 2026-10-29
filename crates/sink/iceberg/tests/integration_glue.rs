//! Integration test: `IcebergSink` on the AWS Glue catalog, served with the
//! table files by one moto server (Glue + S3), #967.
//!
//! Requires Docker and the `catalog-glue` feature:
//!   cargo test -p faucet-sink-iceberg --all-features --test integration_glue

#![cfg(feature = "catalog-glue")]

mod catalog_support;

use catalog_support::{assert_round_trip, create_s3_bucket, s3_props};
use serde_json::json;
use testcontainers::GenericImage;
use testcontainers::core::{IntoContainerPort, WaitFor};

const MOTO_IMAGE: (&str, &str) = ("motoserver/moto", "5.1.4");
const KEY: &str = "testing";
const BUCKET: &str = "faucet-iceberg-glue";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn glue_catalog_creates_and_appends_to_a_table_in_s3() {
    let moto = faucet_conformance::containers::start(|| {
        GenericImage::new(MOTO_IMAGE.0, MOTO_IMAGE.1)
            .with_exposed_port(5000.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Running on"))
    })
    .await;
    let port = moto.get_host_port_ipv4(5000).await.expect("moto port");
    let endpoint = format!("http://127.0.0.1:{port}");
    create_s3_bucket(&endpoint, KEY, KEY, BUCKET).await;

    let mut properties = s3_props(&endpoint, KEY, KEY);
    properties["aws_access_key_id"] = json!(KEY);
    properties["aws_secret_access_key"] = json!(KEY);
    properties["region_name"] = json!("us-east-1");
    let warehouse = format!("s3://{BUCKET}/warehouse");
    let catalog = json!({
        "type": "glue",
        "uri": endpoint,
        "warehouse": warehouse,
        "properties": properties
    });
    assert_round_trip(catalog, "events", &warehouse).await;
}

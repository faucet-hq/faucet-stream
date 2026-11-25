//! Iceberg sink and source on a REST catalog that vends storage credentials
//! (#1110, #1108): pages the sink commits in one run are read back by the
//! source with the credentials the catalog hands out, and one catalog block
//! configures both.

#[path = "../../sink/iceberg/tests/vending_support/mod.rs"]
mod vending_support;

use faucet_core::{Sink, Source};
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use serde_json::json;
use vending_support::{Fakes, Vend};

#[tokio::test(flavor = "multi_thread")]
async fn pages_the_sink_commits_are_read_back_on_vended_credentials() {
    let fakes = Fakes::start(Vend::Always).await;
    let sink: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": fakes.catalog(json!({})), "namespace": ["ns"], "table": "t"
    }))
    .unwrap();
    let sink = IcebergSink::new(sink).await.unwrap();
    for n in 1..=2 {
        sink.write_batch(&[json!({"id": n})]).await.unwrap();
        sink.flush().await.unwrap();
    }
    fakes.expire_issued();
    let source: IcebergSourceConfig = serde_json::from_value(json!({
        "catalog": fakes.catalog(json!({})), "table": "ns.t"
    }))
    .unwrap();
    let mut ids: Vec<i64> = IcebergSource::new(source)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap()
        .iter()
        .map(|r| r["id"].as_i64().unwrap())
        .collect();
    ids.sort();
    assert_eq!(ids, [1, 2]);
    assert!(fakes.s3_calls().iter().all(|c| c.status < 400));
}

#[test]
fn one_catalog_block_configures_the_sink_and_the_source() {
    let catalog = json!({
        "type": "rest",
        "uri": "http://catalog:8181",
        "warehouse": "lake",
        "access_delegation": "vended_credentials",
        "properties": {"s3.endpoint": "http://store:9000"}
    });
    let sink: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": catalog, "namespace": ["ns"], "table": "t"
    }))
    .unwrap();
    sink.validate().unwrap();
    let source: IcebergSourceConfig =
        serde_json::from_value(json!({"catalog": catalog, "table": "ns.t"})).unwrap();
    source.validate().unwrap();
    assert_eq!(
        source.catalog.inner().access_delegation,
        Some(faucet_common_iceberg::AccessDelegation::VendedCredentials)
    );
    assert_eq!(
        serde_json::to_value(&sink.catalog).unwrap(),
        serde_json::to_value(&source.catalog).unwrap()
    );
}

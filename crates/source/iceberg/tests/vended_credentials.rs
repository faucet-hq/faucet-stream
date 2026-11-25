//! The source against a REST catalog that vends storage credentials (#1108):
//! the storage is checked before the first page and by `faucet doctor`.
#![cfg(feature = "catalog-rest")]

#[path = "../../../sink/iceberg/tests/vending_support/mod.rs"]
mod vending_support;

use faucet_core::Source;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use serde_json::{Value, json};
use vending_support::{Fakes, Vend};

async fn source(catalog: Value) -> IcebergSource {
    let config: IcebergSourceConfig =
        serde_json::from_value(json!({"catalog": catalog, "table": "ns.t"})).unwrap();
    IcebergSource::new(config).await.unwrap()
}

async fn storage_probe(source: &IcebergSource) -> ProbeStatus {
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "{report:?}"
    );
    assert_eq!(report.probes[1].name, "storage");
    report.probes[1].status.clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn vended_credentials_pass_the_check_and_the_read() {
    let fakes = Fakes::start(Vend::Always).await;
    fakes.create_table("ns", "t");
    let source = source(fakes.catalog(json!({}))).await;
    assert!(matches!(storage_probe(&source).await, ProbeStatus::Pass));
    assert!(source.fetch_all().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_or_mixed_credentials_fail_the_check_and_the_read() {
    for (vend, extra, says) in [
        (Vend::Never, json!({}), "no usable storage credentials"),
        (
            Vend::Always,
            Fakes::static_keys(),
            "Remove the static keys from catalog.properties",
        ),
    ] {
        let fakes = Fakes::start(vend).await;
        fakes.create_table("ns", "t");
        let source = source(fakes.catalog(extra)).await;
        match storage_probe(&source).await {
            ProbeStatus::Fail { reason } => assert!(reason.contains(says), "{reason}"),
            other => panic!("{other:?}"),
        }
        let err = source.fetch_all().await.unwrap_err().to_string();
        assert!(err.contains(says), "{err}");
    }
}

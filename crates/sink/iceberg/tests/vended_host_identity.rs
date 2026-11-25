//! A run confined to the credentials its config names (a caller of `faucet
//! serve`) never signs with the host's own identity, neither in the storage
//! check nor in the write. Its own test binary: it sets AWS variables.
#![cfg(feature = "catalog-rest")]

mod vending_support;

use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use serde_json::{Value, json};
use vending_support::{Fakes, Vend};

const HOST_KEY: &str = "HOSTKEY";

fn config(catalog: Value) -> IcebergSinkConfig {
    serde_json::from_value(json!({"catalog": catalog, "namespace": ["ns"], "table": "t"})).unwrap()
}

async fn write_two_pages(catalog: Value) -> Result<(), faucet_core::FaucetError> {
    let sink = IcebergSink::new(config(catalog)).await?;
    for n in 1..=2 {
        sink.write_batch(&[json!({"id": n})]).await?;
        sink.flush().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
#[allow(unsafe_code)]
async fn a_caller_run_never_uses_the_host_identity() {
    let host = Fakes::start(Vend::Never).await;
    let caller = Fakes::start(Vend::Never).await;
    let vending = Fakes::start(Vend::Always).await;
    // SAFETY: the only test of this binary; set after the fakes cleared AWS_*.
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", HOST_KEY);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "HOSTSECRET");
    }
    for fakes in [&host, &caller, &vending] {
        fakes.accept_static_key(HOST_KEY);
    }

    write_two_pages(host.catalog_using_the_host())
        .await
        .unwrap();
    assert!(
        host.uploads()
            .iter()
            .all(|c| c.access_key.as_deref() == Some(HOST_KEY)),
        "a trusted run signs with the host's keys: {:?}",
        host.uploads()
    );

    faucet_core::resolve::with_host_credentials(false, async {
        let err = write_two_pages(caller.catalog_using_the_host())
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("no usable storage credentials"),
            "{err}"
        );
        write_two_pages(vending.catalog_using_the_host())
            .await
            .unwrap();
    })
    .await;
    assert!(caller.s3_calls().is_empty(), "{:?}", caller.s3_calls());
    assert_eq!(vending.snapshots("ns.t"), 2);
    let calls = vending.s3_calls();
    assert!(
        calls
            .iter()
            .all(|c| c.access_key.as_deref() != Some(HOST_KEY) && c.generation.is_some()),
        "{calls:?}"
    );
}

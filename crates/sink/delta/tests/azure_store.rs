//! The Delta sink on Azurite through `az://` and `abfss://` table URIs with
//! `DeltaCredentials::Azure` (account name + access key), #967.
//!
//! Azurite ignores the `startFrom` listing parameter, which Delta's log
//! listing relies on; `object_store` filters client-side only in emulator
//! mode, whose blob URL comes from `AZURITE_BLOB_STORAGE_URL`. That variable
//! is process-wide, so this binary holds a single test.
//!
//! Requires Docker:
//!   cargo test -p faucet-sink-delta --all-features --test azure_store

#![cfg(feature = "azure")]

mod store_support;

use faucet_conformance::stores;
use serde_json::{Value, json};
use store_support::{assert_round_trip, sink_config};

fn credentials() -> Value {
    json!({ "type": "azure", "config": {
        "account_name": stores::AZURITE_ACCOUNT,
        "access_key": stores::AZURITE_KEY
    }})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn az_and_abfss_tables_on_azurite_with_azure_credentials() {
    let azurite = stores::azurite().await;
    let blob_url = azurite
        .endpoint
        .strip_suffix(&format!("/{}", stores::AZURITE_ACCOUNT))
        .expect("account-qualified endpoint")
        .to_string();
    // SAFETY: the only test in this binary; set before any Azure client is built.
    unsafe { std::env::set_var("AZURITE_BLOB_STORAGE_URL", &blob_url) };
    for container in ["delta-az", "delta-abfss"] {
        faucet_common_azure::testing::create_container(
            &azurite.endpoint,
            stores::AZURITE_ACCOUNT,
            stores::AZURITE_KEY,
            container,
        )
        .await
        .expect("create azurite container");
    }
    let options = json!({ "azure_storage_use_emulator": "true" });

    assert_round_trip(sink_config(
        "az://delta-az/tables/events",
        credentials(),
        options.clone(),
    ))
    .await;
    assert_round_trip(sink_config(
        &format!(
            "abfss://delta-abfss@{}.dfs.core.windows.net/tables/events",
            stores::AZURITE_ACCOUNT
        ),
        credentials(),
        options,
    ))
    .await;
}

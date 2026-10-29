//! The Delta source on Azurite through `az://` and `abfss://` table URIs with
//! `DeltaCredentials::Azure` (account name + access key), #967.
//!
//! Each test starts its own Azurite and reaches it through
//! `azure_storage_endpoint`, as a real account would be, so Delta's log
//! listing sends `startFrom` to the server (Azurite honours it since 3.37.0,
//! #984).
//!
//! Requires Docker:
//!   cargo test -p faucet-source-delta --all-features --test azure_store

#![cfg(feature = "azure")]

mod store_support;

use faucet_conformance::stores;
use serde_json::{Value, json};
use store_support::{assert_round_trip, connection};

fn credentials() -> Value {
    json!({ "type": "azure", "config": {
        "account_name": stores::AZURITE_ACCOUNT,
        "access_key": stores::AZURITE_KEY
    }})
}

/// Start Azurite, create `container`, and return the store with the
/// `storage_options` that point Delta at it.
async fn azurite_with(container: &str) -> (stores::Store, Value) {
    let azurite = stores::azurite().await;
    faucet_common_azure::testing::create_container(
        &azurite.endpoint,
        stores::AZURITE_ACCOUNT,
        stores::AZURITE_KEY,
        container,
    )
    .await
    .expect("create azurite container");
    let options = json!({
        "azure_storage_endpoint": azurite.endpoint,
        "azure_allow_http": "true",
    });
    (azurite, options)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn az_table_on_azurite_with_azure_credentials() {
    let (_azurite, options) = azurite_with("delta-src-az").await;
    assert_round_trip(connection(
        "az://delta-src-az/tables/events",
        credentials(),
        options,
    ))
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abfss_table_on_azurite_with_azure_credentials() {
    let (_azurite, options) = azurite_with("delta-src-abfss").await;
    assert_round_trip(connection(
        &format!(
            "abfss://delta-src-abfss@{}.dfs.core.windows.net/tables/events",
            stores::AZURITE_ACCOUNT
        ),
        credentials(),
        options,
    ))
    .await;
}

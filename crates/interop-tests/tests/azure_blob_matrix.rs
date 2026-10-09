//! The shared format × option matrix (#777) against Azurite, through the
//! Azure Blob sink. Requires Docker.
#![cfg(all(
    not(target_os = "windows"),
    feature = "file-formats",
    feature = "arrow",
    feature = "compression",
    feature = "encryption"
))]

mod remote_matrix;

use std::sync::Arc;

use faucet_core::{FaucetError, Sink};
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use remote_matrix::{BoxFut, Remote, run_matrix};
use serde_json::{Value, json};
use testcontainers_modules::azurite::{Azurite, BLOB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;

const ACCOUNT: &str = "devstoreaccount1";
const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "faucet-matrix";

struct AzureRemote {
    endpoint: String,
    store: Arc<dyn ObjectStore>,
    _c: ContainerAsync<Azurite>,
}

impl Remote for AzureRemote {
    fn config(&self, prefix: &str, mut fields: Value) -> Value {
        fields["container"] = CONTAINER.into();
        fields["account"] = ACCOUNT.into();
        fields["auth"] = json!({"type": "account_key", "config": {"account_key": KEY}});
        fields["endpoint"] = self.endpoint.clone().into();
        fields["allow_http"] = true.into();
        fields["prefix"] = prefix.into();
        fields
    }

    fn sink(&self, cfg: Value) -> BoxFut<'_, Result<Box<dyn Sink>, FaucetError>> {
        Box::pin(async move {
            let cfg = serde_json::from_value(cfg).map_err(FaucetError::Json)?;
            Ok(Box::new(faucet_sink_azure_blob::AzureBlobSink::new(cfg).await?) as Box<dyn Sink>)
        })
    }

    fn objects(&self, prefix: &str) -> BoxFut<'_, Vec<(String, Vec<u8>)>> {
        let prefix = prefix.to_string();
        Box::pin(async move {
            let root = ObjPath::from(prefix.trim_end_matches('/'));
            let mut listing = self.store.list(Some(&root));
            let mut keys = Vec::new();
            while let Some(meta) = listing.next().await {
                keys.push(meta.expect("list").location);
            }
            let mut out = Vec::new();
            for key in keys {
                let body = self
                    .store
                    .get(&key)
                    .await
                    .expect("get")
                    .bytes()
                    .await
                    .expect("body");
                out.push((key.as_ref()[prefix.len()..].to_string(), body.to_vec()));
            }
            out
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn every_writable_format_takes_every_option_on_azure_blob() {
    let container = faucet_conformance::containers::start(|| {
        // In-memory storage: a nearly full runner disk must not fail the emulator.
        testcontainers::ImageExt::with_cmd(
            Azurite::default(),
            [
                "azurite",
                "--blobHost",
                "0.0.0.0",
                "--queueHost",
                "0.0.0.0",
                "--tableHost",
                "0.0.0.0",
                "--inMemoryPersistence",
            ],
        )
    })
    .await;
    let port = container.get_host_port_ipv4(BLOB_PORT).await.expect("port");
    {
        use azure_storage::{CloudLocation, prelude::*};
        use azure_storage_blobs::prelude::*;
        ClientBuilder::with_location(
            CloudLocation::Emulator {
                address: "127.0.0.1".to_owned(),
                port,
            },
            StorageCredentials::emulator(),
        )
        .container_client(CONTAINER)
        .create()
        .await
        .expect("create container");
    }
    let endpoint = format!("http://127.0.0.1:{port}/{ACCOUNT}");
    let store: Arc<dyn ObjectStore> = Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(ACCOUNT)
            .with_access_key(KEY)
            .with_container_name(CONTAINER)
            .with_endpoint(endpoint.clone())
            .with_allow_http(true)
            .build()
            .expect("verifying store"),
    );
    run_matrix(
        AzureRemote {
            endpoint,
            store,
            _c: container,
        },
        true,
    )
    .await;
}

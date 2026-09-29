//! `format: parquet` against Azurite (#777): pages join one object until the
//! row cap or a flush, later pages widen it, the columnar path writes the
//! same objects, and the `parquet` options land in the uploaded blobs.
#![cfg(all(not(target_os = "windows"), feature = "arrow"))]

use std::sync::Arc;

use faucet_core::file_format::parquet_io::read_bytes;
use faucet_core::{ParquetReadOptions, ParquetWriteOptions, Sink};
use faucet_sink_azure_blob::{
    AzureBlobSink, AzureBlobSinkConfig, AzureCredentials, AzureSinkFormat,
};
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::{Value, json};
use testcontainers_modules::azurite::{Azurite, BLOB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "faucet-parquet-test";

async fn start_azurite() -> (ContainerAsync<Azurite>, u16) {
    let container = Azurite::default()
        .start()
        .await
        .expect("start azurite container");
    let port = container
        .get_host_port_ipv4(BLOB_PORT)
        .await
        .expect("azurite blob host port");
    (container, port)
}

fn endpoint(port: u16) -> String {
    format!("http://127.0.0.1:{port}/{AZURITE_ACCOUNT}")
}

async fn create_container(port: u16) {
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
    .expect("create azurite blob container");
}

fn verify_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(AZURITE_ACCOUNT)
            .with_access_key(AZURITE_KEY)
            .with_container_name(CONTAINER)
            .with_endpoint(endpoint(port))
            .with_allow_http(true)
            .build()
            .expect("build verifying object store"),
    )
}

fn sink_config(port: u16) -> AzureBlobSinkConfig {
    AzureBlobSinkConfig::new(CONTAINER)
        .account(AZURITE_ACCOUNT)
        .auth(AzureCredentials::AccountKey {
            account_key: AZURITE_KEY.into(),
        })
        .endpoint(endpoint(port))
        .allow_http(true)
}

async fn blobs(store: &Arc<dyn ObjectStore>, prefix: &str) -> Vec<Vec<u8>> {
    let mut listing = store.list(Some(&ObjPath::from(prefix)));
    let mut out = Vec::new();
    while let Some(meta) = listing.next().await {
        let meta = meta.expect("list");
        let got = store.get(&meta.location).await.expect("get");
        out.push(got.bytes().await.expect("bytes").to_vec());
    }
    out
}

fn decode(bytes: &[u8]) -> (arrow::datatypes::SchemaRef, Vec<Value>) {
    let (schema, batches) = read_bytes(
        bytes::Bytes::copy_from_slice(bytes),
        &ParquetReadOptions::default(),
        0,
        "blob",
    )
    .expect("decode");
    let rows = batches
        .iter()
        .flat_map(|b| faucet_core::columnar::record_batch_to_values(b).unwrap())
        .collect();
    (schema, rows)
}

#[tokio::test]
async fn parquet_row_and_columnar_paths_roll_widen_and_flush() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = verify_store(port);
    let cfg = sink_config(port)
        .prefix("pq/")
        .file_extension(".parquet")
        .format(AzureSinkFormat::Parquet)
        .with_batch_size(0)
        .max_records_per_file(4);
    let sink = AzureBlobSink::new(cfg).await.expect("sink");
    assert!(sink.supports_columnar());
    sink.write_batch(&[json!({"id": 1}), json!({"id": 2})])
        .await
        .unwrap();
    sink.write_batch(&[json!({"id": 3, "name": "c"})])
        .await
        .unwrap();
    assert!(
        blobs(&store, "pq/").await.is_empty(),
        "the open object waits for the cap"
    );
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&[
        json!({"id": 4, "name": "d"}),
        json!({"id": 5, "name": "e"}),
    ])
    .unwrap();
    sink.write_batch_columnar(&batch).await.unwrap();
    assert_eq!(blobs(&store, "pq/").await.len(), 1, "closed on the row cap");
    sink.flush().await.unwrap();

    let mut all: Vec<Value> = Vec::new();
    for b in blobs(&store, "pq/").await {
        assert_eq!(&b[..4], b"PAR1");
        let (schema, rows) = decode(&b);
        assert!(schema.field_with_name("name").is_ok());
        all.extend(rows);
    }
    all.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(all.len(), 5);
    assert_eq!(all[0], json!({"id": 1, "name": null}));
    assert_eq!(all[4], json!({"id": 5, "name": "e"}));
}

#[tokio::test]
async fn parquet_options_reach_the_blob() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    let store = verify_store(port);
    let opts: ParquetWriteOptions = serde_json::from_value(json!({
        "compression": "lz4", "row_group_size": 2, "on_unknown_field": "error",
        "schema": {"type": "explicit", "fields": [{"name": "id", "type": "int64"}]}
    }))
    .unwrap();
    let cfg = sink_config(port)
        .prefix("opts/")
        .format(AzureSinkFormat::Parquet)
        .parquet(opts);
    let sink = AzureBlobSink::new(cfg).await.expect("sink");
    let rows: Vec<Value> = (0..5).map(|i| json!({"id": i})).collect();
    sink.write_batch(&rows).await.unwrap();
    let err = sink
        .write_batch(&[json!({"id": 9, "x": 1})])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("`x`"), "{err}");
    sink.flush().await.unwrap();
    let b = blobs(&store, "opts/").await;
    assert_eq!(b.len(), 1);
    let (_, got) = decode(&b[0]);
    assert_eq!(got, rows);
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let meta = SerializedFileReader::new(bytes::Bytes::from(b[0].clone())).unwrap();
    assert_eq!(meta.metadata().num_row_groups(), 3);
    assert_eq!(
        meta.metadata().row_group(0).column(0).compression(),
        parquet::basic::Compression::LZ4_RAW
    );
}

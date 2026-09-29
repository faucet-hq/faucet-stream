//! `file_format: parquet` against Azurite (#777): blobs decode on the row and
//! the columnar path, `parquet.columns` projects, and an unknown column or a
//! schema that changes between blobs is an error naming the blob.
#![cfg(all(not(target_os = "windows"), feature = "arrow"))]

use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::{ParquetReadOptions, Source};
use faucet_source_azure_blob::{
    AzureBlobSource, AzureBlobSourceConfig, AzureCredentials, AzureFileFormat,
};
use futures::StreamExt;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use serde_json::{Value, json};
use testcontainers_modules::azurite::{Azurite, BLOB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const AZURITE_ACCOUNT: &str = "devstoreaccount1";
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const CONTAINER: &str = "faucet-parquet-src";

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

fn seed_store(port: u16) -> Arc<dyn ObjectStore> {
    Arc::new(
        MicrosoftAzureBuilder::new()
            .with_account(AZURITE_ACCOUNT)
            .with_access_key(AZURITE_KEY)
            .with_container_name(CONTAINER)
            .with_endpoint(endpoint(port))
            .with_allow_http(true)
            .build()
            .expect("build seeding object store"),
    )
}

fn source_config(port: u16) -> AzureBlobSourceConfig {
    AzureBlobSourceConfig::new(CONTAINER)
        .account(AZURITE_ACCOUNT)
        .auth(AzureCredentials::AccountKey {
            account_key: AZURITE_KEY.into(),
        })
        .endpoint(endpoint(port))
        .allow_http(true)
}

fn parquet(rows: &[Value]) -> Vec<u8> {
    let batch = faucet_core::columnar::values_to_record_batch_inferred(rows).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut buf = Vec::new();
    let mut w =
        parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    buf
}

async fn seed(port: u16, key: &str, body: Vec<u8>) {
    seed_store(port)
        .put(&ObjPath::from(key), PutPayload::from(body))
        .await
        .expect("seed blob");
}

fn rows(from: i64, n: i64) -> Vec<Value> {
    (from..from + n)
        .map(|i| json!({"id": i, "name": format!("n{i}")}))
        .collect()
}

#[tokio::test]
async fn parquet_blobs_read_on_the_row_and_columnar_paths_with_projection() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    seed(port, "pq/a.parquet", parquet(&rows(0, 3))).await;
    seed(port, "pq/b.parquet", parquet(&rows(3, 2))).await;

    let cfg = source_config(port)
        .prefix("pq/")
        .file_format(AzureFileFormat::Parquet);
    let src = AzureBlobSource::new(cfg.clone()).await.expect("source");
    assert!(src.supports_columnar());
    let mut got = src.fetch_all().await.expect("fetch_all");
    got.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(got, rows(0, 5));

    let mut projected = cfg.clone().with_batch_size(2);
    projected.parquet = ParquetReadOptions {
        columns: Some(vec!["name".into()]),
    };
    let src = AzureBlobSource::new(projected).await.expect("source");
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 2);
    let mut names = Vec::new();
    while let Some(p) = pages.next().await {
        for r in p.expect("page").records {
            assert_eq!(r.as_object().unwrap().len(), 1);
            names.push(r["name"].clone());
        }
    }
    assert_eq!(names.len(), 5);
    let mut batches = src.stream_batches(&ctx, 2);
    let mut total = 0;
    while let Some(b) = batches.next().await {
        let b = b.expect("batch");
        assert_eq!(b.batch.num_columns(), 1);
        assert!(b.batch.num_rows() <= 2);
        total += b.num_rows();
    }
    assert_eq!(total, 5);

    let mut bad = cfg;
    bad.parquet = ParquetReadOptions {
        columns: Some(vec!["nope".into()]),
    };
    let err = AzureBlobSource::new(bad)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("`nope`") && err.to_string().contains("pq/a.parquet"),
        "{err}"
    );
}

#[tokio::test]
async fn a_schema_change_between_parquet_blobs_fails_the_columnar_read() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    seed(port, "mix/a.parquet", parquet(&rows(0, 2))).await;
    seed(port, "mix/b.parquet", parquet(&[json!({"other": true})])).await;
    let cfg = source_config(port)
        .prefix("mix/")
        .file_format(AzureFileFormat::Parquet);
    let src = AzureBlobSource::new(cfg).await.expect("source");
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = src.stream_batches(&ctx, 0);
    let mut err = None;
    while let Some(b) = batches.next().await {
        if let Err(e) = b {
            err = Some(e.to_string());
        }
    }
    let err = err.expect("the second blob is refused");
    assert!(err.contains("mix/b.parquet"), "{err}");
}

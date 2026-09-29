//! Read the shared file formats out of a real Azure Blob endpoint (Azurite,
//! #604).
//!
//! `fetch_decoded` is the whole-object path: CSV, XML and Excel are read in
//! one piece and decoded through `faucet_core::file_format` before the page
//! loop chunks them. A unit test of the decoder says nothing about whether
//! the blob body reaches it.
#![cfg(all(not(target_os = "windows"), feature = "file-format-csv"))]

use std::collections::HashMap;
use std::sync::Arc;

use faucet_core::Source;
use faucet_core::file_format::{FileFormat, FormatOptions, encode};
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
const CONTAINER: &str = "faucet-format-src";

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

async fn drain(src: &AzureBlobSource) -> Vec<Value> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        out.extend(page.expect("page").records);
    }
    out
}

fn records() -> Vec<Value> {
    vec![
        json!({"id": "1", "name": "ada"}),
        json!({"id": "2", "name": "grace"}),
    ]
}

/// Seed one blob encoded with the shared writer — the bytes a faucet sink
/// would have produced — then read it back through the source.
async fn round_trip(fmt: AzureFileFormat, shared: FileFormat, key: &str) {
    let body = encode(&records(), shared, &FormatOptions::default()).expect("encode fixture");
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    seed_store(port)
        .put(&ObjPath::from(key), PutPayload::from(body))
        .await
        .expect("seed blob");

    let cfg = source_config(port).file_format(fmt).with_batch_size(0);
    let src = AzureBlobSource::new(cfg).await.expect("source");
    assert_eq!(drain(&src).await, records(), "{shared:?} did not read back");
    assert_eq!(
        src.fetch_all().await.expect("fetch_all"),
        records(),
        "{shared:?} did not read back through fetch_all"
    );
}

#[tokio::test]
async fn csv_blobs_are_decoded() {
    round_trip(AzureFileFormat::Csv, FileFormat::Csv, "rows.csv").await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test]
async fn xml_blobs_are_decoded() {
    round_trip(AzureFileFormat::Xml, FileFormat::Xml, "rows.xml").await;
}

#[cfg(feature = "file-format-excel")]
#[tokio::test]
async fn xlsx_blobs_are_decoded() {
    round_trip(AzureFileFormat::Xlsx, FileFormat::Xlsx, "rows.xlsx").await;
}

/// A non-default dialect has to reach the decoder, or every row parses as a
/// single column and the run still looks successful.
#[tokio::test]
async fn the_configured_csv_dialect_is_honoured() {
    let (_c, port) = start_azurite().await;
    create_container(port).await;
    seed_store(port)
        .put(
            &ObjPath::from("semis.csv"),
            PutPayload::from(b"id;name\n1;ada\n".to_vec()),
        )
        .await
        .expect("seed blob");

    let mut cfg = source_config(port)
        .file_format(AzureFileFormat::Csv)
        .with_batch_size(0);
    cfg.csv = faucet_core::CsvOptions {
        delimiter: ";".into(),
        has_headers: true,
    };
    let src = AzureBlobSource::new(cfg).await.expect("source");
    assert_eq!(drain(&src).await, vec![json!({"id": "1", "name": "ada"})]);
}

/// Avro and ORC blobs (#719): resolved against the first blob's schema on the
/// row path and the columnar path alike.
#[cfg(all(feature = "file-format-avro", feature = "file-format-orc"))]
mod containers {
    use super::*;
    use faucet_core::{AvroCodec, AvroOptions, OrcOptions};

    const ORC: &[u8] = include_bytes!("../../../core/tests/fixtures/orc/people.orc");

    fn avro(records: &[Value]) -> Vec<u8> {
        faucet_core::file_format::avro::encode(
            records,
            &AvroOptions {
                schema: None,
                codec: AvroCodec::Deflate,
            },
        )
        .expect("encode avro")
    }

    async fn put(port: u16, key: &str, body: Vec<u8>) {
        seed_store(port)
            .put(&ObjPath::from(key), PutPayload::from(body))
            .await
            .expect("seed blob");
    }

    async fn columnar_rows(src: &AzureBlobSource) -> Result<usize, String> {
        let ctx = HashMap::new();
        assert!(src.supports_columnar());
        let mut batches = src.stream_batches(&ctx, 0);
        let mut n = 0;
        while let Some(page) = batches.next().await {
            n += page.map_err(|e| e.to_string())?.num_rows();
        }
        Ok(n)
    }

    #[tokio::test]
    async fn avro_and_orc_blobs_decode_on_both_paths() {
        let (_c, port) = start_azurite().await;
        create_container(port).await;
        put(
            port,
            "avro/a.avro",
            avro(&[json!({"id": 1}), json!({"id": 2})]),
        )
        .await;
        put(port, "avro/b.avro", avro(&[json!({"id": 3, "more": "x"})])).await;
        put(port, "bad/a.avro", avro(&[json!({"id": 1})])).await;
        put(port, "bad/b.avro", avro(&[json!({"id": "text"})])).await;
        put(port, "orc/p.orc", ORC.to_vec()).await;

        let cfg = source_config(port)
            .prefix("avro/")
            .file_format(AzureFileFormat::Avro)
            .with_batch_size(0);
        let src = AzureBlobSource::new(cfg).await.expect("source");
        let want = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
        assert_eq!(drain(&src).await, want);
        assert_eq!(
            src.fetch_with_context(&HashMap::new())
                .await
                .expect("fetch"),
            want
        );
        assert_eq!(columnar_rows(&src).await, Ok(3));

        let bad = AzureBlobSource::new(
            source_config(port)
                .prefix("bad/")
                .file_format(AzureFileFormat::Avro),
        )
        .await
        .expect("source");
        let err = columnar_rows(&bad).await.expect_err("conflict");
        assert!(
            err.contains("bad/a.avro") && err.contains("bad/b.avro"),
            "{err}"
        );

        let mut cfg = source_config(port)
            .prefix("orc/")
            .file_format(AzureFileFormat::Orc);
        cfg.orc = OrcOptions {
            columns: Some(vec!["name".into()]),
        };
        let src = AzureBlobSource::new(cfg).await.expect("source");
        assert_eq!(drain(&src).await[0], json!({"name": "ada"}));
        assert_eq!(columnar_rows(&src).await, Ok(3));

        let csv = AzureBlobSource::new(source_config(port).file_format(AzureFileFormat::Csv))
            .await
            .expect("source");
        assert!(!csv.supports_columnar());
        assert!(columnar_rows_unchecked(&csv).await.is_err());
    }

    async fn columnar_rows_unchecked(src: &AzureBlobSource) -> Result<(), String> {
        let ctx = HashMap::new();
        let mut batches = src.stream_batches(&ctx, 0);
        while let Some(page) = batches.next().await {
            page.map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}

//! Read the shared file formats off a real SFTP server (#604).
//!
//! `fetch_decoded` is the whole-file path: CSV, XML and Excel are read in one
//! piece and decoded through `faucet_core::file_format` before the page loop
//! chunks them. Only an integration test shows that the bytes on the remote
//! actually reach that decoder — a unit test of the decoder proves nothing
//! about the transfer.
#![cfg(all(not(target_os = "windows"), feature = "file-format-csv"))]

use std::collections::HashMap;

use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::Source;
use faucet_core::file_format::{FileFormat, FormatOptions, encode};
use faucet_source_sftp::{SftpFormat, SftpSource, SftpSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const USER: &str = "faucet";
const PASS: &str = "secret";

/// Seed the server with `files` (name → raw bytes, so binary formats work).
async fn start_sftp(files: &[(String, Vec<u8>)]) -> Option<(ContainerAsync<GenericImage>, u16)> {
    let mut image = GenericImage::new("atmoz/sftp", "alpine")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_cmd(vec![format!("{USER}:{PASS}:::data")]);
    for (name, body) in files {
        image = image.with_copy_to(format!("/home/{USER}/data/{name}"), body.clone());
    }
    let container = match image.start().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            return None;
        }
    };
    let port = container.get_host_port_ipv4(22).await.ok()?;
    Some((container, port))
}

fn source(port: u16, format: SftpFormat, glob: &str) -> SftpSource {
    let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port);
    SftpSource::new(
        SftpSourceConfig::new(conn, "/data")
            .format(format)
            .glob(glob)
            .with_batch_size(0),
    )
    .expect("config is valid")
}

async fn drain(src: &SftpSource) -> Vec<Value> {
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

/// Seed one file encoded with the shared writer, then read it back through
/// the source — the same bytes a faucet sink would have produced.
async fn round_trip(fmt: SftpFormat, shared: FileFormat, name: &str) {
    let body = encode(&records(), shared, &FormatOptions::default()).expect("encode fixture");
    let Some((_c, port)) = start_sftp(&[(name.to_string(), body)]).await else {
        return;
    };
    let got = drain(&source(port, fmt, name)).await;
    assert_eq!(got, records(), "{shared:?} did not read back off SFTP");
}

#[tokio::test]
async fn csv_files_are_decoded() {
    round_trip(SftpFormat::Csv, FileFormat::Csv, "rows.csv").await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test]
async fn xml_files_are_decoded() {
    round_trip(SftpFormat::Xml, FileFormat::Xml, "rows.xml").await;
}

#[cfg(feature = "file-format-excel")]
#[tokio::test]
async fn xlsx_files_are_decoded() {
    round_trip(SftpFormat::Xlsx, FileFormat::Xlsx, "rows.xlsx").await;
}

/// A dialect that is not the default has to reach the decoder, or the file
/// silently parses as one column per row.
#[tokio::test]
async fn the_configured_csv_dialect_is_honoured() {
    let Some((_c, port)) =
        start_sftp(&[("semis.csv".to_string(), b"id;name\n1;ada\n".to_vec())]).await
    else {
        return;
    };
    let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port);
    let mut cfg = SftpSourceConfig::new(conn, "/data")
        .format(SftpFormat::Csv)
        .glob("semis.csv")
        .with_batch_size(0);
    cfg.csv = faucet_core::CsvOptions {
        delimiter: ";".into(),
        has_headers: true,
    };
    let src = SftpSource::new(cfg).expect("config is valid");
    assert_eq!(drain(&src).await, vec![json!({"id": "1", "name": "ada"})]);
}

/// A malformed file is a typed error naming the path, not a panic.
#[tokio::test]
async fn an_unreadable_workbook_is_a_typed_error() {
    let Some((_c, port)) =
        start_sftp(&[("bad.xlsx".to_string(), b"not a workbook".to_vec())]).await
    else {
        return;
    };
    let src = source(port, SftpFormat::Xlsx, "bad.xlsx");
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let first = pages.next().await.expect("one page");
    let err = first.expect_err("a non-workbook must not decode");
    assert!(err.to_string().contains("bad.xlsx"), "{err}");
}

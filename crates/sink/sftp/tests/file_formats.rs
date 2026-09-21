//! Write the shared file formats to a real SFTP server (#604) and read the
//! bytes back off the remote.
//!
//! `write_encoded_file` is the whole-file path — every format except JSON
//! Lines buffers its records and encodes them together — and the file is
//! written to a temporary name then renamed into place, so a consumer never
//! sees a partial one. Only an integration test shows that what lands
//! decodes back to the records that went in.
#![cfg(all(not(target_os = "windows"), feature = "file-format-csv"))]

use faucet_common_sftp::{SftpConnectionConfig, connect};
use faucet_core::Sink;
use faucet_core::file_format::{FileFormat, FormatOptions, decode};
use faucet_sink_sftp::{SftpSink, SftpSinkConfig, SftpSinkFormat};
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::AsyncReadExt;

const USER: &str = "faucet";
const PASS: &str = "secret";

async fn start_sftp() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let image = GenericImage::new("atmoz/sftp", "alpine")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_cmd(vec![format!("{USER}:{PASS}:::data")]);
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

fn conn(port: u16) -> SftpConnectionConfig {
    SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port)
}

/// The sole file under `/data`, as raw bytes — read over a second session so
/// the assertion goes through the server, not the sink's own handle.
async fn sole_file(port: u16) -> Vec<u8> {
    let sftp = connect(&conn(port)).await.expect("verify session");
    let mut names: Vec<String> = sftp
        .read_dir("/data")
        .await
        .expect("read_dir")
        .map(|e| e.file_name())
        .filter(|n| n != "." && n != "..")
        .collect();
    names.sort();
    assert_eq!(names.len(), 1, "expected exactly one file, got {names:?}");
    let mut file = sftp
        .open(format!("/data/{}", names[0]))
        .await
        .expect("open written file");
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).await.expect("read file");
    buf
}

fn records() -> Vec<Value> {
    vec![
        json!({"id": "1", "name": "ada"}),
        json!({"id": "2", "name": "grace"}),
    ]
}

async fn round_trip(fmt: SftpSinkFormat, shared: FileFormat, ext: &str) {
    let Some((_c, port)) = start_sftp().await else {
        return;
    };
    let sink = SftpSink::new(
        SftpSinkConfig::new(conn(port), "/data")
            .format(fmt)
            .file_extension(ext),
    )
    .expect("sink");

    let expected = records();
    assert_eq!(
        sink.write_batch(&expected).await.expect("write"),
        expected.len()
    );
    // A whole-file format cannot know the group is complete before flush.
    sink.flush().await.expect("flush");

    let bytes = sole_file(port).await;
    let back = decode(&bytes, shared, &FormatOptions::default())
        .await
        .expect("decode the landed file");
    assert_eq!(back, expected, "{shared:?} did not round-trip over SFTP");
}

#[tokio::test]
async fn csv_files_round_trip() {
    round_trip(SftpSinkFormat::Csv, FileFormat::Csv, ".csv").await;
}

#[tokio::test]
async fn json_array_files_round_trip() {
    round_trip(SftpSinkFormat::JsonArray, FileFormat::JsonArray, ".json").await;
}

#[cfg(feature = "file-format-xml")]
#[tokio::test]
async fn xml_files_round_trip() {
    round_trip(SftpSinkFormat::Xml, FileFormat::Xml, ".xml").await;
}

#[cfg(feature = "file-format-excel")]
#[tokio::test]
async fn xlsx_files_round_trip() {
    round_trip(SftpSinkFormat::Xlsx, FileFormat::Xlsx, ".xlsx").await;
}

/// An empty page must not leave a stray zero-record file behind.
#[tokio::test]
async fn an_empty_write_leaves_no_file() {
    let Some((_c, port)) = start_sftp().await else {
        return;
    };
    let sink = SftpSink::new(
        SftpSinkConfig::new(conn(port), "/data")
            .format(SftpSinkFormat::Csv)
            .file_extension(".csv"),
    )
    .expect("sink");

    assert_eq!(sink.write_batch(&[]).await.expect("write"), 0);
    sink.flush().await.expect("flush");

    let sftp = connect(&conn(port)).await.expect("verify session");
    let names: Vec<String> = sftp
        .read_dir("/data")
        .await
        .expect("read_dir")
        .map(|e| e.file_name())
        .filter(|n| n != "." && n != "..")
        .collect();
    assert!(names.is_empty(), "no file for an empty page, got {names:?}");
}

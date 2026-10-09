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
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const USER: &str = "faucet";
const PASS: &str = "secret";

/// Seed the server with `files` (name → raw bytes, so binary formats work).
async fn start_sftp_inner(
    files: &[(String, Vec<u8>)],
) -> Option<(ContainerAsync<GenericImage>, u16)> {
    let container = faucet_conformance::containers::start_or_skip(
        || {
            let mut image = GenericImage::new("atmoz/sftp", "alpine")
                .with_exposed_port(22.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
                .with_cmd(vec![format!("{USER}:{PASS}:::data")]);
            for (name, body) in files {
                image = image.with_copy_to(format!("/home/{USER}/data/{name}"), body.clone());
            }
            image
        },
        &Default::default(),
    )
    .await?;
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
        ..Default::default()
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

/// Avro and ORC files (#719): resolved against the first file's schema on the
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
                codec: AvroCodec::Snappy,
            },
        )
        .expect("encode avro")
    }

    async fn columnar(src: &SftpSource) -> Result<usize, String> {
        let ctx = HashMap::new();
        let mut batches = src.stream_batches(&ctx, 0);
        let mut n = 0;
        while let Some(page) = batches.next().await {
            n += page.map_err(|e| e.to_string())?.num_rows();
        }
        Ok(n)
    }

    #[tokio::test]
    async fn avro_and_orc_files_decode_on_both_paths() {
        let files = vec![
            ("a.avro".to_string(), avro(&[json!({"id": 1})])),
            ("b.avro".to_string(), avro(&[json!({"id": 2})])),
            ("p.orc".to_string(), ORC.to_vec()),
        ];
        let Some((_c, port)) = start_sftp(&files).await else {
            return;
        };
        let src = source(port, SftpFormat::Avro, "*.avro");
        assert!(src.supports_columnar());
        assert_eq!(drain(&src).await, vec![json!({"id": 1}), json!({"id": 2})]);
        assert_eq!(
            src.fetch_with_context(&HashMap::new())
                .await
                .expect("fetch")
                .len(),
            2
        );
        assert_eq!(columnar(&src).await, Ok(2));

        let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port);
        let mut cfg = SftpSourceConfig::new(conn, "/data")
            .format(SftpFormat::Orc)
            .glob("*.orc");
        cfg.orc = OrcOptions {
            columns: Some(vec!["id".into()]),
        };
        let orc = SftpSource::new(cfg).expect("config");
        assert_eq!(drain(&orc).await[1], json!({"id": 2}));
        assert_eq!(columnar(&orc).await, Ok(3));

        let csv = source(port, SftpFormat::Csv, "*.avro");
        assert!(!csv.supports_columnar());
        assert!(columnar(&csv).await.is_err());
    }
}

/// `format: parquet` (#777): files decode on the row and columnar paths and
/// `parquet.columns` projects.
#[cfg(feature = "arrow")]
#[tokio::test]
async fn parquet_files_decode_and_project() {
    fn parquet(rows: &[Value]) -> Vec<u8> {
        let batch = faucet_core::columnar::values_to_record_batch_inferred(rows).unwrap();
        let mut buf = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        buf
    }
    let rows: Vec<Value> = (0..4)
        .map(|i| json!({"id": i, "name": format!("n{i}")}))
        .collect();
    let Some((_c, port)) = start_sftp(&[
        ("a.parquet".to_string(), parquet(&rows[..2])),
        ("b.parquet".to_string(), parquet(&rows[2..])),
    ])
    .await
    else {
        return;
    };
    let src = source(port, SftpFormat::Parquet, "*.parquet");
    assert!(src.supports_columnar());
    let mut got = drain(&src).await;
    got.sort_by_key(|r| r["id"].as_i64());
    assert_eq!(got, rows);

    let conn = SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port);
    let mut cfg = SftpSourceConfig::new(conn, "/data")
        .format(SftpFormat::Parquet)
        .glob("*.parquet");
    cfg.parquet.columns = Some(vec!["name".into()]);
    let src = SftpSource::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut batches = src.stream_batches(&ctx, 0);
    let mut total = 0;
    while let Some(b) = batches.next().await {
        let b = b.expect("batch");
        assert_eq!(b.batch.num_columns(), 1);
        total += b.num_rows();
    }
    assert_eq!(total, 4);
    let all = src.fetch_all().await.unwrap();
    assert!(all.iter().all(|r| r.as_object().unwrap().len() == 1));
}

/// [`start_sftp_inner`], failing instead of skipping when `FAUCET_REQUIRE_BACKENDS`
/// is set — CI provides Docker, so an unavailable backend is a failure there.
async fn start_sftp(files: &[(String, Vec<u8>)]) -> Option<(ContainerAsync<GenericImage>, u16)> {
    let started = start_sftp_inner(files).await;
    if started.is_none() {
        faucet_conformance::containers::backend_missing(
            "start_sftp: the test backend is unavailable",
        );
    }
    started
}

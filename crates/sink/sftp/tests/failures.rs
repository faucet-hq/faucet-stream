//! The SFTP sink's failure paths (#777) against an SFTP server: a listing,
//! download, delete or replace the server refuses fails the call that needed
//! it, naming the operation and the path. The last tests need no server.
//! The server tests require Docker.
#![cfg(not(target_os = "windows"))]

use faucet_common_sftp::{SftpConnectionConfig, SftpSession, connect};
use faucet_core::Sink;
use faucet_sink_sftp::{SftpSink, SftpSinkConfig};
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::AsyncWriteExt;

const USER: &str = "faucet";
const PASS: &str = "secret";

async fn server() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let image = GenericImage::new("atmoz/sftp", "alpine")
        .with_exposed_port(22.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
        .with_cmd(vec![format!("{USER}:{PASS}:::data")]);
    match image.start().await {
        Ok(c) => {
            let port = c.get_host_port_ipv4(22).await.expect("port");
            Some((c, port))
        }
        Err(e) => {
            eprintln!("Skipping: Docker not available ({e})");
            None
        }
    }
}

fn connection(port: u16) -> SftpConnectionConfig {
    SftpConnectionConfig::with_password("127.0.0.1", USER, PASS).port(port)
}

fn sink(port: u16, path: &str, fields: Value) -> SftpSink {
    let mut cfg = serde_json::to_value(SftpSinkConfig::new(connection(port), path)).unwrap();
    for (k, v) in fields.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    SftpSink::new(serde_json::from_value(cfg).unwrap()).expect("SftpSink::new")
}

async fn session(port: u16) -> SftpSession {
    connect(&connection(port)).await.expect("verify session")
}

async fn put(s: &SftpSession, path: &str) {
    let mut f = s.create(path).await.expect("create");
    f.write_all(b"x\n").await.unwrap();
    f.shutdown().await.unwrap();
}

async fn write_and_flush(s: &SftpSink, rows: &[Value]) -> Result<(), faucet_core::FaucetError> {
    s.write_batch(rows).await?;
    s.flush().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_server_calls_name_the_operation_and_the_path() {
    let Some((_c, port)) = server().await else {
        return;
    };
    let admin = session(port).await;

    put(&admin, "/data/f").await;
    let s = sink(
        port,
        "/data/f/",
        json!({"if_exists": "append", "file_name": "part-{part}.jsonl"}),
    );
    let e = write_and_flush(&s, &[json!({"a": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("SFTP create directory '/data/f' failed"), "{e}");

    let s = sink(port, "/data/one/", json!({"file_name": "one.jsonl"}));
    write_and_flush(&s, &[json!({"a": 1})]).await.unwrap();
    admin.remove_file("/data/one/one.jsonl").await.unwrap();
    admin.create_dir("/data/one/one.jsonl").await.unwrap();
    let e = write_and_flush(&s, &[json!({"a": 2})])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("'/data/one/one.jsonl' failed"), "{e}");

    let overwrite = json!({"if_exists": "replace", "write_mode": "overwrite", "file_name": "part-{part}.jsonl"});
    admin.create_dir("/data/o").await.unwrap();
    admin
        .create_dir("/data/o/.faucet-overwrite-part-_part_.jsonl")
        .await
        .unwrap();
    admin
        .create_dir("/data/o/.faucet-overwrite-part-_part_.jsonl/.faucet-swap")
        .await
        .unwrap();
    let s = sink(port, "/data/o/", overwrite.clone());
    assert!(s.is_overwrite());
    let e = s.abort_overwrite().await.unwrap_err().to_string();
    assert!(e.contains("SFTP delete '"), "{e}");
    assert!(e.contains(".faucet-swap' failed"), "{e}");

    let s = sink(port, "/data/r/", overwrite);
    s.begin_overwrite().await.unwrap();
    write_and_flush(&s, &[json!({"a": 1})]).await.unwrap();
    admin.create_dir("/data/r/part-00001.jsonl").await.unwrap();
    put(&admin, "/data/r/part-00001.jsonl/keep").await;
    let e = s.commit_overwrite().await.unwrap_err().to_string();
    assert!(
        e.contains("SFTP replace '/data/r/part-00001.jsonl' failed"),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreachable_server_fails_the_overwrite_abort() {
    let s = sink(
        1,
        "/data/o/",
        json!({"if_exists": "replace", "write_mode": "overwrite", "file_name": "x.jsonl"}),
    );
    assert!(s.abort_overwrite().await.is_err());
}

#[test]
fn a_part_token_in_the_directory_is_a_config_error() {
    let mut cfg = serde_json::to_value(SftpSinkConfig::new(connection(22), "/data/")).unwrap();
    cfg["file_name"] = json!("a-{part}/x.jsonl");
    let e = SftpSink::new(serde_json::from_value(cfg).unwrap())
        .err()
        .expect("refused")
        .to_string();
    assert!(e.contains("SFTP sink: "), "{e}");
    assert!(e.contains("may appear only in the file name"), "{e}");
}

#[cfg(feature = "arrow")]
#[tokio::test(flavor = "multi_thread")]
async fn a_parquet_sink_encodes_batches_before_any_upload() {
    let s = sink(1, "/data/p/", json!({"format": "parquet"}));
    assert!(s.supports_columnar());
    let rows = [json!({"a": 1}), json!({"a": 2})];
    let batch = faucet_core::columnar::values_to_record_batch(
        &rows,
        faucet_core::columnar::infer_arrow_schema(&rows).unwrap(),
    )
    .unwrap();
    assert_eq!(s.write_batch_columnar(&batch).await.unwrap(), 2);
    assert!(s.flush().await.is_err(), "no server to upload to");
    assert!(!sink(1, "/data/p/", json!({})).supports_columnar());
}

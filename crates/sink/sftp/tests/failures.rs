//! The SFTP sink's failure paths (#777) against an SFTP server: a listing,
//! download, delete or replace the server refuses fails the call that needed
//! it, naming the operation and the path. The last tests need no server.
//! The server tests require Docker.
#![cfg(not(target_os = "windows"))]
#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)
use faucet_common_sftp::{SftpConnectionConfig, SftpSession, connect};
use faucet_core::Sink;
use faucet_sink_sftp::{SftpSink, SftpSinkConfig};
use serde_json::{Value, json};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::io::AsyncWriteExt;

const USER: &str = "faucet";
const PASS: &str = "secret";

async fn server_inner() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let c = faucet_conformance::containers::start_or_skip(
        || {
            GenericImage::new(
                "atmoz/sftp",
                "alpine@sha256:43dd1c023cf1bae487127d4b54660b0aee6e94c188a6599be36fb89ba115f961",
            )
            .with_exposed_port(22.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
            .with_cmd(vec![format!("{USER}:{PASS}:::data")])
        },
        &Default::default(),
    )
    .await?;
    let port = c.get_host_port_ipv4(22).await.expect("port");
    Some((c, port))
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

/// #789 FILE-06: an upload a crashed run left under its scratch name is
/// removed when the next run of the same output starts; a scratch file of
/// another output is kept, and the published file never carries the name.
#[tokio::test(flavor = "multi_thread")]
async fn a_new_run_removes_stale_upload_scratch_of_its_output() {
    let Some((_c, port)) = server().await else {
        return;
    };
    let s = session(port).await;
    let _ = s.create_dir("data/out").await;
    put(&s, "data/out/x.jsonl.faucet-tmp-upload-dead").await;
    put(&s, "data/out/other.csv.faucet-tmp-upload-live").await;

    let sk = sink(port, "data/out", json!({"file_name": "x.jsonl"}));
    write_and_flush(&sk, &[json!({"id": 1})]).await.unwrap();

    let mut names: Vec<String> = s
        .read_dir("data/out")
        .await
        .unwrap()
        .map(|e| e.file_name())
        .collect();
    names.sort();
    assert_eq!(
        names,
        ["other.csv.faucet-tmp-upload-live", "x.jsonl"],
        "{names:?}"
    );
}

/// [`server_inner`], failing instead of skipping when `FAUCET_REQUIRE_BACKENDS`
/// is set — CI provides Docker, so an unavailable backend is a failure there.
async fn server() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let started = server_inner().await;
    if started.is_none() {
        faucet_conformance::containers::backend_missing("server: the test backend is unavailable");
    }
    started
}

/// The image's sshd configuration with one session per connection.
const ONE_SESSION_SSHD_CONFIG: &str = "Protocol 2
HostKey /etc/ssh/ssh_host_ed25519_key
HostKey /etc/ssh/ssh_host_rsa_key
UseDNS no
PermitRootLogin no
X11Forwarding no
AllowTcpForwarding no
Subsystem sftp internal-sftp
ForceCommand internal-sftp
ChrootDirectory %h
MaxSessions 1
";

async fn one_session_server() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let started = faucet_conformance::containers::start_or_skip(
        || {
            GenericImage::new(
                "atmoz/sftp",
                "alpine@sha256:43dd1c023cf1bae487127d4b54660b0aee6e94c188a6599be36fb89ba115f961",
            )
            .with_exposed_port(22.tcp())
            .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
            .with_cmd(vec![format!("{USER}:{PASS}:::data")])
            .with_copy_to(
                "/etc/ssh/sshd_config",
                ONE_SESSION_SSHD_CONFIG.as_bytes().to_vec(),
            )
        },
        &Default::default(),
    )
    .await;
    let Some(c) = started else {
        faucet_conformance::containers::backend_missing(
            "one_session_server: the test backend is unavailable",
        );
        return None;
    };
    let port = c.get_host_port_ipv4(22).await.expect("port");
    Some((c, port))
}

fn backend(port: u16, max_listed_entries: usize) -> faucet_common_sftp::SftpBackend {
    let template = faucet_common_file::write::NameTemplate::from_path(
        "x.jsonl",
        faucet_core::FileFormat::JsonLines,
        faucet_core::Compression::None,
        false,
    )
    .unwrap()
    .1;
    faucet_common_sftp::SftpBackend::builder(connection(port))
        .max_listed_entries(max_listed_entries)
        .build("/data/out/", &template)
        .expect("backend")
}

#[tokio::test(flavor = "multi_thread")]
async fn sink_listings_are_bounded() {
    use faucet_common_file::write::{Area, StorageBackend};
    let Some((_c, port)) = server().await else {
        return;
    };
    let admin = session(port).await;
    admin.create_dir("/data/out").await.unwrap();
    for name in ["a.jsonl", "b.jsonl", "c.jsonl"] {
        put(&admin, &format!("/data/out/{name}")).await;
    }
    let template = faucet_common_file::write::NameTemplate::from_path(
        "x.jsonl",
        faucet_core::FileFormat::JsonLines,
        faucet_core::Compression::None,
        false,
    )
    .unwrap()
    .1;
    for token in ["67000000aaaaa1", "67000000aaaaa2", "67000000aaaaa3"] {
        let dir = template.swap_run_dir_name(Some(token));
        admin.create_dir(format!("/data/out/{dir}")).await.unwrap();
    }

    let roomy = backend(port, 6);
    let mut files = roomy
        .list(Area::Destination)
        .await
        .expect("six entries fit");
    files.sort();
    assert_eq!(files, ["a.jsonl", "b.jsonl", "c.jsonl"]);
    let mut runs = roomy.swap_runs().await.expect("six entries fit");
    runs.sort();
    assert_eq!(runs, ["67000000aaaaa1", "67000000aaaaa2", "67000000aaaaa3"]);

    let tight = backend(port, 5);
    let e = tight.list(Area::Destination).await.unwrap_err().to_string();
    assert!(
        e.contains("'/data/out'") && e.contains("max_listed_entries"),
        "{e}"
    );
    let e = tight.swap_runs().await.unwrap_err().to_string();
    assert!(e.contains("max_listed_entries"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_one_session_server_lists_over_a_second_connection() {
    use faucet_common_file::write::{Area, StorageBackend};
    let Some((_c, port)) = one_session_server().await else {
        return;
    };
    let admin = session(port).await;
    admin.create_dir("/data/out").await.unwrap();
    put(&admin, "/data/out/a.jsonl").await;
    let template = faucet_common_file::write::NameTemplate::from_path(
        "x.jsonl",
        faucet_core::FileFormat::JsonLines,
        faucet_core::Compression::None,
        false,
    )
    .unwrap()
    .1;
    let dir = template.swap_run_dir_name(Some("67000000aaaaa1"));
    admin.create_dir(format!("/data/out/{dir}")).await.unwrap();
    drop(admin);

    let b = backend(port, 100);
    assert_eq!(b.list(Area::Destination).await.unwrap(), ["a.jsonl"]);
    assert_eq!(b.swap_runs().await.unwrap(), ["67000000aaaaa1"]);

    let overwrite = json!({"if_exists": "replace", "write_mode": "overwrite", "file_name": "part-{part}.jsonl"});
    let s = sink(port, "/data/r/", overwrite.clone());
    s.begin_overwrite().await.unwrap();
    write_and_flush(&s, &[json!({"a": 1})]).await.unwrap();
    s.commit_overwrite().await.unwrap();
    let s = sink(port, "/data/r/", overwrite);
    s.begin_overwrite().await.unwrap();
    write_and_flush(&s, &[json!({"a": 2})]).await.unwrap();
    s.commit_overwrite().await.unwrap();

    let s = sink(
        port,
        "/data/r/",
        json!({"if_exists": "append", "file_name": "part-{part}.jsonl"}),
    );
    write_and_flush(&s, &[json!({"a": 3})]).await.unwrap();

    let admin = session(port).await;
    let mut names: Vec<String> = admin
        .read_dir("/data/r")
        .await
        .unwrap()
        .map(|e| e.file_name())
        .filter(|n| n.starts_with("part-"))
        .collect();
    names.sort();
    assert_eq!(names, ["part-00001.jsonl", "part-00002.jsonl"], "{names:?}");
    let first = admin.read("/data/r/part-00001.jsonl").await.unwrap();
    assert_eq!(first, b"{\"a\":2}\n");
}

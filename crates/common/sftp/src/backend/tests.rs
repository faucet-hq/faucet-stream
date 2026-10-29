
use super::*;

#[tokio::test]
async fn the_backend_is_lazy_and_probes_by_connecting() {
    use faucet_common_file::write::{Area, StorageBackend};
    let template = NameTemplate::from_path(
        "x.jsonl",
        faucet_core::FileFormat::JsonLines,
        faucet_core::Compression::None,
        false,
    )
    .unwrap()
    .1;
    let mut conn = SftpConnectionConfig::with_password("127.0.0.1", "u", "p");
    conn.port = 1;
    let backend = SftpBackend::builder(conn)
        .concurrency(2)
        .scratch_dir(None)
        .build("/out/", &template)
        .unwrap();
    assert_eq!(
        backend.describe(Area::Destination, "x.jsonl"),
        "sftp://127.0.0.1:1/out/x.jsonl"
    );
    assert_eq!(backend.connection().port, 1);
    assert_eq!(backend.remote().upload_concurrency(), 2);
    assert!(format!("{backend:?}").contains("SftpBackend"));
    backend.set_call_context(faucet_core::CallContext::new());
    backend.set_roundtrip_recorder(Arc::new(RoundtripRecorder::new(
        faucet_core::observability::RoundtripSide::Sink,
        "p",
        "r",
        "sftp",
    )));
    let report = backend
        .check(&CheckContext {
            timeout: std::time::Duration::from_secs(3),
        })
        .await
        .unwrap()
        .expect("a probe");
    assert_eq!(report.failed_count(), 1, "{report:?}");
}

#[test]
fn prefixes_split_into_directory_and_name_start() {
    assert_eq!(split_prefix("/out/"), ("/out".to_string(), ""));
    assert_eq!(split_prefix("/out/run-"), ("/out".to_string(), "run-"));
    assert_eq!(split_prefix("/x"), ("/".to_string(), "x"));
    assert_eq!(split_prefix("rel"), (".".to_string(), "rel"));
    assert_eq!(parent("/a/b/c"), Some("/a/b"));
    assert_eq!(parent("c"), None);
    assert_eq!(ancestors("/a/b"), ["/a", "/a/b"]);
    assert_eq!(ancestors("a/b"), ["a", "a/b"]);
    assert_eq!(ancestors("/"), Vec::<String>::new());
    let o = SftpObjects::new(SftpConnectionConfig::with_password("h", "u", "p"));
    assert_eq!(o.describe("/out/a.jsonl"), "sftp://h:22/out/a.jsonl");
}

fn status(code: crate::StatusCode) -> SftpError {
    SftpError::Status(russh_sftp::protocol::Status {
        id: 1,
        status_code: code,
        error_message: "m".into(),
        language_tag: "en".into(),
    })
}

#[test]
fn only_a_failure_below_the_protocol_marks_the_session_dead() {
    type Probe = Result<Result<String, SftpError>, tokio::time::error::Elapsed>;
    let alive: Probe = Ok(Ok("/".into()));
    assert!(!session_is_dead(&alive));
    let refused: Probe = Ok(Err(status(crate::StatusCode::PermissionDenied)));
    assert!(!session_is_dead(&refused));
    for e in [
        SftpError::IO("channel closed".into()),
        SftpError::Timeout,
        SftpError::UnexpectedBehavior("RecvError".into()),
    ] {
        let p: Probe = Ok(Err(e));
        assert!(session_is_dead(&p));
    }
}

#[tokio::test]
async fn a_failed_operation_without_a_session_stays_an_error() {
    let objects = SftpObjects::new(SftpConnectionConfig::with_password("127.0.0.1", "u", "p"));
    let r: Result<(), FaucetError> = objects
        .guarded(async { Err(FaucetError::Sink("boom".into())) })
        .await;
    assert!(r.unwrap_err().to_string().contains("boom"));
    assert!(objects.session.lock().await.is_none());
}

#[test]
fn only_no_such_file_means_missing() {
    use crate::StatusCode;
    assert!(!missing("stat", "/p", status(StatusCode::NoSuchFile)).unwrap());
    let e = missing("stat", "/p", status(StatusCode::PermissionDenied))
        .unwrap_err()
        .to_string();
    assert!(e.contains("SFTP stat '/p' failed"), "{e}");
    assert!(missing("stat", "/p", status(StatusCode::Failure)).is_err());
    assert!(missing("stat", "/p", SftpError::Timeout).is_err());
}

async fn server() -> Option<(
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
    u16,
)> {
    use testcontainers::ImageExt;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    let c = faucet_conformance::containers::start_or_skip(
        || {
            testcontainers::GenericImage::new("atmoz/sftp", "alpine")
                .with_exposed_port(22.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
                .with_cmd(vec!["faucet:secret:::data".to_string()])
        },
        &Default::default(),
    )
    .await?;
    let port = c.get_host_port_ipv4(22).await.expect("port");
    Some((c, port))
}

/// C3 (#783): a stat the server refuses (here: a directory the user cannot
/// traverse) is an error, not "missing" — a listing that silently came
/// back empty let an overwrite commit promote nothing and prune the
/// destination, and a delete that silently did nothing left files behind.
#[tokio::test(flavor = "multi_thread")]
async fn refused_stats_are_errors_not_missing_files() {
    let Some((_c, port)) = server().await else {
        return;
    };
    let cfg = SftpConnectionConfig::with_password("127.0.0.1", "faucet", "secret").port(port);
    let o = SftpObjects::new(cfg);
    let conn = o.connection().await.unwrap();
    let sftp = conn.session();
    sftp.create_dir("/data/locked").await.unwrap();
    sftp.create_dir("/data/locked/sub").await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("f");
    std::fs::write(&src, b"x\n").unwrap();
    o.upload(&src, "/data/locked/sub/f").await.unwrap();
    assert_eq!(
        o.list("/data/locked/sub/").await.unwrap(),
        ["/data/locked/sub/f"]
    );
    assert!(o.list("/data/nope/").await.unwrap().is_empty());
    o.delete("/data/nope/f").await.unwrap();

    let mut attrs = russh_sftp::protocol::FileAttributes::empty();
    attrs.permissions = Some(0o040000);
    sftp.set_metadata("/data/locked", attrs).await.unwrap();

    let e = o.list("/data/locked/sub/").await.unwrap_err().to_string();
    assert!(e.contains("SFTP list '/data/locked/sub' failed"), "{e}");
    let e = o
        .delete("/data/locked/sub/f")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("SFTP delete '/data/locked/sub/f' failed"), "{e}");
    assert!(o.exists("/data/locked/sub/f").await.is_err());
    let e = o
        .upload(&src, "/data/locked/sub/new/g")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("/data/locked/sub"), "{e}");

    let mut attrs = russh_sftp::protocol::FileAttributes::empty();
    attrs.permissions = Some(0o040755);
    sftp.set_metadata("/data/locked", attrs).await.unwrap();
    o.upload(&src, "/data/locked/sub/f").await.unwrap();
    o.rename("/data/locked/sub/f", "/data/moved/deep/f")
        .await
        .unwrap();
    assert!(o.exists("/data/moved/deep/f").await.unwrap());
    assert!(!o.exists("/data/locked/sub/f").await.unwrap());
    o.upload(&src, "/data/moved/deep/g").await.unwrap();
    o.rename("/data/moved/deep/g", "/data/moved/deep/f")
        .await
        .unwrap();
    let out = dir.path().join("out");
    o.download("/data/moved/deep/f", &out).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), b"x\n");
    assert!(conn.supports_posix_rename());

    let mut plain = SftpObjects::new(o.connection.clone());
    plain.posix_rename = false;
    std::fs::write(&src, b"y\n").unwrap();
    plain.upload(&src, "/data/moved/deep/f").await.unwrap();
    plain.upload(&src, "/data/moved/deep/h").await.unwrap();
    plain
        .rename("/data/moved/deep/h", "/data/moved/deep/f")
        .await
        .unwrap();
    plain.download("/data/moved/deep/f", &out).await.unwrap();
    assert_eq!(std::fs::read(&out).unwrap(), b"y\n");
    let e = plain
        .rename("/data/moved/deep/none", "/data/moved/deep/f")
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("rename into place"), "{e}");
}

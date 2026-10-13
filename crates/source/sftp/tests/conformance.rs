//! `faucet-conformance` battery for the SFTP source.
//!
//! Check 1 (config-schema validity) and check 6 (errors-not-panics) are pure /
//! offline and always run. Check 2 (bounded-memory streaming) boots an
//! `atmoz/sftp` container via `testcontainers` and so requires Docker — it runs
//! in CI alongside the other integration tests and skips cleanly when Docker is
//! unavailable.

use faucet_common_sftp::{HostKeyPolicy, SftpAuth, SftpConnectionConfig};
use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_errors_not_panics,
};
use faucet_source_sftp::{SftpSource, SftpSourceConfig};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
};

const USER: &str = "faucet";
const PASS: &str = "faucetpass";
const UPLOAD_DIR: &str = "upload";

// ── Check 1: config schema (offline) ────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SftpSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-sftp");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// `SftpSource::new` is lazy (no connect at build time), so this runs
/// unconditionally with no container.
#[test]
fn conformance_connector_name_nonempty() {
    let conn = SftpConnectionConfig {
        host: "127.0.0.1".to_string(),
        port: 1,
        username: "nobody".to_string(),
        auth: SftpAuth::Password {
            password: "x".to_string(),
        },
        known_hosts: HostKeyPolicy::Insecure,
        connect_timeout_secs: 30,
        keepalive_interval_secs: 15,
    };
    let source =
        SftpSource::new(SftpSourceConfig::new(conn, "/data")).expect("source builds lazily");
    faucet_conformance::assert_connector_name_nonempty(&source);
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Start an `atmoz/sftp` container with user `faucet:faucetpass` and a writable
/// `upload` directory. Returns the container handle and mapped port, or `None`
/// when Docker is unavailable so the test skips cleanly.
async fn start_sftp_inner() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let container = faucet_conformance::containers::start_or_skip(
        || {
            GenericImage::new("atmoz/sftp", "latest")
                .with_exposed_port(22.tcp())
                .with_wait_for(WaitFor::message_on_stderr("Server listening on"))
                .with_cmd(vec![format!("{USER}:{PASS}:::{UPLOAD_DIR}")])
        },
        &Default::default(),
    )
    .await?;
    let port = container.get_host_port_ipv4(22).await.ok()?;
    Some((container, port))
}

fn connection(port: u16) -> SftpConnectionConfig {
    SftpConnectionConfig {
        host: "127.0.0.1".to_string(),
        port,
        username: USER.to_string(),
        auth: SftpAuth::Password {
            password: PASS.to_string(),
        },
        // The container's host key is ephemeral, so verification is disabled
        // for the test only.
        known_hosts: HostKeyPolicy::Insecure,
        connect_timeout_secs: 30,
        keepalive_interval_secs: 15,
    }
}

fn jsonl_body(start: i64, end_inclusive: i64) -> String {
    let mut out = String::new();
    for i in start..=end_inclusive {
        out.push_str(&format!("{{\"id\":{i}}}\n"));
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let Some((_container, port)) = start_sftp().await else {
        return;
    };
    let conn = connection(port);

    // Seed a 5,000-record JSONL file into the writable upload directory.
    // `SftpSession::write` opens with WRITE only (no CREATE), so it cannot
    // create a new file — open explicitly with CREATE|WRITE|TRUNCATE.
    use faucet_common_sftp::OpenFlags;
    use tokio::io::AsyncWriteExt;
    let sftp = faucet_common_sftp::connect(&conn)
        .await
        .expect("seed connect");
    let body = jsonl_body(1, 5_000);
    let mut file = sftp
        .open_with_flags(
            format!("{UPLOAD_DIR}/data.jsonl"),
            OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
        )
        .await
        .expect("seed open");
    file.write_all(body.as_bytes()).await.expect("seed write");
    file.shutdown().await.expect("seed close");
    drop(file);
    drop(sftp);

    let config = SftpSourceConfig::new(conn, UPLOAD_DIR).with_batch_size(250);
    let source = SftpSource::new(config).expect("SftpSource::new");

    assert_bounded_memory(&source, 250, 5_000).await;
    // _container stays alive to here.
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

/// Point the source at an unreachable endpoint (`127.0.0.1:1`, which refuses
/// connections immediately). `new()` stays lazy — no container needed — and the
/// first connect on both `fetch_all` and `stream_pages` fails with a typed
/// `FaucetError`, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let conn = SftpConnectionConfig {
        host: "127.0.0.1".to_string(),
        port: 1,
        username: "nobody".to_string(),
        auth: SftpAuth::Password {
            password: "x".to_string(),
        },
        known_hosts: HostKeyPolicy::Insecure,
        connect_timeout_secs: 30,
        keepalive_interval_secs: 15,
    };
    let source =
        SftpSource::new(SftpSourceConfig::new(conn, "/data")).expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}

/// [`start_sftp_inner`], failing instead of skipping when `FAUCET_REQUIRE_BACKENDS`
/// is set — CI provides Docker, so an unavailable backend is a failure there.
async fn start_sftp() -> Option<(ContainerAsync<GenericImage>, u16)> {
    let started = start_sftp_inner().await;
    if started.is_none() {
        faucet_conformance::containers::backend_missing(
            "start_sftp: the test backend is unavailable",
        );
    }
    started
}

// ── Descriptor (RFC 0013): agrees with the built source ─────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[test]
fn descriptor_matches_built_source() {
    let base = serde_json::to_value(SftpSourceConfig::new(
        SftpConnectionConfig {
            host: "127.0.0.1".to_string(),
            port: 1,
            username: "nobody".to_string(),
            auth: SftpAuth::Password {
                password: "x".to_string(),
            },
            known_hosts: HostKeyPolicy::Insecure,
            connect_timeout_secs: 30,
            keepalive_interval_secs: 15,
        },
        "/data",
    ))
    .unwrap();
    let mut cases = vec![
        serde_json::json!({}),
        serde_json::json!({"format": "json_array"}),
        serde_json::json!({"format": "raw_text"}),
    ];
    #[cfg(feature = "arrow")]
    cases.push(serde_json::json!({"format": "parquet"}));
    #[cfg(feature = "file-format-avro")]
    cases.push(serde_json::json!({"format": "avro"}));
    #[cfg(feature = "file-format-orc")]
    cases.push(serde_json::json!({"format": "orc"}));
    for extra in cases {
        let v = descriptor_config(base.clone(), extra);
        let cfg: SftpSourceConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SftpSource::new(cfg).expect("source builds lazily");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_sftp::DESCRIPTOR,
            "sftp",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

fn hung_connection(port: u16) -> SftpConnectionConfig {
    SftpConnectionConfig::with_password("127.0.0.1", "u", "p")
        .port(port)
        .known_hosts(HostKeyPolicy::Insecure)
}

#[tokio::test]
async fn conformance_call_deadline() {
    let hung = faucet_conformance::hung::SilentListener::start()
        .await
        .expect("listener");
    let port = hung.port();
    faucet_conformance::calls::assert_source_call_deadline(|| {
        SftpSource::new(SftpSourceConfig::new(hung_connection(port), "/data")).unwrap()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_roundtrips_recorded() {
    let Some((_container, port)) = start_sftp().await else {
        return;
    };
    let conn = connection(port);
    use faucet_common_sftp::OpenFlags;
    use tokio::io::AsyncWriteExt;
    let sftp = faucet_common_sftp::connect(&conn)
        .await
        .expect("seed connect");
    let mut file = sftp
        .open_with_flags(
            format!("{UPLOAD_DIR}/rt.jsonl"),
            OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
        )
        .await
        .expect("seed open");
    file.write_all(jsonl_body(1, 3).as_bytes())
        .await
        .expect("seed write");
    file.shutdown().await.expect("seed close");
    drop(file);
    drop(sftp);
    let source = SftpSource::new(SftpSourceConfig::new(conn, UPLOAD_DIR)).expect("source");
    faucet_conformance::calls::assert_source_records_roundtrips(&source).await;
}

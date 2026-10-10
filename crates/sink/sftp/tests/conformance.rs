//! `faucet-conformance` battery against the SFTP sink.
//!
//! The SFTP sink writes JSON Lines objects and is append-only — it advertises
//! no idempotency mechanism, so check 5 exercises the **honest branch**: Append
//! works, and the sink does not claim idempotent/keyed dedup.
//!
//! Check 1 (`assert_config_schema_valid_value`) is offline and always runs.
//! Check 5 (`assert_capabilities_truthful`) boots an `atmoz/sftp` container via
//! `testcontainers` and so requires Docker; it skips cleanly when Docker is
//! unavailable.

use faucet_common_sftp::{HostKeyPolicy, SftpAuth, SftpConnectionConfig};
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_sftp::{SftpSink, SftpSinkConfig};
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
    let schema = serde_json::to_value(schemars::schema_for!(SftpSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "sftp");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// `SftpSink::new` is lazy (no connect at build time), so this runs
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
    let sink = SftpSink::new(SftpSinkConfig::new(conn, "/data")).expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
}

// ── Check 5: capabilities truthful (Docker) ─────────────────────────────────

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
        known_hosts: HostKeyPolicy::Insecure,
        connect_timeout_secs: 30,
        keepalive_interval_secs: 15,
    }
}

/// Count durable records: sum the JSONL lines across every object under the
/// upload directory.
async fn count_records(conn: &SftpConnectionConfig) -> usize {
    let sftp = faucet_common_sftp::connect(conn)
        .await
        .expect("count connect");
    let entries = sftp.read_dir(UPLOAD_DIR).await.expect("read_dir");
    let mut total = 0usize;
    for entry in entries {
        if !entry.file_type().is_file() {
            continue;
        }
        // Skip any in-flight temporary object.
        if entry.file_name().ends_with(".tmp") {
            continue;
        }
        let bytes = sftp.read(entry.path()).await.expect("read object");
        let body = String::from_utf8(bytes).expect("utf-8");
        total += body.lines().filter(|l| !l.trim().is_empty()).count();
    }
    total
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let Some((_container, port)) = start_sftp().await else {
        return;
    };
    let conn = connection(port);

    let sink = SftpSink::new(SftpSinkConfig::new(conn.clone(), UPLOAD_DIR)).expect("SftpSink::new");

    let conn_ref = &conn;
    faucet_conformance::assert_capabilities_truthful(&sink, || async move {
        count_records(conn_ref).await
    })
    .await;

    // The honest branch must leave the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
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

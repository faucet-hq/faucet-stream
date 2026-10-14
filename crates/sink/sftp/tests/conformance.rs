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
    // RFC 0014: the sink runs on the object-store family driver.
    faucet_conformance::assert_driver_declarations_truthful(&sink).await;
    faucet_conformance::assert_driver_row_paths_agree(&sink).await;
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

// ── Descriptor (RFC 0013): agrees with the built sink ───────────────────────

fn descriptor_config(base: serde_json::Value, extra: serde_json::Value) -> serde_json::Value {
    let mut v = base;
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

#[test]
fn descriptor_matches_built_sink() {
    let base = serde_json::to_value(SftpSinkConfig::new(
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
        serde_json::json!({"format": "raw_text"}),
        serde_json::json!({"format": "auto", "file_name": "x.json"}),
        serde_json::json!({"format": "auto", "file_extension": ".txt"}),
    ];
    #[cfg(feature = "file-format-csv")]
    cases.extend([
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "warn"}}),
        serde_json::json!({"format": "csv", "csv": {"on_unknown_field": "error"}}),
    ]);
    #[cfg(feature = "arrow")]
    cases.push(serde_json::json!({"format": "auto", "file_name": "x-{part}.parquet"}));
    for extra in cases {
        let v = descriptor_config(base.clone(), extra);
        let cfg: SftpSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SftpSink::new(cfg).expect("sink builds lazily");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_sftp::DESCRIPTOR,
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
    faucet_conformance::calls::assert_sink_call_deadline(
        || SftpSink::new(SftpSinkConfig::new(hung_connection(port), "/data")).unwrap(),
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

// ── #878: declared capabilities against the SFTP container ────────────────────

/// Records in the files directly inside `dir`: the overwrite swap area is a
/// subdirectory (`.faucet-overwrite-…/`), so a reader of the destination never
/// sees it.
async fn records_in(conn: &SftpConnectionConfig, dir: &str) -> usize {
    let sftp = faucet_common_sftp::connect(conn)
        .await
        .expect("count connect");
    let Ok(entries) = sftp.read_dir(dir).await else {
        return 0;
    };
    let mut total = 0usize;
    for entry in entries {
        if !entry.file_type().is_file() || entry.file_name().starts_with('.') {
            continue;
        }
        let bytes = sftp
            .read(format!("{dir}/{}", entry.file_name()))
            .await
            .expect("read object");
        total += String::from_utf8_lossy(&bytes)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .count();
    }
    total
}

/// The file is uploaded when the sink flushes, so a cancelled run must still
/// flush; and `write_mode: overwrite` stages in a swap area until commit.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_declared_capabilities() {
    let Some((_container, port)) = start_sftp().await else {
        return;
    };
    let conn = connection(port);
    let conn_ref = &conn;

    let sink = SftpSink::new(SftpSinkConfig::new(
        conn.clone(),
        format!("{UPLOAD_DIR}/cancel"),
    ))
    .expect("SftpSink::new");
    faucet_conformance::assert_cancellation_flushes(&sink, || async move {
        records_in(conn_ref, &format!("{UPLOAD_DIR}/cancel")).await
    })
    .await;

    let sftp = faucet_common_sftp::connect(&conn)
        .await
        .expect("seed connect");
    let dir = format!("{UPLOAD_DIR}/ovw");
    sftp.create_dir(dir.as_str()).await.expect("create dir");
    let seed = format!("{dir}/out-00001.jsonl");
    sftp.create(seed.as_str()).await.expect("create seed");
    sftp.write(seed.as_str(), b"{\"id\":-1}\n{\"id\":-2}\n{\"id\":-3}\n")
        .await
        .expect("write seed");
    let mut cfg = SftpSinkConfig::new(conn.clone(), dir.clone());
    cfg.file_name = Some("out-{part}.jsonl".into());
    cfg.write_mode = faucet_common_file::write::FileWriteMode::Overwrite;
    let overwrite = SftpSink::new(cfg).expect("overwrite sink");
    let dir_ref = dir.as_str();
    faucet_conformance::assert_overwrite_lifecycle(&overwrite, || async move {
        records_in(conn_ref, dir_ref).await
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_roundtrips_recorded() {
    let Some((_container, port)) = start_sftp().await else {
        return;
    };
    let sink =
        SftpSink::new(SftpSinkConfig::new(connection(port), UPLOAD_DIR)).expect("SftpSink::new");
    faucet_conformance::calls::assert_sink_records_roundtrips(
        &sink,
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

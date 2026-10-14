//! Descriptor conformance for the Singer target bridge: building the sink only
//! validates its config, so no target process is spawned here.

use faucet_sink_singer::{SingerSink, SingerSinkConfig};
use serde_json::json;

#[test]
fn conformance_descriptor_matches_built_sink() {
    for v in [
        json!({"target_command": "target-x"}),
        json!({"target_command": "target-x", "write_mode": "upsert", "key": ["id"]}),
        json!({"target_command": "target-x", "write_mode": "overwrite"}),
    ] {
        let cfg: SingerSinkConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SingerSink::new(cfg).unwrap();
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_singer::DESCRIPTOR,
            "singer",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

/// A tap/target that reads nothing and never writes or exits.
#[cfg(unix)]
fn hung_executable(dir: &tempfile::TempDir) -> String {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.path().join("hung.sh");
    std::fs::write(&path, "#!/bin/sh\nexec sleep 60\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    path.display().to_string()
}

#[cfg(unix)]
#[tokio::test]
async fn conformance_call_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let target = hung_executable(&dir);
    faucet_conformance::calls::assert_sink_call_deadline(
        || {
            faucet_sink_singer::SingerSink::new(faucet_sink_singer::SingerSinkConfig::new(
                target.clone(),
            ))
            .unwrap()
        },
        &[serde_json::json!({"id": 1})],
    )
    .await;
}

/// The sink confirms a page only when the target exits or echoes a STATE, so
/// a cancelled run must still flush (#878). The fake target appends every
/// record to `path` and closes the file on exit.
#[cfg(unix)]
#[tokio::test]
async fn conformance_cancellation_flushes() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    let mut cfg = SingerSinkConfig::new(format!(
        "{}/tests/fake_targets/fake_target.py",
        env!("CARGO_MANIFEST_DIR")
    ));
    cfg.target_config = json!({ "path": out });
    cfg.stream = Some("orders".into());
    let sink = SingerSink::new(cfg).unwrap();
    faucet_conformance::assert_cancellation_flushes(&sink, || {
        let out = out.clone();
        async move {
            std::fs::read_to_string(&out)
                .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
                .unwrap_or(0)
        }
    })
    .await;
}

/// RFC 0014: the sink runs on the family-neutral sink driver; its overwrite
/// is a table version, so the staging probe answers "unknown".
#[cfg(unix)]
#[tokio::test]
#[allow(deprecated)] // probes the 1.x staging hook the driver answers until faucet-core 2.0 (RFC 0014)
async fn conformance_driver_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("out.jsonl");
    let mut cfg = SingerSinkConfig::new(format!(
        "{}/tests/fake_targets/fake_target.py",
        env!("CARGO_MANIFEST_DIR")
    ));
    cfg.target_config = json!({ "path": out });
    cfg.stream = Some("orders".into());
    let sink = SingerSink::new(cfg).unwrap();
    faucet_conformance::assert_driver_declarations_truthful(&sink).await;
    faucet_conformance::assert_driver_row_paths_agree(&sink).await;
    faucet_core::Sink::flush(&sink).await.unwrap();
    let written = std::fs::read_to_string(&out)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0);
    assert_eq!(written, 12);
    assert_eq!(
        faucet_core::Sink::overwrite_staging_exists(&sink)
            .await
            .unwrap(),
        None
    );
}

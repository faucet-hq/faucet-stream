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

//! Proves `faucet-source-singer` upholds the shared connector contract by
//! invoking the reusable `faucet-conformance` battery (checks 1, 2, 3 & 6),
//! driving the dependency-free fake tap.
//!
//! The Singer bridge is single-stream output; it is a source (not a sink), so
//! checks 4/5 do not apply.

use faucet_source_singer::{SingerSource, SingerSourceConfig};

fn fake_tap() -> String {
    format!("{}/tests/fake_taps/fake_tap.sh", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn conformance_config_schema_valid() {
    let source = SingerSource::new(SingerSourceConfig::new(fake_tap(), "s"));
    faucet_conformance::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn conformance_bounded_memory() {
    let total = 2_000;
    let batch = 100;
    // Fake tap emits `total` records; the bridge pages at the stream_pages hint.
    let source = SingerSource::new(SingerSourceConfig {
        args: vec![
            "--stream".into(),
            "s".into(),
            "--total".into(),
            total.to_string(),
        ],
        state_key: Some("singer_conf".into()),
        ..SingerSourceConfig::new(fake_tap(), "s")
    });
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_bookmark_roundtrip() {
    // The fake tap emits a final STATE {"last_id": total} on clean completion,
    // and on resume re-emits only the boundary record — so a round-trip through
    // apply_start_bookmark yields strictly fewer records the second time.
    let total = 6;
    let source = SingerSource::new(SingerSourceConfig {
        args: vec![
            "--stream".into(),
            "s".into(),
            "--total".into(),
            total.to_string(),
        ],
        state_key: Some("singer_conf_bookmark".into()),
        ..SingerSourceConfig::new(fake_tap(), "s")
    });
    faucet_conformance::assert_bookmark_roundtrip(&source).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // A tap executable that does not exist must surface a typed FaucetError when
    // the bridge tries to spawn it — never a panic.
    faucet_conformance::assert_errors_not_panics(&missing_tap_source()).await;
}

/// A source whose configured tap binary does not exist.
fn missing_tap_source() -> SingerSource {
    SingerSource::new(SingerSourceConfig::new(
        "/nonexistent/faucet-conformance-tap-binary",
        "s",
    ))
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    let source = SingerSource::new(SingerSourceConfig::new(fake_tap(), "s"));
    faucet_conformance::assert_connector_name_nonempty(&source);
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    // The Singer source overrides `check()` (tap-executable + catalog probes);
    // a missing tap surfaces as a `Fail` probe inside `Ok(report)`, never an
    // `Err`.
    faucet_conformance::assert_preflight_check_wellformed(
        &missing_tap_source(),
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[test]
fn conformance_descriptor_matches_built_source() {
    for v in [
        serde_json::json!({"executable": fake_tap(), "stream": "s"}),
        serde_json::json!({"executable": fake_tap(), "stream": "s", "flush_on_state": false}),
    ] {
        let cfg: faucet_source_singer::SingerSourceConfig =
            serde_json::from_value(v.clone()).unwrap();
        let built = faucet_source_singer::SingerSource::new(cfg);
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_singer::DESCRIPTOR,
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
    let tap = hung_executable(&dir);
    faucet_conformance::calls::assert_source_call_deadline(|| {
        SingerSource::new(SingerSourceConfig::new(tap.clone(), "s"))
    })
    .await;
}

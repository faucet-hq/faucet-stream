//! `inherit_env` controls which of faucet's environment variables the target
//! sees; the explicit `env` map still applies on top.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;

use faucet_core::Sink;
use faucet_sink_singer::{InheritEnv, SingerSink, SingerSinkConfig};
use serde_json::json;

const TARGET: &str = r#"#!/bin/sh
printf 'cargo=%s path=%s extra=%s\n' "${CARGO_MANIFEST_DIR:+set}" "${PATH:+set}" "$EXTRA" > "$DUMP"
cat > /dev/null
"#;

async fn seen(dir: &tempfile::TempDir, name: &str, inherit_env: InheritEnv) -> String {
    let target = dir.path().join("env_target.sh");
    std::fs::write(&target, TARGET).unwrap();
    std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755)).unwrap();
    let dump = dir.path().join(format!("{name}.txt"));
    let mut cfg = SingerSinkConfig::new(target.to_string_lossy());
    cfg.stream = Some("s".into());
    cfg.inherit_env = inherit_env;
    cfg.env
        .insert("DUMP".into(), dump.to_string_lossy().into_owned());
    cfg.env.insert("EXTRA".into(), "yes".into());
    let sink = SingerSink::new(cfg).unwrap();
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    sink.flush().await.unwrap();
    std::fs::read_to_string(dump).unwrap().trim().to_string()
}

#[tokio::test]
async fn the_target_sees_the_environment_inherit_env_allows() {
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
    let dir = tempfile::tempdir().unwrap();
    assert_eq!(
        seen(&dir, "all", InheritEnv::default()).await,
        "cargo=set path=set extra=yes"
    );
    assert_eq!(
        seen(&dir, "baseline", InheritEnv::All(false)).await,
        "cargo= path=set extra=yes"
    );
    assert_eq!(
        seen(
            &dir,
            "listed",
            InheritEnv::Only(vec!["CARGO_MANIFEST_DIR".into()])
        )
        .await,
        "cargo=set path=set extra=yes"
    );
}

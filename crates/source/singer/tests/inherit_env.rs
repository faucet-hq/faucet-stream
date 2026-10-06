//! `inherit_env` controls which of faucet's environment variables the tap sees.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;

use faucet_core::{Source, Value};
use faucet_source_singer::{InheritEnv, SingerSource, SingerSourceConfig, discover};

const TAP: &str = r#"#!/bin/sh
if [ "$3" = "--discover" ]; then
  printf '{"streams":[],"cargo":"%s"}\n' "${CARGO_MANIFEST_DIR:+set}"
  exit 0
fi
printf '{"type":"SCHEMA","stream":"s","schema":{"type":"object"},"key_properties":[]}\n'
printf '{"type":"RECORD","stream":"s","record":{"cargo":"%s","path":"%s"}}\n' "${CARGO_MANIFEST_DIR:+set}" "${PATH:+set}"
"#;

fn config(dir: &tempfile::TempDir, inherit_env: InheritEnv) -> SingerSourceConfig {
    let tap = dir.path().join("env_tap.sh");
    std::fs::write(&tap, TAP).unwrap();
    std::fs::set_permissions(&tap, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut cfg = SingerSourceConfig::new(tap.to_string_lossy(), "s");
    cfg.state_key = Some("env_it".into());
    cfg.inherit_env = inherit_env;
    cfg
}

async fn seen(dir: &tempfile::TempDir, inherit_env: InheritEnv) -> Value {
    let cfg = config(dir, inherit_env);
    let records = SingerSource::new(cfg).fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    records[0].clone()
}

#[tokio::test]
async fn the_tap_sees_the_environment_inherit_env_allows() {
    assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
    let dir = tempfile::tempdir().unwrap();

    let all = seen(&dir, InheritEnv::default()).await;
    assert_eq!(all["cargo"], "set");

    let baseline = seen(&dir, InheritEnv::All(false)).await;
    assert_eq!(baseline["cargo"], "");
    assert_eq!(baseline["path"], "set");

    let listed = seen(&dir, InheritEnv::Only(vec!["CARGO_MANIFEST_DIR".into()])).await;
    assert_eq!(listed["cargo"], "set");
}

#[tokio::test]
async fn discovery_honours_inherit_env() {
    let dir = tempfile::tempdir().unwrap();
    let all = discover(&config(&dir, InheritEnv::All(true)))
        .await
        .unwrap();
    assert_eq!(all["cargo"], "set");
    let baseline = discover(&config(&dir, InheritEnv::All(false)))
        .await
        .unwrap();
    assert_eq!(baseline["cargo"], "");
}

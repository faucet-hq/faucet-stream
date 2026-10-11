//! #862 M-077: a rotated refresh token left under the shared persist key
//! earlier versions used is named by `faucet validate` (a warning) and
//! `faucet doctor` (a failing probe), each with the one-line `persist.key` fix.
#![cfg(all(feature = "source-csv", feature = "sink-stdout"))]

use assert_cmd::Command;
use faucet_core::StateStore as _;
use predicates::prelude::*;
use predicates::str::contains;
use std::path::Path;

const TOKEN_URL: &str = "http://127.0.0.1:9/token";

fn fnv1a_64(s: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn legacy_key() -> String {
    format!(
        "oauth2_refresh_{:016x}",
        fnv1a_64(&format!("{TOKEN_URL}\u{0}app"))
    )
}

fn config(dir: &Path, key: Option<&str>) -> std::path::PathBuf {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id\n1\n").unwrap();
    let store = dir.join("tokens");
    let key = key.map(|k| format!(", key: {k}")).unwrap_or_default();
    let cfg = dir.join("pipeline.yaml");
    std::fs::write(
        &cfg,
        format!(
            "version: 1\nname: legacy\nauth:\n  crm:\n    type: oauth2_refresh\n    config: {{ token_url: \"{TOKEN_URL}\", client_id: app, client_secret: s, refresh_token: seed, persist: {{ path: \"{}\"{key} }} }}\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: stdout, config: {{}} }}\n",
            store.display(),
            input.display()
        ),
    )
    .unwrap();
    cfg
}

async fn seed_legacy(dir: &Path) {
    faucet_core::FileStateStore::new(dir.join("tokens"))
        .put(
            &legacy_key(),
            &serde_json::json!({"refresh_token": "rotated"}),
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_warns_and_doctor_fails_on_a_legacy_persisted_token() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), None);
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("validate")
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("auth: WARNING").not());

    seed_legacy(dir.path()).await;
    let key = legacy_key();
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("validate")
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("auth: WARNING"))
        .stdout(contains(format!("add `key: {key}`")));
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .arg("validate")
        .arg(&cfg)
        .arg("--json")
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    assert!(
        v["auth_warnings"][0].as_str().unwrap().contains(&key),
        "{v}"
    );

    let out = Command::cargo_bin("faucet")
        .unwrap()
        .arg("doctor")
        .arg(&cfg)
        .args(["--json", "--timeout-secs", "5"])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    let auth = v["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["id"] == "auth")
        .expect("an auth invocation");
    assert_eq!(auth["probes"][0]["connector"], "crm");
    assert_eq!(auth["probes"][0]["status"], "fail", "{auth}");
    assert!(auth["probes"][0]["reason"].as_str().unwrap().contains(&key));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_explicit_persist_key_clears_the_warning() {
    let dir = tempfile::tempdir().unwrap();
    seed_legacy(dir.path()).await;
    let cfg = config(dir.path(), Some(&legacy_key()));
    Command::cargo_bin("faucet")
        .unwrap()
        .arg("validate")
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("auth: WARNING").not());
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .arg("doctor")
        .arg(&cfg)
        .args(["--json", "--timeout-secs", "5"])
        .assert()
        .get_output()
        .stdout
        .clone();
    let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
    let auth = v["invocations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["id"] == "auth")
        .expect("an auth invocation");
    assert_eq!(auth["probes"][0]["status"], "pass", "{auth}");
}

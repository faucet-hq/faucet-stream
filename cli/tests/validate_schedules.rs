//! `faucet validate --schedules FILE` (#1111): the offline check of a
//! `faucet serve --schedules` file before an upgrade.
#![cfg(all(feature = "schedule", feature = "templates"))]

use assert_cmd::Command;

fn write(text: &str, name: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(name);
    std::fs::write(&path, text).unwrap();
    (dir, path)
}

#[test]
fn a_valid_file_passes() {
    let (_dir, path) = write(
        "version: 1\nschedules:\n  - name: nightly\n    cron: '0 2 * * *'\n    timezone: America/New_York\n    template: { id: acme/billing }\n",
        "s.yaml",
    );
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", "--schedules"])
        .arg(&path)
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    assert!(stdout.contains("1 schedule(s) valid"), "{stdout}");
}

#[test]
fn every_refused_schedule_is_listed_and_the_exit_is_non_zero() {
    let (_dir, path) = write(
        r#"{"version": 1, "schedules": [
            {"name": "a", "cron": "0 0 * * SUN-SUN", "template": {"id": "t"}},
            {"name": "b", "cron": "0 0 * * 1,L", "template": {"id": "t"}},
            {"name": "c", "cron": "0 3 * * *", "template": {"id": "t"}}
        ]}"#,
        "s.json",
    );
    let out = Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", "--json", "--schedules"])
        .arg(&path)
        .assert()
        .failure();
    let stdout = String::from_utf8_lossy(&out.get_output().stdout).to_string();
    let report: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap();
    assert_eq!(report["valid"], false);
    assert_eq!(report["schedules"], 3);
    let problems = report["problems"].as_array().unwrap();
    assert_eq!(problems.len(), 2, "{problems:?}");
    assert!(problems[0].as_str().unwrap().contains("'a'"));
    assert!(problems[1].as_str().unwrap().contains("'b'"));
    let stderr = String::from_utf8_lossy(&out.get_output().stderr).to_string();
    assert!(stderr.contains("2 problem(s)"), "{stderr}");
}

#[test]
fn an_unreadable_file_is_a_clear_error() {
    Command::cargo_bin("faucet")
        .unwrap()
        .args(["validate", "--schedules", "/nonexistent/schedules.yaml"])
        .assert()
        .failure()
        .stderr(predicates::str::contains("reading schedules file"));
}

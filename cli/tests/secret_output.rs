//! Commands never print secret material: `--help` hides secret-bearing env
//! values (CLI-153) and `template run --dry-run --json` masks values bound
//! from the environment (CLI-40).
#![cfg(all(
    feature = "templates",
    feature = "source-csv",
    feature = "sink-jsonl"
))]

use assert_cmd::Command;

fn faucet() -> Command {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.env_remove("FAUCET_TEMPLATE_STORE");
    cmd
}

#[test]
fn help_hides_secret_env_values() {
    for (args, var, value) in [
        (
            vec!["template", "list", "--help"],
            "FAUCET_TEMPLATE_STORE",
            "postgres://u:store-pass-9@db/x",
        ),
        (
            vec!["serve", "--help"],
            "FAUCET_SERVE_AUTH_TOKEN",
            "serve-admin-token-9",
        ),
        (
            vec!["serve", "--help"],
            "FAUCET_SERVE_ADMIN_TOKEN",
            "serve-admin-token-8",
        ),
    ] {
        let out = faucet().env(var, value).args(&args).output().unwrap();
        let text = String::from_utf8_lossy(&out.stdout);
        assert!(out.status.success(), "{args:?}: {text}");
        assert!(text.contains(var), "{args:?} names the variable");
        assert!(!text.contains(value), "{args:?} printed {var}: {text}");
    }
}

#[test]
fn template_dry_run_json_masks_values_bound_from_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let store = format!("sqlite:{}", dir.path().join("t.db").display());
    let tpl = dir.path().join("t.yaml");
    std::fs::write(
        &tpl,
        format!(
            "version: 1\nname: tpl\nkind: pipeline\npipeline:\n  source: {{ type: csv, config: {{ path: {in_} }} }}\n  sink: {{ type: jsonl, config: {{ path: \"${{env:FAUCET_TEST_SINK_PATH}}\" }} }}\n",
            in_ = dir.path().join("in.csv").display()
        ),
    )
    .unwrap();
    faucet()
        .args(["template", "register"])
        .arg(&tpl)
        .args(["--store", &store, "--launch"])
        .assert()
        .success();
    let out = faucet()
        .env("FAUCET_TEST_SINK_PATH", "/tmp/sink-secret-path-77.jsonl")
        .args(["template", "run", "tpl", "--dry-run", "--json", "--store", &store])
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(out.status.success(), "{text} {}", String::from_utf8_lossy(&out.stderr));
    assert!(text.contains("\"pipeline\""), "{text}");
    assert!(!text.contains("sink-secret-path-77"), "{text}");
}

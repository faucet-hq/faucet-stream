//! `run_from_yaml_str` loads a document the way `faucet run` loads a file.
#![cfg(all(feature = "source-file", feature = "sink-file"))]

#[tokio::test]
async fn vars_resolve_and_requires_faucet_is_checked() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    std::fs::write(dir.path().join("in.jsonl"), "{\"id\":1}\n{\"id\":2}\n").unwrap();
    let yaml = format!(
        "version: 1\nname: vars_load\nvars:\n  base: \"{d}\"\npipeline:\n  source: {{ type: file, config: {{ path: \"${{vars.base}}/in.jsonl\" }} }}\n  sink: {{ type: file, config: {{ path: \"${{vars.base}}/out.jsonl\" }} }}\n"
    );
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    assert_eq!(summary.failure_count(), 0);
    let out = std::fs::read_to_string(dir.path().join("out.jsonl")).unwrap();
    assert_eq!(out.lines().count(), 2);

    let refused = format!("requires_faucet: \">=999\"\n{yaml}");
    let err = faucet_cli::run_from_yaml_str(&refused).await.unwrap_err();
    assert!(
        matches!(err, faucet_cli::CliError::IncompatibleFaucet { .. }),
        "{err:?}"
    );
}

//! The file source through the CLI config layer (#720): a JSON Lines file
//! written by one pipeline is the input of the next, and incremental mode
//! reads only a newly added file on the second run.
#![cfg(all(
    feature = "source-file",
    feature = "source-csv",
    feature = "sink-jsonl"
))]

use std::fs;

fn lines(path: &std::path::Path) -> Vec<serde_json::Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[tokio::test]
async fn a_jsonl_output_feeds_the_next_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    fs::write(dir.path().join("in.csv"), "id,name\n1,ada\n2,grace\n").unwrap();
    faucet_cli::run_from_yaml_str(&format!(
        "version: 1\nname: stage_one\npipeline:\n  source:\n    type: csv\n    config: {{ path: {d}/in.csv }}\n  sink:\n    type: jsonl\n    config: {{ path: {d}/stage/one.jsonl }}\n"
    ))
    .await
    .unwrap();
    let summary = faucet_cli::run_from_yaml_str(&format!(
        "version: 1\nname: stage_two\npipeline:\n  source:\n    type: file\n    config: {{ path: {d}/stage }}\n  sink:\n    type: jsonl\n    config: {{ path: {d}/final.jsonl }}\n"
    ))
    .await
    .unwrap();
    assert_eq!(summary.failure_count(), 0);
    let got = lines(&dir.path().join("final.jsonl"));
    assert_eq!(got, lines(&dir.path().join("stage/one.jsonl")));
    assert_eq!(got.len(), 2);
}

#[tokio::test]
async fn incremental_runs_read_only_new_files() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    fs::create_dir(dir.path().join("inbox")).unwrap();
    fs::write(dir.path().join("inbox/a.jsonl"), "{\"n\":1}\n").unwrap();
    let yaml = format!(
        "version: 1\nname: inbox\npipeline:\n  source:\n    type: file\n    config:\n      path: {d}/inbox\n      incremental: {{ by: name }}\n  sink:\n    type: jsonl\n    config: {{ path: {d}/out.jsonl, append: true }}\n  state:\n    type: file\n    config: {{ path: {d}/state }}\n"
    );
    faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    fs::write(dir.path().join("inbox/b.jsonl"), "{\"n\":2}\n").unwrap();
    faucet_cli::run_from_yaml_str(&yaml).await.unwrap();
    let got = lines(&dir.path().join("out.jsonl"));
    assert_eq!(
        got,
        vec![serde_json::json!({"n": 1}), serde_json::json!({"n": 2})]
    );
}

#[test]
fn the_registry_knows_the_file_source() {
    use faucet_cli::registry;
    assert!(
        registry::validate_source_config("file", "row", serde_json::json!({"path": "in/"})).is_ok()
    );
    assert!(
        registry::validate_source_config("file", "row", serde_json::json!({"path": " "})).is_err()
    );
    assert!(
        registry::source_descriptions()
            .iter()
            .any(|(k, _)| *k == "file")
    );
    assert!(
        registry::source_schema("file").unwrap()["properties"]
            .get("path")
            .is_some()
    );
    assert!(registry::source_supports_discover("file"));
}

#[tokio::test]
async fn discover_refuses_a_source_without_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("c.yaml");
    fs::write(
        &p,
        "version: 1\nname: d\npipeline:\n  source:\n    type: csv\n    config: { path: x.csv }\n  sink:\n    type: jsonl\n    config: { path: y.jsonl }\n",
    )
    .unwrap();
    use clap::Parser;
    let args = faucet_cli::cli::DiscoverArgs::parse_from(["discover", p.to_str().unwrap()]);
    let err = faucet_cli::commands::discover::run(args).await.unwrap_err();
    assert!(err.to_string().contains("the `file` source"), "{err}");
}

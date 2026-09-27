//! The file sink through the CLI config layer (#743): a REST source into dated
//! directories, an overwrite fan-out driven by the executor's lifecycle, and
//! the load-time refusals for writers that would share a path.
#![cfg(all(
    feature = "sink-file",
    feature = "source-file",
    feature = "source-rest"
))]

use faucet_cli::config::PipelineConfig;
use serde_json::json;
use std::path::Path;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn api(n: usize) -> MockServer {
    let server = MockServer::start().await;
    let rows: Vec<_> = (0..n)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": rows })))
        .mount(&server)
        .await;
    server
}

fn yaml(server: &MockServer, sink: &str) -> String {
    format!(
        "version: 1\nname: rest_to_file\npipeline:\n  source:\n    type: rest\n    config:\n      base_url: {}\n      path: /items\n      records_path: \"$.data[*]\"\n      pagination: {{ type: None }}\n  sink:\n    type: file\n    config:\n{sink}",
        server.uri()
    )
}

#[tokio::test]
async fn rest_into_a_dated_directory() {
    let dir = tempfile::tempdir().unwrap();
    let server = api(5).await;
    let d = dir.path().display();
    let summary = faucet_cli::run_from_yaml_str(&yaml(
        &server,
        &format!("      path: \"{d}/out/${{now.date}}/items.jsonl\"\n"),
    ))
    .await
    .unwrap();
    assert_eq!(summary.failure_count(), 0);
    let day = std::fs::read_dir(dir.path().join("out"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let name = day.file_name().unwrap().to_str().unwrap().to_string();
    assert_eq!(name.len(), 10, "{name}");
    let back = faucet_source_file::read_records(day.join("items.jsonl").to_string_lossy())
        .await
        .unwrap();
    assert_eq!(back.len(), 5);
}

#[tokio::test]
async fn overwrite_replaces_the_previous_part_set() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path().display();
    let sink = format!(
        "      path: \"{d}/snap/rows-{{part}}.jsonl\"\n      write_mode: overwrite\n      max_records_per_file: 2\n"
    );
    let big = api(6).await;
    faucet_cli::run_from_yaml_str(&yaml(&big, &sink))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_dir(dir.path().join("snap")).unwrap().count(),
        3
    );
    let small = api(1).await;
    faucet_cli::run_from_yaml_str(&yaml(&small, &sink))
        .await
        .unwrap();
    let names: Vec<String> = std::fs::read_dir(dir.path().join("snap"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names, vec!["rows-00001.jsonl".to_string()]);
}

#[test]
fn rows_sharing_a_path_are_refused_at_load() {
    let cfg = PipelineConfig::from_text(
        "version: 1\nname: dup\npipeline:\n  source:\n    type: file\n    config: { path: in/ }\n  sink:\n    type: file\n    config: { path: out/x.jsonl }\nmatrix:\n  - id: a\n  - id: b\n",
        Path::new("dup.yaml"),
    )
    .unwrap();
    let e = faucet_cli::expand::expand(&cfg).unwrap_err().to_string();
    assert!(e.contains("both write"), "{e}");

    let cfg = PipelineConfig::from_text(
        "version: 1\nname: fan\npipeline:\n  source:\n    type: file\n    config: { path: in/ }\n  sink:\n    type: file\n    config: { path: \"out/${now.date}.jsonl\" }\nmatrix:\n  - id: parent\n    sink: { config: { path: out/p.jsonl } }\n  - id: child\n    parent: parent\n",
        Path::new("fan.yaml"),
    )
    .unwrap();
    let e = faucet_cli::expand::expand(&cfg).unwrap_err().to_string();
    assert!(e.contains("per-invocation"), "{e}");

    let cfg = PipelineConfig::from_text(
        "version: 1\nname: ok\npipeline:\n  source:\n    type: file\n    config: { path: in/ }\n  sink:\n    type: file\n    config: { path: \"out/${parent.id}.jsonl\" }\nmatrix:\n  - id: parent\n    sink: { config: { path: out/p.jsonl } }\n  - id: child\n    parent: parent\n",
        Path::new("ok.yaml"),
    )
    .unwrap();
    faucet_cli::expand::expand(&cfg).unwrap();
}

#[test]
fn the_registry_knows_the_file_sink() {
    use faucet_cli::registry;
    use faucet_core::WriteMode;
    assert!(registry::validate_sink_config("file", "row", json!({"path": "out/x.jsonl"})).is_ok());
    assert!(registry::validate_sink_config("file", "row", json!({"path": "out/x.orc"})).is_err());
    assert!(
        registry::sink_descriptions()
            .iter()
            .any(|(k, _)| *k == "file")
    );
    assert!(
        registry::sink_schema("file").unwrap()["properties"]
            .get("path")
            .is_some()
    );
    assert!(registry::sink_supports_overwrite("file"));
    assert_eq!(
        registry::sink_supported_write_modes("file"),
        &[WriteMode::Append, WriteMode::Overwrite]
    );
    assert_eq!(
        registry::sink_batch_atomicity("file", &json!({"path": "x.jsonl"})),
        Some(faucet_core::BatchAtomicity::Atomic)
    );
}

#[tokio::test]
async fn the_shipped_example_validates() {
    let p = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/rest_to_file.yaml");
    let cfg = PipelineConfig::from_path(&p, None).unwrap();
    faucet_cli::expand::expand(&cfg).unwrap();
}

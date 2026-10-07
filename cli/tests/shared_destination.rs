//! #789 FILE-05: rows, root rows included, must not share a file-writing
//! sink's destination, and a fan-out row needs a per-invocation token in it —
//! for the object-store sinks with a fixed `path` as for the local file sinks.
#![cfg(all(feature = "sink-s3", feature = "sink-jsonl", feature = "source-file"))]

use faucet_cli::config::PipelineConfig;
use std::path::Path;

fn expand(sink: &str, matrix: &str) -> Result<(), String> {
    let text = format!(
        "version: 1\nname: t\npipeline:\n  source:\n    type: file\n    config: {{ path: in/ }}\n  sink:\n{sink}matrix:\n{matrix}"
    );
    let cfg = PipelineConfig::from_text(&text, Path::new("t.yaml")).map_err(|e| e.to_string())?;
    faucet_cli::expand::expand(&cfg)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

const S3_FIXED: &str =
    "    type: s3\n    config: { bucket: b, prefix: out/, path: \"part-{part}.jsonl\" }\n";

#[test]
fn root_rows_sharing_a_jsonl_path_are_refused() {
    let e = expand(
        "    type: jsonl\n    config: { path: out/x.jsonl }\n",
        "  - id: a\n  - id: b\n",
    )
    .unwrap_err();
    assert!(
        e.contains("both write") && e.contains("file:out/x.jsonl"),
        "{e}"
    );
    expand(
        "    type: jsonl\n    config: { path: out/x.jsonl, append: true }\n",
        "  - id: a\n",
    )
    .unwrap();
}

#[test]
fn root_rows_sharing_a_fixed_object_store_path_are_refused() {
    let e = expand(S3_FIXED, "  - id: a\n  - id: b\n").unwrap_err();
    assert!(e.contains("s3://b/out/part-{part}.jsonl"), "{e}");
    expand(
        S3_FIXED,
        "  - id: a\n  - id: b\n    sink: { config: { prefix: other/ } }\n",
    )
    .unwrap();
    expand(
        "    type: s3\n    config: { bucket: b, prefix: out/ }\n",
        "  - id: a\n  - id: b\n",
    )
    .unwrap();
}

#[test]
fn a_fan_out_row_on_a_fixed_object_store_path_is_refused() {
    let matrix =
        "  - id: parent\n    sink: { config: { prefix: p/ } }\n  - id: child\n    parent: parent\n";
    let e = expand(S3_FIXED, matrix).unwrap_err();
    assert!(e.contains("per-invocation"), "{e}");
    expand(
        "    type: s3\n    config: { bucket: b, prefix: \"out/${parent.id}/\", path: \"part-{part}.jsonl\" }\n",
        matrix,
    )
    .unwrap();
}

#[test]
fn the_hub_treats_a_fixed_remote_path_like_a_truncating_file() {
    use faucet_cli::hub::spec::SinkTemplate;
    let fixed: SinkTemplate = serde_yaml::from_str(
        "kind: sink-template\nname: s3\nsink:\n  type: s3\n  config: { bucket: b }\nper_stream: { path: \"${stream}.jsonl\" }\n",
    )
    .unwrap();
    assert!(fixed.truncates_per_invocation());
    let unique: SinkTemplate = serde_yaml::from_str(
        "kind: sink-template\nname: s3\nsink:\n  type: s3\n  config: { bucket: b }\nper_stream: { prefix: \"${stream}/\" }\n",
    )
    .unwrap();
    assert!(!unique.truncates_per_invocation());
}

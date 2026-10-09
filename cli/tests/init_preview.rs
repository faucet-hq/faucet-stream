//! Integration tests for the `faucet init` and `faucet preview` subcommands,
//! driven through their clap-free `run(...)` entry points (the same way the
//! binary's `main` dispatches into them).

use faucet_cli::cli::{InitArgs, PreviewArgs};
use faucet_cli::commands::{init, preview};
use faucet_cli::error::CliError;
use std::path::PathBuf;

/// Build an `InitArgs` with the schema-driven defaults the binary uses, then
/// override only what each test needs.
fn init_args(output: PathBuf, source: Option<&str>, sink: Option<&str>) -> InitArgs {
    InitArgs {
        name: Some("smoke-pipeline".to_string()),
        source: source.map(str::to_string),
        sink: sink.map(str::to_string),
        output,
        force: false,
        interactive: false,
        template: "default".to_string(),
        discover: false,
        executable: None,
        stream: None,
        no_pin: true,
    }
}

// ---------------------------------------------------------------------------
// init
// ---------------------------------------------------------------------------

#[cfg(all(feature = "source-csv", feature = "sink-jsonl"))]
#[tokio::test]
async fn init_renders_csv_to_jsonl_template_with_required_markers() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");

    init::run(init_args(out.clone(), Some("csv"), Some("jsonl")))
        .await
        .expect("init should write the scaffold");

    let body = std::fs::read_to_string(&out).unwrap();

    // Header + chosen name.
    assert!(body.starts_with("version: 1\n"), "{body}");
    assert!(body.contains("name: smoke-pipeline\n"), "{body}");

    // The named-template shape with our `default` template name.
    assert!(body.contains("  sources:\n"), "{body}");
    assert!(body.contains("    default:\n"), "{body}");
    assert!(body.contains("      type: csv\n"), "{body}");
    assert!(body.contains("  sinks:\n"), "{body}");
    assert!(body.contains("      type: jsonl\n"), "{body}");

    // Both csv source and jsonl sink have a required `path` field — the
    // schema-driven renderer surfaces it with a `# REQUIRED` marker.
    assert!(body.contains("path:"), "expected a path field: {body}");
    assert!(
        body.contains("# REQUIRED"),
        "expected a REQUIRED marker for the required path field: {body}"
    );

    // The matrix scaffold references the same template name.
    assert!(
        body.contains("source: { ref: default,"),
        "expected matrix example referencing the template: {body}"
    );
}

#[cfg(all(feature = "source-rest", feature = "sink-file"))]
#[tokio::test]
async fn init_defaults_to_rest_to_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");

    // No --source / --sink and no name → schema-driven defaults.
    let args = InitArgs {
        name: None,
        source: None,
        sink: None,
        output: out.clone(),
        force: false,
        interactive: false,
        template: "default".to_string(),
        discover: false,
        executable: None,
        stream: None,
        no_pin: true,
    };
    init::run(args).await.expect("init defaults should write");

    let body = std::fs::read_to_string(&out).unwrap();
    assert!(body.contains("name: my-pipeline\n"), "{body}");
    assert!(body.contains("      type: rest\n"), "{body}");
    assert!(body.contains("      type: file\n"), "{body}");
}

#[tokio::test]
async fn init_refuses_to_overwrite_existing_file_without_force() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    std::fs::write(&out, "pre-existing").unwrap();

    let err = init::run(init_args(out.clone(), None, None))
        .await
        .expect_err("init must not clobber an existing file");
    match err {
        CliError::ScaffoldExists { path } => assert_eq!(path, out),
        other => panic!("expected ScaffoldExists, got {other:?}"),
    }
    // The original content is untouched.
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "pre-existing");
}

#[cfg(all(feature = "source-csv", feature = "sink-jsonl"))]
#[tokio::test]
async fn init_force_overwrites_existing_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    std::fs::write(&out, "stale").unwrap();

    let mut args = init_args(out.clone(), Some("csv"), Some("jsonl"));
    args.force = true;
    init::run(args).await.expect("force should overwrite");

    let body = std::fs::read_to_string(&out).unwrap();
    assert!(body.starts_with("version: 1\n"), "{body}");
    assert!(
        !body.contains("stale"),
        "stale content not replaced: {body}"
    );
}

#[tokio::test]
async fn init_rejects_unknown_source_kind() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");

    let err = init::run(init_args(out, Some("definitely-not-a-source"), None))
        .await
        .expect_err("unknown source kind must be rejected");
    match err {
        CliError::UnknownConnector { kind, name, .. } => {
            assert_eq!(kind, "source");
            assert_eq!(name, "definitely-not-a-source");
        }
        other => panic!("expected UnknownConnector, got {other:?}"),
    }
}

#[cfg(feature = "source-csv")]
#[tokio::test]
async fn init_rejects_unknown_sink_kind() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");

    let err = init::run(init_args(out, Some("csv"), Some("definitely-not-a-sink")))
        .await
        .expect_err("unknown sink kind must be rejected");
    match err {
        CliError::UnknownConnector { kind, name, .. } => {
            assert_eq!(kind, "sink");
            assert_eq!(name, "definitely-not-a-sink");
        }
        other => panic!("expected UnknownConnector, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// preview
// ---------------------------------------------------------------------------

fn preview_args(config: Option<PathBuf>, limit: usize) -> PreviewArgs {
    PreviewArgs {
        config,
        limit,
        env_file: None,
        no_env_file: true,
        profile: None,
        selection: Default::default(),
    }
}

#[cfg(all(feature = "source-csv", feature = "sink-stdout"))]
#[tokio::test]
async fn preview_runs_first_root_csv_source() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    let cfg = dir.path().join("faucet.yaml");
    std::fs::write(&csv, "id,name\n1,alice\n2,bob\n3,carol\n").unwrap();

    let yaml = format!(
        r#"version: 1
name: preview_smoke
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  sink:
    type: stdout
    config: {{}}
"#,
        csv = csv.display(),
    );
    std::fs::write(&cfg, yaml).unwrap();

    // limit smaller than the record count: the preview path takes the first N.
    preview::run(preview_args(Some(cfg), 2))
        .await
        .expect("preview of a valid csv root should succeed");
}

#[cfg(all(
    feature = "source-csv",
    feature = "sink-stdout",
    feature = "transforms"
))]
#[tokio::test]
async fn preview_applies_transforms_before_emitting() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    let cfg = dir.path().join("faucet.yaml");
    std::fs::write(&csv, "id,name\n1,alice\n").unwrap();

    // A pipeline-level transform exercises the compile_transforms + apply path
    // inside preview (the `stages.is_empty()` false branch).
    let yaml = format!(
        r#"version: 1
name: preview_transform
pipeline:
  source:
    type: csv
    config:
      path: {csv}
  transforms:
    - type: select
      config:
        fields: [id]
  sink:
    type: stdout
    config: {{}}
"#,
        csv = csv.display(),
    );
    std::fs::write(&cfg, yaml).unwrap();

    preview::run(preview_args(Some(cfg), 10))
        .await
        .expect("preview with a transform should succeed");
}

#[cfg(feature = "sink-stdout")]
#[tokio::test]
async fn preview_unknown_source_kind_errors() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = dir.path().join("faucet.yaml");

    let yaml = r#"version: 1
name: preview_bad
pipeline:
  source:
    type: definitely-not-a-source
    config: {}
  sink:
    type: stdout
    config: {}
"#;
    std::fs::write(&cfg, yaml).unwrap();

    let err = preview::run(preview_args(Some(cfg), 10))
        .await
        .expect_err("preview of an unknown source kind must fail");
    match err {
        CliError::UnknownConnector { kind, name, .. } => {
            assert_eq!(kind, "source");
            assert_eq!(name, "definitely-not-a-source");
        }
        other => panic!("expected UnknownConnector, got {other:?}"),
    }
}

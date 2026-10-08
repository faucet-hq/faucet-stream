//! #823: `hub lint`, `hub check` and `template test` run the same typed config
//! validation as `faucet validate --source … --sink …`. The shape that slipped
//! through: a `type: int` param used as a whole `query_params` value, which
//! the REST source's config rejects ("expected a string").
#![cfg(all(feature = "source-rest", feature = "sink-jsonl"))]

use clap::Parser as _;
use faucet_cli::cli::Cli;
use std::path::Path;

async fn run(args: &[&str]) -> Result<(), faucet_cli::error::CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn source_template(page_size_type: &str) -> String {
    format!(
        r#"
kind: source-template
name: acme
description: Acme items API
params:
  page_size: {{ type: {page_size_type}, default: 100 }}
source:
  type: rest
  config:
    base_url: "https://api.example.com"
    path: /items
    query_params:
      limit: "${{param.page_size}}"
streams:
  - name: items
"#
    )
}

const SINK: &str = r#"
kind: sink-template
name: files
description: JSON Lines files
sink:
  type: jsonl
  config: {}
per_stream:
  path: "./out/${stream}.jsonl"
"#;

/// A hub with the #823 source template (`page_size` declared as
/// `page_size_type`) and a JSONL sink template.
fn hub(dir: &Path, page_size_type: &str) -> String {
    let hub = dir.join("hub");
    std::fs::create_dir_all(hub.join("source-templates")).unwrap();
    std::fs::create_dir_all(hub.join("sink-templates")).unwrap();
    std::fs::write(
        hub.join("source-templates/acme.yaml"),
        source_template(page_size_type),
    )
    .unwrap();
    std::fs::write(hub.join("sink-templates/files.yaml"), SINK).unwrap();
    hub.to_string_lossy().into_owned()
}

#[tokio::test]
async fn validate_lint_and_check_agree_on_an_int_query_param() {
    let dir = tempfile::tempdir().unwrap();
    let hub = hub(dir.path(), "int");
    let file = format!("{hub}/source-templates/acme.yaml");

    // The reference verdict.
    let err = run(&[
        "validate",
        "--source",
        "acme",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
    ])
    .await
    .expect_err("faucet validate rejects the composed config");
    assert!(err.to_string().contains("expected a string"), "{err}");

    // `hub lint` on the file and on the whole catalog — naming the param.
    let err = run(&["hub", "lint", &file])
        .await
        .expect_err("lint flags it");
    assert!(
        err.to_string().contains("1 template(s) with findings"),
        "{err}"
    );
    let err = run(&["hub", "lint", "--hub", &hub])
        .await
        .expect_err("catalog lint flags it");
    assert!(err.to_string().contains("1 template(s)"), "{err}");
    let findings = faucet_cli::hub::catalog::lint_source_all(
        &faucet_cli::hub::parse_source_file(Path::new(&file)).unwrap(),
    );
    assert!(
        findings
            .iter()
            .any(|f| f.contains("expected a string") && f.contains("param `page_size` (type: int)")),
        "{findings:?}"
    );

    // `hub check` names both files and the param.
    let err = run(&[
        "hub", "check", "--source", "acme", "--sink", "files", "--hub", &hub,
    ])
    .await
    .expect_err("check flags it");
    let msg = err.to_string();
    assert!(msg.contains("does not validate"), "{msg}");
    assert!(msg.contains("acme.yaml"), "{msg}");
    assert!(msg.contains("files.yaml"), "{msg}");
    assert!(msg.contains("param `page_size` (type: int)"), "{msg}");
    let err = run(&[
        "hub", "check", "--source", "acme", "--sink", "files", "--hub", &hub, "--json",
    ])
    .await
    .expect_err("--json still fails");
    assert!(err.to_string().contains("does not validate"), "{err}");
}

#[tokio::test]
async fn a_string_param_passes_every_check() {
    let dir = tempfile::tempdir().unwrap();
    let hub = hub(dir.path(), "string");
    run(&[
        "validate",
        "--source",
        "acme",
        "--sink",
        "files",
        "--hub",
        &hub,
        "--no-env-file",
    ])
    .await
    .expect("validate");
    run(&["hub", "lint", "--hub", &hub]).await.expect("lint");
    run(&[
        "hub", "check", "--source", "acme", "--sink", "files", "--hub", &hub,
    ])
    .await
    .expect("check");
}

#[cfg(feature = "templates")]
#[tokio::test]
async fn template_test_fails_a_case_whose_param_type_the_connector_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let template = dir.path().join("tpl.yaml");
    std::fs::write(
        &template,
        r#"
version: 1
kind: pipeline
params:
  page_size: { type: int, default: 100 }
pipeline:
  source:
    type: rest
    config:
      base_url: "https://api.example.com"
      path: /items
      query_params: { limit: "${param.page_size}" }
  sink: { type: jsonl, config: { path: ./out.jsonl } }
"#,
    )
    .unwrap();
    let suite = dir.path().join("suite.yaml");
    std::fs::write(
        &suite,
        format!(
            "version: 1\ntemplate: {}\nsuite:\n  cases:\n    - name: small-pages\n      params: {{ page_size: 10 }}\n",
            template.display()
        ),
    )
    .unwrap();
    let err = run(&["template", "test", suite.to_str().unwrap(), "--no-env-file"])
        .await
        .expect_err("the case fails");
    assert!(
        matches!(err, faucet_cli::error::CliError::TestsFailed { failed: 1 }),
        "{err}"
    );
}

//! #752: a child fan-out row writing a truncating file sink at one path kept
//! only the last parent's rows. `validate` / `run` now refuse it, the Template
//! Hub composer refuses the pairing, and `append: true` keeps every parent's
//! children.

use clap::Parser as _;
use faucet_cli::cli::Cli;
use std::path::Path;

fn on_big_stack<F>(f: impl FnOnce() -> F + Send + 'static)
where
    F: std::future::Future<Output = ()>,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(f())
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run(args: &[&str]) -> Result<(), faucet_cli::error::CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn pipeline(dir: &Path, append: bool) -> String {
    let parents = dir.join("parents.csv");
    let child = dir.join("lines.csv");
    std::fs::write(&parents, "id\n1\n2\n").unwrap();
    std::fs::write(&child, "line\na\nb\nc\n").unwrap();
    let cfg = dir.join(format!("pipeline-{append}.yaml"));
    std::fs::write(
        &cfg,
        format!(
            "version: 1\n\
             name: fanout\n\
             pipeline:\n\
             \x20 source: {{ type: csv, config: {{ path: \"{parents}\" }} }}\n\
             \x20 sink: {{ type: jsonl, config: {{ path: \"{dir}/parents.jsonl\" }} }}\n\
             matrix:\n\
             \x20 - id: bills\n\
             \x20 - id: lines\n\
             \x20   parent: bills\n\
             \x20   source: {{ config: {{ path: \"{child}\" }} }}\n\
             \x20   sink: {{ config: {{ path: \"{dir}/lines.jsonl\", append: {append} }} }}\n\
             \x20   transforms:\n\
             \x20     - {{ type: set, config: {{ values: {{ bill: \"${{bills.id}}\" }} }} }}\n",
            parents = parents.display(),
            child = child.display(),
            dir = dir.display(),
        ),
    )
    .unwrap();
    cfg.to_string_lossy().into_owned()
}

#[test]
fn a_truncating_child_sink_is_refused_and_append_keeps_every_parent() {
    on_big_stack(|| async {
        let dir = tempfile::tempdir().unwrap();
        let refused = pipeline(dir.path(), false);
        for verb in ["validate", "run"] {
            let err = run(&[verb, &refused, "--no-env-file"])
                .await
                .expect_err("truncating child sink must be refused")
                .to_string();
            assert!(
                err.contains("row 'lines' runs once per parent record"),
                "{verb}: {err}"
            );
        }
        assert!(!dir.path().join("lines.jsonl").exists());

        let ok = pipeline(dir.path(), true);
        run(&["run", &ok, "--no-env-file"]).await.expect("run");
        let body = std::fs::read_to_string(dir.path().join("lines.jsonl")).unwrap();
        let mut seen: Vec<(String, String)> = body
            .lines()
            .map(|l| {
                let v: serde_json::Value = serde_json::from_str(l).unwrap();
                (
                    v["bill"].as_str().unwrap().into(),
                    v["line"].as_str().unwrap().into(),
                )
            })
            .collect();
        seen.sort();
        let want: Vec<(String, String)> = ["1", "2"]
            .iter()
            .flat_map(|b| {
                ["a", "b", "c"]
                    .iter()
                    .map(move |l| (b.to_string(), l.to_string()))
            })
            .collect();
        assert_eq!(seen, want, "{body}");
    });
}

fn hub(dir: &Path) -> String {
    let hub = dir.join("hub");
    std::fs::create_dir_all(hub.join("source-templates")).unwrap();
    std::fs::create_dir_all(hub.join("sink-templates")).unwrap();
    std::fs::write(
        hub.join("source-templates/billing.yaml"),
        r#"
kind: source-template
name: billing
description: Bills and their lines
source:
  type: csv
  config: { path: ./bills.csv }
streams:
  - name: bills
    write: overwrite
  - name: bill_lines
    parent: bills
    parent_key: id
    source: { config: { path: ./lines.csv } }
    write: overwrite
"#,
    )
    .unwrap();
    for (name, append) in [("files", false), ("appending", true)] {
        std::fs::write(
            hub.join(format!("sink-templates/{name}.yaml")),
            format!(
                "kind: sink-template\nname: {name}\ndescription: JSON Lines\nsink:\n  type: jsonl\n  \
                 config: {{ append: {append} }}\nper_stream:\n  path: \"./out/${{stream}}.jsonl\"\n\
                 write_mode_aliases:\n  overwrite: append\n"
            ),
        )
        .unwrap();
    }
    hub.to_string_lossy().into_owned()
}

#[tokio::test]
async fn the_composer_refuses_a_child_stream_on_a_truncating_sink() {
    let dir = tempfile::tempdir().unwrap();
    let hub = hub(dir.path());
    for sink in ["files", "appending"] {
        let err = run(&[
            "hub", "compose", "--source", "billing", "--sink", sink, "--hub", &hub,
        ])
        .await
        .expect_err("child overwrite via append must be refused")
        .to_string();
        // The appending template is refused before any stream resolves: its
        // `overwrite: append` alias would re-append everything (#789 CLI-125).
        let expected = if sink == "appending" {
            "re-append the whole table"
        } else {
            "child stream 'bill_lines' (parent: bills)"
        };
        assert!(err.contains(expected), "{sink}: {err}");
    }
}

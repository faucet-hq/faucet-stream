//! `faucet init --source singer --discover` end-to-end (drives the command
//! function directly against the dependency-free fake tap — no binary spawn).

#![cfg(feature = "source-singer")]

use std::path::PathBuf;

use faucet_cli::cli::InitArgs;

fn fake_tap() -> String {
    format!(
        "{}/../crates/source/singer/tests/fake_taps/fake_tap.sh",
        env!("CARGO_MANIFEST_DIR")
    )
}

fn init_args(output: PathBuf, executable: Option<String>, discover: bool) -> InitArgs {
    init_args_stream(output, executable, discover, None)
}

fn init_args_stream(
    output: PathBuf,
    executable: Option<String>,
    discover: bool,
    stream: Option<String>,
) -> InitArgs {
    InitArgs {
        name: Some("t".into()),
        source: Some("singer".into()),
        sink: Some("jsonl".into()),
        output,
        force: true,
        interactive: false,
        template: "default".into(),
        discover,
        executable,
        stream,
        no_pin: true,
    }
}

#[tokio::test]
async fn discover_writes_catalog_and_scaffold() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    let args = init_args(out.clone(), Some(fake_tap()), true);

    faucet_cli::commands::init::run(args)
        .await
        .expect("init --discover");

    let yaml = std::fs::read_to_string(&out).unwrap();
    // Lists discovered streams and leaves stream unset.
    assert!(
        yaml.contains("Discovered streams: s, audit_log"),
        "yaml:\n{yaml}"
    );
    assert!(yaml.contains("stream: \"\""));
    // Catalog inlined as a JSON object (not a `${file:}` string).
    assert!(yaml.contains("\"streams\""));
    // Catalog also written to disk.
    let catalog = dir.path().join("catalog.json");
    assert!(catalog.exists(), "catalog.json should be written");
    let cat: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(catalog).unwrap()).unwrap();
    assert!(cat.get("streams").is_some());
}

#[tokio::test]
async fn discover_with_stream_marks_it_selected() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    // The fake tap's discovery advertises streams "s" and "audit_log".
    let args = init_args_stream(out.clone(), Some(fake_tap()), true, Some("s".into()));

    faucet_cli::commands::init::run(args)
        .await
        .expect("init --discover --stream s");

    let yaml = std::fs::read_to_string(&out).unwrap();
    // The scaffold's stream is set to the target.
    assert!(yaml.contains("stream: \"s\""), "yaml:\n{yaml}");

    // The written catalog marks stream "s" selected (stream-level flag + the
    // breadcrumb-[] metadata that DB/SDK taps require).
    let cat: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.path().join("catalog.json")).unwrap())
            .unwrap();
    let s = cat["streams"]
        .as_array()
        .unwrap()
        .iter()
        .find(|st| st["tap_stream_id"] == "s")
        .expect("stream s in catalog");
    assert_eq!(
        s["selected"],
        serde_json::json!(true),
        "stream-level selected"
    );
    let root_selected = s["metadata"].as_array().unwrap().iter().any(|m| {
        m["breadcrumb"]
            .as_array()
            .map(|b| b.is_empty())
            .unwrap_or(false)
            && m["metadata"]["selected"] == serde_json::json!(true)
    });
    assert!(root_selected, "breadcrumb-[] metadata selected: {s}");
}

#[tokio::test]
async fn discover_requires_executable() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("pipeline.yaml");
    let args = init_args(out, None, true);
    let err = faucet_cli::commands::init::run(args)
        .await
        .expect_err("must fail without --executable");
    assert!(err.to_string().contains("--executable"), "got: {err}");
}

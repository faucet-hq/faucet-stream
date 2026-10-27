//! A new source + sink template pair on an empty registry (#951): each one's
//! `tests:` names the other, so neither has a launched companion when it is
//! first tested. The bundle falls back to the companion's newest version, a
//! companion registered later re-runs the tests that waited for it, and both
//! launch with no selector anywhere. Plus a sink template whose destination
//! comes from a param (#950): it registers, composes with a one-stream
//! source, and is refused with a multi-stream one.
#![cfg(feature = "templates")]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use serde_json::json;

#[path = "support/template_server.rs"]
mod template_server;
use template_server::Server;

fn source(dir: &std::path::Path, sink_line: &str) -> String {
    std::fs::write(dir.join("invoices.csv"), "id,total\n1,10\n2,20\n").unwrap();
    std::fs::write(dir.join("payments.csv"), "id,amount\n7,5\n").unwrap();
    format!(
        "kind: source-template\nname: billing\ndescription: Billing exports\nparams:\n  data_dir: {{ type: string, default: {} }}\nsource:\n  type: csv\n  config:\n    path: \"${{param.data_dir}}/invoices.csv\"\nstreams:\n  - name: invoices\n    write: [append]\ntests:\n  {sink_line}\n  suite:\n    auto: {{ defaults_baseline: true }}\n",
        dir.display()
    )
}

fn sink(dir: &std::path::Path) -> String {
    format!(
        "kind: sink-template\nname: files\ndescription: Local JSON Lines\nparams:\n  out_dir: {{ type: string, default: {} }}\nsink:\n  type: jsonl\n  config: {{}}\nper_stream:\n  path: \"${{param.out_dir}}/${{stream}}.jsonl\"\ntests:\n  source: billing\n  suite:\n    auto: {{ defaults_baseline: true }}\n",
        dir.join("out").display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_pair_registers_tests_and_launches_with_no_selector() {
    let dir = tempfile::tempdir().unwrap();
    let s = Server::start().await;

    // The source first: its sink does not exist yet, so its tests are red and
    // say exactly why and how to fix it.
    let (code, r) = s.register(&source(dir.path(), "sink: files")).await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests"]["passed"], false, "{r}");
    assert_eq!(r["tests"]["missing_companions"], json!(["files"]), "{r}");
    let err = r["tests"]["error"].as_str().unwrap();
    assert!(
        err.contains("sink-template 'files' (`tests.sink`) is not registered")
            && err.contains("POST /v1/templates/billing/versions/1/test")
            && !err.contains("pipeline template"),
        "{err}"
    );

    // The sink: tested against billing's newest (it has no launched version),
    // and billing's waiting tests re-run.
    let (code, r) = s.register(&sink(dir.path())).await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests"]["passed"], true, "{r}");
    let notes = r["tests"]["companion_notes"].to_string();
    assert!(
        notes.contains(
            "source 'billing' has no launched version, so the bundle ran with its newest, v1"
        ),
        "{notes}"
    );
    let retested = r["companions_retested"].as_array().unwrap();
    assert_eq!(retested.len(), 1, "{r}");
    assert_eq!(retested[0]["id"], "billing");
    assert_eq!(retested[0]["passed"], true, "{r}");

    for id in ["billing", "files"] {
        let (code, r) = s
            .call(
                "POST",
                &format!("/v1/templates/{id}/launch"),
                Some(json!({ "version": 1 })),
            )
            .await;
        assert_eq!(code, 200, "{id}: {r}");
    }

    let (code, r) = s
        .call(
            "POST",
            "/v1/templates/billing/runs",
            Some(json!({ "sink": "files" })),
        )
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = s.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert_eq!(rec["invocations"][0]["status"], "succeeded", "{rec}");
    let out = std::fs::read_to_string(dir.path().join("out/invoices.jsonl")).unwrap();
    assert_eq!(out.lines().count(), 2, "{out}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_shorthand_selects_a_version_and_a_missing_companion_names_its_kind() {
    let dir = tempfile::tempdir().unwrap();
    let s = Server::start().await;
    let (code, r) = s.register(&sink(dir.path())).await;
    assert_eq!(code, 201, "{r}");
    // `files@newest` is `sink: files` + `sink_select: newest`.
    let (code, r) = s.register(&source(dir.path(), "sink: files@newest")).await;
    assert_eq!(code, 201, "{r}");
    assert_eq!(r["tests"]["passed"], true, "{r}");
    assert_eq!(r["tests"]["companions"], json!(["files@1"]), "{r}");
    assert!(
        r["tests"].get("companion_notes").is_none(),
        "an explicit selector is not a fallback: {r}"
    );

    // A companion that names the wrong kind, or none at all.
    let body = source(dir.path(), "sink: billing").replace("name: billing", "name: other");
    let (_, r) = s.register(&body).await;
    let err = r["tests"]["error"].as_str().unwrap_or_default();
    assert!(
        err.contains(
            "`sink: billing` must name a sink-template, but 'billing' is a source-template"
        ),
        "{r}"
    );
    let body = source(dir.path(), "sink: nope@newest").replace("name: billing", "name: third");
    let (_, r) = s.register(&body).await;
    let err = r["tests"]["error"].as_str().unwrap_or_default();
    assert!(err.contains("sink-template 'nope'"), "{r}");

    // A trigger naming a missing sink says which kind it looked for.
    let (code, r) = s
        .call(
            "POST",
            "/v1/templates/billing/runs",
            Some(json!({ "sink": "nope", "version": "newest" })),
        )
        .await;
    assert_eq!(code, 422, "{r}");
    assert!(
        r.to_string()
            .contains("no sink-template 'nope' in the registry"),
        "{r}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_param_only_destination_composes_with_one_stream_only() {
    let dir = tempfile::tempdir().unwrap();
    let s = Server::start().await;
    let one_file = format!(
        "kind: sink-template\nname: one-file\nparams:\n  target: {{ type: string, default: {} }}\nsink:\n  type: jsonl\n  config: {{}}\nper_stream:\n  path: \"${{param.target}}\"\n",
        dir.path().join("chosen.jsonl").display()
    );
    let (code, r) = s.register(&one_file).await;
    assert_eq!(code, 201, "a param-only per_stream registers: {r}");

    let one = source(dir.path(), "sink: one-file").replace("tests:", "untested:");
    let one = one.split("untested:").next().unwrap().to_string();
    let (code, r) = s.register(&one).await;
    assert_eq!(code, 201, "{r}");
    let (code, r) = s
        .call(
            "POST",
            "/v1/templates/billing/runs",
            Some(json!({ "sink": "one-file", "version": "newest", "sink_version": "newest" })),
        )
        .await;
    assert_eq!(code, 202, "{r}");
    let rec = s.wait_run(r["run_id"].as_str().unwrap()).await;
    assert_eq!(rec["status"], "completed", "{rec}");
    assert!(dir.path().join("chosen.jsonl").is_file());

    let two = format!(
        "{one}  - name: payments\n    source: {{ config: {{ path: \"${{param.data_dir}}/payments.csv\" }} }}\n    write: [append]\n"
    )
    .replace("name: billing", "name: ledger");
    let (code, r) = s.register(&two).await;
    assert_eq!(code, 201, "{r}");
    let (code, r) = s
        .call(
            "POST",
            "/v1/templates/ledger/runs",
            Some(json!({ "sink": "one-file", "version": "newest", "sink_version": "newest" })),
        )
        .await;
    assert_eq!(code, 422, "{r}");
    assert!(
        r.to_string().contains(
            "sink-template 'one-file': per_stream renders the same destination for streams \
             invoices and payments"
        ),
        "{r}"
    );
    let (_, m) = s.call("GET", "/v1/templates/matrix", None).await;
    let cell = m["matrix"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["source"] == "ledger" && c["sink"] == "one-file")
        .unwrap()
        .clone();
    assert_eq!(cell["compatible"], false, "{cell}");
    assert!(
        cell["collision"]
            .as_str()
            .unwrap()
            .contains("invoices and payments")
    );
}

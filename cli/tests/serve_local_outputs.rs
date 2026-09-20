//! End-to-end tests for local-output retention (#587) and its control surface
//! (#588). Boots a real RBAC server, runs csv→jsonl pipelines through
//! `POST /v1/runs`, and asserts the whole chain the feature depends on:
//!
//! 1. The **sink** reports the concrete file it opened, the executor records it,
//!    and it survives every decorator between them (a wrapper that dropped
//!    `local_outputs` would leave the file untracked and unreclaimable, with no
//!    other symptom).
//! 2. `GET /v1/local-outputs` lists it with an age and a state.
//! 3. Cleanup actually unlinks the file, keeps the **run record**, and re-renders
//!    the output as `expired`.
//! 4. The guardrail holds over HTTP: a file faucet wrote but did not create is
//!    refused, and only the named file is ever removed — never its siblings and
//!    never the directory.
//! 5. RBAC: a viewer can look but not delete.
#![cfg(all(feature = "catalog", feature = "source-csv", feature = "sink-jsonl"))]

use serde_json::Value;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// `admin-tok` → admin, `viewer-tok` → viewer (read-only).
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - name: alice\n\
    \x20   token: admin-tok\n\
    \x20   role: admin\n\
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n";

fn serve_args_with_retention(
    port: u16,
    auth_config: std::path::PathBuf,
    retention_days: u32,
) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_config),
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        // Long window: these tests drive cleanup explicitly, so the background
        // sweeper must not race them by collecting a fresh file first.
        local_output_retention_days: retention_days,
        // …and no mtime grace: every file here is written microseconds before it
        // is cleaned, so the real guard would (correctly) skip them all. The
        // guard itself is covered by unit tests in `local_outputs::sweep`; what
        // these tests exercise is the plumbing around it.
        local_output_in_flight_grace_secs: 0,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path) {
    spawn_server_with_retention(port, dir, 3650).await
}

async fn spawn_server_with_retention(port: u16, dir: &std::path::Path, retention_days: u32) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config = faucet_cli::serve::ServeConfig::from_args(serve_args_with_retention(
        port,
        auth_path,
        retention_days,
    ))
    .unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become healthy on port {port}");
}

/// Submit a csv→jsonl run and wait for it to complete. Returns the run id.
async fn run_pipeline(base: &str, client: &reqwest::Client, input: &str, output: &str) -> String {
    let config = format!(
        "version: 1\nname: lo-e2e\npipeline:\n  source: {{ type: csv, config: {{ path: {input} }} }}\n  sink: {{ type: jsonl, config: {{ path: {output} }} }}\n",
    );
    let resp = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "config": config }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        202,
        "{}",
        resp.text().await.unwrap()
    );
    let body: Value = resp.json().await.unwrap();
    let run_id = body["run_id"].as_str().unwrap().to_string();

    for _ in 0..200 {
        let rec: Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .bearer_auth("admin-tok")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        match rec["status"].as_str().unwrap() {
            "completed" => return run_id,
            "failed" | "cancelled" => panic!("run finished {rec}"),
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("run did not complete in time");
}

async fn list_outputs(base: &str, client: &reqwest::Client, query: &str) -> Value {
    client
        .get(format!("{base}/v1/local-outputs?{query}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Find the listed output whose path ends with `suffix`.
fn find<'a>(list: &'a Value, suffix: &str) -> Option<&'a Value> {
    list["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["path"].as_str().unwrap().ends_with(suffix))
}

#[tokio::test(flavor = "multi_thread")]
async fn records_lists_and_cleans_a_local_output_leaving_the_run_record() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // Unauthenticated → 401, before anything else.
    assert_eq!(
        client
            .get(format!("{base}/v1/local-outputs"))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        401
    );

    // Before any run: an empty list, not an error.
    let empty = list_outputs(&base, &client, "").await;
    assert_eq!(empty["outputs"].as_array().unwrap().len(), 0);
    assert_eq!(empty["retention_days"], 3650);
    assert_eq!(empty["gc_enabled"], true);

    std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
    let run_id = run_pipeline(
        &base,
        &client,
        &input.display().to_string(),
        &output.display().to_string(),
    )
    .await;
    assert!(output.exists(), "the pipeline should have written the file");

    // 1+2) The sink's file reached the ledger through the whole decorator chain.
    let listed = list_outputs(&base, &client, "").await;
    let row = find(&listed, "out.jsonl").expect("the written file must be tracked");
    assert_eq!(row["kind"], "jsonl");
    assert_eq!(row["state"], "present");
    assert_eq!(row["pre_existing"], false);
    assert_eq!(row["pipeline"], "lo-e2e");
    assert_eq!(row["retention_days_effective"], 3650);
    assert!(row["age_secs"].as_u64().unwrap() < 600);
    let id = row["id"].as_str().unwrap().to_string();

    // 3) Delete it: the file goes, the report says so.
    let report: Value = client
        .delete(format!("{base}/v1/local-outputs/{id}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 1, "{report}");
    assert!(report["bytes"].as_u64().unwrap() > 0);
    assert!(!output.exists(), "the file must actually be unlinked");
    assert!(
        dir.path().exists() && input.exists(),
        "nothing else in the directory may be touched"
    );

    // The record survives as `expired` — visible only when asked for, so the
    // default list is "what local data exists".
    assert!(
        find(&list_outputs(&base, &client, "").await, "out.jsonl").is_none(),
        "a cleaned output is not in the default list"
    );
    let with_expired = list_outputs(&base, &client, "include_expired=true").await;
    let gone = find(&with_expired, "out.jsonl").expect("the record is kept");
    assert_eq!(gone["state"], "expired");
    assert!(gone["deleted_at"].is_string());

    // …and the run history is untouched: data artifacts are disposable, the
    // record of what ran is not.
    let rec: Value = client
        .get(format!("{base}/v1/runs/{run_id}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rec["status"], "completed", "{rec}");

    // A second delete is a no-op that explains itself rather than an error.
    let again: Value = client
        .delete(format!("{base}/v1/local-outputs/{id}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(again["deleted"], 0);
    assert_eq!(again["outputs"][0]["skipped"], "already_deleted");

    // An unknown id is a 404, distinguishable from a refusal.
    assert_eq!(
        client
            .delete(format!("{base}/v1/local-outputs/deadbeefdeadbeef"))
            .bearer_auth("admin-tok")
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        404
    );

    // Re-running the pipeline re-creates the file and un-expires the record —
    // the console must stop showing it as cleaned.
    run_pipeline(
        &base,
        &client,
        &input.display().to_string(),
        &output.display().to_string(),
    )
    .await;
    let again = list_outputs(&base, &client, "").await;
    let row = find(&again, "out.jsonl").expect("re-written file is tracked again");
    assert_eq!(row["state"], "present");
    assert_eq!(
        row["pre_existing"], false,
        "a rerun over faucet's own output must not reclassify it as external"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn never_deletes_a_file_faucet_did_not_create() {
    // The guardrail, over HTTP: the sink is pointed at a file that already
    // exists, so faucet wrote it but did not create it. No scope may delete it —
    // not "clean all", not an explicit single-output delete.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("theirs.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    std::fs::write(&output, "{\"pre\":\"existing\"}\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &input.display().to_string(),
        &output.display().to_string(),
    )
    .await;

    let listed = list_outputs(&base, &client, "").await;
    let row = find(&listed, "theirs.jsonl").expect("still tracked, so a user can see why");
    assert_eq!(row["pre_existing"], true);
    assert_eq!(row["state"], "external");
    let id = row["id"].as_str().unwrap().to_string();

    // Explicit single-output delete → refused, with the reason.
    let report: Value = client
        .delete(format!("{base}/v1/local-outputs/{id}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 0, "{report}");
    assert_eq!(report["outputs"][0]["skipped"], "pre_existing");
    assert!(output.exists(), "the file must survive");

    // "Clean all" → same refusal.
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "all": true, "confirm": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 0, "{report}");
    assert!(output.exists(), "clean-all must not delete it either");
}

#[tokio::test(flavor = "multi_thread")]
async fn bulk_cleanup_scopes_and_dry_run() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let a = dir.path().join("a.jsonl");
    let b = dir.path().join("b.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let input_s = input.display().to_string();

    run_pipeline(&base, &client, &input_s, &a.display().to_string()).await;
    run_pipeline(&base, &client, &input_s, &b.display().to_string()).await;
    assert!(a.exists() && b.exists());

    // A scopeless request is refused rather than defaulting to something wide.
    let bad = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 400);

    // Combining scopes is likewise refused, not guessed at.
    let ambiguous = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "all": true, "older_than_days": 30 }))
        .send()
        .await
        .unwrap();
    assert_eq!(ambiguous.status().as_u16(), 400);

    // Nothing is old enough for an age-bounded purge.
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "older_than_days": 1 }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 0, "{report}");
    assert!(a.exists() && b.exists());

    // A dry run reports what `all` would remove without touching anything.
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "all": true, "dry_run": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["deleted"], 2, "{report}");
    assert!(a.exists() && b.exists(), "a dry run deletes nothing");

    // Scoped to one dataset: only that dataset's file goes.
    let listed = list_outputs(&base, &client, "").await;
    let dataset_a = find(&listed, "a.jsonl").unwrap()["dataset_id"]
        .as_str()
        .unwrap()
        .to_string();
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "dataset_id": dataset_a }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 1, "{report}");
    assert!(!a.exists(), "the scoped file is gone");
    assert!(b.exists(), "another dataset's file must survive");

    // Then the rest.
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "all": true, "confirm": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 1, "{report}");
    assert!(!b.exists());
    assert!(
        dir.path().exists() && input.exists(),
        "clean-all removes recorded files only — never the directory or the input"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unconfirmed_unbounded_scope_is_refused() {
    // The gate the CLI spells `--yes`, on the API path. A scripted caller must
    // not inherit the console's confirm dialog by accident — and the two scopes
    // that ignore retention windows (`all`, and a zero-day age which matches
    // everything) go through the same predicate.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &input.display().to_string(),
        &output.display().to_string(),
    )
    .await;

    for body in [
        serde_json::json!({ "all": true }),
        serde_json::json!({ "older_than_days": 0 }),
    ] {
        let resp = client
            .post(format!("{base}/v1/local-outputs/cleanup"))
            .bearer_auth("admin-tok")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 400, "{body} should be refused");
        assert!(
            output.exists(),
            "nothing may be deleted by a refused request"
        );
    }

    // A dry run needs no confirmation: it deletes nothing.
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "all": true, "dry_run": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["deleted"], 1, "{report}");
    assert!(output.exists());

    // With the confirmation it proceeds.
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "older_than_days": 0, "confirm": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 1, "{report}");
    assert!(!output.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn cleaning_up_after_one_run_leaves_another_runs_output_alone() {
    // "Immediate" cleanup, run-scoped: remove what that run wrote, and nothing
    // else — and leave its history record intact.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let first = dir.path().join("first.jsonl");
    let second = dir.path().join("second.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let input_s = input.display().to_string();

    let run_a = run_pipeline(&base, &client, &input_s, &first.display().to_string()).await;
    run_pipeline(&base, &client, &input_s, &second.display().to_string()).await;

    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "run_id": run_a }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 1, "{report}");
    assert_eq!(report["scope"], "run");
    assert!(!first.exists(), "that run's output is gone");
    assert!(second.exists(), "another run's output must survive");

    // The run record itself is untouched.
    let rec: Value = client
        .get(format!("{base}/v1/runs/{run_a}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rec["status"], "completed", "{rec}");
}

#[tokio::test(flavor = "multi_thread")]
async fn disabling_the_sweeper_still_allows_on_demand_cleanup() {
    // `--local-output-retention-days 0` turns the background sweep off. The
    // documented promise is that outputs are *still tracked* and can *still* be
    // cleaned on demand — so this pins the claim rather than leaving it to the
    // docs, and checks the console gets `gc_enabled: false` to render it with.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let port = free_port();
    spawn_server_with_retention(port, dir.path(), 0).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &input.display().to_string(),
        &output.display().to_string(),
    )
    .await;

    let listed = list_outputs(&base, &client, "").await;
    assert_eq!(listed["gc_enabled"], false);
    assert_eq!(listed["retention_days"], 0);
    let row = find(&listed, "out.jsonl").expect("outputs are still tracked with the GC off");
    assert_eq!(row["state"], "present");
    assert!(
        row["retention_days_effective"].is_null(),
        "a zero window means never expires, not expires immediately"
    );

    // Nothing is ever "expired" under a zero window…
    let report: Value = client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "expired": true }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 0, "{report}");
    assert!(output.exists());

    // …but an explicit delete still works, which is the documented escape hatch.
    let id = row["id"].as_str().unwrap();
    let report: Value = client
        .delete(format!("{base}/v1/local-outputs/{id}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["deleted"], 1, "{report}");
    assert!(!output.exists());
}

#[tokio::test(flavor = "multi_thread")]
async fn destructive_actions_are_recorded_in_the_audit_log() {
    // A wipe with no attributable trace is the gap an audit log exists to close,
    // and these are the only endpoints that delete a user's data off disk.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let a = dir.path().join("a.jsonl");
    let b = dir.path().join("b.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let input_s = input.display().to_string();

    run_pipeline(&base, &client, &input_s, &a.display().to_string()).await;
    run_pipeline(&base, &client, &input_s, &b.display().to_string()).await;

    let id = find(&list_outputs(&base, &client, "").await, "a.jsonl").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .delete(format!("{base}/v1/local-outputs/{id}"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap();
    client
        .post(format!("{base}/v1/local-outputs/cleanup"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "all": true, "confirm": true }))
        .send()
        .await
        .unwrap();

    let audit: Value = client
        .get(format!("{base}/v1/audit"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let actions: Vec<&str> = audit["entries"]
        .as_array()
        .expect("audit entries")
        .iter()
        .map(|e| e["action"].as_str().unwrap())
        .collect();
    assert!(
        actions.contains(&"local_output.delete"),
        "a per-output delete must be attributable: {actions:?}"
    );
    assert!(
        actions.contains(&"local_output.cleanup"),
        "a bulk wipe must be attributable: {actions:?}"
    );
    // …and the record says what happened, so a wipe is distinguishable from a no-op.
    let wipe = audit["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["action"] == "local_output.cleanup")
        .unwrap();
    assert!(
        wipe["result"].as_str().unwrap().starts_with("deleted="),
        "{wipe}"
    );
    assert_eq!(wipe["principal"], "alice");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_viewer_can_look_but_not_delete() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n").unwrap();
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &input.display().to_string(),
        &output.display().to_string(),
    )
    .await;

    // A viewer's list works and tells the console not to render the controls.
    let listed: Value = client
        .get(format!("{base}/v1/local-outputs"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["can_manage"], false);
    let id = find(&listed, "out.jsonl").unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // …and every destructive route is refused.
    assert_eq!(
        client
            .delete(format!("{base}/v1/local-outputs/{id}"))
            .bearer_auth("viewer-tok")
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403
    );
    assert_eq!(
        client
            .post(format!("{base}/v1/local-outputs/cleanup"))
            .bearer_auth("viewer-tok")
            .json(&serde_json::json!({ "all": true, "confirm": true }))
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        403
    );
    assert!(output.exists(), "nothing was deleted");

    // An admin sees the manage flag set.
    let listed = list_outputs(&base, &client, "").await;
    assert_eq!(listed["can_manage"], true);
}

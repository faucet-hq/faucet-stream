//! End-to-end tests for the dataset preview of local sink outputs (#586).
//!
//! Boots a real RBAC server, runs a csv→jsonl pipeline through `POST /v1/runs`,
//! and asserts the whole chain the feature depends on:
//!
//! 1. The **gate**: a server started without `--preview-local-outputs` refuses
//!    with a `403` that names the flag, and its `GET /v1/local-outputs` says
//!    `preview_enabled: false` so a client renders no control.
//! 2. The **read**: with the flag on, the rows the sink wrote come back with
//!    their columns, through the same source connector a pipeline would use.
//! 3. The **caps**: `row_count_to_load` overrides the soft cap and is clamped to
//!    the hard cap, a capped read reports `truncated` + `capped_by`, and
//!    `row_count_to_load=all` reads the whole dataset on a server whose operator
//!    lifted the ceiling — but is clamped by one that did not.
//! 4. The **input policy**: the request names a ledger id; an unknown one is a
//!    404, and a csv output previews as csv (the kind comes from the ledger, not
//!    the caller).
//! 5. **Retention interaction (#587)**: once an output is cleaned, previewing it
//!    is a `409` explaining that the file is gone and the run record is kept —
//!    never a 500 from a failed open.
//! 6. **Every readable format is actually read back**, parquet included — and in
//!    the rolled multi-part shape the handler is built for, where one UUID-named
//!    part is one ledger row. A spec-inspection test cannot catch an Arrow
//!    regression; only a real read can.
#![cfg(all(
    feature = "catalog",
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "sink-csv"
))]

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

/// Server args with the preview knobs under test.
fn serve_args(
    port: u16,
    auth_config: std::path::PathBuf,
    preview: bool,
    default_rows: usize,
    max_rows: usize,
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
        // A long window so the background sweeper cannot collect a file these
        // tests are about to preview.
        local_output_retention_days: 3650,
        local_output_in_flight_grace_secs: 0,
        preview_local_outputs: preview,
        preview_default_rows: default_rows,
        preview_max_rows: max_rows,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
    }
}

async fn spawn_server(
    port: u16,
    dir: &std::path::Path,
    preview: bool,
    default_rows: usize,
    max_rows: usize,
) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config = faucet_cli::serve::ServeConfig::from_args(serve_args(
        port,
        auth_path,
        preview,
        default_rows,
        max_rows,
    ))
    .unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    // 30s: this polls a server starting up next to the whole
    // workspace test suite, so the budget has to survive a loaded
    // runner. Costs nothing when it is already up.
    for _ in 0..1200 {
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

/// Submit a run and wait for it to finish.
async fn run_pipeline(base: &str, client: &reqwest::Client, config: &str) {
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
    for _ in 0..400 {
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
            "completed" => return,
            "failed" | "cancelled" => panic!("run finished {rec}"),
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("run did not complete in time");
}

fn csv_to(sink_kind: &str, input: &str, output: &str) -> String {
    format!(
        "version: 1\nname: preview-e2e\npipeline:\n  \
         source: {{ type: csv, config: {{ path: {input} }} }}\n  \
         sink: {{ type: {sink_kind}, config: {{ path: {output} }} }}\n",
    )
}

async fn list_outputs(base: &str, client: &reqwest::Client) -> Value {
    client
        .get(format!("{base}/v1/local-outputs"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// The ledger id of the listed output whose path ends with `suffix`.
fn id_of(list: &Value, suffix: &str) -> String {
    list["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|o| o["path"].as_str().unwrap().ends_with(suffix))
        .unwrap_or_else(|| panic!("no tracked output ending in {suffix}: {list}"))["id"]
        .as_str()
        .unwrap()
        .to_string()
}

async fn preview(
    base: &str,
    client: &reqwest::Client,
    token: &str,
    id: &str,
    query: &str,
) -> (u16, Value) {
    let resp = client
        .get(format!("{base}/v1/local-outputs/{id}/preview?{query}"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let body: Value = resp.json().await.unwrap_or(Value::Null);
    (status, body)
}

/// A 20-row csv, so a capped preview has something to leave behind.
fn write_input(path: &std::path::Path, rows: usize) {
    let mut body = String::from("id,name\n");
    for i in 0..rows {
        body.push_str(&format!("{i},name-{i}\n"));
    }
    std::fs::write(path, body).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn previews_a_jsonl_output_and_honours_both_caps() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    write_input(&input, 20);

    let port = free_port();
    // Soft cap 5, hard cap 8 — small enough that both are observable.
    spawn_server(port, dir.path(), true, 5, 8).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "jsonl",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;
    assert!(output.exists());

    // The list advertises the capability and the caps, so a client can render an
    // honest control instead of guessing.
    let listed = list_outputs(&base, &client).await;
    assert_eq!(listed["preview_enabled"], true);
    assert_eq!(listed["preview_default_rows"], 5);
    assert_eq!(listed["preview_max_rows"], 8);
    let id = id_of(&listed, "out.jsonl");

    // Omitted row_count_to_load → the soft cap, and 20 rows behind a cap of 5
    // must report truncation.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["row_limit"], 5);
    assert_eq!(body["max_rows"], 8);
    assert_eq!(body["rows"].as_array().unwrap().len(), 5);
    assert_eq!(body["row_count"], 5);
    assert_eq!(body["truncated"], true);
    assert_eq!(body["capped_by"], "rows");
    assert_eq!(body["kind"], "jsonl");
    assert_eq!(body["pipeline"], "preview-e2e");
    assert_eq!(
        body["columns"],
        serde_json::json!(["id", "name"]),
        "columns are the header the console renders"
    );
    // The rows are the rows the sink wrote, in file order.
    assert_eq!(body["rows"][0]["id"], "0");
    assert_eq!(body["rows"][0]["name"], "name-0");
    assert_eq!(body["rows"][4]["id"], "4");
    assert_eq!(body["path"], output.display().to_string());

    // A request above the hard cap is clamped, never honoured.
    let (status, body) = preview(
        &base,
        &client,
        "admin-tok",
        &id,
        "row_count_to_load=1000000",
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["row_limit"], 8, "clamped to the hard cap");
    assert_eq!(body["rows"].as_array().unwrap().len(), 8);
    assert_eq!(body["truncated"], true);

    // A request below the soft cap is honoured as-is.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "row_count_to_load=2").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 2);

    // `all` cannot argue its way past a configured ceiling — that is the entire
    // reason the hard cap exists.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "row_count_to_load=all").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["row_limit"], 8,
        "clamped to the ceiling, not unlimited"
    );
    assert_eq!(body["rows"].as_array().unwrap().len(), 8);
    assert_eq!(body["truncated"], true);
    assert_eq!(body["capped_by"], "rows");

    // A nonsense row count is a 400 naming the parameter, never a silent fall
    // back to the default — a capped read must not be able to pass for a whole
    // file because the client typo'd.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "row_count_to_load=lots").await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("row_count_to_load"),
        "{body}"
    );

    // A viewer may read a preview (LocalOutputRead) — the gate on this endpoint
    // is the server flag, not the role ladder.
    let (status, _) = preview(&base, &client, "viewer-tok", &id, "").await;
    assert_eq!(status, 200);

    // Unauthenticated is still a 401.
    let resp = client
        .get(format!("{base}/v1/local-outputs/{id}/preview"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 401);

    // An id the ledger does not know is a 404, not an empty preview.
    let (status, _) = preview(&base, &client, "admin-tok", "0000000000000000", "").await;
    assert_eq!(status, 404);
}

#[tokio::test(flavor = "multi_thread")]
async fn previews_a_csv_output_through_the_csv_source() {
    // The kind comes from the ledger row, so a csv sink's output is read back by
    // the csv source — with its header row as column names, not as data.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.csv");
    write_input(&input, 4);

    let port = free_port();
    spawn_server(port, dir.path(), true, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "csv",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;

    let id = id_of(&list_outputs(&base, &client).await, "out.csv");
    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["kind"], "csv");
    assert_eq!(body["rows"].as_array().unwrap().len(), 4);
    assert_eq!(body["row_count"], 4);
    assert_eq!(body["truncated"], false);
    assert_eq!(
        body["capped_by"],
        Value::Null,
        "a complete read must not claim to be capped"
    );
    assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(body["rows"][0]["name"], "name-0");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_with_no_ceiling_serves_the_whole_dataset() {
    // `--preview-max-rows 0` is how an operator says "a preview may read the
    // whole file". With it, `row_count_to_load=all` really means all — and the
    // default soft cap still applies when nothing is asked for, so "everything"
    // stays an explicit request rather than the accidental behaviour.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    write_input(&input, 1_500);

    let port = free_port();
    spawn_server(port, dir.path(), true, 500, 0).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "jsonl",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;

    let listed = list_outputs(&base, &client).await;
    assert_eq!(
        listed["preview_max_rows"],
        Value::Null,
        "null is how the console learns it can offer a real 'all rows'"
    );
    assert_eq!(listed["preview_default_rows"], 500);
    let id = id_of(&listed, "out.jsonl");

    // Nothing asked for → still the soft cap, still honest about the remainder.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 500);
    assert_eq!(body["truncated"], true);
    assert_eq!(body["capped_by"], "rows");

    // …and `all` returns every row, with nothing claiming to be capped.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "row_count_to_load=all").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1_500);
    assert_eq!(body["row_count"], 1_500);
    assert_eq!(body["row_limit"], Value::Null, "null = unlimited");
    assert_eq!(body["truncated"], false);
    assert_eq!(body["capped_by"], Value::Null);
    assert_eq!(body["rows"][1_499]["id"], "1499");

    // `0` is the same request as `all`.
    let (status, body) = preview(&base, &client, "admin-tok", &id, "row_count_to_load=0").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 1_500);
}

/// A csv → parquet pipeline. `dest` is a file for single-file mode, or a
/// directory when `max_rows_per_file` rolls parts over.
#[cfg(all(feature = "source-parquet", feature = "sink-parquet"))]
fn csv_to_parquet(input: &str, dest: &str, max_rows_per_file: Option<usize>) -> String {
    let rollover = match max_rows_per_file {
        // `batch_size` matters as much as the threshold: rollover is evaluated
        // after each batch, so a single 10-row page would otherwise land in one
        // file regardless of `max_rows_per_file`. Slicing the page is what makes
        // the sink actually roll.
        Some(n) => format!(", max_rows_per_file: {n}, batch_size: {n}"),
        None => String::new(),
    };
    format!(
        "version: 1\nname: preview-e2e\npipeline:\n  \
         source: {{ type: csv, config: {{ path: {input} }} }}\n  \
         sink: {{ type: parquet, config: {{ destination: {{ type: local_path, path: {dest} }}{rollover} }} }}\n",
    )
}

#[cfg(all(feature = "source-parquet", feature = "sink-parquet"))]
#[tokio::test(flavor = "multi_thread")]
async fn previews_a_parquet_output_through_the_parquet_source() {
    // Finding #3 of the post-merge review: parquet was only ever checked by
    // inspecting the generated spec JSON, so an Arrow read-back regression would
    // have shipped green. This reads the file.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.parquet");
    write_input(&input, 6);

    let port = free_port();
    spawn_server(port, dir.path(), true, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to_parquet(
            &input.display().to_string(),
            &output.display().to_string(),
            None,
        ),
    )
    .await;
    assert!(output.exists(), "the sink should have written the file");

    let id = id_of(&list_outputs(&base, &client).await, "out.parquet");
    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["kind"], "parquet");
    assert_eq!(body["rows"].as_array().unwrap().len(), 6);
    assert_eq!(body["row_count"], 6);
    assert_eq!(body["truncated"], false);
    assert_eq!(body["capped_by"], Value::Null);
    assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
    // Values come back through Arrow, so this also pins the decode.
    assert_eq!(body["rows"][0]["name"], "name-0");
    assert_eq!(body["rows"][5]["name"], "name-5");
}

#[cfg(all(feature = "source-parquet", feature = "sink-parquet"))]
#[tokio::test(flavor = "multi_thread")]
async fn previews_each_part_of_a_rolled_parquet_output_independently() {
    // The shape the handler's `local_path` (never a glob) choice exists for: a
    // rolling parquet sink names each part with a fresh UUID and the ledger
    // records one row per part, so each preview must read exactly its own part —
    // not the directory, and not the other parts.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let out_dir = dir.path().join("parts");
    std::fs::create_dir_all(&out_dir).unwrap();
    write_input(&input, 10);

    let port = free_port();
    spawn_server(port, dir.path(), true, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to_parquet(
            &input.display().to_string(),
            &out_dir.display().to_string(),
            Some(4),
        ),
    )
    .await;

    let listed = list_outputs(&base, &client).await;
    let parts: Vec<&Value> = listed["outputs"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|o| o["path"].as_str().unwrap().ends_with(".parquet"))
        .collect();
    assert!(
        parts.len() > 1,
        "expected the sink to roll over into several parts, got {}: {listed}",
        parts.len()
    );

    // Every part previews on its own, and the parts together account for all 10
    // rows — which is what proves each read was scoped to one file.
    let mut total = 0usize;
    for part in &parts {
        let id = part["id"].as_str().unwrap();
        let (status, body) = preview(&base, &client, "admin-tok", id, "").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["kind"], "parquet");
        let n = body["rows"].as_array().unwrap().len();
        assert!(
            n > 0 && n <= 4,
            "part should hold 1..=4 rows, got {n}: {body}"
        );
        assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
        total += n;
    }
    assert_eq!(
        total, 10,
        "the parts must partition the dataset, not repeat it"
    );
}

#[cfg(feature = "compression")]
#[tokio::test(flavor = "multi_thread")]
async fn previews_a_compressed_output_by_decompressing_it() {
    // Both sides auto-detect from the `.gz` suffix — the jsonl sink writes gzip,
    // the preview reader decompresses — so a compressed output must round-trip.
    // Untested until now, which meant a compressed preview could have been
    // silently reported as a malformed file.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl.gz");
    write_input(&input, 5);

    let port = free_port();
    spawn_server(port, dir.path(), true, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "jsonl",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;
    // Confirm it really is gzip, or the test proves nothing about decompression.
    let bytes = std::fs::read(&output).unwrap();
    assert_eq!(&bytes[..2], &[0x1f, 0x8b], "sink should have written gzip");

    let id = id_of(&list_outputs(&base, &client).await, "out.jsonl.gz");
    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["rows"].as_array().unwrap().len(), 5);
    assert_eq!(body["columns"], serde_json::json!(["id", "name"]));
    assert_eq!(body["rows"][4]["name"], "name-4");
    assert_eq!(body["truncated"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_output_is_unprocessable_rather_than_a_server_error() {
    // A run that died mid-flush leaves a half-written last line. The file is
    // there, so this is a fact about the data, not a server fault.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    write_input(&input, 3);

    let port = free_port();
    spawn_server(port, dir.path(), true, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "jsonl",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;
    let id = id_of(&list_outputs(&base, &client).await, "out.jsonl");

    // Truncate the last record the way an interrupted flush would.
    let mut body = std::fs::read_to_string(&output).unwrap();
    body.push_str("{\"id\": \"9\", \"name\"");
    std::fs::write(&output, body).unwrap();

    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 422, "{body}");
    let message = body["error"]["message"].as_str().unwrap();
    assert!(message.contains("line 4"), "names the bad line: {message}");
    assert!(message.contains("out.jsonl"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cleaned_output_previews_as_a_conflict_not_a_server_error() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    write_input(&input, 3);

    let port = free_port();
    spawn_server(port, dir.path(), true, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "jsonl",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;
    let id = id_of(&list_outputs(&base, &client).await, "out.jsonl");

    // Reclaim the file the way the Datasets page's "Delete now" does.
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

    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 409, "{body}");
    let message = body["error"]["message"].as_str().unwrap();
    assert!(message.contains("cleaned up"), "{message}");
    assert!(
        message.contains("run record is kept"),
        "the message must say the record survived: {message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_without_the_flag_refuses_and_says_which_flag() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    write_input(&input, 3);

    let port = free_port();
    // The default posture: previews off.
    spawn_server(port, dir.path(), false, 100, 1000).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    run_pipeline(
        &base,
        &client,
        &csv_to(
            "jsonl",
            &input.display().to_string(),
            &output.display().to_string(),
        ),
    )
    .await;

    let listed = list_outputs(&base, &client).await;
    assert_eq!(
        listed["preview_enabled"], false,
        "a client must not render a control that cannot work"
    );
    let id = id_of(&listed, "out.jsonl");

    let (status, body) = preview(&base, &client, "admin-tok", &id, "").await;
    assert_eq!(status, 403, "{body}");
    let message = body["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("--preview-local-outputs"),
        "a refusal with no next step is useless: {message}"
    );
    // Even an admin is refused: this is a server capability, not a permission.
    assert_eq!(body["error"]["code"], "forbidden");
}

//! Integration tests for the `/v1/catalog/*` control-plane endpoints (#279).
//! Boots a real RBAC server, runs a pipeline through `POST /v1/runs`, and
//! asserts the Data Movement Catalog accumulated the run: dataset list /
//! detail (schema timeline + stats + edges) and the lineage graph — plus RBAC
//! (a viewer can read the catalog; an unauthenticated caller cannot).
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

fn serve_args(port: u16, auth_config: std::path::PathBuf) -> faucet_cli::cli::ServeArgs {
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
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
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
        templates_sync: None,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn spawn_server(port: u16, dir: &std::path::Path) {
    let auth_path = dir.join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config =
        faucet_cli::serve::ServeConfig::from_args(serve_args(port, auth_path)).unwrap();
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

/// Submit a csv→jsonl run and wait for it to complete.
async fn run_pipeline(base: &str, client: &reqwest::Client, input: &str, output: &str) {
    let config = format!(
        "version: 1\nname: cat-e2e\npipeline:\n  source: {{ type: csv, config: {{ path: {input} }} }}\n  sink: {{ type: jsonl, config: {{ path: {output} }} }}\n",
    );
    run_config(base, client, &config).await;
}

/// Submit any config and wait for it to complete.
async fn run_config(base: &str, client: &reqwest::Client, config: &str) {
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
            "completed" => return,
            "failed" | "cancelled" => panic!("run finished {rec}"),
            _ => tokio::time::sleep(Duration::from_millis(25)).await,
        }
    }
    panic!("run did not complete in time");
}

#[tokio::test(flavor = "multi_thread")]
async fn catalog_endpoints_accumulate_runs_and_enforce_rbac() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let input_s = input.display().to_string();
    let output_s = output.display().to_string();

    // Unauthenticated catalog read → 401.
    let unauth = client
        .get(format!("{base}/v1/catalog/datasets"))
        .send()
        .await
        .unwrap();
    assert_eq!(unauth.status().as_u16(), 401);

    // Before any run: an empty catalog, not an error.
    let empty: Value = client
        .get(format!("{base}/v1/catalog/datasets"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(empty["datasets"].as_array().unwrap().len(), 0);

    // Run 1, then run 2 with an added column (schema change).
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();
    run_pipeline(&base, &client, &input_s, &output_s).await;
    std::fs::write(&input, "id,name,email\n1,alice,a@x.io\n2,bob,b@x.io\n").unwrap();
    run_pipeline(&base, &client, &input_s, &output_s).await;

    // A viewer can list the datasets (source + sink).
    let page: Value = client
        .get(format!("{base}/v1/catalog/datasets"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let datasets = page["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 2, "{page}");
    let csv = datasets
        .iter()
        .find(|d| d["kind"] == "csv")
        .expect("csv source dataset");
    assert_eq!(csv["runs"], 2);
    assert_eq!(csv["schema_versions"], 2);

    // Kind filter narrows the list.
    let filtered: Value = client
        .get(format!("{base}/v1/catalog/datasets?kind=jsonl"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(filtered["datasets"].as_array().unwrap().len(), 1);

    // Detail: schema timeline (2 entries, the second with a diff), stats, edge.
    let id = csv["id"].as_str().unwrap();
    let detail: Value = client
        .get(format!("{base}/v1/catalog/datasets/{id}"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let timeline = detail["schema_timeline"].as_array().unwrap();
    assert_eq!(timeline.len(), 2, "{detail}");
    assert!(timeline[0].get("diff").is_none());
    assert_eq!(timeline[1]["diff"]["added"][0]["column"], "email");
    assert_eq!(detail["stats"].as_array().unwrap().len(), 2);
    assert_eq!(detail["downstream"].as_array().unwrap().len(), 1);

    // Unknown dataset → handler 404 with the error envelope.
    let missing = client
        .get(format!("{base}/v1/catalog/datasets/nope"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status().as_u16(), 404);
    assert!(missing.text().await.unwrap().contains("\"error\""));

    // Lineage: whole graph and the rooted slice both return the one edge.
    let graph: Value = client
        .get(format!("{base}/v1/catalog/lineage"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let edges = graph["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 1, "{graph}");
    assert_eq!(edges[0]["runs"], 2);
    let rooted: Value = client
        .get(format!("{base}/v1/catalog/lineage?root={id}&depth=2"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(rooted["edges"].as_array().unwrap().len(), 1);
}

/// The in-memory backend keeps column profiles per dataset (#708): a replay
/// of the same `(dataset, recorded_at)` is a no-op, the list is pruned to
/// `PROFILE_RETAIN`, and the history reads back newest first, bounded.
#[tokio::test]
async fn memory_backend_records_and_prunes_column_profiles() {
    use faucet_cli::serve::history::RunHistory;
    use faucet_cli::serve::history::catalog::{CatalogProfileRecord, PROFILE_RETAIN};
    use faucet_cli::serve::history::memory::MemoryHistory;
    let store = MemoryHistory::new(Duration::from_secs(60));
    let base = chrono::Utc::now() - chrono::Duration::hours(24);
    let record = |i: i64| CatalogProfileRecord {
        run_id: format!("run-{i}"),
        pipeline: "p".into(),
        row: "r".into(),
        recorded_at: base + chrono::Duration::seconds(i),
        profile: faucet_core::RunProfile {
            rows: i as u64,
            ..Default::default()
        },
        drift: Vec::new(),
        baseline_runs: 0,
    };
    for i in 0..(PROFILE_RETAIN as i64 + 3) {
        store
            .catalog_record_profile("ds", &record(i))
            .await
            .unwrap();
    }
    store
        .catalog_record_profile("ds", &record(PROFILE_RETAIN as i64 + 2))
        .await
        .unwrap();
    let all = store.catalog_profile_history("ds", 1000).await.unwrap();
    assert_eq!(all.len(), PROFILE_RETAIN);
    assert_eq!(all[0].run_id, format!("run-{}", PROFILE_RETAIN + 2));
    assert_eq!(all.last().unwrap().run_id, "run-3");
    assert_eq!(
        store.catalog_profile_history("ds", 2).await.unwrap().len(),
        2
    );
    assert!(
        store
            .catalog_profile_history("other", 5)
            .await
            .unwrap()
            .is_empty()
    );
}

/// A `profiling:` pipeline records each run's column profile on its sink
/// dataset (#708): the detail carries the latest profile + drift and the recent
/// history, and the source dataset carries none.
#[tokio::test(flavor = "multi_thread")]
async fn dataset_detail_carries_column_profiles() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let port = free_port();
    spawn_server(port, dir.path()).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let config = format!(
        "version: 1\nname: prof-e2e\nprofiling: {{ min_history: 2, window: 5 }}\npipeline:\n  source: {{ type: csv, config: {{ path: {} }} }}\n  sink: {{ type: jsonl, config: {{ path: {}, append: false }} }}\n  state: {{ type: file, config: {{ path: {} }} }}\n",
        input.display(),
        output.display(),
        dir.path().join("state").display()
    );
    for _ in 0..2 {
        std::fs::write(&input, "id,region\n1,eu\n2,us\n3,eu\n4,us\n").unwrap();
        run_config(&base, &client, &config).await;
    }
    std::fs::write(&input, "id,region\n1,eu\n2,latam\n3,eu\n4,latam\n").unwrap();
    run_config(&base, &client, &config).await;

    let page: Value = client
        .get(format!("{base}/v1/catalog/datasets?kind=jsonl"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let sink_id = page["datasets"][0]["id"].as_str().unwrap();
    let detail: Value = client
        .get(format!("{base}/v1/catalog/datasets/{sink_id}"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let profile = &detail["profile"];
    assert_eq!(profile["history"].as_array().unwrap().len(), 3, "{detail}");
    assert_eq!(profile["latest"]["profile"]["rows"], 4);
    assert_eq!(profile["latest"]["baseline_runs"], 2);
    let region = &profile["latest"]["profile"]["columns"]["region"];
    assert_eq!(region["null_rate"], 0.0);
    assert_eq!(region["top_values"].as_array().unwrap().len(), 2);
    let drift = profile["latest"]["drift"].as_array().unwrap();
    assert!(
        drift
            .iter()
            .any(|d| d["metric"] == "new_value" && d["value"] == "latam"),
        "{drift:?}"
    );
    assert!(
        profile["history"][2]["drift"].is_null(),
        "the first run had nothing to compare"
    );

    let source: Value = client
        .get(format!("{base}/v1/catalog/datasets?kind=csv"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let source_id = source["datasets"][0]["id"].as_str().unwrap();
    let source_detail: Value = client
        .get(format!("{base}/v1/catalog/datasets/{source_id}"))
        .bearer_auth("viewer-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(source_detail.get("profile").is_none(), "{source_detail}");
}

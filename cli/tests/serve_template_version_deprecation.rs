//! Per-version deprecation (#697) over HTTP, against the SQLite history
//! backend (through the fallback wrapper the server always uses): retire one
//! version, see `newest` skip it, trigger it pinned (it runs, with a warning),
//! be refused a launch, revive it, and have a delete clear the marker.

#![cfg(all(feature = "templates", feature = "serve-history-sqlite"))]

use std::time::Duration;

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::config::ServeConfig;
use serde_json::{Value, json};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16, history: Option<String>) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: history.clone(),
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
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

async fn start(port: u16, history: Option<String>) -> (reqwest::Client, String) {
    let mut config = ServeConfig::from_args(args(port, history)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    let mut up = false;
    for _ in 0..400 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(up, "server did not come up");
    (client, base)
}

fn pipeline(dir: &std::path::Path, n: u32) -> String {
    std::fs::write(dir.join("in.csv"), "id\n1\n").unwrap();
    format!(
        "kind: pipeline\nversion: 1\nname: orders\nvars: {{ build: {n} }}\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        dir.join("in.csv").display(),
        dir.join(format!("out{n}.jsonl")).display()
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retired_version_warns_is_skipped_by_newest_and_cannot_launch_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let history = format!("sqlite:{}", dir.path().join("h.db").display());
    retire_and_revive(dir.path(), Some(history)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retired_version_warns_is_skipped_by_newest_and_cannot_launch_in_memory() {
    let dir = tempfile::tempdir().unwrap();
    retire_and_revive(dir.path(), None).await;
}

async fn retire_and_revive(dir: &std::path::Path, history: Option<String>) {
    let (client, base) = start(free_port(), history).await;
    for n in 1..=2 {
        let r = client
            .post(format!("{base}/v1/templates"))
            .json(&json!({ "config": pipeline(dir, n), "launch": n == 1 }))
            .send()
            .await
            .unwrap();
        assert!(r.status().is_success(), "{}", r.text().await.unwrap());
    }
    let deprecate = |version: &'static str, body: Value| {
        let client = client.clone();
        let url = format!("{base}/v1/templates/orders/versions/{version}/deprecate");
        async move { client.post(url).json(&body).send().await.unwrap() }
    };

    let r = deprecate("newest", json!({ "reason": "bad build" })).await;
    assert_eq!(r.status().as_u16(), 200, "{}", r.text().await.unwrap());
    let got: Value = client
        .get(format!("{base}/v1/templates/orders"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["newest"], 1, "`newest` skips the retired v2: {got}");
    assert_eq!(got["deprecated_versions"][0]["version"], 2, "{got}");
    assert_eq!(
        got["deprecated_versions"][0]["reason"], "bad build",
        "{got}"
    );

    let run: Value = client
        .post(format!("{base}/v1/templates/orders/runs"))
        .json(&json!({ "version": 2 }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(run["deprecated"], "v2 is deprecated: bad build", "{run}");

    let r = client
        .post(format!("{base}/v1/templates/orders/launch"))
        .json(&json!({ "version": 2 }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.status().as_u16(),
        422,
        "a retired version cannot be launched"
    );

    let r = deprecate("2", json!({ "undo": true })).await;
    assert_eq!(r.status().as_u16(), 200);
    let got: Value = client
        .get(format!("{base}/v1/templates/orders"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["newest"], 2, "{got}");
    assert!(got.get("deprecated_versions").is_none(), "{got}");

    // An unknown version is a 404; deleting a retired version clears it.
    assert_eq!(deprecate("9", json!({})).await.status().as_u16(), 404);
    assert_eq!(deprecate("2", json!({})).await.status().as_u16(), 200);
    let r = client
        .delete(format!("{base}/v1/templates/orders?version=2"))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    let got: Value = client
        .get(format!("{base}/v1/templates/orders"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(got.get("deprecated_versions").is_none(), "{got}");
}

/// A third-party history backend that predates #697 compiles unchanged: the
/// new trait methods default to "unsupported" and "none", and the provided
/// `template_state` still assembles.
#[tokio::test]
async fn a_backend_without_version_deprecation_gets_safe_defaults() {
    use faucet_cli::serve::history::{
        Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunHistory, RunRecord,
    };
    struct Minimal;
    #[async_trait::async_trait]
    impl RunHistory for Minimal {
        async fn claim_idempotency(
            &self,
            _: &str,
            _: &str,
            _: &str,
            _: Duration,
        ) -> Result<Claim, HistoryError> {
            unreachable!()
        }
        async fn upsert(&self, _: &RunRecord) -> Result<(), HistoryError> {
            unreachable!()
        }
        async fn get(&self, _: &str) -> Result<Option<RunRecord>, HistoryError> {
            unreachable!()
        }
        async fn list(&self, _: &ListFilter) -> Result<ListPage, HistoryError> {
            unreachable!()
        }
        async fn delete(&self, _: &str) -> Result<DeleteOutcome, HistoryError> {
            unreachable!()
        }
        async fn purge_expired(&self, _: Duration) -> Result<usize, HistoryError> {
            unreachable!()
        }
        async fn recover_orphans(&self) -> Result<usize, HistoryError> {
            unreachable!()
        }
        fn degraded(&self) -> bool {
            false
        }
    }
    let marker = faucet_cli::serve::history::templates::DeprecationRecord {
        deprecated_at: chrono::Utc::now(),
        deprecated_by: None,
        reason: None,
    };
    let err = Minimal
        .template_set_version_deprecation("x", 1, Some(&marker))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("does not support"), "{err}");
    assert!(
        Minimal
            .template_version_deprecations("x")
            .await
            .unwrap()
            .is_empty()
    );
    let st = Minimal.template_state("x").await.unwrap();
    assert!(st.deprecated_versions.is_empty() && st.newest.is_none());
}

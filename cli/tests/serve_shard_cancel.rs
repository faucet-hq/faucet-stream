//! SERVE-14: `POST /v1/runs/{id}/cancel` on a source-sharded run in cluster mode
//! cancels the shards that have not started and ends the run `cancelled`.
#![cfg(all(feature = "serve", feature = "serve-history-sqlite"))]

use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{RunHistory, RunRecord, RunStatus, ShardInsert, ShardOutcome};
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn args(port: u16, history: String) -> faucet_cli::cli::ServeArgs {
    faucet_cli::cli::ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
        default_config: None,
        history: Some(history),
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 0,
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
        no_ui: true,
        cluster: true,
        cluster_poll_secs: 3600,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: None,
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
    }
}

#[tokio::test]
async fn cancel_stops_unstarted_shards_and_the_run_ends_cancelled() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("h.db").display());
    let ghost = SqliteHistory::connect(
        &url,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        "ghost".into(),
    )
    .await
    .unwrap();

    let port = free_port();
    let mut config = faucet_cli::serve::ServeConfig::from_args(args(port, url)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..1200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut run = RunRecord::queued(
        "big".into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    run.status = RunStatus::Sharded;
    run.config_body = Some("version: 1".into());
    ghost.upsert(&run).await.unwrap();
    let shards: Vec<ShardInsert> = (0..3)
        .map(|i| ShardInsert {
            shard_id: i.to_string(),
            descriptor: serde_json::json!({ "i": i }),
            size_estimate: Some(3 - i),
        })
        .collect();
    ghost.insert_shards("big", &shards).await.unwrap();
    assert_eq!(ghost.claim_shards(1).await.unwrap().len(), 1);
    assert!(
        ghost
            .finalize_shard("big", "0", ShardOutcome::Completed)
            .await
            .unwrap()
    );

    let r = client
        .post(format!("{base}/v1/runs/big/cancel"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status().as_u16(), 202);

    let body: serde_json::Value = client
        .get(format!("{base}/v1/runs/big"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["status"], "cancelled", "{body}");
    let p = ghost.shard_progress("big").await.unwrap();
    assert_eq!((p.completed, p.cancelled, p.pending), (1, 2, 0));
}

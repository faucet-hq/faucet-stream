//! #956: a clustered server with a vault key accepts a template trigger that
//! supplies a `secret: true` param. Instance A triggers it and only stores it
//! (A runs no claim loop); instance B, a real `faucet serve --cluster` on the
//! same SQLite history, claims and executes it with the real value. The value
//! never reaches the history database in plaintext.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
#![cfg(all(
    feature = "templates",
    feature = "serve-history-sqlite",
    feature = "encryption",
    feature = "source-csv",
    feature = "sink-jsonl",
    feature = "transforms"
))]

use std::sync::Arc;
use std::time::Duration;

use faucet_cli::cli::ServeArgs;
use faucet_cli::runtime::server::{TriggerOutcome, trigger_template};
use faucet_cli::runtime::{Origin, Principal};
use faucet_cli::serve::config::ServeConfig;
use faucet_cli::serve::handlers::templates::TriggerBody;
use faucet_cli::serve::history::RunHistory;
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::rbac::AuthContext;
use faucet_cli::serve::state::ServerState;
use faucet_cli::serve::vault::Vault;
use serde_json::{Value, json};

#[path = "support/ports.rs"]
mod ports;
use ports::free_port;

const VAULT_KEY: &str = "e2e-vault-key-0123456789abcdef0123456789abcdef";
const SECRET: &str = "cluster-e2e-secret-7f3a9";

fn args(port: u16, history: &str) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: None,
        read_token: None,
        write_token: None,
        admin_token: None,
        no_auth: true,
        allow_unauthenticated_network: false,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: Some(history.to_string()),
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        log_buffer: Default::default(),
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        preview_local_outputs: false,
        preview_default_rows: 500,
        preview_max_rows: 5_000,
        lease_ttl_secs: 3,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: true,
        cluster: true,
        cluster_poll_secs: 1,
        cluster_max_attempts: 3,
        triggers: None,
        templates_sync: None,
        policy: None,
        otel_config: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
        require_approval: Vec::new(),
        approval_expiry_secs: 86_400,
        vault_key: Some(VAULT_KEY.into()),
        vault_previous_key: Vec::new(),
        connect_providers: None,
        allow_subprocess_connectors: false,
        references: Default::default(),
        pools: Default::default(),
    }
}

async fn start_b(port: u16, history: &str) -> (reqwest::Client, String) {
    let mut config = ServeConfig::from_args(args(port, history)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    for _ in 0..400 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return (client, base);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("instance B did not come up");
}

/// Instance A: a clustered server state over the shared history with the
/// vault key, but no claim loop — it can only store the run.
async fn instance_a(history: &str) -> ServerState {
    let config = ServeConfig::from_args(args(free_port(), history)).unwrap();
    let store = SqliteHistory::connect(
        history,
        Duration::from_secs(3600),
        Duration::from_secs(30),
        "inst-a".into(),
    )
    .await
    .unwrap();
    let state = ServerState::new(
        &config,
        None,
        tokio_util::sync::CancellationToken::new(),
        Arc::new(store) as Arc<dyn RunHistory>,
        faucet_cli::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        faucet_cli::serve::triggers::health::TriggersHandle::empty(),
    );
    state.set_vault(Some(Arc::new(Vault::new(VAULT_KEY, &[]).unwrap())));
    state
}

fn template(dir: &std::path::Path) -> String {
    std::fs::write(dir.join("in.csv"), "id\n1\n2\n").unwrap();
    format!(
        "kind: pipeline\nversion: 1\nname: sealed-e2e\nparams:\n  token: {{ type: string, secret: true, default: default-token-value }}\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  transforms:\n    - type: set\n      config:\n        values:\n          tok: \"${{param.token}}\"\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\ntests:\n  suite:\n    auto: {{ defaults_baseline: true }}\n",
        dir.join("in.csv").display(),
        dir.join("out.jsonl").display()
    )
}

fn files_containing(dir: &std::path::Path, needle: &[u8]) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("history.db"))
        .filter(|e| {
            std::fs::read(e.path())
                .map(|b| b.windows(needle.len()).any(|w| w == needle))
                .unwrap_or(false)
        })
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn one_instance_triggers_and_a_peer_executes_with_the_secret_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let history = format!("sqlite:{}", dir.path().join("history.db").display());
    let (client, base) = start_b(free_port(), &history).await;

    let registered = client
        .post(format!("{base}/v1/templates"))
        .json(&json!({ "config": template(dir.path()), "config_format": "yaml", "launch": true }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        registered.status(),
        201,
        "{}",
        registered.text().await.unwrap()
    );

    let a = instance_a(&history).await;
    let outcome = trigger_template(
        a.clone(),
        Principal::new(AuthContext::system("embedder"), Origin::Trusted),
        "sealed-e2e".into(),
        TriggerBody {
            params: [("token".to_string(), json!(SECRET))].into(),
            idempotency_key: Some("e2e-1".into()),
            ..Default::default()
        },
    )
    .await
    .expect("a clustered trigger with a vault key is accepted");
    let TriggerOutcome::Run(resp) = outcome else {
        panic!("unexpected pending change");
    };
    let run_id = resp.run.run_id.clone();

    let mut detail = Value::Null;
    for _ in 0..240 {
        detail = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            detail["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert_eq!(detail["status"], "completed", "{detail}");
    assert!(
        !detail.to_string().contains(SECRET),
        "GET leaks it: {detail}"
    );

    // B ran it with the real value.
    let out = std::fs::read_to_string(dir.path().join("out.jsonl")).unwrap();
    assert_eq!(out.matches(SECRET).count(), 2, "{out}");

    // A replay with the same key answers with the same run, nothing re-sealed.
    let replay = trigger_template(
        a.clone(),
        Principal::new(AuthContext::system("embedder"), Origin::Trusted),
        "sealed-e2e".into(),
        TriggerBody {
            params: [("token".to_string(), json!(SECRET))].into(),
            idempotency_key: Some("e2e-1".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let TriggerOutcome::Run(again) = replay else {
        panic!("unexpected pending change");
    };
    assert_eq!(again.run.run_id, run_id);

    // The sealed values are gone once the run is terminal.
    assert!(
        a.history()
            .run_secrets_get(&run_id)
            .await
            .unwrap()
            .is_none()
    );

    // Nothing in the history database holds the plaintext: run records, logs,
    // audit, idempotency claims, catalog snapshots — every page of the file.
    let pool = sqlx::SqlitePool::connect(&history).await.unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    let body: String = sqlx::query_scalar("SELECT body FROM faucet_serve_runs WHERE run_id = ?")
        .bind(&run_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        body.contains("${sealed:"),
        "the stored config carries tokens: {body}"
    );
    let audit: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM faucet_serve_audit")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(audit > 0, "the trigger was audited");
    pool.close().await;
    assert!(
        !files_containing(dir.path(), b"${sealed:").is_empty(),
        "the scan reads the database files"
    );
    assert_eq!(
        files_containing(dir.path(), SECRET.as_bytes()),
        Vec::<String>::new(),
        "the secret reached the history database"
    );
}

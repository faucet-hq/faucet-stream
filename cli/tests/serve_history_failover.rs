//! #1050 acceptance: a Postgres primary that fails over to read-only. The
//! server admits nothing while it refuses writes, `/readyz` says since when,
//! a run in flight finishes and its record waits, and once the database takes
//! writes again the record lands complete. Requires Docker.
#![cfg(all(
    feature = "serve-history-postgres",
    feature = "source-webhook",
    feature = "sink-jsonl",
    feature = "triggers"
))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_cli::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
use faucet_cli::serve::logs::LogHub;
use faucet_cli::serve::server::{LeaseHealth, build_router, maintain_leases};
use faucet_cli::serve::state::ServerState;
use faucet_cli::serve::triggers::health::TriggersHandle;
use std::time::Duration;
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

fn config(url: &str) -> ServeConfig {
    ServeConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        log_format: Default::default(),
        auth: AuthMode::None,
        max_concurrent_runs: 4,
        max_queued_runs: 16,
        default_config_path: None,
        history: HistoryBackendSpec::Postgres(url.to_string()),
        cors_origins: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace: Duration::from_secs(5),
        retain_terminal_runs: Duration::from_secs(60),
        idempotency_retention: Duration::from_secs(60),
        log_retention: Duration::from_secs(0),
        log_max_lines_per_run: 100_000,
        log_buffer: Default::default(),
        local_output_retention_days: 7,
        local_output_in_flight_grace: Duration::from_secs(60),
        preview: faucet_cli::serve::preview::PreviewConfig::default(),
        lease_ttl: Duration::from_secs(30),
        probe_timeout: Duration::from_secs(10),
        env_file: None,
        no_env_file: false,
        log_level: "warn".into(),
        ui_enabled: false,
        cluster: faucet_cli::serve::cluster::ClusterConfig {
            enabled: false,
            poll: Duration::from_secs(3600),
            max_attempts: 3,
            pools: faucet_cli::serve::pools::default_only(),
            pool_wait_timeout: None,
        },
        triggers_path: None,
        schedules: Default::default(),
        templates_sync_path: None,
        policy_path: None,
        policy_store: Default::default(),
        otel: None,
        callback_allow_hosts: Vec::new(),
        request_guard: Default::default(),
        require_approval: Vec::new(),
        approval_expiry: Duration::from_secs(86_400),
        vault: None,
        connect_providers_path: None,
        allow_subprocess_connectors: false,
        grafana_url: None,
        references: Default::default(),
    }
}

async fn sql(url: &str, statements: &[&str]) {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    for s in statements {
        client.batch_execute(s).await.expect(s);
    }
}

/// Make every new session read-only, as a demoted primary is, and drop the
/// sessions the server holds.
const READ_ONLY: &[&str] = &[
    "ALTER DATABASE postgres SET default_transaction_read_only = on",
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = 'postgres'",
];
const WRITABLE: &[&str] = &[
    "SET default_transaction_read_only = off",
    "ALTER DATABASE postgres SET default_transaction_read_only = off",
    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = 'postgres'",
];

#[tokio::test(flavor = "multi_thread")]
async fn a_read_only_failover_admits_nothing_and_the_finished_run_lands_after() {
    let container = faucet_conformance::containers::start(|| {
        Postgres::default().with_host_auth().with_tag("16-alpine")
    })
    .await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    while tokio_postgres::connect(&url, NoTls).await.is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "postgres never started"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let cfg = config(&url);
    let history = faucet_cli::serve::history::connect(
        &cfg.history,
        Duration::from_secs(60),
        Duration::from_secs(30),
        "acceptance",
    )
    .await
    .expect("history");
    let state = ServerState::new(
        &cfg,
        None,
        CancellationToken::new(),
        history.clone(),
        LogHub::new(),
        None,
        TriggersHandle::from_compiled(&[]),
    );
    let app = build_router(state.clone(), &cfg, &Default::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let _ = axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await;
    });
    let client = reqwest::Client::new();
    let dir = tempfile::tempdir().unwrap();
    let hook = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let hook_addr = hook.local_addr().unwrap();
    drop(hook);
    let body = |name: &str| {
        serde_json::json!({ "name": name, "config": format!(
            "version: 1\nname: {name}\npipeline:\n  source: {{ type: webhook, config: {{ listen_addr: \"{hook_addr}\", path: /in, timeout_secs: 3 }} }}\n  sink: {{ type: jsonl, config: {{ path: {} }} }}\n",
            dir.path().join(format!("{name}.jsonl")).display()
        )})
    };
    let submitted = client
        .post(format!("{base}/v1/runs"))
        .json(&body("inflight"))
        .send()
        .await
        .unwrap();
    assert_eq!(submitted.status().as_u16(), 202);
    let run_id = submitted.json::<serde_json::Value>().await.unwrap()["run_id"]
        .as_str()
        .unwrap()
        .to_string();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Failover: the database answers, but refuses writes.
    sql(&url, READ_ONLY).await;
    let mut refused = None;
    for _ in 0..20 {
        let r = client
            .post(format!("{base}/v1/runs"))
            .json(&body("during"))
            .send()
            .await
            .unwrap();
        if r.status().as_u16() == 503 {
            refused = Some(r.text().await.unwrap());
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let refused = refused.expect("a submission during the failover is refused with 503");
    assert!(refused.contains("history unavailable"), "{refused}");
    assert!(history.degraded());
    let ready: serde_json::Value = client
        .get(format!("{base}/readyz"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(ready["degraded"]["since"].is_string(), "{ready}");

    // The run in flight finishes; its record waits for the database.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while state.pending_finalize_len() == 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the run never finished"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // Reads still answer, but the probe is a write: still degraded.
    let mut health = LeaseHealth::new(std::time::Instant::now());
    maintain_leases(&state, &mut health, Duration::from_secs(3600)).await;
    assert!(
        history.degraded(),
        "a read-only primary does not count as recovered"
    );
    assert_eq!(state.pending_finalize_len(), 1);

    // Writable again: the server recovers and the record lands complete.
    sql(&url, WRITABLE).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        maintain_leases(&state, &mut health, Duration::from_secs(3600)).await;
        if !history.degraded() && state.pending_finalize_len() == 0 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "never recovered");
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    let rec = history.get(&run_id).await.unwrap().expect("stored");
    assert_eq!(rec.status.as_str(), "completed", "{rec:?}");
    assert_eq!(rec.name.as_deref(), Some("inflight"));
    let ok = client
        .post(format!("{base}/v1/runs"))
        .json(&body("after"))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status().as_u16(), 202);
}

//! Integration tests for event-driven triggers (#196). Drive the serve runner
//! in-process against an in-memory history backend.
#![cfg(all(feature = "triggers", feature = "source-csv", feature = "sink-jsonl"))]

use std::sync::Arc;
use std::time::Duration;

use faucet_cli::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
use faucet_cli::serve::history::memory::MemoryHistory;
use faucet_cli::serve::history::{ListFilter, RunHistory, RunStatus};
use faucet_cli::serve::logs::LogHub;
use faucet_cli::serve::state::ServerState;
use faucet_cli::serve::triggers::compiled::CompiledTriggers;
use faucet_cli::serve::triggers::health::TriggersHandle;
use tokio_util::sync::CancellationToken;

fn test_config() -> ServeConfig {
    ServeConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        log_format: Default::default(),
        auth: AuthMode::None,
        max_concurrent_runs: 4,
        max_queued_runs: 16,
        default_config_path: None,
        history: HistoryBackendSpec::Memory,
        cors_origins: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace: Duration::from_secs(5),
        retain_terminal_runs: Duration::from_secs(60),
        idempotency_retention: Duration::from_secs(60),
        log_retention: Duration::from_secs(0),
        log_max_lines_per_run: 100_000,
        local_output_retention_days: 7,
        local_output_in_flight_grace: Duration::from_secs(60),
        preview: faucet_cli::serve::preview::PreviewConfig::default(),
        lease_ttl: Duration::from_secs(30),
        probe_timeout: Duration::from_secs(10),
        env_file: None,
        no_env_file: false,
        log_level: "warn".into(),
        ui_enabled: false,
        cluster: faucet_cli::serve::cluster::ClusterConfig::disabled(),
        triggers_path: None,
        callback_allow_hosts: Vec::new(),
    }
}

/// A trivial pipeline (CSV from a temp file → jsonl to a temp file) embedded
/// inline so the webhook test needs no external services. `append: true` so
/// multiple concurrent runs that write to the same path don't conflict.
fn inline_pipeline(csv_path: &str, out_path: &str) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "csv", "config": { "path": csv_path } },
            "sink": { "type": "jsonl", "config": { "path": out_path, "append": true } }
        }
    })
}

fn build_state(triggers: &CompiledTriggers) -> ServerState {
    let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
    let handle = TriggersHandle::from_compiled(&triggers.triggers);
    ServerState::new(
        &test_config(),
        None,
        CancellationToken::new(),
        history,
        LogHub::new(),
        None,
        handle,
    )
}

/// Poll `list` until at least `want` terminal runs are present, then return.
async fn wait_for_runs(state: &ServerState, want: usize) {
    for _ in 0..200 {
        let page = state
            .history()
            .list(&ListFilter {
                limit: 1_000,
                ..Default::default()
            })
            .await
            .unwrap();
        let terminal = page
            .runs
            .iter()
            .filter(|r| {
                matches!(
                    r.status,
                    RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
                )
            })
            .count();
        if terminal >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let page = state
        .history()
        .list(&ListFilter {
            limit: 1_000,
            ..Default::default()
        })
        .await
        .unwrap();
    let statuses: Vec<_> = page
        .runs
        .iter()
        .map(|r| format!("{}:{}", r.run_id, r.status.as_str()))
        .collect();
    panic!(
        "timed out waiting for {want} terminal run(s); have {} total: {statuses:?}",
        page.runs.len()
    );
}

#[tokio::test]
async fn webhook_trigger_enqueues_exactly_one_run() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();
    let out = dir.path().join("out.jsonl");

    let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());
    let file: faucet_cli::serve::triggers::spec::TriggersFile = serde_yaml::from_str(&format!(
        "version: 1\ntriggers:\n  - name: hook\n    type: webhook\n    dedupe_header: Idempotency-Key\n    config: {}\n",
        serde_json::to_string(&inline).unwrap()
    ))
    .unwrap();
    let compiled = CompiledTriggers::compile(file).unwrap();
    let state = build_state(&compiled);

    // Fire via the enqueue path directly.
    let event = faucet_cli::serve::triggers::context::TriggerEvent::Webhook {
        method: "POST".into(),
        body: "{}".into(),
        headers: Default::default(),
        query: Default::default(),
        idem: "evt-1".into(),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let outcome =
        faucet_cli::serve::triggers::enqueue::fire(&state, &compiled.triggers[0], event, &now)
            .await;
    assert!(
        matches!(
            outcome,
            faucet_cli::serve::triggers::enqueue::FireOutcome::Enqueued(_)
        ),
        "expected Enqueued, got {outcome:?}"
    );

    // Wait for the run to finish, then assert exactly one terminal run exists.
    wait_for_runs(&state, 1).await;
    let page = state
        .history()
        .list(&ListFilter {
            limit: 1_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page.runs.len(),
        1,
        "expected exactly 1 run, got {}",
        page.runs.len()
    );

    // A second fire with the SAME idempotency key must NOT create a second run.
    let event2 = faucet_cli::serve::triggers::context::TriggerEvent::Webhook {
        method: "POST".into(),
        body: "{}".into(),
        headers: Default::default(),
        query: Default::default(),
        idem: "evt-1".into(),
    };
    let _ = faucet_cli::serve::triggers::enqueue::fire(&state, &compiled.triggers[0], event2, &now)
        .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let page2 = state
        .history()
        .list(&ListFilter {
            limit: 1_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page2.runs.len(),
        1,
        "idempotency must dedupe the replay — got {} runs",
        page2.runs.len()
    );
}

#[cfg(feature = "triggers-object-store")]
#[tokio::test]
async fn object_arrival_enqueues_one_run_per_object_and_dedupes() {
    use faucet_cli::serve::triggers::object_arrival::{Cursor, ListedObject, ObjectArrivalWatcher};
    use faucet_cli::serve::triggers::watcher::Watcher;
    use object_store::memory::InMemory;
    use object_store::path::Path as OPath;
    use object_store::{ObjectStore, ObjectStoreExt};

    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let out = dir.path().join("out.jsonl");
    // Inline pipeline: the run just needs to complete; we don't use
    // ${trigger.object_key} here — we're testing enqueue count not data flow.
    let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());

    let file: faucet_cli::serve::triggers::spec::TriggersFile = serde_yaml::from_str(&format!(
        "version: 1\ntriggers:\n  - name: drop\n    type: object_arrival\n    store: {{ type: s3, bucket: b }}\n    config: {}\n",
        serde_json::to_string(&inline).unwrap()
    ))
    .unwrap();
    let compiled = CompiledTriggers::compile(file).unwrap();
    let state = build_state(&compiled);

    // InMemory object store with one object.
    let store = InMemory::new();
    store
        .put(&OPath::from("incoming/a.json"), b"{}".to_vec().into())
        .await
        .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(store);

    let mut watcher = ObjectArrivalWatcher::new(
        Arc::new(compiled.triggers[0].clone()),
        store.clone(),
        "b".into(),
        None,
        faucet_cli::serve::triggers::spec::ArrivalMode::PerObject,
        Duration::from_secs(30),
        faucet_cli::serve::triggers::spec::StartAt::Beginning,
        chrono::Utc::now(),
    );

    // One poll → exactly one run enqueued.
    let fired = watcher.poll(&state).await.unwrap();
    assert!(fired, "expected first poll to fire");
    wait_for_runs(&state, 1).await;
    let page = state
        .history()
        .list(&ListFilter {
            limit: 1_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.runs.len(), 1, "expected 1 run after first poll");

    // Second poll with same object → cursor committed → no new run.
    let fired2 = watcher.poll(&state).await.unwrap();
    assert!(
        !fired2,
        "expected second poll to be idle (cursor committed)"
    );
    tokio::time::sleep(Duration::from_millis(150)).await;
    let page2 = state
        .history()
        .list(&ListFilter {
            limit: 1_000,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page2.runs.len(),
        1,
        "cursor dedup must prevent a second run"
    );

    // Ensure these public types are reachable (exercises re-exports).
    let _ = (
        Cursor::starting_beginning(),
        ListedObject {
            key: "x".into(),
            last_modified: chrono::Utc::now(),
            size: 0,
            etag: None,
        },
    );
}

#[tokio::test]
async fn queue_depth_fires_once_on_rising_edge() {
    // Only run when the queue_depth module is compiled (any backend feature).
    #[cfg(any(feature = "triggers-redis", feature = "triggers-kafka"))]
    {
        use async_trait::async_trait;
        use faucet_cli::serve::triggers::queue_depth::{DepthProbe, QueueDepthWatcher};
        use faucet_cli::serve::triggers::watcher::Watcher;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct FakeProbe {
            readings: Arc<Vec<u64>>,
            idx: Arc<AtomicU64>,
        }

        #[async_trait]
        impl DepthProbe for FakeProbe {
            async fn depth(&self) -> Result<u64, String> {
                let i = self.idx.fetch_add(1, Ordering::SeqCst) as usize;
                Ok(*self.readings.get(i).unwrap_or(&0))
            }
            fn queue_label(&self) -> String {
                "jobs".into()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("in.csv");
        std::fs::write(&csv, "a\n1\n").unwrap();
        let out = dir.path().join("out.jsonl");
        let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());
        let file: faucet_cli::serve::triggers::spec::TriggersFile = serde_yaml::from_str(&format!(
            "version: 1\ntriggers:\n  - name: drain\n    type: queue_depth\n    threshold: 5\n    queue: {{ type: redis, url: \"redis://x\", key: jobs }}\n    config: {}\n",
            serde_json::to_string(&inline).unwrap()
        ))
        .unwrap();
        let compiled = CompiledTriggers::compile(file).unwrap();
        let state = build_state(&compiled);

        let probe = FakeProbe {
            readings: Arc::new(vec![0, 9, 9, 0, 7]),
            idx: Arc::new(AtomicU64::new(0)),
        };
        let mut w = QueueDepthWatcher::new(
            Arc::new(compiled.triggers[0].clone()),
            Box::new(probe),
            5,
            Duration::from_secs(30),
        );

        assert!(!w.poll(&state).await.unwrap(), "depth 0: no fire");
        assert!(w.poll(&state).await.unwrap(), "depth 9: rising edge fires");
        assert!(!w.poll(&state).await.unwrap(), "depth 9 again: suppressed");
        assert!(!w.poll(&state).await.unwrap(), "depth 0: re-arm, no fire");
        assert!(
            w.poll(&state).await.unwrap(),
            "depth 7: second rising edge fires"
        );

        wait_for_runs(&state, 2).await;
        let page = state
            .history()
            .list(&ListFilter {
                limit: 1_000,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            page.runs.len(),
            2,
            "expected 2 runs (one per rising edge), got {}",
            page.runs.len()
        );
    }
}

// ── HTTP-boundary tests ───────────────────────────────────────────────────────
//
// These drive the REAL `faucet serve` HTTP listener (spawned via `run_server`,
// modelled on `cli/tests/serve_lifecycle.rs`) so the request flows through the
// bearer-auth middleware and the actual `webhook::handle` route — not the
// `enqueue::fire` helper directly. The `triggers` file is written to a temp path
// (inline csv→jsonl pipeline) and passed via `ServeArgs.triggers`.

use faucet_cli::cli::ServeArgs;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Spawn a real `faucet serve` instance with the given triggers file and bearer
/// token, then wait for `/healthz`. Returns the base URL.
async fn spawn_serve_with_triggers(
    port: u16,
    token: Option<&str>,
    triggers_path: &std::path::Path,
) -> String {
    let args = ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: token.map(|t| t.to_string()),
        auth_config: None,
        no_auth: token.is_none(),
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
        triggers: Some(triggers_path.to_path_buf()),
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    };
    let mut config = ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    for _ in 0..200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return base;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become healthy on port {port}");
}

/// Count the runs currently in history via the public `GET /v1/runs` API.
async fn http_run_count(base: &str, token: &str) -> usize {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/v1/runs?limit=1000"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "GET /v1/runs must succeed");
    let body: serde_json::Value = resp.json().await.unwrap();
    body["runs"].as_array().map(|a| a.len()).unwrap_or(0)
}

/// Write a triggers file with a single inline-pipeline webhook trigger. `extra`
/// is injected as additional trigger-level YAML (e.g. `    debounce_secs: 60\n`).
fn write_triggers_file(
    dir: &std::path::Path,
    name: &str,
    csv: &std::path::Path,
    out: &std::path::Path,
    extra: &str,
) -> std::path::PathBuf {
    let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());
    let yaml = format!(
        "version: 1\ntriggers:\n  - name: {name}\n    type: webhook\n    methods: [POST]\n{extra}    config: {}\n",
        serde_json::to_string(&inline).unwrap()
    );
    let path = dir.join("triggers.yaml");
    std::fs::write(&path, yaml).unwrap();
    path
}

#[tokio::test(flavor = "multi_thread")]
async fn webhook_http_boundary_enforces_bearer_auth_and_enqueues() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let triggers = write_triggers_file(dir.path(), "hook", &csv, &out, "");

    let token = "test-token";
    let port = free_port();
    let base = spawn_serve_with_triggers(port, Some(token), &triggers).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/v1/triggers/hook");

    // Without the bearer token → 401 (auth middleware rejects before the handler).
    let unauthorized = client
        .post(&url)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        unauthorized.status(),
        401,
        "missing bearer token must yield 401"
    );

    // With the correct token → 202 and a run is enqueued.
    let ok = client
        .post(&url)
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 202, "authorized fire must yield 202");
    let body: serde_json::Value = ok.json().await.unwrap();
    assert_eq!(body["status"].as_str(), Some("queued"));
    assert!(body["run_id"].is_string(), "response must carry a run_id");

    // The run lands in history (poll until it appears).
    let mut count = 0;
    for _ in 0..200 {
        count = http_run_count(&base, token).await;
        if count >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(count, 1, "exactly one run must be enqueued by the fire");
}

#[tokio::test(flavor = "multi_thread")]
async fn webhook_debounce_coalesces_second_fire() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();
    let out = dir.path().join("out.jsonl");
    // 60 s leading-edge debounce; no dedupe_header so each request gets a fresh
    // UUID idempotency key — proving the coalesce is debounce, not idempotency.
    let triggers = write_triggers_file(dir.path(), "hook", &csv, &out, "    debounce_secs: 60\n");

    let token = "test-token";
    let port = free_port();
    let base = spawn_serve_with_triggers(port, Some(token), &triggers).await;
    let client = reqwest::Client::new();
    let url = format!("{base}/v1/triggers/hook");

    // First fire is accepted (leading edge) → 202.
    let first = client
        .post(&url)
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 202, "first fire (enqueued) must yield 202");
    let first_body: serde_json::Value = first.json().await.unwrap();
    assert_eq!(
        first_body["status"].as_str(),
        Some("queued"),
        "first fire must enqueue: {first_body}"
    );

    // Second fire within the window is coalesced (no second run) → 200.
    let second = client
        .post(&url)
        .bearer_auth(token)
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 200, "coalesced fire must yield 200");
    let second_body: serde_json::Value = second.json().await.unwrap();
    assert_eq!(
        second_body["status"].as_str(),
        Some("coalesced"),
        "second fire within debounce window must coalesce: {second_body}"
    );

    // Give the first run time to land, then confirm the count stays at 1.
    for _ in 0..200 {
        if http_run_count(&base, token).await >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        http_run_count(&base, token).await,
        1,
        "debounce must coalesce the second fire — exactly one run expected"
    );
}

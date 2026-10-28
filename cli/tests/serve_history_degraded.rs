//! SERVE-04: the run-history fallback trips only when the backend is
//! unreachable, never serves templates / tenants / change requests from the
//! empty in-memory store, recovers when the backend answers again, and a
//! clustered instance that cannot renew its leases cancels its own runs before
//! a peer could reclaim them.
#![cfg(all(feature = "serve", feature = "templates", feature = "triggers"))]
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use async_trait::async_trait;
use faucet_cli::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
use faucet_cli::serve::history::fallback::FallbackHistory;
use faucet_cli::serve::history::memory::MemoryHistory;
use faucet_cli::serve::history::{
    Claim, DeleteOutcome, HistoryError, ListFilter, ListPage, RunHistory, RunRecord, Transience,
};
use faucet_cli::serve::logs::LogHub;
use faucet_cli::serve::server::{LeaseHealth, build_router, maintain_leases};
use faucet_cli::serve::state::ServerState;
use faucet_cli::serve::triggers::health::TriggersHandle;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const HEALTHY: u8 = 0;
const UNREACHABLE: u8 = 1;
const PERMANENT: u8 = 2;
/// Every pooled connection checked out; new calls time out acquiring one.
const SATURATED: u8 = 3;

/// A primary that is healthy, unreachable, or answering with a permanent error.
struct Flaky {
    inner: MemoryHistory,
    mode: Arc<AtomicU8>,
}

impl Flaky {
    fn check(&self) -> Result<(), HistoryError> {
        match self.mode.load(Ordering::SeqCst) {
            UNREACHABLE => Err(HistoryError::Unreachable("connection refused".into())),
            SATURATED => Err(HistoryError::PoolTimedOut(
                "pool timed out while waiting for an open connection".into(),
            )),
            PERMANENT => Err(HistoryError::BackendClassified {
                message: "UNIQUE constraint failed".into(),
                transience: Transience::Permanent,
            }),
            _ => Ok(()),
        }
    }
}

#[async_trait]
impl RunHistory for Flaky {
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<Claim, HistoryError> {
        self.check()?;
        self.inner
            .claim_idempotency(key, fingerprint, run_id, window)
            .await
    }
    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError> {
        self.check()?;
        self.inner.upsert(rec).await
    }
    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError> {
        self.check()?;
        self.inner.get(id).await
    }
    async fn list(&self, filter: &ListFilter) -> Result<ListPage, HistoryError> {
        self.check()?;
        self.inner.list(filter).await
    }
    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError> {
        self.check()?;
        self.inner.delete(id).await
    }
    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HistoryError> {
        self.check()?;
        self.inner.purge_expired(retain_for).await
    }
    async fn recover_orphans(&self) -> Result<usize, HistoryError> {
        self.check()?;
        Ok(0)
    }
    async fn renew_leases(&self) -> Result<usize, HistoryError> {
        self.check()?;
        Ok(0)
    }
    async fn renew_shard_leases(&self) -> Result<usize, HistoryError> {
        self.check()?;
        Ok(0)
    }
    /// A run (or shard) named `taken…` has been reclaimed by a peer.
    async fn lease_lost(&self, run_id: &str) -> Result<bool, HistoryError> {
        self.check()?;
        Ok(run_id.starts_with("taken"))
    }
    async fn shard_lease_lost(&self, run_id: &str, _shard: &str) -> Result<bool, HistoryError> {
        self.check()?;
        Ok(run_id.starts_with("taken"))
    }
    async fn template_list(
        &self,
    ) -> Result<Vec<faucet_cli::serve::history::templates::TemplateSummary>, HistoryError> {
        self.check()?;
        self.inner.template_list().await
    }
    fn degraded(&self) -> bool {
        false
    }
    fn pool_saturated(&self) -> bool {
        self.mode.load(Ordering::SeqCst) == SATURATED
    }
}

fn flaky() -> (FallbackHistory, Arc<AtomicU8>) {
    let mode = Arc::new(AtomicU8::new(HEALTHY));
    let primary = Flaky {
        inner: MemoryHistory::new(Duration::from_secs(60)),
        mode: mode.clone(),
    };
    (
        FallbackHistory::healthy(Box::new(primary), Duration::from_secs(60), "sqlite"),
        mode,
    )
}

fn config() -> ServeConfig {
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
            enabled: true,
            poll: Duration::from_secs(3600),
            max_attempts: 3,
            pools: faucet_cli::serve::pools::default_only(),
            pool_wait_timeout: None,
        },
        triggers_path: None,
        templates_sync_path: None,
        policy_path: None,
        otel: None,
        callback_allow_hosts: Vec::new(),
        require_approval: Vec::new(),
        approval_expiry: Duration::from_secs(86_400),
        vault: None,
        connect_providers_path: None,
        allow_subprocess_connectors: false,
        references: Default::default(),
    }
}

fn run(id: &str) -> RunRecord {
    RunRecord::queued(
        id.into(),
        None,
        Default::default(),
        None,
        chrono::Utc::now(),
    )
}

#[tokio::test]
async fn a_saturated_pool_degrades_only_once_the_backend_stops_answering() {
    let (fb, mode) = flaky();
    let fb = fb.with_saturation_grace(Duration::from_secs(2));
    fb.upsert(&run("r0")).await.unwrap();
    mode.store(SATURATED, Ordering::SeqCst);
    let err = fb.upsert(&run("r1")).await.unwrap_err();
    assert!(err.is_pool_timeout(), "{err}");
    assert!(
        !fb.degraded(),
        "a busy pool on a backend that just answered is load, not an outage"
    );
    tokio::time::sleep(Duration::from_millis(2100)).await;
    fb.upsert(&run("r2")).await.unwrap();
    assert!(
        fb.degraded(),
        "a pool that stays saturated with no answer for the grace is a hung backend"
    );
    assert!(
        fb.get("r2").await.unwrap().is_some(),
        "runs keep going in memory"
    );
}

#[tokio::test]
async fn a_permanent_error_is_returned_and_never_trips() {
    let (fb, mode) = flaky();
    mode.store(PERMANENT, Ordering::SeqCst);
    let err = fb.upsert(&run("r1")).await.unwrap_err();
    assert!(!err.is_unreachable(), "{err}");
    assert!(
        !fb.degraded(),
        "a constraint failure must not degrade the store"
    );
    mode.store(HEALTHY, Ordering::SeqCst);
    fb.upsert(&run("r1")).await.unwrap();
    assert!(fb.get("r1").await.unwrap().is_some());
}

#[tokio::test]
async fn an_unreachable_backend_degrades_runs_refuses_the_registry_fences_and_recovers() {
    let (fb, mode) = flaky();
    let history: Arc<dyn RunHistory> = Arc::new(fb);
    let cfg = config();
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
    let status = |path: &'static str| {
        let client = client.clone();
        let base = base.clone();
        async move {
            client
                .get(format!("{base}{path}"))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };
    assert_eq!(status("/v1/templates").await, 200);

    mode.store(UNREACHABLE, Ordering::SeqCst);
    history.upsert(&run("r1")).await.unwrap();
    assert!(
        history.degraded(),
        "an unreachable backend trips after its retries"
    );
    assert!(
        history.get("r1").await.unwrap().is_some(),
        "runs keep going in memory"
    );
    assert_eq!(
        status("/v1/templates").await,
        503,
        "the registry is never served from the empty in-memory store"
    );
    assert_eq!(status("/readyz").await, 503);

    let token = CancellationToken::new();
    state.registry().register("local-run".into(), token.clone());
    let mut health = LeaseHealth::new(std::time::Instant::now());
    assert_eq!(
        maintain_leases(&state, &mut health, Duration::from_secs(3600)).await,
        0,
        "within the window nothing is fenced"
    );
    assert_eq!(
        maintain_leases(&state, &mut health, Duration::ZERO).await,
        1
    );
    assert!(
        token.is_cancelled(),
        "local runs stop before peers may reclaim them"
    );
    assert_eq!(
        maintain_leases(&state, &mut health, Duration::ZERO).await,
        0,
        "fenced once per outage"
    );

    mode.store(HEALTHY, Ordering::SeqCst);
    assert_eq!(
        maintain_leases(&state, &mut health, Duration::ZERO).await,
        0
    );
    assert!(
        !history.degraded(),
        "the recovery probe leaves degraded mode"
    );
    assert_eq!(status("/v1/templates").await, 200);
}

/// #905: a clustered server's backend is strict (it never falls back to
/// memory, so `degraded()` stays false); `/readyz` must still turn 503 while
/// the lease heartbeat cannot reach it, and recover on the next one that can.
#[tokio::test]
async fn readyz_turns_not_ready_while_a_strict_backend_is_unreachable() {
    let mode = Arc::new(AtomicU8::new(HEALTHY));
    let history: Arc<dyn RunHistory> = Arc::new(Flaky {
        inner: MemoryHistory::new(Duration::from_secs(60)),
        mode: mode.clone(),
    });
    let cfg = config();
    assert!(cfg.cluster.enabled);
    let state = ServerState::new(
        &cfg,
        None,
        CancellationToken::new(),
        history,
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
    let readyz = || {
        let url = format!("{base}/readyz");
        async move {
            let resp = reqwest::get(url).await.unwrap();
            let code = resp.status().as_u16();
            let body: serde_json::Value = resp.json().await.unwrap();
            (code, body["history_ok"].as_bool().unwrap())
        }
    };
    let mut health = LeaseHealth::new(std::time::Instant::now());
    let fence_after = Duration::from_secs(3600);

    maintain_leases(&state, &mut health, fence_after).await;
    assert_eq!(readyz().await, (200, true));

    mode.store(UNREACHABLE, Ordering::SeqCst);
    assert_eq!(
        readyz().await,
        (200, true),
        "nothing is known until the next heartbeat"
    );
    maintain_leases(&state, &mut health, fence_after).await;
    assert_eq!(
        readyz().await,
        (503, false),
        "a heartbeat that cannot reach the backend makes the server not ready"
    );

    mode.store(HEALTHY, Ordering::SeqCst);
    maintain_leases(&state, &mut health, fence_after).await;
    assert_eq!(
        readyz().await,
        (200, true),
        "the first heartbeat that gets through clears it"
    );
}

/// A run a peer reclaimed while this instance was still running it (its lease
/// lapsed: clock skew, a stall) is cancelled here on the next heartbeat, so the
/// two executions do not run side by side; runs this instance still owns are
/// left alone.
#[tokio::test]
async fn a_run_whose_lease_a_peer_took_is_cancelled_locally() {
    let (fb, _mode) = flaky();
    let history: Arc<dyn RunHistory> = Arc::new(fb);
    let cfg = config();
    let state = ServerState::new(
        &cfg,
        None,
        CancellationToken::new(),
        history,
        LogHub::new(),
        None,
        TriggersHandle::from_compiled(&[]),
    );
    let taken = CancellationToken::new();
    let mine = CancellationToken::new();
    let taken_shard = CancellationToken::new();
    state.registry().register("taken-1".into(), taken.clone());
    state.registry().register("mine-1".into(), mine.clone());
    state
        .registry()
        .register_shard("taken-2", "0", taken_shard.clone());
    let mut health = LeaseHealth::new(std::time::Instant::now());
    let n = maintain_leases(&state, &mut health, Duration::from_secs(3600)).await;
    assert_eq!(n, 2);
    assert!(taken.is_cancelled());
    assert!(taken_shard.is_cancelled());
    assert!(!mine.is_cancelled());
}

/// While fenced, a cancelled run gets only a fraction of the time left
/// before its lease expires to flush, not the full flush grace.
#[tokio::test]
async fn a_fenced_instance_shortens_the_flush_grace() {
    let (fb, mode) = flaky();
    let history: Arc<dyn RunHistory> = Arc::new(fb);
    let cfg = config();
    let state = ServerState::new(
        &cfg,
        None,
        CancellationToken::new(),
        history,
        LogHub::new(),
        None,
        TriggersHandle::from_compiled(&[]),
    );
    let normal = Duration::from_secs(30);
    assert_eq!(state.cluster().flush_grace(normal), normal);
    mode.store(UNREACHABLE, Ordering::SeqCst);
    let mut health = LeaseHealth::new(std::time::Instant::now() - Duration::from_secs(60));
    maintain_leases(&state, &mut health, Duration::from_secs(20)).await;
    assert_eq!(state.cluster().flush_grace(normal), Duration::from_secs(5));
    mode.store(HEALTHY, Ordering::SeqCst);
    maintain_leases(&state, &mut health, Duration::from_secs(20)).await;
    assert_eq!(state.cluster().flush_grace(normal), normal);
}

/// A claim stored by the previous release holds the fingerprint of the
/// resolved config; a retry of the same request after the upgrade replays it
/// instead of answering 409, while a different request still conflicts.
#[cfg(all(feature = "source-csv", feature = "sink-jsonl"))]
#[tokio::test]
async fn a_claim_from_the_previous_release_still_replays() {
    use faucet_cli::serve::runner::{SubmitRequest, legacy_request_fingerprint, submit};
    let history: Arc<dyn RunHistory> = Arc::new(MemoryHistory::new(Duration::from_secs(60)));
    let state = ServerState::new(
        &config(),
        None,
        CancellationToken::new(),
        history.clone(),
        LogHub::new(),
        None,
        TriggersHandle::from_compiled(&[]),
    );
    let yaml = "version: 1\nname: legacy\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n";
    let req: SubmitRequest =
        serde_json::from_value(serde_json::json!({"config": yaml, "idempotency_key": "k"}))
            .unwrap();
    let cfg =
        faucet_cli::config::PipelineConfig::from_text(yaml, std::path::Path::new("submitted.yaml"))
            .unwrap();
    history.upsert(&run("prior")).await.unwrap();
    history
        .claim_idempotency(
            "k",
            &legacy_request_fingerprint(&req, &cfg),
            "prior",
            Duration::from_secs(60),
        )
        .await
        .unwrap();
    let actor = faucet_cli::serve::rbac::AuthContext {
        principal: "t".into(),
        role: faucet_cli::serve::rbac::Role::Admin,
        source_ip: None,
        tenant: None,
    };
    let replayed = submit(state.clone(), req.clone(), actor.clone())
        .await
        .expect("replay");
    assert_eq!(replayed.run_id, "prior");

    let mut other = req;
    other.labels.insert("team".into(), "x".into());
    let err = submit(state, other, actor).await.unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("idempotency"),
        "{err}"
    );
}

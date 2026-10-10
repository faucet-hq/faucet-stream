//! SERVE-04: the run-history fallback trips only when the backend is
//! unreachable, never serves templates / tenants / change requests from the
//! empty in-memory store, recovers when the backend answers again, and a
//! clustered instance that cannot renew its leases cancels its own runs before
//! a peer could reclaim them.
#![cfg(all(feature = "serve", feature = "templates", feature = "triggers"))]

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

/// A primary that is healthy, unreachable, or answering with a permanent error.
struct Flaky {
    inner: MemoryHistory,
    mode: Arc<AtomicU8>,
}

impl Flaky {
    fn check(&self) -> Result<(), HistoryError> {
        match self.mode.load(Ordering::SeqCst) {
            UNREACHABLE => Err(HistoryError::Unreachable("connection refused".into())),
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
    async fn template_list(
        &self,
    ) -> Result<Vec<faucet_cli::serve::history::templates::TemplateSummary>, HistoryError> {
        self.check()?;
        self.inner.template_list().await
    }
    fn degraded(&self) -> bool {
        false
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
        },
        triggers_path: None,
        templates_sync_path: None,
        policy_path: None,
        otel: None,
        callback_allow_hosts: Vec::new(),
        require_approval: Vec::new(),
        approval_expiry: Duration::from_secs(86_400),
        require_template_tests: false,
        vault: None,
        connect_providers_path: None,
        allow_subprocess_connectors: false,
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

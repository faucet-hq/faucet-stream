//! Shared `ServerState` builders for the serve unit tests.
//!
//! `ServeConfig` has ~20 fields, so every handler test that needs a `ServerState`
//! used to hand-roll the same struct literal — three copies already, and each new
//! field meant editing all of them. One builder here keeps that cost at one edit.

use crate::serve::cluster::ClusterConfig;
use crate::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
use crate::serve::history::RunHistory;
use crate::serve::history::memory::MemoryHistory;
use crate::serve::state::ServerState;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

/// A no-auth, in-memory, single-instance `ServeConfig`.
pub fn test_config() -> ServeConfig {
    ServeConfig {
        listen: "127.0.0.1:0".parse().expect("loopback addr"),
        log_format: crate::cli::LogFormat::Text,
        auth: AuthMode::None,
        max_concurrent_runs: 4,
        max_queued_runs: 8,
        default_config_path: None,
        history: HistoryBackendSpec::Memory,
        cors_origins: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace: Duration::from_secs(60),
        retain_terminal_runs: Duration::from_secs(60),
        idempotency_retention: Duration::from_secs(60),
        log_retention: std::time::Duration::from_secs(0),
        log_max_lines_per_run: 100_000,
        log_buffer: Default::default(),
        local_output_retention_days: 7,
        local_output_in_flight_grace: Duration::from_secs(60),
        preview: crate::serve::preview::PreviewConfig::default(),
        lease_ttl: Duration::from_secs(30),
        probe_timeout: Duration::from_secs(10),
        env_file: None,
        no_env_file: true,
        log_level: "warn".into(),
        ui_enabled: true,
        cluster: ClusterConfig::disabled(),
        triggers_path: None,
        schedules: Default::default(),
        templates_sync_path: None,
        policy_path: None,
        policy_store: Default::default(),
        otel: None,
        callback_allow_hosts: Vec::new(),
        request_guard: Default::default(),
        require_approval: Vec::new(),
        approval_expiry: std::time::Duration::from_secs(86_400),
        vault: None,
        connect_providers_path: None,
        allow_subprocess_connectors: false,
        grafana_url: None,
        references: Default::default(),
    }
}

/// Build a `ServerState` over `config` with a fresh in-memory history backend.
pub fn state_from(config: &ServeConfig) -> ServerState {
    let history = Arc::new(MemoryHistory::new(config.idempotency_retention)) as Arc<dyn RunHistory>;
    ServerState::new(
        config,
        None,
        CancellationToken::new(),
        history,
        crate::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        crate::serve::triggers::health::TriggersHandle::empty(),
    )
}

/// The common case: a single-instance server on an in-memory store.
pub fn test_state() -> ServerState {
    state_from(&test_config())
}

/// A server that believes it is part of a cluster — used to exercise the
/// cluster-only branches (pending runs, cross-instance cancel, the
/// secret-param persistence refusal).
pub fn test_state_clustered() -> ServerState {
    let mut config = test_config();
    config.cluster.enabled = true;
    state_from(&config)
}

type HErr = crate::serve::history::HistoryError;
type PolicyRecord = crate::serve::history::policies::PolicyVersionRecord;

/// An in-memory history that answers "unreachable" while `down` is set:
/// wrapped in a `FallbackHistory` it drives the degraded paths without a
/// database. It implements the run and policy calls the degraded tests make.
pub struct Flaky {
    pub inner: MemoryHistory,
    pub down: Arc<std::sync::atomic::AtomicBool>,
}

impl Flaky {
    fn check(&self) -> Result<(), HErr> {
        if self.down.load(std::sync::atomic::Ordering::Acquire) {
            return Err(HErr::Unreachable("connection refused".into()));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RunHistory for Flaky {
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<crate::serve::history::Claim, HErr> {
        self.check()?;
        self.inner
            .claim_idempotency(key, fingerprint, run_id, window)
            .await
    }
    async fn upsert(&self, rec: &crate::serve::history::RunRecord) -> Result<(), HErr> {
        self.check()?;
        self.inner.upsert(rec).await
    }
    async fn insert_new(&self, rec: &crate::serve::history::RunRecord) -> Result<bool, HErr> {
        self.check()?;
        self.inner.insert_new(rec).await
    }
    async fn get(&self, id: &str) -> Result<Option<crate::serve::history::RunRecord>, HErr> {
        self.check()?;
        self.inner.get(id).await
    }
    async fn list(
        &self,
        filter: &crate::serve::history::ListFilter,
    ) -> Result<crate::serve::history::ListPage, HErr> {
        self.check()?;
        self.inner.list(filter).await
    }
    async fn delete(&self, id: &str) -> Result<crate::serve::history::DeleteOutcome, HErr> {
        self.check()?;
        self.inner.delete(id).await
    }
    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HErr> {
        self.check()?;
        self.inner.purge_expired(retain_for).await
    }
    async fn recover_orphans(&self) -> Result<usize, HErr> {
        self.check()?;
        self.inner.recover_orphans().await
    }
    fn degraded(&self) -> bool {
        false
    }
    async fn policy_version_insert(&self, record: &PolicyRecord) -> Result<bool, HErr> {
        self.check()?;
        self.inner.policy_version_insert(record).await
    }
    async fn policy_versions(&self, scope: &str, limit: usize) -> Result<Vec<PolicyRecord>, HErr> {
        self.check()?;
        self.inner.policy_versions(scope, limit).await
    }
    async fn policy_version_get(
        &self,
        scope: &str,
        version: u32,
    ) -> Result<Option<PolicyRecord>, HErr> {
        self.check()?;
        self.inner.policy_version_get(scope, version).await
    }
}

/// A server over a [`Flaky`] history behind the degraded fallback; set the
/// returned flag to take the "database" down.
pub fn flaky_state() -> (ServerState, Arc<std::sync::atomic::AtomicBool>) {
    let config = test_config();
    let down = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flaky = Flaky {
        inner: MemoryHistory::new(config.idempotency_retention),
        down: down.clone(),
    };
    let history = Arc::new(crate::serve::history::fallback::FallbackHistory::healthy(
        Box::new(flaky),
        config.idempotency_retention,
        "postgres",
    )) as Arc<dyn RunHistory>;
    let state = ServerState::new(
        &config,
        None,
        CancellationToken::new(),
        history,
        crate::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        crate::serve::triggers::health::TriggersHandle::empty(),
    );
    (state, down)
}

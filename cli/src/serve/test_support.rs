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
        templates_sync_path: None,
        policy_path: None,
        callback_allow_hosts: Vec::new(),
        require_approval: Vec::new(),
        approval_expiry: std::time::Duration::from_secs(86_400),
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

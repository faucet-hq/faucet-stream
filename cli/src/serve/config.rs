//! `ServeConfig` — the validated, runtime-ready server configuration built from
//! `ServeArgs`. The no-auth gate lives here so an unauthenticated server can
//! never start silently.

use crate::cli::ServeArgs;
use crate::error::{CliError, CliResult};
use crate::serve::cluster::ClusterConfig;
use crate::serve::preview::PreviewConfig;
use crate::serve::rbac::{AuthContext, RbacConfig, Role};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

/// How `/v1/*` requests are authenticated.
#[derive(Clone)]
pub enum AuthMode {
    /// Require `Authorization: Bearer <token>` — a single implicit `admin`
    /// principal.
    Token(String),
    /// Multi-principal RBAC from `--auth-config`: bearer token → principal → role.
    Rbac(Arc<RbacConfig>),
    /// Authentication explicitly disabled via `--no-auth`.
    None,
}

// Hand-written so `{:?}` of an `AuthMode` (or the `ServeConfig` that embeds one)
// never prints the bearer token in clear. The token is also registered for
// redaction in `ServeConfig::from_args`, but masking here closes the Debug path
// directly.
impl std::fmt::Debug for AuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMode::Token(_) => f.debug_tuple("Token").field(&"***").finish(),
            AuthMode::Rbac(cfg) => f.debug_tuple("Rbac").field(cfg).finish(),
            AuthMode::None => f.write_str("None"),
        }
    }
}

impl AuthMode {
    /// Resolve a request's `Authorization: Bearer` token to its [`AuthContext`],
    /// or `None` if the credential is missing / invalid. `--no-auth` yields an
    /// implicit `anonymous` admin so downstream authz + audit are uniform.
    pub fn resolve(&self, bearer: Option<&str>) -> Option<AuthContext> {
        match self {
            AuthMode::None => Some(AuthContext {
                principal: "anonymous".to_string(),
                role: Role::Admin,
                source_ip: None,
            }),
            AuthMode::Token(expected) => bearer
                .filter(|t| crate::serve::auth::constant_time_eq(t.as_bytes(), expected.as_bytes()))
                .map(|_| AuthContext {
                    principal: "token".to_string(),
                    role: Role::Admin,
                    source_ip: None,
                }),
            AuthMode::Rbac(cfg) => bearer.and_then(|t| cfg.authenticate(t)),
        }
    }
}

/// Run-history storage backend selection (parsed from `--history`).
#[derive(Debug, Clone)]
pub enum HistoryBackendSpec {
    /// In-process `DashMap`; lost on restart (default).
    Memory,
    /// Postgres connection URL (stored verbatim, e.g. `postgres://host/db`).
    Postgres(String),
    /// SQLite connection URL, stored verbatim including the `sqlite:` scheme
    /// (e.g. `sqlite:runs.db`, `sqlite::memory:`) so the backend can hand it
    /// straight to `sqlx` without re-deriving the form.
    Sqlite(String),
}

/// Validated server configuration.
#[derive(Debug, Clone)]
pub struct ServeConfig {
    pub listen: SocketAddr,
    pub auth: AuthMode,
    pub max_concurrent_runs: usize,
    pub max_queued_runs: usize,
    pub default_config_path: Option<PathBuf>,
    pub history: HistoryBackendSpec,
    pub cors_origins: Vec<String>,
    pub body_limit_bytes: usize,
    pub shutdown_grace: Duration,
    pub retain_terminal_runs: Duration,
    pub idempotency_retention: Duration,
    /// How long persisted run logs are kept (#529), independent of run-record
    /// retention. `0` disables durable log persistence (ephemeral SSE only).
    pub log_retention: Duration,
    /// Per-run cap on persisted log lines (#529). Past it, a truncation marker is
    /// recorded and further lines are dropped.
    pub log_max_lines_per_run: usize,
    /// Default retention window, in days, for the local files a run's sinks
    /// wrote (#587). `0` disables the automatic sweep; on-demand cleanup from the
    /// Datasets page / `faucet cleanup` still works. Overridable per pipeline via
    /// `local_outputs.retention_days`.
    pub local_output_retention_days: u32,
    /// Never delete a local sink output touched within this window — the guard
    /// against unlinking a file a live run is still writing (#587). `0` disables.
    pub local_output_in_flight_grace: Duration,
    /// Dataset-preview policy for local sink outputs (#586): the opt-in gate
    /// plus the soft/hard row caps every request is resolved against.
    pub preview: PreviewConfig,
    /// Run-ownership lease TTL for multi-instance orphan fencing (#146 H7).
    pub lease_ttl: Duration,
    pub probe_timeout: Duration,
    pub env_file: Option<PathBuf>,
    pub no_env_file: bool,
    /// Tracing filter directive for serve's own subscriber. Set from the
    /// clap-resolved `--log-level` / `FAUCET_LOG`; defaults to `"info"`.
    pub log_level: String,
    /// Rendering for serve's own stderr log stream (#634). Set from
    /// `--log-format` / `FAUCET_LOG_FORMAT`; defaults to text.
    pub log_format: crate::cli::LogFormat,
    /// Whether to serve the embedded web console. Built only when the `serve-ui`
    /// feature is on; this gates serving at runtime (`--no-ui`).
    #[cfg_attr(not(feature = "serve-ui"), allow(dead_code))]
    pub ui_enabled: bool,
    /// Clustered-execution settings (`--cluster*`). Disabled by default.
    pub cluster: ClusterConfig,
    /// Path to a `--triggers` file. `None` = no event-driven triggers. The file
    /// is loaded + validated at startup (gated on the `triggers` feature).
    pub triggers_path: Option<PathBuf>,
    /// Allowlist of hosts a per-run completion callback may target (#481).
    /// Empty = any host except link-local / cloud-metadata addresses.
    pub callback_allow_hosts: Vec<String>,
}

fn default_max_concurrent() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 16)
}

impl ServeConfig {
    /// Build + validate a `ServeConfig` from parsed CLI args. Enforces the
    /// no-auth gate: a server with neither a token nor `--no-auth` refuses to start.
    pub fn from_args(args: ServeArgs) -> CliResult<Self> {
        // RBAC (`--auth-config`) takes precedence and is mutually exclusive with
        // `--auth-token` / `--no-auth` (enforced by clap `conflicts_with`).
        let auth = if let Some(path) = args.auth_config {
            let rbac = RbacConfig::from_file(&path)?;
            // Register every principal token so the RedactingWriter scrubs it
            // from any tracing/log/error output for the process lifetime.
            for token in rbac.tokens() {
                crate::secrets::registry::register(token);
            }
            AuthMode::Rbac(Arc::new(rbac))
        } else {
            match (args.auth_token, args.no_auth) {
                (Some(t), _) if t.is_empty() => {
                    return Err(CliError::Serve(
                        "--auth-token / FAUCET_SERVE_AUTH_TOKEN must not be empty \
                         (use --no-auth to explicitly disable authentication)"
                            .into(),
                    ));
                }
                (Some(t), _) => {
                    // Register so the RedactingWriter scrubs the token from any
                    // tracing/log/error output for the lifetime of the process.
                    crate::secrets::registry::register(&t);
                    AuthMode::Token(t)
                }
                (None, true) => AuthMode::None,
                (None, false) => {
                    return Err(CliError::Serve(
                        "refusing to start without authentication: pass --auth-token \
                         (or FAUCET_SERVE_AUTH_TOKEN), --auth-config <file> for RBAC, \
                         or --no-auth to explicitly disable it"
                            .into(),
                    ));
                }
            }
        };

        let listen: SocketAddr = args
            .listen
            .parse()
            .map_err(|e| CliError::Serve(format!("invalid --listen '{}': {e}", args.listen)))?;

        let history = match args.history {
            None => HistoryBackendSpec::Memory,
            Some(u) if u.starts_with("postgres://") || u.starts_with("postgresql://") => {
                HistoryBackendSpec::Postgres(u)
            }
            Some(u) if u.starts_with("sqlite:") => HistoryBackendSpec::Sqlite(u),
            Some(u) => {
                return Err(CliError::Serve(format!(
                    "unrecognised --history '{u}': use a postgres:// URL or sqlite:<path>"
                )));
            }
        };

        if args.lease_ttl_secs == 0 {
            return Err(CliError::Serve(
                "--lease-ttl-secs must be > 0 (it gates multi-instance orphan recovery; \
                 0 would immediately expire every run's lease and let a maintenance tick \
                 fail in-flight runs)"
                    .into(),
            ));
        }

        // A zero drain window makes graceful shutdown `timeout(Duration::ZERO,
        // wait_drained())` time out instantly and cancel in-flight runs —
        // defeating the drain. Reject it (mirroring the lease-ttl gate).
        // `--idempotency-retention-secs == 0` is NOT rejected: it is an explicit
        // "dedup disabled" sentinel (every prior claim is immediately expired).
        if args.shutdown_grace_secs == 0 {
            return Err(CliError::Serve(
                "--shutdown-grace-secs must be > 0 (0 makes graceful shutdown cancel \
                 in-flight runs immediately instead of draining them; use a small \
                 value like 1)"
                    .into(),
            ));
        }

        let cluster = if args.cluster {
            if matches!(history, HistoryBackendSpec::Memory) {
                return Err(CliError::Serve(
                    "--cluster requires a persistent --history backend \
                     (postgres://… or sqlite:…); the in-memory store is single-process"
                        .into(),
                ));
            }
            if args.cluster_poll_secs == 0 {
                return Err(CliError::Serve(
                    "--cluster-poll-secs must be > 0 (0 would spin the claim loop \
                     with no back-off, saturating the history DB)"
                        .into(),
                ));
            }
            if args.cluster_max_attempts == 0 {
                return Err(CliError::Serve(
                    "--cluster-max-attempts must be > 0 (0 would never re-run \
                     orphaned runs, defeating failover)"
                        .into(),
                ));
            }
            ClusterConfig {
                enabled: true,
                poll: Duration::from_secs(args.cluster_poll_secs),
                max_attempts: args.cluster_max_attempts,
            }
        } else {
            ClusterConfig::disabled()
        };

        // `0` on either preview cap means "no limit" — the same convention as
        // `batch_size: 0` and `retention_days: 0` — so neither is rejected here.
        // A lifted ceiling is a deliberate local-testing choice, and worth a line
        // in the log: it is the difference between "a client may read 5000 rows"
        // and "a client may read the file".
        if args.preview_local_outputs && args.preview_max_rows == 0 {
            tracing::warn!(
                "--preview-max-rows 0: local-output previews have no row ceiling, so a \
                 request may read an entire output file into one response (still bounded \
                 by the preview byte budget and deadline)"
            );
        }

        let max_concurrent_runs = args
            .max_concurrent_runs
            .unwrap_or_else(default_max_concurrent)
            .max(1);
        let max_queued_runs = args
            .max_queued_runs
            .unwrap_or_else(|| max_concurrent_runs.saturating_mul(8))
            .max(1);

        Ok(Self {
            listen,
            auth,
            max_concurrent_runs,
            max_queued_runs,
            default_config_path: args.default_config,
            history,
            cors_origins: args.cors_origin,
            body_limit_bytes: args.body_limit_bytes,
            shutdown_grace: Duration::from_secs(args.shutdown_grace_secs),
            retain_terminal_runs: Duration::from_secs(args.retain_terminal_runs_secs),
            idempotency_retention: Duration::from_secs(args.idempotency_retention_secs),
            log_retention: Duration::from_secs(args.log_retention_secs),
            log_max_lines_per_run: args.log_max_lines_per_run,
            local_output_retention_days: args.local_output_retention_days,
            local_output_in_flight_grace: Duration::from_secs(
                args.local_output_in_flight_grace_secs,
            ),
            preview: PreviewConfig::new(
                args.preview_local_outputs,
                args.preview_default_rows,
                args.preview_max_rows,
            ),
            lease_ttl: Duration::from_secs(args.lease_ttl_secs),
            probe_timeout: Duration::from_secs(args.probe_timeout_secs),
            env_file: args.env_file,
            no_env_file: args.no_env_file,
            log_level: "info".to_string(),
            log_format: crate::cli::LogFormat::Text,
            ui_enabled: !args.no_ui,
            cluster,
            triggers_path: args.triggers,
            callback_allow_hosts: args.callback_allow_host,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn base_args() -> crate::cli::ServeArgs {
        crate::cli::ServeArgs {
            listen: "127.0.0.1:8080".into(),
            auth_token: None,
            auth_config: None,
            no_auth: false,
            max_concurrent_runs: None,
            max_queued_runs: None,
            default_config: None,
            history: None,
            cors_origin: vec![],
            body_limit_bytes: 1_048_576,
            shutdown_grace_secs: 60,
            retain_terminal_runs_secs: 604_800,
            idempotency_retention_secs: 86_400,
            log_retention_secs: 604_800,
            log_max_lines_per_run: 100_000,
            local_output_retention_days: 7,
            local_output_in_flight_grace_secs: 60,
            preview_local_outputs: false,
            preview_default_rows: crate::serve::preview::DEFAULT_PREVIEW_ROWS,
            preview_max_rows: crate::serve::preview::MAX_PREVIEW_ROWS,
            lease_ttl_secs: 30,
            probe_timeout_secs: 10,
            env_file: None,
            no_env_file: false,
            no_ui: false,
            cluster: false,
            cluster_poll_secs: 2,
            cluster_max_attempts: 3,
            triggers: None,
            callback_allow_host: Vec::new(),
            mcp: false,
            mcp_allow_mutations: false,
        }
    }

    #[test]
    fn no_auth_gate_rejects_silent_unauthenticated_start() {
        let err = ServeConfig::from_args(base_args()).unwrap_err();
        assert!(err.to_string().contains("--no-auth"), "{err}");
    }

    #[test]
    fn explicit_no_auth_is_allowed() {
        let mut a = base_args();
        a.no_auth = true;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(matches!(cfg.auth, AuthMode::None));
    }

    #[test]
    fn token_sets_token_auth() {
        let mut a = base_args();
        a.auth_token = Some("hunter2".into());
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(matches!(cfg.auth, AuthMode::Token(t) if t == "hunter2"));
    }

    #[test]
    fn resolve_none_is_anonymous_admin() {
        let ctx = AuthMode::None.resolve(None).unwrap();
        assert_eq!(ctx.principal, "anonymous");
        assert_eq!(ctx.role, Role::Admin);
    }

    #[test]
    fn resolve_token_matches_only_exact() {
        let mode = AuthMode::Token("s3cret".into());
        let ctx = mode.resolve(Some("s3cret")).unwrap();
        assert_eq!(ctx.principal, "token");
        assert_eq!(ctx.role, Role::Admin);
        assert!(mode.resolve(Some("wrong")).is_none());
        assert!(mode.resolve(None).is_none());
    }

    #[test]
    fn resolve_rbac_maps_token_to_principal() {
        use crate::serve::rbac::{PrincipalSpec, RbacConfig};
        let cfg = RbacConfig::new(vec![PrincipalSpec {
            name: "bob".into(),
            token: "viewer-tok".into(),
            role: Role::Viewer,
        }])
        .unwrap();
        let mode = AuthMode::Rbac(Arc::new(cfg));
        let ctx = mode.resolve(Some("viewer-tok")).unwrap();
        assert_eq!(ctx.principal, "bob");
        assert_eq!(ctx.role, Role::Viewer);
        assert!(mode.resolve(Some("nope")).is_none());
    }

    #[test]
    fn auth_config_file_builds_rbac_and_registers_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.yaml");
        std::fs::write(
            &path,
            "principals:\n  - name: carol\n    token: rbacsecret12345\n    role: operator\n",
        )
        .unwrap();
        let mut a = base_args();
        a.auth_config = Some(path);
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(matches!(cfg.auth, AuthMode::Rbac(_)));
        // Every principal token must be registered for log redaction.
        let scrubbed = crate::secrets::registry::redact("t=rbacsecret12345 end").into_owned();
        assert!(!scrubbed.contains("rbacsecret12345"), "{scrubbed}");
    }

    #[test]
    fn auth_config_debug_masks_tokens() {
        use crate::serve::rbac::{PrincipalSpec, RbacConfig};
        let cfg = RbacConfig::new(vec![PrincipalSpec {
            name: "x".into(),
            token: "supersecretrbac".into(),
            role: Role::Admin,
        }])
        .unwrap();
        let s = format!("{:?}", AuthMode::Rbac(Arc::new(cfg)));
        assert!(!s.contains("supersecretrbac"), "rbac token leaked: {s}");
    }

    #[test]
    fn auth_mode_debug_masks_token() {
        // ServeConfig derives Debug and embeds AuthMode; a {:?} must not print
        // the bearer token in clear.
        let s = format!("{:?}", AuthMode::Token("supersecrettoken".into()));
        assert!(
            !s.contains("supersecrettoken"),
            "serve token leaked via Debug: {s}"
        );
        assert!(s.contains("***"), "token not masked: {s}");
    }

    #[test]
    fn token_is_registered_for_redaction() {
        let mut a = base_args();
        a.auth_token = Some("uniqueserveauthsecret987".into());
        let _cfg = ServeConfig::from_args(a).unwrap();
        // The token must be registered so the RedactingWriter scrubs it from logs.
        let scrubbed =
            crate::secrets::registry::redact("hdr=uniqueserveauthsecret987 end").into_owned();
        assert!(
            !scrubbed.contains("uniqueserveauthsecret987"),
            "serve token not registered for redaction: {scrubbed}"
        );
    }

    #[test]
    fn listen_parses_to_socket_addr() {
        let mut a = base_args();
        a.no_auth = true;
        a.listen = "0.0.0.0:9999".into();
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.listen, "0.0.0.0:9999".parse::<SocketAddr>().unwrap());
    }

    #[test]
    fn history_url_selects_backend() {
        let mut a = base_args();
        a.no_auth = true;
        a.history = Some("postgres://localhost/db".into());
        assert!(matches!(
            ServeConfig::from_args(a.clone()).unwrap().history,
            HistoryBackendSpec::Postgres(_)
        ));
        // The `postgresql://` alias maps to the same backend.
        a.history = Some("postgresql://localhost/db".into());
        assert!(matches!(
            ServeConfig::from_args(a.clone()).unwrap().history,
            HistoryBackendSpec::Postgres(_)
        ));
        // SQLite is stored verbatim, scheme included (for direct sqlx use).
        a.history = Some("sqlite:runs.db".into());
        match ServeConfig::from_args(a).unwrap().history {
            HistoryBackendSpec::Sqlite(url) => assert_eq!(url, "sqlite:runs.db"),
            other => panic!("expected Sqlite, got {other:?}"),
        }
    }

    #[test]
    fn empty_token_is_rejected() {
        let mut a = base_args();
        a.auth_token = Some(String::new());
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("must not be empty"), "{err}");
    }

    #[test]
    fn invalid_listen_returns_error() {
        let mut a = base_args();
        a.no_auth = true;
        a.listen = "not-a-socket".into();
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("invalid --listen"), "{err}");
    }

    #[test]
    fn unrecognised_history_scheme_returns_error() {
        let mut a = base_args();
        a.no_auth = true;
        a.history = Some("mysql://localhost/db".into());
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("unrecognised --history"), "{err}");
    }

    #[test]
    fn zero_lease_ttl_is_rejected() {
        let mut a = base_args();
        a.no_auth = true;
        a.lease_ttl_secs = 0;
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("--lease-ttl-secs"), "{err}");
    }

    #[test]
    fn lease_ttl_maps_to_duration() {
        let mut a = base_args();
        a.no_auth = true;
        a.lease_ttl_secs = 45;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.lease_ttl, Duration::from_secs(45));
    }

    #[test]
    fn zero_shutdown_grace_is_rejected() {
        // A zero drain window makes graceful shutdown `timeout(Duration::ZERO,
        // wait_drained())` cancel in-flight runs instantly — defeating the
        // drain. Reject it the same way the lease-ttl gate does.
        let mut a = base_args();
        a.no_auth = true;
        a.shutdown_grace_secs = 0;
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("--shutdown-grace-secs"), "{err}");
    }

    #[test]
    fn nonzero_shutdown_grace_is_accepted() {
        let mut a = base_args();
        a.no_auth = true;
        a.shutdown_grace_secs = 5;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.shutdown_grace, Duration::from_secs(5));
    }

    #[test]
    fn zero_idempotency_retention_is_accepted_as_dedup_disabled_sentinel() {
        // `idempotency_retention_secs == 0` is an explicit "dedup disabled"
        // sentinel (every prior claim is immediately expired), NOT an error.
        let mut a = base_args();
        a.no_auth = true;
        a.idempotency_retention_secs = 0;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.idempotency_retention, Duration::ZERO);
    }

    #[test]
    fn cluster_requires_persistent_history() {
        let mut a = base_args();
        a.no_auth = true;
        a.cluster = true; // history defaults to memory
        let err = ServeConfig::from_args(a).unwrap_err();
        assert!(err.to_string().contains("--cluster requires"), "{err}");
    }

    #[test]
    fn cluster_with_sqlite_is_enabled() {
        let mut a = base_args();
        a.no_auth = true;
        a.cluster = true;
        a.history = Some("sqlite:runs.db".into());
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(cfg.cluster.enabled);
        assert_eq!(cfg.cluster.max_attempts, 3);
    }

    #[test]
    fn preview_is_off_by_default_with_the_documented_caps() {
        let mut a = base_args();
        a.no_auth = true;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(!cfg.preview.enabled, "preview must be opt-in");
        assert_eq!(cfg.preview.default_rows, 500);
        assert_eq!(cfg.preview.max_rows, 5_000);
    }

    #[test]
    fn preview_flag_and_caps_reach_the_config() {
        let mut a = base_args();
        a.no_auth = true;
        a.preview_local_outputs = true;
        a.preview_default_rows = 25;
        a.preview_max_rows = 250;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert!(cfg.preview.enabled);
        assert_eq!(cfg.preview.default_rows, 25);
        assert_eq!(cfg.preview.max_rows, 250);
    }

    #[test]
    fn a_soft_cap_above_the_hard_cap_boots_clamped_rather_than_failing() {
        // Lowering only the hard cap must not turn a running deployment into a
        // crash loop; the ceiling wins and the server starts.
        let mut a = base_args();
        a.no_auth = true;
        a.preview_default_rows = 100;
        a.preview_max_rows = 20;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.preview.default_rows, 20);
        assert_eq!(cfg.preview.max_rows, 20);
    }

    #[test]
    fn a_zero_hard_cap_lifts_the_ceiling_rather_than_failing_to_boot() {
        // `0` = no limit, the same convention as `batch_size` / `retention_days`.
        // It is how an operator allows a whole-dataset preview.
        use crate::serve::preview::{RowCap, RowRequest};
        let mut a = base_args();
        a.no_auth = true;
        a.preview_local_outputs = true;
        a.preview_max_rows = 0;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.preview.max_rows, 0);
        assert_eq!(cfg.preview.max_rows(), None);
        assert_eq!(
            cfg.preview.resolve_rows(Some(RowRequest::All)),
            RowCap::Unlimited
        );
    }

    #[test]
    fn a_zero_soft_cap_previews_everything_by_default() {
        use crate::serve::preview::RowCap;
        let mut a = base_args();
        a.no_auth = true;
        a.preview_default_rows = 0;
        a.preview_max_rows = 0;
        let cfg = ServeConfig::from_args(a).unwrap();
        assert_eq!(cfg.preview.resolve_rows(None), RowCap::Unlimited);
    }

    #[test]
    fn the_preview_flag_parses_from_the_command_line() {
        use clap::Parser;
        // The gate is a bool flag with an `env`, so clap gives it the strict
        // `bool` value parser: `FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS=false` is off
        // (what the Helm chart emits when disabled) and a garbage value is a
        // startup error rather than a silent "on".
        let args = crate::cli::ServeArgs::try_parse_from([
            "serve",
            "--no-auth",
            "--preview-local-outputs",
            "--preview-default-rows",
            "7",
            "--preview-max-rows",
            "9",
        ])
        .expect("flags parse");
        assert!(args.preview_local_outputs);
        assert_eq!(args.preview_default_rows, 7);
        assert_eq!(args.preview_max_rows, 9);

        let off = crate::cli::ServeArgs::try_parse_from(["serve", "--no-auth"]).unwrap();
        assert!(!off.preview_local_outputs);
    }

    #[test]
    fn cluster_disabled_by_default() {
        let mut a = base_args();
        a.no_auth = true;
        assert!(!ServeConfig::from_args(a).unwrap().cluster.enabled);
    }
}

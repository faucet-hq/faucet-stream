//! Low-level replication-connection wrapper.
//!
//! This module wraps [`pgwire_replication`] to provide the slot lifecycle
//! (`ensure_slot`) and streaming helpers (`start_replication`, `recv`,
//! `send_status_update`) used by the rest of the CDC source.
//!
//! # Design
//!
//! `pgwire_replication` handles everything from TCP connect through auth,
//! `START_REPLICATION`, keepalive replies, and `StandbyStatusUpdate` — all
//! internally.  The library delivers events as a typed enum; [`recv`] surfaces
//! the full [`ReplicationEvent`] to callers (absorbing only [`KeepAlive`] and
//! [`StoppedAt`] internally) so Tasks 9+ can observe transaction boundaries.
//!
//! Slot creation (`CREATE_REPLICATION_SLOT`) is a control-plane operation that
//! requires an ordinary (non-replication) SQL connection, so `ensure_slot`
//! uses [`sqlx`] for that single query.
//!
//! # Type aliases
//!
//! The plan requires stable names `Client` and `Duplex` so that Tasks 9+ can
//! refer to concrete types.  We define:
//!
//! - [`Client`] — a lightweight holder of `ReplicationParams` used to verify
//!   connectivity and create the replication slot, before the stream is
//!   opened.
//! - [`Duplex`] — the live replication stream; a thin wrapper around
//!   [`pgwire_replication::ReplicationClient`].
//!
//! [`KeepAlive`]: ReplicationEvent::KeepAlive
//! [`StoppedAt`]: ReplicationEvent::StoppedAt

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use faucet_core::FaucetError;
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, TlsConfig};
use sqlx::postgres::PgConnectOptions;

/// Re-export so downstream modules (`stream.rs`, Task 9) can import the event
/// type without depending on `pgwire_replication` directly.
pub use pgwire_replication::ReplicationEvent;
use sqlx::{Executor, PgConnection};
use tracing::debug;

/// Microseconds between the Unix epoch (1970-01-01) and the Postgres epoch
/// (2000-01-01).  Used for converting between Postgres timestamps and Unix time.
pub const POSTGRES_EPOCH_MICROS: i64 = 946_684_800_000_000;

// ── Public type aliases ────────────────────────────────────────────────────

/// Pre-stream handle returned by [`connect`] once the connection URL has been
/// validated. The actual connection is opened by [`ensure_slot`] /
/// [`start_replication`] from the borrowed [`ReplicationParams`], so this is a
/// lightweight marker — it deliberately holds no owned copy of the connection
/// string, which previously sat in leaked (`Box::leak`) heap for the process
/// lifetime, including the password (#78/#13).
pub struct Client {
    _private: (),
}

/// Live replication stream. Obtained from [`start_replication`].
///
/// A pump task owns the [`pgwire_replication::ReplicationClient`] and drains
/// its events into a local buffer the whole time, so the client's worker —
/// which sends the Standby Status Updates — never blocks on a full channel
/// while the pipeline is busy writing a page. Without it, a sink write longer
/// than the server's `wal_sender_timeout` during catch-up got the replication
/// connection killed (#789 SQL-88). The buffer holds at most
/// `max_buffered_events` events and `max_buffered_bytes` of change data; past
/// either limit the pump stops reading until the consumer catches up.
pub struct Duplex {
    events:
        tokio::sync::mpsc::UnboundedReceiver<(Result<Option<ReplicationEvent>, FaucetError>, u32)>,
    budget: ReadAhead,
    applied: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pump: tokio::task::JoinHandle<()>,
}

impl Drop for Duplex {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

trait EventSource: Send + 'static {
    fn update_applied(&mut self, lsn: u64);
    fn recv(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<ReplicationEvent>, FaucetError>> + Send;
}

impl EventSource for ReplicationClient {
    fn update_applied(&mut self, lsn: u64) {
        self.update_applied_lsn(Lsn::from_u64(lsn));
    }

    async fn recv(&mut self) -> Result<Option<ReplicationEvent>, FaucetError> {
        ReplicationClient::recv(self)
            .await
            .map_err(|e| pgwire_err("postgres-cdc recv", e))
    }
}

/// Event-count and byte budget of the read-ahead buffer.
#[derive(Clone)]
struct ReadAhead {
    events: std::sync::Arc<tokio::sync::Semaphore>,
    bytes: std::sync::Arc<tokio::sync::Semaphore>,
    max_bytes: u32,
}

impl ReadAhead {
    fn new(max_events: usize, max_bytes: usize) -> Self {
        let max_bytes = u32::try_from(max_bytes.max(1)).unwrap_or(u32::MAX);
        Self {
            events: std::sync::Arc::new(tokio::sync::Semaphore::new(max_events.max(1))),
            bytes: std::sync::Arc::new(tokio::sync::Semaphore::new(max_bytes as usize)),
            max_bytes,
        }
    }

    fn charge(&self, event: &Result<Option<ReplicationEvent>, FaucetError>) -> u32 {
        let size = match event {
            Ok(Some(ReplicationEvent::XLogData { data, .. })) => data.len(),
            _ => 0,
        };
        u32::try_from(size).unwrap_or(u32::MAX).min(self.max_bytes)
    }

    fn release(&self, bytes: u32) {
        self.events.add_permits(1);
        self.bytes.add_permits(bytes as usize);
    }
}

impl Duplex {
    fn spawn(client: ReplicationClient, max_buffered: usize, max_bytes: usize) -> Self {
        Self::spawn_source(client, ReadAhead::new(max_buffered, max_bytes))
    }

    fn spawn_source<S: EventSource>(mut client: S, budget: ReadAhead) -> Self {
        let (tx, events) = tokio::sync::mpsc::unbounded_channel();
        let applied = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let (pump_budget, lsn) = (budget.clone(), applied.clone());
        let pump = tokio::spawn(async move {
            loop {
                let Ok(permit) = pump_budget.events.clone().acquire_owned().await else {
                    return;
                };
                permit.forget();
                let current = lsn.load(std::sync::atomic::Ordering::Acquire);
                if current > 0 {
                    client.update_applied(current);
                }
                let next = client.recv().await;
                let size = pump_budget.charge(&next);
                if size > 0 {
                    let Ok(bytes) = pump_budget.bytes.clone().acquire_many_owned(size).await else {
                        return;
                    };
                    bytes.forget();
                }
                let last = !matches!(next, Ok(Some(_)));
                if tx.send((next, size)).is_err() || last {
                    return;
                }
            }
        });
        Self {
            events,
            budget,
            applied,
            pump,
        }
    }

    async fn next_event(&mut self) -> Result<Option<ReplicationEvent>, FaucetError> {
        match self.events.recv().await {
            Some((event, size)) => {
                self.budget.release(size);
                event
            }
            None => {
                self.budget.release(0);
                Err(FaucetError::Source(
                    "postgres-cdc: replication reader stopped unexpectedly".into(),
                ))
            }
        }
    }
}

// ── Parameters ────────────────────────────────────────────────────────────

/// All parameters required to establish a logical replication connection.
///
/// This struct is accepted by every function in this module.
#[derive(Clone, Debug)]
pub struct ReplicationParams<'a> {
    /// `postgres://user:pass@host:port/db` style URL.
    pub connection_url: &'a str,
    /// Name of the replication slot (must already exist, or `create_slot_if_missing = true`).
    pub slot_name: &'a str,
    /// Publication name — must already exist on the server.
    pub publication_name: &'a str,
    /// pgoutput protocol version. Only `1` is currently supported.
    pub proto_version: u32,
    /// Create the slot if it does not already exist.
    pub create_slot_if_missing: bool,
    /// Optional LSN to resume from.  `None` means "start from the slot's
    /// `confirmed_flush_lsn`".
    pub start_lsn: Option<u64>,
    /// Protocol-level Standby Status Update cadence — must be shorter than
    /// the server's `wal_sender_timeout`.
    pub status_update_interval: Duration,
    /// TCP-level keepalive interval. Larger than `status_update_interval`
    /// in normal operation.
    pub tcp_keepalive: Duration,
    /// Whether a newly-created slot is permanent or temporary.
    pub slot_type: crate::config::SlotType,
    /// TLS settings for the replication connection.
    pub tls: &'a crate::config::CdcTls,
    /// Events read ahead of the consumer while it is busy (see [`Duplex`]).
    pub max_buffered_events: usize,
    /// Bytes of change data read ahead of the consumer (see [`Duplex`]).
    pub max_buffered_bytes: usize,
}

// ── Helper: parse a postgres URL into (host, port, user, password, dbname) ─

#[cfg_attr(test, derive(Debug))]
pub(crate) struct PgCoords {
    pub(crate) host: String,
    pub(crate) port: u16,
    user: String,
    password: String,
    pub(crate) dbname: String,
}

pub(crate) fn parse_url(url: &str) -> Result<PgCoords, FaucetError> {
    // Parse via the standard `url` crate so we can extract all components
    // without relying on sqlx accessor methods that may not be public.
    let parsed = url::Url::parse(url)
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;

    // Fail fast on a missing host or user rather than silently defaulting to
    // `localhost` / empty — a malformed URL otherwise connects to an
    // unintended local instance or produces a confusing late auth failure
    // (#78/#47). Port (5432) and dbname (the user's DB) keep their standard
    // libpq-style defaults since omitting them is conventional.
    let host = parsed
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| {
            FaucetError::Config(
                "postgres-cdc: connection URL is missing a host (expected \
                 postgres://user@host[:port]/dbname)"
                    .to_owned(),
            )
        })?
        .to_owned();
    let port = parsed.port().unwrap_or(5432);
    // The `url` crate returns userinfo and path percent-encoded; libpq (and
    // the sqlx control connections) decode them, and let `?user=`,
    // `?password=` and `?dbname=` override them (#789 SQL-86).
    let decode = |s: &str| -> Result<String, FaucetError> {
        percent_encoding::percent_decode_str(s)
            .decode_utf8()
            .map(|c| c.into_owned())
            .map_err(|e| FaucetError::Config(format!("postgres-cdc: connection URL: {e}")))
    };
    let mut user = decode(parsed.username())?;
    let mut password = decode(parsed.password().unwrap_or(""))?;
    let mut dbname = decode(parsed.path().trim_start_matches('/'))?;
    for (k, v) in parsed.query_pairs() {
        match k.as_ref() {
            "user" => user = v.into_owned(),
            "password" => password = v.into_owned(),
            "dbname" => dbname = v.into_owned(),
            _ => {}
        }
    }
    if user.is_empty() {
        return Err(FaucetError::Config(
            "postgres-cdc: connection URL is missing a user (expected \
             postgres://user@host[:port]/dbname)"
                .to_owned(),
        ));
    }
    let dbname = if dbname.is_empty() {
        "postgres".to_owned()
    } else {
        dbname
    };

    Ok(PgCoords {
        host,
        port,
        user,
        password,
        dbname,
    })
}

// ── Public API ─────────────────────────────────────────────────────────────

/// Validate connectivity and return a [`Client`] handle.
///
/// This function parses the connection URL and records the parameters.
/// It does **not** open a TCP connection; actual connectivity is verified
/// lazily when [`ensure_slot`] or [`start_replication`] is called.
pub async fn connect(params: &ReplicationParams<'_>) -> Result<Client, FaucetError> {
    // Eagerly validate the URL so bad configs fail fast. No part of the
    // connection string is retained past this call.
    let _ = parse_url(params.connection_url)?;
    Ok(Client { _private: () })
}

/// Ensure the replication slot exists.
///
/// If the slot already exists this is a no-op.  If it does not exist and
/// `create_if_missing` is `true`, the slot is created via
/// `pg_create_logical_replication_slot`.  If `create_if_missing` is `false`
/// and the slot is absent, an error is returned.
pub async fn ensure_slot(
    _client: &Client,
    connection_url: &str,
    slot_name: &str,
    create_if_missing: bool,
    slot_type: crate::config::SlotType,
    tls: &crate::config::CdcTls,
) -> Result<(), FaucetError> {
    use crate::config::SlotType;
    // Use sqlx for the control-plane query (not a replication connection).
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);

    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| pg_err("postgres-cdc ensure_slot connect", e))?;

    // Check whether the slot already exists.
    let row: Option<(String,)> =
        sqlx::query_as("SELECT slot_name::text FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot_name)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| pg_err("postgres-cdc slot lookup", e))?;

    if row.is_some() {
        debug!("postgres-cdc: replication slot '{slot_name}' already exists");
        return Ok(());
    }

    if !create_if_missing {
        return Err(FaucetError::Source(format!(
            "postgres-cdc: replication slot '{slot_name}' does not exist \
             and create_slot_if_missing = false"
        )));
    }

    // Create the slot using the pgoutput plugin. The third arg to
    // pg_create_logical_replication_slot is `temporary`: a temporary slot is
    // dropped when this session disconnects; a permanent slot persists (and
    // pins WAL) until explicitly dropped.
    // `escape_simple` prevents injection via the slot name (already validated
    // to [a-z0-9_] by config, but defence-in-depth doesn't hurt).
    let temporary = matches!(slot_type, SlotType::Temporary);
    let sql = format!(
        "SELECT pg_create_logical_replication_slot({}, 'pgoutput', {})",
        quote_literal(slot_name),
        temporary
    );
    conn.execute(sql.as_str())
        .await
        .map_err(|e| pg_err("postgres-cdc create slot", e))?;

    if temporary {
        debug!("postgres-cdc: created temporary replication slot '{slot_name}'");
    } else {
        // A permanent slot retains WAL until consumed or dropped — surface it
        // loudly so an abandoned slot doesn't silently fill pg_wal (#78/#12).
        tracing::warn!(
            "postgres-cdc: created PERMANENT replication slot '{slot_name}' — it will retain \
             WAL on the server until consumed or explicitly dropped (drop_slot). Use \
             slot_type=temporary for ephemeral runs."
        );
    }
    Ok(())
}

/// Drop a logical replication slot via a control-plane SQL call
/// (`pg_drop_replication_slot`). A missing slot is treated as success (no-op);
/// an active slot (currently in use by another connection) surfaces an error.
pub async fn drop_slot(
    connection_url: &str,
    slot_name: &str,
    tls: &crate::config::CdcTls,
) -> Result<(), FaucetError> {
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);
    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| pg_err("postgres-cdc drop_slot connect", e))?;

    let exists: Option<(String,)> =
        sqlx::query_as("SELECT slot_name::text FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot_name)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| pg_err("postgres-cdc slot lookup", e))?;
    if exists.is_none() {
        debug!("postgres-cdc: replication slot '{slot_name}' already absent; drop is a no-op");
        return Ok(());
    }

    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(slot_name)
        .execute(&mut conn)
        .await
        .map_err(|e| pg_err("postgres-cdc drop slot", e))?;
    debug!("postgres-cdc: dropped replication slot '{slot_name}'");
    Ok(())
}

/// Apply the CDC [`CdcTls`](crate::config::CdcTls) policy onto the sqlx
/// `PgConnectOptions` used for the **control-plane** connections (slot
/// create/drop/advance, LSN capture). Without this these privileged
/// connections would use sqlx's default `Prefer` SSL mode — opportunistic and
/// unverified — so `tls: verify_full` would silently fail to protect them, a
/// TLS-downgrade / MITM window on the same credentials the replication stream
/// guards (audit #321 M5). The explicit policy overrides any `sslmode=` in the
/// URL (the config is authoritative).
pub(crate) fn apply_cdc_tls(
    opts: PgConnectOptions,
    tls: &crate::config::CdcTls,
) -> PgConnectOptions {
    use crate::config::CdcTls;
    use sqlx::postgres::PgSslMode;
    faucet_core::ensure_crypto_provider();
    match tls {
        CdcTls::FromUrl | CdcTls::Disable => opts.ssl_mode(PgSslMode::Disable),
        CdcTls::Prefer => opts.ssl_mode(PgSslMode::Prefer),
        CdcTls::Require => opts.ssl_mode(PgSslMode::Require),
        CdcTls::VerifyCa { ca_path } => {
            let o = opts.ssl_mode(PgSslMode::VerifyCa);
            match ca_path {
                Some(p) => o.ssl_root_cert(p),
                None => o,
            }
        }
        CdcTls::VerifyFull { ca_path } => {
            let o = opts.ssl_mode(PgSslMode::VerifyFull);
            match ca_path {
                Some(p) => o.ssl_root_cert(p),
                None => o,
            }
        }
    }
}

/// Map the user-facing [`CdcTls`](crate::config::CdcTls) config onto
/// pgwire-replication's [`TlsConfig`].
fn tls_config(tls: &crate::config::CdcTls) -> TlsConfig {
    use crate::config::CdcTls;
    faucet_core::ensure_crypto_provider();
    use std::path::PathBuf;
    match tls {
        CdcTls::FromUrl | CdcTls::Disable => TlsConfig::disabled(),
        CdcTls::Prefer => TlsConfig {
            mode: pgwire_replication::SslMode::Prefer,
            ..TlsConfig::disabled()
        },
        CdcTls::Require => TlsConfig::require(),
        CdcTls::VerifyCa { ca_path } => TlsConfig::verify_ca(ca_path.clone().map(PathBuf::from)),
        CdcTls::VerifyFull { ca_path } => {
            TlsConfig::verify_full(ca_path.clone().map(PathBuf::from))
        }
    }
}

/// Advance the slot's `confirmed_flush_lsn` to `lsn` via a control-plane SQL
/// call (`pg_replication_slot_advance`), **before** the replication stream is
/// opened.
///
/// This is how the connector resumes past already-consumed, durably-persisted
/// changes without ever advancing the slot ahead of durability (#78/#1). For
/// a logical slot, `START_REPLICATION` resumes decoding from the slot's
/// `confirmed_flush_lsn` — the client-supplied start LSN does not filter
/// transactions that committed below it — so the only way to skip consumed
/// changes is to move `confirmed_flush_lsn` forward here, while the slot is
/// inactive. `pg_replication_slot_advance` refuses to move a slot backwards,
/// so an `lsn` at or below the slot's `confirmed_flush_lsn` is not sent: equal
/// is the normal resume, and behind (a restored older bookmark, a `start_lsn`
/// override) is logged — the changes in between were already confirmed and a
/// slot cannot replay them; re-reading them needs a new slot (#789 SQL-154).
///
/// The slot must be inactive, which it is between [`ensure_slot`] and
/// [`start_replication`].
pub async fn advance_slot(
    connection_url: &str,
    slot_name: &str,
    lsn: u64,
    tls: &crate::config::CdcTls,
) -> Result<(), FaucetError> {
    if lsn == 0 {
        return Ok(());
    }
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);

    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| pg_err("postgres-cdc advance_slot connect", e))?;

    let confirmed: Option<(Option<String>,)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot_name)
    .fetch_optional(&mut conn)
    .await
    .map_err(|e| pg_err("postgres-cdc slot lsn lookup", e))?;
    let confirmed = confirmed
        .and_then(|(c,)| c)
        .map(|c| crate::state::parse_lsn(&c))
        .transpose()?;
    match advance_decision(lsn, confirmed) {
        AdvanceDecision::Advance => {}
        AdvanceDecision::AtSlot => return Ok(()),
        AdvanceDecision::BehindSlot(at) => {
            tracing::warn!(
                slot = slot_name,
                resume = %crate::state::format_lsn(lsn),
                slot_confirmed = %crate::state::format_lsn(at),
                "postgres-cdc: the resume position is behind the slot; the changes in between \
                 were already confirmed and cannot be replayed from this slot — resuming from \
                 the slot. Re-reading them needs a new slot and a fresh snapshot"
            );
            return Ok(());
        }
    }
    // Bind the slot name and the LSN (as pg_lsn text) as parameters — no string
    // interpolation into the SQL. `format_lsn` emits Postgres' canonical
    // `X/X` text form.
    sqlx::query("SELECT pg_replication_slot_advance($1, $2::pg_lsn)")
        .bind(slot_name)
        .bind(crate::state::format_lsn(lsn))
        .execute(&mut conn)
        .await
        .map_err(|e| pg_err("postgres-cdc advance_slot", e))?;

    debug!("postgres-cdc: advanced slot '{slot_name}' confirmed_flush_lsn to {lsn:#x}");
    Ok(())
}

/// Fail with the slot-active SQLSTATE while another walsender still holds the
/// slot. `START_REPLICATION`'s own refusal only surfaces from the stream's first
/// `recv`, past [`retry_on_slot_active`], so callers check here first and the
/// retry waits out a previous run's connection on a rapid restart.
pub async fn ensure_slot_inactive(
    connection_url: &str,
    slot_name: &str,
    tls: &crate::config::CdcTls,
) -> Result<(), FaucetError> {
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);
    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| pg_err("postgres-cdc slot activity connect", e))?;
    let row: Option<(bool, Option<i32>)> =
        sqlx::query_as("SELECT active, active_pid FROM pg_replication_slots WHERE slot_name = $1")
            .bind(slot_name)
            .fetch_optional(&mut conn)
            .await
            .map_err(|e| pg_err("postgres-cdc slot activity lookup", e))?;
    slot_activity_result(slot_name, row)
}

fn slot_activity_result(
    slot_name: &str,
    row: Option<(bool, Option<i32>)>,
) -> Result<(), FaucetError> {
    match row {
        Some((true, pid)) => Err(FaucetError::Custom(Box::new(PostgresError::new(
            Some(SQLSTATE_OBJECT_IN_USE.to_string()),
            "postgres-cdc start_replication",
            format!(
                "replication slot \"{slot_name}\" is active for PID {}",
                pid.map_or_else(|| "?".to_string(), |p| p.to_string())
            ),
        )))),
        _ => Ok(()),
    }
}

/// What [`advance_slot`] does for a resume `lsn` against the slot's
/// `confirmed_flush_lsn`.
#[derive(Debug, PartialEq, Eq)]
enum AdvanceDecision {
    Advance,
    AtSlot,
    BehindSlot(u64),
}

fn advance_decision(lsn: u64, confirmed: Option<u64>) -> AdvanceDecision {
    match confirmed {
        Some(c) if lsn == c => AdvanceDecision::AtSlot,
        Some(c) if lsn < c => AdvanceDecision::BehindSlot(c),
        _ => AdvanceDecision::Advance,
    }
}

/// Ensure the slot exists, then return the slot's **own consistent point** —
/// its `confirmed_flush_lsn` (falling back to `restart_lsn`, then the server's
/// `pg_current_wal_lsn()` only if both are null).
///
/// Anchoring on the slot's consistent point — rather than the server's *current*
/// WAL position — is what makes capture-before-snapshot gapless even when the
/// slot **already existed** with unconsumed WAL behind it. Using
/// `pg_current_wal_lsn()` would skip every change the slot had retained between
/// its confirmed position and "now", silently losing those CDC events (F8).
/// For a freshly-created logical slot, `confirmed_flush_lsn` is the slot's
/// creation consistent point, so the fresh-slot behaviour is preserved.
///
/// Ensuring the slot **first** guarantees the server retains WAL from the
/// returned LSN onward, so a later `START_REPLICATION` from this position has
/// no gap. Used by the replication snapshot→CDC handoff
/// ([`Source::capture_resume_position`](faucet_core::Source::capture_resume_position)).
pub async fn ensure_slot_and_current_lsn(
    connection_url: &str,
    slot_name: &str,
    create_if_missing: bool,
    slot_type: crate::config::SlotType,
    tls: &crate::config::CdcTls,
) -> Result<u64, FaucetError> {
    // `ensure_slot`'s `_client` arg is unused; `Client` is constructible here
    // (same module). This reuses the existing slot existence/create logic.
    let client = Client { _private: () };
    ensure_slot(
        &client,
        connection_url,
        slot_name,
        create_if_missing,
        slot_type,
        tls,
    )
    .await?;

    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);
    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| pg_err("postgres-cdc capture_position connect", e))?;
    // Prefer the slot's retained consistent point over the server's current LSN.
    let slot_lsn: Option<(Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text, restart_lsn::text \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot_name)
    .fetch_optional(&mut conn)
    .await
    .map_err(|e| pg_err("postgres-cdc slot lsn lookup", e))?;
    let anchor = match slot_lsn {
        Some((confirmed, restart)) => confirmed.or(restart),
        None => None,
    };
    let lsn_text = match anchor {
        Some(t) => t,
        None => {
            // Slot reports no LSN yet (e.g. just-created temporary slot on some
            // versions) — fall back to the server's current WAL position.
            let (cur,): (String,) = sqlx::query_as("SELECT pg_current_wal_lsn()::text")
                .fetch_one(&mut conn)
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("postgres-cdc pg_current_wal_lsn: {e}"))
                })?;
            cur
        }
    };
    crate::state::parse_lsn(&lsn_text)
}

/// The server's current WAL position and the slot's `confirmed_flush_lsn`
/// (#733), or `None` when the slot does not exist. Read-only.
pub async fn slot_positions(
    connection_url: &str,
    slot_name: &str,
    tls: &crate::config::CdcTls,
) -> Result<Option<(u64, Option<u64>)>, FaucetError> {
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);
    use sqlx::ConnectOptions as _;
    let mut conn: PgConnection = opts
        .connect()
        .await
        .map_err(|e| pg_err("postgres-cdc lag connect", e))?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT pg_current_wal_lsn()::text, confirmed_flush_lsn::text \
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(slot_name)
    .fetch_optional(&mut conn)
    .await
    .map_err(|e| pg_err("postgres-cdc lag query", e))?;
    let Some((current, confirmed)) = row else {
        return Ok(None);
    };
    let confirmed = confirmed
        .as_deref()
        .map(crate::state::parse_lsn)
        .transpose()?;
    Ok(Some((crate::state::parse_lsn(&current)?, confirmed)))
}

/// The partitioned table (`schema.table`) each of `oids` is a leaf partition
/// of. Oids that are not partitions are absent. pgoutput names a change after
/// the leaf unless the publication sets `publish_via_partition_root`, while
/// discovery lists only the root, so records are routed by this root.
pub async fn partition_roots(
    connection_url: &str,
    tls: &crate::config::CdcTls,
    oids: &[u32],
) -> Result<std::collections::HashMap<u32, String>, FaucetError> {
    if oids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let mut conn = control_connection(connection_url, tls).await?;
    partition_roots_on(&mut conn, oids).await
}

/// The fractional digits the server prints `money` with under the
/// `lc_monetary` locale this role and database get — the setting the
/// walsender renders change values in.
pub async fn money_fraction_digits(
    connection_url: &str,
    tls: &crate::config::CdcTls,
) -> Result<u32, FaucetError> {
    let mut conn = control_connection(connection_url, tls).await?;
    let one: String = sqlx::query_scalar("SELECT (1::money)::text")
        .fetch_one(&mut conn)
        .await
        .map_err(|e| pg_err("postgres-cdc money format", e))?;
    money_digits_of(&one)
}

/// The fraction digits of the server's rendering of `1::money`.
fn money_digits_of(one: &str) -> Result<u32, FaucetError> {
    crate::pgoutput::values::fraction_digits_of_money(one).ok_or_else(|| {
        FaucetError::Source(format!(
            "postgres-cdc: cannot read the money format from the server's rendering {one:?}"
        ))
    })
}

/// A plain (non-replication) connection for catalog lookups.
pub async fn control_connection(
    connection_url: &str,
    tls: &crate::config::CdcTls,
) -> Result<PgConnection, FaucetError> {
    let opts: PgConnectOptions = connection_url
        .parse()
        .map_err(|e| FaucetError::Config(format!("postgres-cdc: invalid connection URL: {e}")))?;
    let opts = apply_cdc_tls(opts, tls);
    use sqlx::ConnectOptions as _;
    opts.connect()
        .await
        .map_err(|e| pg_err("postgres-cdc partition lookup connect", e))
}

/// [`partition_roots`] over an open connection.
pub async fn partition_roots_on(
    conn: &mut PgConnection,
    oids: &[u32],
) -> Result<std::collections::HashMap<u32, String>, FaucetError> {
    let ids: Vec<i64> = oids.iter().map(|o| i64::from(*o)).collect();
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT c.oid::int8, rn.nspname::text, r.relname::text \
           FROM pg_class c \
           JOIN pg_class r ON r.oid = pg_partition_root(c.oid) \
           JOIN pg_namespace rn ON rn.oid = r.relnamespace \
          WHERE c.oid::int8 = ANY($1) AND c.relispartition",
    )
    .bind(&ids)
    .fetch_all(conn)
    .await
    .map_err(|e| pg_err("postgres-cdc partition lookup", e))?;
    Ok(rows
        .into_iter()
        .filter_map(|(oid, schema, table)| {
            u32::try_from(oid)
                .ok()
                .map(|oid| (oid, format!("{schema}.{table}")))
        })
        .collect())
}

/// The pgwire-replication client settings for one fetch cycle.
fn replication_config(coords: PgCoords, params: &ReplicationParams<'_>) -> ReplicationConfig {
    let start_lsn = Lsn::from_u64(params.start_lsn.unwrap_or(0));

    // ack_lsn stays unset: the reported flush position starts at start_lsn,
    // which is always a durable bookmark or the slot's own confirmed_flush_lsn.
    ReplicationConfig::new(
        coords.host,
        coords.user,
        coords.password,
        coords.dbname,
        params.slot_name,
        params.publication_name,
    )
    .with_port(coords.port)
    .with_tls(tls_config(params.tls))
    .with_start_lsn(start_lsn)
    // Use the dedicated status-update interval (not tcp_keepalive) so that
    // Standby Status Updates fire on their own cadence.
    .with_status_interval(params.status_update_interval)
    // Wake up the worker at least as often as we send status updates.
    .with_wakeup_interval(params.status_update_interval)
    .with_buffer_size(8192)
}

/// Open a logical replication stream and return a [`Duplex`] handle.
///
/// Internally this calls `pgwire_replication::ReplicationClient::connect`
/// which handles TCP, TLS negotiation, auth, and `START_REPLICATION` in one
/// shot.
pub async fn start_replication(
    _client: &Client,
    params: &ReplicationParams<'_>,
) -> Result<Duplex, FaucetError> {
    if params.proto_version != 1 {
        return Err(FaucetError::Config(format!(
            "postgres-cdc: pgwire-replication supports proto_version = 1 only; \
             got {}",
            params.proto_version
        )));
    }

    let cfg = replication_config(parse_url(params.connection_url)?, params);

    let inner = ReplicationClient::connect(cfg)
        .await
        .map_err(|e| pgwire_err("postgres-cdc start_replication", e))?;

    Ok(Duplex::spawn(
        inner,
        params.max_buffered_events,
        params.max_buffered_bytes,
    ))
}

/// Report progress to the server (Standby Status Update).
///
/// `confirmed_lsn` is the highest LSN whose changes have been durably
/// written to the sink.  The underlying library sends this feedback on its
/// own keepalive schedule; calling this function additionally marks the
/// progress so the next automatic feedback includes the latest position.
///
/// `reply_requested` mirrors the flag from the server's KeepAlive message
/// (no-op here since the library handles immediate replies internally).
pub async fn send_status_update(
    duplex: &mut Duplex,
    confirmed_lsn: u64,
    _reply_requested: bool,
) -> Result<(), FaucetError> {
    duplex
        .applied
        .fetch_max(confirmed_lsn, std::sync::atomic::Ordering::AcqRel);
    Ok(())
}

/// Receive the next meaningful replication event from the server.
///
/// Returns:
/// - `Ok(Some(event))` — the next [`ReplicationEvent`] that the caller should
///   handle.  This includes [`ReplicationEvent::XLogData`],
///   [`ReplicationEvent::Begin`], [`ReplicationEvent::Commit`], and
///   [`ReplicationEvent::Message`].  Callers (Task 9+) can match on the full
///   event type to observe transaction boundaries.
/// - `Ok(None)` — stream ended cleanly (slot stopped, stop LSN reached, or
///   `Duplex` was shut down).
/// - `Err(_)` — network / protocol error.
///
/// [`ReplicationEvent::KeepAlive`] events are absorbed here.  We deliberately
/// do **not** advance the applied-LSN to the server's `wal_end` on a keepalive
/// (the previous behaviour): that position is not yet durable downstream, and
/// advertising it as `confirmed_flush_lsn` would authorise Postgres to recycle
/// WAL for changes the consumer never persisted — a crash in that window loses
/// data (#78/#1).  The applied-LSN is advanced only from the durable bookmark,
/// via [`send_status_update`] at the start of each run; the library keeps
/// sending its periodic Standby Status Updates (carrying that durable
/// position) to hold the connection open.  [`ReplicationEvent::StoppedAt`] is
/// converted to `Ok(None)`.
pub async fn recv(duplex: &mut Duplex) -> Result<Option<ReplicationEvent>, FaucetError> {
    loop {
        match recv_item(duplex).await? {
            None => return Ok(None),
            Some(StreamItem::KeepAlive { .. }) => {}
            Some(StreamItem::Event(ev)) => return Ok(Some(ev)),
        }
    }
}

/// One item of the replication stream as [`recv_item`] surfaces it.
#[derive(Debug)]
pub enum StreamItem {
    /// A transaction boundary, change data or logical message.
    Event(ReplicationEvent),
    /// A server heartbeat: every transaction committed below `wal_end` has
    /// already been sent. Lets a consumer whose published tables are idle
    /// record progress through WAL the publication does not cover.
    KeepAlive {
        /// The server's sent position.
        wal_end: u64,
    },
}

/// [`recv`], but surfacing keepalives (as [`StreamItem::KeepAlive`]) instead
/// of absorbing them. A keepalive's position is still never advertised to
/// the server here: the caller may only put it in a bookmark, which reaches
/// the slot after the pipeline has persisted it.
pub async fn recv_item(duplex: &mut Duplex) -> Result<Option<StreamItem>, FaucetError> {
    Ok(match duplex.next_event().await? {
        None | Some(ReplicationEvent::StoppedAt { .. }) => None,
        Some(ReplicationEvent::KeepAlive { wal_end, .. }) => Some(StreamItem::KeepAlive {
            wal_end: wal_end.as_u64(),
        }),
        // The commit_lsn carried by Commit, not wal_end, is what reaches the
        // slot once the pipeline persists it.
        Some(ev) => Some(StreamItem::Event(ev)),
    })
}

// ── Clock helpers ──────────────────────────────────────────────────────────

/// Current time as a Postgres-epoch timestamp (µs since 2000-01-01 UTC).
///
/// Used in Standby Status Update messages.
pub fn postgres_clock_now() -> i64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let unix_micros = (now.as_secs() as i64) * 1_000_000 + (now.subsec_micros() as i64);
    unix_micros - POSTGRES_EPOCH_MICROS
}

/// Convert a Postgres-epoch timestamp (µs since 2000-01-01) to Unix
/// milliseconds (ms since 1970-01-01).
pub fn postgres_clock_to_unix_ms(ts: i64) -> i64 {
    (POSTGRES_EPOCH_MICROS.saturating_add(ts)) / 1_000
}

// ── Private SQL helpers ────────────────────────────────────────────────────

/// Wrap `s` in double-quotes for use as a Postgres identifier.
/// Any embedded double-quote is doubled (`"` → `""`).
/// Reserved for DDL statements (e.g. `DROP REPLICATION SLOT`); used in tests.
#[allow(dead_code)]
fn quote_slot(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Escape a string for use in a Postgres literal (single-quote context).
/// Any embedded single-quote is doubled (`'` → `''`).
fn escape_simple(s: &str) -> String {
    s.replace('\'', "''")
}

/// Produce a single-quoted Postgres string literal.
fn quote_literal(s: &str) -> String {
    format!("'{}'", escape_simple(s))
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Returns `true` if `err` is Postgres reporting that the replication slot is
/// still **active** — held by a backend that has not yet released it. Postgres
/// raises *"replication slot \"…\" is active for PID …"* (SQLSTATE `55006`).
///
/// This is transient on a rapid restart: a scheduler or `serve` re-running the
/// pipeline before the previous connection's backend has fully exited finds the
/// slot momentarily still in use. It clears within a short window, so it is
/// safe to retry after a backoff (#146 M12).
pub fn is_slot_active_error(err: &FaucetError) -> bool {
    sqlstate_of(err) == Some(SQLSTATE_OBJECT_IN_USE)
}

/// SQLSTATE `55006` — `object_in_use`. Postgres raises it for
/// *"replication slot \"…\" is active for PID …"*.
pub const SQLSTATE_OBJECT_IN_USE: &str = "55006";

/// A Postgres failure that **retains its SQLSTATE**, so retry decisions are
/// made on the code rather than on rendered text.
///
/// SQLSTATE is the documented, stable API; the message is not — it is
/// localised, it embeds user identifiers (a slot literally named
/// `orders_is_active` matched the old substring rule), and a `sqlx`/server
/// reword silently removed the restart-race backoff altogether (#654 H8).
/// Carried inside [`FaucetError::Custom`], which exists for exactly this kind
/// of connector-specific wrapping.
#[derive(Debug, thiserror::Error)]
#[error("{context}: {message}")]
pub struct PostgresError {
    /// The five-character SQLSTATE, when the server supplied one.
    pub sqlstate: Option<String>,
    context: String,
    message: String,
}

impl PostgresError {
    /// Build one directly, for callers (and tests) that already know the code.
    pub fn new(
        sqlstate: Option<String>,
        context: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            sqlstate,
            context: context.into(),
            message: message.into(),
        }
    }
}

/// Wrap a `sqlx` error, preserving its SQLSTATE. Use this instead of
/// stringifying at the boundary — once the code is gone it cannot be recovered.
pub(crate) fn pg_err(context: &str, e: sqlx::Error) -> FaucetError {
    let sqlstate = e
        .as_database_error()
        .and_then(|d| d.code())
        .map(|c| c.to_string());
    FaucetError::Custom(Box::new(PostgresError {
        sqlstate,
        context: context.to_string(),
        message: e.to_string(),
    }))
}

/// Map a replication-client failure, keeping the server's SQLSTATE (which
/// the client renders as a trailing `(SQLSTATE xxxxx)`) so the slot-active
/// retry can recognise `START_REPLICATION` failures (#789 SQL-137).
pub(crate) fn pgwire_err(context: &str, e: pgwire_replication::PgWireError) -> FaucetError {
    let message = e.to_string();
    let sqlstate = match &e {
        pgwire_replication::PgWireError::Server(text) => sqlstate_suffix(text),
        _ => None,
    };
    FaucetError::Custom(Box::new(PostgresError {
        sqlstate,
        context: context.to_string(),
        message,
    }))
}

/// The five-character code of a trailing `(SQLSTATE xxxxx)`.
fn sqlstate_suffix(text: &str) -> Option<String> {
    let rest = text.trim_end().strip_suffix(')')?;
    let code = &rest[rest.rfind("(SQLSTATE ")? + "(SQLSTATE ".len()..];
    (code.len() == 5 && code.chars().all(|c| c.is_ascii_alphanumeric())).then(|| code.to_string())
}

/// The SQLSTATE carried by a [`PostgresError`], if this error is one.
fn sqlstate_of(err: &FaucetError) -> Option<&str> {
    match err {
        FaucetError::Custom(inner) => inner
            .downcast_ref::<PostgresError>()
            .and_then(|pg| pg.sqlstate.as_deref()),
        _ => None,
    }
}

/// Exponential backoff for slot-acquisition retries: `250ms · 2^attempt`,
/// capped at 4 s.
fn slot_acquire_backoff(attempt: u32) -> Duration {
    let factor = 1u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let ms = 250u64.saturating_mul(factor).min(4000);
    Duration::from_millis(ms)
}

/// Run `op`, retrying up to `max_retries` times with exponential backoff while
/// it fails because the replication slot is still active (#146 M12). Any other
/// error — and the final attempt's error after exhausting retries — is returned
/// immediately. `max_retries = 0` preserves the previous fail-fast behaviour.
pub async fn retry_on_slot_active<F, Fut, T>(max_retries: u32, op: F) -> Result<T, FaucetError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, FaucetError>>,
{
    let ctx = faucet_core::CallContext::new().without_timeout();
    retry_on_slot_active_in(&ctx, "slot", max_retries, op).await
}

/// [`retry_on_slot_active`] through a call context (#880): each attempt is a
/// backend call named `op_name`, and the backoff between attempts ends early
/// on cancel and at the deadline.
pub async fn retry_on_slot_active_in<F, Fut, T>(
    ctx: &faucet_core::CallContext,
    op_name: &'static str,
    max_retries: u32,
    op: F,
) -> Result<T, FaucetError>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, FaucetError>>,
{
    let mut attempt = 0u32;
    loop {
        match ctx.call_once(op_name, op()).await {
            Ok(value) => return Ok(value),
            Err(e) if attempt < max_retries && is_slot_active_error(&e) => {
                let backoff = slot_acquire_backoff(attempt);
                tracing::warn!(
                    attempt = attempt + 1,
                    max_retries,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %e,
                    "postgres-cdc: replication slot still active; retrying after backoff"
                );
                ctx.wait(backoff).await?;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests;

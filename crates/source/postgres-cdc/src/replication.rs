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

/// Live replication stream.  Wraps [`pgwire_replication::ReplicationClient`].
/// Obtained from [`start_replication`].
pub struct Duplex {
    inner: ReplicationClient,
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
}

// ── Helper: parse a postgres URL into (host, port, user, password, dbname) ─

#[cfg_attr(test, derive(Debug))]
struct PgCoords {
    host: String,
    port: u16,
    user: String,
    password: String,
    dbname: String,
}

fn parse_url(url: &str) -> Result<PgCoords, FaucetError> {
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
    let user = parsed.username().to_owned();
    if user.is_empty() {
        return Err(FaucetError::Config(
            "postgres-cdc: connection URL is missing a user (expected \
             postgres://user@host[:port]/dbname)"
                .to_owned(),
        ));
    }
    let password = parsed.password().unwrap_or("").to_owned();
    let dbname = parsed.path().trim_start_matches('/').to_owned();
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
fn apply_cdc_tls(opts: PgConnectOptions, tls: &crate::config::CdcTls) -> PgConnectOptions {
    use crate::config::CdcTls;
    use sqlx::postgres::PgSslMode;
    match tls {
        CdcTls::Disable => opts.ssl_mode(PgSslMode::Disable),
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
    use std::path::PathBuf;
    match tls {
        CdcTls::Disable => TlsConfig::disabled(),
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
/// inactive. `pg_replication_slot_advance` never moves a slot backwards or
/// past the server's insert pointer, so a stale or zero `lsn` is a safe no-op.
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
            "postgres-cdc: pgwire-replication 0.3.2 supports proto_version = 1 only; \
             got {}",
            params.proto_version
        )));
    }

    let coords = parse_url(params.connection_url)?;

    let start_lsn = Lsn::from_u64(params.start_lsn.unwrap_or(0));

    let cfg = ReplicationConfig {
        host: coords.host,
        port: coords.port,
        user: coords.user,
        password: coords.password,
        database: coords.dbname,
        tls: tls_config(params.tls),
        slot: params.slot_name.to_owned(),
        publication: params.publication_name.to_owned(),
        start_lsn,
        stop_at_lsn: None,
        // Use the dedicated status-update interval (not tcp_keepalive) so that
        // Standby Status Updates fire on their own cadence.
        status_interval: params.status_update_interval,
        // Wake up the worker at least as often as we send status updates.
        idle_wakeup_interval: params.status_update_interval,
        buffer_events: 8192,
    };

    let inner = ReplicationClient::connect(cfg)
        .await
        .map_err(|e| FaucetError::Source(format!("postgres-cdc start_replication: {e}")))?;

    Ok(Duplex { inner })
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
        .inner
        .update_applied_lsn(Lsn::from_u64(confirmed_lsn));
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
        match duplex
            .inner
            .recv()
            .await
            .map_err(|e| FaucetError::Source(format!("postgres-cdc recv: {e}")))?
        {
            None => return Ok(None),

            Some(ReplicationEvent::StoppedAt { .. }) => {
                return Ok(None);
            }

            Some(ReplicationEvent::KeepAlive { .. }) => {
                // Absorb keepalives without touching the applied-LSN — see the
                // function doc. Continue the loop; do not surface to the caller.
            }

            Some(ev) => {
                // Surface Begin, Commit, XLogData, Message (and any future
                // variants) to the caller. The commit_lsn carried by Commit is
                // what becomes the durable bookmark once the pipeline persists
                // it — that, not wal_end, is the only position fed back to PG.
                return Ok(Some(ev));
            }
        }
    }
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
    let mut attempt = 0u32;
    loop {
        match op().await {
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
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CdcTls;
    use chrono::{TimeZone, Utc};
    use pgwire_replication::SslMode;

    // ─── SQLSTATE preservation (#654 H8) ────────────────────────────────────

    /// `pg_err` is the whole point of the H8 fix: the SQLSTATE must survive the
    /// hop into `FaucetError`, because once the error is stringified the code
    /// cannot be recovered. A non-database `sqlx::Error` has no code, and must
    /// therefore carry `None` rather than a guess.
    #[test]
    fn pg_err_preserves_context_and_message_without_a_sqlstate() {
        let err = pg_err("postgres-cdc slot lookup", sqlx::Error::PoolTimedOut);
        let FaucetError::Custom(inner) = &err else {
            panic!("pg_err must produce FaucetError::Custom, got {err:?}");
        };
        let pg = inner
            .downcast_ref::<PostgresError>()
            .expect("the boxed error must still be a PostgresError");
        assert_eq!(pg.context, "postgres-cdc slot lookup");
        assert_eq!(pg.message, sqlx::Error::PoolTimedOut.to_string());
        assert_eq!(
            pg.sqlstate, None,
            "a pool timeout is not a database-reported failure, so it has no SQLSTATE"
        );
        // The rendered form still names both halves, so a log line is useful.
        let rendered = err.to_string();
        assert!(
            rendered.contains("postgres-cdc slot lookup"),
            "context must survive into the display form: {rendered}"
        );
    }

    /// `sqlstate_of` reads the code back only from a `PostgresError`; every
    /// other error — including one whose *text* contains a SQLSTATE — yields
    /// `None`, which is what stops `is_slot_active_error` from ever being
    /// fooled by prose.
    #[test]
    fn sqlstate_of_reads_only_the_typed_carrier() {
        let carried = PostgresError::new(
            Some(SQLSTATE_OBJECT_IN_USE.to_string()),
            "acquire slot",
            "replication slot \"s\" is active for PID 42",
        );
        let err = FaucetError::Custom(Box::new(carried));
        assert_eq!(sqlstate_of(&err), Some(SQLSTATE_OBJECT_IN_USE));
        assert!(
            is_slot_active_error(&err),
            "the typed 55006 carrier is what slot-active detection keys off"
        );

        // A PostgresError with no code.
        let codeless = FaucetError::Custom(Box::new(PostgresError::new(
            None,
            "acquire slot",
            "replication slot is active for PID 42",
        )));
        assert_eq!(sqlstate_of(&codeless), None);
        assert!(!is_slot_active_error(&codeless));

        // Not a PostgresError at all, but the message reads exactly like one.
        // Before the fix this class of message drove the retry decision.
        let lookalike = FaucetError::Source(
            "replication slot \"s\" is active for PID 42 (SQLSTATE 55006)".into(),
        );
        assert_eq!(sqlstate_of(&lookalike), None);
        assert!(
            !is_slot_active_error(&lookalike),
            "prose must never be mistaken for a SQLSTATE"
        );

        // A different real SQLSTATE must not match slot-active.
        let other_code = FaucetError::Custom(Box::new(PostgresError::new(
            Some("42P01".to_string()),
            "acquire slot",
            "relation does not exist",
        )));
        assert_eq!(sqlstate_of(&other_code), Some("42P01"));
        assert!(!is_slot_active_error(&other_code));
    }

    #[test]
    fn apply_cdc_tls_sets_ssl_mode_on_control_plane_options() {
        // #321 M5: the control-plane PgConnectOptions must carry the configured
        // SSL mode, not sqlx's default `Prefer`. Assert via the Debug view.
        let base: PgConnectOptions = "postgres://u:p@h:5432/db".parse().unwrap();
        let dbg = |tls: &CdcTls| format!("{:?}", apply_cdc_tls(base.clone(), tls));
        assert!(
            dbg(&CdcTls::Disable).contains("Disable"),
            "{}",
            dbg(&CdcTls::Disable)
        );
        assert!(dbg(&CdcTls::Require).contains("Require"));
        assert!(dbg(&CdcTls::VerifyCa { ca_path: None }).contains("VerifyCa"));
        assert!(
            dbg(&CdcTls::VerifyFull {
                ca_path: Some("/ca.pem".into())
            })
            .contains("VerifyFull")
        );
    }

    #[test]
    fn tls_config_maps_each_mode() {
        assert_eq!(tls_config(&CdcTls::Disable).mode, SslMode::Disable);
        assert_eq!(tls_config(&CdcTls::Require).mode, SslMode::Require);
        assert_eq!(
            tls_config(&CdcTls::VerifyCa { ca_path: None }).mode,
            SslMode::VerifyCa
        );
        assert_eq!(
            tls_config(&CdcTls::VerifyFull {
                ca_path: Some("/ca.pem".into())
            })
            .mode,
            SslMode::VerifyFull
        );
    }

    /// Convenience: turn `postgres_clock_to_unix_ms`-compatible math into a
    /// `DateTime<Utc>`.  Used by tests only.
    fn postgres_clock_to_datetime(ts: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_micros(POSTGRES_EPOCH_MICROS.saturating_add(ts))
            .single()
            .unwrap_or_else(Utc::now)
    }

    #[test]
    fn postgres_clock_round_trip() {
        let dt = Utc.with_ymd_and_hms(2026, 5, 17, 12, 0, 0).unwrap();
        let pg_ts = dt.timestamp_micros() - POSTGRES_EPOCH_MICROS;
        let back = postgres_clock_to_datetime(pg_ts);
        assert_eq!(back, dt);
    }

    #[test]
    fn unix_ms_conversion() {
        let dt = Utc.with_ymd_and_hms(2026, 5, 17, 12, 0, 0).unwrap();
        let pg_ts = dt.timestamp_micros() - POSTGRES_EPOCH_MICROS;
        assert_eq!(postgres_clock_to_unix_ms(pg_ts), 1_779_019_200_000);
    }

    #[test]
    fn quote_slot_simple() {
        assert_eq!(quote_slot("faucet_slot"), "\"faucet_slot\"");
    }

    #[test]
    fn escape_simple_doubles_quotes() {
        assert_eq!(escape_simple("foo'bar"), "foo''bar");
    }

    #[test]
    fn parse_url_extracts_all_components() {
        let c = parse_url("postgres://alice:secret@db.example.com:5544/analytics").unwrap();
        assert_eq!(c.host, "db.example.com");
        assert_eq!(c.port, 5544);
        assert_eq!(c.user, "alice");
        assert_eq!(c.password, "secret");
        assert_eq!(c.dbname, "analytics");
    }

    #[test]
    fn parse_url_defaults_port_and_dbname() {
        let c = parse_url("postgres://alice@db.example.com").unwrap();
        assert_eq!(c.port, 5432);
        assert_eq!(c.dbname, "postgres");
        assert_eq!(c.password, "");
    }

    #[test]
    fn parse_url_rejects_missing_host() {
        // `postgres:///db` parses with an empty host — must fail fast (#78/#47).
        let err = parse_url("postgres:///analytics").unwrap_err();
        assert!(format!("{err}").contains("missing a host"), "{err}");
    }

    #[test]
    fn parse_url_rejects_missing_user() {
        let err = parse_url("postgres://db.example.com/analytics").unwrap_err();
        assert!(format!("{err}").contains("missing a user"), "{err}");
    }

    #[test]
    fn slot_active_is_classified_by_sqlstate_not_message_text() {
        // SQLSTATE 55006 is the API. The message is not: it is localised and
        // it embeds the slot name, so a slot literally named
        // `orders_is_active` used to match the old substring rule, and a
        // server/driver reword silently removed the restart-race backoff.
        assert!(is_slot_active_error(&slot_in_use()));

        // Same prose, no SQLSTATE → not classified (text alone proves nothing).
        assert!(!is_slot_active_error(&FaucetError::Source(
            "replication slot \"orders_is_active\" is active for PID 1".into()
        )));

        // A different SQLSTATE is not the slot-in-use condition.
        assert!(!is_slot_active_error(&FaucetError::Custom(Box::new(
            PostgresError::new(
                Some("42P01".to_string()),
                "postgres-cdc slot lookup",
                "relation does not exist",
            )
        ))));

        // Unrelated error kinds are untouched.
        assert!(!is_slot_active_error(&FaucetError::Config(
            "bad url".into()
        )));
    }

    #[test]
    fn slot_acquire_backoff_grows_and_is_capped() {
        assert_eq!(slot_acquire_backoff(0), Duration::from_millis(250));
        assert_eq!(slot_acquire_backoff(1), Duration::from_millis(500));
        assert_eq!(slot_acquire_backoff(2), Duration::from_millis(1000));
        // Capped at 4s no matter how large the attempt.
        assert_eq!(slot_acquire_backoff(20), Duration::from_millis(4000));
        assert_eq!(slot_acquire_backoff(64), Duration::from_millis(4000));
    }

    /// The typed slot-in-use error, as the sqlx boundary now produces it.
    fn slot_in_use() -> FaucetError {
        FaucetError::Custom(Box::new(PostgresError::new(
            Some(SQLSTATE_OBJECT_IN_USE.to_string()),
            "postgres-cdc create slot",
            "replication slot \"s\" is active for PID 1",
        )))
    }

    #[tokio::test]
    async fn retry_on_slot_active_retries_then_succeeds() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = AtomicU32::new(0);
        let result = retry_on_slot_active(5, || {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(slot_in_use())
                } else {
                    Ok::<u32, FaucetError>(42)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3, "2 failures + 1 success");
    }

    #[tokio::test]
    async fn retry_on_slot_active_gives_up_after_max_retries() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = AtomicU32::new(0);
        let result: Result<(), _> = retry_on_slot_active(2, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(slot_in_use()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "initial attempt + 2 retries"
        );
    }

    #[tokio::test]
    async fn retry_on_slot_active_does_not_retry_unrelated_errors() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let calls = AtomicU32::new(0);
        let result: Result<(), _> = retry_on_slot_active(5, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(FaucetError::Source("connection refused".into())) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "a non-slot-active error must not be retried"
        );
    }
}

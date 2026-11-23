//! The SQL implementation the SQLite and PostgreSQL backends share: one
//! schema, one statement set (written with `?` placeholders and a `{now}`
//! store-clock token) and one [`FeedStore`](crate::FeedStore) body, expanded
//! per backend by `impl_feed_store!`.

use crate::error::{FeedError, Result};
use crate::types::{Seq, Timestamp};
use faucet_core::io::CallContext;
use std::future::Future;
use std::time::Duration;

/// The newest store format this release reads and the one it writes.
pub const SCHEMA_VERSION: i64 = 1;

pub(crate) const INSERT_CHUNK: usize = 500;
pub(crate) const KEY_CHUNK: usize = 500;
pub(crate) const HEADS_CHUNK: usize = 10_000;
const MAX_MS: i64 = 100 * 365 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(all(feature = "sqlite", feature = "postgres")), allow(dead_code))]
pub(crate) enum Dialect {
    Sqlite,
    Postgres,
}

impl Dialect {
    pub(crate) fn index(self) -> usize {
        match self {
            Dialect::Sqlite => 0,
            Dialect::Postgres => 1,
        }
    }

    fn now(self) -> &'static str {
        match self {
            Dialect::Sqlite => "CAST(unixepoch('subsec') * 1000 AS INTEGER)",
            Dialect::Postgres => "(extract(epoch from clock_timestamp()) * 1000)::bigint",
        }
    }

    fn blob(self) -> &'static str {
        match self {
            Dialect::Sqlite => "BLOB",
            Dialect::Postgres => "BYTEA",
        }
    }

    pub(crate) fn lock_sql(self) -> Option<&'static str> {
        match self {
            Dialect::Sqlite => None,
            Dialect::Postgres => Some("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"),
        }
    }

    pub(crate) fn render(self, sql: &str) -> String {
        let sql = sql
            .replace("{now}", self.now())
            .replace("{blob}", self.blob());
        match self {
            Dialect::Sqlite => sql,
            Dialect::Postgres => {
                let mut out = String::with_capacity(sql.len() + 16);
                let mut n = 0usize;
                for ch in sql.chars() {
                    if ch == '?' {
                        n += 1;
                        out.push('$');
                        out.push_str(&n.to_string());
                    } else {
                        out.push(ch);
                    }
                }
                out
            }
        }
    }

    fn transient(self, code: &str) -> bool {
        match self {
            Dialect::Sqlite => code
                .parse::<i32>()
                .map(|n| matches!(n & 0xff, 5 | 6 | 13))
                .unwrap_or(false),
            Dialect::Postgres => {
                matches!(code, "55P03" | "25P03" | "25006")
                    || ["08", "40", "53", "57"].iter().any(|p| code.starts_with(p))
            }
        }
    }
}

pub(crate) const META_DDL: &str =
    "CREATE TABLE IF NOT EXISTS faucet_feed_meta (k TEXT PRIMARY KEY, v BIGINT NOT NULL)";

pub(crate) const DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS faucet_feed_heads (\
        feed TEXT PRIMARY KEY, tenant TEXT, head BIGINT NOT NULL, bytes BIGINT NOT NULL, \
        max_age_ms BIGINT NOT NULL, dedupe_window_ms BIGINT NOT NULL, tenant_max_bytes BIGINT, \
        undelivered_max_events BIGINT, undelivered_max_bytes BIGINT, \
        undelivered_max_age_ms BIGINT, maintenance_owner TEXT, maintenance_expires_at BIGINT)",
    "CREATE INDEX IF NOT EXISTS faucet_feed_heads_tenant ON faucet_feed_heads (tenant)",
    "CREATE TABLE IF NOT EXISTS faucet_feed_events (\
        feed TEXT NOT NULL, lane INTEGER NOT NULL DEFAULT 0, seq BIGINT NOT NULL, \
        id TEXT NOT NULL, received_at BIGINT NOT NULL, stored_at BIGINT NOT NULL, \
        content_type TEXT NOT NULL, headers TEXT NOT NULL, payload {blob} NOT NULL, \
        size BIGINT NOT NULL, sealed BOOLEAN NOT NULL, dedupe_key TEXT, \
        PRIMARY KEY (feed, lane, seq))",
    "CREATE TABLE IF NOT EXISTS faucet_feed_deliveries (\
        feed TEXT NOT NULL, delivery TEXT NOT NULL, epoch BIGINT NOT NULL, \
        \"cursor\" BIGINT NOT NULL, claim_id TEXT, claim_to BIGINT, claim_owner TEXT, \
        holder TEXT, claim_expires_at BIGINT, PRIMARY KEY (feed, delivery))",
    "CREATE UNIQUE INDEX IF NOT EXISTS faucet_feed_deliveries_claim \
        ON faucet_feed_deliveries (claim_id)",
    "CREATE TABLE IF NOT EXISTS faucet_feed_dedupe (\
        feed TEXT NOT NULL, \"key\" TEXT NOT NULL, seq BIGINT NOT NULL, id TEXT NOT NULL, \
        expires_at BIGINT NOT NULL, PRIMARY KEY (feed, \"key\"))",
    "CREATE INDEX IF NOT EXISTS faucet_feed_dedupe_expiry \
        ON faucet_feed_dedupe (feed, expires_at)",
    "CREATE TABLE IF NOT EXISTS faucet_feed_gaps (\
        feed TEXT NOT NULL, delivery TEXT, from_seq BIGINT NOT NULL, to_seq BIGINT NOT NULL, \
        reason TEXT NOT NULL, \"at\" BIGINT NOT NULL, \"by\" TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS faucet_feed_gaps_feed ON faucet_feed_gaps (feed, \"at\")",
];

pub(crate) fn backend(dialect: Dialect, err: sqlx::Error) -> FeedError {
    let transient = match &err {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::WorkerCrashed => true,
        sqlx::Error::Database(db) => db
            .code()
            .map(|code| dialect.transient(&code))
            .unwrap_or(false),
        _ => false,
    };
    if transient {
        FeedError::Unavailable(err.to_string())
    } else {
        FeedError::Backend(err.to_string())
    }
}

/// A new store's identity: random, positive, never zero.
pub(crate) fn fresh_store_id() -> i64 {
    let bytes = uuid::Uuid::new_v4().into_bytes();
    let n = u64::from_le_bytes(bytes[..8].try_into().unwrap_or_default()) >> 1;
    i64::try_from(n).unwrap_or(i64::MAX).max(1)
}

pub(crate) fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(MAX_MS).min(MAX_MS)
}

pub(crate) fn clamp(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

pub(crate) fn seq_param(seq: Seq) -> Result<i64> {
    i64::try_from(seq.get())
        .map_err(|_| FeedError::Invalid(format!("sequence {seq} is out of range")))
}

pub(crate) fn seq_of(value: i64) -> Result<Seq> {
    u64::try_from(value)
        .map(Seq)
        .map_err(|_| FeedError::Corrupt(format!("negative sequence {value}")))
}

pub(crate) fn count_of(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

pub(crate) fn time_of(ms: i64) -> Result<Timestamp> {
    chrono::DateTime::from_timestamp_millis(ms)
        .ok_or_else(|| FeedError::Corrupt(format!("timestamp {ms} is out of range")))
}

pub(crate) fn placeholders(n: usize) -> String {
    let mut out = String::with_capacity(n * 3);
    for i in 0..n {
        if i > 0 {
            out.push_str(", ");
        }
        out.push('?');
    }
    out
}

pub(crate) async fn once<T>(
    calls: &CallContext,
    op: &'static str,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    match calls.call_once(op, async { Ok(fut.await) }).await {
        Ok(result) => result,
        Err(e) => Err(FeedError::Call(e)),
    }
}

pub(crate) async fn each<T, F, Fut>(calls: &CallContext, op: &'static str, mut f: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let attempt = || {
        let fut = f();
        async move { Ok(fut.await) }
    };
    match calls.call(op, attempt).await {
        Ok(result) => result,
        Err(e) => Err(FeedError::Call(e)),
    }
}

pub(crate) struct HeadRow {
    pub tenant: Option<String>,
    pub head: i64,
    pub bytes: i64,
    pub max_age_ms: i64,
    pub dedupe_window_ms: i64,
    pub tenant_max_bytes: Option<i64>,
    pub undelivered_max_events: Option<i64>,
    pub undelivered_max_bytes: Option<i64>,
    pub undelivered_max_age_ms: Option<i64>,
}

macro_rules! stmt {
    ($dialect:expr, $sql:expr) => {{
        static CACHE: [std::sync::OnceLock<String>; 2] =
            [std::sync::OnceLock::new(), std::sync::OnceLock::new()];
        let dialect: $crate::sql::Dialect = $dialect;
        CACHE[dialect.index()]
            .get_or_init(|| dialect.render($sql))
            .as_str()
    }};
}
pub(crate) use stmt;

macro_rules! impl_feed_store {
    ($name:ident, $db:ty, $begin:expr, $dialect:expr, $caps:expr) => {
        impl $name {
            const D: $crate::sql::Dialect = $dialect;

            fn be(err: sqlx::Error) -> $crate::FeedError {
                $crate::sql::backend(Self::D, err)
            }

            async fn begin(&self) -> $crate::Result<sqlx::Transaction<'_, $db>> {
                self.pool.begin_with($begin).await.map_err(Self::be)
            }

            async fn lock(
                tx: &mut sqlx::Transaction<'_, $db>,
                key: &str,
            ) -> $crate::Result<()> {
                if let Some(sql) = Self::D.lock_sql() {
                    sqlx::query(sql)
                        .bind(key)
                        .execute(&mut **tx)
                        .await
                        .map_err(Self::be)?;
                }
                Ok(())
            }

            async fn lock_feed(
                tx: &mut sqlx::Transaction<'_, $db>,
                feed: &$crate::FeedUid,
            ) -> $crate::Result<()> {
                Self::lock(tx, &format!("feed:{feed}")).await
            }

            /// Create or check the schema; returns the store's identity.
            pub(crate) async fn migrate(&self) -> $crate::Result<i64> {
                use sqlx::Row;
                use $crate::sql::{stmt, SCHEMA_VERSION};
                let mut tx = self.begin().await?;
                Self::lock(&mut tx, "faucet_feed_schema").await?;
                sqlx::query($crate::sql::META_DDL)
                    .execute(&mut *tx)
                    .await
                    .map_err(Self::be)?;
                let found = sqlx::query("SELECT v FROM faucet_feed_meta WHERE k = 'schema_version'")
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(Self::be)?;
                if let Some(row) = found {
                    let found: i64 = row.try_get(0).map_err(Self::be)?;
                    if found > SCHEMA_VERSION {
                        return Err($crate::FeedError::NewerSchema {
                            found,
                            supported: SCHEMA_VERSION,
                        });
                    }
                }
                for ddl in $crate::sql::DDL {
                    sqlx::query(&Self::D.render(ddl))
                        .execute(&mut *tx)
                        .await
                        .map_err(Self::be)?;
                }
                sqlx::query(stmt!(
                    Self::D,
                    "INSERT INTO faucet_feed_meta (k, v) VALUES ('schema_version', ?) \
                     ON CONFLICT (k) DO NOTHING"
                ))
                .bind(SCHEMA_VERSION)
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                sqlx::query(stmt!(
                    Self::D,
                    "INSERT INTO faucet_feed_meta (k, v) VALUES ('store_id', ?) \
                     ON CONFLICT (k) DO NOTHING"
                ))
                .bind($crate::sql::fresh_store_id())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                let row = sqlx::query("SELECT v FROM faucet_feed_meta WHERE k = 'store_id'")
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(Self::be)?;
                let id: i64 = row.try_get(0).map_err(Self::be)?;
                tx.commit().await.map_err(Self::be)?;
                Ok(id)
            }

            async fn load_head(
                tx: &mut sqlx::Transaction<'_, $db>,
                feed: &$crate::FeedUid,
            ) -> $crate::Result<$crate::sql::HeadRow> {
                use sqlx::Row;
                use $crate::sql::stmt;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT tenant, head, bytes, max_age_ms, dedupe_window_ms, tenant_max_bytes, \
                     undelivered_max_events, undelivered_max_bytes, undelivered_max_age_ms \
                     FROM faucet_feed_heads WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .fetch_optional(&mut **tx)
                .await
                .map_err(Self::be)?
                .ok_or_else(|| $crate::FeedError::FeedNotFound(feed.clone()))?;
                Ok($crate::sql::HeadRow {
                    tenant: row.try_get(0).map_err(Self::be)?,
                    head: row.try_get(1).map_err(Self::be)?,
                    bytes: row.try_get(2).map_err(Self::be)?,
                    max_age_ms: row.try_get(3).map_err(Self::be)?,
                    dedupe_window_ms: row.try_get(4).map_err(Self::be)?,
                    tenant_max_bytes: row.try_get(5).map_err(Self::be)?,
                    undelivered_max_events: row.try_get(6).map_err(Self::be)?,
                    undelivered_max_bytes: row.try_get(7).map_err(Self::be)?,
                    undelivered_max_age_ms: row.try_get(8).map_err(Self::be)?,
                })
            }

            async fn oldest(
                tx: &mut sqlx::Transaction<'_, $db>,
                feed: &$crate::FeedUid,
                head: i64,
            ) -> $crate::Result<i64> {
                use sqlx::Row;
                use $crate::sql::stmt;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT MIN(seq) FROM faucet_feed_events WHERE feed = ? AND lane = 0"
                ))
                .bind(feed.as_str())
                .fetch_one(&mut **tx)
                .await
                .map_err(Self::be)?;
                let oldest: Option<i64> = row.try_get(0).map_err(Self::be)?;
                Ok(oldest.unwrap_or(head + 1))
            }

            async fn first_at(
                tx: &mut sqlx::Transaction<'_, $db>,
                feed: &$crate::FeedUid,
                time: $crate::Timestamp,
                end: i64,
            ) -> $crate::Result<i64> {
                use sqlx::Row;
                use $crate::sql::stmt;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT MIN(seq) FROM faucet_feed_events \
                     WHERE feed = ? AND lane = 0 AND stored_at >= ?"
                ))
                .bind(feed.as_str())
                .bind(time.timestamp_millis())
                .fetch_one(&mut **tx)
                .await
                .map_err(Self::be)?;
                let first: Option<i64> = row.try_get(0).map_err(Self::be)?;
                Ok(first.unwrap_or(end))
            }

            async fn write_gap(
                tx: &mut sqlx::Transaction<'_, $db>,
                delivery: &$crate::DeliveryUid,
                from: i64,
                to: i64,
                reason: &str,
                by: &str,
            ) -> $crate::Result<()> {
                use $crate::sql::stmt;
                sqlx::query(stmt!(
                    Self::D,
                    "INSERT INTO faucet_feed_gaps (feed, delivery, from_seq, to_seq, reason, \
                     \"at\", \"by\") VALUES (?, ?, ?, ?, ?, {now}, ?)"
                ))
                .bind(delivery.feed().as_str())
                .bind(delivery.id())
                .bind(from)
                .bind(to)
                .bind(reason)
                .bind(by)
                .execute(&mut **tx)
                .await
                .map_err(Self::be)?;
                Ok(())
            }

            async fn do_ensure_feed(
                &self,
                feed: &$crate::FeedUid,
                tenant: Option<&$crate::TenantId>,
                limits: &$crate::StoreLimits,
            ) -> $crate::Result<()> {
                use sqlx::Row;
                use $crate::sql::{clamp, millis, stmt};
                let mut tx = self.begin().await?;
                sqlx::query(stmt!(
                    Self::D,
                    "INSERT INTO faucet_feed_heads (feed, tenant, head, bytes, max_age_ms, \
                     dedupe_window_ms, tenant_max_bytes, undelivered_max_events, \
                     undelivered_max_bytes, undelivered_max_age_ms) \
                     VALUES (?, ?, 0, 0, ?, ?, ?, ?, ?, ?) \
                     ON CONFLICT (feed) DO UPDATE SET max_age_ms = excluded.max_age_ms, \
                     dedupe_window_ms = excluded.dedupe_window_ms, \
                     tenant_max_bytes = excluded.tenant_max_bytes, \
                     undelivered_max_events = excluded.undelivered_max_events, \
                     undelivered_max_bytes = excluded.undelivered_max_bytes, \
                     undelivered_max_age_ms = excluded.undelivered_max_age_ms"
                ))
                .bind(feed.as_str())
                .bind(tenant.map(|t| t.as_str()))
                .bind(millis(limits.max_age))
                .bind(millis(limits.dedupe_window))
                .bind(limits.tenant_max_bytes.map(clamp))
                .bind(limits.undelivered_max_events.map(clamp))
                .bind(limits.undelivered_max_bytes.map(clamp))
                .bind(limits.undelivered_max_age.map(millis))
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT tenant FROM faucet_feed_heads WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .fetch_one(&mut *tx)
                .await
                .map_err(Self::be)?;
                let stored: Option<String> = row.try_get(0).map_err(Self::be)?;
                if stored.as_deref() != tenant.map(|t| t.as_str()) {
                    return Err($crate::FeedError::TenantMismatch {
                        feed: feed.clone(),
                        stored: stored.map($crate::TenantId::new).transpose()?,
                    });
                }
                tx.commit().await.map_err(Self::be)
            }

            async fn do_ensure_delivery(
                &self,
                delivery: &$crate::DeliveryUid,
                start: $crate::Start,
            ) -> $crate::Result<$crate::DeliveryState> {
                use sqlx::Row;
                use $crate::sql::{seq_of, seq_param, stmt};
                use $crate::{DeliveryState, Epoch, FeedError, Start};
                let feed = delivery.feed();
                let mut tx = self.begin().await?;
                Self::lock_feed(&mut tx, feed).await?;
                let head = Self::load_head(&mut tx, feed).await?;
                let existing = sqlx::query(stmt!(
                    Self::D,
                    "SELECT \"cursor\", epoch FROM faucet_feed_deliveries \
                     WHERE feed = ? AND delivery = ?"
                ))
                .bind(feed.as_str())
                .bind(delivery.id())
                .fetch_optional(&mut *tx)
                .await
                .map_err(Self::be)?;
                if let Some(row) = existing {
                    let cursor: i64 = row.try_get(0).map_err(Self::be)?;
                    let epoch: i64 = row.try_get(1).map_err(Self::be)?;
                    tx.commit().await.map_err(Self::be)?;
                    return Ok(DeliveryState {
                        cursor: seq_of(cursor)?,
                        epoch: Epoch(seq_of(epoch)?.get()),
                        created: false,
                    });
                }
                let end = head.head + 1;
                let oldest = Self::oldest(&mut tx, feed, head.head).await?;
                let cursor = match start {
                    Start::Earliest => oldest,
                    Start::Latest => end,
                    Start::Seq(seq) => {
                        let at = seq_param(seq)?;
                        if at < oldest || at > end {
                            return Err(FeedError::OutOfRange {
                                feed: feed.clone(),
                                requested: seq,
                                oldest: seq_of(oldest)?,
                                end: seq_of(end)?,
                            });
                        }
                        at
                    }
                    Start::Time(time) => Self::first_at(&mut tx, feed, time, end).await?,
                };
                sqlx::query(stmt!(
                    Self::D,
                    "INSERT INTO faucet_feed_deliveries (feed, delivery, epoch, \"cursor\") \
                     VALUES (?, ?, 0, ?)"
                ))
                .bind(feed.as_str())
                .bind(delivery.id())
                .bind(cursor)
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                if matches!(start, Start::Earliest) && oldest > 1 {
                    Self::write_gap(
                        &mut tx,
                        delivery,
                        1,
                        oldest,
                        "trimmed before the delivery started",
                        "store",
                    )
                    .await?;
                }
                tx.commit().await.map_err(Self::be)?;
                Ok(DeliveryState {
                    cursor: seq_of(cursor)?,
                    epoch: Epoch(0),
                    created: true,
                })
            }

            async fn do_delete_delivery(
                &self,
                delivery: &$crate::DeliveryUid,
            ) -> $crate::Result<()> {
                use $crate::sql::stmt;
                let mut tx = self.begin().await?;
                sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_gaps WHERE feed = ? AND delivery = ?"
                ))
                .bind(delivery.feed().as_str())
                .bind(delivery.id())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_deliveries WHERE feed = ? AND delivery = ?"
                ))
                .bind(delivery.feed().as_str())
                .bind(delivery.id())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                tx.commit().await.map_err(Self::be)
            }

            async fn do_purge_feed(
                &self,
                feed: &$crate::FeedUid,
            ) -> $crate::Result<$crate::PurgeStats> {
                use $crate::sql::stmt;
                let mut tx = self.begin().await?;
                Self::lock_feed(&mut tx, feed).await?;
                let events = sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_events WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?
                .rows_affected();
                let deliveries = sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_deliveries WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?
                .rows_affected();
                let dedupe = sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_dedupe WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?
                .rows_affected();
                let gaps = sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_gaps WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?
                .rows_affected();
                sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_heads WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                tx.commit().await.map_err(Self::be)?;
                Ok($crate::PurgeStats {
                    events,
                    deliveries,
                    dedupe,
                    gaps,
                })
            }

            async fn check_limits(
                tx: &mut sqlx::Transaction<'_, $db>,
                feed: &$crate::FeedUid,
                head: &$crate::sql::HeadRow,
                new_events: i64,
                new_bytes: i64,
            ) -> $crate::Result<()> {
                use sqlx::Row;
                use $crate::sql::{count_of, stmt};
                use $crate::{FeedError, FullLimit};
                let full = |limit: FullLimit, max: i64, actual: i64| FeedError::Full {
                    feed: feed.clone(),
                    limit,
                    max: count_of(max),
                    actual: count_of(actual),
                };
                if let (Some(tenant), Some(max)) = (&head.tenant, head.tenant_max_bytes) {
                    let row = sqlx::query(stmt!(
                        Self::D,
                        "SELECT CAST(COALESCE(SUM(bytes), 0) AS BIGINT) \
                         FROM faucet_feed_heads WHERE tenant = ?"
                    ))
                    .bind(tenant.as_str())
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(Self::be)?;
                    let held: i64 = row.try_get(0).map_err(Self::be)?;
                    let actual = held.saturating_add(new_bytes);
                    if actual > max {
                        return Err(full(FullLimit::TenantBytes, max, actual));
                    }
                }
                if head.undelivered_max_events.is_none()
                    && head.undelivered_max_bytes.is_none()
                    && head.undelivered_max_age_ms.is_none()
                {
                    return Ok(());
                }
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT MIN(\"cursor\") FROM faucet_feed_deliveries WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .fetch_one(&mut **tx)
                .await
                .map_err(Self::be)?;
                let lowest: Option<i64> = row.try_get(0).map_err(Self::be)?;
                let Some(lowest) = lowest else {
                    return Ok(());
                };
                if let Some(max) = head.undelivered_max_events {
                    let actual = (head.head + 1 - lowest).max(0).saturating_add(new_events);
                    if actual > max {
                        return Err(full(FullLimit::UndeliveredEvents, max, actual));
                    }
                }
                if let Some(max) = head.undelivered_max_bytes
                    && head.bytes.saturating_add(new_bytes) > max
                {
                    let row = sqlx::query(stmt!(
                        Self::D,
                        "SELECT CAST(COALESCE(SUM(size), 0) AS BIGINT) FROM faucet_feed_events \
                         WHERE feed = ? AND lane = 0 AND seq >= ?"
                    ))
                    .bind(feed.as_str())
                    .bind(lowest)
                    .fetch_one(&mut **tx)
                    .await
                    .map_err(Self::be)?;
                    let backlog: i64 = row.try_get(0).map_err(Self::be)?;
                    let actual = backlog.saturating_add(new_bytes);
                    if actual > max {
                        return Err(full(FullLimit::UndeliveredBytes, max, actual));
                    }
                }
                if let Some(max) = head.undelivered_max_age_ms {
                    let row = sqlx::query(stmt!(
                        Self::D,
                        "SELECT {now} - stored_at FROM faucet_feed_events \
                         WHERE feed = ? AND lane = 0 AND seq = ?"
                    ))
                    .bind(feed.as_str())
                    .bind(lowest)
                    .fetch_optional(&mut **tx)
                    .await
                    .map_err(Self::be)?;
                    if let Some(row) = row {
                        let age: i64 = row.try_get(0).map_err(Self::be)?;
                        if age > max {
                            return Err(full(FullLimit::UndeliveredAge, max, age));
                        }
                    }
                }
                Ok(())
            }

            async fn do_append(
                &self,
                feed: &$crate::FeedUid,
                events: Vec<$crate::NewEvent>,
            ) -> $crate::Result<$crate::AppendAck> {
                use sqlx::Row;
                use std::collections::HashMap;
                use $crate::sql::{clamp, placeholders, seq_of, stmt, INSERT_CHUNK, KEY_CHUNK};
                use $crate::{AppendAck, Appended, FeedError};

                enum Slot {
                    Fresh(usize),
                    Stored(i64, String),
                }

                let before = sqlx::query(stmt!(
                    Self::D,
                    "SELECT tenant, tenant_max_bytes FROM faucet_feed_heads WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(Self::be)?
                .ok_or_else(|| FeedError::FeedNotFound(feed.clone()))?;
                let tenant: Option<String> = before.try_get(0).map_err(Self::be)?;
                let quota: Option<i64> = before.try_get(1).map_err(Self::be)?;

                let mut tx = self.begin().await?;
                if let (Some(tenant), Some(_)) = (&tenant, quota) {
                    Self::lock(&mut tx, &format!("tenant:{tenant}")).await?;
                }
                Self::lock_feed(&mut tx, feed).await?;
                let head = Self::load_head(&mut tx, feed).await?;
                let dedupe = head.dedupe_window_ms > 0;

                let mut known: HashMap<String, (i64, String)> = HashMap::new();
                if dedupe {
                    let mut keys: Vec<&str> = events
                        .iter()
                        .filter_map(|e| e.dedupe_key.as_deref())
                        .collect();
                    keys.sort_unstable();
                    keys.dedup();
                    for chunk in keys.chunks(KEY_CHUNK) {
                        let sql = Self::D.render(&format!(
                            "SELECT \"key\", seq, id FROM faucet_feed_dedupe \
                             WHERE feed = ? AND expires_at > {{now}} AND \"key\" IN ({})",
                            placeholders(chunk.len())
                        ));
                        let mut query = sqlx::query(&sql).bind(feed.as_str());
                        for key in chunk {
                            query = query.bind(*key);
                        }
                        for row in query.fetch_all(&mut *tx).await.map_err(Self::be)? {
                            let key: String = row.try_get(0).map_err(Self::be)?;
                            let seq: i64 = row.try_get(1).map_err(Self::be)?;
                            let id: String = row.try_get(2).map_err(Self::be)?;
                            known.insert(key, (seq, id));
                        }
                    }
                }

                let mut slots: Vec<Slot> = Vec::with_capacity(events.len());
                let mut fresh: Vec<($crate::NewEvent, i64, String)> = Vec::new();
                let mut in_batch: HashMap<String, usize> = HashMap::new();
                let mut new_bytes: i64 = 0;
                for event in events {
                    let key = event.dedupe_key.clone().filter(|_| dedupe);
                    if let Some(key) = &key {
                        if let Some((seq, id)) = known.get(key) {
                            slots.push(Slot::Stored(*seq, id.clone()));
                            continue;
                        }
                        if let Some(at) = in_batch.get(key) {
                            let (_, seq, id) = &fresh[*at];
                            slots.push(Slot::Stored(*seq, id.clone()));
                            continue;
                        }
                        in_batch.insert(key.clone(), fresh.len());
                    }
                    let seq = head.head + 1 + clamp(fresh.len() as u64);
                    new_bytes = new_bytes.saturating_add(clamp(event.size()));
                    slots.push(Slot::Fresh(fresh.len()));
                    fresh.push((event, seq, uuid::Uuid::now_v7().to_string()));
                }

                if !fresh.is_empty() {
                    Self::check_limits(&mut tx, feed, &head, clamp(fresh.len() as u64), new_bytes)
                        .await?;
                }

                let mut acked: Vec<(i64, String)> = Vec::with_capacity(fresh.len());
                let mut keyed: Vec<(String, i64, String)> = Vec::new();
                let mut pending = fresh.into_iter().peekable();
                while pending.peek().is_some() {
                    let chunk: Vec<_> = pending.by_ref().take(INSERT_CHUNK).collect();
                    const ROW: &str = "(?, 0, ?, ?, ?, {now}, ?, ?, ?, ?, ?, ?)";
                    const HEAD: &str = "INSERT INTO faucet_feed_events (feed, lane, seq, id, \
                        received_at, stored_at, content_type, headers, payload, size, sealed, \
                        dedupe_key) VALUES ";
                    let many;
                    let sql: &str = if chunk.len() == 1 {
                        stmt!(Self::D, &format!("{HEAD}{ROW}"))
                    } else {
                        let mut text = String::from(HEAD);
                        for i in 0..chunk.len() {
                            if i > 0 {
                                text.push_str(", ");
                            }
                            text.push_str(ROW);
                        }
                        many = Self::D.render(&text);
                        &many
                    };
                    let mut query = sqlx::query(sql);
                    for (event, seq, id) in chunk {
                        let headers = serde_json::to_string(&event.headers)
                            .map_err(|e| FeedError::Invalid(format!("event headers: {e}")))?;
                        let size = clamp(event.size());
                        if let Some(key) = event.dedupe_key.clone().filter(|_| dedupe) {
                            keyed.push((key, seq, id.clone()));
                        }
                        query = query
                            .bind(feed.as_str())
                            .bind(seq)
                            .bind(id.clone())
                            .bind(event.received_at.timestamp_millis())
                            .bind(event.content_type)
                            .bind(headers)
                            .bind(event.payload)
                            .bind(size)
                            .bind(event.sealed)
                            .bind(event.dedupe_key);
                        acked.push((seq, id));
                    }
                    query.execute(&mut *tx).await.map_err(Self::be)?;
                }

                for chunk in keyed.chunks(KEY_CHUNK) {
                    let mut text = String::from(
                        "INSERT INTO faucet_feed_dedupe (feed, \"key\", seq, id, expires_at) VALUES ",
                    );
                    for i in 0..chunk.len() {
                        if i > 0 {
                            text.push_str(", ");
                        }
                        text.push_str("(?, ?, ?, ?, {now} + ?)");
                    }
                    text.push_str(
                        " ON CONFLICT (feed, \"key\") DO UPDATE SET seq = excluded.seq, \
                         id = excluded.id, expires_at = excluded.expires_at",
                    );
                    let sql = Self::D.render(&text);
                    let mut query = sqlx::query(&sql);
                    for (key, seq, id) in chunk {
                        query = query
                            .bind(feed.as_str())
                            .bind(key.as_str())
                            .bind(*seq)
                            .bind(id.as_str())
                            .bind(head.dedupe_window_ms);
                    }
                    query.execute(&mut *tx).await.map_err(Self::be)?;
                }

                if let Some((last, _)) = acked.last() {
                    sqlx::query(stmt!(
                        Self::D,
                        "UPDATE faucet_feed_heads SET head = ?, bytes = bytes + ? WHERE feed = ?"
                    ))
                    .bind(*last)
                    .bind(new_bytes)
                    .bind(feed.as_str())
                    .execute(&mut *tx)
                    .await
                    .map_err(Self::be)?;
                }
                // From the commit on, a failure leaves it unknown whether the
                // events were stored: never report it as a refusal.
                let unknown = |e: FeedError| {
                    FeedError::Unavailable(format!(
                        "the append's outcome is unknown (it failed at or after its commit): {e}"
                    ))
                };
                tx.commit().await.map_err(Self::be).map_err(unknown)?;

                let acknowledge = || -> $crate::Result<AppendAck> {
                    let mut ack = AppendAck {
                        first_seq: acked.first().map(|(s, _)| seq_of(*s)).transpose()?,
                        last_seq: acked.last().map(|(s, _)| seq_of(*s)).transpose()?,
                        ..AppendAck::default()
                    };
                    for slot in slots {
                        match slot {
                            Slot::Fresh(at) => {
                                let (seq, id) = &acked[at];
                                ack.events.push(Appended {
                                    id: id.clone(),
                                    seq: seq_of(*seq)?,
                                    duplicate: false,
                                });
                            }
                            Slot::Stored(seq, id) => {
                                ack.duplicates += 1;
                                ack.events.push(Appended {
                                    id,
                                    seq: seq_of(seq)?,
                                    duplicate: true,
                                });
                            }
                        }
                    }
                    Ok(ack)
                };
                acknowledge().map_err(unknown)
            }

            async fn do_heads(
                &self,
                feeds: &[$crate::FeedUid],
            ) -> $crate::Result<Vec<$crate::Head>> {
                use sqlx::Row;
                use $crate::sql::{count_of, placeholders, seq_of, time_of};
                use $crate::{ClaimId, ClaimInfo, ClaimOwner, DeliveryHead, DeliveryUid, Epoch};
                use $crate::{FeedUid, Head, TenantId};
                let mut heads: Vec<Head> = Vec::new();
                for feeds in feeds.chunks($crate::sql::HEADS_CHUNK) {
                    let sql = Self::D.render(&format!(
                        "SELECT h.feed, h.tenant, h.head, h.bytes, \
                         (SELECT MIN(e.seq) FROM faucet_feed_events e \
                          WHERE e.feed = h.feed AND e.lane = 0), \
                         {{now}}, d.delivery, d.epoch, d.\"cursor\", d.claim_id, d.claim_to, \
                         d.claim_owner, d.holder, d.claim_expires_at, \
                         (SELECT e.stored_at FROM faucet_feed_events e \
                          WHERE e.feed = d.feed AND e.lane = 0 AND e.seq = d.\"cursor\") \
                         FROM faucet_feed_heads h \
                         LEFT JOIN faucet_feed_deliveries d ON d.feed = h.feed \
                         WHERE h.feed IN ({}) ORDER BY h.feed, d.delivery",
                        placeholders(feeds.len())
                    ));
                    let mut query = sqlx::query(&sql);
                    for feed in feeds {
                        query = query.bind(feed.as_str());
                    }
                    let rows = query.fetch_all(&self.pool).await.map_err(Self::be)?;
                    for row in rows {
                        let feed: String = row.try_get(0).map_err(Self::be)?;
                        if heads.last().map(|h| h.feed.as_str()) != Some(feed.as_str()) {
                            let tenant: Option<String> = row.try_get(1).map_err(Self::be)?;
                            let head: i64 = row.try_get(2).map_err(Self::be)?;
                            let bytes: i64 = row.try_get(3).map_err(Self::be)?;
                            let oldest: Option<i64> = row.try_get(4).map_err(Self::be)?;
                            let now: i64 = row.try_get(5).map_err(Self::be)?;
                            heads.push(Head {
                                feed: FeedUid::new(feed)?,
                                tenant: tenant.map(TenantId::new).transpose()?,
                                watermark: seq_of(head)?,
                                bytes: count_of(bytes),
                                oldest: seq_of(oldest.unwrap_or(head + 1))?,
                                now: time_of(now)?,
                                deliveries: Vec::new(),
                            });
                        }
                        let delivery: Option<String> = row.try_get(6).map_err(Self::be)?;
                        let (Some(delivery), Some(head)) = (delivery, heads.last_mut()) else {
                            continue;
                        };
                        let epoch: i64 = row.try_get(7).map_err(Self::be)?;
                        let cursor: i64 = row.try_get(8).map_err(Self::be)?;
                        let claim_id: Option<String> = row.try_get(9).map_err(Self::be)?;
                        let claim_to: Option<i64> = row.try_get(10).map_err(Self::be)?;
                        let claim_owner: Option<String> = row.try_get(11).map_err(Self::be)?;
                        let holder: Option<String> = row.try_get(12).map_err(Self::be)?;
                        let expires: Option<i64> = row.try_get(13).map_err(Self::be)?;
                        let undelivered: Option<i64> = row.try_get(14).map_err(Self::be)?;
                        let now = head.now.timestamp_millis();
                        let claim = match (claim_id, claim_to, claim_owner, expires) {
                            (Some(id), Some(to), Some(owner), Some(expires)) if expires > now => {
                                Some(ClaimInfo {
                                    id: ClaimId::new(id)?,
                                    to: seq_of(to)?,
                                    owner: ClaimOwner::new(owner)?,
                                    expires_at: time_of(expires)?,
                                    attached: holder.is_some(),
                                })
                            }
                            _ => None,
                        };
                        head.deliveries.push(DeliveryHead {
                            delivery: DeliveryUid::new(head.feed.clone(), delivery)?,
                            cursor: seq_of(cursor)?,
                            epoch: Epoch(count_of(epoch)),
                            claim,
                            oldest_undelivered_at: undelivered.map(time_of).transpose()?,
                        });
                    }
                }
                Ok(heads)
            }

            async fn head_of(&self, feed: &$crate::FeedUid) -> $crate::Result<i64> {
                use sqlx::Row;
                use $crate::sql::stmt;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT head FROM faucet_feed_heads WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(Self::be)?
                .ok_or_else(|| $crate::FeedError::FeedNotFound(feed.clone()))?;
                row.try_get(0).map_err(Self::be)
            }

            async fn oldest_of(&self, feed: &$crate::FeedUid, head: i64) -> $crate::Result<i64> {
                use sqlx::Row;
                use $crate::sql::stmt;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT MIN(seq) FROM faucet_feed_events WHERE feed = ? AND lane = 0"
                ))
                .bind(feed.as_str())
                .fetch_one(&self.pool)
                .await
                .map_err(Self::be)?;
                let oldest: Option<i64> = row.try_get(0).map_err(Self::be)?;
                Ok(oldest.unwrap_or(head + 1))
            }

            async fn do_read(
                &self,
                feed: &$crate::FeedUid,
                from: $crate::Seq,
                to: $crate::Seq,
            ) -> $crate::Result<$crate::Batch> {
                use sqlx::Row;
                use $crate::sql::{count_of, seq_of, seq_param, stmt, time_of};
                use $crate::{Batch, FeedError, StoredEvent};
                if to < from {
                    return Err(FeedError::Invalid(format!("empty range [{from}, {to})")));
                }
                let start = seq_param(from)?;
                let end = (self.head_of(feed).await? + 1).min(seq_param(to)?);
                if start > end {
                    return Err(FeedError::PastWatermark {
                        feed: feed.clone(),
                        requested: from,
                        end: seq_of(end)?,
                    });
                }
                let rows = sqlx::query(stmt!(
                    Self::D,
                    "SELECT seq, id, received_at, stored_at, content_type, headers, payload, \
                     size, sealed, dedupe_key FROM faucet_feed_events \
                     WHERE feed = ? AND lane = 0 AND seq >= ? AND seq < ? ORDER BY seq"
                ))
                .bind(feed.as_str())
                .bind(start)
                .bind(end)
                .fetch_all(&self.pool)
                .await
                .map_err(Self::be)?;
                let mut events = Vec::with_capacity(rows.len());
                for (i, row) in rows.into_iter().enumerate() {
                    let seq: i64 = row.try_get(0).map_err(Self::be)?;
                    if seq != start + i as i64 {
                        break;
                    }
                    let headers: String = row.try_get(5).map_err(Self::be)?;
                    let size: i64 = row.try_get(7).map_err(Self::be)?;
                    events.push(StoredEvent {
                        seq: seq_of(seq)?,
                        id: row.try_get(1).map_err(Self::be)?,
                        received_at: time_of(row.try_get(2).map_err(Self::be)?)?,
                        stored_at: time_of(row.try_get(3).map_err(Self::be)?)?,
                        content_type: row.try_get(4).map_err(Self::be)?,
                        headers: serde_json::from_str(&headers).map_err(|e| {
                            FeedError::Corrupt(format!("event {seq} headers: {e}"))
                        })?,
                        payload: row.try_get(6).map_err(Self::be)?,
                        size: count_of(size),
                        sealed: row.try_get(8).map_err(Self::be)?,
                        dedupe_key: row.try_get(9).map_err(Self::be)?,
                    });
                }
                if events.len() as i64 != end - start {
                    let oldest = self.oldest_of(feed, end - 1).await?;
                    return Err(FeedError::RangeUnavailable {
                        feed: feed.clone(),
                        from,
                        to: seq_of(end)?,
                        oldest: seq_of(oldest)?,
                    });
                }
                Ok(Batch {
                    feed: feed.clone(),
                    from,
                    to: seq_of(end)?,
                    events,
                })
            }

            async fn do_bound(
                &self,
                feed: &$crate::FeedUid,
                from: $crate::Seq,
                max: $crate::BatchLimits,
            ) -> $crate::Result<$crate::Seq> {
                use sqlx::Row;
                use $crate::sql::{clamp, seq_of, seq_param, stmt};
                use $crate::FeedError;
                let start = seq_param(from)?;
                let head = self.head_of(feed).await?;
                if start > head + 1 {
                    return Err(FeedError::PastWatermark {
                        feed: feed.clone(),
                        requested: from,
                        end: seq_of(head + 1)?,
                    });
                }
                let rows = sqlx::query(stmt!(
                    Self::D,
                    "SELECT seq, size FROM faucet_feed_events \
                     WHERE feed = ? AND lane = 0 AND seq >= ? AND seq <= ? ORDER BY seq LIMIT ?"
                ))
                .bind(feed.as_str())
                .bind(start)
                .bind(head)
                .bind(clamp(max.events))
                .fetch_all(&self.pool)
                .await
                .map_err(Self::be)?;
                let limit = clamp(max.bytes);
                let mut to = start;
                let mut bytes: i64 = 0;
                for row in rows {
                    let seq: i64 = row.try_get(0).map_err(Self::be)?;
                    let size: i64 = row.try_get(1).map_err(Self::be)?;
                    if seq != to {
                        let oldest = self.oldest_of(feed, head).await?;
                        return Err(FeedError::RangeUnavailable {
                            feed: feed.clone(),
                            from,
                            to: seq_of(head + 1)?,
                            oldest: seq_of(oldest)?,
                        });
                    }
                    if to > start && bytes.saturating_add(size) > limit {
                        break;
                    }
                    bytes = bytes.saturating_add(size);
                    to = seq + 1;
                }
                if to == start && start <= head && max.events > 0 {
                    let oldest = self.oldest_of(feed, head).await?;
                    return Err(FeedError::RangeUnavailable {
                        feed: feed.clone(),
                        from,
                        to: seq_of(head + 1)?,
                        oldest: seq_of(oldest)?,
                    });
                }
                seq_of(to)
            }

            async fn do_seq_at(
                &self,
                feed: &$crate::FeedUid,
                time: $crate::Timestamp,
            ) -> $crate::Result<$crate::Seq> {
                let mut tx = self.pool.begin().await.map_err(Self::be)?;
                let head = Self::load_head(&mut tx, feed).await?;
                let at = Self::first_at(&mut tx, feed, time, head.head + 1).await?;
                tx.commit().await.map_err(Self::be)?;
                $crate::sql::seq_of(at)
            }

            async fn do_bytes_from(
                &self,
                feed: &$crate::FeedUid,
                from: $crate::Seq,
            ) -> $crate::Result<Option<u64>> {
                use sqlx::Row;
                use $crate::sql::{seq_param, stmt};
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT CAST(COALESCE(SUM(size), 0) AS BIGINT) FROM faucet_feed_events \
                     WHERE feed = ? AND lane = 0 AND seq >= ?"
                ))
                .bind(feed.as_str())
                .bind(seq_param(from)?)
                .fetch_one(&self.pool)
                .await
                .map_err(Self::be)?;
                let bytes: i64 = row.try_get(0).map_err(Self::be)?;
                Ok(Some(u64::try_from(bytes).unwrap_or(0)))
            }

            async fn do_tenant_bytes(
                &self,
                tenant: &$crate::TenantId,
            ) -> $crate::Result<Option<u64>> {
                use sqlx::Row;
                use $crate::sql::stmt;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT CAST(COALESCE(SUM(bytes), 0) AS BIGINT) \
                     FROM faucet_feed_heads WHERE tenant = ?"
                ))
                .bind(tenant.as_str())
                .fetch_one(&self.pool)
                .await
                .map_err(Self::be)?;
                let bytes: i64 = row.try_get(0).map_err(Self::be)?;
                Ok(Some(u64::try_from(bytes).unwrap_or(0)))
            }

            async fn do_hourly_counts(
                &self,
                feed: &$crate::FeedUid,
                hours: u32,
            ) -> $crate::Result<Option<Vec<u64>>> {
                use sqlx::Row;
                use $crate::sql::{count_of, stmt};
                let hours = hours.clamp(1, 24 * 31);
                let rows = sqlx::query(stmt!(
                    Self::D,
                    "SELECT CAST(({now} - stored_at) / 3600000 AS BIGINT), \
                     CAST(COUNT(*) AS BIGINT) FROM faucet_feed_events \
                     WHERE feed = ? AND lane = 0 AND stored_at > {now} - ? GROUP BY 1"
                ))
                .bind(feed.as_str())
                .bind(i64::from(hours) * 3_600_000)
                .fetch_all(&self.pool)
                .await
                .map_err(Self::be)?;
                let mut counts = vec![0u64; hours as usize];
                for row in rows {
                    let ago: i64 = row.try_get(0).map_err(Self::be)?;
                    let n: i64 = row.try_get(1).map_err(Self::be)?;
                    let ago = usize::try_from(ago.max(0)).unwrap_or(usize::MAX);
                    if let Some(slot) = (hours as usize)
                        .checked_sub(1 + ago)
                        .and_then(|i| counts.get_mut(i))
                    {
                        *slot += count_of(n);
                    }
                }
                Ok(Some(counts))
            }

            async fn do_newest_received(
                &self,
                feeds: &[$crate::FeedUid],
            ) -> $crate::Result<Option<Vec<($crate::FeedUid, $crate::Timestamp)>>> {
                use sqlx::Row;
                use $crate::sql::{placeholders, time_of};
                let mut out = Vec::new();
                for feeds in feeds.chunks($crate::sql::HEADS_CHUNK) {
                    let sql = Self::D.render(&format!(
                        "SELECT e.feed, e.received_at FROM faucet_feed_events e \
                         JOIN faucet_feed_heads h ON h.feed = e.feed AND h.head = e.seq \
                         WHERE e.lane = 0 AND e.feed IN ({})",
                        placeholders(feeds.len())
                    ));
                    let mut query = sqlx::query(&sql);
                    for feed in feeds {
                        query = query.bind(feed.as_str());
                    }
                    for row in query.fetch_all(&self.pool).await.map_err(Self::be)? {
                        let feed: String = row.try_get(0).map_err(Self::be)?;
                        let at: i64 = row.try_get(1).map_err(Self::be)?;
                        out.push(($crate::FeedUid::new(feed)?, time_of(at)?));
                    }
                }
                Ok(Some(out))
            }

            async fn do_claim(
                &self,
                delivery: &$crate::DeliveryUid,
                expected: $crate::Seq,
                to: $crate::Seq,
                owner: &$crate::ClaimOwner,
                ttl: std::time::Duration,
            ) -> $crate::Result<Option<$crate::Claim>> {
                use sqlx::Row;
                use $crate::sql::{count_of, millis, seq_param, stmt};
                use $crate::{Claim, ClaimId, Epoch, FeedError};
                if to <= expected {
                    return Err(FeedError::Invalid(format!(
                        "a claim must cover at least one event: [{expected}, {to})"
                    )));
                }
                let id = ClaimId::generate();
                let row = sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_deliveries SET claim_id = ?, claim_to = ?, \
                     claim_owner = ?, holder = NULL, claim_expires_at = {now} + ? \
                     WHERE feed = ? AND delivery = ? AND \"cursor\" = ? \
                     AND (claim_id IS NULL OR claim_expires_at <= {now}) \
                     AND ? <= (SELECT head + 1 FROM faucet_feed_heads WHERE feed = ?) \
                     RETURNING epoch"
                ))
                .bind(id.as_str())
                .bind(seq_param(to)?)
                .bind(owner.as_str())
                .bind(millis(ttl))
                .bind(delivery.feed().as_str())
                .bind(delivery.id())
                .bind(seq_param(expected)?)
                .bind(seq_param(to)?)
                .bind(delivery.feed().as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(Self::be)?;
                let Some(row) = row else {
                    return Ok(None);
                };
                let epoch: i64 = row.try_get(0).map_err(Self::be)?;
                Ok(Some(Claim {
                    id,
                    delivery: delivery.clone(),
                    from: expected,
                    to,
                    owner: owner.clone(),
                    epoch: Epoch(count_of(epoch)),
                }))
            }

            async fn do_attach(&self, claim: &$crate::ClaimId) -> $crate::Result<$crate::Holder> {
                use sqlx::Row;
                use $crate::sql::{count_of, seq_of, stmt};
                use $crate::{DeliveryUid, Epoch, FeedError, FeedUid, Holder};
                let token = uuid::Uuid::new_v4().simple().to_string();
                let row = sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_deliveries SET holder = ? \
                     WHERE claim_id = ? AND claim_expires_at > {now} \
                     RETURNING feed, delivery, \"cursor\", claim_to, epoch"
                ))
                .bind(token.as_str())
                .bind(claim.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(Self::be)?
                .ok_or_else(|| FeedError::ClaimLost {
                    claim: claim.clone(),
                })?;
                let feed: String = row.try_get(0).map_err(Self::be)?;
                let delivery: String = row.try_get(1).map_err(Self::be)?;
                let cursor: i64 = row.try_get(2).map_err(Self::be)?;
                let to: i64 = row.try_get(3).map_err(Self::be)?;
                let epoch: i64 = row.try_get(4).map_err(Self::be)?;
                Ok(Holder {
                    claim: claim.clone(),
                    token,
                    delivery: DeliveryUid::new(FeedUid::new(feed)?, delivery)?,
                    cursor: seq_of(cursor)?,
                    to: seq_of(to)?,
                    epoch: Epoch(count_of(epoch)),
                })
            }

            async fn do_renew(
                &self,
                claim: &$crate::ClaimId,
                holder: Option<&str>,
                ttl: std::time::Duration,
            ) -> $crate::Result<bool> {
                use $crate::sql::{millis, stmt};
                let done = match holder {
                    Some(token) => sqlx::query(stmt!(
                        Self::D,
                        "UPDATE faucet_feed_deliveries SET claim_expires_at = {now} + ? \
                         WHERE claim_id = ? AND holder = ? AND claim_expires_at > {now}"
                    ))
                    .bind(millis(ttl))
                    .bind(claim.as_str())
                    .bind(token)
                    .execute(&self.pool)
                    .await,
                    None => sqlx::query(stmt!(
                        Self::D,
                        "UPDATE faucet_feed_deliveries SET claim_expires_at = {now} + ? \
                         WHERE claim_id = ? AND claim_expires_at > {now}"
                    ))
                    .bind(millis(ttl))
                    .bind(claim.as_str())
                    .execute(&self.pool)
                    .await,
                };
                Ok(done.map_err(Self::be)?.rows_affected() == 1)
            }

            async fn do_commit(
                &self,
                holder: &$crate::Holder,
                expected: $crate::Seq,
                new: $crate::Seq,
            ) -> $crate::Result<()> {
                use sqlx::Row;
                use $crate::sql::{seq_of, seq_param, stmt};
                use $crate::FeedError;
                if new < expected {
                    return Err(FeedError::Invalid(format!(
                        "a commit never moves the cursor back: {expected} to {new}"
                    )));
                }
                let moved = sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_deliveries SET \"cursor\" = ? \
                     WHERE claim_id = ? AND holder = ? AND claim_expires_at > {now} \
                     AND \"cursor\" = ? AND ? <= (SELECT h.head + 1 FROM faucet_feed_heads h \
                     WHERE h.feed = faucet_feed_deliveries.feed)"
                ))
                .bind(seq_param(new)?)
                .bind(holder.claim.as_str())
                .bind(holder.token.as_str())
                .bind(seq_param(expected)?)
                .bind(seq_param(new)?)
                .execute(&self.pool)
                .await
                .map_err(Self::be)?
                .rows_affected();
                if moved == 1 {
                    return Ok(());
                }
                let lost = || FeedError::ClaimLost {
                    claim: holder.claim.clone(),
                };
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT d.\"cursor\", d.holder, d.claim_expires_at, {now}, h.head \
                     FROM faucet_feed_deliveries d JOIN faucet_feed_heads h ON h.feed = d.feed \
                     WHERE d.claim_id = ?"
                ))
                .bind(holder.claim.as_str())
                .fetch_optional(&self.pool)
                .await
                .map_err(Self::be)?
                .ok_or_else(lost)?;
                let cursor: i64 = row.try_get(0).map_err(Self::be)?;
                let token: Option<String> = row.try_get(1).map_err(Self::be)?;
                let expires: Option<i64> = row.try_get(2).map_err(Self::be)?;
                let now: i64 = row.try_get(3).map_err(Self::be)?;
                let head: i64 = row.try_get(4).map_err(Self::be)?;
                if token.as_deref() != Some(holder.token.as_str())
                    || expires.is_none_or(|at| at <= now)
                {
                    return Err(lost());
                }
                if cursor != seq_param(expected)? {
                    return Err(FeedError::CursorConflict {
                        delivery: holder.delivery.clone(),
                        expected,
                        actual: seq_of(cursor)?,
                    });
                }
                Err(FeedError::PastWatermark {
                    feed: holder.delivery.feed().clone(),
                    requested: new,
                    end: seq_of(head + 1)?,
                })
            }

            async fn do_release(&self, claim: &$crate::ClaimId) -> $crate::Result<()> {
                use $crate::sql::stmt;
                sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_deliveries SET claim_id = NULL, claim_to = NULL, \
                     claim_owner = NULL, holder = NULL, claim_expires_at = NULL \
                     WHERE claim_id = ?"
                ))
                .bind(claim.as_str())
                .execute(&self.pool)
                .await
                .map_err(Self::be)?;
                Ok(())
            }

            async fn do_move_cursor(
                &self,
                delivery: &$crate::DeliveryUid,
                expected: $crate::Seq,
                new: $crate::Seq,
                why: $crate::Move,
            ) -> $crate::Result<$crate::Epoch> {
                use sqlx::Row;
                use $crate::sql::{count_of, seq_of, seq_param, stmt};
                use $crate::{Epoch, FeedError, Move};
                let feed = delivery.feed();
                let from = seq_param(expected)?;
                let to = seq_param(new)?;
                let mut tx = self.begin().await?;
                Self::lock_feed(&mut tx, feed).await?;
                let head = Self::load_head(&mut tx, feed).await?;
                let end = head.head + 1;
                let oldest = Self::oldest(&mut tx, feed, head.head).await?;
                let out_of_range = || -> $crate::Result<FeedError> {
                    Ok(FeedError::OutOfRange {
                        feed: feed.clone(),
                        requested: new,
                        oldest: seq_of(oldest)?,
                        end: seq_of(end)?,
                    })
                };
                let gap: Option<(i64, i64, String, String)> = match why {
                    Move::Replay => {
                        if to >= from {
                            return Err(FeedError::Invalid(format!(
                                "a replay moves the cursor back: {expected} to {new}"
                            )));
                        }
                        if to < oldest {
                            return Err(out_of_range()?);
                        }
                        None
                    }
                    Move::Skip { reason, by } => {
                        if to <= from {
                            return Err(FeedError::Invalid(format!(
                                "a skip moves the cursor forward: {expected} to {new}"
                            )));
                        }
                        if to > end {
                            return Err(FeedError::PastWatermark {
                                feed: feed.clone(),
                                requested: new,
                                end: seq_of(end)?,
                            });
                        }
                        Some((from, to, reason, by))
                    }
                    Move::Recover => {
                        if to < oldest || to > end {
                            return Err(out_of_range()?);
                        }
                        (to != from).then(|| {
                            (from.min(to), from.max(to), "recover".to_string(), "store".to_string())
                        })
                    }
                };
                let moved = sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_deliveries SET \"cursor\" = ?, epoch = epoch + 1, \
                     claim_id = NULL, claim_to = NULL, claim_owner = NULL, holder = NULL, \
                     claim_expires_at = NULL \
                     WHERE feed = ? AND delivery = ? AND \"cursor\" = ? \
                     AND (claim_id IS NULL OR claim_expires_at <= {now}) RETURNING epoch"
                ))
                .bind(to)
                .bind(feed.as_str())
                .bind(delivery.id())
                .bind(from)
                .fetch_optional(&mut *tx)
                .await
                .map_err(Self::be)?;
                let Some(moved) = moved else {
                    let row = sqlx::query(stmt!(
                        Self::D,
                        "SELECT \"cursor\" FROM faucet_feed_deliveries \
                         WHERE feed = ? AND delivery = ?"
                    ))
                    .bind(feed.as_str())
                    .bind(delivery.id())
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(Self::be)?
                    .ok_or_else(|| FeedError::DeliveryNotFound(delivery.clone()))?;
                    let cursor: i64 = row.try_get(0).map_err(Self::be)?;
                    if cursor != from {
                        return Err(FeedError::CursorConflict {
                            delivery: delivery.clone(),
                            expected,
                            actual: seq_of(cursor)?,
                        });
                    }
                    return Err(FeedError::ClaimLive {
                        delivery: delivery.clone(),
                    });
                };
                let epoch: i64 = moved.try_get(0).map_err(Self::be)?;
                if let Some((from, to, reason, by)) = gap {
                    Self::write_gap(&mut tx, delivery, from, to, &reason, &by).await?;
                }
                tx.commit().await.map_err(Self::be)?;
                Ok(Epoch(count_of(epoch)))
            }

            async fn do_gaps(
                &self,
                feed: &$crate::FeedUid,
                delivery: Option<&$crate::DeliveryUid>,
            ) -> $crate::Result<Vec<$crate::Gap>> {
                use sqlx::Row;
                use $crate::sql::{seq_of, stmt, time_of};
                use $crate::{DeliveryUid, Gap};
                let rows = match delivery {
                    Some(d) => sqlx::query(stmt!(
                        Self::D,
                        "SELECT delivery, from_seq, to_seq, reason, \"at\", \"by\" \
                         FROM faucet_feed_gaps WHERE feed = ? AND delivery = ? \
                         ORDER BY \"at\", from_seq"
                    ))
                    .bind(feed.as_str())
                    .bind(d.id())
                    .fetch_all(&self.pool)
                    .await,
                    None => sqlx::query(stmt!(
                        Self::D,
                        "SELECT delivery, from_seq, to_seq, reason, \"at\", \"by\" \
                         FROM faucet_feed_gaps WHERE feed = ? ORDER BY \"at\", from_seq"
                    ))
                    .bind(feed.as_str())
                    .fetch_all(&self.pool)
                    .await,
                }
                .map_err(Self::be)?;
                let mut gaps = Vec::with_capacity(rows.len());
                for row in rows {
                    let delivery: Option<String> = row.try_get(0).map_err(Self::be)?;
                    gaps.push(Gap {
                        feed: feed.clone(),
                        delivery: delivery
                            .map(|id| DeliveryUid::new(feed.clone(), id))
                            .transpose()?,
                        from: seq_of(row.try_get(1).map_err(Self::be)?)?,
                        to: seq_of(row.try_get(2).map_err(Self::be)?)?,
                        reason: row.try_get(3).map_err(Self::be)?,
                        at: time_of(row.try_get(4).map_err(Self::be)?)?,
                        by: row.try_get(5).map_err(Self::be)?,
                    });
                }
                Ok(gaps)
            }

            async fn do_trim(
                &self,
                feed: &$crate::FeedUid,
                max_rows: u32,
            ) -> $crate::Result<$crate::TrimStats> {
                use sqlx::Row;
                use $crate::sql::{count_of, stmt};
                use $crate::TrimStats;
                if max_rows == 0 {
                    return Ok(TrimStats::default());
                }
                let mut tx = self.begin().await?;
                Self::lock_feed(&mut tx, feed).await?;
                let head = Self::load_head(&mut tx, feed).await?;
                let row = sqlx::query(stmt!(
                    Self::D,
                    "SELECT MIN(\"cursor\") FROM faucet_feed_deliveries WHERE feed = ?"
                ))
                .bind(feed.as_str())
                .fetch_one(&mut *tx)
                .await
                .map_err(Self::be)?;
                let lowest: Option<i64> = row.try_get(0).map_err(Self::be)?;
                let below = lowest.unwrap_or(head.head + 1).min(head.head + 1);
                let rows = sqlx::query(stmt!(
                    Self::D,
                    "SELECT seq, size, {now} - stored_at FROM faucet_feed_events \
                     WHERE feed = ? AND lane = 0 AND seq < ? ORDER BY seq LIMIT ?"
                ))
                .bind(feed.as_str())
                .bind(below)
                .bind(i64::from(max_rows))
                .fetch_all(&mut *tx)
                .await
                .map_err(Self::be)?;
                let candidates = rows.len();
                let mut last: Option<i64> = None;
                let mut events: u64 = 0;
                let mut bytes: i64 = 0;
                for row in rows {
                    let seq: i64 = row.try_get(0).map_err(Self::be)?;
                    let size: i64 = row.try_get(1).map_err(Self::be)?;
                    let age: i64 = row.try_get(2).map_err(Self::be)?;
                    if age < head.max_age_ms {
                        break;
                    }
                    last = Some(seq);
                    events += 1;
                    bytes = bytes.saturating_add(size);
                }
                let Some(last) = last else {
                    return Ok(TrimStats::default());
                };
                sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_events WHERE feed = ? AND lane = 0 AND seq <= ?"
                ))
                .bind(feed.as_str())
                .bind(last)
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_heads SET bytes = CASE WHEN bytes > ? THEN bytes - ? \
                     ELSE 0 END WHERE feed = ?"
                ))
                .bind(bytes)
                .bind(bytes)
                .bind(feed.as_str())
                .execute(&mut *tx)
                .await
                .map_err(Self::be)?;
                tx.commit().await.map_err(Self::be)?;
                Ok(TrimStats {
                    events,
                    bytes: count_of(bytes),
                    more: events as usize == candidates && candidates == max_rows as usize,
                })
            }

            async fn do_purge_dedupe(
                &self,
                feed: &$crate::FeedUid,
                max_rows: u32,
            ) -> $crate::Result<u64> {
                use $crate::sql::stmt;
                let removed = sqlx::query(stmt!(
                    Self::D,
                    "DELETE FROM faucet_feed_dedupe WHERE feed = ? AND \"key\" IN (\
                     SELECT \"key\" FROM faucet_feed_dedupe \
                     WHERE feed = ? AND expires_at <= {now} LIMIT ?)"
                ))
                .bind(feed.as_str())
                .bind(feed.as_str())
                .bind(i64::from(max_rows))
                .execute(&self.pool)
                .await
                .map_err(Self::be)?
                .rows_affected();
                Ok(removed)
            }

            async fn do_maintenance(
                &self,
                feed: &$crate::FeedUid,
                owner: &$crate::ClaimOwner,
                ttl: std::time::Duration,
            ) -> $crate::Result<bool> {
                use $crate::sql::{millis, stmt};
                let taken = sqlx::query(stmt!(
                    Self::D,
                    "UPDATE faucet_feed_heads SET maintenance_owner = ?, \
                     maintenance_expires_at = {now} + ? WHERE feed = ? \
                     AND (maintenance_owner IS NULL OR maintenance_expires_at <= {now} \
                     OR maintenance_owner = ?)"
                ))
                .bind(owner.as_str())
                .bind(millis(ttl))
                .bind(feed.as_str())
                .bind(owner.as_str())
                .execute(&self.pool)
                .await
                .map_err(Self::be)?
                .rows_affected();
                if taken == 1 {
                    return Ok(true);
                }
                self.head_of(feed).await?;
                Ok(false)
            }
        }

        #[async_trait::async_trait]
        impl $crate::FeedStore for $name {
            fn store_id(&self) -> Option<i64> {
                Some(self.store_id)
            }

            async fn ensure_feed(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                tenant: Option<&$crate::TenantId>,
                limits: &$crate::StoreLimits,
            ) -> $crate::Result<()> {
                $crate::sql::once(calls, "ensure_feed", self.do_ensure_feed(feed, tenant, limits))
                    .await
            }

            async fn ensure_delivery(
                &self,
                calls: &faucet_core::io::CallContext,
                delivery: &$crate::DeliveryUid,
                start: $crate::Start,
            ) -> $crate::Result<$crate::DeliveryState> {
                $crate::sql::once(
                    calls,
                    "ensure_delivery",
                    self.do_ensure_delivery(delivery, start),
                )
                .await
            }

            async fn delete_delivery(
                &self,
                calls: &faucet_core::io::CallContext,
                delivery: &$crate::DeliveryUid,
            ) -> $crate::Result<()> {
                $crate::sql::once(calls, "delete_delivery", self.do_delete_delivery(delivery)).await
            }

            async fn purge_feed(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
            ) -> $crate::Result<$crate::PurgeStats> {
                $crate::sql::once(calls, "purge_feed", self.do_purge_feed(feed)).await
            }

            async fn append(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                events: Vec<$crate::NewEvent>,
            ) -> $crate::Result<$crate::AppendAck> {
                $crate::sql::once(calls, "append", self.do_append(feed, events)).await
            }

            async fn heads(
                &self,
                calls: &faucet_core::io::CallContext,
                feeds: &[$crate::FeedUid],
            ) -> $crate::Result<Vec<$crate::Head>> {
                $crate::sql::each(calls, "heads", || self.do_heads(feeds)).await
            }

            async fn read(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                from: $crate::Seq,
                to: $crate::Seq,
            ) -> $crate::Result<$crate::Batch> {
                $crate::sql::each(calls, "read", || self.do_read(feed, from, to)).await
            }

            async fn bound(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                from: $crate::Seq,
                max: $crate::BatchLimits,
            ) -> $crate::Result<$crate::Seq> {
                $crate::sql::each(calls, "bound", || self.do_bound(feed, from, max)).await
            }

            async fn seq_at(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                time: $crate::Timestamp,
            ) -> $crate::Result<$crate::Seq> {
                $crate::sql::each(calls, "seq_at", || self.do_seq_at(feed, time)).await
            }

            async fn bytes_from(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                from: $crate::Seq,
            ) -> $crate::Result<Option<u64>> {
                $crate::sql::each(calls, "bytes_from", || self.do_bytes_from(feed, from)).await
            }

            async fn tenant_bytes(
                &self,
                calls: &faucet_core::io::CallContext,
                tenant: &$crate::TenantId,
            ) -> $crate::Result<Option<u64>> {
                $crate::sql::each(calls, "tenant_bytes", || self.do_tenant_bytes(tenant)).await
            }

            async fn hourly_counts(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                hours: u32,
            ) -> $crate::Result<Option<Vec<u64>>> {
                $crate::sql::each(calls, "hourly_counts", || {
                    self.do_hourly_counts(feed, hours)
                })
                .await
            }

            async fn newest_received(
                &self,
                calls: &faucet_core::io::CallContext,
                feeds: &[$crate::FeedUid],
            ) -> $crate::Result<Option<Vec<($crate::FeedUid, $crate::Timestamp)>>> {
                $crate::sql::each(calls, "newest_received", || self.do_newest_received(feeds))
                    .await
            }

            async fn claim(
                &self,
                calls: &faucet_core::io::CallContext,
                delivery: &$crate::DeliveryUid,
                expected_cursor: $crate::Seq,
                to: $crate::Seq,
                owner: &$crate::ClaimOwner,
                ttl: std::time::Duration,
            ) -> $crate::Result<Option<$crate::Claim>> {
                $crate::sql::once(
                    calls,
                    "claim",
                    self.do_claim(delivery, expected_cursor, to, owner, ttl),
                )
                .await
            }

            async fn attach(
                &self,
                calls: &faucet_core::io::CallContext,
                claim: &$crate::ClaimId,
            ) -> $crate::Result<$crate::Holder> {
                $crate::sql::once(calls, "attach", self.do_attach(claim)).await
            }

            async fn renew(
                &self,
                calls: &faucet_core::io::CallContext,
                holder: &$crate::Holder,
                ttl: std::time::Duration,
            ) -> $crate::Result<bool> {
                $crate::sql::once(
                    calls,
                    "renew",
                    self.do_renew(&holder.claim, Some(holder.token.as_str()), ttl),
                )
                .await
            }

            async fn renew_claim(
                &self,
                calls: &faucet_core::io::CallContext,
                claim: &$crate::ClaimId,
                ttl: std::time::Duration,
            ) -> $crate::Result<bool> {
                $crate::sql::once(calls, "renew_claim", self.do_renew(claim, None, ttl)).await
            }

            async fn commit(
                &self,
                calls: &faucet_core::io::CallContext,
                holder: &$crate::Holder,
                expected: $crate::Seq,
                new: $crate::Seq,
            ) -> $crate::Result<()> {
                $crate::sql::once(calls, "commit", self.do_commit(holder, expected, new)).await
            }

            async fn release(
                &self,
                calls: &faucet_core::io::CallContext,
                claim: &$crate::ClaimId,
            ) -> $crate::Result<()> {
                $crate::sql::once(calls, "release", self.do_release(claim)).await
            }

            async fn move_cursor(
                &self,
                calls: &faucet_core::io::CallContext,
                delivery: &$crate::DeliveryUid,
                expected: $crate::Seq,
                new: $crate::Seq,
                why: $crate::Move,
            ) -> $crate::Result<$crate::Epoch> {
                $crate::sql::once(
                    calls,
                    "move_cursor",
                    self.do_move_cursor(delivery, expected, new, why),
                )
                .await
            }

            async fn gaps(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                delivery: Option<&$crate::DeliveryUid>,
            ) -> $crate::Result<Vec<$crate::Gap>> {
                $crate::sql::each(calls, "gaps", || self.do_gaps(feed, delivery)).await
            }

            async fn trim(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                max_rows: u32,
            ) -> $crate::Result<$crate::TrimStats> {
                $crate::sql::once(calls, "trim", self.do_trim(feed, max_rows)).await
            }

            async fn purge_dedupe(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                max_rows: u32,
            ) -> $crate::Result<u64> {
                $crate::sql::once(calls, "purge_dedupe", self.do_purge_dedupe(feed, max_rows)).await
            }

            async fn maintenance(
                &self,
                calls: &faucet_core::io::CallContext,
                feed: &$crate::FeedUid,
                owner: &$crate::ClaimOwner,
                ttl: std::time::Duration,
            ) -> $crate::Result<bool> {
                $crate::sql::once(calls, "maintenance", self.do_maintenance(feed, owner, ttl)).await
            }

            fn capabilities(&self) -> $crate::StoreCaps {
                $caps
            }
        }
    };
}
pub(crate) use impl_feed_store;

#[cfg(test)]
mod tests {
    use super::Dialect;

    #[test]
    fn a_read_only_or_restarting_postgres_is_unavailable_not_a_refusal() {
        for code in ["25006", "57P04", "55P03", "40001", "08006", "53300"] {
            assert!(Dialect::Postgres.transient(code), "{code}");
        }
        for code in ["22021", "23505", "42501", "42P01"] {
            assert!(!Dialect::Postgres.transient(code), "{code}");
        }
        assert!(Dialect::Sqlite.transient("5") && !Dialect::Sqlite.transient("19"));
    }
}

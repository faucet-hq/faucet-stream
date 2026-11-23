//! The PostgreSQL backend: safe for several servers.

use crate::error::{FeedError, Result};
use crate::sql::{Dialect, backend, impl_feed_store};
use crate::types::StoreCaps;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

/// How a [`PostgresFeedStore`] connects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PostgresOptions {
    /// Connections in the store's own pool.
    pub max_connections: u32,
    /// How long a call waits for a pooled connection.
    pub acquire_timeout: Duration,
    /// `lock_timeout`: how long a statement waits for a lock, the per-feed
    /// append lock included.
    pub lock_timeout: Duration,
    /// `statement_timeout`: the longest one statement may run.
    pub statement_timeout: Duration,
    /// `idle_in_transaction_session_timeout`: how long a session may sit idle
    /// inside a transaction before the database ends it.
    pub idle_in_transaction_timeout: Duration,
}

impl Default for PostgresOptions {
    fn default() -> Self {
        Self {
            max_connections: 10,
            acquire_timeout: Duration::from_secs(10),
            lock_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(30),
            idle_in_transaction_timeout: Duration::from_secs(30),
        }
    }
}

impl PostgresOptions {
    /// The defaults: 10 connections, a 5 second lock timeout, 30 second
    /// statement and idle-in-transaction timeouts.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the pool size.
    pub fn max_connections(mut self, connections: u32) -> Self {
        self.max_connections = connections.max(1);
        self
    }

    /// Set the pool acquire timeout.
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set `lock_timeout`.
    pub fn lock_timeout(mut self, timeout: Duration) -> Self {
        self.lock_timeout = timeout;
        self
    }

    /// Set `statement_timeout`.
    pub fn statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = timeout;
        self
    }

    /// Set `idle_in_transaction_session_timeout`.
    pub fn idle_in_transaction_timeout(mut self, timeout: Duration) -> Self {
        self.idle_in_transaction_timeout = timeout;
        self
    }
}

/// A feed store in PostgreSQL. Sequence numbers are assigned under a per-feed
/// `pg_advisory_xact_lock`; every session carries lock, statement and
/// idle-in-transaction timeouts, so the database ends a stuck appender. The
/// store always has its own connection pool.
#[derive(Debug, Clone)]
pub struct PostgresFeedStore {
    pool: PgPool,
    store_id: i64,
}

impl PostgresFeedStore {
    /// Connect to `url` and create the schema when missing.
    /// [`FeedError::NewerSchema`] when a newer release wrote it.
    pub async fn connect(url: &str, options: &PostgresOptions) -> Result<Self> {
        faucet_core::ensure_crypto_provider();
        let ms = |d: Duration| d.as_millis().max(1).to_string();
        let connect = faucet_core::libpq::connect_options("feed store", url, |e| {
            faucet_core::FaucetError::Config(format!("feed store: invalid PostgreSQL URL: {e}"))
        })
        .map_err(|e| FeedError::Invalid(e.to_string()))?
        .options([
            ("lock_timeout", ms(options.lock_timeout)),
            ("statement_timeout", ms(options.statement_timeout)),
            (
                "idle_in_transaction_session_timeout",
                ms(options.idle_in_transaction_timeout),
            ),
        ]);
        let pool = PgPoolOptions::new()
            .max_connections(options.max_connections.max(1))
            .acquire_timeout(options.acquire_timeout)
            .connect_with(connect)
            .await
            .map_err(|e| backend(Dialect::Postgres, e))?;
        let mut store = Self { pool, store_id: 0 };
        match store.migrate().await {
            Ok(id) => store.store_id = id,
            Err(e) => {
                store.pool.close().await;
                return Err(e);
            }
        }
        Ok(store)
    }

    /// Close the pool; later calls fail with [`FeedError::Unavailable`].
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

impl_feed_store!(
    PostgresFeedStore,
    sqlx::Postgres,
    "BEGIN",
    Dialect::Postgres,
    StoreCaps {
        backend: "postgres",
        cluster_safe: true,
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FeedStore, FeedUid, NewEvent, Seq, StoreLimits};
    use faucet_core::io::CallContext;
    use sqlx::Row;
    use std::time::Instant;
    use testcontainers::{ContainerAsync, ImageExt};
    use testcontainers_modules::postgres::Postgres;

    async fn start(options: &PostgresOptions) -> (ContainerAsync<Postgres>, PostgresFeedStore) {
        let container =
            faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine"))
                .await;
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("postgres port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        let store = PostgresFeedStore::connect(&url, options)
            .await
            .expect("connect");
        (container, store)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_session_of_the_pool_carries_the_timeouts() {
        let options = PostgresOptions::new()
            .lock_timeout(Duration::from_millis(1500))
            .statement_timeout(Duration::from_secs(7))
            .idle_in_transaction_timeout(Duration::from_secs(9))
            .max_connections(3);
        let (_container, store) = start(&options).await;
        let mut sessions = Vec::new();
        for _ in 0..3 {
            sessions.push(store.pool.acquire().await.expect("acquire"));
        }
        for session in &mut sessions {
            for (setting, expected) in [
                ("lock_timeout", "1500ms"),
                ("statement_timeout", "7s"),
                ("idle_in_transaction_session_timeout", "9s"),
            ] {
                let row = sqlx::query(&format!("SHOW {setting}"))
                    .fetch_one(&mut **session)
                    .await
                    .expect("show");
                let value: String = row.try_get(0).expect("a setting value");
                assert_eq!(value, expected, "{setting}");
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_database_ends_a_stuck_appender() {
        let options = PostgresOptions::new()
            .lock_timeout(Duration::from_secs(20))
            .idle_in_transaction_timeout(Duration::from_secs(1));
        let (_container, store) = start(&options).await;
        let calls = CallContext::new();
        let feed = FeedUid::generate();
        store
            .ensure_feed(&calls, &feed, None, &StoreLimits::new())
            .await
            .expect("ensure_feed");

        let mut stuck = store.pool.acquire().await.expect("acquire").detach();
        sqlx::query("BEGIN")
            .execute(&mut stuck)
            .await
            .expect("begin");
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
            .bind(format!("feed:{feed}"))
            .execute(&mut stuck)
            .await
            .expect("take the feed's append lock");

        let started = Instant::now();
        let ack = store
            .append(&calls, &feed, vec![NewEvent::new("text/plain", "x")])
            .await
            .expect("the append goes through once the database ends the idle appender");
        assert_eq!(ack.first_seq, Some(Seq(1)));
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "the idle-in-transaction timeout ended the holder, not the lock timeout"
        );
        assert!(
            sqlx::query("SELECT 1").execute(&mut stuck).await.is_err(),
            "the stuck session is gone"
        );
    }
}

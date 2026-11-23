//! The SQLite backend: one server per database file.

use crate::error::Result;
use crate::sql::{Dialect, backend, impl_feed_store};
use crate::types::StoreCaps;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteSynchronous,
};
use std::path::Path;
use std::time::Duration;

/// How a [`SqliteFeedStore`] opens its database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteOptions {
    /// How long a writer waits for the database's single write lock.
    pub busy_timeout: Duration,
    /// Connections in the store's own pool.
    pub max_connections: u32,
}

impl Default for SqliteOptions {
    fn default() -> Self {
        Self {
            busy_timeout: Duration::from_secs(5),
            max_connections: 8,
        }
    }
}

impl SqliteOptions {
    /// The defaults: a 5 second busy timeout and 8 connections.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the busy timeout.
    pub fn busy_timeout(mut self, timeout: Duration) -> Self {
        self.busy_timeout = timeout;
        self
    }

    /// Set the pool size.
    pub fn max_connections(mut self, connections: u32) -> Self {
        self.max_connections = connections.max(1);
        self
    }
}

/// A feed store in one SQLite file (WAL, `synchronous = FULL`). Sequence
/// numbers are assigned under `BEGIN IMMEDIATE`, the database's single write
/// lock, so it is not safe for several servers.
#[derive(Debug, Clone)]
pub struct SqliteFeedStore {
    pool: SqlitePool,
    store_id: i64,
}

impl SqliteFeedStore {
    /// Open the database at `path`, creating it (owner-only) and its schema
    /// when missing. [`FeedError::NewerSchema`](crate::FeedError::NewerSchema) when a newer release wrote it.
    pub async fn open(path: impl AsRef<Path>, options: &SqliteOptions) -> Result<Self> {
        let path = path.as_ref();
        create_private(path);
        let connect = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(options.busy_timeout);
        let pool = SqlitePoolOptions::new()
            .max_connections(options.max_connections.max(1))
            .connect_with(connect)
            .await
            .map_err(|e| backend(Dialect::Sqlite, e))?;
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

    /// Close the pool; later calls fail with [`FeedError::Unavailable`](crate::FeedError::Unavailable).
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

fn create_private(path: &Path) {
    let mut open = std::fs::OpenOptions::new();
    open.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
    let _ = open.open(path);
}

impl_feed_store!(
    SqliteFeedStore,
    sqlx::Sqlite,
    "BEGIN IMMEDIATE",
    Dialect::Sqlite,
    StoreCaps {
        backend: "sqlite",
        cluster_safe: false,
    }
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FeedError;

    #[cfg(unix)]
    #[tokio::test]
    async fn a_new_database_is_private_to_its_owner() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("feeds.db");
        let store = SqliteFeedStore::open(&path, &SqliteOptions::new())
            .await
            .unwrap();
        store.close().await;
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn a_store_that_cannot_be_opened_is_a_typed_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = SqliteFeedStore::open(dir.path().join("missing/feeds.db"), &SqliteOptions::new())
            .await
            .unwrap_err();
        assert!(
            matches!(err, FeedError::Backend(_) | FeedError::Unavailable(_)),
            "{err}"
        );
    }
}

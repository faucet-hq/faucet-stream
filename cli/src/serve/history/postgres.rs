//! Postgres-backed run history (`serve-history-postgres`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with SQLite via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{Dialect, Stmts, classify_backend_error_with_context, impl_sql_history};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

impl_sql_history!(PostgresHistory, sqlx::PgPool, "BEGIN");

impl PostgresHistory {
    /// Connect, create the schema if absent, and return the backend. `lease_ttl`
    /// and `instance_id` drive instance-fenced orphan recovery (#146 H7).
    pub async fn connect(
        url: &str,
        idem_retention: Duration,
        lease_ttl: Duration,
        instance_id: String,
    ) -> Result<Self, HistoryError> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| classify_backend_error_with_context("Postgres connection failed", e))?;
        migrate_serialized(&pool).await?;
        Ok(Self::from_parts(
            pool,
            idem_retention,
            lease_ttl,
            instance_id,
            Stmts::new(Dialect::Postgres),
        ))
    }
}

/// Advisory-lock key the migration holds ("faucet" in ASCII).
const MIGRATION_LOCK: i64 = 0x6661_7563_6574;

/// Run the migration under a session advisory lock, so instances starting
/// together take turns: Postgres does not make concurrent `CREATE TABLE IF NOT
/// EXISTS` of one table safe, and the loser would start degraded (#914).
async fn migrate_serialized(pool: &sqlx::PgPool) -> Result<(), HistoryError> {
    let mut lock = pool.acquire().await.map_err(|e| {
        classify_backend_error_with_context("taking the run-history migration lock", e)
    })?;
    sqlx::query("SET lock_timeout = '120s'")
        .execute(&mut *lock)
        .await
        .and(
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(MIGRATION_LOCK)
                .execute(&mut *lock)
                .await,
        )
        .map_err(|e| {
            classify_backend_error_with_context("taking the run-history migration lock", e)
        })?;
    let migrated = super::sql::migrate!(pool);
    // Ending the session releases its advisory lock, and the connection never
    // goes back to the pool still holding it.
    let _ = sqlx::Connection::close(lock.detach()).await;
    migrated
}

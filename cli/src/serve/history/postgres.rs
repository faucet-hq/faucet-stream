//! Postgres-backed run history (`serve-history-postgres`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with SQLite via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{DDL, Dialect, Stmts, classify_backend_error_with_context, impl_sql_history};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

impl_sql_history!(PostgresHistory, sqlx::PgPool);

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
        for stmt in DDL {
            sqlx::query(stmt).execute(&pool).await.map_err(|e| {
                classify_backend_error_with_context("creating run-history schema", e)
            })?;
        }
        Ok(Self::from_parts(
            pool,
            idem_retention,
            lease_ttl,
            instance_id,
            Stmts::new(Dialect::Postgres),
        ))
    }
}

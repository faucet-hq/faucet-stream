//! PostgreSQL-backed [`StateStore`].

use async_trait::async_trait;
use faucet_core::state::{DOCTOR_SENTINEL_KEY, StateStore, validate_state_key};
use faucet_core::util::quote_ident;
use faucet_core::{FaucetError, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// Default name for the state-store table. Override via
/// [`PostgresStateStore::connect_with`].
pub const DEFAULT_TABLE: &str = "faucet_state";

/// A `StateStore` that persists each entry as a row in a single Postgres
/// table.
///
/// The table layout is:
///
/// ```sql
/// CREATE TABLE faucet_state (
///     key        TEXT        PRIMARY KEY,
///     value      JSONB       NOT NULL,
///     updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
/// );
/// ```
///
/// Calling [`PostgresStateStore::ensure_table`] creates the table if it does
/// not exist; integrators that manage schema separately can skip this call.
pub struct PostgresStateStore {
    pool: PgPool,
    table: String,
}

impl PostgresStateStore {
    /// Connect to a PostgreSQL server with the default `faucet_state` table.
    pub async fn connect(connection_url: &str) -> Result<Self, FaucetError> {
        Self::connect_with(connection_url, 5, DEFAULT_TABLE).await
    }

    /// Connect with explicit pool size and table name.
    pub async fn connect_with(
        connection_url: &str,
        max_connections: u32,
        table: &str,
    ) -> Result<Self, FaucetError> {
        validate_table_name(table)?;
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(connection_url)
            .await
            .map_err(|e| {
                FaucetError::State(format!("PostgreSQL state-store connection failed: {e}"))
            })?;
        Ok(Self {
            pool,
            table: table.to_owned(),
        })
    }

    /// Construct from an existing pool. Useful when integrators already
    /// manage a shared `PgPool`.
    pub fn from_pool(pool: PgPool, table: impl Into<String>) -> Result<Self, FaucetError> {
        let table = table.into();
        validate_table_name(&table)?;
        Ok(Self { pool, table })
    }

    /// Returns the table name in use.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Create the state-store table if it does not already exist.
    pub async fn ensure_table(&self) -> Result<(), FaucetError> {
        let sql = create_table_sql(&self.table);
        sqlx::query(&sql).execute(&self.pool).await.map_err(|e| {
            FaucetError::State(format!("failed to ensure state table {}: {e}", self.table))
        })?;
        Ok(())
    }
}

/// SQL identifiers in faucet-stream must already be safe for `quote_ident`;
/// additionally restrict the table name to printable ASCII to keep error
/// messages readable.
pub(crate) fn validate_table_name(table: &str) -> Result<(), FaucetError> {
    if table.is_empty() {
        return Err(FaucetError::Config(
            "state-store table name must not be empty".into(),
        ));
    }
    if table.len() > 63 {
        return Err(FaucetError::Config(format!(
            "state-store table name '{table}' exceeds Postgres' 63-character identifier limit"
        )));
    }
    for (i, c) in table.char_indices() {
        let ok = c.is_ascii_alphanumeric() || c == '_';
        if !ok {
            return Err(FaucetError::Config(format!(
                "state-store table name '{table}' contains illegal character {c:?} at byte {i}"
            )));
        }
    }
    Ok(())
}

pub(crate) fn create_table_sql(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {table_ident} (\
            key TEXT PRIMARY KEY,\
            value JSONB NOT NULL,\
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()\
        )",
        table_ident = quote_ident(table)
    )
}

pub(crate) fn select_sql(table: &str) -> String {
    format!("SELECT value FROM {} WHERE key = $1", quote_ident(table))
}

pub(crate) fn upsert_sql(table: &str) -> String {
    format!(
        "INSERT INTO {tbl} (key, value, updated_at) VALUES ($1, $2, NOW()) \
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()",
        tbl = quote_ident(table)
    )
}

pub(crate) fn delete_sql(table: &str) -> String {
    format!("DELETE FROM {} WHERE key = $1", quote_ident(table))
}

pub(crate) fn list_sql(table: &str) -> String {
    format!(
        "SELECT key FROM {} WHERE left(key, char_length($1)) = $1 ORDER BY key",
        quote_ident(table)
    )
}

/// The single key of the envelope a value containing U+0000 is stored in.
const NUL_ENVELOPE_KEY: &str = "$faucet_nul_escaped_json";

fn holds_nul(v: &Value) -> bool {
    match v {
        Value::String(s) => s.contains('\0'),
        Value::Array(a) => a.iter().any(holds_nul),
        Value::Object(o) => o.iter().any(|(k, v)| k.contains('\0') || holds_nul(v)),
        _ => false,
    }
}

/// JSONB rejects U+0000, so a value holding it (an opaque cursor, a text
/// bookmark) is stored as its JSON text inside a one-key envelope, where the
/// NUL is the escape `\u0000`, not a character.
pub(crate) fn wrap_nul(value: &Value) -> Result<std::borrow::Cow<'_, Value>, FaucetError> {
    if !holds_nul(value) {
        return Ok(std::borrow::Cow::Borrowed(value));
    }
    let text = serde_json::to_string(value)
        .map_err(|e| FaucetError::State(format!("failed to serialize state: {e}")))?;
    Ok(std::borrow::Cow::Owned(
        serde_json::json!({ NUL_ENVELOPE_KEY: text }),
    ))
}

/// The inverse of [`wrap_nul`].
pub(crate) fn unwrap_nul(value: Value) -> Result<Value, FaucetError> {
    match &value {
        Value::Object(o) if o.len() == 1 => match o.get(NUL_ENVELOPE_KEY) {
            Some(Value::String(text)) => serde_json::from_str(text).map_err(|e| {
                FaucetError::State(format!("stored escaped state is not valid JSON: {e}"))
            }),
            _ => Ok(value),
        },
        _ => Ok(value),
    }
}

#[async_trait]
impl StateStore for PostgresStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        let row = sqlx::query(&select_sql(&self.table))
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::State(format!("Postgres SELECT for key '{key}' failed: {e}"))
            })?;
        match row {
            None => Ok(None),
            Some(r) => {
                let value: Value = r.try_get(0).map_err(|e| {
                    FaucetError::State(format!(
                        "failed to decode JSONB column for key '{key}': {e}"
                    ))
                })?;
                unwrap_nul(value).map(Some)
            }
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let stored = wrap_nul(value)?;
        sqlx::query(&upsert_sql(&self.table))
            .bind(key)
            .bind(stored.as_ref())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::State(format!("Postgres UPSERT for key '{key}' failed: {e}"))
            })?;
        tracing::debug!(key, table = %self.table, "state written to Postgres");
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        sqlx::query(&delete_sql(&self.table))
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::State(format!("Postgres DELETE for key '{key}' failed: {e}"))
            })?;
        Ok(())
    }

    fn supports_list(&self) -> bool {
        true
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        let rows = sqlx::query(&list_sql(&self.table))
            .bind(prefix)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::State(format!("Postgres key listing failed: {e}")))?;
        rows.iter()
            .map(|r| {
                r.try_get::<String, _>(0)
                    .map_err(|e| FaucetError::State(format!("failed to decode state key: {e}")))
            })
            .collect()
    }

    fn supports_atomic_batch(&self) -> bool {
        true
    }

    async fn put_batch(&self, entries: &[(String, Value)]) -> Result<(), FaucetError> {
        for (key, _) in entries {
            validate_state_key(key)?;
        }
        let err = |e: sqlx::Error| FaucetError::State(format!("Postgres batch UPSERT failed: {e}"));
        let mut tx = self.pool.begin().await.map_err(err)?;
        let sql = upsert_sql(&self.table);
        for (key, value) in entries {
            let stored = wrap_nul(value)?;
            sqlx::query(&sql)
                .bind(key)
                .bind(stored.as_ref())
                .execute(&mut *tx)
                .await
                .map_err(err)?;
        }
        tx.commit().await.map_err(err)?;
        Ok(())
    }

    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // Exercise the real upsert → select → delete cycle on a sentinel key.
        // This validates connectivity, auth, the table's existence, and
        // read/write permissions through the actual code path and leaves no
        // residue.
        let start = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.sentinel_roundtrip()).await {
            Ok(Ok(())) => Probe::pass("sentinel", start.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                e.to_string(),
                format!(
                    "verify the server is reachable, the credentials grant read/write access, \
                     and the '{}' table exists (call ensure_table or create it manually)",
                    self.table
                ),
            ),
            Err(_) => Probe::fail_hint(
                "sentinel",
                start.elapsed(),
                format!(
                    "round-trip timed out after {:?}; Postgres did not respond",
                    ctx.timeout
                ),
                "verify the server is reachable or raise the check timeout",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

impl PostgresStateStore {
    /// Write, read back, and delete a sentinel key — the body of the `check()`
    /// probe, factored out so the happy path stays linear. Reuses the store's
    /// own `put`/`get`/`delete` against the configured table.
    async fn sentinel_roundtrip(&self) -> Result<(), FaucetError> {
        let probe = serde_json::json!({ "faucet_doctor": true });
        self.put(DOCTOR_SENTINEL_KEY, &probe).await?;
        let got = self.get(DOCTOR_SENTINEL_KEY).await?;
        // Best-effort cleanup regardless of the read result.
        let _ = self.delete(DOCTOR_SENTINEL_KEY).await;
        match got {
            Some(v) if v == probe => Ok(()),
            _ => Err(FaucetError::State(
                "sentinel readback did not match what was written".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nul_values_round_trip_through_the_envelope() {
        let v = serde_json::json!({"cursor": "a\u{0}b", "n": [1, {"k\u{0}": 2}]});
        let wrapped = wrap_nul(&v).unwrap().into_owned();
        assert!(!holds_nul(&wrapped));
        assert_eq!(unwrap_nul(wrapped).unwrap(), v);
        let plain = serde_json::json!({"cursor": "ab"});
        assert!(matches!(
            wrap_nul(&plain).unwrap(),
            std::borrow::Cow::Borrowed(_)
        ));
        assert_eq!(unwrap_nul(plain.clone()).unwrap(), plain);
        let other = serde_json::json!({ NUL_ENVELOPE_KEY: 3 });
        assert_eq!(unwrap_nul(other.clone()).unwrap(), other);
        assert!(unwrap_nul(serde_json::json!({ NUL_ENVELOPE_KEY: "{" })).is_err());
    }

    #[test]
    fn validate_table_name_accepts_typical_values() {
        for t in [
            "faucet_state",
            "FaucetState",
            "state_v2",
            "f1",
            "abcdefghijklmnopqrstuvwxyz_0123456789_FOO",
        ] {
            validate_table_name(t).unwrap_or_else(|e| panic!("expected ok for {t:?}: {e}"));
        }
    }

    #[test]
    fn validate_table_name_rejects_empty() {
        let err = validate_table_name("").unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn validate_table_name_rejects_illegal_chars() {
        for t in [
            "table-name",
            "schema.table",
            "drop table users;--",
            "spaces in name",
        ] {
            let err = validate_table_name(t).expect_err(&format!("expected error for {t:?}"));
            assert!(matches!(err, FaucetError::Config(_)));
        }
    }

    #[test]
    fn validate_table_name_rejects_over_long() {
        let t = "a".repeat(64);
        assert!(validate_table_name(&t).is_err());
    }

    #[test]
    fn create_table_sql_quotes_identifier() {
        let sql = create_table_sql("faucet_state");
        assert!(sql.contains("\"faucet_state\""));
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS"));
        assert!(sql.contains("PRIMARY KEY"));
        assert!(sql.contains("JSONB"));
    }

    #[test]
    fn create_table_sql_escapes_embedded_quote() {
        // quote_ident escapes embedded quotes by doubling — verify the
        // identifier remains a single quoted token.
        let sql = create_table_sql("ab\"c");
        // The opening quote, doubled inner quote, closing quote — should
        // appear in the output regardless of whether the table-name
        // validator would normally allow it.
        assert!(sql.contains("\"ab\"\"c\""));
    }

    #[test]
    fn select_sql_uses_parameter_marker() {
        let sql = select_sql("faucet_state");
        assert_eq!(sql, "SELECT value FROM \"faucet_state\" WHERE key = $1");
    }

    #[test]
    fn upsert_sql_uses_on_conflict_do_update() {
        let sql = upsert_sql("faucet_state");
        assert!(sql.contains("INSERT INTO \"faucet_state\""));
        assert!(sql.contains("ON CONFLICT (key) DO UPDATE"));
        assert!(sql.contains("value = EXCLUDED.value"));
        assert!(sql.contains("updated_at = NOW()"));
    }

    #[test]
    fn list_sql_matches_a_literal_prefix() {
        let sql = list_sql("faucet_state");
        assert_eq!(
            sql,
            "SELECT key FROM \"faucet_state\" WHERE left(key, char_length($1)) = $1 ORDER BY key"
        );
    }

    #[test]
    fn delete_sql_uses_parameter_marker() {
        let sql = delete_sql("faucet_state");
        assert_eq!(sql, "DELETE FROM \"faucet_state\" WHERE key = $1");
    }

    #[tokio::test]
    async fn connect_rejects_invalid_table_name() {
        let result =
            PostgresStateStore::connect_with("postgres://localhost/does_not_matter", 5, "bad-name")
                .await;
        match result {
            Err(FaucetError::Config(_)) => {}
            Err(other) => panic!("expected Config error, got {other:?}"),
            Ok(_) => panic!("expected error, got Ok"),
        }
    }
}

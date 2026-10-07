//! Amazon Redshift query source implementation.
//!
//! Redshift is PostgreSQL wire-compatible, so the source streams rows through
//! `sqlx`'s Postgres cursor, re-framing them into `batch_size`-sized pages. It
//! supports full and incremental replication: the incremental cursor is pushed
//! down via a `${bookmark}` bind when present and always re-checked client-side.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

use async_trait::async_trait;
use faucet_core::replication::{filter_incremental, max_value};
use faucet_core::util::substitute_context_bind_params;
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::PgPool;

use crate::config::{RedshiftReplication, RedshiftSourceConfig};
use crate::convert::{bind_params, row_to_json};

/// A source that executes a SQL query against Amazon Redshift and returns rows
/// as JSON objects.
pub struct RedshiftSource {
    config: RedshiftSourceConfig,
    pool: PgPool,
    /// Bookmark applied via [`Source::apply_start_bookmark`] (incremental only).
    start_bookmark: Mutex<Option<Value>>,
}

/// Client-side incremental filter context (column + effective lower bound).
struct IncrementalCtx {
    column: String,
    start: Value,
}

impl RedshiftSource {
    /// Create a new source. Validates config and builds a lazily-connected pool
    /// (no I/O — connectivity is verified on first query or via
    /// [`Source::check`]).
    pub fn new(config: RedshiftSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool =
            faucet_common_redshift::build_pool_lazy(&config.connection, config.max_connections)?;
        Ok(Self {
            config,
            pool,
            start_bookmark: Mutex::new(None),
        })
    }

    /// The effective incremental start bookmark (persisted bookmark, else the
    /// configured `initial_value`), or `None` for full replication.
    fn incremental_ctx(&self) -> Option<IncrementalCtx> {
        match &self.config.replication {
            RedshiftReplication::Full => None,
            RedshiftReplication::Incremental {
                column,
                initial_value,
            } => {
                let start = self
                    .start_bookmark
                    .lock()
                    .expect("start_bookmark mutex poisoned")
                    .clone()
                    .unwrap_or_else(|| initial_value.clone());
                Some(IncrementalCtx {
                    column: column.clone(),
                    start,
                })
            }
        }
    }

    /// Build the effective SQL and ordered positional bind values for a parent
    /// context + optional incremental cursor.
    ///
    /// Bind order is: static [`config.params`](RedshiftSourceConfig::params),
    /// then parent-context `{key}` values, then the `${bookmark}` value (when
    /// the query contains that token).
    fn resolve_query(
        &self,
        context: &HashMap<String, Value>,
        incr: Option<&IncrementalCtx>,
    ) -> (String, Vec<Value>) {
        let mut binds = self.config.params.clone();
        let mut sql = self.config.query.clone();

        if !context.is_empty() {
            let (rewritten, ctx_values) =
                substitute_context_bind_params(&sql, context, binds.len() + 1, |i| format!("${i}"));
            sql = rewritten;
            binds.extend(ctx_values);
        }

        if let Some(ctx) = incr
            && sql.contains("${bookmark}")
        {
            let marker = format!("${}", binds.len() + 1);
            sql = sql.replace("${bookmark}", &marker);
            binds.push(ctx.start.clone());
        }

        (sql, binds)
    }
}

/// Derive a stable state key from the host, database, and query.
fn default_state_key(config: &RedshiftSourceConfig) -> String {
    // Fixed FNV-1a: the key is durable and `DefaultHasher` is not stable across Rust releases.
    let material = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}",
        config.connection.host, config.connection.port, config.connection.database, config.query
    );
    format!(
        "redshift:{:016x}",
        faucet_core::shard::shard_hash(&material)
    )
}

/// Apply the client-side incremental filter to a page (no-op for full runs).
fn apply_incr_filter(page: Vec<Value>, incr: Option<&IncrementalCtx>) -> Vec<Value> {
    match incr {
        Some(ic) => filter_incremental(page, &ic.column, &ic.start),
        None => page,
    }
}

#[async_trait]
impl Source for RedshiftSource {
    fn connector_name(&self) -> &'static str {
        "redshift"
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(RedshiftSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "redshift://{}:{}/{}?query={}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.connection.database,
            self.config.query
        )
    }

    fn state_key(&self) -> Option<String> {
        match &self.config.replication {
            RedshiftReplication::Full => None,
            RedshiftReplication::Incremental { .. } => Some(
                self.config
                    .state_key
                    .clone()
                    .unwrap_or_else(|| default_state_key(&self.config)),
            ),
        }
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self
            .start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned") = Some(bookmark);
        Ok(())
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut out = Vec::new();
        let mut s = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = s.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    /// Stream rows from the `sqlx` cursor without buffering the full result set.
    /// Each emitted [`StreamPage`] holds up to
    /// [`RedshiftSourceConfig::batch_size`] rows.
    ///
    /// For incremental replication the running maximum of the cursor column is
    /// tracked over the *full* scan (before the client-side filter) and emitted
    /// as the bookmark on the final page.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let incr = self.incremental_ctx();
            let (query_str, binds) = self.resolve_query(context, incr.as_ref());
            let query = bind_params(sqlx::query(&query_str), &binds)?;

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let cap = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(cap);
            // Seeded with the start bookmark so it can never move backwards.
            let mut running_max: Option<Value> = incr.as_ref().map(|ic| ic.start.clone());
            let mut total = 0usize;

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| FaucetError::Source(format!("redshift query failed: {e}")))?
            {
                let obj = row_to_json(&row)?;
                // Track the running max BEFORE the client-side filter so the
                // persisted bookmark reflects the full scan.
                if let Some(ic) = &incr
                    && let Some(v) = obj.get(&ic.column)
                {
                    running_max = Some(match running_max.take() {
                        Some(m) => max_value(m, v.clone()),
                        None => v.clone(),
                    });
                }
                buffer.push(obj);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                    let kept = apply_incr_filter(page, incr.as_ref());
                    total += kept.len();
                    if !kept.is_empty() {
                        yield StreamPage { records: kept, bookmark: None };
                    }
                }
            }

            // Final page carries the new bookmark (incremental only).
            let kept = apply_incr_filter(buffer, incr.as_ref());
            total += kept.len();
            let bookmark = if incr.is_some() { running_max } else { None };
            if !kept.is_empty() || bookmark.is_some() {
                yield StreamPage { records: kept, bookmark };
            }

            tracing::info!(
                rows = total,
                batch_size,
                query = %self.config.query,
                "Redshift source stream complete",
            );
        })
    }

    /// Preflight probe for `faucet doctor`: acquire a connection and run
    /// `SELECT 1` (non-scanning — never executes the configured query).
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe =
            match tokio::time::timeout(ctx.timeout, sqlx::query("SELECT 1").execute(&self.pool))
                .await
            {
                Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    e.to_string(),
                    "check host/port/database/user/credentials and that the cluster is reachable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check host/port/database/user/credentials and that the cluster is reachable",
                ),
            };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RedshiftReplication;
    use faucet_common_redshift::RedshiftConnection;
    use serde_json::json;

    fn base_config() -> RedshiftSourceConfig {
        RedshiftSourceConfig {
            connection: RedshiftConnection::new("host", "db", "user", "pw"),
            query: "SELECT * FROM t".into(),
            params: Vec::new(),
            max_connections: 10,
            batch_size: 1000,
            replication: RedshiftReplication::Full,
            state_key: None,
        }
    }

    fn source(c: RedshiftSourceConfig) -> RedshiftSource {
        RedshiftSource::new(c).unwrap()
    }

    #[test]
    fn new_rejects_invalid_batch_size() {
        let mut c = base_config();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match RedshiftSource::new(c) {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            _ => panic!("expected a batch_size Config error"),
        }
    }

    #[test]
    fn new_surfaces_unsupported_credentials() {
        let mut c = base_config();
        c.connection.credentials = faucet_common_redshift::RedshiftCredentials::Iam {
            region: None,
            cluster_identifier: None,
            db_user: None,
        };
        assert!(matches!(
            RedshiftSource::new(c),
            Err(FaucetError::Config(_))
        ));
    }

    #[tokio::test]
    async fn connector_name_is_redshift() {
        assert_eq!(source(base_config()).connector_name(), "redshift");
    }

    #[tokio::test]
    async fn dataset_uri_has_host_db_and_query() {
        let s = source(base_config());
        assert_eq!(
            s.dataset_uri(),
            "redshift://host:5439/db?query=SELECT * FROM t"
        );
    }

    #[tokio::test]
    async fn config_schema_reports_required_fields() {
        let s = source(base_config());
        let schema = s.config_schema();
        assert!(schema["properties"]["query"].is_object());
        let required = schema["required"].as_array().expect("required array");
        assert!(required.iter().any(|v| v == "query"));
    }

    #[tokio::test]
    async fn full_mode_has_no_state_key() {
        assert!(source(base_config()).state_key().is_none());
    }

    #[tokio::test]
    async fn incremental_state_key_derived_and_stable() {
        let mut c = base_config();
        c.replication = RedshiftReplication::Incremental {
            column: "ts".into(),
            initial_value: json!("2026-01-01"),
        };
        let k1 = source(c.clone()).state_key().unwrap();
        let k2 = source(c).state_key().unwrap();
        assert_eq!(k1, k2);
        assert!(k1.starts_with("redshift:"));
        let c = base_config();
        let material = format!(
            "{}\u{1f}{}\u{1f}{}\u{1f}{}",
            c.connection.host, c.connection.port, c.connection.database, c.query
        );
        assert_eq!(
            k1,
            format!(
                "redshift:{:016x}",
                faucet_core::shard::shard_hash(&material)
            )
        );
    }

    #[tokio::test]
    async fn explicit_state_key_wins_and_bookmark_overrides_initial() {
        let mut c = base_config();
        c.state_key = Some("my-key".into());
        c.replication = RedshiftReplication::Incremental {
            column: "ts".into(),
            initial_value: json!("2026-01-01"),
        };
        let s = source(c);
        assert_eq!(s.state_key().as_deref(), Some("my-key"));
        s.apply_start_bookmark(json!("2026-06-01")).await.unwrap();
        assert_eq!(s.incremental_ctx().unwrap().start, json!("2026-06-01"));
    }

    #[tokio::test]
    async fn resolve_query_no_context_no_incremental_is_verbatim() {
        let mut c = base_config();
        c.params = vec![json!(7)];
        let s = source(c);
        let (sql, binds) = s.resolve_query(&HashMap::new(), None);
        assert_eq!(sql, "SELECT * FROM t");
        assert_eq!(binds, vec![json!(7)]);
    }

    #[tokio::test]
    async fn resolve_query_substitutes_context_positionally() {
        let mut c = base_config();
        c.query = "SELECT * FROM t WHERE id = {parent.id}".into();
        let s = source(c);
        let mut ctx = HashMap::new();
        ctx.insert("parent.id".to_string(), json!(42));
        let (sql, binds) = s.resolve_query(&ctx, None);
        assert_eq!(sql, "SELECT * FROM t WHERE id = $1");
        assert_eq!(binds, vec![json!(42)]);
    }

    #[tokio::test]
    async fn resolve_query_pushes_down_bookmark_after_params_and_context() {
        let mut c = base_config();
        c.params = vec![json!(1)];
        c.query = "SELECT * FROM t WHERE tenant = {p.t} AND ts > ${bookmark}".into();
        c.replication = RedshiftReplication::Incremental {
            column: "ts".into(),
            initial_value: json!("2026-01-01"),
        };
        let s = source(c);
        let incr = s.incremental_ctx();
        let mut ctx = HashMap::new();
        ctx.insert("p.t".to_string(), json!("acme"));
        let (sql, binds) = s.resolve_query(&ctx, incr.as_ref());
        // $1 = static param, $2 = context value, $3 = bookmark.
        assert_eq!(sql, "SELECT * FROM t WHERE tenant = $2 AND ts > $3");
        assert_eq!(binds, vec![json!(1), json!("acme"), json!("2026-01-01")]);
    }

    #[test]
    fn apply_incr_filter_drops_records_at_or_below_start() {
        let page = vec![
            json!({"id": 1, "ts": "2026-01-01"}),
            json!({"id": 2, "ts": "2026-06-01"}),
        ];
        let ic = IncrementalCtx {
            column: "ts".into(),
            start: json!("2026-01-01"),
        };
        let kept = apply_incr_filter(page, Some(&ic));
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0]["id"], 2);
    }

    #[test]
    fn apply_incr_filter_is_noop_for_full_mode() {
        let page = vec![json!({"id": 1})];
        assert_eq!(apply_incr_filter(page.clone(), None), page);
    }
}

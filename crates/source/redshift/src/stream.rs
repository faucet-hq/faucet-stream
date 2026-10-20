//! Amazon Redshift query source implementation.
//!
//! Redshift is PostgreSQL wire-compatible, so the source streams rows through
//! `sqlx`'s Postgres cursor, re-framing them into `batch_size`-sized pages. It
//! supports full and incremental replication: an incremental read wraps the
//! query in a `>=` cursor filter ordered by the cursor and drops the rows it
//! already emitted at the bookmark value (`CursorTracker`).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

use async_trait::async_trait;
use faucet_core::replication::cursor::{CursorBookmark, CursorTracker};
use faucet_core::util::{quote_ident, substitute_context_bind_params};
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use serde_json::Value;
use sqlx::PgPool;

use crate::config::{BOOKMARK_TOKEN, RedshiftReplication, RedshiftSourceConfig};
use crate::convert::{bind_params, row_to_json};

/// A source that executes a SQL query against Amazon Redshift and returns rows
/// as JSON objects.
pub struct RedshiftSource {
    config: RedshiftSourceConfig,
    pool: PgPool,
    /// Bookmark applied via [`Source::apply_start_bookmark`] (incremental only).
    start_bookmark: Mutex<Option<CursorBookmark>>,
    /// Backend-call context (#880): the query and every page of rows run
    /// through it.
    calls: faucet_core::CallSlot,
}

/// Incremental-read context (column + starting position).
struct IncrementalCtx {
    column: String,
    start: CursorBookmark,
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
            calls: faucet_core::CallSlot::new(faucet_core::CallContext::new().without_timeout()),
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
                    .unwrap_or_else(|| CursorBookmark::initial(initial_value.clone()));
                Some(IncrementalCtx {
                    column: column.clone(),
                    start,
                })
            }
        }
    }

    /// Build the effective SQL and ordered positional bind values for a parent
    /// context + optional incremental cursor, whose bookmark binds as
    /// `CAST($n AS <cast>)` (the cursor column's type).
    ///
    /// Bind order is: static [`config.params`](RedshiftSourceConfig::params),
    /// then parent-context `{key}` values, then the `${bookmark}` value (when
    /// the query contains that token), then the bookmark of the outer
    /// `column >= …` filter.
    fn resolve_query(
        &self,
        context: &HashMap<String, Value>,
        incr: Option<&IncrementalCtx>,
        cast: &str,
    ) -> (String, Vec<Value>) {
        let mut binds = self.config.params.clone();
        let mut sql = self.config.query.clone();

        if !context.is_empty() {
            let (rewritten, ctx_values) =
                substitute_context_bind_params(&sql, context, binds.len() + 1, |i| format!("${i}"));
            sql = rewritten;
            binds.extend(ctx_values);
        }

        let Some(ctx) = incr else {
            return (sql, binds);
        };
        if sql.contains(BOOKMARK_TOKEN) {
            let marker = format!("CAST(${} AS {cast})", binds.len() + 1);
            sql = sql.replace(BOOKMARK_TOKEN, &marker);
            binds.push(ctx.start.value.clone());
        }
        binds.push(ctx.start.value.clone());
        let c = quote_ident(&ctx.column);
        let inner = sql.trim_end_matches(|ch: char| ch.is_whitespace() || ch == ';');
        (
            format!(
                "SELECT * FROM ({inner}\n) AS q WHERE q.{c} >= CAST(${n} AS {cast}) ORDER BY q.{c}",
                n = binds.len()
            ),
            binds,
        )
    }

    /// The cursor column's type, as the server describes the query's output.
    async fn cursor_cast(
        &self,
        context: &HashMap<String, Value>,
        column: &str,
    ) -> Result<String, FaucetError> {
        use sqlx::Executor;
        let (probe, _) = self.resolve_query(context, None, "");
        let probe = probe.replace(BOOKMARK_TOKEN, "NULL");
        let calls = self.calls.get();
        let described = faucet_common_redshift::calls::once(&calls, "describe", async {
            (&self.pool).describe(&probe).await.map_err(|e| {
                FaucetError::Source(format!(
                    "redshift: could not describe the incremental query: {e}"
                ))
            })
        })
        .await?;
        let name = described
            .columns()
            .iter()
            .find(|c| sqlx::Column::name(*c) == column)
            .map(|c| sqlx::TypeInfo::name(sqlx::Column::type_info(c)).to_string())
            .ok_or_else(|| {
                FaucetError::Config(format!(
                    "redshift: incremental replication `column` {column:?} is not a column of \
                     the query's output"
                ))
            })?;
        cast_type(&name)
    }
}

/// A server-reported type name usable in `CAST(… AS <name>)`.
fn cast_type(name: &str) -> Result<String, FaucetError> {
    let ok = !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '"' | '[' | ']'));
    if ok {
        Ok(name.to_string())
    } else {
        Err(FaucetError::Config(format!(
            "redshift: the incremental cursor column has type {name:?}, which cannot be \
             compared to a bookmark; cast it in the query"
        )))
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

#[async_trait]
impl Source for RedshiftSource {
    fn connector_name(&self) -> &'static str {
        "redshift"
    }

    fn set_call_context(&self, ctx: faucet_core::io::CallContext) {
        self.calls.install(ctx);
    }

    fn set_roundtrip_recorder(
        &self,
        recorder: std::sync::Arc<faucet_core::observability::RoundtripRecorder>,
    ) {
        self.calls.install_recorder(recorder);
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
        let start = CursorBookmark::from_state(bookmark)?;
        *self
            .start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned") = Some(start);
        Ok(())
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.fetch_with_context_incremental(context).await?.0)
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        use futures::StreamExt;
        let mut records = Vec::new();
        let mut bookmark = None;
        let mut s = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = s.next().await {
            let page = page?;
            records.extend(page.records);
            bookmark = page.bookmark.or(bookmark);
        }
        Ok((records, bookmark))
    }

    /// Stream rows from the `sqlx` cursor without buffering the full result set.
    /// Each emitted [`StreamPage`] holds up to
    /// [`RedshiftSourceConfig::batch_size`] rows.
    ///
    /// For incremental replication rows arrive in cursor order, the rows
    /// already emitted at the bookmark value are dropped, and every page
    /// carries the bookmark after its last row (the final page carries one even
    /// when empty).
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let incr = self.incremental_ctx();
            let cast = match &incr {
                Some(ic) => self.cursor_cast(context, &ic.column).await?,
                None => String::new(),
            };
            let (query_str, binds) = self.resolve_query(context, incr.as_ref(), &cast);
            let mut tracker = incr
                .as_ref()
                .map(|ic| CursorTracker::new(ic.column.clone(), ic.start.clone()));
            let query = bind_params(sqlx::query(&query_str), &binds)?;

            let mut rows = query.fetch(&self.pool);
            let calls = self.calls.get();
            calls.record("query");
            let reads = calls.with_roundtrip_metering(false);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let cap = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(cap);
            let mut total = 0usize;

            loop {
            let fetched = faucet_common_redshift::calls::next_rows(
                &reads,
                "fetch",
                &mut rows,
                chunk,
                |e| FaucetError::Source(format!("redshift query failed: {e}")),
            )
            .await?;
            let exhausted = fetched.len() < chunk;
            for row in fetched {
                let obj = row_to_json(&row)?;
                if tracker.as_mut().is_none_or(|t| t.admit(&obj)) {
                    buffer.push(obj);
                }
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                    total += page.len();
                    let bookmark = tracker.as_ref().map(|t| t.bookmark().to_value());
                    yield StreamPage { records: page, bookmark };
                }
            }
            if exhausted {
                break;
            }
            }

            if !buffer.is_empty() || tracker.is_some() {
                total += buffer.len();
                let bookmark = tracker.as_ref().map(|t| t.bookmark().to_value());
                yield StreamPage { records: buffer, bookmark };
            }
            if let Some(missing) = tracker.as_ref().map(CursorTracker::missing).filter(|n| *n > 0) {
                tracing::warn!(
                    missing,
                    "Redshift incremental read: rows without a cursor value were emitted \
                     without moving the bookmark"
                );
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
        let calls = self.calls.get();
        let ping = faucet_common_redshift::calls::once(&calls, "query", async {
            sqlx::query("SELECT 1")
                .execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Source(e.to_string()))
        });
        let probe = match tokio::time::timeout(ctx.timeout, ping).await {
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
        assert_eq!(
            s.incremental_ctx().unwrap().start.value,
            json!("2026-06-01")
        );
        assert!(
            s.apply_start_bookmark(json!({"value": null}))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn resolve_query_no_context_no_incremental_is_verbatim() {
        let mut c = base_config();
        c.params = vec![json!(7)];
        let s = source(c);
        let (sql, binds) = s.resolve_query(&HashMap::new(), None, "");
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
        let (sql, binds) = s.resolve_query(&ctx, None, "");
        assert_eq!(sql, "SELECT * FROM t WHERE id = $1");
        assert_eq!(binds, vec![json!(42)]);
    }

    #[tokio::test]
    async fn resolve_query_pushes_down_bookmark_after_params_and_context() {
        let mut c = base_config();
        c.params = vec![json!(1)];
        c.query = "SELECT * FROM t WHERE tenant = {p.t} AND ts >= ${bookmark};".into();
        c.replication = RedshiftReplication::Incremental {
            column: "ts".into(),
            initial_value: json!("2026-01-01"),
        };
        let s = source(c);
        let incr = s.incremental_ctx();
        let mut ctx = HashMap::new();
        ctx.insert("p.t".to_string(), json!("acme"));
        let (sql, binds) = s.resolve_query(&ctx, incr.as_ref(), "TIMESTAMP");
        // $1 = static param, $2 = context value, $3 = bookmark, $4 = outer filter.
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM t WHERE tenant = $2 AND ts >= CAST($3 AS TIMESTAMP)\n) \
             AS q WHERE q.\"ts\" >= CAST($4 AS TIMESTAMP) ORDER BY q.\"ts\""
        );
        assert_eq!(
            binds,
            vec![
                json!(1),
                json!("acme"),
                json!("2026-01-01"),
                json!("2026-01-01")
            ]
        );
    }

    #[test]
    fn cast_type_accepts_type_names_only() {
        assert_eq!(cast_type("TIMESTAMPTZ").unwrap(), "TIMESTAMPTZ");
        assert_eq!(cast_type("character varying").unwrap(), "character varying");
        assert!(cast_type("INT4); DROP TABLE t; --").is_err());
        assert!(cast_type("").is_err());
    }
}

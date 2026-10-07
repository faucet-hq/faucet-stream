//! The MSSQL [`Source`] implementation — connection pool, query execution,
//! streaming, and incremental-replication bookkeeping.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::replication::{filter_incremental, max_replication_value, max_value};
use faucet_core::shard::{
    PkShardBounds, ShardSpec, parse_pk_shard, pk_bounds_query, pk_shards_from_bounds,
};
use faucet_core::{FaucetError, Source, StreamPage};
use futures::{Stream, TryStreamExt};
use serde_json::Value;
use tiberius::{QueryItem, ToSql};

use faucet_common_mssql::{MssqlPool, build_pool, quote_ident_mssql, with_statement_timeout};

use crate::config::{MssqlReplication, MssqlSourceConfig};
use crate::convert::row_to_json;

/// Microsoft SQL Server query source.
pub struct MssqlSource {
    config: MssqlSourceConfig,
    pool: MssqlPool,
    /// Bookmark loaded via [`Source::apply_start_bookmark`]; overrides the
    /// configured `initial_value` for incremental runs.
    start_bookmark: Mutex<Option<Value>>,
    /// Shard applied by the cluster coordinator (Mode B), if any. `None` (or the
    /// whole-dataset shard) means the full query is streamed. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<PkShardBounds>>,
}

impl MssqlSource {
    /// Connect, validate the config, and build the connection pool.
    pub async fn new(config: MssqlSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let pool = build_pool(&config.connection, config.max_connections).await?;
        Ok(Self {
            config,
            pool,
            start_bookmark: Mutex::new(None),
            applied_shard: Mutex::new(None),
        })
    }

    fn timeout(&self) -> Option<Duration> {
        match self.config.statement_timeout_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        }
    }

    fn current_start(&self) -> Option<Value> {
        self.start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned")
            .clone()
    }

    /// Apply the currently-set shard (if any) to a resolved query string. The
    /// positional `@Pn` bind markers inside the wrapped subquery are unaffected
    /// (they bind by name, not by position in the text).
    fn shard_wrap(&self, query: String) -> String {
        match &*self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(bounds) => bounds.wrap(&query, bracket_quote),
            None => query,
        }
    }
}

/// Infallible bracket quoting for a shard key whose NUL-freeness was already
/// validated (via [`quote_ident_mssql`]) when the shard was applied/enumerated.
/// Interior `]` are doubled per T-SQL rules, preventing identifier injection.
fn bracket_quote(name: &str) -> String {
    format!("[{}]", name.replace(']', "]]"))
}

/// Incremental-replication context resolved for one run.
#[derive(Debug, Clone, PartialEq)]
struct IncrementalCtx {
    column: String,
    start: Value,
}

/// Build the final query string, the ordered bind values, and (for incremental
/// runs) the client-side filter context.
///
/// Pure function (no pool) so it is unit-testable. Param order is:
/// `config.params` → context-substituted values → the incremental bookmark
/// (only when the query contains the `@bookmark` token).
fn build_query_and_params(
    config: &MssqlSourceConfig,
    context: &HashMap<String, Value>,
    start_bookmark: Option<&Value>,
) -> (String, Vec<Value>, Option<IncrementalCtx>) {
    // Resolve parent-context placeholders to positional @P markers.
    let (mut query, mut values) = if context.is_empty() {
        (config.query.clone(), config.params.clone())
    } else {
        let (q, ctx_values) = faucet_core::util::substitute_context_bind_params(
            &config.query,
            context,
            config.params.len() + 1,
            |i| format!("@P{i}"),
        );
        let mut v = config.params.clone();
        v.extend(ctx_values);
        (q, v)
    };

    let incremental = match &config.replication {
        MssqlReplication::Full => None,
        MssqlReplication::Incremental {
            column,
            initial_value,
        } => {
            let start = start_bookmark
                .cloned()
                .unwrap_or_else(|| initial_value.clone());
            // Server-side pushdown: bind the cursor where the user wrote
            // `@bookmark`. If absent, only the client-side filter applies.
            if query.contains("@bookmark") {
                let idx = values.len() + 1;
                query = query.replace("@bookmark", &format!("@P{idx}"));
                values.push(start.clone());
            }
            Some(IncrementalCtx {
                column: column.clone(),
                start,
            })
        }
    };

    (query, values, incremental)
}

/// Owned bind parameter, so the borrowed `&dyn ToSql` slice handed to
/// `tiberius` outlives nothing it shouldn't.
enum OwnedParam {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    Null(Option<i32>),
}

impl OwnedParam {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::String(s) => OwnedParam::Str(s.clone()),
            Value::Number(n) if n.is_i64() => OwnedParam::I64(n.as_i64().unwrap()),
            // A `u64` above `i64::MAX` cannot be represented as a tiberius `i64`
            // bind value (SQL Server has no unsigned 64-bit type). The previous
            // `as i64` wrapped it to a negative number (e.g. u64::MAX -> -1),
            // silently matching the wrong rows. Bind the exact decimal digits as
            // a string instead — SQL Server implicitly converts it to the
            // column's numeric type for comparison, preserving the value (F41).
            Value::Number(n) if n.is_u64() => OwnedParam::Str(n.as_u64().unwrap().to_string()),
            Value::Number(n) => OwnedParam::F64(n.as_f64().unwrap_or(0.0)),
            Value::Bool(b) => OwnedParam::Bool(*b),
            Value::Null => OwnedParam::Null(None),
            other => OwnedParam::Str(other.to_string()),
        }
    }

    fn as_tosql(&self) -> &dyn ToSql {
        match self {
            OwnedParam::I64(v) => v,
            OwnedParam::F64(v) => v,
            OwnedParam::Bool(v) => v,
            OwnedParam::Str(v) => v,
            OwnedParam::Null(v) => v,
        }
    }
}

/// One flattened `INFORMATION_SCHEMA.COLUMNS` row used by
/// [`Source::discover`]: `(schema, table, column, data_type, is_nullable)`.
type CatalogRow = (String, String, String, String, bool);

/// Group flattened catalog rows (ordered by schema, table, ordinal position)
/// into one [`DatasetDescriptor`](faucet_core::DatasetDescriptor) per table,
/// merging in per-table row estimates keyed by `(schema, table)`. Pure —
/// unit-testable without a live server. `quote` is the dialect's identifier
/// quoter ([`bracket_quote`] for T-SQL).
fn descriptors_from_catalog(
    rows: Vec<CatalogRow>,
    estimates: &HashMap<(String, String), u64>,
    quote: fn(&str) -> String,
) -> Vec<faucet_core::DatasetDescriptor> {
    /// In-progress per-table accumulator: `(schema, table, columns)`.
    type PendingTable = (String, String, Vec<(String, Value)>);

    let mut out: Vec<faucet_core::DatasetDescriptor> = Vec::new();
    let mut current: Option<PendingTable> = None;

    let flush = |cur: Option<PendingTable>, out: &mut Vec<faucet_core::DatasetDescriptor>| {
        if let Some((schema, table, cols)) = cur {
            let est = estimates.get(&(schema.clone(), table.clone())).copied();
            let query = format!("SELECT * FROM {}.{}", quote(&schema), quote(&table));
            let mut d = faucet_core::DatasetDescriptor::new(
                format!("{schema}.{table}"),
                "table",
                serde_json::json!({ "query": query }),
            )
            .with_schema(faucet_core::columns_to_schema(cols));
            if let Some(n) = est {
                d = d.with_estimated_rows(n);
            }
            out.push(d);
        }
    };

    for (schema, table, column, data_type, is_nullable) in rows {
        let same = current
            .as_ref()
            .is_some_and(|(s, t, _)| *s == schema && *t == table);
        if !same {
            flush(current.take(), &mut out);
            current = Some((schema, table, Vec::new()));
        }
        let mut fragment = faucet_core::sql_type_to_json_schema(&data_type);
        if is_nullable {
            fragment = faucet_core::nullable_type(fragment);
        }
        if let Some((_, _, cols)) = current.as_mut() {
            cols.push((column, fragment));
        }
    }
    flush(current, &mut out);
    out
}

/// Derive a default state-store key from the connection host + a query
/// fingerprint, stable across runs.
fn default_state_key(config: &MssqlSourceConfig) -> String {
    let host = config
        .connection
        .connection_url
        .as_deref()
        .and_then(|u| url::Url::parse(u).ok())
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_else(|| "mssql".to_string());

    // Fixed FNV-1a: the key is durable and `DefaultHasher` is not stable across Rust releases.
    let fingerprint = faucet_core::shard::shard_hash(&config.query);
    // Host may contain dots (allowed mid-key); sanitise anything else.
    let host: String = host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("mssql:{host}:{fingerprint:016x}")
}

#[async_trait]
impl Source for MssqlSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.collect_all(context).await?.0)
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.collect_all(context).await
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let cap = if batch_size == 0 { 1024 } else { batch_size };
        let start = self.current_start();
        let (query, values, incr) = build_query_and_params(&self.config, context, start.as_ref());
        let query = self.shard_wrap(query);

        Box::pin(async_stream::try_stream! {
            let mut conn = self
                .pool
                .get()
                .await
                .map_err(|e| FaucetError::Source(format!("MSSQL pool checkout failed: {e}")))?;

            // Scope the borrowed param slice to the query() call — the
            // QueryStream borrows the connection, not the params.
            let mut stream = {
                let owned: Vec<OwnedParam> = values.iter().map(OwnedParam::from_value).collect();
                let refs: Vec<&dyn ToSql> = owned.iter().map(OwnedParam::as_tosql).collect();
                let query_fut = conn.query(&query, &refs);
                match self.timeout() {
                    Some(t) => {
                        with_statement_timeout(t, async {
                            query_fut.await.map_err(|e| {
                                FaucetError::Source(format!("MSSQL query failed: {e}"))
                            })
                        }, || FaucetError::Source("MSSQL query timed out".into()))
                        .await?
                    }
                    None => query_fut
                        .await
                        .map_err(|e| FaucetError::Source(format!("MSSQL query failed: {e}")))?,
                }
            };

            let mut buffer: Vec<Value> = Vec::with_capacity(cap);
            let mut running_max: Option<Value> = None;
            let mut total = 0usize;

            while let Some(item) = stream
                .try_next()
                .await
                .map_err(|e| FaucetError::Source(format!("MSSQL row stream failed: {e}")))?
            {
                let QueryItem::Row(row) = item else { continue };
                buffer.push(row_to_json(&row)?);
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(cap));
                    let kept = apply_incremental(page, incr.as_ref(), &mut running_max);
                    total += kept.len();
                    if !kept.is_empty() {
                        yield StreamPage { records: kept, bookmark: None };
                    }
                }
            }

            // Final page carries the bookmark so the pipeline persists only
            // after everything before it has been written.
            let kept = apply_incremental(buffer, incr.as_ref(), &mut running_max);
            total += kept.len();
            let bookmark = if incr.is_some() { running_max.clone() } else { None };
            if !kept.is_empty() || bookmark.is_some() {
                yield StreamPage { records: kept, bookmark };
            }

            tracing::info!(rows = total, query = %self.config.query, "MSSQL source stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(MssqlSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "mssql"
    }

    fn dataset_uri(&self) -> String {
        let conn = self
            .config
            .connection
            .connection_url
            .as_deref()
            .or(self.config.connection.connection_string.as_deref())
            .unwrap_or("");
        format!(
            "{}?query={}",
            faucet_core::redact_uri_credentials(conn),
            self.config.query
        )
    }

    fn state_key(&self) -> Option<String> {
        match &self.config.replication {
            MssqlReplication::Full => None,
            MssqlReplication::Incremental { .. } => Some(
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

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.pool.get()).await {
            Ok(Ok(_conn)) => Probe::pass("connect", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "connect",
                started.elapsed(),
                e.to_string(),
                "check connection_url / credentials / TLS / that the server is reachable",
            ),
            Err(_) => Probe::fail_hint(
                "connect",
                started.elapsed(),
                "timed out",
                "check connection_url / credentials / TLS / that the server is reachable",
            ),
        };
        Ok(CheckReport::single(probe))
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate every base table visible to the connection, with column
    /// types from `INFORMATION_SCHEMA.COLUMNS` and a row estimate from
    /// `sys.partitions` (catalog metadata only — no data scan). The estimate
    /// query degrades gracefully: when the `sys.*` views aren't readable
    /// (permissions), tables are returned without estimates rather than
    /// failing discovery.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        const CATALOG_SQL: &str = "SELECT c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME, \
                c.DATA_TYPE, c.IS_NULLABLE \
           FROM INFORMATION_SCHEMA.COLUMNS c \
           JOIN INFORMATION_SCHEMA.TABLES t \
             ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
          WHERE t.TABLE_TYPE = 'BASE TABLE' \
          ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION";
        // Heap (index_id 0) or clustered-index (index_id 1) partitions carry
        // the table's row count; secondary indexes would double-count.
        const ESTIMATE_SQL: &str = "SELECT s.name AS sch, o.name AS tbl, SUM(p.rows) AS est \
           FROM sys.objects o \
           JOIN sys.schemas s ON s.schema_id = o.schema_id \
           JOIN sys.partitions p ON p.object_id = o.object_id AND p.index_id IN (0, 1) \
          WHERE o.type = 'U' \
          GROUP BY s.name, o.name";

        let map_err = |e: tiberius::error::Error| {
            FaucetError::Source(format!("mssql: catalog discovery failed: {e}"))
        };

        let mut conn =
            self.pool.get().await.map_err(|e| {
                FaucetError::Source(format!("mssql: catalog discovery failed: {e}"))
            })?;

        let rows = {
            let run = async {
                conn.query(CATALOG_SQL, &[])
                    .await
                    .map_err(map_err)?
                    .into_first_result()
                    .await
                    .map_err(map_err)
            };
            match self.timeout() {
                Some(t) => {
                    with_statement_timeout(t, run, || {
                        FaucetError::Source("mssql: catalog discovery timed out".into())
                    })
                    .await?
                }
                None => run.await?,
            }
        };

        let mut catalog: Vec<CatalogRow> = Vec::with_capacity(rows.len());
        for row in &rows {
            let v = row_to_json(row)?;
            let decode = |col: &str| -> Result<String, FaucetError> {
                v.get(col)
                    .and_then(Value::as_str)
                    .map(str::to_string)
                    .ok_or_else(|| {
                        FaucetError::Source(format!("mssql: catalog decode failed ({col})"))
                    })
            };
            catalog.push((
                decode("TABLE_SCHEMA")?,
                decode("TABLE_NAME")?,
                decode("COLUMN_NAME")?,
                decode("DATA_TYPE")?,
                // Absent/odd IS_NULLABLE over-approximates to nullable.
                decode("IS_NULLABLE")
                    .map(|s| s.eq_ignore_ascii_case("YES"))
                    .unwrap_or(true),
            ));
        }

        let mut estimates: HashMap<(String, String), u64> = HashMap::new();
        let est_rows = {
            let run = async {
                conn.query(ESTIMATE_SQL, &[])
                    .await
                    .map_err(map_err)?
                    .into_first_result()
                    .await
                    .map_err(map_err)
            };
            match self.timeout() {
                Some(t) => {
                    with_statement_timeout(t, run, || {
                        FaucetError::Source("mssql: catalog discovery timed out".into())
                    })
                    .await
                }
                None => run.await,
            }
        };
        match est_rows {
            Ok(rows) => {
                for row in &rows {
                    if let Ok(v) = row_to_json(row)
                        && let (Some(sch), Some(tbl)) = (v["sch"].as_str(), v["tbl"].as_str())
                        && let Some(est) = v["est"].as_i64()
                        && est >= 0
                    {
                        estimates.insert((sch.to_string(), tbl.to_string()), est as u64);
                    }
                }
            }
            // Permissions on sys.* vary by principal — estimates are
            // best-effort metadata, never worth failing discovery over.
            Err(e) => tracing::debug!(
                error = %e,
                "mssql: row-estimate query failed; discovery continues without estimates"
            ),
        }

        const KEY_SQL: &str = "SELECT tc.TABLE_SCHEMA + '.' + tc.TABLE_NAME AS name, \
                  k.COLUMN_NAME AS col \
             FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc \
             JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k \
               ON k.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA \
              AND k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME \
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY' \
            ORDER BY tc.TABLE_SCHEMA, tc.TABLE_NAME, k.ORDINAL_POSITION";
        let key_rows = {
            let run = async {
                conn.query(KEY_SQL, &[])
                    .await
                    .map_err(map_err)?
                    .into_first_result()
                    .await
                    .map_err(map_err)
            };
            match self.timeout() {
                Some(t) => {
                    with_statement_timeout(t, run, || {
                        FaucetError::Source("mssql: primary-key discovery timed out".into())
                    })
                    .await?
                }
                None => run.await?,
            }
        };
        let mut keys: Vec<(String, String)> = Vec::with_capacity(key_rows.len());
        for row in &key_rows {
            let v = row_to_json(row)?;
            if let (Some(name), Some(col)) = (v["name"].as_str(), v["col"].as_str()) {
                keys.push((name.to_string(), col.to_string()));
            }
        }

        Ok(faucet_core::attach_primary_keys(
            descriptors_from_catalog(catalog, &estimates, bracket_quote),
            keys,
        ))
    }

    /// Shardable when a [`ShardConfig`](crate::config::ShardConfig) is set.
    fn is_shardable(&self) -> bool {
        self.config.shard.is_some()
    }

    /// Enumerate contiguous primary-key range shards by computing the `key`
    /// column's `MIN`/`MAX` over the (unsharded) base query and splitting that
    /// range into ~`target` slices. Returns a single whole-dataset shard when no
    /// `shard` config is set or the result set is empty.
    ///
    /// The base query is resolved through the same query builder as a normal
    /// fetch first, so a `@bookmark` token (incremental replication) is bound
    /// rather than left dangling — bounds are then computed over the
    /// not-yet-synced slice, which is exactly the data the shards will read.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        let Some(shard_cfg) = &self.config.shard else {
            return Ok(vec![ShardSpec::whole()]);
        };

        let start = self.current_start();
        let (inner, values, _incr) =
            build_query_and_params(&self.config, &HashMap::new(), start.as_ref());
        let key = quote_ident_mssql(&shard_cfg.key)?;
        let bounds_sql = pk_bounds_query(&inner, &key, "BIGINT");

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| FaucetError::Source(format!("MSSQL pool checkout failed: {e}")))?;

        let rows = {
            let owned: Vec<OwnedParam> = values.iter().map(OwnedParam::from_value).collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(OwnedParam::as_tosql).collect();
            let run = async {
                conn.query(&bounds_sql, &refs)
                    .await
                    .map_err(|e| {
                        FaucetError::Source(format!(
                            "mssql: failed to compute shard bounds for key {:?} \
                             (it must be an integer-typed column, and the query must \
                             not end in a top-level ORDER BY): {e}",
                            shard_cfg.key
                        ))
                    })?
                    .into_first_result()
                    .await
                    .map_err(|e| {
                        FaucetError::Source(format!(
                            "mssql: failed to compute shard bounds for key {:?} \
                             (it must be an integer-typed column, and the query must \
                             not end in a top-level ORDER BY): {e}",
                            shard_cfg.key
                        ))
                    })
            };
            match self.timeout() {
                Some(t) => {
                    with_statement_timeout(t, run, || {
                        FaucetError::Source("MSSQL shard-bounds query timed out".into())
                    })
                    .await?
                }
                None => run.await?,
            }
        };

        let Some(row) = rows.first() else {
            return Ok(vec![ShardSpec::whole()]);
        };
        let decoded = row_to_json(row)?;
        Ok(pk_shards_from_bounds(
            &shard_cfg.key,
            decoded["lo"].as_i64(),
            decoded["hi"].as_i64(),
            target,
        ))
    }

    /// Narrow this source to a single PK-range shard. The whole-dataset shard
    /// clears any applied range (streams the full query).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        let bounds = parse_pk_shard(shard, "mssql")?;
        if let Some(b) = &bounds {
            // Validate the key now (NUL check) so `bracket_quote` in the hot
            // wrap path stays infallible.
            quote_ident_mssql(&b.key)?;
        }
        *self.applied_shard.lock().expect("shard mutex poisoned") = bounds;
        Ok(())
    }
}

impl MssqlSource {
    /// Run the query and return all decoded rows plus (for incremental) the new
    /// bookmark. Used by the non-streaming convenience methods.
    async fn collect_all(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let start = self.current_start();
        let (query, values, incr) = build_query_and_params(&self.config, context, start.as_ref());
        let query = self.shard_wrap(query);

        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| FaucetError::Source(format!("MSSQL pool checkout failed: {e}")))?;

        let rows = {
            let owned: Vec<OwnedParam> = values.iter().map(OwnedParam::from_value).collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(OwnedParam::as_tosql).collect();
            let run = async {
                conn.query(&query, &refs)
                    .await
                    .map_err(|e| FaucetError::Source(format!("MSSQL query failed: {e}")))?
                    .into_first_result()
                    .await
                    .map_err(|e| FaucetError::Source(format!("MSSQL result read failed: {e}")))
            };
            match self.timeout() {
                Some(t) => {
                    with_statement_timeout(t, run, || {
                        FaucetError::Source("MSSQL query timed out".into())
                    })
                    .await?
                }
                None => run.await?,
            }
        };

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            records.push(row_to_json(row)?);
        }

        let mut running_max: Option<Value> = None;
        let records = apply_incremental(records, incr.as_ref(), &mut running_max);
        let bookmark = if incr.is_some() { running_max } else { None };
        Ok((records, bookmark))
    }
}

/// Filter a page for incremental replication and advance `running_max`.
/// For full replication the page passes through unchanged.
fn apply_incremental(
    page: Vec<Value>,
    incr: Option<&IncrementalCtx>,
    running_max: &mut Option<Value>,
) -> Vec<Value> {
    match incr {
        None => page,
        Some(ctx) => {
            let kept = filter_incremental(page, &ctx.column, &ctx.start);
            if let Some(m) = max_replication_value(&kept, &ctx.column) {
                let m = m.clone();
                *running_max = Some(match running_max.take() {
                    Some(prev) => max_value(prev, m),
                    None => m,
                });
            }
            kept
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::shard::plan_pk_shards;
    use serde_json::json;

    fn full_cfg() -> MssqlSourceConfig {
        MssqlSourceConfig::new("mssql://sa:pw@db.example.com:1433/sales", "SELECT * FROM t")
    }

    #[test]
    fn owned_param_binds_large_u64_as_exact_string_not_wrapped_i64() {
        // F41: a u64 above i64::MAX must keep its exact value, not wrap to a
        // negative i64 (`u64::MAX as i64 == -1`).
        let big = u64::MAX; // 18446744073709551615 > i64::MAX
        match OwnedParam::from_value(&json!(big)) {
            OwnedParam::Str(s) => assert_eq!(s, big.to_string()),
            OwnedParam::I64(v) => panic!("u64::MAX bound as wrapped i64 {v}"),
            _ => panic!("expected a string-bound param for a large u64"),
        }
        // A u64 that still fits i64 keeps the native integer binding.
        match OwnedParam::from_value(&json!(42u64)) {
            OwnedParam::I64(v) => assert_eq!(v, 42),
            _ => panic!("small u64 should bind as i64"),
        }
        // i64::MAX itself is representable as i64.
        match OwnedParam::from_value(&json!(i64::MAX)) {
            OwnedParam::I64(v) => assert_eq!(v, i64::MAX),
            _ => panic!("i64::MAX should bind as i64"),
        }
    }

    #[test]
    fn build_full_returns_query_and_params_unchanged() {
        let mut cfg = full_cfg();
        cfg.params = vec![json!(1), json!("x")];
        let (q, v, incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t");
        assert_eq!(v, vec![json!(1), json!("x")]);
        assert!(incr.is_none());
    }

    #[test]
    fn build_incremental_binds_bookmark_token() {
        let cfg = MssqlSourceConfig {
            query: "SELECT * FROM t WHERE updated_at > @bookmark".into(),
            replication: MssqlReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("1970-01-01"),
            },
            ..full_cfg()
        };
        let (q, v, incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t WHERE updated_at > @P1");
        assert_eq!(v, vec![json!("1970-01-01")]);
        assert_eq!(
            incr,
            Some(IncrementalCtx {
                column: "updated_at".into(),
                start: json!("1970-01-01")
            })
        );
    }

    #[test]
    fn build_incremental_uses_stored_bookmark_over_initial() {
        let cfg = MssqlSourceConfig {
            query: "SELECT * FROM t WHERE c > @bookmark".into(),
            params: vec![json!("p0")],
            replication: MssqlReplication::Incremental {
                column: "c".into(),
                initial_value: json!(0),
            },
            ..full_cfg()
        };
        let stored = json!(500);
        let (q, v, incr) = build_query_and_params(&cfg, &HashMap::new(), Some(&stored));
        // bookmark bound after the one configured param → @P2
        assert_eq!(q, "SELECT * FROM t WHERE c > @P2");
        assert_eq!(v, vec![json!("p0"), json!(500)]);
        assert_eq!(incr.unwrap().start, json!(500));
    }

    #[test]
    fn build_incremental_without_token_still_returns_filter_ctx() {
        let cfg = MssqlSourceConfig {
            query: "SELECT * FROM t".into(),
            replication: MssqlReplication::Incremental {
                column: "c".into(),
                initial_value: json!(0),
            },
            ..full_cfg()
        };
        let (q, v, incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t");
        assert!(v.is_empty());
        assert!(incr.is_some(), "client-side filter must still run");
    }

    #[test]
    fn owned_param_classifies_json() {
        assert!(matches!(
            OwnedParam::from_value(&json!("s")),
            OwnedParam::Str(_)
        ));
        assert!(matches!(
            OwnedParam::from_value(&json!(7)),
            OwnedParam::I64(7)
        ));
        assert!(matches!(
            OwnedParam::from_value(&json!(1.5)),
            OwnedParam::F64(_)
        ));
        assert!(matches!(
            OwnedParam::from_value(&json!(true)),
            OwnedParam::Bool(true)
        ));
        assert!(matches!(
            OwnedParam::from_value(&Value::Null),
            OwnedParam::Null(None)
        ));
        assert!(matches!(
            OwnedParam::from_value(&json!({"a":1})),
            OwnedParam::Str(_)
        ));
    }

    #[test]
    fn apply_incremental_filters_and_tracks_max() {
        let ctx = IncrementalCtx {
            column: "c".into(),
            start: json!(10),
        };
        let mut running = None;
        let page = vec![json!({"c": 5}), json!({"c": 15}), json!({"c": 20})];
        let kept = apply_incremental(page, Some(&ctx), &mut running);
        assert_eq!(kept.len(), 2);
        assert_eq!(running, Some(json!(20)));
    }

    #[test]
    fn apply_incremental_full_passes_through() {
        let mut running = None;
        let page = vec![json!({"c": 1}), json!({"c": 2})];
        let kept = apply_incremental(page, None, &mut running);
        assert_eq!(kept.len(), 2);
        assert_eq!(running, None);
    }

    #[test]
    fn default_state_key_is_stable_and_valid() {
        let cfg = full_cfg();
        let k1 = default_state_key(&cfg);
        let k2 = default_state_key(&cfg);
        assert_eq!(k1, k2);
        assert!(k1.starts_with("mssql:db.example.com:"));
        assert!(k1.contains(&format!("{:x}", faucet_core::shard::shard_hash(&cfg.query))));
        faucet_core::state::validate_state_key(&k1).expect("derived key must be valid");
    }

    // dataset_uri is a pure-config method; the source requires a live SQL Server
    // pool to construct so we verify the logic directly.
    #[test]
    fn dataset_uri_redacts_connection_url() {
        let cfg = full_cfg();
        let conn = cfg
            .connection
            .connection_url
            .as_deref()
            .or(cfg.connection.connection_string.as_deref())
            .unwrap_or("");
        let uri = format!(
            "{}?query={}",
            faucet_core::redact_uri_credentials(conn),
            cfg.query
        );
        assert_eq!(
            uri,
            "mssql://db.example.com:1433/sales?query=SELECT * FROM t"
        );
    }

    // ── PK-range sharding (Mode B, #262) ─────────────────────────────────────

    #[test]
    fn shard_wrap_uses_bracket_quoting() {
        let spec = faucet_core::ShardSpec::new(
            "1",
            json!({"key": "id", "lo": 100, "hi": 200, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let bounds = PkShardBounds::from_spec(&spec).unwrap();
        let sql = bounds.wrap("SELECT * FROM t", bracket_quote);
        assert!(sql.contains("(SELECT * FROM t) AS _faucet_shard"), "{sql}");
        assert!(sql.contains("[id] >= 100"), "bracket-quoted key: {sql}");
        assert!(sql.contains("[id] < 200"), "half-open upper bound: {sql}");
    }

    #[test]
    fn last_shard_wrap_covers_null_keys() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        let last = PkShardBounds::from_spec(shards.last().unwrap()).unwrap();
        let sql = last.wrap("SELECT * FROM t", bracket_quote);
        assert!(
            sql.contains("[id] IS NULL"),
            "last shard must match NULL keys: {sql}"
        );
    }

    #[test]
    fn shard_wrap_preserves_positional_bind_markers() {
        // The @Pn markers of a resolved incremental query survive the wrap —
        // tiberius binds them by name, not by textual position.
        let cfg = MssqlSourceConfig {
            query: "SELECT * FROM t WHERE c > @bookmark".into(),
            replication: MssqlReplication::Incremental {
                column: "c".into(),
                initial_value: json!(0),
            },
            ..full_cfg()
        };
        let (q, v, _incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        let spec = faucet_core::ShardSpec::new(
            "0",
            json!({"key": "id", "lo": 0, "hi": 10, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let wrapped = PkShardBounds::from_spec(&spec)
            .unwrap()
            .wrap(&q, bracket_quote);
        assert!(wrapped.contains("@P1"), "bind marker survives: {wrapped}");
        assert_eq!(v.len(), 1, "one bound value for the bookmark");
    }

    // ── discover: pure catalog-row grouping (#211) ───────────────────────────

    fn cat_row(s: &str, t: &str, c: &str, dt: &str, nullable: bool) -> CatalogRow {
        (s.into(), t.into(), c.into(), dt.into(), nullable)
    }

    #[test]
    fn descriptors_group_catalog_rows_per_table() {
        let rows = vec![
            cat_row("dbo", "orders", "id", "int", false),
            cat_row("dbo", "orders", "note", "nvarchar", true),
            cat_row("sales", "orders", "total", "decimal", false),
        ];
        let mut estimates = HashMap::new();
        estimates.insert(("dbo".to_string(), "orders".to_string()), 120u64);
        let ds = descriptors_from_catalog(rows, &estimates, bracket_quote);
        assert_eq!(ds.len(), 2, "same table name in two schemas = two datasets");

        assert_eq!(ds[0].name, "dbo.orders");
        assert_eq!(ds[0].kind, "table");
        assert_eq!(ds[0].estimated_rows, Some(120));
        assert_eq!(ds[0].config_patch["query"], "SELECT * FROM [dbo].[orders]");
        let schema = ds[0].schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(
            schema["properties"]["note"]["type"],
            json!(["string", "null"]),
            "nullable column"
        );

        // Estimate-merge miss: sales.orders has no sys.partitions entry.
        assert_eq!(ds[1].name, "sales.orders");
        assert_eq!(ds[1].estimated_rows, None, "missing estimate = no field");
        assert_eq!(
            ds[1].schema.as_ref().unwrap()["properties"]["total"]["type"],
            "number"
        );
    }

    #[test]
    fn descriptors_quote_hostile_identifiers() {
        let rows = vec![cat_row("dbo", "wei]rd", "id", "int", false)];
        let ds = descriptors_from_catalog(rows, &HashMap::new(), bracket_quote);
        let q = ds[0].config_patch["query"].as_str().unwrap();
        assert_eq!(
            q, "SELECT * FROM [dbo].[wei]]rd]",
            "interior ] must be doubled"
        );
    }

    #[test]
    fn descriptors_empty_catalog_is_empty() {
        assert!(descriptors_from_catalog(Vec::new(), &HashMap::new(), bracket_quote).is_empty());
    }
}

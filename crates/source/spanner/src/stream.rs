//! The Cloud Spanner [`Source`] implementation — client construction, query
//! execution, streaming, and incremental-replication bookkeeping.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use faucet_common_spanner::decode::row_to_json;
use faucet_common_spanner::quote_ident_spanner;
use faucet_common_spanner::types::{parse_spanner_type, spanner_type_to_json_schema};
use faucet_core::replication::{filter_incremental, max_replication_value, max_value};
use faucet_core::shard::{
    PkShardBounds, ShardSpec, parse_pk_shard, pk_bounds_query, pk_shards_from_bounds,
};
use faucet_core::{FaucetError, Source, StreamPage};
use futures::Stream;
use gcloud_spanner::client::Client;
use gcloud_spanner::statement::Statement;
use gcloud_spanner::transaction::QueryOptions;
use gcloud_spanner::transaction_ro::ReadOnlyTransaction;
use gcloud_spanner::value::TimestampBound;
use serde_json::Value;

use crate::config::{SpannerReplication, SpannerSourceConfig};

/// Google Cloud Spanner query source.
pub struct SpannerSource {
    config: SpannerSourceConfig,
    client: Client,
    /// Bookmark loaded via [`Source::apply_start_bookmark`]; overrides the
    /// configured `initial_value` for incremental runs.
    start_bookmark: Mutex<Option<Value>>,
    /// Shard applied by the cluster coordinator (Mode B), if any. `None` (or
    /// the whole-dataset shard) means the full query is streamed.
    applied_shard: Mutex<Option<PkShardBounds>>,
}

impl SpannerSource {
    /// Validate the config and build the Spanner client (session pool).
    pub async fn new(config: SpannerSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = config.connection.connect().await?;
        Ok(Self {
            config,
            client,
            start_bookmark: Mutex::new(None),
            applied_shard: Mutex::new(None),
        })
    }

    fn current_start(&self) -> Option<Value> {
        self.start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned")
            .clone()
    }

    /// Apply the currently-set shard (if any) to a resolved query string. The
    /// `@name` bind parameters inside the wrapped subquery are unaffected
    /// (they bind by name, not by position in the text).
    fn shard_wrap(&self, query: String) -> String {
        match &*self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(bounds) => bounds.wrap(&query, quote_ident_spanner),
            None => query,
        }
    }

    /// Open the single-use read-only transaction this run reads through,
    /// honoring `exact_staleness_secs`.
    async fn read_txn(&self) -> Result<ReadOnlyTransaction, FaucetError> {
        let result = match self.config.exact_staleness_secs {
            Some(secs) => {
                self.client
                    .single_with_timestamp_bound(TimestampBound::exact_staleness(
                        Duration::from_secs(secs),
                    ))
                    .await
            }
            None => self.client.single().await,
        };
        result.map_err(|e| FaucetError::Source(format!("spanner: transaction begin failed: {e}")))
    }
}

/// Incremental-replication context resolved for one run.
#[derive(Debug, Clone, PartialEq)]
struct IncrementalCtx {
    column: String,
    start: Value,
}

/// Build the final query string, the named bind values, and (for incremental
/// runs) the client-side filter context.
///
/// Pure function (no client) so it is unit-testable. Named parameters:
/// `config.params` keep their user-chosen names, parent-context placeholders
/// become `@_faucet_ctx_N`, and the incremental cursor binds as `@bookmark`
/// (only when the query mentions it).
fn build_query_and_params(
    config: &SpannerSourceConfig,
    context: &HashMap<String, Value>,
    start_bookmark: Option<&Value>,
) -> (String, Vec<(String, Value)>, Option<IncrementalCtx>) {
    let mut params: Vec<(String, Value)> = config
        .params
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    // Deterministic bind order (HashMap iteration order is random).
    params.sort_by(|a, b| a.0.cmp(&b.0));

    let query = if context.is_empty() {
        config.query.clone()
    } else {
        let (q, ctx_values) =
            faucet_core::util::substitute_context_bind_params(&config.query, context, 1, |i| {
                format!("@_faucet_ctx_{i}")
            });
        for (i, v) in ctx_values.into_iter().enumerate() {
            params.push((format!("_faucet_ctx_{}", i + 1), v));
        }
        q
    };

    let incremental = match &config.replication {
        SpannerReplication::Full => None,
        SpannerReplication::Incremental {
            column,
            initial_value,
        } => {
            let start = start_bookmark
                .cloned()
                .unwrap_or_else(|| initial_value.clone());
            // Server-side pushdown: bind the cursor where the user wrote
            // `@bookmark`. If absent, only the client-side filter applies.
            if query.contains("@bookmark") {
                params.push(("bookmark".to_string(), start.clone()));
            }
            Some(IncrementalCtx {
                column: column.clone(),
                start,
            })
        }
    };

    (query, params, incremental)
}

/// Bind one JSON scalar as a typed Spanner parameter. Spanner parameters are
/// typed, so each JSON scalar maps to the concrete Rust type whose
/// `ToKind::get_type` matches: string→STRING, integer→INT64, float→FLOAT64,
/// bool→BOOL. Non-scalar values are rejected (config validation catches
/// user params at load time; this also guards bookmark values at runtime).
fn bind_json_param(stmt: &mut Statement, name: &str, value: &Value) -> Result<(), FaucetError> {
    match value {
        Value::String(s) => stmt.add_param(name, s),
        Value::Bool(b) => stmt.add_param(name, b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                stmt.add_param(name, &i);
            } else if let Some(u) = n.as_u64() {
                let i = i64::try_from(u).map_err(|_| {
                    FaucetError::Config(format!(
                        "spanner: parameter `{name}` ({u}) overflows INT64"
                    ))
                })?;
                stmt.add_param(name, &i);
            } else {
                // Finite f64 (serde_json rejects NaN/Inf at parse time).
                stmt.add_param(name, &n.as_f64().unwrap_or(0.0));
            }
        }
        other => {
            return Err(FaucetError::Config(format!(
                "spanner: parameter `{name}` must be a scalar, got {other}"
            )));
        }
    }
    Ok(())
}

/// Build a [`Statement`] from a resolved query and named values.
fn build_statement(query: &str, params: &[(String, Value)]) -> Result<Statement, FaucetError> {
    let mut stmt = Statement::new(query);
    for (name, value) in params {
        bind_json_param(&mut stmt, name, value)?;
    }
    Ok(stmt)
}

/// One flattened `INFORMATION_SCHEMA.COLUMNS` row used by
/// [`Source::discover`]: `(table, column, spanner_type, is_nullable)`.
type CatalogRow = (String, String, String, bool);

/// Group flattened catalog rows (ordered by table, ordinal position) into one
/// [`DatasetDescriptor`](faucet_core::DatasetDescriptor) per table. Pure —
/// unit-testable without a live database. Spanner has no cheap row estimate
/// (no `reltuples` analogue), so descriptors carry none.
fn descriptors_from_catalog(rows: Vec<CatalogRow>) -> Vec<faucet_core::DatasetDescriptor> {
    /// In-progress per-table accumulator: `(table, columns)`.
    type PendingTable = (String, Vec<(String, Value)>);

    let mut out: Vec<faucet_core::DatasetDescriptor> = Vec::new();
    let mut current: Option<PendingTable> = None;

    let flush = |cur: Option<PendingTable>, out: &mut Vec<faucet_core::DatasetDescriptor>| {
        if let Some((table, cols)) = cur {
            let query = format!("SELECT * FROM {}", quote_ident_spanner(&table));
            out.push(
                faucet_core::DatasetDescriptor::new(
                    table,
                    "table",
                    serde_json::json!({ "query": query }),
                )
                .with_schema(faucet_core::columns_to_schema(cols)),
            );
        }
    };

    for (table, column, spanner_type, is_nullable) in rows {
        let same = current.as_ref().is_some_and(|(t, _)| *t == table);
        if !same {
            flush(current.take(), &mut out);
            current = Some((table, Vec::new()));
        }
        let fragment = spanner_type_to_json_schema(&parse_spanner_type(&spanner_type), is_nullable);
        if let Some((_, cols)) = current.as_mut() {
            cols.push((column, fragment));
        }
    }
    flush(current, &mut out);
    out
}

/// Derive a default state-store key from the database path + a query
/// fingerprint, stable across runs.
fn default_state_key(config: &SpannerSourceConfig) -> String {
    // Fixed FNV-1a: the key is durable and `DefaultHasher` is not stable across Rust releases.
    let fingerprint = faucet_core::shard::shard_hash(&config.query);
    // Project/instance/database ids are [a-z0-9-] by Spanner's naming rules;
    // sanitise defensively anyway (`:` is the key-segment separator).
    let path: String = format!(
        "{}.{}.{}",
        config.connection.project_id, config.connection.instance, config.connection.database
    )
    .chars()
    .map(|c| {
        if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
            c
        } else {
            '_'
        }
    })
    .collect();
    format!("spanner:{path}:{fingerprint:016x}")
}

#[async_trait]
impl Source for SpannerSource {
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
        let (query, params, incr) = build_query_and_params(&self.config, context, start.as_ref());
        let query = self.shard_wrap(query);

        Box::pin(async_stream::try_stream! {
            let stmt = build_statement(&query, &params)?;
            let mut tx = self.read_txn().await?;
            // `enable_resume: false`: the resume path buffers up to 128 MiB
            // between resume tokens and makes `next()` non-cancel-safe — the
            // pipeline `select!`s pages against its cancel token, so
            // cancel-safety wins over transparent stream resumption here.
            let opts = QueryOptions {
                enable_resume: false,
                ..Default::default()
            };
            let mut iter = tx
                .query_with_option(stmt, opts)
                .await
                .map_err(|e| FaucetError::Source(format!("spanner: query failed: {e}")))?;

            let mut fields: Option<std::sync::Arc<Vec<_>>> = None;
            let mut buffer: Vec<Value> = Vec::with_capacity(cap);
            let mut running_max: Option<Value> = None;
            let mut total = 0usize;

            while let Some(row) = iter
                .next()
                .await
                .map_err(|e| FaucetError::Source(format!("spanner: row stream failed: {e}")))?
            {
                let fields = fields.get_or_insert_with(|| iter.columns_metadata().clone());
                let record = row_to_json(&row, fields)
                    .map_err(|e| FaucetError::Source(format!("spanner: row decode failed: {e}")))?;
                buffer.push(record);
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

            tracing::info!(rows = total, query = %self.config.query, "spanner source stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SpannerSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "spanner"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "spanner://{}/{}/{}?query={}",
            self.config.connection.project_id,
            self.config.connection.instance,
            self.config.connection.database,
            self.config.query
        )
    }

    fn state_key(&self) -> Option<String> {
        match &self.config.replication {
            SpannerReplication::Full => None,
            SpannerReplication::Incremental { .. } => Some(
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

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate every base table in the database's default schema with
    /// column types from `INFORMATION_SCHEMA.COLUMNS` (catalog metadata only
    /// — no data scan). System schemas (`INFORMATION_SCHEMA`, `SPANNER_SYS`)
    /// carry a non-empty `TABLE_SCHEMA`, so filtering on the default (empty)
    /// schema excludes them. Spanner exposes no cheap row estimate, so
    /// descriptors carry none.
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        const CATALOG_SQL: &str = "SELECT c.TABLE_NAME, c.COLUMN_NAME, c.SPANNER_TYPE, \
                c.IS_NULLABLE \
           FROM INFORMATION_SCHEMA.COLUMNS AS c \
           JOIN INFORMATION_SCHEMA.TABLES AS t \
             ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME \
          WHERE t.TABLE_TYPE = 'BASE TABLE' AND t.TABLE_SCHEMA = '' \
          ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION";

        let map_err =
            |e: String| FaucetError::Source(format!("spanner: catalog discovery failed: {e}"));

        let mut tx = self.read_txn().await?;
        let mut iter = tx
            .query(Statement::new(CATALOG_SQL))
            .await
            .map_err(|e| map_err(e.to_string()))?;

        let mut catalog: Vec<CatalogRow> = Vec::new();
        while let Some(row) = iter.next().await.map_err(|e| map_err(e.to_string()))? {
            let table: String = row
                .column_by_name("TABLE_NAME")
                .map_err(|e| map_err(e.to_string()))?;
            let column: String = row
                .column_by_name("COLUMN_NAME")
                .map_err(|e| map_err(e.to_string()))?;
            let spanner_type: String = row
                .column_by_name("SPANNER_TYPE")
                .map_err(|e| map_err(e.to_string()))?;
            // Absent/odd IS_NULLABLE over-approximates to nullable.
            let nullable: String = row
                .column_by_name("IS_NULLABLE")
                .unwrap_or_else(|_| "YES".to_string());
            catalog.push((
                table,
                column,
                spanner_type,
                nullable.eq_ignore_ascii_case("YES"),
            ));
        }

        Ok(descriptors_from_catalog(catalog))
    }

    /// Shardable when a [`ShardConfig`](crate::config::ShardConfig) is set.
    fn is_shardable(&self) -> bool {
        self.config.shard.is_some()
    }

    /// Enumerate contiguous primary-key range shards by computing the `key`
    /// column's `MIN`/`MAX` over the (unsharded) base query and splitting
    /// that range into ~`target` slices. Returns a single whole-dataset shard
    /// when no `shard` config is set or the result set is empty.
    ///
    /// The base query is resolved through the same query builder as a normal
    /// fetch first, so a `@bookmark` parameter (incremental replication) is
    /// bound rather than left dangling — bounds are then computed over the
    /// not-yet-synced slice, which is exactly the data the shards will read.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        let Some(shard_cfg) = &self.config.shard else {
            return Ok(vec![ShardSpec::whole()]);
        };

        let start = self.current_start();
        let (inner, params, _incr) =
            build_query_and_params(&self.config, &HashMap::new(), start.as_ref());
        let key = quote_ident_spanner(&shard_cfg.key);
        let bounds_sql = pk_bounds_query(&inner, &key, "INT64");

        let map_err = |e: String| {
            FaucetError::Source(format!(
                "spanner: failed to compute shard bounds for key {:?} \
                 (it must be an INT64-typed column present in the query's output): {e}",
                shard_cfg.key
            ))
        };

        let stmt = build_statement(&bounds_sql, &params)?;
        let mut tx = self.read_txn().await?;
        let mut iter = tx.query(stmt).await.map_err(|e| map_err(e.to_string()))?;
        let row = iter.next().await.map_err(|e| map_err(e.to_string()))?;
        let Some(row) = row else {
            return Ok(vec![ShardSpec::whole()]);
        };
        let lo: Option<i64> = row
            .column_by_name("lo")
            .map_err(|e| map_err(e.to_string()))?;
        let hi: Option<i64> = row
            .column_by_name("hi")
            .map_err(|e| map_err(e.to_string()))?;
        Ok(pk_shards_from_bounds(&shard_cfg.key, lo, hi, target))
    }

    /// Narrow this source to a single PK-range shard. The whole-dataset shard
    /// clears any applied range (streams the full query).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        let bounds = parse_pk_shard(shard, "spanner")?;
        *self.applied_shard.lock().expect("shard mutex poisoned") = bounds;
        Ok(())
    }
}

impl SpannerSource {
    /// Run the query and return all decoded rows plus (for incremental) the
    /// new bookmark. Used by the non-streaming convenience methods.
    async fn collect_all(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        use futures::TryStreamExt;
        let mut records: Vec<Value> = Vec::new();
        let mut bookmark: Option<Value> = None;
        let mut stream = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = stream.try_next().await? {
            records.extend(page.records);
            if page.bookmark.is_some() {
                bookmark = page.bookmark;
            }
        }
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

    fn base() -> SpannerSourceConfig {
        SpannerSourceConfig::new("proj", "inst", "db", "SELECT * FROM t")
    }

    #[test]
    fn build_full_returns_query_and_sorted_params() {
        let mut cfg = base();
        cfg.params.insert("zeta".into(), json!(1));
        cfg.params.insert("alpha".into(), json!("x"));
        let (q, p, incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t");
        assert_eq!(
            p,
            vec![
                ("alpha".to_string(), json!("x")),
                ("zeta".to_string(), json!(1))
            ],
            "params bind in deterministic (sorted) order"
        );
        assert!(incr.is_none());
    }

    #[test]
    fn timestamp_cursors_compare_as_instants_across_fraction_widths() {
        // Spanner trims trailing fractional zeros, so `…:00Z` and `…:00.5Z`
        // differ in width and `'.' < 'Z'` reverses their text order (SQL-30).
        let ctx = IncrementalCtx {
            column: "ts".into(),
            start: json!("2024-01-01T00:00:00Z"),
        };
        let page = vec![
            json!({"ts": "2024-01-01T00:00:00.5Z"}),
            json!({"ts": "2024-01-01T00:00:00Z"}),
            json!({"ts": "2024-01-01T00:00:01Z"}),
        ];
        let mut max = None;
        let kept = apply_incremental(page, Some(&ctx), &mut max);
        assert_eq!(
            kept,
            vec![
                json!({"ts": "2024-01-01T00:00:00.5Z"}),
                json!({"ts": "2024-01-01T00:00:01Z"})
            ]
        );
        assert_eq!(max, Some(json!("2024-01-01T00:00:01Z")));

        let mut max = Some(json!("2024-01-01T00:00:00.25Z"));
        apply_incremental(
            vec![json!({"ts": "2024-01-01T00:00:00.5Z"})],
            Some(&ctx),
            &mut max,
        );
        assert_eq!(max, Some(json!("2024-01-01T00:00:00.5Z")));
    }

    #[test]
    fn numeric_cursors_compare_as_decimals() {
        let ctx = IncrementalCtx {
            column: "n".into(),
            start: json!("9.5"),
        };
        let mut max = None;
        let kept = apply_incremental(
            vec![
                json!({"n": "10"}),
                json!({"n": "9"}),
                json!({"n": "100.25"}),
            ],
            Some(&ctx),
            &mut max,
        );
        assert_eq!(kept, vec![json!({"n": "10"}), json!({"n": "100.25"})]);
        assert_eq!(max, Some(json!("100.25")));
    }

    #[test]
    fn build_incremental_binds_bookmark_param() {
        let mut cfg = base();
        cfg.query = "SELECT * FROM t WHERE updated_at > @bookmark".into();
        cfg.replication = SpannerReplication::Incremental {
            column: "updated_at".into(),
            initial_value: json!("1970-01-01"),
        };
        let (q, p, incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t WHERE updated_at > @bookmark");
        assert_eq!(p, vec![("bookmark".to_string(), json!("1970-01-01"))]);
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
        let mut cfg = base();
        cfg.query = "SELECT * FROM t WHERE c > @bookmark".into();
        cfg.replication = SpannerReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0),
        };
        let stored = json!(500);
        let (_q, p, incr) = build_query_and_params(&cfg, &HashMap::new(), Some(&stored));
        assert_eq!(p, vec![("bookmark".to_string(), json!(500))]);
        assert_eq!(incr.unwrap().start, json!(500));
    }

    #[test]
    fn build_incremental_without_token_still_returns_filter_ctx() {
        let mut cfg = base();
        cfg.replication = SpannerReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0),
        };
        let (q, p, incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t");
        assert!(p.is_empty());
        assert!(incr.is_some(), "client-side filter must still run");
    }

    #[test]
    fn build_context_binds_generated_named_params() {
        let mut cfg = base();
        cfg.query = "SELECT * FROM t WHERE tenant = {parent.id}".into();
        let mut ctx = HashMap::new();
        ctx.insert("parent.id".to_string(), json!(7));
        let (q, p, _incr) = build_query_and_params(&cfg, &ctx, None);
        assert_eq!(q, "SELECT * FROM t WHERE tenant = @_faucet_ctx_1");
        assert_eq!(p, vec![("_faucet_ctx_1".to_string(), json!(7))]);
    }

    #[test]
    fn build_statement_binds_scalars_and_rejects_containers() {
        let stmt = build_statement(
            "SELECT 1",
            &[
                ("s".to_string(), json!("x")),
                ("i".to_string(), json!(7)),
                ("f".to_string(), json!(1.5)),
                ("b".to_string(), json!(true)),
            ],
        );
        assert!(stmt.is_ok());

        let err = build_statement("SELECT 1", &[("o".to_string(), json!({"a": 1}))]);
        assert!(matches!(err, Err(FaucetError::Config(_))));
        let err = build_statement("SELECT 1", &[("n".to_string(), Value::Null)]);
        assert!(matches!(err, Err(FaucetError::Config(_))));
        let err = build_statement("SELECT 1", &[("u".to_string(), json!(u64::MAX))]);
        assert!(matches!(err, Err(FaucetError::Config(_))));
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
        let cfg = base();
        let k1 = default_state_key(&cfg);
        let k2 = default_state_key(&cfg);
        assert_eq!(k1, k2);
        assert!(k1.starts_with("spanner:proj.inst.db:"));
        assert!(k1.contains(&format!("{:x}", faucet_core::shard::shard_hash(&cfg.query))));
        faucet_core::state::validate_state_key(&k1).expect("derived key must be valid");
    }

    #[test]
    fn dataset_uri_shape() {
        let cfg = base();
        let uri = format!(
            "spanner://{}/{}/{}?query={}",
            cfg.connection.project_id, cfg.connection.instance, cfg.connection.database, cfg.query
        );
        assert_eq!(uri, "spanner://proj/inst/db?query=SELECT * FROM t");
    }

    // ── PK-range sharding (Mode B) ──────────────────────────────────────────

    #[test]
    fn shard_wrap_uses_backtick_quoting() {
        let spec = faucet_core::ShardSpec::new(
            "1",
            json!({"key": "id", "lo": 100, "hi": 200, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let bounds = PkShardBounds::from_spec(&spec).unwrap();
        let sql = bounds.wrap("SELECT * FROM t", quote_ident_spanner);
        assert!(sql.contains("(SELECT * FROM t) AS _faucet_shard"), "{sql}");
        assert!(sql.contains("`id` >= 100"), "backtick-quoted key: {sql}");
        assert!(sql.contains("`id` < 200"), "half-open upper bound: {sql}");
    }

    #[test]
    fn last_shard_wrap_covers_null_keys() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        let last = PkShardBounds::from_spec(shards.last().unwrap()).unwrap();
        let sql = last.wrap("SELECT * FROM t", quote_ident_spanner);
        assert!(
            sql.contains("`id` IS NULL"),
            "last shard must match NULL keys: {sql}"
        );
    }

    #[test]
    fn shard_wrap_preserves_named_bind_params() {
        // The @name parameters of a resolved incremental query survive the
        // wrap — Spanner binds them by name, not by textual position.
        let mut cfg = base();
        cfg.query = "SELECT * FROM t WHERE c > @bookmark".into();
        cfg.replication = SpannerReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0),
        };
        let (q, p, _incr) = build_query_and_params(&cfg, &HashMap::new(), None);
        let spec = faucet_core::ShardSpec::new(
            "0",
            json!({"key": "id", "lo": 0, "hi": 10, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let wrapped = PkShardBounds::from_spec(&spec)
            .unwrap()
            .wrap(&q, quote_ident_spanner);
        assert!(
            wrapped.contains("@bookmark"),
            "bind param survives: {wrapped}"
        );
        assert_eq!(p.len(), 1, "one bound value for the bookmark");
    }

    #[test]
    fn bounds_query_uses_int64_cast() {
        let sql = pk_bounds_query("SELECT * FROM t", "`id`", "INT64");
        assert!(sql.contains("CAST(MIN(`id`) AS INT64) AS lo"), "{sql}");
        assert!(
            sql.contains("FROM (SELECT * FROM t) AS _faucet_bounds"),
            "{sql}"
        );
    }

    // ── discover: pure catalog-row grouping ─────────────────────────────────

    fn cat_row(t: &str, c: &str, ty: &str, nullable: bool) -> CatalogRow {
        (t.into(), c.into(), ty.into(), nullable)
    }

    #[test]
    fn descriptors_group_catalog_rows_per_table() {
        let rows = vec![
            cat_row("orders", "id", "INT64", false),
            cat_row("orders", "note", "STRING(MAX)", true),
            cat_row("users", "score", "FLOAT64", false),
        ];
        let ds = descriptors_from_catalog(rows);
        assert_eq!(ds.len(), 2);

        assert_eq!(ds[0].name, "orders");
        assert_eq!(ds[0].kind, "table");
        assert_eq!(ds[0].estimated_rows, None, "spanner has no cheap estimate");
        assert_eq!(ds[0].config_patch["query"], "SELECT * FROM `orders`");
        let schema = ds[0].schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(
            schema["properties"]["note"]["type"],
            json!(["string", "null"]),
            "nullable column"
        );

        assert_eq!(ds[1].name, "users");
        assert_eq!(
            ds[1].schema.as_ref().unwrap()["properties"]["score"]["type"],
            "number"
        );
    }

    #[test]
    fn descriptors_quote_hostile_identifiers() {
        let rows = vec![cat_row("we`ird", "id", "INT64", false)];
        let ds = descriptors_from_catalog(rows);
        let q = ds[0].config_patch["query"].as_str().unwrap();
        assert_eq!(
            q, "SELECT * FROM `we``ird`",
            "interior backtick must be doubled"
        );
    }

    #[test]
    fn descriptors_empty_catalog_is_empty() {
        assert!(descriptors_from_catalog(Vec::new()).is_empty());
    }
}

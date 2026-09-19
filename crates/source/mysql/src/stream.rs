//! MySQL source implementation.

use crate::config::MysqlSourceConfig;
use async_trait::async_trait;
use faucet_core::shard::{
    PkShardBounds, ShardSpec, parse_pk_shard, pk_bounds_query, pk_shards_from_bounds,
};
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row};
use std::pin::Pin;
use std::sync::Mutex;

/// A source that executes a SQL query against MySQL and returns rows as JSON.
pub struct MysqlSource {
    config: MysqlSourceConfig,
    pool: MySqlPool,
    /// Shard applied by the cluster coordinator (Mode B), if any. `None` (or the
    /// whole-dataset shard) means the full query is streamed. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<PkShardBounds>>,
}

/// Quote a MySQL identifier with backticks (MySQL's default identifier
/// quoting — double quotes require the non-default `ANSI_QUOTES` sql_mode).
/// Embedded backticks are doubled, preventing identifier injection.
fn quote_ident_mysql(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

impl MysqlSource {
    /// Create a new MySQL source. Establishes a connection pool.
    pub async fn new(config: MysqlSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;

        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            // Connect-time, so `Config` is deliberate and stays: this runs in
            // `new()` at registry-build time, before any data moves, which is
            // what lets `faucet validate` / `doctor` surface an unreachable or
            // misconfigured destination as a configuration problem. A failure
            // *mid-query* is a different thing entirely and is reported as
            // `FaucetError::Source` (#662) — the config was fine; the database
            // went away.
            .map_err(|e| FaucetError::Config(format!("MySQL connection failed: {e}")))?;

        Ok(Self {
            config,
            pool,
            applied_shard: Mutex::new(None),
        })
    }

    /// Apply the currently-set shard (if any) to a resolved query string.
    fn shard_wrap(&self, query: String) -> String {
        match &*self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(bounds) => bounds.wrap(&query, quote_ident_mysql),
            None => query,
        }
    }
}

/// Convert a MySQL row column value to a `serde_json::Value`.
///
/// Attempts common types in order of likelihood. Falls back to `Value::Null`
/// for unsupported or null columns.
fn mysql_value_to_json(row: &sqlx::mysql::MySqlRow, col_name: &str) -> Value {
    // Try JSON first
    if let Ok(v) = row.try_get::<Value, _>(col_name) {
        return v;
    }

    // Try common scalar types
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Value::String(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i16, _>(col_name) {
        return Value::Number(v.into());
    }
    // UNSIGNED integer columns (#264). sqlx-mysql treats UNSIGNED as a
    // distinct type from the signed decoders above, so without these arms
    // UNSIGNED columns fall through to the `bool` probe (TINYINT UNSIGNED ->
    // bool) or to the `Null` fall-through (larger UNSIGNED -> null), silently
    // corrupting every unsigned column including UNSIGNED primary keys.
    //
    // These are placed *after* the signed probes so a signed column always
    // matches a signed arm first, and *before* `bool`/`f64`/`f32` so a
    // `TINYINT UNSIGNED` decodes as a number rather than a bool (MySQL's
    // boolean is `TINYINT(1)`). `u64` fits `serde_json::Number` exactly, so
    // BIGINT UNSIGNED values above `i64::MAX` round-trip losslessly.
    if let Ok(v) = row.try_get::<u64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<u32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<u16, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<u8, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<f32, _>(col_name) {
        return serde_json::Number::from_f64(v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Value::Bool(v);
    }

    // Richer types that would otherwise silently decode to Null (#78/#43).
    if let Ok(v) =
        row.try_get::<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>, _>(col_name)
    {
        return Value::String(v.to_rfc3339());
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDateTime, _>(col_name) {
        return Value::String(v.to_string());
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDate, _>(col_name) {
        return Value::String(v.to_string());
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveTime, _>(col_name) {
        return Value::String(v.to_string());
    }
    // DECIMAL → string, preserving exact precision.
    if let Ok(v) = row.try_get::<sqlx::types::BigDecimal, _>(col_name) {
        return Value::String(v.to_string());
    }
    // BLOB / BINARY → base64.
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Value::String(base64::engine::general_purpose::STANDARD.encode(v));
    }

    Value::Null
}

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
fn resolve_query(
    config: &MysqlSourceConfig,
    context: &std::collections::HashMap<String, Value>,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        (config.query.clone(), Vec::new())
    } else {
        faucet_core::util::substitute_context_bind_params(&config.query, context, 1, |_| {
            "?".to_string()
        })
    }
}

/// How a numeric bind value should be bound onto a sqlx query.
///
/// Classifying *before* binding keeps the integer/float decision in one pure,
/// unit-testable place and — critically — binds any integer in
/// `[i64::MIN, i64::MAX]` as an exact `i64` rather than an `f64`. Binding an
/// integer above `2^53` as `f64` silently rounds it (audit F38), so a large
/// 64-bit id threaded into `WHERE id = ?` would compare against the *wrong*
/// value and return wrong rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumberBind {
    /// Exact `i64` — covers every integer in `[i64::MIN, i64::MAX]`.
    I64,
    /// Value above `i64::MAX`; bind as `u64` (MySQL has native UNSIGNED).
    U64,
    /// Genuine floating-point value — bind as `f64`.
    F64,
}

/// Classify a JSON number into the bind category to use.
///
/// `is_i64()` losslessly covers `[i64::MIN, i64::MAX]` (including the
/// `(2^53, i64::MAX]` range that `f64` would round); `is_u64()` covers values
/// above `i64::MAX`; everything else is a real float.
fn classify_number(n: &serde_json::Number) -> NumberBind {
    if n.is_i64() {
        NumberBind::I64
    } else if n.is_u64() {
        NumberBind::U64
    } else {
        NumberBind::F64
    }
}

/// Apply context-derived bind values onto a sqlx query.
fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    bind_values: &'q [Value],
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    for value in bind_values {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) => match classify_number(n) {
                // `unwrap()` is sound: the classifier proves the predicate.
                NumberBind::I64 => query.bind(n.as_i64().unwrap()),
                // MySQL has a native UNSIGNED BIGINT type, so bind the `u64`
                // directly — values above `i64::MAX` round-trip losslessly.
                NumberBind::U64 => query.bind(n.as_u64().unwrap()),
                NumberBind::F64 => query.bind(n.as_f64().unwrap_or(0.0)),
            },
            Value::Bool(b) => query.bind(*b),
            Value::Null => query.bind(None::<String>),
            _ => query.bind(value.to_string()),
        };
    }
    query
}

/// One flattened `information_schema.columns` row used by [`discover`].
///
/// (table, column, data_type, is_nullable, estimated_rows)
type CatalogRow = (String, String, String, bool, Option<u64>);

/// In-progress per-table accumulator while grouping catalog rows:
/// `(table, estimated_rows, columns)`.
type PendingTable = Option<(String, Option<u64>, Vec<(String, Value)>)>;

/// Group flattened catalog rows (ordered by table name, ordinal position)
/// into one [`DatasetDescriptor`] per table. Pure — unit-testable without a
/// live server.
///
/// The dataset name is the bare table name: a MySQL connection is scoped to a
/// single database (named in the connection URL), so the generated `SELECT`
/// needs no database qualifier.
fn descriptors_from_catalog(rows: Vec<CatalogRow>) -> Vec<faucet_core::DatasetDescriptor> {
    let mut out: Vec<faucet_core::DatasetDescriptor> = Vec::new();
    let mut current: PendingTable = None;

    let flush = |cur: PendingTable, out: &mut Vec<faucet_core::DatasetDescriptor>| {
        if let Some((table, est, cols)) = cur {
            let query = format!("SELECT * FROM {}", quote_ident_mysql(&table));
            let mut d = faucet_core::DatasetDescriptor::new(
                table,
                "table",
                serde_json::json!({ "query": query }),
            )
            .with_schema(faucet_core::columns_to_schema(cols));
            // NULL table_rows (e.g. a view snuck through, or stats missing)
            // means no estimate.
            if let Some(n) = est {
                d = d.with_estimated_rows(n);
            }
            out.push(d);
        }
    };

    for (table, column, data_type, is_nullable, est) in rows {
        let same = current.as_ref().is_some_and(|(t, _, _)| *t == table);
        if !same {
            flush(current.take(), &mut out);
            current = Some((table, est, Vec::new()));
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

/// Convert a single `MySqlRow` into a JSON object whose keys are the row's
/// column names.
fn row_to_json(row: &sqlx::mysql::MySqlRow) -> Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = mysql_value_to_json(row, &name);
        map.insert(name, value);
    }
    Value::Object(map)
}

#[async_trait]
impl faucet_core::Source for MysqlSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = resolve_query(&self.config, context);
        let query_str = self.shard_wrap(query_str);
        let query = bind_params(sqlx::query(&query_str), &bind_values);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Source(format!("MySQL query failed: {e}")))?;

        let records: Vec<Value> = rows.iter().map(row_to_json).collect();
        tracing::info!(rows = records.len(), query = %self.config.query, "MySQL source fetch complete");
        Ok(records)
    }

    /// Stream rows from the underlying sqlx cursor without buffering the full
    /// result set. Each emitted [`StreamPage`] holds up to
    /// [`MysqlSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. The
    /// mysql query source has no incremental-replication mode today, so
    /// every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query_str, bind_values) = resolve_query(&self.config, context);
            let query_str = self.shard_wrap(query_str);
            let query = bind_params(sqlx::query(&query_str), &bind_values);

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| FaucetError::Source(format!("MySQL query failed: {e}")))?
            {
                buffer.push(row_to_json(&row));
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }
            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                rows = total,
                batch_size,
                query = %self.config.query,
                "MySQL source stream complete",
            );
        })
    }

    fn connector_name(&self) -> &'static str {
        "mysql"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MysqlSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?query={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            self.config.query
        )
    }

    fn supports_discover(&self) -> bool {
        true
    }

    /// Enumerate every base table in the connection's current database, with
    /// column types from `information_schema.columns` and a row estimate from
    /// `information_schema.tables.table_rows` (catalog metadata only — no
    /// data scan).
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        // The lowercase aliases matter: MySQL 8 returns information_schema
        // result columns as UPPERCASE (`TABLE_NAME`, …) without them. The
        // CAST(… AS CHAR) wrappers matter too: MySQL 8 reports several
        // information_schema string columns with a binary collation, which
        // sqlx surfaces as VARBINARY and refuses to decode as String.
        let sql = "\
            SELECT CAST(c.table_name AS CHAR) AS table_name, \
                   CAST(c.column_name AS CHAR) AS column_name, \
                   CAST(c.data_type AS CHAR) AS data_type, \
                   CAST(c.is_nullable AS CHAR) AS is_nullable, \
                   t.table_rows AS estimated_rows \
              FROM information_schema.columns c \
              JOIN information_schema.tables t \
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name \
             WHERE t.table_type = 'BASE TABLE' \
               AND c.table_schema = DATABASE() \
             ORDER BY c.table_name, c.ordinal_position";
        let rows = sqlx::query(sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Source(format!("mysql: catalog discovery failed: {e}")))?;

        let catalog: Vec<CatalogRow> = rows
            .iter()
            .map(|row| -> Result<CatalogRow, FaucetError> {
                let decode = |col: &str| -> Result<String, FaucetError> {
                    row.try_get::<String, _>(col).map_err(|e| {
                        FaucetError::Source(format!("mysql: catalog decode failed ({col}): {e}"))
                    })
                };
                Ok((
                    decode("table_name")?,
                    decode("column_name")?,
                    decode("data_type")?,
                    decode("is_nullable")?.eq_ignore_ascii_case("yes"),
                    // NULL (or an unexpected type) → no estimate.
                    row.try_get::<u64, _>("estimated_rows").ok(),
                ))
            })
            .collect::<Result<_, _>>()?;

        Ok(descriptors_from_catalog(catalog))
    }

    /// Shardable when a [`ShardConfig`](crate::config::ShardConfig) is set.
    fn is_shardable(&self) -> bool {
        self.config.shard.is_some()
    }

    /// Enumerate contiguous primary-key range shards by computing the `key`
    /// column's `MIN`/`MAX` over the (unsharded) base query and splitting that
    /// range into ~`target` slices. Returns a single whole-dataset shard when no
    /// `shard` config is set or the result set is empty.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        let Some(shard_cfg) = &self.config.shard else {
            return Ok(vec![ShardSpec::whole()]);
        };

        let bounds_sql = pk_bounds_query(
            &self.config.query,
            &quote_ident_mysql(&shard_cfg.key),
            "SIGNED",
        );
        let row = sqlx::query(&bounds_sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "mysql: failed to compute shard bounds for key {:?} \
                     (it must be an integer-typed column): {e}",
                    shard_cfg.key
                ))
            })?;

        let lo: Option<i64> = row
            .try_get("lo")
            .map_err(|e| FaucetError::Source(format!("mysql: shard bounds decode failed: {e}")))?;
        let hi: Option<i64> = row
            .try_get("hi")
            .map_err(|e| FaucetError::Source(format!("mysql: shard bounds decode failed: {e}")))?;
        Ok(pk_shards_from_bounds(&shard_cfg.key, lo, hi, target))
    }

    /// Narrow this source to a single PK-range shard. The whole-dataset shard
    /// clears any applied range (streams the full query).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        *self.applied_shard.lock().expect("shard mutex poisoned") = parse_pk_shard(shard, "mysql")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::shard::plan_pk_shards;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = MysqlSourceConfig::new("mysql://localhost/test", "SELECT 1");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match MysqlSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // dataset_uri is a pure-config method; the source requires a live DB to
    // construct so we verify the credential-stripping logic directly.
    #[test]
    fn dataset_uri_strips_credentials() {
        let redacted = faucet_core::redact_uri_credentials("mysql://u:p@h:3306/db");
        let uri = format!("{}?query={}", redacted, "SELECT 1");
        assert_eq!(uri, "mysql://h:3306/db?query=SELECT 1");
    }

    // ── F38: numeric bind classification (precision-safe) ───────────────────

    fn num(v: serde_json::Value) -> serde_json::Number {
        match v {
            serde_json::Value::Number(n) => n,
            _ => panic!("not a number"),
        }
    }

    #[test]
    fn classify_small_int_is_i64() {
        assert_eq!(
            classify_number(&num(serde_json::json!(42))),
            NumberBind::I64
        );
        assert_eq!(
            classify_number(&num(serde_json::json!(-7))),
            NumberBind::I64
        );
        assert_eq!(classify_number(&num(serde_json::json!(0))), NumberBind::I64);
    }

    #[test]
    fn classify_above_2_pow_53_stays_i64_not_f64() {
        // 2^53 + 1 must NOT be bound as f64 (which would round it). It is a
        // valid i64, so it must classify as I64.
        let v = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(classify_number(&num(serde_json::json!(v))), NumberBind::I64);
    }

    #[test]
    fn classify_i64_boundaries_are_i64() {
        assert_eq!(
            classify_number(&num(serde_json::json!(i64::MAX))),
            NumberBind::I64
        );
        assert_eq!(
            classify_number(&num(serde_json::json!(i64::MIN))),
            NumberBind::I64
        );
    }

    #[test]
    fn classify_above_i64_max_is_u64() {
        let v: u64 = i64::MAX as u64 + 1;
        assert_eq!(classify_number(&num(serde_json::json!(v))), NumberBind::U64);
        assert_eq!(
            classify_number(&num(serde_json::json!(u64::MAX))),
            NumberBind::U64
        );
    }

    #[test]
    fn classify_float_is_f64() {
        assert_eq!(
            classify_number(&num(serde_json::json!(3.5))),
            NumberBind::F64
        );
    }

    // ── PK-range sharding (Mode B, #262) ─────────────────────────────────────

    #[test]
    fn quote_ident_mysql_backticks_and_escapes() {
        assert_eq!(quote_ident_mysql("id"), "`id`");
        // Embedded backticks are doubled — identifier injection is inert.
        assert_eq!(quote_ident_mysql("we`ird"), "`we``ird`");
    }

    #[test]
    fn shard_wrap_uses_backtick_quoting() {
        let spec = faucet_core::shard::ShardSpec::new(
            "1",
            serde_json::json!({"key": "id", "lo": 100, "hi": 200, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let bounds = PkShardBounds::from_spec(&spec).unwrap();
        let sql = bounds.wrap("SELECT * FROM t", quote_ident_mysql);
        assert!(sql.contains("(SELECT * FROM t) AS _faucet_shard"), "{sql}");
        assert!(sql.contains("`id` >= 100"), "backtick-quoted key: {sql}");
        assert!(sql.contains("`id` < 200"), "half-open upper bound: {sql}");
    }

    #[test]
    fn last_shard_wrap_covers_null_keys() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        let last = PkShardBounds::from_spec(shards.last().unwrap()).unwrap();
        let sql = last.wrap("SELECT * FROM t", quote_ident_mysql);
        assert!(
            sql.contains("`id` IS NULL"),
            "last shard must match NULL keys: {sql}"
        );
    }

    /// Build a source over a lazy pool (no server needed) so the shard glue —
    /// `apply_shard`, `shard_wrap`, and `enumerate_shards`' non-I/O branches —
    /// is testable without Docker.
    fn lazy_source(config: MysqlSourceConfig) -> MysqlSource {
        let pool = MySqlPoolOptions::new()
            // Fail fast at first checkout — these tests never reach a server.
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy(&config.connection_url)
            .expect("lazy pool");
        MysqlSource {
            config,
            pool,
            applied_shard: Mutex::new(None),
        }
    }

    #[tokio::test]
    async fn apply_shard_then_shard_wrap_narrows_query() {
        use faucet_core::Source as _;
        let mut config = MysqlSourceConfig::new("mysql://root@127.0.0.1:1/db", "SELECT * FROM t");
        config.shard = Some(crate::config::ShardConfig { key: "id".into() });
        let source = lazy_source(config);
        assert!(source.is_shardable());

        // No shard applied / whole shard applied → query passes through.
        assert_eq!(source.shard_wrap("SELECT 1".into()), "SELECT 1");
        source
            .apply_shard(&faucet_core::ShardSpec::whole())
            .await
            .unwrap();
        assert_eq!(source.shard_wrap("SELECT 1".into()), "SELECT 1");

        // A real shard narrows with backtick quoting.
        let spec = &plan_pk_shards("id", 0, 99, 2)[0];
        source.apply_shard(spec).await.unwrap();
        let wrapped = source.shard_wrap("SELECT * FROM t".into());
        assert!(wrapped.contains("`id`"), "got: {wrapped}");
        assert!(wrapped.contains("_faucet_shard"), "got: {wrapped}");

        // Malformed descriptor is rejected.
        let bad = faucet_core::ShardSpec::new("0", serde_json::json!({ "key": "id" }));
        assert!(source.apply_shard(&bad).await.is_err());
    }

    // ── discover: pure catalog-row grouping (#211) ───────────────────────────

    #[test]
    fn descriptors_group_catalog_rows_per_table() {
        let rows: Vec<CatalogRow> = vec![
            (
                "orders".to_string(),
                "id".to_string(),
                "bigint".to_string(),
                false,
                Some(120u64),
            ),
            (
                "orders".to_string(),
                "note".to_string(),
                "varchar".to_string(),
                true,
                Some(120u64),
            ),
            (
                "users".to_string(),
                "total".to_string(),
                "decimal".to_string(),
                false,
                None,
            ),
        ];
        let ds = descriptors_from_catalog(rows);
        assert_eq!(ds.len(), 2, "rows group into one descriptor per table");

        assert_eq!(ds[0].name, "orders", "bare table name — no db qualifier");
        assert_eq!(ds[0].kind, "table");
        assert_eq!(ds[0].estimated_rows, Some(120));
        assert_eq!(ds[0].config_patch["query"], "SELECT * FROM `orders`");
        let schema = ds[0].schema.as_ref().unwrap();
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(
            schema["properties"]["note"]["type"],
            serde_json::json!(["string", "null"]),
            "nullable column"
        );

        assert_eq!(ds[1].name, "users");
        assert_eq!(ds[1].estimated_rows, None, "NULL table_rows = no estimate");
        assert_eq!(
            ds[1].schema.as_ref().unwrap()["properties"]["total"]["type"],
            "number"
        );
    }

    #[test]
    fn descriptors_quote_hostile_identifiers() {
        let rows: Vec<CatalogRow> = vec![(
            "we`ird".to_string(),
            "id".to_string(),
            "int".to_string(),
            false,
            None,
        )];
        let ds = descriptors_from_catalog(rows);
        assert_eq!(
            ds[0].config_patch["query"], "SELECT * FROM `we``ird`",
            "embedded backticks are doubled"
        );
    }

    #[test]
    fn descriptors_empty_catalog_is_empty() {
        assert!(descriptors_from_catalog(Vec::new()).is_empty());
    }

    #[tokio::test]
    async fn source_advertises_discover() {
        use faucet_core::Source as _;
        let source = lazy_source(MysqlSourceConfig::new(
            "mysql://root@127.0.0.1:1/db",
            "SELECT 1",
        ));
        assert!(source.supports_discover());
        // Against an unreachable server the catalog query surfaces the typed
        // discovery error (exercises the error path without Docker).
        let err = source.discover().await.unwrap_err();
        assert!(
            err.to_string().contains("catalog discovery failed"),
            "typed error: {err}"
        );
    }

    #[tokio::test]
    async fn enumerate_shards_without_config_is_whole_and_with_config_needs_db() {
        use faucet_core::Source as _;
        // No `shard` config → single whole shard, no I/O.
        let plain = lazy_source(MysqlSourceConfig::new(
            "mysql://root@127.0.0.1:1/db",
            "SELECT 1",
        ));
        assert!(!plain.is_shardable());
        let shards = plain.enumerate_shards(4).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());

        // With config, enumeration must reach the (unreachable) server → the
        // bounds-probe error path surfaces as FaucetError::Source.
        let mut config = MysqlSourceConfig::new("mysql://root@127.0.0.1:1/db", "SELECT 1");
        config.shard = Some(crate::config::ShardConfig { key: "id".into() });
        let sharded = lazy_source(config);
        let err = sharded.enumerate_shards(4).await.unwrap_err();
        assert!(
            err.to_string().contains("shard bounds"),
            "expected bounds-probe error, got: {err}"
        );
    }
}

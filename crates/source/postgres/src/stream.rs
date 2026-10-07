//! PostgreSQL source implementation.

use crate::config::PostgresSourceConfig;
use async_trait::async_trait;
use faucet_core::shard::{
    PkShardBounds, ShardSpec, parse_pk_shard, pk_bounds_query, pk_shards_from_bounds,
};
use faucet_core::util::quote_ident;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row, TypeInfo, ValueRef};
use std::pin::Pin;
use std::sync::Mutex;

/// A source that executes a SQL query against PostgreSQL and returns rows as JSON.
pub struct PostgresSource {
    config: PostgresSourceConfig,
    pool: PgPool,
    /// Shard applied by the cluster coordinator (Mode B), if any. `None` (or the
    /// whole-dataset shard) means the full query is streamed. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<PkShardBounds>>,
    /// Columns already warned about under `json_big_numbers: string`.
    json_warned: Mutex<std::collections::HashSet<String>>,
}

impl PostgresSource {
    /// Create a new PostgreSQL source. Establishes a connection pool.
    pub async fn new(config: PostgresSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;

        let pool = PgPoolOptions::new()
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
            .map_err(|e| FaucetError::Config(format!("PostgreSQL connection failed: {e}")))?;

        Ok(Self {
            config,
            pool,
            applied_shard: Mutex::new(None),
            json_warned: Mutex::new(Default::default()),
        })
    }

    /// `query` wrapped by [`text_cast_query`] for the columns the decoder
    /// has no exact mapping for. A query the server cannot describe is
    /// returned unchanged, so its own error surfaces when it runs.
    async fn typed_query(&self, query: String) -> String {
        use sqlx::Executor;
        match (&self.pool).describe(&query).await {
            Ok(described) => {
                let columns: Vec<(String, String)> = described
                    .columns()
                    .iter()
                    .map(|c| (c.name().to_string(), c.type_info().name().to_string()))
                    .collect();
                text_cast_query(&query, &columns).unwrap_or(query)
            }
            Err(_) => query,
        }
    }

    fn json_context(&self) -> JsonContext<'_> {
        JsonContext {
            mode: self.config.json_big_numbers,
            warned: &self.json_warned,
        }
    }

    /// Apply the currently-set shard (if any) to a resolved query string.
    fn shard_wrap(&self, query: String) -> String {
        match &*self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(bounds) => bounds.wrap(&query, quote_ident),
            None => query,
        }
    }
}

/// Postgres types the decoder maps exactly. Every other column is read through
/// its text form (#789 SQL-12): arrays, enums, `interval`, `inet`, `money`,
/// ranges, `timetz`, `xml`, `hstore` all decoded to `null` before. `NUMERIC`
/// and `TIME` are read as text too, so `NaN` / `Infinity` and `24:00:00`
/// survive.
const NATIVE_TYPES: &[&str] = &[
    "BOOL",
    "INT2",
    "INT4",
    "INT8",
    "FLOAT4",
    "FLOAT8",
    "TEXT",
    "VARCHAR",
    "BPCHAR",
    "NAME",
    "\"CHAR\"",
    "CHAR",
    "JSON",
    "JSONB",
    "TIMESTAMPTZ",
    "TIMESTAMP",
    "DATE",
    "UUID",
    "BYTEA",
];

/// Wrap `query` so every column outside [`NATIVE_TYPES`] is read as text,
/// keeping column names and order. `None` when nothing needs a cast, or when
/// duplicate column names make the columns unaddressable from the wrapper.
fn text_cast_query(query: &str, columns: &[(String, String)]) -> Option<String> {
    let needs_cast = |t: &str| !NATIVE_TYPES.iter().any(|n| n.eq_ignore_ascii_case(t));
    if !columns.iter().any(|(_, t)| needs_cast(t)) {
        return None;
    }
    let mut seen = std::collections::HashSet::new();
    if !columns.iter().all(|(n, _)| seen.insert(n.as_str())) {
        return None;
    }
    let select: Vec<String> = columns
        .iter()
        .map(|(n, t)| {
            let q = quote_ident(n);
            // `format('%s', …)` renders through the type's output function —
            // the same text postgres-cdc emits (`::text` adds a netmask to
            // `inet`, for one) — and the guard keeps NULL a NULL.
            if needs_cast(t) {
                format!("CASE WHEN q.{q} IS NULL THEN NULL ELSE format('%s', q.{q}) END AS {q}")
            } else {
                format!("q.{q}")
            }
        })
        .collect();
    Some(format!("SELECT {} FROM ({query}) AS q", select.join(", ")))
}

/// A float as JSON: a number when finite, else Postgres's own spelling.
fn float_json(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .unwrap_or_else(|| {
            Value::String(
                if v.is_nan() {
                    "NaN"
                } else if v > 0.0 {
                    "Infinity"
                } else {
                    "-Infinity"
                }
                .to_string(),
            )
        })
}

/// A `real` as JSON through its shortest decimal form: `0.1::real` is `0.1`,
/// not the widened `0.10000000149011612`.
fn float4_json(v: f32) -> Value {
    if !v.is_finite() {
        return float_json(f64::from(v));
    }
    v.to_string()
        .parse::<f64>()
        .map_or_else(|_| float_json(f64::from(v)), float_json)
}

/// `"infinity"` / `"-infinity"` for Postgres's date / timestamp sentinels
/// (`i32::MAX` / `i32::MIN` days, `i64::MAX` / `i64::MIN` microseconds).
fn temporal_infinity(raw: &sqlx::postgres::PgValueRef<'_>) -> Option<Value> {
    use sqlx::{TypeInfo as _, ValueRef as _};
    if raw.is_null() {
        return None;
    }
    let name = raw.type_info().name().to_ascii_uppercase();
    let bytes = raw.as_bytes().ok()?;
    let positive = match (name.as_str(), raw.format()) {
        (_, sqlx::postgres::PgValueFormat::Text) => match bytes {
            b"infinity" => true,
            b"-infinity" => false,
            _ => return None,
        },
        ("DATE", _) => match i32::from_be_bytes(bytes.try_into().ok()?) {
            i32::MAX => true,
            i32::MIN => false,
            _ => return None,
        },
        ("TIMESTAMP" | "TIMESTAMPTZ", _) => match i64::from_be_bytes(bytes.try_into().ok()?) {
            i64::MAX => true,
            i64::MIN => false,
            _ => return None,
        },
        _ => return None,
    };
    if !matches!(name.as_str(), "DATE" | "TIMESTAMP" | "TIMESTAMPTZ") {
        return None;
    }
    Some(Value::String(
        if positive { "infinity" } else { "-infinity" }.into(),
    ))
}

/// A JSON / JSONB column read exactly: every number is checked on the
/// column's text, and one a JSON value cannot hold exactly is refused or kept
/// as an exact string per `json_big_numbers` (#789 SQL-49).
fn json_text_to_value(
    text: &str,
    column: &str,
    mode: faucet_core::JsonBigNumbers,
    warned: &Mutex<std::collections::HashSet<String>>,
) -> Result<Value, FaucetError> {
    use faucet_core::json_numbers::JsonNumberError;
    match faucet_core::parse_json_exact(text, mode) {
        Ok((value, inexact)) => {
            if !inexact.is_empty()
                && warned
                    .lock()
                    .expect("json warning mutex poisoned")
                    .insert(column.to_string())
            {
                tracing::warn!(
                    column,
                    "PostgreSQL column holds JSON numbers a 64-bit float cannot represent \
                     exactly; emitting them as strings (json_big_numbers: string)"
                );
            }
            Ok(value)
        }
        Err(JsonNumberError::Inexact(found)) => Err(FaucetError::Source(format!(
            "PostgreSQL column {column} holds a JSON number a 64-bit float cannot represent \
             exactly ({}); set `json_big_numbers: string` to emit such numbers as exact \
             strings, or cast the column in the query",
            found[0].preview()
        ))),
        Err(JsonNumberError::Invalid(e)) => Err(FaucetError::Source(format!(
            "PostgreSQL column {column} holds invalid JSON: {e}"
        ))),
    }
}

/// The text of a `json` / `jsonb` cell (`None` for other types or NULL).
fn pg_json_text<'r>(raw: &sqlx::postgres::PgValueRef<'r>) -> Option<&'r str> {
    use sqlx::{TypeInfo as _, ValueRef as _};
    if raw.is_null() {
        return None;
    }
    let name = raw.type_info().name().to_ascii_uppercase();
    let bytes = raw.as_bytes().ok()?;
    let bytes = match (name.as_str(), raw.format()) {
        ("JSON", _) => bytes,
        // Binary JSONB carries a one-byte format version before the text.
        ("JSONB", sqlx::postgres::PgValueFormat::Binary) => bytes.get(1..)?,
        ("JSONB", _) => bytes,
        _ => return None,
    };
    std::str::from_utf8(bytes).ok()
}

/// Convert a raw sqlx column value to a `serde_json::Value`.
///
/// Tries the native decodes in turn; a non-NULL cell none of them can decode
/// is an error rather than a silent `null`.
fn pg_value_to_json(
    row: &sqlx::postgres::PgRow,
    col_name: &str,
    json: &JsonContext<'_>,
) -> Result<Value, FaucetError> {
    if let Ok(raw) = row.try_get_raw(col_name)
        && let Some(text) = pg_json_text(&raw)
    {
        return json_text_to_value(text, col_name, json.mode, json.warned);
    }

    // Try common scalar types
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Ok(Value::String(v));
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = row.try_get::<i16, _>(col_name) {
        return Ok(Value::Number(v.into()));
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return Ok(float_json(v));
    }
    if let Ok(v) = row.try_get::<f32, _>(col_name) {
        return Ok(float4_json(v));
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Ok(Value::Bool(v));
    }

    // `infinity` / `-infinity` dates and timestamps would overflow chrono
    // (a panic inside sqlx's decode), so the sentinels are read first.
    if let Ok(raw) = row.try_get_raw(col_name) {
        if let Some(v) = temporal_infinity(&raw) {
            return Ok(v);
        }
    }

    // Richer types that would otherwise silently decode to Null (#78/#43).
    // Timestamps → RFC3339 / ISO-8601 strings.
    if let Ok(v) =
        row.try_get::<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>, _>(col_name)
    {
        return Ok(Value::String(v.to_rfc3339()));
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDateTime, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDate, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveTime, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    // UUID → canonical hyphenated string.
    if let Ok(v) = row.try_get::<sqlx::types::Uuid, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    // NUMERIC / DECIMAL → string, preserving exact precision.
    if let Ok(v) = row.try_get::<sqlx::types::BigDecimal, _>(col_name) {
        return Ok(Value::String(v.to_string()));
    }
    // BYTEA → base64 (so binary survives the JSON round-trip).
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Ok(Value::String(
            base64::engine::general_purpose::STANDARD.encode(v),
        ));
    }

    match row.try_get_raw(col_name) {
        Ok(raw) if raw.is_null() => Ok(Value::Null),
        Ok(raw) => Err(FaucetError::Source(format!(
            "PostgreSQL column {col_name} has type {} which the source cannot decode; \
             cast it to text in the query",
            raw.type_info().name()
        ))),
        Err(e) => Err(FaucetError::Source(format!(
            "PostgreSQL column {col_name} read failed: {e}"
        ))),
    }
}

/// Await a server read, failing it after `secs` seconds (`0` = no limit), so a
/// peer that vanished without closing the connection cannot hang the run.
async fn bounded_read<T>(
    secs: u64,
    fut: impl std::future::Future<Output = Result<T, sqlx::Error>>,
) -> Result<T, FaucetError> {
    let result = if secs == 0 {
        fut.await
    } else {
        match tokio::time::timeout(std::time::Duration::from_secs(secs), fut).await {
            Ok(result) => result,
            Err(_) => {
                return Err(FaucetError::Source(format!(
                    "PostgreSQL read timed out after {secs}s waiting for the server (read_timeout_secs)"
                )));
            }
        }
    };
    result.map_err(|e| FaucetError::Source(format!("PostgreSQL query failed: {e}")))
}

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
fn resolve_query(
    config: &PostgresSourceConfig,
    context: &std::collections::HashMap<String, Value>,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        (config.query.clone(), Vec::new())
    } else {
        faucet_core::util::substitute_context_bind_params(
            &config.query,
            context,
            config.params.len() + 1,
            |i| format!("${i}"),
        )
    }
}

/// How a numeric bind value should be bound onto a sqlx query.
///
/// Classifying *before* binding keeps the integer/float decision in one pure,
/// unit-testable place and — critically — binds any integer in
/// `[i64::MIN, i64::MAX]` as an exact `i64` rather than an `f64`. Binding an
/// integer above `2^53` as `f64` silently rounds it (audit F38), so a large
/// 64-bit id threaded into `WHERE id = $1` would compare against the *wrong*
/// value and return wrong rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumberBind {
    /// Exact `i64` — covers every integer in `[i64::MIN, i64::MAX]`.
    I64,
    /// Value above `i64::MAX`; bind the `u64` reinterpreted as `i64` (two's
    /// complement) so the bytes round-trip into an `int8`/`bigint` column.
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

/// Apply configured params followed by context-derived bind values onto a
/// sqlx query.
fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    config_params: &'q [Value],
    bind_values: &'q [Value],
) -> Result<sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>, FaucetError> {
    // Bind the static config params and the per-context values as native
    // scalar types, in positional order ($1, $2, …). Binding a raw
    // `serde_json::Value` encodes it as `jsonb` (sqlx), which breaks comparisons
    // against typed columns — e.g. `WHERE id = $1` against an integer column
    // fails with "operator does not exist: integer = jsonb". config_params
    // previously bound the raw Value and hit exactly this (audit #146 H12).
    for (i, value) in config_params.iter().chain(bind_values).enumerate() {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) => match classify_number(n) {
                // `unwrap()` is sound: the classifier proves the predicate.
                NumberBind::I64 => query.bind(n.as_i64().unwrap()),
                // Above `i64::MAX`. Postgres has no unsigned integer type, and
                // `as i64` would *wrap* — writing a large id as a large negative
                // number, or (when this binds an incremental bookmark) comparing
                // against a negative bound and re-reading or skipping rows. The
                // original intent here was to avoid an `f64` cast's precision
                // loss, which is right; bit-reinterpretation is not the way to
                // get it. Refuse instead (#462).
                NumberBind::U64 => query.bind(faucet_core::util::u64_to_signed(
                    n.as_u64().unwrap(),
                    &format!("bind parameter ${}", i + 1),
                )?),
                NumberBind::F64 => query.bind(n.as_f64().unwrap_or(0.0)),
            },
            Value::Bool(b) => query.bind(*b),
            Value::Null => query.bind(None::<String>),
            _ => query.bind(value.to_string()),
        };
    }
    Ok(query)
}

/// One flattened `information_schema.columns` row used by [`discover`].
///
/// (schema, table, column, data_type, is_nullable, estimated_rows)
type CatalogRow = (String, String, String, String, bool, Option<i64>);

/// A table mid-accumulation while grouping catalog rows:
/// (schema, table, estimated_rows, columns).
type PendingTable = (String, String, Option<i64>, Vec<(String, Value)>);

/// Group flattened catalog rows (ordered by schema, table, ordinal position)
/// into one [`DatasetDescriptor`] per table. Pure — unit-testable without a
/// live server. `quote` is the dialect's identifier quoter.
fn descriptors_from_catalog(
    rows: Vec<CatalogRow>,
    quote: fn(&str) -> String,
) -> Vec<faucet_core::DatasetDescriptor> {
    let mut out: Vec<faucet_core::DatasetDescriptor> = Vec::new();
    let mut current: Option<PendingTable> = None;

    let flush = |cur: Option<PendingTable>, out: &mut Vec<faucet_core::DatasetDescriptor>| {
        if let Some((schema, table, est, cols)) = cur {
            let query = format!("SELECT * FROM {}.{}", quote(&schema), quote(&table));
            let mut d = faucet_core::DatasetDescriptor::new(
                format!("{schema}.{table}"),
                "table",
                serde_json::json!({ "query": query }),
            )
            .with_schema(faucet_core::columns_to_schema(cols));
            // reltuples is -1 for a never-analyzed table — no estimate.
            if let Some(n) = est
                && n >= 0
            {
                d = d.with_estimated_rows(n as u64);
            }
            out.push(d);
        }
    };

    for (schema, table, column, data_type, is_nullable, est) in rows {
        let same = current
            .as_ref()
            .is_some_and(|(s, t, _, _)| *s == schema && *t == table);
        if !same {
            flush(current.take(), &mut out);
            current = Some((schema, table, est, Vec::new()));
        }
        let mut fragment = faucet_core::sql_type_to_json_schema(&data_type);
        if is_nullable {
            fragment = faucet_core::nullable_type(fragment);
        }
        if let Some((_, _, _, cols)) = current.as_mut() {
            cols.push((column, fragment));
        }
    }
    flush(current, &mut out);
    out
}

/// Convert a single `PgRow` into a JSON object whose keys are the row's
/// column names.
/// How JSON columns are read: the `json_big_numbers` mode and the columns
/// already warned about.
struct JsonContext<'a> {
    mode: faucet_core::JsonBigNumbers,
    warned: &'a Mutex<std::collections::HashSet<String>>,
}

fn row_to_json(row: &sqlx::postgres::PgRow, json: &JsonContext<'_>) -> Result<Value, FaucetError> {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = pg_value_to_json(row, &name, json)?;
        map.insert(name, value);
    }
    Ok(Value::Object(map))
}

#[async_trait]
impl faucet_core::Source for PostgresSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = resolve_query(&self.config, context);
        let query_str = self.typed_query(self.shard_wrap(query_str)).await;
        let query = bind_params(sqlx::query(&query_str), &self.config.params, &bind_values)?;

        let rows = bounded_read(self.config.read_timeout_secs, query.fetch_all(&self.pool)).await?;

        let records: Vec<Value> = rows
            .iter()
            .map(|r| row_to_json(r, &self.json_context()))
            .collect::<Result<_, _>>()?;
        tracing::info!(rows = records.len(), query = %self.config.query, "PostgreSQL source fetch complete");
        Ok(records)
    }

    /// Stream rows from the underlying sqlx cursor without buffering the full
    /// result set. Each emitted [`StreamPage`] holds up to
    /// [`PostgresSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. The
    /// postgres query source has no incremental-replication mode today, so
    /// every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query_str, bind_values) = resolve_query(&self.config, context);
            let query_str = self.typed_query(self.shard_wrap(query_str)).await;
            let query = bind_params(
                sqlx::query(&query_str),
                &self.config.params,
                &bind_values,
            )?;

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while let Some(row) = bounded_read(self.config.read_timeout_secs, rows.try_next()).await? {
                buffer.push(row_to_json(&row, &self.json_context())?);
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
                "PostgreSQL source stream complete",
            );
        })
    }

    fn connector_name(&self) -> &'static str {
        "postgres"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSourceConfig))
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

    /// Enumerate every base table outside `pg_catalog` / `information_schema`
    /// (a partitioned table once, as its parent — never also its partitions),
    /// with column types from `information_schema.columns` and a row estimate
    /// from `pg_class.reltuples` (catalog metadata only — no data scan).
    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let sql = r#"
            SELECT c.table_schema, c.table_name, c.column_name, c.data_type,
                   (c.is_nullable = 'YES') AS is_nullable,
                   (SELECT pc.reltuples::bigint
                      FROM pg_class pc
                      JOIN pg_namespace pn ON pn.oid = pc.relnamespace
                     WHERE pn.nspname = c.table_schema
                       AND pc.relname = c.table_name) AS estimated_rows
              FROM information_schema.columns c
              JOIN information_schema.tables t
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
             WHERE t.table_type = 'BASE TABLE'
               AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
               AND NOT EXISTS (
                     SELECT 1
                       FROM pg_class pc
                       JOIN pg_namespace pn ON pn.oid = pc.relnamespace
                      WHERE pn.nspname = t.table_schema
                        AND pc.relname = t.table_name
                        AND pc.relispartition)
             ORDER BY c.table_schema, c.table_name, c.ordinal_position"#;
        let rows = sqlx::query(sql)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Source(format!("postgres: catalog discovery failed: {e}")))?;

        let catalog: Vec<CatalogRow> = rows
            .iter()
            .map(|row| -> Result<CatalogRow, FaucetError> {
                let decode = |col: &str| -> Result<String, FaucetError> {
                    row.try_get::<String, _>(col).map_err(|e| {
                        FaucetError::Source(format!("postgres: catalog decode failed ({col}): {e}"))
                    })
                };
                Ok((
                    decode("table_schema")?,
                    decode("table_name")?,
                    decode("column_name")?,
                    decode("data_type")?,
                    row.try_get::<bool, _>("is_nullable").unwrap_or(true),
                    row.try_get::<i64, _>("estimated_rows").ok(),
                ))
            })
            .collect::<Result<_, _>>()?;

        let keys = primary_keys(&self.pool).await?;
        Ok(faucet_core::attach_primary_keys(
            descriptors_from_catalog(catalog, quote_ident),
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
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        let Some(shard_cfg) = &self.config.shard else {
            return Ok(vec![ShardSpec::whole()]);
        };

        let bounds_sql =
            pk_bounds_query(&self.config.query, &quote_ident(&shard_cfg.key), "BIGINT");
        let row = bind_params(sqlx::query(&bounds_sql), &self.config.params, &[])?
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "postgres: failed to compute shard bounds for key {:?} \
                     (it must be an integer-typed column): {e}",
                    shard_cfg.key
                ))
            })?;

        let lo: Option<i64> = row.try_get("lo").map_err(|e| {
            FaucetError::Source(format!("postgres: shard bounds decode failed: {e}"))
        })?;
        let hi: Option<i64> = row.try_get("hi").map_err(|e| {
            FaucetError::Source(format!("postgres: shard bounds decode failed: {e}"))
        })?;
        Ok(pk_shards_from_bounds(&shard_cfg.key, lo, hi, target))
    }

    /// Narrow this source to a single PK-range shard. The whole-dataset shard
    /// clears any applied range (streams the full query).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        *self.applied_shard.lock().expect("shard mutex poisoned") =
            parse_pk_shard(shard, "postgres")?;
        Ok(())
    }

    /// Server-side content digest of one key range (#701): `count(*)`, the sum
    /// of a 60-bit prefix of each row's `md5` over its compared columns, and
    /// the key bounds — all inside Postgres, so a matching range ships nothing.
    /// Algorithm [`DIGEST_ALGORITHM`]; comparable only with the same algorithm.
    async fn range_digest(
        &self,
        range: &faucet_core::diff::KeyRange,
        key: &str,
        columns: &[String],
    ) -> Result<Option<faucet_core::diff::ServerDigest>, FaucetError> {
        let bounds = PkShardBounds::from_spec(&range.to_shard(key))
            .ok_or_else(|| FaucetError::Source("postgres: invalid digest range".into()))?;
        let inner = bounds.wrap(&self.config.query, quote_ident);
        let sql = digest_query(&inner, key, columns);
        let row = bind_params(sqlx::query(&sql), &self.config.params, &[])?
            .fetch_one(&self.pool)
            .await
            .map_err(|e| FaucetError::Source(format!("postgres: range digest failed: {e}")))?;
        let rows: i64 = row
            .try_get("rows")
            .map_err(|e| FaucetError::Source(format!("postgres: digest decode: {e}")))?;
        let digest: String = row
            .try_get("digest")
            .map_err(|e| FaucetError::Source(format!("postgres: digest decode: {e}")))?;
        let key_min: Option<i64> = row
            .try_get("key_min")
            .map_err(|e| FaucetError::Source(format!("postgres: digest decode: {e}")))?;
        let key_max: Option<i64> = row
            .try_get("key_max")
            .map_err(|e| FaucetError::Source(format!("postgres: digest decode: {e}")))?;
        Ok(Some(faucet_core::diff::ServerDigest {
            algorithm: DIGEST_ALGORITHM.to_string(),
            rows: rows.max(0) as u64,
            digest,
            key_min,
            key_max,
        }))
    }
}

/// The server-side digest algorithm id. Two sides compare only when both
/// report it: each backend hashes its *own* text rendering of a row.
pub const DIGEST_ALGORITHM: &str = "postgres:md5-60-sum:v1";

/// The digest statement over an already range-wrapped `inner` query: one row
/// with `rows`, `digest` (the exact decimal sum, as text — it exceeds a
/// bigint), `key_min` and `key_max`. Every compared column is rendered as text
/// with a control-character sentinel for NULL (so NULL ≠ empty string) and
/// joined with `chr(31)`; the key column leads.
pub fn digest_query(inner: &str, key: &str, columns: &[String]) -> String {
    let mut cols: Vec<&str> = vec![key];
    cols.extend(columns.iter().map(String::as_str).filter(|c| *c != key));
    let rendered: Vec<String> = cols
        .iter()
        .map(|c| format!("coalesce({}::text, chr(1) || 'null')", quote_ident(c)))
        .collect();
    format!(
        "SELECT count(*)::bigint AS rows,          coalesce(sum(('x' || left(md5(concat_ws(chr(31), {cols})), 15))::bit(60)::bigint), 0)::text AS digest,          min({k})::bigint AS key_min, max({k})::bigint AS key_max          FROM ({inner}) AS _faucet_digest",
        cols = rendered.join(", "),
        k = quote_ident(key),
    )
}

/// Every primary-key column in the database, `(table, column)` in key order.
async fn primary_keys(
    pool: &sqlx::Pool<sqlx::Postgres>,
) -> Result<Vec<(String, String)>, FaucetError> {
    let key_sql = r#"
            SELECT tc.table_schema || '.' || tc.table_name AS name, k.column_name
              FROM information_schema.table_constraints tc
              JOIN information_schema.key_column_usage k
                ON k.constraint_schema = tc.constraint_schema
               AND k.constraint_name = tc.constraint_name
               AND k.table_schema = tc.table_schema
               AND k.table_name = tc.table_name
             WHERE tc.constraint_type = 'PRIMARY KEY'
               AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
             ORDER BY tc.table_schema, tc.table_name, k.ordinal_position"#;
    sqlx::query_as(key_sql)
        .fetch_all(pool)
        .await
        .map_err(key_discovery_error)
}

fn key_discovery_error(e: sqlx::Error) -> FaucetError {
    FaucetError::Source(format!("postgres: primary-key discovery failed: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_values_use_their_shortest_form() {
        assert_eq!(float4_json(0.1), serde_json::json!(0.1));
        assert_eq!(float4_json(f32::INFINITY), serde_json::json!("Infinity"));
    }

    #[tokio::test]
    async fn bounded_read_times_out_and_passes_through() {
        let slow = async {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            Ok::<_, sqlx::Error>(1)
        };
        tokio::time::pause();
        let err = bounded_read(1, slow).await.unwrap_err();
        assert!(err.to_string().contains("read_timeout_secs"), "{err}");
        assert_eq!(
            bounded_read(0, async { Ok::<_, sqlx::Error>(2) })
                .await
                .unwrap(),
            2
        );
        let failed = bounded_read(5, async { Err::<i32, _>(sqlx::Error::RowNotFound) })
            .await
            .unwrap_err();
        assert!(
            failed.to_string().contains("PostgreSQL query failed"),
            "{failed}"
        );
    }

    #[test]
    fn text_cast_query_wraps_only_columns_without_a_native_decode() {
        let cols = |c: &[(&str, &str)]| -> Vec<(String, String)> {
            c.iter()
                .map(|(n, t)| (n.to_string(), t.to_string()))
                .collect()
        };
        assert_eq!(
            text_cast_query("SELECT 1", &cols(&[("id", "INT4"), ("v", "jsonb")])),
            None
        );
        assert_eq!(
            text_cast_query(
                "SELECT * FROM t",
                &cols(&[("id", "INT4"), ("tags", "TEXT[]")])
            )
            .as_deref(),
            Some(
                "SELECT q.\"id\", CASE WHEN q.\"tags\" IS NULL THEN NULL \
                 ELSE format('%s', q.\"tags\") END AS \"tags\" FROM (SELECT * FROM t) AS q"
            )
        );
        assert_eq!(
            text_cast_query("SELECT a, a", &cols(&[("a", "NUMERIC"), ("a", "NUMERIC")])),
            None,
            "duplicate names cannot be addressed from the wrapper"
        );
    }

    #[test]
    fn non_finite_floats_keep_their_postgres_spelling() {
        assert_eq!(float_json(1.5), serde_json::json!(1.5));
        assert_eq!(float_json(f64::NAN), serde_json::json!("NaN"));
        assert_eq!(float_json(f64::INFINITY), serde_json::json!("Infinity"));
        assert_eq!(
            float_json(f64::NEG_INFINITY),
            serde_json::json!("-Infinity")
        );
    }
    use faucet_core::shard::plan_pk_shards;

    #[test]
    fn digest_query_leads_with_the_key_and_marks_nulls() {
        let sql = digest_query("SELECT * FROM t", "id", &["v".into(), "id".into()]);
        assert!(sql.starts_with("SELECT count(*)::bigint AS rows"), "{sql}");
        assert!(
            sql.contains("concat_ws(chr(31), coalesce(\"id\"::text, chr(1) || 'null'), coalesce(\"v\"::text, chr(1) || 'null'))"),
            "key first, deduplicated: {sql}"
        );
        assert!(sql.contains("min(\"id\")::bigint AS key_min"));
        assert!(sql.ends_with("FROM (SELECT * FROM t) AS _faucet_digest"));
    }

    /// The shard-bounds type moved to `faucet_core::shard` (#262) so the
    /// PK-range logic is shared across the SQL sources; alias it so the
    /// long-standing tests below keep pinning postgres's behavior unchanged.
    type ShardBounds = PkShardBounds;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = PostgresSourceConfig::new("postgres://localhost/test", "SELECT 1");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match PostgresSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
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
        // The key precision bug: 2^53 + 1 must NOT be bound as f64 (which would
        // round it). It is a valid i64, so it must classify as I64.
        let v = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(classify_number(&num(serde_json::json!(v))), NumberBind::I64);
    }

    #[test]
    fn classify_i64_max_is_i64() {
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
        // i64::MAX + 1 has no i64 representation but fits u64.
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
        assert_eq!(
            classify_number(&num(serde_json::json!(-0.5))),
            NumberBind::F64
        );
    }

    // ── PK-range sharding (pure logic) ──────────────────────────────────────

    #[test]
    fn plan_pk_shards_covers_full_range_without_gaps_or_overlap() {
        let shards = plan_pk_shards("id", 0, 99, 4);
        assert_eq!(shards.len(), 4);
        // Contiguous half-open interior cuts; boundary shards are open-ended.
        let mut expected_lo = 0i64;
        for (i, s) in shards.iter().enumerate() {
            let d = &s.descriptor;
            assert_eq!(d["key"], "id");
            assert_eq!(d["lo"].as_i64().unwrap(), expected_lo);
            let hi = d["hi"].as_i64().unwrap();
            let first = i == 0;
            let last = i == shards.len() - 1;
            assert_eq!(d["lo_unbounded"].as_bool().unwrap(), first);
            assert_eq!(d["hi_unbounded"].as_bool().unwrap(), last);
            expected_lo = hi; // next shard starts where this half-open one ended
        }
    }

    #[test]
    fn plan_pk_shards_never_more_shards_than_values() {
        // Range [5, 7] has 3 values; asking for 10 shards yields at most 3.
        let shards = plan_pk_shards("pk", 5, 7, 10);
        assert!(shards.len() <= 3, "got {} shards", shards.len());
        assert!(
            shards[0].descriptor["lo_unbounded"].as_bool().unwrap(),
            "first shard is unbounded below"
        );
        assert!(
            shards.last().unwrap().descriptor["hi_unbounded"]
                .as_bool()
                .unwrap(),
            "last shard is unbounded above"
        );
    }

    #[test]
    fn plan_pk_shards_single_value_one_shard() {
        let shards = plan_pk_shards("id", 42, 42, 8);
        assert_eq!(shards.len(), 1);
        // A lone shard is open-ended on both sides → the whole dataset.
        assert!(shards[0].descriptor["lo_unbounded"].as_bool().unwrap());
        assert!(shards[0].descriptor["hi_unbounded"].as_bool().unwrap());
    }

    #[test]
    fn plan_pk_shards_target_zero_treated_as_one() {
        let shards = plan_pk_shards("id", 0, 9, 0);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].descriptor["hi"].as_i64().unwrap(), 9);
    }

    #[test]
    fn shard_bounds_wrap_builds_half_open_predicate() {
        // An interior shard (bounded both sides) is half-open `[lo, hi)`.
        let spec = ShardSpec::new(
            "1",
            serde_json::json!({"key": "id", "lo": 100, "hi": 200, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t", quote_ident);
        assert!(sql.contains("(SELECT * FROM t) AS _faucet_shard"));
        assert!(sql.contains(r#""id" >= 100"#), "got: {sql}");
        assert!(
            sql.contains(r#""id" < 200"#),
            "half-open upper bound: {sql}"
        );
    }

    #[test]
    fn shard_bounds_wrap_first_shard_has_no_lower_bound() {
        // F54: the first shard omits the `>= lo` floor so keys below the
        // enumerated MIN are still read.
        let spec = ShardSpec::new(
            "0",
            serde_json::json!({"key": "id", "lo": 0, "hi": 100, "lo_unbounded": true, "hi_unbounded": false}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t", quote_ident);
        assert!(sql.contains(r#""id" < 100"#), "upper bound present: {sql}");
        assert!(!sql.contains(">="), "first shard has no lower floor: {sql}");
    }

    #[test]
    fn shard_bounds_wrap_last_shard_has_no_upper_bound() {
        // F55: the last shard omits the upper bound so keys above the
        // enumerated MAX are still read.
        let spec = ShardSpec::new(
            "2",
            serde_json::json!({"key": "id", "lo": 200, "hi": 300, "lo_unbounded": false, "hi_unbounded": true}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t", quote_ident);
        assert!(sql.contains(r#""id" >= 200"#), "lower bound present: {sql}");
        assert!(
            !sql.contains(" < ") && !sql.contains("<="),
            "last shard has no upper bound: {sql}"
        );
    }

    #[test]
    fn shard_bounds_quotes_key_against_injection() {
        let spec = ShardSpec::new(
            "0",
            serde_json::json!({"key": "weird\"; DROP", "lo": 0, "hi": 1, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT 1", quote_ident);
        // The doubled quote escaping proves the identifier was quoted, not raw.
        assert!(
            sql.contains(r#""weird""; DROP""#),
            "key must be quoted: {sql}"
        );
    }

    #[test]
    fn shard_bounds_from_spec_rejects_malformed_descriptor() {
        let spec = ShardSpec::new("0", serde_json::json!({"key": "id"})); // no lo/hi
        assert!(ShardBounds::from_spec(&spec).is_none());
        assert!(ShardBounds::from_spec(&ShardSpec::whole()).is_none());
    }

    // ── F37: NULL-key shard coverage ────────────────────────────────────────

    #[test]
    fn exactly_one_shard_includes_null() {
        let shards = plan_pk_shards("id", 0, 99, 5);
        let null_owners: Vec<usize> = shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.descriptor["include_null"].as_bool().unwrap_or(false))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            null_owners,
            vec![shards.len() - 1],
            "exactly the last shard owns NULL keys"
        );
    }

    #[test]
    fn single_shard_plan_still_owns_null() {
        // A single value yields one shard; it must still cover NULL keys.
        let shards = plan_pk_shards("id", 7, 7, 4);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].descriptor["include_null"].as_bool().unwrap());
    }

    #[test]
    fn last_shard_wrap_emits_is_null_clause() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        let last = ShardBounds::from_spec(shards.last().unwrap()).unwrap();
        let sql = last.wrap("SELECT * FROM t", quote_ident);
        assert!(
            sql.contains(r#""id" IS NULL"#),
            "last shard must match NULL keys: {sql}"
        );
        assert!(sql.contains(" OR "), "NULL clause OR'd with range: {sql}");
    }

    #[test]
    fn non_last_shard_wrap_omits_is_null_clause() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        // First shard is not the last → no NULL clause.
        let first = ShardBounds::from_spec(&shards[0]).unwrap();
        let sql = first.wrap("SELECT * FROM t", quote_ident);
        assert!(
            !sql.contains("IS NULL"),
            "non-last shard must not match NULL keys: {sql}"
        );
    }

    /// Property check on the generated predicates: OR-ing every shard's WHERE
    /// predicate must cover (a) every non-NULL key — including values *outside*
    /// the enumerated `[min, max]` (F54/F55) — exactly once and (b) NULL keys
    /// exactly once.
    #[test]
    fn predicate_coverage_complete_and_non_overlapping() {
        let (min, max, target) = (0i64, 19i64, 4usize);
        let bounds: Vec<ShardBounds> = plan_pk_shards("k", min, max, target)
            .iter()
            .map(|s| ShardBounds::from_spec(s).unwrap())
            .collect();

        // The boundary shards model SQL membership: open below for the first
        // shard, open above for the last.
        let matches_key = |b: &ShardBounds, key: i64| -> bool {
            let lower = b.lo_unbounded || key >= b.lo;
            let upper = b.hi_unbounded || key < b.hi;
            lower && upper
        };

        // (a) Every non-NULL key — well below min, in range, and well above max
        // — matches exactly one shard. Keys outside [min, max] model rows
        // inserted/backfilled during the coordinate→execute window.
        for key in (min - 50)..=(max + 50) {
            let matches = bounds.iter().filter(|b| matches_key(b, key)).count();
            assert_eq!(matches, 1, "key {key} matched {matches} shards (want 1)");
        }

        // (b) NULL keys match exactly one shard (the one with include_null).
        let null_matches = bounds.iter().filter(|b| b.include_null).count();
        assert_eq!(null_matches, 1, "NULL keys must match exactly one shard");
    }

    #[test]
    fn single_shard_wrap_selects_whole_dataset_including_null() {
        // A lone open-ended shard must select every row, NULL keys included.
        let shards = plan_pk_shards("id", 7, 7, 1);
        assert_eq!(shards.len(), 1);
        let b = ShardBounds::from_spec(&shards[0]).unwrap();
        let sql = b.wrap("SELECT * FROM t", quote_ident);
        assert!(sql.contains("WHERE TRUE"), "whole-dataset predicate: {sql}");
        assert!(!sql.contains(">="), "no bounds on a lone shard: {sql}");
    }

    // ── discover: pure catalog-row grouping ─────────────────────────────────

    #[test]
    fn descriptors_group_catalog_rows_per_table() {
        let rows = vec![
            (
                "public".to_string(),
                "orders".to_string(),
                "id".to_string(),
                "integer".to_string(),
                false,
                Some(120i64),
            ),
            (
                "public".to_string(),
                "orders".to_string(),
                "note".to_string(),
                "text".to_string(),
                true,
                Some(120i64),
            ),
            (
                "sales".to_string(),
                "orders".to_string(),
                "total".to_string(),
                "numeric".to_string(),
                false,
                None,
            ),
        ];
        let ds = descriptors_from_catalog(rows, quote_ident);
        assert_eq!(ds.len(), 2, "same table name in two schemas = two datasets");

        assert_eq!(ds[0].name, "public.orders");
        assert_eq!(ds[0].kind, "table");
        assert_eq!(ds[0].estimated_rows, Some(120));
        assert_eq!(
            ds[0].config_patch["query"],
            r#"SELECT * FROM "public"."orders""#
        );
        let schema = ds[0].schema.as_ref().unwrap();
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(
            schema["properties"]["note"]["type"],
            serde_json::json!(["string", "null"])
        );

        assert_eq!(ds[1].name, "sales.orders");
        assert_eq!(ds[1].estimated_rows, None);
        assert_eq!(schema["type"], "object");
    }

    #[test]
    fn descriptors_negative_reltuples_means_no_estimate() {
        let rows = vec![(
            "public".to_string(),
            "fresh".to_string(),
            "id".to_string(),
            "bigint".to_string(),
            false,
            Some(-1i64),
        )];
        let ds = descriptors_from_catalog(rows, quote_ident);
        assert_eq!(ds.len(), 1);
        assert_eq!(ds[0].estimated_rows, None, "-1 = never analyzed");
    }

    #[test]
    fn descriptors_quote_hostile_identifiers() {
        let rows = vec![(
            "public".to_string(),
            "weird\"; DROP".to_string(),
            "id".to_string(),
            "integer".to_string(),
            false,
            None,
        )];
        let ds = descriptors_from_catalog(rows, quote_ident);
        let q = ds[0].config_patch["query"].as_str().unwrap();
        assert!(q.contains(r#""weird""; DROP""#), "quoted identifier: {q}");
    }

    #[test]
    fn descriptors_empty_catalog_is_empty() {
        assert!(descriptors_from_catalog(Vec::new(), quote_ident).is_empty());
    }

    #[tokio::test]
    async fn source_advertises_discover() {
        use faucet_core::Source as _;
        let config = PostgresSourceConfig::new("postgres://u@127.0.0.1:1/db", "SELECT 1");
        let source = lazy_source(config);
        assert!(source.supports_discover());
        // Against an unreachable server the catalog query surfaces the typed
        // discovery error (exercises the error path without Docker).
        let err = source.discover().await.unwrap_err();
        assert!(
            err.to_string().contains("catalog discovery failed"),
            "typed error: {err}"
        );
    }

    // dataset_uri is a pure-config method; the source requires a live DB to
    // construct so we test it via a config-derived assertion instead.
    #[test]
    fn dataset_uri_strips_credentials() {
        // We cannot construct PostgresSource offline, so we verify the
        // credential-stripping logic used by dataset_uri() directly.
        let redacted = faucet_core::redact_uri_credentials("postgres://u:p@h:5432/db");
        let uri = format!("{}?query={}", redacted, "SELECT 1");
        assert_eq!(uri, "postgres://h:5432/db?query=SELECT 1");
    }

    /// Build a source over a lazy pool (no server needed) so the shard glue —
    /// `apply_shard`, `shard_wrap`, and `enumerate_shards`' error path — is
    /// testable without Docker.
    fn lazy_source(config: PostgresSourceConfig) -> PostgresSource {
        let pool = PgPoolOptions::new()
            // Fail fast at first checkout — these tests never reach a server.
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy(&config.connection_url)
            .expect("lazy pool");
        PostgresSource {
            config,
            pool,
            applied_shard: Mutex::new(None),
            json_warned: Mutex::new(Default::default()),
        }
    }

    #[tokio::test]
    async fn apply_shard_then_shard_wrap_narrows_query() {
        use faucet_core::Source as _;
        let mut config =
            PostgresSourceConfig::new("postgres://u@127.0.0.1:1/db", "SELECT * FROM t");
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

        // A real shard narrows with ANSI double-quote quoting.
        let spec = &plan_pk_shards("id", 0, 99, 2)[0];
        source.apply_shard(spec).await.unwrap();
        let wrapped = source.shard_wrap("SELECT * FROM t".into());
        assert!(wrapped.contains(r#""id""#), "got: {wrapped}");
        assert!(wrapped.contains("_faucet_shard"), "got: {wrapped}");

        // Enumeration against the unreachable server surfaces the bounds-probe
        // error path.
        let err = source.enumerate_shards(4).await.unwrap_err();
        assert!(
            err.to_string().contains("shard bounds"),
            "expected bounds-probe error, got: {err}"
        );
    }
}

#[cfg(test)]
mod bind_overflow_tests {
    use super::*;
    use serde_json::json;

    /// #462: above `i64::MAX` Postgres has no type that fits, and `as i64` would
    /// wrap to a negative. Refuse loudly instead — silently binding a negative
    /// bookmark would make `WHERE key > $1` re-read or skip rows.
    #[test]
    fn u64_above_i64_max_is_refused_not_wrapped() {
        let err = match bind_params(sqlx::query("SELECT 1"), &[json!(u64::MAX)], &[]) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("u64::MAX must not bind"),
        };
        assert!(err.contains(&u64::MAX.to_string()), "{err}");
        assert!(
            !err.contains("-9223372036854775808"),
            "must not show the wrap: {err}"
        );
    }

    #[test]
    fn values_a_signed_column_can_hold_still_bind() {
        for v in [json!(0), json!(-1), json!(i64::MAX), json!(i64::MAX as u64)] {
            assert!(
                bind_params(sqlx::query("SELECT 1"), std::slice::from_ref(&v), &[]).is_ok(),
                "{v} must still bind"
            );
        }
    }

    #[test]
    fn key_discovery_errors_name_the_backend() {
        let e = key_discovery_error(sqlx::Error::RowNotFound);
        assert!(
            e.to_string()
                .contains("postgres: primary-key discovery failed"),
            "{e}"
        );
    }
}

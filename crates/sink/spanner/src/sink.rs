//! Cloud Spanner sink implementation.
//!
//! Records are written as batched **mutations** (`Insert` for append,
//! `InsertOrUpdate` for upsert, `Delete` for delete mode), one atomic
//! `Commit` per chunk. Values are encoded against the destination column
//! types read once from `INFORMATION_SCHEMA` (see
//! [`faucet_common_spanner::encode`]). Exactly-once delivery commits the
//! page's mutations and a `faucet_commit_token` watermark row in a single
//! read-write transaction.

use crate::config::SpannerSinkConfig;
use async_trait::async_trait;
use faucet_common_spanner::decode::SpannerJson;
use faucet_common_spanner::encode::{EncodedKind, encode_to_kind};
use faucet_common_spanner::quote_ident_spanner;
use faucet_common_spanner::types::{SpannerType, parse_spanner_type, spanner_type_to_json_schema};
use faucet_core::FaucetError;
use gcloud_googleapis::spanner::admin::database::v1::UpdateDatabaseDdlRequest;
use gcloud_googleapis::spanner::v1::Mutation;
use gcloud_spanner::client::Client;
use gcloud_spanner::key::Key;
use gcloud_spanner::mutation::{delete, insert, insert_or_update};
use gcloud_spanner::statement::{Statement, ToKind};
use gcloud_spanner::value::CommitTimestamp;
use prost_types::value::Kind;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

/// Conservative bound on mutated cells per commit. Spanner rejects commits
/// above ~80,000 mutated cells (rows × columns, **plus** secondary-index
/// amplification the client cannot see), so the sink chunks well below it.
const CELL_BUDGET: usize = 60_000;

/// Live table metadata read from `INFORMATION_SCHEMA` (cached per sink).
#[derive(Debug, Clone)]
struct TableMeta {
    /// `(name, type, nullable)` in declared column order.
    columns: Vec<(String, SpannerType, bool)>,
    /// PRIMARY KEY column names, in key order.
    pk: Vec<String>,
}

impl TableMeta {
    fn type_of(&self, col: &str) -> Option<&SpannerType> {
        self.columns
            .iter()
            .find(|(name, _, _)| name == col)
            .map(|(_, ty, _)| ty)
    }

    fn has_column(&self, col: &str) -> bool {
        self.columns.iter().any(|(name, _, _)| name == col)
    }
}

/// One planned mutation plus the number of cells it mutates (drives the
/// per-commit cell budget).
#[derive(Debug)]
struct Planned {
    mutation: Mutation,
    cells: usize,
}

/// Which write mutation to build for a data row.
#[derive(Clone, Copy, PartialEq)]
enum WriteOp {
    /// Append mode — duplicate PKs fail the commit.
    Insert,
    /// Upsert mode — Spanner's native keyed upsert.
    InsertOrUpdate,
}

/// Build the mutation for one record: columns are the intersection of the
/// record's top-level fields with the table's columns, encoded per column
/// type. Fields with no matching column are dropped with a one-shot warning
/// (`warned` dedups across the sink's lifetime). A record with *no* matching
/// column, a non-object record, or an encode failure is an `Err` naming the
/// column.
fn build_row_mutation(
    table: &str,
    record: &Value,
    meta: &TableMeta,
    op: WriteOp,
    warned: &mut HashSet<String>,
) -> Result<Planned, String> {
    let obj = record
        .as_object()
        .ok_or_else(|| "record is not a JSON object".to_string())?;

    let mut cols: Vec<&str> = Vec::new();
    let mut vals: Vec<EncodedKind> = Vec::new();
    for (name, ty, _) in &meta.columns {
        if let Some(v) = obj.get(name) {
            let kind = encode_to_kind(v, ty).map_err(|e| format!("column `{name}`: {e}"))?;
            cols.push(name.as_str());
            vals.push(EncodedKind(kind));
        }
    }
    for key in obj.keys() {
        if !meta.has_column(key) && warned.insert(key.clone()) {
            tracing::warn!(
                field = %key,
                table = %table,
                "record field has no matching Spanner column; dropping it (warned once per field)"
            );
        }
    }
    if cols.is_empty() {
        return Err("record has no fields matching table columns".into());
    }

    let refs: Vec<&dyn ToKind> = vals.iter().map(|v| v as &dyn ToKind).collect();
    let mutation = match op {
        WriteOp::Insert => insert(table, &cols, &refs),
        WriteOp::InsertOrUpdate => insert_or_update(table, &cols, &refs),
    };
    Ok(Planned {
        mutation,
        cells: cols.len(),
    })
}

/// Encode a key tuple into **PRIMARY KEY column order**, which may differ from
/// the configured `key` order — Spanner keys are always addressed in PK order.
///
/// Encoding through the destination column type is also what makes two key
/// values comparable: an INT64 key written as the JSON number `1` and the same
/// key read back from Spanner both land as `StringValue("1")`.
fn encode_key_in_pk_order(
    key_tuple: &faucet_core::KeyTuple,
    meta: &TableMeta,
) -> Result<Vec<Kind>, String> {
    let mut vals: Vec<Kind> = Vec::with_capacity(meta.pk.len());
    for pk_col in &meta.pk {
        let (_, v) = key_tuple
            .0
            .iter()
            .find(|(col, _)| col == pk_col)
            .ok_or_else(|| format!("delete key is missing PK column `{pk_col}`"))?;
        let ty = meta
            .type_of(pk_col)
            .ok_or_else(|| format!("PK column `{pk_col}` not found in table metadata"))?;
        vals.push(encode_to_kind(v, ty).map_err(|e| format!("key column `{pk_col}`: {e}"))?);
    }
    Ok(vals)
}

/// Wrap PK-ordered encoded values into a `Delete` mutation.
fn delete_mutation_from_kinds(table: &str, kinds: Vec<Kind>) -> Planned {
    let cells = kinds.len().max(1);
    let vals: Vec<EncodedKind> = kinds.into_iter().map(EncodedKind).collect();
    let refs: Vec<&dyn ToKind> = vals.iter().map(|v| v as &dyn ToKind).collect();
    Planned {
        mutation: delete(table, Key::composite(&refs)),
        cells,
    }
}

/// Build the delete mutation for one planned key tuple.
fn build_delete_mutation(
    table: &str,
    key_tuple: &faucet_core::KeyTuple,
    meta: &TableMeta,
) -> Result<Planned, String> {
    Ok(delete_mutation_from_kinds(
        table,
        encode_key_in_pk_order(key_tuple, meta)?,
    ))
}

/// Split planned mutations into commit-sized chunks: at most `batch_size`
/// rows (0 = unbounded) and at most [`CELL_BUDGET`] cells per chunk. A single
/// row above the budget still ships alone — Spanner, not the sink, is the
/// authority on rejecting it.
fn chunk_by_cells(planned: Vec<Planned>, batch_size: usize, budget: usize) -> Vec<Vec<Mutation>> {
    let row_cap = if batch_size == 0 {
        usize::MAX
    } else {
        batch_size
    };
    let mut chunks: Vec<Vec<Mutation>> = Vec::new();
    let mut current: Vec<Mutation> = Vec::new();
    let mut cells = 0usize;
    for p in planned {
        if !current.is_empty() && (cells + p.cells > budget || current.len() >= row_cap) {
            chunks.push(std::mem::take(&mut current));
            cells = 0;
        }
        cells += p.cells;
        current.push(p.mutation);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

// ---------------------------------------------------------------------------
// Scoped cleanup (issue #478)
// ---------------------------------------------------------------------------

/// SQL expression converting the STRING-bound scope parameter `@name` into the
/// destination column's type.
///
/// Scope values are bound as STRING and cast in SQL rather than bound at their
/// native type: the client derives a parameter's Spanner type from the *Rust*
/// type it is given, and a cleanup scope carries `serde_json::Value`s whose
/// destination type is only known at runtime from `INFORMATION_SCHEMA`. Casting
/// in the statement keeps one binding path for every column type — the same
/// trick the BigQuery cleanup uses with `JSON_VALUE(@scope, …)`.
///
/// Types with no equality semantics (JSON, ARRAY, STRUCT/PROTO) are refused
/// rather than silently compared.
fn scope_cast_expr(param: &str, ty: &SpannerType) -> Result<String, String> {
    Ok(match ty {
        // Already a STRING parameter — no conversion needed.
        SpannerType::String => format!("@{param}"),
        SpannerType::Bool => format!("CAST(@{param} AS BOOL)"),
        SpannerType::Int64 => format!("CAST(@{param} AS INT64)"),
        SpannerType::Float32 => format!("CAST(@{param} AS FLOAT32)"),
        SpannerType::Float64 => format!("CAST(@{param} AS FLOAT64)"),
        SpannerType::Timestamp => format!("CAST(@{param} AS TIMESTAMP)"),
        SpannerType::Date => format!("CAST(@{param} AS DATE)"),
        SpannerType::Numeric => format!("CAST(@{param} AS NUMERIC)"),
        // BYTES travel base64-encoded everywhere else in this connector, so
        // decode rather than reinterpreting the text's UTF-8 bytes.
        SpannerType::Bytes => format!("FROM_BASE64(@{param})"),
        SpannerType::Json | SpannerType::Array(_) | SpannerType::Other => {
            return Err(
                "column type does not support equality and cannot be used in a cleanup scope"
                    .into(),
            );
        }
    })
}

/// Canonical text form of a scope value for its STRING-bound parameter.
///
/// Routed through [`encode_to_kind`] so the scope value is validated against
/// the destination column type exactly like a written value would be (an INT64
/// column rejects `1.5` here rather than at the server).
fn scope_param_text(value: &Value, ty: &SpannerType) -> Result<String, String> {
    match encode_to_kind(value, ty)? {
        Kind::StringValue(s) => Ok(s),
        Kind::BoolValue(b) => Ok(b.to_string()),
        Kind::NumberValue(n) => Ok(n.to_string()),
        // `CleanupPolicy::new` already refuses a null scope value; this is the
        // backstop, because `col = NULL` is never true and would silently make
        // the cleanup a no-op instead of an error.
        Kind::NullValue(_) => Err("scope value is null".into()),
        _ => Err("scope value is not a scalar".into()),
    }
}

/// `SELECT <pk…> FROM <table> WHERE <col> = <expr> AND …` — reads the primary
/// keys of every row the invocation claimed to be authoritative for.
fn build_scope_select_sql(table: &str, pk: &[String], preds: &[(String, String)]) -> String {
    let cols = pk
        .iter()
        .map(|c| quote_ident_spanner(c))
        .collect::<Vec<_>>()
        .join(", ");
    let where_clause = preds
        .iter()
        .map(|(col, expr)| format!("{} = {expr}", quote_ident_spanner(col)))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "SELECT {cols} FROM {} WHERE {where_clause}",
        quote_ident_spanner(table)
    )
}

/// Canonical, hashable form of one encoded key value.
///
/// Length-prefixed so concatenating a composite key cannot alias: the tuples
/// `("ab", "c")` and `("a", "bc")` must not produce the same fingerprint.
fn kind_fingerprint(kind: &Kind) -> String {
    let (tag, body) = match kind {
        Kind::NullValue(_) => ('n', String::new()),
        Kind::BoolValue(b) => ('b', b.to_string()),
        Kind::NumberValue(f) => ('f', format!("{f:?}")),
        Kind::StringValue(s) => ('s', s.clone()),
        // A PK column is always a scalar, so these are unreachable in practice;
        // fall back to the structural form rather than aliasing distinct keys.
        other => ('x', format!("{other:?}")),
    };
    format!("{tag}{}:{body}", body.len())
}

/// Fingerprint of a whole PK-ordered key tuple.
fn tuple_fingerprint(vals: &[Kind]) -> String {
    vals.iter()
        .map(kind_fingerprint)
        .collect::<Vec<_>>()
        .join("|")
}

/// Pure: the live rows in the scope that this run did not write.
///
/// An empty `seen` set leaves every live row stale — the source reported the
/// scope empty, which is exactly the case scoped cleanup exists for.
fn stale_key_rows(live: Vec<Vec<Kind>>, seen: &HashSet<String>) -> Vec<Vec<Kind>> {
    live.into_iter()
        .filter(|vals| !seen.contains(&tuple_fingerprint(vals)))
        .collect()
}

/// Refuse a cleanup whose delete set would exceed the per-commit cell budget.
///
/// Why refuse instead of chunking: the read and the delete must be one
/// transaction (Spanner locks the rows it read, so nothing can slip into the
/// scope between deciding and deleting), and a transaction commits exactly
/// once. Chunking would therefore mean several commits — a partial cleanup,
/// with every chunk after the first deciding from an already-stale snapshot.
/// [`CELL_BUDGET`] rows is far above the per-parent scopes this feature targets
/// (and above what core's own key ceiling makes reachable), so refusing loudly
/// beats half-cleaning silently.
fn check_cleanup_cell_budget(
    rows: usize,
    cells_per_row: usize,
    budget: usize,
) -> Result<(), String> {
    let cells = rows.saturating_mul(cells_per_row.max(1));
    if cells > budget {
        return Err(format!(
            "{rows} stale row(s) in the claimed scope would mutate {cells} cells, above the \
             {budget}-cell budget for a single Spanner commit. Nothing was deleted — the read \
             and the delete must commit together, so this cannot be split without leaving the \
             destination half-cleaned. Narrow the completeness claim (`complete_for`) so each \
             invocation covers fewer rows"
        ));
    }
    Ok(())
}

/// Upsert/delete requires the configured `key` to be exactly the table's
/// PRIMARY KEY columns (Spanner mutations always key on the PK). Order is
/// irrelevant — set equality.
fn validate_key_matches_pk(key: &[String], pk: &[String], table: &str) -> Result<(), FaucetError> {
    let key_set: HashSet<&str> = key.iter().map(|s| s.as_str()).collect();
    let pk_set: HashSet<&str> = pk.iter().map(|s| s.as_str()).collect();
    if key_set != pk_set {
        return Err(FaucetError::Config(format!(
            "spanner sink: write key {key:?} must equal table `{table}`'s PRIMARY KEY columns \
             {pk:?} (Spanner mutations always key on the primary key)"
        )));
    }
    Ok(())
}

/// Map a [`faucet_core::SqlBaseType`] to the Spanner DDL keyword used when
/// adding a column during schema evolution. Integers land as `INT64` and
/// floats as `FLOAT64` — Spanner's widest numeric types.
fn spanner_keyword(t: faucet_core::SqlBaseType) -> &'static str {
    use faucet_core::SqlBaseType::*;
    match t {
        Integer => "INT64",
        Double => "FLOAT64",
        Boolean => "BOOL",
        Text => "STRING(MAX)",
        Json => "JSON",
    }
}

/// Render a [`SpannerType`] back to its DDL form, used when re-emitting a
/// column to relax `NOT NULL`. `Other` (STRUCT/PROTO) cannot be re-emitted.
fn spanner_type_ddl(ty: &SpannerType) -> Result<String, String> {
    Ok(match ty {
        SpannerType::Bool => "BOOL".into(),
        SpannerType::Int64 => "INT64".into(),
        SpannerType::Float32 => "FLOAT32".into(),
        SpannerType::Float64 => "FLOAT64".into(),
        SpannerType::Timestamp => "TIMESTAMP".into(),
        SpannerType::Date => "DATE".into(),
        SpannerType::String => "STRING(MAX)".into(),
        SpannerType::Bytes => "BYTES(MAX)".into(),
        SpannerType::Numeric => "NUMERIC".into(),
        SpannerType::Json => "JSON".into(),
        SpannerType::Array(inner) => format!("ARRAY<{}>", spanner_type_ddl(inner)?),
        SpannerType::Other => {
            return Err("cannot re-emit a STRUCT/PROTO column in DDL".into());
        }
    })
}

/// `ALTER TABLE <t> ADD COLUMN IF NOT EXISTS <c> <kw>` — idempotent addition.
/// New columns are always nullable (Spanner requires a default for NOT NULL
/// additions, and drift additions are nullable by construction).
fn build_add_column_sql(table: &str, col: &str, t: faucet_core::SqlBaseType) -> String {
    format!(
        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
        quote_ident_spanner(table),
        quote_ident_spanner(col),
        spanner_keyword(t)
    )
}

/// `ALTER TABLE <t> ALTER COLUMN <c> <TYPE>` — re-emitting the column at its
/// current type *without* `NOT NULL` relaxes the null constraint (Spanner has
/// no `DROP NOT NULL`). Idempotent.
fn build_alter_column_sql(table: &str, col: &str, type_ddl: &str) -> String {
    format!(
        "ALTER TABLE {} ALTER COLUMN {} {}",
        quote_ident_spanner(table),
        quote_ident_spanner(col),
        type_ddl
    )
}

/// Commit-token watermark table name. Spanner (GoogleSQL) identifiers must
/// start with a letter, so the canonical `_faucet_commit_token` used by the
/// other SQL sinks (`faucet_core::idempotency::COMMIT_TOKEN_TABLE`) is not a
/// valid Spanner table name — this sink drops the leading underscore.
pub(crate) const SPANNER_COMMIT_TOKEN_TABLE: &str = "faucet_commit_token";

/// DDL for the commit-token watermark table (exactly-once). `updated_at` is a
/// commit-timestamp column so replays are debuggable without a clock source.
fn commit_token_ddl() -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {t} ({s} STRING(MAX) NOT NULL, {k} STRING(MAX) NOT NULL, \
         updated_at TIMESTAMP OPTIONS (allow_commit_timestamp=true)) PRIMARY KEY ({s})",
        t = quote_ident_spanner(SPANNER_COMMIT_TOKEN_TABLE),
        s = quote_ident_spanner(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
        k = quote_ident_spanner(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
    )
}

/// Render cached metadata as the `infer_schema`-shaped object
/// (`{"type":"object","properties":{…}}`) the drift pass diffs against.
fn current_schema_json(meta: &TableMeta) -> Value {
    let mut props = serde_json::Map::new();
    for (name, ty, nullable) in &meta.columns {
        props.insert(name.clone(), spanner_type_to_json_schema(ty, *nullable));
    }
    serde_json::json!({ "type": "object", "properties": props })
}

fn sink_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("spanner {context}: {e}"))
}

/// A sink that writes JSON records to a Cloud Spanner table.
pub struct SpannerSink {
    config: SpannerSinkConfig,
    client: Client,
    /// Cached table metadata; `None` until first use, cleared by
    /// [`evolve_schema`](faucet_core::Sink::evolve_schema).
    meta: tokio::sync::RwLock<Option<Arc<TableMeta>>>,
    /// One-shot commit-token-table creation guard.
    token_table_ready: tokio::sync::OnceCell<()>,
    /// Record fields already warned-about as having no matching column.
    warned_fields: std::sync::Mutex<HashSet<String>>,
}

impl SpannerSink {
    /// Create a new Spanner sink. Validates the config and builds the client
    /// (session pool). Table metadata is read lazily on first write.
    pub async fn new(config: SpannerSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let client = config.connection.connect().await?;
        Ok(Self {
            config,
            client,
            meta: tokio::sync::RwLock::new(None),
            token_table_ready: tokio::sync::OnceCell::new(),
            warned_fields: std::sync::Mutex::new(HashSet::new()),
        })
    }

    /// Read `(columns, pk)` for the target table from `INFORMATION_SCHEMA`.
    /// Returns `Ok(None)` when the table does not exist.
    async fn fetch_meta(&self) -> Result<Option<TableMeta>, FaucetError> {
        let mut columns: Vec<(String, SpannerType, bool)> = Vec::new();
        {
            let mut tx = self
                .client
                .single()
                .await
                .map_err(|e| sink_err("metadata read", e))?;
            let mut stmt = Statement::new(
                "SELECT c.COLUMN_NAME AS name, c.SPANNER_TYPE AS spanner_type, \
                 c.IS_NULLABLE AS is_nullable FROM INFORMATION_SCHEMA.COLUMNS c \
                 WHERE c.TABLE_SCHEMA = '' AND c.TABLE_NAME = @table \
                 ORDER BY c.ORDINAL_POSITION",
            );
            stmt.add_param("table", &self.config.table_name);
            let mut iter = tx
                .query(stmt)
                .await
                .map_err(|e| sink_err("metadata query", e))?;
            while let Some(row) = iter
                .next()
                .await
                .map_err(|e| sink_err("metadata read", e))?
            {
                let name = row
                    .column_by_name::<String>("name")
                    .map_err(|e| sink_err("metadata decode", e))?;
                let ty = row
                    .column_by_name::<String>("spanner_type")
                    .map_err(|e| sink_err("metadata decode", e))?;
                let nullable = row
                    .column_by_name::<String>("is_nullable")
                    .map_err(|e| sink_err("metadata decode", e))?;
                columns.push((name, parse_spanner_type(&ty), nullable == "YES"));
            }
        }
        if columns.is_empty() {
            return Ok(None);
        }

        let mut pk: Vec<String> = Vec::new();
        {
            let mut tx = self
                .client
                .single()
                .await
                .map_err(|e| sink_err("metadata read", e))?;
            let mut stmt = Statement::new(
                "SELECT ic.COLUMN_NAME AS name FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic \
                 WHERE ic.TABLE_SCHEMA = '' AND ic.TABLE_NAME = @table \
                 AND ic.INDEX_NAME = 'PRIMARY_KEY' ORDER BY ic.ORDINAL_POSITION",
            );
            stmt.add_param("table", &self.config.table_name);
            let mut iter = tx
                .query(stmt)
                .await
                .map_err(|e| sink_err("primary-key query", e))?;
            while let Some(row) = iter
                .next()
                .await
                .map_err(|e| sink_err("primary-key read", e))?
            {
                pk.push(
                    row.column_by_name::<String>("name")
                        .map_err(|e| sink_err("primary-key decode", e))?,
                );
            }
        }
        Ok(Some(TableMeta { columns, pk }))
    }

    /// Cached metadata, fetched on first use. Errors when the table is absent
    /// — every caller here is a write path that requires it.
    async fn require_meta(&self, records: &[Value]) -> Result<Arc<TableMeta>, FaucetError> {
        if let Some(meta) = self.meta.read().await.as_ref() {
            return Ok(Arc::clone(meta));
        }
        let mut slot = self.meta.write().await;
        // Another writer may have raced us here.
        if let Some(meta) = slot.as_ref() {
            return Ok(Arc::clone(meta));
        }
        let fetched = match self.fetch_meta().await? {
            Some(m) => m,
            None => {
                self.create_target(records).await?;
                self.fetch_meta().await?.ok_or_else(|| {
                    FaucetError::Sink(format!(
                        "spanner table `{}` does not exist in {} even after CREATE TABLE",
                        self.config.table_name,
                        self.config.connection.database_path()
                    ))
                })?
            }
        };
        let arc = Arc::new(fetched);
        *slot = Some(Arc::clone(&arc));
        Ok(arc)
    }

    /// Plan the whole page into mutations (mode-aware). Returns the planned
    /// mutations and the written-row count, or the first failure.
    fn plan_page(
        &self,
        records: &[Value],
        meta: &TableMeta,
    ) -> Result<(Vec<Planned>, usize), FaucetError> {
        let table = &self.config.table_name;
        let mut warned = self
            .warned_fields
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let mut planned = Vec::with_capacity(records.len());
            for (idx, record) in records.iter().enumerate() {
                let p = build_row_mutation(table, record, meta, WriteOp::Insert, &mut warned)
                    .map_err(|msg| {
                        FaucetError::Sink(format!("spanner append: row {idx}: {msg}"))
                    })?;
                planned.push(p);
            }
            let count = planned.len();
            return Ok((planned, count));
        }

        validate_key_matches_pk(&self.config.write.key, &meta.pk, table)?;
        let plan = faucet_core::plan_writes(records, &self.config.write);
        if let Some((idx, msg)) = plan.failed.first() {
            return Err(FaucetError::Sink(format!(
                "spanner {}: row {idx}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        let mut planned = Vec::with_capacity(plan.upserts.len() + plan.deletes.len());
        for record in &plan.upserts {
            let p = build_row_mutation(table, record, meta, WriteOp::InsertOrUpdate, &mut warned)
                .map_err(|msg| FaucetError::Sink(format!("spanner upsert: {msg}")))?;
            planned.push(p);
        }
        for key_tuple in &plan.deletes {
            let p = build_delete_mutation(table, key_tuple, meta)
                .map_err(|msg| FaucetError::Sink(format!("spanner delete: {msg}")))?;
            planned.push(p);
        }
        let count = planned.len();
        Ok((planned, count))
    }

    /// Create the target table from the first page (#580).
    ///
    /// Spanner requires a primary key on every table and faucet has no basis
    /// to invent one, so this needs `key:` — which `write_mode: upsert`/
    /// `delete` already require and which Spanner requires to equal the PK.
    /// Without it, a made-up key column would be worse than a clear error:
    /// it becomes the table's physical layout and cannot be changed later.
    async fn create_target(&self, records: &[Value]) -> Result<(), FaucetError> {
        if !self.config.create_table {
            return Err(faucet_core::missing_target_error(
                "spanner sink",
                &self.config.table_name,
            ));
        }
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(format!(
                "spanner sink: table `{}` does not exist and cannot be auto-created without \
                 `key:` — Spanner requires a primary key on every table, and inventing one \
                 would bake a wrong physical layout in permanently. Set `key:` to the \
                 intended primary-key column(s), or create the table yourself and set \
                 `create_table: false`.",
                self.config.table_name
            )));
        }
        let Some(columns) = faucet_core::plan_columns(records) else {
            return Err(FaucetError::Sink(format!(
                "spanner sink: table `{}` does not exist and the first page carried no \
                 inferable columns to create it from",
                self.config.table_name
            )));
        };
        for k in key {
            if !columns.iter().any(|c| &c.name == k) {
                return Err(FaucetError::Sink(format!(
                    "spanner sink: key column `{k}` is not present in the first page, so the \
                     auto-created table would have a primary key no record can fill"
                )));
            }
        }
        // Key columns must be NOT NULL — Spanner rejects a null in a primary
        // key, so a nullable key column would fail every write.
        let cols: Vec<String> = columns
            .iter()
            .map(|c| {
                let null = if key.iter().any(|k| k == &c.name) {
                    " NOT NULL"
                } else {
                    ""
                };
                format!("{} STRING(MAX){null}", quote_ident_spanner(&c.name))
            })
            .collect();
        let pk: Vec<String> = key.iter().map(|k| quote_ident_spanner(k)).collect();
        let ddl = format!(
            "CREATE TABLE {} ({}) PRIMARY KEY ({})",
            quote_ident_spanner(&self.config.table_name),
            cols.join(", "),
            pk.join(", ")
        );
        self.run_ddl(vec![ddl]).await
    }
    /// Run DDL statements through the admin API, bounded by
    /// `ddl_timeout_secs`.
    async fn run_ddl(&self, statements: Vec<String>) -> Result<(), FaucetError> {
        let admin = self.config.connection.connect_admin().await?;
        let mut op = admin
            .database()
            .update_database_ddl(
                UpdateDatabaseDdlRequest {
                    database: self.config.connection.database_path(),
                    statements,
                    ..Default::default()
                },
                None,
            )
            .await
            .map_err(|e| sink_err("DDL submit", e))?;
        let timeout = Duration::from_secs(self.config.ddl_timeout_secs);
        match tokio::time::timeout(timeout, op.wait(None)).await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(sink_err("DDL operation", e)),
            Err(_) => Err(FaucetError::Sink(format!(
                "spanner DDL operation did not complete within {}s (ddl_timeout_secs)",
                self.config.ddl_timeout_secs
            ))),
        }
    }

    /// Ensure the commit-token watermark table exists (once per sink).
    async fn ensure_token_table(&self) -> Result<(), FaucetError> {
        self.token_table_ready
            .get_or_try_init(|| async { self.run_ddl(vec![commit_token_ddl()]).await })
            .await?;
        Ok(())
    }

    /// Delete rows in `scope` whose key was not written by this run (#478).
    ///
    /// Spanner mutations address the primary key, so "delete everything in the
    /// scope except these keys" cannot be expressed as a mutation alone: the
    /// scope's live keys have to be read first. Both halves run inside **one
    /// read-write transaction** — Spanner locks the rows the transaction read,
    /// so no concurrent writer can change them between the decision and the
    /// delete, and the whole delete set commits or none of it does. A partial
    /// delete would remove rows the run actually wrote.
    ///
    /// An empty `seen` set is meaningful, not a no-op — it means the source
    /// reported the scope as empty, so every live row in it is stale. That is
    /// the case this feature exists for.
    async fn cleanup_scope_impl(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let table = &self.config.table_name;
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "spanner cleanup requires a non-empty `key`".to_string(),
            ));
        }
        let meta = self.require_meta(&[]).await?;
        // The delete is by primary key, so the same PK-equality rule the
        // upsert/delete write path enforces applies here.
        validate_key_matches_pk(key, &meta.pk, table)?;

        // Build the scope SELECT and its parameters up front, so a config error
        // (unknown column, non-comparable type, mistyped value) fails before a
        // transaction is ever opened.
        let mut preds: Vec<(String, String)> = Vec::with_capacity(scope.len());
        let mut params: Vec<(String, String)> = Vec::with_capacity(scope.len());
        for (idx, (col, value)) in scope.iter().enumerate() {
            let ty = meta.type_of(col).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "spanner cleanup: column `{col}` does not exist on table `{table}` — the \
                     completeness claim and `key` are in destination column terms"
                ))
            })?;
            let name = format!("scope_{idx}");
            let expr = scope_cast_expr(&name, ty).map_err(|e| {
                FaucetError::Sink(format!("spanner cleanup: scope column `{col}`: {e}"))
            })?;
            let text = scope_param_text(value, ty).map_err(|e| {
                FaucetError::Sink(format!("spanner cleanup: scope column `{col}`: {e}"))
            })?;
            preds.push((col.clone(), expr));
            params.push((name, text));
        }
        let sql = build_scope_select_sql(table, &meta.pk, &preds);

        // Fingerprint what this run wrote, encoded through the destination
        // column types so it compares like-for-like with what Spanner returns.
        let mut seen_fingerprints = HashSet::with_capacity(seen.len());
        for key_tuple in seen.keys() {
            let vals = encode_key_in_pk_order(key_tuple, &meta)
                .map_err(|e| FaucetError::Sink(format!("spanner cleanup: {e}")))?;
            seen_fingerprints.insert(tuple_fingerprint(&vals));
        }

        let plan = Arc::new(CleanupPlan {
            table: table.clone(),
            sql,
            params,
            meta: Arc::clone(&meta),
            seen: seen_fingerprints,
        });
        let (_, outcome) = self
            .client
            .read_write_transaction(move |tx| {
                let plan = Arc::clone(&plan);
                // The nested `Result` is deliberate: a *logical* failure (an
                // undecodable key, a delete set over the cell budget) comes back
                // as `Ok(Err(msg))` with **nothing buffered**, so the transaction
                // commits no mutations and the message survives intact. The outer
                // `Err` stays reserved for the client's own transport/ABORTED
                // handling, whose retry semantics must not be disturbed.
                Box::pin(async move {
                    Ok::<Result<u64, String>, gcloud_spanner::client::Error>(
                        run_cleanup_txn(tx, &plan).await,
                    )
                })
            })
            .await
            .map_err(|e| sink_err("cleanup", e))?;
        let deleted = outcome.map_err(|e| FaucetError::Sink(format!("spanner cleanup: {e}")))?;

        tracing::info!(
            table = %table,
            deleted,
            written_keys = seen.len(),
            "Spanner scoped cleanup complete"
        );
        Ok(deleted)
    }
}

/// Everything [`run_cleanup_txn`] needs, shared across transaction attempts.
///
/// The retry closure is `Fn` and must hold no state between attempts, so this
/// is immutable and every attempt rebuilds its statement and mutations from it.
struct CleanupPlan {
    table: String,
    sql: String,
    params: Vec<(String, String)>,
    meta: Arc<TableMeta>,
    /// Fingerprints of the key tuples this run wrote.
    seen: HashSet<String>,
}

/// One attempt at the read-then-delete cleanup transaction.
///
/// Returns the number of rows buffered for deletion. `Err` carries a logical
/// failure with **no mutation buffered**, so the transaction commits nothing.
async fn run_cleanup_txn(
    tx: &mut gcloud_spanner::transaction_rw::ReadWriteTransaction,
    plan: &CleanupPlan,
) -> Result<u64, String> {
    let mut stmt = Statement::new(plan.sql.clone());
    for (name, text) in &plan.params {
        stmt.add_param(name, text);
    }

    // Read every live primary key in the scope. Scoped in its own block so the
    // row iterator's borrow of `tx` ends before the mutations are buffered.
    let mut live: Vec<Vec<Kind>> = Vec::new();
    {
        let mut iter = tx
            .query(stmt)
            .await
            .map_err(|e| format!("scope read failed: {e}"))?;
        while let Some(row) = iter
            .next()
            .await
            .map_err(|e| format!("scope read failed: {e}"))?
        {
            let mut vals = Vec::with_capacity(plan.meta.pk.len());
            for (idx, pk_col) in plan.meta.pk.iter().enumerate() {
                let decoded = row
                    .column::<SpannerJson>(idx)
                    .map_err(|e| format!("key column `{pk_col}`: {e}"))?
                    .0;
                let ty = plan
                    .meta
                    .type_of(pk_col)
                    .ok_or_else(|| format!("PK column `{pk_col}` not found in table metadata"))?;
                // Re-encode so a live key and a written key are compared in the
                // one canonical form (see `encode_key_in_pk_order`).
                vals.push(
                    encode_to_kind(&decoded, ty)
                        .map_err(|e| format!("key column `{pk_col}`: {e}"))?,
                );
            }
            live.push(vals);
        }
    }

    let stale = stale_key_rows(live, &plan.seen);
    if stale.is_empty() {
        return Ok(0);
    }
    check_cleanup_cell_budget(stale.len(), plan.meta.pk.len(), CELL_BUDGET)?;

    let deleted = stale.len() as u64;
    let mutations: Vec<Mutation> = stale
        .into_iter()
        .map(|vals| delete_mutation_from_kinds(&plan.table, vals).mutation)
        .collect();
    tx.buffer_write(mutations);
    Ok(deleted)
}

#[async_trait]
impl faucet_core::Sink for SpannerSink {
    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SpannerSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "spanner"
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
        ]
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    /// Scoped cleanup is available whenever the sink can delete by key, which
    /// on Spanner means the configured `key` is the table's PRIMARY KEY — a
    /// requirement `cleanup_scope` re-checks against the live table (this hook
    /// runs before any metadata read, so it cannot check it here).
    fn supports_cleanup(&self) -> bool {
        true
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the live destination schema from `INFORMATION_SCHEMA` as an
    /// `infer_schema`-shaped object, or `None` when the table does not exist
    /// yet (issue #194). Always fetches fresh — the drift pass caches on its
    /// side and refreshes after `evolve`.
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        Ok(self.fetch_meta().await?.map(|m| current_schema_json(&m)))
    }

    /// Apply additive schema evolution: new columns via `ADD COLUMN IF NOT
    /// EXISTS`; nullability relaxations by re-emitting the column at its
    /// current type without `NOT NULL`. Spanner cannot change a column's base
    /// type (e.g. INT64→FLOAT64), so a base-type widening is a typed error —
    /// set `allow_type_widening: false` so the drift policy classifies it
    /// `incompatible` and routes it via `on_incompatible` instead.
    async fn evolve_schema(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        let table = &self.config.table_name;
        let mut statements: Vec<String> = Vec::new();

        for c in &evolution.additions {
            let t =
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text);
            statements.push(build_add_column_sql(table, &c.name, t));
        }

        // Widenings: Spanner can only "widen" nullability. A widening whose
        // base type actually changes is unsupported.
        let meta = if evolution.widenings.is_empty() && evolution.relax_nullability.is_empty() {
            None
        } else {
            Some(self.require_meta(&[]).await?)
        };
        for c in &evolution.widenings {
            let from_base = c.from.as_ref().and_then(faucet_core::json_schema_base_type);
            let to_base = faucet_core::json_schema_base_type(&c.to);
            if from_base != to_base {
                return Err(FaucetError::Sink(format!(
                    "spanner cannot widen column `{}`'s base type ({:?} -> {:?}): Spanner does \
                     not support changing a column's type; set `allow_type_widening: false` so \
                     the drift policy treats this as incompatible",
                    c.name, from_base, to_base
                )));
            }
            let meta = meta.as_ref().expect("meta fetched when widenings present");
            let ty = meta.type_of(&c.name).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "spanner widen: column `{}` not found in table `{table}`",
                    c.name
                ))
            })?;
            let ddl = spanner_type_ddl(ty).map_err(|e| {
                FaucetError::Sink(format!("spanner widen column `{}`: {e}", c.name))
            })?;
            statements.push(build_alter_column_sql(table, &c.name, &ddl));
        }
        for col in &evolution.relax_nullability {
            let meta = meta
                .as_ref()
                .expect("meta fetched when relaxations present");
            let ty = meta.type_of(col).ok_or_else(|| {
                FaucetError::Sink(format!(
                    "spanner relax: column `{col}` not found in table `{table}`"
                ))
            })?;
            let ddl = spanner_type_ddl(ty)
                .map_err(|e| FaucetError::Sink(format!("spanner relax column `{col}`: {e}")))?;
            statements.push(build_alter_column_sql(table, col, &ddl));
        }

        if !statements.is_empty() {
            self.run_ddl(statements).await?;
            // The table changed shape — drop the cached metadata.
            *self.meta.write().await = None;
        }
        Ok(())
    }

    fn dataset_uri(&self) -> String {
        format!(
            "spanner://{}/{}/{}/{}",
            self.config.connection.project_id,
            self.config.connection.instance,
            self.config.connection.database,
            self.config.table_name
        )
    }

    /// Preflight probes (`faucet doctor`): `auth` runs `SELECT 1`; `schema`
    /// verifies the target table exists and — for upsert/delete — that the
    /// configured `key` equals the table's PRIMARY KEY. Non-mutating.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let select_one = async {
            let mut tx = self.client.single().await.map_err(|e| e.to_string())?;
            let mut iter = tx
                .query(Statement::new("SELECT 1"))
                .await
                .map_err(|e| e.to_string())?;
            iter.next().await.map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        };
        let auth = match tokio::time::timeout(ctx.timeout, select_one).await {
            Ok(Ok(())) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e,
                "check project/instance/database and credentials",
            ),
            Err(_) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                "timed out",
                "check project/instance/database and credentials",
            ),
        };

        let started = std::time::Instant::now();
        let schema = match tokio::time::timeout(ctx.timeout, self.fetch_meta()).await {
            Ok(Ok(Some(meta))) => {
                if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
                    Probe::pass("schema", started.elapsed())
                } else {
                    match validate_key_matches_pk(
                        &self.config.write.key,
                        &meta.pk,
                        &self.config.table_name,
                    ) {
                        Ok(()) => Probe::pass("schema", started.elapsed()),
                        Err(e) => Probe::fail_hint(
                            "schema",
                            started.elapsed(),
                            e.to_string(),
                            "set `key` to exactly the table's PRIMARY KEY columns",
                        ),
                    }
                }
            }
            Ok(Ok(None)) => Probe::fail_hint(
                "schema",
                started.elapsed(),
                format!("table `{}` does not exist", self.config.table_name),
                "create the table (Spanner mutations require an existing table)",
            ),
            Ok(Err(e)) => Probe::fail("schema", started.elapsed(), e.to_string()),
            Err(_) => Probe::fail("schema", started.elapsed(), "timed out"),
        };

        Ok(CheckReport {
            probes: vec![auth, schema],
        })
    }

    /// Write records as batched mutations, one atomic commit per chunk
    /// (`batch_size` rows and ≤60,000 cells per commit). Append mode uses
    /// `Insert` (a duplicate PK fails the commit); upsert/delete are planned
    /// via [`faucet_core::plan_writes`].
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let meta = self.require_meta(records).await?;
        let (planned, count) = self.plan_page(records, &meta)?;
        for chunk in chunk_by_cells(planned, self.config.batch_size, CELL_BUDGET) {
            self.client
                .apply(chunk)
                .await
                .map_err(|e| sink_err("commit", e))?;
        }
        tracing::info!(
            table = %self.config.table_name,
            rows = count,
            "Spanner write complete"
        );
        Ok(count)
    }

    /// Write a batch and report per-row outcomes.
    ///
    /// Append mode delegates to `write_batch` (all-or-nothing per chunk). In
    /// upsert/delete mode the good rows are applied and rows whose key could
    /// not be extracted are reported as per-row `Err` so the pipeline routes
    /// them to the DLQ.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let meta = self.require_meta(records).await?;
        validate_key_matches_pk(&self.config.write.key, &meta.pk, &self.config.table_name)?;
        let plan = faucet_core::plan_writes(records, &self.config.write);

        let table = &self.config.table_name;
        let mut planned: Vec<Planned> = Vec::with_capacity(plan.upserts.len() + plan.deletes.len());
        {
            let mut warned = self
                .warned_fields
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for record in &plan.upserts {
                let p =
                    build_row_mutation(table, record, &meta, WriteOp::InsertOrUpdate, &mut warned)
                        .map_err(|msg| FaucetError::Sink(format!("spanner upsert: {msg}")))?;
                planned.push(p);
            }
        }
        for key_tuple in &plan.deletes {
            let p = build_delete_mutation(table, key_tuple, &meta)
                .map_err(|msg| FaucetError::Sink(format!("spanner delete: {msg}")))?;
            planned.push(p);
        }
        for chunk in chunk_by_cells(planned, self.config.batch_size, CELL_BUDGET) {
            self.client
                .apply(chunk)
                .await
                .map_err(|e| sink_err("commit", e))?;
        }

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "spanner {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        // Every commit is already durable; nothing is buffered client-side.
        Ok(())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    /// Read the last committed watermark token for `scope` from the
    /// `faucet_commit_token` table. `Ok(None)` when the table (or row) does
    /// not exist yet.
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let mut tx = self
            .client
            .single()
            .await
            .map_err(|e| sink_err("token read", e))?;
        let scope_key = scope.to_string();
        match tx
            .read_row(
                SPANNER_COMMIT_TOKEN_TABLE,
                &[faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL],
                Key::new(&scope_key),
            )
            .await
        {
            Ok(Some(row)) => Ok(Some(
                row.column_by_name::<String>(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL)
                    .map_err(|e| sink_err("token decode", e))?,
            )),
            Ok(None) => Ok(None),
            // First exactly-once run: the watermark table hasn't been created
            // yet — that's "no token", not an error.
            // The typed status is authoritative. A message-based fallback here
            // would fire only for statuses that are NOT NotFound —
            // PermissionDenied / Unavailable / DeadlineExceeded whose text
            // happens to mention the table — and reporting those as "no token
            // committed" re-writes already-committed pages, i.e. silent
            // duplicate delivery under the one mode that promises otherwise.
            Err(status) if status.code() == gcloud_gax::grpc::Code::NotFound => Ok(None),
            Err(status) => Err(sink_err("token read", status)),
        }
    }

    /// Commit the page's mutations **and** the `(scope, token)` watermark in
    /// one read-write transaction — on crash either both land or neither
    /// does. The whole page is a single commit here (no re-chunking), so it
    /// is bounded by Spanner's ~80,000-cell commit limit: size the source's
    /// `batch_size` down for very wide tables.
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_token_table().await?;
        let meta = self.require_meta(records).await?;
        // Plan before the transaction so planning failures abort cleanly.
        let (planned, count) = self.plan_page(records, &meta)?;

        let mut mutations: Vec<Mutation> = planned.into_iter().map(|p| p.mutation).collect();
        let scope_owned = scope.to_string();
        // The token may carry a `#<bookmark>` suffix — stored verbatim,
        // never parsed.
        let token_owned = token.to_string();
        mutations.push(insert_or_update(
            SPANNER_COMMIT_TOKEN_TABLE,
            &[
                faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL,
                faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL,
                "updated_at",
            ],
            &[&scope_owned, &token_owned, &CommitTimestamp::new()],
        ));

        // The closure retries on ABORTED and must not keep state between
        // attempts — it clones the pre-built mutation set per attempt.
        let mutations = Arc::new(mutations);
        self.client
            .read_write_transaction(move |tx| {
                let mutations = Arc::clone(&mutations);
                Box::pin(async move {
                    tx.buffer_write(mutations.as_ref().clone());
                    Ok::<(), gcloud_spanner::client::Error>(())
                })
            })
            .await
            .map_err(|e| sink_err("idempotent commit", e))?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{WriteMode, WriteSpec};
    use gcloud_googleapis::spanner::v1::mutation::Operation;
    use serde_json::json;

    fn meta() -> TableMeta {
        TableMeta {
            columns: vec![
                ("id".into(), SpannerType::Int64, false),
                ("name".into(), SpannerType::String, true),
                ("score".into(), SpannerType::Float64, true),
                ("meta".into(), SpannerType::Json, true),
            ],
            pk: vec!["id".into()],
        }
    }

    fn composite_meta() -> TableMeta {
        TableMeta {
            columns: vec![
                ("tenant".into(), SpannerType::String, false),
                ("id".into(), SpannerType::Int64, false),
                ("v".into(), SpannerType::String, true),
            ],
            pk: vec!["tenant".into(), "id".into()],
        }
    }

    fn write_columns(m: &Mutation) -> Vec<String> {
        match m.operation.as_ref().expect("operation") {
            Operation::Insert(w) | Operation::InsertOrUpdate(w) => w.columns.clone(),
            other => panic!("expected write mutation, got {other:?}"),
        }
    }

    #[test]
    fn row_mutation_intersects_columns_and_encodes() {
        let mut warned = HashSet::new();
        let p = build_row_mutation(
            "t",
            &json!({"id": 1, "name": "a", "unknown": true}),
            &meta(),
            WriteOp::Insert,
            &mut warned,
        )
        .unwrap();
        assert_eq!(p.cells, 2);
        assert_eq!(write_columns(&p.mutation), vec!["id", "name"]);
        // Unknown field warned exactly once.
        assert!(warned.contains("unknown"));
        // A second record with the same unknown field doesn't re-insert.
        let before = warned.len();
        build_row_mutation(
            "t",
            &json!({"id": 2, "unknown": false}),
            &meta(),
            WriteOp::Insert,
            &mut warned,
        )
        .unwrap();
        assert_eq!(warned.len(), before);
    }

    #[test]
    fn row_mutation_rejects_non_objects_and_no_matches() {
        let mut warned = HashSet::new();
        assert!(
            build_row_mutation("t", &json!([1]), &meta(), WriteOp::Insert, &mut warned)
                .unwrap_err()
                .contains("not a JSON object")
        );
        assert!(
            build_row_mutation(
                "t",
                &json!({"nope": 1}),
                &meta(),
                WriteOp::Insert,
                &mut warned
            )
            .unwrap_err()
            .contains("no fields matching")
        );
    }

    #[test]
    fn row_mutation_surfaces_encode_errors_with_column_name() {
        let mut warned = HashSet::new();
        let err = build_row_mutation(
            "t",
            &json!({"id": "not-an-int-at-all"}),
            &meta(),
            WriteOp::Insert,
            &mut warned,
        )
        .unwrap_err();
        assert!(err.contains("column `id`"), "err: {err}");
    }

    #[test]
    fn upsert_op_builds_insert_or_update() {
        let mut warned = HashSet::new();
        let p = build_row_mutation(
            "t",
            &json!({"id": 1}),
            &meta(),
            WriteOp::InsertOrUpdate,
            &mut warned,
        )
        .unwrap();
        assert!(matches!(
            p.mutation.operation,
            Some(Operation::InsertOrUpdate(_))
        ));
    }

    #[test]
    fn delete_mutation_reorders_keys_into_pk_order() {
        // WriteSpec key order differs from PK order.
        let kt = faucet_core::KeyTuple(vec![
            ("id".into(), json!(7)),
            ("tenant".into(), json!("acme")),
        ]);
        let p = build_delete_mutation("t", &kt, &composite_meta()).unwrap();
        assert_eq!(p.cells, 2);
        let Some(Operation::Delete(d)) = &p.mutation.operation else {
            panic!("expected delete");
        };
        let keys = &d.key_set.as_ref().expect("key set").keys;
        assert_eq!(keys.len(), 1);
        // PK order is (tenant, id) — first value must be the tenant string.
        let vals = &keys[0].values;
        assert_eq!(
            vals[0].kind,
            Some(prost_types::value::Kind::StringValue("acme".into()))
        );
        assert_eq!(
            vals[1].kind,
            Some(prost_types::value::Kind::StringValue("7".into()))
        );
    }

    #[test]
    fn delete_mutation_errors_on_missing_pk_column() {
        let kt = faucet_core::KeyTuple(vec![("id".into(), json!(7))]);
        let err = build_delete_mutation("t", &kt, &composite_meta()).unwrap_err();
        assert!(err.contains("missing PK column `tenant`"));
    }

    fn planned(cells: usize) -> Planned {
        Planned {
            mutation: insert("t", &["a"], &[&"x".to_string()]),
            cells,
        }
    }

    #[test]
    fn chunking_respects_cell_budget() {
        let chunks = chunk_by_cells(vec![planned(40), planned(40), planned(40)], 0, 100);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);
    }

    #[test]
    fn chunking_respects_row_cap() {
        let chunks = chunk_by_cells((0..5).map(|_| planned(1)).collect(), 2, 1000);
        assert_eq!(
            chunks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
    }

    #[test]
    fn chunking_lets_an_oversized_row_ship_alone() {
        let chunks = chunk_by_cells(vec![planned(500), planned(1)], 0, 100);
        assert_eq!(chunks.iter().map(Vec::len).collect::<Vec<_>>(), vec![1, 1]);
    }

    #[test]
    fn chunking_zero_batch_size_is_cell_bounded_only() {
        let chunks = chunk_by_cells((0..100).map(|_| planned(1)).collect(), 0, 1000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 100);
    }

    #[test]
    fn chunking_empty_input_is_empty() {
        assert!(chunk_by_cells(vec![], 10, 100).is_empty());
    }

    #[test]
    fn key_pk_validation_is_order_insensitive_set_equality() {
        let key = vec!["id".to_string(), "tenant".to_string()];
        let pk = vec!["tenant".to_string(), "id".to_string()];
        assert!(validate_key_matches_pk(&key, &pk, "t").is_ok());
        let err = validate_key_matches_pk(&["id".to_string()], &pk, "t").unwrap_err();
        assert!(err.to_string().contains("PRIMARY KEY"));
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn add_column_ddl() {
        assert_eq!(
            build_add_column_sql("t", "email", faucet_core::SqlBaseType::Text),
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `email` STRING(MAX)"
        );
        assert_eq!(
            build_add_column_sql("t", "n", faucet_core::SqlBaseType::Integer),
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `n` INT64"
        );
        assert_eq!(
            build_add_column_sql("t", "j", faucet_core::SqlBaseType::Json),
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `j` JSON"
        );
    }

    #[test]
    fn alter_column_ddl_reemits_type_without_not_null() {
        assert_eq!(
            build_alter_column_sql("t", "name", "STRING(MAX)"),
            "ALTER TABLE `t` ALTER COLUMN `name` STRING(MAX)"
        );
    }

    #[test]
    fn spanner_type_ddl_round_trips() {
        assert_eq!(spanner_type_ddl(&SpannerType::Int64).unwrap(), "INT64");
        assert_eq!(
            spanner_type_ddl(&SpannerType::Array(Box::new(SpannerType::Float64))).unwrap(),
            "ARRAY<FLOAT64>"
        );
        assert_eq!(spanner_type_ddl(&SpannerType::Bytes).unwrap(), "BYTES(MAX)");
        assert!(spanner_type_ddl(&SpannerType::Other).is_err());
    }

    #[test]
    fn commit_token_ddl_shape() {
        let ddl = commit_token_ddl();
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS `faucet_commit_token`"));
        assert!(ddl.contains("`scope` STRING(MAX) NOT NULL"));
        assert!(ddl.contains("`token` STRING(MAX) NOT NULL"));
        assert!(ddl.contains("allow_commit_timestamp=true"));
        assert!(ddl.ends_with("PRIMARY KEY (`scope`)"));
    }

    #[test]
    fn current_schema_json_shape() {
        let schema = current_schema_json(&meta());
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(
            schema["properties"]["name"]["type"],
            json!(["string", "null"])
        );
        assert_eq!(
            schema["properties"]["score"]["type"],
            json!(["number", "null"])
        );
    }

    #[test]
    fn table_meta_lookups() {
        let m = meta();
        assert!(m.has_column("id"));
        assert!(!m.has_column("missing"));
        assert_eq!(m.type_of("score"), Some(&SpannerType::Float64));
        assert_eq!(m.type_of("missing"), None);
    }

    /// Offline plan_page coverage through a sink built without a server —
    /// exercised via the pure helpers instead (plan_page requires a client).
    #[test]
    fn write_spec_key_reorder_composite_delete_encodes_types() {
        let kt = faucet_core::KeyTuple(vec![
            ("tenant".into(), json!("t1")),
            ("id".into(), json!(9_007_199_254_740_993_i64)),
        ]);
        let p = build_delete_mutation("t", &kt, &composite_meta()).unwrap();
        let Some(Operation::Delete(d)) = &p.mutation.operation else {
            panic!("expected delete");
        };
        let vals = &d.key_set.as_ref().unwrap().keys[0].values;
        // INT64 keys travel string-encoded, losslessly.
        assert_eq!(
            vals[1].kind,
            Some(prost_types::value::Kind::StringValue(
                "9007199254740993".into()
            ))
        );
    }

    // --- scoped cleanup (issue #478) ---

    #[test]
    fn scope_cast_expr_per_column_type() {
        let e = |ty| scope_cast_expr("scope_0", &ty).unwrap();
        // STRING is already the bound parameter's type — no cast.
        assert_eq!(e(SpannerType::String), "@scope_0");
        assert_eq!(e(SpannerType::Int64), "CAST(@scope_0 AS INT64)");
        assert_eq!(e(SpannerType::Bool), "CAST(@scope_0 AS BOOL)");
        assert_eq!(e(SpannerType::Float64), "CAST(@scope_0 AS FLOAT64)");
        assert_eq!(e(SpannerType::Float32), "CAST(@scope_0 AS FLOAT32)");
        assert_eq!(e(SpannerType::Timestamp), "CAST(@scope_0 AS TIMESTAMP)");
        assert_eq!(e(SpannerType::Date), "CAST(@scope_0 AS DATE)");
        assert_eq!(e(SpannerType::Numeric), "CAST(@scope_0 AS NUMERIC)");
        // BYTES travel base64-encoded, so decode rather than reinterpret.
        assert_eq!(e(SpannerType::Bytes), "FROM_BASE64(@scope_0)");
    }

    #[test]
    fn scope_cast_expr_refuses_types_without_equality() {
        for ty in [
            SpannerType::Json,
            SpannerType::Array(Box::new(SpannerType::Int64)),
            SpannerType::Other,
        ] {
            let err = scope_cast_expr("p", &ty).unwrap_err();
            assert!(err.contains("does not support equality"), "{err}");
        }
    }

    #[test]
    fn scope_param_text_renders_scalars_and_validates_against_the_column() {
        assert_eq!(
            scope_param_text(&json!(42), &SpannerType::Int64).unwrap(),
            "42"
        );
        assert_eq!(
            scope_param_text(&json!("eu"), &SpannerType::String).unwrap(),
            "eu"
        );
        assert_eq!(
            scope_param_text(&json!(true), &SpannerType::Bool).unwrap(),
            "true"
        );
        assert_eq!(
            scope_param_text(&json!(1.5), &SpannerType::Float64).unwrap(),
            "1.5"
        );
        // Type mismatches surface here, not at the server.
        assert!(scope_param_text(&json!(1.5), &SpannerType::Int64).is_err());
        // Backstop for a null the core policy already refuses: `col = NULL` is
        // never true, which would silently make the cleanup a no-op.
        let err = scope_param_text(&Value::Null, &SpannerType::Int64).unwrap_err();
        assert!(err.contains("null"), "{err}");
    }

    #[test]
    fn scope_select_sql_quotes_and_ands() {
        let sql = build_scope_select_sql(
            "orders",
            &["id".into()],
            &[("contact_id".into(), "CAST(@scope_0 AS INT64)".into())],
        );
        assert_eq!(
            sql,
            "SELECT `id` FROM `orders` WHERE `contact_id` = CAST(@scope_0 AS INT64)"
        );

        let sql = build_scope_select_sql(
            "orders",
            &["tenant".into(), "id".into()],
            &[
                ("region".into(), "@scope_0".into()),
                ("kind".into(), "@scope_1".into()),
            ],
        );
        assert_eq!(
            sql,
            "SELECT `tenant`, `id` FROM `orders` WHERE `region` = @scope_0 AND `kind` = @scope_1"
        );
    }

    #[test]
    fn key_fingerprints_do_not_alias_across_composite_boundaries() {
        let s = |v: &str| Kind::StringValue(v.into());
        assert_ne!(
            tuple_fingerprint(&[s("ab"), s("c")]),
            tuple_fingerprint(&[s("a"), s("bc")]),
            "a length-prefixed fingerprint must keep these distinct"
        );
        // Different kinds carrying the same text are distinct too.
        assert_ne!(
            kind_fingerprint(&s("true")),
            kind_fingerprint(&Kind::BoolValue(true))
        );
        // …and equal values fingerprint equally.
        assert_eq!(kind_fingerprint(&s("x")), kind_fingerprint(&s("x")));
    }

    #[test]
    fn written_and_live_keys_compare_after_encoding() {
        // The source may hand a stringified integer while Spanner returns a JSON
        // number for the same INT64 key; encoding both through the destination
        // column type is what makes them compare equal.
        let written = encode_key_in_pk_order(
            &faucet_core::KeyTuple(vec![("id".into(), json!("7"))]),
            &meta(),
        )
        .unwrap();
        let live = vec![encode_to_kind(&json!(7), &SpannerType::Int64).unwrap()];
        assert_eq!(tuple_fingerprint(&written), tuple_fingerprint(&live));
    }

    #[test]
    fn stale_rows_are_the_live_keys_this_run_did_not_write() {
        let k = |n: &str| vec![Kind::StringValue(n.into())];
        let live = vec![k("1"), k("2"), k("3")];
        let seen: HashSet<String> = [k("1"), k("3")]
            .iter()
            .map(|v| tuple_fingerprint(v))
            .collect();
        assert_eq!(stale_key_rows(live, &seen), vec![k("2")]);
    }

    #[test]
    fn an_empty_seen_set_makes_every_live_row_stale() {
        // The motivating case: the source reported the scope empty, so all of
        // it must go. This must never be treated as "nothing to do".
        let live = vec![
            vec![Kind::StringValue("1".into())],
            vec![Kind::StringValue("2".into())],
        ];
        let stale = stale_key_rows(live.clone(), &HashSet::new());
        assert_eq!(stale, live);
    }

    #[test]
    fn cleanup_cell_budget_refuses_rather_than_half_cleaning() {
        assert!(check_cleanup_cell_budget(10, 2, 100).is_ok());
        assert!(check_cleanup_cell_budget(50, 2, 100).is_ok());
        let err = check_cleanup_cell_budget(51, 2, 100).unwrap_err();
        assert!(err.contains("102 cells"), "{err}");
        assert!(err.contains("Nothing was deleted"), "{err}");
        assert!(err.contains("complete_for"), "{err}");
        // A zero-width PK still counts one cell per row rather than dividing by
        // zero or reporting a free delete.
        assert!(check_cleanup_cell_budget(101, 0, 100).is_err());
    }

    #[test]
    fn cleanup_deletes_address_the_primary_key_in_pk_order() {
        let stale = vec![vec![
            Kind::StringValue("acme".into()),
            Kind::StringValue("7".into()),
        ]];
        let p = delete_mutation_from_kinds("t", stale.into_iter().next().unwrap());
        assert_eq!(p.cells, 2);
        let Some(Operation::Delete(d)) = &p.mutation.operation else {
            panic!("expected delete");
        };
        let vals = &d.key_set.as_ref().expect("key set").keys[0].values;
        assert_eq!(
            vals[0].kind,
            Some(prost_types::value::Kind::StringValue("acme".into()))
        );
        assert_eq!(
            vals[1].kind,
            Some(prost_types::value::Kind::StringValue("7".into()))
        );
    }

    #[test]
    fn plan_writes_integration_with_write_spec() {
        // Sanity: plan_writes + our builders compose (dedup by key, delete marker).
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(faucet_core::DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
            rollback: None,
        };
        let page = vec![
            json!({"id": 1, "name": "a"}),
            json!({"id": 1, "name": "b"}),
            json!({"id": 2, "__op": "d"}),
            json!({"name": "no-key"}),
        ];
        let plan = faucet_core::plan_writes(&page, &spec);
        assert_eq!(plan.upserts.len(), 1); // last-write-wins dedup
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.failed.len(), 1);
        let mut warned = HashSet::new();
        for u in &plan.upserts {
            build_row_mutation("t", u, &meta(), WriteOp::InsertOrUpdate, &mut warned).unwrap();
        }
        for d in &plan.deletes {
            build_delete_mutation("t", d, &meta()).unwrap();
        }
    }
}

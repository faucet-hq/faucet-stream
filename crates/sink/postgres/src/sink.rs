//! PostgreSQL sink implementation.

use crate::config::{
    BinaryEncoding, BinaryValue, PostgresColumnMapping, PostgresSinkConfig, PostgresWriteMethod,
};
use crate::copy::{build_auto_map_payload, build_jsonb_payload, copy_statement};
use async_trait::async_trait;
use faucet_common_sql::dialects::Postgres;
use faucet_common_sql::driver::label;
use faucet_common_sql::{
    EvolutionStep, InsertPath, SplitPage, SqlDialect, SqlDriver, SqlEvolution, SqlExecutor,
    SqlFuture, SqlLayout, SqlSinkDriver, SqlSinkEngine, Statement, StatementKind, TableColumn,
    TableTarget,
};
use faucet_core::FaucetError;
use faucet_core::driver::{DriverSink, RollbackDriver};
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// The error text for a record that matches no column of the target.
pub(crate) fn no_matching_column_message(
    idx: usize,
    obj: &serde_json::Map<String, Value>,
    columns: &[(String, String)],
) -> String {
    faucet_common_sql::no_match_message(&Postgres, idx, obj, &table_columns(columns), "")
}

/// `(name, udt)` pairs as engine columns.
pub(crate) fn table_columns(columns: &[(String, String)]) -> Vec<TableColumn> {
    columns
        .iter()
        .map(|(name, udt)| TableColumn::new(name.clone(), udt.clone()))
        .collect()
}

/// Split a page into per-row outcomes, the records to write, and each written
/// record's position in `records`. A record whose `bytea` value
/// `binary_encoding` refuses — or, with `require_match`, that matches no
/// column — fails on its own row instead of failing the page.
fn split_columns(
    records: &[Value],
    columns: &[TableColumn],
    binary: BinaryEncoding,
    require_match: bool,
) -> SplitPage {
    faucet_common_sql::split_page(
        &Postgres,
        "",
        records,
        columns,
        require_match,
        |idx, col, v| {
            pg_bind_text_with(Some(v), &col.sql_type, binary)
                .err()
                .map(|e| format!("row {idx}: column {}: {e}", col.name))
        },
    )
}

/// [`split_columns`] over `(name, udt)` pairs.
#[cfg(test)]
pub(crate) fn split_writable(
    records: &[Value],
    columns: &[(String, String)],
    binary: BinaryEncoding,
    require_match: bool,
) -> (Vec<faucet_core::RowOutcome>, Vec<Value>, Vec<usize>) {
    let split = split_columns(records, &table_columns(columns), binary, require_match);
    (split.outcomes, split.writable, split.positions)
}

/// Render a JSON value as the text to bind for a PostgreSQL column whose
/// underlying type is `udt` (`information_schema.columns.udt_name`), or `None`
/// for SQL `NULL`.
///
/// The accompanying placeholder is emitted as `$N::<udt>`, so PostgreSQL runs
/// the destination column type's input function over this text. That makes
/// `string → timestamptz/uuid/date`, `number → int4/numeric/float8`,
/// `bool → bool`, and `json → jsonb` all work — instead of binding every value
/// as `serde_json::Value` (which sqlx encodes as `jsonb`, so an insert into any
/// non-`jsonb` column fails at runtime with *"column is of type … but
/// expression is of type jsonb"*; this was the C1 bug in audit #146).
///
/// For `json`/`jsonb` columns the value is bound as its JSON text (so a string
/// keeps its quotes and objects/arrays round-trip); the `::jsonb` cast then
/// parses it. For every other type the scalar's plain text form is bound and
/// the column's input function parses it via the cast.
pub(crate) fn pg_bind_text(value: Option<&Value>, udt: &str) -> Option<String> {
    pg_bind_text_with(value, udt, BinaryEncoding::Auto).unwrap_or_default()
}

/// [`pg_bind_text`] with an explicit [`BinaryEncoding`] for `bytea` columns.
/// `Err` only under `binary_encoding: base64` for a string that is not base64.
pub(crate) fn pg_bind_text_with(
    value: Option<&Value>,
    udt: &str,
    binary: BinaryEncoding,
) -> Result<Option<String>, String> {
    Ok(match value {
        None | Some(Value::Null) => None,
        Some(v) => {
            if udt.eq_ignore_ascii_case("json") || udt.eq_ignore_ascii_case("jsonb") {
                Some(v.to_string())
            } else if let (true, Value::String(s)) = (udt.eq_ignore_ascii_case("bytea"), v) {
                Some(bytea_text(s, binary)?)
            } else {
                match v {
                    Value::Bool(b) => Some(b.to_string()),
                    Value::Number(n) => Some(n.to_string()),
                    Value::String(s) => Some(s.clone()),
                    // Arrays/objects have no scalar text form for a non-JSON
                    // column; bind their JSON text so the `::<type>` cast fails
                    // loudly rather than silently coercing.
                    other => Some(other.to_string()),
                }
            }
        }
    })
}

/// The `bytea` input text for a string value (#789 SQL-02, #862 M-064).
///
/// Every faucet source emits binary as base64 text, and `'SGVsbG8='::bytea`
/// reads it as escape format — storing the eight ASCII bytes of the base64
/// spelling. Under `auto` valid base64 is decoded; text already in the `\x…`
/// hex input form, or that is not base64, is passed through as before. Under
/// `text` the string's own bytes are stored; under `base64` every value is
/// decoded.
fn bytea_text(s: &str, binary: BinaryEncoding) -> Result<String, String> {
    if binary == BinaryEncoding::Auto && s.starts_with("\\x") {
        return Ok(s.to_string());
    }
    Ok(match binary.resolve(s)? {
        BinaryValue::Bytes(bytes) => hex_bytea(&bytes),
        BinaryValue::Text(t) if binary == BinaryEncoding::Text => hex_bytea(t.as_bytes()),
        BinaryValue::Text(t) => t.to_string(),
    })
}

fn hex_bytea(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("\\x");
    for b in bytes {
        out.push(HEX[usize::from(b >> 4)] as char);
        out.push(HEX[usize::from(b & 0x0f)] as char);
    }
    out
}

/// Build the SQL relation reference for the configured table, optionally
/// schema-qualified.
///
/// Both the AutoMap column-discovery probe and the `INSERT` statements use this
/// single helper, so column discovery is always scoped to the *exact* relation
/// the `INSERT` targets (#146 M13). With no schema the bare quoted table name
/// resolves against the connection's `search_path`; with a schema it becomes
/// `"schema"."table"`, pinning both discovery and insert to that namespace —
/// otherwise a table of the same name in another schema pollutes the
/// AutoMap column set (duplicate / wrong columns).
pub(crate) fn qualified_table_ref(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        None => quote_ident(table),
    }
}

/// First key of the two-key advisory lock that serializes faucet's bootstrap
/// DDL ("FAUC"), so it never collides with a single-key lock an application
/// takes on the same database.
const BOOTSTRAP_LOCK_CLASS: i32 = 0x4641_5543;

/// How many times a bootstrap is attempted when a concurrent creator outside
/// faucet wins the race on the catalog.
const BOOTSTRAP_ATTEMPTS: usize = 3;

/// The advisory-lock key for bootstrap DDL in `schema` (`None` = the
/// connection's default schema). Every faucet writer creating objects in one
/// schema serializes on it, because concurrent `CREATE SCHEMA IF NOT EXISTS` /
/// `CREATE TABLE IF NOT EXISTS` are not safe against each other: the loser
/// fails on the catalog's unique index (#828).
pub(crate) fn bootstrap_lock_key(schema: Option<&str>) -> String {
    format!("faucet-ddl:{}", schema.unwrap_or(""))
}

/// Whether a SQLSTATE is the catalog-race outcome of two concurrent
/// `IF NOT EXISTS` creates: `23505` on `pg_namespace` / `pg_type` /
/// `pg_class`, or `42P06` / `42P07` / `42710` (duplicate schema / table /
/// object) when the other side committed in between.
pub(crate) fn is_concurrent_ddl_code(code: &str) -> bool {
    matches!(code, "23505" | "42P06" | "42P07" | "42710")
}

fn is_concurrent_ddl(e: &sqlx::Error) -> bool {
    match e {
        sqlx::Error::Database(db) => db.code().is_some_and(|c| is_concurrent_ddl_code(&c)),
        _ => false,
    }
}

/// Run idempotent bootstrap DDL (`IF NOT EXISTS` creates) on `conn`,
/// serialized against every other faucet writer in the same schema by a
/// transaction-scoped advisory lock (#828). When `conn` is already inside a
/// transaction the work runs in a savepoint and the lock is held to the
/// caller's commit. A catalog race lost to a creator outside faucet is retried:
/// the object exists by then, so the `IF NOT EXISTS` statement is a no-op.
pub(crate) async fn run_bootstrap_ddl(
    conn: &mut sqlx::PgConnection,
    lock_key: &str,
    statements: &[String],
) -> Result<(), sqlx::Error> {
    use sqlx::Connection;
    let mut attempt = 0;
    loop {
        attempt += 1;
        let mut tx = conn.begin().await?;
        let result: Result<(), sqlx::Error> = async {
            sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
                .bind(BOOTSTRAP_LOCK_CLASS)
                .bind(lock_key)
                .execute(&mut *tx)
                .await?;
            for sql in statements {
                sqlx::query(sql).execute(&mut *tx).await?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => return tx.commit().await,
            Err(e) if attempt < BOOTSTRAP_ATTEMPTS && is_concurrent_ddl(&e) => {
                tx.rollback().await?;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Run a write under `secs` (0 = unbounded): a server that stops answering
/// fails the write instead of hanging the run forever (#789 SQL-109).
pub(crate) async fn bounded<T>(
    secs: u64,
    what: &str,
    fut: impl std::future::Future<Output = Result<T, FaucetError>>,
) -> Result<T, FaucetError> {
    if secs == 0 {
        return fut.await;
    }
    match tokio::time::timeout(std::time::Duration::from_secs(secs), fut).await {
        Ok(result) => result,
        Err(_) => Err(FaucetError::Sink(format!(
            "postgres {what} timed out after {secs}s waiting for the server (write_timeout_secs)"
        ))),
    }
}

/// PostgreSQL's identifier length limit (NAMEDATALEN - 1).
const PG_MAX_IDENT: usize = 63;

/// `<table><suffix>` within PostgreSQL's 63-byte identifier limit. A longer
/// name is cut and given a hash of the full table name, so it stays distinct
/// from the table and from other derived names — PostgreSQL would otherwise
/// truncate it silently, and a staging name equal to the target would drop the
/// target (#789 SQL-77).
pub(crate) fn derived_table_name(table: &str, suffix: &str) -> String {
    if table.len() + suffix.len() <= PG_MAX_IDENT {
        return format!("{table}{suffix}");
    }
    let hash = table.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    let tag = format!("_{:08x}", hash as u32);
    let mut cut = PG_MAX_IDENT.saturating_sub(suffix.len() + tag.len());
    while cut > 0 && !table.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{tag}{suffix}", &table[..cut])
}

/// The staging table an overwrite of `table` loads into: `<table>__faucet_ovw`,
/// shortened with a hash tag when that would exceed PostgreSQL's 63-byte limit.
pub fn overwrite_staging_table_name(table: &str) -> String {
    derived_table_name(table, faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX)
}

/// The staging table of overwrite run `token` (`<table>__faucet_ovw_<token>`,
/// shortened like [`overwrite_staging_table_name`]); the untokened name
/// without one.
pub(crate) fn run_staging_table_name(table: &str, token: Option<&str>) -> String {
    derived_table_name(
        table,
        &faucet_core::idempotency::overwrite_staging_suffix(token),
    )
}

/// The table part every tokened staging name of `table` shares.
pub(crate) fn run_staging_base(table: &str) -> String {
    let probe = "0".repeat(faucet_core::idempotency::OVERWRITE_RUN_TOKEN_LEN);
    let suffix = faucet_core::idempotency::overwrite_staging_suffix(Some(&probe));
    let name = derived_table_name(table, &suffix);
    name.strip_suffix(suffix.as_str())
        .unwrap_or(&name)
        .to_string()
}

/// How a table's columns may be written: every non-generated column, and
/// which of them are identity columns (`'a'` = `GENERATED ALWAYS`,
/// `'d'` = `BY DEFAULT`).
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ColumnRoles {
    pub(crate) insertable: Vec<String>,
    pub(crate) identity: Vec<(String, char)>,
}

impl ColumnRoles {
    /// `INSERT … SELECT` statements copying `source` into `target` over the
    /// insertable columns, optionally filtered. Rows whose identity columns
    /// are set keep their values (`OVERRIDING SYSTEM VALUE`); rows where they
    /// are NULL get them from the target's own sequence (#789 SQL-107).
    pub(crate) fn copy_statements(
        &self,
        target: &str,
        source: &str,
        filter: Option<&str>,
        keep: impl Fn(&str) -> bool,
    ) -> Vec<String> {
        let cols: Vec<&String> = self.insertable.iter().filter(|c| keep(c)).collect();
        let list = |cols: &[&String]| {
            cols.iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let ident: Vec<&String> = self
            .identity
            .iter()
            .map(|(c, _)| c)
            .filter(|c| cols.contains(c))
            .collect();
        let and = |p: String| match filter {
            Some(f) => format!("({p}) AND ({f})"),
            None => p,
        };
        if ident.is_empty() {
            let all = list(&cols);
            let where_ = filter.map(|f| format!(" WHERE {f}")).unwrap_or_default();
            return vec![format!(
                "INSERT INTO {target} ({all}) SELECT {all} FROM {source}{where_}"
            )];
        }
        let set = ident
            .iter()
            .map(|c| format!("{} IS NOT NULL", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let unset = ident
            .iter()
            .map(|c| format!("{} IS NULL", quote_ident(c)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let all = list(&cols);
        let rest: Vec<&String> = cols
            .iter()
            .copied()
            .filter(|c| !ident.contains(c))
            .collect();
        let mut out = vec![format!(
            "INSERT INTO {target} ({all}) OVERRIDING SYSTEM VALUE SELECT {all} FROM {source} WHERE {}",
            and(set)
        )];
        if !rest.is_empty() {
            let rest = list(&rest);
            out.push(format!(
                "INSERT INTO {target} ({rest}) SELECT {rest} FROM {source} WHERE {}",
                and(unset)
            ));
        }
        out
    }
}

/// Read a table's column roles on `conn`.
pub(crate) async fn column_roles(
    conn: &mut sqlx::PgConnection,
    table_ref: &str,
) -> Result<ColumnRoles, FaucetError> {
    let rows = sqlx::query(
        "SELECT a.attname::text, a.attidentity::text, a.attgenerated::text \
         FROM pg_catalog.pg_attribute a \
         WHERE a.attrelid = to_regclass($1)::oid AND a.attnum > 0 AND NOT a.attisdropped \
         ORDER BY a.attnum",
    )
    .bind(table_ref)
    .fetch_all(&mut *conn)
    .await
    .map_err(|e| FaucetError::Sink(format!("postgres: read columns of {table_ref}: {e}")))?;
    let mut roles = ColumnRoles::default();
    for r in rows {
        let name: String = r.get(0);
        let identity: String = r.get(1);
        let generated: String = r.get(2);
        if !generated.is_empty() {
            continue;
        }
        if let Some(kind) = identity.chars().next() {
            roles.identity.push((name.clone(), kind));
        }
        roles.insertable.push(name);
    }
    Ok(roles)
}

/// A foreign key of another table that references the overwrite target.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ReferencingKey {
    pub(crate) name: String,
    /// The referencing table, as `regclass` text.
    pub(crate) child: String,
    /// `pg_constraint.confdeltype`: `a` no action, `r` restrict, `c` cascade,
    /// `n` set null, `d` set default.
    pub(crate) on_delete: char,
    pub(crate) deferrable: bool,
    /// Whether any child row sets every column of the key.
    pub(crate) has_references: bool,
}

/// Foreign keys of *other* tables that reference `table_ref`.
pub(crate) async fn referencing_foreign_keys(
    conn: &mut sqlx::PgConnection,
    table_ref: &str,
) -> Result<Vec<ReferencingKey>, FaucetError> {
    let err = |e: sqlx::Error| {
        FaucetError::Sink(format!("postgres: read foreign keys of {table_ref}: {e}"))
    };
    let rows = sqlx::query(
        "SELECT c.conname::text, c.conrelid::regclass::text, c.confdeltype::text, \
                c.condeferrable, \
                (SELECT string_agg(format('%I IS NOT NULL', a.attname), ' AND ') \
                   FROM unnest(c.conkey) AS k(attnum) \
                   JOIN pg_catalog.pg_attribute a \
                     ON a.attrelid = c.conrelid AND a.attnum = k.attnum) \
         FROM pg_catalog.pg_constraint c \
         WHERE c.contype = 'f' AND c.confrelid = to_regclass($1)::oid \
           AND c.conrelid <> c.confrelid ORDER BY 1",
    )
    .bind(table_ref)
    .fetch_all(&mut *conn)
    .await
    .map_err(err)?;
    let mut keys = Vec::with_capacity(rows.len());
    for r in &rows {
        let action: String = r.get(2);
        let child: String = r.get(1);
        let set: Option<String> = r.get(4);
        let has_references: bool = sqlx::query_scalar(&format!(
            "SELECT EXISTS (SELECT 1 FROM {child} WHERE {})",
            set.as_deref().unwrap_or("true")
        ))
        .fetch_one(&mut *conn)
        .await
        .map_err(err)?;
        keys.push(ReferencingKey {
            name: r.get(0),
            child,
            on_delete: action.chars().next().unwrap_or('a'),
            deferrable: r.get(3),
            has_references,
        });
    }
    Ok(keys)
}

/// The statements that empty `target` before it is refilled. `TRUNCATE`
/// refuses a table other tables reference, so such a target is emptied with
/// `DELETE` under deferred constraints — children referencing a key the
/// refill brings back stay valid, any other reference fails the swap. A
/// cascading reference is refused: the `DELETE` would remove the children's
/// rows (#789 SQL-107). `SET CONSTRAINTS … DEFERRED` only reaches keys
/// declared `DEFERRABLE`, and `ON DELETE RESTRICT` is always checked at once,
/// so a non-deferrable or restricting key whose table holds references is
/// refused too — the `DELETE` would fail at the swap, after the whole load
/// (#862 M-041).
pub(crate) fn clear_statements(
    target: &str,
    referencing: &[ReferencingKey],
) -> Result<Vec<String>, FaucetError> {
    if referencing.is_empty() {
        return Ok(vec![format!("TRUNCATE TABLE {target}")]);
    }
    if let Some(k) = referencing
        .iter()
        .find(|k| matches!(k.on_delete, 'c' | 'n' | 'd'))
    {
        return Err(FaucetError::Config(format!(
            "postgres overwrite: {target} is referenced by foreign key {} on {} with \
             ON DELETE CASCADE / SET NULL / SET DEFAULT; replacing its rows would change that \
             table. Use write_mode: upsert, or change the foreign key",
            k.name, k.child
        )));
    }
    if let Some(k) = referencing
        .iter()
        .find(|k| k.has_references && (k.on_delete == 'r' || !k.deferrable))
    {
        let why = if k.on_delete == 'r' {
            "ON DELETE RESTRICT, which is checked immediately"
        } else {
            "not DEFERRABLE, so it is checked before the refill"
        };
        return Err(FaucetError::Config(format!(
            "postgres overwrite: {target} is referenced by foreign key {} on {}, which is {why}; \
             emptying {target} would fail while {} holds references. Declare the foreign key \
             DEFERRABLE (ON DELETE NO ACTION), or use write_mode: upsert",
            k.name, k.child, k.child
        )));
    }
    Ok(vec![
        "SET CONSTRAINTS ALL DEFERRED".to_string(),
        format!("DELETE FROM {target}"),
    ])
}

/// Build the `ON CONFLICT (key) DO UPDATE …` tail for an upsert INSERT.
/// Non-key columns are SET from EXCLUDED. If every column is a key column,
/// emit `DO NOTHING`.
pub(crate) fn on_conflict_clause(key: &[String], all_cols: &[String]) -> String {
    Postgres
        .upsert_tail(key, all_cols)
        .expect("postgres upserts with ON CONFLICT")
}

/// The engine target for an already-quoted relation reference.
fn target_ref(table_ref: &str) -> TableTarget {
    TableTarget::new(table_ref, table_ref)
}

/// `CREATE TABLE IF NOT EXISTS` for an auto-created target (#580).
///
/// `IF NOT EXISTS` rather than a probe-then-create: two matrix rows writing the
/// same table would otherwise race between the probe and the DDL, and the
/// loser's `CREATE` would fail a run that had nothing wrong with it.
#[cfg(test)]
fn build_create_table_sql(
    table_ref: &str,
    columns: &[faucet_core::PlannedColumn],
    json_column: Option<&str>,
    key: &[String],
) -> String {
    match json_column {
        Some(col) => Postgres.create_json_table_sql(&target_ref(table_ref), col),
        None => Postgres.create_table_sql(&target_ref(table_ref), columns, key),
    }
    .expect("postgres CREATE TABLE")
}

/// `ALTER TABLE <ref> ADD COLUMN IF NOT EXISTS "<col>" <kw>` — idempotent column
/// addition. `table_ref` is already quoted (`"schema"."table"`).
fn build_add_column_sql(table_ref: &str, col: &str, t: faucet_core::SqlBaseType) -> String {
    Postgres
        .add_column_sql(&target_ref(table_ref), col, t)
        .expect("postgres ADD COLUMN")
}

/// `ALTER TABLE <ref> ALTER COLUMN "<col>" TYPE <kw> USING "<col>"::<kw>` — widen
/// an existing column's type. Naturally idempotent (re-running the same TYPE
/// change is a no-op).
fn build_alter_type_sql(table_ref: &str, col: &str, kw: &str) -> String {
    Postgres
        .alter_column_type_sql(&target_ref(table_ref), col, kw)
        .ok()
        .flatten()
        .expect("postgres ALTER TYPE")
}

/// The type a widening rewrites a column to. An integer column that starts
/// receiving fractions becomes exact `numeric`, never `double precision`,
/// which would silently round every stored value above 2^53 (#789 SQL-78).
#[cfg(test)]
fn widened_keyword(change: &faucet_core::ColumnChange) -> &'static str {
    let to =
        faucet_core::json_schema_base_type(&change.to).unwrap_or(faucet_core::SqlBaseType::Text);
    let from = change
        .from
        .as_ref()
        .and_then(faucet_core::json_schema_base_type);
    Postgres.widened_type_name(from, to)
}

/// `ALTER TABLE <ref> ALTER COLUMN "<col>" DROP NOT NULL` — relax a NOT NULL
/// constraint. Naturally idempotent.
fn build_drop_not_null_sql(table_ref: &str, col: &str) -> String {
    Postgres
        .drop_not_null_sql(&target_ref(table_ref), col)
        .ok()
        .flatten()
        .expect("postgres DROP NOT NULL")
}

/// Map a PostgreSQL type name (`pg_type.typname`, e.g. `int4`, `float8`, `bool`,
/// `jsonb`) back to a JSON-Schema type fragment so [`PostgresSink::current_schema`]
/// round-trips with [`faucet_core::diff_schema`]. `nullable` reflects whether the
/// column allows NULL (`NOT a.attnotnull`).
fn pg_udt_to_json_schema(udt: &str, nullable: bool) -> serde_json::Value {
    let base = match udt {
        "int2" | "int4" | "int8" => "integer",
        "float4" | "float8" | "numeric" => "number",
        "bool" => "boolean",
        "json" | "jsonb" => "object",
        _ => "string",
    };
    if nullable {
        serde_json::json!({ "type": [base, "null"] })
    } else {
        serde_json::json!({ "type": base })
    }
}

/// A sink that writes JSON records to a PostgreSQL table.
///
/// Built on the SQL family driver (RFC 0014): `PostgresDriver` holds the
/// PostgreSQL I/O (the bootstrap-locked DDL, `COPY`, the overwrite swap, the
/// catalog reads), and [`SqlSinkDriver`] drives writes, per-row outcomes, the
/// exactly-once commit, cleanup, the overwrite lifecycle and schema evolution
/// from it.
pub struct PostgresSink {
    inner: DriverSink<SqlSinkDriver<PostgresDriver>>,
}

/// The PostgreSQL I/O primitives the SQL family driver runs on.
pub(crate) struct PostgresDriver {
    pub(crate) config: PostgresSinkConfig,
    pub(crate) pool: PgPool,
    /// The shared SQL planning engine (#884).
    pub(crate) engine: SqlSinkEngine<Postgres>,
    layout: SqlLayout,
}

/// The engine's view of one PostgreSQL connection (or open transaction).
/// Every value binds as text through its placeholder's `::<udt>` cast, so the
/// destination column's input function parses it (#146 C1).
pub(crate) struct PgExec<'c> {
    pub(crate) conn: &'c mut sqlx::PgConnection,
    pub(crate) sink: &'c PostgresDriver,
}

#[async_trait]
impl SqlExecutor for PgExec<'_> {
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        Ok(table_columns(
            &self
                .sink
                .discover_columns(&mut *self.conn, &target.reference)
                .await?,
        ))
    }

    /// The key columns' underlying types drive the `::udt` casts; a key the
    /// table lacks binds as `text`.
    async fn key_types(
        &mut self,
        target: &TableTarget,
        key: &[String],
    ) -> Result<Vec<Option<String>>, FaucetError> {
        let udts: std::collections::HashMap<String, String> = self
            .sink
            .discover_columns(&mut *self.conn, &target.reference)
            .await?
            .into_iter()
            .collect();
        Ok(key
            .iter()
            .map(|k| Some(udts.get(k).cloned().unwrap_or_else(|| "text".to_string())))
            .collect())
    }

    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        let binary = self.sink.config.binary_encoding;
        let mut q = sqlx::query(&stmt.sql);
        for p in &stmt.params {
            let udt = p.sql_type.unwrap_or("text");
            let value = Some(p.value.as_ref());
            q = q.bind(match stmt.kind {
                StatementKind::Insert | StatementKind::Upsert => {
                    pg_bind_text_with(value, udt, binary).map_err(|e| {
                        FaucetError::Sink(format!(
                            "PostgreSQL insert failed: column {}: {e}",
                            p.column
                        ))
                    })?
                }
                StatementKind::Delete | StatementKind::CleanupLoad => {
                    pg_bind_text_with(value, udt, binary)
                        .map_err(|e| FaucetError::Sink(format!("key column {}: {e}", p.column)))?
                }
                StatementKind::CleanupDelete => pg_bind_text(value, udt),
                _ => p.value.as_str().map(str::to_string),
            });
        }
        let context = match stmt.kind {
            StatementKind::Insert | StatementKind::InsertJson | StatementKind::Upsert => {
                "PostgreSQL insert failed"
            }
            StatementKind::Delete => "PostgreSQL delete failed",
            StatementKind::CleanupDrop => "cleanup: temp table drop failed",
            StatementKind::CleanupCreate => "cleanup: temp table creation failed",
            StatementKind::CleanupLoad => "cleanup: loading keys failed",
            StatementKind::CleanupDelete => "cleanup: delete failed",
            StatementKind::TokenTable => "PostgreSQL commit-table create failed",
            _ => "PostgreSQL token upsert failed",
        };
        q.execute(&mut *self.conn)
            .await
            .map(|r| r.rows_affected())
            .map_err(|e| FaucetError::Sink(format!("{context}: {e}")))
    }

    async fn fetch_text(&mut self, stmt: &Statement<'_>) -> Result<Vec<String>, FaucetError> {
        let mut q = sqlx::query_scalar::<_, String>(&stmt.sql);
        for p in &stmt.params {
            q = q.bind(pg_bind_text(
                Some(p.value.as_ref()),
                p.sql_type.unwrap_or("text"),
            ));
        }
        q.fetch_all(&mut *self.conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: reading keys to journal: {e}")))
    }

    async fn journal_plan(
        &mut self,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        self.sink.journal_plan(&mut *self.conn, plan, run_id).await
    }
}

/// A connection the SQL family driver runs statements on: an open write
/// transaction, or a pooled connection for catalog reads.
pub(crate) enum PgConn<'c> {
    /// A `BEGIN` transaction.
    Tx(sqlx::Transaction<'c, sqlx::Postgres>, &'c PostgresDriver),
    /// A plain pooled connection.
    Pooled(
        sqlx::pool::PoolConnection<sqlx::Postgres>,
        &'c PostgresDriver,
    ),
}

impl PgConn<'_> {
    fn parts(&mut self) -> (&mut sqlx::PgConnection, &PostgresDriver) {
        match self {
            PgConn::Tx(tx, sink) => (&mut **tx, *sink),
            PgConn::Pooled(conn, sink) => (&mut **conn, *sink),
        }
    }

    fn exec(&mut self) -> PgExec<'_> {
        let (conn, sink) = self.parts();
        PgExec { conn, sink }
    }
}

#[async_trait]
impl SqlExecutor for PgConn<'_> {
    async fn columns(&mut self, target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        self.exec().columns(target).await
    }

    async fn key_types(
        &mut self,
        target: &TableTarget,
        key: &[String],
    ) -> Result<Vec<Option<String>>, FaucetError> {
        self.exec().key_types(target, key).await
    }

    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        self.exec().execute(stmt).await
    }

    async fn fetch_text(&mut self, stmt: &Statement<'_>) -> Result<Vec<String>, FaucetError> {
        self.exec().fetch_text(stmt).await
    }

    async fn journal_plan(
        &mut self,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        self.exec().journal_plan(plan, run_id).await
    }
}

impl PostgresSink {
    /// Create a new PostgreSQL sink. Establishes a connection pool.
    pub async fn new(config: PostgresSinkConfig) -> Result<Self, FaucetError> {
        config.write.validate()?;
        if matches!(
            config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) && !matches!(config.column_mapping, PostgresColumnMapping::AutoMap)
        {
            return Err(FaucetError::Config(
                "postgres sink: write_mode upsert/delete requires column_mapping: auto_map \
                 (key columns must be real columns, not inside a JSONB blob)"
                    .into(),
            ));
        }
        // COPY has no ON CONFLICT, so it cannot express upsert/delete. It IS
        // fine for overwrite, whose writes are a plain append into the staging
        // table (the atomic swap is separate DDL).
        if matches!(config.write_method, PostgresWriteMethod::Copy)
            && matches!(
                config.write.write_mode,
                faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
            )
        {
            return Err(FaucetError::Config(format!(
                "postgres sink: write_method: copy is append-only (COPY has no ON CONFLICT); \
                 it cannot be combined with write_mode: {} — use write_method: insert",
                config.write.write_mode.as_str()
            )));
        }

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL connection failed: {e}")))?;

        let engine = SqlSinkEngine::new(Postgres, config.write.clone());
        let mut layout = SqlLayout::new(config.table_name.clone())
            .create_table(config.create_table)
            .batch_size(config.batch_size);
        if let PostgresColumnMapping::Jsonb { column } = &config.column_mapping {
            layout = layout.json_column(column.clone());
        }
        let driver = PostgresDriver {
            config,
            pool,
            engine,
            layout,
        };
        Ok(Self {
            inner: DriverSink::new(SqlSinkDriver::new(driver)),
        })
    }
}

impl PostgresSink {
    /// Join the overwrite run `token`
    /// ([`faucet_core::idempotency::OVERWRITE_RUN_KEY`]): every instance of
    /// one run (begin, writers, commit) stages in `<table>__faucet_ovw_<token>`,
    /// so a concurrent overwrite run of the same table neither drops nor
    /// shares it.
    pub fn with_overwrite_run(mut self, token: &str) -> Result<Self, FaucetError> {
        faucet_core::idempotency::check_overwrite_run_token(token)?;
        let driver = self.inner.driver_mut().driver_mut();
        driver.layout = driver.layout.clone().overwrite_run(Some(token.to_string()));
        Ok(self)
    }
}

#[async_trait]
impl faucet_core::Sink for PostgresSink {
    faucet_core::forward_sink!(inner);
}

impl PostgresDriver {
    pub(crate) async fn table_exists(&self, table: &str) -> Result<bool, FaucetError> {
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), table);
        let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(&table_ref)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres table probe failed: {e}")))?;
        Ok(exists.is_some())
    }

    /// Staging table name used while an overwrite run is in flight (same schema
    /// as the target).
    fn staging_table_name(&self) -> String {
        run_staging_table_name(
            &self.config.table_name,
            self.layout.overwrite_run.as_deref(),
        )
    }

    /// Every table in the target's schema named like a tokened staging of it.
    async fn tokened_staging_tables(&self) -> Result<Vec<String>, FaucetError> {
        let pattern = format!(
            "{}%",
            faucet_core::idempotency::like_literal(
                &faucet_core::idempotency::overwrite_staging_prefix(&run_staging_base(
                    &self.config.table_name
                )),
                '\\'
            )
        );
        sqlx::query_scalar(
            "SELECT c.relname::text FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') AND c.relname LIKE $1 ESCAPE '\\' \
             AND n.nspname = COALESCE($2, current_schema())",
        )
        .bind(pattern)
        .bind(self.config.schema.as_deref())
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("postgres overwrite: list staging tables: {e}")))
    }

    async fn acquire(&self) -> Result<sqlx::pool::PoolConnection<sqlx::Postgres>, FaucetError> {
        self.pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL pool acquire failed: {e}")))
    }

    /// Create the target (and its schema, when `schema:` is set) under the
    /// bootstrap lock (#580, #828).
    async fn create_target(&self, create: &str) -> Result<(), FaucetError> {
        let mut statements = Vec::with_capacity(2);
        if let Some(schema) = self.config.schema.as_deref() {
            statements.push(format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                quote_ident(schema)
            ));
        }
        statements.push(create.to_string());
        let mut conn = self.acquire().await?;
        run_bootstrap_ddl(
            &mut conn,
            &bootstrap_lock_key(self.config.schema.as_deref()),
            &statements,
        )
        .await
        .map_err(|e| FaucetError::Sink(format!("postgres CREATE SCHEMA/TABLE failed: {e}")))
    }

    /// Discover the target relation's column names and underlying types
    /// (`pg_type.typname`), scoped to the *exact* relation the writes target
    /// via `to_regclass` (#146 M13). Shared by the INSERT and COPY paths so
    /// both see an identical column set. `::text` casts the `name`-typed
    /// catalog columns so sqlx decodes them as `String`.
    pub(crate) async fn discover_columns(
        &self,
        conn: &mut sqlx::PgConnection,
        table_ref: &str,
    ) -> Result<Vec<(String, String)>, FaucetError> {
        let columns: Vec<(String, String)> = sqlx::query(
            "SELECT a.attname::text AS column_name, t.typname::text AS udt_name \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = to_regclass($1)::oid \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(table_ref)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("column_name"),
                row.get::<String, _>("udt_name"),
            )
        })
        .collect();

        if columns.is_empty() {
            return Err(FaucetError::Sink(format!(
                "table {table_ref} has no columns or does not exist"
            )));
        }
        Ok(columns)
    }

    /// Write one chunk via `COPY … FROM STDIN (FORMAT text)` — the bulk-load
    /// fast-path (issue #308). Append-only (validated at construction); the
    /// server parses each field with the destination column's input function,
    /// so type semantics match the `INSERT` path exactly. A bad row fails the
    /// whole COPY (all-or-nothing, like a failed multi-row `INSERT`).
    async fn copy_batch(
        &self,
        conn: &mut sqlx::PgConnection,
        table_ref: &str,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let (statement, payload) = match &self.config.column_mapping {
            PostgresColumnMapping::Jsonb { column } => {
                let payload = build_jsonb_payload(records);
                (
                    copy_statement(table_ref, std::slice::from_ref(column)),
                    payload,
                )
            }
            PostgresColumnMapping::AutoMap => {
                let columns = self.discover_columns(&mut *conn, table_ref).await?;
                let Some(payload) =
                    build_auto_map_payload(records, &columns, self.config.binary_encoding)
                        .map_err(FaucetError::Sink)?
                else {
                    return Ok(0);
                };
                (copy_statement(table_ref, &payload.columns), payload)
            }
        };

        let mut copy_in = conn
            .copy_in_raw(&statement)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL COPY start failed: {e}")))?;
        // Ship in ~1 MiB slices so a huge page never materializes a second
        // time inside sqlx's write buffer.
        const SEND_CHUNK: usize = 1 << 20;
        for chunk in payload.data.as_bytes().chunks(SEND_CHUNK) {
            if let Err(e) = copy_in.send(chunk).await {
                // Dropping the handle aborts the COPY server-side; surface
                // the original error.
                return Err(FaucetError::Sink(format!(
                    "PostgreSQL COPY send failed: {e}"
                )));
            }
        }
        copy_in
            .finish()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL COPY failed: {e}")))?;
        Ok(payload.rows)
    }

    /// Insert a batch of records using JSONB column mode, on the given
    /// connection: one `INSERT … SELECT * FROM unnest($1::jsonb[])`.
    async fn insert_jsonb(
        &self,
        conn: &mut sqlx::PgConnection,
        table_ref: &str,
        records: &[Value],
        column: &str,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let json_values: Vec<serde_json::Value> = records.to_vec();
        let query = format!(
            "INSERT INTO {table_ref} ({}) SELECT * FROM unnest($1::jsonb[])",
            quote_ident(column)
        );
        sqlx::query(&query)
            .bind(json_values)
            .execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;
        Ok(records.len())
    }

    /// The engine target of the configured table itself (keyed writes and
    /// cleanup never go to the overwrite staging table).
    fn table_target(&self) -> TableTarget {
        target_ref(&qualified_table_ref(
            self.config.schema.as_deref(),
            &self.config.table_name,
        ))
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((v1, …), …)` with per-column `::udt`
    /// casts (the key columns' underlying types), chunked at the param cap.
    pub(crate) async fn delete_by_keys(
        &self,
        conn: &mut sqlx::PgConnection,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        let mut exec = PgExec { conn, sink: self };
        self.engine
            .delete(&mut exec, &self.table_target(), deletes)
            .await
    }

    /// Ensure the commit-token watermark table exists.
    pub(crate) async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = Postgres.commit_token_table_sql();
        let mut conn = self.acquire().await?;
        // Called per page: probe first so the bootstrap lock is only taken
        // while the table is missing.
        let exists: Option<String> = sqlx::query_scalar("SELECT to_regclass($1)::text")
            .bind(quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE))
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL commit-table probe failed: {e}")))?;
        if exists.is_some() {
            return Ok(());
        }
        run_bootstrap_ddl(&mut conn, &bootstrap_lock_key(None), &[sql])
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("PostgreSQL commit-table create failed: {e}"))
            })?;
        Ok(())
    }
}

#[async_trait]
impl SqlDriver for PostgresDriver {
    type Dialect = Postgres;
    type Conn<'a> = PgConn<'a>;

    fn engine(&self) -> &SqlSinkEngine<Postgres> {
        &self.engine
    }

    fn layout(&self) -> &SqlLayout {
        &self.layout
    }

    fn connector_name(&self) -> &'static str {
        "postgres"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let table = match &self.config.schema {
            Some(s) => format!("{}.{}", s, self.config.table_name),
            None => self.config.table_name.clone(),
        };
        format!(
            "{}?table={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            table
        )
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database is
    /// reachable and the credentials are accepted without writing anything.
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
                    "check connection_url / credentials / that the database is reachable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check connection_url / credentials / that the database is reachable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    /// Every operation but the overwrite commit and the cleanup is bounded by
    /// `write_timeout_secs` (#789 SQL-109).
    fn guard<'a, T: Send + 'a>(
        &'a self,
        what: &'static str,
        fut: SqlFuture<'a, T>,
    ) -> SqlFuture<'a, T> {
        match what {
            label::COMMIT_OVERWRITE | label::CLEANUP_SCOPE => fut,
            _ => Box::pin(bounded(self.config.write_timeout_secs, what, fut)),
        }
    }

    /// A configured `scope` replaces only its window (#518).
    fn capabilities(&self, derived: faucet_core::Capabilities) -> faucet_core::Capabilities {
        derived.to_builder().scoped_overwrite(true).build()
    }

    fn target(&self, table: &str) -> TableTarget {
        target_ref(&qualified_table_ref(self.config.schema.as_deref(), table))
    }

    fn staging_table(&self) -> String {
        self.staging_table_name()
    }

    fn staging_base(&self) -> String {
        run_staging_base(&self.config.table_name)
    }

    fn legacy_staging_table(&self) -> String {
        overwrite_staging_table_name(&self.config.table_name)
    }

    async fn overwrite_staging_tables(&self) -> Result<Vec<String>, FaucetError> {
        self.tokened_staging_tables().await
    }

    fn table_display(&self, table: &str) -> String {
        qualified_table_ref(self.config.schema.as_deref(), table)
    }

    async fn begin(&self) -> Result<PgConn<'_>, FaucetError> {
        let tx =
            self.pool.begin().await.map_err(|e| {
                FaucetError::Sink(format!("PostgreSQL transaction begin failed: {e}"))
            })?;
        Ok(PgConn::Tx(tx, self))
    }

    async fn commit(&self, conn: PgConn<'_>) -> Result<(), FaucetError> {
        match conn {
            PgConn::Tx(tx, _) => tx.commit().await.map_err(|e| {
                FaucetError::Sink(format!("PostgreSQL transaction commit failed: {e}"))
            }),
            PgConn::Pooled(..) => Ok(()),
        }
    }

    async fn connection(&self) -> Result<PgConn<'_>, FaucetError> {
        let conn =
            self.pool.acquire().await.map_err(|e| {
                FaucetError::Sink(format!("PostgreSQL connection acquire failed: {e}"))
            })?;
        Ok(PgConn::Pooled(conn, self))
    }

    async fn table_exists(&self, table: &str) -> Result<bool, FaucetError> {
        PostgresDriver::table_exists(self, table).await
    }

    /// The target and the commit-token table are created under the
    /// bootstrap lock (#828); any other statement runs on a pooled
    /// connection.
    async fn ddl(&self, sql: &str, what: &'static str) -> Result<(), FaucetError> {
        match what {
            label::CREATE_TABLE => self.create_target(sql).await,
            label::CREATE_TOKEN_TABLE => self.ensure_commit_table().await,
            _ => {
                let mut conn = self.acquire().await?;
                sqlx::query(sql)
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| FaucetError::Sink(format!("postgres {what}: {e}")))?;
                Ok(())
            }
        }
    }

    async fn read_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let row = sqlx::query(&Postgres.commit_token_select_sql())
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    /// `CREATE TABLE staging (LIKE target INCLUDING DEFAULTS INCLUDING
    /// GENERATED)`, after refusing a target whose swap cannot run (a foreign
    /// key the clear would break). Identity columns are filled by the
    /// target's sequence at the swap, so staging accepts rows that omit them.
    async fn create_staging(&self) -> Result<(), FaucetError> {
        let staging =
            qualified_table_ref(self.config.schema.as_deref(), &self.staging_table_name());
        let target = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let mut conn = self.acquire().await?;
        let referencing = referencing_foreign_keys(&mut conn, &target).await?;
        clear_statements(&target, &referencing)?;
        sqlx::query(&format!(
            "CREATE TABLE {staging} (LIKE {target} INCLUDING DEFAULTS INCLUDING GENERATED)"
        ))
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            FaucetError::Sink(format!(
                "postgres overwrite: create staging from '{}' (does the table exist?): {e}",
                self.config.table_name
            ))
        })?;
        let roles = column_roles(&mut conn, &target).await?;
        for (col, _) in &roles.identity {
            sqlx::query(&format!(
                "ALTER TABLE {staging} ALTER COLUMN {} DROP NOT NULL",
                quote_ident(col)
            ))
            .execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres overwrite: prepare staging: {e}")))?;
        }
        Ok(())
    }

    /// First run: staging holds everything; rename it into place.
    async fn publish_staging(&self) -> Result<(), FaucetError> {
        let staging =
            qualified_table_ref(self.config.schema.as_deref(), &self.staging_table_name());
        sqlx::query(&format!(
            "ALTER TABLE {staging} RENAME TO {}",
            quote_ident(&self.config.table_name)
        ))
        .execute(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("postgres overwrite: publish staging: {e}")))?;
        Ok(())
    }

    /// Atomically replace the destination in one transaction. Full overwrite:
    /// `TRUNCATE target; INSERT INTO target SELECT * FROM staging; DROP staging`.
    /// Scoped/windowed overwrite (#518): `DELETE FROM target WHERE <scope>;
    /// INSERT …; DROP staging` — only the in-scope rows are replaced, the rest
    /// preserved. Postgres runs TRUNCATE and DDL transactionally, so a failure
    /// rolls the whole swap back and the prior rows survive.
    async fn swap_staging(&self) -> Result<(), FaucetError> {
        let staging =
            qualified_table_ref(self.config.schema.as_deref(), &self.staging_table_name());
        let target = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres overwrite: begin swap: {e}")))?;
        // A scope replaces only the matching rows, and only in-scope staged
        // rows may land: a row outside the window would duplicate one the
        // delete never touched (#789 SQL-51).
        let window = self
            .config
            .scope
            .as_ref()
            .map(|scope| scope.render_where_literal(&quote_ident(scope.column())));
        let clear = match &window {
            Some(pred) => {
                let outside: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM {staging} WHERE ({pred}) IS NOT TRUE"
                ))
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("postgres overwrite: check scope: {e}")))?;
                if outside > 0 {
                    return Err(FaucetError::Sink(format!(
                        "postgres overwrite: {outside} staged row(s) fall outside the overwrite \
                         scope ({pred}); nothing was replaced. Filter the source to the window"
                    )));
                }
                vec![format!("DELETE FROM {target} WHERE {pred}")]
            }
            None => clear_statements(&target, &referencing_foreign_keys(&mut tx, &target).await?)?,
        };
        // `rollback.keep_previous`: snapshot the rows about to be replaced, in
        // the same transaction, so a rollback can swap them back (#706).
        if self.config.write.keeps_previous() {
            self.keep_previous_copy(&mut tx).await?;
        }
        let roles = column_roles(&mut tx, &target).await?;
        let staged: Vec<String> = self
            .discover_columns(&mut tx, &staging)
            .await?
            .into_iter()
            .map(|(c, _)| c)
            .collect();
        let copy = roles.copy_statements(&target, &staging, window.as_deref(), |c| {
            staged.iter().any(|s| s == c)
        });
        let statements = clear
            .into_iter()
            .chain(copy)
            .chain([format!("DROP TABLE {staging}")]);
        for stmt in statements {
            sqlx::query(&stmt)
                .execute(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("postgres overwrite swap failed: {e}")))?;
        }
        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres overwrite: commit swap: {e}")))?;
        Ok(())
    }

    /// Read the live destination schema from `pg_catalog` as an
    /// `infer_schema`-shaped object (`{"type":"object","properties":{…}}`), or
    /// `None` when the target table does not exist yet (issue #194).
    ///
    /// Reuses the AutoMap column-discovery query shape (scoped to the exact
    /// relation via `to_regclass`), additionally reading `a.attnotnull` so
    /// nullability round-trips through `pg_udt_to_json_schema`.
    async fn current_schema(&self) -> Result<Option<serde_json::Value>, FaucetError> {
        // JSON-column mode stores each record whole; the physical columns are
        // not the record's fields, so there is nothing to drift against
        // (#789 SQL-20).
        if !matches!(self.config.column_mapping, PostgresColumnMapping::AutoMap) {
            return Ok(None);
        }
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let rows: Vec<(String, String, bool)> = sqlx::query(
            "SELECT a.attname::text AS column_name, t.typname::text AS udt_name, a.attnotnull \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = to_regclass($1)::oid \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&table_ref)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("postgres current_schema query failed: {e}")))?
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("column_name"),
                row.get::<String, _>("udt_name"),
                row.get::<bool, _>("attnotnull"),
            )
        })
        .collect();

        if rows.is_empty() {
            return Ok(None); // table does not exist yet
        }

        let mut props = serde_json::Map::new();
        for (name, udt, notnull) in rows {
            props.insert(name, pg_udt_to_json_schema(&udt, !notnull));
        }
        Ok(Some(
            serde_json::json!({ "type": "object", "properties": props }),
        ))
    }

    fn evolution(&self) -> Option<&dyn SqlEvolution> {
        Some(self)
    }

    /// `COPY` serves plain pages under `write_method: copy`; the exactly-once
    /// page stays a multi-row `INSERT`. JSONB mode is one `unnest` insert.
    async fn insert(
        &self,
        conn: &mut PgConn<'_>,
        target: &TableTarget,
        records: &[Value],
        path: InsertPath,
    ) -> Result<usize, FaucetError> {
        let (raw, _) = conn.parts();
        if path == InsertPath::Page && matches!(self.config.write_method, PostgresWriteMethod::Copy)
        {
            return self.copy_batch(raw, &target.reference, records).await;
        }
        match &self.config.column_mapping {
            PostgresColumnMapping::Jsonb { column } => {
                self.insert_jsonb(raw, &target.reference, records, column)
                    .await
            }
            PostgresColumnMapping::AutoMap => {
                let mut exec = PgExec {
                    conn: raw,
                    sink: self,
                };
                self.engine.insert(&mut exec, target, records, false).await
            }
        }
    }

    /// A `bytea` value `binary_encoding` refuses fails only its own row.
    fn split_page(
        &self,
        _target: &TableTarget,
        records: &[Value],
        columns: &[TableColumn],
        require_match: bool,
    ) -> SplitPage {
        split_columns(records, columns, self.config.binary_encoding, require_match)
    }

    fn splits_keyed_pages(&self) -> bool {
        true
    }

    /// Column-mapping mode only: the run-id column, the journaled keys and the
    /// kept previous table all address real columns (#706).
    fn rollback(&self) -> Option<&dyn RollbackDriver> {
        Some(self)
    }

    fn readback_source(&self) -> Option<(String, Value)> {
        self.readback_source_impl()
    }
}

#[async_trait]
impl SqlEvolution for PostgresDriver {
    /// Apply an additive schema evolution (new columns, lossless widenings,
    /// nullability relaxations) to the destination table. Idempotent —
    /// `ADD COLUMN IF NOT EXISTS`, and re-running the same TYPE / DROP NOT NULL
    /// is a no-op (issue #194).
    async fn apply_evolution(&self, steps: Vec<EvolutionStep>) -> Result<(), FaucetError> {
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres evolve acquire failed: {e}")))?;
        for step in steps {
            let (sql, what, column) = match step {
                EvolutionStep::Add { column, base } => (
                    build_add_column_sql(&table_ref, &column, base),
                    "ADD COLUMN",
                    column,
                ),
                EvolutionStep::Widen {
                    column, type_name, ..
                } => (
                    build_alter_type_sql(&table_ref, &column, type_name),
                    "ALTER TYPE",
                    column,
                ),
                EvolutionStep::Relax { column } => (
                    build_drop_not_null_sql(&table_ref, &column),
                    "DROP NOT NULL",
                    column,
                ),
                _ => continue,
            };
            sqlx::query(&sql)
                .execute(&mut *conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("postgres {what} {column} failed: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl RollbackDriver for PostgresDriver {
    fn enabled(&self) -> bool {
        self.rollback_supported()
    }

    async fn rollback_run(
        &self,
        calls: &faucet_core::CallSlot,
        run_id: &str,
        opts: &faucet_core::rollback::RollbackOptions,
    ) -> Result<faucet_core::rollback::RollbackOutcome, FaucetError> {
        calls
            .call_once("rollback_run", self.rollback_run_impl(run_id, opts))
            .await
    }

    async fn forget_run(
        &self,
        calls: &faucet_core::CallSlot,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        calls
            .call_once(
                "forget_run",
                bounded(
                    self.config.write_timeout_secs,
                    "forget_run",
                    self.forget_run_impl(run_id),
                ),
            )
            .await
    }

    async fn rewind_commit_token(
        &self,
        calls: &faucet_core::CallSlot,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        calls
            .call_once(
                "rewind_commit_token",
                bounded(
                    self.config.write_timeout_secs,
                    "rewind_commit_token",
                    self.rewind_commit_token_impl(scope, token),
                ),
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ColumnRoles, PostgresSinkConfig, ReferencingKey, build_add_column_sql,
        build_alter_type_sql, build_create_table_sql, build_drop_not_null_sql, clear_statements,
        derived_table_name, on_conflict_clause, pg_bind_text, pg_udt_to_json_schema,
        qualified_table_ref, split_writable, widened_keyword,
    };
    use super::{
        FaucetError, bootstrap_lock_key, bounded, is_concurrent_ddl, is_concurrent_ddl_code,
    };

    #[tokio::test]
    async fn bounded_writes_time_out_and_pass_results_through() {
        let ok = bounded(5, "write", async { Ok::<_, FaucetError>(7) }).await;
        assert_eq!(ok.unwrap(), 7);
        let unbounded = bounded(0, "write", async { Ok::<_, FaucetError>(8) }).await;
        assert_eq!(unbounded.unwrap(), 8);
        let err = bounded(
            1,
            "flush",
            std::future::pending::<Result<(), FaucetError>>(),
        )
        .await
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("postgres flush timed out after 1s"),
            "{err}"
        );
    }

    use serde_json::json;

    #[test]
    fn derived_names_stay_within_63_bytes_and_distinct() {
        assert_eq!(derived_table_name("t", "__faucet_ovw"), "t__faucet_ovw");
        let long = "x".repeat(63);
        let ovw = derived_table_name(&long, "__faucet_ovw");
        let prev = derived_table_name(&long, "__faucet_prev");
        assert!(ovw.len() <= 63 && prev.len() <= 63, "{ovw} {prev}");
        assert_ne!(ovw, long);
        assert_ne!(ovw, prev);
        assert!(ovw.ends_with("__faucet_ovw"));
        let other = derived_table_name(&format!("{}y", "x".repeat(62)), "__faucet_ovw");
        assert_ne!(ovw, other, "the hash keeps similar long names apart");
        let multibyte = derived_table_name(&"é".repeat(40), "__faucet_prev");
        assert!(multibyte.len() <= 63);
        assert_eq!(super::overwrite_staging_table_name(&long), ovw);
        // Tokened names shorten the same way, sharing one base per table.
        let token = "67000000abcdef";
        let run = super::run_staging_table_name(&long, Some(token));
        assert!(run.len() <= 63, "{run}");
        assert_eq!(
            run,
            format!("{}__faucet_ovw_{token}", super::run_staging_base(&long))
        );
        assert_eq!(
            super::run_staging_table_name("t", Some(token)),
            "t__faucet_ovw_67000000abcdef"
        );
        assert_eq!(super::run_staging_base("t"), "t");
        assert_eq!(super::run_staging_table_name("t", None), "t__faucet_ovw");
    }

    #[test]
    fn unmatched_records_become_row_failures() {
        let cols = vec![("id".to_string(), "int4".to_string())];
        let (outcomes, writable, _) = split_writable(
            &[json!({"id": 1}), json!({"ID": 2}), json!([1])],
            &cols,
            super::BinaryEncoding::Auto,
            true,
        );
        assert!(outcomes[0].is_ok());
        let err = outcomes[1].as_ref().unwrap_err().to_string();
        assert!(err.contains("record 1 has no field matching"), "{err}");
        assert!(
            outcomes[2].is_ok(),
            "non-objects are left to the write path"
        );
        assert_eq!(writable, vec![json!({"id": 1}), json!([1])]);
    }

    #[test]
    fn a_bytea_value_the_encoding_refuses_fails_only_its_row() {
        let cols = vec![
            ("id".to_string(), "int4".to_string()),
            ("blob".to_string(), "bytea".to_string()),
        ];
        let page = [
            json!({"id": 1, "blob": "SGVsbG8="}),
            json!({"id": 2, "blob": "plain token!"}),
            json!({"id": 3}),
        ];
        let (outcomes, writable, positions) =
            split_writable(&page, &cols, super::BinaryEncoding::Base64, false);
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok());
        let err = outcomes[1].as_ref().unwrap_err().to_string();
        assert!(err.contains("row 1: column blob"), "{err}");
        assert_eq!(writable, vec![page[0].clone(), page[2].clone()]);
        assert_eq!(positions, vec![0, 2]);
        let (outcomes, _, _) = split_writable(&page, &cols, super::BinaryEncoding::Text, false);
        assert!(outcomes.iter().all(Result::is_ok));
    }

    fn referencing(on_delete: char, deferrable: bool, has_references: bool) -> ReferencingKey {
        ReferencingKey {
            name: "fk".into(),
            child: "child".into(),
            on_delete,
            deferrable,
            has_references,
        }
    }

    #[test]
    fn bytea_follows_binary_encoding() {
        use super::{BinaryEncoding, pg_bind_text_with};
        let v = json!("SGVsbG8=");
        let bind = |v: &serde_json::Value, e| pg_bind_text_with(Some(v), "bytea", e);
        assert_eq!(
            bind(&v, BinaryEncoding::Auto).unwrap().as_deref(),
            Some(r"\x48656c6c6f")
        );
        assert_eq!(
            bind(&json!(r"\x0a"), BinaryEncoding::Auto)
                .unwrap()
                .as_deref(),
            Some(r"\x0a")
        );
        assert_eq!(
            bind(&json!("plain text"), BinaryEncoding::Auto)
                .unwrap()
                .as_deref(),
            Some("plain text")
        );
        assert_eq!(
            bind(&json!("abcd"), BinaryEncoding::Text)
                .unwrap()
                .as_deref(),
            Some(r"\x61626364")
        );
        assert_eq!(
            bind(&v, BinaryEncoding::Base64).unwrap().as_deref(),
            Some(r"\x48656c6c6f")
        );
        let err = bind(&json!("abc"), BinaryEncoding::Base64).unwrap_err();
        assert!(err.contains("not base64"), "{err}");
        assert_eq!(
            bind(&json!(1), BinaryEncoding::Base64).unwrap().as_deref(),
            Some("1")
        );
    }

    #[test]
    fn clear_uses_truncate_unless_referenced() {
        assert_eq!(
            clear_statements("\"t\"", &[]).unwrap(),
            vec!["TRUNCATE TABLE \"t\"".to_string()]
        );
        let deferred = vec![
            "SET CONSTRAINTS ALL DEFERRED".to_string(),
            "DELETE FROM \"t\"".to_string(),
        ];
        assert_eq!(
            clear_statements("\"t\"", &[referencing('a', true, true)]).unwrap(),
            deferred
        );
        for (action, deferrable) in [('a', false), ('r', true), ('r', false)] {
            assert_eq!(
                clear_statements("\"t\"", &[referencing(action, deferrable, false)]).unwrap(),
                deferred,
                "no child row references the table"
            );
            let err = clear_statements("\"t\"", &[referencing(action, deferrable, true)])
                .unwrap_err()
                .to_string();
            assert!(err.contains("DEFERRABLE"), "{err}");
        }
        for action in ['c', 'n', 'd'] {
            assert!(clear_statements("\"t\"", &[referencing(action, true, false)]).is_err());
        }
    }

    #[test]
    fn copy_statements_split_on_identity_and_skip_generated() {
        let plain = ColumnRoles {
            insertable: vec!["a".into(), "b".into()],
            identity: vec![],
        };
        assert_eq!(
            plain.copy_statements("t", "s", Some("x > 1"), |_| true),
            vec!["INSERT INTO t (\"a\", \"b\") SELECT \"a\", \"b\" FROM s WHERE x > 1".to_string()]
        );
        let ident = ColumnRoles {
            insertable: vec!["id".into(), "name".into()],
            identity: vec![("id".into(), 'a')],
        };
        let stmts = ident.copy_statements("t", "s", None, |c| c != "gone");
        assert_eq!(
            stmts,
            vec![
                "INSERT INTO t (\"id\", \"name\") OVERRIDING SYSTEM VALUE SELECT \"id\", \"name\" \
                 FROM s WHERE \"id\" IS NOT NULL"
                    .to_string(),
                "INSERT INTO t (\"name\") SELECT \"name\" FROM s WHERE \"id\" IS NULL".to_string(),
            ]
        );
        let filtered = ident.copy_statements("t", "s", Some("w"), |_| true);
        assert!(filtered[0].ends_with("WHERE (\"id\" IS NOT NULL) AND (w)"));
        let only_id = ColumnRoles {
            insertable: vec!["id".into()],
            identity: vec![("id".into(), 'd')],
        };
        assert_eq!(only_id.copy_statements("t", "s", None, |_| true).len(), 1);
    }

    #[test]
    fn pg_add_column_ddl() {
        let sql = build_add_column_sql("\"public\".\"t\"", "email", faucet_core::SqlBaseType::Text);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"t\" ADD COLUMN IF NOT EXISTS \"email\" text"
        );
    }

    #[test]
    fn integer_columns_widen_to_exact_numeric() {
        use serde_json::json;
        let change = |from: serde_json::Value, to: serde_json::Value| faucet_core::ColumnChange {
            name: "c".into(),
            from: Some(from),
            to,
        };
        assert_eq!(
            widened_keyword(&change(
                json!({"type": "integer"}),
                json!({"type": "number"})
            )),
            "numeric"
        );
        assert_eq!(
            widened_keyword(&change(
                json!({"type": "integer"}),
                json!({"type": ["integer", "null"]})
            )),
            "bigint"
        );
        assert_eq!(
            widened_keyword(&change(
                json!({"type": "number"}),
                json!({"type": "number"})
            )),
            "double precision"
        );
    }

    #[test]
    fn pg_widen_column_ddl() {
        let sql = build_alter_type_sql("\"public\".\"t\"", "score", "double precision");
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"t\" ALTER COLUMN \"score\" TYPE double precision USING \"score\"::double precision"
        );
    }

    #[test]
    fn pg_drop_not_null_ddl() {
        let sql = build_drop_not_null_sql("\"t\"", "created_at");
        assert_eq!(
            sql,
            "ALTER TABLE \"t\" ALTER COLUMN \"created_at\" DROP NOT NULL"
        );
    }

    #[test]
    fn pg_udt_round_trips_to_json_schema() {
        assert_eq!(
            pg_udt_to_json_schema("int8", false),
            json!({"type":"integer"})
        );
        assert_eq!(
            pg_udt_to_json_schema("float8", false),
            json!({"type":"number"})
        );
        assert_eq!(
            pg_udt_to_json_schema("bool", false),
            json!({"type":"boolean"})
        );
        assert_eq!(
            pg_udt_to_json_schema("jsonb", false),
            json!({"type":"object"})
        );
        assert_eq!(
            pg_udt_to_json_schema("text", false),
            json!({"type":"string"})
        );
        // Unknown types fall back to string; nullable widens the type array.
        assert_eq!(
            pg_udt_to_json_schema("timestamptz", true),
            json!({"type":["string","null"]})
        );
    }

    #[test]
    fn upsert_on_conflict_clause_for_keys() {
        let clause = on_conflict_clause(
            &["id".to_string()],
            &["id".to_string(), "name".to_string(), "email".to_string()],
        );
        assert_eq!(
            clause,
            r#"ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "email" = EXCLUDED."email""#
        );
    }

    #[test]
    fn upsert_on_conflict_all_columns_are_key_does_nothing() {
        let clause = on_conflict_clause(&["id".to_string()], &["id".to_string()]);
        assert_eq!(clause, r#"ON CONFLICT ("id") DO NOTHING"#);
    }

    #[test]
    fn commit_token_table_is_the_shared_constant() {
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            "_faucet_commit_token"
        );
    }

    // dataset_uri test is skipped: PostgresSink::new() requires a live pool
    // (connects to PostgreSQL in new()), and no offline constructor exists.
    // The URI format is covered by unit tests in faucet-core's redact tests.

    #[test]
    fn bootstrap_lock_key_is_per_schema() {
        assert_eq!(bootstrap_lock_key(None), "faucet-ddl:");
        assert_eq!(bootstrap_lock_key(Some("raw")), "faucet-ddl:raw");
        assert_ne!(bootstrap_lock_key(Some("a")), bootstrap_lock_key(Some("b")));
    }

    #[test]
    fn concurrent_ddl_codes_are_the_catalog_race_outcomes() {
        for code in ["23505", "42P06", "42P07", "42710"] {
            assert!(is_concurrent_ddl_code(code), "{code}");
        }
        for code in ["42601", "42501", "23502", "40001"] {
            assert!(!is_concurrent_ddl_code(code), "{code}");
        }
        assert!(!is_concurrent_ddl(&sqlx::Error::PoolTimedOut));
    }

    #[test]
    fn qualified_table_ref_unqualified_is_bare_quoted_table() {
        // No schema → bare quoted table, resolved against the search_path.
        assert_eq!(qualified_table_ref(None, "events"), "\"events\"");
    }

    #[test]
    fn qualified_table_ref_with_schema_is_schema_dot_table() {
        // With a schema → "schema"."table", so discovery and INSERT both
        // target the same explicit relation (#146 M13).
        assert_eq!(
            qualified_table_ref(Some("analytics"), "events"),
            "\"analytics\".\"events\""
        );
    }

    #[test]
    fn qualified_table_ref_escapes_embedded_quotes() {
        // SQL-injection safety: embedded double-quotes are doubled.
        assert_eq!(
            qualified_table_ref(Some("we\"ird"), "ta\"ble"),
            "\"we\"\"ird\".\"ta\"\"ble\""
        );
    }

    #[test]
    fn null_and_absent_bind_sql_null() {
        assert_eq!(pg_bind_text(None, "text"), None);
        assert_eq!(pg_bind_text(Some(&json!(null)), "int4"), None);
        assert_eq!(pg_bind_text(Some(&json!(null)), "jsonb"), None);
    }

    #[test]
    fn scalars_bind_plain_text_for_typed_columns() {
        // The `$N::<udt>` cast parses these via the column's input function.
        assert_eq!(
            pg_bind_text(Some(&json!(42)), "int4").as_deref(),
            Some("42")
        );
        assert_eq!(
            pg_bind_text(Some(&json!(1.5)), "numeric").as_deref(),
            Some("1.5")
        );
        assert_eq!(
            pg_bind_text(Some(&json!(true)), "bool").as_deref(),
            Some("true")
        );
        assert_eq!(
            pg_bind_text(Some(&json!("2025-01-01T00:00:00Z")), "timestamptz").as_deref(),
            Some("2025-01-01T00:00:00Z")
        );
        // A plain string into TEXT keeps NO JSON quotes (the bug bound `"Bob"`).
        assert_eq!(
            pg_bind_text(Some(&json!("Bob")), "text").as_deref(),
            Some("Bob")
        );
        // Large u64 beyond i64 keeps exact text (no f64 precision loss).
        assert_eq!(
            pg_bind_text(Some(&json!(18446744073709551615u64)), "numeric").as_deref(),
            Some("18446744073709551615")
        );
    }

    #[test]
    fn json_columns_get_json_text_with_quotes_preserved() {
        // For jsonb/json columns the value is bound as JSON text so the
        // `::jsonb` cast parses it: a string keeps its quotes, objects/arrays
        // round-trip.
        assert_eq!(
            pg_bind_text(Some(&json!("Bob")), "jsonb").as_deref(),
            Some("\"Bob\"")
        );
        assert_eq!(
            pg_bind_text(Some(&json!({"a": 1})), "jsonb").as_deref(),
            Some("{\"a\":1}")
        );
        assert_eq!(
            pg_bind_text(Some(&json!([1, 2])), "json").as_deref(),
            Some("[1,2]")
        );
        assert_eq!(pg_bind_text(Some(&json!(5)), "jsonb").as_deref(), Some("5"));
        // udt match is case-insensitive.
        assert_eq!(
            pg_bind_text(Some(&json!("x")), "JSONB").as_deref(),
            Some("\"x\"")
        );
    }

    #[test]
    fn objects_into_non_json_columns_emit_json_text_so_the_cast_fails_loudly() {
        // No scalar text form for an object targeting e.g. an int column; the
        // `::int4` cast will reject this rather than silently coercing.
        assert_eq!(
            pg_bind_text(Some(&json!({"a": 1})), "int4").as_deref(),
            Some("{\"a\":1}")
        );
    }

    #[test]
    fn create_table_sql_renders_inferred_columns() {
        let cols = faucet_core::plan_columns(&[
            serde_json::json!({ "id": 1, "name": "a", "amount": 1.5, "meta": {"k": 1} }),
        ])
        .expect("a plan");
        let sql = build_create_table_sql(r#""public"."orders""#, &cols, None, &[]);
        assert!(
            sql.starts_with(r#"CREATE TABLE IF NOT EXISTS "public"."orders" ("#),
            "{sql}"
        );
        assert!(sql.contains(r#""id" bigint"#), "{sql}");
        assert!(sql.contains(r#""name" text"#), "{sql}");
        assert!(sql.contains(r#""amount" double precision"#), "{sql}");
        assert!(sql.contains(r#""meta" jsonb"#), "{sql}");
        // IF NOT EXISTS, not a probe-then-create: two matrix rows writing one
        // table would otherwise race and fail the loser for no reason.
        assert!(sql.contains("IF NOT EXISTS"), "{sql}");
    }

    #[test]
    fn create_table_sql_for_a_keyed_write_carries_a_primary_key() {
        let key = vec!["id".to_string()];
        let cols =
            faucet_core::plan_keyed_columns(&[serde_json::json!({ "id": 1, "name": "a" })], &key)
                .expect("a plan");
        let sql = build_create_table_sql(r#""t""#, &cols, None, &key);
        assert!(sql.ends_with(r#", PRIMARY KEY ("id"))"#), "{sql}");
    }

    #[test]
    fn create_table_sql_in_jsonb_mode_ignores_the_page_shape() {
        // The whole record lands in one column, so the table is the same
        // whatever arrives — inferring per-page columns here would create a
        // table the writer never uses.
        let cols = faucet_core::plan_columns(&[serde_json::json!({ "a": 1 })]).expect("plan");
        let sql = build_create_table_sql(r#""t""#, &cols, Some("data"), &[]);
        assert!(sql.contains(r#""data" jsonb NOT NULL"#), "{sql}");
        assert!(sql.contains(r#""id" bigserial PRIMARY KEY"#), "{sql}");
        assert!(
            !sql.contains(r#""a""#),
            "the page's own columns must not appear: {sql}"
        );
    }

    #[test]
    fn create_table_sql_quotes_a_hostile_column_name() {
        let cols =
            faucet_core::plan_columns(&[serde_json::json!({ "we\"ird": 1 })]).expect("a plan");
        let sql = build_create_table_sql(r#""t""#, &cols, None, &[]);
        assert!(
            sql.contains(r#""we""ird" bigint"#),
            "a column name must never reach the DDL unquoted: {sql}"
        );
    }

    #[test]
    fn create_table_defaults_on() {
        // A first-ever sync cannot assume the destination exists (#580).
        let cfg = PostgresSinkConfig::new("postgres://u@h/db", "t");
        assert!(cfg.create_table);
        assert!(!cfg.with_create_table(false).create_table);
    }
}

//! Pure planning for the Oracle sink: column metadata, bind typing, value
//! encoding and every SQL statement the sink issues. No driver calls.

use base64::Engine;
use faucet_common_oracle::oracle::sql_type::OracleType;
use faucet_common_oracle::{
    TypeFamily, bytes_to_hex, hex_to_bytes, iso_to_oracle_interval, quote_ident_oracle, split_table,
};
use faucet_common_sql::dialects::{Oracle, oracle};
use faucet_common_sql::{SqlDialect, TableTarget};
use faucet_core::idempotency::COMMIT_TOKEN_TABLE;
use faucet_core::{FaucetError, PlannedColumn, SqlBaseType};
use serde_json::{Value, json};

use crate::config::{OnUnknownField, OracleColumnMapping};

/// ORA-00942: table or view does not exist.
pub(crate) const ORA_TABLE_MISSING: i32 = 942;
/// ORA-00955: name is already used by an existing object.
pub(crate) const ORA_NAME_IN_USE: i32 = 955;
/// ORA-01451: column to be modified to NULL cannot be modified to NULL.
pub(crate) const ORA_ALREADY_NULL: i32 = oracle::ORA_ALREADY_NULL;
/// Width of the watermark table's `scope` key column.
pub(crate) const SCOPE_COL_WIDTH: usize = oracle::SCOPE_COL_WIDTH;

/// One destination column, from `ALL_TAB_COLS`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
    pub nullable: bool,
    /// `false` for virtual columns and `GENERATED ALWAYS` identities.
    pub insertable: bool,
    /// An identity column (`GENERATED … AS IDENTITY`).
    pub identity: bool,
    /// The column's `DEFAULT` expression, if any.
    pub default: Option<String>,
    /// The default is `DEFAULT ON NULL`: it also replaces an explicit `NULL`.
    pub default_on_null: bool,
}

/// How a value is bound for a destination column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindKind {
    Number,
    Double,
    Text,
    NationalText,
    Clob,
    Json,
    Raw,
    Blob,
    Date,
    Timestamp,
    TimestampTz,
    IntervalDs,
    IntervalYm,
    Boolean,
}

impl BindKind {
    pub fn for_column(col: &ColumnInfo) -> Self {
        match TypeFamily::from_data_type(&col.data_type, col.scale) {
            TypeFamily::Integer | TypeFamily::Decimal => BindKind::Number,
            TypeFamily::BinaryFloat => BindKind::Double,
            TypeFamily::NationalText => BindKind::NationalText,
            TypeFamily::Clob => BindKind::Clob,
            TypeFamily::Json => BindKind::Json,
            TypeFamily::Raw => BindKind::Raw,
            TypeFamily::Blob => BindKind::Blob,
            TypeFamily::Date => BindKind::Date,
            TypeFamily::Timestamp => BindKind::Timestamp,
            TypeFamily::TimestampTz => BindKind::TimestampTz,
            TypeFamily::IntervalDs => BindKind::IntervalDs,
            TypeFamily::IntervalYm => BindKind::IntervalYm,
            TypeFamily::Boolean => BindKind::Boolean,
            TypeFamily::Text | TypeFamily::Other => BindKind::Text,
        }
    }

    /// The driver bind type. Text binds start small; the driver grows them.
    /// Numbers bind as text and convert server-side (the session pins
    /// `NLS_NUMERIC_CHARACTERS`), which keeps 38-digit values exact.
    pub fn oracle_type(self) -> OracleType {
        match self {
            BindKind::Double => OracleType::BinaryDouble,
            BindKind::NationalText => OracleType::NVarchar2(1),
            BindKind::Clob | BindKind::Json => OracleType::CLOB,
            BindKind::Raw => OracleType::Raw(1),
            BindKind::Blob => OracleType::BLOB,
            BindKind::Date => OracleType::Date,
            BindKind::Timestamp => OracleType::Timestamp(9),
            BindKind::TimestampTz => OracleType::TimestampTZ(9),
            BindKind::Number
            | BindKind::Text
            | BindKind::IntervalDs
            | BindKind::IntervalYm
            | BindKind::Boolean => OracleType::Varchar2(1),
        }
    }
}

/// True for decimal text the driver accepts for a `NUMBER` bind.
pub(crate) fn is_numeric_text(s: &str) -> bool {
    let b = s.trim().as_bytes();
    let mut i = 0;
    if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
        i += 1;
    }
    let int_start = i;
    while i < b.len() && b[i].is_ascii_digit() {
        i += 1;
    }
    let mut digits = i - int_start;
    if i < b.len() && b[i] == b'.' {
        i += 1;
        let f = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        digits += i - f;
    }
    if digits == 0 {
        return false;
    }
    if i < b.len() && (b[i] == b'e' || b[i] == b'E') {
        i += 1;
        if i < b.len() && (b[i] == b'-' || b[i] == b'+') {
            i += 1;
        }
        let e = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == e {
            return false;
        }
    }
    i == b.len()
}

/// Truncate a date-time's fractional seconds to nanoseconds, the most a
/// `TIMESTAMP` holds.
pub(crate) fn clamp_fraction(s: &str) -> String {
    let Some(dot) = s.find('.') else {
        return s.to_string();
    };
    let digits = s[dot + 1..]
        .chars()
        .take_while(char::is_ascii_digit)
        .count();
    if digits <= 9 {
        return s.to_string();
    }
    format!("{}{}", &s[..dot + 10], &s[dot + 1 + digits..])
}

/// An ISO 8601 date-time carrying an offset as UTC wall time. A tz-less
/// `DATE`/`TIMESTAMP` bind keeps only the wall-clock fields, so without this
/// `…-08:00` and `…Z` values would land as inconsistent instants.
pub(crate) fn utc_wall_time(s: &str) -> Option<String> {
    const OFFSET_FORMATS: [&str; 4] = [
        "%Y-%m-%dT%H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f%#z",
        "%Y-%m-%d %H:%M:%S%.f %#z",
        "%Y-%m-%dT%H:%M%#z",
    ];
    let s = s.trim();
    let dt = chrono::DateTime::parse_from_rfc3339(s).ok().or_else(|| {
        OFFSET_FORMATS
            .iter()
            .find_map(|f| chrono::DateTime::parse_from_str(s, f).ok())
    })?;
    Some(dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.f").to_string())
}

/// Encode one record value as the text bound for a column of `kind`.
/// `Ok(None)` binds SQL `NULL` — also for `""`, which Oracle stores as `NULL`
/// anyway (and a zero-length `CLOB` bind is rejected); `Err` names why the
/// value cannot be stored.
pub(crate) fn value_to_text(v: &Value, kind: BindKind) -> Result<Option<String>, String> {
    if v.is_null() || v.as_str() == Some("") {
        return Ok(None);
    }
    let text = match (kind, v) {
        (BindKind::Number, Value::Bool(b)) => (if *b { "1" } else { "0" }).to_string(),
        (BindKind::Number, Value::Number(n)) => n.to_string(),
        (BindKind::Number, Value::String(s)) if is_numeric_text(s) => s.trim().to_string(),
        (BindKind::Number, other) => return Err(format!("{other} is not a number")),
        (BindKind::Double, Value::Number(n)) => n.to_string(),
        (BindKind::Double, Value::String(s)) if s.trim().parse::<f64>().is_ok() => {
            s.trim().to_string()
        }
        (BindKind::Double, other) => return Err(format!("{other} is not a number")),
        (BindKind::Raw | BindKind::Blob, Value::String(s)) => {
            let bytes = match s.strip_prefix("\\x") {
                Some(hex) => hex_to_bytes(hex),
                None => base64::engine::general_purpose::STANDARD.decode(s).ok(),
            };
            match bytes {
                Some(b) => bytes_to_hex(&b),
                None => return Err("binary value is neither base64 nor \\x-prefixed hex".into()),
            }
        }
        (BindKind::Raw | BindKind::Blob, other) => {
            return Err(format!("{other} is not a base64 binary value"));
        }
        (BindKind::Date | BindKind::Timestamp, Value::String(s)) => {
            let s = clamp_fraction(s);
            utc_wall_time(&s).unwrap_or(s)
        }
        (BindKind::TimestampTz, Value::String(s)) => clamp_fraction(s),
        (BindKind::Date | BindKind::Timestamp | BindKind::TimestampTz, other) => {
            return Err(format!("{other} is not a date-time string"));
        }
        (BindKind::IntervalDs, Value::String(s)) => {
            iso_to_oracle_interval(s, true).unwrap_or_else(|| s.clone())
        }
        (BindKind::IntervalYm, Value::String(s)) => {
            iso_to_oracle_interval(s, false).unwrap_or_else(|| s.clone())
        }
        (BindKind::Boolean, Value::Bool(b)) => b.to_string(),
        (BindKind::Json, Value::String(s)) if serde_json::from_str::<Value>(s).is_ok() => s.clone(),
        (BindKind::Json, other) => other.to_string(),
        (_, Value::String(s)) => s.clone(),
        (_, other) => other.to_string(),
    };
    Ok(Some(text))
}

/// Encode a record's values for `cols`, in column order.
pub(crate) fn encode_row(
    record: &Value,
    cols: &[String],
    kinds: &[BindKind],
) -> Result<Vec<Option<String>>, String> {
    cols.iter()
        .zip(kinds)
        .map(|(c, k)| {
            let v = record.get(c).unwrap_or(&Value::Null);
            value_to_text(v, *k).map_err(|e| format!("column {c:?}: {e}"))
        })
        .collect()
}

/// The column set to write for a batch: insertable columns present in any
/// record, in table order.
pub(crate) fn resolve_insert_columns(
    insertable: &[String],
    records: &[Value],
    on_unknown: OnUnknownField,
) -> Result<Vec<String>, FaucetError> {
    let set: std::collections::HashSet<&str> = insertable.iter().map(String::as_str).collect();
    let mut unknown: Vec<String> = Vec::new();
    for key in records
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|o| o.keys())
    {
        if !set.contains(key.as_str()) && !unknown.contains(key) {
            unknown.push(key.clone());
        }
    }
    if !unknown.is_empty() {
        match on_unknown {
            OnUnknownField::Error => {
                return Err(FaucetError::Sink(format!(
                    "oracle auto_columns: record keys {unknown:?} match no writable column"
                )));
            }
            OnUnknownField::Warn => {
                tracing::warn!(
                    ?unknown,
                    "oracle auto_columns: dropping keys with no column"
                );
            }
            OnUnknownField::Drop => {}
        }
    }
    Ok(insertable
        .iter()
        .filter(|c| {
            records
                .iter()
                .any(|r| r.as_object().is_some_and(|o| o.contains_key(c.as_str())))
        })
        .cloned()
        .collect())
}

/// The engine target of an already-quoted table reference.
fn target(table: &str) -> TableTarget {
    TableTarget::new(table, table)
}

/// `INSERT INTO t ("A", "B") VALUES (:1, :2)` — executed as array DML.
pub(crate) fn insert_sql(table: &str, cols: &[String]) -> Result<String, FaucetError> {
    Oracle.insert_sql(table, cols, &[], 1)
}

/// Keyed `MERGE` upsert over one bound row (executed as array DML).
pub(crate) fn merge_sql(
    table: &str,
    key: &[String],
    cols: &[String],
) -> Result<String, FaucetError> {
    Oracle.upsert_sql(table, key, cols, &[], 1)
}

/// `DELETE FROM t WHERE "K1" = :1 AND "K2" = :2` (executed as array DML).
pub(crate) fn delete_sql(table: &str, key: &[String]) -> Result<String, FaucetError> {
    Oracle.delete_keys_sql(table, key, &[], 1)
}

/// Run `ddl` via `EXECUTE IMMEDIATE`, swallowing the listed `ORA-` codes so
/// idempotent DDL (create-if-missing, drop-if-present) needs no pre-check.
pub(crate) fn ignoring(ddl: &str, codes: &[i32]) -> String {
    oracle::ignoring(ddl, codes)
}

/// Character width of each text key column when `n` of them share a primary
/// key (see [`oracle::key_text_width`]).
#[cfg(test)]
pub(crate) fn key_text_width(n: usize) -> usize {
    oracle::key_text_width(n)
}

/// `CREATE TABLE` for `auto_columns`, from the first page's planned columns.
pub(crate) fn create_table_sql(
    table: &str,
    columns: &[PlannedColumn],
    key: &[String],
) -> Result<String, FaucetError> {
    Oracle.create_table_sql(&target(table), columns, key)
}

/// `CREATE TABLE` for `json_column` mode.
pub(crate) fn create_json_table_sql(table: &str, column: &str) -> Result<String, FaucetError> {
    Oracle.create_json_table_sql(&target(table), column)
}

/// Owner-qualify a helper object so it lives beside the target table.
pub(crate) fn sibling(table: &str, name: &str) -> Result<String, FaucetError> {
    let (owner, _) = split_table(table)?;
    let q = quote_ident_oracle(name)?;
    Ok(match owner {
        Some(o) => format!("{}.{q}", quote_ident_oracle(&o)?),
        None => q,
    })
}

/// The watermark table beside `table`.
pub(crate) fn token_table(table: &str) -> Result<String, FaucetError> {
    sibling(table, COMMIT_TOKEN_TABLE)
}

/// `CREATE TABLE` for the exactly-once watermark (wrap with [`ignoring`]).
pub(crate) fn token_table_ddl(token_table: &str) -> String {
    oracle::token_table_ddl(token_table)
}

/// Upsert of a scope's token (`:1` scope, `:2` token).
pub(crate) fn token_merge_sql(token_table: &str) -> String {
    oracle::token_merge_sql(token_table)
}

/// Remove a scope's token row (`:1` scope).
pub(crate) fn token_delete_sql(token_table: &str) -> String {
    oracle::token_delete_sql(token_table)
}

/// Read a scope's token (`:1` scope).
pub(crate) fn token_select_sql(token_table: &str) -> String {
    oracle::token_select_sql(token_table)
}

/// Bind values `(owner, table)` for the dictionary lookups below.
pub(crate) fn dictionary_binds(table: &str) -> Result<(Option<String>, String), FaucetError> {
    split_table(table)
}

/// Existence probe (`:1` owner or NULL for the current schema, `:2` table).
pub(crate) const TABLE_EXISTS_SQL: &str = "SELECT COUNT(*) FROM ALL_TABLES \
    WHERE OWNER = NVL(:1, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND TABLE_NAME = :2";

/// Tables other than the target whose enabled foreign keys reference it
/// (`:1` owner or NULL for the current schema, `:2` table), as `OWNER.TABLE`.
/// Self-references are excluded: the swap replaces every row of the table at
/// once, so they cannot cascade elsewhere.
pub(crate) const REFERENCING_TABLES_SQL: &str = "SELECT DISTINCT c.OWNER || '.' || c.TABLE_NAME \
    FROM ALL_CONSTRAINTS c JOIN ALL_CONSTRAINTS p \
      ON p.OWNER = c.R_OWNER AND p.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME \
    WHERE c.CONSTRAINT_TYPE = 'R' AND c.STATUS = 'ENABLED' \
      AND p.OWNER = NVL(:1, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND p.TABLE_NAME = :2 \
      AND NOT (c.OWNER = p.OWNER AND c.TABLE_NAME = p.TABLE_NAME) \
    ORDER BY 1";

/// The refusal for an overwrite whose target other tables reference: the
/// swap's `DELETE FROM target` would cascade into them (`ON DELETE CASCADE` /
/// `SET NULL`) or fail after the whole staging load (`NO ACTION`).
pub(crate) fn referenced_overwrite_error(table: &str, referenced_by: &[String]) -> FaucetError {
    FaucetError::Config(format!(
        "oracle overwrite: table '{table}' is referenced by a foreign key from {}; replacing \
         its rows would cascade into, break or be blocked by those tables. Use write_mode: \
         upsert, or drop or disable the foreign key",
        referenced_by.join(", ")
    ))
}

/// Column metadata (`:1` owner or NULL, `:2` table).
pub(crate) const COLUMNS_SQL: &str = "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.DATA_PRECISION, \
    c.DATA_SCALE, c.NULLABLE, c.VIRTUAL_COLUMN, NVL(i.GENERATION_TYPE, 'NONE'), c.DATA_DEFAULT, \
    c.DEFAULT_ON_NULL \
    FROM ALL_TAB_COLS c LEFT JOIN ALL_TAB_IDENTITY_COLS i \
      ON i.OWNER = c.OWNER AND i.TABLE_NAME = c.TABLE_NAME AND i.COLUMN_NAME = c.COLUMN_NAME \
    WHERE c.OWNER = NVL(:1, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND c.TABLE_NAME = :2 \
      AND c.HIDDEN_COLUMN = 'NO' ORDER BY c.COLUMN_ID";

/// One [`COLUMNS_SQL`] row: name, type, precision, scale, nullable,
/// virtual, identity generation, default, default-on-null.
pub(crate) type ColumnRow = (
    String,
    String,
    Option<i64>,
    Option<i64>,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
);

/// Build a [`ColumnInfo`] from one [`COLUMNS_SQL`] row.
pub(crate) fn column_from_row(row: ColumnRow) -> ColumnInfo {
    let (
        name,
        data_type,
        precision,
        scale,
        nullable,
        virtual_column,
        generation,
        data_default,
        default_on_null,
    ) = row;
    let identity = generation != "NONE";
    ColumnInfo {
        name,
        data_type,
        precision,
        scale,
        nullable: nullable != "N",
        insertable: virtual_column != "YES" && generation != "ALWAYS",
        identity,
        // An identity's sequence default is not a copyable expression.
        default: data_default
            .map(|d| d.trim().to_string())
            .filter(|d| !d.is_empty() && !identity && virtual_column != "YES"),
        default_on_null: default_on_null.as_deref() == Some("YES"),
    }
}

/// The destination schema as an `infer_schema`-shaped object for drift.
pub(crate) fn schema_from_columns(cols: &[ColumnInfo]) -> Value {
    let mut props = serde_json::Map::new();
    for c in cols {
        let family = TypeFamily::from_data_type(&c.data_type, c.scale);
        let mut types: Vec<&str> = match family {
            TypeFamily::Integer if c.precision == Some(1) => vec!["boolean", "integer"],
            TypeFamily::Integer => vec!["integer"],
            TypeFamily::Decimal | TypeFamily::BinaryFloat => vec!["number"],
            TypeFamily::Boolean => vec!["boolean"],
            _ => vec!["string"],
        };
        if c.nullable {
            types.push("null");
        }
        let ty = if types.len() == 1 {
            json!(types[0])
        } else {
            json!(types)
        };
        let mut fragment = json!({ "type": ty });
        // Objects and arrays land in text, CLOB and JSON columns as JSON text,
        // so they are not drift there (#862 M-049).
        if matches!(
            family,
            TypeFamily::Text | TypeFamily::NationalText | TypeFamily::Clob | TypeFamily::Json
        ) {
            fragment[faucet_core::DRIFT_ALSO_ACCEPTS] = json!(["object", "array"]);
        }
        props.insert(c.name.clone(), fragment);
    }
    json!({ "type": "object", "properties": props })
}

/// `ALTER TABLE … ADD`, idempotent.
pub(crate) fn add_column_sql(
    table: &str,
    col: &str,
    t: SqlBaseType,
) -> Result<String, FaucetError> {
    Oracle.add_column_sql(&target(table), col, t)
}

/// Widen a column to `t`. Oracle can only widen a numeric column in place.
pub(crate) fn widen_column_sql(
    table: &str,
    col: &str,
    t: SqlBaseType,
) -> Result<String, FaucetError> {
    match t {
        SqlBaseType::Double => Oracle
            .alter_column_type_sql(&target(table), col, Oracle.widened_type_name(None, t))?
            .ok_or_else(|| FaucetError::Sink(format!("oracle cannot widen column {col:?}"))),
        other => Err(FaucetError::Sink(format!(
            "oracle cannot widen column {col:?} to {other:?} in place"
        ))),
    }
}

/// Relax `NOT NULL`, idempotent.
pub(crate) fn relax_null_sql(table: &str, col: &str) -> Result<String, FaucetError> {
    Oracle
        .drop_not_null_sql(&target(table), col)?
        .ok_or_else(|| FaucetError::Sink(format!("oracle cannot relax column {col:?}")))
}

/// Overwrite staging: a structural clone of the target.
pub(crate) fn clone_table_sql(staging: &str, target: &str) -> String {
    format!("CREATE TABLE {staging} AS SELECT * FROM {target} WHERE 1 = 0")
}

/// What `CREATE TABLE … AS SELECT` drops from the clone, put back: every
/// identity column — `BY DEFAULT` and `GENERATED ALWAYS` alike (#862 M-048) —
/// and every other column the writer cannot supply becomes nullable (the
/// clone kept its `NOT NULL`; the swap leaves those columns to the target),
/// and each column `DEFAULT` (`DEFAULT ON NULL` included) is copied, so a
/// column no record of a page carries gets the target's default, not `NULL`.
pub(crate) fn staging_fixups_sql(
    staging: &str,
    target_cols: &[ColumnInfo],
) -> Result<Vec<String>, FaucetError> {
    let mut out = Vec::new();
    for c in target_cols {
        let col = quote_ident_oracle(&c.name)?;
        if c.identity || !c.insertable {
            out.push(ignoring(
                &format!("ALTER TABLE {staging} MODIFY ({col} NULL)"),
                &[ORA_ALREADY_NULL],
            ));
        } else if let Some(d) = &c.default {
            let on_null = if c.default_on_null { " ON NULL" } else { "" };
            out.push(format!(
                "ALTER TABLE {staging} MODIFY ({col} DEFAULT{on_null} {d})"
            ));
        }
    }
    Ok(out)
}

/// Drop a table if it exists.
pub(crate) fn drop_table_sql(table: &str) -> String {
    ignoring(&format!("DROP TABLE {table} PURGE"), &[ORA_TABLE_MISSING])
}

/// Publish a first-run staging table under the target's (bare) name.
pub(crate) fn rename_sql(staging: &str, target_table: &str) -> Result<String, FaucetError> {
    let (_, bare) = split_table(target_table)?;
    Ok(format!(
        "ALTER TABLE {staging} RENAME TO {}",
        quote_ident_oracle(&bare)?
    ))
}

/// The two DML statements of the transactional overwrite swap.
pub(crate) fn swap_sql(
    target: &str,
    staging: &str,
    cols: &[String],
) -> Result<[String; 2], FaucetError> {
    let list = Oracle.column_list(cols)?;
    Ok([
        format!("DELETE FROM {target}"),
        format!("INSERT INTO {target} ({list}) SELECT {list} FROM {staging}"),
    ])
}

/// The overwrite swap for the target's writable columns: delete, then copy
/// staging back. A writable identity column is copied only where staging has
/// a value; rows without one are inserted without it so the target generates
/// it (a table has at most one identity column).
pub(crate) fn overwrite_swap_sql(
    target: &str,
    staging: &str,
    target_cols: &[ColumnInfo],
) -> Result<Vec<String>, FaucetError> {
    let cols: Vec<String> = target_cols
        .iter()
        .filter(|c| c.insertable)
        .map(|c| c.name.clone())
        .collect();
    let [delete, insert] = swap_sql(target, staging, &cols)?;
    let Some(id) = target_cols.iter().find(|c| c.insertable && c.identity) else {
        return Ok(vec![delete, insert]);
    };
    let id_q = quote_ident_oracle(&id.name)?;
    let rest: Vec<String> = cols.into_iter().filter(|c| c != &id.name).collect();
    let mut out = vec![delete, format!("{insert} WHERE {id_q} IS NOT NULL")];
    if !rest.is_empty() {
        let [_, without] = swap_sql(target, staging, &rest)?;
        out.push(format!("{without} WHERE {id_q} IS NULL"));
    }
    Ok(out)
}

/// Apply the identifier case rule to every record's top-level keys.
pub(crate) fn case_records(records: &[Value], upper: bool) -> std::borrow::Cow<'_, [Value]> {
    if !upper {
        return std::borrow::Cow::Borrowed(records);
    }
    std::borrow::Cow::Owned(
        records
            .iter()
            .map(|r| match r {
                Value::Object(o) => Value::Object(
                    o.iter()
                        .map(|(k, v)| (k.to_uppercase(), v.clone()))
                        .collect(),
                ),
                other => other.clone(),
            })
            .collect(),
    )
}

/// The column a record key addresses: the exact name, else — for a key that
/// matches nothing exactly — the column its upper-case form names, when that
/// column is all upper case. Unquoted Oracle DDL (`CREATE TABLE T (ID …)`)
/// stores names upper-cased, and lower-case keys from most sources would
/// otherwise match nothing under `identifier_case: preserve` (SQL-17).
pub(crate) fn column_for<'a>(key: &str, columns: &'a [String]) -> Option<&'a str> {
    faucet_common_sql::mapping::column_for_field(key, columns, Oracle.column_matching())
}

/// Rename every record key that addresses a column only through
/// [`column_for`]'s upper-case fallback. A record already carrying the column's
/// exact name keeps its own value for it.
pub(crate) fn fold_to_columns<'a>(
    records: &'a [Value],
    columns: &[String],
) -> std::borrow::Cow<'a, [Value]> {
    let needs = |o: &serde_json::Map<String, Value>| {
        o.keys()
            .any(|k| column_for(k, columns).is_some_and(|c| c != k && !o.contains_key(c)))
    };
    if !records.iter().any(|r| r.as_object().is_some_and(needs)) {
        return std::borrow::Cow::Borrowed(records);
    }
    std::borrow::Cow::Owned(
        records
            .iter()
            .map(|r| match r {
                Value::Object(o) => Value::Object(
                    o.iter()
                        .map(|(k, v)| match column_for(k, columns) {
                            Some(c) if c != k && !o.contains_key(c) => (c.to_string(), v.clone()),
                            _ => (k.clone(), v.clone()),
                        })
                        .collect(),
                ),
                other => other.clone(),
            })
            .collect(),
    )
}

/// Upsert records grouped by which of `columns` each carries (a key present
/// with `null` counts), in first-seen order. Each group is merged on its own
/// columns, so a column a record omits keeps its stored value instead of being
/// overwritten with NULL (#789 SQL-10).
pub(crate) fn group_by_present_columns(records: &[Value], columns: &[String]) -> Vec<Vec<Value>> {
    let mut groups: Vec<(Vec<bool>, Vec<Value>)> = Vec::new();
    for record in records {
        let present: Vec<bool> = columns
            .iter()
            .map(|c| record.as_object().is_some_and(|o| o.contains_key(c)))
            .collect();
        match groups.iter_mut().find(|(p, _)| *p == present) {
            Some((_, rows)) => rows.push(record.clone()),
            None => groups.push((present, vec![record.clone()])),
        }
    }
    groups.into_iter().map(|(_, rows)| rows).collect()
}

/// A prepared chunk: the columns written, their bind kinds, and each row's
/// encoded values (or its encoding failure), by chunk index.
pub(crate) type PreparedRows = (
    Vec<String>,
    Vec<BindKind>,
    Vec<(usize, Result<Vec<Option<String>>, String>)>,
);

/// Insertable column names, in table order.
pub(crate) fn insertable_names(info: &[ColumnInfo]) -> Vec<String> {
    info.iter()
        .filter(|c| c.insertable)
        .map(|c| c.name.clone())
        .collect()
}

/// Whether `on_unknown_field: drop` lets a record with no matching key be
/// skipped rather than refused.
pub(crate) fn drops_unknown(mapping: &OracleColumnMapping) -> bool {
    matches!(
        mapping,
        OracleColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Drop
        }
    )
}

/// The columns, bind kinds and encoded rows for one chunk. `info` is only read
/// in `auto_columns` mode, where record keys are first folded onto the
/// table's columns (SQL-17).
pub(crate) fn prepare_rows(
    mapping: &OracleColumnMapping,
    info: &[ColumnInfo],
    chunk: &[Value],
) -> Result<PreparedRows, FaucetError> {
    match mapping {
        OracleColumnMapping::JsonColumn { column } => Ok((
            vec![column.clone()],
            vec![BindKind::Clob],
            chunk
                .iter()
                .enumerate()
                .map(|(i, r)| (i, Ok(vec![Some(r.to_string())])))
                .collect(),
        )),
        OracleColumnMapping::AutoColumns { on_unknown_field } => {
            let names = insertable_names(info);
            let chunk = fold_to_columns(chunk, &names);
            let cols = resolve_insert_columns(&names, &chunk, *on_unknown_field)?;
            let kinds: Vec<BindKind> = cols
                .iter()
                .map(|c| {
                    info.iter()
                        .find(|i| &i.name == c)
                        .map(BindKind::for_column)
                        .unwrap_or(BindKind::Text)
                })
                .collect();
            let rows = chunk
                .iter()
                .enumerate()
                .map(|(i, r)| (i, encode_row(r, &cols, &kinds)))
                .collect();
            Ok((cols, kinds, rows))
        }
    }
}

/// The page with record keys (and the write `key`) mapped onto the table's
/// `names` through the unquoted-identifier fallback (SQL-17). JSON-column mode
/// and an empty page pass through.
pub(crate) fn fold_page(
    mapping: &OracleColumnMapping,
    names: &[String],
    records: &[Value],
    spec: &faucet_core::WriteSpec,
) -> (Vec<Value>, faucet_core::WriteSpec) {
    let mut spec = spec.clone();
    if !matches!(mapping, OracleColumnMapping::AutoColumns { .. }) || records.is_empty() {
        return (records.to_vec(), spec);
    }
    spec.key = fold_names(&spec.key, names);
    (fold_to_columns(records, names).into_owned(), spec)
}

/// The upsert groups of a plan: each chunk's records folded onto the table's
/// columns, then grouped by the columns they carry (SQL-10).
pub(crate) fn upsert_groups(chunks: &[&[Value]], insertable: &[String]) -> Vec<Vec<Value>> {
    chunks
        .iter()
        .flat_map(|chunk| group_by_present_columns(&fold_to_columns(chunk, insertable), insertable))
        .collect()
}

/// `key` mapped through [`column_for`]; a name that matches no column is kept.
pub(crate) fn fold_names(names: &[String], columns: &[String]) -> Vec<String> {
    names
        .iter()
        .map(|n| column_for(n, columns).unwrap_or(n).to_string())
        .collect()
}

/// The table to write when `table` itself does not exist: its upper-case form,
/// when that exists — a table created with unquoted DDL (SQL-17). Otherwise
/// `table` (created as written).
pub(crate) fn pick_table(table: &str, exact_exists: bool, upper_exists: bool) -> String {
    let upper = table.to_uppercase();
    if !exact_exists && upper != table && upper_exists {
        upper
    } else {
        table.to_string()
    }
}

/// The error for a non-empty chunk whose keys match no column of `table`.
pub(crate) fn no_columns_error(table: &str, records: &[Value]) -> FaucetError {
    let mut keys: Vec<&str> = records
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|o| o.keys().map(String::as_str))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys.truncate(10);
    FaucetError::Sink(format!(
        "oracle: none of the record keys {keys:?} match a column of {table} — nothing would be \
         written. Check `table` and the column names (Oracle names are case-sensitive once \
         quoted; `identifier_case: upper` matches tables created with unquoted DDL), or set \
         `on_unknown_field: drop` to skip such records"
    ))
}

#[cfg(test)]
mod tests {
    #[test]
    fn referenced_overwrite_is_refused_naming_the_children() {
        let e = referenced_overwrite_error("ORDERS_PARENT", &["APP.ORDERS".into(), "APP.X".into()]);
        let msg = e.to_string();
        assert!(matches!(e, FaucetError::Config(_)));
        assert!(
            msg.contains("'ORDERS_PARENT'") && msg.contains("APP.ORDERS, APP.X"),
            "{msg}"
        );
        assert!(REFERENCING_TABLES_SQL.contains("CONSTRAINT_TYPE = 'R'"));
        assert!(REFERENCING_TABLES_SQL.contains("STATUS = 'ENABLED'"));
    }

    use super::*;

    #[test]
    fn lower_case_keys_reach_unquoted_upper_case_columns() {
        let cols = vec!["ID".to_string(), "NAME".to_string(), "mixed".to_string()];
        assert_eq!(column_for("ID", &cols), Some("ID"));
        assert_eq!(column_for("id", &cols), Some("ID"));
        assert_eq!(column_for("Name", &cols), Some("NAME"));
        assert_eq!(column_for("mixed", &cols), Some("mixed"));
        assert_eq!(column_for("MIXED", &cols), None);
        assert_eq!(column_for("other", &cols), None);

        let recs = vec![json!({"id": 1, "name": "a"}), json!({"ID": 2, "id": 9})];
        let folded = fold_to_columns(&recs, &cols);
        assert_eq!(folded[0], json!({"ID": 1, "NAME": "a"}));
        assert_eq!(folded[1], json!({"ID": 2, "id": 9}));
        let exact = vec![json!({"ID": 1})];
        assert!(matches!(
            fold_to_columns(&exact, &cols),
            std::borrow::Cow::Borrowed(_)
        ));
        assert_eq!(
            fold_names(&["id".to_string(), "zzz".to_string()], &cols),
            vec!["ID".to_string(), "zzz".to_string()]
        );
    }

    #[test]
    fn an_unquoted_table_is_found_by_its_upper_case_name() {
        assert_eq!(pick_table("events", false, true), "EVENTS");
        assert_eq!(pick_table("app.events", false, true), "APP.EVENTS");
        assert_eq!(pick_table("events", true, true), "events");
        assert_eq!(pick_table("events", false, false), "events");
        assert_eq!(pick_table("EVENTS", false, true), "EVENTS");
    }

    #[test]
    fn the_zero_column_error_names_the_keys_and_the_fix() {
        let err =
            no_columns_error("\"T\"", &[json!({"b": 1, "a": 2}), json!({"a": 3})]).to_string();
        assert!(err.contains("[\"a\", \"b\"]"), "{err}");
        assert!(err.contains("identifier_case: upper"), "{err}");
    }

    fn col(name: &str, ty: &str, precision: Option<i64>, scale: Option<i64>) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: ty.into(),
            precision,
            scale,
            nullable: true,
            insertable: true,
            identity: false,
            default: None,
            default_on_null: false,
        }
    }

    #[test]
    fn bind_kinds_follow_the_dictionary_type() {
        use BindKind::*;
        let cases = [
            ("NUMBER", Some(0), Number),
            ("FLOAT", None, Number),
            ("BINARY_DOUBLE", None, Double),
            ("VARCHAR2", None, Text),
            ("NVARCHAR2", None, NationalText),
            ("CLOB", None, Clob),
            ("JSON", None, Json),
            ("RAW", None, Raw),
            ("BLOB", None, Blob),
            ("DATE", None, Date),
            ("TIMESTAMP(6)", None, Timestamp),
            ("TIMESTAMP(6) WITH TIME ZONE", None, TimestampTz),
            ("INTERVAL DAY(2) TO SECOND(6)", None, IntervalDs),
            ("INTERVAL YEAR(2) TO MONTH", None, IntervalYm),
            ("BOOLEAN", None, Boolean),
            ("SDO_GEOMETRY", None, Text),
        ];
        for (ty, scale, want) in cases {
            assert_eq!(
                BindKind::for_column(&col("C", ty, None, scale)),
                want,
                "{ty}"
            );
        }
        for k in [
            Number,
            Double,
            Text,
            NationalText,
            Clob,
            Json,
            Raw,
            Blob,
            Date,
            Timestamp,
            TimestampTz,
            IntervalDs,
            IntervalYm,
            Boolean,
        ] {
            let _ = k.oracle_type();
        }
        assert_eq!(Clob.oracle_type(), OracleType::CLOB);
    }

    #[test]
    fn fraction_clamping() {
        assert_eq!(clamp_fraction("2024-01-02T03:04:05"), "2024-01-02T03:04:05");
        assert_eq!(
            clamp_fraction("2024-01-02T03:04:05.5Z"),
            "2024-01-02T03:04:05.5Z"
        );
        assert_eq!(
            clamp_fraction("2024-01-02T03:04:05.123456789012+01:00"),
            "2024-01-02T03:04:05.123456789+01:00"
        );
    }

    #[test]
    fn other_iso_offset_spellings_also_land_in_utc() {
        for (input, want) in [
            ("2024-01-02T03:04:05-0800", "2024-01-02T11:04:05"),
            ("2024-01-02T03:04:05-08", "2024-01-02T11:04:05"),
            ("2024-01-02 03:04:05.5+0100", "2024-01-02T02:04:05.500"),
            ("2024-01-02 03:04:05 +01:00", "2024-01-02T02:04:05"),
            ("2024-01-02T03:04+02:00", "2024-01-02T01:04:00"),
        ] {
            assert_eq!(utc_wall_time(input).as_deref(), Some(want), "{input}");
        }
        assert_eq!(utc_wall_time("2024-01-02T03:04:05"), None);
        assert_eq!(utc_wall_time("not a date"), None);
    }

    #[test]
    fn offset_timestamps_land_in_utc_for_tzless_columns() {
        let v = serde_json::json!("2024-01-02T03:04:05-08:00");
        assert_eq!(
            value_to_text(&v, BindKind::Timestamp).unwrap().as_deref(),
            Some("2024-01-02T11:04:05")
        );
        assert_eq!(
            value_to_text(&v, BindKind::Date).unwrap().as_deref(),
            Some("2024-01-02T11:04:05")
        );
        assert_eq!(
            value_to_text(&v, BindKind::TimestampTz).unwrap().as_deref(),
            Some("2024-01-02T03:04:05-08:00")
        );
        let naive = serde_json::json!("2024-01-02 03:04:05.123456789012");
        assert_eq!(
            value_to_text(&naive, BindKind::Timestamp)
                .unwrap()
                .as_deref(),
            Some("2024-01-02 03:04:05.123456789")
        );
    }

    #[test]
    fn numeric_text() {
        for ok in ["1", "-1", "+2.5", ".5", "1.", "1e10", "1E-3", " 7 "] {
            assert!(is_numeric_text(ok), "{ok}");
        }
        for bad in ["", "-", ".", "e5", "1e", "1e+", "abc", "1.2.3", "1x"] {
            assert!(!is_numeric_text(bad), "{bad}");
        }
    }

    #[test]
    fn value_encoding() {
        use BindKind::*;
        let t = |v: Value, k| value_to_text(&v, k);
        assert_eq!(t(Value::Null, Text), Ok(None));
        assert_eq!(t(json!(""), Clob), Ok(None));
        assert_eq!(t(json!(true), Number), Ok(Some("1".into())));
        assert_eq!(t(json!(false), Number), Ok(Some("0".into())));
        assert_eq!(t(json!(12), Number), Ok(Some("12".into())));
        assert_eq!(
            t(json!("123456789012345678901234567890"), Number),
            Ok(Some("123456789012345678901234567890".into()))
        );
        assert!(t(json!("abc"), Number).is_err());
        assert!(t(json!({"a": 1}), Number).is_err());
        assert_eq!(t(json!(1.5), Double), Ok(Some("1.5".into())));
        assert_eq!(t(json!("2.5"), Double), Ok(Some("2.5".into())));
        assert!(t(json!("x"), Double).is_err());
        assert_eq!(t(json!("AQI="), Raw), Ok(Some("0102".into())));
        assert_eq!(t(json!("\\x0aff"), Blob), Ok(Some("0AFF".into())));
        assert!(t(json!("!!"), Raw).is_err());
        assert!(t(json!(5), Blob).is_err());
        assert_eq!(
            t(json!("2024-01-02T03:04:05Z"), TimestampTz),
            Ok(Some("2024-01-02T03:04:05Z".into()))
        );
        assert!(t(json!(1700000000), Date).is_err());
        assert_eq!(
            t(json!("P1DT2H"), IntervalDs),
            Ok(Some("+1 02:00:00.000000000".into()))
        );
        assert_eq!(
            t(json!("+1 02:00:00"), IntervalDs),
            Ok(Some("+1 02:00:00".into()))
        );
        assert_eq!(t(json!("P1Y"), IntervalYm), Ok(Some("+1-0".into())));
        assert_eq!(t(json!("+1-0"), IntervalYm), Ok(Some("+1-0".into())));
        assert_eq!(t(json!(true), Boolean), Ok(Some("true".into())));
        assert_eq!(t(json!("{\"a\":1}"), Json), Ok(Some("{\"a\":1}".into())));
        assert_eq!(t(json!("plain"), Json), Ok(Some("\"plain\"".into())));
        assert_eq!(t(json!({"a": [1]}), Json), Ok(Some("{\"a\":[1]}".into())));
        assert_eq!(t(json!("s"), Text), Ok(Some("s".into())));
        assert_eq!(t(json!(3), Clob), Ok(Some("3".into())));
        assert_eq!(t(json!({"k": 1}), Text), Ok(Some("{\"k\":1}".into())));
    }

    #[test]
    fn encode_row_names_the_failing_column() {
        let cols = vec!["A".to_string(), "B".to_string()];
        let kinds = [BindKind::Number, BindKind::Text];
        assert_eq!(
            encode_row(&json!({"A": 1}), &cols, &kinds),
            Ok(vec![Some("1".into()), None])
        );
        let err = encode_row(&json!({"A": "x"}), &cols, &kinds).unwrap_err();
        assert!(err.contains("\"A\""), "{err}");
    }

    #[test]
    fn resolve_columns_unions_and_polices_unknowns() {
        let insertable = vec!["A".to_string(), "B".to_string(), "C".to_string()];
        let recs = vec![json!({"A": 1}), json!({"C": 2, "Z": 3})];
        let cols = resolve_insert_columns(&insertable, &recs, OnUnknownField::Drop).unwrap();
        assert_eq!(cols, vec!["A".to_string(), "C".to_string()]);
        assert!(resolve_insert_columns(&insertable, &recs, OnUnknownField::Warn).is_ok());
        assert!(resolve_insert_columns(&insertable, &recs, OnUnknownField::Error).is_err());
    }

    #[test]
    fn dml_statements() {
        let cols = vec!["ID".to_string(), "V".to_string()];
        assert_eq!(
            insert_sql("\"T\"", &cols).unwrap(),
            "INSERT INTO \"T\" (\"ID\", \"V\") VALUES (:1, :2)"
        );
        assert_eq!(
            merge_sql("\"T\"", &["ID".into()], &cols).unwrap(),
            "MERGE INTO \"T\" t USING (SELECT :1 AS \"ID\", :2 AS \"V\" FROM DUAL) s ON \
             (t.\"ID\" = s.\"ID\") WHEN MATCHED THEN UPDATE SET t.\"V\" = s.\"V\" WHEN NOT \
             MATCHED THEN INSERT (\"ID\", \"V\") VALUES (s.\"ID\", s.\"V\")"
        );
        let key_only = merge_sql("\"T\"", &["ID".into()], &["ID".to_string()]).unwrap();
        assert!(!key_only.contains("WHEN MATCHED"), "{key_only}");
        assert!(merge_sql("\"T\"", &["K".into()], &cols).is_err());
        assert_eq!(
            delete_sql("\"T\"", &["A".into(), "B".into()]).unwrap(),
            "DELETE FROM \"T\" WHERE \"A\" = :1 AND \"B\" = :2"
        );
        assert!(insert_sql("\"T\"", &["a\"".into()]).is_err());
    }

    #[test]
    fn ddl_statements() {
        assert_eq!(
            ignoring("DROP TABLE \"it's\"", &[942, 955]),
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE \"it''s\"'; EXCEPTION WHEN OTHERS THEN IF \
             SQLCODE NOT IN (-942, -955) THEN RAISE; END IF; END;"
        );
        let planned = faucet_core::plan_keyed_columns(
            &[json!({"SKU": "a", "QTY": 1, "OK": true, "P": 1.5, "DOC": {"a": 1}})],
            &["SKU".to_string()],
        )
        .unwrap();
        let sql = create_table_sql("\"T\"", &planned, &["SKU".to_string()]).unwrap();
        assert!(sql.contains("\"SKU\" VARCHAR2(1000 CHAR)"), "{sql}");
        assert!(sql.contains("\"QTY\" NUMBER,"), "{sql}");
        assert!(sql.contains("\"OK\" NUMBER(1)"), "{sql}");
        assert!(sql.contains("\"P\" BINARY_DOUBLE"), "{sql}");
        assert!(sql.contains("\"DOC\" CLOB"), "{sql}");
        assert!(sql.ends_with("PRIMARY KEY (\"SKU\"))"), "{sql}");
        let int_keyed = create_table_sql("\"T\"", &planned, &["QTY".to_string()]).unwrap();
        assert!(int_keyed.contains("\"QTY\" NUMBER(38)"), "{int_keyed}");
        let unkeyed = create_table_sql("\"T\"", &planned, &[]).unwrap();
        assert!(!unkeyed.contains("PRIMARY KEY"));
        assert_eq!(
            create_json_table_sql("\"T\"", "data").unwrap(),
            "CREATE TABLE \"T\" (\"ID\" NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, \
             \"data\" CLOB)"
        );
        assert!(
            create_json_table_sql("\"T\"", "ID")
                .unwrap()
                .contains("\"FAUCET_ID\"")
        );
        assert_eq!(
            clone_table_sql("\"S\"", "\"T\""),
            "CREATE TABLE \"S\" AS SELECT * FROM \"T\" WHERE 1 = 0"
        );
        assert!(drop_table_sql("\"S\"").contains("DROP TABLE \"S\" PURGE"));
        assert_eq!(
            rename_sql("\"APP\".\"T__faucet_ovw\"", "APP.T").unwrap(),
            "ALTER TABLE \"APP\".\"T__faucet_ovw\" RENAME TO \"T\""
        );
        let [del, ins] = swap_sql("\"T\"", "\"S\"", &["A".into()]).unwrap();
        assert_eq!(del, "DELETE FROM \"T\"");
        assert_eq!(ins, "INSERT INTO \"T\" (\"A\") SELECT \"A\" FROM \"S\"");
    }

    #[test]
    fn overwrite_keeps_identity_and_defaults() {
        let mut id = col("ID", "NUMBER", None, Some(0));
        id.identity = true;
        let mut gen_always = col("G", "NUMBER", None, Some(0));
        gen_always.identity = true;
        gen_always.insertable = false;
        let mut def = col("STATUS", "VARCHAR2", None, None);
        def.default = Some("'new'".into());
        let doc = col("DOC", "CLOB", None, None);
        let cols = vec![id.clone(), gen_always, def, doc];
        let fix = staging_fixups_sql("\"S\"", &cols).unwrap();
        assert_eq!(fix.len(), 3);
        assert!(
            fix[0].contains("ALTER TABLE \"S\" MODIFY (\"ID\" NULL)"),
            "{}",
            fix[0]
        );
        assert!(
            fix[1].contains("ALTER TABLE \"S\" MODIFY (\"G\" NULL)"),
            "a GENERATED ALWAYS identity is relaxed too: {}",
            fix[1]
        );
        assert_eq!(
            fix[2],
            "ALTER TABLE \"S\" MODIFY (\"STATUS\" DEFAULT 'new')"
        );
        let mut on_null = col("FLAG", "CHAR", None, None);
        on_null.default = Some("'N'".into());
        on_null.default_on_null = true;
        assert_eq!(
            staging_fixups_sql("\"S\"", &[on_null]).unwrap(),
            vec!["ALTER TABLE \"S\" MODIFY (\"FLAG\" DEFAULT ON NULL 'N')".to_string()]
        );

        let swap = overwrite_swap_sql("\"T\"", "\"S\"", &cols).unwrap();
        assert_eq!(
            swap,
            vec![
                "DELETE FROM \"T\"".to_string(),
                "INSERT INTO \"T\" (\"ID\", \"STATUS\", \"DOC\") SELECT \"ID\", \"STATUS\", \"DOC\" FROM \"S\" WHERE \"ID\" IS NOT NULL".to_string(),
                "INSERT INTO \"T\" (\"STATUS\", \"DOC\") SELECT \"STATUS\", \"DOC\" FROM \"S\" WHERE \"ID\" IS NULL".to_string(),
            ]
        );
        assert_eq!(
            overwrite_swap_sql("\"T\"", "\"S\"", &[id]).unwrap().len(),
            2
        );
        let plain =
            overwrite_swap_sql("\"T\"", "\"S\"", &[col("A", "NUMBER", None, None)]).unwrap();
        assert_eq!(plain.len(), 2);
    }

    #[test]
    fn composite_text_keys_fit_the_index_limit() {
        assert_eq!(key_text_width(0), 1000);
        assert_eq!(key_text_width(1), 1000);
        assert_eq!(key_text_width(2), 750);
        assert_eq!(key_text_width(3), 500);
        let planned = faucet_core::plan_columns(&[json!({"A": "x", "B": "y", "N": 1})]).unwrap();
        let sql = create_table_sql("\"T\"", &planned, &["A".to_string(), "B".to_string()]).unwrap();
        assert!(sql.contains("\"A\" VARCHAR2(750 CHAR)"), "{sql}");
        assert!(sql.contains("\"B\" VARCHAR2(750 CHAR)"), "{sql}");
    }

    #[test]
    fn watermark_statements() {
        assert_eq!(
            token_table("APP.T").unwrap(),
            "\"APP\".\"_faucet_commit_token\""
        );
        assert_eq!(token_table("T").unwrap(), "\"_faucet_commit_token\"");
        let ddl = token_table_ddl("\"W\"");
        assert!(
            ddl.contains("\"scope\" VARCHAR2(1000 CHAR) PRIMARY KEY"),
            "{ddl}"
        );
        assert!(ddl.contains("\"token\" CLOB NOT NULL"), "{ddl}");
        assert!(token_merge_sql("\"W\"").starts_with("MERGE INTO \"W\" w USING (SELECT :1"));
        assert_eq!(
            token_select_sql("\"W\""),
            "SELECT \"token\" FROM \"W\" WHERE \"scope\" = :1"
        );
        assert_eq!(
            token_delete_sql("\"W\""),
            "DELETE FROM \"W\" WHERE \"scope\" = :1"
        );
        assert_eq!(
            dictionary_binds("A.B").unwrap(),
            (Some("A".into()), "B".into())
        );
    }

    #[test]
    fn drift_schema_and_evolution_sql() {
        let mut id = col("ID", "NUMBER", Some(19), Some(0));
        id.nullable = false;
        let cols = vec![
            id,
            col("FLAG", "NUMBER", Some(1), Some(0)),
            col("AMT", "NUMBER", None, None),
            col("B", "BOOLEAN", None, None),
            col("NAME", "VARCHAR2", None, None),
        ];
        let s = schema_from_columns(&cols);
        assert_eq!(s["properties"]["ID"]["type"], "integer");
        assert_eq!(
            s["properties"]["FLAG"]["type"],
            json!(["boolean", "integer", "null"])
        );
        assert_eq!(s["properties"]["AMT"]["type"], json!(["number", "null"]));
        assert_eq!(s["properties"]["B"]["type"], json!(["boolean", "null"]));
        assert_eq!(s["properties"]["NAME"]["type"], json!(["string", "null"]));
        assert_eq!(
            s["properties"]["NAME"][faucet_core::DRIFT_ALSO_ACCEPTS],
            json!(["object", "array"])
        );
        assert!(
            s["properties"]["AMT"]
                .get(faucet_core::DRIFT_ALSO_ACCEPTS)
                .is_none()
        );
        let doc = schema_from_columns(&[col("DOC", "CLOB", None, None)]);
        let page = faucet_core::schema::infer_schema(&[json!({"DOC": {"a": 1}})]);
        let diff = faucet_core::drift::diff_schema(&doc, &page, true);
        assert!(diff.incompatible.is_empty(), "{diff:?}");

        assert!(
            add_column_sql("\"T\"", "X", SqlBaseType::Text)
                .unwrap()
                .contains("ADD (\"X\" CLOB)")
        );
        assert!(
            widen_column_sql("\"T\"", "X", SqlBaseType::Double)
                .unwrap()
                .ends_with("MODIFY (\"X\" NUMBER)")
        );
        assert!(widen_column_sql("\"T\"", "X", SqlBaseType::Text).is_err());
        assert!(
            relax_null_sql("\"T\"", "X")
                .unwrap()
                .contains("MODIFY (\"X\" NULL)")
        );
    }

    #[test]
    fn column_rows_and_case() {
        let c = column_from_row((
            "A".into(),
            "NUMBER".into(),
            Some(5),
            Some(0),
            "N".into(),
            "NO".into(),
            "ALWAYS".into(),
            None,
            None,
        ));
        assert!(!c.nullable);
        assert!(!c.insertable);
        let v = column_from_row((
            "V".into(),
            "NUMBER".into(),
            None,
            None,
            "Y".into(),
            "YES".into(),
            "NONE".into(),
            Some("1".into()),
            None,
        ));
        assert_eq!(
            v.default, None,
            "a virtual column's expression is not a default"
        );
        assert!(!v.insertable);
        let ok = column_from_row((
            "B".into(),
            "NUMBER".into(),
            None,
            None,
            "Y".into(),
            "NO".into(),
            "BY DEFAULT".into(),
            Some("\"ISEQ$$_1\".nextval".into()),
            None,
        ));
        assert!(ok.insertable && ok.nullable && ok.identity);
        assert_eq!(ok.default, None);
        let d = column_from_row((
            "D".into(),
            "VARCHAR2".into(),
            None,
            None,
            "N".into(),
            "NO".into(),
            "NONE".into(),
            Some(" 'x' \n".into()),
            Some("YES".into()),
        ));
        assert_eq!(d.default.as_deref(), Some("'x'"));
        assert!(d.default_on_null);
        assert!(!c.default_on_null);

        let recs = vec![json!({"id": 1}), json!(5)];
        assert_eq!(case_records(&recs, true)[0], json!({"ID": 1}));
        assert_eq!(case_records(&recs, true)[1], json!(5));
        assert_eq!(case_records(&recs, false)[0], json!({"id": 1}));
    }
    #[test]
    fn upsert_records_group_by_the_columns_they_carry() {
        let cols: Vec<String> = ["ID", "A", "B"].iter().map(|s| s.to_string()).collect();
        let records = vec![
            json!({"ID": 1, "A": 1, "B": 1}),
            json!({"ID": 2, "A": null}),
            json!({"ID": 3, "A": 3, "B": 3}),
        ];
        let groups = group_by_present_columns(&records, &cols);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], vec![records[0].clone(), records[2].clone()]);
        assert_eq!(groups[1], vec![records[1].clone()]);
    }
    fn oinfo(name: &str, insertable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: "VARCHAR2".into(),
            precision: None,
            scale: None,
            nullable: true,
            insertable,
            identity: false,
            default: None,
            default_on_null: false,
        }
    }

    #[test]
    fn prepare_rows_folds_keys_and_encodes_per_mode() {
        let info = vec![oinfo("ID", true), oinfo("NAME", true), oinfo("V", false)];
        let auto = OracleColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Drop,
        };
        let (cols, kinds, rows) =
            prepare_rows(&auto, &info, &[json!({"id": "1", "name": "a", "zz": 1})]).unwrap();
        assert_eq!(cols, vec!["ID", "NAME"]);
        assert_eq!(kinds, vec![BindKind::Text, BindKind::Text]);
        assert_eq!(rows[0].1.as_ref().unwrap()[1].as_deref(), Some("a"));
        let json_mode = OracleColumnMapping::JsonColumn {
            column: "DOC".into(),
        };
        let (cols, kinds, rows) = prepare_rows(&json_mode, &[], &[json!({"a": 1})]).unwrap();
        assert_eq!(
            (cols, kinds),
            (vec!["DOC".to_string()], vec![BindKind::Clob])
        );
        assert_eq!(
            rows[0].1.as_ref().unwrap()[0].as_deref(),
            Some(r#"{"a":1}"#)
        );
        assert!(drops_unknown(&auto));
        assert!(!drops_unknown(&json_mode));
        assert_eq!(insertable_names(&info), vec!["ID", "NAME"]);
    }

    #[test]
    fn fold_page_maps_keys_only_in_auto_mode() {
        let names = vec!["ID".to_string(), "NAME".to_string()];
        let spec = faucet_core::WriteSpec {
            write_mode: faucet_core::WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: None,
            rollback: None,
        };
        let auto = OracleColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Error,
        };
        let (records, folded) = fold_page(&auto, &names, &[json!({"id": 1})], &spec);
        assert_eq!(records, vec![json!({"ID": 1})]);
        assert_eq!(folded.key, vec!["ID"]);
        let (records, same) = fold_page(&auto, &names, &[], &spec);
        assert!(records.is_empty() && same.key == spec.key);
        let json_mode = OracleColumnMapping::JsonColumn {
            column: "DOC".into(),
        };
        let (records, same) = fold_page(&json_mode, &names, &[json!({"id": 1})], &spec);
        assert_eq!(
            (records[0].clone(), same.key),
            (json!({"id": 1}), spec.key.clone())
        );
    }

    #[test]
    fn upsert_groups_fold_then_group_each_chunk() {
        let insertable = vec!["ID".to_string(), "A".to_string()];
        let first = [json!({"id": 1, "a": 1}), json!({"id": 2})];
        let second = [json!({"ID": 3, "A": 3})];
        let groups = upsert_groups(&[&first[..], &second[..]], &insertable);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0], vec![json!({"ID": 1, "A": 1})]);
        assert_eq!(groups[1], vec![json!({"ID": 2})]);
    }
}

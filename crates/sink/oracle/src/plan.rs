//! Pure planning for the Oracle sink: column metadata, bind typing, value
//! encoding and every SQL statement the sink issues. No driver calls.

use base64::Engine;
use faucet_common_oracle::oracle::sql_type::OracleType;
use faucet_common_oracle::{
    TypeFamily, bytes_to_hex, hex_to_bytes, iso_to_oracle_interval, quote_ident_oracle,
    split_table, string_literal,
};
use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL,
};
use faucet_core::{FaucetError, PlannedColumn, SqlBaseType};
use serde_json::{Value, json};

use crate::config::OnUnknownField;

/// ORA-00942: table or view does not exist.
pub(crate) const ORA_TABLE_MISSING: i32 = 942;
/// ORA-00955: name is already used by an existing object.
pub(crate) const ORA_NAME_IN_USE: i32 = 955;
/// ORA-01430: column being added already exists.
pub(crate) const ORA_COLUMN_EXISTS: i32 = 1430;
/// ORA-01451: column to be modified to NULL cannot be modified to NULL.
pub(crate) const ORA_ALREADY_NULL: i32 = 1451;
/// Width of the watermark table's `scope` key column.
pub(crate) const SCOPE_COL_WIDTH: usize = 1000;

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

/// Encode one record value as the text bound for a column of `kind`.
/// `Ok(None)` binds SQL `NULL`; `Err` names why the value cannot be stored.
pub(crate) fn value_to_text(v: &Value, kind: BindKind) -> Result<Option<String>, String> {
    if v.is_null() {
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
        (BindKind::Date | BindKind::Timestamp | BindKind::TimestampTz, Value::String(s)) => {
            clamp_fraction(s)
        }
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

fn quote_all(cols: &[String]) -> Result<Vec<String>, FaucetError> {
    cols.iter().map(|c| quote_ident_oracle(c)).collect()
}

fn placeholders(n: usize) -> String {
    (1..=n)
        .map(|i| format!(":{i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `INSERT INTO t ("A", "B") VALUES (:1, :2)` — executed as array DML.
pub(crate) fn insert_sql(table: &str, cols: &[String]) -> Result<String, FaucetError> {
    Ok(format!(
        "INSERT INTO {table} ({}) VALUES ({})",
        quote_all(cols)?.join(", "),
        placeholders(cols.len())
    ))
}

/// Keyed `MERGE` upsert over one bound row (executed as array DML).
pub(crate) fn merge_sql(
    table: &str,
    key: &[String],
    cols: &[String],
) -> Result<String, FaucetError> {
    for k in key {
        if !cols.contains(k) {
            return Err(FaucetError::Sink(format!(
                "oracle upsert: key column {k:?} is not a writable column of the table"
            )));
        }
    }
    let quoted = quote_all(cols)?;
    let select = quoted
        .iter()
        .enumerate()
        .map(|(i, q)| format!(":{} AS {q}", i + 1))
        .collect::<Vec<_>>()
        .join(", ");
    let on = quote_all(key)?
        .iter()
        .map(|q| format!("t.{q} = s.{q}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let updates: Vec<String> = cols
        .iter()
        .zip(&quoted)
        .filter(|(c, _)| !key.contains(c))
        .map(|(_, q)| format!("t.{q} = s.{q}"))
        .collect();
    let matched = if updates.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {}", updates.join(", "))
    };
    Ok(format!(
        "MERGE INTO {table} t USING (SELECT {select} FROM DUAL) s ON ({on}){matched} \
         WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
        quoted.join(", "),
        quoted
            .iter()
            .map(|q| format!("s.{q}"))
            .collect::<Vec<_>>()
            .join(", ")
    ))
}

/// `DELETE FROM t WHERE "K1" = :1 AND "K2" = :2` (executed as array DML).
pub(crate) fn delete_sql(table: &str, key: &[String]) -> Result<String, FaucetError> {
    let pred = quote_all(key)?
        .iter()
        .enumerate()
        .map(|(i, q)| format!("{q} = :{}", i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!("DELETE FROM {table} WHERE {pred}"))
}

/// Run `ddl` via `EXECUTE IMMEDIATE`, swallowing the listed `ORA-` codes so
/// idempotent DDL (create-if-missing, drop-if-present) needs no pre-check.
pub(crate) fn ignoring(ddl: &str, codes: &[i32]) -> String {
    let list = codes
        .iter()
        .map(|c| format!("-{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "BEGIN EXECUTE IMMEDIATE {}; EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN ({list}) \
         THEN RAISE; END IF; END;",
        string_literal(ddl)
    )
}

/// Oracle type for a planned column. Key text is bounded (`CLOB` cannot be
/// indexed); other text is `CLOB` so no value is ever truncated.
pub(crate) fn column_type(t: SqlBaseType, is_key: bool) -> &'static str {
    match t {
        SqlBaseType::Integer => "NUMBER(19)",
        SqlBaseType::Double => "NUMBER",
        SqlBaseType::Boolean => "NUMBER(1)",
        SqlBaseType::Text | SqlBaseType::Json if is_key => "VARCHAR2(1000 CHAR)",
        SqlBaseType::Text | SqlBaseType::Json => "CLOB",
    }
}

/// `CREATE TABLE` for `auto_columns`, from the first page's planned columns.
pub(crate) fn create_table_sql(
    table: &str,
    columns: &[PlannedColumn],
    key: &[String],
) -> Result<String, FaucetError> {
    let mut defs = Vec::with_capacity(columns.len() + 1);
    for c in columns {
        defs.push(format!(
            "{} {}",
            quote_ident_oracle(&c.name)?,
            column_type(c.base_type, key.contains(&c.name))
        ));
    }
    if !key.is_empty() {
        defs.push(format!("PRIMARY KEY ({})", quote_all(key)?.join(", ")));
    }
    Ok(format!("CREATE TABLE {table} ({})", defs.join(", ")))
}

/// `CREATE TABLE` for `json_column` mode.
pub(crate) fn create_json_table_sql(table: &str, column: &str) -> Result<String, FaucetError> {
    let id = if column == "ID" { "FAUCET_ID" } else { "ID" };
    Ok(format!(
        "CREATE TABLE {table} (\"{id}\" NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {} CLOB)",
        quote_ident_oracle(column)?
    ))
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
    format!(
        "CREATE TABLE {token_table} (\"{COMMIT_TOKEN_SCOPE_COL}\" VARCHAR2({SCOPE_COL_WIDTH} CHAR) \
         PRIMARY KEY, \"{COMMIT_TOKEN_TOKEN_COL}\" CLOB NOT NULL, \
         \"updated_at\" TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL)"
    )
}

/// Upsert of a scope's token (`:1` scope, `:2` token).
pub(crate) fn token_merge_sql(token_table: &str) -> String {
    let (s, t) = (COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TOKEN_COL);
    format!(
        "MERGE INTO {token_table} w USING (SELECT :1 AS \"{s}\", :2 AS \"{t}\" FROM DUAL) n \
         ON (w.\"{s}\" = n.\"{s}\") \
         WHEN MATCHED THEN UPDATE SET w.\"{t}\" = n.\"{t}\", w.\"updated_at\" = SYSTIMESTAMP \
         WHEN NOT MATCHED THEN INSERT (\"{s}\", \"{t}\", \"updated_at\") \
         VALUES (n.\"{s}\", n.\"{t}\", SYSTIMESTAMP)"
    )
}

/// Read a scope's token (`:1` scope).
pub(crate) fn token_select_sql(token_table: &str) -> String {
    format!(
        "SELECT \"{COMMIT_TOKEN_TOKEN_COL}\" FROM {token_table} WHERE \"{COMMIT_TOKEN_SCOPE_COL}\" = :1"
    )
}

/// Bind values `(owner, table)` for the dictionary lookups below.
pub(crate) fn dictionary_binds(table: &str) -> Result<(Option<String>, String), FaucetError> {
    split_table(table)
}

/// Existence probe (`:1` owner or NULL for the current schema, `:2` table).
pub(crate) const TABLE_EXISTS_SQL: &str = "SELECT COUNT(*) FROM ALL_TABLES \
    WHERE OWNER = NVL(:1, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND TABLE_NAME = :2";

/// Column metadata (`:1` owner or NULL, `:2` table).
pub(crate) const COLUMNS_SQL: &str = "SELECT c.COLUMN_NAME, c.DATA_TYPE, c.DATA_PRECISION, \
    c.DATA_SCALE, c.NULLABLE, c.VIRTUAL_COLUMN, NVL(i.GENERATION_TYPE, 'NONE') \
    FROM ALL_TAB_COLS c LEFT JOIN ALL_TAB_IDENTITY_COLS i \
      ON i.OWNER = c.OWNER AND i.TABLE_NAME = c.TABLE_NAME AND i.COLUMN_NAME = c.COLUMN_NAME \
    WHERE c.OWNER = NVL(:1, SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')) AND c.TABLE_NAME = :2 \
      AND c.HIDDEN_COLUMN = 'NO' ORDER BY c.COLUMN_ID";

/// Build a [`ColumnInfo`] from one [`COLUMNS_SQL`] row.
pub(crate) fn column_from_row(
    name: String,
    data_type: String,
    precision: Option<i64>,
    scale: Option<i64>,
    nullable: &str,
    virtual_column: &str,
    generation: &str,
) -> ColumnInfo {
    ColumnInfo {
        name,
        data_type,
        precision,
        scale,
        nullable: nullable != "N",
        insertable: virtual_column != "YES" && generation != "ALWAYS",
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
        props.insert(c.name.clone(), json!({ "type": ty }));
    }
    json!({ "type": "object", "properties": props })
}

/// `ALTER TABLE … ADD`, idempotent.
pub(crate) fn add_column_sql(
    table: &str,
    col: &str,
    t: SqlBaseType,
) -> Result<String, FaucetError> {
    Ok(ignoring(
        &format!(
            "ALTER TABLE {table} ADD ({} {})",
            quote_ident_oracle(col)?,
            column_type(t, false)
        ),
        &[ORA_COLUMN_EXISTS],
    ))
}

/// Widen a column to `t`. Oracle can only widen a numeric column in place.
pub(crate) fn widen_column_sql(
    table: &str,
    col: &str,
    t: SqlBaseType,
) -> Result<String, FaucetError> {
    match t {
        SqlBaseType::Double => Ok(format!(
            "ALTER TABLE {table} MODIFY ({} NUMBER)",
            quote_ident_oracle(col)?
        )),
        other => Err(FaucetError::Sink(format!(
            "oracle cannot widen column {col:?} to {other:?} in place"
        ))),
    }
}

/// Relax `NOT NULL`, idempotent.
pub(crate) fn relax_null_sql(table: &str, col: &str) -> Result<String, FaucetError> {
    Ok(ignoring(
        &format!(
            "ALTER TABLE {table} MODIFY ({} NULL)",
            quote_ident_oracle(col)?
        ),
        &[ORA_ALREADY_NULL],
    ))
}

/// Overwrite staging: a structural clone of the target.
pub(crate) fn clone_table_sql(staging: &str, target: &str) -> String {
    format!("CREATE TABLE {staging} AS SELECT * FROM {target} WHERE 1 = 0")
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
    let list = quote_all(cols)?.join(", ");
    Ok([
        format!("DELETE FROM {target}"),
        format!("INSERT INTO {target} ({list}) SELECT {list} FROM {staging}"),
    ])
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

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, ty: &str, precision: Option<i64>, scale: Option<i64>) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: ty.into(),
            precision,
            scale,
            nullable: true,
            insertable: true,
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
        assert!(sql.contains("\"QTY\" NUMBER(19)"), "{sql}");
        assert!(sql.contains("\"OK\" NUMBER(1)"), "{sql}");
        assert!(
            sql.contains("\"P\" NUMBER,") || sql.contains("\"P\" NUMBER)"),
            "{sql}"
        );
        assert!(sql.contains("\"DOC\" CLOB"), "{sql}");
        assert!(sql.ends_with("PRIMARY KEY (\"SKU\"))"), "{sql}");
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
        let c = column_from_row(
            "A".into(),
            "NUMBER".into(),
            Some(5),
            Some(0),
            "N",
            "NO",
            "ALWAYS",
        );
        assert!(!c.nullable);
        assert!(!c.insertable);
        let v = column_from_row("V".into(), "NUMBER".into(), None, None, "Y", "YES", "NONE");
        assert!(!v.insertable);
        let ok = column_from_row(
            "B".into(),
            "NUMBER".into(),
            None,
            None,
            "Y",
            "NO",
            "BY DEFAULT",
        );
        assert!(ok.insertable && ok.nullable);

        let recs = vec![json!({"id": 1}), json!(5)];
        assert_eq!(case_records(&recs, true)[0], json!({"ID": 1}));
        assert_eq!(case_records(&recs, true)[1], json!(5));
        assert_eq!(case_records(&recs, false)[0], json!({"id": 1}));
    }
}

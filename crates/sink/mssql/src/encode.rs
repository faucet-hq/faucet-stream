//! Pure helpers for turning JSON records into MSSQL `INSERT` statements and
//! bound parameters. No I/O — all unit-testable.

use faucet_common_mssql::{PARAM_LIMIT, quote_ident_mssql};
use faucet_core::FaucetError;
use serde_json::Value;
use tiberius::ToSql;

use crate::coltype::placeholder;
use crate::config::OnUnknownField;

/// An owned bind parameter, so the `&dyn ToSql` slice handed to `tiberius`
/// borrows from a value that outlives the call.
pub(crate) enum BoundParam {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    /// A NULL bound as `nvarchar`, which converts to every non-binary type
    /// (an `int` NULL does not: SQL-38).
    Null(Option<String>),
    /// A NULL for a `binary` / `varbinary` / `image` column.
    NullBinary(Option<Vec<u8>>),
}

impl BoundParam {
    pub(crate) fn from_value(v: &Value) -> Self {
        match v {
            Value::String(s) => BoundParam::Str(s.clone()),
            Value::Number(n) if n.is_i64() => BoundParam::I64(n.as_i64().unwrap()),
            Value::Number(n) if n.is_u64() => match i64::try_from(n.as_u64().unwrap()) {
                Ok(i) => BoundParam::I64(i),
                // A u64 above i64::MAX would wrap to a negative i64; bind it as a
                // string so MSSQL coerces to NUMERIC/DECIMAL instead of corrupting
                // the value.
                Err(_) => BoundParam::Str(n.as_u64().unwrap().to_string()),
            },
            Value::Number(n) => BoundParam::F64(n.as_f64().unwrap_or(0.0)),
            Value::Bool(b) => BoundParam::Bool(*b),
            Value::Null => BoundParam::Null(None),
            // Arrays/objects are serialized; for a json_column this is the whole
            // record, for auto_columns it's a nested value bound as NVARCHAR.
            other => BoundParam::Str(other.to_string()),
        }
    }

    /// [`from_value`](Self::from_value), with a NULL typed for a binary column.
    pub(crate) fn for_column(v: &Value, binary: bool) -> Self {
        match v {
            Value::Null if binary => BoundParam::NullBinary(None),
            other => Self::from_value(other),
        }
    }

    pub(crate) fn as_tosql(&self) -> &dyn ToSql {
        match self {
            BoundParam::I64(v) => v,
            BoundParam::F64(v) => v,
            BoundParam::Bool(v) => v,
            BoundParam::Str(v) => v,
            BoundParam::Null(v) => v,
            BoundParam::NullBinary(v) => v,
        }
    }
}

/// `tiberius` routes parameterized statements through `sp_executesql`, which
/// consumes two of the 2100 parameters itself (the statement text and the
/// parameter-declaration string). So the usable budget for bind values is
/// `PARAM_LIMIT - 2` — sending a full 2100 bind values overflows by two.
const SP_EXECUTESQL_RESERVED: usize = 2;

/// MSSQL's table value constructor (the `VALUES (…), (…), …` clause) allows at
/// most 1000 row expressions per statement — a separate limit from the 2100
/// parameters. For narrow tables (1–2 columns) this binds before the parameter
/// budget does.
const MAX_VALUES_ROWS: usize = 1000;

/// Maximum rows per `INSERT` so the request stays within **both** of MSSQL's
/// limits: the 2100-parameter cap (minus the `sp_executesql` overhead) and the
/// 1000-row-values cap on a `VALUES` clause. Always at least 1.
pub(crate) fn max_rows_per_insert(num_cols: usize) -> usize {
    if num_cols == 0 {
        return 1;
    }
    let by_params = (PARAM_LIMIT.saturating_sub(SP_EXECUTESQL_RESERVED) / num_cols).max(1);
    by_params.min(MAX_VALUES_ROWS)
}

/// One `VALUES` row group of `width` placeholders starting at `@P{start}`, each
/// wrapped in its column's `CAST` target when `casts` names one. `casts` is
/// either empty (no casts) or one entry per column.
fn row_group(start: usize, width: usize, casts: &[Option<String>]) -> String {
    let phs: Vec<String> = (0..width)
        .map(|i| placeholder(start + i, casts.get(i).and_then(|c| c.as_deref())))
        .collect();
    format!("({})", phs.join(", "))
}

/// Build a multi-row `INSERT` with `@P`-numbered placeholders:
/// `INSERT INTO <table> (c1, c2) VALUES (@P1, @P2), (@P3, @P4), …`.
///
/// `table_quoted` and the `cols_quoted` entries must already be quoted via
/// `quote_ident_mssql`.
///
/// Each placeholder is cast to its column's type when `casts` names one, so a
/// column mixing NULLs, numbers and strings across rows has one type (SQL-39).
pub(crate) fn build_insert_sql_cast(
    table_quoted: &str,
    cols_quoted: &[String],
    casts: &[Option<String>],
    num_rows: usize,
) -> String {
    let num_cols = cols_quoted.len();
    let tuples: Vec<String> = (0..num_rows)
        .map(|row| row_group(row * num_cols + 1, num_cols, casts))
        .collect();
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        table_quoted,
        cols_quoted.join(", "),
        tuples.join(", ")
    )
}

/// Decide the fixed column list for an `auto_columns` batch.
///
/// `insertable` is the set of writable table columns (IDENTITY columns already
/// excluded), in table order. The column list is the insertable columns present
/// in the first record. Record keys that match no insertable column are
/// "unknown" and handled per `on_unknown`.
pub(crate) fn resolve_insert_columns(
    insertable: &[String],
    records: &[Value],
    on_unknown: OnUnknownField,
) -> Result<Vec<String>, FaucetError> {
    let insertable_set: std::collections::HashSet<&str> =
        insertable.iter().map(|s| s.as_str()).collect();

    // Detect unknown keys across the batch.
    let mut unknown: Vec<String> = Vec::new();
    for record in records {
        if let Some(obj) = record.as_object() {
            for key in obj.keys() {
                if !insertable_set.contains(key.as_str()) && !unknown.contains(key) {
                    unknown.push(key.clone());
                }
            }
        }
    }
    if !unknown.is_empty() {
        match on_unknown {
            OnUnknownField::Error => {
                return Err(FaucetError::Sink(format!(
                    "auto_columns: record keys {unknown:?} do not match any writable column"
                )));
            }
            OnUnknownField::Warn => {
                tracing::warn!(
                    ?unknown,
                    "auto_columns: dropping record keys with no matching column"
                );
            }
            OnUnknownField::Drop => {}
        }
    }

    // Column set = insertable columns present in ANY record (union), preserving
    // the insertable order. Using only the first record's keys would silently
    // drop a field present only in a later record of the batch (audit #146 H1);
    // a row missing a unioned column binds SQL NULL.
    let columns: Vec<String> = insertable
        .iter()
        .filter(|c| {
            records
                .iter()
                .any(|r| r.as_object().is_some_and(|o| o.contains_key(c.as_str())))
        })
        .cloned()
        .collect();
    Ok(columns)
}

/// Build a parameterized `MERGE` upsert. `table` must already be quoted; the
/// `key` / `cols` entries are bare identifiers that this function quotes via
/// [`quote_ident_mssql`].
///
/// Emits `n_rows` `VALUES` groups of `cols.len()` `@PN` params each, numbered
/// row-major so the binding order matches `n_rows` calls of
/// [`auto_row_params_typed(record, cols, …)`](auto_row_params_typed) concatenated in record
/// order. Joins on every `key` column, `UPDATE`s the non-key columns, and
/// `INSERT`s all columns. When every column is a key there is nothing to
/// update, so the `WHEN MATCHED` clause is omitted entirely. T-SQL requires a
/// terminating `;` on `MERGE`, so one is always appended.
///
/// Each placeholder is cast to its column's type (SQL-38, SQL-39); `casts` is
/// empty or one entry per `cols` entry.
pub(crate) fn build_merge_cast(
    table: &str,
    key: &[String],
    cols: &[String],
    casts: &[Option<String>],
    n_rows: usize,
) -> Result<String, FaucetError> {
    let q = |s: &str| quote_ident_mssql(s);
    let quoted_cols: Vec<String> = cols.iter().map(|c| q(c)).collect::<Result<_, _>>()?;
    let quoted_keys: Vec<String> = key.iter().map(|k| q(k)).collect::<Result<_, _>>()?;
    let col_list = quoted_cols.join(", ");

    let groups: Vec<String> = (0..n_rows)
        .map(|row| row_group(row * cols.len() + 1, cols.len(), casts))
        .collect();

    let on = quoted_keys
        .iter()
        .map(|qk| format!("tgt.{qk} = src.{qk}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    // Non-key columns get UPDATE assignments; compare on the *bare* names so a
    // column equal to a key column (after quoting) is correctly excluded.
    let update_set = cols
        .iter()
        .zip(&quoted_cols)
        .filter(|(c, _)| !key.iter().any(|k| k == *c))
        .map(|(_, qc)| format!("tgt.{qc} = src.{qc}"))
        .collect::<Vec<_>>()
        .join(", ");

    let insert_vals = quoted_cols
        .iter()
        .map(|qc| format!("src.{qc}"))
        .collect::<Vec<_>>()
        .join(", ");

    let matched = if update_set.is_empty() {
        String::new()
    } else {
        format!(" WHEN MATCHED THEN UPDATE SET {update_set}")
    };

    Ok(format!(
        "MERGE {table} AS tgt USING (VALUES {}) AS src ({col_list}) ON {on}{matched} \
         WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals});",
        groups.join(", ")
    ))
}

/// Build a parameterized `MERGE … WHEN MATCHED THEN DELETE` for composite-key
/// deletes. `table` must already be quoted; `key` entries are bare identifiers
/// quoted here.
///
/// T-SQL has no row-constructor `IN ((a,b), …)`, so a composite-key delete is
/// expressed as a `MERGE` whose source is the `VALUES` list of key tuples.
/// Params are numbered row-major over the key columns, matching how the caller
/// binds each [`KeyTuple`](faucet_core::KeyTuple)'s values in `key` order.
///
/// Each key placeholder is cast to its column's type when `casts` names one.
pub(crate) fn build_merge_delete_cast(
    table: &str,
    key: &[String],
    casts: &[Option<String>],
    n_rows: usize,
) -> Result<String, FaucetError> {
    let quoted_keys: Vec<String> = key
        .iter()
        .map(|k| quote_ident_mssql(k))
        .collect::<Result<_, _>>()?;
    let key_list = quoted_keys.join(", ");

    let groups: Vec<String> = (0..n_rows)
        .map(|row| row_group(row * key.len() + 1, key.len(), casts))
        .collect();

    let on = quoted_keys
        .iter()
        .map(|qk| format!("tgt.{qk} = src.{qk}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    Ok(format!(
        "MERGE {table} AS tgt USING (VALUES {}) AS src ({key_list}) ON {on} \
         WHEN MATCHED THEN DELETE;",
        groups.join(", ")
    ))
}

/// Session-scoped temp table holding the key tuples one invocation wrote, used
/// by scoped cleanup (#478).
///
/// The `#` prefix scopes it to the connection's session, so two pipelines
/// cleaning up concurrently on different pooled connections cannot collide on
/// the name. It is a fixed constant — no user input reaches it — so it is
/// interpolated bare rather than through [`quote_ident_mssql`].
pub(crate) const CLEANUP_TEMP_TABLE: &str = "#faucet_cleanup_keys";

/// Drop a leftover [`CLEANUP_TEMP_TABLE`] if one exists.
///
/// A pooled connection outlives a single cleanup and a `#temp` table lives for
/// the whole session, so a previous cleanup on the same connection can still own
/// the name. `DROP TABLE IF EXISTS` is SQL Server 2016+, so the older
/// `OBJECT_ID(N'tempdb..#…')` guard is used instead for wider server support.
pub(crate) fn build_cleanup_temp_drop_sql() -> String {
    format!(
        "IF OBJECT_ID(N'tempdb..{CLEANUP_TEMP_TABLE}') IS NOT NULL DROP TABLE {CLEANUP_TEMP_TABLE}"
    )
}

/// Create [`CLEANUP_TEMP_TABLE`] with the *exact* column types of the target
/// table's `key` columns.
///
/// `SELECT … INTO` copies each column's full declared type (length, precision,
/// scale, collation), which reconstructing a `CREATE TABLE` from `sys.columns`
/// would have to re-derive — and getting e.g. `nvarchar` length wrong would
/// silently truncate a key and delete the wrong rows. `WHERE 1 = 0` means no row
/// is ever read.
///
/// The `UNION ALL` derived table is load-bearing, not decoration: a plain
/// `SELECT … INTO` **inherits the IDENTITY property** of a source column, and
/// inserting into an identity column then fails without `IDENTITY_INSERT`. A
/// column produced by a `UNION` never inherits it.
///
/// `table_quoted` must already be quoted; `key` entries are bare identifiers
/// quoted here.
pub(crate) fn build_cleanup_temp_create_sql(
    table_quoted: &str,
    key: &[String],
) -> Result<String, FaucetError> {
    if key.is_empty() {
        return Err(FaucetError::Sink(
            "cleanup: requires a non-empty `key` to build the written-key set".into(),
        ));
    }
    let quoted_keys: Vec<String> = key
        .iter()
        .map(|k| quote_ident_mssql(k))
        .collect::<Result<_, _>>()?;
    let key_list = quoted_keys.join(", ");
    Ok(format!(
        "SELECT {key_list} INTO {CLEANUP_TEMP_TABLE} FROM \
         (SELECT {key_list} FROM {table_quoted} WHERE 1 = 0 \
         UNION ALL SELECT {key_list} FROM {table_quoted} WHERE 1 = 0) AS s"
    ))
}

/// Build the multi-row `INSERT` that loads written key tuples into
/// [`CLEANUP_TEMP_TABLE`]. Params are numbered row-major over the key columns,
/// matching how the caller binds each [`KeyTuple`](faucet_core::KeyTuple)'s
/// values in `key` order.
///
/// The caller must chunk `n_rows` by
/// [`max_rows_per_insert(key.len())`](max_rows_per_insert): the written-key set
/// routinely runs to tens of thousands of rows, far past MSSQL's 2100-parameter
/// and 1000-row-values ceilings.
pub(crate) fn build_cleanup_key_insert_sql(
    key: &[String],
    casts: &[Option<String>],
    n_rows: usize,
) -> Result<String, FaucetError> {
    let quoted_keys: Vec<String> = key
        .iter()
        .map(|k| quote_ident_mssql(k))
        .collect::<Result<_, _>>()?;
    Ok(build_insert_sql_cast(
        CLEANUP_TEMP_TABLE,
        &quoted_keys,
        casts,
        n_rows,
    ))
}

/// Build the scoped-cleanup `DELETE`: every row matching `scope` whose key is
/// **not** in [`CLEANUP_TEMP_TABLE`].
///
/// `NOT EXISTS` against the temp table rather than `key NOT IN (@P1, …)` because
/// the written-key set can reach the cleanup ceiling (100k rows by default),
/// hundreds of times MSSQL's 2100-parameter limit.
///
/// Scope predicates are bound `@P1..@Pn` in the order `scope_cols` is given, so
/// the caller must bind its values in that same order. `table_quoted` must
/// already be quoted; `scope_cols` / `key` entries are bare identifiers quoted
/// here.
pub(crate) fn build_cleanup_delete_sql(
    table_quoted: &str,
    scope_cols: &[&str],
    key: &[String],
) -> Result<String, FaucetError> {
    if scope_cols.is_empty() {
        // Refusing here is the difference between "delete this parent's stale
        // rows" and "truncate the table" — an empty predicate matches everything.
        return Err(FaucetError::Sink(
            "cleanup: the completeness claim is empty — an empty scope would match every \
             row in the destination"
                .into(),
        ));
    }
    if key.is_empty() {
        return Err(FaucetError::Sink(
            "cleanup: requires a non-empty `key` so a written row can be told apart from a \
             stale one"
                .into(),
        ));
    }

    let scope_pred = scope_cols
        .iter()
        .enumerate()
        .map(|(i, c)| Ok(format!("t.{} = @P{}", quote_ident_mssql(c)?, i + 1)))
        .collect::<Result<Vec<_>, FaucetError>>()?
        .join(" AND ");

    let join_pred = key
        .iter()
        .map(|k| {
            let q = quote_ident_mssql(k)?;
            Ok(format!("c.{q} = t.{q}"))
        })
        .collect::<Result<Vec<_>, FaucetError>>()?
        .join(" AND ");

    Ok(format!(
        "DELETE t FROM {table_quoted} AS t WHERE {scope_pred} \
         AND NOT EXISTS (SELECT 1 FROM {CLEANUP_TEMP_TABLE} c WHERE {join_pred})"
    ))
}

/// Bind one record's values in `columns` order (SQL NULL for missing keys),
/// with NULLs typed `varbinary` for the columns flagged in `binary`.
/// Upsert rows grouped by the subset of `columns` each carries (a key present
/// with `null` counts as carried), in first-seen order. An upsert writes each
/// row's own columns only, so a column a row omits keeps its stored value
/// instead of being overwritten with NULL (#789 SQL-10).
pub(crate) fn group_by_present_columns<'a>(
    records: &'a [Value],
    columns: &[String],
) -> Vec<(Vec<String>, Vec<&'a Value>)> {
    let mut groups: Vec<(Vec<String>, Vec<&'a Value>)> = Vec::new();
    for record in records {
        let present: Vec<String> = columns
            .iter()
            .filter(|c| {
                record
                    .as_object()
                    .is_some_and(|o| o.contains_key(c.as_str()))
            })
            .cloned()
            .collect();
        match groups.iter_mut().find(|(p, _)| *p == present) {
            Some((_, rows)) => rows.push(record),
            None => groups.push((present, vec![record])),
        }
    }
    groups
}

pub(crate) fn auto_row_params_typed(
    record: &Value,
    columns: &[String],
    binary: &[bool],
) -> Vec<BoundParam> {
    let obj = record.as_object();
    columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            let v = obj.and_then(|o| o.get(c)).unwrap_or(&Value::Null);
            BoundParam::for_column(v, binary.get(i).copied().unwrap_or(false))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn upsert_rows_group_by_the_columns_they_carry() {
        let cols: Vec<String> = ["id", "a", "b"].iter().map(|s| s.to_string()).collect();
        let records = vec![
            json!({"id": 1, "a": 1, "b": 1}),
            json!({"id": 2, "a": null}),
            json!({"id": 3, "a": 3, "b": 3}),
            json!(7),
        ];
        let groups = group_by_present_columns(&records, &cols);
        assert_eq!(groups.len(), 3);
        assert_eq!(groups[0].0, cols);
        assert_eq!(groups[0].1, vec![&records[0], &records[2]]);
        assert_eq!(groups[1].0, vec!["id".to_string(), "a".to_string()]);
        assert!(groups[2].0.is_empty(), "a non-object carries nothing");
    }

    fn build_merge_cast_t(
        table: &str,
        key: &[String],
        cols: &[String],
        n_rows: usize,
    ) -> Result<String, FaucetError> {
        build_merge_cast(table, key, cols, &[], n_rows)
    }

    fn build_merge_delete_cast_t(
        table: &str,
        key: &[String],
        n_rows: usize,
    ) -> Result<String, FaucetError> {
        build_merge_delete_cast(table, key, &[], n_rows)
    }

    #[test]
    fn param_split_respects_both_mssql_limits() {
        // Narrow tables are capped by the 1000-row-values limit; wider tables by
        // the parameter budget (PARAM_LIMIT - 2 sp_executesql params).
        assert_eq!(max_rows_per_insert(1), 1000); // 2098 params allowed, but row cap is 1000
        assert_eq!(max_rows_per_insert(2), 1000); // 1049 params allowed, row cap 1000
        assert_eq!(max_rows_per_insert(3), 699); // 2098/3 = 699 < 1000
        assert_eq!(max_rows_per_insert(0), 1);
        assert_eq!(max_rows_per_insert(5000), 1);
        // For any column count, a single INSERT must satisfy BOTH limits:
        // (rows*cols + 2 sp params) <= 2100, and rows <= 1000.
        for cols in 1..=64 {
            let rows = max_rows_per_insert(cols);
            assert!(
                rows <= MAX_VALUES_ROWS,
                "cols={cols} exceeds 1000 row values"
            );
            assert!(
                rows * cols + SP_EXECUTESQL_RESERVED <= PARAM_LIMIT,
                "cols={cols} exceeds the 2100 parameter limit"
            );
        }
    }

    #[test]
    fn insert_sql_numbers_placeholders_across_rows() {
        let sql = build_insert_sql_cast("[dbo].[events]", &["[a]".into(), "[b]".into()], &[], 2);
        assert_eq!(
            sql,
            "INSERT INTO [dbo].[events] ([a], [b]) VALUES (@P1, @P2), (@P3, @P4)"
        );
    }

    #[test]
    fn resolve_columns_fixes_from_first_record() {
        let insertable = vec!["id".to_string(), "name".to_string(), "age".to_string()];
        let records = vec![json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})];
        let cols = resolve_insert_columns(&insertable, &records, OnUnknownField::Warn).unwrap();
        assert_eq!(cols, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn resolve_columns_errors_on_unknown_when_configured() {
        let insertable = vec!["id".to_string()];
        let records = vec![json!({"id": 1, "extra": "x"})];
        assert!(resolve_insert_columns(&insertable, &records, OnUnknownField::Error).is_err());
        // Warn/Drop tolerate it.
        assert!(resolve_insert_columns(&insertable, &records, OnUnknownField::Drop).is_ok());
    }

    #[test]
    fn resolve_columns_unions_keys_across_records() {
        // H1 (audit #146): the column set is the UNION across all records, not
        // just the first record's keys — a field present only in a LATER record
        // must not be dropped. Order follows `insertable` (declared column order).
        let insertable = vec!["id".to_string(), "name".to_string(), "email".to_string()];
        // The first record is the sparsest; `name`/`email` appear only later.
        let records = vec![
            json!({ "id": 1 }),
            json!({ "id": 2, "name": "b", "email": "x@y" }),
        ];
        let cols = resolve_insert_columns(&insertable, &records, OnUnknownField::Warn).unwrap();
        assert_eq!(
            cols,
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
            "later-record-only columns (name, email) must be included"
        );
    }

    #[test]
    fn auto_row_params_binds_null_for_missing_keys() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let params = auto_row_params_typed(&json!({"id": 7}), &columns, &[]);
        assert_eq!(params.len(), 2);
        assert!(matches!(params[0], BoundParam::I64(7)));
        assert!(matches!(params[1], BoundParam::Null(None)));
    }

    #[test]
    fn bound_param_classifies_json() {
        assert!(matches!(
            BoundParam::from_value(&json!("s")),
            BoundParam::Str(_)
        ));
        assert!(matches!(
            BoundParam::from_value(&json!(7)),
            BoundParam::I64(7)
        ));
        assert!(matches!(
            BoundParam::from_value(&json!(1.5)),
            BoundParam::F64(_)
        ));
        assert!(matches!(
            BoundParam::from_value(&json!(true)),
            BoundParam::Bool(true)
        ));
        assert!(matches!(
            BoundParam::from_value(&Value::Null),
            BoundParam::Null(None)
        ));
        // nested -> serialized string
        assert!(matches!(
            BoundParam::from_value(&json!({"k":1})),
            BoundParam::Str(_)
        ));
    }

    #[test]
    fn mssql_merge_statement_shape() {
        let sql = build_merge_cast_t(
            "[dbo].[t]",
            &["id".to_string()],
            &["id".to_string(), "name".to_string()],
            1,
        )
        .unwrap();
        assert!(sql.contains("MERGE [dbo].[t] AS tgt"), "{sql}");
        assert!(
            sql.contains("USING (VALUES (@P1, @P2)) AS src ([id], [name])"),
            "{sql}"
        );
        assert!(sql.contains("ON tgt.[id] = src.[id]"), "{sql}");
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET tgt.[name] = src.[name]"),
            "{sql}"
        );
        assert!(
            sql.contains(
                "WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES (src.[id], src.[name])"
            ),
            "{sql}"
        );
        assert!(
            sql.trim_end().ends_with(';'),
            "MERGE needs a terminating semicolon: {sql}"
        );
    }

    #[test]
    fn mssql_merge_all_keys_has_no_update_clause() {
        // When every column is a key, there is nothing to UPDATE — emit no WHEN
        // MATCHED clause.
        let sql = build_merge_cast_t("[t]", &["id".to_string()], &["id".to_string()], 2).unwrap();
        assert!(!sql.contains("WHEN MATCHED"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
        assert!(sql.contains("(@P1), (@P2)"), "two single-col rows: {sql}");
    }

    #[test]
    fn mssql_merge_numbers_params_row_major() {
        // Two rows of two columns → @P1..@P4 in row-major order, matching how
        // `auto_row_params` is concatenated per row.
        let sql = build_merge_cast_t(
            "[t]",
            &["id".to_string()],
            &["id".to_string(), "name".to_string()],
            2,
        )
        .unwrap();
        assert!(
            sql.contains("VALUES (@P1, @P2), (@P3, @P4)"),
            "row-major param numbering: {sql}"
        );
    }

    #[test]
    fn mssql_merge_composite_key_joins_on_all_key_cols() {
        let sql = build_merge_cast_t(
            "[t]",
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
            1,
        )
        .unwrap();
        assert!(
            sql.contains("ON tgt.[a] = src.[a] AND tgt.[b] = src.[b]"),
            "{sql}"
        );
        // Only the non-key column `v` is updated.
        assert!(
            sql.contains("WHEN MATCHED THEN UPDATE SET tgt.[v] = src.[v]"),
            "{sql}"
        );
    }

    #[test]
    fn mssql_merge_delete_statement_shape() {
        let sql = build_merge_delete_cast_t("[dbo].[t]", &["id".to_string()], 2).unwrap();
        assert!(sql.contains("MERGE [dbo].[t] AS tgt"), "{sql}");
        assert!(
            sql.contains("USING (VALUES (@P1), (@P2)) AS src ([id])"),
            "{sql}"
        );
        assert!(sql.contains("ON tgt.[id] = src.[id]"), "{sql}");
        assert!(sql.contains("WHEN MATCHED THEN DELETE"), "{sql}");
        assert!(sql.trim_end().ends_with(';'), "{sql}");
    }

    #[test]
    fn mssql_merge_delete_composite_key() {
        let sql = build_merge_delete_cast_t("[t]", &["a".to_string(), "b".to_string()], 1).unwrap();
        assert!(
            sql.contains("USING (VALUES (@P1, @P2)) AS src ([a], [b])"),
            "{sql}"
        );
        assert!(
            sql.contains("ON tgt.[a] = src.[a] AND tgt.[b] = src.[b]"),
            "{sql}"
        );
        assert!(sql.contains("WHEN MATCHED THEN DELETE"), "{sql}");
    }

    #[test]
    fn cleanup_temp_drop_guard_is_version_portable() {
        let sql = build_cleanup_temp_drop_sql();
        // `DROP TABLE IF EXISTS` is 2016+; the OBJECT_ID guard works everywhere.
        assert_eq!(
            sql,
            "IF OBJECT_ID(N'tempdb..#faucet_cleanup_keys') IS NOT NULL \
             DROP TABLE #faucet_cleanup_keys"
        );
    }

    #[test]
    fn cleanup_temp_create_copies_key_types_via_select_into() {
        let sql = build_cleanup_temp_create_sql("[dbo].[assoc]", &["id".to_string()]).unwrap();
        assert_eq!(
            sql,
            "SELECT [id] INTO #faucet_cleanup_keys FROM \
             (SELECT [id] FROM [dbo].[assoc] WHERE 1 = 0 \
             UNION ALL SELECT [id] FROM [dbo].[assoc] WHERE 1 = 0) AS s"
        );
    }

    #[test]
    fn cleanup_temp_create_strips_identity_via_union_all() {
        // A plain SELECT … INTO inherits the source column's IDENTITY property,
        // which then rejects the key INSERTs. A UNION'd column never does — so
        // the UNION ALL must survive any refactor of this statement.
        let sql =
            build_cleanup_temp_create_sql("[t]", &["a".to_string(), "b".to_string()]).unwrap();
        assert!(sql.contains("UNION ALL"), "identity stripping lost: {sql}");
        assert!(
            sql.contains("SELECT [a], [b] INTO #faucet_cleanup_keys"),
            "{sql}"
        );
        // No row is ever read out of the target table.
        assert_eq!(sql.matches("WHERE 1 = 0").count(), 2, "{sql}");
    }

    #[test]
    fn cleanup_temp_create_requires_a_key() {
        let err = build_cleanup_temp_create_sql("[t]", &[]).unwrap_err();
        assert!(err.to_string().contains("`key`"), "{err}");
    }

    #[test]
    fn casts_give_every_row_of_a_column_one_type() {
        let casts = vec![Some("datetime2(7)".to_string()), None];
        let sql = build_insert_sql_cast("[t]", &["[a]".into(), "[b]".into()], &casts, 2);
        assert_eq!(
            sql,
            "INSERT INTO [t] ([a], [b]) VALUES (CAST(@P1 AS datetime2(7)), @P2), \
             (CAST(@P3 AS datetime2(7)), @P4)"
        );
        let merge = build_merge_cast(
            "[t]",
            &["id".to_string()],
            &["id".to_string(), "note".to_string()],
            &[Some("int".into()), Some("nvarchar(max)".into())],
            2,
        )
        .unwrap();
        assert!(
            merge.contains(
                "(CAST(@P1 AS int), CAST(@P2 AS nvarchar(max))), \
                 (CAST(@P3 AS int), CAST(@P4 AS nvarchar(max)))"
            ),
            "{merge}"
        );
        let delete =
            build_merge_delete_cast("[t]", &["id".to_string()], &[Some("bigint".into())], 2)
                .unwrap();
        assert!(
            delete.contains("(CAST(@P1 AS bigint)), (CAST(@P2 AS bigint))"),
            "{delete}"
        );
        let keys =
            build_cleanup_key_insert_sql(&["id".to_string()], &[Some("int".into())], 1).unwrap();
        assert!(keys.ends_with("VALUES (CAST(@P1 AS int))"), "{keys}");
    }

    #[test]
    fn nulls_bind_as_text_or_binary_never_int() {
        let rows = auto_row_params_typed(
            &json!({"a": null, "b": "x"}),
            &["a".to_string(), "b".to_string(), "c".to_string()],
            &[false, false, true],
        );
        assert!(matches!(rows[0], BoundParam::Null(None)));
        assert!(matches!(rows[1], BoundParam::Str(_)));
        assert!(matches!(rows[2], BoundParam::NullBinary(None)));
        for p in &rows {
            let _ = p.as_tosql();
        }
        assert!(matches!(
            BoundParam::for_column(&json!(1), true),
            BoundParam::I64(1)
        ));
    }

    #[test]
    fn cleanup_key_insert_numbers_params_row_major() {
        let sql =
            build_cleanup_key_insert_sql(&["a".to_string(), "b".to_string()], &[], 2).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO #faucet_cleanup_keys ([a], [b]) VALUES (@P1, @P2), (@P3, @P4)"
        );
    }

    #[test]
    fn cleanup_delete_scopes_then_excludes_written_keys() {
        let sql = build_cleanup_delete_sql("[dbo].[assoc]", &["contact_id"], &["id".to_string()])
            .unwrap();
        assert_eq!(
            sql,
            "DELETE t FROM [dbo].[assoc] AS t WHERE t.[contact_id] = @P1 \
             AND NOT EXISTS (SELECT 1 FROM #faucet_cleanup_keys c WHERE c.[id] = t.[id])"
        );
    }

    #[test]
    fn cleanup_delete_ands_a_composite_scope_and_key() {
        let sql = build_cleanup_delete_sql(
            "[t]",
            &["tenant", "contact_id"],
            &["a".to_string(), "b".to_string()],
        )
        .unwrap();
        // Scope params are numbered in the given column order — the caller binds
        // its values in that same order.
        assert!(
            sql.contains("WHERE t.[tenant] = @P1 AND t.[contact_id] = @P2"),
            "{sql}"
        );
        assert!(
            sql.contains("WHERE c.[a] = t.[a] AND c.[b] = t.[b]"),
            "{sql}"
        );
    }

    #[test]
    fn cleanup_delete_refuses_an_empty_scope() {
        // An empty predicate is a truncate, not a cleanup.
        let err = build_cleanup_delete_sql("[t]", &[], &["id".to_string()]).unwrap_err();
        assert!(err.to_string().contains("every row"), "{err}");
    }

    #[test]
    fn cleanup_delete_refuses_an_empty_key() {
        // Without a key every row in the scope looks unwritten.
        let err = build_cleanup_delete_sql("[t]", &["contact_id"], &[]).unwrap_err();
        assert!(err.to_string().contains("`key`"), "{err}");
    }

    #[test]
    fn cleanup_identifiers_are_bracket_quoted() {
        let sql = build_cleanup_delete_sql("[t]", &["we]ird"], &["k]ey".to_string()]).unwrap();
        assert!(sql.contains("t.[we]]ird] = @P1"), "{sql}");
        assert!(sql.contains("c.[k]]ey] = t.[k]]ey]"), "{sql}");
    }

    #[test]
    fn cleanup_key_insert_chunk_size_respects_param_limits() {
        // The written-key set can reach the 100k cleanup ceiling, so the caller
        // chunks by max_rows_per_insert(key.len()) — assert a full chunk of the
        // widest realistic key still fits both MSSQL limits.
        for key_cols in 1..=8 {
            let rows = max_rows_per_insert(key_cols);
            assert!(rows <= MAX_VALUES_ROWS, "key_cols={key_cols}");
            assert!(
                rows * key_cols + SP_EXECUTESQL_RESERVED <= PARAM_LIMIT,
                "key_cols={key_cols}"
            );
        }
    }

    #[test]
    fn u64_above_i64_max_binds_as_string_not_wrapped() {
        // A u64 that fits in i64 still binds as I64.
        match BoundParam::from_value(&json!(i64::MAX as u64)) {
            BoundParam::I64(v) => assert_eq!(v, i64::MAX),
            _ => panic!("expected I64 for an in-range u64"),
        }
        // A u64 above i64::MAX must NOT wrap to a negative I64 — bind as a string
        // so MSSQL coerces to NUMERIC/DECIMAL rather than corrupting the value.
        match BoundParam::from_value(&json!(u64::MAX)) {
            BoundParam::Str(s) => assert_eq!(s, "18446744073709551615"),
            BoundParam::I64(v) => panic!("u64::MAX wrapped to negative I64({v})"),
            _ => panic!("expected Str for u64 > i64::MAX"),
        }
    }
}

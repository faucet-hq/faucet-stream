//! The decisions behind each write, separated from the connection that runs
//! them: column selection, row preparation, the parameterized statements of
//! the keyed and cleanup paths, and the timeout contract. Pure — no I/O.

use std::future::Future;
use std::time::Duration;

use faucet_common_mssql::quote_ident_mssql;
use faucet_core::{FaucetError, KeyTuple};
use serde_json::Value;

use crate::coltype::{ColumnInfo, binary_flags, casts_for};
use crate::config::{MssqlColumnMapping, OnUnknownField};
use crate::encode::{
    BoundParam, auto_row_params_typed, build_cleanup_key_insert_sql, build_insert_sql_cast,
    build_merge_cast, build_merge_delete_cast, group_by_present_columns, max_rows_per_insert,
    resolve_insert_columns,
};

/// One chunk ready to bind: the column list, each column's `CAST` target, and
/// the per-row parameters.
pub(crate) struct Prepared {
    pub(crate) cols: Vec<String>,
    pub(crate) casts: Vec<Option<String>>,
    pub(crate) rows: Vec<Vec<BoundParam>>,
}

/// One parameterized statement and its parameters, row-major.
pub(crate) struct Statement {
    pub(crate) sql: String,
    pub(crate) params: Vec<BoundParam>,
}

/// `infos` when the table has a writable (non-IDENTITY) column; an error
/// naming `table` otherwise (it also covers a table that does not exist).
pub(crate) fn writable(
    infos: Vec<ColumnInfo>,
    table: &str,
) -> Result<Vec<ColumnInfo>, FaucetError> {
    if infos.iter().any(|c| !c.is_identity) {
        return Ok(infos);
    }
    Err(FaucetError::Sink(format!(
        "MSSQL table '{table}' has no writable columns or does not exist"
    )))
}

/// Writable column names, in table order: not IDENTITY, computed or
/// `rowversion`.
pub(crate) fn insertable(infos: &[ColumnInfo]) -> Vec<String> {
    infos
        .iter()
        .filter(|c| !c.is_identity && copyable(c))
        .map(|c| c.name.clone())
        .collect()
}

/// A column a value can be written to at all: not computed, not `rowversion`.
fn copyable(c: &ColumnInfo) -> bool {
    !c.is_computed && c.type_name != "timestamp"
}

/// Overwrite staging: a clone of the target's writable columns (computed and
/// `rowversion` columns left out) without the IDENTITY property — a `UNION`
/// in `SELECT … INTO` drops it — so record identity values can land.
pub(crate) fn staging_create_sql(
    staging: &str,
    target: &str,
    target_infos: &[ColumnInfo],
) -> Result<String, FaucetError> {
    let cols = target_infos
        .iter()
        .filter(|c| copyable(c))
        .map(|c| quote_ident_mssql(&c.name))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!(
        "SELECT {cols} INTO {staging} FROM (SELECT {cols} FROM {target} WHERE 1 = 0 \
         UNION ALL SELECT {cols} FROM {target} WHERE 1 = 0) AS t"
    ))
}

/// Make the target's IDENTITY column nullable in staging, so a record without
/// it still stages (the target generates it at the swap).
pub(crate) fn staging_relax_sql(
    staging: &str,
    target_infos: &[ColumnInfo],
) -> Result<Option<String>, FaucetError> {
    let Some(id) = target_infos.iter().find(|c| c.is_identity) else {
        return Ok(None);
    };
    let ty = id.declared_type().ok_or_else(|| {
        FaucetError::Sink(format!(
            "mssql overwrite: identity column '{}' has an unsupported type {}",
            id.name, id.type_name
        ))
    })?;
    Ok(Some(format!(
        "ALTER TABLE {staging} ALTER COLUMN {} {ty} NULL",
        quote_ident_mssql(&id.name)?
    )))
}

/// The overwrite swap, column lists built from the **target**: computed and
/// `rowversion` columns are never copied; the IDENTITY column is copied with
/// `IDENTITY_INSERT` where staging has a value (so identities survive the
/// overwrite) and generated where it does not.
pub(crate) fn overwrite_swap_sql(
    target: &str,
    staging: &str,
    target_infos: &[ColumnInfo],
) -> Result<Vec<String>, FaucetError> {
    let list = |cols: &[&ColumnInfo]| -> Result<String, FaucetError> {
        Ok(cols
            .iter()
            .map(|c| quote_ident_mssql(&c.name))
            .collect::<Result<Vec<_>, _>>()?
            .join(", "))
    };
    let plain: Vec<&ColumnInfo> = target_infos
        .iter()
        .filter(|c| !c.is_identity && copyable(c))
        .collect();
    let mut out = vec![format!("DELETE FROM {target}")];
    let Some(id) = target_infos.iter().find(|c| c.is_identity) else {
        let cols = list(&plain)?;
        out.push(format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging}"
        ));
        return Ok(out);
    };
    let id_q = quote_ident_mssql(&id.name)?;
    let mut with_id = vec![id];
    with_id.extend(plain.iter().copied());
    let all = list(&with_id)?;
    out.push(format!("SET IDENTITY_INSERT {target} ON"));
    out.push(format!(
        "INSERT INTO {target} ({all}) SELECT {all} FROM {staging} WHERE {id_q} IS NOT NULL"
    ));
    out.push(format!("SET IDENTITY_INSERT {target} OFF"));
    if !plain.is_empty() {
        let cols = list(&plain)?;
        out.push(format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {staging} WHERE {id_q} IS NULL"
        ));
    }
    Ok(out)
}

/// Refuse every write once a timed-out connection could not be replaced: it
/// may still hold an open transaction (SQL-37).
pub(crate) fn poison_check(poisoned: bool) -> Result<(), FaucetError> {
    if poisoned {
        return Err(FaucetError::Sink(
            "MSSQL: an earlier statement timed out and its connection could not be \
             replaced; it may hold an open transaction, so no further write is safe"
                .into(),
        ));
    }
    Ok(())
}

/// The column list and per-row parameters for one chunk. `infos` is only read
/// in `auto_columns` mode. `None` when no record key matches a column.
pub(crate) fn prepare_rows(
    mapping: &MssqlColumnMapping,
    infos: &[ColumnInfo],
    chunk: &[Value],
) -> Result<Option<Prepared>, FaucetError> {
    match mapping {
        MssqlColumnMapping::JsonColumn { column } => {
            let rows: Vec<Vec<BoundParam>> = chunk
                .iter()
                .map(|r| {
                    serde_json::to_string(r)
                        .map(|s| vec![BoundParam::Str(s)])
                        .map_err(|e| {
                            FaucetError::Sink(format!(
                                "MSSQL json_column: failed to serialize record to JSON: {e}"
                            ))
                        })
                })
                .collect::<Result<_, _>>()?;
            Ok(Some(Prepared {
                cols: vec![column.clone()],
                casts: Vec::new(),
                rows,
            }))
        }
        MssqlColumnMapping::AutoColumns { on_unknown_field } => {
            let cols = resolve_insert_columns(&insertable(infos), chunk, *on_unknown_field)?;
            if cols.is_empty() {
                return Ok(None);
            }
            let binary = binary_flags(infos, &cols);
            let rows = chunk
                .iter()
                .map(|r| auto_row_params_typed(r, &cols, &binary))
                .collect();
            Ok(Some(Prepared {
                casts: casts_for(infos, &cols),
                cols,
                rows,
            }))
        }
    }
}

/// The ≤2100-parameter sub-`INSERT`s for `n_rows` prepared rows: each
/// statement with the range of rows it binds.
pub(crate) fn insert_statements(
    table_quoted: &str,
    cols_quoted: &[String],
    casts: &[Option<String>],
    n_rows: usize,
) -> Vec<(String, std::ops::Range<usize>)> {
    let per = max_rows_per_insert(cols_quoted.len());
    (0..n_rows)
        .step_by(per.max(1))
        .map(|start| {
            let end = (start + per).min(n_rows);
            (
                build_insert_sql_cast(table_quoted, cols_quoted, casts, end - start),
                start..end,
            )
        })
        .collect()
}

/// The upsert `MERGE`s for `upserts`: one per set of carried columns, so a
/// column a record omits keeps its stored value (SQL-10), each chunked under
/// the parameter ceiling.
pub(crate) fn merge_statements(
    table_quoted: &str,
    key: &[String],
    infos: &[ColumnInfo],
    on_unknown: OnUnknownField,
    upserts: &[Value],
) -> Result<Vec<Statement>, FaucetError> {
    let cols = resolve_insert_columns(&insertable(infos), upserts, on_unknown)?;
    let mut out = Vec::new();
    for (cols, rows) in group_by_present_columns(upserts, &cols) {
        if cols.is_empty() {
            continue;
        }
        let casts = casts_for(infos, &cols);
        let binary = binary_flags(infos, &cols);
        for sub in rows.chunks(max_rows_per_insert(cols.len())) {
            out.push(Statement {
                sql: build_merge_cast(table_quoted, key, &cols, &casts, sub.len())?,
                params: sub
                    .iter()
                    .flat_map(|r| auto_row_params_typed(r, &cols, &binary))
                    .collect(),
            });
        }
    }
    Ok(out)
}

/// Each key tuple's values in `key` order, row-major, typed per column.
fn key_params(keys: &[KeyTuple], binary: &[bool]) -> Vec<BoundParam> {
    keys.iter()
        .flat_map(|kt| {
            kt.0.iter().enumerate().map(|(i, (_, v))| {
                BoundParam::for_column(v, binary.get(i).copied().unwrap_or(false))
            })
        })
        .collect()
}

/// The `MERGE … WHEN MATCHED THEN DELETE` statements for `deletes`.
pub(crate) fn delete_statements(
    table_quoted: &str,
    key: &[String],
    infos: &[ColumnInfo],
    deletes: &[KeyTuple],
) -> Result<Vec<Statement>, FaucetError> {
    let casts = casts_for(infos, key);
    let binary = binary_flags(infos, key);
    deletes
        .chunks(max_rows_per_insert(key.len()))
        .map(|chunk| {
            Ok(Statement {
                sql: build_merge_delete_cast(table_quoted, key, &casts, chunk.len())?,
                params: key_params(chunk, &binary),
            })
        })
        .collect()
}

/// Validate a scoped cleanup against the target's live columns and return the
/// key columns' `CAST` targets and binary flags. Every scope and key column
/// must exist: they are written in destination terms, so a missing one is a
/// configuration error worth naming.
pub(crate) fn cleanup_key_typing<'a>(
    live: &[ColumnInfo],
    scope_cols: impl IntoIterator<Item = &'a String>,
    key: &'a [String],
    table: &str,
) -> Result<(Vec<Option<String>>, Vec<bool>), FaucetError> {
    if live.is_empty() {
        return Err(FaucetError::Sink(format!(
            "cleanup: MSSQL table '{table}' has no columns or does not exist"
        )));
    }
    for col in scope_cols.into_iter().chain(key.iter()) {
        if !live.iter().any(|c| &c.name == col) {
            return Err(FaucetError::Sink(format!(
                "cleanup: column '{col}' does not exist on {table} — the completeness \
                 claim and `key` are in destination column terms"
            )));
        }
    }
    Ok((casts_for(live, key), binary_flags(live, key)))
}

/// The statements that load the written keys into the cleanup `#temp` table.
pub(crate) fn key_load_statements(
    key: &[String],
    casts: &[Option<String>],
    binary: &[bool],
    keys: &[KeyTuple],
) -> Result<Vec<Statement>, FaucetError> {
    keys.chunks(max_rows_per_insert(key.len()))
        .map(|chunk| {
            Ok(Statement {
                sql: build_cleanup_key_insert_sql(key, casts, chunk.len())?,
                params: key_params(chunk, binary),
            })
        })
        .collect()
}

/// Run `fut` under the statement timeout. Returns its result and whether it
/// timed out: a timed-out statement was dropped mid-TDS, so its connection is
/// desynced and must be replaced, never rolled back.
pub(crate) async fn with_timeout<T, E, F>(
    timeout: Option<Duration>,
    fut: F,
    timed_out: impl FnOnce() -> E,
) -> (Result<T, E>, bool)
where
    F: Future<Output = Result<T, E>>,
{
    match timeout {
        Some(t) => match tokio::time::timeout(t, fut).await {
            Ok(inner) => (inner, false),
            Err(_) => (Err(timed_out()), true),
        },
        None => (fut.await, false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn info(name: &str, ty: &str, identity: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            type_name: ty.into(),
            max_length: if ty == "nvarchar" { 100 } else { 8 },
            precision: 0,
            scale: 0,
            collation: None,
            is_nullable: true,
            is_identity: identity,
            is_computed: false,
        }
    }

    #[test]
    fn overwrite_skips_computed_and_rowversion_and_keeps_identity() {
        let mut computed = info("TOTAL", "int", false);
        computed.is_computed = true;
        let cols = vec![
            info("ID", "int", true),
            info("NAME", "nvarchar", false),
            computed,
            info("RV", "timestamp", false),
        ];
        assert_eq!(insertable(&cols), vec!["NAME".to_string()]);
        let swap = overwrite_swap_sql("[t]", "[s]", &cols).unwrap();
        assert_eq!(
            swap,
            vec![
                "DELETE FROM [t]".to_string(),
                "SET IDENTITY_INSERT [t] ON".to_string(),
                "INSERT INTO [t] ([ID], [NAME]) SELECT [ID], [NAME] FROM [s] WHERE [ID] IS NOT NULL"
                    .to_string(),
                "SET IDENTITY_INSERT [t] OFF".to_string(),
                "INSERT INTO [t] ([NAME]) SELECT [NAME] FROM [s] WHERE [ID] IS NULL".to_string(),
            ]
        );
        let no_id = overwrite_swap_sql("[t]", "[s]", &cols[1..]).unwrap();
        assert_eq!(no_id[1], "INSERT INTO [t] ([NAME]) SELECT [NAME] FROM [s]");
        assert_eq!(
            overwrite_swap_sql("[t]", "[s]", &cols[..1]).unwrap().len(),
            4
        );
        assert_eq!(
            staging_relax_sql("[s]", &cols).unwrap().as_deref(),
            Some("ALTER TABLE [s] ALTER COLUMN [ID] int NULL")
        );
        assert_eq!(staging_relax_sql("[s]", &cols[1..]).unwrap(), None);
        let odd = vec![info("ID", "geography", true)];
        assert!(staging_relax_sql("[s]", &odd).is_err());
        assert_eq!(
            staging_create_sql("[s]", "[t]", &cols).unwrap(),
            "SELECT [ID], [NAME] INTO [s] FROM (SELECT [ID], [NAME] FROM [t] WHERE 1 = 0 \
             UNION ALL SELECT [ID], [NAME] FROM [t] WHERE 1 = 0) AS t"
        );
    }

    fn table() -> Vec<ColumnInfo> {
        vec![
            info("rid", "bigint", true),
            info("id", "int", false),
            info("a", "nvarchar", false),
            info("blob", "varbinary", false),
        ]
    }

    fn kt(v: Value) -> KeyTuple {
        KeyTuple(vec![("id".to_string(), v)])
    }

    #[test]
    fn writable_requires_a_non_identity_column() {
        assert_eq!(writable(table(), "t").unwrap().len(), 4);
        let err = writable(vec![info("rid", "bigint", true)], "dbo.t").unwrap_err();
        assert!(err.to_string().contains("'dbo.t' has no writable columns"));
        assert!(writable(Vec::new(), "t").is_err());
        assert_eq!(insertable(&table()), vec!["id", "a", "blob"]);
    }

    #[test]
    fn poison_check_refuses_once_poisoned() {
        assert!(poison_check(false).is_ok());
        assert!(
            poison_check(true)
                .unwrap_err()
                .to_string()
                .contains("timed out")
        );
    }

    #[test]
    fn prepare_rows_maps_json_and_auto_columns() {
        let json_mode = MssqlColumnMapping::JsonColumn {
            column: "doc".into(),
        };
        let p = prepare_rows(&json_mode, &[], &[json!({"x": 1})])
            .unwrap()
            .unwrap();
        assert_eq!(p.cols, vec!["doc"]);
        assert!(p.casts.is_empty());
        assert!(matches!(&p.rows[0][0], BoundParam::Str(s) if s == r#"{"x":1}"#));

        let auto = MssqlColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Warn,
        };
        let p = prepare_rows(&auto, &table(), &[json!({"id": 1, "blob": null, "zz": 2})])
            .unwrap()
            .unwrap();
        assert_eq!(p.cols, vec!["id", "blob"]);
        assert_eq!(p.casts, vec![Some("int".to_string()), None]);
        assert!(matches!(p.rows[0][1], BoundParam::NullBinary(None)));
        assert!(
            prepare_rows(&auto, &table(), &[json!({"zz": 1})])
                .unwrap()
                .is_none()
        );
        let strict = MssqlColumnMapping::AutoColumns {
            on_unknown_field: OnUnknownField::Error,
        };
        assert!(prepare_rows(&strict, &table(), &[json!({"zz": 1})]).is_err());
    }

    #[test]
    fn insert_statements_split_under_the_parameter_ceiling() {
        let cols: Vec<String> = (0..300).map(|i| format!("[c{i}]")).collect();
        let per = max_rows_per_insert(cols.len());
        let stmts = insert_statements("[t]", &cols, &[], per + 1);
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[0].1, 0..per);
        assert_eq!(stmts[1].1, per..per + 1);
        assert!(stmts[0].0.starts_with("INSERT INTO [t]"));
        assert!(insert_statements("[t]", &cols, &[], 0).is_empty());
    }

    #[test]
    fn merge_statements_group_rows_by_carried_columns() {
        let key = vec!["id".to_string()];
        let stmts = merge_statements(
            "[t]",
            &key,
            &table(),
            OnUnknownField::Warn,
            &[
                json!({"id": 1, "a": "x"}),
                json!({"id": 2}),
                json!({"id": 3, "a": "y"}),
            ],
        )
        .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].sql.contains("UPDATE SET tgt.[a] = src.[a]"));
        assert_eq!(stmts[0].params.len(), 4);
        assert!(!stmts[1].sql.contains("UPDATE SET"), "{}", stmts[1].sql);
        assert_eq!(stmts[1].params.len(), 1);
        let none =
            merge_statements("[t]", &key, &table(), OnUnknownField::Warn, &[json!(1)]).unwrap();
        assert!(none.is_empty());
        assert!(
            merge_statements(
                "[t]",
                &key,
                &table(),
                OnUnknownField::Error,
                &[json!({"q": 1})]
            )
            .is_err()
        );
    }

    #[test]
    fn delete_and_key_load_statements_bind_keys_row_major() {
        let key = vec!["id".to_string()];
        let keys = vec![kt(json!(1)), kt(json!(2))];
        let del = delete_statements("[t]", &key, &table(), &keys).unwrap();
        assert_eq!(del.len(), 1);
        assert!(del[0].sql.contains("WHEN MATCHED THEN DELETE"));
        assert_eq!(del[0].params.len(), 2);

        let (casts, binary) = cleanup_key_typing(&table(), [&"a".to_string()], &key, "t").unwrap();
        assert_eq!(casts, vec![Some("int".to_string())]);
        assert_eq!(binary, vec![false]);
        let load = key_load_statements(&key, &casts, &binary, &keys).unwrap();
        assert_eq!(load.len(), 1);
        assert_eq!(load[0].params.len(), 2);
        assert!(
            key_load_statements(&key, &casts, &binary, &[])
                .unwrap()
                .is_empty()
        );
        assert_eq!(key_params(&keys, &[]).len(), 2);
    }

    #[test]
    fn cleanup_key_typing_names_a_missing_table_or_column() {
        let key = vec!["id".to_string()];
        let none: [&String; 0] = [];
        let err = cleanup_key_typing(&[], none, &key, "dbo.t").unwrap_err();
        assert!(err.to_string().contains("'dbo.t' has no columns"));
        let err = cleanup_key_typing(&table(), [&"nope".to_string()], &key, "dbo.t").unwrap_err();
        assert!(
            err.to_string()
                .contains("column 'nope' does not exist on dbo.t")
        );
    }

    #[tokio::test]
    async fn with_timeout_reports_which_side_fired() {
        let ok = with_timeout(None, async { Ok::<_, FaucetError>(1) }, || {
            FaucetError::Sink("late".into())
        })
        .await;
        assert_eq!((ok.0.unwrap(), ok.1), (1, false));
        let fast = with_timeout(
            Some(Duration::from_secs(5)),
            async { Err::<u8, _>(FaucetError::Sink("boom".into())) },
            || FaucetError::Sink("late".into()),
        )
        .await;
        assert!(!fast.1 && fast.0.unwrap_err().to_string().contains("boom"));
        let slow = with_timeout(
            Some(Duration::from_millis(10)),
            async {
                tokio::time::sleep(Duration::from_secs(5)).await;
                Ok::<u8, FaucetError>(0)
            },
            || FaucetError::Sink("late".into()),
        )
        .await;
        assert!(slow.1 && slow.0.unwrap_err().to_string().contains("late"));
    }
}

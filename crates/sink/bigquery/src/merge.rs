//! Schema-driven SQL generation for the BigQuery upsert/delete write path (#224).
//!
//! All functions are **pure** (no I/O): given the target table's schema as
//! [`crate::idempotent::FieldSpec`]s and the `key` columns, they generate the
//! `MERGE` / `DELETE` / transaction SQL that merges a page in place via
//! `MERGE … USING (SELECT … FROM UNNEST(JSON_QUERY_ARRAY(@payload)))`. No
//! staging table. See `docs/superpowers/specs/2026-06-13-bigquery-upsert-design.md`.

use crate::idempotent::{
    FieldSpec, build_merge_token, column_expr, json_path_segment, quote_ident, table_ref,
};
use faucet_core::FaucetError;

/// Find the `FieldSpec` for a key column by name.
fn key_field<'a>(columns: &'a [FieldSpec], name: &str) -> Option<&'a FieldSpec> {
    columns.iter().find(|f| f.name == name)
}

/// The alias of the presence flag for the `i`-th column of the MERGE source.
fn presence_alias(i: usize) -> String {
    format!("`__faucet_has_{i}`")
}

/// The MERGE source: the typed columns plus, for every non-key column, whether
/// the record carries that key at all. `JSON_QUERY` returns SQL `NULL` only for
/// a missing key (an explicit JSON `null` comes back as the text `null`), so
/// the flag tells "absent" from "set to null".
fn build_merge_source(columns: &[FieldSpec], key: &[String], payload_param: &str) -> String {
    let mut exprs: Vec<String> = columns
        .iter()
        .map(|f| {
            let path = format!("${}", json_path_segment(&f.name));
            format!(
                "{} AS {}",
                column_expr(f, "r", &path, 0),
                quote_ident(&f.name)
            )
        })
        .collect();
    for (i, f) in columns.iter().enumerate() {
        if key.iter().any(|k| k == &f.name) {
            continue;
        }
        let path = format!("${}", json_path_segment(&f.name));
        exprs.push(format!(
            "JSON_QUERY(r, '{path}') IS NOT NULL AS {}",
            presence_alias(i)
        ));
    }
    format!(
        "SELECT {} FROM UNNEST(JSON_QUERY_ARRAY({payload_param})) AS r",
        exprs.join(", ")
    )
}

/// The in-place upsert `MERGE` over the `@payload` page.
///
/// A matched row keeps its current value for every column the record does not
/// carry: assigning every table column nulled enrichment columns, columns a
/// transform dropped, and anything a partial (CDC) image left out (SQL-11).
pub(crate) fn build_merge_upsert(
    columns: &[FieldSpec],
    key: &[String],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let src = build_merge_source(columns, key, "@payload");
    let on = key
        .iter()
        .map(|k| format!("T.{q} = S.{q}", q = quote_ident(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let insert_cols = columns
        .iter()
        .map(|f| quote_ident(&f.name))
        .collect::<Vec<_>>()
        .join(", ");
    let insert_vals = columns
        .iter()
        .map(|f| format!("S.{}", quote_ident(&f.name)))
        .collect::<Vec<_>>()
        .join(", ");
    let non_key_sets = columns
        .iter()
        .enumerate()
        .filter(|(_, f)| !key.iter().any(|k| k == &f.name))
        .map(|(i, f)| {
            format!(
                "{q} = IF(S.{p}, S.{q}, T.{q})",
                q = quote_ident(&f.name),
                p = presence_alias(i)
            )
        })
        .collect::<Vec<_>>();
    let matched = if non_key_sets.is_empty() {
        String::new()
    } else {
        format!("WHEN MATCHED THEN UPDATE SET {} ", non_key_sets.join(", "))
    };
    format!(
        "MERGE INTO {t} T USING ({src}) S ON {on} {matched}WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})",
        t = table_ref(project, dataset, table),
    )
}

/// Keyed `DELETE` via a semi-join over the `@deletes` payload (an array of
/// `{key_col: value, …}` objects). Key columns are typed via `column_expr` so
/// an INT64 key compares as INT64, not STRING.
pub(crate) fn build_delete_by_keys(
    columns: &[FieldSpec],
    key: &[String],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let preds = key
        .iter()
        .map(|k| {
            let path = format!("${}", json_path_segment(k));
            // Callers run `validate_keys_present` before building any SQL, so
            // every key is guaranteed to be a real column here.
            let fs = key_field(columns, k).unwrap_or_else(|| {
                unreachable!("validate_keys_present runs before build_delete_by_keys")
            });
            let rhs = column_expr(fs, "d", &path, 0);
            format!("T.{} = {}", quote_ident(k), rhs)
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "DELETE FROM {t} T WHERE EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@deletes)) AS d WHERE {preds})",
        t = table_ref(project, dataset, table),
    )
}

fn wrap_transaction(stmts: &[String]) -> String {
    format!(
        "BEGIN TRANSACTION;\n{};\nCOMMIT TRANSACTION;",
        stmts.join(";\n")
    )
}

/// `BEGIN; [MERGE upsert]; [DELETE]; COMMIT;` — the at-least-once upsert/delete path.
pub(crate) fn build_upsert_transaction_sql(
    columns: &[FieldSpec],
    key: &[String],
    has_upserts: bool,
    has_deletes: bool,
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let mut stmts = Vec::new();
    if has_upserts {
        stmts.push(build_merge_upsert(columns, key, project, dataset, table));
    }
    if has_deletes {
        stmts.push(build_delete_by_keys(columns, key, project, dataset, table));
    }
    wrap_transaction(&stmts)
}

/// As [`build_upsert_transaction_sql`] but with the watermark `MERGE` appended
/// inside the same transaction — the exactly-once + upsert composition.
pub(crate) fn build_upsert_idempotent_sql(
    columns: &[FieldSpec],
    key: &[String],
    has_upserts: bool,
    has_deletes: bool,
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    let mut stmts = Vec::new();
    if has_upserts {
        stmts.push(build_merge_upsert(columns, key, project, dataset, table));
    }
    if has_deletes {
        stmts.push(build_delete_by_keys(columns, key, project, dataset, table));
    }
    stmts.push(build_merge_token(project, dataset));
    wrap_transaction(&stmts)
}

/// Validate that every `key` column exists in the target table schema. Returns
/// a `FaucetError::Sink` naming the missing column otherwise.
pub(crate) fn validate_keys_present(
    columns: &[FieldSpec],
    key: &[String],
) -> Result<(), FaucetError> {
    for k in key {
        if !columns.iter().any(|f| &f.name == k) {
            return Err(FaucetError::Sink(format!(
                "bigquery upsert: key column '{k}' is not a column of the target table"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Scoped cleanup (issue #478)
// ---------------------------------------------------------------------------

/// BigQuery rejects a `jobs.query` request whose body exceeds 10 MB. The
/// written-key payload is by far the largest part of a cleanup request, so it
/// is budgeted a little under the hard limit to leave room for the SQL text,
/// the `@scope` parameter, and the JSON envelope.
const CLEANUP_KEYS_PAYLOAD_BUDGET: usize = 9 * 1024 * 1024;

/// Refuse a cleanup whose written-key payload would blow the `jobs.query`
/// request limit.
///
/// Failing here — before any statement runs — is the whole point: the `DELETE`
/// removes everything in the scope that is *not* in `@keys`, so a truncated key
/// list would delete rows the run actually wrote. There is no safe partial
/// form of this request, so it must not be attempted at all.
pub(crate) fn check_cleanup_payload_size(bytes: usize, keys: usize) -> Result<(), FaucetError> {
    if bytes > CLEANUP_KEYS_PAYLOAD_BUDGET {
        return Err(FaucetError::Sink(format!(
            "bigquery cleanup: the {keys} key(s) written by this run serialize to {bytes} bytes, \
             above the {CLEANUP_KEYS_PAYLOAD_BUDGET}-byte budget for a jobs.query request \
             (BigQuery's limit is 10 MB). Nothing was deleted — sending a truncated key list \
             would delete rows this run actually wrote. Narrow the completeness claim \
             (`complete_for`) so each invocation covers fewer rows"
        )));
    }
    Ok(())
}

/// Validate the cleanup `scope` and `key` columns against the target table.
///
/// Both are written in **destination** column terms, so a name that is not a
/// real column is a config error worth naming rather than a mid-`DELETE`
/// BigQuery error. Both are also compared with `=`, so a `REPEATED` or
/// `STRUCT` column is rejected: array equality is not a BigQuery operator, and
/// a struct comparison would silently mean something other than the user
/// intended.
pub(crate) fn validate_cleanup_columns(
    columns: &[FieldSpec],
    scope: &[String],
    key: &[String],
) -> Result<(), FaucetError> {
    for (role, cols) in [("scope", scope), ("key", key)] {
        for c in cols {
            let Some(f) = key_field(columns, c) else {
                return Err(FaucetError::Sink(format!(
                    "bigquery cleanup: {role} column '{c}' is not a column of the target table — \
                     the completeness claim and `key` are in destination column terms"
                )));
            };
            if f.repeated || f.ty == crate::idempotent::BqType::Struct {
                return Err(FaucetError::Sink(format!(
                    "bigquery cleanup: {role} column '{c}' is a repeated/struct column and cannot \
                     be matched with an equality predicate"
                )));
            }
        }
    }
    Ok(())
}

/// The scoped-cleanup `DELETE`: remove every row matching `scope` whose key is
/// **not** among the keys this run wrote (#478).
///
/// Both payloads ride in as a single bound JSON STRING parameter each —
/// `@scope` (one object) and `@keys` (an array of `{key_col: value, …}`
/// objects) — exactly like the typed `INSERT … SELECT FROM
/// UNNEST(JSON_QUERY_ARRAY(@payload))` path. One parameter per written key
/// would blow BigQuery's parameter limits on any realistic scope.
///
/// Every extracted value is typed with the same `column_expr` the write path
/// uses, so an INT64 key compares as INT64 rather than STRING.
///
/// An **empty** `@keys` array is meaningful, not a no-op: `NOT EXISTS` over an
/// empty `UNNEST` is true for every row, so the whole scope is deleted. That is
/// the case this feature exists for — a source that reported its scope empty.
///
/// A single `DELETE` statement is atomic in BigQuery, which is what makes this
/// all-or-nothing: a partial delete would remove rows the run actually wrote.
pub(crate) fn build_cleanup_delete(
    columns: &[FieldSpec],
    scope: &[String],
    key: &[String],
    project: &str,
    dataset: &str,
    table: &str,
) -> String {
    // Callers run `validate_cleanup_columns` before building any SQL, so every
    // scope/key name is guaranteed to be a real scalar column here.
    let typed = |col: &str, var: &str| {
        let path = format!("${}", json_path_segment(col));
        let fs = key_field(columns, col).unwrap_or_else(|| {
            unreachable!("validate_cleanup_columns runs before build_cleanup_delete")
        });
        column_expr(fs, var, &path, 0)
    };

    let scope_pred = scope
        .iter()
        .map(|c| format!("T.{} = {}", quote_ident(c), typed(c, "@scope")))
        .collect::<Vec<_>>()
        .join(" AND ");
    // A destination row carrying NULL in a key column can never have been
    // written by a keyed upsert, so `T.key = k.key` is NULL, `NOT EXISTS`
    // holds, and the row is deleted — which is the correct reading of "in the
    // scope and not written by this run".
    let key_pred = key
        .iter()
        .map(|k| format!("T.{} = {}", quote_ident(k), typed(k, "k")))
        .collect::<Vec<_>>()
        .join(" AND ");
    format!(
        "DELETE FROM {t} T WHERE {scope_pred} AND NOT EXISTS \
         (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@keys)) AS k WHERE {key_pred})",
        t = table_ref(project, dataset, table),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::idempotent::{BqType, FieldSpec};

    fn scalar(name: &str, ty: BqType) -> FieldSpec {
        FieldSpec {
            name: name.into(),
            ty,
            repeated: false,
            fields: vec![],
        }
    }

    fn id_name_cols() -> Vec<FieldSpec> {
        vec![scalar("id", BqType::Int64), scalar("name", BqType::String)]
    }

    #[test]
    fn merge_source_types_each_column_and_flags_non_key_presence() {
        let sql = build_merge_source(&id_name_cols(), &["id".into()], "@payload");
        assert_eq!(
            sql,
            "SELECT CAST(JSON_VALUE(r, '$.id') AS INT64) AS `id`, JSON_VALUE(r, '$.name') AS `name`, JSON_QUERY(r, '$.name') IS NOT NULL AS `__faucet_has_1` FROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r"
        );
    }

    #[test]
    fn merge_upsert_single_key() {
        let sql = build_merge_upsert(&id_name_cols(), &["id".into()], "p", "d", "t");
        assert_eq!(
            sql,
            "MERGE INTO `p.d.t` T USING (SELECT CAST(JSON_VALUE(r, '$.id') AS INT64) AS `id`, JSON_VALUE(r, '$.name') AS `name`, JSON_QUERY(r, '$.name') IS NOT NULL AS `__faucet_has_1` FROM UNNEST(JSON_QUERY_ARRAY(@payload)) AS r) S ON T.`id` = S.`id` WHEN MATCHED THEN UPDATE SET `name` = IF(S.`__faucet_has_1`, S.`name`, T.`name`) WHEN NOT MATCHED THEN INSERT (`id`, `name`) VALUES (S.`id`, S.`name`)"
        );
    }

    #[test]
    fn merge_upsert_composite_key() {
        let cols = vec![
            scalar("tenant", BqType::String),
            scalar("id", BqType::Int64),
            scalar("name", BqType::String),
        ];
        let sql = build_merge_upsert(&cols, &["tenant".into(), "id".into()], "p", "d", "t");
        assert!(
            sql.contains("ON T.`tenant` = S.`tenant` AND T.`id` = S.`id`"),
            "got: {sql}"
        );
        assert!(
            sql.contains(
                "WHEN MATCHED THEN UPDATE SET `name` = IF(S.`__faucet_has_2`, S.`name`, T.`name`)"
            ),
            "got: {sql}"
        );
        assert!(
            sql.contains("INSERT (`tenant`, `id`, `name`) VALUES (S.`tenant`, S.`id`, S.`name`)"),
            "got: {sql}"
        );
    }

    #[test]
    fn merge_upsert_all_columns_are_key_omits_update() {
        // Only a key column: no non-key columns to SET, so emit INSERT-only.
        let sql = build_merge_upsert(
            &[scalar("id", BqType::Int64)],
            &["id".into()],
            "p",
            "d",
            "t",
        );
        assert!(!sql.contains("WHEN MATCHED"), "got: {sql}");
        assert!(
            !sql.contains("__faucet_has"),
            "keys need no presence flag: {sql}"
        );
        assert!(
            sql.contains("ON T.`id` = S.`id` WHEN NOT MATCHED THEN INSERT (`id`) VALUES (S.`id`)"),
            "got: {sql}"
        );
    }

    #[test]
    fn delete_by_keys_typed_single_key() {
        let sql = build_delete_by_keys(&id_name_cols(), &["id".into()], "p", "d", "t");
        assert_eq!(
            sql,
            "DELETE FROM `p.d.t` T WHERE EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@deletes)) AS d WHERE T.`id` = CAST(JSON_VALUE(d, '$.id') AS INT64))"
        );
    }

    #[test]
    fn delete_by_keys_composite() {
        let cols = vec![
            scalar("tenant", BqType::String),
            scalar("id", BqType::Int64),
        ];
        let sql = build_delete_by_keys(&cols, &["tenant".into(), "id".into()], "p", "d", "t");
        assert!(
            sql.contains("WHERE T.`tenant` = JSON_VALUE(d, '$.tenant') AND T.`id` = CAST(JSON_VALUE(d, '$.id') AS INT64)"),
            "got: {sql}"
        );
    }

    #[test]
    fn transaction_upserts_and_deletes() {
        let sql = build_upsert_transaction_sql(
            &id_name_cols(),
            &["id".into()],
            true,
            true,
            "p",
            "d",
            "t",
        );
        assert!(sql.starts_with("BEGIN TRANSACTION;\n"), "got: {sql}");
        assert!(
            sql.trim_end().ends_with("COMMIT TRANSACTION;"),
            "got: {sql}"
        );
        let m = sql.find("MERGE INTO").unwrap();
        let d = sql.find("DELETE FROM").unwrap();
        let c = sql.find("COMMIT TRANSACTION").unwrap();
        assert!(m < d && d < c, "order wrong: {sql}");
        assert!(
            !sql.contains("_faucet_commit_token"),
            "no watermark in non-EO path: {sql}"
        );
    }

    #[test]
    fn transaction_upserts_only() {
        let sql = build_upsert_transaction_sql(
            &id_name_cols(),
            &["id".into()],
            true,
            false,
            "p",
            "d",
            "t",
        );
        assert!(sql.contains("MERGE INTO"), "got: {sql}");
        assert!(!sql.contains("DELETE FROM"), "got: {sql}");
    }

    #[test]
    fn idempotent_transaction_appends_watermark_merge() {
        let sql =
            build_upsert_idempotent_sql(&id_name_cols(), &["id".into()], true, true, "p", "d", "t");
        let m = sql.find("MERGE INTO `p.d.t`").unwrap();
        let d = sql.find("DELETE FROM").unwrap();
        let w = sql.find("MERGE `p.d._faucet_commit_token`").unwrap();
        let c = sql.find("COMMIT TRANSACTION").unwrap();
        assert!(m < d && d < w && w < c, "order wrong: {sql}");
    }

    #[test]
    fn validate_keys_present_ok_and_err() {
        assert!(validate_keys_present(&id_name_cols(), &["id".into()]).is_ok());
        let err = validate_keys_present(&id_name_cols(), &["missing".into()]).unwrap_err();
        assert!(format!("{err}").contains("missing"), "got: {err}");
    }

    // --- scoped cleanup (issue #478) ---

    fn cleanup_cols() -> Vec<FieldSpec> {
        vec![
            scalar("id", BqType::Int64),
            scalar("contact_id", BqType::Int64),
            scalar("name", BqType::String),
        ]
    }

    #[test]
    fn cleanup_delete_single_scope_and_key() {
        let sql = build_cleanup_delete(
            &cleanup_cols(),
            &["contact_id".into()],
            &["id".into()],
            "p",
            "d",
            "t",
        );
        assert_eq!(
            sql,
            "DELETE FROM `p.d.t` T WHERE T.`contact_id` = CAST(JSON_VALUE(@scope, '$.contact_id') AS INT64) \
             AND NOT EXISTS (SELECT 1 FROM UNNEST(JSON_QUERY_ARRAY(@keys)) AS k \
             WHERE T.`id` = CAST(JSON_VALUE(k, '$.id') AS INT64))"
        );
    }

    #[test]
    fn cleanup_delete_binds_exactly_two_named_params() {
        // One parameter per written key would blow BigQuery's parameter limits;
        // the whole key set must ride in a single JSON STRING param.
        let sql = build_cleanup_delete(
            &cleanup_cols(),
            &["contact_id".into()],
            &["id".into()],
            "p",
            "d",
            "t",
        );
        assert_eq!(sql.matches('@').count(), 2, "got: {sql}");
        assert!(
            sql.contains("@scope") && sql.contains("@keys"),
            "got: {sql}"
        );
    }

    #[test]
    fn cleanup_delete_composite_scope_and_key_ands_predicates() {
        let cols = vec![
            scalar("tenant", BqType::String),
            scalar("region", BqType::String),
            scalar("id", BqType::Int64),
            scalar("part", BqType::String),
        ];
        let sql = build_cleanup_delete(
            &cols,
            &["tenant".into(), "region".into()],
            &["id".into(), "part".into()],
            "p",
            "d",
            "t",
        );
        assert!(
            sql.contains(
                "WHERE T.`tenant` = JSON_VALUE(@scope, '$.tenant') \
                 AND T.`region` = JSON_VALUE(@scope, '$.region') AND NOT EXISTS"
            ),
            "got: {sql}"
        );
        assert!(
            sql.contains(
                "WHERE T.`id` = CAST(JSON_VALUE(k, '$.id') AS INT64) \
                 AND T.`part` = JSON_VALUE(k, '$.part'))"
            ),
            "got: {sql}"
        );
    }

    #[test]
    fn cleanup_delete_types_the_key_like_the_write_path() {
        // Regression: an INT64 key must compare as INT64, not as the STRING
        // JSON_VALUE returns — otherwise every row looks unwritten and the
        // whole scope is deleted.
        let sql = build_cleanup_delete(
            &cleanup_cols(),
            &["name".into()],
            &["id".into()],
            "p",
            "d",
            "t",
        );
        assert!(
            sql.contains("CAST(JSON_VALUE(k, '$.id') AS INT64)"),
            "{sql}"
        );
        // A STRING scope column needs no cast.
        assert!(
            sql.contains("T.`name` = JSON_VALUE(@scope, '$.name')"),
            "{sql}"
        );
    }

    #[test]
    fn cleanup_delete_bracket_quotes_awkward_column_names() {
        let cols = vec![scalar("a.b", BqType::String), scalar("id", BqType::Int64)];
        let sql = build_cleanup_delete(&cols, &["a.b".into()], &["id".into()], "p", "d", "t");
        // `$.a.b` would be an ambiguous path, so the segment is bracket-quoted
        // and the quotes are backslash-escaped inside the SQL string literal.
        assert!(
            sql.contains(r"T.`a.b` = JSON_VALUE(@scope, '$[\'a.b\']')"),
            "{sql}"
        );
    }

    #[test]
    fn validate_cleanup_columns_names_the_missing_column_and_its_role() {
        let cols = cleanup_cols();
        assert!(validate_cleanup_columns(&cols, &["contact_id".into()], &["id".into()]).is_ok());

        let err = validate_cleanup_columns(&cols, &["nope".into()], &["id".into()]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("scope column 'nope'"), "{msg}");
        assert!(msg.contains("destination column terms"), "{msg}");

        let err =
            validate_cleanup_columns(&cols, &["contact_id".into()], &["nope".into()]).unwrap_err();
        assert!(err.to_string().contains("key column 'nope'"), "{err}");
    }

    #[test]
    fn validate_cleanup_columns_rejects_repeated_and_struct_columns() {
        let cols = vec![
            FieldSpec {
                name: "tags".into(),
                ty: BqType::String,
                repeated: true,
                fields: vec![],
            },
            FieldSpec {
                name: "addr".into(),
                ty: BqType::Struct,
                repeated: false,
                fields: vec![scalar("city", BqType::String)],
            },
            scalar("id", BqType::Int64),
        ];
        let err = validate_cleanup_columns(&cols, &["tags".into()], &["id".into()]).unwrap_err();
        assert!(err.to_string().contains("repeated/struct"), "{err}");
        let err = validate_cleanup_columns(&cols, &["addr".into()], &["id".into()]).unwrap_err();
        assert!(err.to_string().contains("repeated/struct"), "{err}");
        // …and on the key side too.
        let err = validate_cleanup_columns(&cols, &["id".into()], &["tags".into()]).unwrap_err();
        assert!(err.to_string().contains("key column 'tags'"), "{err}");
    }

    #[test]
    fn cleanup_payload_size_guard() {
        assert!(check_cleanup_payload_size(1_024, 10).is_ok());
        assert!(check_cleanup_payload_size(CLEANUP_KEYS_PAYLOAD_BUDGET, 1).is_ok());
        let err = check_cleanup_payload_size(CLEANUP_KEYS_PAYLOAD_BUDGET + 1, 500_000)
            .expect_err("over budget must be refused");
        let msg = err.to_string();
        assert!(msg.contains("500000 key(s)"), "{msg}");
        assert!(msg.contains("Nothing was deleted"), "{msg}");
        assert!(msg.contains("complete_for"), "{msg}");
    }
}

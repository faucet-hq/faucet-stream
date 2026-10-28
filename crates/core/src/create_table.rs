//! Shared auto-create-table planning for table-based sinks (#580).
//!
//! A first-ever sync cannot assume the destination table exists. The BigQuery
//! sink solved this in #578 by inferring a schema from the first written page
//! and creating the table; every other table destination has the same problem,
//! and before this module each one answered it differently (or not at all) —
//! different field names, different defaults, some with no create path at all.
//!
//! This module holds the **dialect-neutral half**: turn a page of records into
//! an ordered, deduplicated column plan. Each sink supplies only the mapping
//! from [`SqlBaseType`] to its own type keyword — the same mapping its
//! `evolve_schema` already needs, so the two agree by construction rather than
//! by inspection.
//!
//! ## Why column order is sorted, not source order
//!
//! Source order would read better in a `SELECT *`, but it is not recoverable:
//! `serde_json::Map` is a `BTreeMap` or an `IndexMap` depending on whether the
//! `preserve_order` feature is unified into the build, so "the order the
//! record listed its keys" is a *build-time* property. A created table's
//! column order would then differ between a `-p crate` build and an
//! `--all-features` one — the kind of difference that only shows up as a
//! diffed schema in production. Sorted is worse to read and always the same.

use crate::drift::{SqlBaseType, json_schema_base_type};
use serde_json::Value;

/// One column of a planned `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedColumn {
    /// Column name, exactly as it appeared in the record. Quoting is the
    /// sink's job (each dialect quotes differently).
    pub name: String,
    /// Dialect-neutral type, to be mapped by the sink.
    pub base_type: SqlBaseType,
    /// Whether any observed value for this column was null, or the column was
    /// absent from at least one record. Planned columns are always created
    /// nullable in practice — see [`plan_columns`] — but the flag is kept so a
    /// sink that wants to emit `NOT NULL` for a provably-present column can.
    pub nullable: bool,
    /// The type the source declared for this column (#955), when it fits every
    /// value of the planned page. A dialect that maps it creates that type;
    /// `base_type` is the fallback.
    pub declared: Option<crate::declared::LogicalType>,
}

/// Plan the columns for a table created from `page`.
///
/// Returns `None` when the page carries nothing to infer from (empty, or no
/// record is a JSON object) — the caller must then leave the table
/// uncreated rather than emit a zero-column `CREATE TABLE`, and try again on
/// the next page.
///
/// **Every column is planned nullable.** A column that happened to be present
/// and non-null in the first page is not thereby required forever, and a
/// `NOT NULL` inferred from one page turns the *second* page into a hard write
/// failure the moment a real-world record omits the field. Narrowing later is
/// the schema-drift policy's job (#194), which can see more than one page.
pub fn plan_columns(page: &[Value]) -> Option<Vec<PlannedColumn>> {
    let schema = crate::schema::infer_schema(page);
    let props = schema.get("properties")?.as_object()?;
    if props.is_empty() {
        return None;
    }

    // Sorted, for the determinism reason in the module docs. `infer_schema`
    // already unions every record's keys, so a column that appears only in a
    // later record is still planned.
    let mut ordered: Vec<String> = props.keys().cloned().collect();
    ordered.sort_unstable();

    let columns: Vec<PlannedColumn> = ordered
        .into_iter()
        .map(|name| {
            let fragment = &props[&name];
            // A column whose every observed value was null carries no type
            // information; TEXT is the one choice that can hold whatever
            // shows up next without a lossy cast.
            let mut base_type = json_schema_base_type(fragment).unwrap_or(SqlBaseType::Text);
            // An integer above i64::MAX fits no BIGINT/INTEGER column (SQLite
            // would silently store it as a rounded REAL); TEXT keeps its digits.
            if base_type == SqlBaseType::Integer && holds_wide_integer(page, &name) {
                base_type = SqlBaseType::Text;
            }
            PlannedColumn {
                base_type,
                nullable: true,
                name,
                declared: None,
            }
        })
        .collect();

    (!columns.is_empty()).then_some(columns)
}

/// Whether any record holds an integer above `i64::MAX` under `column`.
fn holds_wide_integer(page: &[Value], column: &str) -> bool {
    page.iter().any(|r| {
        r.get(column)
            .and_then(Value::as_number)
            .is_some_and(|n| n.is_u64() && n.as_i64().is_none())
    })
}

/// Render a `CREATE TABLE` column list, given a dialect's type mapper and
/// identifier quoter.
///
/// Kept separate from [`plan_columns`] so a sink whose `CREATE TABLE` needs
/// extra clauses (a primary key, a partition spec, an engine) composes this
/// into its own statement rather than fighting a one-size template.
pub fn render_columns<Q, T>(columns: &[PlannedColumn], quote: Q, ty: T) -> String
where
    Q: Fn(&str) -> String,
    T: Fn(SqlBaseType) -> &'static str,
{
    columns
        .iter()
        .map(|c| format!("{} {}", quote(&c.name), ty(c.base_type)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Plan the columns for a table whose writes dedup on `key` (#676).
///
/// Like [`plan_columns`], plus: every key column is planned even when the
/// first page never carries it (typed TEXT, the type that holds anything), and
/// key columns are marked non-nullable. A created table can then carry a
/// primary key on `key` — which is what gives an upsert's `ON CONFLICT` /
/// `MERGE` a target on the very first run.
pub fn plan_keyed_columns(page: &[Value], key: &[String]) -> Option<Vec<PlannedColumn>> {
    let mut columns = plan_columns(page)?;
    for k in key {
        match columns.iter_mut().find(|c| &c.name == k) {
            Some(c) => c.nullable = false,
            None => columns.push(PlannedColumn {
                name: k.clone(),
                base_type: SqlBaseType::Text,
                nullable: false,
                declared: None,
            }),
        }
    }
    Some(columns)
}

/// Plan the columns for a table created from `page` and a source's
/// `declared` columns (#955), following the rules of [`crate::declared`].
///
/// Without a declaration this is [`plan_columns`]. With one, a page column
/// whose declared type accepts every value of the page takes that type (and a
/// declared column is ordered as the source declared it, the rest sorted after
/// it); a page with nothing to infer from plans every declared column when the
/// declaration is complete.
pub fn plan_columns_declared(
    page: &[Value],
    declared: Option<&crate::declared::DeclaredColumns>,
) -> Option<Vec<PlannedColumn>> {
    let Some(declared) = declared.filter(|d| !d.is_empty()) else {
        return plan_columns(page);
    };
    let Some(mut columns) = plan_columns(page) else {
        if !declared.is_complete() {
            return None;
        }
        return Some(
            declared
                .columns()
                .iter()
                .map(|d| PlannedColumn {
                    name: d.name.clone(),
                    base_type: d.logical_type.base_type(),
                    nullable: true,
                    declared: Some(d.logical_type),
                })
                .collect(),
        );
    };
    for column in &mut columns {
        let Some(d) = declared.get(&column.name) else {
            continue;
        };
        let fits = page.iter().all(|r| {
            r.get(&column.name)
                .is_none_or(|v| d.logical_type.accepts(v))
        });
        if fits {
            column.base_type = d.logical_type.base_type();
            column.declared = Some(d.logical_type);
        } else {
            tracing::warn!(
                column = %column.name,
                declared = ?d.logical_type,
                "the first page holds values the declared column type does not accept; \
                 creating the column with its inferred type"
            );
        }
    }
    let rank = |name: &str| {
        declared
            .columns()
            .iter()
            .position(|d| d.name == name)
            .unwrap_or(usize::MAX)
    };
    columns.sort_by(|a, b| rank(&a.name).cmp(&rank(&b.name)).then(a.name.cmp(&b.name)));
    Some(columns)
}

/// [`plan_columns_declared`] for a table whose writes dedup on `key`: every
/// key column is planned (typed as declared, else TEXT) and non-nullable.
pub fn plan_keyed_columns_declared(
    page: &[Value],
    key: &[String],
    declared: Option<&crate::declared::DeclaredColumns>,
) -> Option<Vec<PlannedColumn>> {
    let mut columns = plan_columns_declared(page, declared)?;
    for k in key {
        match columns.iter_mut().find(|c| &c.name == k) {
            Some(c) => c.nullable = false,
            None => {
                let d = declared.and_then(|d| d.get(k)).map(|d| d.logical_type);
                columns.push(PlannedColumn {
                    name: k.clone(),
                    base_type: d.map_or(SqlBaseType::Text, |t| t.base_type()),
                    nullable: false,
                    declared: d,
                })
            }
        }
    }
    Some(columns)
}

/// [`plan_columns_declared`], or [`plan_keyed_columns_declared`] when `key`
/// is non-empty.
pub fn plan_table_declared(
    page: &[Value],
    key: &[String],
    declared: Option<&crate::declared::DeclaredColumns>,
) -> Option<Vec<PlannedColumn>> {
    if key.is_empty() {
        plan_columns_declared(page, declared)
    } else {
        plan_keyed_columns_declared(page, key, declared)
    }
}

/// Render a column list where the type keyword may depend on the column, not
/// just its base type — for dialects that cannot index an unbounded type and so
/// need a bounded one for key columns (MySQL `TEXT`, SQL Server `NVARCHAR(MAX)`).
pub fn render_column_defs<Q, T>(columns: &[PlannedColumn], quote: Q, ty: T) -> String
where
    Q: Fn(&str) -> String,
    T: Fn(&PlannedColumn) -> &'static str,
{
    columns
        .iter()
        .map(|c| format!("{} {}", quote(&c.name), ty(c)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `PRIMARY KEY (<key…>)` for a created table, or `None` for an empty key.
pub fn render_primary_key<Q>(key: &[String], quote: Q) -> Option<String>
where
    Q: Fn(&str) -> String,
{
    (!key.is_empty()).then(|| {
        format!(
            "PRIMARY KEY ({})",
            key.iter().map(|k| quote(k)).collect::<Vec<_>>().join(", ")
        )
    })
}

/// The uniform error a sink raises when its target is missing and
/// `create_table: false` (#580).
///
/// One wording across every sink, because the fix is always the same two
/// choices and an operator hitting it on connector number three should not
/// have to re-read a different sentence.
pub fn missing_target_error(connector: &str, target: &str) -> crate::error::FaucetError {
    crate::error::FaucetError::Sink(format!(
        "{connector}: target `{target}` does not exist and `create_table: false`. \
         Create it first, or set `create_table: true` to have faucet create it from \
         the first page's inferred schema."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn plans_every_column_with_its_inferred_type() {
        let page = vec![
            json!({ "id": 1, "name": "a", "amount": 1.5, "ok": true }),
            json!({ "id": 2, "name": "b", "amount": 2.5, "ok": false }),
        ];
        let cols = plan_columns(&page).expect("a plan");
        let by_name: std::collections::HashMap<&str, SqlBaseType> = cols
            .iter()
            .map(|c| (c.name.as_str(), c.base_type))
            .collect();
        assert_eq!(by_name["id"], SqlBaseType::Integer);
        assert_eq!(by_name["name"], SqlBaseType::Text);
        assert_eq!(by_name["amount"], SqlBaseType::Double);
        assert_eq!(by_name["ok"], SqlBaseType::Boolean);
    }

    #[test]
    fn column_order_is_deterministic_regardless_of_record_key_order() {
        // `serde_json::Map` is a BTreeMap or an IndexMap depending on feature
        // unification, so a plan that read order off the record would produce
        // a different table under `-p crate` than under `--all-features`.
        let a = plan_columns(&[json!({ "z": 1, "a": 2, "m": 3 })]).expect("a");
        let b = plan_columns(&[json!({ "a": 2, "m": 3, "z": 1 })]).expect("b");
        assert_eq!(a, b, "the same record set must plan the same table");
        assert_eq!(
            a.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["a", "m", "z"]
        );
    }

    #[test]
    fn a_column_appearing_only_in_a_later_record_is_still_planned() {
        // A sparse first record must not silently drop a column — the next
        // page would then fail to write it, on a table faucet itself created.
        let page = vec![json!({ "id": 1 }), json!({ "id": 2, "note": "hi" })];
        let cols = plan_columns(&page).expect("a plan");
        assert_eq!(
            cols.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            vec!["id", "note"]
        );
    }

    #[test]
    fn an_integer_column_above_i64_plans_as_text() {
        let cols = plan_columns(&[
            json!({"big": 1, "small": 1}),
            json!({"big": 18446744073709551615u64, "small": 2}),
        ])
        .unwrap();
        let ty = |n: &str| cols.iter().find(|c| c.name == n).unwrap().base_type;
        assert_eq!(ty("big"), SqlBaseType::Text);
        assert_eq!(ty("small"), SqlBaseType::Integer);
    }

    #[test]
    fn every_planned_column_is_nullable() {
        // Inferring NOT NULL from one page turns the second page into a hard
        // write failure the first time a record omits the field.
        let page = vec![json!({ "id": 1, "name": "a" })];
        let cols = plan_columns(&page).expect("a plan");
        assert!(cols.iter().all(|c| c.nullable), "{cols:?}");
    }

    #[test]
    fn nested_values_plan_as_json() {
        let page = vec![json!({ "obj": {"a": 1}, "arr": [1, 2] })];
        let cols = plan_columns(&page).expect("a plan");
        assert!(
            cols.iter().all(|c| c.base_type == SqlBaseType::Json),
            "{cols:?}"
        );
    }

    #[test]
    fn an_all_null_column_falls_back_to_text() {
        // No type information at all; TEXT is the only choice that can hold
        // whatever turns up next without a lossy cast.
        let page = vec![json!({ "id": 1, "maybe": Value::Null })];
        let cols = plan_columns(&page).expect("a plan");
        let maybe = cols.iter().find(|c| c.name == "maybe").expect("column");
        assert_eq!(maybe.base_type, SqlBaseType::Text);
    }

    #[test]
    fn an_empty_or_non_object_page_plans_nothing() {
        // A zero-column CREATE TABLE is invalid in every dialect, so the sink
        // must wait for a page it can actually learn from.
        assert!(plan_columns(&[]).is_none());
        assert!(plan_columns(&[json!(1), json!("x")]).is_none());
        assert!(plan_columns(&[json!({})]).is_none());
    }

    #[test]
    fn mixed_int_and_float_widens_to_double() {
        // Creating an INTEGER column and then writing 1.5 into it is exactly
        // the silent-truncation class this inference exists to avoid.
        let page = vec![json!({ "n": 1 }), json!({ "n": 1.5 })];
        let cols = plan_columns(&page).expect("a plan");
        assert_eq!(cols[0].base_type, SqlBaseType::Double);
    }

    #[test]
    fn render_columns_uses_the_dialects_quoting_and_types() {
        let cols = plan_columns(&[json!({ "id": 1, "name": "a" })]).expect("a plan");
        let sql = render_columns(
            &cols,
            |n| format!("\"{}\"", n.replace('"', "\"\"")),
            |t| match t {
                SqlBaseType::Integer => "BIGINT",
                SqlBaseType::Double => "DOUBLE PRECISION",
                SqlBaseType::Boolean => "BOOLEAN",
                SqlBaseType::Text => "TEXT",
                SqlBaseType::Json => "JSONB",
            },
        );
        assert_eq!(sql, "\"id\" BIGINT, \"name\" TEXT");
    }

    #[test]
    fn the_missing_target_error_names_both_ways_out() {
        let e = missing_target_error("postgres sink", "public.orders");
        let msg = e.to_string();
        assert!(msg.contains("public.orders"), "{msg}");
        assert!(
            msg.contains("create_table: true"),
            "must name the fix: {msg}"
        );
        assert!(
            matches!(e, crate::error::FaucetError::Sink(_)),
            "a missing destination is a sink failure, not a config one — the config \
             was legal, the destination was not there"
        );
    }

    #[test]
    fn render_columns_quotes_a_hostile_identifier() {
        // The quoter is the sink's, but the renderer must actually route every
        // name through it — a name reaching the SQL unquoted is an injection.
        let cols = plan_columns(&[json!({ "we\"ird": 1 })]).expect("a plan");
        let sql = render_columns(
            &cols,
            |n| format!("\"{}\"", n.replace('"', "\"\"")),
            |_| "TEXT",
        );
        assert_eq!(sql, "\"we\"\"ird\" TEXT");
    }

    #[test]
    fn keyed_plan_marks_key_columns_required_and_adds_missing_ones() {
        let page = vec![json!({ "id": 1, "name": "a" })];
        let cols = plan_keyed_columns(&page, &["id".into(), "tenant".into()]).expect("a plan");
        let id = cols.iter().find(|c| c.name == "id").unwrap();
        assert!(!id.nullable);
        assert_eq!(id.base_type, SqlBaseType::Integer);
        let tenant = cols.iter().find(|c| c.name == "tenant").unwrap();
        assert!(!tenant.nullable);
        assert_eq!(tenant.base_type, SqlBaseType::Text);
        assert!(cols.iter().find(|c| c.name == "name").unwrap().nullable);
    }

    #[test]
    fn keyed_plan_is_none_for_an_empty_page() {
        assert!(plan_keyed_columns(&[], &["id".into()]).is_none());
    }

    #[test]
    fn column_defs_can_type_by_column() {
        let cols = vec![
            PlannedColumn {
                name: "id".into(),
                base_type: SqlBaseType::Text,
                nullable: false,
                declared: None,
            },
            PlannedColumn {
                name: "note".into(),
                base_type: SqlBaseType::Text,
                nullable: true,
                declared: None,
            },
        ];
        let sql = render_column_defs(
            &cols,
            |n| format!("`{n}`"),
            |c| {
                if c.nullable { "TEXT" } else { "VARCHAR(255)" }
            },
        );
        assert_eq!(sql, "`id` VARCHAR(255), `note` TEXT");
    }

    #[test]
    fn primary_key_renders_every_key_column_in_order() {
        let q = |n: &str| format!("\"{n}\"");
        assert_eq!(
            render_primary_key(&["a".into(), "b".into()], q).as_deref(),
            Some("PRIMARY KEY (\"a\", \"b\")")
        );
        assert!(render_primary_key(&[], q).is_none());
    }

    mod declared {
        use super::*;
        use crate::declared::{DeclaredColumn, DeclaredColumns, LogicalType};

        fn decl() -> DeclaredColumns {
            DeclaredColumns::new([
                DeclaredColumn::new("id", LogicalType::Int64, false),
                DeclaredColumn::new(
                    "amount",
                    LogicalType::Decimal {
                        precision: Some(12),
                        scale: Some(2),
                    },
                    true,
                ),
                DeclaredColumn::new("day", LogicalType::Date, true),
                DeclaredColumn::new("at", LogicalType::Timestamp { tz: true }, true),
            ])
        }

        #[test]
        fn without_a_declaration_it_is_plan_columns() {
            let page = [json!({"b": 1, "a": "x"})];
            assert_eq!(plan_columns_declared(&page, None), plan_columns(&page));
            let empty = DeclaredColumns::new([]);
            assert_eq!(
                plan_columns_declared(&page, Some(&empty)),
                plan_columns(&page)
            );
        }

        #[test]
        fn a_fitting_page_takes_the_declared_types_in_source_order() {
            let page = [
                json!({"zz": "extra", "at": "2024-02-22T02:00:00+00:00", "day": "2024-10-27", "amount": "1.50", "id": 1}),
                json!({"zz": null, "at": null, "day": null, "amount": null, "id": 2}),
            ];
            let cols = plan_columns_declared(&page, Some(&decl())).expect("plan");
            let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
            assert_eq!(names, ["id", "amount", "day", "at", "zz"]);
            assert_eq!(
                cols[1].declared,
                Some(LogicalType::Decimal {
                    precision: Some(12),
                    scale: Some(2)
                })
            );
            assert_eq!(cols[1].base_type, SqlBaseType::Text);
            assert_eq!(cols[3].declared, Some(LogicalType::Timestamp { tz: true }));
            assert_eq!(cols[0].base_type, SqlBaseType::Integer);
            assert_eq!(cols[4].declared, None);
            assert!(cols.iter().all(|c| c.nullable));
        }

        #[test]
        fn a_value_the_declared_type_refuses_keeps_the_inferred_type() {
            let page = [json!({"id": 1, "day": "***", "amount": "2.00"})];
            let cols = plan_columns_declared(&page, Some(&decl())).expect("plan");
            let day = cols.iter().find(|c| c.name == "day").unwrap();
            assert_eq!(day.declared, None);
            assert_eq!(day.base_type, SqlBaseType::Text);
            assert!(
                cols.iter()
                    .find(|c| c.name == "amount")
                    .unwrap()
                    .declared
                    .is_some()
            );
            assert!(
                cols.iter().all(|c| c.name != "at"),
                "a column the page lacks is not created"
            );
        }

        #[test]
        fn an_empty_page_creates_a_complete_declaration_only() {
            let cols = plan_columns_declared(&[], Some(&decl())).expect("plan");
            assert_eq!(cols.len(), 4);
            assert!(cols.iter().all(|c| c.declared.is_some() && c.nullable));
            assert_eq!(cols[2].base_type, SqlBaseType::Text);
            assert!(plan_columns_declared(&[], Some(&decl().reshaped())).is_none());
            assert!(plan_columns_declared(&[], None).is_none());
        }

        #[test]
        fn keyed_plans_type_and_require_the_key() {
            let cols =
                plan_keyed_columns_declared(&[], &["id".into()], Some(&decl())).expect("plan");
            let id = cols.iter().find(|c| c.name == "id").unwrap();
            assert!(!id.nullable);
            assert_eq!(id.declared, Some(LogicalType::Int64));
            let page = [json!({"amount": "1.00"})];
            let cols =
                plan_keyed_columns_declared(&page, &["id".into(), "other".into()], Some(&decl()))
                    .expect("plan");
            let id = cols.iter().find(|c| c.name == "id").unwrap();
            assert_eq!(
                (id.base_type, id.declared, id.nullable),
                (SqlBaseType::Integer, Some(LogicalType::Int64), false)
            );
            let other = cols.iter().find(|c| c.name == "other").unwrap();
            assert_eq!((other.base_type, other.declared), (SqlBaseType::Text, None));
            assert_eq!(
                plan_table_declared(&page, &[], Some(&decl())),
                plan_columns_declared(&page, Some(&decl()))
            );
            assert_eq!(
                plan_table_declared(&page, &["id".into()], Some(&decl())),
                plan_keyed_columns_declared(&page, &["id".into()], Some(&decl()))
            );
        }
    }
}

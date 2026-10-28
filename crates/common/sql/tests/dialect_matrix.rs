//! The shared planning matrix (#884): every planning case runs against every
//! dialect, so a rule fixed for one backend is checked for all of them.

use faucet_common_sql::dialects::{
    BigQuery, ClickHouse, Databricks, Duckdb, Mssql, Mysql, Oracle, Postgres, Redshift, Snowflake,
    Sqlite,
};
use faucet_common_sql::{
    ColumnMatching, EngineOptions, EvolutionStep, SqlDialect, SqlExecutor, SqlSinkEngine,
    Statement, StatementKind, TableColumn, TableTarget,
};
use faucet_core::write_mode::{WriteMode, WriteSpec};
use faucet_core::{FaucetError, SchemaEvolution, SeenKeys, SqlBaseType, WritePlan};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// One recorded statement: kind, SQL and the bound values.
type Recorded = (StatementKind, String, Vec<Value>);

/// An executor that records every statement instead of running it.
struct Recorder {
    columns: Vec<TableColumn>,
    statements: Vec<Recorded>,
    journaled: Vec<usize>,
    /// Key rows a `CleanupJournal` read returns, as JSON text.
    absent: Vec<String>,
    /// The key tuples each journal call recorded.
    journaled_keys: Vec<Vec<Value>>,
    /// Catalog reads: `columns` and `key_types` calls.
    catalog_reads: usize,
}

impl Recorder {
    fn new(columns: &[(&str, &str)]) -> Self {
        Self {
            columns: columns
                .iter()
                .map(|(n, t)| TableColumn::new(*n, *t))
                .collect(),
            statements: Vec::new(),
            journaled: Vec::new(),
            absent: Vec::new(),
            journaled_keys: Vec::new(),
            catalog_reads: 0,
        }
    }

    fn kinds(&self) -> Vec<StatementKind> {
        self.statements.iter().map(|(k, _, _)| *k).collect()
    }
}

#[faucet_core::async_trait]
impl SqlExecutor for Recorder {
    async fn columns(&mut self, _target: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        self.catalog_reads += 1;
        Ok(self.columns.clone())
    }

    async fn key_types(
        &mut self,
        _target: &TableTarget,
        key: &[String],
    ) -> Result<Vec<Option<String>>, FaucetError> {
        self.catalog_reads += 1;
        Ok(key
            .iter()
            .map(|k| {
                self.columns
                    .iter()
                    .find(|c| &c.name == k)
                    .and_then(|c| c.sql_type().map(str::to_string))
            })
            .collect())
    }

    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        self.statements.push((
            stmt.kind,
            stmt.sql.clone(),
            stmt.params
                .iter()
                .map(|p| p.value.as_ref().clone())
                .collect(),
        ));
        Ok(1)
    }

    async fn fetch_text(&mut self, stmt: &Statement<'_>) -> Result<Vec<String>, FaucetError> {
        self.execute(stmt).await?;
        Ok(self.absent.clone())
    }

    async fn journal_plan(&mut self, plan: &WritePlan, _run_id: &str) -> Result<(), FaucetError> {
        self.journaled.push(plan.upserts.len() + plan.deletes.len());
        self.journaled_keys.extend(
            plan.deletes
                .iter()
                .map(|k| k.0.iter().map(|(_, v)| v.clone()).collect()),
        );
        Ok(())
    }
}

fn spec(mode: WriteMode, key: &[&str]) -> WriteSpec {
    WriteSpec {
        write_mode: mode,
        key: key.iter().map(|k| k.to_string()).collect(),
        ..Default::default()
    }
}

fn target<D: SqlDialect>(d: &D, name: &str) -> TableTarget {
    TableTarget::new(name, d.quote_ident(name))
}

/// Run `case` against every built-in dialect.
macro_rules! every_dialect {
    ($case:ident) => {
        every_keyed_dialect!($case);
        $case(Duckdb);
        $case(Redshift);
        $case(ClickHouse);
        $case(Snowflake);
        $case(Databricks);
        $case(BigQuery);
    };
}

/// Run `case` against every dialect whose sink upserts and deletes by key.
macro_rules! every_keyed_dialect {
    ($case:ident) => {
        every_cleanup_dialect!($case);
        $case(Oracle);
    };
}

/// Run `case` against every dialect whose sink runs scoped cleanup.
macro_rules! every_cleanup_dialect {
    ($case:ident) => {
        $case(Postgres);
        $case(Mysql);
        $case(Sqlite);
        $case(Mssql);
    };
}

fn quoting_escapes_the_quote_character<D: SqlDialect>(d: D) {
    let q = d.quote_ident("a\"b`c]d");
    assert!(q.len() > "a\"b`c]d".len(), "{}: {q}", d.name());
    let lit = d.quote_literal("o'k");
    assert!(
        lit.ends_with('\'') && (lit.contains("''") || lit.contains("\\'")),
        "{}: the quote is escaped: {lit}",
        d.name()
    );
    assert!(d.quote("plain").is_ok());
}

#[test]
fn matrix_quoting() {
    every_dialect!(quoting_escapes_the_quote_character);
    assert!(Mssql.quote("bad\0name").is_err());
    assert!(Oracle.quote("has\"quote").is_err());
    assert!(Oracle.quote("").is_err());
    assert!(Oracle.quote(&"x".repeat(129)).is_err());
    assert_eq!(Mysql.quote_literal("a\\b"), "'a\\\\b'");
    // Backslash-escaping dialects escape the escape first, so it cannot be forged.
    assert_eq!(ClickHouse.quote_ident("a\\\"b"), "\"a\\\\\\\"b\"");
    assert_eq!(BigQuery.quote_ident("a`b"), "`a\\`b`");
    assert_eq!(Databricks.quote_literal("a\\'b"), "'a\\\\\\'b'");
    assert_eq!(Snowflake.quote_literal("a\\'b"), "'a\\\\''b'");
}

fn mapping_follows_the_dialect_rule<D: SqlDialect>(d: D) {
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]));
    let columns = vec![TableColumn::untyped("UserId"), TableColumn::untyped("name")];
    let records = vec![json!({"userid": 1, "name": "a", "extra": true})];
    let t = target(engine.dialect(), "t");
    let mapped = engine.map_page(&t, &columns, &records);
    match engine.dialect().column_matching() {
        ColumnMatching::Exact | ColumnMatching::UpperFold => {
            let rows = mapped.expect("name matches exactly");
            assert_eq!(
                rows[0].cells.len(),
                1,
                "a mixed-case column is not folded to"
            );
        }
        _ => {
            let rows = mapped.expect("both fields match");
            assert_eq!(rows[0].cells.len(), 2, "{}", engine.dialect().name());
        }
    }
    let none = vec![json!({"zzz": 1})];
    let err = engine
        .map_page(&t, &columns, &none)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("record 0"),
        "{}: {err}",
        engine.dialect().name()
    );
    let not_object = vec![json!(3)];
    assert!(
        engine
            .map_page(&t, &columns, &not_object)
            .unwrap_err()
            .to_string()
            .contains("JSON object")
    );
}

#[test]
fn matrix_column_mapping() {
    every_dialect!(mapping_follows_the_dialect_rule);
}

fn duplicate_keys_coalesce_last_write_wins<D: SqlDialect>(d: D) {
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Upsert, &["id"]));
    let page = vec![
        json!({"id": 1, "v": "a"}),
        json!({"id": 1, "v": "b"}),
        json!({"id": 2, "v": "c"}),
    ];
    let plans = engine.plan_keyed(&page).unwrap();
    let upserts: Vec<&Value> = plans.iter().flat_map(|p| &p.upserts).collect();
    assert_eq!(upserts.len(), 2, "{}", engine.dialect().name());
    assert!(upserts.contains(&&json!({"id": 1, "v": "b"})));
    let report = engine.report(page.len(), 2, &plans);
    assert_eq!((report.accepted, report.coalesced), (2, 1));
    let err = engine
        .plan_keyed(&[json!({"v": 1})])
        .unwrap_err()
        .to_string();
    assert!(
        err.contains(&format!("{} upsert: row 0", engine.dialect().name())),
        "{err}"
    );
    let (_, failed) = engine.plan_keyed_partial(&[json!({"id": 1}), json!({"v": 1})]);
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].0, 1);
}

#[test]
fn matrix_dedup() {
    every_dialect!(duplicate_keys_coalesce_last_write_wins);
}

fn groups_run_in_page_order_with_journal_first<D: SqlDialect>(d: D) {
    let mut s = spec(WriteMode::Upsert, &["id"]);
    s.delete_marker = Some(faucet_core::DeleteMarker {
        field: "op".into(),
        values: vec!["d".into()],
    });
    s.rollback = Some(faucet_core::rollback::RollbackWriteSpec {
        run_id: "r1".into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous: false,
    });
    let name = d.name();
    let engine = SqlSinkEngine::new(d, s);
    let page = vec![json!({"id": 1, "op": "d"}), json!({"id": 1, "v": "new"})];
    let plans = engine.plan_keyed(&page).unwrap();
    let mut rec = Recorder::new(&[("id", "int"), ("v", "text")]);
    let t = target(engine.dialect(), "t");
    futures_lite_block_on(engine.apply_plans(&mut rec, &t, &plans)).unwrap();
    let kinds = rec.kinds();
    let delete = kinds.iter().position(|k| *k == StatementKind::Delete);
    let upsert = kinds.iter().position(|k| *k == StatementKind::Upsert);
    assert!(
        delete.is_some() && upsert.is_some() && delete < upsert,
        "{name}: the delete precedes the upsert of the same key: {kinds:?}"
    );
    assert_eq!(
        rec.journaled.len(),
        plans.len(),
        "{name}: one journal per group"
    );
}

#[test]
fn matrix_upsert_delete_ordering() {
    every_keyed_dialect!(groups_run_in_page_order_with_journal_first);
}

/// A page alternating upserts and deletes is one group per row; the table's
/// catalog is still read once per kind, not once per group.
fn alternating_groups_read_the_catalog_once<D: SqlDialect>(d: D) {
    let mut s = spec(WriteMode::Upsert, &["id"]);
    s.delete_marker = Some(faucet_core::DeleteMarker {
        field: "op".into(),
        values: vec!["d".into()],
    });
    let name = d.name();
    let engine = SqlSinkEngine::new(d, s);
    let page: Vec<Value> = (0..20)
        .map(|i| {
            if i % 2 == 0 {
                json!({"id": i, "v": "x"})
            } else {
                json!({"id": i, "op": "d"})
            }
        })
        .collect();
    let plans = engine.plan_keyed(&page).unwrap();
    assert_eq!(plans.len(), 20, "{name}");
    let mut rec = Recorder::new(&[("id", "int"), ("v", "text")]);
    let t = target(engine.dialect(), "t");
    let affected = futures_lite_block_on(engine.apply_plans(&mut rec, &t, &plans)).unwrap();
    assert_eq!(affected, 20, "{name}");
    assert_eq!(
        rec.catalog_reads, 2,
        "{name}: one columns + one key_types read"
    );
}

#[test]
fn matrix_alternating_groups_catalog_reads() {
    every_keyed_dialect!(alternating_groups_read_the_catalog_once);
}

fn statements_stay_under_the_parameter_limit<D: SqlDialect>(d: D) {
    let name = d.name();
    for width in [1usize, 2, 3, 7, 64, 500] {
        let rows = d.rows_per_statement(width);
        assert!(rows >= 1);
        assert!(
            rows.saturating_mul(width) <= d.param_limit().max(width),
            "{name}: {rows} rows of {width} exceed {}",
            d.param_limit()
        );
    }
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]));
    let columns = vec![TableColumn::new("a", "int"), TableColumn::new("b", "int")];
    let per = engine.dialect().rows_per_statement(2).min(100_000);
    let page: Vec<Value> = (0..per + 3).map(|i| json!({"a": i, "b": i})).collect();
    let t = target(engine.dialect(), "t");
    let (written, stmts) = engine
        .insert_statements(&t, &columns, &page, false)
        .unwrap();
    assert_eq!(written, page.len());
    let expected = page.len().div_ceil(engine.dialect().rows_per_statement(2));
    assert_eq!(stmts.len(), expected, "{name}");
    let params: usize = stmts.iter().map(|s| s.params.len()).sum();
    assert_eq!(params, page.len() * 2, "{name}: every cell bound once");
    assert!(
        stmts
            .iter()
            .all(|s| s.params.len() <= engine.dialect().param_limit()),
        "{name}"
    );
    let narrow: Vec<Value> = (0..3).map(|i| json!({"a": i})).collect();
    let (_, stmts) = engine
        .insert_statements(&t, &columns, &narrow, false)
        .unwrap();
    let params: usize = stmts.iter().map(|s| s.params.len()).sum();
    assert_eq!(params, 3, "{name}: only the carried column");
}

#[test]
fn matrix_chunking() {
    every_dialect!(statements_stay_under_the_parameter_limit);
}

fn append_unions_columns_and_upsert_groups_them<D: SqlDialect>(d: D) {
    let name = d.name();
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Upsert, &["id"]));
    let columns = vec![
        TableColumn::new("id", "int"),
        TableColumn::new("a", "int"),
        TableColumn::new("b", "int"),
    ];
    let page = vec![json!({"id": 1, "a": 1}), json!({"id": 2, "b": 2})];
    let t = target(engine.dialect(), "t");
    let (_, append) = engine
        .insert_statements(&t, &columns, &page, false)
        .unwrap();
    let bound: Vec<&Value> = append
        .iter()
        .flat_map(|s| s.params.iter().map(|p| p.value.as_ref()))
        .collect();
    assert_eq!(bound.len(), 6, "{name}: union of 3 columns × 2 rows");
    assert_eq!(
        bound[2],
        &Value::Null,
        "{name}: a missing column binds NULL"
    );
    let (_, upsert) = engine.insert_statements(&t, &columns, &page, true).unwrap();
    assert_eq!(upsert.len(), 2, "{name}: one statement per carried set");
    assert!(upsert.iter().all(|s| s.kind == StatementKind::Upsert));
    assert!(upsert.iter().all(|s| s.params.len() == 2));
}

#[test]
fn matrix_present_column_groups() {
    every_keyed_dialect!(append_unions_columns_and_upsert_groups_them);
}

/// Two writers upserting one new key must not both take the insert branch:
/// the statement is either an atomic insert-or-update, or a `MERGE` that
/// serializes the key (SQL Server's `HOLDLOCK`; Oracle's sink re-runs a
/// `MERGE` that lost the race with ORA-00001).
fn upsert_is_safe_under_concurrent_writers<D: SqlDialect>(d: D) {
    let key = vec!["id".to_string()];
    let cols = vec!["id".to_string(), "v".to_string()];
    let sql = d
        .upsert_sql(&d.quote("t").unwrap(), &key, &cols, &[None, None], 2)
        .unwrap();
    let name = d.name();
    let safe = if sql.starts_with("INSERT") {
        sql.contains("ON CONFLICT") || sql.contains("ON DUPLICATE KEY UPDATE")
    } else if name == "oracle" {
        sql.starts_with("MERGE INTO")
    } else {
        sql.starts_with("MERGE") && sql.contains("WITH (HOLDLOCK)")
    };
    assert!(safe, "{name}: {sql}");
}

#[test]
fn matrix_upsert_concurrency() {
    every_keyed_dialect!(upsert_is_safe_under_concurrent_writers);
}

#[test]
fn matrix_upsert_text() {
    let key = vec!["id".to_string()];
    let cols = vec!["id".to_string(), "v".to_string()];
    let types = [None, None];
    assert_eq!(
        Postgres
            .upsert_sql("\"t\"", &key, &cols, &types, 1)
            .unwrap(),
        "INSERT INTO \"t\" (\"id\", \"v\") VALUES ($1, $2) ON CONFLICT (\"id\") DO UPDATE SET \"v\" = EXCLUDED.\"v\""
    );
    assert_eq!(
        Sqlite.upsert_sql("`t`", &key, &cols, &types, 1).unwrap(),
        "INSERT INTO `t` (`id`, `v`) VALUES (?, ?) ON CONFLICT(`id`) DO UPDATE SET `v` = excluded.`v`"
    );
    assert_eq!(
        Mysql.upsert_sql("`t`", &key, &cols, &types, 1).unwrap(),
        "INSERT INTO `t` (`id`, `v`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)"
    );
    assert_eq!(
        Mssql.upsert_sql("[t]", &key, &cols, &types, 1).unwrap(),
        "MERGE [t] WITH (HOLDLOCK) AS tgt USING (VALUES (@P1, @P2)) AS src ([id], [v]) ON tgt.[id] = src.[id] \
         WHEN MATCHED THEN UPDATE SET tgt.[v] = src.[v] \
         WHEN NOT MATCHED THEN INSERT ([id], [v]) VALUES (src.[id], src.[v]);"
    );
    assert_eq!(
        Mssql
            .delete_keys_sql("[t]", &key, &[Some("int")], 2)
            .unwrap(),
        "MERGE [t] AS tgt USING (VALUES (CAST(@P1 AS int)), (CAST(@P2 AS int))) AS src ([id]) \
         ON tgt.[id] = src.[id] WHEN MATCHED THEN DELETE;"
    );
    assert_eq!(
        Postgres
            .delete_keys_sql("\"t\"", &key, &[Some("int4")], 2)
            .unwrap(),
        "DELETE FROM \"t\" WHERE (\"id\") IN (($1::int4), ($2::int4))"
    );
    // Every column a key: nothing to update, the row is left as it is.
    let all_key = ["id".to_string()];
    assert!(
        Postgres
            .upsert_tail(&key, &all_key)
            .unwrap()
            .ends_with("DO NOTHING")
    );
    assert!(
        Sqlite
            .upsert_tail(&key, &all_key)
            .unwrap()
            .ends_with("DO NOTHING")
    );
    assert!(
        Mysql
            .upsert_tail(&key, &all_key)
            .unwrap()
            .ends_with("`id` = `id`")
    );
    assert!(
        !Mssql
            .upsert_sql("[t]", &key, &all_key, &[None], 1)
            .unwrap()
            .contains("WHEN MATCHED")
    );
}

fn integer_to_fraction_widens_exactly<D: SqlDialect>(d: D) {
    let ty = d.widened_type_name(Some(SqlBaseType::Integer), SqlBaseType::Double);
    let name = d.name();
    match name {
        "sqlite" => assert_eq!(ty, "REAL", "dynamic typing: widening is a no-op"),
        // Delta widens only the narrow integer types, exactly; the sink
        // refuses a BIGINT column rather than round it.
        "databricks" => assert_eq!(ty, "DOUBLE"),
        _ => {
            let upper = ty.to_ascii_uppercase();
            assert!(
                upper.contains("NUMERIC")
                    || upper.contains("DECIMAL")
                    || upper.starts_with("NUMBER"),
                "{name}: an integer column must widen to an exact type, got {ty}"
            );
        }
    }
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]));
    let evolution = SchemaEvolution {
        additions: vec![faucet_core::ColumnChange {
            name: "new".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![faucet_core::ColumnChange {
            name: "n".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec!["r".into()],
    };
    let steps = engine.evolution_steps(&evolution, None);
    assert!(
        matches!(steps[0], EvolutionStep::Add { ref column, base: SqlBaseType::Text } if column == "new")
    );
    assert!(matches!(steps[1], EvolutionStep::Widen { type_name, .. } if type_name == ty));
    assert!(matches!(steps[2], EvolutionStep::Relax { .. }));
    let existing = ["new".to_string()].into_iter().collect();
    assert_eq!(engine.evolution_steps(&evolution, Some(&existing)).len(), 2);
}

#[test]
fn matrix_widening() {
    every_dialect!(integer_to_fraction_widens_exactly);
}

fn keyed_create_table_bounds_key_text<D: SqlDialect>(d: D) {
    let name = d.name();
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Upsert, &["sku"]));
    let t = target(engine.dialect(), "t");
    let sql = engine
        .create_table_sql(&t, &[json!({"sku": "a", "note": "x"})], None)
        .unwrap()
        .expect("a page to infer from");
    assert!(sql.contains("PRIMARY KEY ("), "{name}: {sql}");
    let key_ty = engine.dialect().key_type_name(SqlBaseType::Text);
    assert!(
        sql.contains(key_ty),
        "{name}: key column typed {key_ty}: {sql}"
    );
    assert!(
        engine
            .create_table_sql(&t, &[json!(1)], None)
            .unwrap()
            .is_none(),
        "{name}: nothing to infer"
    );
    let json_table = engine.create_table_sql(&t, &[], Some("doc")).unwrap();
    assert!(json_table.is_some_and(|s| s.contains(&engine.dialect().quote_ident("doc"))));
}

fn append_create_table_types_every_column<D: SqlDialect>(d: D) {
    let name = d.name();
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]));
    let t = target(engine.dialect(), "t");
    let sql = engine
        .create_table_sql(&t, &[json!({"n": 1, "s": "x", "f": 1.5, "b": true})], None)
        .unwrap()
        .expect("a page to infer from");
    assert!(!sql.contains("PRIMARY KEY"), "{name}: {sql}");
    for base in [
        SqlBaseType::Integer,
        SqlBaseType::Text,
        SqlBaseType::Double,
        SqlBaseType::Boolean,
    ] {
        let ty = engine.dialect().type_name(base);
        assert!(sql.contains(ty), "{name}: {ty} in {sql}");
    }
    let add = engine
        .dialect()
        .add_column_sql(&t, "extra", SqlBaseType::Json)
        .unwrap();
    assert!(
        add.contains(&engine.dialect().quote_ident("extra")),
        "{name}: {add}"
    );
    assert!(
        add.contains(engine.dialect().type_name(SqlBaseType::Json)),
        "{name}"
    );
}

#[test]
fn matrix_append_create_table() {
    every_dialect!(append_create_table_types_every_column);
}

#[test]
fn matrix_create_table() {
    every_keyed_dialect!(keyed_create_table_bounds_key_text);
}

fn cleanup_runs_create_load_delete<D: SqlDialect>(d: D) {
    let name = d.name();
    let has_drop = d.cleanup_drop_sql().is_some();
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Upsert, &["id"]));
    let mut rec = Recorder::new(&[("id", "int"), ("parent", "int")]);
    let t = target(engine.dialect(), "t");
    let mut seen = SeenKeys::new();
    seen.record_page(
        &[json!({"id": 0}), json!({"id": 1}), json!({"id": 2})],
        &["id".to_string()],
        100,
    );
    let scope: BTreeMap<String, Value> = [("parent".to_string(), json!(7))].into();
    futures_lite_block_on(engine.cleanup(&mut rec, &t, &scope, &seen)).unwrap();
    let kinds = rec.kinds();
    let core: Vec<StatementKind> = kinds
        .iter()
        .copied()
        .filter(|k| *k != StatementKind::CleanupDrop)
        .collect();
    assert_eq!(
        core,
        vec![
            StatementKind::CleanupCreate,
            StatementKind::CleanupLoad,
            StatementKind::CleanupDelete
        ],
        "{name}"
    );
    assert_eq!(
        kinds.len() - core.len(),
        if has_drop { 2 } else { 0 },
        "{name}: drops before and after"
    );
    let delete = &rec
        .statements
        .iter()
        .find(|s| s.0 == StatementKind::CleanupDelete)
        .unwrap();
    assert!(delete.1.contains("NOT EXISTS"), "{name}");
    assert_eq!(delete.2, vec![json!(7)]);

    let bad: BTreeMap<String, Value> = [("nope".to_string(), json!(1))].into();
    let err = futures_lite_block_on(engine.cleanup(&mut rec, &t, &bad, &seen))
        .unwrap_err()
        .to_string();
    assert!(err.contains("'nope' does not exist"), "{name}: {err}");
    let keyless = SqlSinkEngine::new(Postgres, spec(WriteMode::Upsert, &[]));
    assert!(keyless.check_cleanup_key().is_err());
}

#[test]
fn matrix_cleanup() {
    every_cleanup_dialect!(cleanup_runs_create_load_delete);
}

/// A rollback-enabled cleanup journals the rows it is about to delete, in
/// its transaction, before the delete — so a rollback restores them (#706).
/// A dialect that cannot journal refuses instead of deleting unjournaled.
fn journaling_cleanup_records_the_rows_it_deletes<D: SqlDialect>(d: D) {
    let name = d.name();
    let journals = d.journal_sql().is_some();
    let mut s = spec(WriteMode::Upsert, &["id", "kind"]);
    s.rollback = Some(faucet_core::rollback::RollbackWriteSpec {
        run_id: "r1".into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous: false,
    });
    let engine = SqlSinkEngine::new(d, s);
    let mut rec = Recorder::new(&[("id", "int"), ("kind", "text"), ("parent", "int")]);
    rec.absent = vec![
        r#"{"id": 5, "kind": "a"}"#.into(),
        r#"{"id": 12345678901234567890123, "kind": "b"}"#.into(),
    ];
    let t = target(engine.dialect(), "t");
    let mut seen = SeenKeys::new();
    seen.record_page(
        &[json!({"id": 1, "kind": "a"})],
        &["id".to_string(), "kind".to_string()],
        100,
    );
    let scope: BTreeMap<String, Value> = [("parent".to_string(), json!(7))].into();
    let result = futures_lite_block_on(engine.cleanup(&mut rec, &t, &scope, &seen));
    if !journals {
        let err = result.unwrap_err().to_string();
        assert!(err.contains("cannot be journaled"), "{name}: {err}");
        assert!(
            !rec.kinds().contains(&StatementKind::CleanupDelete),
            "{name}: nothing is deleted unjournaled"
        );
        return;
    }
    result.unwrap();
    let kinds = rec.kinds();
    let read = kinds
        .iter()
        .position(|k| *k == StatementKind::CleanupJournal)
        .unwrap_or_else(|| panic!("{name}: reads the doomed keys: {kinds:?}"));
    let delete = kinds
        .iter()
        .position(|k| *k == StatementKind::CleanupDelete)
        .unwrap();
    assert!(read < delete, "{name}: journals before the delete");
    let (_, sql, params) = &rec.statements[read];
    assert!(sql.starts_with("SELECT"), "{name}: {sql}");
    assert!(sql.contains("NOT EXISTS"), "{name}: {sql}");
    assert!(
        sql.contains("'id'") && sql.contains("'kind'"),
        "{name}: {sql}"
    );
    assert_eq!(params, &vec![json!(7)], "{name}: bound like the delete");
    assert_eq!(
        rec.journaled_keys,
        vec![
            vec![json!(5), json!("a")],
            vec![json!("12345678901234567890123"), json!("b")]
        ],
        "{name}: every doomed key is journaled, exact"
    );

    // Nothing to delete: no journal call.
    let mut empty = Recorder::new(&[("id", "int"), ("kind", "text"), ("parent", "int")]);
    futures_lite_block_on(engine.cleanup(&mut empty, &t, &scope, &seen)).unwrap();
    assert!(empty.journaled.is_empty(), "{name}");

    // An unreadable key row fails the cleanup before anything is deleted.
    let mut bad = Recorder::new(&[("id", "int"), ("kind", "text"), ("parent", "int")]);
    bad.absent = vec!["not json".into()];
    let err = futures_lite_block_on(engine.cleanup(&mut bad, &t, &scope, &seen))
        .unwrap_err()
        .to_string();
    assert!(err.contains("journal"), "{name}: {err}");
    assert!(
        !bad.kinds().contains(&StatementKind::CleanupDelete),
        "{name}"
    );
}

#[test]
fn matrix_journaling_cleanup() {
    every_cleanup_dialect!(journaling_cleanup_records_the_rows_it_deletes);
}

fn whole_destination_cleanup_has_no_scope_predicate<D: SqlDialect>(d: D) {
    let name = d.name();
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Upsert, &["id"]));
    let mut rec = Recorder::new(&[("id", "int")]);
    let t = target(engine.dialect(), "t");
    let mut seen = SeenKeys::new();
    seen.record_page(&[json!({"id": 1})], &["id".to_string()], 100);
    futures_lite_block_on(engine.cleanup(&mut rec, &t, &BTreeMap::new(), &seen)).unwrap();
    let delete = rec
        .statements
        .iter()
        .find(|s| s.0 == StatementKind::CleanupDelete)
        .unwrap();
    assert!(
        delete.1.contains("WHERE NOT EXISTS"),
        "{name}: {}",
        delete.1
    );
    assert!(
        !delete.1.contains(" AND NOT EXISTS"),
        "{name}: {}",
        delete.1
    );
    assert!(delete.2.is_empty(), "{name}: no scope parameters");
}

#[test]
fn matrix_whole_destination_cleanup() {
    every_cleanup_dialect!(whole_destination_cleanup_has_no_scope_predicate);
}

/// A rollback-enabled whole-destination cleanup (empty scope) journals with
/// the same unscoped predicate the delete uses.
fn journaling_whole_destination_cleanup<D: SqlDialect>(d: D) {
    let name = d.name();
    if d.journal_sql().is_none() {
        return;
    }
    let mut s = spec(WriteMode::Upsert, &["id"]);
    s.rollback = Some(faucet_core::rollback::RollbackWriteSpec {
        run_id: "r1".into(),
        run_id_column: "_faucet_run_id".into(),
        journal: true,
        keep_previous: false,
    });
    let engine = SqlSinkEngine::new(d, s);
    let mut rec = Recorder::new(&[("id", "int")]);
    rec.absent = vec![r#"{"id": 5}"#.into()];
    let t = target(engine.dialect(), "t");
    let mut seen = SeenKeys::new();
    seen.record_page(&[json!({"id": 1})], &["id".to_string()], 100);
    futures_lite_block_on(engine.cleanup(&mut rec, &t, &BTreeMap::new(), &seen)).unwrap();
    let (_, sql, params) = rec
        .statements
        .iter()
        .find(|s| s.0 == StatementKind::CleanupJournal)
        .unwrap_or_else(|| panic!("{name}: journals before the delete"));
    assert!(sql.contains("WHERE NOT EXISTS"), "{name}: {sql}");
    assert!(params.is_empty(), "{name}: no scope parameters");
    assert_eq!(rec.journaled_keys, vec![vec![json!(5)]], "{name}");
}

#[test]
fn matrix_journaling_whole_destination_cleanup() {
    every_cleanup_dialect!(journaling_whole_destination_cleanup);
}

fn token_and_journal_sql_use_the_dialect<D: SqlDialect>(d: D) {
    let name = d.name();
    assert!(
        d.commit_token_table_sql().contains("_faucet_commit_token"),
        "{name}"
    );
    assert!(
        d.commit_token_select_sql()
            .contains(&d.placeholder(1, None)),
        "{name}"
    );
    assert!(
        d.commit_token_delete_sql().starts_with("DELETE FROM"),
        "{name}"
    );
    let upsert = d.commit_token_upsert_sql();
    assert!(upsert.contains(&d.quote_ident("token")), "{name}: {upsert}");
    if let Some(j) = d.journal_sql() {
        assert!(j.create().contains("_faucet_run_journal"), "{name}");
        let insert = j.insert(2);
        let numbered = d.placeholder(1, None) != d.placeholder(2, None);
        assert!(
            if numbered {
                insert.contains(&d.placeholder(8, None))
            } else {
                insert.matches(&d.placeholder(1, None)).count() == 8
            },
            "{name}: eight placeholders in the dialect's style: {insert}"
        );
        assert!(j.select().contains("run_id ="), "{name}");
    } else {
        assert!(
            !["postgres", "mysql", "sqlite"].contains(&name),
            "{name}: the rollback-capable dialects journal"
        );
    }
}

#[test]
fn matrix_token_and_journal_sql() {
    every_dialect!(token_and_journal_sql_use_the_dialect);
}

fn token_write_binds_scope_then_token<D: SqlDialect>(d: D) {
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]));
    let mut rec = Recorder::new(&[]);
    futures_lite_block_on(engine.write_token(&mut rec, "scope", "tok")).unwrap();
    assert_eq!(rec.statements[0].0, StatementKind::TokenUpsert);
    assert_eq!(rec.statements[0].2, vec![json!("scope"), json!("tok")]);
}

#[test]
fn matrix_token_write() {
    every_dialect!(token_write_binds_scope_then_token);
}

fn json_mode_serializes_each_record<D: SqlDialect>(d: D) {
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]))
        .with_options(EngineOptions::new().warn_unmatched_fields(true));
    let mut rec = Recorder::new(&[]);
    let t = target(engine.dialect(), "t");
    let n =
        futures_lite_block_on(engine.insert_json(&mut rec, &t, "doc", &[json!({"a": 1})])).unwrap();
    assert_eq!(n, 1);
    assert_eq!(rec.statements[0].0, StatementKind::InsertJson);
    assert_eq!(rec.statements[0].2, vec![json!("{\"a\":1}")]);
    assert_eq!(
        futures_lite_block_on(engine.insert_json(&mut rec, &t, "doc", &[])).unwrap(),
        0
    );
}

#[test]
fn matrix_json_mode() {
    every_dialect!(json_mode_serializes_each_record);
}

fn executed_insert_reports_rows<D: SqlDialect>(d: D) {
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]))
        .with_options(EngineOptions::new().warn_unmatched_fields(true));
    let mut rec = Recorder::new(&[("a", "int")]);
    let t = target(engine.dialect(), "t");
    let written = [json!({"a": 1, "unknown": 2}), json!({"a": 2})];
    let n = futures_lite_block_on(engine.insert(&mut rec, &t, &written, false)).unwrap();
    assert_eq!(n, 2);
    assert_eq!(engine.report(2, n, &[]).accepted, 2);
    let page = [json!({"a": 1, "unknown": 2}), json!({"A": 3})];
    let split = engine.split_writable(&t, &page, &rec.columns, true, |_, _, v| {
        (v == &json!(3)).then(|| "three".to_string())
    });
    let failed = split.outcomes.iter().filter(|o| o.is_err()).count();
    match engine.dialect().column_matching() {
        ColumnMatching::Exact => assert_eq!(failed, 1, "`A` matches no column exactly"),
        _ => assert_eq!(failed, 1, "the validator refuses the folded `A`"),
    }
    let deletes = [faucet_core::KeyTuple(vec![("a".into(), json!(1))])];
    let keyed = SqlSinkEngine::new(Postgres, spec(WriteMode::Delete, &["a"]));
    assert_eq!(
        futures_lite_block_on(keyed.delete(&mut rec, &t, &deletes)).unwrap(),
        1
    );
    assert_eq!(
        futures_lite_block_on(keyed.delete(&mut rec, &t, &[])).unwrap(),
        0
    );
}

#[test]
fn matrix_insert_and_delete_execution() {
    every_dialect!(executed_insert_reports_rows);
}

fn evolution_ddl_is_either_planned_or_left_to_the_backend<D: SqlDialect>(d: D) {
    let name = d.name();
    let t = target(&d, "t");
    let alter = d.alter_column_type_sql(&t, "n", "NUMERIC").unwrap();
    let relax = d.drop_not_null_sql(&t, "n").unwrap();
    match name {
        "postgres" | "oracle" => {
            assert!(alter.is_some_and(|s| s.contains("NUMERIC")), "{name}");
            assert!(relax.is_some_and(|s| s.contains("NULL")), "{name}");
        }
        _ => {
            assert!(alter.is_none(), "{name}: needs the live column definition");
            assert!(relax.is_none(), "{name}");
        }
    }
    assert!(!d.cleanup_table_display(&t).is_empty());
    assert!(!d.cleanup_keys_table().is_empty());
}

#[test]
fn matrix_evolution_ddl() {
    every_dialect!(evolution_ddl_is_either_planned_or_left_to_the_backend);
}

#[test]
fn backend_specific_text() {
    let t = TableTarget::new("dbo.t", "[dbo].[t]");
    assert_eq!(
        Mssql.add_column_sql(&t, "c", SqlBaseType::Integer).unwrap(),
        "IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.t') \
         AND name = N'c') ALTER TABLE [dbo].[t] ADD [c] BIGINT"
    );
    assert_eq!(
        Mssql.create_json_table_sql(&t, "doc").unwrap(),
        "IF OBJECT_ID(N'dbo.t', N'U') IS NULL CREATE TABLE [dbo].[t] \
         (id BIGINT IDENTITY(1,1) PRIMARY KEY, [doc] NVARCHAR(MAX))"
    );
    assert_eq!(Mssql.cleanup_table_display(&t), "dbo.t");
    assert!(Mssql.cleanup_create_sql(&t, &[], &[]).is_err());
    assert_eq!(
        Mssql
            .cleanup_delete_sql(&t, &[], &[], &["k".into()])
            .unwrap(),
        "DELETE t FROM [dbo].[t] AS t WHERE NOT EXISTS (SELECT 1 FROM #faucet_cleanup_keys c \
         WHERE c.[k] = t.[k])",
        "an empty scope is the whole destination (a complete-read cleanup)"
    );
    assert!(
        Mssql
            .cleanup_delete_sql(&t, &["s".into()], &[], &[])
            .is_err()
    );
    assert_eq!(Mssql.rows_per_statement(0), 1);
    assert_eq!(Mssql.rows_per_statement(3), 699);
    assert_eq!(Mssql.quote_literal("o'k"), "N'o''k'");

    let o = TableTarget::new("T", "\"T\"");
    assert_eq!(
        Oracle
            .upsert_sql("\"T\"", &["ID".into()], &["ID".into(), "V".into()], &[], 1)
            .unwrap(),
        "MERGE INTO \"T\" t USING (SELECT :1 AS \"ID\", :2 AS \"V\" FROM DUAL) s ON \
         (t.\"ID\" = s.\"ID\") WHEN MATCHED THEN UPDATE SET t.\"V\" = s.\"V\" \
         WHEN NOT MATCHED THEN INSERT (\"ID\", \"V\") VALUES (s.\"ID\", s.\"V\")"
    );
    assert!(
        Oracle
            .upsert_sql("\"T\"", &["K".into()], &["V".into()], &[], 1)
            .is_err()
    );
    assert!(
        !Oracle
            .upsert_sql("\"T\"", &["ID".into()], &["ID".into()], &[], 1)
            .unwrap()
            .contains("WHEN MATCHED")
    );
    assert_eq!(
        Oracle
            .delete_keys_sql("\"T\"", &["A".into(), "B".into()], &[], 1)
            .unwrap(),
        "DELETE FROM \"T\" WHERE \"A\" = :1 AND \"B\" = :2"
    );
    assert!(
        Oracle
            .create_json_table_sql(&o, "ID")
            .unwrap()
            .contains("\"FAUCET_ID\"")
    );
    assert!(
        Oracle
            .add_column_sql(&o, "C", SqlBaseType::Text)
            .unwrap()
            .contains("-1430")
    );
    assert_eq!(Oracle.widened_type_name(None, SqlBaseType::Text), "CLOB");
    assert_eq!(Oracle.key_type_name(SqlBaseType::Integer), "NUMBER(38)");
    assert_eq!(faucet_common_sql::dialects::oracle::key_text_width(3), 500);
    assert!(faucet_common_sql::dialects::oracle::quote_ident(&"é".repeat(70)).is_err());

    assert_eq!(ClickHouse.placeholder(2, Some("Int64")), "{p2:Int64}");
    assert_eq!(
        Redshift.placeholder(3, Some("numeric(38,9)")),
        "$3::numeric(38,9)"
    );
    assert_eq!(Redshift.placeholder(3, None), "$3");
    assert_eq!(ClickHouse.placeholder(1, None), "{p1:String}");
    assert_eq!(BigQuery.placeholder(1, None), "@p1");
    assert_eq!(Databricks.placeholder(1, None), ":p1");
    assert_eq!(
        Databricks
            .create_table_sql(&TableTarget::new("t", "`t`"), &[], &[])
            .unwrap(),
        "CREATE TABLE IF NOT EXISTS `t` () USING DELTA"
    );
    assert_eq!(Snowflake.type_name(SqlBaseType::Integer), "STRING");
    assert_eq!(
        Duckdb.json_table_columns("doc").unwrap(),
        "\"doc\" TEXT NOT NULL"
    );
    assert_eq!(
        faucet_common_sql::dialects::duckdb::quote_table("a.b"),
        "\"a\".\"b\""
    );
    assert_eq!(
        faucet_common_sql::dialects::clickhouse::quote_table("a.b"),
        "\"a\".\"b\""
    );
    // A dialect without an upsert tail watermarks through a standard MERGE.
    assert!(Redshift.commit_token_upsert_sql().starts_with("MERGE INTO"));
    assert!(Postgres.commit_token_upsert_sql().contains("ON CONFLICT"));
    assert!(
        Redshift
            .commit_token_table_sql()
            .contains("VARCHAR(65535) NOT NULL")
    );
}

/// Drive a future to completion on the current thread.
fn futures_lite_block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("runtime")
        .block_on(f)
}

/// Concurrent overwrite runs of one table stage apart (#930): a run token
/// makes the staging name its own, a plain identifier each dialect accepts
/// and quotes without escaping, and two runs never share one.
fn overwrite_runs_stage_under_their_own_name<D: SqlDialect>(d: D) {
    use faucet_common_sql::SqlLayout;
    use faucet_core::idempotency::{abandoned_overwrite_staging, new_overwrite_run_token};
    let (a, b) = (new_overwrite_run_token(), new_overwrite_run_token());
    let untokened = SqlLayout::new("orders").staging_table();
    let staging_a = SqlLayout::new("orders")
        .overwrite_run(Some(a.clone()))
        .staging_table();
    let staging_b = SqlLayout::new("orders")
        .overwrite_run(Some(b))
        .staging_table();
    assert_eq!(untokened, "orders__faucet_ovw");
    assert_eq!(staging_a, format!("orders__faucet_ovw_{a}"));
    assert_ne!(staging_a, staging_b);
    for name in [&untokened, &staging_a] {
        d.check_ident(name).unwrap();
        let quoted = d.quote_ident(name);
        assert!(quoted.contains(name.as_str()), "{quoted}");
    }
    // A live sibling run's staging is never swept by another run's begin.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!(!abandoned_overwrite_staging(
        "orders",
        &staging_b,
        Some(&a),
        now
    ));
}

#[test]
fn matrix_overwrite_staging_names() {
    every_dialect!(overwrite_runs_stage_under_their_own_name);
}

/// The cleanup journal read renders a binary key as unwrapped base64 — the
/// form the writes decode — on MySQL and PostgreSQL; other keys stay raw and
/// a dialect without the typed override falls back to the untyped query.
#[test]
fn cleanup_journal_renders_binary_keys_as_base64() {
    use faucet_common_sql::dialects::{Mysql, Postgres, Sqlite};
    let key = ["id".to_string(), "n".to_string()];
    let types = [Some("VARBINARY(16)"), Some("INT")];
    let m = Mysql
        .cleanup_absent_keys_typed_sql(&target(&Mysql, "t"), &[], &[], &key, &types)
        .unwrap()
        .unwrap();
    assert!(
        m.contains("'id', REPLACE(TO_BASE64(`t`.`id`), CHAR(10 USING ascii), '')"),
        "{m}"
    );
    assert!(m.contains("'n', `t`.`n`"), "{m}");
    let untyped = Mysql
        .cleanup_absent_keys_sql(&target(&Mysql, "t"), &[], &[], &key)
        .unwrap()
        .unwrap();
    assert!(!untyped.contains("TO_BASE64"), "{untyped}");

    let p = Postgres
        .cleanup_absent_keys_typed_sql(
            &target(&Postgres, "t"),
            &[],
            &[],
            &key,
            &[Some("bytea"), Some("int4")],
        )
        .unwrap()
        .unwrap();
    assert!(
        p.contains("'id', replace(encode(t.\"id\", 'base64'), E'\\n', '')"),
        "{p}"
    );
    assert!(p.contains("'n', t.\"n\""), "{p}");

    let s = Sqlite
        .cleanup_absent_keys_typed_sql(&target(&Sqlite, "t"), &[], &[], &key, &types)
        .unwrap();
    assert_eq!(
        s,
        Sqlite
            .cleanup_absent_keys_sql(&target(&Sqlite, "t"), &[], &[], &key)
            .unwrap()
    );
}

/// Inside a page scope the engine reads a target's live columns once for every
/// chunk of the page; outside one (or after it closes) every insert reads them.
fn page_scope_reads_columns_once<D: SqlDialect>(d: D) {
    let engine = SqlSinkEngine::new(d, WriteSpec::default());
    let t = target(engine.dialect(), "t");
    let mut rec = Recorder::new(&[("id", "bigint"), ("v", "text")]);
    let rows = vec![json!({"id": 1, "v": "a"})];
    {
        let _page = engine.page_scope();
        for _ in 0..3 {
            futures_lite_block_on(engine.insert(&mut rec, &t, &rows, false)).unwrap();
        }
        let other = target(engine.dialect(), "u");
        futures_lite_block_on(engine.insert(&mut rec, &other, &rows, false)).unwrap();
    }
    assert_eq!(
        rec.catalog_reads,
        2,
        "{}: one read per target",
        engine.dialect().name()
    );
    futures_lite_block_on(engine.insert(&mut rec, &t, &rows, false)).unwrap();
    futures_lite_block_on(engine.insert(&mut rec, &t, &rows, false)).unwrap();
    assert_eq!(
        rec.catalog_reads,
        4,
        "{}: no cache outside a scope",
        engine.dialect().name()
    );
}

#[test]
fn page_scope_reads_columns_once_per_target() {
    every_dialect!(page_scope_reads_columns_once);
}

/// An integer column widened for fractions never narrows the integer range
/// an earlier release gave it (28 integer digits: every 64-bit integer and
/// floats up to 10^28). A backend whose decimals go past 38 digits also keeps
/// at least 19 fractional digits; one capped at 38 cannot have both.
fn integer_to_fraction_widening_keeps_its_integer_range<D: SqlDialect>(d: D) {
    let t = d.widened_type_name(Some(SqlBaseType::Integer), SqlBaseType::Double);
    let Some((p, s)) = t.split_once('(').and_then(|(_, rest)| {
        let (p, s) = rest.split_once(',')?;
        let digits = |x: &str| {
            x.trim_matches(|c: char| !c.is_ascii_digit())
                .parse::<u32>()
                .ok()
        };
        Some((digits(p)?, digits(s)?))
    }) else {
        return;
    };
    assert!(p - s >= 28, "{}: {t} narrows the integer range", d.name());
    if p > 38 {
        assert!(s >= 19, "{}: {t}", d.name());
    }
}

#[test]
fn matrix_integer_to_fraction_widening() {
    every_dialect!(integer_to_fraction_widening_keeps_its_integer_range);
}

/// The written-key table's alias in a cleanup delete must not shadow a target
/// table named like it, or the join compares the keys table with itself and
/// the delete removes nothing.
fn cleanup_delete_alias_never_shadows_the_target<D: SqlDialect>(d: D) {
    for name in ["c", "t", "_faucet_seen"] {
        let target = target(&d, name);
        let sql = d
            .cleanup_delete_sql(
                &target,
                &["tenant".to_string()],
                &[None],
                &["id".to_string()],
            )
            .unwrap();
        let keys = d.cleanup_keys_table();
        let after = sql
            .split(&format!("FROM {keys} "))
            .nth(1)
            .unwrap_or_else(|| panic!("{}: {sql}", d.name()));
        let alias = after.split_whitespace().next().unwrap();
        let outer_named = sql.starts_with(&format!("DELETE FROM {} WHERE", target.reference));
        if outer_named {
            assert_ne!(
                alias.trim_matches(['`', '"', '[', ']']),
                name,
                "{}: the keys alias shadows the target: {sql}",
                d.name()
            );
        }
    }
}

#[test]
fn matrix_cleanup_alias() {
    every_cleanup_dialect!(cleanup_delete_alias_never_shadows_the_target);
}

fn declared() -> faucet_core::DeclaredColumns {
    use faucet_core::{DeclaredColumn, LogicalType};
    faucet_core::DeclaredColumns::new([
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
        DeclaredColumn::new("note", LogicalType::String, true),
    ])
}

/// What each dialect creates a declared column as, else its base type.
fn expected_type<D: SqlDialect>(d: &D, c: &faucet_core::DeclaredColumn, key: bool) -> String {
    let declared = if key {
        d.declared_key_type_name(&c.logical_type)
    } else {
        d.declared_type_name(&c.logical_type)
    };
    declared.unwrap_or_else(|| {
        if key {
            d.key_type_name(c.logical_type.base_type())
        } else {
            d.type_name(c.logical_type.base_type())
        }
        .to_string()
    })
}

fn declared_create_table_types_each_column<D: SqlDialect>(d: D) {
    let name = d.name();
    let keyed = matches!(name, "postgres" | "mysql" | "sqlite" | "mssql" | "oracle");
    let key: &[&str] = if keyed { &["id"] } else { &[] };
    let mode = if keyed {
        WriteMode::Upsert
    } else {
        WriteMode::Append
    };
    let engine = SqlSinkEngine::new(d, spec(mode, key));
    let t = target(engine.dialect(), "t");
    let decl = declared();
    let empty = engine
        .create_table_sql_declared(&t, &[], None, Some(&decl))
        .unwrap()
        .expect("a complete declaration creates an empty table");
    for c in decl.columns() {
        let ty = expected_type(engine.dialect(), c, key.contains(&c.name.as_str()));
        let col = engine.dialect().quote_ident(&c.name);
        assert!(
            empty.contains(&format!("{col} {ty}")),
            "{name}: {} typed {ty}: {empty}",
            c.name
        );
    }
    let pos = |n: &str| empty.find(&engine.dialect().quote_ident(n)).unwrap();
    assert!(
        pos("id") < pos("amount") && pos("day") < pos("at"),
        "{name}: source order: {empty}"
    );
    assert!(
        engine
            .create_table_sql_declared(&t, &[], None, Some(&decl.clone().reshaped()))
            .unwrap()
            .is_none(),
        "{name}: a reshaped declaration creates nothing for an empty page"
    );
    let page = [json!({"id": 1, "amount": "1.50", "day": "not a date", "extra": true})];
    let sql = engine
        .create_table_sql_declared(&t, &page, None, Some(&decl))
        .unwrap()
        .expect("a page");
    let day = engine.dialect().quote_ident("day");
    let text = engine.dialect().type_name(SqlBaseType::Text);
    assert!(
        sql.contains(&format!("{day} {text}")),
        "{name}: refused values keep text: {sql}"
    );
    assert!(
        !sql.contains(&engine.dialect().quote_ident("at")),
        "{name}: {sql}"
    );
    assert_eq!(
        engine
            .create_table_sql_declared(&t, &page, None, None)
            .unwrap(),
        engine.create_table_sql(&t, &page, None).unwrap(),
        "{name}: no declaration is today's inference"
    );
}

#[test]
fn matrix_declared_create_table() {
    every_dialect!(declared_create_table_types_each_column);
}

#[test]
fn declared_types_per_dialect() {
    use faucet_core::LogicalType as L;
    let dec = |p, s| L::Decimal {
        precision: Some(p),
        scale: Some(s),
    };
    let unbounded = L::Decimal {
        precision: None,
        scale: None,
    };
    let bq = BigQuery;
    assert_eq!(
        bq.declared_type_name(&dec(12, 2)).as_deref(),
        Some("NUMERIC")
    );
    assert_eq!(
        bq.declared_type_name(&dec(38, 9)).as_deref(),
        Some("NUMERIC")
    );
    assert_eq!(
        bq.declared_type_name(&dec(40, 2)).as_deref(),
        Some("BIGNUMERIC")
    );
    assert_eq!(
        bq.declared_type_name(&dec(20, 12)).as_deref(),
        Some("BIGNUMERIC")
    );
    assert_eq!(
        bq.declared_type_name(&unbounded).as_deref(),
        Some("BIGNUMERIC")
    );
    assert_eq!(bq.declared_type_name(&dec(100, 50)), None);
    assert_eq!(bq.declared_type_name(&L::Date).as_deref(), Some("DATE"));
    assert_eq!(
        bq.declared_type_name(&L::Timestamp { tz: true }).as_deref(),
        Some("TIMESTAMP")
    );
    assert_eq!(
        bq.declared_type_name(&L::Timestamp { tz: false })
            .as_deref(),
        Some("DATETIME")
    );
    assert_eq!(bq.declared_type_name(&L::Uuid).as_deref(), Some("STRING"));
    assert_eq!(bq.declared_type_name(&L::Bytes).as_deref(), Some("BYTES"));
    assert_eq!(
        Postgres.declared_type_name(&dec(12, 2)).as_deref(),
        Some("numeric(12,2)")
    );
    assert_eq!(
        Postgres.declared_type_name(&unbounded).as_deref(),
        Some("numeric")
    );
    assert_eq!(
        Postgres.declared_key_type_name(&L::Uuid).as_deref(),
        Some("uuid")
    );
    assert_eq!(
        Mysql.declared_type_name(&unbounded).as_deref(),
        Some("DECIMAL(65,30)")
    );
    assert_eq!(Mysql.declared_key_type_name(&L::String), None);
    assert_eq!(Mssql.declared_type_name(&unbounded), None);
    assert_eq!(Mssql.declared_type_name(&dec(40, 2)), None);
    assert_eq!(
        Duckdb.declared_type_name(&dec(18, 4)).as_deref(),
        Some("DECIMAL(18,4)")
    );
    assert_eq!(Redshift.declared_type_name(&L::Uuid), None);
    assert_eq!(
        ClickHouse.declared_type_name(&unbounded).as_deref(),
        Some("Nullable(Decimal(76, 38))")
    );
    assert_eq!(ClickHouse.declared_type_name(&L::Time), None);
    assert_eq!(
        Oracle.declared_type_name(&unbounded).as_deref(),
        Some("NUMBER")
    );
    assert_eq!(Oracle.declared_type_name(&L::Time), None);
    assert_eq!(Snowflake.declared_type_name(&L::Bytes), None);
    assert_eq!(
        Snowflake
            .declared_type_name(&L::Timestamp { tz: true })
            .as_deref(),
        Some("TIMESTAMP_TZ(9)")
    );
    assert_eq!(Databricks.declared_type_name(&L::Time), None);
    assert_eq!(
        Databricks
            .declared_type_name(&L::Timestamp { tz: false })
            .as_deref(),
        Some("TIMESTAMP_NTZ")
    );
    assert_eq!(Sqlite.declared_type_name(&L::Date), None);
    let planned = faucet_core::PlannedColumn {
        name: "k".into(),
        base_type: SqlBaseType::Text,
        nullable: false,
        declared: Some(L::String),
    };
    assert_eq!(Mysql.planned_type_name(&planned, true), "VARCHAR(191)");
    assert_eq!(Mysql.planned_type_name(&planned, false), "LONGTEXT");
}

/// #975: a declared decimal is created typed only when every first-page value
/// fits the column the dialect would create, so nothing is rounded.
fn declared_decimals_fit_the_created_column<D: SqlDialect>(d: D) {
    use faucet_core::{DeclaredColumn, LogicalType as L};
    let name = d.name();
    let unbounded = L::Decimal {
        precision: None,
        scale: None,
    };
    let bounded = L::Decimal {
        precision: Some(12),
        scale: Some(2),
    };
    let big_frac = json!(format!("0.{}", "1".repeat(40)));
    let big_int = json!("9".repeat(40));
    let frac3 = json!("1.125");
    let int11 = json!("12345678901.5");
    let decl = faucet_core::DeclaredColumns::new([
        DeclaredColumn::new("big_frac", unbounded, true),
        DeclaredColumn::new("big_int", unbounded, true),
        DeclaredColumn::new("frac3", bounded, true),
        DeclaredColumn::new("int11", bounded, true),
    ]);
    let page = [json!({
        "big_frac": big_frac, "big_int": big_int, "frac3": frac3, "int11": int11,
    })];
    let engine = SqlSinkEngine::new(d, spec(WriteMode::Append, &[]));
    let t = target(engine.dialect(), "t");
    let sql = engine
        .create_table_sql_declared(&t, &page, None, Some(&decl))
        .unwrap()
        .expect("a page");
    let dialect = engine.dialect();
    for c in decl.columns() {
        let value = &page[0][&c.name];
        let col = dialect.quote_ident(&c.name);
        let fits = dialect.declared_fits(&c.logical_type, value);
        let ty = if fits {
            expected_type(dialect, c, false)
        } else {
            dialect.type_name(SqlBaseType::Text).to_string()
        };
        assert!(
            sql.contains(&format!("{col} {ty}")),
            "{name}: {} (fits: {fits}) typed {ty}: {sql}",
            c.name
        );
    }
    let fixed_scale_unbounded = matches!(name, "mysql" | "clickhouse" | "oracle" | "bigquery");
    for v in [&big_frac, &big_int] {
        assert_eq!(
            dialect.declared_fits(&unbounded, v),
            !fixed_scale_unbounded,
            "{name}: an unbounded decimal created as a fixed-scale type refuses {v}"
        );
    }
    for v in [&frac3, &int11] {
        assert_eq!(
            dialect.declared_fits(&bounded, v),
            name == "bigquery",
            "{name}: DECIMAL(12,2) holds {v} only as BigQuery NUMERIC"
        );
    }
    assert!(
        dialect.declared_fits(&bounded, &json!("1234567890.12")),
        "{name}"
    );
}

#[test]
fn matrix_declared_decimal_bounds() {
    every_dialect!(declared_decimals_fit_the_created_column);
}

#[test]
fn declared_time_fractions_fit_the_created_column() {
    use faucet_core::LogicalType as L;
    let ts = L::Timestamp { tz: true };
    let nanos = json!("2024-02-22T02:00:00.123456789Z");
    let micros = json!("2024-02-22T02:00:00.123456+05:30");
    for (name, fits_nanos) in [
        ("mysql", Mysql.declared_fits(&ts, &nanos)),
        ("clickhouse", ClickHouse.declared_fits(&ts, &nanos)),
        ("bigquery", BigQuery.declared_fits(&ts, &nanos)),
    ] {
        assert!(!fits_nanos, "{name} keeps microseconds");
    }
    assert!(Mysql.declared_fits(&ts, &micros));
    assert!(Mysql.declared_fits(&L::Time, &json!("10:11:12.5")));
    assert!(!Mysql.declared_fits(&L::Time, &json!("10:11:12.1234567")));
    assert!(Oracle.declared_fits(&ts, &nanos));
    assert!(Postgres.declared_fits(&ts, &nanos));
}

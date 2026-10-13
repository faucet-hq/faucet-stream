# faucet-common-sql

The shared SQL layer of the [faucet-stream](https://crates.io/crates/faucet-stream)
SQL sinks (RFC 0013, #884): one implementation of the planning every SQL sink
used to carry its own copy of.

- **`SqlDialect`** — how a backend spells SQL: identifier quoting
  (`check_ident` refuses names the dialect cannot express), literal escaping,
  placeholders and the parameter limit, created / key / widened type names,
  keyed upsert (`ON CONFLICT`, `ON DUPLICATE KEY`, `MERGE`) and delete
  statements, scoped-cleanup statements, the commit-token watermark table and
  the rollback-journal SQL (`faucet_core::rollback::JournalSql`). Statement
  builders are provided methods; a dialect overrides only where its SQL
  differs.
- **`dialects`** — `Postgres`, `Mysql`, `Sqlite`, `Mssql`, `Duckdb`,
  `Redshift`, `ClickHouse`, `Oracle`, `Snowflake`, `Databricks`, `BigQuery`.
- **`mapping`** — record-to-column mapping. `ColumnMatching` is `Exact`,
  `CaseInsensitive` (exact first, else the first field equal ignoring ASCII
  case), `FoldUnique` (a field folds to the one column equal ignoring case,
  unless the record already carries that column or several match) or
  `UpperFold` (a field reaches an all-upper-case column through its
  upper-case form). Union column sets and present-column groups.
- **`SqlSinkEngine<D>`** — the planning, run over a backend's `SqlExecutor`:
  mapping (a record matching no column is an error), union-column appends
  (a missing column binds NULL), one upsert per set of carried columns (an
  absent column keeps its stored value), page-ordered keyed groups with the
  rollback journal written first on the same connection, statements chunked
  under the parameter limit, create-table and evolution steps, the scoped
  cleanup sequence (validate scope and key, load the written keys into a
  temporary table, `NOT EXISTS` delete), the commit-token write and the row
  report (`WriteReport`).

## Using it in a sink

```rust,ignore
use faucet_common_sql::dialects::Postgres;
use faucet_common_sql::{SqlExecutor, SqlSinkEngine, Statement, TableColumn, TableTarget};

struct Exec<'c> { conn: &'c mut sqlx::PgConnection }

#[faucet_core::async_trait]
impl SqlExecutor for Exec<'_> {
    async fn columns(&mut self, t: &TableTarget) -> Result<Vec<TableColumn>, FaucetError> {
        /* read the catalog for t.reference */
    }
    async fn execute(&mut self, stmt: &Statement<'_>) -> Result<u64, FaucetError> {
        /* bind every stmt.params value as its column needs; run stmt.sql */
    }
}

let engine = SqlSinkEngine::new(Postgres, config.write.clone());
let plans = engine.plan_keyed(records)?;
engine.apply_plans(&mut Exec { conn: &mut tx }, &target, &plans).await?;
```

The backend keeps transactions, bulk-load paths (`COPY`), overwrite swaps and
catalog queries, and words its own errors per `StatementKind`.

## Who uses what

| Sink | Uses |
|---|---|
| postgres, mysql, sqlite | the engine end to end, over an executor |
| mssql | the dialect and the shared mapping; its own retry-classified chunk execution over `tiberius` |
| duckdb | the engine's row planning, executed on the blocking driver |
| redshift, clickhouse, oracle, snowflake, databricks, bigquery | the dialect for quoting, escaping, types and DDL; their own row handling |

## The planning matrix

`tests/dialect_matrix.rs` runs the same planning cases — quoting, column
mapping, dedup, upsert/delete ordering, chunking, present-column groups,
widening, create-table, cleanup, journal and token SQL, JSON mode — against
every dialect. Add a new dialect there.

## License

MIT OR Apache-2.0

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

## The SQL family driver (RFC 0014)

Implement `SqlDriver` — the backend's primitives: `begin` / `commit` /
`connection`, `table_exists`, `ddl`, `read_token`, `create_staging` /
`publish_staging` / `swap_staging`, `current_schema`, `existing_columns` /
`apply_evolution`, and optionally `rollback()` — and `SqlSinkDriver` drives the
whole sink from it and the engine: create-on-first-write, keyed and chunked
writes in transactions, per-row splits for the dead-letter path, rows and the
commit token in one transaction, the key-table cleanup, the overwrite
lifecycle (drop stale staging, clone, first-run publish or swap, discard) and
schema-evolution steps. Wrap it in `faucet_core::driver::DriverSink`:

```rust,ignore
pub struct AcmeSink { inner: DriverSink<SqlSinkDriver<AcmeDriver>> }
#[async_trait] impl Sink for AcmeSink { forward_sink!(inner); }
```

`SqlLayout` says where the sink writes (table, JSON column, `create_table`,
`batch_size`). `faucet-sink-sqlite` is the reference implementation.

A backend whose rules differ overrides a defaulted hook rather than the
lifecycle; every default reproduces the generic behaviour:

| Hook | Default | Overridden by |
|---|---|---|
| `insert(conn, target, rows, InsertPath)` | the engine's mapped or JSON-column insert | postgres: `COPY` for a plain page, JSONB `unnest` |
| `write_token(conn, scope, token)` | the engine's token upsert | mysql: the fitted scope key, retiring the pre-1.13 key |
| `prepare_keyed_write()` | nothing | mysql: create the rollback journal before the transaction (DDL commits) |
| `drop_staging(stale)` / `staging_exists()` | the staging table only | mysql: also the swap's `__faucet_ovw_old` table |
| `staging_table()` | `SqlLayout::staging_table` | postgres: the name shortened to 63 bytes |
| `split_page(..)` / `splits_keyed_pages()` | no-matching-column refusals, keyed pages unsplit | postgres, mysql: binary values the encoding refuses fail their row, keyed pages too |
| `guard(what, fut)` | unbounded | postgres, mysql: `write_timeout_secs` per operation |
| `table_display(table)` | as configured | postgres: the schema-qualified name |
| `capabilities(derived)` | unchanged | postgres: scoped overwrite |

## Using the engine directly

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
| sqlite, postgres, mysql | the SQL family driver (`SqlDriver` + `SqlSinkDriver`); postgres and mysql override the backend hooks |
| mssql | the dialect and the shared mapping; its own retry-classified chunk execution over `tiberius`, on the core `SinkDriver` |
| duckdb | the engine's row planning, executed on the blocking driver, on the core `SinkDriver` |
| redshift, clickhouse, oracle, snowflake, databricks, bigquery | the dialect for quoting, escaping, types and DDL; their own row handling |

## The planning matrix

`tests/dialect_matrix.rs` runs the same planning cases — quoting, column
mapping, dedup, upsert/delete ordering, chunking, present-column groups,
widening, create-table, cleanup, journal and token SQL, JSON mode — against
every dialect. Add a new dialect there.

## License

MIT OR Apache-2.0

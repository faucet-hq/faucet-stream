//! PostgreSQL sink implementation.

use crate::config::{PostgresColumnMapping, PostgresSinkConfig, PostgresWriteMethod};
use crate::copy::{build_auto_map_payload, build_jsonb_payload, copy_statement};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

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
    match value {
        None | Some(Value::Null) => None,
        Some(v) => {
            if udt.eq_ignore_ascii_case("json") || udt.eq_ignore_ascii_case("jsonb") {
                Some(v.to_string())
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
    }
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
fn qualified_table_ref(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        None => quote_ident(table),
    }
}

/// Build the `ON CONFLICT (key) DO UPDATE …` tail for an upsert INSERT.
/// Non-key columns are SET from EXCLUDED. If every column is a key column,
/// emit `DO NOTHING`.
fn on_conflict_clause(key: &[String], all_cols: &[String]) -> String {
    let key_list = key
        .iter()
        .map(|k| quote_ident(k))
        .collect::<Vec<_>>()
        .join(", ");
    let updates: Vec<String> = all_cols
        .iter()
        .filter(|c| !key.iter().any(|k| k == *c))
        .map(|c| format!("{q} = EXCLUDED.{q}", q = quote_ident(c)))
        .collect();
    if updates.is_empty() {
        format!("ON CONFLICT ({key_list}) DO NOTHING")
    } else {
        format!(
            "ON CONFLICT ({key_list}) DO UPDATE SET {}",
            updates.join(", ")
        )
    }
}

/// Map a [`faucet_core::SqlBaseType`] to the PostgreSQL type keyword used when
/// adding/widening a column during schema evolution (issue #194). Integers
/// always widen to `bigint` and floats to `double precision` so a later, wider
/// value never overflows a narrower column.
fn pg_keyword(t: faucet_core::SqlBaseType) -> &'static str {
    use faucet_core::SqlBaseType::*;
    match t {
        Integer => "bigint",
        Double => "double precision",
        Boolean => "boolean",
        Text => "text",
        Json => "jsonb",
    }
}

/// `ALTER TABLE <ref> ADD COLUMN IF NOT EXISTS "<col>" <kw>` — idempotent column
/// addition. `table_ref` is already quoted (`"schema"."table"`).
fn build_add_column_sql(table_ref: &str, col: &str, t: faucet_core::SqlBaseType) -> String {
    format!(
        "ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS {} {}",
        quote_ident(col),
        pg_keyword(t)
    )
}

/// `ALTER TABLE <ref> ALTER COLUMN "<col>" TYPE <kw> USING "<col>"::<kw>` — widen
/// an existing column's type. Naturally idempotent (re-running the same TYPE
/// change is a no-op).
fn build_alter_type_sql(table_ref: &str, col: &str, t: faucet_core::SqlBaseType) -> String {
    let q = quote_ident(col);
    let kw = pg_keyword(t);
    format!("ALTER TABLE {table_ref} ALTER COLUMN {q} TYPE {kw} USING {q}::{kw}")
}

/// `ALTER TABLE <ref> ALTER COLUMN "<col>" DROP NOT NULL` — relax a NOT NULL
/// constraint. Naturally idempotent.
fn build_drop_not_null_sql(table_ref: &str, col: &str) -> String {
    format!(
        "ALTER TABLE {table_ref} ALTER COLUMN {} DROP NOT NULL",
        quote_ident(col)
    )
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
pub struct PostgresSink {
    config: PostgresSinkConfig,
    pool: PgPool,
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

        Ok(Self { config, pool })
    }

    /// Staging table name used while an overwrite run is in flight (same schema
    /// as the target).
    fn staging_table_name(&self) -> String {
        format!(
            "{}{}",
            self.config.table_name,
            faucet_core::idempotency::OVERWRITE_STAGING_SUFFIX
        )
    }

    /// The base table name the data-write path targets. For `write_mode:
    /// overwrite` every write in this sink's lifetime lands in the staging
    /// table (created by [`begin_overwrite`], swapped by [`commit_overwrite`]);
    /// otherwise the configured table.
    fn effective_table_name(&self) -> String {
        if self.config.write.is_overwrite() {
            self.staging_table_name()
        } else {
            self.config.table_name.clone()
        }
    }

    /// Discover the target relation's column names and underlying types
    /// (`pg_type.typname`), scoped to the *exact* relation the writes target
    /// via `to_regclass` (#146 M13). Shared by the INSERT and COPY paths so
    /// both see an identical column set. `::text` casts the `name`-typed
    /// catalog columns so sqlx decodes them as `String`.
    async fn discover_columns(
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
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let table_ref =
            qualified_table_ref(self.config.schema.as_deref(), &self.effective_table_name());

        let (statement, payload) = match &self.config.column_mapping {
            PostgresColumnMapping::Jsonb { column } => {
                let payload = build_jsonb_payload(records);
                (
                    copy_statement(&table_ref, std::slice::from_ref(column)),
                    payload,
                )
            }
            PostgresColumnMapping::AutoMap => {
                let columns = self.discover_columns(&mut *conn, &table_ref).await?;
                let Some(payload) =
                    build_auto_map_payload(records, &columns).map_err(FaucetError::Sink)?
                else {
                    return Ok(0);
                };
                (copy_statement(&table_ref, &payload.columns), payload)
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

    /// Insert a batch of records using JSONB column mode, on the given connection.
    ///
    /// Accepts `&mut sqlx::PgConnection` so the same logic runs both standalone
    /// (via a pool-acquired connection, autocommit) and inside the idempotent
    /// transaction (where `&mut *tx` is passed — `Transaction<'_, Postgres>`
    /// derefs to `PgConnection`).
    async fn insert_jsonb(
        &self,
        conn: &mut sqlx::PgConnection,
        records: &[Value],
        column: &str,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Use a single INSERT with unnest for efficiency.
        let json_values: Vec<serde_json::Value> = records.to_vec();
        let query = format!(
            "INSERT INTO {} ({}) SELECT * FROM unnest($1::jsonb[])",
            qualified_table_ref(self.config.schema.as_deref(), &self.effective_table_name()),
            quote_ident(column)
        );

        sqlx::query(&query)
            .bind(json_values)
            .execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns, on the given connection.
    ///
    /// Accepts `&mut sqlx::PgConnection` so the same logic runs both standalone
    /// (via a pool-acquired connection, autocommit) and inside the idempotent
    /// transaction (where `&mut *tx` is passed — `Transaction<'_, Postgres>`
    /// derefs to `PgConnection`). Running the column-discovery query on the same
    /// connection is harmless and avoids any cross-connection visibility surprise.
    ///
    /// Discovers each column's name *and* underlying type (`udt_name`) from the
    /// table schema and maps top-level JSON fields to columns. Each placeholder
    /// is emitted as `$N::<udt>` and the value is bound as text (see
    /// [`pg_bind_text`]), so the destination column's input function parses it —
    /// numbers, booleans, timestamps, uuids, and JSON all land in their native
    /// column types. (Previously every value was bound as `serde_json::Value`,
    /// which sqlx encodes as `jsonb`, so an insert into any non-`jsonb` column
    /// failed at runtime — audit #146 C1.) Uses a single multi-row INSERT
    /// (sub-chunked at the 65535-parameter cap) for efficiency.
    ///
    /// When `conflict_key` is `Some(key)`, each sub-chunk's INSERT is given an
    /// `ON CONFLICT (key) DO UPDATE …` tail so it upserts by the key columns
    /// (last-write-wins within the batch is already handled by the planner's
    /// dedup, so a single sub-chunk never double-hits the same conflict target).
    async fn insert_auto_map_with_conflict(
        &self,
        conn: &mut sqlx::PgConnection,
        records: &[Value],
        conflict_key: Option<&[String]>,
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names AND their underlying types for the *exact* relation
        // the INSERT will target. Scoping by `to_regclass(<qualified ref>)`
        // resolves the relation the same way the INSERT does — by the configured
        // schema if set, otherwise by the connection's `search_path` — so a
        // table of the same name in another schema can no longer pollute the
        // column set with duplicate/wrong columns (#146 M13). The previous query
        // filtered `information_schema.columns` by `table_name` alone (no schema
        // predicate), merging every same-named table across all schemas.
        //
        // `pg_type.typname` is the concrete type (`int4`, `timestamptz`,
        // `numeric`, `jsonb`, `uuid`, `text`, …) — identical to the old
        // `information_schema.columns.udt_name` — used as the per-placeholder
        // cast target below.
        let table_ref =
            qualified_table_ref(self.config.schema.as_deref(), &self.effective_table_name());
        let columns = self.discover_columns(&mut *conn, &table_ref).await?;

        // Pre-validate all records and collect matched (column, udt, value)
        // triples per record. The INSERT column set is the UNION of table
        // columns present in ANY record (in declared table order), not just the
        // first record's keys — otherwise a field present only in a later record
        // of the batch would be silently dropped (audit #146 H1). A row missing
        // a unioned column binds SQL NULL.
        let mut matched_rows: Vec<Vec<(&String, &String, &Value)>> =
            Vec::with_capacity(records.len());
        let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

            let matching: Vec<(&String, &String, &Value)> = columns
                .iter()
                .filter_map(|(col, udt)| obj.get(col).map(|v| (col, udt, v)))
                .collect();

            if matching.is_empty() {
                tracing::warn!(
                    record_keys = ?obj.keys().collect::<Vec<_>>(),
                    table_columns = ?columns,
                    "record has no keys matching table columns, skipping"
                );
                continue;
            }

            for (c, _, _) in &matching {
                used.insert(c.as_str());
            }
            matched_rows.push(matching);
        }

        if matched_rows.is_empty() {
            return Ok(0);
        }

        // Table columns (in declared order, with their udt) present in at least
        // one record.
        let insert_columns: Vec<(String, String)> = columns
            .iter()
            .filter(|(c, _)| used.contains(c.as_str()))
            .cloned()
            .collect();

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns.iter().map(|(c, _)| quote_ident(c)).collect();

        // PostgreSQL caps bind parameters per statement at 65535. A multi-row
        // INSERT binds `rows × num_cols` parameters, so a wide table at a large
        // batch_size can exceed it and fail at runtime (#78/#21). Split into
        // sub-INSERTs of at most floor(MAX_PARAMS / num_cols) rows.
        const MAX_PG_PARAMS: usize = 65535;
        let max_rows_per_insert = (MAX_PG_PARAMS / num_cols).max(1);

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause with per-column casts so the column
            // type's input function parses the bound text:
            //   ($1::int4, $2::timestamptz), ($3::int4, $4::timestamptz), ...
            let mut value_tuples: Vec<String> = Vec::with_capacity(sub.len());
            for row_idx in 0..sub.len() {
                let start = row_idx * num_cols + 1;
                let placeholders: Vec<String> = (0..num_cols)
                    .map(|c| format!("${}::{}", start + c, insert_columns[c].1))
                    .collect();
                value_tuples.push(format!("({})", placeholders.join(", ")));
            }

            let query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_ref,
                col_names.join(", "),
                value_tuples.join(", ")
            );
            let query = match conflict_key {
                Some(key) => format!(
                    "{query} {}",
                    on_conflict_clause(
                        key,
                        &insert_columns
                            .iter()
                            .map(|(c, _)| c.clone())
                            .collect::<Vec<_>>()
                    )
                ),
                None => query,
            };

            let mut q = sqlx::query(&query);
            for matched in sub {
                // Bind values in the fixed column order, as text matching each
                // column's type. A record missing a column that appeared in the
                // first record binds SQL NULL.
                for (col, udt) in &insert_columns {
                    let val = matched
                        .iter()
                        .find(|(c, _, _)| *c == col)
                        .map(|(_, _, v)| *v);
                    q = q.bind(pg_bind_text(val, udt));
                }
            }

            q.execute(&mut *conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;
        }

        Ok(num_rows)
    }

    /// Insert a batch of records using auto-mapped columns, on the given
    /// connection, with plain append semantics (no `ON CONFLICT` tail).
    ///
    /// Thin wrapper over [`insert_auto_map_with_conflict`](Self::insert_auto_map_with_conflict)
    /// so the append path and the idempotent-write path keep their original
    /// signature.
    async fn insert_auto_map(
        &self,
        conn: &mut sqlx::PgConnection,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        self.insert_auto_map_with_conflict(conn, records, None)
            .await
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((v1, …), …)` with per-column `::udt`
    /// casts (the key columns' underlying types), chunked at the param cap.
    async fn delete_by_keys(
        &self,
        conn: &mut sqlx::PgConnection,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);

        // Underlying types for the key columns → drives the ::udt casts, same
        // source the insert path uses.
        let udts: std::collections::HashMap<String, String> = self
            .discover_columns(&mut *conn, &table_ref)
            .await?
            .into_iter()
            .collect();
        let key_udts: Vec<String> = key
            .iter()
            .map(|k| udts.get(k).cloned().unwrap_or_else(|| "text".to_string()))
            .collect();
        let col_list = key
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");

        const MAX_PG_PARAMS: usize = 65535;
        let per = (MAX_PG_PARAMS / key.len().max(1)).max(1);
        let mut total = 0usize;
        for chunk in deletes.chunks(per) {
            let mut ph = 1usize;
            let tuples: Vec<String> = chunk
                .iter()
                .map(|_| {
                    let group = key_udts
                        .iter()
                        .map(|udt| {
                            let p = format!("${ph}::{udt}");
                            ph += 1;
                            p
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("({group})")
                })
                .collect();
            let sql = format!(
                "DELETE FROM {table_ref} WHERE ({col_list}) IN ({})",
                tuples.join(", ")
            );
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for ((_, v), udt) in kt.0.iter().zip(key_udts.iter()) {
                    q = q.bind(pg_bind_text(Some(v), udt));
                }
            }
            let res = q
                .execute(&mut *conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("PostgreSQL delete failed: {e}")))?;
            total += res.rows_affected() as usize;
        }
        Ok(total)
    }

    /// Delete rows in `scope` whose key was not written by this run (#478).
    ///
    /// Uses a temp table + `NOT EXISTS` rather than `key NOT IN (…)` because the
    /// written-key set routinely exceeds PostgreSQL's 65535 bind-parameter limit
    /// (the cleanup ceiling defaults to 100k rows). It also makes the whole thing
    /// one transaction, so the delete is all-or-nothing: a partial delete would
    /// remove rows the run actually wrote.
    ///
    /// An empty `seen` set is meaningful, not a no-op — it means the source
    /// reported the scope as empty, so every row in it is stale and must go. That
    /// is the case this feature exists for.
    async fn cleanup_scope_impl(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        let key = &self.config.write.key;
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup requires a non-empty `key`".to_string(),
            ));
        }
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL begin failed: {e}")))?;

        let udts: std::collections::HashMap<String, String> = self
            .discover_columns(&mut tx, &table_ref)
            .await?
            .into_iter()
            .collect();

        // Fail with a clear message rather than letting PostgreSQL reject an
        // unknown column mid-DELETE. The scope is written in *destination* terms,
        // so a name that isn't a real column is a config error worth naming.
        for col in scope.keys().chain(key.iter()) {
            if !udts.contains_key(col) {
                return Err(FaucetError::Sink(format!(
                    "cleanup: column '{col}' does not exist on {table_ref} — the \
                     completeness claim and `key` are in destination column terms"
                )));
            }
        }
        let udt_of = |c: &str| udts.get(c).cloned().unwrap_or_else(|| "text".to_string());

        // Temp table mirroring the key columns' types. `ON COMMIT DROP` scopes it
        // to this transaction, so concurrent cleanups on other connections cannot
        // collide on the name.
        let temp_cols = key
            .iter()
            .map(|k| format!("{} {}", quote_ident(k), udt_of(k)))
            .collect::<Vec<_>>()
            .join(", ");
        sqlx::query(&format!(
            "CREATE TEMP TABLE faucet_cleanup_keys ({temp_cols}) ON COMMIT DROP"
        ))
        .execute(&mut *tx)
        .await
        .map_err(|e| FaucetError::Sink(format!("cleanup: temp table creation failed: {e}")))?;

        // Load the written keys.
        const MAX_PG_PARAMS: usize = 65535;
        let per = (MAX_PG_PARAMS / key.len()).max(1);
        let key_udts: Vec<String> = key.iter().map(|k| udt_of(k)).collect();
        let col_list = key
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        for chunk in seen.keys().chunks(per) {
            let mut ph = 1usize;
            let tuples: Vec<String> = chunk
                .iter()
                .map(|_| {
                    let group = key_udts
                        .iter()
                        .map(|udt| {
                            let s = format!("${ph}::{udt}");
                            ph += 1;
                            s
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!("({group})")
                })
                .collect();
            let sql = format!(
                "INSERT INTO faucet_cleanup_keys ({col_list}) VALUES {}",
                tuples.join(", ")
            );
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for ((_, v), udt) in kt.0.iter().zip(key_udts.iter()) {
                    q = q.bind(pg_bind_text(Some(v), udt));
                }
            }
            q.execute(&mut *tx)
                .await
                .map_err(|e| FaucetError::Sink(format!("cleanup: loading keys failed: {e}")))?;
        }

        // DELETE everything in scope that isn't in the written-key set.
        let mut ph = 1usize;
        let scope_pred = scope
            .keys()
            .map(|c| {
                let s = format!("t.{} = ${}::{}", quote_ident(c), ph, udt_of(c));
                ph += 1;
                s
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let join_pred = key
            .iter()
            .map(|k| {
                let q = quote_ident(k);
                format!("c.{q} = t.{q}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!(
            "DELETE FROM {table_ref} t WHERE {scope_pred} \
             AND NOT EXISTS (SELECT 1 FROM faucet_cleanup_keys c WHERE {join_pred})"
        );
        let mut q = sqlx::query(&sql);
        for (col, v) in scope {
            q = q.bind(pg_bind_text(Some(v), &udt_of(col)));
        }
        let res = q
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: delete failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("cleanup: commit failed: {e}")))?;
        Ok(res.rows_affected())
    }

    /// Apply a planned upsert/delete batch on one connection.
    async fn apply_plan(
        &self,
        conn: &mut sqlx::PgConnection,
        plan: &faucet_core::WritePlan,
    ) -> Result<usize, FaucetError> {
        let mut affected = 0usize;
        if !plan.upserts.is_empty() {
            affected += self
                .insert_auto_map_with_conflict(conn, &plan.upserts, Some(&self.config.write.key))
                .await?;
        }
        if !plan.deletes.is_empty() {
            affected += self.delete_by_keys(conn, &plan.deletes).await?;
        }
        Ok(affected)
    }

    /// Ensure the commit-token watermark table exists.
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} TEXT PRIMARY KEY, {k} TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT now())",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&sql).execute(&self.pool).await.map_err(|e| {
            FaucetError::Sink(format!("PostgreSQL commit-table create failed: {e}"))
        })?;
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for PostgresSink {
    fn connector_name(&self) -> &'static str {
        "postgres"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSinkConfig))
            .expect("schema serialization")
    }

    fn supports_cleanup(&self) -> bool {
        // Column-mapping mode only: the scope + key predicates address real
        // columns, which a single JSONB payload column does not have.
        matches!(self.config.column_mapping, PostgresColumnMapping::AutoMap)
    }

    async fn cleanup_scope(
        &self,
        scope: &std::collections::BTreeMap<String, Value>,
        seen: &faucet_core::SeenKeys,
    ) -> Result<u64, FaucetError> {
        self.cleanup_scope_impl(scope, seen).await
    }

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
            faucet_core::WriteMode::Overwrite,
        ]
    }

    fn is_overwrite(&self) -> bool {
        self.config.write.is_overwrite()
    }

    /// Create the staging table as an empty clone of the target's columns
    /// (`CREATE TABLE staging (LIKE target INCLUDING DEFAULTS)`), dropping any
    /// leftover staging from a crashed run first. The target must already exist
    /// (the sink never auto-creates it) — overwrite replaces its rows, not its
    /// definition.
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        let staging =
            qualified_table_ref(self.config.schema.as_deref(), &self.staging_table_name());
        let target = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL pool acquire failed: {e}")))?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("postgres overwrite: drop stale staging: {e}"))
            })?;
        sqlx::query(&format!(
            "CREATE TABLE {staging} (LIKE {target} INCLUDING DEFAULTS)"
        ))
        .execute(&mut *conn)
        .await
        .map_err(|e| {
            FaucetError::Sink(format!(
                "postgres overwrite: create staging from '{}' (does the table exist?): {e}",
                self.config.table_name
            ))
        })?;
        Ok(())
    }

    /// Atomically replace the destination in one transaction. Full overwrite:
    /// `TRUNCATE target; INSERT INTO target SELECT * FROM staging; DROP staging`.
    /// Scoped/windowed overwrite (#518): `DELETE FROM target WHERE <scope>;
    /// INSERT …; DROP staging` — only the in-scope rows are replaced, the rest
    /// preserved. Postgres runs TRUNCATE and DDL transactionally, so a failure
    /// rolls the whole swap back and the prior rows survive.
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        let staging =
            qualified_table_ref(self.config.schema.as_deref(), &self.staging_table_name());
        let target = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        // Full replace truncates; a scope replaces only the matching rows.
        let clear = match &self.config.scope {
            Some(scope) => {
                let col = quote_ident(scope.column());
                format!(
                    "DELETE FROM {target} WHERE {}",
                    scope.render_where_literal(&col)
                )
            }
            None => format!("TRUNCATE TABLE {target}"),
        };
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres overwrite: begin swap: {e}")))?;
        for stmt in [
            clear,
            format!("INSERT INTO {target} SELECT * FROM {staging}"),
            format!("DROP TABLE {staging}"),
        ] {
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

    /// Drop the staging table so a failed/cancelled overwrite leaves nothing
    /// behind. Best-effort — the destination was never touched.
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        let staging =
            qualified_table_ref(self.config.schema.as_deref(), &self.staging_table_name());
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL pool acquire failed: {e}")))?;
        sqlx::query(&format!("DROP TABLE IF EXISTS {staging}"))
            .execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres overwrite: drop staging: {e}")))?;
        Ok(())
    }

    fn dedups_by_key(&self) -> bool {
        self.config.write.dedups_by_key()
    }

    fn supports_schema_evolution(&self) -> bool {
        true
    }

    /// Read the live destination schema from `pg_catalog` as an
    /// `infer_schema`-shaped object (`{"type":"object","properties":{…}}`), or
    /// `None` when the target table does not exist yet (issue #194).
    ///
    /// Reuses the AutoMap column-discovery query shape (scoped to the exact
    /// relation via `to_regclass`), additionally reading `a.attnotnull` so
    /// nullability round-trips through `pg_udt_to_json_schema`.
    async fn current_schema(&self) -> Result<Option<serde_json::Value>, FaucetError> {
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

    /// Apply an additive schema evolution (new columns, lossless widenings,
    /// nullability relaxations) to the destination table. Idempotent —
    /// `ADD COLUMN IF NOT EXISTS`, and re-running the same TYPE / DROP NOT NULL
    /// is a no-op (issue #194).
    async fn evolve_schema(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("postgres evolve acquire failed: {e}")))?;

        for c in &evolution.additions {
            let t =
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text);
            sqlx::query(&build_add_column_sql(&table_ref, &c.name, t))
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("postgres ADD COLUMN {} failed: {e}", c.name))
                })?;
        }
        for c in &evolution.widenings {
            let t =
                faucet_core::json_schema_base_type(&c.to).unwrap_or(faucet_core::SqlBaseType::Text);
            sqlx::query(&build_alter_type_sql(&table_ref, &c.name, t))
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("postgres ALTER TYPE {} failed: {e}", c.name))
                })?;
        }
        for col in &evolution.relax_nullability {
            sqlx::query(&build_drop_not_null_sql(&table_ref, col))
                .execute(&mut *conn)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!("postgres DROP NOT NULL {col} failed: {e}"))
                })?;
        }
        Ok(())
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

    /// Write records to PostgreSQL.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate multi-row `INSERT`. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `INSERT` — useful when upstream `StreamPage`s are already sized for
    /// Postgres' per-statement bind-parameter limit (~65 535 / num_columns
    /// in AutoMap mode).
    ///
    /// Acquires one connection from the pool and routes all chunks through it.
    /// Each INSERT executes as its own autocommit statement — identical
    /// observable behaviour to executing directly on the pool, while keeping the
    /// same connection for the entire call (avoids repeated pool-checkout
    /// overhead on large batches).
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        if matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "postgres {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            let mut conn =
                self.pool.acquire().await.map_err(|e| {
                    FaucetError::Sink(format!("PostgreSQL pool acquire failed: {e}"))
                })?;
            return self.apply_plan(&mut conn, &plan).await;
        }
        // Append and overwrite are insert-shaped; overwrite writes land in the
        // staging table via `effective_table_name`.

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: pass the entire upstream page through in a single
            // INSERT statement. Subject to Postgres' 65 535 bind-parameter
            // limit in AutoMap mode; JSONB mode binds a single array.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        // Acquire once; reuse for all chunks (each statement autocommits —
        // no BEGIN is issued, so behaviour is identical to using the pool).
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL pool acquire failed: {e}")))?;

        let mut total = 0;
        for chunk in chunks {
            total += match self.config.write_method {
                // Bulk-load fast-path: COPY the chunk instead of a multi-row
                // INSERT (append-only; validated at construction, #308).
                PostgresWriteMethod::Copy => self.copy_batch(&mut conn, chunk).await?,
                PostgresWriteMethod::Insert => match &self.config.column_mapping {
                    PostgresColumnMapping::Jsonb { column } => {
                        self.insert_jsonb(&mut conn, chunk, column).await?
                    }
                    PostgresColumnMapping::AutoMap => {
                        self.insert_auto_map(&mut conn, chunk).await?
                    }
                },
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "PostgreSQL write complete"
        );
        Ok(total)
    }

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](faucet_core::Sink::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if !matches!(
            self.config.write.write_mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            // Append and overwrite: insert-shaped, no per-row key failures.
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL pool acquire failed: {e}")))?;
        self.apply_plan(&mut conn, &plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "postgres {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let sql = format!(
            "SELECT {k} FROM {t} WHERE {s} = $1",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
        );
        let row = sqlx::query(&sql)
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;

        // For upsert/delete modes, plan the page before opening the transaction
        // so a key-extraction failure aborts without leaving an open tx.
        let plan = if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            None
        } else {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "postgres {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            Some(plan)
        };

        let mut tx =
            self.pool.begin().await.map_err(|e| {
                FaucetError::Sink(format!("PostgreSQL transaction begin failed: {e}"))
            })?;

        // Data write(s) and the commit-token upsert share ONE transaction so
        // the page is committed atomically with its watermark: on crash either
        // both land or neither does, which is what makes a replay skip-on-resume
        // produce zero duplicates. For upsert/delete this means the planned
        // upserts/deletes commit together with the watermark in the same tx.
        let written = match &plan {
            Some(plan) => self.apply_plan(&mut tx, plan).await?,
            None => match &self.config.column_mapping {
                PostgresColumnMapping::Jsonb { column } => {
                    self.insert_jsonb(&mut tx, records, column).await?
                }
                PostgresColumnMapping::AutoMap => self.insert_auto_map(&mut tx, records).await?,
            },
        };

        let upsert = format!(
            "INSERT INTO {t} ({s}, {k}) VALUES ($1, $2) ON CONFLICT ({s}) DO UPDATE SET {k} = EXCLUDED.{k}, updated_at = now()",
            t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&upsert)
            .bind(scope)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL token upsert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL transaction commit failed: {e}")))?;
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_add_column_sql, build_alter_type_sql, build_drop_not_null_sql, on_conflict_clause,
        pg_bind_text, pg_udt_to_json_schema, qualified_table_ref,
    };
    use serde_json::json;

    #[test]
    fn pg_add_column_ddl() {
        let sql = build_add_column_sql("\"public\".\"t\"", "email", faucet_core::SqlBaseType::Text);
        assert_eq!(
            sql,
            "ALTER TABLE \"public\".\"t\" ADD COLUMN IF NOT EXISTS \"email\" text"
        );
    }

    #[test]
    fn pg_widen_column_ddl() {
        let sql = build_alter_type_sql(
            "\"public\".\"t\"",
            "score",
            faucet_core::SqlBaseType::Double,
        );
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
}

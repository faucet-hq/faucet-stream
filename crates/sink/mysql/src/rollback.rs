//! Run rollback for the MySQL sink (#706): the before-image journal written
//! alongside upserts/deletes, the kept previous table of an overwrite, and the
//! per-mode undo.
//!
//! Append and upsert undo run in one transaction. The overwrite undo is a
//! single atomic `RENAME TABLE` (MySQL auto-commits DDL, so a transaction
//! cannot span it — the rename itself is the atomic unit, as in the swap).

use crate::config::MysqlColumnMapping;
use crate::sink::{MysqlSink, bind_value, quote_ident_mysql, scope_key};
use faucet_core::FaucetError;
use faucet_core::rollback::{
    JournalEntry, JournalSql, RollbackMode, RollbackOptions, RollbackOutcome, key_json, plan_keys,
    plan_restore,
};
use serde_json::Value;
use sqlx::{MySqlConnection, Row};

const MAX_MYSQL_PARAMS: usize = 65535;

fn placeholder(_n: usize) -> String {
    "?".to_string()
}

fn journal_sql() -> JournalSql {
    JournalSql {
        quote: quote_ident_mysql,
        placeholder,
        before_type: "LONGTEXT",
        insert_prefix: "INSERT IGNORE",
        insert_suffix: "",
        now: "CURRENT_TIMESTAMP",
        // InnoDB caps an index at 3072 bytes, and utf8mb4 text is 4 bytes a
        // character, so the key is a stored SHA-256 of the JSON rather than
        // the JSON itself (which stays readable in `key_json`).
        key_column: "key_json TEXT NOT NULL, key_hash CHAR(64) CHARACTER SET ascii \
                     AS (SHA2(key_json, 256)) STORED",
        primary_key: "PRIMARY KEY (run_id, table_name, key_hash)",
    }
}

fn sink_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("mysql rollback: {context}: {e}"))
}

/// A column as the journal sees it: name, lowercase `DATA_TYPE`, `EXTRA`.
pub(crate) struct JournalColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
    pub(crate) extra: String,
}

impl JournalColumn {
    /// A stored or virtual generated column: the database computes it, so it
    /// is neither journaled nor restored (#789 SQL-107).
    fn is_generated(&self) -> bool {
        let extra = self.extra.to_ascii_lowercase();
        extra.contains("generated") && !extra.contains("default_generated")
    }

    /// The value as it goes into the before-image, typed so the restore binds
    /// it back exactly: binary as base64 (the form every faucet source emits,
    /// decoded by the insert path for binary columns), BIT as its unsigned
    /// integer, DECIMAL as its exact text — `JSON_OBJECT` alone renders binary
    /// as `"base64:typeNN:…"` text and DECIMAL as a JSON number that is parsed
    /// through `f64` (#789 SQL-75).
    fn image_expr(&self) -> String {
        let q = quote_ident_mysql(&self.name);
        match self.data_type.as_str() {
            "binary" | "varbinary" | "tinyblob" | "blob" | "mediumblob" | "longblob" => {
                format!("TO_BASE64({q})")
            }
            "bit" => format!("CAST({q} AS UNSIGNED)"),
            "decimal" => format!("CAST({q} AS CHAR)"),
            _ => q,
        }
    }
}

/// `CAST(JSON_OBJECT('c1', <c1>, …) AS CHAR)` over the journaled columns —
/// the row as JSON text.
fn row_json_expr(columns: &[JournalColumn]) -> String {
    let parts: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_generated())
        .map(|c| format!("'{}', {}", c.name.replace('\'', "''"), c.image_expr()))
        .collect();
    format!("CAST(JSON_OBJECT({}) AS CHAR)", parts.join(", "))
}

impl MysqlSink {
    /// `<table>__faucet_prev` — the replaced table an overwrite keeps.
    pub(crate) fn previous_table_name(&self) -> String {
        format!(
            "{}{}",
            self.config.table_name,
            faucet_core::rollback::PREVIOUS_TABLE_SUFFIX
        )
    }

    async fn columns_of(
        &self,
        conn: &mut MySqlConnection,
        table: &str,
    ) -> Result<Vec<String>, FaucetError> {
        let rows = sqlx::query(
            "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION",
        )
        .bind(table)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| sink_err("read table columns", e))?;
        Ok(rows
            .iter()
            .map(|r| r.get::<String, _>("COLUMN_NAME"))
            .collect())
    }

    /// The target's columns with their types, for the before-image.
    async fn journal_columns(
        &self,
        conn: &mut MySqlConnection,
    ) -> Result<Vec<JournalColumn>, FaucetError> {
        let rows = sqlx::query(
            "SELECT CAST(COLUMN_NAME AS CHAR) AS COLUMN_NAME, \
                    CAST(DATA_TYPE AS CHAR) AS DATA_TYPE, CAST(EXTRA AS CHAR) AS EXTRA \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION",
        )
        .bind(&self.config.table_name)
        .fetch_all(&mut *conn)
        .await
        .map_err(|e| sink_err("read table columns", e))?;
        Ok(rows
            .iter()
            .map(|r| JournalColumn {
                name: r.get("COLUMN_NAME"),
                data_type: r.get::<String, _>("DATA_TYPE").to_ascii_lowercase(),
                extra: r.get::<Option<String>, _>("EXTRA").unwrap_or_default(),
            })
            .collect())
    }

    /// Create the journal table. MySQL commits the open transaction on any
    /// `CREATE TABLE` — even `IF NOT EXISTS` on a table that exists — so this
    /// runs on its own connection **before** a write transaction begins
    /// (#789 SQL-74).
    pub(crate) async fn ensure_journal(&self) -> Result<(), FaucetError> {
        use std::sync::atomic::Ordering;
        if self.journal_ready.load(Ordering::Relaxed) {
            return Ok(());
        }
        sqlx::query(&journal_sql().create())
            .execute(&self.pool)
            .await
            .map_err(|e| sink_err("create journal", e))?;
        self.journal_ready.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Record the pre-run image of every key `plan` touches, on `conn`
    /// (inside the caller's transaction). A key already journaled for the run
    /// is skipped (the first page to touch a key holds its true before-image);
    /// a key with no current row is journaled with a null image so the restore
    /// deletes it.
    pub(crate) async fn journal_plan(
        &self,
        conn: &mut MySqlConnection,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        let key = &self.config.write.key;
        let keys = plan_keys(plan, key);
        if keys.is_empty() {
            return Ok(());
        }
        let table_ref = quote_ident_mysql(&self.config.table_name);
        let columns = self.journal_columns(&mut *conn).await?;
        let row_expr = row_json_expr(&columns);
        let sql = journal_sql();

        let per = (MAX_MYSQL_PARAMS / (key.len().max(1) * 4)).max(1);
        for chunk in keys.chunks(per) {
            // Attribute each current row to its key by position: re-deriving
            // the key from the row's text misses a row whose stored form
            // differs from the record's (a case-variant key under a `_ci`
            // collation), and a missed row would be journaled as created by
            // the run and deleted on rollback (#789 SQL-32).
            let select = positional_key_select(&table_ref, key, &row_expr, chunk.len());
            let mut q = sqlx::query(&select);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    q = bind_value(q, v);
                }
            }
            let rows = q
                .fetch_all(&mut *conn)
                .await
                .map_err(|e| sink_err("read before-images", e))?;
            let mut before: std::collections::HashMap<usize, String> =
                std::collections::HashMap::with_capacity(rows.len());
            for r in rows {
                let idx: i64 = r.get("__faucet_idx");
                let text: String = r.get("row_json");
                if let Ok(idx) = usize::try_from(idx) {
                    before.insert(idx, text);
                }
            }
            let insert = sql.insert(chunk.len());
            let mut q = sqlx::query(&insert);
            for (idx, kt) in chunk.iter().enumerate() {
                let kj = key_json(kt);
                let img = before.get(&idx).cloned();
                q = q
                    .bind(run_id)
                    .bind(&self.config.table_name)
                    .bind(kj)
                    .bind(img);
            }
            q.execute(&mut *conn)
                .await
                .map_err(|e| sink_err("write journal", e))?;
        }
        Ok(())
    }

    async fn journal_entries(
        &self,
        conn: &mut MySqlConnection,
        run_id: &str,
    ) -> Result<Vec<JournalEntry>, FaucetError> {
        let rows = sqlx::query(&journal_sql().select())
            .bind(run_id)
            .bind(&self.config.table_name)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| sink_err("read journal", e))?;
        rows.iter()
            .map(|r| {
                let key: String = r.get(0);
                let before: Option<String> = r.get(1);
                JournalEntry::decode(&key, before.as_deref())
            })
            .collect()
    }

    /// Journaled keys whose current row was written by another run since.
    async fn count_conflicts(
        &self,
        conn: &mut MySqlConnection,
        entries: &[JournalEntry],
        run_id: &str,
        run_col: &str,
    ) -> Result<u64, FaucetError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let columns = self.columns_of(&mut *conn, &self.config.table_name).await?;
        if !columns.iter().any(|c| c == run_col) {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident_mysql(&self.config.table_name);
        let sql = journal_sql();
        let per = ((MAX_MYSQL_PARAMS - 1) / key.len().max(1)).max(1);
        let mut total = 0u64;
        for chunk in entries.chunks(per) {
            let (predicate, _) = sql.keys_in(key, chunk.len(), 0);
            let count_sql = format!(
                "SELECT count(*) AS n FROM {table_ref} WHERE {predicate} AND NOT ({c} <=> ?)",
                c = quote_ident_mysql(run_col)
            );
            let mut q = sqlx::query(&count_sql);
            for e in chunk {
                for (_, v) in &e.tuple(key).0 {
                    q = bind_value(q, v);
                }
            }
            q = q.bind(run_id);
            let row = q
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| sink_err("count conflicts", e))?;
            total += row.get::<i64, _>("n") as u64;
        }
        Ok(total)
    }

    /// Journaled keys with a before-image whose row is gone. Only called when
    /// a later run wrote the table: such a key was deleted by it, and restoring
    /// the before-image would resurrect it.
    async fn count_vanished(
        &self,
        conn: &mut MySqlConnection,
        entries: &[JournalEntry],
    ) -> Result<u64, FaucetError> {
        let restorable = faucet_core::rollback::restorable(entries);
        if restorable.is_empty() {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident_mysql(&self.config.table_name);
        let sql = journal_sql();
        let per = (MAX_MYSQL_PARAMS / key.len().max(1)).max(1);
        let mut present = 0u64;
        for chunk in restorable.chunks(per) {
            let (predicate, _) = sql.keys_in(key, chunk.len(), 0);
            let count_sql = format!("SELECT count(*) AS n FROM {table_ref} WHERE {predicate}");
            let mut q = sqlx::query(&count_sql);
            for e in chunk {
                for (_, v) in &e.tuple(key).0 {
                    q = bind_value(q, v);
                }
            }
            let row = q
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| sink_err("count vanished keys", e))?;
            present += row.get::<i64, _>("n") as u64;
        }
        Ok((restorable.len() as u64).saturating_sub(present))
    }

    pub(crate) fn rollback_supported(&self) -> bool {
        matches!(self.config.column_mapping, MysqlColumnMapping::AutoMap)
    }

    pub(crate) async fn rollback_run_impl(
        &self,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if !self.rollback_supported() {
            return Err(FaucetError::Sink(
                "mysql rollback requires column_mapping: auto_map".into(),
            ));
        }
        if !self.table_exists(&self.config.table_name).await? {
            return Ok(RollbackOutcome::nothing(format!(
                "table {} does not exist",
                self.config.table_name
            )));
        }
        if matches!(opts.mode, RollbackMode::Overwrite) {
            let mut conn = self
                .pool
                .acquire()
                .await
                .map_err(|e| sink_err("acquire", e))?;
            return self.rollback_overwrite(&mut conn, run_id, opts).await;
        }
        self.ensure_journal().await?;
        let mut tx = self.pool.begin().await.map_err(|e| sink_err("begin", e))?;
        let outcome = match opts.mode {
            RollbackMode::Append => self.rollback_append(&mut tx, run_id, opts).await?,
            RollbackMode::Upsert | RollbackMode::Overwrite => {
                self.rollback_upsert(&mut tx, run_id, opts).await?
            }
        };
        if outcome.applied && !opts.dry_run {
            tx.commit().await.map_err(|e| sink_err("commit", e))?;
        } else {
            tx.rollback()
                .await
                .map_err(|e| sink_err("rollback tx", e))?;
        }
        Ok(outcome)
    }

    async fn rollback_append(
        &self,
        conn: &mut MySqlConnection,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        let col = &opts.run_id_column;
        let columns = self.columns_of(&mut *conn, &self.config.table_name).await?;
        if !columns.iter().any(|c| c == col) {
            return Err(FaucetError::Sink(format!(
                "mysql rollback: table {} has no {col} column, so the run's rows cannot be \
                 identified (enable metadata_columns with run_id, or use the journal for upserts)",
                self.config.table_name
            )));
        }
        let sql = journal_sql();
        let table_ref = quote_ident_mysql(&self.config.table_name);
        let count: i64 = sqlx::query_scalar(&sql.count_by_run(&table_ref, col))
            .bind(run_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| sink_err("count run rows", e))?;
        if count == 0 {
            return Ok(RollbackOutcome::nothing("no rows carry this run id"));
        }
        if opts.dry_run {
            return Ok(RollbackOutcome {
                deleted: count as u64,
                applied: false,
                note: Some("dry run".into()),
                ..Default::default()
            });
        }
        let res = sqlx::query(&sql.delete_by_run(&table_ref, col))
            .bind(run_id)
            .execute(&mut *conn)
            .await
            .map_err(|e| sink_err("delete run rows", e))?;
        Ok(RollbackOutcome {
            deleted: res.rows_affected(),
            applied: true,
            ..Default::default()
        })
    }

    async fn rollback_upsert(
        &self,
        conn: &mut MySqlConnection,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if self.config.write.key.is_empty() {
            return Err(FaucetError::Sink(
                "mysql rollback: the sink config has no `key`, so journaled keys cannot be \
                 addressed"
                    .into(),
            ));
        }
        let entries = self.journal_entries(&mut *conn, run_id).await?;
        if entries.is_empty() {
            return Ok(RollbackOutcome::nothing(
                "no journal rows for this run (was `rollback.journal` enabled when it ran?)",
            ));
        }
        let mut conflicts = self
            .count_conflicts(&mut *conn, &entries, run_id, &opts.run_id_column)
            .await?;
        if opts.later_runs {
            conflicts += self.count_vanished(&mut *conn, &entries).await?;
        }
        if conflicts > 0 && !opts.force {
            return Ok(RollbackOutcome::blocked(conflicts));
        }
        let (deletes, restores) = plan_restore(&entries);
        if opts.dry_run {
            return Ok(RollbackOutcome {
                deleted: deletes.len() as u64,
                restored: restores.len() as u64,
                conflicts,
                applied: false,
                note: Some("dry run".into()),
            });
        }
        let key = &self.config.write.key;
        let tuples: Vec<faucet_core::KeyTuple> = deletes.iter().map(|e| e.tuple(key)).collect();
        let deleted = self.delete_by_keys(&mut *conn, &tuples).await? as u64;
        let restored = if restores.is_empty() {
            0
        } else {
            self.insert_auto_map_with_conflict(&mut *conn, &restores, Some(key))
                .await? as u64
        };
        sqlx::query(&journal_sql().delete_table())
            .bind(run_id)
            .bind(&self.config.table_name)
            .execute(&mut *conn)
            .await
            .map_err(|e| sink_err("clear journal", e))?;
        Ok(RollbackOutcome {
            deleted,
            restored,
            conflicts,
            applied: true,
            note: None,
        })
    }

    /// Swap the kept previous table back with one atomic `RENAME TABLE`.
    async fn rollback_overwrite(
        &self,
        conn: &mut MySqlConnection,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if !self.table_exists(&self.previous_table_name()).await? {
            return Err(FaucetError::Sink(format!(
                "mysql rollback: no previous copy {} of {} exists (was `rollback.keep_previous` \
                 enabled when the run overwrote it?)",
                self.previous_table_name(),
                self.config.table_name
            )));
        }
        let target = quote_ident_mysql(&self.config.table_name);
        let prev = quote_ident_mysql(&self.previous_table_name());
        let old = quote_ident_mysql(&self.old_table_name());
        let col = &opts.run_id_column;
        let columns = self.columns_of(&mut *conn, &self.config.table_name).await?;
        // The kept table is the image before the *latest* overwrite; if the
        // target no longer holds this run's rows, a later run replaced them.
        let conflicts: i64 = if columns.iter().any(|c| c == col) {
            sqlx::query_scalar(&format!(
                "SELECT count(*) FROM {target} WHERE NOT ({c} <=> ?)",
                c = quote_ident_mysql(col)
            ))
            .bind(run_id)
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| sink_err("count conflicts", e))?
        } else {
            0
        };
        if conflicts > 0 && !opts.force {
            return Ok(RollbackOutcome::blocked(conflicts as u64));
        }
        let restored: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {prev}"))
            .fetch_one(&mut *conn)
            .await
            .map_err(|e| sink_err("count previous rows", e))?;
        if opts.dry_run {
            return Ok(RollbackOutcome {
                restored: restored as u64,
                conflicts: conflicts as u64,
                applied: false,
                note: Some("dry run".into()),
                ..Default::default()
            });
        }
        // A target with foreign keys or triggers gets its rows back in place,
        // keeping its definition (#789 SQL-108).
        let deps = crate::sink::table_dependents(&mut *conn, &self.config.table_name).await?;
        if !deps.pinned.is_empty() || !deps.referenced_by.is_empty() {
            crate::sink::replace_rows_in_place(
                &mut *conn,
                &self.config.table_name,
                &self.previous_table_name(),
            )
            .await?;
            sqlx::query(&format!("DROP TABLE IF EXISTS {prev}"))
                .execute(&mut *conn)
                .await
                .map_err(|e| sink_err("drop previous table", e))?;
            return Ok(RollbackOutcome {
                restored: restored as u64,
                conflicts: conflicts as u64,
                applied: true,
                ..Default::default()
            });
        }
        for stmt in [
            format!("DROP TABLE IF EXISTS {old}"),
            format!("RENAME TABLE {target} TO {old}, {prev} TO {target}"),
            format!("DROP TABLE IF EXISTS {old}"),
        ] {
            sqlx::query(&stmt)
                .execute(&mut *conn)
                .await
                .map_err(|e| sink_err("restore previous table", e))?;
        }
        Ok(RollbackOutcome {
            restored: restored as u64,
            conflicts: conflicts as u64,
            applied: true,
            ..Default::default()
        })
    }

    /// Drop the journal rows of `run_id` for this table.
    pub(crate) async fn forget_run_impl(&self, run_id: &str) -> Result<(), FaucetError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| sink_err("acquire", e))?;
        self.ensure_journal().await?;
        sqlx::query(&journal_sql().delete_table())
            .bind(run_id)
            .bind(&self.config.table_name)
            .execute(&mut *conn)
            .await
            .map_err(|e| sink_err("forget run", e))?;
        Ok(())
    }

    /// Set (or clear) the exactly-once watermark for `scope`.
    pub(crate) async fn rewind_commit_token_impl(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.ensure_commit_table().await?;
        let t = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TABLE);
        let s = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL);
        let k = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL);
        match token {
            Some(token) => sqlx::query(&format!(
                "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON DUPLICATE KEY UPDATE {k} = VALUES({k})"
            ))
            .bind(scope_key(scope))
            .bind(token)
            .execute(&self.pool)
            .await,
            None => {
                sqlx::query(&format!("DELETE FROM {t} WHERE {s} = ?"))
                    .bind(scope_key(scope))
                    .execute(&self.pool)
                    .await
            }
        }
        .map_err(|e| sink_err("rewind commit token", e))?;
        Ok(())
    }

    /// The `mysql` source config that reads this destination back.
    pub(crate) fn readback_source_impl(&self) -> Option<(String, Value)> {
        if !self.rollback_supported() {
            return None;
        }
        Some((
            "mysql".to_string(),
            serde_json::json!({
                "connection_url": self.config.connection_url,
                "query": format!("SELECT * FROM {}", quote_ident_mysql(&self.config.table_name)),
                "max_connections": 2,
            }),
        ))
    }
}

/// `SELECT idx, <row json>` over a `UNION ALL` list of `(idx, key…)` joined
/// to the target, so each current row comes back tagged with the position of
/// the key that matched it.
fn positional_key_select(table_ref: &str, key: &[String], row_expr: &str, rows: usize) -> String {
    let keys: Vec<String> = (0..rows)
        .map(|idx| {
            let cells: Vec<String> = std::iter::once(format!("{idx} AS __faucet_idx"))
                .chain((0..key.len()).map(|i| format!("? AS __faucet_k{i}")))
                .collect();
            format!("SELECT {}", cells.join(", "))
        })
        .collect();
    let join: Vec<String> = key
        .iter()
        .enumerate()
        .map(|(i, k)| format!("t.{} = v.__faucet_k{i}", quote_ident_mysql(k)))
        .collect();
    format!(
        "SELECT v.__faucet_idx, {row_expr} AS row_json FROM ({}) AS v \
         JOIN {table_ref} AS t ON {}",
        keys.join(" UNION ALL "),
        join.join(" AND ")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_json_expr_quotes_names_and_identifiers() {
        let col = |name: &str, data_type: &str, extra: &str| JournalColumn {
            name: name.into(),
            data_type: data_type.into(),
            extra: extra.into(),
        };
        assert_eq!(
            row_json_expr(&[col("id", "int", ""), col("o'k", "varchar", "")]),
            "CAST(JSON_OBJECT('id', `id`, 'o''k', `o'k`) AS CHAR)"
        );
        assert_eq!(
            row_json_expr(&[
                col("b", "varbinary", ""),
                col("f", "bit", ""),
                col("d", "decimal", ""),
                col("g", "int", "STORED GENERATED"),
                col("ts", "timestamp", "DEFAULT_GENERATED"),
            ]),
            "CAST(JSON_OBJECT('b', TO_BASE64(`b`), 'f', CAST(`f` AS UNSIGNED), \
             'd', CAST(`d` AS CHAR), 'ts', `ts`) AS CHAR)"
        );
    }

    #[test]
    fn journal_ddl_uses_mysql_spellings() {
        let sql = journal_sql();
        assert!(
            sql.create().contains("before_json LONGTEXT"),
            "{}",
            sql.create()
        );
        assert!(
            sql.create()
                .contains("PRIMARY KEY (run_id, table_name, key_hash)"),
            "{}",
            sql.create()
        );
        assert!(sql.create().contains("SHA2(key_json, 256)"));
        assert!(
            sql.insert(1).starts_with("INSERT IGNORE INTO"),
            "{}",
            sql.insert(1)
        );
        assert!(sql.insert(2).ends_with("(?, ?, ?, ?), (?, ?, ?, ?)"));
    }
}

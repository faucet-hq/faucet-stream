//! Run rollback for the SQLite sink (#706): the before-image journal written
//! alongside upserts/deletes, the kept previous table of an overwrite, and the
//! per-mode undo. Every undo runs in one `BEGIN IMMEDIATE` transaction, so a
//! failure mid-restore leaves the destination exactly as it was.

use crate::config::SqliteColumnMapping;
use crate::sink::{BEGIN_WRITE, SqliteSink};
use faucet_core::FaucetError;
use faucet_core::rollback::{
    JournalEntry, JournalSql, RollbackMode, RollbackOptions, RollbackOutcome, canonical_key,
    key_json, plan_keys, plan_restore,
};
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::Row;

type Tx<'c> = sqlx::Transaction<'c, sqlx::Sqlite>;

/// SQLite's default bind-variable ceiling.
const MAX_SQLITE_PARAMS: usize = 32766;

fn placeholder(_n: usize) -> String {
    "?".to_string()
}

fn journal_sql() -> JournalSql {
    JournalSql {
        quote: quote_ident,
        placeholder,
        before_type: "TEXT",
        insert_prefix: "INSERT OR IGNORE",
        insert_suffix: "",
        now: "(datetime('now'))",
        key_column: faucet_core::rollback::KEY_COLUMN_TEXT,
        primary_key: faucet_core::rollback::PRIMARY_KEY_TEXT,
    }
}

fn sink_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("sqlite rollback: {context}: {e}"))
}

/// `json_object('c1', "c1", …)` over `columns` — the row as JSON text.
fn row_json_expr(columns: &[String]) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|c| format!("'{}', {}", c.replace('\'', "''"), quote_ident(c)))
        .collect();
    format!("json_object({})", parts.join(", "))
}

impl SqliteSink {
    /// `<table>__faucet_prev` — the replaced table an overwrite keeps.
    pub(crate) fn previous_table(&self) -> String {
        format!(
            "{}{}",
            self.config.table_name,
            faucet_core::rollback::PREVIOUS_TABLE_SUFFIX
        )
    }

    async fn columns_of(&self, tx: &mut Tx<'_>, table: &str) -> Result<Vec<String>, FaucetError> {
        let rows = sqlx::query("SELECT name FROM pragma_table_info(?) ORDER BY cid")
            .bind(table)
            .fetch_all(&mut **tx)
            .await
            .map_err(|e| sink_err("read table columns", e))?;
        Ok(rows.iter().map(|r| r.get::<String, _>("name")).collect())
    }

    async fn ensure_journal(&self, tx: &mut Tx<'_>) -> Result<(), FaucetError> {
        sqlx::query(&journal_sql().create())
            .execute(&mut **tx)
            .await
            .map_err(|e| sink_err("create journal", e))?;
        Ok(())
    }

    /// Record the pre-run image of every key `plan` touches, inside the
    /// caller's transaction. A key already journaled for the run is skipped
    /// (the first page to touch a key holds its true before-image); a key with
    /// no current row is journaled with a null image so the restore deletes it.
    pub(crate) async fn journal_plan(
        &self,
        tx: &mut Tx<'_>,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        let key = &self.config.write.key;
        let keys = plan_keys(plan, key);
        if keys.is_empty() {
            return Ok(());
        }
        self.ensure_journal(tx).await?;
        let table_ref = quote_ident(&self.config.table_name);
        let columns = self.columns_of(tx, &self.config.table_name).await?;
        let row_expr = row_json_expr(&columns);
        let sql = journal_sql();

        let per = (MAX_SQLITE_PARAMS / (key.len().max(1) * 4)).max(1);
        for chunk in keys.chunks(per) {
            let (predicate, _) = sql.keys_in(key, chunk.len(), 0);
            let select = format!("SELECT {row_expr} FROM {table_ref} WHERE {predicate}");
            let mut q = sqlx::query_scalar::<_, String>(&select);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    q = bind_key_value(q, v);
                }
            }
            let rows = q
                .fetch_all(&mut **tx)
                .await
                .map_err(|e| sink_err("read before-images", e))?;
            let mut before: std::collections::HashMap<String, String> =
                std::collections::HashMap::with_capacity(rows.len());
            for text in rows {
                let row: Value =
                    serde_json::from_str(&text).map_err(|e| sink_err("decode before-image", e))?;
                if let Some(kt) = faucet_core::write_mode::record_key(&row, key) {
                    before.insert(key_json(&canonical_key(&kt)), text);
                }
            }
            let insert = sql.insert(chunk.len());
            let mut q = sqlx::query(&insert);
            for kt in chunk {
                let kj = key_json(kt);
                let img = before.get(&kj).cloned();
                q = q
                    .bind(run_id)
                    .bind(&self.config.table_name)
                    .bind(kj)
                    .bind(img);
            }
            q.execute(&mut **tx)
                .await
                .map_err(|e| sink_err("write journal", e))?;
        }
        Ok(())
    }

    async fn journal_entries(
        &self,
        tx: &mut Tx<'_>,
        run_id: &str,
    ) -> Result<Vec<JournalEntry>, FaucetError> {
        let rows = sqlx::query(&journal_sql().select())
            .bind(run_id)
            .bind(&self.config.table_name)
            .fetch_all(&mut **tx)
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
        tx: &mut Tx<'_>,
        entries: &[JournalEntry],
        run_id: &str,
        run_col: &str,
    ) -> Result<u64, FaucetError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let columns = self.columns_of(tx, &self.config.table_name).await?;
        if !columns.iter().any(|c| c == run_col) {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident(&self.config.table_name);
        let sql = journal_sql();
        let per = ((MAX_SQLITE_PARAMS - 1) / key.len().max(1)).max(1);
        let mut total = 0u64;
        for chunk in entries.chunks(per) {
            let (predicate, _) = sql.keys_in(key, chunk.len(), 0);
            let count_sql = format!(
                "SELECT count(*) FROM {table_ref} WHERE {predicate} AND {c} IS NOT ?",
                c = quote_ident(run_col)
            );
            let mut q = sqlx::query_scalar::<_, i64>(&count_sql);
            for e in chunk {
                for (_, v) in &e.tuple(key).0 {
                    q = bind_key_value_scalar(q, v);
                }
            }
            q = q.bind(run_id);
            total += q
                .fetch_one(&mut **tx)
                .await
                .map_err(|e| sink_err("count conflicts", e))? as u64;
        }
        Ok(total)
    }

    pub(crate) fn rollback_supported(&self) -> bool {
        matches!(self.config.column_mapping, SqliteColumnMapping::AutoMap)
    }

    pub(crate) async fn rollback_run_impl(
        &self,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if !self.rollback_supported() {
            return Err(FaucetError::Sink(
                "sqlite rollback requires column_mapping: auto_map".into(),
            ));
        }
        if !self.table_exists(&self.config.table_name).await? {
            return Ok(RollbackOutcome::nothing(format!(
                "table {} does not exist",
                self.config.table_name
            )));
        }
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| sink_err("begin", e))?;
        let outcome = match opts.mode {
            RollbackMode::Append => self.rollback_append(&mut tx, run_id, opts).await?,
            RollbackMode::Upsert => self.rollback_upsert(&mut tx, run_id, opts).await?,
            RollbackMode::Overwrite => self.rollback_overwrite(&mut tx, run_id, opts).await?,
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
        tx: &mut Tx<'_>,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        let col = &opts.run_id_column;
        let columns = self.columns_of(tx, &self.config.table_name).await?;
        if !columns.iter().any(|c| c == col) {
            return Err(FaucetError::Sink(format!(
                "sqlite rollback: table {} has no {col} column, so the run's rows cannot be \
                 identified (enable metadata_columns with run_id, or use the journal for upserts)",
                self.config.table_name
            )));
        }
        let sql = journal_sql();
        let table_ref = quote_ident(&self.config.table_name);
        let count: i64 = sqlx::query_scalar(&sql.count_by_run(&table_ref, col))
            .bind(run_id)
            .fetch_one(&mut **tx)
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
            .execute(&mut **tx)
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
        tx: &mut Tx<'_>,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if self.config.write.key.is_empty() {
            return Err(FaucetError::Sink(
                "sqlite rollback: the sink config has no `key`, so journaled keys cannot be \
                 addressed"
                    .into(),
            ));
        }
        self.ensure_journal(tx).await?;
        let entries = self.journal_entries(tx, run_id).await?;
        if entries.is_empty() {
            return Ok(RollbackOutcome::nothing(
                "no journal rows for this run (was `rollback.journal` enabled when it ran?)",
            ));
        }
        let conflicts = self
            .count_conflicts(tx, &entries, run_id, &opts.run_id_column)
            .await?;
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
        let deleted = self.delete_by_keys(tx, &tuples).await? as u64;
        let restored = if restores.is_empty() {
            0
        } else {
            self.insert_auto_map_with_conflict_tx(tx, &restores, Some(key))
                .await? as u64
        };
        sqlx::query(&journal_sql().delete_table())
            .bind(run_id)
            .bind(&self.config.table_name)
            .execute(&mut **tx)
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

    async fn rollback_overwrite(
        &self,
        tx: &mut Tx<'_>,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        // Probe on the transaction's own connection: with the default
        // single-connection pool a query sent to `&self.pool` would wait
        // forever for the connection this transaction holds.
        let prev_exists: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        )
        .bind(self.previous_table())
        .fetch_one(&mut **tx)
        .await
        .map_err(|e| sink_err("probe previous table", e))?;
        if prev_exists == 0 {
            return Err(FaucetError::Sink(format!(
                "sqlite rollback: no previous copy {} of {} exists (was `rollback.keep_previous` \
                 enabled when the run overwrote it?)",
                self.previous_table(),
                self.config.table_name
            )));
        }
        let target = quote_ident(&self.config.table_name);
        let prev = quote_ident(&self.previous_table());
        let col = &opts.run_id_column;
        let columns = self.columns_of(tx, &self.config.table_name).await?;
        // The kept copy is the image before the *latest* overwrite; if the
        // target no longer holds this run's rows, a later run replaced them.
        let conflicts: i64 = if columns.iter().any(|c| c == col) {
            sqlx::query_scalar(&format!(
                "SELECT count(*) FROM {target} WHERE {c} IS NOT ?",
                c = quote_ident(col)
            ))
            .bind(run_id)
            .fetch_one(&mut **tx)
            .await
            .map_err(|e| sink_err("count conflicts", e))?
        } else {
            0
        };
        if conflicts > 0 && !opts.force {
            return Ok(RollbackOutcome::blocked(conflicts as u64));
        }
        let restored: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {prev}"))
            .fetch_one(&mut **tx)
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
        // Column list from the kept copy: the target may have gained columns
        // since (schema evolution), and a bare `SELECT *` would misalign.
        let cols = self
            .columns_of(tx, &self.previous_table())
            .await?
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        for stmt in [
            format!("DELETE FROM {target}"),
            format!("INSERT INTO {target} ({cols}) SELECT {cols} FROM {prev}"),
            format!("DROP TABLE {prev}"),
        ] {
            sqlx::query(&stmt)
                .execute(&mut **tx)
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
        let mut tx = self
            .pool
            .begin_with(BEGIN_WRITE)
            .await
            .map_err(|e| sink_err("begin", e))?;
        self.ensure_journal(&mut tx).await?;
        sqlx::query(&journal_sql().delete_table())
            .bind(run_id)
            .bind(&self.config.table_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| sink_err("forget run", e))?;
        tx.commit().await.map_err(|e| sink_err("commit", e))?;
        Ok(())
    }

    /// Set (or clear) the exactly-once watermark for `scope`.
    pub(crate) async fn rewind_commit_token_impl(
        &self,
        scope: &str,
        token: Option<&str>,
    ) -> Result<(), FaucetError> {
        self.ensure_commit_table().await?;
        let t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE);
        let s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL);
        let k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL);
        match token {
            Some(token) => {
                sqlx::query(&format!(
                    "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON CONFLICT({s}) DO UPDATE SET {k} = excluded.{k}, updated_at = datetime('now')"
                ))
                .bind(scope)
                .bind(token)
                .execute(&self.pool)
                .await
            }
            None => {
                sqlx::query(&format!("DELETE FROM {t} WHERE {s} = ?"))
                    .bind(scope)
                    .execute(&self.pool)
                    .await
            }
        }
        .map_err(|e| sink_err("rewind commit token", e))?;
        Ok(())
    }

    /// The `sqlite` source config that reads this destination back.
    pub(crate) fn readback_source_impl(&self) -> Option<(String, Value)> {
        if !self.rollback_supported() {
            return None;
        }
        Some((
            "sqlite".to_string(),
            serde_json::json!({
                "database_url": self.config.database_url,
                "query": format!("SELECT * FROM {}", quote_ident(&self.config.table_name)),
                "max_connections": 1,
            }),
        ))
    }

    /// Inside the overwrite swap: keep a copy of the target as
    /// `<table>__faucet_prev` (replacing an older copy) before it is cleared.
    pub(crate) async fn keep_previous_copy(&self, tx: &mut Tx<'_>) -> Result<(), FaucetError> {
        let target = quote_ident(&self.config.table_name);
        let prev = quote_ident(&self.previous_table());
        for stmt in [
            format!("DROP TABLE IF EXISTS {prev}"),
            format!("CREATE TABLE {prev} AS SELECT * FROM {target}"),
        ] {
            sqlx::query(&stmt)
                .execute(&mut **tx)
                .await
                .map_err(|e| sink_err("keep previous copy", e))?;
        }
        Ok(())
    }
}

/// Bind a canonical (text) key value; SQLite's column affinity converts the
/// text back to the column's storage class for the comparison.
fn bind_key_value<'q>(
    q: sqlx::query::QueryScalar<'q, sqlx::Sqlite, String, sqlx::sqlite::SqliteArguments<'q>>,
    v: &Value,
) -> sqlx::query::QueryScalar<'q, sqlx::Sqlite, String, sqlx::sqlite::SqliteArguments<'q>> {
    match v {
        Value::Null => q.bind(None::<String>),
        Value::String(s) => q.bind(s.clone()),
        other => q.bind(other.to_string()),
    }
}

fn bind_key_value_scalar<'q>(
    q: sqlx::query::QueryScalar<'q, sqlx::Sqlite, i64, sqlx::sqlite::SqliteArguments<'q>>,
    v: &Value,
) -> sqlx::query::QueryScalar<'q, sqlx::Sqlite, i64, sqlx::sqlite::SqliteArguments<'q>> {
    match v {
        Value::Null => q.bind(None::<String>),
        Value::String(s) => q.bind(s.clone()),
        other => q.bind(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_json_expr_quotes_names_and_identifiers() {
        assert_eq!(
            row_json_expr(&["id".into(), "o'k".into()]),
            "json_object('id', \"id\", 'o''k', \"o'k\")"
        );
    }

    #[test]
    fn journal_ddl_uses_sqlite_spellings() {
        let sql = journal_sql();
        assert!(
            sql.create().contains("DEFAULT (datetime('now'))"),
            "{}",
            sql.create()
        );
        assert!(
            sql.insert(1).starts_with("INSERT OR IGNORE INTO"),
            "{}",
            sql.insert(1)
        );
        assert!(sql.insert(1).ends_with("(?, ?, ?, ?)"));
    }
}

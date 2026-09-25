//! Run rollback for the PostgreSQL sink (#706): the before-image journal
//! written alongside upserts/deletes, the kept previous table of an overwrite,
//! and the per-mode undo.
//!
//! Every undo is one transaction — Postgres runs `TRUNCATE` and DDL
//! transactionally, so a failure mid-restore leaves the destination exactly as
//! it was. The conflict guard reads the run-id column the metadata decorator
//! stamps: a journaled key whose current row carries a *different* run id was
//! changed by a later run and blocks the rollback unless `force` is set.

use crate::config::PostgresColumnMapping;
use crate::sink::{PostgresSink, pg_bind_text, qualified_table_ref};
use faucet_core::FaucetError;
use faucet_core::rollback::{
    JournalEntry, JournalSql, RollbackMode, RollbackOptions, RollbackOutcome, canonical_key,
    key_json, plan_keys, plan_restore,
};
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::Row;

const MAX_PG_PARAMS: usize = 65535;

fn pg_placeholder(n: usize) -> String {
    format!("${n}")
}

fn journal_sql() -> JournalSql {
    JournalSql {
        quote: quote_ident,
        placeholder: pg_placeholder,
        before_type: "JSONB",
        insert_prefix: "INSERT",
        insert_suffix: " ON CONFLICT DO NOTHING",
        now: "now()",
        key_column: faucet_core::rollback::KEY_COLUMN_TEXT,
        primary_key: faucet_core::rollback::PRIMARY_KEY_TEXT,
    }
}

fn sink_err(context: &str, e: impl std::fmt::Display) -> FaucetError {
    FaucetError::Sink(format!("postgres rollback: {context}: {e}"))
}

impl PostgresSink {
    /// `<table>__faucet_prev` — the replaced table an overwrite keeps.
    pub(crate) fn previous_table_name(&self) -> String {
        format!(
            "{}{}",
            self.config.table_name,
            faucet_core::rollback::PREVIOUS_TABLE_SUFFIX
        )
    }

    fn target_ref(&self) -> String {
        qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name)
    }

    fn previous_ref(&self) -> String {
        qualified_table_ref(self.config.schema.as_deref(), &self.previous_table_name())
    }

    /// The journal lives next to the target (same schema), so one database
    /// can hold several journaled destinations.
    fn journal_ref(&self) -> String {
        qualified_table_ref(
            self.config.schema.as_deref(),
            faucet_core::rollback::RUN_JOURNAL_TABLE,
        )
    }

    async fn ensure_journal(&self, conn: &mut sqlx::PgConnection) -> Result<(), FaucetError> {
        let sql = journal_sql().create().replace(
            &quote_ident(faucet_core::rollback::RUN_JOURNAL_TABLE),
            &self.journal_ref(),
        );
        sqlx::query(&sql)
            .execute(&mut *conn)
            .await
            .map_err(|e| sink_err("create journal", e))?;
        Ok(())
    }

    /// Record the pre-run image of every key `plan` touches, on `conn`
    /// (inside the caller's transaction). A key the run already journaled is
    /// skipped — the first page to touch a key holds its true before-image; a
    /// key with no current row is journaled with a null image so the restore
    /// deletes it.
    pub(crate) async fn journal_plan(
        &self,
        conn: &mut sqlx::PgConnection,
        plan: &faucet_core::WritePlan,
        run_id: &str,
    ) -> Result<(), FaucetError> {
        let key = &self.config.write.key;
        let keys = plan_keys(plan, key);
        if keys.is_empty() {
            return Ok(());
        }
        self.ensure_journal(&mut *conn).await?;
        let table_ref = self.target_ref();
        let udts: std::collections::HashMap<String, String> = self
            .discover_columns(&mut *conn, &table_ref)
            .await?
            .into_iter()
            .collect();
        let key_udts: Vec<String> = key
            .iter()
            .map(|k| udts.get(k).cloned().unwrap_or_else(|| "text".to_string()))
            .collect();
        let sql = journal_sql();

        let per = (MAX_PG_PARAMS / key.len().max(1)).max(1);
        for chunk in keys.chunks(per) {
            // 1) Current rows for the chunk's keys, as JSON.
            let (predicate, _) = sql.keys_in(key, chunk.len(), 0);
            let predicate = cast_key_placeholders(&predicate, &key_udts, key.len());
            let select =
                format!("SELECT row_to_json(t)::text FROM {table_ref} AS t WHERE {predicate}");
            let mut q = sqlx::query_scalar::<_, String>(&select);
            for kt in chunk {
                for ((_, v), udt) in kt.0.iter().zip(key_udts.iter()) {
                    q = q.bind(pg_bind_text(Some(v), udt));
                }
            }
            let rows = q
                .fetch_all(&mut *conn)
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

            // 2) One journal row per key (null image = the run created it).
            let insert = sql.insert(chunk.len()).replace(
                &quote_ident(faucet_core::rollback::RUN_JOURNAL_TABLE),
                &self.journal_ref(),
            );
            let insert = cast_journal_placeholders(&insert, chunk.len());
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
            q.execute(&mut *conn)
                .await
                .map_err(|e| sink_err("write journal", e))?;
        }
        Ok(())
    }

    async fn journal_entries(
        &self,
        conn: &mut sqlx::PgConnection,
        run_id: &str,
    ) -> Result<Vec<JournalEntry>, FaucetError> {
        let select = journal_sql().select().replace(
            &quote_ident(faucet_core::rollback::RUN_JOURNAL_TABLE),
            &self.journal_ref(),
        );
        let rows = sqlx::query(&select)
            .bind(run_id)
            .bind(&self.config.table_name)
            .fetch_all(&mut *conn)
            .await
            .map_err(|e| sink_err("read journal", e))?;
        rows.iter()
            .map(|r| {
                let key: String = r.get(0);
                let before: Option<Value> = r.try_get(1).map_err(|e| sink_err("journal row", e))?;
                let before_text = before.map(|v| v.to_string());
                JournalEntry::decode(&key, before_text.as_deref())
            })
            .collect()
    }

    /// Whether `column` exists on the target (the run-id column may be absent
    /// when the run was not stamped).
    async fn has_column(
        &self,
        conn: &mut sqlx::PgConnection,
        column: &str,
    ) -> Result<bool, FaucetError> {
        let cols = self
            .discover_columns(&mut *conn, &self.target_ref())
            .await?;
        Ok(cols.iter().any(|(c, _)| c == column))
    }

    /// Journaled keys whose current row was written by another run since.
    async fn count_conflicts(
        &self,
        conn: &mut sqlx::PgConnection,
        entries: &[JournalEntry],
        run_id: &str,
        run_col: &str,
    ) -> Result<u64, FaucetError> {
        if entries.is_empty() || !self.has_column(&mut *conn, run_col).await? {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = self.target_ref();
        let udts: std::collections::HashMap<String, String> = self
            .discover_columns(&mut *conn, &table_ref)
            .await?
            .into_iter()
            .collect();
        let key_udts: Vec<String> = key
            .iter()
            .map(|k| udts.get(k).cloned().unwrap_or_else(|| "text".to_string()))
            .collect();
        let sql = journal_sql();
        let per = ((MAX_PG_PARAMS - 1) / key.len().max(1)).max(1);
        let mut total = 0u64;
        for chunk in entries.chunks(per) {
            let (predicate, next) = sql.keys_in(key, chunk.len(), 0);
            let predicate = cast_key_placeholders(&predicate, &key_udts, key.len());
            let count_sql = format!(
                "SELECT count(*) FROM {table_ref} WHERE {predicate} AND {c} IS DISTINCT FROM ${n}",
                c = quote_ident(run_col),
                n = next + 1
            );
            let mut q = sqlx::query_scalar::<_, i64>(&count_sql);
            for e in chunk {
                for ((_, v), udt) in e.tuple(key).0.iter().zip(key_udts.iter()) {
                    q = q.bind(pg_bind_text(Some(v), udt));
                }
            }
            q = q.bind(run_id);
            total += q
                .fetch_one(&mut *conn)
                .await
                .map_err(|e| sink_err("count conflicts", e))? as u64;
        }
        Ok(total)
    }

    pub(crate) fn rollback_supported(&self) -> bool {
        matches!(self.config.column_mapping, PostgresColumnMapping::AutoMap)
    }

    pub(crate) async fn rollback_run_impl(
        &self,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if !self.rollback_supported() {
            return Err(FaucetError::Sink(
                "postgres rollback requires column_mapping: auto_map".into(),
            ));
        }
        if !self.table_exists(&self.config.table_name).await? {
            return Ok(RollbackOutcome::nothing(format!(
                "table {} does not exist",
                self.config.table_name
            )));
        }
        let mut tx = self.pool.begin().await.map_err(|e| sink_err("begin", e))?;
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
        conn: &mut sqlx::PgConnection,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        let col = &opts.run_id_column;
        if !self.has_column(&mut *conn, col).await? {
            return Err(FaucetError::Sink(format!(
                "postgres rollback: table {} has no {col} column, so the run's rows cannot be \
                 identified (enable metadata_columns with run_id, or use the journal for upserts)",
                self.config.table_name
            )));
        }
        let sql = journal_sql();
        let table_ref = self.target_ref();
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
        conn: &mut sqlx::PgConnection,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if self.config.write.key.is_empty() {
            return Err(FaucetError::Sink(
                "postgres rollback: the sink config has no `key`, so journaled keys cannot be \
                 addressed"
                    .into(),
            ));
        }
        self.ensure_journal(&mut *conn).await?;
        let entries = self.journal_entries(&mut *conn, run_id).await?;
        if entries.is_empty() {
            return Ok(RollbackOutcome::nothing(
                "no journal rows for this run (was `rollback.journal` enabled when it ran?)",
            ));
        }
        let conflicts = self
            .count_conflicts(&mut *conn, &entries, run_id, &opts.run_id_column)
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
        let deleted = self.delete_by_keys(&mut *conn, &tuples).await? as u64;
        let restored = if restores.is_empty() {
            0
        } else {
            self.insert_auto_map_with_conflict(&mut *conn, &restores, Some(key))
                .await? as u64
        };
        let delete_journal = journal_sql().delete_table().replace(
            &quote_ident(faucet_core::rollback::RUN_JOURNAL_TABLE),
            &self.journal_ref(),
        );
        sqlx::query(&delete_journal)
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

    async fn rollback_overwrite(
        &self,
        conn: &mut sqlx::PgConnection,
        run_id: &str,
        opts: &RollbackOptions,
    ) -> Result<RollbackOutcome, FaucetError> {
        if !self.table_exists(&self.previous_table_name()).await? {
            return Err(FaucetError::Sink(format!(
                "postgres rollback: no previous copy {} of {} exists (was `rollback.keep_previous` \
                 enabled when the run overwrote it?)",
                self.previous_table_name(),
                self.config.table_name
            )));
        }
        let target = self.target_ref();
        let prev = self.previous_ref();
        let col = &opts.run_id_column;
        // The kept copy is the image before the *latest* overwrite; if the
        // target no longer holds this run's rows, a later run replaced them.
        let conflicts: i64 = if self.has_column(&mut *conn, col).await? {
            sqlx::query_scalar(&format!(
                "SELECT count(*) FROM {target} WHERE {c} IS DISTINCT FROM $1",
                c = quote_ident(col)
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
        // Column list from the kept copy: the target may have gained columns
        // since (schema evolution), and a bare `SELECT *` would misalign.
        let cols = self
            .discover_columns(&mut *conn, &prev)
            .await?
            .into_iter()
            .map(|(c, _)| quote_ident(&c))
            .collect::<Vec<_>>()
            .join(", ");
        for stmt in [
            format!("TRUNCATE TABLE {target}"),
            format!("INSERT INTO {target} ({cols}) SELECT {cols} FROM {prev}"),
            format!("DROP TABLE {prev}"),
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
        self.ensure_journal(&mut conn).await?;
        let delete_journal = journal_sql().delete_table().replace(
            &quote_ident(faucet_core::rollback::RUN_JOURNAL_TABLE),
            &self.journal_ref(),
        );
        sqlx::query(&delete_journal)
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
        let t = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE);
        let s = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL);
        let k = quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL);
        match token {
            Some(token) => {
                sqlx::query(&format!(
                    "INSERT INTO {t} ({s}, {k}) VALUES ($1, $2) ON CONFLICT ({s}) DO UPDATE SET {k} = EXCLUDED.{k}, updated_at = now()"
                ))
                .bind(scope)
                .bind(token)
                .execute(&self.pool)
                .await
            }
            None => {
                sqlx::query(&format!("DELETE FROM {t} WHERE {s} = $1"))
                    .bind(scope)
                    .execute(&self.pool)
                    .await
            }
        }
        .map_err(|e| sink_err("rewind commit token", e))?;
        Ok(())
    }

    /// The `postgres` source config that reads this destination back.
    pub(crate) fn readback_source_impl(&self) -> Option<(String, Value)> {
        if !self.rollback_supported() {
            return None;
        }
        Some((
            "postgres".to_string(),
            serde_json::json!({
                "connection_url": self.config.connection_url,
                "query": format!("SELECT * FROM {}", self.target_ref()),
                "max_connections": 2,
            }),
        ))
    }

    /// Inside the overwrite swap: keep a copy of the target as
    /// `<table>__faucet_prev` (replacing an older copy) before it is cleared.
    pub(crate) async fn keep_previous_copy(
        &self,
        conn: &mut sqlx::PgConnection,
    ) -> Result<(), FaucetError> {
        let target = self.target_ref();
        let prev = self.previous_ref();
        for stmt in [
            format!("DROP TABLE IF EXISTS {prev}"),
            format!("CREATE TABLE {prev} AS SELECT * FROM {target}"),
        ] {
            sqlx::query(&stmt)
                .execute(&mut *conn)
                .await
                .map_err(|e| sink_err("keep previous copy", e))?;
        }
        Ok(())
    }
}

/// Postgres binds every key value as text; `(k) IN (($1, $2), …)` needs each
/// placeholder cast to its column's type. Rewrites `$n` → `$n::udt` in a
/// `keys_in` predicate, cycling through the key columns' types.
fn cast_key_placeholders(predicate: &str, key_udts: &[String], key_len: usize) -> String {
    let mut out = String::with_capacity(predicate.len() + key_udts.len() * 8);
    let mut chars = predicate.chars().peekable();
    let mut idx = 0usize;
    while let Some(c) = chars.next() {
        out.push(c);
        if c == '$' {
            let mut digits = String::new();
            while let Some(d) = chars.peek().filter(|d| d.is_ascii_digit()) {
                digits.push(*d);
                chars.next();
            }
            out.push_str(&digits);
            if !digits.is_empty() {
                out.push_str("::");
                out.push_str(&key_udts[idx % key_len.max(1)]);
                idx += 1;
            }
        }
    }
    out
}

/// The journal insert binds `before_json` as text; cast it to `jsonb`.
fn cast_journal_placeholders(insert: &str, rows: usize) -> String {
    let mut out = insert.to_string();
    for row in 0..rows {
        let n = row * 4 + 4;
        out = out.replacen(&format!("${n})"), &format!("${n}::jsonb)"), 1);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_placeholders_get_column_casts() {
        let (pred, _) = journal_sql().keys_in(&["a".into(), "b".into()], 2, 0);
        let cast = cast_key_placeholders(&pred, &["int4".into(), "text".into()], 2);
        assert_eq!(
            cast,
            "(\"a\", \"b\") IN (($1::int4, $2::text), ($3::int4, $4::text))"
        );
    }

    #[test]
    fn journal_insert_casts_the_image_to_jsonb() {
        let sql = cast_journal_placeholders(&journal_sql().insert(2), 2);
        assert!(
            sql.contains("($1, $2, $3, $4::jsonb), ($5, $6, $7, $8::jsonb)"),
            "{sql}"
        );
        assert!(sql.ends_with(" ON CONFLICT DO NOTHING"));
    }
}

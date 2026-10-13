//! PostgreSQL.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::rollback::JournalSql;
use faucet_core::{FaucetError, SqlBaseType};

/// PostgreSQL: double-quoted identifiers, `$n::type` placeholders (the bound
/// text goes through the column type's input function), 65535 parameters,
/// `ON CONFLICT … DO UPDATE SET … = EXCLUDED.…`.
#[derive(Debug, Clone, Copy, Default)]
pub struct Postgres;

/// PostgreSQL's per-statement bind-parameter ceiling.
pub const PARAM_LIMIT: usize = 65535;

fn quote(name: &str) -> String {
    faucet_core::util::quote_ident(name)
}

fn placeholder(n: usize) -> String {
    format!("${n}")
}

impl SqlDialect for Postgres {
    fn name(&self) -> &'static str {
        "postgres"
    }

    fn quote_ident(&self, name: &str) -> String {
        quote(name)
    }

    fn placeholder(&self, n: usize, sql_type: Option<&str>) -> String {
        match sql_type {
            Some(t) => format!("${n}::{t}"),
            None => placeholder(n),
        }
    }

    fn param_limit(&self) -> usize {
        PARAM_LIMIT
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::Exact
    }

    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "bigint",
            SqlBaseType::Double => "double precision",
            SqlBaseType::Boolean => "boolean",
            SqlBaseType::Text => "text",
            SqlBaseType::Json => "jsonb",
        }
    }

    /// An integer column that starts receiving fractions becomes exact
    /// `numeric`, never `double precision`, which would round every stored
    /// value above 2^53.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "numeric",
            _ => self.type_name(to),
        }
    }

    fn upsert_tail(&self, key: &[String], columns: &[String]) -> Option<String> {
        let key_list = key.iter().map(|k| quote(k)).collect::<Vec<_>>().join(", ");
        let updates: Vec<String> = columns
            .iter()
            .filter(|c| !key.iter().any(|k| k == *c))
            .map(|c| format!("{q} = EXCLUDED.{q}", q = quote(c)))
            .collect();
        Some(if updates.is_empty() {
            format!("ON CONFLICT ({key_list}) DO NOTHING")
        } else {
            format!(
                "ON CONFLICT ({key_list}) DO UPDATE SET {}",
                updates.join(", ")
            )
        })
    }

    fn json_table_columns(&self, column: &str) -> Result<String, FaucetError> {
        Ok(format!(
            "{} bigserial PRIMARY KEY, {} jsonb NOT NULL",
            quote("id"),
            quote(column)
        ))
    }

    fn add_column_sql(
        &self,
        target: &TableTarget,
        column: &str,
        t: SqlBaseType,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
            target.reference,
            quote(column),
            self.type_name(t)
        ))
    }

    fn alter_column_type_sql(
        &self,
        target: &TableTarget,
        column: &str,
        type_name: &str,
    ) -> Result<Option<String>, FaucetError> {
        let q = quote(column);
        Ok(Some(format!(
            "ALTER TABLE {} ALTER COLUMN {q} TYPE {type_name} USING {q}::{type_name}",
            target.reference
        )))
    }

    fn drop_not_null_sql(
        &self,
        target: &TableTarget,
        column: &str,
    ) -> Result<Option<String>, FaucetError> {
        Ok(Some(format!(
            "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
            target.reference,
            quote(column)
        )))
    }

    /// `ON COMMIT DROP` scopes the table to the cleanup's transaction, so no
    /// drop is needed and concurrent cleanups never collide on the name.
    fn cleanup_create_sql(
        &self,
        _target: &TableTarget,
        key: &[String],
        key_types: &[Option<&str>],
    ) -> Result<String, FaucetError> {
        let cols = key
            .iter()
            .zip(key_types)
            .map(|(k, t)| format!("{} {}", quote(k), t.unwrap_or("text")))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(format!(
            "CREATE TEMP TABLE {} ({cols}) ON COMMIT DROP",
            self.cleanup_keys_table()
        ))
    }

    fn cleanup_delete_sql(
        &self,
        target: &TableTarget,
        scope: &[String],
        scope_types: &[Option<&str>],
        key: &[String],
    ) -> Result<String, FaucetError> {
        let scope_pred = scope
            .iter()
            .enumerate()
            .map(|(i, c)| {
                format!(
                    "t.{} = {}",
                    quote(c),
                    self.placeholder(i + 1, scope_types.get(i).copied().flatten())
                )
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        let join_pred = key
            .iter()
            .map(|k| {
                let q = quote(k);
                format!("c.{q} = t.{q}")
            })
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(format!(
            "DELETE FROM {} t WHERE {scope_pred} \
             AND NOT EXISTS (SELECT 1 FROM {} c WHERE {join_pred})",
            target.reference,
            self.cleanup_keys_table()
        ))
    }

    fn cleanup_table_display(&self, target: &TableTarget) -> String {
        target.reference.clone()
    }

    fn commit_token_table_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} TEXT PRIMARY KEY, {k} TEXT NOT NULL, updated_at TIMESTAMPTZ DEFAULT now())"
        )
    }

    fn commit_token_upsert_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "INSERT INTO {t} ({s}, {k}) VALUES ($1, $2) ON CONFLICT ({s}) DO UPDATE SET {k} = EXCLUDED.{k}, updated_at = now()"
        )
    }

    fn journal_sql(&self) -> Option<JournalSql> {
        Some(JournalSql {
            quote,
            placeholder,
            before_type: "JSONB",
            insert_prefix: "INSERT",
            insert_suffix: " ON CONFLICT DO NOTHING",
            now: "now()",
            key_column: faucet_core::rollback::KEY_COLUMN_TEXT,
            primary_key: faucet_core::rollback::PRIMARY_KEY_TEXT,
        })
    }

    fn no_matching_column_message(
        &self,
        index: usize,
        fields: &[&String],
        columns: &[&String],
        _table: &str,
    ) -> String {
        format!(
            "postgres: record {index} has no field matching a column of the target table \
             (record fields: {fields:?}; table columns: {columns:?}) — column names are case-sensitive"
        )
    }
}

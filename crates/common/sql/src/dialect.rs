//! The SQL a backend speaks.
//!
//! A [`SqlDialect`] answers every "how is this spelled here" question a SQL
//! sink has: identifier quoting, literal escaping, placeholders and the
//! parameter limit, type names for created and widened columns, the keyed
//! upsert and delete statements, the scoped-cleanup statements, the
//! commit-token watermark table and the rollback journal. Every statement
//! builder is a provided method over a few required ones, so a dialect
//! overrides only where its SQL actually differs.

use crate::mapping::ColumnMatching;
use faucet_core::rollback::JournalSql;
use faucet_core::{FaucetError, PlannedColumn, SqlBaseType};

/// The table a statement targets: its bare name and its quoted reference.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TableTarget {
    /// The table name, unquoted (used for catalog lookups and messages).
    pub name: String,
    /// The quoted, possibly schema-qualified reference used in SQL text.
    pub reference: String,
}

impl TableTarget {
    /// A target from its bare name and quoted reference.
    pub fn new(name: impl Into<String>, reference: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reference: reference.into(),
        }
    }
}

/// How one backend spells SQL. See the module docs.
pub trait SqlDialect: Send + Sync {
    /// The backend's short name, used in messages (`"postgres"`, `"sqlite"`).
    fn name(&self) -> &'static str;

    /// Quote an identifier, escaping the quote character.
    fn quote_ident(&self, name: &str) -> String;

    /// Refuse an identifier the dialect cannot express. The default accepts
    /// every name.
    fn check_ident(&self, name: &str) -> Result<(), FaucetError> {
        let _ = name;
        Ok(())
    }

    /// [`check_ident`](Self::check_ident) then [`quote_ident`](Self::quote_ident).
    fn quote(&self, name: &str) -> Result<String, FaucetError> {
        self.check_ident(name)?;
        Ok(self.quote_ident(name))
    }

    /// A string literal, single quotes doubled.
    fn quote_literal(&self, value: &str) -> String {
        format!("'{}'", value.replace('\'', "''"))
    }

    /// The `n`th placeholder (1-based), cast to `sql_type` when the dialect
    /// binds through a cast.
    fn placeholder(&self, n: usize, sql_type: Option<&str>) -> String;

    /// The most parameters one statement may bind.
    fn param_limit(&self) -> usize;

    /// Rows of `width` parameters one statement may carry.
    fn rows_per_statement(&self, width: usize) -> usize {
        (self.param_limit() / width.max(1)).max(1)
    }

    /// How a record field is compared with a column name.
    fn column_matching(&self) -> ColumnMatching;

    /// The column type a created or added column of base type `t` gets.
    fn type_name(&self, t: SqlBaseType) -> &'static str;

    /// The column type of a primary-key column of a created table. Defaults
    /// to [`type_name`](Self::type_name); a dialect that cannot index its
    /// unbounded text type returns a bounded one.
    fn key_type_name(&self, t: SqlBaseType) -> &'static str {
        self.type_name(t)
    }

    /// The type a column widens to when its values go from `from` to `to`.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        let _ = from;
        self.type_name(to)
    }

    /// The upsert tail appended to a multi-row `INSERT`, or `None` when the
    /// dialect upserts some other way (override
    /// [`upsert_sql`](Self::upsert_sql)).
    fn upsert_tail(&self, key: &[String], columns: &[String]) -> Option<String> {
        let _ = (key, columns);
        None
    }

    /// `rows` placeholder groups of `width`, numbered row-major from
    /// `start + 1`, each cast to its column's type in `types`.
    fn values_sql(
        &self,
        width: usize,
        types: &[Option<&str>],
        rows: usize,
        start: usize,
    ) -> String {
        (0..rows)
            .map(|r| {
                let phs: Vec<String> = (0..width)
                    .map(|c| {
                        self.placeholder(start + r * width + c + 1, types.get(c).copied().flatten())
                    })
                    .collect();
                format!("({})", phs.join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Quote and comma-join `columns`.
    fn column_list(&self, columns: &[String]) -> Result<String, FaucetError> {
        Ok(columns
            .iter()
            .map(|c| self.quote(c))
            .collect::<Result<Vec<_>, _>>()?
            .join(", "))
    }

    /// A multi-row `INSERT` of `rows` rows over `columns`.
    fn insert_sql(
        &self,
        table_ref: &str,
        columns: &[String],
        types: &[Option<&str>],
        rows: usize,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "INSERT INTO {table_ref} ({}) VALUES {}",
            self.column_list(columns)?,
            self.values_sql(columns.len(), types, rows, 0)
        ))
    }

    /// A multi-row keyed upsert of `rows` rows over `columns`.
    fn upsert_sql(
        &self,
        table_ref: &str,
        key: &[String],
        columns: &[String],
        types: &[Option<&str>],
        rows: usize,
    ) -> Result<String, FaucetError> {
        let insert = self.insert_sql(table_ref, columns, types, rows)?;
        match self.upsert_tail(key, columns) {
            Some(tail) => Ok(format!("{insert} {tail}")),
            None => Err(FaucetError::Config(format!(
                "{}: the dialect has no keyed upsert",
                self.name()
            ))),
        }
    }

    /// Delete the rows whose `key` equals one of `rows` key tuples.
    fn delete_keys_sql(
        &self,
        table_ref: &str,
        key: &[String],
        types: &[Option<&str>],
        rows: usize,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "DELETE FROM {table_ref} WHERE ({}) IN ({})",
            self.column_list(key)?,
            self.values_sql(key.len(), types, rows, 0)
        ))
    }

    /// `CREATE TABLE IF NOT EXISTS` for a table inferred from a page, with a
    /// primary key over `key` when it is non-empty.
    fn create_table_sql(
        &self,
        target: &TableTarget,
        columns: &[PlannedColumn],
        key: &[String],
    ) -> Result<String, FaucetError> {
        let mut defs: Vec<String> = Vec::with_capacity(columns.len() + 1);
        for c in columns {
            let ty = if key.contains(&c.name) {
                self.key_type_name(c.base_type)
            } else {
                self.type_name(c.base_type)
            };
            defs.push(format!("{} {ty}", self.quote(&c.name)?));
        }
        if !key.is_empty() {
            defs.push(format!("PRIMARY KEY ({})", self.column_list(key)?));
        }
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            target.reference,
            defs.join(", ")
        ))
    }

    /// The columns of a table that stores each record whole in `column`.
    fn json_table_columns(&self, column: &str) -> Result<String, FaucetError> {
        Ok(format!(
            "{} BIGINT PRIMARY KEY, {} TEXT NOT NULL",
            self.quote("id")?,
            self.quote(column)?
        ))
    }

    /// `CREATE TABLE IF NOT EXISTS` for a whole-record table.
    fn create_json_table_sql(
        &self,
        target: &TableTarget,
        column: &str,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            target.reference,
            self.json_table_columns(column)?
        ))
    }

    /// Add `column` of base type `t`.
    fn add_column_sql(
        &self,
        target: &TableTarget,
        column: &str,
        t: SqlBaseType,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            target.reference,
            self.quote(column)?,
            self.type_name(t)
        ))
    }

    /// Change `column` to `type_name`, or `None` when the dialect needs the
    /// column's live definition to do it (the backend builds that statement).
    fn alter_column_type_sql(
        &self,
        target: &TableTarget,
        column: &str,
        type_name: &str,
    ) -> Result<Option<String>, FaucetError> {
        let _ = (target, column, type_name);
        Ok(None)
    }

    /// Drop `NOT NULL` from `column`, or `None` when the dialect needs the
    /// column's live definition to do it.
    fn drop_not_null_sql(
        &self,
        target: &TableTarget,
        column: &str,
    ) -> Result<Option<String>, FaucetError> {
        let _ = (target, column);
        Ok(None)
    }

    /// The scoped-cleanup key table, as referenced in SQL.
    fn cleanup_keys_table(&self) -> String {
        "faucet_cleanup_keys".to_string()
    }

    /// Drop a leftover cleanup key table, run before creating it and after
    /// the delete; `None` when the table cannot outlive its transaction.
    fn cleanup_drop_sql(&self) -> Option<String> {
        None
    }

    /// Create the cleanup key table with one column per `key` column, typed
    /// like the destination's (`key_types`, from the executor).
    fn cleanup_create_sql(
        &self,
        target: &TableTarget,
        key: &[String],
        key_types: &[Option<&str>],
    ) -> Result<String, FaucetError> {
        let _ = target;
        let defs = key
            .iter()
            .zip(key_types)
            .map(|(k, t)| {
                Ok(match t {
                    Some(t) => format!("{} {t}", self.quote(k)?),
                    None => self.quote(k)?,
                })
            })
            .collect::<Result<Vec<_>, FaucetError>>()?;
        Ok(format!(
            "CREATE TEMPORARY TABLE {} ({})",
            self.cleanup_keys_table(),
            defs.join(", ")
        ))
    }

    /// Load `rows` written key tuples into the cleanup key table.
    fn cleanup_insert_sql(
        &self,
        key: &[String],
        key_types: &[Option<&str>],
        rows: usize,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "INSERT INTO {} ({}) VALUES {}",
            self.cleanup_keys_table(),
            self.column_list(key)?,
            self.values_sql(key.len(), key_types, rows, 0)
        ))
    }

    /// Delete every row matching the scope (one bind per scope column, in
    /// `scope` order) whose key is absent from the cleanup key table.
    fn cleanup_delete_sql(
        &self,
        target: &TableTarget,
        scope: &[String],
        scope_types: &[Option<&str>],
        key: &[String],
    ) -> Result<String, FaucetError> {
        let t = &target.reference;
        let scope_pred = scope
            .iter()
            .enumerate()
            .map(|(i, c)| {
                Ok(format!(
                    "{t}.{} = {}",
                    self.quote(c)?,
                    self.placeholder(i + 1, scope_types.get(i).copied().flatten())
                ))
            })
            .collect::<Result<Vec<_>, FaucetError>>()?
            .join(" AND ");
        let join_pred = key
            .iter()
            .map(|k| {
                let q = self.quote(k)?;
                Ok(format!("c.{q} = {t}.{q}"))
            })
            .collect::<Result<Vec<_>, FaucetError>>()?
            .join(" AND ");
        Ok(format!(
            "DELETE FROM {t} WHERE {scope_pred} AND NOT EXISTS (SELECT 1 FROM {} c WHERE {join_pred})",
            self.cleanup_keys_table()
        ))
    }

    /// How a cleanup names its destination in a "column does not exist"
    /// error.
    fn cleanup_table_display(&self, target: &TableTarget) -> String {
        format!("table '{}'", target.name)
    }

    /// `CREATE TABLE IF NOT EXISTS` for the commit-token watermark table.
    fn commit_token_table_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} VARCHAR(255) PRIMARY KEY, {k} {} NOT NULL)",
            self.type_name(SqlBaseType::Text)
        )
    }

    /// Upsert one scope's commit token (binds: scope, token): the dialect's
    /// upsert tail when it has one, else a standard `MERGE`.
    fn commit_token_upsert_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        let key = [faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL.to_string()];
        let cols = [
            faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL.to_string(),
            faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL.to_string(),
        ];
        match self.upsert_tail(&key, &cols) {
            Some(tail) => format!(
                "INSERT INTO {t} ({s}, {k}) VALUES ({}) {tail}",
                self.values_sql(2, &[], 1, 0)
                    .trim_start_matches('(')
                    .trim_end_matches(')')
            ),
            None => format!(
                "MERGE INTO {t} w USING (SELECT {} AS {s}, {} AS {k}) n ON w.{s} = n.{s} \
                 WHEN MATCHED THEN UPDATE SET {k} = n.{k} \
                 WHEN NOT MATCHED THEN INSERT ({s}, {k}) VALUES (n.{s}, n.{k})",
                self.placeholder(1, None),
                self.placeholder(2, None)
            ),
        }
    }

    /// Read one scope's commit token (binds: scope).
    fn commit_token_select_sql(&self) -> String {
        let (t, s, k) = self.commit_token_names();
        format!(
            "SELECT {k} FROM {t} WHERE {s} = {}",
            self.placeholder(1, None)
        )
    }

    /// Delete one scope's commit token (binds: scope).
    fn commit_token_delete_sql(&self) -> String {
        let (t, s, _) = self.commit_token_names();
        format!("DELETE FROM {t} WHERE {s} = {}", self.placeholder(1, None))
    }

    /// The quoted watermark table, scope column and token column.
    fn commit_token_names(&self) -> (String, String, String) {
        (
            self.quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            self.quote_ident(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            self.quote_ident(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        )
    }

    /// The rollback-journal SQL, or `None` for a dialect without run rollback.
    fn journal_sql(&self) -> Option<JournalSql> {
        None
    }

    /// The error text for the record at `index` matching no column of
    /// `table`.
    fn no_matching_column_message(
        &self,
        index: usize,
        fields: &[&String],
        columns: &[&String],
        table: &str,
    ) -> String {
        let _ = table;
        format!(
            "{}: record {index} has no field matching a column of the target table \
             (record fields: {fields:?}; table columns: {columns:?})",
            self.name()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Ansi;
    impl SqlDialect for Ansi {
        fn name(&self) -> &'static str {
            "ansi"
        }
        fn quote_ident(&self, name: &str) -> String {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
        fn placeholder(&self, _n: usize, _t: Option<&str>) -> String {
            "?".into()
        }
        fn param_limit(&self) -> usize {
            10
        }
        fn column_matching(&self) -> ColumnMatching {
            ColumnMatching::Exact
        }
        fn type_name(&self, _t: SqlBaseType) -> &'static str {
            "TEXT"
        }
        fn commit_token_table_sql(&self) -> String {
            String::new()
        }
        fn commit_token_upsert_sql(&self) -> String {
            String::new()
        }
    }

    #[test]
    fn provided_builders_have_a_generic_shape() {
        let d = Ansi;
        let t = TableTarget::new("t", "\"t\"");
        assert_eq!(d.rows_per_statement(3), 3);
        assert_eq!(d.rows_per_statement(0), 10);
        assert_eq!(d.rows_per_statement(50), 1);
        assert_eq!(d.quote_literal("o'k"), "'o''k'");
        assert!(
            d.upsert_sql("\"t\"", &["a".into()], &["a".into()], &[], 1)
                .is_err()
        );
        assert_eq!(d.alter_column_type_sql(&t, "a", "X").unwrap(), None);
        assert_eq!(d.drop_not_null_sql(&t, "a").unwrap(), None);
        assert_eq!(d.cleanup_drop_sql(), None);
        assert_eq!(d.journal_sql().map(|j| j.before_type), None);
        assert_eq!(
            d.create_json_table_sql(&t, "doc").unwrap(),
            "CREATE TABLE IF NOT EXISTS \"t\" (\"id\" BIGINT PRIMARY KEY, \"doc\" TEXT NOT NULL)"
        );
        assert_eq!(
            d.cleanup_create_sql(&t, &["a".into(), "b".into()], &[Some("INT"), None])
                .unwrap(),
            "CREATE TEMPORARY TABLE faucet_cleanup_keys (\"a\" INT, \"b\")"
        );
        assert_eq!(d.widened_type_name(None, SqlBaseType::Integer), "TEXT");
        assert_eq!(d.cleanup_table_display(&t), "table 't'");
        assert!(d.commit_token_select_sql().starts_with("SELECT \"token\""));
        assert!(
            d.commit_token_delete_sql()
                .starts_with("DELETE FROM \"_faucet_commit_token\"")
        );
        assert!(
            d.no_matching_column_message(0, &[], &[], "t")
                .starts_with("ansi: record 0")
        );
    }
}

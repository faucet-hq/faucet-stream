//! Microsoft SQL Server.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::{FaucetError, SqlBaseType};

/// SQL Server: `[bracket]` identifiers (NUL refused), `@Pn` placeholders
/// wrapped in `CAST(… AS type)` so a multi-row `VALUES` column has one type,
/// 2100 parameters (two consumed by `sp_executesql`) and at most 1000 rows per
/// `VALUES` constructor, field names folded to a unique column ignoring case,
/// and `MERGE` for keyed upserts and deletes (T-SQL has no row-constructor
/// `IN`).
#[derive(Debug, Clone, Copy, Default)]
pub struct Mssql;

/// SQL Server's per-request parameter ceiling.
pub const PARAM_LIMIT: usize = 2100;

/// Parameters `sp_executesql` consumes itself (the statement text and the
/// parameter declarations).
pub const SP_EXECUTESQL_RESERVED: usize = 2;

/// Row expressions allowed in one table value constructor.
pub const MAX_VALUES_ROWS: usize = 1000;

/// The session-scoped cleanup key table. A fixed constant, so interpolated
/// bare.
pub const CLEANUP_TEMP_TABLE: &str = "#faucet_cleanup_keys";

/// Bracket-quote an identifier, doubling `]`; a NUL is refused.
pub fn quote_ident(name: &str) -> Result<String, FaucetError> {
    if name.contains('\0') {
        return Err(FaucetError::Config(format!(
            "invalid MSSQL identifier (contains NUL): {name:?}"
        )));
    }
    Ok(format!("[{}]", name.replace(']', "]]")))
}

fn quote_all(names: &[String]) -> Result<Vec<String>, FaucetError> {
    names.iter().map(|n| quote_ident(n)).collect()
}

impl SqlDialect for Mssql {
    fn name(&self) -> &'static str {
        "mssql"
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("[{}]", name.replace(']', "]]"))
    }

    fn check_ident(&self, name: &str) -> Result<(), FaucetError> {
        quote_ident(name).map(|_| ())
    }

    fn quote_literal(&self, value: &str) -> String {
        format!("N'{}'", value.replace('\'', "''"))
    }

    fn placeholder(&self, n: usize, sql_type: Option<&str>) -> String {
        match sql_type {
            Some(ty) => format!("CAST(@P{n} AS {ty})"),
            None => format!("@P{n}"),
        }
    }

    fn param_limit(&self) -> usize {
        PARAM_LIMIT - SP_EXECUTESQL_RESERVED
    }

    /// Both ceilings: the parameter budget and the 1000-row `VALUES` limit.
    fn rows_per_statement(&self, width: usize) -> usize {
        if width == 0 {
            return 1;
        }
        (self.param_limit() / width).clamp(1, MAX_VALUES_ROWS)
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::FoldUnique
    }

    fn type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Integer => "BIGINT",
            SqlBaseType::Double => "FLOAT",
            SqlBaseType::Boolean => "BIT",
            SqlBaseType::Text => "NVARCHAR(MAX)",
            SqlBaseType::Json => "NVARCHAR(MAX)",
        }
    }

    /// `NVARCHAR(MAX)` cannot be indexed; 450 characters is the 900-byte key
    /// limit.
    fn key_type_name(&self, t: SqlBaseType) -> &'static str {
        match t {
            SqlBaseType::Text | SqlBaseType::Json => "NVARCHAR(450)",
            other => self.type_name(other),
        }
    }

    /// `DECIMAL(38,10)` — exact for every `BIGINT` — when an integer column
    /// starts receiving fractions, never `FLOAT`.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (Some(SqlBaseType::Integer), SqlBaseType::Double) => "DECIMAL(38,10)",
            _ => self.type_name(to),
        }
    }

    /// Joins on every key column, updates the non-key columns (no `WHEN
    /// MATCHED` when every column is a key) and inserts all columns; T-SQL
    /// requires the terminating `;`.
    fn upsert_sql(
        &self,
        table_ref: &str,
        key: &[String],
        columns: &[String],
        types: &[Option<&str>],
        rows: usize,
    ) -> Result<String, FaucetError> {
        let quoted_cols = quote_all(columns)?;
        let quoted_keys = quote_all(key)?;
        let col_list = quoted_cols.join(", ");
        let on = quoted_keys
            .iter()
            .map(|qk| format!("tgt.{qk} = src.{qk}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let update_set = columns
            .iter()
            .zip(&quoted_cols)
            .filter(|(c, _)| !key.iter().any(|k| k == *c))
            .map(|(_, qc)| format!("tgt.{qc} = src.{qc}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert_vals = quoted_cols
            .iter()
            .map(|qc| format!("src.{qc}"))
            .collect::<Vec<_>>()
            .join(", ");
        let matched = if update_set.is_empty() {
            String::new()
        } else {
            format!(" WHEN MATCHED THEN UPDATE SET {update_set}")
        };
        Ok(format!(
            // HOLDLOCK holds the key-range lock from the match to the insert,
            // so two sessions merging one new key cannot both insert it.
            "MERGE {table_ref} WITH (HOLDLOCK) AS tgt USING (VALUES {}) AS src ({col_list}) \
             ON {on}{matched} WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals});",
            self.values_sql(columns.len(), types, rows, 0)
        ))
    }

    fn delete_keys_sql(
        &self,
        table_ref: &str,
        key: &[String],
        types: &[Option<&str>],
        rows: usize,
    ) -> Result<String, FaucetError> {
        let quoted_keys = quote_all(key)?;
        let on = quoted_keys
            .iter()
            .map(|qk| format!("tgt.{qk} = src.{qk}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(format!(
            "MERGE {table_ref} AS tgt USING (VALUES {}) AS src ({}) ON {on} \
             WHEN MATCHED THEN DELETE;",
            self.values_sql(key.len(), types, rows, 0),
            quoted_keys.join(", ")
        ))
    }

    /// `IF OBJECT_ID(…) IS NULL CREATE TABLE` (T-SQL has no `IF NOT EXISTS`).
    fn create_table_sql(
        &self,
        target: &TableTarget,
        columns: &[faucet_core::PlannedColumn],
        key: &[String],
    ) -> Result<String, FaucetError> {
        let mut rendered: Vec<String> = Vec::with_capacity(columns.len() + 1);
        for c in columns {
            let ty = if key.contains(&c.name) {
                self.key_type_name(c.base_type)
            } else {
                self.type_name(c.base_type)
            };
            rendered.push(format!("{} {ty}", quote_ident(&c.name)?));
        }
        if !key.is_empty() {
            rendered.push(format!("PRIMARY KEY ({})", quote_all(key)?.join(", ")));
        }
        Ok(format!(
            "IF OBJECT_ID(N'{}', N'U') IS NULL CREATE TABLE {} ({})",
            target.name.replace('\'', "''"),
            target.reference,
            rendered.join(", ")
        ))
    }

    fn json_table_columns(&self, column: &str) -> Result<String, FaucetError> {
        Ok(format!(
            "id BIGINT IDENTITY(1,1) PRIMARY KEY, {} NVARCHAR(MAX)",
            quote_ident(column)?
        ))
    }

    fn create_json_table_sql(
        &self,
        target: &TableTarget,
        column: &str,
    ) -> Result<String, FaucetError> {
        Ok(format!(
            "IF OBJECT_ID(N'{}', N'U') IS NULL CREATE TABLE {} ({})",
            target.name.replace('\'', "''"),
            target.reference,
            self.json_table_columns(column)?
        ))
    }

    /// Guarded by `sys.columns` (T-SQL has no `ADD COLUMN IF NOT EXISTS`).
    fn add_column_sql(
        &self,
        target: &TableTarget,
        column: &str,
        t: SqlBaseType,
    ) -> Result<String, FaucetError> {
        let qcol = quote_ident(column)?;
        Ok(format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.columns \
             WHERE object_id = OBJECT_ID(N'{}') AND name = N'{}') \
             ALTER TABLE {} ADD {qcol} {}",
            target.name.replace('\'', "''"),
            column.replace('\'', "''"),
            target.reference,
            self.type_name(t),
        ))
    }

    fn cleanup_keys_table(&self) -> String {
        CLEANUP_TEMP_TABLE.to_string()
    }

    /// `DROP TABLE IF EXISTS` is SQL Server 2016+; the `OBJECT_ID` guard
    /// works on older servers too.
    fn cleanup_drop_sql(&self) -> Option<String> {
        Some(format!(
            "IF OBJECT_ID(N'tempdb..{CLEANUP_TEMP_TABLE}') IS NOT NULL DROP TABLE {CLEANUP_TEMP_TABLE}"
        ))
    }

    /// `SELECT … INTO` copies each key column's exact declared type; the
    /// `UNION ALL` keeps it from inheriting an `IDENTITY` property.
    fn cleanup_create_sql(
        &self,
        target: &TableTarget,
        key: &[String],
        _key_types: &[Option<&str>],
    ) -> Result<String, FaucetError> {
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup: requires a non-empty `key` to build the written-key set".into(),
            ));
        }
        let key_list = quote_all(key)?.join(", ");
        let t = &target.reference;
        Ok(format!(
            "SELECT {key_list} INTO {CLEANUP_TEMP_TABLE} FROM \
             (SELECT {key_list} FROM {t} WHERE 1 = 0 \
             UNION ALL SELECT {key_list} FROM {t} WHERE 1 = 0) AS s"
        ))
    }

    fn cleanup_delete_sql(
        &self,
        target: &TableTarget,
        scope: &[String],
        _scope_types: &[Option<&str>],
        key: &[String],
    ) -> Result<String, FaucetError> {
        if key.is_empty() {
            return Err(FaucetError::Sink(
                "cleanup: requires a non-empty `key` so a written row can be told apart from a \
                 stale one"
                    .into(),
            ));
        }
        let scope_pred = scope
            .iter()
            .enumerate()
            .map(|(i, c)| Ok(format!("t.{} = @P{}", quote_ident(c)?, i + 1)))
            .collect::<Result<Vec<_>, FaucetError>>()?
            .join(" AND ");
        let join_pred = key
            .iter()
            .map(|k| {
                let q = quote_ident(k)?;
                Ok(format!("c.{q} = t.{q}"))
            })
            .collect::<Result<Vec<_>, FaucetError>>()?
            .join(" AND ");
        Ok(format!(
            "DELETE t FROM {} AS t WHERE {}NOT EXISTS (SELECT 1 FROM {CLEANUP_TEMP_TABLE} c \
             WHERE {join_pred})",
            target.reference,
            crate::dialect::scope_and(&scope_pred),
        ))
    }

    fn cleanup_table_display(&self, target: &TableTarget) -> String {
        target.name.clone()
    }

    /// `scope` is `NVARCHAR(450)` (the 900-byte key budget); `token` is
    /// unbounded because a commit token embeds the page's resume bookmark.
    fn commit_token_table_sql(&self) -> String {
        format!(
            "IF OBJECT_ID(N'{tbl}', N'U') IS NULL \
             CREATE TABLE [{tbl}] ([scope] NVARCHAR({w}) PRIMARY KEY, \
             [token] NVARCHAR(MAX) NOT NULL, \
             [updated_at] DATETIME2 DEFAULT SYSUTCDATETIME())",
            tbl = faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            w = 450,
        )
    }

    fn commit_token_upsert_sql(&self) -> String {
        format!(
            "MERGE [{tbl}] AS t \
             USING (SELECT @P1 AS [scope], @P2 AS [token]) AS s \
             ON t.[scope] = s.[scope] \
             WHEN MATCHED THEN UPDATE SET t.[token] = s.[token], t.[updated_at] = SYSUTCDATETIME() \
             WHEN NOT MATCHED THEN INSERT ([scope], [token]) VALUES (s.[scope], s.[token]);",
            tbl = faucet_core::idempotency::COMMIT_TOKEN_TABLE,
        )
    }
}

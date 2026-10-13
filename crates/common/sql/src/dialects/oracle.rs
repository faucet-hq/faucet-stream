//! Oracle Database.

use crate::dialect::{SqlDialect, TableTarget};
use crate::mapping::ColumnMatching;
use faucet_core::idempotency::{
    COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TABLE, COMMIT_TOKEN_TOKEN_COL,
};
use faucet_core::{FaucetError, PlannedColumn, SqlBaseType};

/// Oracle: double-quoted, case-sensitive identifiers (no escape exists for
/// `"` inside one, so it is refused, as are NUL, empty names and names over
/// 128 bytes), `:n` placeholders, array DML (one row of placeholders per
/// statement, bound for many rows), unquoted DDL names stored upper-cased (a
/// field reaches an all-upper-case column through its upper-case form),
/// single-row `MERGE … FROM DUAL` upserts, idempotent DDL through a PL/SQL
/// block that swallows the expected `ORA-` codes.
#[derive(Debug, Clone, Copy, Default)]
pub struct Oracle;

/// Oracle's identifier length limit (12.2+), in bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 128;

/// ORA-01430: column being added already exists.
pub const ORA_COLUMN_EXISTS: i32 = 1430;
/// ORA-01451: column to be modified to NULL cannot be modified to NULL.
pub const ORA_ALREADY_NULL: i32 = 1451;
/// Width of the watermark table's `scope` key column.
pub const SCOPE_COL_WIDTH: usize = 1000;

/// Quote an Oracle identifier as `"name"`, refusing what cannot be quoted.
pub fn quote_ident(name: &str) -> Result<String, FaucetError> {
    if name.is_empty() {
        return Err(FaucetError::Config("empty Oracle identifier".into()));
    }
    if name.contains('"') || name.contains('\0') {
        return Err(FaucetError::Config(format!(
            "invalid Oracle identifier {name:?}: '\"' and NUL are not allowed"
        )));
    }
    if name.len() > MAX_IDENTIFIER_BYTES {
        return Err(FaucetError::Config(format!(
            "Oracle identifier {name:?} exceeds {MAX_IDENTIFIER_BYTES} bytes"
        )));
    }
    Ok(format!("\"{name}\""))
}

fn quote_all(cols: &[String]) -> Result<Vec<String>, FaucetError> {
    cols.iter().map(|c| quote_ident(c)).collect()
}

/// Run `ddl` via `EXECUTE IMMEDIATE`, swallowing the listed `ORA-` codes so
/// idempotent DDL (create-if-missing, drop-if-present) needs no pre-check.
pub fn ignoring(ddl: &str, codes: &[i32]) -> String {
    let list = codes
        .iter()
        .map(|c| format!("-{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "BEGIN EXECUTE IMMEDIATE {}; EXCEPTION WHEN OTHERS THEN IF SQLCODE NOT IN ({list}) \
         THEN RAISE; END IF; END;",
        Oracle.quote_literal(ddl)
    )
}

/// The column type of a planned column. A whole-number column is an
/// unconstrained `NUMBER` (scale 0 would round a later fraction); only a key
/// column, which must stay integral, is `NUMBER(38)`. Doubles are IEEE
/// `BINARY_DOUBLE`; key text is bounded (`CLOB` cannot be indexed); other
/// text is `CLOB` so no value is truncated.
pub fn column_type(t: SqlBaseType, is_key: bool) -> &'static str {
    match t {
        SqlBaseType::Integer if is_key => "NUMBER(38)",
        SqlBaseType::Integer => "NUMBER",
        SqlBaseType::Double => "BINARY_DOUBLE",
        SqlBaseType::Boolean => "NUMBER(1)",
        SqlBaseType::Text | SqlBaseType::Json if is_key => "VARCHAR2(1000 CHAR)",
        SqlBaseType::Text | SqlBaseType::Json => "CLOB",
    }
}

/// Character width of each text key column when `n` share a primary key: the
/// key must fit about 6,400 bytes at an 8 KB block and an AL32UTF8 character
/// takes up to 4 bytes, so the text columns share 1,500 characters (≤ 1,000
/// each).
pub fn key_text_width(n: usize) -> usize {
    (1500 / n.max(1)).min(1000)
}

/// `CREATE TABLE` for the exactly-once watermark (wrap with [`ignoring`]).
pub fn token_table_ddl(token_table: &str) -> String {
    format!(
        "CREATE TABLE {token_table} (\"{COMMIT_TOKEN_SCOPE_COL}\" VARCHAR2({SCOPE_COL_WIDTH} CHAR) \
         PRIMARY KEY, \"{COMMIT_TOKEN_TOKEN_COL}\" CLOB NOT NULL, \
         \"updated_at\" TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL)"
    )
}

/// Upsert of a scope's token (`:1` scope, `:2` token).
pub fn token_merge_sql(token_table: &str) -> String {
    let (s, t) = (COMMIT_TOKEN_SCOPE_COL, COMMIT_TOKEN_TOKEN_COL);
    format!(
        "MERGE INTO {token_table} w USING (SELECT :1 AS \"{s}\", :2 AS \"{t}\" FROM DUAL) n \
         ON (w.\"{s}\" = n.\"{s}\") \
         WHEN MATCHED THEN UPDATE SET w.\"{t}\" = n.\"{t}\", w.\"updated_at\" = SYSTIMESTAMP \
         WHEN NOT MATCHED THEN INSERT (\"{s}\", \"{t}\", \"updated_at\") \
         VALUES (n.\"{s}\", n.\"{t}\", SYSTIMESTAMP)"
    )
}

/// Remove a scope's token row (`:1` scope).
pub fn token_delete_sql(token_table: &str) -> String {
    format!("DELETE FROM {token_table} WHERE \"{COMMIT_TOKEN_SCOPE_COL}\" = :1")
}

/// Read a scope's token (`:1` scope).
pub fn token_select_sql(token_table: &str) -> String {
    format!(
        "SELECT \"{COMMIT_TOKEN_TOKEN_COL}\" FROM {token_table} WHERE \"{COMMIT_TOKEN_SCOPE_COL}\" = :1"
    )
}

fn token_table() -> String {
    format!("\"{COMMIT_TOKEN_TABLE}\"")
}

impl SqlDialect for Oracle {
    fn name(&self) -> &'static str {
        "oracle"
    }

    fn quote_ident(&self, name: &str) -> String {
        format!("\"{name}\"")
    }

    fn check_ident(&self, name: &str) -> Result<(), FaucetError> {
        quote_ident(name).map(|_| ())
    }

    fn placeholder(&self, n: usize, _sql_type: Option<&str>) -> String {
        format!(":{n}")
    }

    fn param_limit(&self) -> usize {
        65535
    }

    /// Array DML: a statement carries one row of placeholders and is bound
    /// for every row of the chunk.
    fn rows_per_statement(&self, _width: usize) -> usize {
        1
    }

    fn column_matching(&self) -> ColumnMatching {
        ColumnMatching::UpperFold
    }

    fn type_name(&self, t: SqlBaseType) -> &'static str {
        column_type(t, false)
    }

    fn key_type_name(&self, t: SqlBaseType) -> &'static str {
        column_type(t, true)
    }

    /// Oracle widens a numeric column in place only; an integer column that
    /// starts receiving fractions becomes an unconstrained (exact) `NUMBER`.
    fn widened_type_name(&self, from: Option<SqlBaseType>, to: SqlBaseType) -> &'static str {
        match (from, to) {
            (_, SqlBaseType::Double) => "NUMBER",
            _ => self.type_name(to),
        }
    }

    /// A single-row `MERGE … USING (SELECT :1 AS … FROM DUAL)`, executed as
    /// array DML. Every key column must be one of `columns`.
    fn upsert_sql(
        &self,
        table_ref: &str,
        key: &[String],
        columns: &[String],
        _types: &[Option<&str>],
        _rows: usize,
    ) -> Result<String, FaucetError> {
        for k in key {
            if !columns.contains(k) {
                return Err(FaucetError::Sink(format!(
                    "oracle upsert: key column {k:?} is not a writable column of the table"
                )));
            }
        }
        let quoted = quote_all(columns)?;
        let select = quoted
            .iter()
            .enumerate()
            .map(|(i, q)| format!(":{} AS {q}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let on = quote_all(key)?
            .iter()
            .map(|q| format!("t.{q} = s.{q}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let updates: Vec<String> = columns
            .iter()
            .zip(&quoted)
            .filter(|(c, _)| !key.contains(c))
            .map(|(_, q)| format!("t.{q} = s.{q}"))
            .collect();
        let matched = if updates.is_empty() {
            String::new()
        } else {
            format!(" WHEN MATCHED THEN UPDATE SET {}", updates.join(", "))
        };
        Ok(format!(
            "MERGE INTO {table_ref} t USING (SELECT {select} FROM DUAL) s ON ({on}){matched} \
             WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
            quoted.join(", "),
            quoted
                .iter()
                .map(|q| format!("s.{q}"))
                .collect::<Vec<_>>()
                .join(", ")
        ))
    }

    /// `DELETE FROM t WHERE "K1" = :1 AND "K2" = :2`, executed as array DML.
    fn delete_keys_sql(
        &self,
        table_ref: &str,
        key: &[String],
        _types: &[Option<&str>],
        _rows: usize,
    ) -> Result<String, FaucetError> {
        let pred = quote_all(key)?
            .iter()
            .enumerate()
            .map(|(i, q)| format!("{q} = :{}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        Ok(format!("DELETE FROM {table_ref} WHERE {pred}"))
    }

    /// Plain `CREATE TABLE` (wrap with [`ignoring`] for idempotency); text key
    /// columns share the index budget ([`key_text_width`]).
    fn create_table_sql(
        &self,
        target: &TableTarget,
        columns: &[PlannedColumn],
        key: &[String],
    ) -> Result<String, FaucetError> {
        let text_keys = columns
            .iter()
            .filter(|c| {
                key.contains(&c.name)
                    && matches!(c.base_type, SqlBaseType::Text | SqlBaseType::Json)
            })
            .count();
        let mut defs = Vec::with_capacity(columns.len() + 1);
        for c in columns {
            let is_key = key.contains(&c.name);
            let ty = match c.base_type {
                SqlBaseType::Text | SqlBaseType::Json if is_key => {
                    format!("VARCHAR2({} CHAR)", key_text_width(text_keys))
                }
                t => column_type(t, is_key).to_string(),
            };
            defs.push(format!("{} {ty}", quote_ident(&c.name)?));
        }
        if !key.is_empty() {
            defs.push(format!("PRIMARY KEY ({})", quote_all(key)?.join(", ")));
        }
        Ok(format!(
            "CREATE TABLE {} ({})",
            target.reference,
            defs.join(", ")
        ))
    }

    /// An identity key beside the record column (named `FAUCET_ID` when the
    /// record column is itself `ID`).
    fn create_json_table_sql(
        &self,
        target: &TableTarget,
        column: &str,
    ) -> Result<String, FaucetError> {
        let id = if column == "ID" { "FAUCET_ID" } else { "ID" };
        Ok(format!(
            "CREATE TABLE {} (\"{id}\" NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, {} CLOB)",
            target.reference,
            quote_ident(column)?
        ))
    }

    /// `ALTER TABLE … ADD`, idempotent through [`ignoring`].
    fn add_column_sql(
        &self,
        target: &TableTarget,
        column: &str,
        t: SqlBaseType,
    ) -> Result<String, FaucetError> {
        Ok(ignoring(
            &format!(
                "ALTER TABLE {} ADD ({} {})",
                target.reference,
                quote_ident(column)?,
                column_type(t, false)
            ),
            &[ORA_COLUMN_EXISTS],
        ))
    }

    fn alter_column_type_sql(
        &self,
        target: &TableTarget,
        column: &str,
        type_name: &str,
    ) -> Result<Option<String>, FaucetError> {
        Ok(Some(format!(
            "ALTER TABLE {} MODIFY ({} {type_name})",
            target.reference,
            quote_ident(column)?
        )))
    }

    fn drop_not_null_sql(
        &self,
        target: &TableTarget,
        column: &str,
    ) -> Result<Option<String>, FaucetError> {
        Ok(Some(ignoring(
            &format!(
                "ALTER TABLE {} MODIFY ({} NULL)",
                target.reference,
                quote_ident(column)?
            ),
            &[ORA_ALREADY_NULL],
        )))
    }

    fn commit_token_table_sql(&self) -> String {
        token_table_ddl(&token_table())
    }

    fn commit_token_upsert_sql(&self) -> String {
        token_merge_sql(&token_table())
    }

    fn commit_token_select_sql(&self) -> String {
        token_select_sql(&token_table())
    }

    fn commit_token_delete_sql(&self) -> String {
        token_delete_sql(&token_table())
    }
}

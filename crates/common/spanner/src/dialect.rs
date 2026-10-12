//! Spanner SQL dialects.
//!
//! A Cloud Spanner database is created with exactly one dialect, fixed for its
//! lifetime: GoogleSQL (the default) or PostgreSQL. The dialect changes how
//! identifiers are quoted, how query parameters are written and bound, which
//! `information_schema` objects exist, and how column types are spelled.
//!
//! [`SpannerDialect`] centralises those differences. Every method returns the
//! exact pre-existing GoogleSQL text for [`SpannerDialect::GoogleStandardSql`],
//! so the default path is unchanged.

use crate::types::SpannerType;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The SQL dialect a Spanner database was created with.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SpannerDialect {
    /// GoogleSQL (`GOOGLE_STANDARD_SQL`). The default.
    #[default]
    GoogleStandardSql,
    /// PostgreSQL dialect.
    Postgresql,
}

impl SpannerDialect {
    /// `true` for the default (GoogleSQL) dialect; used to omit it when
    /// serializing.
    pub fn is_default(&self) -> bool {
        matches!(self, Self::GoogleStandardSql)
    }

    /// `true` for the PostgreSQL dialect.
    pub fn is_postgresql(self) -> bool {
        matches!(self, Self::Postgresql)
    }

    /// Quote an identifier: backticks for GoogleSQL, double quotes for
    /// PostgreSQL. An embedded quote character is doubled.
    pub fn quote_ident(self, ident: &str) -> String {
        match self {
            Self::GoogleStandardSql => crate::quote_ident_spanner(ident),
            Self::Postgresql => format!("\"{}\"", ident.replace('"', "\"\"")),
        }
    }

    /// How a bind parameter is written in SQL text. GoogleSQL binds by name
    /// (`@name`); PostgreSQL binds by position (`$1`, `$2`, ...).
    pub fn placeholder(self, name: &str, index: usize) -> String {
        match self {
            Self::GoogleStandardSql => format!("@{name}"),
            Self::Postgresql => format!("${index}"),
        }
    }

    /// The name a parameter is bound under on the wire. GoogleSQL uses the
    /// name written in the SQL; PostgreSQL parameters are named `p1`, `p2`, ...
    pub fn bind_name(self, name: &str, index: usize) -> String {
        match self {
            Self::GoogleStandardSql => name.to_string(),
            Self::Postgresql => format!("p{index}"),
        }
    }

    /// 64-bit integer cast target used by `MIN`/`MAX` shard-bounds probes.
    pub fn int64_cast_target(self) -> &'static str {
        match self {
            Self::GoogleStandardSql => "INT64",
            Self::Postgresql => "bigint",
        }
    }

    /// Parse an `information_schema.columns.spanner_type` string.
    pub fn parse_type(self, raw: &str) -> SpannerType {
        match self {
            Self::GoogleStandardSql => crate::types::parse_spanner_type(raw),
            Self::Postgresql => crate::types::parse_pg_spanner_type(raw),
        }
    }

    /// Render a column type as DDL text.
    pub fn type_ddl(self, ty: &SpannerType) -> Result<String, String> {
        if let SpannerType::Other = ty {
            return Err("cannot re-emit a STRUCT/PROTO column in DDL".into());
        }
        Ok(match self {
            Self::GoogleStandardSql => match ty {
                SpannerType::Bool => "BOOL".into(),
                SpannerType::Int64 => "INT64".into(),
                SpannerType::Float32 => "FLOAT32".into(),
                SpannerType::Float64 => "FLOAT64".into(),
                SpannerType::Timestamp => "TIMESTAMP".into(),
                SpannerType::Date => "DATE".into(),
                SpannerType::String => "STRING(MAX)".into(),
                SpannerType::Bytes => "BYTES(MAX)".into(),
                SpannerType::Numeric => "NUMERIC".into(),
                SpannerType::Json => "JSON".into(),
                SpannerType::Array(inner) => format!("ARRAY<{}>", self.type_ddl(inner)?),
                SpannerType::Other => unreachable!("handled above"),
            },
            Self::Postgresql => match ty {
                SpannerType::Bool => "boolean".into(),
                SpannerType::Int64 => "bigint".into(),
                SpannerType::Float32 => "real".into(),
                SpannerType::Float64 => "double precision".into(),
                SpannerType::Timestamp => "timestamp with time zone".into(),
                SpannerType::Date => "date".into(),
                SpannerType::String => "character varying".into(),
                SpannerType::Bytes => "bytea".into(),
                SpannerType::Numeric => "numeric".into(),
                SpannerType::Json => "jsonb".into(),
                SpannerType::Array(inner) => format!("{}[]", self.type_ddl(inner)?),
                SpannerType::Other => unreachable!("handled above"),
            },
        })
    }

    /// The text type used for auto-created columns.
    pub fn text_ddl(self) -> &'static str {
        match self {
            Self::GoogleStandardSql => "STRING(MAX)",
            Self::Postgresql => "character varying",
        }
    }

    /// The type of the commit-timestamp column on the watermark table.
    pub fn commit_timestamp_column(self, name: &str) -> String {
        match self {
            Self::GoogleStandardSql => {
                format!("{name} TIMESTAMP OPTIONS (allow_commit_timestamp=true)")
            }
            Self::Postgresql => format!("{name} spanner.commit_timestamp"),
        }
    }

    /// `ALTER TABLE ... ALTER COLUMN` statement that drops `NOT NULL` from a
    /// column whose type is `type_ddl`.
    ///
    /// GoogleSQL has no `DROP NOT NULL`, so the column is re-emitted at its
    /// current type without the constraint. PostgreSQL has the real thing.
    pub fn relax_not_null_sql(self, table: &str, col: &str, type_ddl: &str) -> String {
        match self {
            Self::GoogleStandardSql => format!(
                "ALTER TABLE {} ALTER COLUMN {} {}",
                self.quote_ident(table),
                self.quote_ident(col),
                type_ddl
            ),
            Self::Postgresql => format!(
                "ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL",
                self.quote_ident(table),
                self.quote_ident(col)
            ),
        }
    }

    /// SQL converting the STRING-bound scope parameter `placeholder` into the
    /// column's type. `Err` for types without equality semantics.
    pub fn scope_cast_expr(self, placeholder: &str, ty: &SpannerType) -> Result<String, String> {
        let unsupported = || {
            Err(
                "column type does not support equality and cannot be used in a cleanup scope"
                    .to_string(),
            )
        };
        match self {
            Self::GoogleStandardSql => Ok(match ty {
                SpannerType::String => placeholder.to_string(),
                SpannerType::Bool => format!("CAST({placeholder} AS BOOL)"),
                SpannerType::Int64 => format!("CAST({placeholder} AS INT64)"),
                SpannerType::Float32 => format!("CAST({placeholder} AS FLOAT32)"),
                SpannerType::Float64 => format!("CAST({placeholder} AS FLOAT64)"),
                SpannerType::Timestamp => format!("CAST({placeholder} AS TIMESTAMP)"),
                SpannerType::Date => format!("CAST({placeholder} AS DATE)"),
                SpannerType::Numeric => format!("CAST({placeholder} AS NUMERIC)"),
                // BYTES travel base64-encoded everywhere else in this
                // connector, so decode rather than reinterpreting UTF-8.
                SpannerType::Bytes => format!("FROM_BASE64({placeholder})"),
                SpannerType::Json | SpannerType::Array(_) | SpannerType::Other => {
                    return unsupported();
                }
            }),
            Self::Postgresql => Ok(match ty {
                SpannerType::String => placeholder.to_string(),
                SpannerType::Bool => format!("CAST({placeholder} AS boolean)"),
                SpannerType::Int64 => format!("CAST({placeholder} AS bigint)"),
                SpannerType::Float32 => format!("CAST({placeholder} AS real)"),
                SpannerType::Float64 => format!("CAST({placeholder} AS double precision)"),
                SpannerType::Timestamp => {
                    format!("CAST({placeholder} AS timestamp with time zone)")
                }
                SpannerType::Date => format!("CAST({placeholder} AS date)"),
                SpannerType::Numeric => format!("CAST({placeholder} AS numeric)"),
                SpannerType::Bytes => {
                    return Err(
                        "bytea columns cannot be used in a cleanup scope on a PostgreSQL-dialect \
                         database"
                            .to_string(),
                    );
                }
                SpannerType::Json | SpannerType::Array(_) | SpannerType::Other => {
                    return unsupported();
                }
            }),
        }
    }

    /// Catalog query listing the columns of one table, returning
    /// `name`, `spanner_type`, `is_nullable` ordered by position. The single
    /// bind parameter is the table name.
    pub fn table_columns_sql(self) -> &'static str {
        match self {
            Self::GoogleStandardSql => {
                "SELECT c.COLUMN_NAME AS name, c.SPANNER_TYPE AS spanner_type, \
                 c.IS_NULLABLE AS is_nullable FROM INFORMATION_SCHEMA.COLUMNS c \
                 WHERE c.TABLE_SCHEMA = '' AND c.TABLE_NAME = @table \
                 ORDER BY c.ORDINAL_POSITION"
            }
            Self::Postgresql => {
                "SELECT c.column_name AS name, c.spanner_type AS spanner_type, \
                 c.is_nullable AS is_nullable FROM information_schema.columns c \
                 WHERE c.table_schema = 'public' AND c.table_name = $1 \
                 ORDER BY c.ordinal_position"
            }
        }
    }

    /// Catalog query listing the primary-key columns of one table in key
    /// order, returning `name`. The single bind parameter is the table name.
    pub fn primary_key_sql(self) -> &'static str {
        match self {
            Self::GoogleStandardSql => {
                "SELECT ic.COLUMN_NAME AS name FROM INFORMATION_SCHEMA.INDEX_COLUMNS ic \
                 WHERE ic.TABLE_SCHEMA = '' AND ic.TABLE_NAME = @table \
                 AND ic.INDEX_NAME = 'PRIMARY_KEY' ORDER BY ic.ORDINAL_POSITION"
            }
            Self::Postgresql => {
                "SELECT ic.column_name AS name FROM information_schema.index_columns ic \
                 WHERE ic.table_schema = 'public' AND ic.table_name = $1 \
                 AND ic.index_name = 'PRIMARY_KEY' ORDER BY ic.ordinal_position"
            }
        }
    }

    /// The name a bind parameter that [`table_columns_sql`](Self::table_columns_sql)
    /// and [`primary_key_sql`](Self::primary_key_sql) take is bound under.
    pub fn table_param_name(self) -> String {
        self.bind_name("table", 1)
    }
}

/// Rewrite `@name` references to PostgreSQL positional parameters.
///
/// `names` lists the bind parameters that exist. Each is assigned the number
/// of its first appearance in the SQL, so a name used twice maps to the same
/// `$N`. Only names in `names` are rewritten, and `@` inside string literals,
/// quoted identifiers and comments is left alone, so the PostgreSQL absolute
/// value operator keeps working.
///
/// Returns the rewritten SQL and the names in `$N` order (index 0 is `$1`).
/// Names that never appear in the SQL are omitted from the list, so the caller
/// binds only what the statement uses.
pub fn named_to_positional(sql: &str, names: &[String]) -> (String, Vec<String>) {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut order: Vec<String> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            // String literal or quoted identifier: copy to the closing quote,
            // treating a doubled quote as an escaped one.
            '\'' | '"' => {
                out.push(c);
                i += 1;
                while i < chars.len() {
                    out.push(chars[i]);
                    if chars[i] == c {
                        if i + 1 < chars.len() && chars[i + 1] == c {
                            out.push(c);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            // `-- line comment`
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    out.push(chars[i]);
                    i += 1;
                }
            }
            // `/* block comment */`
            '/' if chars.get(i + 1) == Some(&'*') => {
                out.push_str("/*");
                i += 2;
                while i < chars.len() {
                    if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                        out.push_str("*/");
                        i += 2;
                        break;
                    }
                    out.push(chars[i]);
                    i += 1;
                }
            }
            '@' => {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_')
                {
                    end += 1;
                }
                let ident: String = chars[start..end].iter().collect();
                if !ident.is_empty() && names.contains(&ident) {
                    let pos = match order.iter().position(|n| *n == ident) {
                        Some(p) => p + 1,
                        None => {
                            order.push(ident);
                            order.len()
                        }
                    };
                    out.push('$');
                    out.push_str(&pos.to_string());
                    i = end;
                } else {
                    out.push('@');
                    i += 1;
                }
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    (out, order)
}

#[cfg(test)]
mod tests {
    use super::*;

    const G: SpannerDialect = SpannerDialect::GoogleStandardSql;
    const P: SpannerDialect = SpannerDialect::Postgresql;

    #[test]
    fn default_is_googlesql_and_serde_is_snake_case() {
        assert_eq!(SpannerDialect::default(), G);
        assert_eq!(serde_json::to_value(P).unwrap(), "postgresql");
        assert_eq!(serde_json::to_value(G).unwrap(), "google_standard_sql");
        let d: SpannerDialect = serde_json::from_value(serde_json::json!("postgresql")).unwrap();
        assert_eq!(d, P);
    }

    #[test]
    fn quoting() {
        assert_eq!(G.quote_ident("we`ird"), "`we``ird`");
        assert_eq!(P.quote_ident("a\"b"), "\"a\"\"b\"");
        assert_eq!(P.quote_ident("t"), "\"t\"");
    }

    #[test]
    fn placeholders_and_bind_names() {
        assert_eq!(G.placeholder("table", 1), "@table");
        assert_eq!(P.placeholder("table", 3), "$3");
        assert_eq!(G.bind_name("table", 1), "table");
        assert_eq!(P.bind_name("table", 3), "p3");
    }

    #[test]
    fn googlesql_type_ddl_is_unchanged() {
        assert_eq!(G.type_ddl(&SpannerType::Int64).unwrap(), "INT64");
        assert_eq!(G.type_ddl(&SpannerType::Bytes).unwrap(), "BYTES(MAX)");
        assert_eq!(
            G.type_ddl(&SpannerType::Array(Box::new(SpannerType::Float64)))
                .unwrap(),
            "ARRAY<FLOAT64>"
        );
        assert!(G.type_ddl(&SpannerType::Other).is_err());
    }

    #[test]
    fn postgresql_type_ddl() {
        assert_eq!(P.type_ddl(&SpannerType::Int64).unwrap(), "bigint");
        assert_eq!(
            P.type_ddl(&SpannerType::String).unwrap(),
            "character varying"
        );
        assert_eq!(P.type_ddl(&SpannerType::Json).unwrap(), "jsonb");
        assert_eq!(
            P.type_ddl(&SpannerType::Array(Box::new(SpannerType::String)))
                .unwrap(),
            "character varying[]"
        );
        assert!(P.type_ddl(&SpannerType::Other).is_err());
    }

    #[test]
    fn relax_not_null_differs_by_dialect() {
        assert_eq!(
            G.relax_not_null_sql("t", "c", "STRING(MAX)"),
            "ALTER TABLE `t` ALTER COLUMN `c` STRING(MAX)"
        );
        assert_eq!(
            P.relax_not_null_sql("t", "c", "character varying"),
            "ALTER TABLE \"t\" ALTER COLUMN \"c\" DROP NOT NULL"
        );
    }

    #[test]
    fn scope_cast_per_dialect() {
        assert_eq!(
            G.scope_cast_expr("@s", &SpannerType::Int64).unwrap(),
            "CAST(@s AS INT64)"
        );
        assert_eq!(
            P.scope_cast_expr("$1", &SpannerType::Int64).unwrap(),
            "CAST($1 AS bigint)"
        );
        assert_eq!(P.scope_cast_expr("$1", &SpannerType::String).unwrap(), "$1");
        assert!(P.scope_cast_expr("$1", &SpannerType::Bytes).is_err());
        assert!(P.scope_cast_expr("$1", &SpannerType::Json).is_err());
        assert_eq!(
            G.scope_cast_expr("@s", &SpannerType::Bytes).unwrap(),
            "FROM_BASE64(@s)"
        );
    }

    fn names(n: &[&str]) -> Vec<String> {
        n.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn rewrites_named_params_in_first_use_order() {
        let (sql, order) = named_to_positional(
            "SELECT * FROM t WHERE b > @bookmark AND a = @region AND c < @bookmark",
            &names(&["region", "bookmark"]),
        );
        assert_eq!(sql, "SELECT * FROM t WHERE b > $1 AND a = $2 AND c < $1");
        assert_eq!(order, names(&["bookmark", "region"]));
    }

    #[test]
    fn leaves_literals_identifiers_comments_and_unknown_names_alone() {
        let (sql, order) = named_to_positional(
            "SELECT '@bookmark', \"@bookmark\", @@x, @unknown -- @bookmark\n /* @bookmark */ \
             FROM t WHERE c = @bookmark",
            &names(&["bookmark"]),
        );
        assert_eq!(
            sql,
            "SELECT '@bookmark', \"@bookmark\", @@x, @unknown -- @bookmark\n /* @bookmark */ \
             FROM t WHERE c = $1"
        );
        assert_eq!(order, names(&["bookmark"]));
    }

    #[test]
    fn escaped_quotes_do_not_end_a_literal_early() {
        let (sql, order) =
            named_to_positional("SELECT 'it''s @bookmark', @bookmark", &names(&["bookmark"]));
        assert_eq!(sql, "SELECT 'it''s @bookmark', $1");
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn unused_names_are_not_bound() {
        let (sql, order) = named_to_positional("SELECT 1", &names(&["bookmark"]));
        assert_eq!(sql, "SELECT 1");
        assert!(order.is_empty());
    }

    #[test]
    fn a_longer_name_is_not_clobbered_by_a_shorter_prefix() {
        let (sql, order) =
            named_to_positional("WHERE a = @id_2 AND b = @id", &names(&["id", "id_2"]));
        assert_eq!(sql, "WHERE a = $1 AND b = $2");
        assert_eq!(order, names(&["id_2", "id"]));
    }
}

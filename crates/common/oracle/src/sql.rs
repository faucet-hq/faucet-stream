//! Pure SQL-text helpers: identifier quoting, literal escaping, table-name
//! splitting and the Oracle flavour of PK-range shard wrapping.

use faucet_core::FaucetError;
use faucet_core::shard::PkShardBounds;

/// Oracle's identifier length limit (12.2+), in bytes.
pub const MAX_IDENTIFIER_BYTES: usize = 128;

/// Quote an Oracle identifier as `"name"`.
///
/// Quoted identifiers are case-sensitive and may hold any character except a
/// double quote or NUL — Oracle has no escape for `"` inside a quoted name, so
/// one is rejected rather than risk an injection.
pub fn quote_ident_oracle(name: &str) -> Result<String, FaucetError> {
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

/// Split `OWNER.TABLE` (or a bare `TABLE`) into its parts.
pub fn split_table(table: &str) -> Result<(Option<String>, String), FaucetError> {
    let parts: Vec<&str> = table.split('.').collect();
    match parts.as_slice() {
        [t] if !t.is_empty() => Ok((None, (*t).to_string())),
        [o, t] if !o.is_empty() && !t.is_empty() => Ok((Some((*o).to_string()), (*t).to_string())),
        _ => Err(FaucetError::Config(format!(
            "invalid Oracle table name {table:?}: expected `TABLE` or `OWNER.TABLE`"
        ))),
    }
}

/// Quote a (possibly owner-qualified) table: `app.events` → `"app"."events"`.
pub fn quote_table_oracle(table: &str) -> Result<String, FaucetError> {
    let (owner, name) = split_table(table)?;
    let name = quote_ident_oracle(&name)?;
    Ok(match owner {
        Some(o) => format!("{}.{name}", quote_ident_oracle(&o)?),
        None => name,
    })
}

/// Render a string as an Oracle single-quoted literal, doubling interior quotes.
pub fn string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Strip trailing whitespace and statement terminators so a query can be
/// embedded as a sub-select.
pub fn trim_statement(sql: &str) -> &str {
    sql.trim_end_matches(|c: char| c.is_whitespace() || c == ';')
}

/// Infallible quoting for a shard key already validated with
/// [`quote_ident_oracle`].
pub fn quote_validated(name: &str) -> String {
    format!("\"{name}\"")
}

/// Wrap a query in an Oracle PK-range predicate. Oracle rejects `AS` before a
/// table alias and (before 23ai) the bare `TRUE` literal that
/// [`PkShardBounds::wrap`] emits, so the predicate is rendered here.
pub fn shard_wrap(bounds: &PkShardBounds, inner: &str) -> String {
    let key = quote_validated(&bounds.key);
    let mut parts: Vec<String> = Vec::with_capacity(2);
    if !bounds.lo_unbounded {
        parts.push(format!("{key} >= {}", bounds.lo));
    }
    if !bounds.hi_unbounded {
        parts.push(format!("{key} < {}", bounds.hi));
    }
    let range = parts.join(" AND ");
    let predicate = match (range.is_empty(), bounds.include_null) {
        (true, _) => "1 = 1".to_string(),
        (false, true) => format!("(({range}) OR {key} IS NULL)"),
        (false, false) => range,
    };
    format!(
        "SELECT * FROM ({}) \"_FAUCET_SHARD\" WHERE {predicate}",
        trim_statement(inner)
    )
}

/// The `MIN`/`MAX` probe a PK-range enumeration runs once over the base query.
pub fn shard_bounds_query(inner: &str, quoted_key: &str) -> String {
    format!(
        "SELECT CAST(MIN({quoted_key}) AS NUMBER(19)) AS \"LO\", \
         CAST(MAX({quoted_key}) AS NUMBER(19)) AS \"HI\" FROM ({}) \"_FAUCET_BOUNDS\"",
        trim_statement(inner)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::ShardSpec;
    use serde_json::json;

    #[test]
    fn quote_ident_rules() {
        assert_eq!(quote_ident_oracle("ID").unwrap(), "\"ID\"");
        assert_eq!(quote_ident_oracle("mixed Case").unwrap(), "\"mixed Case\"");
        assert!(quote_ident_oracle("").is_err());
        assert!(quote_ident_oracle("a\"b").is_err());
        assert!(quote_ident_oracle("a\0b").is_err());
        assert!(quote_ident_oracle(&"x".repeat(129)).is_err());
        assert!(quote_ident_oracle(&"x".repeat(128)).is_ok());
    }

    #[test]
    fn split_and_quote_table() {
        assert_eq!(split_table("T").unwrap(), (None, "T".into()));
        assert_eq!(
            split_table("APP.T").unwrap(),
            (Some("APP".into()), "T".into())
        );
        assert!(split_table("a.b.c").is_err());
        assert!(split_table(".t").is_err());
        assert!(split_table("").is_err());
        assert_eq!(
            quote_table_oracle("app.events").unwrap(),
            "\"app\".\"events\""
        );
        assert_eq!(quote_table_oracle("EVENTS").unwrap(), "\"EVENTS\"");
        assert!(quote_table_oracle("a\".b").is_err());
    }

    #[test]
    fn literals_and_trim() {
        assert_eq!(string_literal("o'k"), "'o''k'");
        assert_eq!(
            trim_statement("SELECT 1 FROM dual ;\n"),
            "SELECT 1 FROM dual"
        );
    }

    fn bounds(lo: i64, hi: i64, lo_u: bool, hi_u: bool, null: bool) -> PkShardBounds {
        PkShardBounds {
            key: "ID".into(),
            lo,
            hi,
            lo_unbounded: lo_u,
            hi_unbounded: hi_u,
            include_null: null,
        }
    }

    #[test]
    fn shard_wrap_predicates() {
        let sql = shard_wrap(&bounds(10, 20, false, false, false), "SELECT * FROM T;");
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT * FROM T) \"_FAUCET_SHARD\" WHERE \"ID\" >= 10 AND \"ID\" < 20"
        );
        let last = shard_wrap(&bounds(10, 0, false, true, true), "q");
        assert!(
            last.ends_with("WHERE ((\"ID\" >= 10) OR \"ID\" IS NULL)"),
            "{last}"
        );
        let whole = shard_wrap(&bounds(0, 0, true, true, true), "q");
        assert!(whole.ends_with("WHERE 1 = 1"), "{whole}");
        let first = shard_wrap(&bounds(0, 5, true, false, false), "q");
        assert!(first.ends_with("WHERE \"ID\" < 5"), "{first}");
    }

    #[test]
    fn shard_wrap_from_planned_spec() {
        let spec = ShardSpec::new(
            "0",
            json!({"key": "ID", "lo": 1, "hi": 9, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let b = PkShardBounds::from_spec(&spec).unwrap();
        assert!(shard_wrap(&b, "q").contains("\"ID\" >= 1 AND \"ID\" < 9"));
    }

    #[test]
    fn bounds_query_shape() {
        let q = shard_bounds_query("SELECT * FROM T;", "\"ID\"");
        assert_eq!(
            q,
            "SELECT CAST(MIN(\"ID\") AS NUMBER(19)) AS \"LO\", CAST(MAX(\"ID\") AS NUMBER(19)) \
             AS \"HI\" FROM (SELECT * FROM T) \"_FAUCET_BOUNDS\""
        );
    }
}

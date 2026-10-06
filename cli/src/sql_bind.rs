//! Fan-out tokens in SQL source queries become bind parameters (#789 SQL-01).
//!
//! A child or `for_each` row may reference upstream data in its SQL source
//! query (`WHERE customer_id = ${customers.id}`). Those values come from
//! another system, so they must never be spliced into the SQL text. Before the
//! executor's generic token pass runs, [`plan`] rewrites every such token in the
//! connector's query field into a `{faucet_bind_N}` placeholder; the values are
//! resolved by [`bind_values`] and handed to the connector as its fetch context,
//! which every SQL source turns into native bind markers.
//!
//! A token inside a quoted literal (`'${p.name}'`) binds when it is the whole
//! literal; a token embedded in a longer literal, inside a quoted identifier or
//! inside a comment cannot be bound and is refused.

use std::collections::HashMap;

use faucet_core::Value;

use crate::error::{CliError, CliResult};
use crate::interpolate::{Directive, classify_directive};

/// Source kinds whose query text is SQL, with the config field that holds it.
pub const SQL_QUERY_FIELDS: &[(&str, &str)] = &[
    ("postgres", "query"),
    ("mysql", "query"),
    ("sqlite", "query"),
    ("duckdb", "query"),
    ("redshift", "query"),
    ("mssql", "query"),
    ("clickhouse", "query"),
    ("spanner", "query"),
    ("snowflake", "query"),
    ("bigquery", "query"),
    ("oracle", "query"),
    ("databricks", "sql"),
];

/// Prefix of the context keys [`plan`] generates.
pub const BIND_KEY_PREFIX: &str = "faucet_bind_";

/// The SQL query field of a source kind, when it has one.
pub fn query_field(kind: &str) -> Option<&'static str> {
    SQL_QUERY_FIELDS
        .iter()
        .find(|(k, _)| *k == kind)
        .map(|(_, f)| *f)
}

/// One token [`plan`] replaced: the generated context key and the `${id.path}`
/// reference it stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindParam {
    pub key: String,
    pub id: String,
    pub path: String,
}

/// A query with its fan-out tokens replaced by `{faucet_bind_N}` placeholders.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindPlan {
    pub query: String,
    pub params: Vec<BindParam>,
}

#[derive(Clone, Copy)]
enum Region {
    Quoted(u8),
    LineComment,
    BlockComment,
}

/// Rewrite every `${id.path}` token whose `id` satisfies `is_fanout` into a
/// bind placeholder. Other tokens (`${now.*}`, `$${` escapes) are copied
/// verbatim. Errors name the token and why it cannot be bound.
pub fn plan(query: &str, is_fanout: impl Fn(&str) -> bool) -> Result<BindPlan, String> {
    let bytes = query.as_bytes();
    let mut out = String::with_capacity(query.len());
    let mut params: Vec<BindParam> = Vec::new();
    let mut i = 0;
    let mut copied = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'$' if bytes.get(i + 1) == Some(&b'$') && bytes.get(i + 2) == Some(&b'{') => {
                i += 2;
            }
            b'$' if bytes.get(i + 1) == Some(&b'{') => {
                let Some((end, id, path)) = token_at(query, i) else {
                    break;
                };
                if is_fanout(id) {
                    out.push_str(&query[copied..i]);
                    push_param(&mut out, &mut params, id, path);
                    copied = end;
                }
                i = end;
            }
            b'\'' => {
                let end = region_end(bytes, i, Region::Quoted(b'\''));
                let closed = end >= i + 2 && bytes[end - 1] == b'\'';
                let body = &query[i + 1..if closed { end - 1 } else { end }];
                if closed
                    && let Some((tok_end, id, path)) = token_at(body, 0)
                    && tok_end == body.len()
                    && is_fanout(id)
                {
                    out.push_str(&query[copied..i]);
                    push_param(&mut out, &mut params, id, path);
                    copied = end;
                } else if let Some(token) = first_fanout_token(body, &is_fanout) {
                    return Err(format!(
                        "`{token}` is part of a longer quoted string, which a bind parameter \
                         cannot fill — make the token the whole literal (`'{token}'`) or build \
                         the string in SQL (e.g. `'prefix-' || {token}`)"
                    ));
                }
                i = end;
            }
            q @ (b'"' | b'`') => {
                let end = region_end(bytes, i, Region::Quoted(q));
                if let Some(token) = first_fanout_token(&query[i..end], &is_fanout) {
                    return Err(format!(
                        "`{token}` is inside a quoted identifier; an identifier cannot be a \
                         bind parameter, so upstream data must not choose it"
                    ));
                }
                i = end;
            }
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                let end = region_end(bytes, i, Region::LineComment);
                reject_in_comment(&query[i..end], &is_fanout)?;
                i = end;
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                let end = region_end(bytes, i, Region::BlockComment);
                reject_in_comment(&query[i..end], &is_fanout)?;
                i = end;
            }
            _ => i += 1,
        }
    }
    out.push_str(&query[copied..]);
    Ok(BindPlan { query: out, params })
}

fn push_param(out: &mut String, params: &mut Vec<BindParam>, id: &str, path: &str) {
    let key = format!("{BIND_KEY_PREFIX}{}", params.len());
    out.push('{');
    out.push_str(&key);
    out.push('}');
    params.push(BindParam {
        key,
        id: id.to_string(),
        path: path.to_string(),
    });
}

fn reject_in_comment(text: &str, is_fanout: &impl Fn(&str) -> bool) -> Result<(), String> {
    match first_fanout_token(text, is_fanout) {
        Some(token) => Err(format!(
            "`{token}` is inside a SQL comment, where a bind parameter cannot go — remove it"
        )),
        None => Ok(()),
    }
}

/// The `${...}` token starting at `at`: its end offset (exclusive), id and path.
fn token_at(s: &str, at: usize) -> Option<(usize, &str, &str)> {
    if !s[at..].starts_with("${") {
        return None;
    }
    let body_start = at + 2;
    let end = body_start + s[body_start..].find('}')?;
    match classify_directive(&s[body_start..end]) {
        Directive::Deferred { id, path } => Some((end + 1, id, path)),
        Directive::LoadTime { .. } => Some((end + 1, "", "")),
    }
}

fn first_fanout_token(s: &str, is_fanout: &impl Fn(&str) -> bool) -> Option<String> {
    crate::interpolate::iter_directives(s).find_map(|(token, dir)| match dir {
        Directive::Deferred { id, .. } if is_fanout(id) => Some(token.to_string()),
        _ => None,
    })
}

/// The exclusive end offset of the quoted string or comment starting at `start`.
fn region_end(bytes: &[u8], start: usize, region: Region) -> usize {
    match region {
        Region::Quoted(q) => {
            let mut i = start + 1;
            while i < bytes.len() {
                if bytes[i] == q {
                    if bytes.get(i + 1) == Some(&q) {
                        i += 2;
                        continue;
                    }
                    return i + 1;
                }
                i += 1;
            }
            bytes.len()
        }
        Region::LineComment => bytes[start..]
            .iter()
            .position(|b| *b == b'\n')
            .map_or(bytes.len(), |p| start + p),
        Region::BlockComment => bytes[start + 2..]
            .windows(2)
            .position(|w| w == b"*/")
            .map_or(bytes.len(), |p| start + 2 + p + 2),
    }
}

/// Resolve each planned token against the fan-out context into the connector's
/// fetch context.
pub fn bind_values(
    plan: &BindPlan,
    ctx: &HashMap<String, Value>,
) -> CliResult<HashMap<String, Value>> {
    let mut out = HashMap::with_capacity(plan.params.len());
    for p in &plan.params {
        let record = ctx
            .get(&p.id)
            .ok_or_else(|| CliError::UnknownInterpolationId {
                id: p.id.clone(),
                token: format!("${{{}}}", reference(p)),
            })?;
        let value = crate::interpolate::resolve_dotted(record, &p.path).ok_or_else(|| {
            CliError::MissingRecordField {
                id: p.id.clone(),
                path: p.path.clone(),
            }
        })?;
        if matches!(value, Value::Array(_) | Value::Object(_)) {
            return Err(CliError::Config(format!(
                "`${{{}}}` resolves to a list or object, which cannot be bound into a SQL \
                 query as one value",
                reference(p)
            )));
        }
        out.insert(p.key.clone(), value);
    }
    Ok(out)
}

fn reference(p: &BindParam) -> String {
    if p.path.is_empty() {
        p.id.clone()
    } else {
        format!("{}.{}", p.id, p.path)
    }
}

/// Plan the query of a SQL source config in place: the query field is
/// rewritten and the bind values returned. Non-SQL kinds and queries without
/// fan-out tokens return an empty map and leave the config untouched.
pub fn bind_source_query(
    kind: &str,
    cfg: &mut Value,
    ctx: &HashMap<String, Value>,
) -> CliResult<HashMap<String, Value>> {
    let Some(field) = query_field(kind) else {
        return Ok(HashMap::new());
    };
    let Some(Value::String(query)) = cfg.get(field) else {
        return Ok(HashMap::new());
    };
    let planned = plan(query, |id| ctx.contains_key(id))
        .map_err(|e| CliError::Config(format!("{kind} source `{field}`: {e}")))?;
    if planned.params.is_empty() {
        return Ok(HashMap::new());
    }
    let values = bind_values(&planned, ctx)?;
    cfg[field] = Value::String(planned.query);
    Ok(values)
}

/// Load-time check that every fan-out token in a SQL source query can be bound.
pub fn check_source_query(
    kind: &str,
    cfg: &Value,
    is_fanout: impl Fn(&str) -> bool,
    owner: &str,
) -> CliResult<()> {
    let Some(field) = query_field(kind) else {
        return Ok(());
    };
    let Some(query) = cfg.get(field).and_then(Value::as_str) else {
        return Ok(());
    };
    plan(query, is_fanout)
        .map(|_| ())
        .map_err(|e| CliError::Config(format!("{owner}: {kind} source `{field}`: {e}")))
}

/// Config fields that name a table, collection or index. A fan-out token may
/// fill one (one table per parent), but the resolved value must be a plain
/// identifier so upstream data cannot smuggle SQL or a path through it.
pub const IDENTIFIER_FIELDS: &[&str] = &[
    "table",
    "table_name",
    "table_id",
    "collection",
    "index",
    "schema",
    "dataset",
    "dataset_id",
];

/// Whether `s` is a plain (optionally dotted) identifier.
pub fn is_safe_identifier(s: &str) -> bool {
    !s.is_empty()
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'$'))
        && !s.starts_with(['.', '-'])
}

/// Refuse a fan-out-resolved identifier field (`table`, `collection`, …) whose
/// value is not a plain identifier. `before` is the config as written, `after`
/// the config with fan-out tokens resolved; only fields that carried a fan-out
/// token are checked, so static values keep whatever syntax they had.
pub fn check_identifier_fields(
    before: &Value,
    after: &Value,
    is_fanout: impl Fn(&str) -> bool,
    owner: &str,
) -> CliResult<()> {
    for field in IDENTIFIER_FIELDS {
        let Some(raw) = before.get(*field).and_then(Value::as_str) else {
            continue;
        };
        if first_fanout_token(raw, &is_fanout).is_none() {
            continue;
        }
        let resolved = after.get(*field).and_then(Value::as_str).unwrap_or("");
        if !is_safe_identifier(resolved) {
            return Err(CliError::Config(format!(
                "{owner} `{field}` resolved to {resolved:?} from upstream data, which is not a \
                 plain identifier (letters, digits, `_`, `.`, `-`, `$`)"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::json;

    fn fanout(id: &str) -> bool {
        id == "p"
    }

    #[test]
    fn bare_token_becomes_placeholder() {
        let p = plan("SELECT * FROM t WHERE id = ${p.id} AND x = 1", fanout).unwrap();
        assert_eq!(
            p.query,
            "SELECT * FROM t WHERE id = {faucet_bind_0} AND x = 1"
        );
        assert_eq!(
            p.params,
            vec![BindParam {
                key: "faucet_bind_0".into(),
                id: "p".into(),
                path: "id".into()
            }]
        );
    }

    #[test]
    fn whole_literal_token_drops_its_quotes() {
        let p = plan("WHERE name = '${p.name}' AND b = ${p}", fanout).unwrap();
        assert_eq!(
            p.query,
            "WHERE name = {faucet_bind_0} AND b = {faucet_bind_1}"
        );
        assert_eq!(p.params[1].path, "");
    }

    #[test]
    fn embedded_literal_token_is_refused() {
        let e = plan("WHERE name = 'x-${p.name}'", fanout).unwrap_err();
        assert!(e.contains("longer quoted string"), "{e}");
    }

    #[test]
    fn quoted_identifier_token_is_refused() {
        for q in ["SELECT * FROM \"${p.t}\"", "SELECT * FROM `${p.t}`"] {
            let e = plan(q, fanout).unwrap_err();
            assert!(e.contains("quoted identifier"), "{e}");
        }
    }

    #[test]
    fn comment_tokens_are_refused() {
        for q in [
            "SELECT 1 -- ${p.id}\nFROM t",
            "SELECT 1 /* ${p.id} */ FROM t",
        ] {
            let e = plan(q, fanout).unwrap_err();
            assert!(e.contains("comment"), "{e}");
        }
    }

    #[test]
    fn non_fanout_and_escaped_tokens_are_copied() {
        let q = "SELECT '${now.date}', $${p.id}, '' , 'it''s', \"col\" -- note\n/* c */ FROM t";
        let p = plan(q, fanout).unwrap();
        assert_eq!(p.query, q);
        assert!(p.params.is_empty());
    }

    #[test]
    fn unterminated_regions_and_tokens_end_cleanly() {
        assert_eq!(
            plan("SELECT '${p.id", fanout).unwrap().query,
            "SELECT '${p.id"
        );
        assert_eq!(
            plan("SELECT ${p.id", fanout).unwrap().query,
            "SELECT ${p.id"
        );
        assert_eq!(plan("SELECT /* x", fanout).unwrap().query, "SELECT /* x");
        assert!(plan("SELECT \"${p.id}", fanout).is_err());
        assert_eq!(
            plan("SELECT '${env:X}'", fanout).unwrap().query,
            "SELECT '${env:X}'"
        );
    }

    #[test]
    fn bind_values_resolve_paths_and_refuse_lists() {
        let ctx = HashMap::from([("p".to_string(), json!({"id": 7, "tags": ["a"]}))]);
        let p = plan("WHERE id = ${p.id}", fanout).unwrap();
        assert_eq!(
            bind_values(&p, &ctx).unwrap(),
            HashMap::from([("faucet_bind_0".to_string(), json!(7))])
        );
        let lists = plan("WHERE t = ${p.tags}", fanout).unwrap();
        assert!(bind_values(&lists, &ctx).is_err());
        let missing = plan("WHERE t = ${p.nope}", fanout).unwrap();
        assert!(matches!(
            bind_values(&missing, &ctx),
            Err(CliError::MissingRecordField { .. })
        ));
        let empty = HashMap::new();
        assert!(matches!(
            bind_values(&p, &empty),
            Err(CliError::UnknownInterpolationId { .. })
        ));
    }

    #[test]
    fn bind_source_query_rewrites_only_sql_kinds() {
        let ctx = HashMap::from([("p".to_string(), json!({"id": "1' OR '1'='1"}))]);
        let mut cfg = json!({"query": "SELECT * FROM t WHERE id = '${p.id}'"});
        let binds = bind_source_query("sqlite", &mut cfg, &ctx).unwrap();
        assert_eq!(cfg["query"], "SELECT * FROM t WHERE id = {faucet_bind_0}");
        assert_eq!(binds["faucet_bind_0"], json!("1' OR '1'='1"));

        let mut rest = json!({"url": "https://x/${p.id}"});
        assert!(
            bind_source_query("rest", &mut rest, &ctx)
                .unwrap()
                .is_empty()
        );
        let mut no_tokens = json!({"query": "SELECT 1"});
        assert!(
            bind_source_query("postgres", &mut no_tokens, &ctx)
                .unwrap()
                .is_empty()
        );
        let mut not_string = json!({"query": 5});
        assert!(
            bind_source_query("mysql", &mut not_string, &ctx)
                .unwrap()
                .is_empty()
        );
        let mut bad = json!({"sql": "SELECT 'a${p.id}'"});
        assert!(bind_source_query("databricks", &mut bad, &ctx).is_err());
    }

    #[test]
    fn check_source_query_reports_the_owner() {
        let ok = json!({"query": "WHERE id = ${p.id}"});
        check_source_query("postgres", &ok, fanout, "row 'c'").unwrap();
        check_source_query("rest", &json!({"query": "'x${p.id}'"}), fanout, "row").unwrap();
        check_source_query("postgres", &json!({}), fanout, "row").unwrap();
        let e = check_source_query("mysql", &json!({"query": "'x${p.id}'"}), fanout, "row 'c'")
            .unwrap_err()
            .to_string();
        assert!(e.contains("row 'c'") && e.contains("mysql"), "{e}");
    }

    #[test]
    fn identifier_fields_must_resolve_to_plain_names() {
        let before = json!({"table": "t_${p.id}", "path": "${p.id}", "schema": "fixed"});
        let ok = json!({"table": "t_42", "path": "x; y", "schema": "fixed"});
        check_identifier_fields(&before, &ok, fanout, "sink").unwrap();
        let bad = json!({"table": "t_1; DROP TABLE x", "schema": "fixed"});
        let e = check_identifier_fields(&before, &bad, fanout, "sink")
            .unwrap_err()
            .to_string();
        assert!(e.contains("`table`"), "{e}");
        let static_odd = json!({"table": "odd name"});
        check_identifier_fields(&static_odd, &static_odd, fanout, "sink").unwrap();
        assert!(is_safe_identifier("ds.tbl$1-x"));
        assert!(!is_safe_identifier(""));
        assert!(!is_safe_identifier(".x"));
        assert!(!is_safe_identifier("a b"));
    }
}

//! Shared utilities used across faucet source and sink crates.

use std::collections::HashMap;

use crate::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;

// ── SQL Utilities ───────────────────────────────────────────────────────────

/// Quote a SQL identifier to prevent SQL injection.
///
/// Wraps the name in double quotes and doubles any embedded double-quotes
/// per the SQL standard (ANSI SQL).
///
/// ```
/// use faucet_core::util::quote_ident;
/// assert_eq!(quote_ident("my_table"), "\"my_table\"");
/// assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
/// ```
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ── JSONPath Extraction ─────────────────────────────────────────────────────

/// Extract records from a JSON value using an optional JSONPath expression.
///
/// - If `path` is `Some`, queries the body with the JSONPath and returns
///   all matched values.
/// - If `path` is `None`, returns the body as-is: arrays are unpacked into
///   individual records, objects/scalars are returned as a single-element vec.
pub fn extract_records(body: &Value, path: Option<&str>) -> Result<Vec<Value>, FaucetError> {
    match path {
        Some(p) => {
            let results = body
                .query(p)
                .map_err(|e| FaucetError::JsonPath(format!("invalid JSONPath '{p}': {e}")))?;
            Ok(results.into_iter().cloned().collect())
        }
        None => match body {
            Value::Array(arr) => Ok(arr.clone()),
            other => Ok(vec![other.clone()]),
        },
    }
}

// ── HTTP Response Handling ──────────────────────────────────────────────────

/// Check an HTTP response status and return a [`FaucetError::HttpStatus`] on
/// non-success responses.
///
/// Reads the response body for error context, truncating to `max_body_len`
/// bytes (default: 2048) to avoid large error messages.
pub async fn check_http_response(
    resp: reqwest::Response,
    max_body_len: usize,
) -> Result<reqwest::Response, FaucetError> {
    if resp.status().is_success() {
        return Ok(resp);
    }

    let status = resp.status().as_u16();
    let url = resp.url().to_string();
    let body_text = resp.text().await.unwrap_or_default();

    let body = if body_text.len() > max_body_len {
        let end = body_text.floor_char_boundary(max_body_len);
        format!("{}...(truncated)", &body_text[..end])
    } else {
        body_text
    };

    Err(FaucetError::HttpStatus { status, url, body })
}

/// Default maximum body length for error responses.
pub const DEFAULT_ERROR_BODY_MAX_LEN: usize = 2048;

// ── Context Utilities ──────────────────────────────────────────────────────

/// Substitute `{key}` placeholders in a template string with values from context.
///
/// Value conversion rules:
/// - `String` -> raw string (no quotes)
/// - `Number` -> number as string
/// - `Bool` -> `"true"` / `"false"`
/// - `Null` -> `"null"`
/// - `Array` / `Object` -> JSON-serialized string
///
/// Unmatched placeholders are left as-is.
///
/// **Warning:** Do NOT use this for SQL queries (SQL injection risk) or for
/// substitution into serialized JSON (corruption risk with special characters).
/// Use [`substitute_context_bind_params`] for SQL and [`substitute_context_json`]
/// for serialized JSON.
pub fn substitute_context(template: &str, context: &HashMap<String, Value>) -> String {
    substitute_single_pass(template, context, |value| match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    })
}

/// Single left-to-right scan that replaces each recognised `{key}` placeholder
/// with `render(value)`. Unmatched placeholders are left verbatim; replacement
/// text is never re-scanned. Shared by [`substitute_context`] and
/// [`substitute_context_json`] so neither is O(template × context) (#78/#36).
fn substitute_single_pass(
    template: &str,
    context: &HashMap<String, Value>,
    render: impl Fn(&Value) -> String,
) -> String {
    if context.is_empty() {
        return template.to_string();
    }
    let mut result = String::with_capacity(template.len());
    let mut last_copied = 0;
    let mut search_from = 0;

    while search_from < template.len() {
        let Some(open_offset) = template[search_from..].find('{') else {
            break;
        };
        let open = search_from + open_offset;
        let Some(close_offset) = template[open + 1..].find('}') else {
            break;
        };
        let close = open + 1 + close_offset;
        let key = &template[open + 1..close];

        if let Some(value) = context.get(key) {
            result.push_str(&template[last_copied..open]);
            result.push_str(&render(value));
            last_copied = close + 1;
            search_from = close + 1;
        } else {
            search_from = open + 1;
        }
    }

    result.push_str(&template[last_copied..]);
    result
}

/// Replace `{key}` placeholders with SQL bind-parameter markers, returning
/// the rewritten query and an ordered list of values to bind.
///
/// Scans the template left-to-right; each recognised placeholder is replaced
/// with the marker produced by `marker_fn(index)`, and the corresponding
/// value is appended to the returned vector.  The same key appearing multiple
/// times produces one bind value per occurrence.
///
/// `start_index` is the 1-based index for the first parameter.
///
/// # Marker functions
///
/// - PostgreSQL: `|i| format!("${i}")`
/// - MySQL / SQLite: `|_| "?".to_string()`
///
/// Placeholders whose key is not present in `context` are left unchanged.
pub fn substitute_context_bind_params(
    template: &str,
    context: &HashMap<String, Value>,
    start_index: usize,
    marker_fn: impl Fn(usize) -> String,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        return (template.to_string(), Vec::new());
    }

    let mut result = String::with_capacity(template.len());
    let mut values = Vec::new();
    let mut param_idx = start_index;
    let mut last_copied = 0;
    let mut search_from = 0;

    while search_from < template.len() {
        let Some(open_offset) = template[search_from..].find('{') else {
            break;
        };
        let open = search_from + open_offset;

        let Some(close_offset) = template[open + 1..].find('}') else {
            break;
        };
        let close = open + 1 + close_offset;
        let key = &template[open + 1..close];

        if let Some(value) = context.get(key) {
            result.push_str(&template[last_copied..open]);
            result.push_str(&marker_fn(param_idx));
            values.push(value.clone());
            param_idx += 1;
            last_copied = close + 1;
            search_from = close + 1;
        } else {
            search_from = open + 1;
        }
    }

    result.push_str(&template[last_copied..]);
    (result, values)
}

/// Substitute `{key}` placeholders within a serialized JSON string, escaping
/// string values so that the result remains valid JSON.
///
/// Use this instead of [`substitute_context`] when the template is a
/// `serde_json`-serialized value that will be deserialized back after
/// substitution.  String values are JSON-escaped (double-quotes, backslashes,
/// and control characters).  Numbers, bools, and null are substituted as-is.
pub fn substitute_context_json(template: &str, context: &HashMap<String, Value>) -> String {
    substitute_single_pass(template, context, |value| match value {
        Value::String(s) => json_escape_string(s),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => "null".to_string(),
        other => other.to_string(),
    })
}

/// Escape a string for safe embedding inside a JSON string value.
///
/// Handles double-quotes, backslashes, and control characters per RFC 8259.
fn json_escape_string(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c.is_control() => {
                escaped.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => escaped.push(c),
        }
    }
    escaped
}

/// Strip credentials from a connection string so it can be used as a lineage
/// dataset URI without leaking secrets. Handles two shapes, best-effort:
///
/// - **URL userinfo** — `scheme://user:pass@host/...` → `scheme://host/...`
///   (the `user[:pass]@` between `://` and the authority terminator is removed).
/// - **Key/value (ADO.NET) connection strings** — any `Password=...` / `Pwd=...`
///   segment (case-insensitive key) has its value replaced with `***`.
///
/// Input with neither shape is returned unchanged.
pub fn redact_uri_credentials(uri: &str) -> String {
    let mut out = uri.to_string();
    // 1) URL userinfo: `scheme://user:pass@host/...` → `scheme://host/...`.
    //
    // A naive "first '/' or '?' terminates the authority, first '@' delimits
    // userinfo" scan LEAKS passwords that contain '/', '?' or '@' (very common
    // in unencoded DB connection strings): the early terminator truncates the
    // authority before the real '@', and the first '@' splits inside the
    // password. Since a host/port never contains '@', the userinfo→host
    // delimiter is the LAST '@' whose following host segment (up to the next
    // '/', '?' or '#') is a non-empty, host-shaped token. Picking that '@'
    // tolerates arbitrary '/', '?' and '@' inside the password.
    if let Some(scheme_end) = out.find("://") {
        let after = scheme_end + 3;
        let tail = &out[after..];
        let delim = tail
            .char_indices()
            .rev()
            .find(|&(at, c)| {
                c == '@' && {
                    let host = &tail[at + 1..];
                    let host_end = host.find(['/', '?', '#']).unwrap_or(host.len());
                    let host = &host[..host_end];
                    !host.is_empty() && !host.contains('@') && is_host_shaped(host)
                }
            })
            .map(|(at, _)| at);
        if let Some(at) = delim {
            // Remove "user:pass@" inclusive of the '@'.
            out.replace_range(after..after + at + 1, "");
        }
    }
    // 2) ADO.NET-style and query-string secret tokens. Splitting on both
    //    ';' (ADO.NET) and '&'/'?' (URL query) catches a secret carried as a
    //    query parameter (`...?token=secret`) as well as keyword form. The key
    //    denylist covers the common secret parameter names, not just
    //    password/pwd (audit #321 M11): a leaked `?api_key=…` or SAS `?sig=…`
    //    was previously passed through verbatim into lineage / catalog output.
    if out.contains('=') {
        out = out
            .split_inclusive([';', '&', '?'])
            .map(|seg| {
                // Preserve any trailing delimiter the split kept on the segment.
                let (body, delim) = match seg.char_indices().next_back() {
                    Some((i, ';' | '&' | '?')) => (&seg[..i], &seg[i..]),
                    _ => (seg, ""),
                };
                match body.find('=') {
                    Some(eq) if is_secret_kv_key(&body[..eq]) => {
                        format!("{}=***{delim}", &body[..eq])
                    }
                    _ => seg.to_string(),
                }
            })
            .collect::<String>();
    }
    out
}

/// Whether a connection-string / query-string key names a credential whose
/// value must be redacted from a lineage / catalog URI.
///
/// Matches a denylist case-insensitively after stripping `-`/`_`/space, so
/// `api_key`, `api-key`, and `apikey` all match one entry. Covers the common
/// secret parameter names beyond `password`/`pwd` (audit #321 M11).
fn is_secret_kv_key(key: &str) -> bool {
    let norm: String = key
        .trim()
        .chars()
        .filter(|c| !matches!(c, '-' | '_' | ' '))
        .flat_map(char::to_lowercase)
        .collect();
    matches!(
        norm.as_str(),
        "password"
            | "pwd"
            | "token"
            | "apikey"
            | "accesstoken"
            | "refreshtoken"
            | "sessiontoken"
            | "clientsecret"
            | "secret"
            | "secretkey"
            | "accesskey"
            | "accesskeyid"
            | "secretaccesskey"
            | "sig"
            | "sas"
            | "accountkey"
            | "sharedaccesskey"
            | "authorization"
            | "auth"
    )
}

/// Best-effort check that a string looks like a `host[:port]` authority — used
/// to identify the userinfo→host `@` delimiter when redacting credentials.
/// Accepts letters, digits, `.`, `-`, `:`, `_`, and bracketed IPv6 forms.
fn is_host_shaped(s: &str) -> bool {
    s.bytes().all(|b| {
        b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'_' | b'[' | b']' | b'%')
    })
}

/// Extract context values from a record using JSONPath expressions.
///
/// Each entry in `mapping` is `context_key -> json_path`. The function queries
/// the record with each JSONPath and stores the first matched value under the
/// corresponding context key.
///
/// Returns an error if any JSONPath matches nothing.
pub fn extract_context(
    record: &Value,
    mapping: &HashMap<String, String>,
) -> Result<HashMap<String, Value>, FaucetError> {
    let mut context = HashMap::with_capacity(mapping.len());
    for (context_key, json_path) in mapping {
        let results = record
            .query(json_path.as_str())
            .map_err(|e| FaucetError::JsonPath(format!("invalid JSONPath '{json_path}': {e}")))?;
        let value = results.first().ok_or_else(|| {
            FaucetError::JsonPath(format!(
                "JSONPath '{json_path}' matched nothing in record for context key '{context_key}'"
            ))
        })?;
        context.insert(context_key.clone(), (*value).clone());
    }
    Ok(context)
}

/// Split an identifier into word tokens. Boundaries: whitespace, `_`, `-`, any
/// other non-alphanumeric char, and lower→upper transitions (so `firstName` →
/// `["first", "Name"]`). A multi-char uppercase run stays one token
/// (`"XMLParser"` → `["XMLParser"]`). This is the single source of truth for the
/// `keys_case` transform's word-splitting and for connector code (e.g. the OData
/// fan-out) that must snake_case column names **identically** to that transform —
/// a divergence would land a schema column name that never matches the record
/// key it describes, silently dropping that column's data.
pub fn tokenize_identifier(key: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut prev_was_lower = false;
    for ch in key.chars() {
        if ch.is_alphanumeric() {
            if prev_was_lower && ch.is_uppercase() && !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            current.push(ch);
            prev_was_lower = ch.is_lowercase();
        } else {
            if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
            prev_was_lower = false;
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

/// `snake_case` an identifier using [`tokenize_identifier`] (lowercase tokens
/// joined by `_`). Matches the `keys_case: snake` transform exactly. An
/// all-symbol key (no tokens) is returned unchanged.
pub fn snake_case(key: &str) -> String {
    let tokens = tokenize_identifier(key);
    if tokens.is_empty() {
        return key.to_string();
    }
    tokens
        .iter()
        .map(|t| t.to_lowercase())
        .collect::<Vec<_>>()
        .join("_")
}

#[cfg(test)]
mod snake_tests {
    use super::snake_case;

    #[test]
    fn snake_case_matches_keys_case_transform_rules() {
        // Lower→upper boundaries split; digits stay with their word.
        assert_eq!(snake_case("CustomersV3"), "customers_v3");
        assert_eq!(snake_case("RecId"), "rec_id");
        assert_eq!(
            snake_case("MainAccountBiEntities"),
            "main_account_bi_entities"
        );
        assert_eq!(snake_case("DataAreaId"), "data_area_id");
        // A multi-char uppercase run stays ONE token (the load-bearing case the
        // per-char snake got wrong: "i_s_o..." would never match the record key).
        assert_eq!(snake_case("ISOCurrencyCode"), "isocurrency_code");
        assert_eq!(snake_case("XMLParser"), "xmlparser");
        // Separators + all-symbol keys.
        assert_eq!(snake_case("already_snake"), "already_snake");
        assert_eq!(snake_case("__"), "__");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── quote_ident ─────────────────────────────────────────────────────

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("my_table"), "\"my_table\"");
    }

    #[test]
    fn quote_ident_with_embedded_quotes() {
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn quote_ident_special_chars() {
        assert_eq!(quote_ident("table; DROP"), "\"table; DROP\"");
    }

    // ── extract_records ─────────────────────────────────────────────────

    #[test]
    fn extract_with_path() {
        let body = json!({"data": [{"id": 1}, {"id": 2}]});
        let records = extract_records(&body, Some("$.data[*]")).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_without_path_array() {
        let body = json!([{"id": 1}, {"id": 2}]);
        let records = extract_records(&body, None).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn extract_without_path_object() {
        let body = json!({"id": 1});
        let records = extract_records(&body, None).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn extract_empty_result() {
        let body = json!({"data": []});
        let records = extract_records(&body, Some("$.data[*]")).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn extract_invalid_path_returns_error() {
        let body = json!({"data": 1});
        // jsonpath-rust handles most paths gracefully; test error propagation.
        let result = extract_records(&body, Some("$.data[*]"));
        // This should succeed (empty match) or fail; either is fine as long as
        // it doesn't panic.
        let _ = result;
    }

    // ── substitute_context ──────────────────────────────────────────────

    #[test]
    fn substitute_context_string_values() {
        let mut ctx = HashMap::new();
        ctx.insert("org".to_string(), json!("acme"));
        ctx.insert("repo".to_string(), json!("widgets"));
        let result = substitute_context("/orgs/{org}/repos/{repo}", &ctx);
        assert_eq!(result, "/orgs/acme/repos/widgets");
    }

    #[test]
    fn substitute_context_number_value() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(42));
        let result = substitute_context("/items/{id}", &ctx);
        assert_eq!(result, "/items/42");
    }

    #[test]
    fn substitute_context_bool_value() {
        let mut ctx = HashMap::new();
        ctx.insert("active".to_string(), json!(true));
        let result = substitute_context("/filter?active={active}", &ctx);
        assert_eq!(result, "/filter?active=true");
    }

    #[test]
    fn substitute_context_null_value() {
        let mut ctx = HashMap::new();
        ctx.insert("val".to_string(), json!(null));
        let result = substitute_context("/x/{val}", &ctx);
        assert_eq!(result, "/x/null");
    }

    #[test]
    fn substitute_context_array_value() {
        let mut ctx = HashMap::new();
        ctx.insert("ids".to_string(), json!([1, 2, 3]));
        let result = substitute_context("/x/{ids}", &ctx);
        assert_eq!(result, "/x/[1,2,3]");
    }

    #[test]
    fn substitute_context_unmatched_placeholder_left_as_is() {
        let ctx = HashMap::new();
        let result = substitute_context("/orgs/{org}/repos", &ctx);
        assert_eq!(result, "/orgs/{org}/repos");
    }

    #[test]
    fn substitute_context_empty_template() {
        let ctx = HashMap::new();
        let result = substitute_context("", &ctx);
        assert_eq!(result, "");
    }

    #[test]
    fn substitute_context_replaces_all_occurrences() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), Value::String("42".to_string()));
        let result = substitute_context("/a/{id}/b/{id}", &ctx);
        assert_eq!(result, "/a/42/b/42");
    }

    #[test]
    fn substitute_context_does_not_rescan_replacement() {
        // Single-pass: a replacement value that itself looks like a placeholder
        // is emitted verbatim, never re-substituted (#78/#36).
        let mut ctx = HashMap::new();
        ctx.insert("a".to_string(), Value::String("{b}".to_string()));
        ctx.insert("b".to_string(), Value::String("SECRET".to_string()));
        let result = substitute_context("{a}", &ctx);
        assert_eq!(result, "{b}");
    }

    // ── extract_context ─────────────────────────────────────────────────

    #[test]
    fn extract_context_simple_paths() {
        let record = json!({"id": 1, "name": "alice"});
        let mut mapping = HashMap::new();
        mapping.insert("user_id".to_string(), "$.id".to_string());
        mapping.insert("user_name".to_string(), "$.name".to_string());
        let ctx = extract_context(&record, &mapping).unwrap();
        assert_eq!(ctx["user_id"], json!(1));
        assert_eq!(ctx["user_name"], json!("alice"));
    }

    #[test]
    fn extract_context_nested_path() {
        let record = json!({"data": {"info": {"id": 99}}});
        let mut mapping = HashMap::new();
        mapping.insert("deep_id".to_string(), "$.data.info.id".to_string());
        let ctx = extract_context(&record, &mapping).unwrap();
        assert_eq!(ctx["deep_id"], json!(99));
    }

    #[test]
    fn extract_context_missing_path_returns_error() {
        let record = json!({"id": 1});
        let mut mapping = HashMap::new();
        mapping.insert("missing".to_string(), "$.nonexistent".to_string());
        let result = extract_context(&record, &mapping);
        assert!(result.is_err());
    }

    #[test]
    fn extract_context_empty_mapping() {
        let record = json!({"id": 1});
        let mapping = HashMap::new();
        let ctx = extract_context(&record, &mapping).unwrap();
        assert!(ctx.is_empty());
    }

    // ── substitute_context_bind_params ──────────────────────────────────

    #[test]
    fn bind_params_postgres_style() {
        let mut ctx = HashMap::new();
        ctx.insert("org".to_string(), json!("acme"));
        ctx.insert("id".to_string(), json!(42));
        let (query, values) = substitute_context_bind_params(
            "SELECT * FROM t WHERE org = {org} AND id = {id}",
            &ctx,
            1,
            |i| format!("${i}"),
        );
        assert_eq!(query, "SELECT * FROM t WHERE org = $1 AND id = $2");
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], json!("acme"));
        assert_eq!(values[1], json!(42));
    }

    #[test]
    fn bind_params_question_mark_style() {
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), json!("test"));
        let (query, values) =
            substitute_context_bind_params("SELECT * FROM t WHERE name = {name}", &ctx, 1, |_| {
                "?".to_string()
            });
        assert_eq!(query, "SELECT * FROM t WHERE name = ?");
        assert_eq!(values, vec![json!("test")]);
    }

    #[test]
    fn bind_params_duplicate_key_produces_multiple_binds() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(5));
        let (query, values) = substitute_context_bind_params(
            "SELECT * FROM t WHERE a = {id} OR b = {id}",
            &ctx,
            3,
            |i| format!("${i}"),
        );
        assert_eq!(query, "SELECT * FROM t WHERE a = $3 OR b = $4");
        assert_eq!(values, vec![json!(5), json!(5)]);
    }

    #[test]
    fn bind_params_unknown_key_left_as_is() {
        let ctx = HashMap::new();
        let (query, values) =
            substitute_context_bind_params("SELECT * FROM t WHERE x = {unknown}", &ctx, 1, |i| {
                format!("${i}")
            });
        assert_eq!(query, "SELECT * FROM t WHERE x = {unknown}");
        assert!(values.is_empty());
    }

    #[test]
    fn bind_params_mixed_known_and_unknown() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(1));
        let (query, values) = substitute_context_bind_params(
            "SELECT * FROM t WHERE id = {id} AND x = {unknown}",
            &ctx,
            1,
            |i| format!("${i}"),
        );
        assert_eq!(query, "SELECT * FROM t WHERE id = $1 AND x = {unknown}");
        assert_eq!(values, vec![json!(1)]);
    }

    #[test]
    fn bind_params_empty_context() {
        let ctx = HashMap::new();
        let (query, values) =
            substitute_context_bind_params("SELECT 1", &ctx, 1, |i| format!("${i}"));
        assert_eq!(query, "SELECT 1");
        assert!(values.is_empty());
    }

    #[test]
    fn bind_params_start_index_offset() {
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), json!("x"));
        let (query, values) =
            substitute_context_bind_params("SELECT * FROM t WHERE name = {name}", &ctx, 5, |i| {
                format!("${i}")
            });
        assert_eq!(query, "SELECT * FROM t WHERE name = $5");
        assert_eq!(values, vec![json!("x")]);
    }

    // ── substitute_context_json ─────────────────────────────────────────

    #[test]
    fn json_sub_escapes_double_quotes() {
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), json!(r#"O'Brien "Bob""#));
        let template = r#"{"name":"{name}"}"#;
        let result = substitute_context_json(template, &ctx);
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["name"], r#"O'Brien "Bob""#);
    }

    #[test]
    fn json_sub_escapes_backslashes() {
        let mut ctx = HashMap::new();
        ctx.insert("path".to_string(), json!("C:\\Users\\test"));
        let template = r#"{"path":"{path}"}"#;
        let result = substitute_context_json(template, &ctx);
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["path"], "C:\\Users\\test");
    }

    #[test]
    fn json_sub_escapes_control_chars() {
        let mut ctx = HashMap::new();
        ctx.insert("text".to_string(), json!("line1\nline2\ttab"));
        let template = r#"{"text":"{text}"}"#;
        let result = substitute_context_json(template, &ctx);
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["text"], "line1\nline2\ttab");
    }

    #[test]
    fn json_sub_number_value() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(42));
        let template = r#"{"user_id":"{id}"}"#;
        let result = substitute_context_json(template, &ctx);
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["user_id"], "42");
    }

    #[test]
    fn json_sub_preserves_valid_json_without_special_chars() {
        let mut ctx = HashMap::new();
        ctx.insert("name".to_string(), json!("alice"));
        let template = r#"{"filter":{"name":"{name}"}}"#;
        let result = substitute_context_json(template, &ctx);
        let parsed: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["filter"]["name"], "alice");
    }

    // ── json_escape_string ──────────────────────────────────────────────

    #[test]
    fn json_escape_plain_string() {
        assert_eq!(json_escape_string("hello"), "hello");
    }

    #[test]
    fn json_escape_quotes_and_backslashes() {
        assert_eq!(json_escape_string(r#"a"b\c"#), r#"a\"b\\c"#);
    }

    #[test]
    fn json_escape_newlines_and_tabs() {
        assert_eq!(json_escape_string("a\nb\tc"), "a\\nb\\tc");
    }

    // ── redact_uri_credentials ──────────────────────────────────────────

    #[test]
    fn redact_strips_url_userinfo() {
        assert_eq!(
            redact_uri_credentials("postgres://user:pass@host:5432/db"),
            "postgres://host:5432/db"
        );
        assert_eq!(
            redact_uri_credentials("mongodb://u:p@h/db?x=1"),
            "mongodb://h/db?x=1"
        );
    }

    #[test]
    fn redact_strips_user_only_userinfo() {
        assert_eq!(
            redact_uri_credentials("redis://user@127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
    }

    #[test]
    fn redact_handles_adonet_password_tokens() {
        assert_eq!(
            redact_uri_credentials("Server=tcp:h,1433;Database=db;User Id=sa;Password=secret;"),
            "Server=tcp:h,1433;Database=db;User Id=sa;Password=***;"
        );
        assert_eq!(
            redact_uri_credentials("server=h;pwd=secret"),
            "server=h;pwd=***"
        );
    }

    #[test]
    fn redact_passthrough_when_no_credentials() {
        assert_eq!(
            redact_uri_credentials("s3://bucket/prefix"),
            "s3://bucket/prefix"
        );
        assert_eq!(
            redact_uri_credentials("file:///tmp/x.csv"),
            "file:///tmp/x.csv"
        );
    }

    #[test]
    fn redact_strips_password_containing_special_chars() {
        // Passwords with '/', '?' or '@' must not leak (F5): the userinfo→host
        // delimiter is the LAST '@', and the host segment after it is what's kept.
        assert_eq!(
            redact_uri_credentials("postgres://user:p/w@host:5432/db"),
            "postgres://host:5432/db"
        );
        assert_eq!(
            redact_uri_credentials("postgres://user:p?w@host/db"),
            "postgres://host/db"
        );
        assert_eq!(
            redact_uri_credentials("postgres://user:p@ss@host/db"),
            "postgres://host/db"
        );
        assert_eq!(
            redact_uri_credentials("mysql://u:a/b?c@d@127.0.0.1:3306/app"),
            "mysql://127.0.0.1:3306/app"
        );
    }

    #[test]
    fn redact_strips_query_string_password() {
        assert_eq!(
            redact_uri_credentials("https://host/api?user=sa&password=secret&x=1"),
            "https://host/api?user=sa&password=***&x=1"
        );
        assert_eq!(
            redact_uri_credentials("snowflake://host/db?password=secret"),
            "snowflake://host/db?password=***"
        );
    }

    #[test]
    fn redact_strips_common_secret_query_keys() {
        // #321 M11: secret parameter names beyond password/pwd must be redacted
        // before a URI is emitted to lineage / the catalog.
        assert_eq!(
            redact_uri_credentials("https://api.example.com/v2/?api_key=SECRET"),
            "https://api.example.com/v2/?api_key=***"
        );
        assert_eq!(
            redact_uri_credentials("https://h/?token=abc&access_token=xyz&keep=1"),
            "https://h/?token=***&access_token=***&keep=1"
        );
        // Azure SAS style, and separator/casing variants normalize to one key.
        assert_eq!(
            redact_uri_credentials("https://acct.blob.core.windows.net/c?sig=DEAD&sp=r"),
            "https://acct.blob.core.windows.net/c?sig=***&sp=r"
        );
        assert_eq!(
            redact_uri_credentials("db=x;Client-Secret=shh;Account_Key=k"),
            "db=x;Client-Secret=***;Account_Key=***"
        );
        // A non-secret key is untouched.
        assert_eq!(
            redact_uri_credentials("https://h/?region=us-east-1"),
            "https://h/?region=us-east-1"
        );
    }
}

/// Narrow a JSON `u64` to the `i64` a **signed** 64-bit column can hold, or fail.
///
/// A `serde_json::Number` that reports `is_u64()` but not `is_i64()` is above
/// `i64::MAX`. Casting it with `as i64` *wraps* — `9223372036854775808u64`
/// becomes `-9223372036854775808` — so a large unsigned id would be written, or
/// compared against, as a large negative number with nothing raised. Two ways
/// that bites: a sink silently corrupts the value, and a source binding an
/// incremental-replication bookmark compares against a negative bound, so rows
/// are re-read or skipped (#462).
///
/// Backends with a native unsigned type (MySQL's `UNSIGNED BIGINT`) must **not**
/// use this — they bind the `u64` directly and round-trip losslessly.
///
/// `context` names what is being bound (a column, or `parameter N`) so the error
/// tells an operator where to look.
pub fn u64_to_signed(value: u64, context: &str) -> Result<i64, crate::FaucetError> {
    i64::try_from(value).map_err(|_| {
        crate::FaucetError::Config(format!(
            "{context}: {value} exceeds the maximum a signed 64-bit column can hold \
             ({}). This backend has no unsigned integer type — store the value in a \
             NUMERIC/TEXT column, or convert it with a `cast` transform. It is not \
             silently truncated because the wrapped value would be negative",
            i64::MAX
        ))
    })
}

#[cfg(test)]
mod u64_to_signed_tests {
    use super::*;

    #[test]
    fn accepts_everything_a_signed_column_can_hold() {
        assert_eq!(u64_to_signed(0, "c").unwrap(), 0);
        assert_eq!(u64_to_signed(42, "c").unwrap(), 42);
        assert_eq!(
            u64_to_signed(i64::MAX as u64, "c").unwrap(),
            i64::MAX,
            "the boundary itself fits"
        );
    }

    #[test]
    fn rejects_above_the_boundary_instead_of_wrapping() {
        // The exact value that `as i64` would turn into i64::MIN.
        let just_over = i64::MAX as u64 + 1;
        let err = match u64_to_signed(just_over, "column \"id\"") {
            Err(e) => e,
            Ok(v) => panic!("must not accept {just_over} (as i64 would be {})", v),
        };
        let msg = err.to_string();
        assert!(msg.contains("column \"id\""), "{msg}");
        assert!(msg.contains(&just_over.to_string()), "{msg}");
        // The wrapped form must never appear as if it were the value.
        assert!(!msg.contains("-9223372036854775808"), "{msg}");

        assert!(u64_to_signed(u64::MAX, "c").is_err());
    }
}

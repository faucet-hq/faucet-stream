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

/// [`extract_records`] over an owned body: a plain path (`$.a.b`, `$.a.b[*]`)
/// and the no-path case move the records out of `body` instead of copying
/// them; any other path is evaluated as JSONPath. The result is the same.
pub fn extract_records_owned(
    mut body: Value,
    path: Option<&str>,
) -> Result<Vec<Value>, FaucetError> {
    let Some(p) = path else {
        return Ok(match body {
            Value::Array(arr) => arr,
            other => vec![other],
        });
    };
    let Some((segments, wildcard)) = plain_path(p) else {
        return extract_records(&body, path);
    };
    let mut node = &mut body;
    for seg in &segments {
        match node.as_object_mut().and_then(|m| m.get_mut(*seg)) {
            Some(next) => node = next,
            None => return extract_records(&body, path),
        }
    }
    if !wildcard {
        return Ok(vec![std::mem::take(node)]);
    }
    match node {
        Value::Array(arr) => Ok(std::mem::take(arr)),
        _ => extract_records(&body, path),
    }
}

/// `$` + `.name` segments + an optional trailing `[*]`; `None` for anything
/// else.
fn plain_path(p: &str) -> Option<(Vec<&str>, bool)> {
    let rest = p.strip_prefix('$')?;
    let (rest, wildcard) = match rest.strip_suffix("[*]") {
        Some(r) => (r, true),
        None => (rest, false),
    };
    if rest.is_empty() {
        return Some((Vec::new(), wildcard));
    }
    let segments: Vec<&str> = rest.strip_prefix('.')?.split('.').collect();
    let ident = |s: &&str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    };
    segments.iter().all(ident).then_some((segments, wildcard))
}

// ── HTTP Response Handling ──────────────────────────────────────────────────

/// Check an HTTP response status and return a [`FaucetError::HttpStatus`] on
/// non-success responses.
///
/// Reads at most `max_body_len` bytes (default: 2048) of the response body for
/// error context, so a huge error page is never buffered whole.
pub async fn check_http_response(
    resp: reqwest::Response,
    max_body_len: usize,
) -> Result<reqwest::Response, FaucetError> {
    if resp.status().is_success() {
        return Ok(resp);
    }

    let status = resp.status().as_u16();
    let url = resp.url().to_string();
    let mut resp = resp;
    let mut raw: Vec<u8> = Vec::new();
    let mut more = false;
    while let Ok(Some(chunk)) = resp.chunk().await {
        raw.extend_from_slice(&chunk);
        if raw.len() > max_body_len {
            more = true;
            break;
        }
    }
    let body_text = String::from_utf8_lossy(&raw);

    let body = if more || body_text.len() > max_body_len {
        let end = body_text.floor_char_boundary(max_body_len.min(body_text.len()));
        format!("{}...(truncated)", &body_text[..end])
    } else {
        body_text.into_owned()
    };

    Err(FaucetError::HttpStatus { status, url, body })
}

/// The deepest element nesting any XML → JSON converter accepts. A deeper
/// document is refused with a typed error: the JSON value it would build is
/// as deep as the document, and dropping or serialising such a value recurses
/// once per level, so a hostile or broken document could overflow the stack.
pub const MAX_XML_DEPTH: usize = 256;

/// The error for an XML document nested deeper than [`MAX_XML_DEPTH`].
pub fn xml_too_deep() -> FaucetError {
    FaucetError::Transform(format!(
        "XML nesting exceeds {MAX_XML_DEPTH} levels — refusing to convert the document"
    ))
}

/// Default maximum body length for error responses.
pub const DEFAULT_ERROR_BODY_MAX_LEN: usize = 2048;

/// The wait a `Retry-After` header states: delta-seconds or an HTTP date (a
/// date in the past is zero). `None` when absent or unreadable.
pub fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<std::time::Duration> {
    let raw = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(secs) = raw.parse::<u64>() {
        return Some(std::time::Duration::from_secs(secs));
    }
    if let Ok(secs) = raw.parse::<f64>()
        && secs.is_finite()
        && secs >= 0.0
    {
        return Some(std::time::Duration::from_secs(secs.ceil() as u64));
    }
    let when = parse_http_date(raw)?;
    Some((when - chrono::Utc::now()).to_std().unwrap_or_default())
}

/// An HTTP date in any of the three forms RFC 9110 §5.6.7 requires a recipient
/// to accept: IMF-fixdate, the obsolete RFC 850 form and asctime.
fn parse_http_date(raw: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Ok(when) = chrono::DateTime::parse_from_rfc2822(raw) {
        return Some(when.with_timezone(&chrono::Utc));
    }
    let collapsed = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    ["%A, %d-%b-%y %H:%M:%S GMT", "%a %b %e %H:%M:%S %Y"]
        .iter()
        .find_map(|f| chrono::NaiveDateTime::parse_from_str(&collapsed, f).ok())
        .map(|n| n.and_utc())
}

/// `FaucetError::RateLimited(wait)` when `wait` is at most `max`; above it a
/// non-retriable error naming the wait, so a run fails instead of sleeping for
/// hours on a quota reset.
pub fn rate_limited(wait: std::time::Duration, max: std::time::Duration) -> FaucetError {
    if wait > max {
        return FaucetError::Source(format!(
            "rate limited: the server asks to wait {}s, more than the {}s ceiling",
            wait.as_secs(),
            max.as_secs()
        ));
    }
    FaucetError::RateLimited(wait)
}

/// [`check_http_response`] that also honours a server-stated wait: a `429`, or
/// a `503` carrying `Retry-After`, with a readable `Retry-After` becomes
/// [`FaucetError::RateLimited`] (failing above
/// [`DEFAULT_MAX_WAIT_SECS`](crate::DEFAULT_MAX_WAIT_SECS)); everything else
/// behaves exactly like [`check_http_response`].
pub async fn check_http_response_rate_limited(
    resp: reqwest::Response,
    max_body_len: usize,
) -> Result<reqwest::Response, FaucetError> {
    let status = resp.status().as_u16();
    if (status == 429 || status == 503)
        && let Some(wait) = retry_after(resp.headers())
    {
        return Err(rate_limited(
            wait,
            std::time::Duration::from_secs(crate::DEFAULT_MAX_WAIT_SECS),
        ));
    }
    check_http_response(resp, max_body_len).await
}

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

/// `Debug`-format `value` through its `Serialize` form, with the value of every
/// object entry whose key is in `secret_keys` masked as `"***"` — at any
/// depth; an object or array under such a key keeps its shape (header names
/// stay visible) with every leaf masked. `null` is left as is.
///
/// For connector config and credential types whose derived `Debug` would
/// print secrets (#789 SUPPLY-17). Serialization failures print the type
/// name only.
pub fn fmt_redacted<T: serde::Serialize + ?Sized>(
    f: &mut std::fmt::Formatter<'_>,
    name: &str,
    value: &T,
    secret_keys: &[&str],
) -> std::fmt::Result {
    fn mask_all(v: &mut Value) {
        match v {
            Value::Null => {}
            Value::Object(map) => map.values_mut().for_each(mask_all),
            Value::Array(items) => items.iter_mut().for_each(mask_all),
            other => *other = Value::String("***".into()),
        }
    }
    fn mask(v: &mut Value, keys: &[&str]) {
        match v {
            Value::Object(map) => {
                for (k, child) in map.iter_mut() {
                    if keys.contains(&k.as_str()) {
                        mask_all(child);
                    } else {
                        mask(child, keys);
                    }
                }
            }
            Value::Array(items) => items.iter_mut().for_each(|i| mask(i, keys)),
            _ => {}
        }
    }
    match serde_json::to_value(value) {
        Ok(mut v) => {
            mask(&mut v, secret_keys);
            write!(f, "{name}({v})")
        }
        Err(_) => write!(f, "{name}(..)"),
    }
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
                    !host.is_empty()
                        && !host.contains('@')
                        && is_host_shaped(host)
                        && is_userinfo_shaped(&tail[..at])
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
        out = redact_secret_kv_values(&out);
    }
    out
}

/// Whether the text before a candidate userinfo `@` can be userinfo. Inside
/// the authority anything goes; text that crosses a `/`, `?` or `#` is only
/// userinfo when it starts `user:` with a password that is not a bare port
/// number, so `s3://bucket/data@2024/x` keeps its path.
fn is_userinfo_shaped(candidate: &str) -> bool {
    let Some(first_delim) = candidate.find(['/', '?', '#']) else {
        return true;
    };
    let head = &candidate[..first_delim];
    if head.contains('@') {
        return false;
    }
    match head.split_once(':') {
        Some((_, pass_head)) => {
            pass_head.is_empty() || !pass_head.bytes().all(|b| b.is_ascii_digit())
        }
        None => false,
    }
}

/// Replace the value of every secret `key=value` pair with `***`. A value
/// runs to its format's field terminator — `;` in a keyword (ADO.NET / JDBC)
/// string, `&` in a URL query — so `Password=a&b;` hides all of `a&b`; a
/// quoted value runs to its closing quote first.
fn redact_secret_kv_values(s: &str) -> String {
    let keyword_form = s.contains(';');
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    let mut prev: Option<char> = None;
    loop {
        let seg_end = rest.find([';', '&', '?']).unwrap_or(rest.len());
        let seg = &rest[..seg_end];
        match seg.find('=') {
            Some(eq) if is_secret_kv_key(&seg[..eq]) => {
                let term = match prev {
                    Some(';') => ';',
                    Some(_) => '&',
                    None if keyword_form => ';',
                    None => '&',
                };
                let vstart = eq + 1;
                let value = &rest[vstart..];
                let scan_from = match value.chars().next() {
                    Some(q @ ('"' | '\'')) => value[1..].find(q).map(|i| i + 2).unwrap_or(0),
                    _ => 0,
                };
                let vend = value[scan_from..]
                    .find(term)
                    .map(|i| vstart + scan_from + i)
                    .unwrap_or(rest.len());
                out.push_str(&rest[..vstart]);
                out.push_str("***");
                rest = &rest[vend..];
            }
            _ => {
                out.push_str(seg);
                rest = &rest[seg_end..];
            }
        }
        let mut chars = rest.chars();
        let Some(c) = chars.next() else { break };
        out.push(c);
        prev = Some(c);
        rest = chars.as_str();
    }
    out
}

/// Whether a connection-string / query-string key names a credential whose
/// value must be redacted from a lineage / catalog URI.
///
/// Matches case-insensitively after stripping `-`/`_`/space, so `api_key`,
/// `api-key`, and `apikey` all match one entry: an exact name (`password`,
/// `sig`, `sas`, `auth`, `auth_code`, `jwt`, …) or a credential suffix
/// (`…token`, `…secret`, `…password`, `…signature`, `…credential`, `…apikey`,
/// …), which covers `api_token`, `auth_token`, `X-Amz-Security-Token`,
/// `X-Amz-Signature` and `X-Goog-Signature`. Over-redacting a harmless
/// parameter costs little; missing a credential leaks it. A bare `key` and a
/// bare `code` are left alone, and so are pagination cursors (`pageToken`,
/// `nextToken`, …): the URI is also a catalog / lineage dataset identity, and
/// such parameters name the dataset (`redis://…?key=list`, `…/rates?code=USD`).
fn is_secret_kv_key(key: &str) -> bool {
    let norm: String = key
        .trim()
        .chars()
        .filter(|c| !matches!(c, '-' | '_' | ' '))
        .flat_map(char::to_lowercase)
        .collect();
    const EXACT: &[&str] = &[
        "pwd",
        "pass",
        "passwd",
        "sig",
        "sas",
        "auth",
        "authorization",
        "authcode",
        "authorizationcode",
        "oauthcode",
        "jwt",
        "accesskeyid",
    ];
    const SUFFIXES: &[&str] = &[
        "token",
        "secret",
        "password",
        "passwd",
        "signature",
        "credential",
        "credentials",
        "apikey",
        "accesskey",
        "secretkey",
        "privatekey",
        "accountkey",
        "sharedaccesskey",
        "signingkey",
    ];
    // Pagination and sync cursors end in `token` but name a position, not a
    // credential, and a dataset URI may carry one.
    const CURSORS: &[&str] = &[
        "pagetoken",
        "nextpagetoken",
        "nexttoken",
        "pagingtoken",
        "continuationtoken",
        "cursortoken",
        "synctoken",
        "resumetoken",
        "starttoken",
    ];
    if CURSORS.contains(&norm.as_str()) {
        return false;
    }
    EXACT.contains(&norm.as_str()) || SUFFIXES.iter().any(|s| norm.ends_with(s))
}

/// Best-effort check that a string looks like a `host[:port]` authority — used
/// to identify the userinfo→host `@` delimiter when redacting credentials.
/// Accepts letters, digits, `.`, `-`, `:`, `_`, bracketed IPv6 forms, and `,`
/// for a multi-host seed list (`h1:27017,h2:27017`).
/// `scheme://host[:port][,host…]` of a connection URI with its credentials,
/// path, query and fragment removed — a base for dataset URIs that must not
/// carry connection options. Input without `://` is returned redacted only.
pub fn redacted_uri_authority(uri: &str) -> String {
    let redacted = redact_uri_credentials(uri);
    match redacted.find("://") {
        Some(i) => {
            let rest = &redacted[i + 3..];
            let end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
            redacted[..i + 3 + end].to_string()
        }
        None => redacted,
    }
}

fn is_host_shaped(s: &str) -> bool {
    s.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(b, b'.' | b'-' | b':' | b'_' | b'[' | b']' | b'%' | b',')
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

/// Characters a URL component keeps as-is: RFC 3986 unreserved.
const COMPONENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Percent-encode `s` for one URL path segment or query value: every byte
/// outside `A-Z a-z 0-9 - . _ ~` becomes `%XX` (UTF-8), so `/`, `?`, `&`, `+`
/// and space never change how the URL parses.
pub fn encode_url_component(s: &str) -> String {
    percent_encoding::utf8_percent_encode(s, COMPONENT).to_string()
}

/// Compact JSON text for `v` with object keys sorted (byte order) at every
/// depth; arrays keep their element order.
///
/// Use this wherever a JSON value becomes *record data* (an encoded column, a
/// hash input, a CSV cell, a row identity). `serde_json`'s own `to_string`
/// follows its map's iteration order, which flips between sorted and
/// insertion order depending on whether Cargo feature unification enabled
/// `serde_json/preserve_order` in the build — so the same record would
/// serialize differently from one binary to the next (#817). Sorted order is
/// what every build without `preserve_order` has always produced.
pub fn canonical_json(v: &Value) -> String {
    let mut out = String::new();
    write_canonical_json(v, &mut out);
    out
}

fn write_canonical_json(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, val)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(k.clone()).to_string());
                out.push(':');
                write_canonical_json(val, out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

#[cfg(test)]
mod canonical_json_tests {
    use super::canonical_json;
    use serde_json::{Map, Value, json};

    /// Build an object whose *insertion* order is the reverse of sorted order,
    /// so the test is meaningful whichever way `preserve_order` resolves.
    fn reversed(pairs: Vec<(&str, Value)>) -> Value {
        let mut m = Map::new();
        for (k, v) in pairs {
            m.insert(k.to_string(), v);
        }
        Value::Object(m)
    }

    #[test]
    fn sorts_keys_at_every_depth_and_keeps_array_order() {
        let owner = reversed(vec![("login", json!("octo-org")), ("id", json!(2))]);
        assert_eq!(canonical_json(&owner), r#"{"id":2,"login":"octo-org"}"#);

        let nested = reversed(vec![
            ("z", json!([3, 1, 2])),
            (
                "m",
                Value::Array(vec![reversed(vec![("y", json!(null)), ("x", json!(true))])]),
            ),
            ("a", reversed(vec![("b", json!(1.5)), ("a", json!("s"))])),
        ]);
        assert_eq!(
            canonical_json(&nested),
            r#"{"a":{"a":"s","b":1.5},"m":[{"x":true,"y":null}],"z":[3,1,2]}"#
        );
    }

    #[test]
    fn escapes_keys_and_strings_like_serde_json() {
        let v = reversed(vec![
            ("q\"k", json!("line\nbreak\u{7f}\u{8}")),
            ("\u{e9}", json!(-1)),
        ]);
        let text = canonical_json(&v);
        // Keys and strings are escaped exactly as serde_json escapes them.
        let mut sorted = std::collections::BTreeMap::new();
        for (k, val) in v.as_object().unwrap() {
            sorted.insert(k.clone(), val.clone());
        }
        assert_eq!(text, serde_json::to_string(&sorted).unwrap());
        assert_eq!(serde_json::from_str::<Value>(&text).unwrap(), v);
        assert_eq!(canonical_json(&json!("x")), r#""x""#);
        assert_eq!(canonical_json(&json!({})), "{}");
        assert_eq!(canonical_json(&json!([])), "[]");
    }
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
#[allow(clippy::disallowed_methods)] // tests drive time and raw clients directly
mod tests;

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

#[cfg(test)]
mod encode_tests {
    use super::encode_url_component;

    #[test]
    fn encode_url_component_keeps_only_unreserved() {
        assert_eq!(encode_url_component("Az09-._~"), "Az09-._~");
        assert_eq!(
            encode_url_component("a b/c?d&e+f=g%h#i"),
            "a%20b%2Fc%3Fd%26e%2Bf%3Dg%25h%23i"
        );
        assert_eq!(encode_url_component("é"), "%C3%A9");
        assert_eq!(encode_url_component(""), "");
    }
}

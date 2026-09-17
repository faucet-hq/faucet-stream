#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-clickhouse
//!
//! Shared configuration and HTTP-protocol helpers for the
//! [`faucet-stream`](https://crates.io/crates/faucet-stream) ClickHouse source
//! and sink connectors. Both connectors talk to ClickHouse over its
//! [HTTP interface](https://clickhouse.com/docs/en/interfaces/http) using
//! [`reqwest`], so the shared surface here is:
//!
//! - [`ClickHouseConnection`] — endpoint (`url` **or** `host` + `http_port` +
//!   `tls`), target `database`, and optional `user` / `password`. Flattened
//!   into both end configs so the wire shape is identical on the source and the
//!   sink. Its `Debug` impl masks the password as `"***"`.
//! - [`ClickHouseConnection::base_url`] — resolves the scheme://host:port base
//!   URL (no trailing slash) the HTTP interface is reached at.
//! - [`build_client`] — the single place a reqwest [`Client`](reqwest::Client)
//!   is constructed.
//! - [`query_params`] — builds the `?database=…&<setting>=…` query string the
//!   HTTP interface expects (settings such as `async_insert`,
//!   `default_format`).
//! - [`apply_auth`] — attaches the `X-ClickHouse-User` / `X-ClickHouse-Key`
//!   authentication headers.
//! - [`parse_json_each_row`] / [`build_json_each_row`] — decode / encode the
//!   newline-delimited `JSONEachRow` format used for both reads and writes.
//! - [`sql_literal`] — inject-safe SQL literal encoding for a JSON scalar
//!   (used to push an incremental bookmark down into the `WHERE` clause).
//!
//! Authentication is username + password (ClickHouse native HTTP auth) only in
//! v1.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Default ClickHouse HTTP-interface port.
pub const DEFAULT_HTTP_PORT: u16 = 8123;
/// Default ClickHouse database when none is configured.
pub const DEFAULT_DATABASE: &str = "default";

fn default_database() -> String {
    DEFAULT_DATABASE.to_string()
}

/// Shared connection configuration for the ClickHouse source and sink.
///
/// The endpoint is specified **either** as a full `url`
/// (`http://host:8123`) **or** as a `host` (+ optional `http_port` / `tls`).
/// Exactly one of the two forms must be provided.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClickHouseConnection {
    /// Full base URL of the ClickHouse HTTP interface, e.g.
    /// `"http://localhost:8123"`. Mutually exclusive with
    /// [`host`](Self::host). A trailing slash is trimmed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// Hostname of the ClickHouse server. Mutually exclusive with
    /// [`url`](Self::url); combined with [`http_port`](Self::http_port) and
    /// [`tls`](Self::tls) to build the base URL.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// HTTP-interface port used with [`host`](Self::host). Defaults to
    /// [`DEFAULT_HTTP_PORT`] (`8123`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_port: Option<u16>,
    /// Use `https://` instead of `http://` when building the base URL from
    /// [`host`](Self::host). Ignored when [`url`](Self::url) is set. Defaults to
    /// `false`.
    #[serde(default)]
    pub tls: bool,
    /// Target database. Defaults to [`DEFAULT_DATABASE`] (`"default"`).
    #[serde(default = "default_database")]
    pub database: String,
    /// ClickHouse user. When set, sent as the `X-ClickHouse-User` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// ClickHouse password. When set, sent as the `X-ClickHouse-Key` header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl Default for ClickHouseConnection {
    fn default() -> Self {
        Self {
            url: None,
            host: None,
            http_port: None,
            tls: false,
            database: default_database(),
            user: None,
            password: None,
        }
    }
}

impl std::fmt::Debug for ClickHouseConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseConnection")
            .field("url", &self.url)
            .field("host", &self.host)
            .field("http_port", &self.http_port)
            .field("tls", &self.tls)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

impl ClickHouseConnection {
    /// Build a connection from a full base URL, leaving credentials unset.
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            ..Default::default()
        }
    }

    /// Validate that exactly one of `url` / `host` is set.
    pub fn validate(&self) -> Result<(), FaucetError> {
        match (&self.url, &self.host) {
            (Some(_), Some(_)) => Err(FaucetError::Config(
                "ClickHouse config sets both `url` and `host`; set exactly one".into(),
            )),
            (None, None) => Err(FaucetError::Config(
                "ClickHouse config requires either `url` or `host`".into(),
            )),
            _ => Ok(()),
        }
    }

    /// Resolve the base URL (scheme://host:port, no trailing slash) of the
    /// ClickHouse HTTP interface.
    ///
    /// Returns [`FaucetError::Config`] when neither `url` nor `host` is set.
    pub fn base_url(&self) -> Result<String, FaucetError> {
        if let Some(url) = &self.url {
            return Ok(url.trim_end_matches('/').to_string());
        }
        if let Some(host) = &self.host {
            let scheme = if self.tls { "https" } else { "http" };
            let port = self.http_port.unwrap_or(DEFAULT_HTTP_PORT);
            return Ok(format!("{scheme}://{host}:{port}"));
        }
        Err(FaucetError::Config(
            "ClickHouse config requires either `url` or `host`".into(),
        ))
    }
}

/// Build a reqwest [`Client`](reqwest::Client) for the ClickHouse HTTP
/// interface. Kept in one place so both connectors share the client-construction
/// path and connection pool.
pub fn build_client(_conn: &ClickHouseConnection) -> Result<reqwest::Client, FaucetError> {
    reqwest::Client::builder()
        .build()
        .map_err(FaucetError::Http)
}

/// Build the ordered `(key, value)` query parameters for a ClickHouse HTTP
/// request: the `database` parameter followed by any extra `settings`
/// (e.g. `("default_format", "JSONEachRow")`, `("async_insert", "1")`).
///
/// The values are handed to reqwest's `.query()`, which performs URL encoding.
pub fn query_params(database: &str, settings: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut params = Vec::with_capacity(1 + settings.len());
    params.push(("database".to_string(), database.to_string()));
    for (k, v) in settings {
        params.push((k.to_string(), v.to_string()));
    }
    params
}

/// Attach the ClickHouse authentication headers to a request when a user /
/// password is configured. Uses the `X-ClickHouse-User` / `X-ClickHouse-Key`
/// headers (never URL query parameters, so credentials do not leak into
/// request logs).
pub fn apply_auth(
    mut req: reqwest::RequestBuilder,
    conn: &ClickHouseConnection,
) -> reqwest::RequestBuilder {
    if let Some(user) = &conn.user {
        req = req.header("X-ClickHouse-User", user);
    }
    if let Some(password) = &conn.password {
        req = req.header("X-ClickHouse-Key", password);
    }
    req
}

/// Parse a `JSONEachRow` response body (one JSON object per line) into records.
///
/// Blank lines are skipped. A line that is not valid JSON surfaces as a typed
/// [`FaucetError::Source`] naming the 1-based line number — never a silent drop.
pub fn parse_json_each_row(body: &str) -> Result<Vec<Value>, FaucetError> {
    let mut out = Vec::new();
    for (idx, line) in body.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(trimmed).map_err(|e| {
            FaucetError::Source(format!(
                "ClickHouse: failed to parse JSONEachRow line {}: {e}",
                idx + 1
            ))
        })?;
        out.push(value);
    }
    Ok(out)
}

/// Serialize records into a `JSONEachRow` request body (one JSON object per
/// line, each line newline-terminated).
///
/// A record that cannot be serialized surfaces as a typed
/// [`FaucetError::Sink`].
pub fn build_json_each_row(records: &[Value]) -> Result<String, FaucetError> {
    let mut body = String::new();
    for record in records {
        let line = serde_json::to_string(record).map_err(|e| {
            FaucetError::Sink(format!("ClickHouse: failed to serialize record: {e}"))
        })?;
        body.push_str(&line);
        body.push('\n');
    }
    Ok(body)
}

/// Encode a JSON scalar as an injection-safe ClickHouse SQL literal.
///
/// Strings are single-quoted with `\` and `'` backslash-escaped, booleans map to
/// `1` / `0`, numbers pass through, and `null` becomes `NULL`. Non-scalar values
/// (arrays / objects) fall back to their quoted JSON string form. Used to push
/// an incremental-replication bookmark or a `{key}` context value down into a
/// `WHERE` clause without interpolating attacker-influenced text unescaped.
///
/// # Why not [`faucet_core::sql_literal`]
///
/// Core's escaper implements the ANSI rule — double the single quotes and leave
/// everything else alone — and is the right choice for every dialect that treats
/// a backslash as an ordinary character. **ClickHouse does not.** Its string
/// literals accept C-style escape sequences, and its syntax reference states you
/// must escape *at least* `'` and `\`. So the ANSI rule is unsafe here in two
/// distinct ways:
///
/// - a value ending in a backslash (`foo\`) would emit `'foo\'`, whose backslash
///   escapes the closing quote and swallows the rest of the statement;
/// - an interior `\'` would survive as an escaped quote rather than a literal
///   one, shifting where the literal ends.
///
/// Doubling quotes on top of backslash-escaping is unnecessary (ClickHouse
/// accepts either form for `'`), so this escapes both meta-characters the one
/// way: `\\` and `\'`. Any future dialect helper that diverges from core must
/// likewise name the extra meta-characters and say why.
pub fn sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => {
            if *b {
                "1".to_string()
            } else {
                "0".to_string()
            }
        }
        Value::Number(n) => n.to_string(),
        Value::String(s) => quote_string(s),
        other => quote_string(&other.to_string()),
    }
}

fn quote_string(s: &str) -> String {
    let escaped = s.replace('\\', "\\\\").replace('\'', "\\'");
    format!("'{escaped}'")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn base_url_from_url_trims_trailing_slash() {
        let conn = ClickHouseConnection::from_url("http://localhost:8123/");
        assert_eq!(conn.base_url().unwrap(), "http://localhost:8123");
    }

    #[test]
    fn base_url_from_host_defaults_port_and_scheme() {
        let conn = ClickHouseConnection {
            host: Some("db.example.com".into()),
            ..Default::default()
        };
        assert_eq!(conn.base_url().unwrap(), "http://db.example.com:8123");
    }

    #[test]
    fn base_url_from_host_honors_tls_and_port() {
        let conn = ClickHouseConnection {
            host: Some("db.example.com".into()),
            http_port: Some(8443),
            tls: true,
            ..Default::default()
        };
        assert_eq!(conn.base_url().unwrap(), "https://db.example.com:8443");
    }

    #[test]
    fn base_url_requires_url_or_host() {
        let conn = ClickHouseConnection::default();
        assert!(conn.base_url().is_err());
    }

    #[test]
    fn validate_rejects_both_and_neither() {
        let both = ClickHouseConnection {
            url: Some("http://h:8123".into()),
            host: Some("h".into()),
            ..Default::default()
        };
        assert!(both.validate().is_err());
        assert!(ClickHouseConnection::default().validate().is_err());
    }

    #[test]
    fn validate_accepts_exactly_one() {
        assert!(
            ClickHouseConnection::from_url("http://h:8123")
                .validate()
                .is_ok()
        );
        let host_only = ClickHouseConnection {
            host: Some("h".into()),
            ..Default::default()
        };
        assert!(host_only.validate().is_ok());
    }

    #[test]
    fn debug_masks_password() {
        let conn = ClickHouseConnection {
            url: Some("http://h:8123".into()),
            user: Some("alice".into()),
            password: Some("s3cret".into()),
            ..Default::default()
        };
        let dbg = format!("{conn:?}");
        assert!(dbg.contains("alice"));
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("s3cret"));
    }

    #[test]
    fn database_defaults_when_missing() {
        let conn: ClickHouseConnection =
            serde_json::from_value(json!({ "url": "http://h:8123" })).unwrap();
        assert_eq!(conn.database, "default");
    }

    #[test]
    fn query_params_puts_database_first_then_settings() {
        let params = query_params("analytics", &[("default_format", "JSONEachRow")]);
        assert_eq!(
            params,
            vec![
                ("database".to_string(), "analytics".to_string()),
                ("default_format".to_string(), "JSONEachRow".to_string()),
            ]
        );
    }

    #[test]
    fn query_params_async_insert_on_and_off() {
        let on = query_params(
            "db",
            &[("async_insert", "1"), ("wait_for_async_insert", "1")],
        );
        assert!(on.contains(&("async_insert".to_string(), "1".to_string())));
        let off = query_params("db", &[]);
        assert_eq!(off.len(), 1, "only the database param when no settings");
    }

    #[test]
    fn parse_json_each_row_multiple_rows() {
        let body = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n";
        let rows = parse_json_each_row(body).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[2]["a"], 3);
    }

    #[test]
    fn parse_json_each_row_empty_result_is_empty() {
        assert!(parse_json_each_row("").unwrap().is_empty());
        assert!(parse_json_each_row("\n\n").unwrap().is_empty());
    }

    #[test]
    fn parse_json_each_row_skips_blank_lines_between_rows() {
        let rows = parse_json_each_row("{\"a\":1}\n\n{\"a\":2}\n").unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn parse_json_each_row_malformed_line_is_typed_error() {
        let err = parse_json_each_row("{\"a\":1}\nnot-json\n").unwrap_err();
        match err {
            FaucetError::Source(m) => assert!(m.contains("line 2"), "got: {m}"),
            other => panic!("expected Source error, got {other:?}"),
        }
    }

    #[test]
    fn build_json_each_row_exact_ndjson() {
        let page = vec![json!({"id": 1, "v": "a"}), json!({"id": 2, "v": "b"})];
        let body = build_json_each_row(&page).unwrap();
        assert_eq!(body, "{\"id\":1,\"v\":\"a\"}\n{\"id\":2,\"v\":\"b\"}\n");
    }

    #[test]
    fn build_json_each_row_empty_is_empty_string() {
        assert_eq!(build_json_each_row(&[]).unwrap(), "");
    }

    #[test]
    fn build_and_parse_round_trip() {
        let page = vec![json!({"id": 1, "s": "héllo"}), json!({"id": 2, "s": "x"})];
        let body = build_json_each_row(&page).unwrap();
        let back = parse_json_each_row(&body).unwrap();
        assert_eq!(back, page);
    }

    #[test]
    fn sql_literal_scalars() {
        assert_eq!(sql_literal(&Value::Null), "NULL");
        assert_eq!(sql_literal(&json!(true)), "1");
        assert_eq!(sql_literal(&json!(false)), "0");
        assert_eq!(sql_literal(&json!(42)), "42");
        assert_eq!(sql_literal(&json!(-1.5)), "-1.5");
        assert_eq!(sql_literal(&json!("2024-01-01")), "'2024-01-01'");
    }

    #[test]
    fn sql_literal_escapes_quote_and_backslash() {
        assert_eq!(sql_literal(&json!("O'Brien")), "'O\\'Brien'");
        assert_eq!(sql_literal(&json!("a\\b")), "'a\\\\b'");
        // A classic injection attempt is neutralised into a single quoted literal.
        assert_eq!(
            sql_literal(&json!("x' OR '1'='1")),
            "'x\\' OR \\'1\\'=\\'1'"
        );
    }

    #[test]
    fn sql_literal_escapes_the_cases_the_ansi_rule_would_miss() {
        // These are exactly why this dialect keeps its own escaper instead of
        // consuming `faucet_core::sql_literal` (#654 M14). Under the ANSI rule
        // each of these emits a literal whose closing quote is neutered by a
        // backslash, so the statement continues into attacker text.
        //
        // Trailing backslash: ANSI would emit `'foo\'` — quote escaped, literal
        // never closes. Backslash-escaping doubles it, so the quote closes.
        assert_eq!(sql_literal(&json!("foo\\")), "'foo\\\\'");
        // Pre-escaped quote: ANSI would emit `'a\''` — `\'` is an escaped quote
        // and the trailing `'` re-opens a literal, so the statement runs on.
        assert_eq!(sql_literal(&json!("a\\'")), "'a\\\\\\''");
        // Full breakout attempt riding a trailing backslash.
        assert_eq!(
            sql_literal(&json!("x\\' OR 1=1 --")),
            "'x\\\\\\' OR 1=1 --'"
        );
        // Property: after escaping, every backslash in the body starts a
        // two-character escape pair, so the final quote can never be consumed.
        for probe in ["foo\\", "a\\'", "\\\\", "\\", "'\\"] {
            let out = sql_literal(&json!(probe));
            let body = &out[1..out.len() - 1];
            let mut chars = body.chars();
            while let Some(c) = chars.next() {
                if c == '\\' {
                    assert!(
                        matches!(chars.next(), Some('\\') | Some('\'')),
                        "dangling escape in {out} for input {probe:?}"
                    );
                } else {
                    assert_ne!(c, '\'', "unescaped quote in {out} for input {probe:?}");
                }
            }
        }
    }

    #[test]
    fn build_client_succeeds() {
        assert!(build_client(&ClickHouseConnection::from_url("http://h:8123")).is_ok());
    }
}

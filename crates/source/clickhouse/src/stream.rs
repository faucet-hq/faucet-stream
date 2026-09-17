//! The ClickHouse [`Source`] implementation — HTTP client, query execution,
//! streaming JSONEachRow decode, and incremental-replication bookkeeping.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;

use async_trait::async_trait;
use faucet_common_clickhouse::{
    apply_auth, build_client, parse_json_each_row, query_params, sql_literal,
};
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::replication::{filter_incremental, max_replication_value, max_value};
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{FaucetError, Source, Stream, StreamPage};
use futures::StreamExt;
use serde_json::Value;

use crate::config::{ClickHouseReplication, ClickHouseSourceConfig};

/// ClickHouse query source (HTTP interface, `JSONEachRow`).
pub struct ClickHouseSource {
    config: ClickHouseSourceConfig,
    client: reqwest::Client,
    /// Resolved once in [`ClickHouseSource::new`] so the hot path never re-parses.
    base_url: String,
    /// Bookmark loaded via [`Source::apply_start_bookmark`]; overrides the
    /// configured `initial_value` for incremental runs.
    start_bookmark: Mutex<Option<Value>>,
}

/// Incremental-replication context resolved for one run.
#[derive(Debug, Clone, PartialEq)]
struct IncrementalCtx {
    column: String,
    start: Value,
}

impl ClickHouseSource {
    /// Validate the config and build the reusable HTTP client.
    pub fn new(config: ClickHouseSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let base_url = config.connection.base_url()?;
        let client = build_client(&config.connection)?;
        Ok(Self {
            config,
            client,
            base_url,
            start_bookmark: Mutex::new(None),
        })
    }

    fn current_start(&self) -> Option<Value> {
        self.start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned")
            .clone()
    }

    /// Build the POST request that runs `query` and returns `JSONEachRow`.
    fn request(&self, query: String) -> reqwest::RequestBuilder {
        let params = query_params(
            &self.config.connection.database,
            &[("default_format", "JSONEachRow")],
        );
        let req = self.client.post(&self.base_url).query(&params).body(query);
        apply_auth(req, &self.config.connection)
    }

    /// Run the query and collect all decoded rows plus (for incremental) the new
    /// bookmark. Used by the non-streaming convenience methods.
    async fn collect_all(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let start = self.current_start();
        let (query, incr) = build_effective_query(&self.config, context, start.as_ref());

        let resp = self.request(query).send().await?;
        let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
        let body = resp.text().await.map_err(|e| {
            FaucetError::Source(format!("ClickHouse: reading response failed: {e}"))
        })?;
        let records = parse_json_each_row(&body)?;

        let mut running_max: Option<Value> = None;
        let records = apply_incremental(records, incr.as_ref(), &mut running_max);
        let bookmark = if incr.is_some() { running_max } else { None };
        Ok((records, bookmark))
    }
}

/// The user-facing incremental-cursor token, normalised into a `{key}`
/// placeholder so the bookmark and the parent-context values are substituted by
/// the *same* single pass. Substituting them in two passes let either one's
/// output be re-scanned by the other, which is the injection hazard #654 M14
/// describes.
const BOOKMARK_TOKEN: &str = "@bookmark";

/// Reserved placeholder key that [`BOOKMARK_TOKEN`] is rewritten to. Named (not
/// inlined) because it is a reserved word in the substitution namespace: a
/// parent-context key of the same name is shadowed by the bookmark.
const BOOKMARK_KEY: &str = "__faucet_bookmark";

/// Build the final query string and (for incremental runs) the client-side
/// filter context. Pure (no client) so it is unit-testable.
///
/// Both the parent-context `{key}` tokens and the incremental cursor written as
/// `@bookmark` are rendered as injection-safe SQL literals and substituted in
/// **one** left-to-right pass, so no substituted value is ever re-scanned as a
/// placeholder for another. The `@bookmark` → `{BOOKMARK_KEY}` rewrite happens
/// on the raw, author-controlled query template (never on substituted output),
/// which is why it is safe to do with a plain `replace`.
fn build_effective_query(
    config: &ClickHouseSourceConfig,
    context: &HashMap<String, Value>,
    start_bookmark: Option<&Value>,
) -> (String, Option<IncrementalCtx>) {
    let mut template = config.query.clone();
    let mut literals: HashMap<String, Value> = HashMap::new();

    let incremental = match &config.replication {
        ClickHouseReplication::Full => None,
        ClickHouseReplication::Incremental {
            column,
            initial_value,
        } => {
            let start = start_bookmark
                .cloned()
                .unwrap_or_else(|| initial_value.clone());
            // Server-side pushdown where the user wrote `@bookmark`; absent
            // that, only the client-side filter applies.
            if template.contains(BOOKMARK_TOKEN) {
                template = template.replace(BOOKMARK_TOKEN, &format!("{{{BOOKMARK_KEY}}}"));
                literals.insert(BOOKMARK_KEY.to_owned(), Value::String(sql_literal(&start)));
            }
            Some(IncrementalCtx {
                column: column.clone(),
                start,
            })
        }
    };

    if literals.is_empty() && context.is_empty() {
        return (template, incremental);
    }
    (
        substitute_context_sql(&template, context, literals),
        incremental,
    )
}

/// Replace each `{key}` token with the injection-safe SQL literal of the
/// corresponding value, drawn from `context` and then from `reserved` (which
/// wins on a name clash). Tokens with no matching entry are left verbatim.
///
/// Escaping happens **before** substitution and the substituted text is never
/// re-scanned. The previous implementation looped `String::replace` once per
/// context entry over the *running output*, which had two defects (#654 M14):
///
/// - **second-order injection** — a value whose text contained another key's
///   placeholder (`{other}`) was substituted again on a later iteration, so a
///   record field could inject a different context value into the query;
/// - **nondeterminism** — `HashMap` iteration order varies per process, so which
///   re-substitution happened (if any) changed run to run.
///
/// Core's [`substitute_context`](faucet_core::util::substitute_context) is the
/// single left-to-right scanner that cannot re-scan its own output, so the
/// values are pre-rendered to ClickHouse literals and handed to it as strings.
/// That keeps the escaping in one audited place ([`sql_literal`]) and the
/// scanning in another, rather than re-deriving either here.
///
/// ClickHouse's own parameter binding (`{name:Type}` + `param_name=…`) is not
/// used: it requires an explicit ClickHouse type inside the placeholder, so
/// routing `{key}` through it would change the documented token grammar and
/// force us to infer a ClickHouse type per JSON value — a breaking config change
/// carrying its own mis-typing hazards. Escaping via one audited literal encoder
/// closes the injection path without that.
fn substitute_context_sql(
    query: &str,
    context: &HashMap<String, Value>,
    reserved: HashMap<String, Value>,
) -> String {
    let mut literals: HashMap<String, Value> = context
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(sql_literal(value))))
        .collect();
    // Already rendered to literals by the caller; reserved keys shadow context.
    literals.extend(reserved);
    faucet_core::util::substitute_context(query, &literals)
}

/// Filter a page for incremental replication and advance `running_max`.
/// For full replication the page passes through unchanged.
fn apply_incremental(
    page: Vec<Value>,
    incr: Option<&IncrementalCtx>,
    running_max: &mut Option<Value>,
) -> Vec<Value> {
    match incr {
        None => page,
        Some(ctx) => {
            let kept = filter_incremental(page, &ctx.column, &ctx.start);
            if let Some(m) = max_replication_value(&kept, &ctx.column) {
                let m = m.clone();
                *running_max = Some(match running_max.take() {
                    Some(prev) => max_value(prev, m),
                    None => m,
                });
            }
            kept
        }
    }
}

/// Drain every complete `\n`-terminated line from `buf`, returning each line's
/// bytes (without the trailing newline). Splitting on the `0x0A` byte is safe
/// for UTF-8 because a newline never appears inside a multi-byte sequence, so a
/// line split across two network chunks is reassembled correctly. Pure and
/// unit-testable.
fn split_complete_lines(buf: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    while let Some(pos) = buf.iter().position(|&b| b == b'\n') {
        let mut line: Vec<u8> = buf.drain(..=pos).collect();
        line.pop(); // strip the trailing '\n'
        lines.push(line);
    }
    lines
}

/// Parse one raw JSONEachRow line into a JSON value. Blank lines yield `None`.
/// Invalid UTF-8 or JSON surfaces as a typed [`FaucetError::Source`].
fn parse_line(line: &[u8]) -> Result<Option<Value>, FaucetError> {
    let text = std::str::from_utf8(line)
        .map_err(|e| FaucetError::Source(format!("ClickHouse: non-UTF-8 response line: {e}")))?
        .trim();
    if text.is_empty() {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(text).map_err(|e| {
        FaucetError::Source(format!("ClickHouse: failed to parse JSONEachRow line: {e}"))
    })?;
    Ok(Some(value))
}

/// Derive a default state-store key from the connection host + a query
/// fingerprint, stable across runs.
fn default_state_key(config: &ClickHouseSourceConfig) -> String {
    let host = config
        .connection
        .base_url()
        .ok()
        .and_then(|u| url::Url::parse(&u).ok())
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| "clickhouse".to_string());

    // FNV-1a from core, NOT `DefaultHasher`: this value is part of a
    // **durable** state-store key, and `DefaultHasher`'s output is explicitly
    // not stable across Rust releases. A toolchain bump would silently re-key
    // the bookmark, orphaning it and re-replicating the whole table on the
    // next run with a green exit code.
    let fingerprint = faucet_core::shard::shard_hash(&config.query);
    let host: String = host
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("clickhouse:{host}:{fingerprint:016x}")
}

#[async_trait]
impl Source for ClickHouseSource {
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.collect_all(context).await?.0)
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.collect_all(context).await
    }

    /// Stream rows straight off the HTTP response body without buffering the
    /// whole result set: bytes are accumulated, split into complete
    /// `JSONEachRow` lines, and yielded in [`ClickHouseSourceConfig::batch_size`]
    /// pages. The final page carries the incremental bookmark (when replicating
    /// incrementally) so the pipeline persists only after everything before it
    /// is written.
    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let cap = if batch_size == 0 { 1024 } else { batch_size };
        let start = self.current_start();
        let (query, incr) = build_effective_query(&self.config, context, start.as_ref());

        Box::pin(async_stream::try_stream! {
            let resp = self.request(query).send().await?;
            let resp = check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
            let mut body = resp.bytes_stream();

            let mut buf: Vec<u8> = Vec::new();
            let mut page: Vec<Value> = Vec::with_capacity(cap);
            let mut running_max: Option<Value> = None;
            let mut total = 0usize;

            while let Some(chunk_result) = body.next().await {
                let bytes = chunk_result.map_err(FaucetError::Http)?;
                buf.extend_from_slice(&bytes);
                for line in split_complete_lines(&mut buf) {
                    if let Some(value) = parse_line(&line)? {
                        page.push(value);
                        if page.len() >= chunk {
                            let ready = std::mem::replace(&mut page, Vec::with_capacity(cap));
                            let kept = apply_incremental(ready, incr.as_ref(), &mut running_max);
                            total += kept.len();
                            if !kept.is_empty() {
                                yield StreamPage { records: kept, bookmark: None };
                            }
                        }
                    }
                }
            }
            // Trailing line without a terminating newline (ClickHouse always
            // newline-terminates, but be robust).
            if let Some(value) = parse_line(&buf)? {
                page.push(value);
            }

            // Final page carries the bookmark.
            let kept = apply_incremental(page, incr.as_ref(), &mut running_max);
            total += kept.len();
            let bookmark = if incr.is_some() { running_max.clone() } else { None };
            if !kept.is_empty() || bookmark.is_some() {
                yield StreamPage { records: kept, bookmark };
            }

            tracing::info!(rows = total, batch_size, query = %self.config.query, "ClickHouse source stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(ClickHouseSourceConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "clickhouse"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?query={}",
            faucet_core::redact_uri_credentials(&self.base_url),
            self.config.query
        )
    }

    fn state_key(&self) -> Option<String> {
        match &self.config.replication {
            ClickHouseReplication::Full => None,
            ClickHouseReplication::Incremental { .. } => Some(
                self.config
                    .state_key
                    .clone()
                    .unwrap_or_else(|| default_state_key(&self.config)),
            ),
        }
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self
            .start_bookmark
            .lock()
            .expect("start_bookmark mutex poisoned") = Some(bookmark);
        Ok(())
    }

    /// Non-mutating preflight probe (`connect`): runs `SELECT 1` over the HTTP
    /// interface.
    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let hint = "check url / host / database / credentials / that the server is reachable";
        let req = self.request("SELECT 1".to_string());
        let probe = match tokio::time::timeout(ctx.timeout, req.send()).await {
            Ok(Ok(resp)) => match check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await {
                Ok(_) => Probe::pass("connect", started.elapsed()),
                Err(e) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            },
            Ok(Err(e)) => Probe::fail_hint("connect", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("connect", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn full_cfg() -> ClickHouseSourceConfig {
        ClickHouseSourceConfig::new("http://db.example.com:8123", "SELECT * FROM t")
    }

    #[test]
    fn build_full_returns_query_unchanged() {
        let (q, incr) = build_effective_query(&full_cfg(), &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t");
        assert!(incr.is_none());
    }

    #[test]
    fn build_incremental_substitutes_bookmark_literal() {
        let cfg = ClickHouseSourceConfig::new(
            "http://h:8123",
            "SELECT * FROM t WHERE updated_at > @bookmark",
        )
        .incremental("updated_at", json!("1970-01-01"));
        let (q, incr) = build_effective_query(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t WHERE updated_at > '1970-01-01'");
        assert_eq!(
            incr,
            Some(IncrementalCtx {
                column: "updated_at".into(),
                start: json!("1970-01-01"),
            })
        );
    }

    #[test]
    fn build_incremental_prefers_stored_bookmark_over_initial() {
        let cfg =
            ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t WHERE c > @bookmark")
                .incremental("c", json!(0));
        let stored = json!(500);
        let (q, incr) = build_effective_query(&cfg, &HashMap::new(), Some(&stored));
        assert_eq!(q, "SELECT * FROM t WHERE c > 500");
        assert_eq!(incr.unwrap().start, json!(500));
    }

    #[test]
    fn build_incremental_without_token_still_returns_filter_ctx() {
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t")
            .incremental("c", json!(0));
        let (q, incr) = build_effective_query(&cfg, &HashMap::new(), None);
        assert_eq!(q, "SELECT * FROM t");
        assert!(incr.is_some(), "client-side filter must still run");
    }

    #[test]
    fn context_substitution_uses_injection_safe_literals() {
        let mut ctx = HashMap::new();
        ctx.insert("tenant".to_string(), json!("ac'me"));
        ctx.insert("id".to_string(), json!(7));
        let cfg = ClickHouseSourceConfig::new(
            "http://h:8123",
            "SELECT * FROM t WHERE tenant = {tenant} AND id = {id}",
        );
        let (q, _incr) = build_effective_query(&cfg, &ctx, None);
        assert!(q.contains("tenant = 'ac\\'me'"), "got: {q}");
        assert!(q.contains("id = 7"), "got: {q}");
    }

    #[test]
    fn substituted_value_is_not_re_substituted() {
        // Second-order injection (#654 M14): the value of `a` names another
        // context key. The old per-entry `String::replace` loop re-scanned its
        // own output, so `{b}` inside a's value was substituted on a later
        // iteration and `b`'s value leaked into the query.
        let mut ctx = HashMap::new();
        ctx.insert("a".to_string(), json!("{b}"));
        ctx.insert("b".to_string(), json!("SECRET"));
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t WHERE x = {a}");
        let (q, _incr) = build_effective_query(&cfg, &ctx, None);
        assert_eq!(q, "SELECT * FROM t WHERE x = '{b}'");
        assert!(
            !q.contains("SECRET"),
            "value of `b` was re-substituted into a's output: {q}"
        );
    }

    #[test]
    fn substituted_value_cannot_inject_the_bookmark_token() {
        // The same hazard across the two substitution kinds: a context value
        // containing `@bookmark` must not pick up the cursor literal, which
        // would close the quote it sits inside.
        let mut ctx = HashMap::new();
        ctx.insert("a".to_string(), json!("x@bookmark"));
        let cfg = ClickHouseSourceConfig::new(
            "http://h:8123",
            "SELECT * FROM t WHERE x = {a} AND c > @bookmark",
        )
        .incremental("c", json!("2024-01-01"));
        let (q, _incr) = build_effective_query(&cfg, &ctx, None);
        assert_eq!(
            q,
            "SELECT * FROM t WHERE x = 'x@bookmark' AND c > '2024-01-01'"
        );
    }

    #[test]
    fn bookmark_literal_cannot_inject_a_context_placeholder() {
        // And the mirror direction: a bookmark read back from the state store is
        // source-derived, so its text must not be re-scanned for `{key}` either.
        let mut ctx = HashMap::new();
        ctx.insert("a".to_string(), json!("SECRET"));
        let cfg =
            ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t WHERE c > @bookmark")
                .incremental("c", json!(0));
        let stored = json!("{a}");
        let (q, _incr) = build_effective_query(&cfg, &ctx, Some(&stored));
        assert_eq!(q, "SELECT * FROM t WHERE c > '{a}'");
        assert!(!q.contains("SECRET"), "bookmark output was re-scanned: {q}");
    }

    #[test]
    fn substitution_is_independent_of_context_insertion_order() {
        // `HashMap` iteration order varies per process, so the old loop could
        // produce different SQL run to run. Build the same context two ways and
        // require identical output.
        let cfg = ClickHouseSourceConfig::new(
            "http://h:8123",
            "SELECT * FROM t WHERE a = {a} AND b = {b} AND c = {c}",
        );
        let entries = [
            ("a", json!("{b}")),
            ("b", json!("{c}")),
            ("c", json!("{a}")),
        ];

        let forward: HashMap<String, Value> = entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        let reverse: HashMap<String, Value> = entries
            .iter()
            .rev()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();

        let (q1, _) = build_effective_query(&cfg, &forward, None);
        let (q2, _) = build_effective_query(&cfg, &reverse, None);
        assert_eq!(q1, q2);
        assert_eq!(
            q1,
            "SELECT * FROM t WHERE a = '{b}' AND b = '{c}' AND c = '{a}'"
        );
    }

    #[test]
    fn a_context_key_cannot_shadow_the_reserved_bookmark_key() {
        // The `@bookmark` token is normalised to a reserved placeholder key; a
        // parent-context entry of that name must not be able to take it over and
        // substitute attacker text where the cursor belongs.
        let mut ctx = HashMap::new();
        ctx.insert(BOOKMARK_KEY.to_string(), json!("HIJACKED"));
        let cfg =
            ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t WHERE c > @bookmark")
                .incremental("c", json!(42));
        let (q, _incr) = build_effective_query(&cfg, &ctx, None);
        assert_eq!(q, "SELECT * FROM t WHERE c > 42");
        assert!(!q.contains("HIJACKED"), "reserved key was shadowed: {q}");
    }

    #[test]
    fn unmatched_placeholders_are_left_verbatim() {
        let mut ctx = HashMap::new();
        ctx.insert("known".to_string(), json!(1));
        let cfg = ClickHouseSourceConfig::new(
            "http://h:8123",
            "SELECT * FROM t WHERE a = {known} AND b = {unknown}",
        );
        let (q, _incr) = build_effective_query(&cfg, &ctx, None);
        assert_eq!(q, "SELECT * FROM t WHERE a = 1 AND b = {unknown}");
    }

    #[test]
    fn apply_incremental_filters_and_tracks_max() {
        let ctx = IncrementalCtx {
            column: "c".into(),
            start: json!(10),
        };
        let mut running = None;
        let page = vec![json!({"c": 5}), json!({"c": 15}), json!({"c": 20})];
        let kept = apply_incremental(page, Some(&ctx), &mut running);
        assert_eq!(kept.len(), 2);
        assert_eq!(running, Some(json!(20)));
    }

    #[test]
    fn apply_incremental_full_passes_through() {
        let mut running = None;
        let page = vec![json!({"c": 1}), json!({"c": 2})];
        let kept = apply_incremental(page, None, &mut running);
        assert_eq!(kept.len(), 2);
        assert_eq!(running, None);
    }

    #[test]
    fn split_complete_lines_drains_full_lines_only() {
        let mut buf = b"{\"a\":1}\n{\"a\":2}\n{\"a\":3".to_vec();
        let lines = split_complete_lines(&mut buf);
        assert_eq!(lines.len(), 2, "the unterminated tail stays buffered");
        assert_eq!(lines[0], b"{\"a\":1}");
        assert_eq!(buf, b"{\"a\":3", "partial line remains in the buffer");
    }

    #[test]
    fn split_complete_lines_reassembles_across_chunks() {
        // A line split across two network chunks is reassembled once the
        // second chunk (carrying the newline) arrives.
        let mut buf = b"{\"a\":".to_vec();
        assert!(split_complete_lines(&mut buf).is_empty());
        buf.extend_from_slice(b"1}\n");
        let lines = split_complete_lines(&mut buf);
        assert_eq!(lines, vec![b"{\"a\":1}".to_vec()]);
    }

    #[test]
    fn parse_line_handles_blank_and_valid_and_invalid() {
        assert_eq!(parse_line(b"").unwrap(), None);
        assert_eq!(parse_line(b"   ").unwrap(), None);
        assert_eq!(parse_line(b"{\"a\":1}").unwrap(), Some(json!({"a": 1})));
        assert!(parse_line(b"not-json").is_err());
    }

    #[test]
    fn parse_line_rejects_invalid_utf8() {
        assert!(parse_line(&[0xff, 0xfe]).is_err());
    }

    #[test]
    fn state_key_only_for_incremental_and_is_stable() {
        let source = ClickHouseSource::new(full_cfg()).unwrap();
        assert_eq!(source.state_key(), None, "full replication has no bookmark");

        let cfg = ClickHouseSourceConfig::new("http://db.example.com:8123", "SELECT * FROM t")
            .incremental("c", json!(0));
        let source = ClickHouseSource::new(cfg).unwrap();
        let k1 = source.state_key().unwrap();
        let k2 = source.state_key().unwrap();
        assert_eq!(k1, k2);
        assert!(k1.starts_with("clickhouse:db.example.com:"), "got: {k1}");
        faucet_core::state::validate_state_key(&k1).expect("derived key must be valid");
    }

    #[test]
    fn explicit_state_key_overrides_default() {
        let mut cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t")
            .incremental("c", json!(0));
        cfg.state_key = Some("custom-key".into());
        let source = ClickHouseSource::new(cfg).unwrap();
        assert_eq!(source.state_key().as_deref(), Some("custom-key"));
    }

    #[tokio::test]
    async fn apply_start_bookmark_overrides_initial() {
        let cfg =
            ClickHouseSourceConfig::new("http://h:8123", "SELECT * FROM t WHERE c > @bookmark")
                .incremental("c", json!(0));
        let source = ClickHouseSource::new(cfg).unwrap();
        source.apply_start_bookmark(json!(999)).await.unwrap();
        let (q, incr) = build_effective_query(
            &source.config,
            &HashMap::new(),
            source.current_start().as_ref(),
        );
        assert_eq!(q, "SELECT * FROM t WHERE c > 999");
        assert_eq!(incr.unwrap().start, json!(999));
    }

    #[test]
    fn dataset_uri_has_no_credentials() {
        let source = ClickHouseSource::new(full_cfg()).unwrap();
        assert_eq!(
            source.dataset_uri(),
            "http://db.example.com:8123?query=SELECT * FROM t"
        );
    }

    #[test]
    fn connector_name_is_clickhouse() {
        let source = ClickHouseSource::new(full_cfg()).unwrap();
        assert_eq!(source.connector_name(), "clickhouse");
    }

    #[test]
    fn config_schema_is_object() {
        let source = ClickHouseSource::new(full_cfg()).unwrap();
        assert_eq!(source.config_schema()["type"], "object");
    }

    #[test]
    fn new_rejects_invalid_config() {
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(ClickHouseSource::new(cfg).is_err());
    }

    #[tokio::test]
    async fn check_fails_against_unreachable_server() {
        // Connection-refused on a closed local port exercises the check() I/O
        // path offline (no external dependency).
        let source = ClickHouseSource::new(ClickHouseSourceConfig::new(
            "http://127.0.0.1:1",
            "SELECT 1",
        ))
        .unwrap();
        let ctx = CheckContext {
            timeout: std::time::Duration::from_secs(2),
        };
        let report = source.check(&ctx).await.unwrap();
        assert!(
            matches!(
                report.probes[0].status,
                faucet_core::check::ProbeStatus::Fail { .. }
            ),
            "unreachable server must fail the connect probe"
        );
    }

    #[test]
    fn default_state_key_is_stable_and_query_scoped() {
        // The key is durable state: it must be a pure function of the config
        // (stable across processes and toolchains — hence core's FNV-1a rather
        // than `DefaultHasher`) and must differ per query so two sources on
        // one host cannot collide on a single bookmark.
        let mk = |q: &str| {
            let mut c = ClickHouseSourceConfig::new("http://ch.example.com:8123", q);
            c.state_key = None;
            default_state_key(&c)
        };
        let a = mk("SELECT * FROM events");
        assert_eq!(a, mk("SELECT * FROM events"), "stable for one query");
        assert_ne!(a, mk("SELECT * FROM orders"), "scoped per query");
        assert!(a.contains("ch.example.com"), "host-scoped: {a}");
    }
}

//! The main REST stream executor.

use crate::auth::Auth;
use crate::auth::oauth2::TokenCache;
use crate::auth::token_endpoint::TokenEndpointCache;
use crate::config::{RestStreamConfig, TlsClientConfig};
use crate::extract;
use crate::pagination::{PaginationState, PaginationStyle};
use crate::retry;
use async_trait::async_trait;
use faucet_core::replication::{
    BindTarget, ReplicationMethod, filter_incremental, max_replication_value, max_value,
};
use faucet_core::schema;
use faucet_core::{AuthSpec, Credential, CredentialPlacement, FaucetError, SharedAuthProvider};
use futures_core::Stream;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;

/// A configured REST API stream that handles pagination, auth, and extraction.
pub struct RestStream {
    config: RestStreamConfig,
    client: Client,
    /// Shared OAuth2 token cache (only used when `config.auth` is `Auth::OAuth2`).
    token_cache: TokenCache,
    /// Shared token endpoint cache (only used when `config.auth` is `Auth::TokenEndpoint`).
    token_endpoint_cache: TokenEndpointCache,
    /// Optional shared auth provider. Set when `config.auth` is an
    /// `AuthSpec::Reference` resolved by the caller (e.g. the CLI `auth:`
    /// catalog), or injected directly by a library caller to share one token
    /// across multiple sources. When present it takes precedence over inline
    /// auth.
    auth_provider: Option<SharedAuthProvider>,
    /// Bookmark applied at runtime via
    /// [`Source::apply_start_bookmark`](faucet_core::Source::apply_start_bookmark).
    /// Takes precedence over `config.start_replication_value` when set.
    runtime_start: Arc<AsyncMutex<Option<Value>>>,
    /// Rendered lower/upper bounds for the current datetime window (#527),
    /// applied to each request by [`execute_request_once`](Self::execute_request_once)
    /// alongside any [`replication_bind`](RestStreamConfig::replication_bind). Set
    /// by the window loop in `stream_pages_inner` before each window's pages;
    /// empty when no `window:` block is configured. Each entry is
    /// `(target, name, rendered-value)`.
    window_binds: Arc<AsyncMutex<Vec<(BindTarget, String, String)>>>,
    /// Test-only override for the "now" upper bound of datetime window slicing
    /// (#527). `None` in production (uses `Utc::now()`); set by unit tests so the
    /// window enumeration is deterministic.
    now_override: Option<chrono::DateTime<chrono::Utc>>,
    /// Retry policy for transient request failures. Built in `new()` from the
    /// REST source's own `config.max_retries` / `config.retry_backoff`. Fed into
    /// the REST `retry::execute_with_retry` runner (which keeps its 429 /
    /// `Retry-After` handling). Overridable via
    /// [`with_retry_policy`](Self::with_retry_policy) — but the REST connector's
    /// own legacy `max_retries` / `retry_backoff` fields take precedence when the
    /// user has set them away from their defaults.
    retry_policy: faucet_core::RetryPolicy,
    /// Static request headers (from `config.headers`, #539), validated once in
    /// [`new`](Self::new) into a [`HeaderMap`] and merged into **every** request
    /// (data pages, async-job requests, `$metadata` probes) *before* the auth
    /// provider's placements — so an auth header of the same name wins.
    static_headers: HeaderMap,
    /// The service `$metadata` CSDL, fetched lazily at most once per instance
    /// (see [`metadata_xml`](Self::metadata_xml)). Instance-owned by design —
    /// no process-global cache state.
    metadata_xml_cache: tokio::sync::OnceCell<Arc<String>>,
    /// Upstream round-trip counter (#638), installed once by the pipeline via
    /// [`Source::set_roundtrip_recorder`]. `OnceLock` rather than a field on
    /// the config because the labels are only known at run time, and because
    /// the hook takes `&self`.
    roundtrips: std::sync::OnceLock<Arc<faucet_core::observability::RoundtripRecorder>>,
}

/// Default value of [`RestStreamConfig::max_retries`]. When the user leaves this
/// untouched, an injected [`RetryPolicy`](faucet_core::RetryPolicy) is allowed to
/// override it (see [`RestStream::with_retry_policy`]).
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default value of [`RestStreamConfig::retry_backoff`]. Same precedence rule as
/// [`DEFAULT_MAX_RETRIES`].
const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// Starting delay for async-job poll backoff (doubles each Pending poll, up to
/// the configured `poll.interval_secs` cap).
const POLL_BACKOFF_BASE_SECS: u64 = 1;

/// Next async-job poll delay: exponential backoff (double the current delay),
/// capped at `cap`. `interval_secs` is the ceiling, so a quick job is noticed in
/// ~1s while a long job settles at the configured interval. Saturating so a
/// large current delay never overflows.
fn next_poll_delay(current: Duration, cap: Duration) -> Duration {
    std::cmp::min(current.saturating_mul(2), cap)
}

/// Attach a mutual-TLS client identity (from [`TlsClientConfig`]) to the HTTP
/// client builder. Only compiled with the `mtls` feature; the non-`mtls` stub
/// errors so a `tls:` block on a build without the feature fails loudly rather
/// than silently sending no client certificate.
#[cfg(feature = "mtls")]
fn apply_client_tls(
    builder: reqwest::ClientBuilder,
    tls: &TlsClientConfig,
) -> Result<reqwest::ClientBuilder, FaucetError> {
    let identity = build_identity(tls)?;
    // Use the native-tls backend explicitly: the identity is built with
    // native-tls constructors, and the workspace may also have rustls compiled
    // in (feature unification) which would otherwise be selected.
    let mut builder = builder.identity(identity).use_native_tls();
    if let Some(v) = &tls.min_version {
        // `TlsClientConfig::validate` guarantees `v` is "1.2" or "1.3".
        let version = if v == "1.3" {
            reqwest::tls::Version::TLS_1_3
        } else {
            reqwest::tls::Version::TLS_1_2
        };
        builder = builder.min_tls_version(version);
    }
    Ok(builder)
}

#[cfg(not(feature = "mtls"))]
fn apply_client_tls(
    _builder: reqwest::ClientBuilder,
    _tls: &TlsClientConfig,
) -> Result<reqwest::ClientBuilder, FaucetError> {
    Err(FaucetError::Config(
        "a `tls:` (mutual-TLS) block is configured, but this build of \
         faucet-source-rest lacks the `mtls` feature; rebuild with \
         `--features mtls`"
            .into(),
    ))
}

/// Build a [`reqwest::Identity`] from the PEM pair or the PKCS#12 file. Errors
/// never echo key material — only the backend's opaque parse message.
#[cfg(feature = "mtls")]
fn build_identity(tls: &TlsClientConfig) -> Result<reqwest::Identity, FaucetError> {
    if let Some(p12_path) = &tls.client_identity_pkcs12 {
        let der = std::fs::read(p12_path).map_err(|e| {
            FaucetError::Config(format!(
                "tls: could not read PKCS#12 file {p12_path:?}: {e}"
            ))
        })?;
        let password = tls.pkcs12_password.as_deref().unwrap_or("");
        reqwest::Identity::from_pkcs12_der(&der, password)
            .map_err(|e| FaucetError::Config(format!("tls: invalid PKCS#12 identity: {e}")))
    } else {
        // `validate()` guarantees both are present on the PEM path.
        let cert = tls.client_cert.as_deref().unwrap_or_default();
        let key = tls.client_key.as_deref().unwrap_or_default();
        reqwest::Identity::from_pkcs8_pem(cert.as_bytes(), key.as_bytes())
            .map_err(|e| FaucetError::Config(format!("tls: invalid PEM client identity: {e}")))
    }
}

/// Map a [`Credential`] from a shared provider onto the REST [`Auth`]
/// representation so the existing header-application path can be reused.
/// Substitute `${name}` tokens with flow-captured login values (#567). Only
/// exact `${name}` occurrences for a captured `name` are replaced; any other
/// `${...}` is left untouched. Applied to the URL and config header values so a
/// captured session value can travel there per request.
fn substitute_captured(s: &str, captured: &std::collections::BTreeMap<String, String>) -> String {
    if captured.is_empty() || !s.contains("${") {
        return s.to_string();
    }
    let mut out = s.to_string();
    for (k, v) in captured {
        out = out.replace(&format!("${{{k}}}"), v);
    }
    out
}

fn credential_to_auth(cred: Credential) -> Auth {
    match cred {
        Credential::Bearer(token) => Auth::Bearer { token },
        Credential::Token(token) => Auth::Custom {
            headers: std::iter::once(("Authorization".to_string(), token)).collect(),
        },
        Credential::Basic { username, password } => Auth::Basic { username, password },
        Credential::Header { name, value } => Auth::Custom {
            headers: std::iter::once((name, value)).collect(),
        },
    }
}

/// First JSONPath match rendered as a string (string verbatim, number as text).
/// Used by the async-job runner to read the job id / status from responses.
fn jsonpath_first_string(v: &Value, path: &str) -> Option<String> {
    use jsonpath_rust::JsonPath;
    let results = v.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// First JSONPath match as an owned [`Value`] (type-preserving). Used by the
/// resumable-cursor bookmark (#547) so a numeric cursor stays a number.
fn jsonpath_first_value(v: &Value, path: &str) -> Option<Value> {
    use jsonpath_rust::JsonPath;
    v.query(path).ok()?.first().map(|x| (*x).clone())
}

/// A locator value counts as "no more pages" when it is empty, or when it
/// matches one of the configured `locator_terminal_values` (default `["null"]`
/// — Salesforce Bulk sends `Sforce-Locator: null` when done). Comparison is
/// case-insensitive after trimming.
fn is_terminal_locator(value: &str, terminal: &[String]) -> bool {
    let v = value.trim();
    v.is_empty() || terminal.iter().any(|t| v.eq_ignore_ascii_case(t.trim()))
}

/// Derive the queried object name for an async-job source's `dataset_uri` (#640).
/// Looks for a `query` string in the submit body (Salesforce Bulk SOQL, etc.) and
/// returns its `FROM <object>`. `None` when there's no query or it can't be parsed
/// (the caller then falls back to a hash of the submit body).
fn async_job_object(submit_json: Option<&Value>, query_path: &str) -> Option<String> {
    let query = submit_json?.pointer(query_path)?.as_str()?;
    sql_from_object(query)
}

/// Extract the driving object from a bulk-query SQL statement: the token after
/// the first **top-level** `FROM` (a subquery's `FROM` — e.g. a parent-child
/// relationship query in the SELECT list — is never mistaken for it).
/// Case-insensitive on the keyword; preserves the object's own casing. Returns
/// `None` if there's no top-level `FROM` or the following token is empty.
fn sql_from_object(query: &str) -> Option<String> {
    let toks = top_level_tokens(query);
    let mut after_from = false;
    for (_, tok) in toks {
        if after_from {
            let obj = tok.trim_matches(|c: char| !c.is_alphanumeric() && c != '_');
            return if obj.is_empty() {
                None
            } else {
                Some(obj.to_string())
            };
        }
        if tok.eq_ignore_ascii_case("from") {
            after_from = true;
        }
    }
    None
}

/// Whitespace-split tokens of a query with their byte offsets, **restricted to
/// tokens that sit entirely at paren depth 0 and outside single-quoted string
/// literals**. This is what makes clause detection safe for the mainstream
/// query shapes: a subquery's `WHERE`/`FROM` (inside parens) and a keyword that
/// merely appears inside a quoted value (`WHERE name = 'a limit b'`) are never
/// mistaken for the outer statement's own clauses. Quote handling honors
/// backslash escapes (the escaping convention of the bulk-query grammars this
/// path targets).
fn top_level_tokens(s: &str) -> Vec<(usize, &str)> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut depth: i64 = 0;
    let mut in_quote = false;
    let mut tok_start: Option<usize> = None;
    let mut tok_clean = true;
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() && !in_quote {
            if let Some(start) = tok_start.take()
                && tok_clean
            {
                out.push((start, &s[start..i]));
            }
            tok_clean = true;
            i += 1;
            continue;
        }
        if tok_start.is_none() {
            tok_start = Some(i);
            tok_clean = depth == 0 && !in_quote;
        }
        if in_quote {
            match b {
                b'\\' => i += 1, // skip the escaped byte below
                b'\'' => in_quote = false,
                _ => {}
            }
        } else {
            match b {
                b'\'' => {
                    in_quote = true;
                    tok_clean = false;
                }
                b'(' => {
                    depth += 1;
                    tok_clean = false;
                }
                b')' => {
                    depth = depth.saturating_sub(1);
                    tok_clean = false;
                }
                _ => {}
            }
        }
        i += 1;
    }
    if let Some(start) = tok_start
        && tok_clean
        && !in_quote
    {
        out.push((start, &s[start..]));
    }
    out
}

/// Inject an incremental-replication predicate into a bulk-query SQL statement
/// (#630).
///
/// Adds `WHERE <predicate>` when there is no existing top-level `WHERE`, or
/// wraps the existing condition as `WHERE (<existing>) AND (<predicate>)` — the
/// parens keep operator precedence correct when the existing filter contains
/// `OR`. The predicate is placed *before* any trailing top-level clause
/// (`GROUP BY` / `HAVING` / `ORDER BY` / `LIMIT` / `OFFSET` / `WITH` / `FOR`),
/// which is where a `WHERE` must sit. Clause detection is whitespace-tolerant
/// (handles newlines) and, via [`top_level_tokens`], ignores keywords inside
/// subqueries and quoted literals.
fn inject_sql_predicate(query: &str, predicate: &str) -> String {
    let toks = top_level_tokens(query);
    let kw = |t: &str, k: &str| t.eq_ignore_ascii_case(k);
    let mut where_at: Option<usize> = None;
    let mut boundary: Option<usize> = None;
    let mut i = 0;
    while i < toks.len() {
        let (pos, t) = toks[i];
        let two = |a: &str, b: &str| kw(t, a) && i + 1 < toks.len() && kw(toks[i + 1].1, b);
        if where_at.is_none() && kw(t, "where") {
            where_at = Some(pos);
        } else if two("order", "by")
            || two("group", "by")
            || kw(t, "having")
            || kw(t, "limit")
            || kw(t, "offset")
            || kw(t, "with")
            || kw(t, "for")
        {
            boundary = Some(pos);
            break;
        }
        i += 1;
    }
    let boundary = boundary.unwrap_or(query.len());
    match where_at {
        Some(w) => {
            let head = query[..w + "where".len()].trim_end(); // "… WHERE"
            let cond = query[w + "where".len()..boundary].trim();
            let tail = query[boundary..].trim_start();
            let sep = if tail.is_empty() { "" } else { " " };
            format!("{head} ({cond}) AND ({predicate}){sep}{tail}")
        }
        None => {
            let head = query[..boundary].trim_end();
            let tail = query[boundary..].trim_start();
            let sep = if tail.is_empty() { "" } else { " " };
            format!("{head} WHERE {predicate}{sep}{tail}")
        }
    }
}

/// Render a bookmark value as a literal for the incremental predicate:
/// datetime/date values are **unquoted** (the datetime-literal convention of
/// the bulk-query grammars this path targets — replication keys are datetimes),
/// everything else is single-quoted with `'` backslash-escaped (same grammar
/// convention; standard-SQL `''` doubling can become a dialect knob if a second
/// grammar ever needs it).
fn sql_literal(v: &Value) -> String {
    match v {
        Value::String(s) if is_sql_datetime(s) => s.clone(),
        Value::String(s) => format!("'{}'", s.replace('\'', "\\'")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => format!("'{}'", other.to_string().replace('\'', "\\'")),
    }
}

/// Whether a string is a datetime/date literal (RFC3339 or `YYYY-MM-DD`).
fn is_sql_datetime(s: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(s).is_ok()
        || chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").is_ok()
}

/// Read the next result-set locator (#557) from the fetch response header or
/// body, per the `fetch` config. Returns `None` when no locator source is
/// configured or the locator signals completion.
fn next_locator(
    headers: &HeaderMap,
    body: Option<&Value>,
    job: &crate::async_job::AsyncJobConfig,
) -> Option<String> {
    let terminal = &job.fetch.locator_terminal_values;
    if let Some(name) = &job.fetch.locator_header
        && let Some(raw) = headers.get(name).and_then(|v| v.to_str().ok())
        && !is_terminal_locator(raw, terminal)
    {
        return Some(raw.trim().to_string());
    }
    if let Some(path) = &job.fetch.locator_body
        && let Some(body) = body
        && let Some(raw) = jsonpath_first_string(body, path)
        && !is_terminal_locator(&raw, terminal)
    {
        return Some(raw.trim().to_string());
    }
    None
}

/// Insert a header from string parts, mapping invalid names/values to a typed
/// config error rather than panicking.
fn insert_header(headers: &mut HeaderMap, name: &str, value: &str) -> Result<(), FaucetError> {
    let hn = HeaderName::from_bytes(name.as_bytes())
        .map_err(|e| FaucetError::Config(format!("rest: invalid header name '{name}': {e}")))?;
    let hv = HeaderValue::from_str(value).map_err(|e| {
        FaucetError::Config(format!("rest: invalid value for header '{name}': {e}"))
    })?;
    headers.insert(hn, hv);
    Ok(())
}

impl RestStream {
    /// Create a new stream from the given configuration.
    pub fn new(mut config: RestStreamConfig) -> Result<Self, FaucetError> {
        // Derive OData request defaults (paging, `$.value`, query sugar, Prefer)
        // before validation so the checks see the effective request shape.
        config.apply_odata_defaults();
        // Cross-field config invariants (e.g. file response formats can't paginate).
        config.validate()?;
        // Validate expiry_ratio at construction time.
        let expiry_ratio_to_validate = match &config.auth {
            AuthSpec::Inline(Auth::OAuth2 { expiry_ratio, .. })
            | AuthSpec::Inline(Auth::TokenEndpoint { expiry_ratio, .. }) => Some(*expiry_ratio),
            _ => None,
        };
        if let Some(ratio) = expiry_ratio_to_validate
            && (ratio <= 0.0 || ratio > 1.0)
        {
            return Err(FaucetError::Auth(format!(
                "expiry_ratio must be in (0.0, 1.0], got {ratio}"
            )));
        }

        let mut builder = Client::builder();
        // Transparently request + decode gzip/brotli/deflate so large JSON APIs
        // (e.g. D365 F&O OData, which compresses ~5-10x) ship compressed bytes
        // instead of raw JSON. reqwest sets the `Accept-Encoding` header and
        // decompresses the body automatically.
        builder = builder.gzip(true).brotli(true).deflate(true);
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        // Mutual TLS: attach a client certificate/identity to the shared client
        // so it is presented on every request — data pages AND any inline auth
        // token request (both use `self.client`).
        if let Some(tls) = &config.tls {
            tls.validate()?;
            builder = apply_client_tls(builder, tls)?;
        }
        // Build the default retry policy from REST's own legacy reliability
        // fields so behavior is unchanged when no policy is injected. The REST
        // `retry::execute_with_retry` runner is driven by `max_retries`
        // (retries-after-first) + `base`, so `max_attempts = max_retries + 1`.
        let retry_policy = faucet_core::RetryPolicy {
            max_attempts: config.max_retries.saturating_add(1),
            backoff: faucet_core::BackoffKind::Exponential,
            base: config.retry_backoff,
            ..faucet_core::RetryPolicy::default()
        };
        // Static custom headers (#539): validated once here (also validated in
        // `config.validate()` above, so this cannot fail) and reused per request.
        let static_headers = crate::config::build_header_map(&config.headers)?;
        Ok(Self {
            config,
            client: builder.build()?,
            token_cache: TokenCache::new(),
            token_endpoint_cache: TokenEndpointCache::new(),
            auth_provider: None,
            runtime_start: Arc::new(AsyncMutex::new(None)),
            window_binds: Arc::new(AsyncMutex::new(Vec::new())),
            now_override: None,
            retry_policy,
            static_headers,
            metadata_xml_cache: tokio::sync::OnceCell::new(),
            roundtrips: std::sync::OnceLock::new(),
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set, the
    /// provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who construct one provider and inject it into many
    /// sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Test-only: pin the "now" upper bound used by datetime window slicing (#527)
    /// to a fixed RFC 3339 instant, so the window enumeration is deterministic in
    /// tests. No effect in production (which uses `Utc::now()`). Hidden from docs;
    /// takes a string so callers need not depend on `chrono`.
    #[doc(hidden)]
    pub fn with_now_override_rfc3339(mut self, rfc3339: &str) -> Self {
        self.now_override = chrono::DateTime::parse_from_rfc3339(rfc3339)
            .ok()
            .map(|d| d.with_timezone(&chrono::Utc));
        self
    }

    /// Attach a custom [`RetryPolicy`](faucet_core::RetryPolicy) for transient
    /// request failures, used by the CLI to inject a pipeline-level
    /// `resilience:` policy.
    ///
    /// **Legacy-field precedence:** the REST connector predates the unified
    /// resilience policy and exposes its own `max_retries` / `retry_backoff`
    /// config fields. If the user has set either of those away from its default
    /// (`max_retries: 3`, `retry_backoff: 1s`), those explicit values win and the
    /// injected `policy` is ignored — an explicit per-connector setting is never
    /// silently overridden by a pipeline-wide default. When both fields are at
    /// their defaults, the injected policy takes effect.
    ///
    /// **Inert fields on REST:** because the REST source keeps its own
    /// `429`/`Retry-After`-aware retry runner, it honors only the injected
    /// policy's `max_attempts` (→ `max_retries`) and `base` (→ `retry_backoff`).
    /// The policy's `max` (per-sleep cap), `jitter`, and `retry_on` fields are
    /// **not** honored here — they apply on the `xml`/`graphql` sources and on
    /// every sink-side write.
    pub fn with_retry_policy(mut self, policy: faucet_core::RetryPolicy) -> Self {
        let user_changed_legacy_fields = self.config.max_retries != DEFAULT_MAX_RETRIES
            || self.config.retry_backoff != DEFAULT_RETRY_BACKOFF;
        if !user_changed_legacy_fields {
            self.retry_policy = policy;
        }
        self
    }

    /// Fetch all records across all pages as raw JSON values.
    ///
    /// When `partitions` are configured, the stream is executed once per
    /// partition and all results are concatenated.
    ///
    /// When `replication_method` is `Incremental` and `replication_key` +
    /// `start_replication_value` are both set, records at or before the
    /// bookmark are filtered out.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        if self.config.partitions.is_empty() {
            self.fetch_partition(None, None).await
        } else if let Some(concurrency) = self.config.partition_concurrency {
            // Process partitions concurrently using a semaphore to limit parallelism.
            let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
            let mut handles = Vec::with_capacity(self.config.partitions.len());

            for ctx in &self.config.partitions {
                let permit =
                    semaphore.clone().acquire_owned().await.map_err(|e| {
                        FaucetError::Config(format!("semaphore acquire failed: {e}"))
                    })?;
                let fut = self.fetch_partition(Some(ctx), None);
                handles.push(async move {
                    let result = fut.await;
                    drop(permit);
                    result
                });
            }

            let results = futures::future::try_join_all(handles).await?;
            Ok(results.into_iter().flatten().collect())
        } else {
            let mut all_records = Vec::new();
            for ctx in &self.config.partitions {
                let records = self.fetch_partition(Some(ctx), None).await?;
                all_records.extend(records);
            }
            Ok(all_records)
        }
    }

    /// Fetch all records and deserialize into typed structs.
    pub async fn fetch_all_as<T: for<'de> Deserialize<'de>>(&self) -> Result<Vec<T>, FaucetError> {
        let values = self.fetch_all().await?;
        values
            .into_iter()
            .map(|v| serde_json::from_value(v).map_err(FaucetError::Json))
            .collect()
    }

    /// Infer a JSON Schema for this stream's records.
    ///
    /// If a `schema` is already set on the config, it is returned immediately
    /// without making any HTTP requests.
    ///
    /// Otherwise the stream fetches up to `schema_sample_size` records
    /// (respecting `max_pages`) and derives a JSON Schema from them.  Fields
    /// that are absent in some records, or that carry a `null` value, are
    /// marked as nullable (`["<type>", "null"]`).
    ///
    /// Set `schema_sample_size` to `0` to sample all available records.
    pub async fn infer_schema(&self) -> Result<Value, FaucetError> {
        if let Some(ref s) = self.config.schema {
            return Ok(s.clone());
        }
        let limit = match self.config.schema_sample_size {
            0 => None,
            n => Some(n),
        };
        let records = self.fetch_partition(None, limit).await?;
        Ok(schema::infer_schema(&records))
    }

    /// Fetch all records in incremental mode, returning the records along with
    /// the maximum value of `replication_key` observed across those records.
    ///
    /// The returned bookmark should be persisted by the caller and passed back
    /// as `start_replication_value` on the next run.
    ///
    /// If no `replication_key` is configured, this behaves identically to
    /// [`fetch_all`](Self::fetch_all) and the bookmark is `None`.
    pub async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let records = self.fetch_all().await?;
        let bookmark = self
            .config
            .replication_key
            .as_deref()
            .and_then(|key| max_replication_value(&records, key))
            .cloned();
        Ok((records, bookmark))
    }

    /// Stream API pages without buffering the full result set.
    ///
    /// This is a thin convenience wrapper around the
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages) trait
    /// method — it discards bookmarks and yields one `Vec<Value>` per
    /// upstream API page. Use the trait method directly if you need
    /// per-page bookmarks for incremental replication.
    ///
    /// Note: this inherent convenience method does not fan out over
    /// `partitions`. The `Source::stream_pages` trait impl (what the pipeline
    /// drives) and [`fetch_all`](Self::fetch_all) do handle multi-partition
    /// streams (#535).
    ///
    /// ```rust,no_run
    /// use faucet_source_rest::{RestStream, RestStreamConfig};
    /// use futures::StreamExt;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let stream = RestStream::new(RestStreamConfig::new("https://api.example.com", "/items"))?;
    /// let mut pages = stream.stream_pages();
    /// while let Some(page) = pages.next().await {
    ///     let records = page?;
    ///     println!("got {} records", records.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stream_pages(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Value>, FaucetError>> + Send + '_>> {
        let mut inner = self.stream_pages_inner(None, None, faucet_core::DEFAULT_BATCH_SIZE);
        Box::pin(async_stream::try_stream! {
            loop {
                let page = std::future::poll_fn(|cx| inner.as_mut().poll_next(cx)).await;
                match page {
                    Some(Ok(p)) => yield p.records,
                    Some(Err(e)) => Err(e)?,
                    None => break,
                }
            }
        })
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    /// Extract a page's records from a parsed response body, honouring the
    /// configured extraction mode: `records_multi` (#548, op-stamped multi-array
    /// fan-out), `record_ancestors` (#549, nested path with lifted ancestor
    /// fields), or the classic single `records_path`.
    /// The per-record key prefixes to drop. An `odata:` block implies
    /// `@odata.`; explicit `drop_key_prefixes` are added to it.
    /// Count one upstream round trip, when the pipeline installed a recorder
    /// (#638). `op` is drawn from this connector's closed set:
    /// `submit` / `poll` / `fetch` / `page`.
    fn roundtrip(&self, op: &'static str) {
        if let Some(r) = self.roundtrips.get() {
            r.record(op);
        }
    }

    /// Run the row-count probe and read the count out of its response (#629).
    ///
    /// A probe that fails is **not** fatal: falling back to the async job is
    /// the pre-#629 behaviour and always correct, just slower — failing the
    /// run because an optimisation's probe 404'd would be the worse trade.
    async fn probe_row_count(
        &self,
        probe: &crate::async_job::RowCountProbe,
    ) -> Result<u64, FaucetError> {
        let base = &self.config.base_url;
        let url =
            crate::async_job::resolve_url(base, probe.request.url.as_deref().unwrap_or_default());
        let body = match self
            .job_request_json(
                "count",
                &probe.request.method,
                &url,
                &probe.request.headers,
                &probe.request.query,
                probe.request.json.as_ref(),
            )
            .await
        {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "rest: row-count probe failed; using the async job path"
                );
                return Ok(u64::MAX);
            }
        };
        let found = jsonpath_first_value(&body, &probe.count_path);
        match found {
            Some(Value::Number(n)) => Ok(n.as_u64().unwrap_or(u64::MAX)),
            // A count arriving as a string is common enough (and unambiguous)
            // to accept; anything else means the path is wrong, which is a
            // config error worth surfacing rather than silently guessing.
            Some(Value::String(t)) => t.trim().parse::<u64>().map_err(|_| {
                FaucetError::Config(format!(
                    "async_job: count_path '{}' matched '{t}', which is not a row count",
                    probe.count_path
                ))
            }),
            Some(other) => Err(FaucetError::Config(format!(
                "async_job: count_path '{}' matched {other}, which is not a row count",
                probe.count_path
            ))),
            None => Err(FaucetError::Config(format!(
                "async_job: count_path '{}' matched nothing in the probe response",
                probe.count_path
            ))),
        }
    }

    /// Page size for the whole-file tabular formats, or `None` when this is
    /// not file mode (#624).
    ///
    /// `response_format: csv | excel` fetches one body that is the entire
    /// table — `validate()` guarantees `pagination: none` — so the only place
    /// a page boundary can come from is here. `0` is the house "no batching"
    /// sentinel and keeps the single-page behaviour.
    fn file_mode_page_size(&self, csv_page_size: usize) -> Option<usize> {
        use crate::config::ResponseFormat;
        matches!(
            self.config.response_format,
            ResponseFormat::Csv | ResponseFormat::Excel
        )
        .then_some(csv_page_size)
        .filter(|n| *n > 0)
    }

    fn drop_key_prefixes(&self) -> Vec<String> {
        let mut out = self.config.drop_key_prefixes.clone();
        if self.config.odata.is_some() && !out.iter().any(|p| p == "@odata.") {
            out.push("@odata.".to_string());
        }
        out
    }

    fn extract_page(&self, body: &Value) -> Result<Vec<Value>, FaucetError> {
        let mut records = extract::extract_configured(
            body,
            self.config.records_path.as_deref(),
            self.config.record_ancestors.as_ref(),
            &self.config.records_multi,
            self.config.op_field.as_deref().unwrap_or("_op"),
        )?;
        // Protocol control fields stamped per record (`@odata.etag`, JSON:API's
        // `links`, HAL's `_links`, …) are metadata, not data, and are often
        // invalid column names downstream. Drop them so records carry only the
        // entity's own fields. The prefix list is configurable
        // (`drop_key_prefixes`); an `odata:` block implies `@odata.` so existing
        // configs keep working without naming it (#654 M24).
        let prefixes = self.drop_key_prefixes();
        if !prefixes.is_empty() {
            for rec in &mut records {
                if let Value::Object(map) = rec {
                    map.retain(|k, _| !prefixes.iter().any(|p| k.starts_with(p.as_str())));
                }
            }
        }
        Ok(records)
    }

    /// Core pagination loop shared by [`Source::stream_pages`] and
    /// [`fetch_partition`](Self::fetch_partition).
    ///
    /// Yields one [`faucet_core::StreamPage`] per page. The final page carries
    /// the consolidated replication bookmark (`Some(value)`); all intermediate
    /// pages carry `None`. When `context` is `Some`, path placeholders are
    /// substituted for partition support.
    fn stream_pages_inner(
        &self,
        context: Option<&HashMap<String, Value>>,
        range_filter: Option<String>,
        // Records per emitted page for the bounded-memory CSV path (#626).
        // `0` is the house "no batching" sentinel. Ignored by the HTTP-paged
        // paths, which chunk on upstream page boundaries.
        csv_page_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::StreamPage, FaucetError>> + Send + '_>> {
        // Clone the context into an owned map so it can live inside the
        // `async_stream` generator without borrowing from the caller.
        let owned_context: Option<HashMap<String, Value>> = context.cloned();

        Box::pin(async_stream::try_stream! {
            // Async-job lifecycle (#514/#623): submit → poll → resolve the fetch
            // URL once, then stream one `StreamPage` per locator-paged result set
            // (#557) instead of buffering the entire extract into a single page.
            // Size-based routing (#629): when the object is small enough, skip
            // the job's async floor entirely and fall through to the ordinary
            // paginated read below. Decided per run from a probe rather than
            // from a guess baked into the config, because which objects are
            // "small" changes.
            let take_async_job = match self.config.async_job.as_ref() {
                None => false,
                Some(job) => match job.sync_routing() {
                    None => true,
                    Some((threshold, probe)) => {
                        let rows = self.probe_row_count(probe).await?;
                        let small = rows < threshold;
                        tracing::info!(
                            rows,
                            threshold,
                            path = %if small { "synchronous" } else { "async job" },
                            "rest: routed by row count"
                        );
                        !small
                    }
                },
            };
            if let Some(job) = self.config.async_job.as_ref().filter(|_| take_async_job) {
                // Capture the incremental bookmark (#630) BEFORE submitting, so it
                // reflects the query's start time (conservative — a small re-read
                // overlap next run, deduped by an upsert sink). `None` for full-table.
                let new_bookmark = self.async_job_new_bookmark();
                let fetch_url = self.prepare_async_job().await?;
                let mut locator: Option<String> = None;
                loop {
                    // Send the locator (when we have one) as the configured query param.
                    let mut query = job.fetch.query.clone();
                    if let (Some(loc), Some(param)) = (&locator, &job.fetch.locator_param) {
                        query.insert(param.clone(), loc.clone());
                    }
                    // Bounded-memory CSV (#626). A locator page is one HTTP
                    // response, and for a Bulk extract that response *is* the
                    // whole object — 3.96 GB peak for 838k rows once it is
                    // buffered and turned into one `Vec<Value>`. When the decode
                    // chain is a bare `parse: csv`, decode straight off the body
                    // and emit `batch_size` pages, so peak memory is one page.
                    //
                    // Requires the locator to come from a **header**: reading it
                    // from the body would need the parsed document this path
                    // deliberately never materializes.
                    let plan = crate::decode::csv_stream_plan(&self.config.decode)
                        .filter(|_| job.fetch.locator_body.is_none());
                    // Each branch returns the response headers (for the locator
                    // advance) and, for the buffered path only, the parsed body.
                    let (body_value, resp_headers) = if let Some(plan) = plan {
                        let resp = self
                            .job_request_response(
                                "fetch",
                                &job.fetch.method,
                                &fetch_url,
                                &job.fetch.headers,
                                &query,
                                job.fetch.json.as_ref(),
                            )
                            .await?;
                        // Read the locator before the body is consumed.
                        let resp_headers_streamed = resp.headers().clone();
                        use futures::TryStreamExt as _;
                        let body = resp.bytes_stream().map_err(std::io::Error::other);
                        let reader = tokio_util::io::StreamReader::new(body);
                        let mut pages = Box::pin(crate::format::csv_reader_to_value_pages_with(
                            reader,
                            crate::format::CsvDialect {
                                delimiter: plan.delimiter,
                                has_headers: plan.has_headers,
                                quote: self.config.csv_quote,
                            },
                            csv_page_size,
                        ));
                        use futures::StreamExt as _;
                        let mut emitted = false;
                        while let Some(page) = pages.next().await {
                            let records = page?;
                            emitted = true;
                            yield faucet_core::StreamPage { records, bookmark: None };
                        }
                        // A header-only (or empty) result still produces one
                        // page, matching the buffered path: every locator fetch
                        // yields at least one `StreamPage`, so a downstream that
                        // counts pages per fetch sees the same thing either way.
                        if !emitted {
                            yield faucet_core::StreamPage {
                                records: Vec::new(),
                                bookmark: None,
                            };
                        }
                        (None, resp_headers_streamed)
                    } else {
                        let (bytes, resp_headers_buffered) = self
                            .job_request_bytes(
                                "fetch",
                                &job.fetch.method,
                                &fetch_url,
                                &job.fetch.headers,
                                &query,
                                job.fetch.json.as_ref(),
                            )
                            .await?;
                        let (records, body_value) =
                            self.parse_fetch_page(&bytes, job).await?;
                        // Stream this locator page immediately — peak memory is
                        // O(one page), not O(whole extract). Per-page bookmark
                        // stays `None`; the incremental bookmark is emitted once
                        // at the end.
                        yield faucet_core::StreamPage { records, bookmark: None };
                        (body_value, resp_headers_buffered)
                    };

                    // Advance to the next locator; stop when it is absent, empty,
                    // `"null"`, or repeats (loop guard) — matching the previous
                    // buffering behavior.
                    let next = next_locator(&resp_headers, body_value.as_ref(), job);
                    match next {
                        Some(loc) if locator.as_deref() != Some(loc.as_str()) => {
                            locator = Some(loc);
                        }
                        _ => break,
                    }
                }
                // Incremental (#630): emit the run-start bookmark on a final empty
                // page so the pipeline persists it after the sink confirms — the
                // next run injects `WHERE <key> > <this>` and pulls only the delta.
                if let Some(bm) = new_bookmark {
                    yield faucet_core::StreamPage { records: Vec::new(), bookmark: Some(bm) };
                }
                return;
            }

            // Resolve the effective start-bookmark once at the top of the stream.
            // A runtime override (applied via `Source::apply_start_bookmark` —
            // typically by the pipeline reading from a `StateStore`) takes
            // precedence over the static config value.
            let effective_start: Option<Value> = {
                let guard = self.runtime_start.lock().await;
                guard
                    .clone()
                    .or_else(|| self.config.start_replication_value.clone())
            };

            // H13 (audit #146): combining `max_pages` with incremental
            // replication only makes safe forward progress when the API returns
            // rows ordered ascending by the replication key. On truncation we
            // advance the bookmark to the max key seen so far (so the next run
            // resumes past it — without this the stream would re-read the same
            // first `max_pages` window forever and never progress); but if the
            // feed is unordered, unfetched later pages may hold lower keys that
            // resuming past `running_max` would then drop. Warn loudly so the
            // requirement is explicit rather than a silent data-loss edge.
            if self.config.max_pages.is_some()
                && self.config.replication_method == ReplicationMethod::Incremental
                && self.config.replication_key.is_some()
            {
                tracing::warn!(
                    "max_pages combined with incremental replication assumes the API returns rows \
                     ordered ascending by the replication key; an unordered feed can drop unfetched \
                     lower-key records on resume. Ensure ordering, or remove max_pages for a full \
                     incremental sweep."
                );
            }

            // #527: build the pass plan. Without a `window:` block this is a
            // single "unbounded" pass with the classic record-derived bookmark.
            // With one, each rolling `[start, end)` window is its own pass whose
            // bookmark is the window's end boundary — so a mid-sweep crash resumes
            // from the last completed window (per-window durability).
            let windowed = self.config.window.is_some();
            let passes: Vec<Option<faucet_core::Window>> = if let Some(win) = &self.config.window {
                let start_val = effective_start.clone().ok_or_else(|| {
                    FaucetError::Config(
                        "rest: `window` slicing requires a start bookmark (from a `state:` store) \
                         or `start_replication_value` to anchor the first window".into(),
                    )
                })?;
                let start_instant = faucet_core::parse_instant(&start_val)?;
                let now = self.now_override.unwrap_or_else(chrono::Utc::now);
                let step = win.step_duration()?;
                let lookback = win.lookback_duration()?;
                let (windows, truncated) =
                    faucet_core::enumerate_windows(start_instant, now, step, lookback, win.max_windows);
                if truncated {
                    tracing::warn!(
                        max_windows = win.max_windows,
                        "window slicing hit `max_windows`; this run's sweep is truncated — the next \
                         run resumes from the last completed window"
                    );
                }
                if windows.is_empty() {
                    tracing::debug!(
                        "window slicing: the bookmark is at or ahead of now; nothing to fetch"
                    );
                }
                windows.into_iter().map(Some).collect()
            } else {
                vec![None]
            };

            for pass in passes {
                // Set the window bounds applied to every request in this pass
                // (an unbounded pass leaves `window_binds` empty).
                // `execute_request_once` reads `self.window_binds`.
                if let Some(w) = &pass {
                    let win = self
                        .config
                        .window
                        .as_ref()
                        .expect("a window pass implies a `window:` block");
                    let lower = (win.lower.into, win.lower.name.clone(), win.render_lower(w));
                    let upper_rendered = win.render_upper(w)?;
                    let upper = (win.upper.into, win.upper.name.clone(), upper_rendered);
                    *self.window_binds.lock().await = vec![lower, upper];
                }

                // The bookmark this pass persists on its final page: the window's
                // end (a half-open boundary, so resume neither gaps nor overlaps)
                // for a windowed pass, or the record-derived running max for the
                // classic unbounded pass.
                let window_bookmark: Option<Value> =
                    pass.as_ref().map(|w| Value::String(w.end.to_rfc3339()));

                let mut state = PaginationState::default();
                // #547: on resume, seed the stored cursor into the first request
                // (query param for `Cursor`, body field for `CursorInBody`).
                if self.config.persist_cursor
                    && let Some(seed) = effective_start.as_ref()
                {
                    state.next_token =
                        Some(crate::pagination::value_to_param_string(seed));
                }
                let mut pages_fetched = 0usize;
                let mut running_max: Option<Value> = effective_start.clone();
                // #547: the terminal cursor to persist as this run's bookmark.
                let mut running_cursor: Option<Value> = effective_start.clone();
                let mut bookmark_emitted = false;

                loop {
                    if let Some(max) = self.config.max_pages
                        && pages_fetched >= max
                    {
                        tracing::warn!("max pages ({max}) reached");
                        break;
                    }

                    let mut params = self.config.query_params.clone();
                    self.config.pagination.apply_params(&mut params, &state);

                    // Key-range partition filter: the first request of a range
                    // carries the half-open PK `$filter`; `@odata.nextLink`
                    // preserves it on every subsequent page, so it is applied once.
                    if let Some(range_filter) = range_filter.as_deref() {
                        let combined = match params.get("$filter") {
                            Some(existing) if !existing.is_empty() => {
                                format!("({existing}) and {range_filter}")
                            }
                            _ => range_filter.to_string(),
                        };
                        params.insert("$filter".to_string(), combined);
                    }

                    let url_override = match &self.config.pagination {
                        PaginationStyle::LinkHeader | PaginationStyle::NextLinkInBody { .. } => {
                            state.next_link.clone()
                        }
                        _ => None,
                    };

                    // Body-carrying pagination (CursorInBody / OffsetInBody /
                    // RecordFieldCursor into:body): fields injected into the
                    // request JSON body for this page.
                    let body_params = self.config.pagination.body_params(&state);

                    let params_clone = params.clone();
                    let ctx_ref = owned_context.as_ref();
                    let is_first_page = pages_fetched == 0;
                    let (body, resp_headers) = retry::execute_with_retry(
                        // The REST runner takes retries-after-first; the policy holds
                        // total attempts. Feed both knobs from the resolved policy so
                        // an injected `resilience:` policy (when legacy fields are
                        // untouched) governs the retry budget + base backoff while the
                        // runner keeps its 429 / `Retry-After` handling.
                        self.retry_policy.max_attempts.saturating_sub(1),
                        self.retry_policy.base,
                        || {
                            self.execute_request(
                                &params_clone,
                                url_override.as_deref(),
                                ctx_ref,
                                is_first_page,
                                &body_params,
                            )
                        },
                    )
                    .await?;

                    let raw_records = self.extract_page(&body)?;
                    let raw_count = raw_records.len();

                    // #547: track the terminal cursor to persist as the bookmark.
                    if self.config.persist_cursor
                        && let Some(path) = self.config.pagination.cursor_path()
                        && let Some(cursor) = jsonpath_first_value(&body, path)
                    {
                        match &cursor {
                            Value::Null => {}
                            Value::String(s) if s.is_empty() => {}
                            _ => running_cursor = Some(cursor),
                        }
                    }

                    // Client-side incremental filter. Skipped for windowed passes:
                    // the server already bounds each window, and filtering by the
                    // overall start would drop `lookback` rows that fall before it.
                    let records = if !windowed
                        && self.config.replication_method == ReplicationMethod::Incremental
                    {
                        if let (Some(key), Some(start)) =
                            (&self.config.replication_key, effective_start.as_ref())
                        {
                            filter_incremental(raw_records, key, start)
                        } else {
                            raw_records
                        }
                    } else {
                        raw_records
                    };

                    // Track the running max replication value across pages so the
                    // final page of an unbounded pass can carry the consolidated
                    // bookmark. When the replication bind declares `advance_from`,
                    // the next bookmark is read from that JSONPath in the response
                    // body (#513); otherwise it is `max(record[replication_key])`.
                    // Windowed passes ignore this — their bookmark is the window end.
                    if !windowed
                        && self.config.replication_method == ReplicationMethod::Incremental
                    {
                        let page_max: Option<Value> = match self
                            .config
                            .replication_bind
                            .as_ref()
                            .and_then(|b| b.advance_from.as_deref())
                        {
                            Some(path) => faucet_core::util::extract_records(&body, Some(path))
                                .ok()
                                .and_then(|vs| vs.into_iter().next()),
                            None => self
                                .config
                                .replication_key
                                .as_deref()
                                .and_then(|key| max_replication_value(&records, key).cloned()),
                        };
                        if let Some(page_max) = page_max {
                            running_max = Some(match running_max.take() {
                                Some(prev) => max_value(prev, page_max),
                                None => page_max,
                            });
                        }
                    }

                    // #554: derive this page's keyset cursor (max/min of the
                    // configured field) so the next request can page by it. A
                    // no-op for every non-RecordFieldCursor style.
                    self.config
                        .pagination
                        .update_record_cursor(&records, &mut state);

                    // Advance pagination state to learn whether there is a next
                    // page BEFORE yielding the current one. This way the bookmark
                    // is only attached to pages where `has_next == false`, and we
                    // never pre-fetch the next page just to classify the current
                    // one as "final" (which would prevent early exit in callers
                    // such as `fetch_partition` with `max_records`).
                    let has_next = self
                        .config
                        .pagination
                        .advance(&body, &resp_headers, &mut state, raw_count)?;
                    pages_fetched += 1;

                    if has_next {
                        // Intermediate page — yield without bookmark so the
                        // pipeline does not persist a partial checkpoint.
                        yield faucet_core::StreamPage { records, bookmark: None };
                    } else if state.current_page_is_duplicate {
                        // The content-stagnation guard flagged this page as a
                        // duplicate of the previous one — DROP it (do not emit the
                        // repeated records to the sink) and stop. The trailing
                        // bookmark checkpoint below still fires (#321 L1).
                        break;
                    } else {
                        // Final page of this pass — attach the pass bookmark.
                        let bookmark = if self.config.persist_cursor {
                            running_cursor.clone()
                        } else if windowed {
                            window_bookmark.clone()
                        } else {
                            running_max.clone()
                        };
                        bookmark_emitted = bookmark.is_some();
                        // File mode (`response_format: csv | excel`) downloads
                        // the whole tabular body in one response, so without
                        // this the entire sheet lands in a single page and the
                        // pipeline holds every record at once (#624). The
                        // bytes are still whole — there is no ranged parse of
                        // a CSV over HTTP — but the *parsed* page is bounded.
                        let chunk = self.file_mode_page_size(csv_page_size);
                        if let Some(size) = chunk.filter(|s| records.len() > *s) {
                            let total = records.len();
                            let mut emitted = 0usize;
                            for part in records.chunks(size) {
                                emitted += part.len();
                                let last = emitted == total;
                                yield faucet_core::StreamPage {
                                    records: part.to_vec(),
                                    // Only the final chunk carries the
                                    // bookmark: a mid-file checkpoint would
                                    // claim progress the sink has not been
                                    // handed yet.
                                    bookmark: if last { bookmark.clone() } else { None },
                                };
                            }
                        } else {
                            yield faucet_core::StreamPage { records, bookmark };
                        }
                        break;
                    }

                    if let Some(delay) = self.config.request_delay {
                        tokio::time::sleep(delay).await;
                    }
                }

                // Trailing checkpoint: if the pass loop exited without carrying the
                // bookmark on a real page (max_pages truncation, or a duplicate-page
                // stop), emit one empty page carrying the pass bookmark so progress
                // still persists and the next run resumes from here. (Safe forward
                // progress under max_pages assumes ascending order by the
                // replication key — see the warning emitted above, audit #146 H13.)
                let pass_bookmark = if self.config.persist_cursor {
                    running_cursor.clone()
                } else if windowed {
                    window_bookmark.clone()
                } else {
                    running_max.clone()
                };
                if !bookmark_emitted && pass_bookmark.is_some() {
                    yield faucet_core::StreamPage {
                        records: Vec::new(),
                        bookmark: pass_bookmark,
                    };
                }
            }

            // Clear the window bounds so a reused source instance starts clean.
            if windowed {
                self.window_binds.lock().await.clear();
            }
        })
    }

    /// Run the full pagination loop for a single partition context.
    ///
    /// `max_records`: when `Some(n)`, stop collecting after `n` records
    /// (used for schema sampling).
    async fn fetch_partition(
        &self,
        context: Option<&HashMap<String, Value>>,
        max_records: Option<usize>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut pages_fetched = 0usize;
        let mut pages = self.stream_pages_inner(context, None, faucet_core::DEFAULT_BATCH_SIZE);

        // Poll the stream without requiring StreamExt (avoids extra dependency).
        loop {
            let page = std::future::poll_fn(|cx: &mut std::task::Context<'_>| {
                pages.as_mut().poll_next(cx)
            })
            .await;

            match page {
                Some(Ok(page)) => {
                    pages_fetched += 1;
                    let records = page.records;
                    match max_records {
                        Some(limit) => {
                            let remaining = limit.saturating_sub(all_records.len());
                            all_records.extend(records.into_iter().take(remaining));
                            if all_records.len() >= limit {
                                break;
                            }
                        }
                        None => all_records.extend(records),
                    }
                }
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }

        tracing::info!(
            stream = self.config.name.as_deref().unwrap_or("(unnamed)"),
            records = all_records.len(),
            pages = pages_fetched,
            "fetch complete"
        );
        Ok(all_records)
    }

    /// Execute a request, transparently refreshing an inline OAuth2 /
    /// TokenEndpoint token once on a 401.
    ///
    /// The cached token's validity is tracked purely by the server-reported
    /// `expires_in` (and a token with no `expires_in` is cached as valid
    /// forever), so a *server-side* expiry surfaces only as a 401 on a real
    /// request. The documented contract is "valid until a 401 forces a
    /// refresh" — so on a 401 with an inline cached token we invalidate the
    /// cache and retry exactly once with a freshly-fetched token (F57). Shared
    /// auth providers manage their own refresh and are not retried here.
    async fn execute_request(
        &self,
        params: &HashMap<String, String>,
        url_override: Option<&str>,
        path_context: Option<&HashMap<String, Value>>,
        is_first_page: bool,
        body_params: &[(String, Value)],
    ) -> Result<(Value, HeaderMap), FaucetError> {
        match self
            .execute_request_once(
                params,
                url_override,
                path_context,
                is_first_page,
                body_params,
            )
            .await
        {
            Err(FaucetError::HttpStatus { status: 401, .. }) if self.uses_inline_cached_token() => {
                tracing::warn!(
                    "401 Unauthorized with a cached inline OAuth2/TokenEndpoint token; \
                     invalidating the token cache and retrying once with a fresh token"
                );
                self.invalidate_inline_token_cache().await;
                self.execute_request_once(
                    params,
                    url_override,
                    path_context,
                    is_first_page,
                    body_params,
                )
                .await
            }
            // #511: a shared provider (e.g. a multi-step flow) whose session
            // expired mid-run — re-auth on a status it declared in `reauth_on`
            // and retry once.
            Err(FaucetError::HttpStatus { status, .. }) if self.provider_wants_reauth(status) => {
                if let Some(provider) = &self.auth_provider {
                    tracing::warn!(
                        status,
                        "shared auth provider requested re-auth on this status; \
                         re-authenticating and retrying once"
                    );
                    let _ = provider.invalidate(&Credential::Token(String::new())).await;
                }
                self.execute_request_once(
                    params,
                    url_override,
                    path_context,
                    is_first_page,
                    body_params,
                )
                .await
            }
            other => other,
        }
    }

    /// `true` when a shared provider declared `status` in its `reauth_statuses`.
    fn provider_wants_reauth(&self, status: u16) -> bool {
        self.auth_provider
            .as_ref()
            .is_some_and(|p| p.reauth_statuses().contains(&status))
    }

    /// `true` when this source resolves its bearer token from one of the inline
    /// time-cached auth modes (no shared provider) — the only case where a 401
    /// should trigger a cache invalidation + retry (F57).
    fn uses_inline_cached_token(&self) -> bool {
        self.auth_provider.is_none()
            && matches!(
                self.config.auth,
                AuthSpec::Inline(Auth::OAuth2 { .. })
                    | AuthSpec::Inline(Auth::TokenEndpoint { .. })
            )
    }

    /// Invalidate whichever inline token cache backs the current auth mode, so
    /// the next request fetches a fresh token (F57).
    async fn invalidate_inline_token_cache(&self) {
        match &self.config.auth {
            AuthSpec::Inline(Auth::OAuth2 { .. }) => self.token_cache.invalidate().await,
            AuthSpec::Inline(Auth::TokenEndpoint { .. }) => {
                self.token_endpoint_cache.invalidate().await
            }
            _ => {}
        }
    }

    /// Resolve the server-side push-down binding for this run:
    /// `(target, name, rendered-value)`. Returns `None` when no `replication_bind`
    /// is configured or there is no bookmark yet (first run — a full pull).
    async fn resolved_bind(&self) -> Result<Option<(BindTarget, String, String)>, FaucetError> {
        let Some(bind) = &self.config.replication_bind else {
            return Ok(None);
        };
        let bookmark = {
            let guard = self.runtime_start.lock().await;
            guard.clone()
        }
        .or_else(|| self.config.start_replication_value.clone());
        match bookmark {
            Some(bm) => Ok(Some((bind.into, bind.name.clone(), bind.render(&bm)?))),
            None => Ok(None),
        }
    }

    /// Build + send one job-lifecycle request (auth via `metadata_headers`,
    /// plus the connector's static headers and the request's own headers/query/
    /// json). Returns the raw response bytes; errors on non-2xx.
    async fn job_request_bytes(
        &self,
        op: &'static str,
        method: &str,
        url: &str,
        headers: &HashMap<String, String>,
        query: &HashMap<String, String>,
        json: Option<&Value>,
    ) -> Result<(Vec<u8>, HeaderMap), FaucetError> {
        // Retry the request *and* the body read as one unit — a transient 502
        // on poll #40 of a 30-minute bulk job must not discard the whole
        // submitted job, and a connection dropped mid-body is retried the same
        // way the pagination path retries a page. Same policy knobs as the
        // pagination runner.
        retry::execute_with_retry(
            self.retry_policy.max_attempts.saturating_sub(1),
            self.retry_policy.base,
            || async {
                let resp = self
                    .job_request_response_once(op, method, url, headers, query, json)
                    .await?;
                let resp_headers = resp.headers().clone();
                let bytes = resp.bytes().await.map_err(FaucetError::Http)?;
                Ok((bytes.to_vec(), resp_headers))
            },
        )
        .await
    }

    /// Send a fetch request and return the raw [`reqwest::Response`] with its body
    /// **unconsumed** — the caller reads headers (e.g. the locator header) and
    /// then streams the body. Retries the request itself under the shared
    /// policy; a failure *while streaming the body* is not retried here (the
    /// caller owns what has already been consumed downstream). Note a retried
    /// submit may orphan a job the server created before the failure — harmless
    /// (never fetched, expires server-side) and at-least-once-consistent.
    async fn job_request_response(
        &self,
        op: &'static str,
        method: &str,
        url: &str,
        headers: &HashMap<String, String>,
        query: &HashMap<String, String>,
        json: Option<&Value>,
    ) -> Result<reqwest::Response, FaucetError> {
        retry::execute_with_retry(
            self.retry_policy.max_attempts.saturating_sub(1),
            self.retry_policy.base,
            || self.job_request_response_once(op, method, url, headers, query, json),
        )
        .await
    }

    /// One unretried attempt — the shared request-building core of
    /// [`job_request_bytes`](Self::job_request_bytes) /
    /// [`job_request_response`](Self::job_request_response).
    async fn job_request_response_once(
        &self,
        op: &'static str,
        method: &str,
        url: &str,
        headers: &HashMap<String, String>,
        query: &HashMap<String, String>,
        json: Option<&Value>,
    ) -> Result<reqwest::Response, FaucetError> {
        let m = reqwest::Method::from_bytes(method.to_uppercase().as_bytes()).map_err(|_| {
            FaucetError::Config(format!("async_job: invalid HTTP method '{method}'"))
        })?;
        // Precedence: static config headers (base) < auth < this request's own
        // headers — so an auth header always wins over a same-named config one.
        let mut hdrs = self.static_headers.clone();
        for (k, v) in self.metadata_headers(url).await?.iter() {
            hdrs.insert(k.clone(), v.clone());
        }
        for (k, v) in headers {
            insert_header(&mut hdrs, k, v)?;
        }
        let mut req = self.client.request(m, url).headers(hdrs);
        if !query.is_empty() {
            let pairs: Vec<(&str, &str)> = query
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            req = req.query(&pairs);
        }
        if let Some(j) = json {
            req = req.json(j);
        }
        // Counted here, at the one unretried attempt, so a retried call counts
        // again — a retry is a real round trip against the API's quota (#638).
        self.roundtrip(op);
        // Transport errors stay typed (`FaucetError::Http`) so the shared retry
        // runner's `is_retriable` classification sees connect/timeout failures —
        // stringifying them into `Source(...)` would silently make every
        // transient network blip fatal to a 30-minute bulk job.
        let resp = req.send().await.map_err(FaucetError::Http)?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url: url.to_string(),
                body: format!("async_job: {url} returned HTTP {}", status.as_u16()),
            });
        }
        Ok(resp)
    }

    async fn job_request_json(
        &self,
        op: &'static str,
        method: &str,
        url: &str,
        headers: &HashMap<String, String>,
        query: &HashMap<String, String>,
        json: Option<&Value>,
    ) -> Result<Value, FaucetError> {
        let (bytes, _headers) = self
            .job_request_bytes(op, method, url, headers, query, json)
            .await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| FaucetError::Source(format!("async_job: {url} returned non-JSON: {e}")))
    }

    /// Run the async-job lifecycle up to resolving the fetch URL (#514): submit
    /// → poll-to-terminal → resolve `fetch.url` / `fetch.url_from`. The caller
    /// then fetches the (possibly locator-paged, #557) result and streams one
    /// [`faucet_core::StreamPage`] per locator page, rather than buffering the
    /// whole extract into a single page (#623).
    /// Incremental replication for the async-job path (#630): if replicating
    /// incrementally with a start bookmark (from the state store via
    /// `apply_start_bookmark`, else `start_replication_value`), return a clone of
    /// the submit body with `WHERE <replication_key> > <bookmark>` injected into
    /// its `query`. Returns `None` (→ unmodified submit, full export) on the first
    /// run, for full-table replication, or when there is no `query` to amend.
    async fn incremental_submit_json(
        &self,
        job: &crate::async_job::AsyncJobConfig,
    ) -> Option<Value> {
        if self.config.replication_method != ReplicationMethod::Incremental {
            return None;
        }
        let key = self.config.replication_key.as_ref()?;
        let start = {
            let guard = self.runtime_start.lock().await;
            guard
                .clone()
                .or_else(|| self.config.start_replication_value.clone())
        }?;
        let submit = job.submit.json.as_ref()?;
        let query = job.submit_query()?;
        let predicate = format!("{key} > {}", sql_literal(&start));
        let mut cloned = submit.clone();
        *cloned.pointer_mut(&job.query_path)? =
            Value::String(inject_sql_predicate(query, &predicate));
        Some(cloned)
    }

    /// The bookmark to persist after an incremental async-job run: the run's
    /// start time (RFC3339) **minus the `lookback` margin**. Using the start
    /// time (not `max(replication_key)` scraped from the rows) keeps this
    /// native/streaming-compatible — no row parsing; the subtracted margin
    /// makes it safe against a client clock running ahead of the server, which
    /// would otherwise permanently exclude records stamped in the skew gap
    /// from every future `key > bookmark` predicate. The cost is a bounded
    /// re-read overlap next run (deduped by an upsert sink). `None` for
    /// full-table replication, and `None` when the submit body has no
    /// amendable `query` — a bookmark that advances while every run stays a
    /// full export would be a lie (`validate()` rejects that config shape;
    /// this is the backstop for callers that skipped validation).
    fn async_job_new_bookmark(&self) -> Option<Value> {
        if self.config.replication_method != ReplicationMethod::Incremental
            || self.config.replication_key.is_none()
        {
            return None;
        }
        let job = self.config.async_job.as_ref()?;
        if !job.supports_incremental_query() {
            return None;
        }
        let now = self.now_override.unwrap_or_else(chrono::Utc::now) - job.lookback_duration();
        Some(Value::String(
            now.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        ))
    }

    async fn prepare_async_job(&self) -> Result<String, FaucetError> {
        use crate::async_job::{JobOutcome, resolve_url, substitute_job_id};
        let job = self
            .config
            .async_job
            .as_ref()
            .expect("run_async_job called with async_job set");
        let base = &self.config.base_url;

        // 1) Submit → capture the job id. Incremental (#630): inject a
        // `WHERE <replication_key> > <bookmark>` predicate into the submit SOQL
        // when replicating incrementally with a start bookmark; falls back to the
        // unmodified submit body on the first run / full-table.
        let submit_url = resolve_url(base, job.submit.url.as_deref().unwrap_or_default());
        let injected = self.incremental_submit_json(job).await;
        let submit_json = injected.as_ref().or(job.submit.json.as_ref());
        let submit_body = self
            .job_request_json(
                "submit",
                &job.submit.method,
                &submit_url,
                &job.submit.headers,
                &job.submit.query,
                submit_json,
            )
            .await?;
        let job_id = jsonpath_first_string(&submit_body, &job.job_id).ok_or_else(|| {
            FaucetError::Source(format!(
                "async_job: submit response had no job id at '{}'",
                job.job_id
            ))
        })?;

        // 2) Poll until a terminal state (with interval + timeout).
        let poll_url = resolve_url(base, &substitute_job_id(&job.poll.url, &job_id));
        let deadline =
            tokio::time::Instant::now() + std::time::Duration::from_secs(job.poll.timeout_secs);
        // Exponential poll backoff: start small so a job that finishes seconds
        // after submit is noticed in ~1s, doubling up to `interval_secs` (the
        // cap) so a long-running job doesn't hammer the API. `interval_secs` is
        // the ceiling, not a fixed wait — a fixed 15s made an instant job take
        // ~15s of dead poll-wait.
        let poll_cap = std::time::Duration::from_secs(job.poll.interval_secs);
        let mut poll_delay = std::cmp::min(
            std::time::Duration::from_secs(POLL_BACKOFF_BASE_SECS),
            poll_cap,
        );
        // Retain the last poll response so `fetch.url_from` (#543) can source the
        // download URL from the terminal (success) poll body.
        let last_poll_body: Value = loop {
            let body = self
                .job_request_json(
                    "poll",
                    &job.poll.method,
                    &poll_url,
                    &job.poll.headers,
                    &job.poll.query,
                    None,
                )
                .await?;
            let status = jsonpath_first_string(&body, &job.status.path).unwrap_or_default();
            match job.status.classify(&status) {
                JobOutcome::Success => break body,
                JobOutcome::Failure => {
                    return Err(FaucetError::Source(format!(
                        "async_job: job failed with status '{status}'"
                    )));
                }
                JobOutcome::Pending => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(FaucetError::Source(format!(
                            "async_job: polling timed out after {}s (last status '{status}')",
                            job.poll.timeout_secs
                        )));
                    }
                    tokio::time::sleep(poll_delay).await;
                    poll_delay = next_poll_delay(poll_delay, poll_cap);
                }
            }
        };

        // 3) Resolve the fetch URL (#543): from the poll body via `url_from`, or
        // by rendering the templated `url`. Exactly one is set (validated).
        let fetch_url = match (&job.fetch.url_from, &job.fetch.url) {
            (Some(path), _) => {
                let resolved = jsonpath_first_string(&last_poll_body, path).ok_or_else(|| {
                    FaucetError::Source(format!(
                        "async_job: fetch.url_from '{path}' matched no string in the poll response"
                    ))
                })?;
                resolve_url(base, &resolved)
            }
            (None, Some(url)) => resolve_url(base, &substitute_job_id(url, &job_id)),
            (None, None) => {
                return Err(FaucetError::Config(
                    "async_job: `fetch` requires exactly one of `url` or `url_from`".into(),
                ));
            }
        };

        // Steps 1-3 done; the caller fetches the (locator-paged) result and
        // streams it page-by-page. See `stream_pages_inner` (#623).
        Ok(fetch_url)
    }

    /// Parse one async-job fetch page into records, returning the parsed JSON
    /// body too (for `locator_body` extraction) when the result is JSON. Mirrors
    /// the single-fetch parsing: a `decode:` pipeline wins, else `response_format`
    /// (JSON honouring `fetch.records_path` or the source `records_path`).
    async fn parse_fetch_page(
        &self,
        bytes: &[u8],
        job: &crate::async_job::AsyncJobConfig,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        if !self.config.decode.is_empty() {
            let records = crate::decode::run_decode(bytes, &self.config.decode).await?;
            return Ok((records, None));
        }
        match self.config.response_format {
            crate::config::ResponseFormat::Json => {
                let v: Value = serde_json::from_slice(bytes).map_err(|e| {
                    FaucetError::Source(format!("async_job: result is not JSON: {e}"))
                })?;
                let records = match job.fetch.records_path.as_deref() {
                    Some(rp) => extract::extract_records(&v, Some(rp))?,
                    None => self.extract_page(&v)?,
                };
                Ok((records, Some(v)))
            }
            crate::config::ResponseFormat::Csv => {
                let records = crate::format::parse_csv_with(
                    bytes,
                    crate::format::CsvDialect {
                        delimiter: self.config.csv_delimiter,
                        has_headers: self.config.csv_has_headers,
                        quote: self.config.csv_quote,
                    },
                )
                .await?;
                Ok((records, None))
            }
            crate::config::ResponseFormat::Excel => {
                let records = crate::format::parse_excel(
                    bytes,
                    self.config.excel_sheet.as_deref(),
                    self.config.excel_header_row,
                )?;
                Ok((records, None))
            }
        }
    }

    /// Resolve auth headers for a non-paginated preflight request (OData
    /// `$metadata`). Applies a flow provider's header/cookie placements or its
    /// credential; else the inline auth (bearer via cache for OAuth2/token
    /// endpoint). Query/body placements and `ApiKeyQuery` are not applied here.
    async fn metadata_headers(&self, url: &str) -> Result<HeaderMap, FaucetError> {
        let mut headers = HeaderMap::new();
        if let Some(provider) = &self.auth_provider {
            let ra = provider
                .request_auth("GET", url, &std::collections::BTreeMap::new())
                .await?;
            if ra.is_empty() {
                credential_to_auth(provider.credential().await?).apply(&mut headers)?;
            } else {
                for p in ra.placements {
                    match p {
                        CredentialPlacement::Header { name, value } => {
                            insert_header(&mut headers, &name, &value)?
                        }
                        CredentialPlacement::Cookie { name, value } => {
                            insert_header(&mut headers, "Cookie", &format!("{name}={value}"))?
                        }
                        _ => {}
                    }
                }
            }
        } else {
            match &self.config.auth {
                AuthSpec::Inline(Auth::OAuth2 {
                    token_url,
                    client_id,
                    client_secret,
                    scopes,
                    expiry_ratio,
                }) => {
                    let token = self
                        .token_cache
                        .get_or_refresh(
                            &self.client,
                            token_url,
                            client_id,
                            client_secret,
                            scopes,
                            *expiry_ratio,
                        )
                        .await?;
                    Auth::Bearer { token }.apply(&mut headers)?;
                }
                AuthSpec::Inline(Auth::TokenEndpoint {
                    url: token_url,
                    method: token_method,
                    headers: token_headers,
                    body: token_body,
                    token_path,
                    expiry_path,
                    expiry_ratio,
                    encoding,
                    response_validator,
                }) => {
                    let token = self
                        .token_endpoint_cache
                        .get_or_refresh_with_encoding(
                            &self.client,
                            token_url,
                            token_method,
                            token_headers,
                            token_body.as_ref(),
                            token_path,
                            expiry_path.as_deref(),
                            *expiry_ratio,
                            *encoding,
                            response_validator.as_ref(),
                        )
                        .await?;
                    Auth::Bearer { token }.apply(&mut headers)?;
                }
                AuthSpec::Inline(other) => other.apply(&mut headers)?,
                AuthSpec::Reference(_) => {}
            }
        }
        Ok(headers)
    }

    /// Execute a single HTTP request and return the response body and headers.
    ///
    /// - When `url_override` is `Some`, that full URL is used and query params
    ///   are **not** appended (Link header pagination encodes them in the URL).
    /// - When `path_context` is `Some`, `{key}` placeholders in `config.path`
    ///   are substituted with values from the context map (partition support).
    async fn execute_request_once(
        &self,
        params: &HashMap<String, String>,
        url_override: Option<&str>,
        path_context: Option<&HashMap<String, Value>>,
        is_first_page: bool,
        body_params: &[(String, Value)],
    ) -> Result<(Value, HeaderMap), FaucetError> {
        let use_override = url_override.is_some();

        // #513 server-side push-down + #527 window slicing: the outgoing request
        // carries the bookmark binding (0 or 1) plus the current window's rendered
        // lower/upper bounds (0 or 2). They apply at the same four placement sites.
        let mut binds: Vec<(BindTarget, String, String)> = Vec::new();
        if let Some(b) = self.resolved_bind().await? {
            binds.push(b);
        }
        binds.extend(self.window_binds.lock().await.iter().cloned());

        let query_btree: std::collections::BTreeMap<String, String> =
            params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        // #511 rich per-request auth: a flow provider may place credentials
        // across header/query/cookie/body and override the base-URL for this
        // session. When it contributes anything, it supersedes the plain
        // credential()/sign_request() path below.
        let mut base_url = self.config.base_url.clone();
        let mut ra_headers: Vec<(String, String)> = Vec::new();
        let mut ra_query: Vec<(String, String)> = Vec::new();
        let mut ra_cookies: Vec<(String, String)> = Vec::new();
        let mut ra_body: Vec<(String, String)> = Vec::new();
        let mut captured: std::collections::BTreeMap<String, String> =
            std::collections::BTreeMap::new();
        let mut used_request_auth = false;
        if let Some(provider) = &self.auth_provider {
            let ra = provider
                .request_auth(self.config.method.as_str(), &base_url, &query_btree)
                .await?;
            if !ra.is_empty() {
                used_request_auth = true;
                if let Some(b) = ra.base_url {
                    base_url = b;
                }
                captured = ra.captured;
                for p in ra.placements {
                    match p {
                        CredentialPlacement::Header { name, value } => {
                            ra_headers.push((name, value))
                        }
                        CredentialPlacement::Query { name, value } => ra_query.push((name, value)),
                        CredentialPlacement::Cookie { name, value } => {
                            ra_cookies.push((name, value))
                        }
                        CredentialPlacement::BodyField { name, value } => {
                            ra_body.push((name, value))
                        }
                        _ => {}
                    }
                }
            }
        }

        // Build the URL (honouring any dynamic base-URL) and apply a `path`-target
        // push-down binding.
        let mut url = match url_override {
            Some(u) => u.to_string(),
            None => {
                let path = match path_context {
                    Some(ctx) => faucet_core::util::substitute_context(&self.config.path, ctx),
                    None => self.config.path.clone(),
                };
                format!("{}/{}", base_url, path.trim_start_matches('/'))
            }
        };
        for (target, name, rendered) in &binds {
            if *target == BindTarget::Path {
                url = url.replace(&format!("{{{name}}}"), rendered);
            }
        }
        // #567: substitute flow-captured `${name}` values into the URL (a
        // captured session id in the path, say). No-op when nothing was captured.
        url = substitute_captured(&url, &captured);

        // Resolve inline / signed credentials — unless a flow provider already
        // supplied the request auth. A shared provider (from `auth: { ref }` or
        // a library caller) takes precedence over inline; inline OAuth2 /
        // TokenEndpoint resolve to a Bearer token via the per-source cache.
        let resolved_auth: Option<Auth> = if used_request_auth {
            None
        } else if let Some(provider) = &self.auth_provider {
            // A per-request signer (OAuth1, #496) signs this exact method + URL +
            // query; every other provider returns `None` here and we apply its
            // reusable credential.
            let cred = match provider
                .sign_request(self.config.method.as_str(), &url, &query_btree)
                .await?
            {
                Some(cred) => cred,
                None => provider.credential().await?,
            };
            Some(credential_to_auth(cred))
        } else {
            match &self.config.auth {
                AuthSpec::Inline(Auth::OAuth2 {
                    token_url,
                    client_id,
                    client_secret,
                    scopes,
                    expiry_ratio,
                }) => {
                    let token = self
                        .token_cache
                        .get_or_refresh(
                            &self.client,
                            token_url,
                            client_id,
                            client_secret,
                            scopes,
                            *expiry_ratio,
                        )
                        .await?;
                    Some(Auth::Bearer { token })
                }
                AuthSpec::Inline(Auth::TokenEndpoint {
                    url: token_url,
                    method: token_method,
                    headers: token_headers,
                    body: token_body,
                    token_path,
                    expiry_path,
                    expiry_ratio,
                    encoding,
                    response_validator,
                }) => {
                    let token = self
                        .token_endpoint_cache
                        .get_or_refresh_with_encoding(
                            &self.client,
                            token_url,
                            token_method,
                            token_headers,
                            token_body.as_ref(),
                            token_path,
                            expiry_path.as_deref(),
                            *expiry_ratio,
                            *encoding,
                            response_validator.as_ref(),
                        )
                        .await?;
                    Some(Auth::Bearer { token })
                }
                AuthSpec::Inline(other) => Some(other.clone()),
                AuthSpec::Reference(r) => {
                    return Err(FaucetError::Auth(format!(
                        "auth references provider '{}' but no provider was supplied; \
                         set one via the CLI `auth:` catalog or `with_auth_provider`",
                        r.name
                    )));
                }
            }
        };

        // Static config headers form the base; auth (inline or provider) is
        // applied on top so an auth header of the same name wins (#539). A
        // flow-captured `${name}` in a header value is substituted per request
        // (#567); the map is empty (and this a plain clone) for non-flow auth.
        let mut headers = if captured.is_empty() {
            self.static_headers.clone()
        } else {
            let mut h = HeaderMap::new();
            for (name, value) in self.static_headers.iter() {
                let sv = substitute_captured(value.to_str().unwrap_or_default(), &captured);
                let hv =
                    reqwest::header::HeaderValue::from_str(&sv).unwrap_or_else(|_| value.clone());
                h.insert(name.clone(), hv);
            }
            h
        };
        if let Some(auth) = &resolved_auth {
            auth.apply(&mut headers)?;
        }
        // #511 header + cookie placements from the flow provider.
        for (name, value) in &ra_headers {
            insert_header(&mut headers, name, value)?;
        }
        if !ra_cookies.is_empty() {
            let cookie = ra_cookies
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("; ");
            insert_header(&mut headers, "Cookie", &cookie)?;
        }
        // #513/#527 header-target bindings.
        for (target, name, rendered) in &binds {
            if *target == BindTarget::Header {
                insert_header(&mut headers, name, rendered)?;
            }
        }

        let mut req = self
            .client
            .request(self.config.method.clone(), &url)
            .headers(headers);

        if !use_override {
            // When parent context is available, substitute {placeholders} in
            // query param values so child sources can be parameterised.
            if let Some(ctx) = path_context {
                let substituted: HashMap<String, String> = params
                    .iter()
                    .map(|(k, v)| (k.clone(), faucet_core::util::substitute_context(v, ctx)))
                    .collect();
                req = req.query(&substituted.iter().collect::<Vec<_>>());
            } else {
                req = req.query(params);
            }
            // #536: repeated / array-valued query params, rendered as repeated
            // keys (`?k=a&k=b`). reqwest's `.query()` appends, so this composes
            // with the scalar params above.
            if !self.config.query_params_multi.is_empty() {
                let pairs: Vec<(String, String)> = self
                    .config
                    .query_params_multi
                    .iter()
                    .flat_map(|(k, vals)| {
                        vals.iter().map(move |v| {
                            let rendered = match path_context {
                                Some(ctx) => faucet_core::util::substitute_context(v, ctx),
                                None => v.clone(),
                            };
                            (k.clone(), rendered)
                        })
                    })
                    .collect();
                req = req.query(
                    &pairs
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect::<Vec<_>>(),
                );
            }
        }
        // #511 query placements from the flow provider.
        if !ra_query.is_empty() {
            let pairs: Vec<(&str, &str)> = ra_query
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            req = req.query(&pairs);
        }
        // #513/#527 query-target bindings.
        for (target, name, rendered) in &binds {
            if *target == BindTarget::Query {
                req = req.query(&[(name.as_str(), rendered.as_str())]);
            }
        }

        // ApiKeyQuery: inject the API key as a query parameter.
        if let AuthSpec::Inline(Auth::ApiKeyQuery { param, value }) = &self.config.auth {
            req = req.query(&[(param.as_str(), value.as_str())]);
        }

        // Build the request JSON body, if any. Substitute context into body
        // string values when available. Use the JSON-safe variant:
        // `substitute_context` does NOT escape the value, so a context value
        // carrying a JSON metacharacter (`"`, `\`, newline) corrupts the
        // serialized body — the old `unwrap_or(Value::String(..))` fallback then
        // silently coerced the whole object into a bare string and POSTed garbage
        // (audit #321 H7). `substitute_context_json` JSON-escapes string values;
        // an un-parseable result is now a hard error rather than a silently-wrong
        // payload.
        let mut body_value: Option<Value> = match &self.config.body {
            Some(body) => match path_context {
                Some(ctx) => {
                    let body_str = body.to_string();
                    let substituted = faucet_core::util::substitute_context_json(&body_str, ctx);
                    let substituted_value: Value =
                        serde_json::from_str(&substituted).map_err(|e| {
                            FaucetError::Source(format!(
                                "REST source: context substitution produced an invalid JSON body: {e}"
                            ))
                        })?;
                    Some(substituted_value)
                }
                None => Some(body.clone()),
            },
            None => None,
        };
        // Body-carrying pagination (CursorInBody / OffsetInBody / RecordFieldCursor
        // with `into: body`): inject the pagination fields into the request body.
        // If no base body was configured, start from an empty object so the
        // fields still land somewhere.
        if !body_params.is_empty() {
            let obj = body_value.get_or_insert_with(|| Value::Object(serde_json::Map::new()));
            match obj.as_object_mut() {
                Some(map) => {
                    for (field, value) in body_params {
                        map.insert(field.clone(), value.clone());
                    }
                }
                None => {
                    return Err(FaucetError::Source(
                        "REST source: body-carrying pagination requires a JSON object request \
                         body to inject the pagination fields into"
                            .into(),
                    ));
                }
            }
        }
        // #511 body-field placements + #513/#527 body-target bindings.
        let has_body_bind = binds.iter().any(|(t, _, _)| *t == BindTarget::Body);
        if !ra_body.is_empty() || has_body_bind {
            let obj = body_value.get_or_insert_with(|| Value::Object(serde_json::Map::new()));
            match obj.as_object_mut() {
                Some(map) => {
                    for (name, value) in &ra_body {
                        map.insert(name.clone(), Value::String(value.clone()));
                    }
                    for (target, name, rendered) in &binds {
                        if *target == BindTarget::Body {
                            map.insert(name.clone(), Value::String(rendered.clone()));
                        }
                    }
                }
                None => {
                    return Err(FaucetError::Source(
                        "REST source: a body-target auth/replication binding requires a JSON \
                         object request body"
                            .into(),
                    ));
                }
            }
        }
        if let Some(body) = &body_value {
            req = req.json(body);
        }

        // One data-page request against the API (#638). Counted per attempt,
        // so a 429-then-retry counts twice — it consumed two calls of quota.
        self.roundtrip("page");
        let resp = req.send().await?;
        let status = resp.status();

        // 429 Too Many Requests: honour Retry-After before retrying.
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let wait = parse_retry_after(resp.headers());
            return Err(FaucetError::RateLimited(wait));
        }

        // Tolerated errors: treat as an empty page ONLY on the first request,
        // where they legitimately mean "this resource is absent/empty". Mid-
        // pagination, an empty page makes every pagination style read "last
        // page" and stop, silently dropping every remaining page as a
        // "successful" run (#78/#7). There we fall through to the real error
        // path: the retry executor retries 5xx, and a persistent error fails
        // loudly instead of truncating the stream.
        if is_first_page && self.config.tolerated_http_errors.contains(&status.as_u16()) {
            tracing::debug!(
                status = status.as_u16(),
                "tolerated HTTP error on first request; treating as empty page"
            );
            return Ok((Value::Array(vec![]), HeaderMap::new()));
        }
        if !is_first_page && self.config.tolerated_http_errors.contains(&status.as_u16()) {
            tracing::warn!(
                status = status.as_u16(),
                "tolerated HTTP error mid-pagination; surfacing as an error to avoid \
                 silently truncating the stream"
            );
        }

        // For non-success responses, capture the body for debugging before
        // returning the error. This gives callers (and logs) the server's
        // error message rather than just a status code.
        if !status.is_success() {
            // Redact any auth secret carried in the query string before it lands
            // in the error (which renders the URL in `Display` → logs). The
            // `api_key_query` value is user-configured, so it is not marked
            // sensitive like a Bearer/Basic header and would otherwise leak on
            // any 4xx/5xx (audit #321 L2).
            let resp_url = redact_error_url(resp.url(), &self.config.auth);
            let body_text = resp.text().await.unwrap_or_default();
            // Truncate very long error bodies to avoid bloating logs/errors.
            let truncated = if body_text.len() > 1024 {
                // Find a safe UTF-8 boundary at or before 1024 bytes.
                let end = body_text.floor_char_boundary(1024);
                format!("{}...(truncated)", &body_text[..end])
            } else {
                body_text
            };
            return Err(FaucetError::HttpStatus {
                status: status.as_u16(),
                url: resp_url,
                body: truncated,
            });
        }

        let resp_headers = resp.headers().clone();

        // A 204 No Content — or any 2xx with an empty / whitespace-only body —
        // carries no JSON to parse. `resp.json()` on such a response yields a
        // non-retriable decode error ("EOF while parsing a value") that aborts
        // the run; treat it as an empty page ("no data") instead (#146 M10). A
        // non-empty body that isn't valid JSON still surfaces as a parse error.
        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok((Value::Array(vec![]), resp_headers));
        }
        let bytes = resp.bytes().await?;
        if bytes.iter().all(u8::is_ascii_whitespace) {
            return Ok((Value::Array(vec![]), resp_headers));
        }
        // A `decode:` pipeline (#515) takes the raw body and produces records
        // directly (extract → base64 → gunzip/unzip → parse). It replaces the
        // `response_format` parsing; `validate()` guarantees pagination is
        // `none`. The records land as an array the downstream
        // (records_path-less) extraction passes straight through.
        if !self.config.decode.is_empty() {
            let records = crate::decode::run_decode(&bytes, &self.config.decode).await?;
            return Ok((Value::Array(records), resp_headers));
        }
        // For file response formats the whole body is a tabular file — parse it
        // into a record array here so the downstream (records_path-less)
        // extraction passes it straight through. `validate()` guarantees
        // pagination is `none`, so a single response is fetched.
        let body: Value = match self.config.response_format {
            crate::config::ResponseFormat::Json => serde_json::from_slice(&bytes)?,
            crate::config::ResponseFormat::Csv => Value::Array(
                crate::format::parse_csv_with(
                    &bytes,
                    crate::format::CsvDialect {
                        delimiter: self.config.csv_delimiter,
                        has_headers: self.config.csv_has_headers,
                        quote: self.config.csv_quote,
                    },
                )
                .await?,
            ),
            crate::config::ResponseFormat::Excel => Value::Array(crate::format::parse_excel(
                &bytes,
                self.config.excel_sheet.as_deref(),
                self.config.excel_header_row,
            )?),
        };
        Ok((body, resp_headers))
    }
}

/// Render a response URL for an error message with any auth secret in the query
/// string redacted (audit #321 L2). Redacts the user-configured
/// `api_key_query` parameter by name (which `redact_uri_credentials` cannot
/// know), then applies the shared credential/query-secret redaction for the
/// common key names and any URL userinfo.
fn redact_error_url(url: &reqwest::Url, auth: &AuthSpec<Auth>) -> String {
    let mut redacted = url.clone();
    if let AuthSpec::Inline(Auth::ApiKeyQuery { param, .. }) = auth {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| {
                if k == param.as_str() {
                    (k.into_owned(), "***".to_string())
                } else {
                    (k.into_owned(), v.into_owned())
                }
            })
            .collect();
        redacted.set_query(None);
        if !pairs.is_empty() {
            let mut qp = redacted.query_pairs_mut();
            for (k, v) in &pairs {
                qp.append_pair(k, v);
            }
        }
    }
    faucet_core::redact_uri_credentials(redacted.as_str())
}

/// Parse the `Retry-After` header. RFC 7231 permits **either** delta-seconds
/// **or** an HTTP-date; we honour both. An HTTP-date in the past yields a zero
/// wait (retry now). Falls back to 60 s only when the header is absent or in
/// neither form.
fn parse_retry_after(headers: &HeaderMap) -> Duration {
    const DEFAULT: Duration = Duration::from_secs(60);
    let Some(raw) = headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
    else {
        return DEFAULT;
    };
    // delta-seconds form.
    if let Ok(secs) = raw.parse::<u64>() {
        return Duration::from_secs(secs);
    }
    // HTTP-date form (IMF-fixdate / RFC 850 / asctime).
    if let Ok(when) = httpdate::parse_http_date(raw) {
        return when
            .duration_since(std::time::SystemTime::now())
            .unwrap_or(Duration::ZERO);
    }
    DEFAULT
}

/// Keep the larger of two bookmark values when consolidating per-partition
/// bookmarks in [`Source::stream_pages`] (#535). Numbers compare numerically,
/// strings lexicographically (the usual timestamp/id bookmark shapes); any
/// other or heterogeneous pair prefers the newer value.
fn value_max(current: Option<Value>, candidate: Value) -> Option<Value> {
    match current {
        None => Some(candidate),
        Some(cur) => {
            let take_candidate = match (&cur, &candidate) {
                (Value::Number(a), Value::Number(b)) => {
                    b.as_f64().unwrap_or(f64::MIN) > a.as_f64().unwrap_or(f64::MIN)
                }
                (Value::String(a), Value::String(b)) => b > a,
                _ => true,
            };
            Some(if take_candidate { candidate } else { cur })
        }
    }
}

#[async_trait]
impl faucet_core::Source for RestStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            // No parent context — use normal fetch_all with partitions
            RestStream::fetch_all(self).await
        } else if self.config.partitions.is_empty() {
            // Parent context, no partitions — use context directly as partition context
            self.fetch_partition(Some(context), None).await
        } else {
            // Both parent context and partitions — merge context into each partition
            let mut all_records = Vec::new();
            for partition in &self.config.partitions {
                let mut merged = context.clone();
                merged.extend(partition.iter().map(|(k, v)| (k.clone(), v.clone())));
                all_records.extend(self.fetch_partition(Some(&merged), None).await?);
            }
            Ok(all_records)
        }
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let records = self.fetch_with_context(context).await?;
        let bookmark = self
            .config
            .replication_key
            .as_deref()
            .and_then(|key| faucet_core::replication::max_replication_value(&records, key))
            .cloned();
        Ok((records, bookmark))
    }

    fn connector_name(&self) -> &'static str {
        "rest"
    }

    fn set_roundtrip_recorder(&self, recorder: Arc<faucet_core::observability::RoundtripRecorder>) {
        // First install wins; the pipeline installs exactly once per run, and
        // ignoring a second call keeps a re-used source instance from losing
        // the labels it is already counting under.
        let _ = self.roundtrips.set(recorder);
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(RestStreamConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let base = format!(
            "{}{}",
            faucet_core::redact_uri_credentials(&self.config.base_url),
            self.config.path
        );
        // Async-job sources (Salesforce Bulk etc.) address every object through the
        // *same* endpoint — the object lives in the SOQL query body, not the URL. So
        // without this, a 21-object matrix collapses to one catalog/lineage dataset
        // (#640). Derive a per-object URI from the query: the `FROM <SObject>` when
        // parseable, else a stable hash of the submit body (distinct query → distinct
        // dataset either way).
        if let Some(job) = &self.config.async_job {
            let sep = if base.ends_with('/') { "" } else { "/" };
            if let Some(obj) = async_job_object(job.submit.json.as_ref(), &job.query_path) {
                return format!("{base}{sep}objects/{obj}");
            }
            if let Some(j) = &job.submit.json {
                // FNV-1a from core (stable across Rust releases — this feeds persisted
                // catalog/lineage identity, so `DefaultHasher` would re-key it on a
                // toolchain bump).
                return format!(
                    "{base}{sep}job/{:016x}",
                    faucet_core::shard::shard_hash(&j.to_string())
                );
            }
        }
        base
    }

    fn state_key(&self) -> Option<String> {
        // A source is only made resumable (executor wraps it + persists the
        // bookmark) when it reports a state key. Incremental replication (#630)
        // is meaningless without persistence, so opt in automatically when
        // replicating incrementally — otherwise `replication_method: incremental`
        // + a `state:` block would silently full-refresh every run. The CLI
        // executor overrides the concrete key per invocation (StateKeyOverride);
        // the fallback below matters for library callers driving `Pipeline`
        // directly, so it derives from the dataset identity — two REST sources
        // sharing one StateStore must never collide on a fixed literal and
        // resume from each other's bookmark. FNV-1a from core keeps the derived
        // key stable across Rust releases.
        self.config.state_key.clone().or_else(|| {
            (self.config.replication_method == ReplicationMethod::Incremental
                && self.config.replication_key.is_some())
            .then(|| {
                format!(
                    "rest:{:016x}",
                    faucet_core::shard::shard_hash(&faucet_core::Source::dataset_uri(self))
                )
            })
        })
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::StreamPage, FaucetError>> + Send + 'a>> {
        // RestStream chunks by upstream-API page boundaries for the HTTP-paged
        // modes. The hint *is* honoured by the bounded-memory CSV path (#626),
        // where there is no upstream page to chunk on and the whole result set
        // would otherwise land in one page.
        //
        // Key-range partitioning (#479): when an integer PK is configured, tile the
        // key space and stream the tiles concurrently (the fix for a huge OData
        // entity whose sequential `@odata.nextLink` paging dominates the run).
        if let Some(key) = self
            .config
            .odata
            .as_ref()
            .and_then(|o| o.partition.as_ref())
            .and_then(|p| p.key.clone())
        {
            return self.stream_key_range_partitions(context, key, batch_size);
        }
        // Partition fan-out (#535): when `partitions` are configured the stream
        // must run once per partition — mirroring `fetch_all` / `fetch_with_context`
        // — or every partition's records are silently dropped under `faucet run`
        // (the pipeline drives this method). Any parent `context` is merged into
        // each partition context, exactly as `fetch_with_context` does.
        if self.config.partitions.is_empty() {
            return self.stream_pages_inner(Some(context), None, batch_size);
        }
        let contexts: Vec<HashMap<String, Value>> = self
            .config
            .partitions
            .iter()
            .map(|p| {
                let mut merged = context.clone();
                merged.extend(p.iter().map(|(k, v)| (k.clone(), v.clone())));
                merged
            })
            .collect();
        // `partition_concurrency` used to be honoured only by the buffering
        // `fetch_all`, so the documented knob did nothing on the path the
        // pipeline actually drives (#624 — the same no-op class as the
        // object-store `concurrency` in #619). `1`, `0` and `None` all mean
        // "one at a time", which is the pre-#624 behaviour.
        let concurrency = self.config.partition_concurrency.unwrap_or(1).max(1);
        Box::pin(async_stream::try_stream! {
            // Per-partition streams each emit their own final bookmark; we
            // suppress those and emit a single consolidated (max) bookmark after
            // the last partition, so the persisted state is the global high-water
            // mark rather than whichever partition happened to finish last.
            let mut max_bookmark: Option<Value> = None;
            if concurrency <= 1 {
                for ctx in &contexts {
                    let mut inner = self.stream_pages_inner(Some(ctx), None, batch_size);
                    loop {
                        let page = std::future::poll_fn(|cx| inner.as_mut().poll_next(cx)).await;
                        match page {
                            Some(Ok(p)) => {
                                if let Some(bm) = p.bookmark {
                                    max_bookmark = value_max(max_bookmark.take(), bm);
                                    yield faucet_core::StreamPage { records: p.records, bookmark: None };
                                } else {
                                    yield p;
                                }
                            }
                            Some(Err(e)) => Err(e)?,
                            None => break,
                        }
                    }
                }
            } else {
                // Interleaved, not ordered: `flatten_unordered` polls up to
                // `concurrency` partition streams at once, so pages arrive as
                // each partition produces them. Partitions are disjoint by
                // construction and the consolidated bookmark is a max over all
                // of them, so interleaving changes throughput, not the
                // persisted position — but a downstream that assumed
                // partition-at-a-time page order no longer gets it.
                use futures::StreamExt as _;
                // Built eagerly into a `Vec` rather than mapped lazily: a
                // closure returning a borrow-capturing stream is not
                // higher-ranked enough for the combinator, and forcing it
                // here costs one allocation per partition.
                let mut inners = Vec::with_capacity(contexts.len());
                for ctx in &contexts {
                    inners.push(self.stream_pages_inner(Some(ctx), None, batch_size));
                }
                let mut merged =
                    futures::stream::iter(inners).flatten_unordered(Some(concurrency));
                while let Some(page) = merged.next().await {
                    let p = page?;
                    if let Some(bm) = p.bookmark {
                        max_bookmark = value_max(max_bookmark.take(), bm);
                        yield faucet_core::StreamPage { records: p.records, bookmark: None };
                    } else {
                        yield p;
                    }
                }
            }
            if max_bookmark.is_some() {
                yield faucet_core::StreamPage { records: Vec::new(), bookmark: max_bookmark };
            }
        })
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.runtime_start.lock().await = Some(bookmark);
        Ok(())
    }

    /// Native byte-passthrough (#633): an `async_job` (Salesforce Bulk-style)
    /// source whose fetch pages are CSV can stream straight to a byte-loading sink
    /// (e.g. BigQuery's load job) as NDJSON, never building `Vec<Value>`. Advertised
    /// only for the CSV async-job path with no custom `decode` (JSON async jobs and
    /// the paginated non-job path keep the `Value` path). Emits `NdJson` — the
    /// converted bytes are identical to the `Value` path's, preserving the
    /// destination's autodetected schema (see [`crate::format::csv_to_ndjson`]).
    fn native_output_formats(&self) -> &'static [faucet_core::NativeFormat] {
        let csv_async_job = self.config.async_job.is_some()
            && self.config.response_format == crate::config::ResponseFormat::Csv
            && self.config.decode.is_empty();
        if csv_async_job {
            &[faucet_core::NativeFormat::NdJson]
        } else {
            &[]
        }
    }

    fn stream_native<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        format: faucet_core::NativeFormat,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::NativeBatch, FaucetError>> + Send + 'a>>
    {
        Box::pin(async_stream::try_stream! {
            let job = self.config.async_job.as_ref().ok_or_else(|| {
                FaucetError::Source(
                    "rest: stream_native invoked without an async_job config".into(),
                )
            })?;
            if format != faucet_core::NativeFormat::NdJson {
                Err(FaucetError::Source(format!(
                    "rest: stream_native only emits NdJson, got {format:?}"
                )))?;
            }
            // Mirror the async-job locator loop from `stream_pages_inner`, but
            // **stream** each CSV page's response body straight into the CSV→NDJSON
            // converter and emit a `NativePayload::Stream` — the full page is never
            // buffered on either side (source or sink), so peak memory is O(one
            // ~256 KiB chunk), independent of page/row count (#633).
            use futures::TryStreamExt as _;
            // Incremental bookmark (#630) captured before submit — same semantics
            // as the Value path; emitted on a final empty batch below.
            let new_bookmark = self.async_job_new_bookmark();
            let fetch_url = self.prepare_async_job().await?;
            let mut locator: Option<String> = None;
            loop {
                let mut query = job.fetch.query.clone();
                if let (Some(loc), Some(param)) = (&locator, &job.fetch.locator_param) {
                    query.insert(param.clone(), loc.clone());
                }
                let resp = self
                    .job_request_response(
                        "fetch",
                        &job.fetch.method,
                        &fetch_url,
                        &job.fetch.headers,
                        &query,
                        job.fetch.json.as_ref(),
                    )
                    .await?;
                // Read the locator from headers *before* the body is consumed.
                let resp_headers = resp.headers().clone();
                let delimiter = self.config.csv_delimiter;
                let has_headers = self.config.csv_has_headers;
                // reqwest (tokio) byte stream → AsyncRead → futures AsyncRead (compat)
                // → the CSV→NDJSON chunk stream. Owns `resp`, so it is `'static`.
                let body = resp
                    .bytes_stream()
                    .map_err(std::io::Error::other);
                let reader = tokio_util::io::StreamReader::new(body);
                let ndjson_chunks =
                    crate::format::csv_reader_to_ndjson_stream(reader, delimiter, has_headers);
                yield faucet_core::NativeBatch {
                    format: faucet_core::NativeFormat::NdJson,
                    payload: faucet_core::NativePayload::Stream(Box::pin(ndjson_chunks)),
                    csv: faucet_core::CsvDialect { has_header: has_headers, delimiter },
                    records: None,
                    bookmark: None,
                };

                // Per-page bookmark stays `None`; the incremental bookmark (#630)
                // is emitted once at the end. Advance the locator (same guard as
                // the Value path).
                let next = next_locator(&resp_headers, None, job);
                match next {
                    Some(loc) if locator.as_deref() != Some(loc.as_str()) => {
                        locator = Some(loc);
                    }
                    _ => break,
                }
            }
            // Incremental (#630): final empty batch carrying the run-start bookmark
            // (load_native no-ops on empty bytes, then the pipeline flushes the
            // session + persists this bookmark). Native/streaming-compatible.
            if let Some(bm) = new_bookmark {
                yield faucet_core::NativeBatch::bytes(faucet_core::NativeFormat::NdJson, Vec::new())
                    .with_bookmark(Some(bm));
            }
        })
    }

    /// Arrow-columnar streaming (#635): the same CSV `async_job` shape the
    /// native byte path serves, but emitted as Arrow `RecordBatch`es so a
    /// columnar sink (BigQuery's Parquet load) receives typed columns instead
    /// of bytes it must re-parse.
    ///
    /// Gated on exactly the conditions under which a Bulk page can be decoded
    /// row-at-a-time: an `async_job`, `response_format: csv`, no custom
    /// `decode` chain, and the locator in a **header** — a body locator would
    /// need the parsed document this path deliberately never materializes.
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        self.config.async_job.is_some()
            && self.config.response_format == crate::config::ResponseFormat::Csv
            && self.config.decode.is_empty()
            && self
                .config
                .async_job
                .as_ref()
                .is_some_and(|j| j.fetch.locator_body.is_none())
    }

    #[cfg(feature = "arrow")]
    fn stream_batches<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<faucet_core::columnar::ColumnarPage, FaucetError>> + Send + 'a,
        >,
    > {
        Box::pin(async_stream::try_stream! {
            let job = self.config.async_job.as_ref().ok_or_else(|| {
                FaucetError::Source(
                    "rest: stream_batches invoked without an async_job config".into(),
                )
            })?;
            // Mirrors the locator loop in `stream_native` / `stream_pages_inner`;
            // only the per-page decoder differs.
            use futures::TryStreamExt as _;
            let new_bookmark = self.async_job_new_bookmark();
            let fetch_url = self.prepare_async_job().await?;
            let mut locator: Option<String> = None;
            // `batch_size = 0` is the documented "no batching" sentinel: one
            // batch per locator page.
            let rows_per_batch = batch_size;
            loop {
                let mut query = job.fetch.query.clone();
                if let (Some(loc), Some(param)) = (&locator, &job.fetch.locator_param) {
                    query.insert(param.clone(), loc.clone());
                }
                let resp = self
                    .job_request_response(
                        "fetch",
                        &job.fetch.method,
                        &fetch_url,
                        &job.fetch.headers,
                        &query,
                        job.fetch.json.as_ref(),
                    )
                    .await?;
                // Read the locator from headers *before* the body is consumed.
                let resp_headers = resp.headers().clone();
                let delimiter = self.config.csv_delimiter;
                let has_headers = self.config.csv_has_headers;
                let quote = self.config.csv_quote;
                let body = resp.bytes_stream().map_err(std::io::Error::other);
                let reader = tokio_util::io::StreamReader::new(body);
                let batches = crate::format::csv_reader_to_record_batches_with(
                    reader,
                    crate::format::CsvDialect { delimiter, has_headers, quote },
                    rows_per_batch,
                );
                futures::pin_mut!(batches);
                use futures::StreamExt as _;
                while let Some(batch) = batches.next().await {
                    // Per-page bookmark stays `None`; the incremental bookmark
                    // (#630) is emitted once at the end, as on every other path.
                    yield faucet_core::columnar::ColumnarPage::new(batch?, None);
                }
                // A header-only page yields no batch at all — unlike the `Value`
                // path there is no empty `RecordBatch` to emit without inventing
                // a schema, and a zero-row batch would give the sink a schema the
                // data never had. The locator loop below still advances.

                let next = next_locator(&resp_headers, None, job);
                match next {
                    Some(loc) if locator.as_deref() != Some(loc.as_str()) => {
                        locator = Some(loc);
                    }
                    _ => break,
                }
            }
            // Incremental (#630): a final zero-row batch carrying the run-start
            // bookmark, so the pipeline checkpoints exactly as on the other paths.
            if let Some(bm) = new_bookmark {
                let schema = std::sync::Arc::new(arrow::datatypes::Schema::empty());
                let empty = arrow::record_batch::RecordBatch::new_empty(schema);
                yield faucet_core::columnar::ColumnarPage::new(empty, Some(bm));
            }
        })
    }

    fn supports_discover(&self) -> bool {
        // A generic `discovery:` recipe or an OData `$metadata` catalog. A plain
        // REST API has neither.
        self.config.discovery.is_some() || self.config.odata.is_some()
    }

    async fn discover(&self) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        if let Some(spec) = &self.config.discovery {
            return self.discover_via_recipe(spec).await;
        }
        let Some(odata) = self.config.odata.as_ref() else {
            return Err(FaucetError::Source(
                "rest: discovery needs a `discovery:` recipe (config-driven API calls) or an \
                 `odata:` block (OData `$metadata`)"
                    .into(),
            ));
        };
        // Explicit object list (run-time fan-out): one descriptor per entity, its
        // sink `table_id` rendered from `emit.table_id` (default `${name_snake}`).
        // Type each entity's columns from the service `$metadata` (authoritative
        // EDM types → the sink declares the real column types instead of
        // autodetecting and rejecting a row that doesn't fit the guessed type).
        // `$metadata` is a single monolithic CSDL for the whole service, so it
        // cannot be scoped to the synced entities and some services regenerate
        // tens of MB on every call — hence it is fetched once per source instance
        // (see `metadata_xml`); only the requested entities are parsed out. On any
        // `$metadata` failure, fall back to untyped descriptors (sink autodetect)
        // so the run proceeds.
        if !odata.objects.is_empty() {
            let table_template = odata
                .emit
                .as_ref()
                .and_then(|e| e.table_id.as_deref())
                .unwrap_or("${name_snake}");
            let xml = match self.metadata_xml().await {
                Ok(xml) => xml,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "OData $metadata unavailable; fanning out untyped (sink autodetect)"
                    );
                    return Ok(crate::odata::descriptors_from_objects(
                        &odata.objects,
                        table_template,
                    ));
                }
            };
            let mut descs = crate::odata::descriptors_from_edmx_for_objects(
                &xml,
                &odata.objects,
                table_template,
            )?;
            // Key-range partitioning opt-in: for each entity in `partition.objects`
            // whose `$metadata` declares a single integer key, stamp a resolved
            // `partition` block onto its source `config_patch` so the run tiles it
            // concurrently. Entities without a single int key (or absent from
            // `$metadata`) are left to extract sequentially.
            if let Some(partition) = odata.partition.as_ref().filter(|p| !p.objects.is_empty()) {
                let targets: std::collections::HashSet<&str> =
                    partition.objects.iter().map(String::as_str).collect();
                for d in &mut descs {
                    if !targets.contains(d.name.as_str()) {
                        continue;
                    }
                    match crate::odata::single_int_key_from_edmx(&xml, &d.name) {
                        Some(key) => {
                            if let Some(o) = d
                                .config_patch
                                .get_mut("odata")
                                .and_then(Value::as_object_mut)
                            {
                                // Per-entity resolved block: the key is concrete;
                                // workers/count carry over only when set (the
                                // row's `PartitionSpec` applies the defaults).
                                let mut block = serde_json::Map::new();
                                block.insert("key".into(), Value::String(key));
                                if let Some(w) = partition.workers {
                                    block.insert("workers".into(), serde_json::json!(w));
                                }
                                if let Some(c) = partition.count {
                                    block.insert("count".into(), serde_json::json!(c));
                                }
                                o.insert("partition".into(), Value::Object(block));
                            }
                        }
                        None => tracing::warn!(
                            entity = %d.name,
                            "partition.objects entity has no single integer key; \
                             extracting sequentially"
                        ),
                    }
                }
            }
            return Ok(descs);
        }
        let url = format!("{}/$metadata", self.config.base_url.trim_end_matches('/'));
        let xml = self.discover_get_text(&url, "OData $metadata").await?;
        crate::odata::descriptors_from_edmx(&xml)
    }
}

impl RestStream {
    /// Authed GET for a discovery probe, returning the response body as text.
    /// Shared by the OData `$metadata` and Salesforce `/sobjects` paths.
    async fn discover_get_text(&self, url: &str, what: &str) -> Result<String, FaucetError> {
        // Static config headers (#539) form the base; auth is applied on top.
        let mut headers = self.static_headers.clone();
        for (k, v) in self.metadata_headers(url).await?.iter() {
            headers.insert(k.clone(), v.clone());
        }
        self.roundtrip("discover");
        let resp = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| FaucetError::Source(format!("rest: {what} request failed: {e}")))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(FaucetError::Source(format!(
                "rest: {what} returned HTTP {}",
                status.as_u16()
            )));
        }
        resp.text()
            .await
            .map_err(|e| FaucetError::Source(format!("rest: reading {what} failed: {e}")))
    }

    /// Authed GET returning a parsed JSON body (discovery probes).
    async fn discover_get_json(
        &self,
        url: &str,
        what: &str,
    ) -> Result<serde_json::Value, FaucetError> {
        let txt = self.discover_get_text(url, what).await?;
        serde_json::from_str(&txt)
            .map_err(|e| FaucetError::Source(format!("rest: {what} returned invalid JSON: {e}")))
    }

    /// The service `$metadata` CSDL, fetched at most once **per source instance**
    /// (an instance serves one `base_url`). Some services regenerate a very large
    /// CSDL on every call and it cannot be scoped to a subset of entities, so one
    /// fan-out/discover shares a single fetch. Deliberately instance-owned rather
    /// than process-global: no hidden cross-pipeline state, testable, and dropped
    /// with the source.
    async fn metadata_xml(&self) -> Result<Arc<String>, FaucetError> {
        self.metadata_xml_cache
            .get_or_try_init(|| async {
                let url = format!("{}/$metadata", self.config.base_url.trim_end_matches('/'));
                Ok(Arc::new(
                    self.discover_get_text(&url, "OData $metadata").await?,
                ))
            })
            .await
            .cloned()
    }

    /// Discover the inclusive `(min, max)` of an integer primary key for key-range
    /// partitioning — two cheap requests (`$orderby {key} asc|desc, $top=1,
    /// $select={key}`). Returns `None` when the entity is empty (nothing to
    /// partition). Mirrors the reference tap's `discover_key_bounds`.
    async fn discover_key_bounds(
        &self,
        entity: &str,
        key: &str,
    ) -> Result<Option<(i64, i64)>, FaucetError> {
        fn coerce_i64(v: &serde_json::Value) -> Option<i64> {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
        }
        let base = self.config.base_url.trim_end_matches('/');
        let url = format!("{base}/{entity}");
        let mut bounds = [0i64; 2];
        for (i, dir) in ["asc", "desc"].into_iter().enumerate() {
            let mut headers = self.static_headers.clone();
            for (k, v) in self.metadata_headers(&url).await?.iter() {
                headers.insert(k.clone(), v.clone());
            }
            let resp = self
                .client
                .get(&url)
                .headers(headers)
                .query(&[
                    ("cross-company", "true"),
                    ("$orderby", &format!("{key} {dir}")),
                    ("$top", "1"),
                    ("$select", key),
                    ("$format", "json"),
                ])
                .send()
                .await
                .map_err(|e| {
                    FaucetError::Source(format!("rest: OData key-bounds request failed: {e}"))
                })?;
            if !resp.status().is_success() {
                return Err(FaucetError::Source(format!(
                    "rest: OData key-bounds returned HTTP {}",
                    resp.status().as_u16()
                )));
            }
            let body: serde_json::Value = resp
                .json()
                .await
                .map_err(|e| FaucetError::Source(format!("rest: OData key-bounds body: {e}")))?;
            let Some(row) = body
                .get("value")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
            else {
                return Ok(None); // empty entity
            };
            bounds[i] = row.get(key).and_then(coerce_i64).ok_or_else(|| {
                FaucetError::Source(format!("rest: OData key '{key}' is not an integer"))
            })?;
        }
        Ok(Some((bounds[0], bounds[1])))
    }

    /// Stream an OData entity as concurrent key-range tiles: discover the key's
    /// min/max, plan contiguous ranges with the shared
    /// [`faucet_core::shard::plan_pk_shards`] primitive (i128-width math — a
    /// full-range i64 key cannot overflow — and unbounded first/last ranges so
    /// rows inserted outside `[MIN, MAX]` mid-run are still read), and page each
    /// range's `$filter` concurrently, bounded by `partition.workers`. Ranges are
    /// intra-run only — never bookmarked; every run re-tiles. An empty entity
    /// yields nothing.
    fn stream_key_range_partitions<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        key: String,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<faucet_core::StreamPage, FaucetError>> + Send + 'a>> {
        use futures::StreamExt as _;
        let odata = self.config.odata.as_ref().expect("odata configured");
        let partition = odata.partition.as_ref().expect("partition configured");
        let entity = odata.entity.clone().unwrap_or_default();
        let workers = partition.resolved_workers();
        let count = partition.resolved_count();
        let parent = context.clone();
        Box::pin(async_stream::try_stream! {
            let Some((low, high)) = self.discover_key_bounds(&entity, &key).await? else {
                return; // empty entity → no rows
            };
            let shards = faucet_core::shard::plan_pk_shards(&key, low, high, count);
            tracing::info!(
                entity = %entity, key = %key, low, high,
                ranges = shards.len(), workers,
                "OData key-range partitioned extraction"
            );
            // One filtered page-stream per range; poll up to `workers` concurrently.
            let streams: Vec<_> = shards
                .iter()
                .filter_map(faucet_core::shard::PkShardBounds::from_spec)
                .map(|bounds| {
                    let filter = crate::odata::key_range_filter(&bounds);
                    self.stream_pages_inner(Some(&parent), filter, batch_size)
                })
                .collect();
            let mut merged = futures::stream::iter(streams).flatten_unordered(workers);
            while let Some(page) = merged.next().await {
                // Partitioning is a full-table intra-run read: suppress per-range
                // bookmarks so no partial high-water mark is persisted.
                let page = page?;
                yield faucet_core::StreamPage { records: page.records, bookmark: None };
            }
        })
    }

    /// Generic, config-driven discovery (#647): run the [`DiscoverySpec`]
    /// recipe — enumerate datasets (a `list` request or an explicit `objects`
    /// list), optionally `describe` each, and emit one
    /// [`DatasetDescriptor`](faucet_core::DatasetDescriptor) per dataset. All
    /// requests reuse this source's authenticated client; nothing here is
    /// connector-specific.
    async fn discover_via_recipe(
        &self,
        spec: &crate::discovery::DiscoverySpec,
    ) -> Result<Vec<faucet_core::DatasetDescriptor>, FaucetError> {
        let base = self.config.base_url.trim_end_matches('/');
        // Dataset names: an explicit `objects:` list wins; else run the `list`
        // request and extract/filter names from it.
        let names = if !spec.objects.is_empty() {
            spec.objects.clone()
        } else if let Some(list) = &spec.list {
            let url = format!("{base}{}", list.get);
            let resp = self.discover_get_json(&url, "discovery list").await?;
            crate::discovery::dataset_names(&resp, list)
        } else {
            return Err(FaucetError::Source(
                "rest discovery: neither `list` nor `objects` produced any datasets".into(),
            ));
        };
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let describe_resp = match &spec.describe {
                Some(desc) => {
                    let url = format!("{base}{}", desc.get.replace("${name}", &name));
                    Some(self.discover_get_json(&url, "discovery describe").await?)
                }
                None => None,
            };
            if let Some(d) = crate::discovery::build_descriptor(&name, describe_resp.as_ref(), spec)
            {
                out.push(d);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn next_poll_delay_doubles_then_caps() {
        let cap = Duration::from_secs(15);
        // Exponential doubling below the cap.
        assert_eq!(
            next_poll_delay(Duration::from_secs(1), cap),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_poll_delay(Duration::from_secs(2), cap),
            Duration::from_secs(4)
        );
        assert_eq!(
            next_poll_delay(Duration::from_secs(4), cap),
            Duration::from_secs(8)
        );
        // Doubling past the cap clamps to the cap.
        assert_eq!(next_poll_delay(Duration::from_secs(8), cap), cap);
        assert_eq!(next_poll_delay(cap, cap), cap);
        // A zero cap (interval_secs: 0) keeps the delay at zero (poll as fast as possible).
        assert_eq!(
            next_poll_delay(Duration::ZERO, Duration::ZERO),
            Duration::ZERO
        );
        // Saturating: a huge current delay never overflows.
        assert_eq!(next_poll_delay(Duration::from_secs(u64::MAX), cap), cap);
    }

    #[test]
    fn value_max_consolidates_partition_bookmarks() {
        // First value seeds the max.
        assert_eq!(
            value_max(None, json!("2026-01-01")),
            Some(json!("2026-01-01"))
        );
        // Strings compare lexicographically (ISO timestamps sort correctly).
        assert_eq!(
            value_max(Some(json!("2026-01-01")), json!("2026-03-01")),
            Some(json!("2026-03-01"))
        );
        assert_eq!(
            value_max(Some(json!("2026-03-01")), json!("2026-01-01")),
            Some(json!("2026-03-01"))
        );
        // Numbers compare numerically.
        assert_eq!(value_max(Some(json!(5)), json!(10)), Some(json!(10)));
        assert_eq!(value_max(Some(json!(10)), json!(5)), Some(json!(10)));
        // Heterogeneous / other → prefer the latest candidate.
        assert_eq!(value_max(Some(json!("a")), json!(3)), Some(json!(3)));
    }

    #[test]
    fn injected_policy_applies_when_legacy_fields_at_defaults() {
        // Config left at the default max_retries/retry_backoff → injection wins.
        let stream =
            RestStream::new(RestStreamConfig::new("https://api.example.com", "/items")).unwrap();
        let injected = faucet_core::RetryPolicy {
            max_attempts: 9,
            base: Duration::from_secs(7),
            ..faucet_core::RetryPolicy::default()
        };
        let stream = stream.with_retry_policy(injected);
        assert_eq!(stream.retry_policy.max_attempts, 9);
        assert_eq!(stream.retry_policy.base, Duration::from_secs(7));
    }

    #[test]
    fn legacy_fields_take_precedence_over_injected_policy() {
        // User set max_retries explicitly → the injected policy is ignored and
        // the connector's own legacy fields keep governing retries.
        let config = RestStreamConfig::new("https://api.example.com", "/items").max_retries(7);
        let stream = RestStream::new(config).unwrap();
        // Default policy derived from legacy fields: max_attempts = 7 + 1.
        assert_eq!(stream.retry_policy.max_attempts, 8);
        let injected = faucet_core::RetryPolicy {
            max_attempts: 99,
            base: Duration::from_secs(42),
            ..faucet_core::RetryPolicy::default()
        };
        let stream = stream.with_retry_policy(injected);
        // Unchanged: the legacy max_retries(7) still wins.
        assert_eq!(stream.retry_policy.max_attempts, 8);
        assert_eq!(stream.retry_policy.base, DEFAULT_RETRY_BACKOFF);
    }

    #[test]
    fn redact_error_url_hides_api_key_query_param() {
        // #321 L2: a custom `api_key_query` param name is redacted by name.
        let auth = AuthSpec::Inline(Auth::ApiKeyQuery {
            param: "api_token".into(),
            value: "SUPERSECRET".into(),
        });
        let url =
            reqwest::Url::parse("https://api.example.com/v1/items?page=2&api_token=SUPERSECRET")
                .unwrap();
        let redacted = redact_error_url(&url, &auth);
        assert!(
            !redacted.contains("SUPERSECRET"),
            "secret must be gone: {redacted}"
        );
        assert!(redacted.contains("api_token=%2A%2A%2A") || redacted.contains("api_token=***"));
        assert!(
            redacted.contains("page=2"),
            "non-secret param kept: {redacted}"
        );
    }

    #[test]
    fn redact_error_url_without_api_key_query_still_scrubs_common_keys() {
        // Non-ApiKeyQuery auth: the shared redaction still strips common secret
        // query keys and userinfo.
        let auth: AuthSpec<Auth> = AuthSpec::Inline(Auth::None);
        let url = reqwest::Url::parse("https://u:pw@api.example.com/v1/items?token=abc").unwrap();
        let redacted = redact_error_url(&url, &auth);
        assert!(
            !redacted.contains("abc"),
            "common secret key redacted: {redacted}"
        );
        assert!(!redacted.contains("pw@"), "userinfo redacted: {redacted}");
    }

    #[test]
    fn test_substitute_context_substitutes_placeholders() {
        let mut ctx = HashMap::new();
        ctx.insert("org_id".to_string(), json!("acme"));
        ctx.insert("repo".to_string(), json!("myrepo"));
        let result =
            faucet_core::util::substitute_context("/orgs/{org_id}/repos/{repo}/issues", &ctx);
        assert_eq!(result, "/orgs/acme/repos/myrepo/issues");
    }

    #[test]
    fn test_substitute_context_no_placeholders() {
        let ctx = HashMap::new();
        let result = faucet_core::util::substitute_context("/api/users", &ctx);
        assert_eq!(result, "/api/users");
    }

    #[test]
    fn test_substitute_context_numeric_value() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(42));
        let result = faucet_core::util::substitute_context("/items/{id}", &ctx);
        assert_eq!(result, "/items/42");
    }

    #[test]
    fn test_parse_retry_after_valid() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("30"),
        );
        assert_eq!(parse_retry_after(&headers), Duration::from_secs(30));
    }

    #[test]
    fn test_parse_retry_after_missing_defaults_to_60() {
        assert_eq!(
            parse_retry_after(&HeaderMap::new()),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_parse_retry_after_non_numeric_defaults_to_60() {
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_static("not-a-number"),
        );
        assert_eq!(parse_retry_after(&headers), Duration::from_secs(60));
    }

    #[test]
    fn test_parse_retry_after_http_date() {
        // RFC 7231 permits an HTTP-date form instead of delta-seconds.
        let future = std::time::SystemTime::now() + Duration::from_secs(7200);
        let date = httpdate::fmt_http_date(future);
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_str(&date).unwrap(),
        );
        let d = parse_retry_after(&headers);
        // ~2 hours out — must not collapse to the 60s fallback.
        assert!(
            d > Duration::from_secs(3600),
            "expected ~2h from HTTP-date, got {d:?}"
        );
        assert!(
            d <= Duration::from_secs(7200),
            "should not exceed the target instant, got {d:?}"
        );
    }

    #[test]
    fn test_parse_retry_after_past_http_date_is_zero() {
        // A date already in the past → retry now (zero wait), not the fallback.
        let past = std::time::SystemTime::now() - Duration::from_secs(3600);
        let date = httpdate::fmt_http_date(past);
        let mut headers = HeaderMap::new();
        headers.insert(
            reqwest::header::RETRY_AFTER,
            reqwest::header::HeaderValue::from_str(&date).unwrap(),
        );
        assert_eq!(parse_retry_after(&headers), Duration::ZERO);
    }

    #[test]
    fn test_new_rejects_invalid_expiry_ratio_zero() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 0.0,
        });
        let result = RestStream::new(config);
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Auth(_))));
    }

    #[test]
    fn test_new_rejects_invalid_expiry_ratio_negative() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: -0.5,
        });
        assert!(RestStream::new(config).is_err());
    }

    #[test]
    fn test_new_rejects_invalid_expiry_ratio_above_one() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 1.5,
        });
        assert!(RestStream::new(config).is_err());
    }

    #[test]
    fn test_new_accepts_valid_expiry_ratio() {
        let config = RestStreamConfig::new("https://example.com", "/data").auth(Auth::OAuth2 {
            token_url: "https://auth.example.com/token".into(),
            client_id: "id".into(),
            client_secret: "secret".into(),
            scopes: vec![],
            expiry_ratio: 1.0,
        });
        assert!(RestStream::new(config).is_ok());
    }

    #[test]
    fn test_new_with_no_auth_succeeds() {
        let config = RestStreamConfig::new("https://example.com", "/data");
        assert!(RestStream::new(config).is_ok());
    }

    #[test]
    fn test_new_with_timeout() {
        let config =
            RestStreamConfig::new("https://example.com", "/data").timeout(Duration::from_secs(10));
        assert!(RestStream::new(config).is_ok());
    }

    #[test]
    fn test_substitute_context_missing_placeholder_unchanged() {
        let mut ctx = HashMap::new();
        ctx.insert("org".to_string(), json!("acme"));
        let result = faucet_core::util::substitute_context("/items/{missing}", &ctx);
        assert_eq!(result, "/items/{missing}");
    }

    #[test]
    fn test_substitute_context_boolean_value() {
        let mut ctx = HashMap::new();
        ctx.insert("flag".to_string(), json!(true));
        let result = faucet_core::util::substitute_context("/items/{flag}", &ctx);
        assert_eq!(result, "/items/true");
    }

    #[test]
    fn rest_source_connector_name_is_rest() {
        use faucet_core::Source;
        let source = RestStream::new(RestStreamConfig::new("https://example.com", "/data"))
            .expect("minimal RestStream construction");
        assert_eq!(source.connector_name(), "rest");
    }

    #[test]
    fn dataset_uri_combines_base_and_path() {
        use faucet_core::Source;
        let source = RestStream::new(RestStreamConfig::new(
            "https://api.example.com",
            "/v1/users",
        ))
        .unwrap();
        assert_eq!(source.dataset_uri(), "https://api.example.com/v1/users");
    }

    #[test]
    fn sql_from_object_parses_the_driving_object() {
        // Common shapes: with WHERE, with a leading newline/whitespace, lowercase
        // keyword, trailing clause, and a field literally containing "from".
        assert_eq!(
            sql_from_object("SELECT Id, Name FROM Account WHERE IsDeleted = false"),
            Some("Account".to_string())
        );
        assert_eq!(
            sql_from_object("SELECT Id\nFROM SBQQ__Quote__c\nORDER BY Id"),
            Some("SBQQ__Quote__c".to_string())
        );
        assert_eq!(
            sql_from_object("select id from contact"),
            Some("contact".to_string())
        );
        assert_eq!(
            sql_from_object("SELECT Id FROM Opportunity_Line_Item"),
            Some("Opportunity_Line_Item".to_string())
        );
        // No FROM → None (caller falls back to a hash).
        assert_eq!(sql_from_object("SELECT 1"), None);
    }

    #[test]
    fn sql_from_object_ignores_subquery_and_quoted_from() {
        // A parent-child relationship subquery in the SELECT list: its FROM is
        // inside parens and must not be mistaken for the driving object.
        assert_eq!(
            sql_from_object(
                "SELECT Id, (SELECT Id FROM Contacts WHERE IsDeleted = false) FROM Account"
            ),
            Some("Account".to_string())
        );
        // "from" inside a quoted literal is not a clause keyword.
        assert_eq!(
            sql_from_object("SELECT Id FROM Lead WHERE Source = 'from web'"),
            Some("Lead".to_string())
        );
    }

    #[test]
    fn async_job_object_reads_query_field() {
        assert_eq!(
            async_job_object(
                Some(&serde_json::json!({
                    "operation": "queryAll",
                    "query": "SELECT Id FROM Lead"
                })),
                "/query"
            ),
            Some("Lead".to_string())
        );
        // Missing query / missing body → None.
        assert_eq!(
            async_job_object(
                Some(&serde_json::json!({"operation": "queryAll"})),
                "/query"
            ),
            None
        );
        assert_eq!(async_job_object(None, "/query"), None);
    }

    #[test]
    fn async_job_object_follows_a_non_default_query_path() {
        // The statement need not sit at top-level `query`: a nested or
        // differently-named key used to silently collapse every object onto
        // one dataset identity (#654 M23).
        assert_eq!(
            async_job_object(
                Some(&serde_json::json!({"request": {"sql": "SELECT Id FROM Account"}})),
                "/request/sql"
            ),
            Some("Account".to_string())
        );
        // The default pointer finds nothing in that body.
        assert_eq!(
            async_job_object(
                Some(&serde_json::json!({"request": {"sql": "SELECT Id FROM Account"}})),
                "/query"
            ),
            None
        );
    }

    #[test]
    fn terminal_locator_honours_the_configured_sentinels() {
        let default = vec!["null".to_string()];
        // Empty is always terminal, whatever the list says.
        assert!(is_terminal_locator("", &default));
        assert!(is_terminal_locator("   ", &[]));
        // The default sentinel, case-insensitively and after trimming.
        assert!(is_terminal_locator(" NULL ", &default));
        assert!(!is_terminal_locator("abc123", &default));
        // A vendor signalling with its own sentinel is now expressible; the
        // default one stops being special once replaced.
        let custom = vec!["EOF".to_string(), "-1".to_string()];
        assert!(is_terminal_locator("eof", &custom));
        assert!(is_terminal_locator("-1", &custom));
        assert!(
            !is_terminal_locator("null", &custom),
            "replacing the list must replace it, not extend it"
        );
    }

    #[test]
    fn inject_sql_predicate_adds_or_wraps_where() {
        // No WHERE, no trailing clause → append WHERE.
        assert_eq!(
            inject_sql_predicate(
                "SELECT Id FROM Account",
                "SystemModstamp > 2026-01-01T00:00:00Z"
            ),
            "SELECT Id FROM Account WHERE SystemModstamp > 2026-01-01T00:00:00Z"
        );
        // Existing WHERE → wrap in parens + AND (keeps OR precedence correct).
        assert_eq!(
            inject_sql_predicate(
                "SELECT Id FROM Account WHERE IsActive = true OR Rating = 'Hot'",
                "SystemModstamp > 2026-01-01T00:00:00Z"
            ),
            "SELECT Id FROM Account WHERE (IsActive = true OR Rating = 'Hot') AND (SystemModstamp > 2026-01-01T00:00:00Z)"
        );
        // Trailing ORDER BY → predicate goes before it.
        assert_eq!(
            inject_sql_predicate("SELECT Id FROM Account ORDER BY Id", "X > 1"),
            "SELECT Id FROM Account WHERE X > 1 ORDER BY Id"
        );
        // WHERE + trailing LIMIT (newline-tolerant).
        assert_eq!(
            inject_sql_predicate("SELECT Id\nFROM Account\nWHERE A = 1\nLIMIT 10", "X > 1"),
            "SELECT Id\nFROM Account\nWHERE (A = 1) AND (X > 1) LIMIT 10"
        );
    }

    #[test]
    fn inject_sql_predicate_ignores_subquery_and_quoted_keywords() {
        // A subquery's WHERE (inside parens) is not the outer statement's WHERE:
        // the predicate must attach at the top level, after the subquery.
        assert_eq!(
            inject_sql_predicate(
                "SELECT Id, (SELECT Id FROM Contacts WHERE IsDeleted = false) FROM Account",
                "X > 1"
            ),
            "SELECT Id, (SELECT Id FROM Contacts WHERE IsDeleted = false) FROM Account WHERE X > 1"
        );
        // Outer WHERE + subquery WHERE: only the outer one is wrapped.
        assert_eq!(
            inject_sql_predicate(
                "SELECT Id, (SELECT Id FROM Contacts WHERE A = 1) FROM Account WHERE B = 2",
                "X > 1"
            ),
            "SELECT Id, (SELECT Id FROM Contacts WHERE A = 1) FROM Account WHERE (B = 2) AND (X > 1)"
        );
        // A clause keyword inside a quoted literal is data, not a boundary.
        assert_eq!(
            inject_sql_predicate("SELECT Id FROM A WHERE Name = 'a limit b'", "X > 1"),
            "SELECT Id FROM A WHERE (Name = 'a limit b') AND (X > 1)"
        );
    }

    #[test]
    fn incremental_source_auto_opts_into_state_key() {
        use faucet_core::{ReplicationMethod, Source};
        // Full-table + no explicit key → not resumable.
        let ft = RestStream::new(RestStreamConfig::new("https://api.example.com", "/x")).unwrap();
        assert_eq!(ft.state_key(), None);
        // Incremental + replication_key → auto-opts-in (Some), so the executor
        // wraps it and persists the bookmark (#630).
        let mut cfg = RestStreamConfig::new("https://api.example.com", "/x");
        cfg.replication_method = ReplicationMethod::Incremental;
        cfg.replication_key = Some("SystemModstamp".into());
        let inc = RestStream::new(cfg).unwrap();
        assert!(inc.state_key().is_some());
        // An explicit key always wins.
        let mut cfg2 = RestStreamConfig::new("https://api.example.com", "/x");
        cfg2.state_key = Some("mykey".into());
        let ex = RestStream::new(cfg2).unwrap();
        assert_eq!(ex.state_key().as_deref(), Some("mykey"));
    }

    #[test]
    fn sql_literal_quotes_by_type() {
        use serde_json::json;
        // Datetime / date → unquoted (datetime literal).
        assert_eq!(
            sql_literal(&json!("2026-08-28T12:00:00Z")),
            "2026-08-28T12:00:00Z"
        );
        assert_eq!(sql_literal(&json!("2026-08-28")), "2026-08-28");
        // Plain string → single-quoted.
        assert_eq!(sql_literal(&json!("Hot")), "'Hot'");
        // Number → bare.
        assert_eq!(sql_literal(&json!(42)), "42");
        assert!(is_sql_datetime("2026-08-28T00:00:00+05:30"));
        assert!(!is_sql_datetime("not-a-date"));
    }

    #[test]
    fn dataset_uri_redacts_credentials() {
        use faucet_core::Source;
        let source = RestStream::new(RestStreamConfig::new(
            "https://user:secret@api.example.com",
            "/v1/data",
        ))
        .unwrap();
        assert_eq!(source.dataset_uri(), "https://api.example.com/v1/data");
    }
}

/// Mutual-TLS unit tests (#495). Lib-level so llvm-cov attributes coverage of
/// `apply_client_tls` / `build_identity` / the `new()` TLS branch reliably.
#[cfg(all(test, feature = "mtls"))]
mod mtls_tests {
    use super::*;
    use crate::config::TlsClientConfig;

    const CERT: &str = include_str!("../tests/fixtures/mtls/cert.pem");
    const KEY: &str = include_str!("../tests/fixtures/mtls/key.pem");

    fn pem() -> TlsClientConfig {
        TlsClientConfig {
            client_cert: Some(CERT.to_string()),
            client_key: Some(KEY.to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn pem_identity_builds() {
        let cfg = RestStreamConfig::new("https://x.test", "/y").tls(pem());
        assert!(RestStream::new(cfg).is_ok());
    }

    #[test]
    fn min_version_branches_are_exercised() {
        // 1.2 is universally supported and must build.
        let mut tls = pem();
        tls.min_version = Some("1.2".into());
        assert!(RestStream::new(RestStreamConfig::new("https://x.test", "/y").tls(tls)).is_ok());
        // 1.3 exercises the other branch; some native-tls backends (e.g. macOS
        // SecureTransport) reject a 1.3 floor at client-build time, so only
        // require it not to panic.
        let mut tls = pem();
        tls.min_version = Some("1.3".into());
        let _ = RestStream::new(RestStreamConfig::new("https://x.test", "/y").tls(tls));
    }

    #[test]
    fn pkcs12_identity_builds() {
        let p12 = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/mtls/identity.p12"
        );
        let tls = TlsClientConfig {
            client_identity_pkcs12: Some(p12.to_string()),
            pkcs12_password: Some("changeit".into()),
            ..Default::default()
        };
        let cfg = RestStreamConfig::new("https://x.test", "/y").tls(tls);
        assert!(RestStream::new(cfg).is_ok());
    }

    #[test]
    fn invalid_pem_errors_without_leaking_key() {
        let tls = TlsClientConfig {
            client_cert: Some("-----BEGIN CERTIFICATE-----\nbad\n-----END CERTIFICATE-----".into()),
            client_key: Some("SUPERSECRETKEY".into()),
            ..Default::default()
        };
        let cfg = RestStreamConfig::new("https://x.test", "/y").tls(tls);
        let err = RestStream::new(cfg)
            .map(|_| ())
            .expect_err("bad PEM must error");
        assert!(!err.to_string().contains("SUPERSECRETKEY"));
    }

    #[test]
    fn missing_pkcs12_file_errors() {
        let tls = TlsClientConfig {
            client_identity_pkcs12: Some("/no/such.p12".into()),
            pkcs12_password: Some("x".into()),
            ..Default::default()
        };
        let cfg = RestStreamConfig::new("https://x.test", "/y").tls(tls);
        assert!(RestStream::new(cfg).is_err());
    }
}

#[cfg(test)]
mod patch_edge_tests {
    use super::*;
    use crate::config::RestStreamConfig;
    use serde_json::json;

    fn bulk_job_json(submit_json: Option<Value>) -> crate::async_job::AsyncJobConfig {
        let mut submit = json!({ "method": "POST", "url": "/jobs" });
        if let Some(j) = submit_json {
            submit["json"] = j;
        }
        serde_json::from_value(json!({
            "submit": submit,
            "job_id": "$.id",
            "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 1 },
            "status": { "path": "$.state", "success": ["ok"] },
            "fetch": { "url": "/jobs/${job_id}/result" }
        }))
        .unwrap()
    }

    #[test]
    fn sql_from_object_empty_identifier_is_none() {
        // The token after FROM strips to nothing → no driving object.
        assert_eq!(sql_from_object("SELECT Id FROM ,"), None);
    }

    #[test]
    fn top_level_tokens_honors_backslash_escaped_quotes() {
        // The escaped quote must not close the literal: the quoted `limit`
        // stays inside one dirty token and never surfaces as a clause keyword.
        let q = r"SELECT Id FROM A WHERE n = 'a\'limit\'b' LIMIT 5";
        let toks = top_level_tokens(q);
        assert!(toks.iter().any(|(_, t)| *t == "LIMIT"));
        assert!(toks.iter().all(|(_, t)| !t.contains('\'')));
        // …and the injector places the predicate before the REAL clause.
        assert_eq!(
            inject_sql_predicate(q, "X > 1"),
            r"SELECT Id FROM A WHERE (n = 'a\'limit\'b') AND (X > 1) LIMIT 5"
        );
    }

    #[test]
    fn sql_literal_bool_and_composite_values() {
        assert_eq!(sql_literal(&json!(true)), "true");
        assert_eq!(sql_literal(&json!(false)), "false");
        // Non-scalars stringify quoted (defensive; bookmarks are scalars).
        assert_eq!(sql_literal(&json!([1, 2])), "'[1,2]'");
    }

    #[test]
    fn full_table_async_job_emits_no_bookmark() {
        let mut cfg = RestStreamConfig::new("https://api.example.com", "");
        cfg.async_job = Some(bulk_job_json(Some(json!({ "query": "SELECT Id FROM A" }))));
        let s = RestStream::new(cfg).unwrap();
        assert!(s.async_job_new_bookmark().is_none());
    }

    #[test]
    fn dataset_uri_uses_object_or_stable_job_hash() {
        let mut cfg = RestStreamConfig::new("https://api.example.com", "");
        cfg.async_job = Some(bulk_job_json(Some(
            json!({ "query": "SELECT Id FROM Lead" }),
        )));
        let s = RestStream::new(cfg).unwrap();
        assert!(
            faucet_core::Source::dataset_uri(&s).ends_with("/objects/Lead"),
            "{}",
            faucet_core::Source::dataset_uri(&s)
        );

        // No parseable object → a stable 16-hex job hash, deterministic across
        // instances (it feeds persisted catalog/lineage identity).
        let mk = || {
            let mut cfg = RestStreamConfig::new("https://api.example.com", "");
            cfg.async_job = Some(bulk_job_json(Some(json!({ "report_type": "x" }))));
            RestStream::new(cfg).unwrap()
        };
        let (a, b) = (
            faucet_core::Source::dataset_uri(&mk()),
            faucet_core::Source::dataset_uri(&mk()),
        );
        assert_eq!(a, b);
        let suffix = a.rsplit("/job/").next().unwrap();
        assert_eq!(suffix.len(), 16, "{a}");
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn auto_state_key_derives_from_dataset_identity() {
        let mk = |query: &str| {
            let mut cfg = RestStreamConfig::new("https://api.example.com", "");
            cfg.replication_method = ReplicationMethod::Incremental;
            cfg.replication_key = Some("m".into());
            cfg.async_job = Some(bulk_job_json(Some(json!({ "query": query }))));
            faucet_core::Source::state_key(&RestStream::new(cfg).unwrap()).unwrap()
        };
        let (a, b) = (mk("SELECT Id FROM Lead"), mk("SELECT Id FROM Account"));
        assert!(a.starts_with("rest:"), "{a}");
        assert_ne!(a, b, "two sources must never collide on one bookmark key");
        assert_eq!(a, mk("SELECT Id FROM Lead"), "stable across constructions");
    }

    #[test]
    fn extract_page_strips_odata_control_fields() {
        let mut cfg = RestStreamConfig::new("https://api.example.com", "");
        cfg.odata = Some(serde_json::from_value(json!({ "entity": "Orders" })).unwrap());
        cfg.records_path = Some("$.value[*]".into());
        let s = RestStream::new(cfg).unwrap();
        let body = json!({ "value": [
            { "@odata.etag": "W/\"1\"", "Id": 1, "Name": "a" },
        ]});
        let recs = s.extract_page(&body).unwrap();
        assert_eq!(recs, vec![json!({ "Id": 1, "Name": "a" })]);
    }

    #[test]
    fn extract_page_drops_configured_prefixes_for_any_protocol() {
        // The need is generic; only the prefix was OData's. A JSON:API / HAL
        // envelope can now be stripped without a code change (#654 M24).
        let mut cfg = RestStreamConfig::new("https://api.example.com", "");
        cfg.drop_key_prefixes = vec!["_".to_string(), "links".to_string()];
        cfg.records_path = Some("$.data[*]".into());
        let s = RestStream::new(cfg).unwrap();
        let body = json!({ "data": [
            { "_links": {"self": "/x"}, "links": {"next": "/y"}, "id": 1, "name": "a" },
        ]});
        assert_eq!(
            s.extract_page(&body).unwrap(),
            vec![json!({ "id": 1, "name": "a" })]
        );
    }

    #[test]
    fn an_odata_block_still_implies_the_odata_prefix_alongside_explicit_ones() {
        // Existing `odata:` configs must keep stripping `@odata.` without
        // naming it, even once the list is used for something else.
        let mut cfg = RestStreamConfig::new("https://api.example.com", "");
        cfg.odata = Some(serde_json::from_value(json!({ "entity": "Orders" })).unwrap());
        cfg.drop_key_prefixes = vec!["x-".to_string()];
        cfg.records_path = Some("$.value[*]".into());
        let s = RestStream::new(cfg).unwrap();
        let body = json!({ "value": [
            { "@odata.etag": "W/\"1\"", "x-trace": "t", "Id": 1 },
        ]});
        assert_eq!(s.extract_page(&body).unwrap(), vec![json!({ "Id": 1 })]);
    }

    #[test]
    fn native_output_formats_gated_on_csv_async_job() {
        // CSV async-job with no decode → NDJSON advertised.
        let mut cfg = RestStreamConfig::new("https://api.example.com", "");
        cfg.async_job = Some(bulk_job_json(None));
        cfg.response_format = crate::config::ResponseFormat::Csv;
        let s = RestStream::new(cfg).unwrap();
        assert_eq!(
            faucet_core::Source::native_output_formats(&s),
            &[faucet_core::NativeFormat::NdJson]
        );
        // Plain paginated source → no native formats.
        let plain = RestStream::new(RestStreamConfig::new("https://api.example.com", "")).unwrap();
        assert!(faucet_core::Source::native_output_formats(&plain).is_empty());
    }
}

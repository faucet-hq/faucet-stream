//! GraphQL stream executor.

use crate::config::{
    GraphqlAuth, GraphqlOffsetPagination, GraphqlPagination, GraphqlPaginationSpec,
    GraphqlStreamConfig,
};
use async_trait::async_trait;
use base64::Engine as _;
use faucet_core::util::{self, DEFAULT_ERROR_BODY_MAX_LEN};
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider, Stream, StreamPage};
use jsonpath_rust::JsonPath;
use reqwest::Client;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::pin::Pin;
use std::time::Duration;

/// Retries on transient (5xx / connection) failures before giving up.
const RETRY_MAX_ATTEMPTS: u32 = 3;
/// Base exponential-backoff delay between retries.
const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(500);

/// A configured GraphQL source that handles pagination and extraction.
pub struct GraphqlStream {
    config: GraphqlStreamConfig,
    client: Client,
    /// Optional shared auth provider. When set, it takes precedence over inline
    /// auth. Used by the CLI to resolve `auth: { ref }`, and by library callers
    /// who construct one provider and inject it into many sources.
    auth_provider: Option<SharedAuthProvider>,
    /// Retry policy for transient request failures. Defaulted in `new()` to
    /// reproduce the legacy `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_BACKOFF`
    /// constants; overridable via [`with_retry_policy`](Self::with_retry_policy).
    retry_policy: faucet_core::RetryPolicy,
}

/// Attach a mutual-TLS client identity to the HTTP client builder (#495). Only
/// compiled with the `mtls` feature; the stub errors so a `tls:` block on a
/// build without the feature fails loudly instead of silently sending no cert.
#[cfg(feature = "mtls")]
fn apply_client_tls(
    builder: reqwest::ClientBuilder,
    tls: &faucet_core::TlsClientConfig,
) -> Result<reqwest::ClientBuilder, FaucetError> {
    let identity = build_identity(tls)?;
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
    _tls: &faucet_core::TlsClientConfig,
) -> Result<reqwest::ClientBuilder, FaucetError> {
    Err(FaucetError::Config(
        "a `tls:` (mutual-TLS) block is configured, but this build of \
         faucet-source-graphql lacks the `mtls` feature; rebuild with `--features mtls`"
            .into(),
    ))
}

/// Build a [`reqwest::Identity`] from the PEM pair or the PKCS#12 file. Errors
/// never echo key material — only the backend's opaque parse message.
#[cfg(feature = "mtls")]
fn build_identity(tls: &faucet_core::TlsClientConfig) -> Result<reqwest::Identity, FaucetError> {
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
        let cert = tls.client_cert.as_deref().unwrap_or_default();
        let key = tls.client_key.as_deref().unwrap_or_default();
        reqwest::Identity::from_pkcs8_pem(cert.as_bytes(), key.as_bytes())
            .map_err(|e| FaucetError::Config(format!("tls: invalid PEM client identity: {e}")))
    }
}

/// Map a [`Credential`] from a shared provider onto the GraphQL [`GraphqlAuth`]
/// representation so the existing header-application path can be reused.
fn credential_to_auth(cred: Credential) -> GraphqlAuth {
    match cred {
        Credential::Bearer(token) => GraphqlAuth::Bearer { token },
        Credential::Token(token) => GraphqlAuth::Custom {
            headers: HashMap::from([("Authorization".into(), token)]),
        },
        Credential::Header { name, value } => GraphqlAuth::Custom {
            headers: HashMap::from([(name, value)]),
        },
        Credential::Basic { username, password } => GraphqlAuth::Custom {
            headers: HashMap::from([(
                "Authorization".into(),
                format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD
                        .encode(format!("{username}:{password}"))
                ),
            )]),
        },
    }
}

impl GraphqlStream {
    /// Create a new GraphQL stream from the given configuration.
    ///
    /// Infallible for the common case. Prefer [`try_new`](Self::try_new) when the
    /// config may carry a `tls:` (mutual-TLS) block: this panics if the client
    /// (or the TLS identity) fails to build, matching the pre-existing
    /// `Client::new()` behavior.
    pub fn new(config: GraphqlStreamConfig) -> Self {
        Self::try_new(config).expect(
            "GraphqlStream::new: client build failed; use try_new() for fallible construction",
        )
    }

    /// Fallible constructor — builds the HTTP client, including any mutual-TLS
    /// client identity. The CLI registry uses this (after `config.validate()`) so
    /// a bad `tls:` block surfaces as a typed error instead of a panic. Only the
    /// `tls:` block is validated here; the registry still calls
    /// [`GraphqlStreamConfig::validate`] for the rest, keeping `new()` infallible
    /// for non-TLS configs exactly as before.
    pub fn try_new(config: GraphqlStreamConfig) -> Result<Self, FaucetError> {
        let mut builder = Client::builder();
        if let Some(tls) = &config.tls {
            tls.validate()?;
            builder = apply_client_tls(builder, tls)?;
        }
        let client = builder.build().map_err(|e| {
            FaucetError::Config(format!("graphql: failed to build HTTP client: {e}"))
        })?;
        Ok(Self {
            config,
            client,
            auth_provider: None,
            // Reproduce the legacy `execute_with_retry(RETRY_MAX_ATTEMPTS,
            // RETRY_BASE_BACKOFF, …)` behavior exactly: `max_retries` is
            // retries-after-first, so `max_attempts = RETRY_MAX_ATTEMPTS + 1`.
            retry_policy: faucet_core::RetryPolicy {
                max_attempts: RETRY_MAX_ATTEMPTS + 1,
                backoff: faucet_core::BackoffKind::Exponential,
                base: RETRY_BASE_BACKOFF,
                max: Duration::from_secs(60),
                jitter: true,
                retry_on: faucet_core::RetryClassSet::default(),
            },
        })
    }

    /// Attach a custom [`RetryPolicy`](faucet_core::RetryPolicy) for transient
    /// request failures, replacing the default derived from
    /// `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_BACKOFF`. Used by the CLI to inject a
    /// pipeline-level `resilience:` policy into the source.
    pub fn with_retry_policy(mut self, policy: faucet_core::RetryPolicy) -> Self {
        self.retry_policy = policy;
        self
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set, the
    /// provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who construct one provider and inject it into many sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Fetch all records across all pages.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_all_with_context(&std::collections::HashMap::new())
            .await
    }

    /// Fetch all records, merging parent context values into GraphQL variables.
    async fn fetch_all_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut all_records = Vec::new();
        let mut cursor: Option<String> = None;
        let mut offset = 0usize;
        let mut pages_fetched = 0usize;
        let mut warned_unresolved_has_next = false;
        let mut cursor_guard = CursorGuard::new();

        loop {
            if let Some(max) = self.config.max_pages
                && pages_fetched >= max
            {
                tracing::warn!("max pages ({max}) reached");
                break;
            }

            let body = self.execute_query(&cursor, offset, context).await?;
            let records = self.extract_records(&body)?;
            let records_in_page = records.len();
            all_records.extend(records);
            pages_fetched += 1;

            // Check pagination.
            match &self.config.pagination {
                Some(GraphqlPaginationSpec::Cursor(pag)) => {
                    let (step, unresolved) = decide_next_page(&body, pag, cursor.as_deref());
                    if unresolved && !warned_unresolved_has_next {
                        tracing::warn!(
                            path = %pag.has_next_page_path,
                            "GraphQL has_next_page path did not resolve to a boolean; \
                             deferring to cursor presence to decide pagination"
                        );
                        warned_unresolved_has_next = true;
                    }
                    match step {
                        PageStep::Stop => break,
                        PageStep::StopLoop => {
                            tracing::warn!("cursor loop detected, stopping pagination");
                            break;
                        }
                        PageStep::Advance(next) => {
                            if cursor_guard.is_repeat(&next) {
                                tracing::warn!(
                                    "cursor cycle detected (cursor already seen), stopping pagination"
                                );
                                break;
                            }
                            cursor = Some(next);
                        }
                    }
                }
                Some(GraphqlPaginationSpec::Offset(off)) => {
                    if offset_should_continue(records_in_page, off) {
                        offset += off.page_size;
                    } else {
                        break;
                    }
                }
                None => break,
            }
        }

        tracing::info!(
            records = all_records.len(),
            pages = pages_fetched,
            "GraphQL fetch complete"
        );
        Ok(all_records)
    }

    /// Execute a single GraphQL query, merging parent context into variables.
    ///
    /// `cursor` carries the Relay cursor for the next request (cursor mode);
    /// `offset` carries the current offset (offset mode). Only the field the
    /// active pagination style uses is injected — the other stays inert.
    async fn execute_query(
        &self,
        cursor: &Option<String>,
        offset: usize,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Value, FaucetError> {
        let mut variables = self.config.variables.clone();

        // Merge parent context values into GraphQL variables.
        if !context.is_empty()
            && let Value::Object(ref mut map) = variables
        {
            for (key, value) in context {
                map.insert(key.clone(), value.clone());
            }
        }

        // The query string; for offset `substitute_in_query` mode it is
        // rewritten per request with the current offset (ShopifyQL etc.).
        let mut query = self.config.query.clone();

        // Inject the per-request pagination variable(s).
        match &self.config.pagination {
            // Cursor mode: inject the `after` cursor (once we have one) and the
            // page-size variable from `batch_size`. `batch_size = 0` is the
            // "use upstream default" sentinel — we omit the size variable.
            Some(GraphqlPaginationSpec::Cursor(pag)) => {
                if let (Some(cursor_val), Value::Object(map)) = (cursor, &mut variables) {
                    map.insert(pag.cursor_variable.clone(), json!(cursor_val));
                }
                if self.config.batch_size != 0
                    && let Value::Object(map) = &mut variables
                {
                    map.insert(
                        pag.page_size_variable.clone(),
                        json!(self.config.batch_size),
                    );
                }
            }
            // Offset mode: either substitute `${offset_variable}` into the query
            // string (ShopifyQL's string-literal `LIMIT … OFFSET …`, #569) or
            // inject the current offset as a JSON GraphQL variable (#550). The
            // page size is not injected — the user bakes the limit into the query.
            Some(GraphqlPaginationSpec::Offset(off)) => {
                if off.substitute_in_query {
                    let token = format!("${{{}}}", off.offset_variable);
                    query = query.replace(&token, &offset.to_string());
                } else if let Value::Object(map) = &mut variables {
                    map.insert(off.offset_variable.clone(), json!(offset));
                }
            }
            None => {}
        }

        let payload = json!({
            "query": query,
            "variables": variables,
        });

        let mut req = self
            .client
            .post(&self.config.endpoint)
            .headers(self.config.headers.clone())
            .json(&payload);

        // Resolve credentials to concrete auth. A shared auth provider (from
        // `auth: { ref }` or injected by a library caller) takes precedence;
        // otherwise the inline auth config is used directly.
        let effective_auth: GraphqlAuth = if let Some(provider) = &self.auth_provider {
            credential_to_auth(provider.credential().await?)
        } else {
            match &self.config.auth {
                AuthSpec::Inline(a) => a.clone(),
                AuthSpec::Reference(r) => {
                    return Err(FaucetError::Auth(format!(
                        "auth references provider '{}' but no provider was supplied; \
                         set one via the CLI `auth:` catalog or `with_auth_provider`",
                        r.name
                    )));
                }
            }
        };

        // Apply resolved auth to the request.
        match effective_auth {
            GraphqlAuth::None => {}
            GraphqlAuth::Bearer { token } => {
                req = req.bearer_auth(token);
            }
            GraphqlAuth::Custom { headers } => {
                let mut hm = reqwest::header::HeaderMap::new();
                for (name, value) in &headers {
                    let n =
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                            FaucetError::Auth(format!("invalid custom header name {name:?}: {e}"))
                        })?;
                    let v = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                        FaucetError::Auth(format!("invalid custom header value for {name:?}: {e}"))
                    })?;
                    hm.insert(n, v);
                }
                req = req.headers(hm);
            }
        }

        // Retry transient failures (5xx / connection resets) with jittered
        // backoff, matching the REST source's reliability layer (#78/#16).
        // GraphQL-level `errors` in a 200 body are application errors and are
        // handled below — they are not retried here.
        let body: Value = faucet_core::execute_with_policy(&self.retry_policy, None, || {
            let attempt = req.try_clone();
            async move {
                let req = attempt.ok_or_else(|| {
                    FaucetError::Source("graphql: request is not cloneable for retry".into())
                })?;
                let resp = req.send().await.map_err(FaucetError::Http)?;
                let resp = util::check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await?;
                resp.json().await.map_err(FaucetError::Http)
            }
        })
        .await?;

        // Check for GraphQL-level errors.
        if let Some(errors) = body.get("errors")
            && let Some(arr) = errors.as_array()
            && !arr.is_empty()
        {
            return Err(graphql_error_from_payload(
                arr,
                sentinel_page_size_variable(&self.config.pagination, self.config.batch_size),
                &self.config.endpoint,
            ));
        }

        Ok(body)
    }

    /// Extract records from a GraphQL response using the configured JSONPath.
    fn extract_records(&self, body: &Value) -> Result<Vec<Value>, FaucetError> {
        match &self.config.records_path {
            Some(path) => util::extract_records(body, Some(path)),
            None => {
                // GraphQL-specific: return the `data` field as a single
                // record. A `data` that is JSON null (or absent entirely)
                // means there is nothing to extract — emit an empty page
                // rather than forwarding a bogus null record to the sink
                // (#146 LOW).
                match body.get("data") {
                    Some(Value::Null) | None => Ok(Vec::new()),
                    Some(data) => Ok(vec![data.clone()]),
                }
            }
        }
    }

    /// Core pagination loop yielded as a [`StreamPage`] stream.
    ///
    /// Each upstream GraphQL response → one [`StreamPage`]. The page size
    /// variable in the request comes from [`GraphqlStreamConfig::batch_size`];
    /// `batch_size = 0` omits it so the upstream uses its own default page
    /// size and emits a single page.
    ///
    /// Bookmarks are always `None` — the GraphQL source has no
    /// incremental-replication mode today. The
    /// [`bookmark_emitted`-style trailing-checkpoint](https://github.com/faucet-hq/faucet-stream/commit/e6fdca5)
    /// guard from the REST source is preserved structurally so any future
    /// incremental mode picks it up without re-deriving the pattern.
    fn stream_pages_inner(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + '_>> {
        // Own the context so it can live inside the async-stream generator.
        let owned_context: std::collections::HashMap<String, Value> = context.clone();

        Box::pin(async_stream::try_stream! {
            let mut cursor: Option<String> = None;
            let mut offset = 0usize;
            let mut cursor_guard = CursorGuard::new();
            let mut pages_fetched = 0usize;
            let mut warned_unresolved_has_next = false;
            // No incremental replication today — `running_max` stays `None`.
            // The structure mirrors the REST source so a future replication
            // mode can plug into the same scaffolding without reworking the
            // bookmark guard.
            let running_max: Option<Value> = None;
            let mut bookmark_emitted = false;

            loop {
                if let Some(max) = self.config.max_pages
                    && pages_fetched >= max
                {
                    tracing::warn!("max pages ({max}) reached");
                    break;
                }

                let body = self.execute_query(&cursor, offset, &owned_context).await?;
                let records = self.extract_records(&body)?;
                let records_in_page = records.len();
                pages_fetched += 1;

                // Advance pagination state BEFORE yielding the current page,
                // so the bookmark is only attached on the final page.
                let has_next = match &self.config.pagination {
                    Some(GraphqlPaginationSpec::Cursor(pag)) => {
                        let (step, unresolved) =
                            decide_next_page(&body, pag, cursor.as_deref());
                        if unresolved && !warned_unresolved_has_next {
                            tracing::warn!(
                                path = %pag.has_next_page_path,
                                "GraphQL has_next_page path did not resolve to a boolean; \
                                 deferring to cursor presence to decide pagination"
                            );
                            warned_unresolved_has_next = true;
                        }
                        match step {
                            PageStep::Stop => false,
                            PageStep::StopLoop => {
                                tracing::warn!("cursor loop detected, stopping pagination");
                                false
                            }
                            PageStep::Advance(next) => {
                                if cursor_guard.is_repeat(&next) {
                                    tracing::warn!(
                                        "cursor cycle detected (cursor already seen), stopping pagination"
                                    );
                                    false
                                } else {
                                    cursor = Some(next);
                                    true
                                }
                            }
                        }
                    }
                    Some(GraphqlPaginationSpec::Offset(off)) => {
                        let advance = offset_should_continue(records_in_page, off);
                        if advance {
                            offset += off.page_size;
                        }
                        advance
                    }
                    None => false,
                };

                if has_next {
                    // Intermediate page — bookmark stays `None`.
                    yield StreamPage { records, bookmark: None };
                } else {
                    // Final page — attach the consolidated bookmark (always
                    // `None` until incremental mode lands).
                    bookmark_emitted = running_max.is_some();
                    yield StreamPage {
                        records,
                        bookmark: running_max.clone(),
                    };
                    break;
                }
            }

            // Trailing checkpoint: if the loop exited (e.g. via `max_pages`
            // truncation) without carrying the bookmark on a real page, emit
            // one empty page carrying it so the pipeline persists progress.
            // No-op today because `running_max` is always `None`, but kept so
            // a future incremental mode inherits the guard from the REST
            // source's regression fix (commit e6fdca5).
            if !bookmark_emitted && running_max.is_some() {
                yield StreamPage {
                    records: Vec::new(),
                    bookmark: running_max,
                };
            }

            tracing::info!(
                pages = pages_fetched,
                batch_size = self.config.batch_size,
                "GraphQL source stream complete",
            );
        })
    }
}

#[async_trait]
impl faucet_core::Source for GraphqlStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.fetch_all_with_context(context).await
    }

    /// Stream GraphQL responses page-by-page without buffering the full
    /// result set. The trait-level `batch_size` argument is ignored in
    /// favour of [`GraphqlStreamConfig::batch_size`] — the config field is
    /// the user-facing knob the README documents, and routing the
    /// pipeline-supplied hint through it would silently override an
    /// explicit config value.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        self.stream_pages_inner(context)
    }

    fn connector_name(&self) -> &'static str {
        "graphql"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(GraphqlStreamConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        faucet_core::redact_uri_credentials(&self.config.endpoint)
    }
}

fn extract_string(body: &Value, path: &str) -> Option<String> {
    let results = body.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn extract_bool(body: &Value, path: &str) -> Option<bool> {
    let results = body.query(path).ok()?;
    results.first()?.as_bool()
}

/// `extensions.code` values that mean "the request itself was rejected" — an
/// operator-fixable fault rather than a server-side condition. The `extensions`
/// map is part of the GraphQL spec and `code` is the de-facto standard key every
/// major server implementation populates, so this is a *typed* classification,
/// not a prose match (PRINCIPLES.md §6).
const CONFIG_ERROR_CODES: &[&str] = &[
    "BAD_REQUEST",
    "BAD_USER_INPUT",
    "GRAPHQL_PARSE_FAILED",
    "GRAPHQL_VALIDATION_FAILED",
];

/// Which `FaucetError` variant a GraphQL `errors[]` payload maps to.
#[derive(Debug, PartialEq, Eq)]
enum GraphqlErrorClass {
    /// A typed request-rejection code — the caller's config is wrong.
    Config,
    /// Everything else. Keeps the transport-shaped classification the source
    /// has always used for a `200` carrying application errors.
    Transport,
}

/// Verdict for a GraphQL `errors[]` payload.
#[derive(Debug, PartialEq, Eq)]
struct GraphqlErrorVerdict {
    class: GraphqlErrorClass,
    /// Append the `batch_size = 0` page-size hint to the error message.
    page_size_hint: bool,
}

/// Concatenate the `message` field of every GraphQL error, in order.
fn join_error_messages(errors: &[Value]) -> String {
    errors
        .iter()
        .filter_map(|e| e.get("message").and_then(|m| m.as_str()))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Pure classification of a GraphQL `errors[]` array (#654 M3).
///
/// The variant is decided **only** from the typed `extensions.code`
/// ([`CONFIG_ERROR_CODES`]); a server that ships no code keeps the transport
/// classification. Error prose never flips the variant, because prose is not
/// API — a reworded validator used to silently turn a server error into a
/// config error (and vice versa).
///
/// `page_size_variable` is `Some` only when the `batch_size = 0` sentinel is
/// live (cursor pagination, no page size sent). When the errors mention that
/// variable, the caller appends a hint naming it. The variable name comes from
/// our own config and GraphQL validation errors are specified to name the
/// offending variable, so this is a stable signal — and because it only adds
/// *context* to the message, a miss degrades the diagnostic instead of changing
/// how the error is handled.
fn classify_graphql_errors(
    errors: &[Value],
    page_size_variable: Option<&str>,
) -> GraphqlErrorVerdict {
    let class = if errors.iter().any(|e| {
        e.get("extensions")
            .and_then(|x| x.get("code"))
            .and_then(|c| c.as_str())
            .is_some_and(|code| {
                CONFIG_ERROR_CODES
                    .iter()
                    .any(|known| known.eq_ignore_ascii_case(code))
            })
    }) {
        GraphqlErrorClass::Config
    } else {
        GraphqlErrorClass::Transport
    };
    let page_size_hint = page_size_variable.is_some_and(|var| {
        !var.is_empty()
            && join_error_messages(errors)
                .to_lowercase()
                .contains(&var.to_lowercase())
    });
    GraphqlErrorVerdict {
        class,
        page_size_hint,
    }
}

/// The page-size variable name whose absence the `batch_size = 0` sentinel
/// causes, or `None` when the sentinel is not in play.
///
/// The sentinel only omits a page size under **cursor** pagination — the one
/// style that sends one — so any other pagination (or a real `batch_size`)
/// means a validation error naming that variable has some other cause and must
/// not be explained away with the sentinel hint. Pure.
fn sentinel_page_size_variable(
    pagination: &Option<GraphqlPaginationSpec>,
    batch_size: usize,
) -> Option<&str> {
    match (pagination, batch_size) {
        (Some(GraphqlPaginationSpec::Cursor(pag)), 0) => Some(pag.page_size_variable.as_str()),
        _ => None,
    }
}

/// Build the error for a `200` response carrying a non-empty GraphQL `errors[]`.
///
/// Pure, and deliberately the *whole* decision: which `FaucetError` variant,
/// what the message says, and whether the `batch_size = 0` hint is appended.
/// Keeping it out of the async fetch path is what makes every branch reachable
/// from a unit test (PRINCIPLES §7) — the fetch path is left with one call.
fn graphql_error_from_payload(
    errors: &[Value],
    page_size_variable: Option<&str>,
    endpoint: &str,
) -> FaucetError {
    let msg = join_error_messages(errors);
    let verdict = classify_graphql_errors(errors, page_size_variable);
    let hint = match (verdict.page_size_hint, page_size_variable) {
        (true, Some(var)) => format!(
            " (hint: batch_size = 0 omits the {var}: argument — set an explicit \
             batch_size if the upstream requires a non-null value)"
        ),
        _ => String::new(),
    };
    let body = format!("GraphQL errors: {msg}{hint}");
    match verdict.class {
        GraphqlErrorClass::Config => FaucetError::Config(body),
        // A `200` carrying application errors keeps the transport-shaped
        // classification the source has always used for it.
        GraphqlErrorClass::Transport => FaucetError::HttpStatus {
            status: 200,
            url: endpoint.to_string(),
            body,
        },
    }
}

/// What to do after fetching a page.
#[derive(Debug, PartialEq)]
enum PageStep {
    /// No further pages (has-next is `false`, or there is no next cursor).
    Stop,
    /// The server returned the cursor we just used — advancing would re-fetch
    /// the same page. Caller warns and stops.
    StopLoop,
    /// Fetch another page with this cursor.
    Advance(String),
}

/// Pure pagination-advance decision shared by the eager and streaming paths.
///
/// `prev_cursor` is the cursor just used (for loop detection). The returned
/// bool is `true` when the configured `has_next_page_path` did **not** resolve
/// to a boolean: that is treated as "can't tell" and we **defer to cursor
/// presence** rather than silently stopping — an unmatched has-next path must
/// not drop the remaining pages of a paginated result (F52). The caller warns
/// once on that condition.
fn decide_next_page(
    body: &Value,
    pag: &GraphqlPagination,
    prev_cursor: Option<&str>,
) -> (PageStep, bool) {
    let (stop, unresolved) = match extract_bool(body, &pag.has_next_page_path) {
        Some(false) => (true, false),
        Some(true) => (false, false),
        // Path absent / not a boolean: defer the decision to the cursor signal.
        None => (false, true),
    };
    if stop {
        return (PageStep::Stop, unresolved);
    }
    match extract_string(body, &pag.cursor_path) {
        None => (PageStep::Stop, unresolved),
        Some(next) if Some(next.as_str()) == prev_cursor => (PageStep::StopLoop, unresolved),
        Some(next) => (PageStep::Advance(next), unresolved),
    }
}

/// Pure offset-pagination advance decision.
///
/// Returns `true` when another page should be fetched (the caller then advances
/// the offset by `page_size`), `false` to stop. Termination rules:
///
/// - A **fully empty** page (0 records) always stops — this is the unconditional
///   loop guard that keeps `stop_when_short: false` from paginating forever.
/// - With `stop_when_short` (the default), a **short** page — fewer than
///   `page_size` records — is the last one and stops pagination.
/// - Otherwise (a full page, or `stop_when_short: false` with a non-empty page)
///   pagination continues.
fn offset_should_continue(records_in_page: usize, off: &GraphqlOffsetPagination) -> bool {
    if records_in_page == 0 {
        return false;
    }
    if off.stop_when_short && records_in_page < off.page_size {
        return false;
    }
    true
}

/// Bounded record of recently-advanced pagination cursors.
///
/// [`decide_next_page`] only compares against the *immediately previous* cursor,
/// so a server that returns `hasNextPage: true` while cycling its cursor across
/// two or more values (`c1→c2→c1→c2…`) would never trip that guard and — with
/// `max_pages` unset — paginate forever, re-emitting the same pages (#466 M2).
/// This catches any such cycle by remembering the cursors already seen.
///
/// Bounded to [`Self::CAP`] entries so the streaming path keeps its O(page)
/// memory guarantee on a legitimately large result set (whose cursors are all
/// distinct, so eviction never causes a false positive). A cycle length beyond
/// the cap is not realistic for a real endpoint.
struct CursorGuard {
    seen: HashMap<String, ()>,
    order: std::collections::VecDeque<String>,
}

impl CursorGuard {
    const CAP: usize = 4096;

    fn new() -> Self {
        Self {
            seen: HashMap::new(),
            order: std::collections::VecDeque::new(),
        }
    }

    /// Record `cursor`; return `true` if it had already been seen (a cycle).
    fn is_repeat(&mut self, cursor: &str) -> bool {
        if self.seen.contains_key(cursor) {
            return true;
        }
        if self.order.len() >= Self::CAP
            && let Some(old) = self.order.pop_front()
        {
            self.seen.remove(&old);
        }
        self.seen.insert(cursor.to_string(), ());
        self.order.push_back(cursor.to_string());
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_string_from_json() {
        let body = json!({"data": {"users": {"pageInfo": {"endCursor": "abc123"}}}});
        assert_eq!(
            extract_string(&body, "$.data.users.pageInfo.endCursor"),
            Some("abc123".into())
        );
    }

    #[test]
    fn extract_bool_from_json() {
        let body = json!({"data": {"users": {"pageInfo": {"hasNextPage": true}}}});
        assert_eq!(
            extract_bool(&body, "$.data.users.pageInfo.hasNextPage"),
            Some(true)
        );
    }

    fn pageinfo_pagination() -> GraphqlPagination {
        GraphqlPagination {
            has_next_page_path: "$.data.users.pageInfo.hasNextPage".into(),
            cursor_path: "$.data.users.pageInfo.endCursor".into(),
            ..GraphqlPagination::default()
        }
    }

    #[test]
    fn decide_next_page_advances_when_has_next_true() {
        let body =
            json!({"data": {"users": {"pageInfo": {"hasNextPage": true, "endCursor": "c2"}}}});
        let (step, unresolved) = decide_next_page(&body, &pageinfo_pagination(), Some("c1"));
        assert_eq!(step, PageStep::Advance("c2".into()));
        assert!(!unresolved);
    }

    #[test]
    fn decide_next_page_stops_when_has_next_false() {
        let body =
            json!({"data": {"users": {"pageInfo": {"hasNextPage": false, "endCursor": "c2"}}}});
        let (step, unresolved) = decide_next_page(&body, &pageinfo_pagination(), Some("c1"));
        assert_eq!(step, PageStep::Stop);
        assert!(!unresolved);
    }

    #[test]
    fn decide_next_page_detects_cursor_loop() {
        let body =
            json!({"data": {"users": {"pageInfo": {"hasNextPage": true, "endCursor": "c1"}}}});
        let (step, _) = decide_next_page(&body, &pageinfo_pagination(), Some("c1"));
        assert_eq!(step, PageStep::StopLoop);
    }

    #[test]
    fn decide_next_page_defers_to_cursor_when_has_next_unresolved() {
        // F52: an absent / non-boolean has-next path must NOT silently stop
        // pagination. With a valid distinct next cursor we keep going, and the
        // unresolved flag is raised so the caller warns once.
        let body = json!({"data": {"users": {"pageInfo": {"endCursor": "c2"}}}}); // no hasNextPage
        let (step, unresolved) = decide_next_page(&body, &pageinfo_pagination(), Some("c1"));
        assert_eq!(
            step,
            PageStep::Advance("c2".into()),
            "unresolved has-next must defer to cursor presence, not stop"
        );
        assert!(unresolved, "the caller is told to warn once");

        // Unresolved has-next AND no cursor → genuinely stop (nothing to follow).
        let body_no_cursor = json!({"data": {"users": {"pageInfo": {}}}});
        let (step, unresolved) =
            decide_next_page(&body_no_cursor, &pageinfo_pagination(), Some("c1"));
        assert_eq!(step, PageStep::Stop);
        assert!(unresolved);
    }

    fn offset_pagination(page_size: usize, stop_when_short: bool) -> GraphqlOffsetPagination {
        GraphqlOffsetPagination {
            r#type: crate::config::OffsetPaginationKind::Offset,
            offset_variable: "q_offset".into(),
            page_size,
            stop_when_short,
            substitute_in_query: false,
        }
    }

    #[test]
    fn offset_continues_on_full_page() {
        // A page filled to page_size means there may be more — keep going.
        assert!(offset_should_continue(250, &offset_pagination(250, true)));
    }

    #[test]
    fn offset_stops_on_short_page_when_stop_when_short() {
        // Fewer than page_size records with stop_when_short: the final page.
        assert!(!offset_should_continue(100, &offset_pagination(250, true)));
    }

    #[test]
    fn offset_continues_on_short_page_when_not_stop_when_short() {
        // stop_when_short: false keeps paginating on a non-empty short page.
        assert!(offset_should_continue(100, &offset_pagination(250, false)));
    }

    #[test]
    fn offset_always_stops_on_empty_page() {
        // An empty page terminates regardless of stop_when_short (loop guard).
        assert!(!offset_should_continue(0, &offset_pagination(250, true)));
        assert!(!offset_should_continue(0, &offset_pagination(250, false)));
    }

    #[test]
    fn offset_exact_page_size_is_full_not_short() {
        // records_in_page == page_size is a full page (>= not <), so continue.
        assert!(offset_should_continue(1, &offset_pagination(1, true)));
    }

    #[test]
    fn extract_records_with_path() {
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { users { id } }")
                .records_path("$.data.users[*]");
        let stream = GraphqlStream::new(config);
        let body = json!({"data": {"users": [{"id": 1}, {"id": 2}]}});
        let records = stream.extract_records(&body).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_records_without_path_returns_data() {
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { user { id } }");
        let stream = GraphqlStream::new(config);
        let body = json!({"data": {"user": {"id": 1}}});
        let records = stream.extract_records(&body).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["user"]["id"], 1);
    }

    #[test]
    fn extract_records_without_path_null_data_yields_empty() {
        // A response of `{"data": null}` must NOT emit a bogus null record:
        // `data` being JSON null means there is nothing to extract, so the
        // page is empty (#146 LOW).
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { user { id } }");
        let stream = GraphqlStream::new(config);
        let body = json!({ "data": null });
        let records = stream.extract_records(&body).unwrap();
        assert!(
            records.is_empty(),
            "expected empty Vec for null `data`, got {records:?}"
        );
    }

    #[test]
    fn extract_records_without_path_absent_data_yields_empty() {
        // No `data` field at all → nothing to extract → empty page (matches
        // the null-data case rather than forwarding the whole body).
        let config =
            GraphqlStreamConfig::new("https://api.example.com/graphql", "query { user { id } }");
        let stream = GraphqlStream::new(config);
        let body = json!({ "extensions": { "foo": 1 } });
        let records = stream.extract_records(&body).unwrap();
        assert!(
            records.is_empty(),
            "expected empty Vec when `data` is absent, got {records:?}"
        );
    }

    #[test]
    fn dataset_uri_returns_endpoint() {
        use faucet_core::Source;
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { id }",
        ));
        assert_eq!(stream.dataset_uri(), "https://api.example.com/graphql");
    }

    #[test]
    fn dataset_uri_redacts_credentials() {
        use faucet_core::Source;
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://user:pw@api.example.com/graphql",
            "query { id }",
        ));
        assert_eq!(stream.dataset_uri(), "https://api.example.com/graphql");
    }

    #[test]
    fn default_retry_policy_reproduces_legacy_constants() {
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { id }",
        ));
        assert_eq!(stream.retry_policy.max_attempts, RETRY_MAX_ATTEMPTS + 1);
        assert_eq!(stream.retry_policy.base, RETRY_BASE_BACKOFF);
    }

    #[test]
    fn with_retry_policy_overrides_the_default() {
        let policy = faucet_core::RetryPolicy {
            max_attempts: 9,
            base: Duration::from_secs(7),
            ..faucet_core::RetryPolicy::default()
        };
        let stream = GraphqlStream::new(GraphqlStreamConfig::new(
            "https://api.example.com/graphql",
            "query { id }",
        ))
        .with_retry_policy(policy);
        assert_eq!(stream.retry_policy.max_attempts, 9);
        assert_eq!(stream.retry_policy.base, Duration::from_secs(7));
    }

    #[test]
    fn cursor_guard_detects_repeats_and_bounds_memory() {
        let mut g = CursorGuard::new();
        assert!(!g.is_repeat("a"));
        assert!(!g.is_repeat("b"));
        // Any earlier cursor repeating is a cycle, not just the adjacent one.
        assert!(g.is_repeat("a"));
        assert!(g.is_repeat("b"));

        // Bounded: after CAP distinct cursors, the oldest is evicted, so the
        // set never grows without bound on a legitimately large pagination.
        let mut g = CursorGuard::new();
        for i in 0..CursorGuard::CAP {
            assert!(!g.is_repeat(&format!("c{i}")));
        }
        assert_eq!(g.order.len(), CursorGuard::CAP);
        // One more distinct cursor evicts the oldest ("c0").
        assert!(!g.is_repeat("overflow"));
        assert_eq!(g.order.len(), CursorGuard::CAP);
        assert!(!g.seen.contains_key("c0"), "oldest cursor evicted");
        assert!(g.seen.contains_key("overflow"));
    }

    // ─── GraphQL error classification (#654 M3) ──────────────────────────────

    fn errs(v: Value) -> Vec<Value> {
        v.as_array().expect("array fixture").clone()
    }

    #[test]
    fn join_error_messages_concatenates_and_skips_non_strings() {
        let errors = errs(json!([
            {"message": "first bad"},
            {"extensions": {"code": "X"}},
            {"message": 42},
            {"message": "then worse"}
        ]));
        assert_eq!(join_error_messages(&errors), "first bad; then worse");
    }

    #[test]
    fn classify_bad_user_input_code_is_config() {
        let errors = errs(json!([
            {"message": "unknown argument", "extensions": {"code": "BAD_USER_INPUT"}}
        ]));
        assert_eq!(
            classify_graphql_errors(&errors, None),
            GraphqlErrorVerdict {
                class: GraphqlErrorClass::Config,
                page_size_hint: false,
            }
        );
    }

    #[test]
    fn classify_recognises_every_config_code_case_insensitively() {
        for code in CONFIG_ERROR_CODES {
            let errors =
                errs(json!([{"message": "nope", "extensions": {"code": code.to_lowercase()}}]));
            assert_eq!(
                classify_graphql_errors(&errors, None).class,
                GraphqlErrorClass::Config,
                "{code} must classify as config regardless of case"
            );
        }
    }

    #[test]
    fn classify_uses_a_config_code_from_any_position() {
        let errors = errs(json!([
            {"message": "transient", "extensions": {"code": "INTERNAL_SERVER_ERROR"}},
            {"message": "bad arg", "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}}
        ]));
        assert_eq!(
            classify_graphql_errors(&errors, None).class,
            GraphqlErrorClass::Config
        );
    }

    #[test]
    fn classify_non_config_code_stays_transport() {
        let errors = errs(json!([
            {"message": "boom", "extensions": {"code": "INTERNAL_SERVER_ERROR"}}
        ]));
        assert_eq!(
            classify_graphql_errors(&errors, None).class,
            GraphqlErrorClass::Transport
        );
    }

    #[test]
    fn classify_non_string_code_stays_transport() {
        // A server that puts a non-string in `code` must not be mis-typed.
        let errors = errs(json!([{"message": "boom", "extensions": {"code": 400}}]));
        assert_eq!(
            classify_graphql_errors(&errors, None).class,
            GraphqlErrorClass::Transport
        );
    }

    #[test]
    fn classify_does_not_reclassify_non_null_prose_without_extensions() {
        // The behaviour change: prose alone never flips the variant. The
        // diagnostic survives as a hint instead.
        let errors = errs(json!([
            {"message": "Variable \"$first\" of required type \"Int!\" must not be null."}
        ]));
        assert_eq!(
            classify_graphql_errors(&errors, Some("first")),
            GraphqlErrorVerdict {
                class: GraphqlErrorClass::Transport,
                page_size_hint: true,
            }
        );
    }

    #[test]
    fn classify_unrelated_error_is_untouched() {
        let errors = errs(json!([{"message": "upstream timed out talking to the database"}]));
        assert_eq!(
            classify_graphql_errors(&errors, Some("first")),
            GraphqlErrorVerdict {
                class: GraphqlErrorClass::Transport,
                page_size_hint: false,
            }
        );
    }

    #[test]
    fn classify_omits_hint_when_sentinel_is_not_live() {
        // `page_size_variable: None` means batch_size != 0 (or no cursor
        // pagination) — the hint would be misleading there.
        let errors = errs(json!([
            {"message": "Variable \"$first\" of required type \"Int!\" must not be null."}
        ]));
        assert!(!classify_graphql_errors(&errors, None).page_size_hint);
    }

    #[test]
    fn classify_hint_matches_variable_name_case_insensitively() {
        let errors = errs(json!([{"message": "Variable \"$pageSize\" is required."}]));
        assert!(classify_graphql_errors(&errors, Some("PAGESIZE")).page_size_hint);
    }

    #[test]
    fn classify_empty_variable_name_never_hints() {
        let errors = errs(json!([{"message": "anything at all"}]));
        assert!(!classify_graphql_errors(&errors, Some("")).page_size_hint);
    }

    // ─── The pure error builder behind the fetch path ───────────────────────

    /// Cursor pagination naming `var` as its page-size variable.
    fn cursor_pagination(var: &str) -> Option<GraphqlPaginationSpec> {
        Some(GraphqlPaginationSpec::Cursor(GraphqlPagination {
            page_size_variable: var.into(),
            ..GraphqlPagination::default()
        }))
    }

    #[test]
    fn sentinel_variable_only_under_cursor_pagination_with_batch_size_zero() {
        // The sentinel is live: cursor pagination + batch_size 0.
        assert_eq!(
            sentinel_page_size_variable(&cursor_pagination("first"), 0),
            Some("first")
        );
        // A real batch_size sends the variable, so an error naming it has some
        // other cause and must not be explained away.
        assert_eq!(
            sentinel_page_size_variable(&cursor_pagination("first"), 50),
            None
        );
        // Offset pagination never sends a page-size variable at all.
        assert_eq!(
            sentinel_page_size_variable(
                &Some(GraphqlPaginationSpec::Offset(offset_pagination(100, true))),
                0
            ),
            None
        );
        // No pagination configured.
        assert_eq!(sentinel_page_size_variable(&None, 0), None);
    }

    #[test]
    fn payload_error_is_transport_with_the_endpoint_and_joined_messages() {
        let errors = errs(json!([{"message": "upstream exploded"}, {"message": "and again"}]));
        match graphql_error_from_payload(&errors, None, "https://api.example/graphql") {
            FaucetError::HttpStatus { status, url, body } => {
                assert_eq!(status, 200, "a GraphQL error arrives on a 200");
                assert_eq!(url, "https://api.example/graphql");
                assert_eq!(body, "GraphQL errors: upstream exploded; and again");
            }
            other => panic!("expected HttpStatus, got {other:?}"),
        }
    }

    #[test]
    fn payload_error_is_config_for_a_typed_request_rejection() {
        let errors = errs(json!([
            {"message": "unknown argument", "extensions": {"code": "BAD_USER_INPUT"}}
        ]));
        match graphql_error_from_payload(&errors, None, "https://api.example/graphql") {
            FaucetError::Config(body) => {
                assert_eq!(body, "GraphQL errors: unknown argument");
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn payload_error_appends_the_hint_only_when_the_sentinel_is_live() {
        let errors = errs(json!([
            {"message": "Variable \"$first\" of required type \"Int!\" must not be null."}
        ]));
        // Sentinel live and the message names the variable — hint appended, and
        // the variant stays transport (prose never flips it).
        let with_hint = graphql_error_from_payload(&errors, Some("first"), "https://e/g");
        let msg = with_hint.to_string();
        assert!(
            msg.contains("hint: batch_size = 0 omits the first: argument"),
            "hint missing: {msg}"
        );
        assert!(matches!(with_hint, FaucetError::HttpStatus { .. }));

        // Same errors, sentinel not live — no hint.
        let without = graphql_error_from_payload(&errors, None, "https://e/g").to_string();
        assert!(
            !without.contains("hint:"),
            "hint must not appear: {without}"
        );
    }

    #[test]
    fn payload_error_composes_a_config_code_with_the_hint() {
        let errors = errs(json!([
            {
                "message": "Variable \"$first\" was not provided.",
                "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}
            }
        ]));
        match graphql_error_from_payload(&errors, Some("first"), "https://e/g") {
            FaucetError::Config(body) => {
                assert!(body.contains("was not provided"), "{body}");
                assert!(body.contains("hint: batch_size = 0"), "{body}");
            }
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn payload_error_survives_errors_with_no_message_field() {
        // A server that ships only `extensions` leaves the joined message empty;
        // the error must still be well-formed rather than panicking or losing
        // the classification.
        let errors = errs(json!([{"extensions": {"code": "BAD_REQUEST"}}]));
        match graphql_error_from_payload(&errors, None, "https://e/g") {
            FaucetError::Config(body) => assert_eq!(body, "GraphQL errors: "),
            other => panic!("expected Config, got {other:?}"),
        }
    }

    #[test]
    fn classify_config_code_and_hint_compose() {
        let errors = errs(json!([
            {
                "message": "Variable \"$first\" of required type \"Int!\" was not provided.",
                "extensions": {"code": "GRAPHQL_VALIDATION_FAILED"}
            }
        ]));
        assert_eq!(
            classify_graphql_errors(&errors, Some("first")),
            GraphqlErrorVerdict {
                class: GraphqlErrorClass::Config,
                page_size_hint: true,
            }
        );
    }
}

/// Mutual-TLS unit tests (#495) — lib-level for reliable llvm-cov attribution.
#[cfg(all(test, feature = "mtls"))]
mod mtls_tests {
    use super::*;
    use faucet_core::TlsClientConfig;

    const CERT: &str = include_str!("../tests/fixtures/mtls/cert.pem");
    const KEY: &str = include_str!("../tests/fixtures/mtls/key.pem");

    fn pem() -> TlsClientConfig {
        TlsClientConfig {
            client_cert: Some(CERT.to_string()),
            client_key: Some(KEY.to_string()),
            ..Default::default()
        }
    }

    fn cfg(tls: TlsClientConfig) -> GraphqlStreamConfig {
        GraphqlStreamConfig::new("https://x.test/graphql", "{ ping }").tls(tls)
    }

    #[test]
    fn pem_identity_builds() {
        assert!(GraphqlStream::try_new(cfg(pem())).is_ok());
    }

    #[test]
    fn min_version_branches_are_exercised() {
        let mut tls = pem();
        tls.min_version = Some("1.2".into());
        assert!(GraphqlStream::try_new(cfg(tls)).is_ok());
        // 1.3 exercises the other branch; some native-tls backends reject a 1.3
        // floor at build time, so only require it not to panic.
        let mut tls = pem();
        tls.min_version = Some("1.3".into());
        let _ = GraphqlStream::try_new(cfg(tls));
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
        assert!(GraphqlStream::try_new(cfg(tls)).is_ok());
    }

    #[test]
    fn invalid_pem_errors_without_leaking_key() {
        let tls = TlsClientConfig {
            client_cert: Some("-----BEGIN CERTIFICATE-----\nbad\n-----END CERTIFICATE-----".into()),
            client_key: Some("SUPERSECRETKEY".into()),
            ..Default::default()
        };
        let err = GraphqlStream::try_new(cfg(tls))
            .map(|_| ())
            .expect_err("bad PEM must error");
        assert!(!err.to_string().contains("SUPERSECRETKEY"));
    }

    #[test]
    fn invalid_tls_shape_errors() {
        let mut tls = pem();
        tls.client_identity_pkcs12 = Some("/x.p12".into());
        assert!(GraphqlStream::try_new(cfg(tls)).is_err());
    }

    #[test]
    fn missing_pkcs12_file_errors() {
        let tls = TlsClientConfig {
            client_identity_pkcs12: Some("/no/such.p12".into()),
            pkcs12_password: Some("x".into()),
            ..Default::default()
        };
        assert!(GraphqlStream::try_new(cfg(tls)).is_err());
    }

    #[test]
    fn config_validate_checks_tls() {
        assert!(cfg(pem()).validate().is_ok());
        let mut bad = pem();
        bad.client_identity_pkcs12 = Some("/x.p12".into());
        assert!(cfg(bad).validate().is_err());
    }
}

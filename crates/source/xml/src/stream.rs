//! XML stream executor.

use crate::config::{XmlAuth, XmlPagination, XmlStreamConfig};
use crate::convert;
use async_trait::async_trait;
use faucet_core::driver::{DriverSource, PageStream, SourceDriver};
use faucet_core::http::Client;
use faucet_core::io::{CallContext, CallSlot};
use faucet_core::util::{self, DEFAULT_ERROR_BODY_MAX_LEN};
use faucet_core::{AuthSpec, Credential, CredentialPlacement, FaucetError, SharedAuthProvider};
use faucet_core::{Stream, StreamPage};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::time::Duration;

/// Content fingerprint of a fetched page, used as a pagination loop guard: a
/// server that ignores the page/offset parameter (or clamps to the last page)
/// returns the same non-empty page on every request, which would otherwise loop
/// forever. Stopping when two consecutive pages fingerprint identically mirrors
/// the REST source's body-fingerprint guard (audit #146 H4/H5).
fn page_fingerprint(records: &[Value]) -> u64 {
    use std::hash::{Hash, Hasher};
    // `serde_json::Value` is not `Hash`; hash its canonical string form.
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    records.len().hash(&mut hasher);
    for r in records {
        r.to_string().hash(&mut hasher);
    }
    hasher.finish()
}

/// Render an XML request-body template in one pass: `${name}` takes a
/// flow-captured login value (#567) or, for `name = next_token`, the body-cursor
/// token; `{key}` takes a parent-record value. Every substituted value is
/// XML-escaped and never re-scanned, so a value such as `A&B Ltd` or one
/// containing markup or another placeholder cannot change the request (API-25).
fn render_xml_body(
    template: &str,
    context: &HashMap<String, Value>,
    captured: &BTreeMap<String, String>,
    next_token: Option<&str>,
) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open + 1..].find('}').map(|c| open + 1 + c) else {
            break;
        };
        let key = &rest[open + 1..close];
        let dollar = rest[..open].ends_with('$');
        let dollar_value = if dollar {
            captured
                .get(key)
                .map(String::as_str)
                .or(next_token.filter(|_| key == "next_token"))
        } else {
            None
        };
        let value = match dollar_value {
            Some(v) => {
                out.push_str(&rest[..open - 1]);
                Some(v.to_string())
            }
            None => context.get(key).map(|v| {
                out.push_str(&rest[..open]);
                match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                }
            }),
        };
        match value {
            Some(v) => {
                out.push_str(&quick_xml::escape::escape(v.as_str()));
                rest = &rest[close + 1..];
            }
            None => {
                out.push_str(&rest[..=open]);
                rest = &rest[open + 1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Substitute `${name}` captured login values into a header or query value
/// (no XML escaping — these are not XML).
fn substitute_captured(s: &str, captured: &BTreeMap<String, String>) -> String {
    if captured.is_empty() || !s.contains("${") {
        return s.to_string();
    }
    let mut out = s.to_string();
    for (k, v) in captured {
        out = out.replace(&format!("${{{k}}}"), v);
    }
    out
}

/// A SOAP endpoint's HTTP 500: a `<Fault>` body is returned for fault handling
/// (never retried — re-POSTing a failed operation is not safe, API-46); any
/// other 500 is the usual retriable status error.
async fn soap_fault_or_error(
    client: &Client,
    resp: reqwest::Response,
) -> Result<String, FaucetError> {
    let url = resp.url().to_string();
    let text = client.text(resp).await?;
    if convert::xml_to_json(&text)
        .ok()
        .and_then(|doc| convert::detect_soap_fault(&doc))
        .is_some()
    {
        return Ok(text);
    }
    let body = if text.len() > DEFAULT_ERROR_BODY_MAX_LEN {
        let end = text.floor_char_boundary(DEFAULT_ERROR_BODY_MAX_LEN);
        format!("{}...(truncated)", &text[..end])
    } else {
        text
    };
    Err(FaucetError::HttpStatus {
        status: 500,
        url,
        body,
    })
}

/// Retries on transient (5xx / connection) failures before giving up.
const RETRY_MAX_ATTEMPTS: u32 = 3;
/// Base exponential-backoff delay between retries.
const RETRY_BASE_BACKOFF: Duration = Duration::from_millis(500);

/// A configured XML API source that handles pagination and extraction.
pub struct XmlStream {
    inner: DriverSource<XmlDriver>,
}

/// The connector's I/O primitives (RFC 0014).
pub(crate) struct XmlDriver {
    config: XmlStreamConfig,
    client: Client,
    /// Optional shared auth provider. When present it takes precedence over
    /// inline auth, so several sources can share one token with single-flight
    /// refresh. Used by the CLI to resolve `auth: { ref }`, and by library
    /// callers who construct one provider and inject it into many sources.
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
         faucet-source-xml lacks the `mtls` feature; rebuild with `--features mtls`"
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

/// Map a [`Credential`] from a shared provider onto the XML [`XmlAuth`]
/// representation so the existing header-application path can be reused.
fn credential_to_auth(cred: Credential) -> XmlAuth {
    match cred {
        Credential::Bearer(token) => XmlAuth::Bearer { token },
        Credential::Token(token) => XmlAuth::Custom {
            headers: std::iter::once(("Authorization".to_string(), token)).collect(),
        },
        Credential::Basic { username, password } => XmlAuth::Basic { username, password },
        Credential::Header { name, value } => XmlAuth::Custom {
            headers: std::iter::once((name, value)).collect(),
        },
    }
}

/// Fill `{key}` placeholders in a URL path with percent-encoded values, so a
/// parent value holding `/`, `?` or `#` stays inside its segment instead of
/// addressing another resource; a value of `.` or `..` is refused.
fn substitute_path(
    template: &str,
    ctx: &HashMap<String, serde_json::Value>,
) -> Result<String, FaucetError> {
    let mut encoded = HashMap::with_capacity(ctx.len());
    for (k, v) in ctx {
        let raw = match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if template.contains(&format!("{{{k}}}")) && (raw == "." || raw == "..") {
            return Err(FaucetError::Source(format!(
                "xml: path placeholder `{{{k}}}` resolved to `{raw}`, which would change the \
                 request path"
            )));
        }
        encoded.insert(
            k.clone(),
            serde_json::Value::String(encode_path_segment(&raw)),
        );
    }
    Ok(faucet_core::util::substitute_context(template, &encoded))
}

/// Percent-encode everything but RFC 3986 unreserved characters.
fn encode_path_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl XmlStream {
    /// Create a new XML stream from the given configuration.
    ///
    /// Infallible for the common case. Prefer [`try_new`](Self::try_new) when the
    /// config may carry a `tls:` (mutual-TLS) block: this panics if the client
    /// (or the TLS identity) fails to build, matching the pre-existing
    /// `Client::new()` behavior.
    pub fn new(config: XmlStreamConfig) -> Self {
        Self {
            inner: DriverSource::new(XmlDriver::new(config)),
        }
    }

    /// Fallible constructor — builds the HTTP client, including any mutual-TLS
    /// client identity. The CLI registry uses this so a bad `tls:` block surfaces
    /// as a typed error instead of a panic.
    ///
    /// Note: this does **not** run the full [`XmlStreamConfig::validate`] (SOAP
    /// checks) — that stays at fetch time, unchanged — it only validates and
    /// applies the `tls:` block, so `new()` remains infallible for non-TLS
    /// configs exactly as before.
    pub fn try_new(config: XmlStreamConfig) -> Result<Self, FaucetError> {
        Ok(Self {
            inner: DriverSource::new(XmlDriver::try_new(config)?),
        })
    }

    /// Attach a custom [`RetryPolicy`](faucet_core::RetryPolicy) for transient
    /// request failures, replacing the default derived from
    /// `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_BACKOFF`. Used by the CLI to inject a
    /// pipeline-level `resilience:` policy into the source.
    pub fn with_retry_policy(self, policy: faucet_core::RetryPolicy) -> Self {
        let mut driver = self.inner.into_driver();
        driver.retry_policy = policy;
        Self {
            inner: DriverSource::new(driver),
        }
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking precedence
    /// over inline auth), so several sources can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and by
    /// library callers who construct one provider and inject it into many
    /// sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.inner.driver_mut().auth_provider = Some(provider);
        self
    }

    /// Fetch all records across all pages.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.inner
            .driver()
            .fetch_all_with_context(self.inner.calls(), &HashMap::new())
            .await
    }

    #[cfg(test)]
    fn effective_records_path(&self) -> Option<String> {
        self.inner.driver().effective_records_path()
    }

    #[cfg(test)]
    fn extract_records_eager(
        &self,
        xml_text: &str,
        fault_logged: &mut bool,
    ) -> Result<Vec<Value>, FaucetError> {
        self.inner
            .driver()
            .extract_records_eager(xml_text, fault_logged)
    }
}

#[async_trait]
impl faucet_core::Source for XmlStream {
    faucet_core::forward_source!(inner);
}

impl XmlDriver {
    fn new(config: XmlStreamConfig) -> Self {
        Self::try_new(config)
            .expect("XmlStream::new: client build failed; use try_new() for fallible construction")
    }

    fn try_new(config: XmlStreamConfig) -> Result<Self, FaucetError> {
        let mut builder = Client::builder();
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        if let Some(t) = config.connect_timeout {
            builder = builder.connect_timeout(t);
        }
        if let Some(tls) = &config.tls {
            tls.validate()?;
            builder = builder.try_configure(|b| apply_client_tls(b, tls))?;
        }
        let client = builder
            .build()
            .map_err(|e| FaucetError::Config(format!("xml: failed to build HTTP client: {e}")))?;
        // Reproduce the legacy `execute_with_retry(RETRY_MAX_ATTEMPTS,
        // RETRY_BASE_BACKOFF, …)` behavior exactly: `max_retries` is
        // retries-after-first, so `max_attempts = RETRY_MAX_ATTEMPTS + 1`.
        let retry_policy = faucet_core::RetryPolicy {
            max_attempts: RETRY_MAX_ATTEMPTS + 1,
            backoff: faucet_core::BackoffKind::Exponential,
            base: RETRY_BASE_BACKOFF,
            max: Duration::from_secs(60),
            jitter: true,
            retry_on: faucet_core::RetryClassSet::default(),
        };
        Ok(Self {
            config,
            client,
            auth_provider: None,
            retry_policy,
        })
    }

    /// The effective record element path after applying SOAP ergonomics.
    ///
    /// When a `soap:` block is present with `path_relative_to_body` (the
    /// default), the configured `records_element_path` is resolved relative to
    /// the SOAP body — `Envelope.Body.` is prepended so the user writes
    /// `GetUsersResponse.Users.User`. Otherwise the configured path is used
    /// verbatim (the non-SOAP behavior).
    fn effective_records_path(&self) -> Option<String> {
        match (&self.config.soap, &self.config.records_element_path) {
            (Some(soap), Some(path)) if soap.path_relative_to_body => {
                Some(format!("Envelope.Body.{path}"))
            }
            (_, path) => path.clone(),
        }
    }

    /// A SOAP `<Fault>`: an error with `fault_as_error`, else zero records and
    /// one warning per run.
    fn soap_fault(
        &self,
        message: String,
        fault_logged: &mut bool,
    ) -> Result<Vec<Value>, FaucetError> {
        if self.config.soap.as_ref().is_some_and(|s| s.fault_as_error) {
            return Err(FaucetError::Source(format!("SOAP fault: {message}")));
        }
        if !*fault_logged {
            tracing::warn!(
                fault = %message,
                "SOAP fault in response; emitting zero records (fault_as_error=false)"
            );
            *fault_logged = true;
        }
        Ok(Vec::new())
    }

    /// Eagerly convert one HTTP page of XML to JSON and extract its records,
    /// applying SOAP fault handling when a `soap:` block is present.
    ///
    /// When `soap` is absent this reproduces the legacy eager path exactly
    /// (`xml_to_json` + `extract_at_path`), so non-SOAP behavior is unchanged.
    /// When `soap` is present it additionally detects a SOAP `<Fault>` under
    /// `Envelope.Body`: with `fault_as_error` it raises
    /// [`FaucetError::Source`]; otherwise it emits zero records and logs the
    /// fault once (tracked via `fault_logged`).
    fn extract_records_eager(
        &self,
        xml_text: &str,
        fault_logged: &mut bool,
    ) -> Result<Vec<Value>, FaucetError> {
        let doc = convert::xml_to_json(xml_text)?;

        if self.config.soap.is_some()
            && let Some(message) = convert::detect_soap_fault(&doc)
        {
            return self.soap_fault(message, fault_logged);
        }

        let records = match (&self.config.soap, &self.config.records_element_path) {
            (Some(soap), Some(path)) => {
                convert::extract_soap_records(&doc, path, soap.path_relative_to_body)?
            }
            _ => match self.effective_records_path() {
                Some(path) => convert::extract_at_path(&doc, &path),
                None => vec![doc],
            },
        };
        Ok(records)
    }

    /// Fetch all records, substituting parent context into path, query_params, and body.
    async fn fetch_all_with_context(
        &self,
        calls: &CallSlot,
        context: &HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        use futures::StreamExt;
        let mut all_records = Vec::new();
        let mut pages = self.http_pages(calls, context);
        while let Some(page) = pages.next().await {
            all_records.extend(page?);
        }
        tracing::info!(records = all_records.len(), "XML fetch complete");
        Ok(all_records)
    }

    /// The records of one HTTP response: the decode pipeline's output when one
    /// is configured, else the elements at `records_element_path`. With a
    /// `soap:` block a `<Fault>` is detected first.
    async fn page_records(
        &self,
        xml_text: &str,
        fault_logged: &mut bool,
    ) -> Result<Vec<Value>, FaucetError> {
        if self.config.soap.is_some() {
            if self.config.decode.is_empty() {
                return self.extract_records_eager(xml_text, fault_logged);
            }
            let doc = convert::xml_to_json(xml_text)?;
            if let Some(message) = convert::detect_soap_fault(&doc) {
                return self.soap_fault(message, fault_logged);
            }
        }
        if !self.config.decode.is_empty() {
            return crate::decode::run_decode(xml_text.as_bytes(), &self.config.decode).await;
        }
        let mut records = Vec::new();
        convert::stream_extract(
            xml_text,
            self.config.records_element_path.as_deref(),
            |rec| records.push(rec),
        )?;
        Ok(records)
    }

    /// One item per HTTP response, for every pagination mode, so a caller
    /// holds at most one response's records at a time.
    fn http_pages<'a>(
        &'a self,
        calls: &'a CallSlot,
        context: &'a HashMap<String, Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<Value>, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            self.config.validate()?;
            let mut pages_fetched = 0usize;
            let mut offset = 0usize;
            let mut page_number = match &self.config.pagination {
                Some(XmlPagination::PageNumber { start_page, .. }) => Some(*start_page),
                _ => None,
            };
            let mut prev_fingerprint: Option<u64> = None;
            let mut fault_logged = false;
            // Body-cursor pagination (#544): the token for the next request;
            // `None` on the first page → the configured body/soap is sent.
            let mut token: Option<String> = None;

            loop {
                if let Some(max) = self.config.max_pages
                    && pages_fetched >= max
                {
                    tracing::warn!("max pages ({max}) reached");
                    break;
                }

                let mut params = self.config.query_params.clone();
                self.apply_pagination_params(&mut params, page_number, offset);
                let next = match (&self.config.pagination, token.as_deref()) {
                    (Some(XmlPagination::BodyCursor { next_body, .. }), Some(t)) => {
                        Some((next_body.as_str(), t))
                    }
                    _ => None,
                };
                let xml_text = self.execute_request(calls, &params, context, next).await?;
                let records = self.page_records(&xml_text, &mut fault_logged).await?;
                let record_count = records.len();
                let fingerprint = page_fingerprint(&records);
                pages_fetched += 1;

                // Loop guard: a server that ignores the page/offset parameter
                // (or clamps to the last page) returns the same non-empty page
                // forever (#146 H4/H5); stop before emitting the duplicate
                // (#321 M4).
                if record_count > 0 && prev_fingerprint == Some(fingerprint) {
                    tracing::warn!(
                        "XML pagination returned an identical page; stopping to avoid an infinite loop"
                    );
                    break;
                }
                prev_fingerprint = Some(fingerprint);

                // Advance pagination. Without a total, only an empty page ends
                // page/offset paging: a short page may just be the server
                // clamping the requested size (API-15).
                let more = match &self.config.pagination {
                    Some(XmlPagination::PageNumber { page_size, stop_when_short, .. }) => {
                        page_number = page_number.map(|p| p + 1);
                        record_count > 0
                            && !short_page(*stop_when_short, record_count, *page_size)
                    }
                    Some(XmlPagination::Offset { limit, stop_when_short, .. }) => {
                        offset += record_count;
                        record_count > 0
                            && !short_page(*stop_when_short, record_count, Some(*limit))
                    }
                    Some(XmlPagination::BodyCursor { next_token_path, .. }) => {
                        match crate::decode::xml_extract_text(xml_text.as_bytes(), next_token_path)? {
                            Some(t)
                                if !t.trim().is_empty() && token.as_deref() != Some(t.as_str()) =>
                            {
                                token = Some(t);
                                true
                            }
                            _ => false,
                        }
                    }
                    None => false,
                };
                yield records;
                if !more {
                    break;
                }
            }
        })
    }

    fn apply_pagination_params(
        &self,
        params: &mut HashMap<String, String>,
        page_number: Option<usize>,
        offset: usize,
    ) {
        match &self.config.pagination {
            Some(XmlPagination::PageNumber {
                param_name,
                page_size,
                page_size_param,
                ..
            }) => {
                if let Some(page) = page_number {
                    params.insert(param_name.clone(), page.to_string());
                }
                if let (Some(size), Some(param)) = (page_size, page_size_param) {
                    params.insert(param.clone(), size.to_string());
                }
            }
            Some(XmlPagination::Offset {
                offset_param,
                limit_param,
                limit,
                ..
            }) => {
                params.insert(offset_param.clone(), offset.to_string());
                params.insert(limit_param.clone(), limit.to_string());
            }
            // Body-cursor paging carries its token in the request body, not the
            // query string — nothing to add here.
            Some(XmlPagination::BodyCursor { .. }) => {}
            None => {}
        }
    }

    /// Run one request; when a shared provider's credential is rejected, have
    /// the provider re-authenticate and run it once more (#789 API-06).
    async fn execute_request(
        &self,
        calls: &CallSlot,
        params: &HashMap<String, String>,
        context: &HashMap<String, serde_json::Value>,
        next: Option<(&str, &str)>,
    ) -> Result<String, FaucetError> {
        match self
            .execute_request_once(calls, params, context, next)
            .await
        {
            Err(e)
                if self
                    .auth_provider
                    .as_ref()
                    .is_some_and(|p| faucet_core::rejects_credential(&e, p.as_ref())) =>
            {
                let provider = self.auth_provider.as_ref().expect("checked above");
                let name = provider.provider_name();
                tracing::warn!(
                    provider = name,
                    "the server rejected the shared credential; re-authenticating and retrying once"
                );
                provider
                    .invalidate(&faucet_core::Credential::Token(String::new()))
                    .await?;
                self.execute_request_once(calls, params, context, next)
                    .await
            }
            other => other,
        }
    }

    async fn execute_request_once(
        &self,
        calls: &CallSlot,
        params: &HashMap<String, String>,
        context: &HashMap<String, serde_json::Value>,
        next: Option<(&str, &str)>,
    ) -> Result<String, FaucetError> {
        let path = if context.is_empty() {
            self.config.path.clone()
        } else {
            substitute_path(&self.config.path, context)?
        };

        // #567 rich per-request auth: a flow provider may override the base-URL,
        // place credentials across header/query/cookie, and expose captured
        // login values for `${name}` substitution into the raw body/headers
        // (an XML-gateway `sessionid`). When it contributes anything it supersedes
        // the plain credential() path below.
        let mut base_url = self.config.base_url.clone();
        let mut ra_headers: Vec<(String, String)> = Vec::new();
        let mut ra_query: Vec<(String, String)> = Vec::new();
        let mut ra_cookies: Vec<(String, String)> = Vec::new();
        let mut captured: BTreeMap<String, String> = BTreeMap::new();
        let mut used_request_auth = false;
        if let Some(provider) = &self.auth_provider {
            let q: BTreeMap<String, String> =
                params.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            let ra = provider
                .request_auth(self.config.method.as_str(), &base_url, &q)
                .await?;
            if !ra.is_empty() {
                used_request_auth = true;
                if let Some(b) = ra.base_url {
                    base_url = b;
                }
                for p in ra.placements {
                    match p {
                        CredentialPlacement::Header { name, value } => {
                            ra_headers.push((name, value))
                        }
                        CredentialPlacement::Query { name, value } => ra_query.push((name, value)),
                        CredentialPlacement::Cookie { name, value } => {
                            ra_cookies.push((name, value))
                        }
                        // BodyField is JSON-body-specific; an XML body carries a
                        // captured value via `${name}` substitution instead.
                        _ => {}
                    }
                }
                captured = ra.captured;
            }
        }

        let url = format!("{}/{}", base_url, path.trim_start_matches('/'));

        // Query values: parent context + captured substitution, then the flow
        // provider's query placements.
        let mut resolved_params: HashMap<String, String> = params
            .iter()
            .map(|(k, v)| {
                let v = if context.is_empty() {
                    v.clone()
                } else {
                    faucet_core::util::substitute_context(v, context)
                };
                (k.clone(), substitute_captured(&v, &captured))
            })
            .collect();
        for (k, v) in ra_query {
            resolved_params.insert(k, v);
        }

        // Config headers with captured substitution, plus the flow provider's
        // header + cookie placements.
        let mut header_map = reqwest::header::HeaderMap::new();
        for (name, value) in self.config.headers.iter() {
            let sv = substitute_captured(value.to_str().unwrap_or_default(), &captured);
            match reqwest::header::HeaderValue::from_str(&sv) {
                Ok(hv) => header_map.insert(name.clone(), hv),
                Err(_) => header_map.insert(name.clone(), value.clone()),
            };
        }
        for (name, value) in &ra_headers {
            if let (Ok(n), Ok(v)) = (
                reqwest::header::HeaderName::from_bytes(name.as_bytes()),
                reqwest::header::HeaderValue::from_str(value),
            ) {
                header_map.insert(n, v);
            }
        }
        if !ra_cookies.is_empty() {
            let cookie = ra_cookies
                .iter()
                .map(|(n, v)| format!("{n}={v}"))
                .collect::<Vec<_>>()
                .join("; ");
            if let Ok(v) = reqwest::header::HeaderValue::from_str(&cookie) {
                header_map.insert(reqwest::header::COOKIE, v);
            }
        }

        let mut req = self
            .client
            .request(self.config.method.clone(), &url)
            .headers(header_map)
            .query(&resolved_params);

        // Resolve inline / single credentials — unless the flow provider already
        // supplied the request auth above.
        if !used_request_auth {
            let effective_auth: XmlAuth = if let Some(provider) = &self.auth_provider {
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

            match &effective_auth {
                XmlAuth::None => {}
                XmlAuth::Bearer { token } => {
                    req = req.bearer_auth(token);
                }
                XmlAuth::Basic { username, password } => {
                    req = req.basic_auth(username, Some(password));
                }
                XmlAuth::Custom { headers } => {
                    let mut hm = reqwest::header::HeaderMap::new();
                    for (name, value) in headers {
                        let n = reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(
                            |e| {
                                FaucetError::Auth(format!(
                                    "invalid custom header name {name:?}: {e}"
                                ))
                            },
                        )?;
                        let mut v = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                            FaucetError::Auth(format!(
                                "invalid custom header value for {name:?}: {e}"
                            ))
                        })?;
                        v.set_sensitive(true);
                        hm.insert(n, v);
                    }
                    req = req.headers(hm);
                }
            }
        }

        // Set the request body for POST (SOAP), with context substitution.
        //
        // A `soap:` block takes precedence: it assembles the envelope and
        // injects the version-appropriate headers (Content-Type + SOAPAction).
        // These headers are set here regardless of the `auth` variant, so real
        // bearer / basic auth (applied above) is left untouched. Otherwise the
        // legacy raw-`body` path is used verbatim (byte-for-byte unchanged).
        if let Some((next_body, token)) = next {
            // #544 body-cursor: a rendered `next_body` replaces the request body
            // for pages after the first (e.g. a `readMore` request). Takes
            // precedence over the configured soap/raw body.
            req = req
                .header("Content-Type", "text/xml; charset=utf-8")
                .body(render_xml_body(next_body, context, &captured, Some(token)));
        } else if let Some(soap) = &self.config.soap {
            let inner = soap.body_inner.as_deref().unwrap_or("");
            let envelope = soap.build_envelope(&render_xml_body(inner, context, &captured, None));
            req = req
                .header("Content-Type", soap.content_type())
                .body(envelope);
            if let Some(action) = soap.soap_action_header() {
                req = req.header("SOAPAction", action);
            }
        } else if let Some(body) = &self.config.body {
            req = req
                .header("Content-Type", "text/xml; charset=utf-8")
                .body(render_xml_body(body, context, &captured, None));
        }

        // Retry transient failures (5xx / connection resets) with jittered
        // backoff, matching the REST source's reliability layer (#78/#16).
        // The request body is a String, so `try_clone` always succeeds.
        calls
            .call("request", || {
                let attempt = req.try_clone();
                async move {
                    let req = attempt.ok_or_else(|| {
                        FaucetError::Source("xml: request is not cloneable for retry".into())
                    })?;
                    let resp = self.client.send(req).await?;
                    if self.config.soap.is_some()
                        && resp.status() == reqwest::StatusCode::INTERNAL_SERVER_ERROR
                    {
                        return soap_fault_or_error(&self.client, resp).await;
                    }
                    let resp =
                        util::check_http_response_rate_limited(resp, DEFAULT_ERROR_BODY_MAX_LEN)
                            .await?;
                    self.client.text(resp).await
                }
            })
            .await
    }
}

#[async_trait]
impl SourceDriver for XmlDriver {
    /// This source's retry policy over the run's cancel token, recorder and
    /// deadline (#880). Meters `request` round trips and throttling (#638,
    /// #734).
    fn call_context(&self) -> CallContext {
        CallContext::new().with_retry(self.retry_policy.clone())
    }

    async fn fetch(
        &self,
        calls: &CallSlot,
        context: &HashMap<String, Value>,
    ) -> Option<Result<(Vec<Value>, Option<Value>), FaucetError>> {
        Some(
            self.fetch_all_with_context(calls, context)
                .await
                .map(|records| (records, None)),
        )
    }

    /// Stream records from the XML response without materialising the whole
    /// document tree. The event-driven parser only builds JSON values for
    /// elements matching [`XmlStreamConfig::records_element_path`]; other
    /// elements are observed and discarded, so client-side memory is bounded
    /// at `O(batch_size * record_size)` regardless of how large the document
    /// is.
    ///
    /// Records are accumulated into a buffer of
    /// [`XmlStreamConfig::batch_size`] entries and yielded as a
    /// [`StreamPage`] once the buffer is full. The trailing partial buffer
    /// (if any) is emitted after the parser hits EOF and all pagination
    /// rounds drain.
    ///
    /// The trait-level `batch_size` argument is intentionally ignored in
    /// favour of the config field — the config is the user-facing knob the
    /// README documents, and routing the pipeline-supplied hint through it
    /// would silently override an explicit config value. `batch_size = 0`
    /// drains every page into a single emitted page.
    ///
    /// Bookmarks are always `None` — the XML source has no
    /// incremental-replication mode today; pagination only walks the
    /// API's own page-number / offset cursor.
    fn read<'a>(
        &'a self,
        calls: &'a CallSlot,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> PageStream<'a> {
        let batch_size = self.config.batch_size;
        let chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt;
            let mut buffer: Vec<Value> = Vec::new();
            let mut total = 0usize;
            let mut pages = self.http_pages(calls, context);
            while let Some(page) = pages.next().await {
                for rec in page? {
                    buffer.push(rec);
                    if buffer.len() >= chunk {
                        let flush = std::mem::take(&mut buffer);
                        total += flush.len();
                        yield StreamPage { records: flush, bookmark: None };
                    }
                }
            }
            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }
            tracing::info!(records = total, batch_size, "XML source stream complete");
        })
    }

    fn connector_name(&self) -> &'static str {
        "xml"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(XmlStreamConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}{}",
            faucet_core::redact_uri_credentials(&self.config.base_url),
            self.config.path
        )
    }
}

/// Whether an opted-in `stop_when_short` ends paging: the page holds fewer
/// records than the requested size. Without a known size it never stops.
fn short_page(stop_when_short: bool, record_count: usize, size: Option<usize>) -> bool {
    stop_when_short && size.is_some_and(|n| record_count < n)
}

#[cfg(test)]
mod tests {

    #[test]
    fn path_placeholders_are_percent_encoded() {
        let ctx = HashMap::from([
            (
                "acct".to_string(),
                serde_json::json!("../admin/export?all=1#"),
            ),
            ("n".to_string(), serde_json::json!(7)),
        ]);
        assert_eq!(
            substitute_path("/accounts/{acct}/invoices/{n}", &ctx).unwrap(),
            "/accounts/..%2Fadmin%2Fexport%3Fall%3D1%23/invoices/7"
        );
        let dots = HashMap::from([("acct".to_string(), serde_json::json!(".."))]);
        let err = substitute_path("/accounts/{acct}", &dots).unwrap_err();
        assert!(
            err.to_string().contains("would change the request path"),
            "{err}"
        );
        assert_eq!(substitute_path("/fixed", &dots).unwrap(), "/fixed");
    }
    use super::*;

    #[test]
    fn short_page_stops_only_when_opted_in_with_a_known_size() {
        assert!(short_page(true, 2, Some(5)));
        assert!(!short_page(true, 5, Some(5)));
        assert!(!short_page(false, 2, Some(5)));
        assert!(!short_page(true, 2, None));
    }
    use crate::config::{SoapConfig, SoapVersion};
    use faucet_core::Source;

    #[test]
    fn body_templates_keep_unknown_and_unclosed_braces_and_render_non_strings() {
        let ctx: HashMap<String, Value> = [("n".to_string(), serde_json::json!(42))]
            .into_iter()
            .collect();
        let none = BTreeMap::new();
        assert_eq!(
            render_xml_body("<a>{n}</a><b>{missing}</b>", &ctx, &none, None),
            "<a>42</a><b>{missing}</b>"
        );
        assert_eq!(render_xml_body("<a>{n</a>", &ctx, &none, None), "<a>{n</a>");
    }

    fn soap_response(records: &str) -> String {
        format!(
            "<Envelope xmlns=\"http://schemas.xmlsoap.org/soap/envelope/\"><Body>\
             <GetUsersResponse><Users>{records}</Users></GetUsersResponse></Body></Envelope>"
        )
    }

    #[test]
    fn effective_path_prepends_envelope_body_by_default() {
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc")
                .method(reqwest::Method::POST)
                .records_element_path("GetUsersResponse.Users.User")
                .with_soap(SoapConfig {
                    body_inner: Some("<Op/>".into()),
                    ..Default::default()
                }),
        );
        assert_eq!(
            source.effective_records_path().as_deref(),
            Some("Envelope.Body.GetUsersResponse.Users.User")
        );
    }

    #[test]
    fn effective_path_absolute_override_when_not_relative() {
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc")
                .method(reqwest::Method::POST)
                .records_element_path("Envelope.Body.GetUsersResponse.Users.User")
                .with_soap(SoapConfig {
                    body_inner: Some("<Op/>".into()),
                    path_relative_to_body: false,
                    ..Default::default()
                }),
        );
        assert_eq!(
            source.effective_records_path().as_deref(),
            Some("Envelope.Body.GetUsersResponse.Users.User")
        );
    }

    #[test]
    fn effective_path_unchanged_without_soap() {
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc").records_element_path("root.item"),
        );
        assert_eq!(
            source.effective_records_path().as_deref(),
            Some("root.item")
        );
    }

    #[test]
    fn extract_records_eager_resolves_relative_soap_path() {
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc")
                .method(reqwest::Method::POST)
                .records_element_path("GetUsersResponse.Users.User")
                .with_soap(SoapConfig {
                    body_inner: Some("<Op/>".into()),
                    ..Default::default()
                }),
        );
        let xml = soap_response("<User><Name>Alice</Name></User><User><Name>Bob</Name></User>");
        let mut logged = false;
        let records = source.extract_records_eager(&xml, &mut logged).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["Name"], "Alice");
        assert_eq!(records[1]["Name"], "Bob");
    }

    #[test]
    fn extract_records_eager_fault_as_error_raises_source_error() {
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc")
                .method(reqwest::Method::POST)
                .records_element_path("GetUsersResponse.Users.User")
                .with_soap(SoapConfig {
                    body_inner: Some("<Op/>".into()),
                    ..Default::default()
                }),
        );
        let xml = r#"<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"><Body>
            <Fault><faultcode>Server</faultcode><faultstring>kaboom</faultstring></Fault>
        </Body></Envelope>"#;
        let mut logged = false;
        let err = source.extract_records_eager(xml, &mut logged).unwrap_err();
        assert!(
            matches!(&err, FaucetError::Source(m) if m.contains("SOAP fault") && m.contains("kaboom")),
            "got {err:?}"
        );
    }

    #[test]
    fn extract_records_eager_fault_not_error_yields_zero_records() {
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc")
                .method(reqwest::Method::POST)
                .records_element_path("GetUsersResponse.Users.User")
                .with_soap(SoapConfig {
                    body_inner: Some("<Op/>".into()),
                    fault_as_error: false,
                    ..Default::default()
                }),
        );
        let xml = r#"<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"><Body>
            <Fault><faultstring>ignored</faultstring></Fault>
        </Body></Envelope>"#;
        let mut logged = false;
        let records = source.extract_records_eager(xml, &mut logged).unwrap();
        assert!(records.is_empty());
        assert!(logged, "fault should be recorded as logged");
    }

    #[test]
    fn extract_records_eager_non_soap_matches_legacy_eager_path() {
        // Regression: with no soap block, extraction is byte-for-byte the
        // legacy xml_to_json + extract_at_path behavior.
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc").records_element_path("root.item"),
        );
        let xml = "<root><item><id>1</id></item><item><id>2</id></item></root>";
        let mut logged = false;
        let records = source.extract_records_eager(xml, &mut logged).unwrap();
        let legacy = convert::extract_at_path(&convert::xml_to_json(xml).unwrap(), "root.item");
        assert_eq!(records, legacy);
        assert_eq!(records.len(), 2);
    }

    #[tokio::test]
    async fn fetch_all_rejects_invalid_soap_config() {
        // A soap block with the default GET method fails validation before any
        // request is attempted.
        let source = XmlStream::new(
            XmlStreamConfig::new("https://s", "/svc").with_soap(SoapConfig::default()),
        );
        let err = source.fetch_all().await.unwrap_err();
        assert!(matches!(&err, FaucetError::Config(_)), "got {err:?}");
    }

    #[test]
    fn soap12_content_type_used_for_envelope() {
        // Sanity: a 1.2 soap block produces the 1.2 content type.
        let soap = SoapConfig {
            version: SoapVersion::Soap12,
            action: Some("urn:Op".into()),
            ..Default::default()
        };
        assert!(soap.content_type().starts_with("application/soap+xml"));
    }

    #[test]
    fn dataset_uri_combines_base_and_path() {
        let source = XmlStream::new(XmlStreamConfig::new(
            "https://soap.example.com",
            "/api/v1/service",
        ));
        assert_eq!(
            source.dataset_uri(),
            "https://soap.example.com/api/v1/service"
        );
    }

    #[test]
    fn dataset_uri_redacts_credentials() {
        let source = XmlStream::new(XmlStreamConfig::new(
            "https://user:pass@soap.example.com",
            "/svc",
        ));
        assert_eq!(source.dataset_uri(), "https://soap.example.com/svc");
    }

    #[test]
    fn default_retry_policy_reproduces_legacy_constants() {
        let source = XmlStream::new(XmlStreamConfig::new("https://soap.example.com", "/svc"));
        assert_eq!(
            source.inner.driver().retry_policy.max_attempts,
            RETRY_MAX_ATTEMPTS + 1
        );
        assert_eq!(source.inner.driver().retry_policy.base, RETRY_BASE_BACKOFF);
    }

    #[test]
    fn with_retry_policy_overrides_the_default() {
        let policy = faucet_core::RetryPolicy {
            max_attempts: 9,
            base: Duration::from_secs(7),
            ..faucet_core::RetryPolicy::default()
        };
        let source = XmlStream::new(XmlStreamConfig::new("https://soap.example.com", "/svc"))
            .with_retry_policy(policy);
        assert_eq!(source.inner.driver().retry_policy.max_attempts, 9);
        assert_eq!(
            source.inner.driver().retry_policy.base,
            Duration::from_secs(7)
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

    #[test]
    fn pem_identity_builds() {
        let cfg = XmlStreamConfig::new("https://x.test", "/y").tls(pem());
        assert!(XmlStream::try_new(cfg).is_ok());
    }

    #[test]
    fn min_version_branches_are_exercised() {
        let mut tls = pem();
        tls.min_version = Some("1.2".into());
        assert!(XmlStream::try_new(XmlStreamConfig::new("https://x.test", "/y").tls(tls)).is_ok());
        // 1.3 exercises the other branch; some native-tls backends reject a 1.3
        // floor at build time, so only require it not to panic.
        let mut tls = pem();
        tls.min_version = Some("1.3".into());
        let _ = XmlStream::try_new(XmlStreamConfig::new("https://x.test", "/y").tls(tls));
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
        let cfg = XmlStreamConfig::new("https://x.test", "/y").tls(tls);
        assert!(XmlStream::try_new(cfg).is_ok());
    }

    #[test]
    fn invalid_pem_errors_without_leaking_key() {
        let tls = TlsClientConfig {
            client_cert: Some("-----BEGIN CERTIFICATE-----\nbad\n-----END CERTIFICATE-----".into()),
            client_key: Some("SUPERSECRETKEY".into()),
            ..Default::default()
        };
        let cfg = XmlStreamConfig::new("https://x.test", "/y").tls(tls);
        let err = XmlStream::try_new(cfg)
            .map(|_| ())
            .expect_err("bad PEM must error");
        assert!(!err.to_string().contains("SUPERSECRETKEY"));
    }

    #[test]
    fn invalid_tls_shape_errors() {
        // Both PEM and PKCS#12 set → validation error.
        let mut tls = pem();
        tls.client_identity_pkcs12 = Some("/x.p12".into());
        let cfg = XmlStreamConfig::new("https://x.test", "/y").tls(tls);
        assert!(XmlStream::try_new(cfg).is_err());
    }

    #[test]
    fn missing_pkcs12_file_errors() {
        let tls = TlsClientConfig {
            client_identity_pkcs12: Some("/no/such.p12".into()),
            pkcs12_password: Some("x".into()),
            ..Default::default()
        };
        let cfg = XmlStreamConfig::new("https://x.test", "/y").tls(tls);
        assert!(XmlStream::try_new(cfg).is_err());
    }

    #[test]
    fn config_validate_checks_tls() {
        assert!(
            XmlStreamConfig::new("https://x.test", "/y")
                .tls(pem())
                .validate()
                .is_ok()
        );
        let mut bad = pem();
        bad.client_identity_pkcs12 = Some("/x.p12".into());
        assert!(
            XmlStreamConfig::new("https://x.test", "/y")
                .tls(bad)
                .validate()
                .is_err()
        );
    }
}

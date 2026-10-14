//! HTTP sink executor.

use crate::config::{HttpBatchMode, HttpSinkAuth, HttpSinkConfig};
use async_trait::async_trait;
use faucet_core::driver::{DriverSink, HttpBatching, HttpDriver, HttpSinkDriver};
use faucet_core::http::Client;
use faucet_core::io::{CallContext, CallSlot};
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response_rate_limited};
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider};
use serde_json::Value;
use std::collections::HashMap;

/// Map a [`Credential`] from a shared provider onto the [`HttpSinkAuth`]
/// representation so the existing header-application path can be reused.
fn credential_to_auth(cred: Credential) -> HttpSinkAuth {
    match cred {
        Credential::Bearer(token) => HttpSinkAuth::Bearer { token },
        Credential::Token(token) => HttpSinkAuth::Custom {
            headers: HashMap::from([("Authorization".to_string(), token)]),
        },
        Credential::Basic { username, password } => HttpSinkAuth::Basic { username, password },
        Credential::Header { name, value } => HttpSinkAuth::Custom {
            headers: HashMap::from([(name, value)]),
        },
    }
}

/// An HTTP sink that sends records to an HTTP endpoint.
///
/// Built on the HTTP family driver (RFC 0014): `Endpoint` authenticates a
/// page and sends one body; [`HttpSinkDriver`] drives the batch mode, the
/// in-flight limit and which rows a failed request leaves undelivered.
pub struct HttpSink {
    inner: DriverSink<HttpSinkDriver<Endpoint>>,
}

/// The HTTP I/O primitives the HTTP family driver runs on.
pub(crate) struct Endpoint {
    config: HttpSinkConfig,
    client: Client,
    /// Optional shared auth provider. When set, it takes precedence over inline
    /// auth. Set via [`HttpSink::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
}

/// One page's credentials: the auth every request applies, and the shared
/// provider's credential it came from (so a rejected request can tell the
/// provider which one went stale).
pub(crate) struct Session {
    auth: HttpSinkAuth,
    sent: Option<Credential>,
}

/// Base delay for a retried request; the cap and jitter come from the core
/// retry runner, so this sink cannot drift from the rest of the repo's backoff
/// behaviour.
const RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(250);

/// The retry policy for `max_retries`: a `RateLimited` error waits the
/// server's own `Retry-After`; everything else gets capped, jittered
/// exponential backoff. Retrying with **no** delay (the original behaviour)
/// turned a brief upstream 503 into an amplifying burst of requests within
/// microseconds — the thundering herd core's backoff exists to prevent.
fn retry_policy(max_retries: usize) -> faucet_core::RetryPolicy {
    faucet_core::RetryPolicy {
        max_attempts: u32::try_from(max_retries)
            .unwrap_or(u32::MAX)
            .saturating_add(1),
        backoff: faucet_core::BackoffKind::Exponential,
        base: RETRY_BASE,
        max: std::time::Duration::from_secs(60),
        jitter: true,
        retry_on: faucet_core::RetryClassSet::default(),
    }
}

impl HttpSink {
    /// Create a new HTTP sink from the given configuration.
    pub fn new(config: HttpSinkConfig) -> Self {
        let mut builder = Client::builder();
        if let Some(t) = config.timeout {
            builder = builder.timeout(t);
        }
        if let Some(t) = config.connect_timeout {
            builder = builder.connect_timeout(t);
        }
        if let HttpSinkAuth::Custom { headers } = match &config.auth {
            AuthSpec::Inline(a) => a,
            AuthSpec::Reference(_) => &HttpSinkAuth::None,
        } {
            for name in headers.keys() {
                if reqwest::header::HeaderName::from_bytes(name.as_bytes()).is_ok() {
                    builder = builder.sensitive_header(name);
                }
            }
        }
        let endpoint = Endpoint {
            client: builder
                .build()
                .expect("an HTTP client with only timeouts set always builds"),
            config,
            auth_provider: None,
        };
        Self {
            inner: DriverSink::new(HttpSinkDriver::new(endpoint)),
        }
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sinks can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and
    /// by library callers who construct one provider and inject it into many
    /// sinks.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.inner.driver_mut().driver_mut().auth_provider = Some(provider);
        self
    }

    #[cfg(test)]
    fn build_request_with_auth(
        &self,
        body: &Value,
        auth: &HttpSinkAuth,
    ) -> Result<reqwest::RequestBuilder, FaucetError> {
        self.inner
            .driver()
            .driver()
            .build_request_with_auth(body, auth)
    }
}

#[async_trait]
impl faucet_core::Sink for HttpSink {
    faucet_core::forward_sink!(inner);
}

impl Endpoint {
    /// Resolve the effective auth for the current batch (the provider, if
    /// any, takes precedence over inline auth; a bare `AuthSpec::Reference`
    /// with no provider is an error), plus the shared provider's credential it
    /// came from, so a rejected request can tell the provider which one went
    /// stale.
    async fn resolve_auth_and_credential(
        &self,
    ) -> Result<(HttpSinkAuth, Option<Credential>), FaucetError> {
        if let Some(provider) = &self.auth_provider {
            let cred = provider.credential().await?;
            Ok((credential_to_auth(cred.clone()), Some(cred)))
        } else {
            Ok((self.inline_auth()?, None))
        }
    }

    fn inline_auth(&self) -> Result<HttpSinkAuth, FaucetError> {
        match &self.config.auth {
            AuthSpec::Inline(a) => Ok(a.clone()),
            AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                "auth references provider '{}' but no provider was supplied; \
                 set one via the CLI `auth:` catalog or `with_auth_provider`",
                r.name
            ))),
        }
    }

    /// Build an HTTP request with auth and headers applied.
    fn apply_auth(
        &self,
        mut req: reqwest::RequestBuilder,
        auth: &HttpSinkAuth,
    ) -> Result<reqwest::RequestBuilder, FaucetError> {
        match auth {
            HttpSinkAuth::None => {}
            HttpSinkAuth::Bearer { token } => {
                req = req.bearer_auth(token);
            }
            HttpSinkAuth::Basic { username, password } => {
                req = req.basic_auth(username, Some(password));
            }
            HttpSinkAuth::Custom { headers } => {
                let mut hm = reqwest::header::HeaderMap::new();
                for (name, value) in headers {
                    let n =
                        reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|e| {
                            FaucetError::Auth(format!("invalid custom header name {name:?}: {e}"))
                        })?;
                    let mut v = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                        FaucetError::Auth(format!("invalid custom header value for {name:?}: {e}"))
                    })?;
                    v.set_sensitive(true);
                    hm.insert(n, v);
                }
                req = req.headers(hm);
            }
        }
        Ok(req)
    }

    /// Build an HTTP request with the given pre-resolved auth and body.
    fn build_request_with_auth(
        &self,
        body: &Value,
        auth: &HttpSinkAuth,
    ) -> Result<reqwest::RequestBuilder, FaucetError> {
        let req = self
            .client
            .request(self.config.method.clone(), &self.config.url)
            .headers(self.config.headers.clone())
            .json(body);
        self.apply_auth(req, auth)
    }

    /// Send a single request with retry logic, using the pre-resolved `auth`.
    /// A transport error and a retriable status take the same retry path.
    async fn send_with_retry(
        &self,
        calls: &CallSlot,
        body: &Value,
        auth: &HttpSinkAuth,
    ) -> Result<(), FaucetError> {
        calls
            .call("send", || async {
                let req = self.build_request_with_auth(body, auth)?;
                let resp = self.client.send(req).await?;
                match check_http_response_rate_limited(resp, DEFAULT_ERROR_BODY_MAX_LEN).await {
                    Ok(_) => Ok(()),
                    Err(FaucetError::Source(message)) => Err(FaucetError::Sink(message)),
                    Err(e) => Err(e),
                }
            })
            .await
    }
}

#[async_trait]
impl HttpDriver for Endpoint {
    type Session = Session;

    fn connector_name(&self) -> &'static str {
        "http"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(HttpSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        faucet_core::redact_uri_credentials(&self.config.url)
    }

    /// `max_retries` over the run's recorder and deadline; each `send` round
    /// trip is metered.
    fn call_context(&self) -> CallContext {
        CallContext::new().with_retry(retry_policy(self.config.max_retries))
    }

    fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        self.config.batch_atomicity()
    }

    fn batching(&self) -> HttpBatching {
        match &self.config.batch_mode {
            HttpBatchMode::Individual => HttpBatching::PerRecord {
                concurrency: self.config.concurrency,
            },
            HttpBatchMode::Array => HttpBatching::Array {
                batch_size: self.config.batch_size,
            },
        }
    }

    /// Resolve auth once per batch (provider-first, then inline).
    async fn session(&self, _calls: &CallSlot) -> Result<Session, FaucetError> {
        let (auth, sent) = self.resolve_auth_and_credential().await?;
        Ok(Session { auth, sent })
    }

    /// [`send_with_retry`](Self::send_with_retry), re-authenticating once when
    /// the server rejects the shared provider's credential (#789 API-06). The
    /// provider refreshes once even when concurrent requests all hit the same
    /// rejection.
    async fn send(
        &self,
        calls: &CallSlot,
        session: &Session,
        body: &Value,
    ) -> Result<(), FaucetError> {
        match self.send_with_retry(calls, body, &session.auth).await {
            Err(e) => match (&self.auth_provider, session.sent.as_ref()) {
                (Some(provider), Some(sent))
                    if faucet_core::rejects_credential(&e, provider.as_ref()) =>
                {
                    let name = provider.provider_name();
                    tracing::warn!(
                        provider = name,
                        "the server rejected the shared credential; re-authenticating and \
                         retrying once"
                    );
                    let fresh = credential_to_auth(provider.invalidate(sent).await?);
                    self.send_with_retry(calls, body, &fresh).await
                }
                _ => Err(e),
            },
            ok => ok,
        }
    }

    /// Non-mutating preflight probe (probe name `"network"`).
    ///
    /// Issues a lightweight `HEAD` request to the configured endpoint over the
    /// existing reqwest client. We only care that the host is reachable — that
    /// DNS, TCP, TLS and the server all work — so **any** HTTP response (2xx,
    /// 4xx including `405 Method Not Allowed`, or 5xx) counts as a pass. Only a
    /// transport/connection error (no response at all) is a failure.
    async fn check(
        &self,
        _calls: &CallSlot,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};
        // Resolve auth so authenticated endpoints don't reject the connection
        // before we learn the host is reachable. An unresolvable auth ref is a
        // configuration failure surfaced on this probe.
        let auth = match self.resolve_auth_and_credential().await {
            Ok((a, _)) => a,
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "network",
                    std::time::Duration::ZERO,
                    e.to_string(),
                    "check the configured auth / that a shared auth provider is wired up",
                )));
            }
        };
        let started = std::time::Instant::now();
        let hint = "check the url / DNS / TLS / that the host is reachable";
        let req = self
            .client
            .head(&self.config.url)
            .headers(self.config.headers.clone());
        let req = match self.apply_auth(req, &auth) {
            Ok(r) => r,
            Err(e) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "network",
                    started.elapsed(),
                    e.to_string(),
                    hint,
                )));
            }
        };
        let probe = match tokio::time::timeout(ctx.timeout, self.client.send(req)).await {
            // Any HTTP response means DNS + TCP + TLS + the host all work.
            Ok(Ok(_)) => Probe::pass("network", started.elapsed()),
            // Transport/connection error: no response received.
            Ok(Err(e)) => Probe::fail_hint("network", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("network", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpSinkConfig;
    use faucet_core::Sink as _;

    #[test]
    fn dataset_uri_redacts_credentials() {
        let config = HttpSinkConfig::new("https://user:secret@api.example.com/ingest");
        let sink = HttpSink::new(config);
        assert_eq!(sink.dataset_uri(), "https://api.example.com/ingest");
    }

    #[test]
    fn creates_sink() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest");
        let _sink = HttpSink::new(config);
    }

    #[test]
    fn http_sink_is_not_idempotent() {
        // F32: in Array mode `write_batch` POSTs chunk-by-chunk (non-atomic
        // forward progress). The HTTP sink must report it does NOT support
        // idempotent writes, so the pipeline's retry gate (F29) never replays a
        // partially-delivered page — which would re-POST already-delivered
        // chunks and silently duplicate rows against the live endpoint.
        let array = HttpSink::new(
            HttpSinkConfig::new("https://api.example.com/ingest")
                .batch_mode(crate::config::HttpBatchMode::Array),
        );
        assert!(!array.supports_idempotent_writes());
        let individual = HttpSink::new(HttpSinkConfig::new("https://api.example.com/ingest"));
        assert!(!individual.supports_idempotent_writes());
    }

    #[test]
    fn build_request_applies_bearer_auth() {
        let auth = HttpSinkAuth::Bearer {
            token: "my-token".into(),
        };
        let config = HttpSinkConfig::new("https://api.example.com/ingest").auth(auth.clone());
        let sink = HttpSink::new(config);

        let req = sink
            .build_request_with_auth(&serde_json::json!({"test": true}), &auth)
            .unwrap()
            .build()
            .unwrap();

        let auth_header = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("Bearer "));
        assert!(auth_header.contains("my-token"));
    }

    #[test]
    fn build_request_applies_basic_auth() {
        let auth = HttpSinkAuth::Basic {
            username: "user".into(),
            password: "pass".into(),
        };
        let config = HttpSinkConfig::new("https://api.example.com/ingest").auth(auth.clone());
        let sink = HttpSink::new(config);

        let req = sink
            .build_request_with_auth(&serde_json::json!({"test": true}), &auth)
            .unwrap()
            .build()
            .unwrap();

        let auth_header = req
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(auth_header.starts_with("Basic "));
    }

    #[test]
    fn build_request_uses_configured_method() {
        let config =
            HttpSinkConfig::new("https://api.example.com/ingest").method(reqwest::Method::PUT);
        let sink = HttpSink::new(config);

        let req = sink
            .build_request_with_auth(&serde_json::json!({"test": true}), &HttpSinkAuth::None)
            .unwrap()
            .build()
            .unwrap();

        assert_eq!(req.method(), reqwest::Method::PUT);
    }

    #[test]
    fn retry_policy_backs_off_with_jitter_from_the_base() {
        use super::{RETRY_BASE, retry_policy};

        // `max_retries` counts retries after the first attempt; the backoff is
        // core's capped, jittered exponential from a non-zero base (a zero
        // delay turned a brief 503 into an amplifying burst). A `RateLimited`
        // error waits its own `Retry-After` in the core runner.
        let p = retry_policy(3);
        assert_eq!(p.max_attempts, 4);
        assert_eq!(p.base, RETRY_BASE);
        assert!(p.jitter);
        assert_eq!(p.backoff, faucet_core::BackoffKind::Exponential);
        assert_eq!(retry_policy(0).max_attempts, 1);
        assert_eq!(retry_policy(usize::MAX).max_attempts, u32::MAX);
    }
}

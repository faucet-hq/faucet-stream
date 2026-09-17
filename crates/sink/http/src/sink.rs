//! HTTP sink executor.

use crate::config::{HttpBatchMode, HttpSinkAuth, HttpSinkConfig};
use async_trait::async_trait;
use faucet_core::util::{DEFAULT_ERROR_BODY_MAX_LEN, check_http_response};
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider};
use futures::stream::{FuturesUnordered, StreamExt};
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
pub struct HttpSink {
    config: HttpSinkConfig,
    client: reqwest::Client,
    /// Optional shared auth provider. When set, it takes precedence over inline
    /// auth. Set via [`HttpSink::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
}

/// Base delay for a retried request; the cap and jitter come from
/// [`faucet_core::retry::backoff_with_jitter`], so this sink cannot drift from
/// the rest of the repo's backoff behaviour.
const RETRY_BASE: std::time::Duration = std::time::Duration::from_millis(250);

/// Sleep before re-sending. A `RateLimited` error carries the server's own
/// `Retry-After`, which always wins; everything else gets capped, jittered
/// exponential backoff. Retrying with **no** delay (the previous behaviour)
/// turned a brief upstream 503 into an amplifying burst of requests within
/// microseconds — the thundering herd core's backoff exists to prevent.
async fn retry_delay(err: &FaucetError, attempt: u32) {
    let wait = match err {
        FaucetError::RateLimited(d) => *d,
        _ => faucet_core::retry::backoff_with_jitter(RETRY_BASE, attempt),
    };
    tokio::time::sleep(wait).await;
}

impl HttpSink {
    /// Create a new HTTP sink from the given configuration.
    pub fn new(config: HttpSinkConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::new(),
            auth_provider: None,
        }
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sinks can share one token with
    /// single-flight refresh. Used by the CLI to resolve `auth: { ref }`, and
    /// by library callers who construct one provider and inject it into many
    /// sinks.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Resolve the effective auth for the current batch. The provider (if any)
    /// takes precedence; otherwise inline auth is used. A bare
    /// `AuthSpec::Reference` with no provider is an error.
    async fn resolve_auth(&self) -> Result<HttpSinkAuth, FaucetError> {
        if let Some(provider) = &self.auth_provider {
            Ok(credential_to_auth(provider.credential().await?))
        } else {
            match &self.config.auth {
                AuthSpec::Inline(a) => Ok(a.clone()),
                AuthSpec::Reference(r) => Err(FaucetError::Auth(format!(
                    "auth references provider '{}' but no provider was supplied; \
                     set one via the CLI `auth:` catalog or `with_auth_provider`",
                    r.name
                ))),
            }
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
                    let v = reqwest::header::HeaderValue::from_str(value).map_err(|e| {
                        FaucetError::Auth(format!("invalid custom header value for {name:?}: {e}"))
                    })?;
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
    async fn send_with_retry(&self, body: &Value, auth: &HttpSinkAuth) -> Result<(), FaucetError> {
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            let req = self.build_request_with_auth(body, auth)?;

            match req.send().await {
                Ok(resp) => match check_http_response(resp, DEFAULT_ERROR_BODY_MAX_LEN).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        if attempt < self.config.max_retries && e.is_retriable() {
                            tracing::warn!(
                                attempt = attempt + 1,
                                max_retries = self.config.max_retries,
                                error = %e,
                                "retrying request"
                            );
                            retry_delay(&e, attempt as u32).await;
                            last_error = Some(e);
                            continue;
                        }
                        return Err(e);
                    }
                },
                Err(e) => {
                    let faucet_err = FaucetError::Http(e);
                    if attempt < self.config.max_retries && faucet_err.is_retriable() {
                        tracing::warn!(
                            attempt = attempt + 1,
                            max_retries = self.config.max_retries,
                            error = %faucet_err,
                            "retrying request"
                        );
                        retry_delay(&faucet_err, attempt as u32).await;
                        last_error = Some(faucet_err);
                        continue;
                    }
                    return Err(faucet_err);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| FaucetError::Sink("max retries exhausted".into())))
    }
}

#[async_trait]
impl faucet_core::Sink for HttpSink {
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

    /// Non-mutating preflight probe (probe name `"network"`).
    ///
    /// Issues a lightweight `HEAD` request to the configured endpoint over the
    /// existing reqwest client. We only care that the host is reachable — that
    /// DNS, TCP, TLS and the server all work — so **any** HTTP response (2xx,
    /// 4xx including `405 Method Not Allowed`, or 5xx) counts as a pass. Only a
    /// transport/connection error (no response at all) is a failure.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        // Resolve auth so authenticated endpoints don't reject the connection
        // before we learn the host is reachable. An unresolvable auth ref is a
        // configuration failure surfaced on this probe.
        let auth = match self.resolve_auth().await {
            Ok(a) => a,
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

        let probe = match tokio::time::timeout(ctx.timeout, req.send()).await {
            // Any HTTP response means DNS + TCP + TLS + the host all work.
            Ok(Ok(_)) => Probe::pass("network", started.elapsed()),
            // Transport/connection error: no response received.
            Ok(Err(e)) => Probe::fail_hint("network", started.elapsed(), e.to_string(), hint),
            Err(_) => Probe::fail_hint("network", started.elapsed(), "timed out", hint),
        };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Resolve auth once per batch (provider-first, then inline).
        let auth = self.resolve_auth().await?;

        match &self.config.batch_mode {
            HttpBatchMode::Individual => {
                // Run `send_with_retry` for every record with at most
                // `concurrency` in-flight at once. We drive a
                // `FuturesUnordered` directly, refilling it as each future
                // completes, instead of acquiring permits up-front the way
                // the previous semaphore-based code did — that approach
                // deadlocked because permits were acquired sequentially in
                // a loop before any future actually ran, so after
                // `concurrency` iterations the next `acquire_owned().await`
                // would block forever (closes #59).
                let concurrency = self.config.concurrency.max(1);
                let mut in_flight = FuturesUnordered::new();
                let mut iter = records.iter();
                for record in iter.by_ref().take(concurrency) {
                    in_flight.push(self.send_with_retry(record, &auth));
                }
                while let Some(result) = in_flight.next().await {
                    result?;
                    if let Some(record) = iter.next() {
                        in_flight.push(self.send_with_retry(record, &auth));
                    }
                }

                tracing::debug!(records = records.len(), "HTTP individual batch written");
                Ok(records.len())
            }
            HttpBatchMode::Array => {
                // `batch_size = 0` is the "no batching" sentinel: forward
                // whatever upstream handed us as a single JSON-array POST,
                // preserving `StreamPage` framing. Otherwise re-chunk into
                // `batch_size` slices and issue one POST per chunk.
                let effective_chunk = if self.config.batch_size == 0 {
                    records.len()
                } else {
                    self.config.batch_size
                };

                let mut total = 0;
                for chunk in records.chunks(effective_chunk) {
                    let array = Value::Array(chunk.to_vec());
                    self.send_with_retry(&array, &auth).await?;
                    total += chunk.len();
                }
                tracing::debug!(
                    records = total,
                    batch_size = self.config.batch_size,
                    "HTTP array batch written"
                );
                Ok(total)
            }
        }
    }

    /// Report per-row outcomes so the DLQ router dead-letters only the records
    /// that genuinely failed.
    ///
    /// In **Individual** mode every record is an independent POST, so each
    /// record's success/failure is attributable: we attempt *all* of them
    /// (unlike `write_batch`, whose `?` short-circuits on the first failure)
    /// and return one `Ok`/`Err` per record. Without this
    /// override the default impl would surface the first error as an outer
    /// `Err`, and under `on_batch_error: dlq_all` the pipeline would route the
    /// *entire* batch to the DLQ — duplicating the already-delivered rows
    /// against a non-idempotent endpoint (#146 M14).
    ///
    /// In **Array** mode the page is POSTed chunk-by-chunk (`batch_size`
    /// slices), so forward progress is *not* atomic across the whole page —
    /// each chunk is a separate, independently-committed array POST. The
    /// override is therefore **chunk-aware** rather than all-or-nothing: it
    /// iterates the chunks itself and POSTs each array; a row whose chunk was
    /// delivered is reported `Ok(())`, while the rows of the first failing
    /// chunk (and every not-yet-sent chunk after it) are reported `Err`.
    ///
    /// This is the fix for the duplicate-data bug (F15 / audit #264): the old
    /// implementation delegated to the all-or-nothing `write_batch`, so a late
    /// chunk failure surfaced the *whole* page as an outer `Err`. Under
    /// `on_batch_error: dlq_all` the router then dead-lettered every row —
    /// including rows from earlier chunks already successfully delivered to the
    /// live endpoint — producing silent downstream duplicates. By reporting
    /// per-row outcomes, an already-delivered row is **never** marked failed and
    /// so can never land in the DLQ.
    ///
    /// Within a single failed chunk a single array POST cannot attribute the
    /// failure to specific rows, so all rows of *that* chunk are reported `Err`
    /// (acceptable — none of them were delivered). When the **first** chunk
    /// fails (nothing has been delivered yet) the override preserves the
    /// original all-or-nothing contract and surfaces an outer `Err`, so the
    /// router's `on_batch_error` policy (abort vs. dead-letter) still applies to
    /// a wholly-undelivered page exactly as before.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        let auth = self.resolve_auth().await?;

        match &self.config.batch_mode {
            HttpBatchMode::Individual => {
                let concurrency = self.config.concurrency.max(1);
                let auth = &auth;
                // Attempt every record (failures don't short-circuit the
                // siblings) with at most `concurrency` POSTs in flight. Tag each
                // outcome with its index so we can restore record order after
                // the unordered completion. The per-record futures are built
                // eagerly (lazy, not yet polled) so `buffer_unordered` drives a
                // single concrete future type.
                let pending: Vec<_> =
                    records
                        .iter()
                        .enumerate()
                        .map(|(idx, record)| async move {
                            (idx, self.send_with_retry(record, auth).await)
                        })
                        .collect();
                let mut indexed: Vec<(usize, faucet_core::RowOutcome)> =
                    futures::stream::iter(pending)
                        .buffer_unordered(concurrency)
                        .collect()
                        .await;
                indexed.sort_by_key(|(idx, _)| *idx);
                tracing::debug!(
                    records = records.len(),
                    "HTTP individual partial batch written"
                );
                Ok(indexed.into_iter().map(|(_, outcome)| outcome).collect())
            }
            HttpBatchMode::Array => {
                // `batch_size = 0` is the "no batching" sentinel: forward the
                // whole page as a single array POST (one chunk). Otherwise
                // re-chunk into `batch_size` slices and POST one array per
                // chunk — mirroring `write_batch`, but tracking per-chunk
                // delivery so a late failure doesn't poison earlier chunks that
                // were already delivered.
                let effective_chunk = if self.config.batch_size == 0 {
                    records.len()
                } else {
                    self.config.batch_size
                };

                let mut outcomes: Vec<faucet_core::RowOutcome> = Vec::with_capacity(records.len());
                let mut delivered = 0usize;
                let mut chunks = records.chunks(effective_chunk);
                let mut failed_chunk: Option<FaucetError> = None;

                for chunk in chunks.by_ref() {
                    let array = Value::Array(chunk.to_vec());
                    match self.send_with_retry(&array, &auth).await {
                        Ok(()) => {
                            // This chunk was delivered to the live endpoint.
                            outcomes.extend(chunk.iter().map(|_| Ok(())));
                            delivered += chunk.len();
                        }
                        Err(e) => {
                            // First failing chunk before any delivery: preserve
                            // the original all-or-nothing contract so the
                            // router's `on_batch_error` policy still governs a
                            // wholly-undelivered page.
                            if delivered == 0 {
                                return Err(e);
                            }
                            // Otherwise some earlier chunk(s) were delivered;
                            // mark this chunk's rows (and all remaining,
                            // never-sent chunks) failed without poisoning the
                            // delivered rows.
                            failed_chunk = Some(e);
                            outcomes.extend(chunk.iter().map(|_| {
                                Err(FaucetError::Sink(
                                    "array-mode chunk POST failed; rows not delivered".into(),
                                ))
                            }));
                            break;
                        }
                    }
                }

                if let Some(e) = failed_chunk {
                    // Remaining chunks were never sent — report them failed too
                    // so the DLQ captures every undelivered row.
                    let msg = e.to_string();
                    for chunk in chunks {
                        outcomes.extend(chunk.iter().map(|_| {
                            Err(FaucetError::Sink(format!(
                                "array-mode chunk not sent after earlier failure: {msg}"
                            )))
                        }));
                    }
                }

                debug_assert_eq!(
                    outcomes.len(),
                    records.len(),
                    "one outcome per record in array mode"
                );
                tracing::debug!(
                    delivered,
                    records = records.len(),
                    batch_size = self.config.batch_size,
                    "HTTP array partial batch written"
                );
                Ok(outcomes)
            }
        }
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
}

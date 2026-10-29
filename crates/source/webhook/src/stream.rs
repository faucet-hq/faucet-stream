//! Webhook source stream executor.

use crate::config::{SignatureAlgorithm, SignatureEncoding, WebhookSignature, WebhookSourceConfig};
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use faucet_core::driver::{DriverSource, PageStream, SourceDriver};
use faucet_core::{CallSlot, FaucetError};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, Notify};

/// Shared state for the webhook HTTP handler.
struct AppState {
    inbox: Mutex<Inbox>,
    max_payloads: Option<usize>,
    max_buffered_bytes: usize,
    done: Notify,
    /// Optional shared secret required in the `Authorization` header.
    auth_token: Option<String>,
    signature: Option<WebhookSignature>,
}

/// Stored payloads plus the closed flag, under one lock so a payload is either
/// stored before the window closes or refused with a retryable status.
#[derive(Default)]
struct Inbox {
    records: Vec<Value>,
    bytes: usize,
    closed: bool,
    /// Signatures of stored timestamped requests, by their signed timestamp,
    /// so a repeat inside the tolerance window is not stored twice.
    seen: HashMap<String, u64>,
}

/// How long a refused sender is told to wait before retrying.
const RETRY_AFTER_SECS: &str = "60";

/// How long in-flight requests get to finish once the window closes.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

impl WebhookDriver {
    fn new_state(&self) -> Arc<AppState> {
        Arc::new(AppState {
            inbox: Mutex::new(Inbox::default()),
            max_payloads: self.config.max_payloads,
            max_buffered_bytes: self.config.max_buffered_bytes,
            done: Notify::new(),
            auth_token: self.config.auth_token.clone(),
            signature: self.config.signature.clone(),
        })
    }

    fn build_router(&self, path: &str, state: Arc<AppState>) -> Router {
        Router::new()
            .route(path, post(webhook_handler))
            // Bound request body size so a single huge POST can't exhaust
            // memory (#78/#26).
            .layer(axum::extract::DefaultBodyLimit::max(
                self.config.max_body_bytes,
            ))
            .with_state(state)
    }
}

/// A webhook receiver source that starts a temporary HTTP server and
/// collects incoming POST payloads as records.
pub struct WebhookSource {
    inner: DriverSource<WebhookDriver>,
}

/// The connector's I/O primitives (RFC 0014).
pub(crate) struct WebhookDriver {
    config: WebhookSourceConfig,
}

impl WebhookSource {
    /// Create a new webhook source from the given configuration.
    pub fn new(config: WebhookSourceConfig) -> Self {
        Self {
            inner: DriverSource::new(WebhookDriver { config }),
        }
    }

    /// Start the webhook server, collect payloads, and return them.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.inner.driver().fetch_all().await
    }
}

#[async_trait]
impl faucet_core::Source for WebhookSource {
    faucet_core::forward_source!(inner);
}

impl WebhookDriver {
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.config.validate()?;
        self.receive(&self.config.path).await
    }

    /// Serve `path` until the window closes, then stop taking payloads (later
    /// ones are answered `503` + `Retry-After`), let in-flight requests finish,
    /// and return what was stored (API-14).
    async fn receive(&self, path: &str) -> Result<Vec<Value>, FaucetError> {
        crate::config::validate_resolved_path(path)?;
        let state = self.new_state();
        let app = self.build_router(path, Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind(&self.config.listen_addr)
            .await
            .map_err(|e| {
                FaucetError::Config(format!(
                    "failed to bind to {}: {e}",
                    self.config.listen_addr
                ))
            })?;

        tracing::info!(
            addr = %self.config.listen_addr,
            path = %path,
            "webhook server listening"
        );

        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
        let mut server = tokio::spawn(
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stop_rx.await;
                })
                .into_future(),
        );
        #[allow(clippy::disallowed_methods)] // the collection window, not a backend wait
        let timeout = tokio::time::sleep(std::time::Duration::from_secs(self.config.timeout_secs));
        let done_notified = state.done.notified();

        let early = tokio::select! {
            result = &mut server => Some(result),
            () = timeout => {
                tracing::info!("webhook timeout reached");
                None
            }
            () = done_notified => {
                tracing::info!("max payloads reached");
                None
            }
        };
        state.inbox.lock().await.closed = true;
        match early {
            Some(result) => server_result(result)?,
            None => {
                let _ = stop_tx.send(());
                if let Ok(result) = tokio::time::timeout(SHUTDOWN_GRACE, &mut server).await {
                    server_result(result)?;
                } else {
                    server.abort();
                }
            }
        }

        let records = std::mem::take(&mut state.inbox.lock().await.records);
        tracing::info!(records = records.len(), "webhook fetch complete");
        Ok(records)
    }
}

fn server_result(
    result: Result<std::io::Result<()>, tokio::task::JoinError>,
) -> Result<(), FaucetError> {
    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(FaucetError::Config(format!("webhook server error: {e}"))),
        Err(e) => Err(FaucetError::Config(format!(
            "webhook server task failed: {e}"
        ))),
    }
}

/// Verify an HMAC signature over the raw body (and the signed timestamp, when
/// configured) in constant time.
/// For a request signed with a timestamp: its signature header value, its
/// timestamp, and the oldest timestamp still inside the tolerance window.
fn signed_request_key(
    sig: &WebhookSignature,
    headers: &axum::http::HeaderMap,
    now_unix: u64,
) -> Option<(String, u64, u64)> {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let ts = header(sig.timestamp_header.as_deref()?)?
        .trim()
        .parse::<u64>()
        .ok()?;
    let provided = header(&sig.header)?.trim();
    let provided = match sig.encoding {
        SignatureEncoding::Hex => provided.to_ascii_lowercase(),
        _ => provided.to_string(),
    };
    Some((provided, ts, now_unix.saturating_sub(sig.tolerance_secs)))
}

fn signature_ok(
    sig: &WebhookSignature,
    headers: &axum::http::HeaderMap,
    body: &[u8],
    now_unix: u64,
) -> bool {
    use hmac::{Mac, digest::KeyInit};
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let Some(provided) = header(&sig.header) else {
        return false;
    };
    let provided = provided.trim();
    let provided = match &sig.prefix {
        Some(p) => match provided.strip_prefix(p.as_str()) {
            Some(rest) => rest,
            None => return false,
        },
        None => provided,
    };
    let mut signed: Vec<u8> = Vec::with_capacity(body.len() + 16);
    if let Some(ts_name) = &sig.timestamp_header {
        let Some(ts) = header(ts_name).and_then(|t| t.trim().parse::<u64>().ok()) else {
            return false;
        };
        if ts.abs_diff(now_unix) > sig.tolerance_secs {
            return false;
        }
        signed.extend_from_slice(ts.to_string().as_bytes());
        signed.push(b'.');
    }
    signed.extend_from_slice(body);
    let digest: Vec<u8> = match sig.algorithm {
        SignatureAlgorithm::Sha256 => {
            let Ok(mut mac) =
                <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(sig.secret.as_bytes())
            else {
                return false;
            };
            mac.update(&signed);
            mac.finalize().into_bytes().to_vec()
        }
        SignatureAlgorithm::Sha512 => {
            let Ok(mut mac) =
                <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(sig.secret.as_bytes())
            else {
                return false;
            };
            mac.update(&signed);
            mac.finalize().into_bytes().to_vec()
        }
    };
    let expected = match sig.encoding {
        SignatureEncoding::Hex => digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        SignatureEncoding::Base64 => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&digest)
        }
    };
    let provided = match sig.encoding {
        SignatureEncoding::Hex => provided.to_ascii_lowercase(),
        SignatureEncoding::Base64 => provided.to_string(),
    };
    bool::from(provided.as_bytes().ct_eq(expected.as_bytes()))
}

/// Constant-time check of an `Authorization` header value against the shared
/// secret. Accepts either the raw token or a `Bearer <token>` form, matching the
/// original behaviour. The comparison uses [`subtle::ConstantTimeEq`] and a
/// non-short-circuiting `|` so neither *which* form matched nor *where* the first
/// byte differs is observable via response timing — closing a side-channel that
/// could otherwise leak the secret byte-by-byte. (Length difference short-circuits;
/// the secret's length is not itself sensitive.)
fn token_matches(provided: Option<&str>, expected: &str) -> bool {
    let Some(p) = provided else {
        return false;
    };
    let exp = expected.as_bytes();
    let raw = bool::from(p.as_bytes().ct_eq(exp));
    let stripped = p
        .strip_prefix("Bearer ")
        .map(|s| bool::from(s.as_bytes().ct_eq(exp)))
        .unwrap_or(false);
    raw | stripped
}

/// Decision for an incoming payload, given the current record count and the
/// configured cap. Extracted as a pure function so the cap invariant can be
/// tested deterministically without driving concurrent HTTP.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PayloadDecision {
    /// Whether to store the just-received payload.
    accept: bool,
    /// Whether the receive loop is now done (cap reached) and `done` should be
    /// notified.
    done: bool,
}

/// Decide whether to store a payload and whether the cap is now satisfied.
///
/// `current_len` is the number of records already stored (observed under the
/// records lock). With no cap, every payload is accepted and the loop never
/// self-terminates. With a cap, the cap is **exact**: once `current_len` has
/// reached `max`, further concurrently-accepted POSTs are dropped (so the Vec
/// never exceeds `max`) while still notifying `done` so a slightly-late request
/// doesn't wedge the receive loop (#146 LOW).
fn decide_payload(current_len: usize, max_payloads: Option<usize>) -> PayloadDecision {
    match max_payloads {
        None => PayloadDecision {
            accept: true,
            done: false,
        },
        Some(max) => {
            if current_len >= max {
                // Already at (or somehow past) the cap — drop this payload and
                // signal completion. Pushing here is what let concurrent
                // in-flight POSTs overflow the Vec past `max`.
                PayloadDecision {
                    accept: false,
                    done: true,
                }
            } else {
                // Room for this one. Accept it; we're done once it fills the
                // last slot.
                PayloadDecision {
                    accept: true,
                    done: current_len + 1 >= max,
                }
            }
        }
    }
}

/// Axum handler for incoming webhook POST requests.
async fn webhook_handler(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Optional shared-secret check: accept either the raw token or
    // `Bearer <token>` in the Authorization header (#78/#26).
    if let Some(expected) = &state.auth_token {
        let provided = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        if !token_matches(provided, expected) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }
    let mut replay_key: Option<(String, u64, u64)> = None;
    if let Some(sig) = &state.signature {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if !signature_ok(sig, &headers, &body, now) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        replay_key = signed_request_key(sig, &headers, now);
    }

    let value = match faucet_core::fidelity::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            // If the body is not valid JSON, wrap it as a string.
            match String::from_utf8(body.to_vec()) {
                Ok(s) => Value::String(s),
                Err(_) => return StatusCode::BAD_REQUEST.into_response(),
            }
        }
    };

    let mut inbox = state.inbox.lock().await;
    if let Some((key, _, cutoff)) = &replay_key {
        inbox.seen.retain(|_, ts| *ts >= *cutoff);
        if inbox.seen.contains_key(key) {
            // The same signed request was already stored: acknowledge it again.
            return StatusCode::OK.into_response();
        }
    }
    // Decide under the lock so the cap is exact: concurrent in-flight POSTs
    // that have already been accepted can't push the Vec past `max_payloads`.
    // A payload that is not stored — over the cap, or after the window closed
    // — is refused with a retryable status rather than a `200` the sender
    // would read as delivered (API-14).
    let decision = decide_payload(inbox.records.len(), state.max_payloads);
    // An empty inbox always takes the payload, as the count cap does: a body
    // larger than the byte cap (allowed by `max_body_bytes`) would otherwise
    // be refused in every window and retried forever.
    let over_bytes = state.max_buffered_bytes > 0
        && !inbox.records.is_empty()
        && inbox.bytes.saturating_add(body.len()) > state.max_buffered_bytes;
    if decision.done || over_bytes {
        state.done.notify_one();
    }
    if inbox.closed || !decision.accept || over_bytes {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, RETRY_AFTER_SECS)],
        )
            .into_response();
    }
    inbox.bytes += body.len();
    inbox.records.push(value);
    if state.max_buffered_bytes > 0 && inbox.bytes >= state.max_buffered_bytes {
        // The window is full: emit it rather than wait for a refused POST.
        state.done.notify_one();
    }
    if let Some((key, ts, _)) = replay_key {
        inbox.seen.insert(key, ts);
    }
    StatusCode::OK.into_response()
}

#[async_trait]
impl SourceDriver for WebhookDriver {
    /// Serve until the collection window closes, then emit what arrived:
    /// one page per `batch_size` records.
    fn read<'a>(
        &'a self,
        _calls: &'a CallSlot,
        context: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> PageStream<'a> {
        Box::pin(faucet_core::async_stream::try_stream! {
            for page in faucet_core::driver::pages_of(self.fetch_with_context(context).await?, None, batch_size) {
                yield page;
            }
        })
    }

    async fn fetch(
        &self,
        _calls: &CallSlot,
        context: &HashMap<String, Value>,
    ) -> Option<Result<(Vec<Value>, Option<Value>), FaucetError>> {
        Some(self.fetch_with_context(context).await.map(|r| (r, None)))
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(WebhookSourceConfig))
            .expect("schema serialization")
    }

    fn capabilities(&self, derived: faucet_core::Capabilities) -> faucet_core::Capabilities {
        derived.to_builder().read_side_effects(true).build()
    }

    fn connector_name(&self) -> &'static str {
        "webhook"
    }

    fn dataset_uri(&self) -> String {
        format!("webhook://{}{}", self.config.listen_addr, self.config.path)
    }

    /// Preflight probe that does **not** start the receive loop.
    ///
    /// The default `Source::check` would call `stream_pages`, which boots the
    /// HTTP server and blocks for the whole receive window waiting for inbound
    /// POSTs — useless as a fast preflight. Instead we just verify the
    /// configured `listen_addr` is bindable: bind a `tokio::net::TcpListener`
    /// to it and immediately drop it. Success means the port is free; a bind
    /// error (port in use, permission denied, bad address) fails the probe.
    async fn check(
        &self,
        _calls: &CallSlot,
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<Option<faucet_core::check::CheckReport>, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let start = std::time::Instant::now();
        match tokio::net::TcpListener::bind(&self.config.listen_addr).await {
            Ok(listener) => {
                // Drop the listener immediately so we don't hold the port.
                drop(listener);
                Ok(Some(CheckReport::single(Probe::pass(
                    "io",
                    start.elapsed(),
                ))))
            }
            Err(e) => Ok(Some(CheckReport::single(Probe::fail_hint(
                "io",
                start.elapsed(),
                e.to_string(),
                format!("{} is not bindable", self.config.listen_addr),
            )))),
        }
    }
}

impl WebhookDriver {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return self.fetch_all().await;
        }

        // Substitute context into the webhook path, percent-encoding each
        // value so a parent value cannot add segments or route syntax (API-49).
        let encoded: std::collections::HashMap<String, Value> = context
            .iter()
            .map(|(k, v)| {
                let raw = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                (
                    k.clone(),
                    Value::String(faucet_core::encode_url_component(&raw)),
                )
            })
            .collect();
        self.config.validate_settings()?;
        let resolved_path = faucet_core::util::substitute_context(&self.config.path, &encoded);
        self.receive(&resolved_path).await
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests post to the server with a plain client
mod tests;

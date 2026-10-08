//! Webhook source stream executor.

use crate::config::{SignatureAlgorithm, SignatureEncoding, WebhookSignature, WebhookSourceConfig};
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use faucet_core::FaucetError;
use serde_json::Value;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, Notify};

/// Shared state for the webhook HTTP handler.
struct AppState {
    inbox: Mutex<Inbox>,
    max_payloads: Option<usize>,
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
    closed: bool,
}

/// How long a refused sender is told to wait before retrying.
const RETRY_AFTER_SECS: &str = "60";

/// How long in-flight requests get to finish once the window closes.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

impl WebhookSource {
    fn new_state(&self) -> Arc<AppState> {
        Arc::new(AppState {
            inbox: Mutex::new(Inbox::default()),
            max_payloads: self.config.max_payloads,
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
    config: WebhookSourceConfig,
}

impl WebhookSource {
    /// Create a new webhook source from the given configuration.
    pub fn new(config: WebhookSourceConfig) -> Self {
        Self { config }
    }

    /// Start the webhook server, collect payloads, and return them.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.config.validate()?;
        self.receive(&self.config.path).await
    }

    /// Serve `path` until the window closes, then stop taking payloads (later
    /// ones are answered `503` + `Retry-After`), let in-flight requests finish,
    /// and return what was stored (API-14).
    async fn receive(&self, path: &str) -> Result<Vec<Value>, FaucetError> {
        crate::config::validate_path(path)?;
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
    if let Some(sig) = &state.signature {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        if !signature_ok(sig, &headers, &body, now) {
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }

    let value = match serde_json::from_slice::<Value>(&body) {
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
    // Decide under the lock so the cap is exact: concurrent in-flight POSTs
    // that have already been accepted can't push the Vec past `max_payloads`.
    // A payload that is not stored — over the cap, or after the window closed
    // — is refused with a retryable status rather than a `200` the sender
    // would read as delivered (API-14).
    let decision = decide_payload(inbox.records.len(), state.max_payloads);
    if decision.done {
        state.done.notify_one();
    }
    if inbox.closed || !decision.accept {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(axum::http::header::RETRY_AFTER, RETRY_AFTER_SECS)],
        )
            .into_response();
    }
    inbox.records.push(value);
    StatusCode::OK.into_response()
}

#[async_trait]
impl faucet_core::Source for WebhookSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return WebhookSource::fetch_all(self).await;
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
                    Value::String(urlencoding::encode(&raw).into_owned()),
                )
            })
            .collect();
        let resolved_path = faucet_core::util::substitute_context(&self.config.path, &encoded);
        self.config.validate()?;
        self.receive(&resolved_path).await
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(WebhookSourceConfig))
            .expect("schema serialization")
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
        _ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let start = std::time::Instant::now();
        match tokio::net::TcpListener::bind(&self.config.listen_addr).await {
            Ok(listener) => {
                // Drop the listener immediately so we don't hold the port.
                drop(listener);
                Ok(CheckReport::single(Probe::pass("io", start.elapsed())))
            }
            Err(e) => Ok(CheckReport::single(Probe::fail_hint(
                "io",
                start.elapsed(),
                e.to_string(),
                format!("{} is not bindable", self.config.listen_addr),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn server_results_map_io_and_task_failures() {
        assert!(super::server_result(Ok(Ok(()))).is_ok());
        let io = super::server_result(Ok(Err(std::io::Error::other("boom"))))
            .unwrap_err()
            .to_string();
        assert!(io.contains("webhook server error: boom"), "{io}");
        let join = tokio::spawn(async { panic!("task died") })
            .await
            .unwrap_err();
        let task = super::server_result(Err(join)).unwrap_err().to_string();
        assert!(task.contains("webhook server task failed"), "{task}");
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn token_matches_accepts_raw_and_bearer() {
        assert!(token_matches(
            Some("sekret-token-value"),
            "sekret-token-value"
        ));
        assert!(token_matches(
            Some("Bearer sekret-token-value"),
            "sekret-token-value"
        ));
    }

    #[test]
    fn token_matches_rejects_wrong_and_missing() {
        assert!(!token_matches(
            Some("wrong-token-value"),
            "sekret-token-value"
        ));
        assert!(!token_matches(
            Some("Bearer wrong-token-value"),
            "sekret-token-value"
        ));
        assert!(!token_matches(None, "sekret-token-value"));
        // Differing length must reject (not panic).
        assert!(!token_matches(
            Some("sekret-token-valu"),
            "sekret-token-value"
        ));
    }

    #[test]
    fn decide_payload_no_cap_always_accepts() {
        for len in [0usize, 1, 100, 10_000] {
            assert_eq!(
                decide_payload(len, None),
                PayloadDecision {
                    accept: true,
                    done: false
                }
            );
        }
    }

    #[test]
    fn decide_payload_accepts_until_cap_then_drops() {
        let max = Some(2);
        // 0 stored: accept, not yet done.
        assert_eq!(
            decide_payload(0, max),
            PayloadDecision {
                accept: true,
                done: false
            }
        );
        // 1 stored: accept the last slot, now done.
        assert_eq!(
            decide_payload(1, max),
            PayloadDecision {
                accept: true,
                done: true
            }
        );
        // 2 stored (at cap): drop, signal done.
        assert_eq!(
            decide_payload(2, max),
            PayloadDecision {
                accept: false,
                done: true
            }
        );
        // 3 stored (somehow past cap): still drop, still done.
        assert_eq!(
            decide_payload(3, max),
            PayloadDecision {
                accept: false,
                done: true
            }
        );
    }

    #[test]
    fn cap_invariant_never_exceeded_under_concurrent_arrivals() {
        // Simulate many in-flight POSTs all racing the cap: the same record
        // count can be observed by several requests before any of them push.
        // Applying `decide_payload` per request must still bound the Vec at
        // `max`, dropping the surplus rather than overflowing (#146 LOW).
        let max = 2usize;
        let mut records: Vec<Value> = Vec::new();
        // 5 concurrent arrivals; each makes its decision against the current
        // length and only pushes if accepted (mirroring the handler under the
        // records lock).
        for i in 0..5 {
            let decision = decide_payload(records.len(), Some(max));
            if decision.accept {
                records.push(json!({ "id": i }));
            }
        }
        assert_eq!(
            records.len(),
            max,
            "Vec must never exceed max_payloads, got {}",
            records.len()
        );
    }

    #[tokio::test]
    async fn handler_never_exceeds_cap_under_concurrent_posts() {
        // Drive the real handler with many concurrent requests against one
        // shared AppState. The records Vec must never exceed `max_payloads`,
        // even though several requests are accepted in-flight before any of
        // them observe the cap being hit (#146 LOW).
        let max = 3usize;
        let state = Arc::new(AppState {
            inbox: Mutex::new(Inbox::default()),
            max_payloads: Some(max),
            done: Notify::new(),
            auth_token: None,
            signature: None,
        });

        let mut handles = Vec::new();
        for i in 0..50 {
            let st = Arc::clone(&state);
            handles.push(tokio::spawn(async move {
                let body = axum::body::Bytes::from(format!("{{\"id\":{i}}}"));
                webhook_handler(State(st), axum::http::HeaderMap::new(), body).await
            }));
        }
        // API-14: exactly `max` are stored and answered 200; every surplus
        // payload is refused with a retryable 503 + Retry-After, never a 200.
        let mut ok = 0;
        for h in handles {
            let resp = h.await.unwrap();
            if resp.status() == StatusCode::OK {
                ok += 1;
            } else {
                assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
                assert_eq!(resp.headers()[axum::http::header::RETRY_AFTER], "60");
            }
        }
        assert_eq!(ok, max);

        let records = &state.inbox.lock().await.records;
        assert_eq!(
            records.len(),
            max,
            "Vec must never exceed max_payloads, got {}",
            records.len()
        );
    }

    #[tokio::test]
    async fn webhook_collects_payloads() {
        // Use port 0 to get a random available port.
        let config = WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:0")
            .max_payloads(2)
            .timeout_secs(5);

        let state = Arc::new(AppState {
            inbox: Mutex::new(Inbox::default()),
            max_payloads: config.max_payloads,
            done: Notify::new(),
            auth_token: config.auth_token.clone(),
            signature: None,
        });

        let server_state = Arc::clone(&state);
        let app = Router::new()
            .route(&config.path, post(webhook_handler))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind(&config.listen_addr)
            .await
            .unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let done_notified = server_state.done.notified();
            tokio::select! {
                result = axum::serve(listener, app).into_future() => {
                    if let Err(e) = result {
                        panic!("server error: {e}");
                    }
                }
                () = done_notified => {}
            }
        });

        let client = reqwest::Client::new();
        let url = format!("http://{addr}/webhook");

        // Send two payloads.
        let resp1 = client
            .post(&url)
            .json(&json!({"event": "created", "id": 1}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp1.status(), 200);

        let resp2 = client
            .post(&url)
            .json(&json!({"event": "updated", "id": 2}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp2.status(), 200);

        // Wait for the server to notice max_payloads.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        server_handle.abort();

        let records = &state.inbox.lock().await.records;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["event"], "created");
        assert_eq!(records[1]["event"], "updated");
    }

    #[tokio::test]
    async fn check_passes_when_port_is_bindable() {
        use faucet_core::Source;
        use faucet_core::check::{CheckContext, ProbeStatus};

        // Port 0 = let the OS pick a free port, so the bind always succeeds.
        let source = WebhookSource::new(WebhookSourceConfig::new().listen_addr("127.0.0.1:0"));
        let report = source.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].name, "io");
        assert!(
            matches!(report.probes[0].status, ProbeStatus::Pass),
            "expected Pass, got {:?}",
            report.probes[0].status
        );
        assert_eq!(report.failed_count(), 0);
    }

    #[tokio::test]
    async fn check_fails_when_port_is_already_bound() {
        use faucet_core::Source;
        use faucet_core::check::{CheckContext, ProbeStatus};

        // Hold a real listener, then point the source at the same address so
        // the probe's bind collides.
        let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = held.local_addr().unwrap();

        let source = WebhookSource::new(WebhookSourceConfig::new().listen_addr(addr.to_string()));
        let report = source.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].name, "io");
        assert!(
            matches!(report.probes[0].status, ProbeStatus::Fail { .. }),
            "expected Fail, got {:?}",
            report.probes[0].status
        );
        assert_eq!(report.failed_count(), 1);
        assert!(
            report.probes[0]
                .hint
                .as_deref()
                .unwrap()
                .contains("not bindable")
        );
    }

    #[tokio::test]
    async fn webhook_handles_non_json_body() {
        let state = Arc::new(AppState {
            inbox: Mutex::new(Inbox::default()),
            max_payloads: Some(1),
            done: Notify::new(),
            auth_token: None,
            signature: None,
        });

        let server_state = Arc::clone(&state);
        let app = Router::new()
            .route("/webhook", post(webhook_handler))
            .with_state(Arc::clone(&state));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let done_notified = server_state.done.notified();
            tokio::select! {
                result = axum::serve(listener, app).into_future() => {
                    if let Err(e) = result {
                        panic!("server error: {e}");
                    }
                }
                () = done_notified => {}
            }
        });

        let client = reqwest::Client::new();
        let resp = client
            .post(format!("http://{addr}/webhook"))
            .body("plain text body")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        server_handle.abort();

        let records = &state.inbox.lock().await.records;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0], Value::String("plain text body".into()));
    }

    #[test]
    fn dataset_uri_combines_addr_and_path() {
        use faucet_core::Source;
        let source = WebhookSource::new(
            WebhookSourceConfig::new()
                .listen_addr("127.0.0.1:8080")
                .path("/hooks/incoming"),
        );
        assert_eq!(
            source.dataset_uri(),
            "webhook://127.0.0.1:8080/hooks/incoming"
        );
    }

    fn sig(algorithm: SignatureAlgorithm, encoding: SignatureEncoding) -> WebhookSignature {
        WebhookSignature {
            header: "x-sig".into(),
            secret: "k".into(),
            algorithm,
            encoding,
            prefix: None,
            timestamp_header: None,
            tolerance_secs: 300,
        }
    }

    #[test]
    fn signature_variants_verify_and_reject() {
        use base64::Engine;
        use hmac::{Mac, digest::KeyInit};
        let body = b"payload";
        let mut mac = <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(b"k").unwrap();
        mac.update(body);
        let b64 = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let mut headers = axum::http::HeaderMap::new();
        headers.insert("x-sig", b64.parse().unwrap());
        let s512 = sig(SignatureAlgorithm::Sha512, SignatureEncoding::Base64);
        assert!(signature_ok(&s512, &headers, body, 0));
        assert!(!signature_ok(&s512, &headers, b"tampered", 0));
        // Wrong prefix, missing header, unparsable timestamp.
        let mut prefixed = s512.clone();
        prefixed.prefix = Some("v1=".into());
        assert!(!signature_ok(&prefixed, &headers, body, 0));
        assert!(!signature_ok(&s512, &axum::http::HeaderMap::new(), body, 0));
        let mut stamped = sig(SignatureAlgorithm::Sha256, SignatureEncoding::Hex);
        stamped.timestamp_header = Some("x-ts".into());
        headers.insert("x-ts", "not-a-number".parse().unwrap());
        assert!(!signature_ok(&stamped, &headers, body, 0));
    }

    #[test]
    fn signature_config_requires_header_and_secret() {
        let mut s = sig(SignatureAlgorithm::Sha256, SignatureEncoding::Hex);
        s.secret.clear();
        let cfg = WebhookSourceConfig::new().signature(s);
        assert!(cfg.validate().is_err());
    }
}

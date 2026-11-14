//! Webhook source stream executor.

use crate::config::{
    AckMode, SignatureAlgorithm, SignatureEncoding, WebhookSignature, WebhookSourceConfig,
};
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, routing::post};
use faucet_core::driver::{DriverSource, PageStream, SourceDriver};
use faucet_core::{CallSlot, CancellationToken, FaucetError, StreamPage};
use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;
use tokio::sync::{Mutex, Notify, oneshot};

/// Shared state for the webhook HTTP handler.
struct AppState {
    inbox: Mutex<Inbox>,
    max_payloads: Option<usize>,
    max_buffered_bytes: usize,
    done: Notify,
    /// Optional shared secret required in the `Authorization` header.
    auth_token: Option<String>,
    signature: Option<WebhookSignature>,
    /// `ack: on_commit`: hold each request until its page is committed.
    hold: bool,
    /// Most requests held at once (`0` = no cap).
    max_held: usize,
    /// Wakes the page stream when a request is held.
    arrived: Notify,
    /// Cancelled when the page stream ends or is dropped, so every request
    /// still held is answered `503` instead of waiting for its client to time
    /// out (connection tasks outlive the stream).
    abort: CancellationToken,
}

/// Stored payloads plus the closed flag, under one lock so a payload is either
/// stored before the window closes or refused with a retryable status.
#[derive(Default)]
struct Inbox {
    /// `ack: on_receive`: payloads stored this window.
    records: Vec<Value>,
    /// `ack: on_commit`: requests held until their page commits.
    held: VecDeque<Held>,
    /// Payloads accepted this window (the `max_payloads` count).
    stored: usize,
    /// Body bytes: accepted this window under `ack: on_receive`; held and not
    /// yet acknowledged under `ack: on_commit`.
    bytes: usize,
    /// `ack: on_commit`: requests taken into the page being written, still
    /// open until that page is acknowledged.
    in_page: usize,
    closed: bool,
    /// Signatures of stored timestamped requests, so a byte-identical resend
    /// inside the tolerance window is not stored twice.
    seen: HashMap<String, Seen>,
}

/// A held request: its record, and the channel its `200` is sent on once the
/// page holding it is committed.
struct Held {
    record: Value,
    bytes: usize,
    tx: oneshot::Sender<()>,
    at: Instant,
    key: Option<String>,
}

/// A signed request already stored: its signed timestamp, and whether its
/// page has been committed yet.
#[derive(Debug, Clone, Copy)]
struct Seen {
    ts: u64,
    committed: bool,
}

/// How long a refused sender is told to wait before retrying.
const RETRY_AFTER_SECS: &str = "60";

/// How long in-flight requests get to finish once the window closes.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// A page whose oldest request waited longer than this logs a warning: many
/// senders give up after 10–30 s and retry.
const SLOW_ACK: Duration = Duration::from_secs(10);

impl WebhookDriver {
    fn new_state(&self, hold: bool) -> Arc<AppState> {
        Arc::new(AppState {
            inbox: Mutex::new(Inbox::default()),
            max_payloads: self.config.max_payloads,
            max_buffered_bytes: self.config.max_buffered_bytes,
            done: Notify::new(),
            auth_token: self.config.auth_token.clone(),
            signature: self.config.signature.clone(),
            hold,
            max_held: self.config.max_held,
            arrived: Notify::new(),
            abort: CancellationToken::new(),
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
    /// The run's cancellation token (`ack: on_commit` drains on cancel).
    cancel: std::sync::Mutex<Option<CancellationToken>>,
}

impl WebhookSource {
    /// Create a new webhook source from the given configuration.
    pub fn new(config: WebhookSourceConfig) -> Self {
        Self {
            inner: DriverSource::new(WebhookDriver {
                config,
                cancel: std::sync::Mutex::new(None),
            }),
        }
    }

    /// Start the webhook server, collect payloads, and return them.
    ///
    /// This is a buffered read: it cannot know when the caller has written
    /// what it returns, so it answers each POST when it is received, whatever
    /// `ack` says. A pipeline reads through the page stream instead, which
    /// honours `ack: on_commit`.
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
        self.config.check_listener()?;
        let state = self.new_state(false);
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
        // A key whose page is still being written stays until it commits.
        inbox.seen.retain(|_, s| s.ts >= *cutoff || !s.committed);
        match inbox.seen.get(key) {
            // The same signed request was already stored: acknowledge it again.
            Some(s) if s.committed => return StatusCode::OK.into_response(),
            // Its first copy is still being written: ask the sender to retry.
            Some(_) => return retry_later(),
            None => {}
        }
    }
    if inbox.closed {
        return retry_later();
    }
    // `ack: on_commit` bounds what is open — held, or in the page being
    // written — rather than the window's total: over `max_held` requests or
    // `max_buffered_bytes` a POST is asked to retry and the window stays
    // open. Checked before the count cap, so a refused POST never closes the
    // window. An empty inbox always takes one POST, however large.
    if state.hold {
        let open = inbox.held.len() + inbox.in_page;
        let over_bytes = state.max_buffered_bytes > 0
            && open > 0
            && inbox.bytes.saturating_add(body.len()) > state.max_buffered_bytes;
        if (state.max_held > 0 && open >= state.max_held) || over_bytes {
            return retry_later();
        }
    }
    // Decide under the lock so the cap is exact: concurrent in-flight POSTs
    // that have already been accepted can't push the count past `max_payloads`.
    // A payload that is not stored — over the cap, or after the window closed
    // — is refused with a retryable status rather than a `200` the sender
    // would read as delivered (API-14).
    let decision = decide_payload(inbox.stored, state.max_payloads);
    // `ack: on_receive`: the window's bytes are bounded and a full window is
    // emitted. An empty window always takes the payload, as the count cap
    // does: a body larger than the byte cap (allowed by `max_body_bytes`)
    // would otherwise be refused in every window and retried forever.
    let over_bytes = !state.hold
        && state.max_buffered_bytes > 0
        && inbox.stored > 0
        && inbox.bytes.saturating_add(body.len()) > state.max_buffered_bytes;
    if decision.done || over_bytes {
        state.done.notify_one();
    }
    if !decision.accept || over_bytes {
        return retry_later();
    }
    inbox.bytes += body.len();
    inbox.stored += 1;
    if !state.hold && state.max_buffered_bytes > 0 && inbox.bytes >= state.max_buffered_bytes {
        // The window is full: emit it rather than wait for a refused POST.
        state.done.notify_one();
    }
    let key = replay_key.map(|(key, ts, _)| {
        inbox.seen.insert(
            key.clone(),
            Seen {
                ts,
                committed: !state.hold,
            },
        );
        key
    });
    if !state.hold {
        inbox.records.push(value);
        return StatusCode::OK.into_response();
    }
    let (tx, rx) = oneshot::channel();
    inbox.held.push_back(Held {
        record: value,
        bytes: body.len(),
        tx,
        at: Instant::now(),
        key,
    });
    state.arrived.notify_one();
    // Release the lock before waiting: the page stream takes held requests
    // under it.
    drop(inbox);
    tokio::select! {
        biased;
        acked = rx => match acked {
            Ok(()) => StatusCode::OK.into_response(),
            // The page was dropped unwritten (the run failed or was cancelled).
            Err(_) => retry_later(),
        },
        () = state.abort.cancelled() => retry_later(),
    }
}

/// `503 Service Unavailable` + `Retry-After`: the sender should try again.
fn retry_later() -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(axum::http::header::RETRY_AFTER, RETRY_AFTER_SECS)],
    )
        .into_response()
}

/// What woke the page stream while the window was open.
enum Wake {
    /// A request arrived, or the oldest held one reached `max_hold_secs`.
    Look,
    /// The window closed for this reason.
    Close(&'static str),
    /// The server task ended on its own.
    Server(Result<std::io::Result<()>, tokio::task::JoinError>),
}

/// One `ack: on_commit` receive window: the listener, and the guard that
/// answers every request still held with `503` however the stream ends.
struct Session {
    state: Arc<AppState>,
    stop: Option<oneshot::Sender<()>>,
    server: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    window_end: tokio::time::Instant,
    cancel: Option<CancellationToken>,
}

impl Session {
    async fn open(driver: &WebhookDriver, path: &str) -> Result<Self, FaucetError> {
        crate::config::validate_resolved_path(path)?;
        driver.config.check_listener()?;
        let state = driver.new_state(true);
        let app = driver.build_router(path, Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind(&driver.config.listen_addr)
            .await
            .map_err(|e| {
                FaucetError::Config(format!(
                    "failed to bind to {}: {e}",
                    driver.config.listen_addr
                ))
            })?;
        tracing::info!(
            addr = %driver.config.listen_addr,
            path = %path,
            "webhook server listening; each POST is answered once its page is written"
        );
        let (stop_tx, stop_rx) = oneshot::channel::<()>();
        let server = tokio::spawn(
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stop_rx.await;
                })
                .into_future(),
        );
        // Each run hands its own token through `set_cancel`; take it so a
        // later run of the same source does not inherit a cancelled one.
        let cancel = driver
            .cancel
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        Ok(Self {
            state,
            stop: Some(stop_tx),
            server: Some(server),
            window_end: tokio::time::Instant::now()
                + Duration::from_secs(driver.config.timeout_secs),
            cancel,
        })
    }

    /// The next page's held requests: `batch_size` of them (`0` = no count
    /// trigger), or what is held once the oldest has waited `max_hold`, or
    /// what is left once the window has closed. `None` when the window is
    /// closed and nothing is held.
    async fn next_batch(
        &mut self,
        batch_size: usize,
        max_hold: Duration,
    ) -> Result<Option<Vec<Held>>, FaucetError> {
        loop {
            let deadline = {
                let mut inbox = self.state.inbox.lock().await;
                if let Some(n) = take_count(
                    inbox.held.len(),
                    inbox.held.front().map(|h| h.at.elapsed()),
                    inbox.closed,
                    batch_size,
                    max_hold,
                ) {
                    inbox.in_page += n;
                    return Ok(Some(inbox.held.drain(..n).collect()));
                }
                if inbox.closed {
                    return Ok(None);
                }
                inbox
                    .held
                    .front()
                    .map(|h| tokio::time::Instant::from_std(h.at + max_hold))
            };
            let wake = {
                let server = self.server.as_mut();
                let cancel = self.cancel.clone();
                #[allow(clippy::disallowed_methods)] // the collection window, not a backend wait
                let aged = async {
                    match deadline {
                        Some(at) => tokio::time::sleep_until(at).await,
                        None => std::future::pending().await,
                    }
                };
                #[allow(clippy::disallowed_methods)] // the collection window, not a backend wait
                let window = tokio::time::sleep_until(self.window_end);
                tokio::select! {
                    () = self.state.arrived.notified() => Wake::Look,
                    () = aged => Wake::Look,
                    () = window => Wake::Close("webhook timeout reached"),
                    () = self.state.done.notified() => Wake::Close("max payloads reached"),
                    () = async {
                        match &cancel {
                            Some(token) => token.cancelled().await,
                            None => std::future::pending().await,
                        }
                    } => Wake::Close("run cancelled; emitting what is held"),
                    result = async {
                        match server {
                            Some(handle) => handle.await,
                            None => std::future::pending().await,
                        }
                    } => Wake::Server(result),
                }
            };
            match wake {
                Wake::Look => {}
                Wake::Close(why) => {
                    tracing::info!("{why}");
                    self.state.inbox.lock().await.closed = true;
                }
                Wake::Server(result) => {
                    self.server = None;
                    server_result(result)?;
                    return Err(FaucetError::Source(
                        "webhook server stopped before the receive window closed".into(),
                    ));
                }
            }
        }
    }

    /// Answer `200` to every request of a committed page, release what it
    /// held, and remember its signed requests as committed.
    async fn acknowledge(&self, page: Vec<(oneshot::Sender<()>, Option<String>)>, bytes: usize) {
        if page.is_empty() {
            return;
        }
        let mut inbox = self.state.inbox.lock().await;
        inbox.in_page = inbox.in_page.saturating_sub(page.len());
        inbox.bytes = inbox.bytes.saturating_sub(bytes);
        for key in page.iter().filter_map(|(_, k)| k.as_ref()) {
            if let Some(seen) = inbox.seen.get_mut(key) {
                seen.committed = true;
            }
        }
        drop(inbox);
        for (tx, _) in page {
            // The client may have gone; its record is still written.
            let _ = tx.send(());
        }
    }

    /// Every held request is answered: stop the server and let in-flight
    /// requests finish within the grace period.
    async fn finish(&mut self) -> Result<(), FaucetError> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut server) = self.server.take() {
            match tokio::time::timeout(SHUTDOWN_GRACE, &mut server).await {
                Ok(result) => server_result(result)?,
                Err(_) => server.abort(),
            }
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.state.abort.cancel();
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(mut server) = self.server.take() {
            match tokio::runtime::Handle::try_current() {
                Ok(rt) => {
                    rt.spawn(async move {
                        if tokio::time::timeout(SHUTDOWN_GRACE, &mut server)
                            .await
                            .is_err()
                        {
                            server.abort();
                        }
                    });
                }
                Err(_) => server.abort(),
            }
        }
    }
}

/// How many held requests to take into the next page, if any: `batch_size`
/// once that many are held (`0` = no count trigger), everything (up to
/// `batch_size`) once the oldest has waited `max_hold` or the window has
/// closed.
fn take_count(
    held: usize,
    oldest_waited: Option<Duration>,
    closed: bool,
    batch_size: usize,
    max_hold: Duration,
) -> Option<usize> {
    if held == 0 {
        return None;
    }
    let cap = if batch_size == 0 {
        held
    } else {
        batch_size.min(held)
    };
    let full = batch_size > 0 && held >= batch_size;
    let aged = oldest_waited.is_some_and(|w| w >= max_hold);
    (full || aged || closed).then_some(cap)
}

impl WebhookDriver {
    /// `ack: on_commit`: serve while the window is open and emit each page
    /// with a bookmark, so the pipeline writes and flushes it before polling
    /// again; a page's requests are answered `200` at that next poll.
    fn held_pages<'a>(&'a self, context: &'a HashMap<String, Value>) -> PageStream<'a> {
        let max_hold = Duration::from_secs(self.config.max_hold_secs);
        let batch_size = self.config.batch_size;
        Box::pin(faucet_core::async_stream::try_stream! {
            let path = self.resolve_path(context)?;
            let mut session = Session::open(self, &path).await?;
            let mut page_acks: Vec<(oneshot::Sender<()>, Option<String>)> = Vec::new();
            let mut page_bytes = 0usize;
            let mut received = 0usize;
            loop {
                // Being polled again means the last page was written and flushed.
                session
                    .acknowledge(std::mem::take(&mut page_acks), std::mem::take(&mut page_bytes))
                    .await;
                let Some(batch) = session.next_batch(batch_size, max_hold).await? else {
                    break;
                };
                received += batch.len();
                let oldest = batch.first().map(|h| h.at);
                let mut records = Vec::with_capacity(batch.len());
                for held in batch {
                    page_bytes += held.bytes;
                    records.push(held.record);
                    page_acks.push((held.tx, held.key));
                }
                tracing::debug!(records = records.len(), received, "webhook page emitted");
                yield StreamPage {
                    records,
                    bookmark: Some(serde_json::json!({ "received": received })),
                };
                if let Some(at) = oldest
                    && at.elapsed() > SLOW_ACK
                {
                    tracing::warn!(
                        waited_secs = at.elapsed().as_secs(),
                        "webhook: a sender waited over {}s for its event to be written; \
                         lower `max_hold_secs` or speed up the sink, or senders may time out \
                         and retry",
                        SLOW_ACK.as_secs()
                    );
                }
            }
            session.finish().await?;
            tracing::info!(records = received, "webhook receive window complete");
        })
    }

    /// The endpoint path for this read: the configured one, or (with a
    /// parent context) the template filled with percent-encoded values.
    fn resolve_path(&self, context: &HashMap<String, Value>) -> Result<String, FaucetError> {
        if context.is_empty() {
            self.config.validate()?;
            return Ok(self.config.path.clone());
        }
        // Percent-encode each value so a parent value cannot add segments or
        // route syntax (API-49).
        let encoded: HashMap<String, Value> = context
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
        Ok(faucet_core::util::substitute_context(
            &self.config.path,
            &encoded,
        ))
    }
}

#[async_trait]
impl SourceDriver for WebhookDriver {
    /// `ack: on_commit` streams pages while the window is open and answers
    /// each request after its page commits; `ack: on_receive` serves until
    /// the window closes and then emits what arrived, `batch_size` per page.
    fn read<'a>(
        &'a self,
        _calls: &'a CallSlot,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> PageStream<'a> {
        if self.config.ack == AckMode::OnCommit {
            return self.held_pages(context);
        }
        Box::pin(faucet_core::async_stream::try_stream! {
            let records = self.fetch_with_context(context).await?;
            for page in faucet_core::driver::pages_of(records, None, self.config.batch_size) {
                yield page;
            }
        })
    }

    /// With `ack: on_commit` a read answers senders as the pipeline writes
    /// their events, so reading without writing would acknowledge events
    /// nobody stored.
    fn consumes_destructively(&self) -> bool {
        self.config.ack == AckMode::OnCommit
    }

    /// With `ack: on_commit`, a cancel closes the window and the source emits
    /// what it holds, so it is written and answered rather than refused.
    fn set_cancel(&self, cancel: CancellationToken) -> bool {
        if self.config.ack != AckMode::OnCommit {
            return false;
        }
        *self.cancel.lock().unwrap_or_else(|e| e.into_inner()) = Some(cancel);
        true
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

        self.config.check_listener()?;
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
        let path = self.resolve_path(context)?;
        self.receive(&path).await
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)] // tests post to the server with a plain client
mod tests;

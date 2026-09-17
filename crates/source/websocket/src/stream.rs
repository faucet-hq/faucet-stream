//! WebSocket source stream executor.

use crate::config::{WebsocketAuth, WebsocketSourceConfig, decode_frame, shape_record};
use async_trait::async_trait;
use base64::Engine;
use faucet_core::{
    AuthSpec, Credential, FaucetError, SharedAuthProvider, Source, Stream, StreamPage,
};
use futures::{SinkExt, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue, header};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{Message, WebSocketConfig};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Delay before the next (re)connect after `attempt` consecutive failures,
/// using the configured `reconnect_backoff` as the exponential base.
///
/// It grows rather than repeating the base forever: with the default
/// `max_reconnect_attempts: None`, a constant base meant a dead endpoint was
/// redialled once per `reconnect_backoff` indefinitely — a client-side hot loop
/// against a host that is already down, and no cap because the config has no
/// ceiling field (#654 M6). [`faucet_core::retry::backoff_with_jitter`] supplies
/// both the ceiling and the decorrelating jitter, so many clients watching one
/// feed don't all redial on the same tick. Pure.
fn reconnect_delay(base: Duration, attempt: usize) -> Duration {
    faucet_core::retry::backoff_with_jitter(base, u32::try_from(attempt).unwrap_or(u32::MAX))
}

/// Map a [`Credential`] from a shared provider onto [`WebsocketAuth`] so the
/// existing header-application path can be reused.
///
/// This mapping is infallible: every credential kind can be expressed as either
/// `Bearer` or `Custom` headers.
fn credential_to_auth(cred: Credential) -> WebsocketAuth {
    use std::collections::BTreeMap;
    match cred {
        Credential::Bearer(token) => WebsocketAuth::Bearer { token },
        Credential::Token(t) => WebsocketAuth::Custom {
            headers: BTreeMap::from([("Authorization".to_string(), t)]),
        },
        Credential::Header { name, value } => WebsocketAuth::Custom {
            headers: BTreeMap::from([(name, value)]),
        },
        Credential::Basic { username, password } => {
            let encoded =
                base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
            WebsocketAuth::Custom {
                headers: BTreeMap::from([(
                    "Authorization".to_string(),
                    format!("Basic {encoded}"),
                )]),
            }
        }
    }
}

/// Apply `auth` to the HTTP upgrade `request`.
pub(crate) fn apply_auth(request: &mut Request, auth: &WebsocketAuth) -> Result<(), FaucetError> {
    let headers = request.headers_mut();
    match auth {
        WebsocketAuth::None => {}
        WebsocketAuth::Bearer { token } => {
            let value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| FaucetError::Config(format!("websocket bearer header: {e}")))?;
            headers.insert(header::AUTHORIZATION, value);
        }
        WebsocketAuth::Custom { headers: custom } => {
            for (k, v) in custom {
                let name = HeaderName::from_bytes(k.as_bytes())
                    .map_err(|e| FaucetError::Config(format!("websocket header name {k}: {e}")))?;
                let value = HeaderValue::from_str(v)
                    .map_err(|e| FaucetError::Config(format!("websocket header value {k}: {e}")))?;
                headers.insert(name, value);
            }
        }
    }
    Ok(())
}

/// A WebSocket streaming source.
pub struct WebsocketSource {
    config: WebsocketSourceConfig,
    /// Optional shared auth provider. When present, its [`Credential`] is
    /// resolved on every (re)connect so a freshly-rotated token is used after
    /// reconnect. Takes precedence over inline auth.
    auth_provider: Option<SharedAuthProvider>,
}

impl WebsocketSource {
    /// Create a new WebSocket source. Validates the config; the connection is
    /// established lazily inside the stream loop (so reconnect can re-establish
    /// it mid-run).
    pub fn new(config: WebsocketSourceConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        Ok(Self {
            config,
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider's credential is resolved on every (re)connect — so a
    /// refreshed token is used automatically after reconnect. Takes precedence
    /// over inline auth.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Connect, apply auth + size limits, and send the subscribe frames.
    ///
    /// Auth is resolved here (not once at construction) so that reconnects
    /// always pick up a freshly-rotated token from a shared provider.
    async fn connect(&self, url: &str) -> Result<WsStream, FaucetError> {
        let mut request = url
            .into_client_request()
            .map_err(|e| FaucetError::Config(format!("websocket url {url}: {e}")))?;

        // Resolve effective auth: provider-first, then inline, or error on Reference.
        let effective_auth = if let Some(p) = &self.auth_provider {
            credential_to_auth(p.credential().await?)
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
        apply_auth(&mut request, &effective_auth)?;

        let ws_config = self.config.max_message_bytes.map(|n| {
            WebSocketConfig::default()
                .max_message_size(Some(n))
                .max_frame_size(Some(n))
        });

        let (mut ws, _resp) = connect_async_with_config(request, ws_config, false)
            .await
            .map_err(|e| FaucetError::Source(format!("websocket connect {url}: {e}")))?;

        for msg in &self.config.subscribe_messages {
            ws.send(Message::Text(msg.clone().into()))
                .await
                .map_err(|e| FaucetError::Source(format!("websocket subscribe: {e}")))?;
        }
        Ok(ws)
    }
}

#[async_trait]
impl Source for WebsocketSource {
    /// Drain the entire run window into memory. This buffers every record the
    /// run produces (bounded only by `max_messages` / `idle_timeout`); prefer
    /// [`Source::stream_pages`] for large or long-running feeds so memory stays
    /// bounded at `batch_size`.
    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut out = Vec::new();
        let mut pages = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = pages.next().await {
            out.extend(page?.records);
        }
        Ok(out)
    }

    fn stream_pages<'a>(
        &'a self,
        context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let resolved_url = faucet_core::util::substitute_context(&self.config.url, context);
        let batch_size = self.config.batch_size;
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let idle_timeout = self.config.idle_timeout;
        let reconnect = self.config.reconnect;
        let reconnect_backoff = self.config.reconnect_backoff;
        let max_attempts = self.config.max_reconnect_attempts;
        let ping_interval = self.config.ping_interval;
        let format = self.config.message_format;
        let on_parse_error = self.config.on_parse_error;
        let envelope = self.config.envelope;

        Box::pin(async_stream::try_stream! {
            let mut buffer: Vec<Value> = Vec::new();
            let mut total: usize = 0;
            let mut last_message_at = Instant::now();
            let mut reconnect_attempts: usize = 0;

            'outer: loop {
                // Idle cap also bounds connect-failure spins and reconnect gaps.
                if let Some(t) = idle_timeout
                    && Instant::now() >= last_message_at + t
                {
                    tracing::debug!("websocket source: idle_timeout reached, stopping");
                    break 'outer;
                }

                // (Re)connect.
                let ws = match self.connect(&resolved_url).await {
                    Ok(ws) => {
                        reconnect_attempts = 0;
                        ws
                    }
                    Err(e) => {
                        if reconnect
                            && max_attempts.is_none_or(|m| reconnect_attempts < m)
                        {
                            let delay = reconnect_delay(reconnect_backoff, reconnect_attempts);
                            reconnect_attempts += 1;
                            tracing::warn!(error = %e, attempt = reconnect_attempts, delay_ms = delay.as_millis() as u64, "websocket source: connect failed, retrying");
                            tokio::time::sleep(delay).await;
                            continue 'outer;
                        }
                        Err(e)?;
                        break 'outer; // unreachable; satisfies the type checker
                    }
                };

                let (mut write, mut read) = ws.split();
                // Start one interval out so the first `tick()` does not fire
                // immediately (which would send a Ping before any read on every
                // (re)connect). `tokio::time::interval` ticks at t=0.
                let mut ping_timer = ping_interval.map(|interval| {
                    tokio::time::interval_at(tokio::time::Instant::now() + interval, interval)
                });

                loop {
                    let idle_deadline = idle_timeout.map(|t| last_message_at + t);
                    let poll_budget = match idle_deadline {
                        Some(d) => d.saturating_duration_since(Instant::now()),
                        None => Duration::from_secs(3600),
                    };

                    // Flags collected from the select arms; `?` cannot cross
                    // the select match boundary into the try_stream! body.
                    let mut stop = false;
                    let mut fatal: Option<FaucetError> = None;
                    let mut reconnect_now = false;

                    // Decode a data-frame payload (Text or Binary), shape it,
                    // push it, and update the run-window counters. The only
                    // per-arm difference is `t.as_bytes()` vs `&b`, so both
                    // arms funnel through this single closure.
                    let mut handle_payload = |payload: &[u8]| {
                        // A data frame (Text/Binary) arrived, so the server is
                        // delivering — reset the idle timer here, before decode,
                        // so a frame dropped by on_parse_error=skip (Ok(None))
                        // still counts as activity (#146 M9). Control frames
                        // (Ping/Pong/Close) deliberately do NOT reset it: a
                        // client keepalive (ping_interval) elicits pongs, and
                        // resetting on those would make idle_timeout unreachable
                        // whenever ping_interval < idle_timeout.
                        last_message_at = Instant::now();
                        match decode_frame(format, on_parse_error, payload) {
                            Ok(Some(v)) => {
                                let now = if envelope { now_unix_ms() } else { 0 };
                                buffer.push(shape_record(v, envelope, &resolved_url, now));
                                reconnect_attempts = 0;
                                total += 1;
                                if total >= max_messages {
                                    stop = true;
                                }
                            }
                            Ok(None) => {}
                            Err(e) => fatal = Some(e),
                        }
                    };

                    tokio::select! {
                        biased;
                        _ = tokio::signal::ctrl_c() => {
                            tracing::info!("websocket source: ctrl_c received, stopping cleanly");
                            stop = true;
                        }
                        _ = async { ping_timer.as_mut().unwrap().tick().await }, if ping_timer.is_some() => {
                            if let Err(e) = write.send(Message::Ping(Vec::new().into())).await {
                                tracing::warn!(error = %e, "websocket source: ping failed, treating as disconnect");
                                reconnect_now = true;
                            }
                        }
                        recv = tokio::time::timeout(poll_budget, read.next()) => {
                            match recv {
                                Ok(Some(Ok(msg))) => {
                                    match msg {
                                        Message::Text(t) => handle_payload(t.as_bytes()),
                                        Message::Binary(b) => handle_payload(&b),
                                        Message::Ping(payload) => {
                                            if let Err(e) = write.send(Message::Pong(payload)).await {
                                                tracing::warn!(error = %e, "websocket source: pong failed");
                                                reconnect_now = true;
                                            }
                                        }
                                        Message::Pong(_) | Message::Frame(_) => {}
                                        Message::Close(frame) => {
                                            let clean = frame
                                                .as_ref()
                                                .map(|f| f.code == CloseCode::Normal)
                                                .unwrap_or(true);
                                            if clean && !reconnect {
                                                tracing::info!("websocket source: server closed (1000), stopping");
                                                stop = true;
                                            } else {
                                                tracing::warn!(?frame, "websocket source: connection closed");
                                                reconnect_now = true;
                                            }
                                        }
                                    }
                                }
                                Ok(Some(Err(e))) => {
                                    tracing::warn!(error = %e, "websocket source: read error");
                                    reconnect_now = true;
                                }
                                Ok(None) => {
                                    tracing::warn!("websocket source: stream ended");
                                    reconnect_now = true;
                                }
                                Err(_elapsed) => {
                                    if let Some(d) = idle_deadline
                                        && Instant::now() >= d
                                    {
                                        tracing::debug!("websocket source: idle_timeout reached, stopping");
                                        stop = true;
                                    }
                                }
                            }
                        }
                    }

                    if let Some(e) = fatal {
                        Err(e)?;
                    }

                    if !buffer.is_empty() && buffer.len() >= page_chunk {
                        let page = std::mem::take(&mut buffer);
                        yield StreamPage { records: page, bookmark: None };
                    }

                    if stop {
                        break 'outer;
                    }

                    if reconnect_now {
                        // At-most-once across a reconnect: a live WebSocket has no
                        // replayable offset, so `continue 'outer` re-connects and
                        // re-subscribes to the *current* stream — any frames the
                        // server pushed during the disconnect gap are not replayed
                        // and are lost. Inherent to live feeds (documented in the
                        // README under "Not resumable"), not a bug.
                        if reconnect && max_attempts.is_none_or(|m| reconnect_attempts < m) {
                            let delay = reconnect_delay(reconnect_backoff, reconnect_attempts);
                            reconnect_attempts += 1;
                            tracing::warn!(attempt = reconnect_attempts, delay_ms = delay.as_millis() as u64, "websocket source: reconnecting");
                            tokio::time::sleep(delay).await;
                            continue 'outer;
                        } else if reconnect {
                            Err(FaucetError::Source(format!(
                                "websocket source: exceeded max_reconnect_attempts ({})",
                                max_attempts.unwrap_or(0)
                            )))?;
                        } else {
                            Err(FaucetError::Source(
                                "websocket source: connection closed and reconnect=false".into(),
                            ))?;
                        }
                    }
                }
            }

            if !buffer.is_empty() {
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(messages = total, "websocket source: stream complete");
        })
    }

    fn config_schema(&self) -> Value {
        let schema = schemars::schema_for!(WebsocketSourceConfig);
        serde_json::to_value(&schema).unwrap_or(Value::Null)
    }

    fn connector_name(&self) -> &'static str {
        "websocket"
    }

    fn dataset_uri(&self) -> String {
        faucet_core::redact_uri_credentials(&self.config.url)
    }

    /// Preflight probe that does **not** open the WebSocket stream.
    ///
    /// The default `Source::check` would call `stream_pages`, which connects,
    /// sends subscribe frames, and then blocks waiting for inbound frames until
    /// `max_messages` / `idle_timeout` — useless as a fast preflight. Instead we
    /// only verify TCP reachability of the configured endpoint: parse the
    /// `ws://`/`wss://` URL, resolve host + port (default 80 for `ws`, 443 for
    /// `wss`), open a [`tokio::net::TcpStream`] (no WS upgrade handshake, no
    /// auth, no frames), and immediately drop it.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let start = std::time::Instant::now();

        // Resolve host + port from the configured URL. The config is validated
        // at construction (`ws://`/`wss://`), so parse failures here are
        // probe-level failures, never panics.
        let (host, port) = match resolve_host_port(&self.config.url) {
            Ok(hp) => hp,
            Err(reason) => {
                return Ok(CheckReport::single(Probe::fail_hint(
                    "network",
                    start.elapsed(),
                    reason,
                    "url must be ws://host[:port]/... or wss://host[:port]/...",
                )));
            }
        };

        let connect = tokio::net::TcpStream::connect((host.as_str(), port));
        match tokio::time::timeout(ctx.timeout, connect).await {
            Ok(Ok(stream)) => {
                drop(stream);
                Ok(CheckReport::single(Probe::pass("network", start.elapsed())))
            }
            Ok(Err(e)) => Ok(CheckReport::single(Probe::fail_hint(
                "network",
                start.elapsed(),
                e.to_string(),
                format!("cannot reach {host}:{port} over TCP"),
            ))),
            Err(_elapsed) => Ok(CheckReport::single(Probe::fail_hint(
                "network",
                start.elapsed(),
                format!("TCP connect to {host}:{port} timed out"),
                format!("{host}:{port} did not accept a connection within the check timeout"),
            ))),
        }
    }
}

/// Parse a `ws://`/`wss://` URL into `(host, port)`, applying the scheme's
/// default port (80 for `ws`, 443 for `wss`) when none is specified.
///
/// Returns the human-readable reason string on failure (never leaks the full
/// URL, which may carry query-string credentials — only the host is surfaced).
fn resolve_host_port(url: &str) -> Result<(String, u16), String> {
    let request = url
        .into_client_request()
        .map_err(|e| format!("invalid websocket url: {e}"))?;
    let uri = request.uri();
    let host = uri
        .host()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| "websocket url is missing a host".to_string())?
        .to_string();
    let default_port = match uri.scheme_str() {
        Some("wss") => 443,
        _ => 80,
    };
    let port = uri.port_u16().unwrap_or(default_port);
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn reconnect_delay_grows_and_stays_bounded() {
        let base = Duration::from_secs(1);
        // Successive attempts must actually grow, not repeat the base forever
        // (#654 M6). Compare against the un-jittered `base * 2^attempt`, since
        // the jitter band ([0.5, 1.5)) overlaps between adjacent attempts and
        // makes a direct delay-to-delay comparison flaky.
        for attempt in 0..5u32 {
            let expected = base * 2u32.pow(attempt);
            let d = reconnect_delay(base, attempt as usize);
            assert!(
                d >= expected.mul_f64(0.5) && d < expected.mul_f64(1.5),
                "attempt {attempt}: {d:?} outside the jitter band around {expected:?}"
            );
            assert!(!d.is_zero(), "attempt {attempt}: delay collapsed to zero");
        }

        // Bounded: core caps the exponential at 60s, so even a saturating
        // attempt count stays under 60s × the 1.5 jitter ceiling. Without the
        // cap a long outage would sleep for centuries.
        for attempt in [30usize, 1_000, usize::MAX] {
            let d = reconnect_delay(base, attempt);
            assert!(
                d < Duration::from_secs(90),
                "attempt {attempt}: delay {d:?} is not bounded"
            );
            assert!(
                d >= Duration::from_secs(30),
                "attempt {attempt}: delay {d:?} should have reached the cap"
            );
        }
    }

    #[test]
    fn reconnect_delay_is_decorrelated_across_calls() {
        // Many clients watching one feed must not all redial on the same tick.
        let base = Duration::from_secs(1);
        let delays: Vec<Duration> = (0..32).map(|_| reconnect_delay(base, 2)).collect();
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "every delay identical — jitter is not being applied: {delays:?}"
        );
    }

    #[test]
    fn credential_bearer_maps_to_bearer() {
        let auth = credential_to_auth(Credential::Bearer("tok".into()));
        assert_eq!(
            auth,
            WebsocketAuth::Bearer {
                token: "tok".into()
            }
        );
    }

    #[test]
    fn credential_token_maps_to_custom_authorization() {
        let auth = credential_to_auth(Credential::Token("Custom xyz".into()));
        assert_eq!(
            auth,
            WebsocketAuth::Custom {
                headers: BTreeMap::from([("Authorization".into(), "Custom xyz".into())])
            }
        );
    }

    #[test]
    fn credential_header_maps_to_custom() {
        let auth = credential_to_auth(Credential::Header {
            name: "X-Api-Key".into(),
            value: "k123".into(),
        });
        assert_eq!(
            auth,
            WebsocketAuth::Custom {
                headers: BTreeMap::from([("X-Api-Key".into(), "k123".into())])
            }
        );
    }

    #[test]
    fn credential_basic_maps_to_base64_authorization() {
        let auth = credential_to_auth(Credential::Basic {
            username: "user".into(),
            password: "pass".into(),
        });
        // base64("user:pass") == "dXNlcjpwYXNz"
        assert_eq!(
            auth,
            WebsocketAuth::Custom {
                headers: BTreeMap::from([("Authorization".into(), "Basic dXNlcjpwYXNz".into())])
            }
        );
    }

    #[test]
    fn resolve_host_port_applies_scheme_defaults() {
        assert_eq!(
            resolve_host_port("ws://example.com/feed").unwrap(),
            ("example.com".to_string(), 80)
        );
        assert_eq!(
            resolve_host_port("wss://example.com/feed").unwrap(),
            ("example.com".to_string(), 443)
        );
        assert_eq!(
            resolve_host_port("wss://example.com:9443/feed").unwrap(),
            ("example.com".to_string(), 9443)
        );
    }

    #[tokio::test]
    async fn check_passes_against_a_live_tcp_listener() {
        use faucet_core::check::{CheckContext, ProbeStatus};

        // Bind a real TCP listener and point the source's URL at it. The probe
        // only needs the TCP handshake to succeed — no WS upgrade is performed,
        // so a plain listener is enough.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let config = WebsocketSourceConfig {
            url: format!("ws://{addr}/feed"),
            auth: AuthSpec::Inline(WebsocketAuth::None),
            subscribe_messages: vec![],
            message_format: crate::config::WsMessageFormat::Json,
            on_parse_error: crate::config::OnParseError::Fail,
            envelope: false,
            ping_interval: None,
            max_messages: Some(1),
            idle_timeout: None,
            reconnect: false,
            reconnect_backoff: Duration::from_secs(1),
            max_reconnect_attempts: None,
            max_message_bytes: None,
            batch_size: faucet_core::DEFAULT_BATCH_SIZE,
        };
        let source = WebsocketSource::new(config).unwrap();
        let report = source.check(&CheckContext::default()).await.unwrap();
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].name, "network");
        assert!(
            matches!(report.probes[0].status, ProbeStatus::Pass),
            "expected Pass, got {:?}",
            report.probes[0].status
        );
    }

    #[tokio::test]
    async fn check_fails_against_a_closed_port() {
        use faucet_core::check::{CheckContext, ProbeStatus};

        // Bind then drop a listener to obtain a port that is (almost certainly)
        // closed, so the connect is refused quickly.
        let addr = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };

        let config = WebsocketSourceConfig {
            url: format!("ws://{addr}/feed"),
            auth: AuthSpec::Inline(WebsocketAuth::None),
            subscribe_messages: vec![],
            message_format: crate::config::WsMessageFormat::Json,
            on_parse_error: crate::config::OnParseError::Fail,
            envelope: false,
            ping_interval: None,
            max_messages: Some(1),
            idle_timeout: None,
            reconnect: false,
            reconnect_backoff: Duration::from_secs(1),
            max_reconnect_attempts: None,
            max_message_bytes: None,
            batch_size: faucet_core::DEFAULT_BATCH_SIZE,
        };
        let source = WebsocketSource::new(config).unwrap();
        let report = source
            .check(&CheckContext {
                timeout: Duration::from_secs(2),
            })
            .await
            .unwrap();
        assert_eq!(report.probes.len(), 1);
        assert_eq!(report.probes[0].name, "network");
        assert!(
            matches!(report.probes[0].status, ProbeStatus::Fail { .. }),
            "expected Fail, got {:?}",
            report.probes[0].status
        );
        assert_eq!(report.failed_count(), 1);
    }

    fn make_config(url: &str) -> WebsocketSourceConfig {
        WebsocketSourceConfig {
            url: url.into(),
            auth: faucet_core::AuthSpec::Inline(crate::config::WebsocketAuth::None),
            subscribe_messages: vec![],
            message_format: crate::config::WsMessageFormat::Json,
            on_parse_error: crate::config::OnParseError::Fail,
            envelope: false,
            ping_interval: None,
            max_messages: Some(1),
            idle_timeout: None,
            reconnect: false,
            reconnect_backoff: Duration::from_secs(1),
            max_reconnect_attempts: None,
            max_message_bytes: None,
            batch_size: faucet_core::DEFAULT_BATCH_SIZE,
        }
    }

    #[test]
    fn dataset_uri_returns_url() {
        use faucet_core::Source;
        let source = WebsocketSource::new(make_config("wss://stream.example.com/feed")).unwrap();
        assert_eq!(source.dataset_uri(), "wss://stream.example.com/feed");
    }

    #[test]
    fn dataset_uri_redacts_credentials() {
        use faucet_core::Source;
        let source =
            WebsocketSource::new(make_config("wss://user:pass@stream.example.com/feed")).unwrap();
        assert_eq!(source.dataset_uri(), "wss://stream.example.com/feed");
    }
}

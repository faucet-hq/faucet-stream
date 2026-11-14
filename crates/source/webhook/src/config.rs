//! Webhook source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default [`WebhookSourceConfig::max_buffered_bytes`]: 256 MiB.
pub const DEFAULT_MAX_BUFFERED_BYTES: usize = 256 * 1024 * 1024;

/// Default [`WebhookSourceConfig::max_hold_secs`]: 5 seconds.
pub const DEFAULT_MAX_HOLD_SECS: u64 = 5;

/// Largest accepted [`WebhookSourceConfig::max_hold_secs`].
pub const MAX_HOLD_SECS: u64 = 300;

/// Default [`WebhookSourceConfig::max_held`]: 2000 requests, room for one
/// default-sized page being written and the next one filling.
pub const DEFAULT_MAX_HELD: usize = 2000;

/// When the source answers a sender's POST.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Answer `200` only after the page holding the event has been written
    /// and flushed by the pipeline; answer `503` + `Retry-After` when the run
    /// fails first, so the sender retries. Nothing acknowledged is lost; a
    /// sender may see a duplicate after a crash or a slow commit.
    #[default]
    OnCommit,
    /// Answer `200` as soon as the event is held in memory, and emit the
    /// window's events when it closes. Events acknowledged this way are lost
    /// if the run fails before the sink commits them.
    OnReceive,
}

/// Configuration for the webhook receiver source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[serde(deny_unknown_fields)]
pub struct WebhookSourceConfig {
    /// Address to bind the HTTP server to (default: `"127.0.0.1:8080"`).
    ///
    /// **Security:** the default binds to loopback only. Binding to
    /// `0.0.0.0` exposes the receiver to the whole network — only do so
    /// behind a trusted gateway, and set `auth_token` to require a shared
    /// secret on every request.
    #[schemars(extend("x-faucet-resource" = "listen"))]
    pub listen_addr: String,
    /// Endpoint path for receiving webhooks (default: `"/webhook"`).
    pub path: String,
    /// Stop after receiving this many payloads.
    #[schemars(extend("x-faucet-caller-max" = 1_000_000))]
    pub max_payloads: Option<usize>,
    /// How long to listen before returning, in seconds (default: 30).
    #[schemars(extend("x-faucet-caller-max" = 3_600))]
    pub timeout_secs: u64,
    /// Maximum accepted request body size in bytes (default: 1 MiB). Larger
    /// POSTs are rejected with `413 Payload Too Large` so a single huge
    /// request can't exhaust memory.
    pub max_body_bytes: usize,
    /// Most request-body bytes held in memory (default: 256 MiB). With `ack:
    /// on_receive` it bounds one window: once a POST would cross it the window
    /// closes, the payloads already stored are emitted, and that POST and
    /// later ones are refused with `503 Service Unavailable` + `Retry-After`.
    /// With `ack: on_commit` it bounds the bodies held and not yet
    /// acknowledged: a POST that would cross it is refused with `503` +
    /// `Retry-After` and the window stays open. An empty inbox always takes
    /// one POST, however large (`max_body_bytes` bounds it). `0` removes the
    /// bound.
    #[schemars(extend("x-faucet-caller-max" = 1_073_741_824))]
    pub max_buffered_bytes: usize,
    /// Optional shared secret. When set, every request must carry it in the
    /// `Authorization` header (either the raw token or `Bearer <token>`);
    /// requests without it are rejected with `401 Unauthorized`. When `None`
    /// (default) the endpoint is unauthenticated.
    pub auth_token: Option<String>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage) (default
    /// [`DEFAULT_BATCH_SIZE`]). With `ack: on_commit` a page is emitted while
    /// the window is open, as soon as this many requests are held or the
    /// oldest has waited `max_hold_secs`; `0` drops the count trigger. With
    /// `ack: on_receive` the window's events are emitted when it closes, in
    /// pages of this size; `0` emits them as one page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional HMAC request-signature verification for senders that sign the
    /// raw body instead of sending a static secret. Unsigned or mis-signed
    /// requests are rejected with `401`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<WebhookSignature>,
    /// When a sender's POST is answered (default `on_commit`): after the
    /// pipeline has written and flushed its event (`on_commit`), or as soon
    /// as it is held in memory (`on_receive`, which loses acknowledged events
    /// if the run fails before the sink commits). With `on_commit`, commands
    /// that read without writing (`--dry-run`, `--limit`, `preview`, `plan
    /// --live`, `verify`), topology graphs and `write_mode: overwrite` refuse
    /// this source.
    pub ack: AckMode,
    /// `ack: on_commit`: the longest a held request waits before its page is
    /// emitted, in seconds (default 5, at most 300). A sender then waits for
    /// up to this plus the sink's write time, twice over when a write is
    /// already in progress, so keep it well under the senders' timeout.
    #[schemars(extend("x-faucet-caller-max" = 300))]
    pub max_hold_secs: u64,
    /// `ack: on_commit`: the most requests open at once, waiting for a page or
    /// in the page being written (default 2000). Above it a POST is refused
    /// with `503` + `Retry-After` and not stored, so a slow sink cannot pile
    /// up open connections. `0` removes the cap.
    #[schemars(extend("x-faucet-caller-max" = 100_000))]
    pub max_held: usize,
}

/// HMAC signature verification over the raw request body.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WebhookSignature {
    /// Request header carrying the signature (e.g. `X-Hub-Signature-256`).
    pub header: String,
    /// The shared signing secret.
    pub secret: String,
    /// HMAC digest (default `sha256`).
    #[serde(default)]
    pub algorithm: SignatureAlgorithm,
    /// How the digest is written in the header (default `hex`).
    #[serde(default)]
    pub encoding: SignatureEncoding,
    /// Text before the digest in the header value (e.g. `sha256=`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Header carrying the Unix-seconds timestamp the sender signed. When set,
    /// the signed content is `<timestamp>.<body>`, a timestamp more than
    /// `tolerance_secs` from now is rejected, and a request repeating one
    /// already stored in this window is acknowledged without being stored
    /// again. Without it, identical signed requests are each stored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_header: Option<String>,
    /// Accepted clock difference for `timestamp_header`, in seconds (default 300).
    #[serde(default = "default_tolerance_secs")]
    pub tolerance_secs: u64,
}

/// HMAC digest for [`WebhookSignature`].
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignatureAlgorithm {
    /// HMAC-SHA256.
    #[default]
    Sha256,
    /// HMAC-SHA512.
    Sha512,
}

/// Text encoding of the signature digest.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignatureEncoding {
    /// Hexadecimal (case-insensitive).
    #[default]
    Hex,
    /// Standard base64.
    Base64,
}

fn default_tolerance_secs() -> u64 {
    300
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Check a (possibly context-substituted) endpoint path before it reaches the
/// router, which panics on an invalid one (API-49).
pub fn validate_path(path: &str) -> Result<(), faucet_core::FaucetError> {
    let bad = |why: &str| {
        faucet_core::FaucetError::Config(format!("webhook source: `path` {path:?} {why}"))
    };
    if !path.starts_with('/') {
        return Err(bad("must start with `/`"));
    }
    if path.contains(['?', '#']) || path.chars().any(char::is_whitespace) {
        return Err(bad("must not contain `?`, `#` or whitespace"));
    }
    for seg in path.split('/') {
        if seg.starts_with(':') || seg.starts_with('*') {
            return Err(bad("must not have a segment starting with `:` or `*`"));
        }
        let braces = seg.contains(['{', '}']);
        let placeholder = seg.len() > 2
            && seg.starts_with('{')
            && seg.ends_with('}')
            && !seg[1..seg.len() - 1].contains(['{', '}', '*']);
        if braces && !placeholder {
            return Err(bad("may use braces only as a whole `{name}` segment"));
        }
    }
    Ok(())
}

/// [`validate_path`] for the path the server actually serves: a `{name}` left
/// over after context substitution would become a route capture matching any
/// segment, so it is refused.
pub(crate) fn validate_resolved_path(path: &str) -> Result<(), faucet_core::FaucetError> {
    validate_path(path)?;
    if path.contains(['{', '}']) {
        return Err(faucet_core::FaucetError::Config(format!(
            "webhook source: `path` {path:?} has a `{{name}}` placeholder the context did not fill"
        )));
    }
    Ok(())
}

impl Default for WebhookSourceConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:8080".into(),
            path: "/webhook".into(),
            max_payloads: None,
            timeout_secs: 30,
            max_body_bytes: 1024 * 1024,
            max_buffered_bytes: DEFAULT_MAX_BUFFERED_BYTES,
            auth_token: None,
            batch_size: DEFAULT_BATCH_SIZE,
            signature: None,
            ack: AckMode::OnCommit,
            max_hold_secs: DEFAULT_MAX_HOLD_SECS,
            max_held: DEFAULT_MAX_HELD,
        }
    }
}

impl WebhookSourceConfig {
    /// Hold `listen_addr` to the reference policy scoped over the run, if
    /// any ([`faucet_core::http::current_reference_policy`]): an address the
    /// policy's listener ranges allow, and, off loopback, `auth_token` or
    /// `signature` set.
    pub fn check_listener(&self) -> Result<(), faucet_core::FaucetError> {
        let Some(policy) = faucet_core::http::current_reference_policy() else {
            return Ok(());
        };
        policy.check_listen(&self.listen_addr).map_err(|r| {
            faucet_core::FaucetError::Config(format!("webhook source: `listen_addr` {r}"))
        })?;
        let open_to_network = policy.listen_ranges().is_some()
            && self
                .listen_addr
                .trim()
                .parse::<std::net::SocketAddr>()
                .is_ok_and(|a| !a.ip().is_loopback());
        let token = self
            .auth_token
            .as_ref()
            .is_some_and(|t| !t.trim().is_empty());
        let signature = self
            .signature
            .as_ref()
            .is_some_and(|s| !s.secret.is_empty());
        if open_to_network && !token && !signature {
            return Err(faucet_core::FaucetError::Config(format!(
                "webhook source: `listen_addr` {} is reachable from the network; set \
                 `auth_token` or `signature`",
                self.listen_addr
            )));
        }
        Ok(())
    }

    /// Create a new config with sensible defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Validate the path and signature settings.
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        validate_path(&self.path)?;
        self.validate_settings()
    }

    /// Everything [`validate`](Self::validate) checks except the path, for a
    /// path template that is resolved against a context first.
    pub(crate) fn validate_settings(&self) -> Result<(), faucet_core::FaucetError> {
        if self
            .auth_token
            .as_ref()
            .is_some_and(|t| t.trim().is_empty())
        {
            return Err(faucet_core::FaucetError::Config(
                "webhook source: `auth_token` must not be empty".into(),
            ));
        }
        if let Some(sig) = &self.signature
            && (sig.header.trim().is_empty() || sig.secret.is_empty())
        {
            return Err(faucet_core::FaucetError::Config(
                "webhook source: `signature.header` and `signature.secret` must not be empty"
                    .into(),
            ));
        }
        if !(1..=MAX_HOLD_SECS).contains(&self.max_hold_secs) {
            return Err(faucet_core::FaucetError::Config(format!(
                "webhook source: `max_hold_secs` must be between 1 and {MAX_HOLD_SECS}, got {}",
                self.max_hold_secs
            )));
        }
        Ok(())
    }

    /// Set the listen address.
    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = addr.into();
        self
    }

    /// Set the webhook endpoint path.
    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// Stop after receiving this many payloads.
    pub fn max_payloads(mut self, max: usize) -> Self {
        self.max_payloads = Some(max);
        self
    }

    /// Set the most request-body bytes held in memory for one window.
    pub fn max_buffered_bytes(mut self, bytes: usize) -> Self {
        self.max_buffered_bytes = bytes;
        self
    }

    /// Set the timeout in seconds.
    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout_secs = secs;
        self
    }

    /// Set the maximum accepted request body size in bytes.
    pub fn max_body_bytes(mut self, bytes: usize) -> Self {
        self.max_body_bytes = bytes;
        self
    }

    /// Verify an HMAC request signature on every request.
    pub fn signature(mut self, signature: WebhookSignature) -> Self {
        self.signature = Some(signature);
        self
    }

    /// Require a shared-secret token in the `Authorization` header.
    pub fn auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Set when a sender's POST is answered.
    pub fn ack(mut self, ack: AckMode) -> Self {
        self.ack = ack;
        self
    }

    /// Set the longest a held request waits before its page is emitted.
    pub fn max_hold_secs(mut self, secs: u64) -> Self {
        self.max_hold_secs = secs;
        self
    }

    /// Set the most requests held open at once (`0` = no cap).
    pub fn max_held(mut self, max: usize) -> Self {
        self.max_held = max;
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire flush window is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage). The webhook source's
    /// server-side buffering is unaffected; only the downstream chunking
    /// changes.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_listener_is_held_to_the_runs_reference_policy() {
        use faucet_core::resolve::{ListenRange, ReferencePolicy};
        let at = |addr: &str| WebhookSourceConfig::new().listen_addr(addr);
        at("127.0.0.1:0").check_listener().unwrap();
        let policy = std::sync::Arc::new(ReferencePolicy::deny_all().with_listen([
            ListenRange::parse("127.0.0.1:29100-29199").unwrap(),
            ListenRange::parse("[::]:9000").unwrap(),
        ]));
        let checked = |cfg: WebhookSourceConfig| {
            faucet_core::http::with_reference_policy(
                policy.clone(),
                async move { cfg.check_listener() },
            )
        };
        checked(at("127.0.0.1:29100")).await.unwrap();
        let e = checked(at("127.0.0.1:8080")).await.unwrap_err().to_string();
        assert!(e.contains("127.0.0.1:29100-29199"), "{e}");
        let e = checked(at("0.0.0.0:9000")).await.unwrap_err().to_string();
        assert!(e.contains("auth_token"), "{e}");
        checked(at("0.0.0.0:9000").auth_token("t")).await.unwrap();
        let e = checked(at("0.0.0.0:9000").auth_token(" "))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("auth_token"), "{e}");
        // An operator's run binds where it says.
        faucet_core::http::with_reference_policy(
            std::sync::Arc::new(ReferencePolicy::open()),
            async { at("0.0.0.0:8080").check_listener() },
        )
        .await
        .unwrap();
    }

    #[test]
    fn an_empty_auth_token_is_refused() {
        for blank in ["", " \t"] {
            let e = WebhookSourceConfig::new()
                .auth_token(blank)
                .validate()
                .unwrap_err()
                .to_string();
            assert!(e.contains("`auth_token` must not be empty"), "{e}");
        }
        WebhookSourceConfig::new()
            .auth_token("t")
            .validate()
            .unwrap();
    }

    #[test]
    fn a_resolved_path_may_not_keep_a_placeholder() {
        assert!(validate_resolved_path("/hooks/user-7").is_ok());
        let err = validate_resolved_path("/hooks/{id}")
            .unwrap_err()
            .to_string();
        assert!(err.contains("did not fill"), "{err}");
        assert!(validate_resolved_path("hooks").is_err());
    }

    #[test]
    fn default_config() {
        let config = WebhookSourceConfig::new();
        assert_eq!(
            config.listen_addr, "127.0.0.1:8080",
            "default must bind loopback only, not 0.0.0.0"
        );
        assert_eq!(config.path, "/webhook");
        assert!(config.max_payloads.is_none());
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_body_bytes, 1024 * 1024);
        assert!(config.auth_token.is_none());
        assert_eq!(config.ack, AckMode::OnCommit);
        assert_eq!(config.max_hold_secs, DEFAULT_MAX_HOLD_SECS);
        assert_eq!(config.max_held, DEFAULT_MAX_HELD);
    }

    #[test]
    fn ack_settings_parse_and_max_hold_is_bounded() {
        let config: WebhookSourceConfig =
            serde_json::from_str(r#"{"ack": "on_receive", "max_hold_secs": 2, "max_held": 0}"#)
                .unwrap();
        assert_eq!(config.ack, AckMode::OnReceive);
        assert_eq!((config.max_hold_secs, config.max_held), (2, 0));
        for bad in [0, MAX_HOLD_SECS + 1] {
            let e = WebhookSourceConfig::new()
                .max_hold_secs(bad)
                .validate()
                .unwrap_err()
                .to_string();
            assert!(
                e.contains("`max_hold_secs` must be between 1 and 300"),
                "{e}"
            );
        }
        let built = WebhookSourceConfig::new()
            .ack(AckMode::OnReceive)
            .max_hold_secs(MAX_HOLD_SECS)
            .max_held(3);
        built.validate().unwrap();
        assert_eq!((built.ack, built.max_held), (AckMode::OnReceive, 3));
        assert!(serde_json::from_str::<WebhookSourceConfig>(r#"{"ack": "later"}"#).is_err());
    }

    #[test]
    fn builder_methods() {
        let config = WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:9090")
            .path("/hooks/incoming")
            .max_payloads(10)
            .timeout_secs(60);
        assert_eq!(config.listen_addr, "127.0.0.1:9090");
        assert_eq!(config.path, "/hooks/incoming");
        assert_eq!(config.max_payloads, Some(10));
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = WebhookSourceConfig::new();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = WebhookSourceConfig::new().with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = WebhookSourceConfig::new().with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = WebhookSourceConfig::new().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "listen_addr": "127.0.0.1:8080",
            "path": "/webhook",
            "timeout_secs": 30,
            "batch_size": 500
        }"#;
        let config: WebhookSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 500);
    }
}

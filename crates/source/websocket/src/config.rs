//! Configuration types for the WebSocket source.

use base64::Engine;
use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::time::Duration;

/// Configuration for the WebSocket source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WebsocketSourceConfig {
    /// WebSocket endpoint, `ws://` or `wss://`. Supports `{placeholder}`
    /// parent-matrix context substitution.
    pub url: String,

    /// Authentication applied to the HTTP upgrade request. Either inline
    /// (`{ type, config }`) or a `{ ref: <name> }` pointer to a shared
    /// provider in the CLI's top-level `auth:` catalog.
    #[serde(default)]
    pub auth: AuthSpec<WebsocketAuth>,

    /// Subscription frames sent (in order) immediately after every
    /// (re)connect. Empty = send nothing.
    #[serde(default)]
    pub subscribe_messages: Vec<String>,

    /// How to interpret each incoming frame.
    #[serde(default)]
    pub message_format: WsMessageFormat,

    /// In `Json` mode, what to do when a frame is not valid JSON.
    #[serde(default)]
    pub on_parse_error: OnParseError,

    /// `false` (default) emits the record raw; `true` wraps it as
    /// `{ data, received_at, url }`.
    #[serde(default)]
    pub envelope: bool,

    /// If set, send a WebSocket Ping frame on this interval (seconds) to keep
    /// the connection alive through proxies/load balancers.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "faucet_core::config::duration_secs_option"
    )]
    #[schemars(with = "Option<u64>")]
    pub ping_interval: Option<Duration>,

    /// Stop after this many messages. At least one of `max_messages` /
    /// `idle_timeout` must be set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,

    /// Stop after this many seconds with no message. The idle clock keeps
    /// ticking across reconnect gaps, so it also caps a connection outage.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "faucet_core::config::duration_secs_option"
    )]
    #[schemars(with = "Option<u64>")]
    pub idle_timeout: Option<Duration>,

    /// Reconnect on transport error / non-1000 close.
    #[serde(default)]
    pub reconnect: bool,

    /// Base wait (seconds) for reconnect backoff. Default 1s.
    ///
    /// Retries grow **exponentially with jitter** from this base and are
    /// capped, so a dead endpoint is not dialled at a fixed rate forever.
    /// Set it to the delay you want before the *first* retry.
    #[serde(
        default = "default_backoff",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub reconnect_backoff: Duration,

    /// Cap on *consecutive* failed reconnects (resets on any received
    /// message). `None` = unlimited (then `idle_timeout` is the natural cap).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_reconnect_attempts: Option<usize>,

    /// Bound the max WebSocket message/frame size (bytes) to prevent runaway
    /// memory. `None` = tungstenite default (64 MiB message / 16 MiB frame).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_message_bytes: Option<usize>,

    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Default
    /// [`DEFAULT_BATCH_SIZE`]. `0` drains the entire run window into a single
    /// page (same sentinel as the Kafka source).
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_backoff() -> Duration {
    Duration::from_secs(1)
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Authentication for the WebSocket upgrade request.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum WebsocketAuth {
    /// No authentication (default).
    #[default]
    None,
    /// `Authorization: Bearer <token>`.
    Bearer { token: String },
    /// Arbitrary request headers.
    Custom { headers: BTreeMap<String, String> },
}

/// How each incoming frame is converted into a record.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WsMessageFormat {
    /// Parse the frame payload as JSON (default).
    #[default]
    Json,
    /// Emit the frame payload as a UTF-8 string (lossy for invalid UTF-8).
    RawString,
    /// Base64-encode the frame payload as a string.
    Binary,
}

/// What to do when a `Json`-mode frame is not valid JSON.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnParseError {
    /// Abort the run with a [`FaucetError::Source`] (default).
    #[default]
    Fail,
    /// Log a warning and drop the frame.
    Skip,
}

impl WebsocketSourceConfig {
    /// Validate the config at construction time. Called by `WebsocketSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        let url = self.url.trim();
        if url.is_empty() {
            return Err(FaucetError::Config(
                "websocket source: url must not be empty".into(),
            ));
        }
        if !(url.starts_with("ws://") || url.starts_with("wss://")) {
            return Err(FaucetError::Config(format!(
                "websocket source: url must start with ws:// or wss:// (got {url})"
            )));
        }
        if self.max_messages.is_none() && self.idle_timeout.is_none() {
            return Err(FaucetError::Config(
                "websocket source: at least one of max_messages or idle_timeout must be set".into(),
            ));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages). Pass `0` to
    /// drain the entire run window into a single page.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// Convert a data-frame payload into a record value per `format`.
///
/// Returns `Ok(None)` only when `on_parse_error == Skip` swallows an invalid
/// `Json` frame; the caller drops it. Used for both Text and Binary frames —
/// `payload` is the raw frame bytes (for Text frames, the UTF-8 bytes).
pub(crate) fn decode_frame(
    format: WsMessageFormat,
    on_parse_error: OnParseError,
    payload: &[u8],
) -> Result<Option<Value>, FaucetError> {
    match format {
        WsMessageFormat::Json => match serde_json::from_slice::<Value>(payload) {
            Ok(v) => Ok(Some(v)),
            Err(e) => match on_parse_error {
                OnParseError::Fail => {
                    Err(FaucetError::Source(format!("websocket json parse: {e}")))
                }
                OnParseError::Skip => {
                    tracing::warn!(error = %e, "websocket source: dropping non-JSON frame");
                    Ok(None)
                }
            },
        },
        WsMessageFormat::RawString => Ok(Some(Value::String(
            String::from_utf8_lossy(payload).into_owned(),
        ))),
        WsMessageFormat::Binary => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(payload);
            Ok(Some(Value::String(encoded)))
        }
    }
}

/// Wrap (or not) the decoded value into the emitted record shape.
///
/// `now_ms` is injected so the function stays pure and testable; the stream
/// loop passes `now_unix_ms()` only when `envelope` is true.
pub(crate) fn shape_record(value: Value, envelope: bool, url: &str, now_ms: u64) -> Value {
    if envelope {
        json!({ "data": value, "received_at": now_ms, "url": url })
    } else {
        value
    }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    fn minimal() -> WebsocketSourceConfig {
        WebsocketSourceConfig {
            url: "wss://example.com/ws".into(),
            auth: AuthSpec::Inline(WebsocketAuth::None),
            subscribe_messages: vec![],
            message_format: WsMessageFormat::Json,
            on_parse_error: OnParseError::Fail,
            envelope: false,
            ping_interval: None,
            max_messages: Some(10),
            idle_timeout: None,
            reconnect: false,
            reconnect_backoff: Duration::from_secs(1),
            max_reconnect_attempts: None,
            max_message_bytes: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    #[test]
    fn validate_accepts_minimal() {
        assert!(minimal().validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_url() {
        let mut c = minimal();
        c.url = "  ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_non_ws_scheme() {
        let mut c = minimal();
        c.url = "https://example.com".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_no_termination() {
        let mut c = minimal();
        c.max_messages = None;
        c.idle_timeout = None;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_idle_only() {
        let mut c = minimal();
        c.max_messages = None;
        c.idle_timeout = Some(Duration::from_secs(5));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_rejects_oversize_batch() {
        let mut c = minimal();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn auth_bearer_round_trips_as_adjacently_tagged() {
        // WebsocketAuth uses tag="type", content="config" (adjacent tagging).
        let json = serde_json::json!({"type": "bearer", "config": {"token": "abc"}});
        let auth: WebsocketAuth = serde_json::from_value(json).unwrap();
        assert_eq!(
            auth,
            WebsocketAuth::Bearer {
                token: "abc".into()
            }
        );
    }

    #[test]
    fn auth_spec_inline_round_trips() {
        // AuthSpec wraps WebsocketAuth; the inline shape uses the adjacent-tagged format.
        let json = serde_json::json!({"type": "bearer", "config": {"token": "tok"}});
        let spec: AuthSpec<WebsocketAuth> = serde_json::from_value(json).unwrap();
        assert!(matches!(
            spec,
            AuthSpec::Inline(WebsocketAuth::Bearer { .. })
        ));
    }

    #[test]
    fn auth_spec_ref_round_trips() {
        let json = serde_json::json!({"ref": "my-provider"});
        let spec: AuthSpec<WebsocketAuth> = serde_json::from_value(json).unwrap();
        assert_eq!(spec.reference_name(), Some("my-provider"));
    }
}

#[cfg(test)]
mod helper_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn decode_json_object() {
        let v = decode_frame(WsMessageFormat::Json, OnParseError::Fail, br#"{"a":1}"#)
            .unwrap()
            .unwrap();
        assert_eq!(v, json!({"a": 1}));
    }

    #[test]
    fn decode_json_invalid_fails() {
        let r = decode_frame(WsMessageFormat::Json, OnParseError::Fail, b"not json");
        assert!(r.is_err());
    }

    #[test]
    fn decode_json_invalid_skipped_yields_none() {
        let r = decode_frame(WsMessageFormat::Json, OnParseError::Skip, b"not json").unwrap();
        assert!(r.is_none());
    }

    #[test]
    fn decode_raw_string() {
        let v = decode_frame(WsMessageFormat::RawString, OnParseError::Fail, b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(v, json!("hello"));
    }

    #[test]
    fn decode_binary_base64() {
        let v = decode_frame(WsMessageFormat::Binary, OnParseError::Fail, b"hello")
            .unwrap()
            .unwrap();
        assert_eq!(v, json!("aGVsbG8=")); // base64("hello")
    }

    #[test]
    fn shape_raw_passthrough() {
        let v = shape_record(json!({"a": 1}), false, "wss://x", 123);
        assert_eq!(v, json!({"a": 1}));
    }

    #[test]
    fn shape_enveloped() {
        let v = shape_record(json!({"a": 1}), true, "wss://x", 123);
        assert_eq!(
            v,
            json!({"data": {"a": 1}, "received_at": 123, "url": "wss://x"})
        );
    }
}

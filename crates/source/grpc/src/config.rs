//! gRPC source configuration.

use faucet_core::{AuthSpec, DEFAULT_BATCH_SIZE};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;
use std::time::Duration;

/// A single piece of gRPC request metadata.
///
/// Use a `Vec<MetadataEntry>` rather than a map because gRPC allows duplicate
/// keys and order is occasionally observable.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(extend("x-faucet-aliases" = ["max_reconnect_attempts"]))]
#[serde(deny_unknown_fields)]
pub struct MetadataEntry {
    pub key: String,
    pub value: String,
}

/// Authentication for gRPC endpoints.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum GrpcAuth {
    /// No authentication.
    #[default]
    None,
    /// Bearer token sent as `authorization` metadata.
    Bearer { token: String },
    /// Custom metadata key-value pairs.
    Metadata { entries: Vec<MetadataEntry> },
}

/// Kind of gRPC RPC to invoke.
///
/// Selected via the `rpc_kind` config field; defaults to [`RpcKind::Unary`]
/// so existing configs are backward-compatible.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RpcKind {
    /// One request, one response (default).
    #[default]
    Unary,
    /// One request, a server-driven stream of responses. The source consumes
    /// each streamed message and emits records as they arrive. Useful for
    /// event feeds, change feeds, log tails, and any long-lived gRPC stream.
    ServerStreaming,
}

/// Configuration for the gRPC source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GrpcStreamConfig {
    /// gRPC endpoint URL (e.g. `"http://localhost:50051"`).
    pub endpoint: String,
    /// Fully qualified service name (e.g. `"mypackage.MyService"`).
    pub service_name: String,
    /// Method name (e.g. `"ListUsers"`).
    pub method_name: String,
    /// Request message as JSON. Fields are mapped to protobuf fields
    /// using the `FileDescriptorSet`.
    pub request: Value,
    /// Path to the compiled `FileDescriptorSet` file.
    pub descriptor_set_path: PathBuf,
    /// Authentication: either inline (`{ type, config }`) or a `{ ref: <name> }`
    /// pointer to a shared provider in the CLI's top-level `auth:` catalog.
    pub auth: AuthSpec<GrpcAuth>,
    /// Whether to use TLS (detected from `https://` in endpoint by default).
    pub tls: Option<bool>,
    /// JSONPath to extract records from the response.
    /// If not set, the entire response is returned as a single record.
    pub records_path: Option<String>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Defaults
    /// to [`DEFAULT_BATCH_SIZE`].
    ///
    /// For unary RPCs the source has no native paging primitive to honour
    /// this hint: the default
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages) impl
    /// buffers the full response and then chunks it in memory, bounding
    /// **sink-side** memory only.
    ///
    /// For server-streaming RPCs (`rpc_kind = "server_streaming"`) the source
    /// overrides `stream_pages` and flushes a page each time `batch_size`
    /// messages accumulate, bounding both source-side and sink-side memory.
    /// `batch_size = 0` drains the entire stream into a single page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Kind of RPC to invoke. Defaults to [`RpcKind::Unary`].
    #[serde(default)]
    pub rpc_kind: RpcKind,
    /// For [`RpcKind::ServerStreaming`]: maximum number of messages to consume
    /// before terminating the stream. `None` means consume until the server
    /// closes the stream (or the run is cancelled).
    #[serde(default)]
    pub max_messages: Option<usize>,
    /// For [`RpcKind::ServerStreaming`]: if `true`, transient stream errors
    /// (server-side disconnects, transport errors, etc.) terminate the run
    /// with [`FaucetError::Source`](faucet_core::FaucetError::Source). When
    /// `false` (the default), the source reconnects with exponential backoff
    /// up to [`reconnect_max_attempts`](Self::reconnect_max_attempts).
    ///
    /// Ignored for [`RpcKind::Unary`].
    #[serde(default)]
    pub terminate_on_error: bool,
    /// For [`RpcKind::ServerStreaming`] reconnect: initial backoff delay
    /// before the first retry. Doubles after each failure up to
    /// [`reconnect_max_backoff`](Self::reconnect_max_backoff). Defaults to 1s.
    #[serde(
        default = "default_reconnect_initial_backoff",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub reconnect_initial_backoff: Duration,
    /// For [`RpcKind::ServerStreaming`] reconnect: maximum backoff cap.
    /// Defaults to 30s.
    #[serde(
        default = "default_reconnect_max_backoff",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub reconnect_max_backoff: Duration,
    /// For [`RpcKind::ServerStreaming`] reconnect: maximum reconnect attempts
    /// before surfacing the error. `None` (the default) means unlimited
    /// retries.
    ///
    /// The historical WebSocket-source spelling `max_reconnect_attempts` is
    /// accepted as an alias (#654 M20) — the prefix used to be reversed
    /// between the two connectors, so a user who learned one spelling had the
    /// other silently ignored and reconnected without a bound.
    #[serde(default, alias = "max_reconnect_attempts")]
    pub reconnect_max_attempts: Option<u32>,
    /// For [`RpcKind::ServerStreaming`] reconnect: whether the server replays
    /// the response stream from the beginning when the identical request is
    /// re-issued after a disconnect. Defaults to `true`.
    ///
    /// Because the request is resolved once per run, a reconnect sends the
    /// *same* request — a stateless server therefore re-streams from message
    /// 0. When `true` the source skips the messages it already emitted before
    /// the disconnect, so consumers see each message once. Set to `false`
    /// only for servers that resume mid-stream on the same request (rare):
    /// there, skipping would drop genuinely-new messages, so every received
    /// message is emitted (at-least-once; duplicates possible).
    #[serde(default = "default_reconnect_replay_from_start")]
    pub reconnect_replay_from_start: bool,
    /// Maximum size, in bytes, of a single inbound (decoded) gRPC message.
    /// `None` (the default) keeps tonic's built-in 4 MiB limit. Raise this
    /// for sources that legitimately return large messages; a too-low limit
    /// surfaces as a decode error and aborts the call. Applies to both unary
    /// and server-streaming RPCs.
    #[serde(default)]
    pub max_decoding_message_size: Option<usize>,
    /// Maximum size, in bytes, of a single outbound (encoded) gRPC request
    /// message. `None` (the default) keeps tonic's built-in limit. Rarely
    /// needs tuning for a data source, since requests are typically small.
    #[serde(default)]
    pub max_encoding_message_size: Option<usize>,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_reconnect_replay_from_start() -> bool {
    true
}

fn default_reconnect_initial_backoff() -> Duration {
    Duration::from_secs(1)
}

fn default_reconnect_max_backoff() -> Duration {
    Duration::from_secs(30)
}

impl GrpcStreamConfig {
    /// Create a new config with the required fields.
    pub fn new(
        endpoint: impl Into<String>,
        service_name: impl Into<String>,
        method_name: impl Into<String>,
        descriptor_set_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            service_name: service_name.into(),
            method_name: method_name.into(),
            request: Value::Object(Default::default()),
            descriptor_set_path: descriptor_set_path.into(),
            auth: AuthSpec::Inline(GrpcAuth::None),
            tls: None,
            records_path: None,
            batch_size: DEFAULT_BATCH_SIZE,
            rpc_kind: RpcKind::Unary,
            max_messages: None,
            terminate_on_error: false,
            reconnect_initial_backoff: default_reconnect_initial_backoff(),
            reconnect_max_backoff: default_reconnect_max_backoff(),
            reconnect_max_attempts: None,
            reconnect_replay_from_start: default_reconnect_replay_from_start(),
            max_decoding_message_size: None,
            max_encoding_message_size: None,
        }
    }

    /// Set the request message as JSON.
    pub fn request(mut self, request: Value) -> Self {
        self.request = request;
        self
    }

    /// Set the authentication method (inline).
    pub fn auth(mut self, auth: GrpcAuth) -> Self {
        self.auth = AuthSpec::Inline(auth);
        self
    }

    /// Set the TLS mode explicitly.
    pub fn tls(mut self, tls: bool) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Set the JSONPath for record extraction from the response.
    pub fn records_path(mut self, path: impl Into<String>) -> Self {
        self.records_path = Some(path.into());
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage). For unary RPCs this
    /// is observably identical to any positive `batch_size`, since the full
    /// response is buffered before any page is yielded. For server-streaming
    /// RPCs, `0` drains the entire stream before yielding.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the RPC kind (unary or server-streaming).
    pub fn rpc_kind(mut self, rpc_kind: RpcKind) -> Self {
        self.rpc_kind = rpc_kind;
        self
    }

    /// Cap the number of messages to consume from a server-streaming RPC.
    /// Ignored for unary RPCs.
    pub fn max_messages(mut self, max_messages: usize) -> Self {
        self.max_messages = Some(max_messages);
        self
    }

    /// Whether transient server-streaming errors should terminate the run
    /// (`true`) or trigger a reconnect with exponential backoff (`false`,
    /// the default).
    pub fn terminate_on_error(mut self, terminate_on_error: bool) -> Self {
        self.terminate_on_error = terminate_on_error;
        self
    }

    /// Set the initial reconnect backoff for server-streaming RPCs.
    pub fn reconnect_initial_backoff(mut self, backoff: Duration) -> Self {
        self.reconnect_initial_backoff = backoff;
        self
    }

    /// Set the maximum reconnect backoff for server-streaming RPCs.
    pub fn reconnect_max_backoff(mut self, backoff: Duration) -> Self {
        self.reconnect_max_backoff = backoff;
        self
    }

    /// Cap the number of reconnect attempts for server-streaming RPCs.
    pub fn reconnect_max_attempts(mut self, attempts: u32) -> Self {
        self.reconnect_max_attempts = Some(attempts);
        self
    }

    /// Set whether the server replays the stream from the start on reconnect
    /// (default `true`). See
    /// [`reconnect_replay_from_start`](Self::reconnect_replay_from_start).
    pub fn reconnect_replay_from_start(mut self, replay: bool) -> Self {
        self.reconnect_replay_from_start = replay;
        self
    }

    /// Set the maximum inbound (decoded) gRPC message size in bytes.
    pub fn max_decoding_message_size(mut self, bytes: usize) -> Self {
        self.max_decoding_message_size = Some(bytes);
        self
    }

    /// Set the maximum outbound (encoded) gRPC message size in bytes.
    pub fn max_encoding_message_size(mut self, bytes: usize) -> Self {
        self.max_encoding_message_size = Some(bytes);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        );
        assert_eq!(config.endpoint, "http://localhost:50051");
        assert_eq!(config.service_name, "users.UserService");
        assert_eq!(config.method_name, "ListUsers");
        assert!(config.records_path.is_none());
        assert!(matches!(config.auth, AuthSpec::Inline(GrpcAuth::None)));
        assert_eq!(config.rpc_kind, RpcKind::Unary);
        assert!(config.max_messages.is_none());
        assert!(!config.terminate_on_error);
        assert_eq!(config.reconnect_initial_backoff, Duration::from_secs(1));
        assert_eq!(config.reconnect_max_backoff, Duration::from_secs(30));
        assert!(config.reconnect_max_attempts.is_none());
        assert!(config.reconnect_replay_from_start);
        assert!(config.max_decoding_message_size.is_none());
        assert!(config.max_encoding_message_size.is_none());
    }

    #[test]
    fn message_size_and_replay_builders() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "svc.Svc",
            "Tail",
            "proto/descriptor.bin",
        )
        .reconnect_replay_from_start(false)
        .max_decoding_message_size(16 * 1024 * 1024)
        .max_encoding_message_size(1024);
        assert!(!config.reconnect_replay_from_start);
        assert_eq!(config.max_decoding_message_size, Some(16 * 1024 * 1024));
        assert_eq!(config.max_encoding_message_size, Some(1024));
    }

    #[test]
    fn builder_methods() {
        let config =
            GrpcStreamConfig::new("https://grpc.example.com", "svc.Svc", "Get", "desc.bin")
                .request(json!({"page_size": 100}))
                .auth(GrpcAuth::Bearer {
                    token: "tok".into(),
                })
                .tls(true)
                .records_path("$.users[*]");
        assert_eq!(config.request["page_size"], 100);
        assert!(matches!(
            config.auth,
            AuthSpec::Inline(GrpcAuth::Bearer { .. })
        ));
        assert_eq!(config.tls, Some(true));
        assert_eq!(config.records_path.unwrap(), "$.users[*]");
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        );
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        )
        .with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        )
        .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "users.UserService",
            "ListUsers",
            "proto/descriptor.bin",
        )
        .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "endpoint": "http://localhost:50051",
            "service_name": "users.UserService",
            "method_name": "ListUsers",
            "request": {},
            "descriptor_set_path": "proto/descriptor.bin",
            "auth": { "type": "none" },
            "tls": null,
            "records_path": null,
            "batch_size": 250
        }"#;
        let config: GrpcStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_from_json() {
        let json = r#"{
            "endpoint": "http://localhost:50051",
            "service_name": "users.UserService",
            "method_name": "ListUsers",
            "request": {},
            "descriptor_set_path": "proto/descriptor.bin",
            "auth": { "type": "none" },
            "tls": null,
            "records_path": null
        }"#;
        let config: GrpcStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn server_streaming_fields_deserialize_from_json() {
        let json = r#"{
            "endpoint": "http://localhost:50051",
            "service_name": "events.EventService",
            "method_name": "Tail",
            "request": {},
            "descriptor_set_path": "proto/descriptor.bin",
            "auth": { "type": "none" },
            "tls": null,
            "records_path": null,
            "rpc_kind": "server_streaming",
            "max_messages": 100,
            "terminate_on_error": true,
            "reconnect_initial_backoff": 2,
            "reconnect_max_backoff": 60,
            "reconnect_max_attempts": 5
        }"#;
        let config: GrpcStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.rpc_kind, RpcKind::ServerStreaming);
        assert_eq!(config.max_messages, Some(100));
        assert!(config.terminate_on_error);
        assert_eq!(config.reconnect_initial_backoff, Duration::from_secs(2));
        assert_eq!(config.reconnect_max_backoff, Duration::from_secs(60));
        assert_eq!(config.reconnect_max_attempts, Some(5));
    }

    #[test]
    fn server_streaming_fields_default_when_absent_from_json() {
        let json = r#"{
            "endpoint": "http://localhost:50051",
            "service_name": "users.UserService",
            "method_name": "ListUsers",
            "request": {},
            "descriptor_set_path": "proto/descriptor.bin",
            "auth": { "type": "none" },
            "tls": null,
            "records_path": null
        }"#;
        let config: GrpcStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.rpc_kind, RpcKind::Unary);
        assert!(config.max_messages.is_none());
        assert!(!config.terminate_on_error);
        assert_eq!(config.reconnect_initial_backoff, Duration::from_secs(1));
        assert_eq!(config.reconnect_max_backoff, Duration::from_secs(30));
        assert!(config.reconnect_max_attempts.is_none());
        assert!(config.reconnect_replay_from_start);
        assert!(config.max_decoding_message_size.is_none());
    }

    #[test]
    fn rpc_kind_builder() {
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "events.EventService",
            "Tail",
            "proto/descriptor.bin",
        )
        .rpc_kind(RpcKind::ServerStreaming)
        .max_messages(50)
        .terminate_on_error(true)
        .reconnect_initial_backoff(Duration::from_secs(5))
        .reconnect_max_backoff(Duration::from_secs(120))
        .reconnect_max_attempts(10);
        assert_eq!(config.rpc_kind, RpcKind::ServerStreaming);
        assert_eq!(config.max_messages, Some(50));
        assert!(config.terminate_on_error);
        assert_eq!(config.reconnect_initial_backoff, Duration::from_secs(5));
        assert_eq!(config.reconnect_max_backoff, Duration::from_secs(120));
        assert_eq!(config.reconnect_max_attempts, Some(10));
    }

    #[test]
    fn both_reconnect_attempt_spellings_deserialize() {
        // Mirror of the WebSocket source's test: either prefix order resolves
        // to the same knob (#654 M20).
        let canonical: GrpcStreamConfig = serde_json::from_value(serde_json::json!({
            "endpoint": "http://localhost:50051",
            "descriptor_set_path": "/tmp/d.bin",
            "service_name": "S", "method_name": "M", "request": {},
            "auth": { "type": "none" },
            "reconnect_max_attempts": 4
        }))
        .unwrap();
        assert_eq!(canonical.reconnect_max_attempts, Some(4));

        let historical: GrpcStreamConfig = serde_json::from_value(serde_json::json!({
            "endpoint": "http://localhost:50051",
            "descriptor_set_path": "/tmp/d.bin",
            "service_name": "S", "method_name": "M", "request": {},
            "auth": { "type": "none" },
            "max_reconnect_attempts": 4
        }))
        .unwrap();
        assert_eq!(historical.reconnect_max_attempts, Some(4));
    }
}

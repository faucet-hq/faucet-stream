//! gRPC stream executor using dynamic protobuf messages.

use crate::config::{GrpcAuth, GrpcStreamConfig, MetadataEntry, RpcKind};
use async_trait::async_trait;
use base64::Engine as _;
use faucet_core::{AuthSpec, Credential, FaucetError, SharedAuthProvider, StreamPage};
use futures_core::Stream;
use prost::Message;
use prost::bytes::Bytes;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use serde_json::Value;
use std::pin::Pin;
use std::time::Duration;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::transport::Channel;

/// A configured gRPC source that uses protobuf reflection to call
/// any gRPC method and return JSON records.
pub struct GrpcStream {
    config: GrpcStreamConfig,
    pool: DescriptorPool,
    /// Optional shared auth provider. When set, takes precedence over inline
    /// auth in `config.auth`. Injected via [`GrpcStream::with_auth_provider`].
    auth_provider: Option<SharedAuthProvider>,
}

impl GrpcStream {
    /// Create a new gRPC stream. Loads the `FileDescriptorSet` from disk.
    pub fn new(config: GrpcStreamConfig) -> Result<Self, FaucetError> {
        // A zero initial backoff keeps every `reconnect_delay` at zero (the
        // exponential and the jitter are both multiplicative), so a
        // server-streaming reconnect loop would busy-spin with no delay.
        if config.reconnect_initial_backoff.is_zero() {
            return Err(FaucetError::Config(
                "grpc reconnect_initial_backoff must be > 0 (a zero backoff busy-spins reconnects)"
                    .into(),
            ));
        }
        let descriptor_bytes = std::fs::read(&config.descriptor_set_path).map_err(|e| {
            FaucetError::Config(format!(
                "failed to read descriptor set at {}: {e}",
                config.descriptor_set_path.display()
            ))
        })?;

        let pool = DescriptorPool::decode(Bytes::from(descriptor_bytes))
            .map_err(|e| FaucetError::Config(format!("failed to parse FileDescriptorSet: {e}")))?;

        Ok(Self {
            config,
            pool,
            auth_provider: None,
        })
    }

    /// Attach a shared [`AuthProvider`](faucet_core::AuthProvider). When set,
    /// the provider supplies the credential for every request (taking
    /// precedence over inline auth), so several sources can share one token
    /// with single-flight refresh. Used by the CLI to resolve `auth: { ref }`,
    /// and by library callers who construct one provider and inject it into
    /// many sources.
    pub fn with_auth_provider(mut self, provider: SharedAuthProvider) -> Self {
        self.auth_provider = Some(provider);
        self
    }

    /// Fetch all records by calling the configured gRPC method.
    pub async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_resolved(
            &self.config.endpoint,
            &self.config.service_name,
            &self.config.method_name,
            &self.config.request,
        )
        .await
    }

    /// Internal fetch with resolved (context-substituted) parameters.
    async fn fetch_resolved(
        &self,
        endpoint: &str,
        service_name: &str,
        method_name: &str,
        request: &Value,
    ) -> Result<Vec<Value>, FaucetError> {
        let (output_desc, request_bytes) = self.prepare_call(service_name, method_name, request)?;
        let path = parse_method_path(service_name, method_name)?;

        match self.config.rpc_kind {
            RpcKind::Unary => {
                let channel = self.connect_channel(endpoint).await?;
                let mut grpc_client = self.configure_client(tonic::client::Grpc::new(channel));
                grpc_client
                    .ready()
                    .await
                    .map_err(|e| FaucetError::Config(format!("gRPC channel not ready: {e}")))?;

                let codec = DynamicCodec::new(output_desc);
                let request = self.build_grpc_request(request_bytes).await?;

                let response: tonic::Response<DynamicMessage> = grpc_client
                    .unary(request, path, codec)
                    .await
                    .map_err(|e| FaucetError::Source(format!("gRPC unary call failed: {e}")))?;

                let resp_msg = response.into_inner();
                let records =
                    serialize_and_extract(&resp_msg, self.config.records_path.as_deref())?;
                tracing::info!(records = records.len(), "gRPC unary fetch complete");
                Ok(records)
            }
            RpcKind::ServerStreaming => {
                self.fetch_server_streaming_collect(endpoint, path, output_desc, request_bytes)
                    .await
            }
        }
    }

    /// Drain a server-streaming RPC into a fully-buffered `Vec<Value>`.
    ///
    /// Reconnect-on-error and `max_messages` are honoured. The full result
    /// is collected before returning, so memory is O(stream length). For
    /// memory-bounded consumption use [`Source::stream_pages`].
    async fn fetch_server_streaming_collect(
        &self,
        endpoint: &str,
        path: tonic::codegen::http::uri::PathAndQuery,
        output_desc: MessageDescriptor,
        request_bytes: Vec<u8>,
    ) -> Result<Vec<Value>, FaucetError> {
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let mut all: Vec<Value> = Vec::new();
        let mut messages_seen: usize = 0;
        let mut attempt: u32 = 0;
        let initial_backoff = self.config.reconnect_initial_backoff;
        let max_backoff = self.config.reconnect_max_backoff;

        loop {
            match self
                .drive_server_streaming_once(
                    endpoint,
                    path.clone(),
                    output_desc.clone(),
                    request_bytes.clone(),
                    max_messages,
                    messages_seen,
                    |records| {
                        all.extend(records);
                        Ok(())
                    },
                )
                .await
            {
                Ok(consumed) => {
                    tracing::info!(
                        records = all.len(),
                        messages = messages_seen + consumed,
                        "gRPC server-streaming fetch complete"
                    );
                    return Ok(all);
                }
                Err(StreamOutcome::Done(consumed)) => {
                    messages_seen += consumed;
                    tracing::info!(
                        records = all.len(),
                        messages = messages_seen,
                        "gRPC server-streaming fetch complete (max_messages reached)"
                    );
                    return Ok(all);
                }
                Err(StreamOutcome::Transient { consumed, error }) => {
                    messages_seen += consumed;
                    if self.config.terminate_on_error {
                        return Err(error);
                    }
                    // If this attempt delivered messages before failing, the
                    // stream recovered and made progress — so this disconnect is
                    // a fresh transient failure, not a consecutive one. Reset the
                    // attempt counter and backoff; otherwise a healthy long-lived
                    // stream is aborted once its *lifetime* reconnects reach
                    // reconnect_max_attempts, and backoff stays pinned at the cap
                    // after one early hiccup (audit #146 H8).
                    if consumed > 0 {
                        attempt = 0;
                    }
                    if let Some(max_attempts) = self.config.reconnect_max_attempts
                        && attempt >= max_attempts
                    {
                        return Err(FaucetError::Source(format!(
                            "gRPC server-streaming exceeded reconnect_max_attempts={max_attempts}: {error}"
                        )));
                    }
                    let backoff = reconnect_delay(initial_backoff, max_backoff, attempt);
                    attempt += 1;
                    tracing::warn!(
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %error,
                        "gRPC server-streaming transient error, reconnecting"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Open one server-streaming attempt and drain it until the server closes,
    /// `max_messages` is hit, or a transport/status error occurs.
    ///
    /// On clean end-of-stream returns `Ok(consumed_messages)` so the caller can
    /// stop retrying. On `max_messages` returns `Err(StreamOutcome::Done(...))`
    /// (a "logical done", not a transient failure). On transient error returns
    /// `Err(StreamOutcome::Transient { ... })` and the caller may reconnect.
    #[allow(clippy::too_many_arguments)]
    async fn drive_server_streaming_once<F>(
        &self,
        endpoint: &str,
        path: tonic::codegen::http::uri::PathAndQuery,
        output_desc: MessageDescriptor,
        request_bytes: Vec<u8>,
        max_messages: usize,
        already_seen: usize,
        mut on_records: F,
    ) -> Result<usize, StreamOutcome>
    where
        F: FnMut(Vec<Value>) -> Result<(), FaucetError>,
    {
        let channel = match self.connect_channel(endpoint).await {
            Ok(c) => c,
            Err(e) => {
                return Err(StreamOutcome::Transient {
                    consumed: 0,
                    error: e,
                });
            }
        };

        let mut grpc_client = self.configure_client(tonic::client::Grpc::new(channel));
        if let Err(e) = grpc_client.ready().await {
            return Err(StreamOutcome::Transient {
                consumed: 0,
                error: FaucetError::Source(format!("gRPC channel not ready: {e}")),
            });
        }

        let codec = DynamicCodec::new(output_desc);
        let request = match self.build_grpc_request(request_bytes).await {
            Ok(r) => r,
            Err(e) => {
                // Auth/metadata errors are not transient — propagate directly.
                return Err(StreamOutcome::Transient {
                    consumed: 0,
                    error: e,
                });
            }
        };

        let response = match grpc_client.server_streaming(request, path, codec).await {
            Ok(r) => r,
            Err(status) => {
                return Err(StreamOutcome::Transient {
                    consumed: 0,
                    error: FaucetError::Source(format!(
                        "gRPC server-streaming start failed: {status}"
                    )),
                });
            }
        };

        let mut streaming = response.into_inner();
        let records_path = self.config.records_path.as_deref();
        // On a reconnect a stateless server replays the stream from message 0.
        // Skip the messages we already emitted before the disconnect so each
        // is delivered to the consumer exactly once. Disabled (skip = 0) when
        // the server is known to resume mid-stream on the same request.
        let skip = if self.config.reconnect_replay_from_start {
            already_seen
        } else {
            0
        };
        let mut position: usize = 0; // messages read from this attempt's stream
        let mut emitted: usize = 0; // messages newly emitted this attempt

        loop {
            if already_seen + emitted >= max_messages {
                return Err(StreamOutcome::Done(emitted));
            }
            match streaming.message().await {
                Ok(Some(msg)) => {
                    position += 1;
                    if position <= skip {
                        // Replayed message we already emitted — discard it.
                        continue;
                    }
                    let records = match serialize_and_extract(&msg, records_path) {
                        Ok(r) => r,
                        Err(e) => {
                            return Err(StreamOutcome::Transient {
                                consumed: emitted,
                                error: e,
                            });
                        }
                    };
                    if let Err(e) = on_records(records) {
                        return Err(StreamOutcome::Transient {
                            consumed: emitted,
                            error: e,
                        });
                    }
                    emitted += 1;
                }
                Ok(None) => {
                    return Ok(emitted);
                }
                Err(status) => {
                    return Err(StreamOutcome::Transient {
                        consumed: emitted,
                        error: FaucetError::Source(format!(
                            "gRPC server-streaming recv failed: {status}"
                        )),
                    });
                }
            }
        }
    }

    /// Look up the method's output descriptor and encode the request message.
    fn prepare_call(
        &self,
        service_name: &str,
        method_name: &str,
        request: &Value,
    ) -> Result<(MessageDescriptor, Vec<u8>), FaucetError> {
        let service = self.pool.get_service_by_name(service_name).ok_or_else(|| {
            FaucetError::Config(format!(
                "service '{service_name}' not found in descriptor set",
            ))
        })?;

        let method = service
            .methods()
            .find(|m| m.name() == method_name)
            .ok_or_else(|| {
                FaucetError::Config(format!(
                    "method '{method_name}' not found in service '{service_name}'",
                ))
            })?;

        let input_desc = method.input();
        let request_msg = DynamicMessage::deserialize(input_desc, request)
            .map_err(|e| FaucetError::Config(format!("failed to build request message: {e}")))?;
        let request_bytes = request_msg.encode_to_vec();

        Ok((method.output(), request_bytes))
    }

    /// Connect a tonic `Channel`, honouring the configured TLS mode.
    async fn connect_channel(&self, endpoint: &str) -> Result<Channel, FaucetError> {
        let use_tls = self
            .config
            .tls
            .unwrap_or_else(|| endpoint.starts_with("https"));

        let channel_endpoint = Channel::from_shared(endpoint.to_string())
            .map_err(|e| FaucetError::Url(format!("invalid gRPC endpoint: {e}")))?;

        let channel = if use_tls {
            channel_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|e| FaucetError::Config(format!("TLS config failed: {e}")))?
                .connect()
                .await
                .map_err(|e| FaucetError::Source(format!("gRPC connect failed: {e}")))?
        } else {
            channel_endpoint
                .connect()
                .await
                .map_err(|e| FaucetError::Source(format!("gRPC connect failed: {e}")))?
        };

        Ok(channel)
    }

    /// Apply the configured inbound/outbound message-size limits to a tonic
    /// client. `None` leaves tonic's built-in defaults (4 MiB decode) in place.
    fn configure_client(
        &self,
        mut client: tonic::client::Grpc<Channel>,
    ) -> tonic::client::Grpc<Channel> {
        if let Some(n) = self.config.max_decoding_message_size {
            client = client.max_decoding_message_size(n);
        }
        if let Some(n) = self.config.max_encoding_message_size {
            client = client.max_encoding_message_size(n);
        }
        client
    }

    /// Wrap raw request bytes in a `tonic::Request` and attach auth metadata.
    ///
    /// Resolves the effective auth by consulting the shared provider first
    /// (if one was injected), then falling back to the inline config. An
    /// unresolved `AuthSpec::Reference` without a provider yields
    /// [`FaucetError::Auth`].
    async fn build_grpc_request(
        &self,
        request_bytes: Vec<u8>,
    ) -> Result<tonic::Request<Vec<u8>>, FaucetError> {
        let effective = if let Some(provider) = &self.auth_provider {
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

        let mut request = tonic::Request::new(request_bytes);
        apply_grpc_auth(&effective, &mut request)?;
        Ok(request)
    }
}

#[async_trait]
impl faucet_core::Source for GrpcStream {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        if context.is_empty() {
            return GrpcStream::fetch_all(self).await;
        }

        let endpoint = faucet_core::util::substitute_context(&self.config.endpoint, context);
        let service_name =
            faucet_core::util::substitute_context(&self.config.service_name, context);
        let method_name = faucet_core::util::substitute_context(&self.config.method_name, context);

        let request = {
            let s = serde_json::to_string(&self.config.request)
                .map_err(|e| FaucetError::Config(format!("failed to serialize request: {e}")))?;
            let s = faucet_core::util::substitute_context_json(&s, context);
            serde_json::from_str(&s).map_err(|e| {
                FaucetError::Config(format!("failed to parse substituted request: {e}"))
            })?
        };

        self.fetch_resolved(&endpoint, &service_name, &method_name, &request)
            .await
    }

    /// Stream records page-by-page.
    ///
    /// For [`RpcKind::Unary`] this falls back to the default trait impl
    /// (buffer the full response, then chunk in memory) — see the README's
    /// "Streaming and batching" section for the rationale.
    ///
    /// For [`RpcKind::ServerStreaming`] this opens the gRPC stream and
    /// flushes a page each time the buffer hits `config.batch_size`
    /// (`batch_size = 0` drains the entire stream before yielding). The
    /// trait-level `batch_size` argument is ignored in favour of the config
    /// field so a pipeline-supplied hint cannot silently override an explicit
    /// config value. Transient stream errors reconnect with exponential
    /// backoff up to `reconnect_max_attempts`, unless `terminate_on_error =
    /// true` in which case the run terminates. Server-streaming pages carry
    /// `bookmark: None` — this source has no native cursor/offset, so
    /// resumption is driven by user-supplied request fields (e.g. an event
    /// id), not a faucet-managed bookmark.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        match self.config.rpc_kind {
            RpcKind::Unary => {
                // Default impl: buffer via fetch_with_context_incremental and
                // chunk in memory.
                self.default_stream_pages(context, batch_size)
            }
            RpcKind::ServerStreaming => self.server_streaming_pages(context),
        }
    }

    fn connector_name(&self) -> &'static str {
        "grpc"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(GrpcStreamConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}/{}/{}",
            faucet_core::redact_uri_credentials(&self.config.endpoint),
            self.config.service_name,
            self.config.method_name
        )
    }
}

impl GrpcStream {
    /// Replica of the default `stream_pages` impl, kept private so the
    /// override above can delegate to it for unary RPCs without exposing
    /// trait internals.
    fn default_stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        use faucet_core::Source;
        Box::pin(async_stream::try_stream! {
            let (records, bookmark) = Source::fetch_with_context_incremental(self, context).await?;
            let total = records.len();
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };

            if total == 0 {
                if bookmark.is_some() {
                    yield StreamPage { records: Vec::new(), bookmark };
                }
                return;
            }

            let mut iter = records.into_iter();
            let mut consumed = 0usize;
            loop {
                let batch: Vec<Value> = iter.by_ref().take(chunk).collect();
                if batch.is_empty() {
                    break;
                }
                consumed += batch.len();
                let page_bookmark = if consumed >= total { bookmark.clone() } else { None };
                yield StreamPage { records: batch, bookmark: page_bookmark };
            }
        })
    }

    /// Server-streaming page stream — opens the stream, buffers per
    /// `config.batch_size`, and yields a page on each fill. Reconnects on
    /// transient errors unless `terminate_on_error` is set.
    fn server_streaming_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;
        let page_chunk = if batch_size == 0 {
            usize::MAX
        } else {
            batch_size
        };
        let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
        let max_messages = self.config.max_messages.unwrap_or(usize::MAX);
        let terminate_on_error = self.config.terminate_on_error;
        let reconnect_max_attempts = self.config.reconnect_max_attempts;
        let reconnect_initial_backoff = self.config.reconnect_initial_backoff;
        let max_backoff = self.config.reconnect_max_backoff;

        Box::pin(async_stream::try_stream! {
            // Resolve context-substituted strings once per run — reconnects
            // re-use the same resolved values so a long-lived stream is
            // stable even if matrix placeholders change between runs.
            let endpoint = if context.is_empty() {
                self.config.endpoint.clone()
            } else {
                faucet_core::util::substitute_context(&self.config.endpoint, context)
            };
            let service_name = if context.is_empty() {
                self.config.service_name.clone()
            } else {
                faucet_core::util::substitute_context(&self.config.service_name, context)
            };
            let method_name = if context.is_empty() {
                self.config.method_name.clone()
            } else {
                faucet_core::util::substitute_context(&self.config.method_name, context)
            };
            let request: Value = if context.is_empty() {
                self.config.request.clone()
            } else {
                let s = serde_json::to_string(&self.config.request)
                    .map_err(|e| FaucetError::Config(format!("failed to serialize request: {e}")))?;
                let s = faucet_core::util::substitute_context_json(&s, context);
                serde_json::from_str(&s).map_err(|e| FaucetError::Config(format!(
                    "failed to parse substituted request: {e}"
                )))?
            };

            let (output_desc, request_bytes) =
                self.prepare_call(&service_name, &method_name, &request)?;
            let path = parse_method_path(&service_name, &method_name)?;

            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut messages_seen: usize = 0;
            let mut attempt: u32 = 0;

            'reconnect: loop {
                // Open one streaming attempt and drain it. The closure
                // appends extracted records into `buffer` so we can keep
                // emitting pages across reconnects.
                let outcome = self.drive_server_streaming_once(
                    &endpoint,
                    path.clone(),
                    output_desc.clone(),
                    request_bytes.clone(),
                    max_messages,
                    messages_seen,
                    |records| {
                        buffer.extend(records);
                        Ok(())
                    },
                ).await;

                // Flush any complete pages accumulated in the buffer, even
                // when the attempt ended in a transient error — records
                // received before the disconnect are still valid.
                while buffer.len() >= page_chunk {
                    let drained: Vec<Value> = buffer.drain(..page_chunk).collect();
                    yield StreamPage { records: drained, bookmark: None };
                }

                match outcome {
                    Ok(consumed) => {
                        messages_seen += consumed;
                        // Clean end-of-stream — flush the trailing partial
                        // buffer and stop.
                        if !buffer.is_empty() {
                            let final_records = std::mem::take(&mut buffer);
                            yield StreamPage { records: final_records, bookmark: None };
                        }
                        tracing::info!(
                            messages = messages_seen,
                            "gRPC server-streaming complete"
                        );
                        break 'reconnect;
                    }
                    Err(StreamOutcome::Done(consumed)) => {
                        messages_seen += consumed;
                        if !buffer.is_empty() {
                            let final_records = std::mem::take(&mut buffer);
                            yield StreamPage { records: final_records, bookmark: None };
                        }
                        tracing::info!(
                            messages = messages_seen,
                            "gRPC server-streaming complete (max_messages reached)"
                        );
                        break 'reconnect;
                    }
                    Err(StreamOutcome::Transient { consumed, error }) => {
                        messages_seen += consumed;
                        if terminate_on_error {
                            // `?` yields the error and ends the stream; the
                            // trailing `return` both makes that explicit and
                            // lets the borrow checker see this branch diverges
                            // (so `error` is still available below). Replaces a
                            // fragile `unreachable!()` (#78 LOW).
                            Err(error)?;
                            return;
                        }
                        // Progress before the disconnect → the stream recovered,
                        // so reset the attempt counter and backoff (count only
                        // *consecutive* failures, don't pin backoff at the cap) —
                        // audit #146 H8.
                        if consumed > 0 {
                            attempt = 0;
                        }
                        if let Some(max_attempts) = reconnect_max_attempts
                            && attempt >= max_attempts
                        {
                            let final_err = FaucetError::Source(format!(
                                "gRPC server-streaming exceeded reconnect_max_attempts={max_attempts}: {error}"
                            ));
                            Err(final_err)?;
                            return;
                        }
                        let backoff =
                            reconnect_delay(reconnect_initial_backoff, max_backoff, attempt);
                        attempt += 1;
                        tracing::warn!(
                            attempt,
                            backoff_ms = backoff.as_millis() as u64,
                            error = %error,
                            "gRPC server-streaming transient error, reconnecting"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        })
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

/// Map a [`Credential`] from a shared provider onto the gRPC [`GrpcAuth`]
/// representation so the existing metadata-application path can be reused.
///
/// This mapping is infallible: gRPC's `Metadata` variant can carry any
/// header-style credential, so every [`Credential`] variant has a clear home.
fn credential_to_auth(cred: Credential) -> GrpcAuth {
    match cred {
        Credential::Bearer(token) => GrpcAuth::Bearer { token },
        Credential::Token(token) => GrpcAuth::Metadata {
            entries: vec![MetadataEntry {
                key: "authorization".into(),
                value: token,
            }],
        },
        Credential::Header { name, value } => GrpcAuth::Metadata {
            entries: vec![MetadataEntry { key: name, value }],
        },
        Credential::Basic { username, password } => GrpcAuth::Metadata {
            entries: vec![MetadataEntry {
                key: "authorization".into(),
                value: format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD
                        .encode(format!("{username}:{password}"))
                ),
            }],
        },
    }
}

/// Apply a resolved [`GrpcAuth`] to a tonic request's metadata.
fn apply_grpc_auth(
    auth: &GrpcAuth,
    request: &mut tonic::Request<Vec<u8>>,
) -> Result<(), FaucetError> {
    match auth {
        GrpcAuth::None => {}
        GrpcAuth::Bearer { token } => {
            let val: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
                format!("Bearer {token}")
                    .parse()
                    .map_err(|e| FaucetError::Auth(format!("invalid bearer token: {e}")))?;
            request.metadata_mut().insert("authorization", val);
        }
        GrpcAuth::Metadata { entries } => {
            for entry in entries {
                let val: tonic::metadata::MetadataValue<tonic::metadata::Ascii> = entry
                    .value
                    .parse()
                    .map_err(|e| FaucetError::Auth(format!("invalid metadata value: {e}")))?;
                let key: tonic::metadata::MetadataKey<tonic::metadata::Ascii> =
                    entry
                        .key
                        .parse()
                        .map_err(|e| FaucetError::Auth(format!("invalid metadata key: {e}")))?;
                request.metadata_mut().insert(key, val);
            }
        }
    }
    Ok(())
}

fn parse_method_path(
    service_name: &str,
    method_name: &str,
) -> Result<tonic::codegen::http::uri::PathAndQuery, FaucetError> {
    let full_method = format!("/{service_name}/{method_name}");
    tonic::codegen::http::uri::PathAndQuery::from_maybe_shared(full_method)
        .map_err(|e| FaucetError::Url(format!("invalid method path: {e}")))
}

fn serialize_and_extract(
    msg: &DynamicMessage,
    records_path: Option<&str>,
) -> Result<Vec<Value>, FaucetError> {
    let serialize_opts = SerializeOptions::new().stringify_64_bit_integers(false);
    let json_value = msg
        .serialize_with_options(serde_json::value::Serializer, &serialize_opts)
        .map_err(|e| {
            FaucetError::Transform(format!("failed to serialize gRPC response to JSON: {e}"))
        })?;
    faucet_core::util::extract_records(&json_value, records_path)
}

/// Reconnect delay after `attempt` consecutive transient failures: the
/// configured `reconnect_initial_backoff` doubled toward the configured
/// `reconnect_max_backoff`, then jittered through core's shared helper.
///
/// `attempt` is the count of *consecutive* prior failures (reset whenever a
/// stream made progress), so the exponential is derived rather than carried in a
/// mutable variable — one less piece of state to keep in step with the counter.
/// The jitter matters because every replica of a pipeline pointed at one gRPC
/// endpoint reconnects off the same server-side fault: without it they all redial
/// in the same instant and keep the endpoint down (#654 M13). Pure.
fn reconnect_delay(base: Duration, cap: Duration, attempt: u32) -> Duration {
    let exp = base.saturating_mul(2u32.saturating_pow(attempt)).min(cap);
    faucet_core::retry::apply_jitter(exp)
}

/// Outcome of one server-streaming attempt. `Done` is a logical stop signal
/// (e.g. `max_messages` hit) that should not trigger a reconnect; `Transient`
/// carries a real error that may be retried subject to the reconnect config.
enum StreamOutcome {
    Done(usize),
    Transient { consumed: usize, error: FaucetError },
}

// ── Dynamic Codec ───────────────────────────────────────────────────────────

/// A tonic codec that encodes raw bytes and decodes into `DynamicMessage`.
struct DynamicCodec {
    output_desc: prost_reflect::MessageDescriptor,
}

impl DynamicCodec {
    fn new(output_desc: prost_reflect::MessageDescriptor) -> Self {
        Self { output_desc }
    }
}

impl Codec for DynamicCodec {
    type Encode = Vec<u8>;
    type Decode = DynamicMessage;
    type Encoder = RawEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            desc: self.output_desc.clone(),
        }
    }
}

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        use prost::bytes::BufMut;
        buf.put_slice(&item);
        Ok(())
    }
}

struct DynamicDecoder {
    desc: prost_reflect::MessageDescriptor,
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = tonic::Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        use prost::bytes::Buf;
        if !buf.has_remaining() {
            return Ok(None);
        }
        let bytes = buf.copy_to_bytes(buf.remaining());
        let msg = DynamicMessage::decode(self.desc.clone(), bytes)
            .map_err(|e| tonic::Status::internal(format!("protobuf decode error: {e}")))?;
        Ok(Some(msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError};
    use std::sync::Arc;

    // ── Backoff ───────────────────────────────────────────────────────────────

    /// Assert a jittered delay sits in core's documented `[0.5, 1.5)` band
    /// around `expected` and never collapses to zero (a zero delay busy-spins
    /// the reconnect loop — the hazard `reconnect_initial_backoff` validation
    /// already guards at config load).
    fn assert_jittered_around(actual: Duration, expected: Duration) {
        let lo = expected.mul_f64(0.5);
        let hi = expected.mul_f64(1.5);
        assert!(
            actual >= lo && actual < hi,
            "delay {actual:?} outside jitter band [{lo:?}, {hi:?}) for {expected:?}"
        );
        assert!(!actual.is_zero(), "jittered delay collapsed to zero");
    }

    #[test]
    fn reconnect_delay_doubles_up_to_the_configured_cap() {
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(30);
        assert_jittered_around(reconnect_delay(base, cap, 0), Duration::from_secs(1));
        assert_jittered_around(reconnect_delay(base, cap, 1), Duration::from_secs(2));
        assert_jittered_around(reconnect_delay(base, cap, 2), Duration::from_secs(4));
        assert_jittered_around(reconnect_delay(base, cap, 3), Duration::from_secs(8));
        assert_jittered_around(reconnect_delay(base, cap, 4), Duration::from_secs(16));
        // The *configured* cap wins — not core's own 60s ceiling — and holds
        // even once `2^attempt` saturates.
        assert_jittered_around(reconnect_delay(base, cap, 5), cap);
        assert_jittered_around(reconnect_delay(base, cap, 40), cap);
        assert_jittered_around(reconnect_delay(base, cap, u32::MAX), cap);
    }

    #[test]
    fn reconnect_delay_is_decorrelated_across_calls() {
        // Replicas reconnecting off one server-side fault must not redial in
        // lockstep (#654 M13): identical inputs must not give identical delays.
        let base = Duration::from_secs(1);
        let cap = Duration::from_secs(30);
        let delays: Vec<Duration> = (0..32).map(|_| reconnect_delay(base, cap, 2)).collect();
        assert!(
            delays.iter().any(|d| *d != delays[0]),
            "every delay identical — jitter is not being applied: {delays:?}"
        );
    }

    // ── credential_to_auth mapping ────────────────────────────────────────────

    #[test]
    fn credential_bearer_maps_to_grpc_bearer() {
        let auth = credential_to_auth(Credential::Bearer("tok".into()));
        assert!(matches!(auth, GrpcAuth::Bearer { token } if token == "tok"));
    }

    #[test]
    fn credential_token_maps_to_metadata_authorization() {
        let auth = credential_to_auth(Credential::Token("Custom xyz".into()));
        match auth {
            GrpcAuth::Metadata { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].key, "authorization");
                assert_eq!(entries[0].value, "Custom xyz");
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[test]
    fn credential_header_maps_to_metadata_with_given_name() {
        let auth = credential_to_auth(Credential::Header {
            name: "x-api-key".into(),
            value: "secret".into(),
        });
        match auth {
            GrpcAuth::Metadata { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].key, "x-api-key");
                assert_eq!(entries[0].value, "secret");
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    #[test]
    fn credential_basic_maps_to_base64_authorization_metadata() {
        let auth = credential_to_auth(Credential::Basic {
            username: "alice".into(),
            password: "p@ss".into(),
        });
        match auth {
            GrpcAuth::Metadata { entries } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].key, "authorization");
                let expected = format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode("alice:p@ss")
                );
                assert_eq!(entries[0].value, expected);
            }
            other => panic!("expected Metadata, got {other:?}"),
        }
    }

    // ── Helpers for tests that need a live GrpcStream ─────────────────────────

    /// Build a minimal valid `GrpcStream` from an in-memory `FileDescriptorSet`
    /// so we can test `build_grpc_request` without a real proto file on disk.
    fn make_dummy_stream() -> GrpcStream {
        use prost::Message;
        let fds_set = prost_types::FileDescriptorSet {
            file: vec![prost_types::FileDescriptorProto {
                name: Some("dummy.proto".into()),
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        };
        let bytes = fds_set.encode_to_vec();
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(tmp.path(), &bytes).expect("write descriptor");
        let config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "dummy.Svc",
            "Call",
            tmp.path().to_str().unwrap(),
        );
        // `tmp` is dropped here but the bytes were already decoded by `new()`.
        GrpcStream::new(config).expect("new from in-memory descriptor")
    }

    #[test]
    fn rejects_zero_reconnect_initial_backoff() {
        use prost::Message;
        // A valid descriptor on disk, so the only thing that can fail `new` is
        // the backoff validation (not a missing-file read).
        let fds_set = prost_types::FileDescriptorSet {
            file: vec![prost_types::FileDescriptorProto {
                name: Some("dummy.proto".into()),
                syntax: Some("proto3".into()),
                ..Default::default()
            }],
        };
        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        std::fs::write(tmp.path(), fds_set.encode_to_vec()).expect("write descriptor");
        let mut config = GrpcStreamConfig::new(
            "http://localhost:50051",
            "dummy.Svc",
            "Call",
            tmp.path().to_str().unwrap(),
        );
        config.reconnect_initial_backoff = std::time::Duration::ZERO;
        let Err(err) = GrpcStream::new(config) else {
            panic!("a zero reconnect_initial_backoff must be rejected (it busy-spins)");
        };
        assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
        assert!(
            err.to_string().contains("reconnect_initial_backoff"),
            "{err}"
        );
    }

    // ── Unresolved Reference error path ───────────────────────────────────────

    #[tokio::test]
    async fn unresolved_auth_reference_errors_at_request_time() {
        let mut stream = make_dummy_stream();
        stream.config.auth = AuthSpec::Reference(AuthReference {
            name: "missing-provider".into(),
        });
        let err = stream.build_grpc_request(vec![]).await.unwrap_err();
        assert!(
            matches!(err, FaucetError::Auth(_)),
            "expected Auth error, got {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains("missing-provider"),
            "error message should name the provider: {msg}"
        );
    }

    // ── Injected provider takes precedence ────────────────────────────────────

    #[derive(Debug)]
    struct FixedBearer(&'static str);

    #[async_trait::async_trait]
    impl AuthProvider for FixedBearer {
        async fn credential(&self) -> Result<Credential, FaucetError> {
            Ok(Credential::Bearer(self.0.to_string()))
        }
        fn provider_name(&self) -> &'static str {
            "fixed-bearer"
        }
    }

    #[tokio::test]
    async fn injected_provider_overrides_inline_none() {
        let provider: SharedAuthProvider = Arc::new(FixedBearer("MYTOKEN"));
        let stream = make_dummy_stream().with_auth_provider(provider);
        // config.auth is Inline(None) but the provider should inject Bearer.
        let req = stream
            .build_grpc_request(vec![])
            .await
            .expect("build request");
        let auth_header = req
            .metadata()
            .get("authorization")
            .expect("authorization metadata must be present")
            .to_str()
            .expect("ascii");
        assert_eq!(auth_header, "Bearer MYTOKEN");
    }

    #[test]
    fn dataset_uri_combines_endpoint_service_method() {
        use faucet_core::Source;
        let stream = make_dummy_stream();
        assert_eq!(
            stream.dataset_uri(),
            "http://localhost:50051/dummy.Svc/Call"
        );
    }
}

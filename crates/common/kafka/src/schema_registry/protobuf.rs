//! Protobuf encode/decode wrapped in the Confluent wire envelope.
//!
//! Confluent's Protobuf format is `envelope + [message_indexes][protobuf bytes]`.
//! `message_indexes` is a varint-encoded array used when a `.proto` file contains
//! multiple message types. For v1 we restrict to single-message schemas where
//! it is `[0]`, encoded as a single byte `0x00`.
//!
//! Schema compilation uses `protox` — a pure-Rust `.proto` compiler — so no
//! `protoc` binary is needed at runtime. The compiled `FileDescriptorSet` bytes
//! are fed into `prost_reflect::DescriptorPool` via its raw-bytes `decode()`
//! path, which bridges the minor prost version difference between `protox` (prost
//! 0.14) and the workspace (prost 0.13). The protobuf wire format is binary-
//! compatible so the roundtrip is safe.

use crate::schema_registry::{client::SchemaRegistryClient, envelope};
use faucet_core::FaucetError;
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::Value;
use std::sync::Arc;

fn compiled(
    client: &SchemaRegistryClient,
    proto_source: &str,
) -> Result<Arc<(DescriptorPool, String)>, FaucetError> {
    client.parsed("protobuf", proto_source, compile_proto)
}

/// JSON for a decoded message: every field present (proto3 defaults such as
/// `0`, `false` and `""` included) under its `.proto` name (#789 MSG-28).
fn message_to_json(message: &DynamicMessage) -> Result<Value, FaucetError> {
    let options = SerializeOptions::new()
        .skip_default_fields(false)
        .use_proto_field_name(true);
    message
        .serialize_with_options(serde_json::value::Serializer, &options)
        .map_err(FaucetError::Json)
}

/// Decode `bytes` (with Confluent envelope + message-index prefix) into JSON.
pub async fn decode(client: &SchemaRegistryClient, bytes: &[u8]) -> Result<Value, FaucetError> {
    let (schema_id, body) = envelope::decode(bytes)?;
    let registered = client.get_schema(schema_id).await?;
    let payload = parse_message_indexes(body)?;
    let compiled = compiled(client, &registered.schema)?;
    let (pool, message_name) = &*compiled;
    let message_descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
        FaucetError::Source(format!(
            "protobuf: message '{message_name}' not found in compiled schema"
        ))
    })?;
    let dynamic = DynamicMessage::decode(message_descriptor, payload)
        .map_err(|e| FaucetError::Source(format!("protobuf decode: {e}")))?;
    message_to_json(&dynamic)
}

/// Encode `value` as the first message in `proto_source` under `subject`.
/// Returns wire-envelope bytes ready for the Kafka wire format.
pub async fn encode(
    client: &SchemaRegistryClient,
    subject: &str,
    proto_source: &str,
    value: &Value,
) -> Result<Vec<u8>, FaucetError> {
    let compiled = compiled(client, proto_source)?;
    let (pool, message_name) = &*compiled;
    let message_descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
        FaucetError::Config(format!(
            "protobuf: message '{message_name}' not found in schema"
        ))
    })?;
    let id = client
        .register_schema(subject, "PROTOBUF", proto_source)
        .await?;
    let dynamic = DynamicMessage::deserialize(message_descriptor, value.clone())
        .map_err(|e| FaucetError::Sink(format!("json→protobuf: {e}")))?;
    // message_indexes prefix: a single 0x00 byte means "first message in file".
    let mut payload = vec![0u8];
    dynamic
        .encode(&mut payload)
        .map_err(|e| FaucetError::Sink(format!("protobuf encode: {e}")))?;
    Ok(envelope::encode(id, &payload))
}

/// Compile a `.proto` source string via `protox` (pure Rust, no protoc needed)
/// and return a `DescriptorPool` plus the fully-qualified name of the first
/// message defined in the file.
///
/// v1 restriction: only single-message schemas (one top-level `message` block)
/// are supported. The auto-selected message is the first one encountered.
/// Multi-message support is a future enhancement.
fn compile_proto(proto_source: &str) -> Result<(DescriptorPool, String), FaucetError> {
    use protox::{
        Compiler,
        file::{ChainFileResolver, File, FileResolver, GoogleFileResolver},
    };

    // Implement an in-memory resolver that serves our proto_source as "schema.proto".
    struct MemResolver {
        source: String,
    }
    impl FileResolver for MemResolver {
        fn open_file(&self, name: &str) -> Result<File, protox::Error> {
            if name == "schema.proto" {
                File::from_source("schema.proto", &self.source)
                    .map_err(|e| protox::Error::new(format!("parse error: {e}")))
            } else {
                Err(protox::Error::file_not_found(name))
            }
        }
    }

    // Build a chain: our in-memory resolver first, then the built-in google/* resolver.
    let mut chain = ChainFileResolver::new();
    chain.add(MemResolver {
        source: proto_source.to_owned(),
    });
    chain.add(GoogleFileResolver::new());

    let mut compiler = Compiler::with_file_resolver(chain);
    compiler
        .open_file("schema.proto")
        .map_err(|e| FaucetError::Config(format!("protox compile: {e}")))?;

    // `encode_file_descriptor_set()` returns raw protobuf bytes from protox's
    // internal prost version. `DescriptorPool::decode()` accepts raw bytes, so
    // this bridges the workspace prost 0.13 ↔ protox prost 0.14 gap cleanly.
    let fds_bytes = compiler.encode_file_descriptor_set();
    let pool = DescriptorPool::decode(fds_bytes.as_slice())
        .map_err(|e| FaucetError::Config(format!("descriptor pool build: {e}")))?;

    // v1: take the first message in the file. The file is always "schema.proto".
    let first_file = pool.get_file_by_name("schema.proto").ok_or_else(|| {
        FaucetError::Config("protobuf schema produced no file descriptor for schema.proto".into())
    })?;
    let first_message = first_file
        .messages()
        .next()
        .ok_or_else(|| FaucetError::Config("protobuf schema defines no messages".into()))?;
    Ok((pool, first_message.full_name().to_owned()))
}

/// Strip the message_indexes prefix.
///
/// v1 only supports the single-message case (message_indexes = `[0]`, encoded
/// as a single `0x00` byte). Multi-message schemas are rejected with a clear
/// error pointing to the follow-up issue.
fn parse_message_indexes(body: &[u8]) -> Result<&[u8], FaucetError> {
    if body.is_empty() {
        return Err(FaucetError::Source(
            "protobuf payload is empty (missing message_indexes)".into(),
        ));
    }
    if body[0] != 0 {
        return Err(FaucetError::Source(
            "ConfluentProtobuf v1 only supports single-message schemas \
             (message_indexes = [0]). Multi-message support is a follow-up."
                .into(),
        ));
    }
    Ok(&body[1..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SchemaRegistryConfig;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const USER_PROTO: &str =
        "syntax = \"proto3\";\npackage example;\nmessage User { int64 id = 1; string name = 2; }\n";

    #[tokio::test]
    async fn protobuf_round_trip_through_mock_registry() {
        let server = MockServer::start().await;

        // Mock the schema registration endpoint (POST /subjects/{subject}/versions)
        Mock::given(method("POST"))
            .and(path("/subjects/users-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 11})))
            .mount(&server)
            .await;

        // Mock the schema lookup endpoint (GET /schemas/ids/{id})
        Mock::given(method("GET"))
            .and(path("/schemas/ids/11"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": USER_PROTO,
                "schemaType": "PROTOBUF",
            })))
            .mount(&server)
            .await;

        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let record = serde_json::json!({"id": 42, "name": "alice"});

        let bytes = encode(&client, "users-value", USER_PROTO, &record)
            .await
            .unwrap();

        let decoded = decode(&client, &bytes).await.unwrap();

        // proto3 int64 round-trips through JSON as a string per the proto3 JSON mapping spec.
        assert_eq!(decoded["id"], serde_json::json!("42"));
        assert_eq!(decoded["name"], serde_json::json!("alice"));
    }

    #[tokio::test]
    async fn defaults_survive_and_names_stay_snake_case() {
        let proto = "syntax = \"proto3\";\npackage p;\nmessage Order { int32 order_id = 1; bool paid = 2; string note = 3; }\n";
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/subjects/o-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 5})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/schemas/ids/5"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "schema": proto,
                "schemaType": "PROTOBUF",
            })))
            .mount(&server)
            .await;
        let client = SchemaRegistryClient::new(&SchemaRegistryConfig::new(server.uri())).unwrap();
        let bytes = encode(
            &client,
            "o-value",
            proto,
            &serde_json::json!({"order_id": 0, "paid": false, "note": ""}),
        )
        .await
        .unwrap();
        assert_eq!(
            decode(&client, &bytes).await.unwrap(),
            serde_json::json!({"order_id": 0, "paid": false, "note": ""})
        );
        let a = compiled(&client, proto).unwrap();
        let b = compiled(&client, proto).unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "the compiled descriptor pool is cached"
        );
    }

    #[test]
    fn compile_proto_extracts_first_message_name() {
        let proto = "syntax = \"proto3\";\npackage acme;\nmessage Widget { string color = 1; }\n";
        let (_, name) = compile_proto(proto).unwrap();
        assert_eq!(name, "acme.Widget");
    }

    #[test]
    fn compile_proto_rejects_syntax_errors() {
        let bad_proto = "this is not valid proto";
        assert!(compile_proto(bad_proto).is_err());
    }

    #[test]
    fn parse_message_indexes_rejects_empty_payload() {
        assert!(parse_message_indexes(&[]).is_err());
    }

    #[test]
    fn parse_message_indexes_rejects_multi_message_marker() {
        // A nonzero first byte indicates message_indexes is something other than [0].
        assert!(parse_message_indexes(&[0x01]).is_err());
    }

    #[test]
    fn parse_message_indexes_strips_single_zero() {
        let body = &[0x00, 0xDE, 0xAD, 0xBE, 0xEF];
        let stripped = parse_message_indexes(body).unwrap();
        assert_eq!(stripped, &[0xDE, 0xAD, 0xBE, 0xEF]);
    }
}

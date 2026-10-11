//! Encode Kafka message bytes from a serde_json::Value, dispatching on
//! KafkaValueFormat.

use base64::Engine;
use faucet_common_kafka::KafkaValueFormat;
use faucet_core::FaucetError;
use serde_json::Value;

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::{
    avro, client::SchemaRegistryClient, json_schema, protobuf,
};

#[derive(Default, Clone)]
pub struct SchemaContext {
    /// Subject name used for ConfluentAvro/Protobuf/JsonSchema. Usually `{topic}-value`.
    pub subject: String,
    /// Schema text to register (Avro JSON, JSON Schema JSON, Protobuf source).
    /// Required for the Confluent variants on encode.
    pub schema_text: Option<String>,
}

pub async fn encode(
    value: &Value,
    format: &KafkaValueFormat,
    #[cfg(feature = "schema-registry")] sr_client: Option<&SchemaRegistryClient>,
    #[cfg(feature = "schema-registry")] schema_ctx: &SchemaContext,
) -> Result<Vec<u8>, FaucetError> {
    match format {
        KafkaValueFormat::Json => serde_json::to_vec(value)
            .map_err(|e| FaucetError::Sink(format!("kafka json encode: {e}"))),
        KafkaValueFormat::RawString => match value {
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            other => Ok(other.to_string().into_bytes()),
        },
        KafkaValueFormat::Bytes => match value {
            Value::String(s) => base64::engine::general_purpose::STANDARD
                .decode(s)
                .map_err(|e| FaucetError::Sink(format!("kafka bytes base64 decode: {e}"))),
            _ => Err(FaucetError::Sink(
                "kafka Bytes format requires the record value to be a base64-encoded string".into(),
            )),
        },
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentAvro { bytes_encoding, .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "value format 'confluent_avro' requires a Schema Registry client — set \
                     `schema_registry.url` (and any auth) in the Kafka sink config"
                        .into(),
                )
            })?;
            let schema_text = schema_ctx
                .schema_text
                .as_deref()
                .ok_or_else(|| FaucetError::Config("ConfluentAvro requires schema_text".into()))?;
            avro::encode_with(
                client,
                &schema_ctx.subject,
                schema_text,
                value,
                *bytes_encoding,
            )
            .await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentProtobuf { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "value format 'confluent_protobuf' requires a Schema Registry client — set \
                     `schema_registry.url` (and any auth) in the Kafka sink config"
                        .into(),
                )
            })?;
            let schema_text = schema_ctx.schema_text.as_deref().ok_or_else(|| {
                FaucetError::Config("ConfluentProtobuf requires schema_text".into())
            })?;
            protobuf::encode(client, &schema_ctx.subject, schema_text, value).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentJsonSchema { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "value format 'confluent_json_schema' requires a Schema Registry client — set \
                     `schema_registry.url` (and any auth) in the Kafka sink config"
                        .into(),
                )
            })?;
            let schema_text = schema_ctx.schema_text.as_deref().ok_or_else(|| {
                FaucetError::Config("ConfluentJsonSchema requires schema_text".into())
            })?;
            json_schema::encode(client, &schema_ctx.subject, schema_text, value).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn encode_json_object() {
        let bytes = encode(
            &json!({"a": 1}),
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, br#"{"a":1}"#);
    }

    #[tokio::test]
    async fn encode_raw_string_passes_through() {
        let bytes = encode(
            &Value::String("hello".into()),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, b"hello");
    }

    #[tokio::test]
    async fn encode_raw_string_stringifies_non_string() {
        let bytes = encode(
            &json!(42),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, b"42");
    }

    #[tokio::test]
    async fn encode_bytes_decodes_base64() {
        let bytes = encode(
            &Value::String("3q2+7w==".into()),
            &KafkaValueFormat::Bytes,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap();
        assert_eq!(bytes, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[tokio::test]
    async fn encode_bytes_errors_on_non_string() {
        let err = encode(
            &json!({"x": 1}),
            &KafkaValueFormat::Bytes,
            #[cfg(feature = "schema-registry")]
            None,
            #[cfg(feature = "schema-registry")]
            &SchemaContext::default(),
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("base64"));
    }

    #[cfg(feature = "schema-registry")]
    mod schema_registry {
        use super::*;
        use faucet_common_kafka::SchemaRegistryConfig;
        use faucet_common_kafka::schema_registry::client::SchemaRegistryClient;

        fn avro_format() -> KafkaValueFormat {
            KafkaValueFormat::ConfluentAvro {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
                bytes_encoding: Default::default(),
            }
        }

        fn protobuf_format() -> KafkaValueFormat {
            KafkaValueFormat::ConfluentProtobuf {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            }
        }

        fn json_schema_format() -> KafkaValueFormat {
            KafkaValueFormat::ConfluentJsonSchema {
                schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
                validate: false,
            }
        }

        /// A `SchemaRegistryClient::new` only builds the HTTP client and
        /// validates the URL — no network I/O — so it is safe to construct
        /// offline for the "client present but schema_text missing" branch.
        fn offline_client() -> SchemaRegistryClient {
            SchemaRegistryClient::new(&SchemaRegistryConfig::new("http://localhost:8081"))
                .expect("offline client builds")
        }

        #[tokio::test]
        async fn encode_confluent_avro_without_client_is_config_error() {
            let err = encode(
                &json!({"a": 1}),
                &avro_format(),
                None,
                &SchemaContext::default(),
            )
            .await
            .unwrap_err();
            match err {
                // The message now names the wire format and the fix (#431).
                FaucetError::Config(msg) => assert!(
                    msg.contains("confluent_avro") && msg.contains("schema_registry.url"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Config error, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn encode_confluent_protobuf_without_client_is_config_error() {
            let err = encode(
                &json!({"a": 1}),
                &protobuf_format(),
                None,
                &SchemaContext::default(),
            )
            .await
            .unwrap_err();
            match err {
                FaucetError::Config(msg) => assert!(
                    msg.contains("confluent_protobuf") && msg.contains("schema_registry.url"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Config error, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn encode_confluent_json_schema_without_client_is_config_error() {
            let err = encode(
                &json!({"a": 1}),
                &json_schema_format(),
                None,
                &SchemaContext::default(),
            )
            .await
            .unwrap_err();
            match err {
                FaucetError::Config(msg) => assert!(
                    msg.contains("confluent_json_schema") && msg.contains("schema_registry.url"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Config error, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn encode_confluent_avro_without_schema_text_is_config_error() {
            let client = offline_client();
            // schema_text defaults to None — the schema_text guard fires before
            // any registry network call.
            let err = encode(
                &json!({"a": 1}),
                &avro_format(),
                Some(&client),
                &SchemaContext::default(),
            )
            .await
            .unwrap_err();
            match err {
                FaucetError::Config(msg) => assert!(
                    msg.contains("ConfluentAvro") && msg.contains("schema_text"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Config error, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn encode_confluent_protobuf_without_schema_text_is_config_error() {
            let client = offline_client();
            let err = encode(
                &json!({"a": 1}),
                &protobuf_format(),
                Some(&client),
                &SchemaContext::default(),
            )
            .await
            .unwrap_err();
            match err {
                FaucetError::Config(msg) => assert!(
                    msg.contains("ConfluentProtobuf") && msg.contains("schema_text"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Config error, got {other:?}"),
            }
        }

        #[tokio::test]
        async fn encode_confluent_json_schema_without_schema_text_is_config_error() {
            let client = offline_client();
            let err = encode(
                &json!({"a": 1}),
                &json_schema_format(),
                Some(&client),
                &SchemaContext::default(),
            )
            .await
            .unwrap_err();
            match err {
                FaucetError::Config(msg) => assert!(
                    msg.contains("ConfluentJsonSchema") && msg.contains("schema_text"),
                    "unexpected message: {msg}"
                ),
                other => panic!("expected Config error, got {other:?}"),
            }
        }
    }
}

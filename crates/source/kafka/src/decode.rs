//! Decode Kafka message bytes into JSON `Value`s, dispatching on
//! `KafkaValueFormat`.

use base64::Engine;
use faucet_common_kafka::KafkaValueFormat;
use faucet_core::FaucetError;
use serde_json::Value;

#[cfg(feature = "schema-registry")]
use faucet_common_kafka::schema_registry::{
    avro, client::SchemaRegistryClient, json_schema, protobuf,
};

/// Decode `bytes` per `format`. For Confluent SR formats, `sr_client` must be `Some`.
pub async fn decode(
    bytes: Option<&[u8]>,
    format: &KafkaValueFormat,
    #[cfg(feature = "schema-registry")] sr_client: Option<&SchemaRegistryClient>,
) -> Result<Value, FaucetError> {
    let Some(bytes) = bytes else {
        return Ok(Value::Null);
    };
    match format {
        KafkaValueFormat::Json => faucet_core::fidelity::from_slice(bytes)
            .map_err(|e| FaucetError::Source(format!("kafka json decode: {e}"))),
        KafkaValueFormat::RawString => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| FaucetError::Source(format!("kafka raw_string decode: {e}")))?;
            Ok(Value::String(s.to_string()))
        }
        KafkaValueFormat::Bytes => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
            Ok(Value::String(encoded))
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentAvro { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentAvro selected but no SchemaRegistryClient available".into(),
                )
            })?;
            avro::decode(client, bytes).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentProtobuf { .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentProtobuf selected but no SchemaRegistryClient available".into(),
                )
            })?;
            protobuf::decode(client, bytes).await
        }
        #[cfg(feature = "schema-registry")]
        KafkaValueFormat::ConfluentJsonSchema { validate, .. } => {
            let client = sr_client.ok_or_else(|| {
                FaucetError::Config(
                    "ConfluentJsonSchema selected but no SchemaRegistryClient available".into(),
                )
            })?;
            json_schema::decode(client, bytes, *validate).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// The value-fidelity corpus (#885): every record, serialized the way a
    /// producer sends it, decodes back unchanged.
    #[tokio::test]
    async fn the_fidelity_corpus_decodes_unchanged() {
        use faucet_conformance::fidelity;
        let sent = fidelity::corpus();
        let mut landed = Vec::new();
        for record in &sent {
            let bytes = serde_json::to_vec(record).unwrap();
            landed.push(
                decode(
                    Some(&bytes),
                    &KafkaValueFormat::Json,
                    #[cfg(feature = "schema-registry")]
                    None,
                )
                .await
                .unwrap(),
            );
        }
        fidelity::assert_round_trip(&sent, &landed, fidelity::Tolerance::exact());
    }

    #[tokio::test]
    async fn decode_json_object() {
        let bytes = br#"{"id": 7, "name": "x"}"#;
        let v = decode(
            Some(bytes),
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v["id"], 7);
    }

    #[tokio::test]
    async fn decode_raw_string_utf8() {
        let bytes = "hello".as_bytes();
        let v = decode(
            Some(bytes),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, Value::String("hello".into()));
    }

    #[tokio::test]
    async fn decode_bytes_base64_encodes() {
        let bytes = &[0xDE, 0xAD, 0xBE, 0xEF];
        let v = decode(
            Some(bytes),
            &KafkaValueFormat::Bytes,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, Value::String("3q2+7w==".into()));
    }

    #[tokio::test]
    async fn decode_none_returns_null() {
        let v = decode(
            None,
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap();
        assert_eq!(v, Value::Null);
    }

    #[tokio::test]
    async fn decode_invalid_json_errors() {
        let bytes = b"{not json";
        let err = decode(
            Some(bytes),
            &KafkaValueFormat::Json,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap_err();
        assert!(format!("{err}").contains("json"));
    }

    #[tokio::test]
    async fn decode_raw_string_rejects_invalid_utf8() {
        let bytes = &[0xFF, 0xFE];
        let err = decode(
            Some(bytes),
            &KafkaValueFormat::RawString,
            #[cfg(feature = "schema-registry")]
            None,
        )
        .await
        .unwrap_err();
        let m = format!("{err}").to_lowercase();
        assert!(m.contains("utf-8") || m.contains("utf"));
    }

    #[cfg(feature = "schema-registry")]
    #[tokio::test]
    async fn decode_confluent_avro_without_client_is_config_error() {
        use faucet_common_kafka::SchemaRegistryConfig;
        let format = KafkaValueFormat::ConfluentAvro {
            schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            bytes_encoding: Default::default(),
        };
        let err = decode(Some(b"\x00\x00\x00\x00\x01"), &format, None)
            .await
            .unwrap_err();
        match err {
            FaucetError::Config(msg) => assert!(
                msg.contains("ConfluentAvro") && msg.contains("no SchemaRegistryClient"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "schema-registry")]
    #[tokio::test]
    async fn decode_confluent_protobuf_without_client_is_config_error() {
        use faucet_common_kafka::SchemaRegistryConfig;
        let format = KafkaValueFormat::ConfluentProtobuf {
            schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
        };
        let err = decode(Some(b"\x00\x00\x00\x00\x01"), &format, None)
            .await
            .unwrap_err();
        match err {
            FaucetError::Config(msg) => assert!(
                msg.contains("ConfluentProtobuf") && msg.contains("no SchemaRegistryClient"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "schema-registry")]
    #[tokio::test]
    async fn decode_confluent_json_schema_without_client_is_config_error() {
        use faucet_common_kafka::SchemaRegistryConfig;
        let format = KafkaValueFormat::ConfluentJsonSchema {
            schema_registry: SchemaRegistryConfig::new("http://localhost:8081"),
            validate: false,
        };
        let err = decode(Some(b"\x00\x00\x00\x00\x01"), &format, None)
            .await
            .unwrap_err();
        match err {
            FaucetError::Config(msg) => assert!(
                msg.contains("ConfluentJsonSchema") && msg.contains("no SchemaRegistryClient"),
                "unexpected message: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }
}

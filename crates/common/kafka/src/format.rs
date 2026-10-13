//! Kafka value/key serialization formats and policy enums shared by
//! `faucet-source-kafka` and `faucet-sink-kafka`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How a Kafka message value (or key) is encoded on the wire.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KafkaValueFormat {
    /// Parse value bytes as a JSON document.
    #[default]
    Json,
    /// Treat value bytes as a UTF-8 string. Invalid UTF-8 fails per `OnDecodeError`.
    RawString,
    /// Pass value bytes through as a base64-encoded string inside the JSON record.
    /// On the sink side, expects a base64 string in the source record.
    Bytes,

    /// Confluent-wire-format Avro: magic byte + schema_id + Avro binary.
    #[cfg(feature = "schema-registry")]
    ConfluentAvro {
        schema_registry: crate::SchemaRegistryConfig,
        /// How the sink reads a JSON string for an Avro `bytes` / `fixed`
        /// field: `base64` (default — the shape the Kafka source decodes those
        /// fields to, so records round-trip), `hex` (the source's rendering
        /// before #885) or `utf8` (the string's UTF-8 bytes, the reading before
        /// faucet-common-kafka 1.1.1). A JSON array of byte numbers is accepted
        /// either way. Ignored by the source.
        #[serde(default)]
        bytes_encoding: faucet_core::file_format::avro::BytesText,
    },

    /// Confluent-wire-format Protobuf: magic byte + schema_id + protobuf binary.
    #[cfg(feature = "schema-registry")]
    ConfluentProtobuf {
        schema_registry: crate::SchemaRegistryConfig,
    },

    /// Confluent-wire-format JSON Schema: magic byte + schema_id + JSON payload.
    /// When `validate` is true, decoded JSON is validated against the registered schema.
    #[cfg(feature = "schema-registry")]
    ConfluentJsonSchema {
        schema_registry: crate::SchemaRegistryConfig,
        #[serde(default)]
        validate: bool,
    },
}

impl KafkaValueFormat {
    /// True for Confluent Schema Registry wire formats (`ConfluentAvro`,
    /// `ConfluentProtobuf`, `ConfluentJsonSchema`), which require a schema to
    /// encode on the sink side. Always `false` when the `schema-registry`
    /// feature is disabled (those variants don't exist).
    pub fn is_schema_registry(&self) -> bool {
        #[cfg(feature = "schema-registry")]
        {
            matches!(
                self,
                KafkaValueFormat::ConfluentAvro { .. }
                    | KafkaValueFormat::ConfluentProtobuf { .. }
                    | KafkaValueFormat::ConfluentJsonSchema { .. }
            )
        }
        #[cfg(not(feature = "schema-registry"))]
        {
            false
        }
    }
}

/// Producer-side compression for outbound batches.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    #[default]
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::None => "none",
            CompressionType::Gzip => "gzip",
            CompressionType::Snappy => "snappy",
            CompressionType::Lz4 => "lz4",
            CompressionType::Zstd => "zstd",
        }
    }
}

/// What the source does when a single message fails to decode.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnDecodeError {
    /// Drop the message and continue (logs a warning).
    Skip,
    /// Surface `FaucetError::Source` and abort the batch.
    #[default]
    Fail,
}

/// What the sink does when key/partition extraction fails for a record.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnKeyError {
    /// Drop the record (logs a warning).
    Skip,
    /// Surface `FaucetError::Sink` and abort the batch.
    #[default]
    Fail,
    /// Send the record with no key (librdkafka picks the partition).
    RoundRobin,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_format_json_serializes_as_object_with_type() {
        let v = serde_json::to_value(KafkaValueFormat::Json).unwrap();
        assert_eq!(v["type"], "json");
    }

    #[test]
    fn value_format_deserializes_raw_string() {
        let parsed: KafkaValueFormat =
            serde_json::from_value(serde_json::json!({"type": "raw_string"})).unwrap();
        assert!(matches!(parsed, KafkaValueFormat::RawString));
    }

    #[test]
    fn value_format_bytes_round_trip() {
        let parsed: KafkaValueFormat =
            serde_json::from_value(serde_json::json!({"type": "bytes"})).unwrap();
        assert!(matches!(parsed, KafkaValueFormat::Bytes));
    }

    #[test]
    fn compression_round_trip() {
        for v in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Snappy,
            CompressionType::Lz4,
            CompressionType::Zstd,
        ] {
            let s = serde_json::to_value(v).unwrap();
            let back: CompressionType = serde_json::from_value(s.clone()).unwrap();
            assert_eq!(v, back);
            assert_eq!(s.as_str().unwrap(), v.as_str());
        }
    }

    #[test]
    fn on_decode_error_default_is_fail() {
        assert_eq!(OnDecodeError::default(), OnDecodeError::Fail);
    }

    #[test]
    fn on_key_error_default_is_fail() {
        assert_eq!(OnKeyError::default(), OnKeyError::Fail);
    }

    #[test]
    fn compression_default_is_none() {
        assert_eq!(CompressionType::default(), CompressionType::None);
    }

    #[test]
    fn schema_for_format_types_compile() {
        let _ = schemars::schema_for!(KafkaValueFormat);
        let _ = schemars::schema_for!(CompressionType);
        let _ = schemars::schema_for!(OnDecodeError);
        let _ = schemars::schema_for!(OnKeyError);
    }

    #[cfg(feature = "schema-registry")]
    #[test]
    fn confluent_avro_round_trips_through_serde() {
        let cfg = crate::SchemaRegistryConfig::new("http://localhost:8081");
        let format = KafkaValueFormat::ConfluentAvro {
            schema_registry: cfg,
            bytes_encoding: Default::default(),
        };
        let s = serde_json::to_value(&format).unwrap();
        assert_eq!(s["type"], "confluent_avro");
        let parsed: KafkaValueFormat = serde_json::from_value(s).unwrap();
        assert!(matches!(parsed, KafkaValueFormat::ConfluentAvro { .. }));
    }
}

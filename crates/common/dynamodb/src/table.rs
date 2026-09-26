//! Key-schema helpers over `DescribeTable` output. Pure.

use aws_sdk_dynamodb::types::{KeyType, ScalarAttributeType, TableDescription};
use faucet_core::FaucetError;
use serde_json::{Map, Value, json};

/// Whether a key attribute is the partition (hash) or sort (range) key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyRole {
    /// Partition key.
    Hash,
    /// Sort key.
    Range,
}

/// The scalar type of a key attribute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarType {
    /// String (`S`).
    String,
    /// Number (`N`).
    Number,
    /// Binary (`B`).
    Binary,
}

impl ScalarType {
    /// JSON-Schema fragment for values of this type as emitted by the source
    /// (numbers may fall back to strings to preserve precision).
    pub fn json_schema(self) -> Value {
        match self {
            Self::String | Self::Binary => json!({"type": "string"}),
            Self::Number => json!({"type": ["number", "string"]}),
        }
    }
}

/// One attribute of a table's primary key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyAttribute {
    /// Attribute name.
    pub name: String,
    /// Partition or sort key.
    pub role: KeyRole,
    /// Declared scalar type.
    pub scalar: ScalarType,
}

/// Extract the primary key (partition key first, then the optional sort key)
/// from a `DescribeTable` result.
pub fn key_schema(desc: &TableDescription) -> Result<Vec<KeyAttribute>, FaucetError> {
    let table = desc.table_name().unwrap_or("?");
    let mut out = Vec::new();
    for el in desc.key_schema() {
        let role = match el.key_type() {
            KeyType::Hash => KeyRole::Hash,
            KeyType::Range => KeyRole::Range,
            other => {
                return Err(FaucetError::Config(format!(
                    "dynamodb: table '{table}' has unknown key type {other:?}"
                )));
            }
        };
        let name = el.attribute_name().to_string();
        let scalar = desc
            .attribute_definitions()
            .iter()
            .find(|d| d.attribute_name() == name)
            .map(|d| match d.attribute_type() {
                ScalarAttributeType::N => ScalarType::Number,
                ScalarAttributeType::B => ScalarType::Binary,
                _ => ScalarType::String,
            })
            .unwrap_or(ScalarType::String);
        out.push(KeyAttribute { name, role, scalar });
    }
    if out.is_empty() {
        return Err(FaucetError::Config(format!(
            "dynamodb: table '{table}' reports no key schema"
        )));
    }
    out.sort_by_key(|k| match k.role {
        KeyRole::Hash => 0,
        KeyRole::Range => 1,
    });
    Ok(out)
}

/// Project the key attributes out of a JSON record (`null` for missing ones).
pub fn key_of(record: &Value, keys: &[KeyAttribute]) -> Value {
    let mut obj = Map::new();
    for k in keys {
        obj.insert(
            k.name.clone(),
            record.get(&k.name).cloned().unwrap_or(Value::Null),
        );
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::{AttributeDefinition, KeySchemaElement};

    pub(crate) fn table(keys: &[(&str, KeyType, ScalarAttributeType)]) -> TableDescription {
        let mut b = TableDescription::builder().table_name("t");
        for (name, kt, st) in keys {
            b = b
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(*name)
                        .key_type(kt.clone())
                        .build()
                        .unwrap(),
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(*name)
                        .attribute_type(st.clone())
                        .build()
                        .unwrap(),
                );
        }
        b.build()
    }

    #[test]
    fn key_schema_orders_hash_first_and_maps_types() {
        let desc = table(&[
            ("sk", KeyType::Range, ScalarAttributeType::N),
            ("pk", KeyType::Hash, ScalarAttributeType::S),
        ]);
        let keys = key_schema(&desc).unwrap();
        assert_eq!(keys[0].name, "pk");
        assert_eq!(keys[0].role, KeyRole::Hash);
        assert_eq!(keys[0].scalar, ScalarType::String);
        assert_eq!(keys[1].scalar, ScalarType::Number);
        let bin = key_schema(&table(&[("b", KeyType::Hash, ScalarAttributeType::B)])).unwrap();
        assert_eq!(bin[0].scalar, ScalarType::Binary);
        assert_eq!(
            key_of(&json!({"pk": "a", "x": 1}), &keys),
            json!({"pk": "a", "sk": null})
        );
    }

    #[test]
    fn missing_definitions_default_to_string_and_empty_schema_errors() {
        let desc = TableDescription::builder()
            .table_name("t")
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .build();
        assert_eq!(key_schema(&desc).unwrap()[0].scalar, ScalarType::String);
        let err = key_schema(&TableDescription::builder().build()).unwrap_err();
        assert!(err.to_string().contains("no key schema"));
        let odd = TableDescription::builder()
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("pk")
                    .key_type(KeyType::from("WEIRD"))
                    .build()
                    .unwrap(),
            )
            .build();
        assert!(
            key_schema(&odd)
                .unwrap_err()
                .to_string()
                .contains("unknown key type")
        );
    }

    #[test]
    fn scalar_json_schema() {
        assert_eq!(ScalarType::String.json_schema(), json!({"type": "string"}));
        assert_eq!(ScalarType::Binary.json_schema(), json!({"type": "string"}));
        assert_eq!(
            ScalarType::Number.json_schema(),
            json!({"type": ["number", "string"]})
        );
    }
}

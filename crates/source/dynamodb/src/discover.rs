//! `DescribeTable` → [`DatasetDescriptor`]. Pure.

use aws_sdk_dynamodb::types::TableDescription;
use faucet_common_dynamodb::key_schema;
use faucet_core::DatasetDescriptor;
use serde_json::json;

/// One descriptor per table. DynamoDB is schemaless beyond the key, so the
/// schema lists the key attributes only; `config_patch` selects the table.
pub fn descriptor_from_table(desc: &TableDescription) -> Option<DatasetDescriptor> {
    let name = desc.table_name()?;
    let mut d = DatasetDescriptor::new(name, "table", json!({ "table_name": name }));
    if let Ok(keys) = key_schema(desc) {
        d = d.with_schema(faucet_core::columns_to_schema(
            keys.iter()
                .map(|k| (k.name.clone(), k.scalar.json_schema())),
        ));
    }
    if let Some(rows) = desc.item_count().and_then(|n| u64::try_from(n).ok()) {
        d = d.with_estimated_rows(rows);
    }
    Some(d)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    #[test]
    fn describes_table_with_key_schema_and_count() {
        let desc = TableDescription::builder()
            .table_name("orders")
            .item_count(42)
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
            )
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::N)
                    .build()
                    .unwrap(),
            )
            .build();
        let d = descriptor_from_table(&desc).unwrap();
        assert_eq!(d.name, "orders");
        assert_eq!(d.kind, "table");
        assert_eq!(d.config_patch, json!({"table_name": "orders"}));
        assert_eq!(d.estimated_rows, Some(42));
        assert_eq!(
            d.schema.unwrap()["properties"]["id"],
            json!({"type": ["number", "string"]})
        );
    }

    #[test]
    fn tolerates_missing_parts() {
        assert!(descriptor_from_table(&TableDescription::builder().build()).is_none());
        let d = descriptor_from_table(
            &TableDescription::builder()
                .table_name("t")
                .item_count(-1)
                .build(),
        )
        .unwrap();
        assert!(d.schema.is_none() && d.estimated_rows.is_none());
    }
}

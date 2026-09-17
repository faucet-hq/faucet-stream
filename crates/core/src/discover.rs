//! Live source introspection (`faucet discover`, #211).
//!
//! A [`Source`](crate::Source) that can enumerate the datasets living behind
//! its connection (tables in a database schema, collections in MongoDB,
//! indices in Elasticsearch, key prefixes in an object store) overrides
//! [`Source::discover`](crate::Source::discover) to return one
//! [`DatasetDescriptor`] per dataset. The CLI turns that list into a
//! ready-to-run config: one matrix row per dataset, each row deep-merging the
//! descriptor's [`config_patch`](DatasetDescriptor::config_patch) over the
//! connection config.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// One dataset discovered behind a source's connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DatasetDescriptor {
    /// Human-readable dataset identity — a qualified table name
    /// (`public.orders`), a collection, an index, or an object-key prefix.
    pub name: String,
    /// What kind of dataset this is: `table`, `collection`, `index`,
    /// `prefix`, or `object`. Informational (rendered in output); the
    /// selection mechanics live entirely in [`config_patch`](Self::config_patch).
    pub kind: String,
    /// The dataset's column shape as an
    /// [`infer_schema`](crate::schema::infer_schema)-shaped object
    /// (`{"type":"object","properties":{…}}`), or `None` when the source
    /// cannot cheaply introspect it (object stores, schemaless collections
    /// with no sample).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<Value>,
    /// Approximate row/object count when the catalog exposes one cheaply
    /// (e.g. `pg_class.reltuples`). Never computed via a full scan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimated_rows: Option<u64>,
    /// Partial source-config **override** selecting exactly this dataset —
    /// deep-merged over the connection config by a matrix row (e.g.
    /// `{"query": "SELECT * FROM \"public\".\"orders\""}` for a SQL source,
    /// `{"collection": "orders"}` for MongoDB, `{"prefix": "raw/orders/"}`
    /// for an object store). Must never contain credentials.
    pub config_patch: Value,
    /// Optional partial **sink**-config override routing this dataset to its own
    /// destination — deep-merged over the sink template by a matrix row (e.g.
    /// `{"table_id": "account"}` so a fan-out of Salesforce objects lands one
    /// table per object). `None` (the common case) leaves the sink untouched.
    /// Must never contain credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_patch: Option<Value>,
}

impl DatasetDescriptor {
    /// Construct a descriptor with no schema / row estimate.
    pub fn new(name: impl Into<String>, kind: impl Into<String>, config_patch: Value) -> Self {
        Self {
            name: name.into(),
            kind: kind.into(),
            schema: None,
            estimated_rows: None,
            config_patch,
            sink_patch: None,
        }
    }

    /// Attach an inferred/introspected schema.
    pub fn with_schema(mut self, schema: Value) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Attach a sink-config override routing this dataset to its own destination.
    pub fn with_sink_patch(mut self, sink_patch: Value) -> Self {
        self.sink_patch = Some(sink_patch);
        self
    }

    /// Attach a catalog-provided row estimate.
    pub fn with_estimated_rows(mut self, rows: u64) -> Self {
        self.estimated_rows = Some(rows);
        self
    }
}

/// Map a SQL catalog type name (as reported by `information_schema.columns`
/// or an equivalent) to a JSON-Schema type fragment matching the shape
/// [`infer_schema`](crate::schema::infer_schema) produces.
///
/// The mapping is deliberately conservative and dialect-tolerant — it matches
/// on lowercase substrings so `TIMESTAMP WITH TIME ZONE`, `timestamptz`,
/// `Nullable(Int64)` etc. all land on a sensible JSON type. Unknown types map
/// to `string` (every SQL value has a textual form, so `string` is the safe
/// over-approximation — never a lossy one like `number`).
pub fn sql_type_to_json_schema(data_type: &str) -> Value {
    let t = data_type.to_ascii_lowercase();
    let ty = if t.contains("bool") || t == "bit" {
        "boolean"
    } else if t.contains("json") || t.contains("variant") || t == "object" || t.contains("struct") {
        // checked before the integer branch so `STRUCT<a INT>` maps to object
        "object"
    } else if t.contains("array") || t.starts_with('_') {
        // Postgres reports array types as `ARRAY` (information_schema) or
        // `_int4`-style internal names.
        "array"
    } else if (t.contains("int") && !t.contains("interval") && !t.contains("point"))
        || t == "serial"
        || t == "bigserial"
        || t == "smallserial"
    {
        // covers int2/4/8, integer, bigint, smallint, tinyint, mediumint —
        // but not `interval` / `point`, whose substring would false-match
        "integer"
    } else if t.contains("double")
        || t.contains("float")
        || t.contains("real")
        || t.contains("decimal")
        || t.contains("numeric")
        || t.contains("money")
        || t == "number"
    {
        "number"
    } else {
        // text, varchar, char, uuid, date, time, timestamp, bytea, blob,
        // enum, inet, … — all serialized as JSON strings by the sources.
        "string"
    };
    json!({ "type": ty })
}

/// Wrap a type fragment as nullable (`{"type": ["T", "null"]}`), matching the
/// nullable shape [`infer_schema`](crate::schema::infer_schema) emits.
pub fn nullable_type(mut fragment: Value) -> Value {
    let single = match fragment.get("type") {
        Some(Value::String(t)) => t.clone(),
        _ => return fragment,
    };
    // Wrap only the `type` in place so sibling keys (e.g. a `format: date-time`
    // hint a typed sink maps to TIMESTAMP/DATE) survive the nullability wrap.
    if let Some(obj) = fragment.as_object_mut() {
        obj.insert("type".into(), json!([single, "null"]));
    }
    fragment
}

/// Assemble an `infer_schema`-shaped object schema from
/// `(column_name, type_fragment)` pairs, preserving input order semantics
/// (`serde_json` map ordering applies).
pub fn columns_to_schema(columns: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut properties = serde_json::Map::new();
    for (name, fragment) in columns {
        properties.insert(name, fragment);
    }
    json!({ "type": "object", "properties": Value::Object(properties) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_builder_round_trips() {
        let d = DatasetDescriptor::new("public.orders", "table", json!({"query": "SELECT 1"}))
            .with_schema(json!({"type": "object", "properties": {}}))
            .with_estimated_rows(42);
        assert_eq!(d.name, "public.orders");
        assert_eq!(d.kind, "table");
        assert_eq!(d.estimated_rows, Some(42));
        let v = serde_json::to_value(&d).unwrap();
        let back: DatasetDescriptor = serde_json::from_value(v).unwrap();
        assert_eq!(back, d);
    }

    #[test]
    fn descriptor_omits_absent_optionals_in_json() {
        let d = DatasetDescriptor::new("t", "table", json!({}));
        let v = serde_json::to_value(&d).unwrap();
        assert!(v.get("schema").is_none());
        assert!(v.get("estimated_rows").is_none());
    }

    #[test]
    fn sql_types_map_to_json_types() {
        for (sql, want) in [
            ("integer", "integer"),
            ("BIGINT", "integer"),
            ("smallint", "integer"),
            ("tinyint(1)", "integer"),
            ("serial", "integer"),
            ("double precision", "number"),
            ("NUMERIC(10,2)", "number"),
            ("decimal", "number"),
            ("float8", "number"),
            ("money", "number"),
            ("boolean", "boolean"),
            ("BOOL", "boolean"),
            ("bit", "boolean"),
            ("json", "object"),
            ("JSONB", "object"),
            ("VARIANT", "object"),
            ("STRUCT<a INT>", "object"),
            ("ARRAY", "array"),
            ("_int4", "array"),
            ("text", "string"),
            ("character varying", "string"),
            ("uuid", "string"),
            ("timestamp with time zone", "string"),
            ("date", "string"),
            ("bytea", "string"),
            ("some_exotic_type", "string"),
        ] {
            assert_eq!(
                sql_type_to_json_schema(sql),
                json!({ "type": want }),
                "for SQL type {sql:?}"
            );
        }
    }

    #[test]
    fn nullable_wraps_scalar_type() {
        assert_eq!(
            nullable_type(json!({"type": "integer"})),
            json!({"type": ["integer", "null"]})
        );
        // Already-complex fragments pass through untouched.
        let complex = json!({"type": ["string", "null"]});
        assert_eq!(nullable_type(complex.clone()), complex);
        // Sibling keys survive the wrap — a temporal `format` hint must not be
        // lost when the column is nullable (a typed sink maps it to TIMESTAMP).
        assert_eq!(
            nullable_type(json!({"type": "string", "format": "date-time"})),
            json!({"type": ["string", "null"], "format": "date-time"})
        );
    }

    #[test]
    fn columns_to_schema_shapes_like_infer_schema() {
        let schema = columns_to_schema(vec![
            ("id".to_string(), json!({"type": "integer"})),
            ("name".to_string(), json!({"type": ["string", "null"]})),
        ]);
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"]["id"]["type"], "integer");
        assert_eq!(schema["properties"]["name"]["type"][1], "null");
    }
}

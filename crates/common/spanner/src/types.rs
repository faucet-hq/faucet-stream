//! Spanner `INFORMATION_SCHEMA` type-string parsing and the mapping from
//! Spanner column types to JSON-Schema fragments.

use serde_json::{Value, json};

/// A parsed Spanner column type (GoogleSQL dialect).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpannerType {
    Bool,
    Int64,
    Float32,
    Float64,
    Timestamp,
    Date,
    /// `STRING(n)` / `STRING(MAX)` — the length bound is not retained.
    String,
    /// `BYTES(n)` / `BYTES(MAX)` — values travel base64-encoded.
    Bytes,
    /// Arbitrary-precision decimal; decoded/encoded as a string to preserve
    /// precision.
    Numeric,
    Json,
    Array(Box<SpannerType>),
    /// STRUCT / PROTO / ENUM / anything unrecognized — decoded generically,
    /// not writable by the sink.
    Other,
}

/// Parse an `INFORMATION_SCHEMA.COLUMNS.SPANNER_TYPE` string
/// (e.g. `STRING(MAX)`, `INT64`, `ARRAY<FLOAT64>`) into a [`SpannerType`].
/// Unrecognized types parse as [`SpannerType::Other`], never an error.
pub fn parse_spanner_type(raw: &str) -> SpannerType {
    let s = raw.trim();
    let upper = s.to_ascii_uppercase();
    if let Some(inner) = upper
        .strip_prefix("ARRAY<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        return SpannerType::Array(Box::new(parse_spanner_type(inner)));
    }
    // Strip a length parameter: `STRING(MAX)` / `BYTES(256)`.
    let base = upper.split('(').next().unwrap_or(&upper).trim();
    match base {
        "BOOL" => SpannerType::Bool,
        "INT64" => SpannerType::Int64,
        "FLOAT32" => SpannerType::Float32,
        "FLOAT64" => SpannerType::Float64,
        "TIMESTAMP" => SpannerType::Timestamp,
        "DATE" => SpannerType::Date,
        "STRING" => SpannerType::String,
        "BYTES" => SpannerType::Bytes,
        "NUMERIC" => SpannerType::Numeric,
        "JSON" => SpannerType::Json,
        _ => SpannerType::Other,
    }
}

/// Parse a PostgreSQL-dialect `information_schema.columns.spanner_type`
/// string (e.g. `character varying`, `bigint`, `character varying[]`).
/// Unrecognized types parse as [`SpannerType::Other`], never an error.
pub fn parse_pg_spanner_type(raw: &str) -> SpannerType {
    let lower = raw.trim().to_ascii_lowercase();
    if let Some(inner) = lower.strip_suffix("[]") {
        return SpannerType::Array(Box::new(parse_pg_spanner_type(inner)));
    }
    // Strip a length / precision parameter: `character varying(100)`.
    let base = lower.split('(').next().unwrap_or(&lower).trim();
    match base {
        "boolean" | "bool" => SpannerType::Bool,
        "bigint" | "int8" => SpannerType::Int64,
        "real" | "float4" => SpannerType::Float32,
        "double precision" | "float8" => SpannerType::Float64,
        "timestamp with time zone" | "timestamptz" | "spanner.commit_timestamp" => {
            SpannerType::Timestamp
        }
        "date" => SpannerType::Date,
        "character varying" | "varchar" | "text" => SpannerType::String,
        "bytea" => SpannerType::Bytes,
        "numeric" | "decimal" => SpannerType::Numeric,
        "jsonb" => SpannerType::Json,
        _ => SpannerType::Other,
    }
}

/// Map a [`SpannerType`] to the JSON-Schema fragment faucet's discover /
/// schema-drift machinery expects (an `infer_schema`-shaped `{"type": …}`).
/// `nullable` widens the type with `"null"`.
///
/// NUMERIC maps to `string` (matching the decoder, which emits NUMERIC as a
/// string to preserve precision).
pub fn spanner_type_to_json_schema(ty: &SpannerType, nullable: bool) -> Value {
    let base: Value = match ty {
        SpannerType::Bool => json!({"type": "boolean"}),
        SpannerType::Int64 => json!({"type": "integer"}),
        SpannerType::Float32 | SpannerType::Float64 => json!({"type": "number"}),
        SpannerType::Json => json!({"type": "object"}),
        SpannerType::Array(inner) => {
            json!({"type": "array", "items": spanner_type_to_json_schema(inner, false)})
        }
        SpannerType::Timestamp
        | SpannerType::Date
        | SpannerType::String
        | SpannerType::Bytes
        | SpannerType::Numeric => json!({"type": "string"}),
        SpannerType::Other => json!({"type": "object"}),
    };
    if nullable {
        let mut obj = base;
        if let Some(t) = obj.get("type").cloned() {
            obj["type"] = json!([t, "null"]);
        }
        obj
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scalar_types_case_insensitively_and_strips_lengths() {
        assert_eq!(parse_spanner_type("INT64"), SpannerType::Int64);
        assert_eq!(parse_spanner_type("string(MAX)"), SpannerType::String);
        assert_eq!(parse_spanner_type("BYTES(256)"), SpannerType::Bytes);
        assert_eq!(parse_spanner_type("Float64"), SpannerType::Float64);
        assert_eq!(parse_spanner_type("FLOAT32"), SpannerType::Float32);
        assert_eq!(parse_spanner_type("NUMERIC"), SpannerType::Numeric);
        assert_eq!(parse_spanner_type("JSON"), SpannerType::Json);
        assert_eq!(parse_spanner_type("TIMESTAMP"), SpannerType::Timestamp);
        assert_eq!(parse_spanner_type("DATE"), SpannerType::Date);
        assert_eq!(parse_spanner_type("BOOL"), SpannerType::Bool);
    }

    #[test]
    fn parses_arrays_recursively() {
        assert_eq!(
            parse_spanner_type("ARRAY<INT64>"),
            SpannerType::Array(Box::new(SpannerType::Int64))
        );
        assert_eq!(
            parse_spanner_type("ARRAY<STRING(MAX)>"),
            SpannerType::Array(Box::new(SpannerType::String))
        );
    }

    #[test]
    fn parses_postgresql_type_strings() {
        for (raw, want) in [
            ("bigint", SpannerType::Int64),
            ("character varying", SpannerType::String),
            ("character varying(100)", SpannerType::String),
            ("numeric", SpannerType::Numeric),
            ("jsonb", SpannerType::Json),
            ("timestamp with time zone", SpannerType::Timestamp),
            ("bytea", SpannerType::Bytes),
            ("double precision", SpannerType::Float64),
            ("real", SpannerType::Float32),
            ("boolean", SpannerType::Bool),
            ("date", SpannerType::Date),
        ] {
            assert_eq!(parse_pg_spanner_type(raw), want, "{raw}");
        }
        assert_eq!(
            parse_pg_spanner_type("character varying[]"),
            SpannerType::Array(Box::new(SpannerType::String))
        );
        assert_eq!(parse_pg_spanner_type("interval"), SpannerType::Other);
        assert_eq!(parse_pg_spanner_type(""), SpannerType::Other);
    }

    #[test]
    fn unknown_types_map_to_other_not_error() {
        assert_eq!(
            parse_spanner_type("STRUCT<a INT64, b STRING(MAX)>"),
            SpannerType::Other
        );
        assert_eq!(parse_spanner_type("PROTO<x.Y>"), SpannerType::Other);
        assert_eq!(parse_spanner_type(""), SpannerType::Other);
    }

    #[test]
    fn json_schema_mapping_and_nullability() {
        assert_eq!(
            spanner_type_to_json_schema(&SpannerType::Int64, false),
            serde_json::json!({"type": "integer"})
        );
        assert_eq!(
            spanner_type_to_json_schema(&SpannerType::Numeric, false),
            serde_json::json!({"type": "string"})
        );
        assert_eq!(
            spanner_type_to_json_schema(&SpannerType::Bool, true),
            serde_json::json!({"type": ["boolean", "null"]})
        );
        assert_eq!(
            spanner_type_to_json_schema(&SpannerType::Array(Box::new(SpannerType::Float64)), false),
            serde_json::json!({"type": "array", "items": {"type": "number"}})
        );
    }
}

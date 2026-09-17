//! Compile YAML/JSON transform declarations into `TransformStage` values.
//!
//! The built-in transforms are exposed via config — custom closure
//! transforms require Rust code and are reserved for the library API.

use crate::config::TransformSpec;
use crate::error::{CliError, CliResult};
#[cfg(feature = "transform-zip-columns")]
use faucet_core::ZipColumnsSpec;
#[cfg(feature = "transforms")]
use faucet_core::{
    CastOnError, CastType, CrossJoinSpec, HashAlgorithm, HashEncoding, JsonParseOnError,
    KeyCaseMode, KeyCollision, LookupOnMissing, TreeFlattenSpec, UnpivotSpec, ValueCaseMode,
};
#[cfg(any(feature = "transforms", feature = "transform-cdc-unwrap"))]
use faucet_core::{JsonSchema, schema_for};
use faucet_core::{RecordTransform, TransformStage};
#[cfg(any(feature = "transforms", feature = "transform-cdc-unwrap"))]
use serde::Deserialize;
use serde_json::Value;
#[cfg(feature = "transforms")]
use std::collections::HashMap;

/// Inline-config schema for the `flatten` transform.
#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct FlattenConfig {
    /// Separator joining nested keys (default: `"__"`).
    #[serde(default = "default_separator")]
    separator: String,
}

#[cfg(feature = "transforms")]
fn default_separator() -> String {
    "__".to_owned()
}

/// Inline-config schema for the `rename_keys` transform.
#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct RenameKeysConfig {
    /// Rust regex matched against every key.
    pattern: String,
    /// Replacement string. May reference capture groups (`$1`, `${name}`).
    replacement: String,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct FieldsConfig {
    /// Top-level field names to act on.
    fields: Vec<String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct SetConfig {
    /// Map of field name → constant value to set on every record.
    values: serde_json::Map<String, Value>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct RenameFieldConfig {
    /// Map of old field name → new field name.
    fields: HashMap<String, String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct CastConfig {
    /// Map of field name → target type.
    fields: HashMap<String, CastType>,
    /// What to do when a value cannot be cast. Default: `error`.
    #[serde(default)]
    on_error: CastOnError,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct RedactConfig {
    /// Top-level field names to overwrite with `mask`.
    fields: Vec<String>,
    /// Replacement value. Default: the string `"***"`.
    #[serde(default = "default_mask")]
    mask: Value,
}

#[cfg(feature = "transforms")]
fn default_mask() -> Value {
    Value::String("***".to_owned())
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct ValueCaseConfig {
    /// String-valued fields to re-case.
    fields: Vec<String>,
    /// Casing convention to apply to each listed field.
    mode: ValueCaseMode,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct SpellSymbolsConfig {
    /// Extra symbol → word overrides layered on top of the built-in map.
    #[serde(default)]
    extra: HashMap<String, String>,
    /// String inserted between expanded words. Default: a single space.
    #[serde(default = "default_spell_separator")]
    separator: String,
}

#[cfg(feature = "transforms")]
fn default_spell_separator() -> String {
    " ".to_owned()
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct KeysCaseConfig {
    /// Output convention for every key in the record.
    mode: KeyCaseMode,
    /// What to do when two distinct keys re-case to the same name:
    /// `error` (default — fail loudly) or `suffix` (append `_2`, `_3`, …).
    #[serde(default)]
    on_collision: KeyCollision,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct HashConfig {
    /// One or more field names to hash. When `into` is set, exactly one.
    fields: Vec<String>,
    /// Hash algorithm. Default: `sha256`.
    #[serde(default)]
    algorithm: HashAlgorithm,
    /// Digest encoding. Default: `hex`.
    #[serde(default)]
    encoding: HashEncoding,
    /// Optional salt prepended before hashing. Recommend `${env:...}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    salt: Option<String>,
    /// Optional target key for the digest (source left intact). `null` =
    /// replace in place. Only valid with a single field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    into: Option<String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct JsonParseConfig {
    /// Field names holding stringified JSON to parse.
    fields: Vec<String>,
    /// What to do when a value is a string but not valid JSON. Default: `keep`.
    #[serde(default)]
    on_error: JsonParseOnError,
    /// Optional target key for the parsed value. `null` = replace in place.
    /// Only valid with a single field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    into: Option<String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct CoalesceConfig {
    /// Target field to fill when missing or null.
    field: String,
    /// Literal default value. Mutually exclusive with `from`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default: Option<Value>,
    /// Fallback keys; the first non-null wins. Mutually exclusive with `default`.
    #[serde(default)]
    from: Vec<String>,
    /// Treat an empty string as null. Default: `false`.
    #[serde(default)]
    treat_empty_string_as_null: bool,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct SplitConfig {
    /// String field to split into an array.
    field: String,
    /// Delimiter to split on.
    delimiter: String,
    /// Trim whitespace from each element. Default: `false`.
    #[serde(default)]
    trim: bool,
    /// Optional target key for the array. `null` = replace in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    into: Option<String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct JoinConfig {
    /// Array field to join into a string.
    field: String,
    /// Delimiter placed between elements.
    delimiter: String,
    /// Optional target key for the string. `null` = replace in place.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    into: Option<String>,
}

#[cfg(feature = "transform-filter")]
#[derive(Debug, Deserialize, JsonSchema)]
struct FilterConfig {
    /// JSONPath subset: bare key, dot path, or bracketed string key.
    path: String,
    /// One of `eq`, `ne`, `exists`, `in`, `not_in`.
    op: faucet_core::FilterOp,
    /// Required for `eq`/`ne`/`in`/`not_in`. For `in`/`not_in`, must be an array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

#[cfg(feature = "transform-explode")]
#[derive(Debug, Deserialize, JsonSchema)]
struct ExplodeConfig {
    /// JSONPath subset: bare key, dot path, or bracketed string key.
    path: String,
    /// Prefix prepended to object-element fields. Defaults to the last
    /// segment of `path`. Empty string = pure LATERAL FLATTEN (no prefix).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix: Option<String>,
    /// Separator between prefix and element field key. Default `"_"`.
    #[serde(default = "default_explode_separator_cli")]
    separator: String,
    /// `passthrough` (default), `drop`, or `error` when path doesn't yield a
    /// non-empty array.
    #[serde(default)]
    on_missing: faucet_core::OnMissing,
    /// Copy named fields from the parent record onto every exploded child
    /// (#555): `{ dest_field: "source.dot.path" }`. The source is a dot path
    /// into the *parent* record (a leading `$.`/`$` is accepted). Useful when
    /// the exploded array is nested and the child rows need a parent key (e.g.
    /// `employee_id: "id"`) to stay joinable. Absent ⇒ standard explode.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    carry: HashMap<String, String>,
}

#[cfg(feature = "transform-explode")]
fn default_explode_separator_cli() -> String {
    "_".to_owned()
}

/// Resolve a dot path (`id`, `a.b.c`, a leading `$.`/`$` accepted) against a
/// record for `explode`'s `carry` (#555). Returns `None` if any segment is
/// missing.
#[cfg(feature = "transform-explode")]
fn get_dot_path(rec: &Value, path: &str) -> Option<Value> {
    let p = path.trim().trim_start_matches('$').trim_start_matches('.');
    let mut cur = rec;
    for seg in p.split('.').filter(|s| !s.is_empty()) {
        cur = cur.get(seg)?;
    }
    Some(cur.clone())
}

#[cfg(feature = "transform-cdc-unwrap")]
#[derive(Debug, Deserialize, JsonSchema)]
struct CdcUnwrapConfig {
    /// Envelope field holding the operation code. Default `"op"`.
    #[serde(default = "cdc_op_field")]
    op_field: String,
    /// Envelope field holding the post-image row. Default `"after"`.
    #[serde(default = "cdc_after_field")]
    after_field: String,
    /// Envelope field holding the pre-image row. Default `"before"`.
    #[serde(default = "cdc_before_field")]
    before_field: String,
    /// Fallback key field for deletes when `before` is absent. Default `"document_key"`.
    #[serde(default = "cdc_key_field")]
    key_field: String,
    /// Field stamped onto every emitted row with the op value. Default `"__op"`.
    #[serde(default = "cdc_marker_field")]
    marker_field: String,
    /// Op values that mean delete. Default `["d", "delete"]`.
    #[serde(default = "cdc_delete_ops")]
    delete_ops: Vec<String>,
    /// Op values that cause the record to be dropped (1→0). Default `["ddl", "truncate"]`.
    #[serde(default = "cdc_drop_ops")]
    drop_ops: Vec<String>,
}

#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_op_field() -> String {
    "op".into()
}
#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_after_field() -> String {
    "after".into()
}
#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_before_field() -> String {
    "before".into()
}
#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_key_field() -> String {
    "document_key".into()
}
#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_marker_field() -> String {
    "__op".into()
}
#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_delete_ops() -> Vec<String> {
    vec!["d".into(), "delete".into()]
}
#[cfg(feature = "transform-cdc-unwrap")]
fn cdc_drop_ops() -> Vec<String> {
    vec!["ddl".into(), "truncate".into()]
}

/// One row in the transform registry — the single source of truth for every
/// built-in transform's kind, one-line description, JSON Schema, and
/// `TransformSpec → TransformStage` decoder. `compile_one`,
/// Match-key config for the `lookup` transform.
#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct LookupOnConfig {
    /// Field on the incoming record to match on.
    record: String,
    /// Field on the reference rows to match against.
    #[serde(rename = "ref")]
    ref_field: String,
}

/// Inline-config schema for the `lookup` transform.
#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct LookupConfig {
    /// Inline reference rows. Mutually exclusive with `jsonl`.
    #[serde(default)]
    values: Option<Vec<serde_json::Map<String, Value>>>,
    /// Path to a JSONL file of reference rows (one JSON object per line).
    /// Mutually exclusive with `values`.
    #[serde(default)]
    jsonl: Option<String>,
    /// Match keys: `{ record: <record field>, ref: <reference field> }`.
    on: LookupOnConfig,
    /// Output columns to add, as `{ <output column>: <reference column> }`.
    add: HashMap<String, String>,
    /// Behaviour on a miss: `null` (default), `keep`, or `error`.
    #[serde(default)]
    on_missing: LookupOnMissing,
}

/// Resolve a `lookup` reference set from inline `values` or a `jsonl` file
/// (exactly one must be set).
#[cfg(feature = "transforms")]
fn load_lookup_reference(
    kind: &str,
    cfg: &LookupConfig,
) -> CliResult<Vec<serde_json::Map<String, Value>>> {
    match (&cfg.values, &cfg.jsonl) {
        (Some(_), Some(_)) => Err(CliError::InvalidTransform {
            name: kind.to_owned(),
            message: "set exactly one of `values` or `jsonl`, not both".to_owned(),
        }),
        (Some(rows), None) => Ok(rows.clone()),
        (None, Some(path)) => {
            let text = std::fs::read_to_string(path).map_err(|e| CliError::InvalidTransform {
                name: kind.to_owned(),
                message: format!("reading lookup jsonl '{path}': {e}"),
            })?;
            let mut rows = Vec::new();
            for (i, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let row: serde_json::Map<String, Value> =
                    serde_json::from_str(line).map_err(|e| CliError::InvalidTransform {
                        name: kind.to_owned(),
                        message: format!("lookup jsonl '{path}' line {}: {e}", i + 1),
                    })?;
                rows.push(row);
            }
            Ok(rows)
        }
        (None, None) => Err(CliError::InvalidTransform {
            name: kind.to_owned(),
            message: "a reference set is required: set `values` or `jsonl`".to_owned(),
        }),
    }
}

/// `transform_descriptions`, and `transform_schema` all read from this list
/// so adding a new transform means appending one entry (no parallel match
/// arms to keep in sync).
struct TransformDef {
    kind: &'static str,
    description: &'static str,
    schema_fn: fn() -> Value,
    compile_fn: fn(&str, Value) -> CliResult<TransformStage>,
}

/// Every transform compiled into this build, in display order.
///
/// Non-capturing closures coerce to the `fn` pointers held by `TransformDef`,
/// so each row stays a single self-contained record next to its sibling
/// entries.
fn registry() -> Vec<TransformDef> {
    #[allow(unused_mut)]
    let mut defs: Vec<TransformDef> = Vec::new();
    #[cfg(feature = "transforms")]
    {
        defs.extend(vec![
            TransformDef {
                kind: "flatten",
                description: "Flatten nested objects into a single level (configurable separator).",
                schema_fn: || schema::<FlattenConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FlattenConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Flatten {
                        separator: cfg.separator,
                    }))
                },
            },
            TransformDef {
                kind: "rename_keys",
                description: "Rewrite every key via a regex pattern + replacement.",
                schema_fn: || schema::<RenameKeysConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<RenameKeysConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::RenameKeys {
                        pattern: cfg.pattern,
                        replacement: cfg.replacement,
                    }))
                },
            },
            TransformDef {
                kind: "keys_case",
                description: "Re-case every key (snake / camel / pascal / kebab / screaming_snake).",
                schema_fn: || schema::<KeysCaseConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<KeysCaseConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::KeysCase {
                        mode: cfg.mode,
                        on_collision: cfg.on_collision,
                    }))
                },
            },
            TransformDef {
                kind: "select",
                description: "Keep only the listed top-level fields; drop the rest.",
                schema_fn: || schema::<FieldsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FieldsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Select {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "drop",
                description: "Remove the listed top-level fields.",
                schema_fn: || schema::<FieldsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FieldsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Drop {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "set",
                description: "Set named fields to constant values on every record.",
                schema_fn: || schema::<SetConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<SetConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Set {
                        values: cfg.values,
                    }))
                },
            },
            TransformDef {
                kind: "rename_field",
                description: "Rename specific top-level fields by name.",
                schema_fn: || schema::<RenameFieldConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<RenameFieldConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::RenameField {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "cast",
                description: "Coerce named fields to int / float / bool / string / timestamp.",
                schema_fn: || schema::<CastConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<CastConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Cast {
                        fields: cfg.fields,
                        on_error: cfg.on_error,
                    }))
                },
            },
            TransformDef {
                kind: "redact",
                description: "Overwrite the listed fields with a mask value (default `***`).",
                schema_fn: || schema::<RedactConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<RedactConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Redact {
                        fields: cfg.fields,
                        mask: cfg.mask,
                    }))
                },
            },
            TransformDef {
                kind: "value_case",
                description: "Lowercase, uppercase, or trim the value of named string fields.",
                schema_fn: || schema::<ValueCaseConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<ValueCaseConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::ValueCase {
                        fields: cfg.fields,
                        mode: cfg.mode,
                    }))
                },
            },
            TransformDef {
                kind: "spell_symbols",
                description: "Replace punctuation/symbols in string values with their spelled-out words.",
                schema_fn: || schema::<SpellSymbolsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<SpellSymbolsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::SpellSymbols {
                        extra: cfg.extra,
                        separator: cfg.separator,
                    }))
                },
            },
            TransformDef {
                kind: "hash",
                description: "Hash listed fields (SHA-256 / BLAKE3) into stable, join-able tokens.",
                schema_fn: || schema::<HashConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<HashConfig>(kind, config)?;
                    let stage = TransformStage::Map(RecordTransform::Hash {
                        fields: cfg.fields,
                        algorithm: cfg.algorithm,
                        encoding: cfg.encoding,
                        salt: cfg.salt,
                        into: cfg.into,
                    });
                    validate_stage(kind, &stage)?;
                    Ok(stage)
                },
            },
            TransformDef {
                kind: "json_parse",
                description: "Parse a stringified-JSON field into a real nested JSON value.",
                schema_fn: || schema::<JsonParseConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<JsonParseConfig>(kind, config)?;
                    let stage = TransformStage::Map(RecordTransform::JsonParse {
                        fields: cfg.fields,
                        on_error: cfg.on_error,
                        into: cfg.into,
                    });
                    validate_stage(kind, &stage)?;
                    Ok(stage)
                },
            },
            TransformDef {
                kind: "coalesce",
                description: "Fill a missing/null field from a default or first non-null fallback key.",
                schema_fn: || schema::<CoalesceConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<CoalesceConfig>(kind, config)?;
                    let stage = TransformStage::Map(RecordTransform::Coalesce {
                        field: cfg.field,
                        default: cfg.default,
                        from: cfg.from,
                        treat_empty_string_as_null: cfg.treat_empty_string_as_null,
                    });
                    validate_stage(kind, &stage)?;
                    Ok(stage)
                },
            },
            TransformDef {
                kind: "split",
                description: "Split a string field into an array on a delimiter.",
                schema_fn: || schema::<SplitConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<SplitConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Split {
                        field: cfg.field,
                        delimiter: cfg.delimiter,
                        trim: cfg.trim,
                        into: cfg.into,
                    }))
                },
            },
            TransformDef {
                kind: "join",
                description: "Join an array field into a string with a delimiter.",
                schema_fn: || schema::<JoinConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<JoinConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Join {
                        field: cfg.field,
                        delimiter: cfg.delimiter,
                        into: cfg.into,
                    }))
                },
            },
            TransformDef {
                kind: "json_encode",
                description: "Serialize a nested field to a JSON string (inverse of json_parse).",
                schema_fn: || schema::<FieldsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FieldsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::JsonEncode {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "unpivot",
                description: "Reshape wide columns or a map field into long key/value rows.",
                schema_fn: || schema::<UnpivotSpec>(),
                compile_fn: |kind, config| {
                    // `unpivot` is 1→N, so it compiles to an object-safe
                    // `TransformStage::Custom` (no dedicated enum variant —
                    // keeps `TransformStage` additive-only). `into_stage`
                    // validates the spec at load time.
                    let spec = decode::<UnpivotSpec>(kind, config)?;
                    spec.into_stage().map_err(|e| CliError::InvalidTransform {
                        name: kind.to_owned(),
                        message: e.to_string(),
                    })
                },
            },
            TransformDef {
                kind: "tree_flatten",
                description: "Flatten a recursive report tree / matrix (nested Rows) into one row per leaf.",
                schema_fn: || schema::<TreeFlattenSpec>(),
                compile_fn: |kind, config| {
                    // `tree_flatten` is 1→N, so it compiles to an object-safe
                    // `TransformStage::Custom` (no dedicated enum variant).
                    // `into_stage` validates the spec at load time.
                    let spec = decode::<TreeFlattenSpec>(kind, config)?;
                    spec.into_stage().map_err(|e| CliError::InvalidTransform {
                        name: kind.to_owned(),
                        message: e.to_string(),
                    })
                },
            },
            TransformDef {
                kind: "cross_join",
                description: "Expand a record into the cartesian product of two or more of its sibling array fields.",
                schema_fn: || schema::<CrossJoinSpec>(),
                compile_fn: |kind, config| {
                    // `cross_join` is 1→N and fails loudly on product overflow,
                    // so it compiles to a fallible `TransformStage::PageFn`.
                    // `into_stage` validates the spec at load time.
                    let spec = decode::<CrossJoinSpec>(kind, config)?;
                    spec.into_stage().map_err(|e| CliError::InvalidTransform {
                        name: kind.to_owned(),
                        message: e.to_string(),
                    })
                },
            },
            #[cfg(feature = "transform-zip-columns")]
            TransformDef {
                kind: "zip_columns",
                description: "Zip a columnar payload ({columns, rows}) into one object per row.",
                schema_fn: || schema::<ZipColumnsSpec>(),
                compile_fn: |kind, config| {
                    // `zip_columns` is 1→N and fails loudly on a row/column
                    // width mismatch, so it compiles to a fallible
                    // `TransformStage::PageFn`. `into_stage` validates the spec.
                    let spec = decode::<ZipColumnsSpec>(kind, config)?;
                    spec.into_stage().map_err(|e| CliError::InvalidTransform {
                        name: kind.to_owned(),
                        message: e.to_string(),
                    })
                },
            },
            TransformDef {
                kind: "lookup",
                description: "Enrich records by joining against an inline/JSONL reference table.",
                schema_fn: || schema::<LookupConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<LookupConfig>(kind, config)?;
                    let reference = load_lookup_reference(kind, &cfg)?;
                    let add: Vec<(String, String)> = cfg.add.into_iter().collect();
                    let stage = TransformStage::Map(RecordTransform::Lookup {
                        reference,
                        on_record: cfg.on.record,
                        on_ref: cfg.on.ref_field,
                        add,
                        on_missing: cfg.on_missing,
                    });
                    validate_stage(kind, &stage)?;
                    Ok(stage)
                },
            },
            #[cfg(feature = "transform-filter")]
            TransformDef {
                kind: "filter",
                description: "Keep records where a JSONPath predicate is true.",
                schema_fn: || schema::<FilterConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FilterConfig>(kind, config)?;
                    // Re-use stage's compile-time validation so error messages match.
                    let stage = TransformStage::Filter(faucet_core::FilterSpec {
                        path: cfg.path,
                        op: cfg.op,
                        value: cfg.value,
                    });
                    faucet_core::compile_stage(&stage).map_err(|e| match e {
                        faucet_core::FaucetError::Transform(msg) => CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message: msg,
                        },
                        other => CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message: format!("{other}"),
                        },
                    })?;
                    Ok(stage)
                },
            },
            #[cfg(feature = "transform-explode")]
            TransformDef {
                kind: "explode",
                description: "Expand an array field into one record per element.",
                schema_fn: || schema::<ExplodeConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<ExplodeConfig>(kind, config)?;
                    let carry = cfg.carry.clone();
                    let stage = TransformStage::Explode(faucet_core::ExplodeSpec {
                        path: cfg.path,
                        prefix: cfg.prefix,
                        separator: cfg.separator,
                        on_missing: cfg.on_missing,
                    });
                    let compiled =
                        faucet_core::compile_stage(&stage).map_err(|e| match e {
                            faucet_core::FaucetError::Transform(msg) => {
                                CliError::InvalidTransform {
                                    name: kind.to_owned(),
                                    message: msg,
                                }
                            }
                            other => CliError::InvalidTransform {
                                name: kind.to_owned(),
                                message: format!("{other}"),
                            },
                        })?;
                    if carry.is_empty() {
                        return Ok(stage);
                    }
                    // #555: carry named parent fields onto every child. Reuse the
                    // core explode (`apply_stages`) then inject the carried
                    // values, resolved from the ORIGINAL parent record. A
                    // page-level fallible stage so an explode error still
                    // propagates.
                    let carry: Vec<(String, String)> = carry.into_iter().collect();
                    Ok(TransformStage::PageFn(std::sync::Arc::new(
                        move |page: Vec<Value>| {
                            let mut out = Vec::with_capacity(page.len());
                            for rec in page {
                                let carried: Vec<(String, Value)> = carry
                                    .iter()
                                    .filter_map(|(dest, src)| {
                                        get_dot_path(&rec, src).map(|v| (dest.clone(), v))
                                    })
                                    .collect();
                                let children = faucet_core::apply_stages(
                                    rec,
                                    std::slice::from_ref(&compiled),
                                )?;
                                for mut child in children {
                                    if let Value::Object(map) = &mut child {
                                        for (dest, val) in &carried {
                                            map.insert(dest.clone(), val.clone());
                                        }
                                    }
                                    out.push(child);
                                }
                            }
                            Ok(out)
                        },
                    )))
                },
            },
        ]);
    }
    #[cfg(feature = "transform-sql")]
    {
        defs.push(TransformDef {
            kind: "sql",
            description: "Run DuckDB SQL over the whole page; records are the `batch` relation.",
            schema_fn: || schema_sql(),
            compile_fn: |kind, config| {
                let cfg: faucet_transform_sql::SqlTransformConfig = decode_sql(kind, config)?;
                let transform = faucet_transform_sql::SqlTransform::compile(&cfg).map_err(|e| {
                    let message = match &e {
                        faucet_core::FaucetError::Transform(m)
                        | faucet_core::FaucetError::Config(m) => m.clone(),
                        other => format!("{other}"),
                    };
                    CliError::InvalidTransform {
                        name: kind.to_owned(),
                        message,
                    }
                })?;
                Ok(transform.into_page_stage())
            },
        });
    }
    #[cfg(feature = "transform-wasm")]
    {
        defs.push(TransformDef {
            kind: "wasm",
            description: "Run a user-provided sandboxed .wasm module over each record (wasmtime).",
            schema_fn: || schema_wasm(),
            compile_fn: |kind, config| {
                let cfg: faucet_transform_wasm::WasmTransformConfig = decode_wasm(kind, config)?;
                let transform =
                    faucet_transform_wasm::WasmTransform::compile(&cfg).map_err(|e| {
                        let message = match &e {
                            faucet_core::FaucetError::Transform(m)
                            | faucet_core::FaucetError::Config(m) => m.clone(),
                            other => format!("{other}"),
                        };
                        CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message,
                        }
                    })?;
                Ok(transform.into_page_stage())
            },
        });
    }
    #[cfg(feature = "transform-cdc-unwrap")]
    {
        defs.push(TransformDef {
            kind: "cdc_unwrap",
            description: "Normalize a CDC envelope into a flat row + delete marker (for upsert sinks).",
            schema_fn: || schema::<CdcUnwrapConfig>(),
            compile_fn: |kind, config| {
                let cfg = decode::<CdcUnwrapConfig>(kind, config)?;
                Ok(faucet_core::TransformStage::CdcUnwrap(faucet_core::CdcUnwrapSpec {
                    op_field: cfg.op_field,
                    after_field: cfg.after_field,
                    before_field: cfg.before_field,
                    key_field: cfg.key_field,
                    marker_field: cfg.marker_field,
                    delete_ops: cfg.delete_ops,
                    drop_ops: cfg.drop_ops,
                }))
            },
        });
    }
    defs
}

/// Compile a list of [`TransformSpec`]s into [`TransformStage`]s in the
/// declared order. Most built-ins compile to a [`TransformStage::Map`];
/// richer stages (e.g. `filter`, future fan-outs) compile to other
/// variants. Unknown or malformed entries surface as a `CliError`.
pub fn compile_transforms(specs: &[TransformSpec]) -> CliResult<Vec<TransformStage>> {
    let mut out = Vec::with_capacity(specs.len());
    for s in specs {
        out.push(compile_one(s)?);
    }
    Ok(out)
}

fn compile_one(spec: &TransformSpec) -> CliResult<TransformStage> {
    match registry().into_iter().find(|t| t.kind == spec.kind) {
        Some(def) => (def.compile_fn)(&spec.kind, spec.config.clone()),
        None => Err(unknown_transform(&spec.kind)),
    }
}

/// Like [`compile_transforms`] but also returns each stage's Arrow
/// `RecordBatch` form (parallel to the stages), so the executor can build a
/// columnar-capable `TransformingSource` (#375). Only the `sql` transform
/// supplies a batch form today; every other stage's entry is `None`, which
/// keeps the whole chain on the `Value` path unless every stage is columnar.
#[cfg(feature = "arrow")]
pub fn compile_transforms_columnar(
    specs: &[TransformSpec],
) -> CliResult<(
    Vec<TransformStage>,
    Vec<Option<faucet_core::stage::PageFnBatchBox>>,
)> {
    let mut stages = Vec::with_capacity(specs.len());
    let mut batches = Vec::with_capacity(specs.len());
    for s in specs {
        #[cfg(feature = "transform-sql")]
        if s.kind == "sql" {
            let cfg = decode_sql("sql", s.config.clone())?;
            let transform = faucet_transform_sql::SqlTransform::compile(&cfg).map_err(|e| {
                let message = match &e {
                    faucet_core::FaucetError::Transform(m)
                    | faucet_core::FaucetError::Config(m) => m.clone(),
                    other => format!("{other}"),
                };
                CliError::InvalidTransform {
                    name: "sql".to_owned(),
                    message,
                }
            })?;
            let (stage, batch) = transform.into_columnar_stage();
            stages.push(stage);
            batches.push(Some(batch));
            continue;
        }
        stages.push(compile_one(s)?);
        batches.push(None);
    }
    Ok((stages, batches))
}

/// One-line summary of every transform compiled into this build. Used by
/// `faucet list`.
pub fn transform_descriptions() -> Vec<(&'static str, &'static str)> {
    registry()
        .into_iter()
        .map(|t| (t.kind, t.description))
        .collect()
}

/// Names of every transform compiled into this build.
pub fn available_transforms() -> Vec<&'static str> {
    registry().into_iter().map(|t| t.kind).collect()
}

// Keep in sync with faucet_core::{RecordCheck, BatchCheck} — one entry per check variant.
/// One-line descriptions of the available quality checks, for `faucet list`.
/// The `json_schema` entry only appears when the `quality-jsonschema` feature
/// is enabled, mirroring `faucet schema quality` so `list` and `schema` agree.
#[cfg(feature = "quality")]
pub fn quality_descriptions() -> Vec<(&'static str, &'static str)> {
    let mut checks = vec![
        ("not_null", "field present and non-null"),
        ("not_empty", "string non-empty after trim"),
        ("regex_match", "string matches a regex"),
        ("value_in_set", "value is in an allowed set"),
        ("not_in_set", "value is not in a forbidden set"),
        ("compare", "numeric/scalar comparison (gt/gte/lt/lte/eq/ne)"),
        ("type_is", "value is of an expected JSON type"),
        ("string_length", "string length within [min,max]"),
    ];
    #[cfg(feature = "quality-jsonschema")]
    checks.push((
        "json_schema",
        "record validates against a JSON Schema (feature-gated)",
    ));
    checks.extend([
        ("row_count", "batch row count within [min,max]"),
        ("null_rate", "batch null rate of a field <= max"),
        ("unique", "composite key unique within the batch"),
        (
            "distinct_count",
            "distinct values of a field within [min,max]",
        ),
    ]);
    checks
}

/// Return the JSON Schema for the named transform's config. Mirrors
/// `registry::source_schema` / `sink_schema` so `faucet schema transform <name>`
/// reads symmetrically with the connector variants.
pub fn transform_schema(name: &str) -> CliResult<Value> {
    registry()
        .into_iter()
        .find(|t| t.kind == name)
        .map(|t| (t.schema_fn)())
        .ok_or_else(|| unknown_transform(name))
}

fn unknown_transform(name: &str) -> CliError {
    let available = available_transforms();
    CliError::UnknownTransform {
        name: name.to_owned(),
        available: if available.is_empty() {
            "(none — rebuild faucet-cli with the `transforms` feature enabled)".to_owned()
        } else {
            available.join(", ")
        },
    }
}

#[cfg(any(feature = "transforms", feature = "transform-cdc-unwrap"))]
fn schema<T: JsonSchema>() -> Value {
    serde_json::to_value(schema_for!(T)).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

#[cfg(any(feature = "transforms", feature = "transform-cdc-unwrap"))]
fn decode<T: serde::de::DeserializeOwned>(name: &str, config: Value) -> CliResult<T> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidTransform {
        name: name.to_owned(),
        message: e.to_string(),
    })
}

/// Compile-validate a `Map` stage so its config errors (empty `fields`, `into`
/// with multiple fields, `coalesce` requiring exactly one source) surface at
/// load time as `CliError::InvalidTransform` — mirroring how `filter` /
/// `explode` validate their specs at compile time.
#[cfg(feature = "transforms")]
fn validate_stage(kind: &str, stage: &TransformStage) -> CliResult<()> {
    faucet_core::compile_stage(stage).map(|_| ()).map_err(|e| {
        let message = match e {
            faucet_core::FaucetError::Transform(m) | faucet_core::FaucetError::Config(m) => m,
            other => format!("{other}"),
        };
        CliError::InvalidTransform {
            name: kind.to_owned(),
            message,
        }
    })
}

#[cfg(feature = "transform-sql")]
fn schema_sql() -> Value {
    serde_json::to_value(faucet_core::schema_for!(
        faucet_transform_sql::SqlTransformConfig
    ))
    .unwrap_or(Value::Null)
}

#[cfg(feature = "transform-sql")]
fn decode_sql(kind: &str, config: Value) -> CliResult<faucet_transform_sql::SqlTransformConfig> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidTransform {
        name: kind.to_owned(),
        message: e.to_string(),
    })
}

#[cfg(feature = "transform-wasm")]
fn schema_wasm() -> Value {
    serde_json::to_value(faucet_core::schema_for!(
        faucet_transform_wasm::WasmTransformConfig
    ))
    .unwrap_or(Value::Null)
}

#[cfg(feature = "transform-wasm")]
fn decode_wasm(kind: &str, config: Value) -> CliResult<faucet_transform_wasm::WasmTransformConfig> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidTransform {
        name: kind.to_owned(),
        message: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_list_compiles_to_empty() {
        let out = compile_transforms(&[]).unwrap();
        assert!(out.is_empty());
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_keys_case_and_flatten() {
        let specs = vec![
            TransformSpec {
                kind: "keys_case".into(),
                config: json!({"mode": "snake"}),
            },
            TransformSpec {
                kind: "flatten".into(),
                config: json!({"separator": "."}),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn keys_case_rejects_unknown_mode() {
        let specs = vec![TransformSpec {
            kind: "keys_case".into(),
            config: json!({"mode": "spongebob"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "keys_case"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn keys_case_requires_mode() {
        let specs = vec![TransformSpec {
            kind: "keys_case".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "keys_case"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn snake_case_kind_is_no_longer_recognized() {
        // Removed in favour of `keys_case { mode: snake }`.
        let specs = vec![TransformSpec {
            kind: "snake_case".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::UnknownTransform { name, .. } => assert_eq!(name, "snake_case"),
            other => panic!("expected UnknownTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn rename_keys_requires_pattern_and_replacement() {
        let specs = vec![TransformSpec {
            kind: "rename_keys".into(),
            config: json!({"pattern": "^_"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "rename_keys"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[test]
    fn unknown_transform_errors() {
        let specs = vec![TransformSpec {
            kind: "make_uppercase".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::UnknownTransform { name, .. } => assert_eq!(name, "make_uppercase"),
            other => panic!("expected UnknownTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_select_and_drop() {
        let specs = vec![
            TransformSpec {
                kind: "select".into(),
                config: json!({"fields": ["id", "name"]}),
            },
            TransformSpec {
                kind: "drop".into(),
                config: json!({"fields": ["secret"]}),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_set_with_object_values() {
        let specs = vec![TransformSpec {
            kind: "set".into(),
            config: json!({"values": {"_source": "api", "version": 1}}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_rename_field() {
        let specs = vec![TransformSpec {
            kind: "rename_field".into(),
            config: json!({"fields": {"old": "new"}}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_cast_with_default_on_error() {
        let specs = vec![TransformSpec {
            kind: "cast".into(),
            config: json!({"fields": {"age": "int", "price": "float"}}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn cast_rejects_unknown_target_type() {
        let specs = vec![TransformSpec {
            kind: "cast".into(),
            config: json!({"fields": {"x": "uuid"}}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "cast"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn cast_rejects_unknown_on_error_mode() {
        let specs = vec![TransformSpec {
            kind: "cast".into(),
            config: json!({"fields": {"x": "int"}, "on_error": "explode"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "cast"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn redact_uses_default_mask_when_omitted() {
        let specs = vec![TransformSpec {
            kind: "redact".into(),
            config: json!({"fields": ["ssn"]}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_hash_with_defaults_and_options() {
        let specs = vec![
            TransformSpec {
                kind: "hash".into(),
                config: json!({"fields": ["email"]}),
            },
            TransformSpec {
                kind: "hash".into(),
                config: json!({
                    "fields": ["user_id"],
                    "algorithm": "blake3",
                    "encoding": "base64",
                    "salt": "pepper",
                    "into": "user_id_hash"
                }),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_reshape_transforms() {
        let specs = vec![
            TransformSpec {
                kind: "json_encode".into(),
                config: json!({"fields": ["addr", "line_items"]}),
            },
            TransformSpec {
                kind: "unpivot".into(),
                config: json!({"id_fields": ["id"], "key_name": "month", "value_name": "amount"}),
            },
            TransformSpec {
                kind: "lookup".into(),
                config: json!({
                    "values": [{"id": 1, "name": "Alice"}],
                    "on": {"record": "user_id", "ref": "id"},
                    "add": {"user_name": "name"}
                }),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 3);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_cross_join() {
        let specs = vec![TransformSpec {
            kind: "cross_join".into(),
            config: json!({"arrays": ["jobs", "compensation"], "prefix": true}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], TransformStage::PageFn(_)));
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn cross_join_single_array_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "cross_join".into(),
            config: json!({"arrays": ["only"]}),
        }];
        assert!(compile_transforms(&specs).is_err());
    }

    #[cfg(feature = "transform-zip-columns")]
    #[test]
    fn zip_columns_compiles_and_zips() {
        let specs = vec![TransformSpec {
            kind: "zip_columns".into(),
            config: json!({"columns_path": "columns[*].name", "rows_path": "rows"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        let TransformStage::PageFn(f) = &out[0] else {
            panic!("expected PageFn");
        };
        let page = vec![json!({
            "columns": [{"name": "day"}, {"name": "n"}],
            "rows": [["2026-01-01", 3], ["2026-01-02", 5]],
        })];
        let got = f(page).unwrap();
        assert_eq!(
            got,
            vec![
                json!({"day": "2026-01-01", "n": 3}),
                json!({"day": "2026-01-02", "n": 5}),
            ]
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_carry_copies_parent_field_onto_children() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({
                "path": "values",
                "prefix": "",
                "carry": {"employee_id": "id"},
            }),
        }];
        let out = compile_transforms(&specs).unwrap();
        // With `carry`, explode compiles to a page-level stage.
        let TransformStage::PageFn(f) = &out[0] else {
            panic!("expected PageFn for explode+carry");
        };
        let page = vec![json!({"id": 7, "values": [{"v": "a"}, {"v": "b"}]})];
        let got = f(page).unwrap();
        assert_eq!(got.len(), 2);
        // Each child keeps the exploded element AND the carried parent id.
        assert_eq!(got[0]["v"], json!("a"));
        assert_eq!(got[0]["employee_id"], json!(7));
        assert_eq!(got[1]["v"], json!("b"));
        assert_eq!(got[1]["employee_id"], json!(7));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_without_carry_stays_a_plain_explode_stage() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "values"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert!(matches!(out[0], TransformStage::Explode(_)));
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn unpivot_empty_key_name_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "unpivot".into(),
            config: json!({"key_name": "", "value_name": "v"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        assert!(
            matches!(&err, CliError::InvalidTransform { name, .. } if name == "unpivot"),
            "got {err:?}"
        );
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn lookup_requires_a_reference_and_add() {
        // no `values`/`jsonl`
        let no_ref = vec![TransformSpec {
            kind: "lookup".into(),
            config: json!({"on": {"record": "k", "ref": "k"}, "add": {"x": "y"}}),
        }];
        assert!(matches!(
            compile_transforms(&no_ref).unwrap_err(),
            CliError::InvalidTransform { .. }
        ));
        // empty `add`
        let no_add = vec![TransformSpec {
            kind: "lookup".into(),
            config: json!({"values": [], "on": {"record": "k", "ref": "k"}, "add": {}}),
        }];
        assert!(matches!(
            compile_transforms(&no_add).unwrap_err(),
            CliError::InvalidTransform { .. }
        ));
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn lookup_reads_a_jsonl_reference_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ref.jsonl");
        std::fs::write(
            &path,
            "{\"id\":\"1\",\"name\":\"Alice\"}\n\n{\"id\":\"2\",\"name\":\"Bob\"}\n",
        )
        .unwrap();
        let specs = vec![TransformSpec {
            kind: "lookup".into(),
            config: json!({
                "jsonl": path.to_str().unwrap(),
                "on": {"record": "uid", "ref": "id"},
                "add": {"uname": "name"}
            }),
        }];
        assert_eq!(compile_transforms(&specs).unwrap().len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn lookup_rejects_both_values_and_jsonl() {
        let specs = vec![TransformSpec {
            kind: "lookup".into(),
            config: json!({
                "values": [{"id": "1"}],
                "jsonl": "ref.jsonl",
                "on": {"record": "uid", "ref": "id"},
                "add": {"uname": "name"}
            }),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        assert!(
            matches!(&err, CliError::InvalidTransform { name, message }
                if name == "lookup" && message.contains("exactly one")),
            "got {err:?}"
        );
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn lookup_jsonl_missing_file_is_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "lookup".into(),
            config: json!({
                "jsonl": "/nonexistent/faucet-lookup-ref.jsonl",
                "on": {"record": "uid", "ref": "id"},
                "add": {"uname": "name"}
            }),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        assert!(
            matches!(&err, CliError::InvalidTransform { name, message }
                if name == "lookup" && message.contains("reading lookup jsonl")),
            "got {err:?}"
        );
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn lookup_jsonl_malformed_line_is_rejected_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.jsonl");
        std::fs::write(&path, "{\"id\":\"1\"}\nnot json\n").unwrap();
        let specs = vec![TransformSpec {
            kind: "lookup".into(),
            config: json!({
                "jsonl": path.to_str().unwrap(),
                "on": {"record": "uid", "ref": "id"},
                "add": {"uname": "name"}
            }),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        assert!(
            matches!(&err, CliError::InvalidTransform { name, message }
                if name == "lookup" && message.contains("line 2")),
            "got {err:?}"
        );
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn reshape_transforms_have_schema_and_descriptions() {
        for k in ["json_encode", "unpivot", "lookup"] {
            assert!(transform_schema(k).is_ok(), "schema for {k}");
            assert!(
                transform_descriptions().iter().any(|(n, _)| *n == k),
                "description for {k}"
            );
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn hash_empty_fields_is_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "hash".into(),
            config: json!({"fields": []}),
        }];
        match compile_transforms(&specs).unwrap_err() {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "hash"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn hash_into_with_multiple_fields_is_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "hash".into(),
            config: json!({"fields": ["a", "b"], "into": "x"}),
        }];
        match compile_transforms(&specs).unwrap_err() {
            CliError::InvalidTransform { name, message } => {
                assert_eq!(name, "hash");
                assert!(message.contains("into"), "{message}");
            }
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_json_parse_with_on_error() {
        let specs = vec![TransformSpec {
            kind: "json_parse".into(),
            config: json!({"fields": ["payload"], "on_error": "null", "into": "parsed"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn json_parse_into_with_multiple_fields_is_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "json_parse".into(),
            config: json!({"fields": ["a", "b"], "into": "x"}),
        }];
        match compile_transforms(&specs).unwrap_err() {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "json_parse"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_coalesce_with_default() {
        let specs = vec![TransformSpec {
            kind: "coalesce".into(),
            config: json!({"field": "status", "default": "unknown"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_coalesce_with_from() {
        let specs = vec![TransformSpec {
            kind: "coalesce".into(),
            config: json!({"field": "status", "from": ["status", "state"]}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn coalesce_both_default_and_from_is_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "coalesce".into(),
            config: json!({"field": "s", "default": "x", "from": ["y"]}),
        }];
        match compile_transforms(&specs).unwrap_err() {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "coalesce"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn coalesce_neither_default_nor_from_is_rejected_at_load() {
        let specs = vec![TransformSpec {
            kind: "coalesce".into(),
            config: json!({"field": "s"}),
        }];
        match compile_transforms(&specs).unwrap_err() {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "coalesce"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_split_and_join() {
        let specs = vec![
            TransformSpec {
                kind: "split".into(),
                config: json!({"field": "tags", "delimiter": ",", "trim": true}),
            },
            TransformSpec {
                kind: "join".into(),
                config: json!({"field": "tags", "delimiter": ",", "into": "csv"}),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn value_case_requires_mode() {
        let specs = vec![TransformSpec {
            kind: "value_case".into(),
            config: json!({"fields": ["email"]}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "value_case"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn available_transforms_lists_every_kind() {
        let names = available_transforms();
        for expected in [
            "flatten",
            "rename_keys",
            "keys_case",
            "select",
            "drop",
            "set",
            "rename_field",
            "cast",
            "redact",
            "value_case",
            "spell_symbols",
            "hash",
            "json_parse",
            "coalesce",
            "split",
            "join",
            "filter",
            "explode",
        ] {
            assert!(names.contains(&expected), "missing {expected}");
        }
        assert!(
            !names.contains(&"snake_case"),
            "snake_case must be removed in favour of keys_case"
        );
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn transform_descriptions_covers_every_compiled_kind() {
        // descriptions and available_transforms must never drift — `faucet list`
        // and the `UnknownTransform` "Available:" line both read from this.
        let names = available_transforms();
        let desc_names: Vec<&'static str> = transform_descriptions()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, desc_names);
        for (_, desc) in transform_descriptions() {
            assert!(!desc.is_empty(), "every transform needs a description");
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn transform_schema_returns_object_for_every_kind() {
        for name in available_transforms() {
            let schema = transform_schema(name).unwrap_or_else(|e| {
                panic!("schema lookup failed for {name}: {e}");
            });
            assert!(schema.is_object(), "schema for {name} must be an object");
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn transform_schema_select_and_drop_share_shape() {
        // Both accept `{ fields: Vec<String> }` — the schema is the same object,
        // just titled `FieldsConfig`.
        let select = transform_schema("select").unwrap();
        let drop = transform_schema("drop").unwrap();
        assert_eq!(select, drop);
    }

    #[test]
    fn transform_schema_unknown_errors_with_available_list() {
        let err = transform_schema("make_uppercase").unwrap_err();
        match err {
            CliError::UnknownTransform { name, available } => {
                assert_eq!(name, "make_uppercase");
                #[cfg(feature = "transforms")]
                assert!(available.contains("flatten"), "{available}");
                #[cfg(not(feature = "transforms"))]
                assert!(available.contains("rebuild"), "{available}");
            }
            other => panic!("expected UnknownTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn compiles_filter_eq() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "status", "op": "eq", "value": "active"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], TransformStage::Filter(_)));
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_rejects_in_with_non_array_value() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "v", "op": "in", "value": "scalar"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, message } => {
                assert_eq!(name, "filter");
                assert!(message.contains("requires an array"), "{message}");
            }
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_rejects_exists_with_value() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "v", "op": "exists", "value": "x"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "filter"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_rejects_bad_path() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "$..items", "op": "exists"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "filter"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn compiles_explode_with_defaults() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "items"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], TransformStage::Explode(_)));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn compiles_explode_with_custom_prefix_and_on_missing() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({
                "path": "items",
                "prefix": "item",
                "separator": "_",
                "on_missing": "drop"
            }),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_rejects_bad_path() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "$..items"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "explode"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_rejects_invalid_on_missing() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "items", "on_missing": "explode_harder"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "explode"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-sql")]
    #[test]
    fn compiles_sql_to_page_fn() {
        let specs = vec![TransformSpec {
            kind: "sql".into(),
            config: json!({"query": "SELECT * FROM batch"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], faucet_core::TransformStage::PageFn(_)));
    }

    #[cfg(feature = "transform-sql")]
    #[test]
    fn sql_bad_query_is_invalid_transform() {
        let specs = vec![TransformSpec {
            kind: "sql".into(),
            config: json!({"query": "SELEKT bad"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "sql"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-sql")]
    #[test]
    fn sql_schema_and_listing_present() {
        assert!(transform_schema("sql").is_ok());
        assert!(available_transforms().contains(&"sql"));
    }

    #[cfg(feature = "transform-wasm")]
    #[test]
    fn wasm_schema_and_listing_present() {
        assert!(transform_schema("wasm").is_ok());
        assert!(available_transforms().contains(&"wasm"));
    }

    #[cfg(feature = "transform-wasm")]
    #[test]
    fn wasm_missing_module_field_is_invalid_transform() {
        let specs = vec![TransformSpec {
            kind: "wasm".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "wasm"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-wasm")]
    #[test]
    fn wasm_nonexistent_module_is_invalid_transform() {
        let specs = vec![TransformSpec {
            kind: "wasm".into(),
            config: json!({"module": "/no/such/path/mod.wasm"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, message } => {
                assert_eq!(name, "wasm");
                assert!(message.contains("cannot read module"), "{message}");
            }
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-cdc-unwrap")]
    #[test]
    fn compiles_cdc_unwrap_with_defaults() {
        let specs = vec![TransformSpec {
            kind: "cdc_unwrap".into(),
            config: json!({}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], faucet_core::TransformStage::CdcUnwrap(_)));
    }

    #[cfg(feature = "transform-cdc-unwrap")]
    #[test]
    fn cdc_unwrap_schema_and_listing_present() {
        assert!(transform_schema("cdc_unwrap").is_ok());
        assert!(available_transforms().contains(&"cdc_unwrap"));
    }

    #[cfg(feature = "quality")]
    #[test]
    fn quality_descriptions_has_one_entry_per_check() {
        // 8 always-on per-record checks + 4 per-batch checks = 12; the
        // `json_schema` per-record check is only present (→ 13) when the
        // `quality-jsonschema` feature is enabled, matching `faucet schema
        // quality`. If you add a RecordCheck/BatchCheck variant in faucet-core,
        // add its description here too.
        #[cfg(feature = "quality-jsonschema")]
        assert_eq!(quality_descriptions().len(), 13);
        #[cfg(not(feature = "quality-jsonschema"))]
        assert_eq!(quality_descriptions().len(), 12);
    }
}

//! Record transformation pipeline.
//!
//! ## Built-in transforms (optional Cargo features)
//!
//! | Variant | Feature flag | Default |
//! |---------|-------------|---------|
//! | [`RecordTransform::Flatten`] | `transform-flatten` | enabled |
//! | [`RecordTransform::RenameKeys`] | `transform-rename-keys` | enabled |
//! | [`RecordTransform::KeysCase`] | `transform-keys-case` | enabled |
//! | [`RecordTransform::Select`] | `transform-select` | off |
//! | [`RecordTransform::Drop`] | `transform-drop` | off |
//! | [`RecordTransform::Set`] | `transform-set` | off |
//! | [`RecordTransform::RenameField`] | `transform-rename-field` | off |
//! | [`RecordTransform::Cast`] | `transform-cast` | off |
//! | [`RecordTransform::Redact`] | `transform-redact` | off |
//! | [`RecordTransform::ValueCase`] | `transform-value-case` | off |
//! | [`RecordTransform::SpellSymbols`] | `transform-spell-symbols` | off |
//! | [`RecordTransform::Hash`] | `transform-hash` | off |
//! | [`RecordTransform::JsonParse`] | `transform-json-parse` | off |
//! | [`RecordTransform::Coalesce`] | `transform-coalesce` | off |
//! | [`RecordTransform::Split`] / [`RecordTransform::Join`] | `transform-split-join` | off |
//!
//! The `transforms` aggregate feature pulls in every variant above.
//!
//! Disable a transform (and its dependencies) by opting out of its feature:
//!
//! ```toml
//! [dependencies]
//! faucet-stream = { version = "*", default-features = false,
//!                   features = ["transform-flatten"] }
//! ```
//!
//! ## Stage-level transforms (filter / explode)
//!
//! `filter` and `explode` are not `RecordTransform` variants — they live as
//! [`crate::stage::TransformStage::Filter`] / `TransformStage::Explode` because
//! they may emit 0 or N records per input. Their feature flags are
//! `transform-filter` and `transform-explode`. See the `stage` module for
//! details.
//!
//! ## Custom transforms
//!
//! [`RecordTransform::Custom`] is always available regardless of features.
//! Pass any closure or function pointer via [`RecordTransform::custom`].

use crate::error::FaucetError;
#[cfg(any(
    feature = "transform-flatten",
    feature = "transform-rename-keys",
    feature = "transform-keys-case",
    feature = "transform-set",
))]
use serde_json::Map;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;

#[cfg(any(
    feature = "transform-cast",
    feature = "transform-rename-field",
    feature = "transform-value-case",
    feature = "transform-spell-symbols",
    feature = "transform-lookup",
))]
use std::collections::HashMap;

#[cfg(feature = "transform-rename-keys")]
use regex::Regex;

// ── Support enums for the new transforms ──────────────────────────────────────

/// Target type for [`RecordTransform::Cast`].
///
/// Coerces a JSON value to the requested concrete type.  `Timestamp` parses
/// RFC 3339 / ISO 8601 strings and normalises them back to RFC 3339 (so
/// `"2026-05-28T00:00:00Z"` round-trips unchanged but `"2026-05-28T00:00:00+00:00"`
/// becomes the canonical form).
#[cfg(feature = "transform-cast")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum CastType {
    /// 64-bit signed integer (`i64`).
    Int,
    /// 64-bit float (`f64`).
    Float,
    /// Boolean.  Accepts `true`/`false`/`1`/`0` (case-insensitive) when the
    /// source value is a string.
    Bool,
    /// String.  Numbers and booleans are stringified via `to_string()`.
    String,
    /// RFC 3339 timestamp, returned as a normalised RFC 3339 string.
    Timestamp,
}

/// Failure policy for [`RecordTransform::Cast`].  Default: `Error`.
#[cfg(feature = "transform-cast")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
    Default,
)]
#[serde(rename_all = "lowercase")]
pub enum CastOnError {
    /// Return [`FaucetError::Transform`] when a value cannot be cast.
    #[default]
    Error,
    /// Replace the un-castable value with [`Value::Null`].
    Null,
    /// Leave the un-castable value unchanged in the record.
    Skip,
}

/// Output convention for [`RecordTransform::KeysCase`].
///
/// The transform tokenises each key on whitespace, `_`, `-`, dropped
/// punctuation, and lower→upper transitions, then re-joins the tokens in
/// the requested style.
#[cfg(feature = "transform-keys-case")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
// Non-exhaustive so future output conventions can be added as a minor
// (additive) release rather than a breaking one.
#[non_exhaustive]
pub enum KeyCaseMode {
    /// `snake_case` — words separated by `_`, all lowercase.
    Snake,
    /// `camelCase` — first token lowercase, subsequent tokens capitalised,
    /// no separator.
    Camel,
    /// `PascalCase` — every token capitalised, no separator.
    Pascal,
    /// `kebab-case` — words separated by `-`, all lowercase.
    Kebab,
    /// `SCREAMING_SNAKE_CASE` — words separated by `_`, all uppercase.
    ScreamingSnake,
    /// `dot.case` — words separated by `.`, all lowercase. Useful for
    /// dotted-field backends (some search / metrics systems).
    Dot,
}

/// Policy for [`RecordTransform::KeysCase`] when two distinct source keys
/// re-case to the same name (common on wide, vendor-namespaced sources such
/// as Salesforce, e.g. `AccountId__c` + `Account_Id__c` → `account_id_c`).
#[cfg(feature = "transform-keys-case")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum KeyCollision {
    /// Fail the record with a `Transform` error naming the colliding key
    /// (the default — never silently drops a column).
    #[default]
    Error,
    /// Disambiguate colliding keys by appending `_2`, `_3`, … in original
    /// key order (first occurrence keeps the base name). Deterministic and
    /// order-stable, mirroring z3z1ma `target-bigquery`'s
    /// `add_underscore_when_invalid`.
    Suffix,
}

/// String-value casing mode for [`RecordTransform::ValueCase`].
#[cfg(feature = "transform-value-case")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
// Non-exhaustive so future casing modes can be added as a minor
// (additive) release rather than a breaking one.
#[non_exhaustive]
pub enum ValueCaseMode {
    /// Lowercase the value.
    Lower,
    /// Uppercase the value.
    Upper,
    /// Trim leading/trailing whitespace from the value.
    Trim,
    /// Title Case — upper-case the first letter of each whitespace-delimited
    /// word, lower-case the rest. ASCII/Unicode via `char::to_uppercase`.
    Title,
    /// Capitalize — upper-case only the first character of the whole string,
    /// lower-case the rest.
    Capitalize,
}

/// Hash algorithm for [`RecordTransform::Hash`].
#[cfg(feature = "transform-hash")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
    Default,
)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgorithm {
    /// SHA-256 (default).
    #[default]
    Sha256,
    /// BLAKE3.
    Blake3,
}

/// Output encoding for [`RecordTransform::Hash`] digests.
#[cfg(feature = "transform-hash")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
    Default,
)]
#[serde(rename_all = "lowercase")]
pub enum HashEncoding {
    /// Lowercase hexadecimal (default).
    #[default]
    Hex,
    /// Standard (padded) base64.
    Base64,
}

/// Failure policy for [`RecordTransform::JsonParse`]. Default: `Keep`.
///
/// A 1→1 record transform cannot drop a record, so there is no `skip_record`
/// policy — compose a downstream `filter` stage if you need to drop rows whose
/// JSON failed to parse.
#[cfg(feature = "transform-json-parse")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    schemars::JsonSchema,
    Default,
)]
#[serde(rename_all = "snake_case")]
pub enum JsonParseOnError {
    /// Leave the original (unparsed) string value unchanged.
    #[default]
    Keep,
    /// Replace the un-parseable value with [`Value::Null`].
    Null,
    /// Return [`FaucetError::Transform`].
    Error,
}

// ── Public config-facing type ─────────────────────────────────────────────────

/// A transformation applied to every record fetched by a source (e.g. the REST
/// source's `RestStream`).
///
/// Transforms are applied in the order they are added via the owning source's
/// configuration (e.g. `RestStreamConfig::add_transform`).
///
/// The three built-in variants are each guarded by a Cargo feature flag
/// (all enabled by default — see module-level docs).
/// [`RecordTransform::Custom`] is always available and accepts any closure.
///
/// Non-exhaustive: new built-in transforms are added as variants over time, so
/// this enum is marked `#[non_exhaustive]` — adding a transform is an additive
/// (minor) change, and downstream matches must include a wildcard arm.
#[non_exhaustive]
pub enum RecordTransform {
    /// Flatten nested JSON objects into a single-level map.
    ///
    /// Nested key paths are joined with `separator`.  Arrays are left as-is.
    ///
    /// _Requires feature `transform-flatten` (default)._
    ///
    /// # Example
    ///
    /// ```text
    /// {"user": {"id": 1, "addr": {"city": "NYC"}}}  →  (separator = "__")
    /// {"user__id": 1, "user__addr__city": "NYC"}
    /// ```
    #[cfg(feature = "transform-flatten")]
    Flatten { separator: String },

    /// Apply a single regex substitution to every key in the record.
    ///
    /// Keys in nested objects and objects inside arrays are also renamed
    /// recursively.  `pattern` is a Rust regex; `replacement` may reference
    /// capture groups with `$1`, `${name}`, etc.  Chain multiple `RenameKeys`
    /// transforms for multi-step pipelines.
    ///
    /// _Requires feature `transform-rename-keys` (default)._
    ///
    /// # Example
    ///
    /// ```text
    /// pattern = r"^_sdc_", replacement = ""   →   strip "_sdc_" prefix
    /// ```
    #[cfg(feature = "transform-rename-keys")]
    RenameKeys {
        pattern: String,
        replacement: String,
    },

    /// Re-case every key in the record according to `mode`.
    ///
    /// Tokenises each key on whitespace, `_`, `-`, dropped punctuation, and
    /// lower→upper transitions, then re-joins in the requested convention.
    /// Walks recursively into nested objects and arrays.  Two distinct keys
    /// that re-case to the same name error rather than silently overwriting.
    ///
    /// _Requires feature `transform-keys-case` (default)._
    ///
    /// | Input          | `Snake`        | `Camel`       | `Pascal`     | `Kebab`        | `ScreamingSnake` |
    /// |----------------|----------------|---------------|--------------|----------------|------------------|
    /// | `"First Name"` | `"first_name"` | `"firstName"` | `"FirstName"`| `"first-name"` | `"FIRST_NAME"`   |
    /// | `"last-name"`  | `"last_name"`  | `"lastName"`  | `"LastName"` | `"last-name"`  | `"LAST_NAME"`    |
    /// | `"camelCase"`  | `"camel_case"` | `"camelCase"` | `"CamelCase"`| `"camel-case"` | `"CAMEL_CASE"`   |
    #[cfg(feature = "transform-keys-case")]
    KeysCase {
        /// Output convention for every key.
        mode: KeyCaseMode,
        /// What to do when two distinct keys re-case to the same name.
        on_collision: KeyCollision,
    },

    /// Keep only the listed top-level fields on each record; remove the rest.
    ///
    /// Missing fields are silently skipped (they don't introduce `null`s).
    /// Non-object records pass through unchanged.
    ///
    /// _Requires feature `transform-select`._
    #[cfg(feature = "transform-select")]
    Select { fields: Vec<String> },

    /// Remove the listed top-level fields from each record.
    ///
    /// Missing fields are silently skipped. Non-object records pass through.
    ///
    /// _Requires feature `transform-drop`._
    #[cfg(feature = "transform-drop")]
    Drop { fields: Vec<String> },

    /// Insert or overwrite top-level fields on each record with constant values.
    ///
    /// Existing fields with the same name are overwritten. Non-object records
    /// pass through unchanged.
    ///
    /// _Requires feature `transform-set`._
    #[cfg(feature = "transform-set")]
    Set { values: Map<String, Value> },

    /// Exact-name rename of one or more top-level fields.
    ///
    /// Unlike [`RecordTransform::RenameKeys`] (regex, recursive), this only
    /// touches exact top-level keys. Missing source fields are silently skipped.
    /// If a target name already exists on the record, the rename errors rather
    /// than silently overwriting.
    ///
    /// _Requires feature `transform-rename-field`._
    #[cfg(feature = "transform-rename-field")]
    RenameField {
        /// Map of `old_name -> new_name`.
        fields: HashMap<String, String>,
    },

    /// Coerce per-field types on each record.
    ///
    /// Each named field is converted to the matching [`CastType`]. The
    /// [`CastOnError`] policy controls failure behaviour. Missing fields are
    /// silently skipped (no `null`s introduced).
    ///
    /// _Requires feature `transform-cast`._
    #[cfg(feature = "transform-cast")]
    Cast {
        fields: HashMap<String, CastType>,
        on_error: CastOnError,
    },

    /// Replace each listed field's value with a constant mask.
    ///
    /// Missing fields are silently skipped (no mask inserted). Default mask is
    /// `"***"` when constructed from CLI config.
    ///
    /// _Requires feature `transform-redact`._
    #[cfg(feature = "transform-redact")]
    Redact { fields: Vec<String>, mask: Value },

    /// Lowercase / uppercase / trim string values on listed fields.
    ///
    /// Non-string field values pass through unchanged. Missing fields are
    /// silently skipped.
    ///
    /// _Requires feature `transform-value-case`._
    #[cfg(feature = "transform-value-case")]
    ValueCase {
        fields: Vec<String>,
        mode: ValueCaseMode,
    },

    /// Recursively spell out symbols inside every key with their word
    /// equivalents (`%` → `percent`, `#` → `number`, `$` → `dollar`, …).
    ///
    /// Built-in defaults cover the common ASCII symbols (see
    /// [`default_symbol_map`]); `extra` adds or overrides entries.  Each
    /// replacement is surrounded by `separator` (default `" "`) so a chained
    /// [`RecordTransform::KeysCase`] picks up the word boundary.
    /// Keys are walked recursively into nested objects and arrays, mirroring
    /// the existing key-shape transforms.  Two distinct keys that collapse to
    /// the same name error rather than silently overwriting.
    ///
    /// _Requires feature `transform-spell-symbols`._
    ///
    /// # Example
    ///
    /// ```text
    /// {"% sold": 1, "C# courses": 2}
    ///   →  (defaults, separator=" ")
    /// {" percent  sold": 1, "C number  courses": 2}
    /// ```
    #[cfg(feature = "transform-spell-symbols")]
    SpellSymbols {
        /// Additional mappings (merged on top of [`default_symbol_map`];
        /// entries with the same `from` override the default).
        extra: HashMap<String, String>,
        /// Inserted around each replacement so word boundaries survive a
        /// downstream `keys_case` step. Default `" "`.
        separator: String,
    },

    /// Replace (or copy) each listed field's value with a cryptographic hash of
    /// that value — stable, join-able pseudonymization that preserves
    /// referential integrity (equal inputs → equal tokens).
    ///
    /// String values are hashed over their raw UTF-8 bytes; every other JSON
    /// value is hashed over its canonical serialization. An optional `salt` is
    /// prepended before hashing. Missing fields are silently skipped.
    ///
    /// When `into` is `Some`, exactly one field is allowed and the digest is
    /// written to `into` (the source field is left intact); when `None`, each
    /// field is replaced in place.
    ///
    /// _Requires feature `transform-hash`._
    #[cfg(feature = "transform-hash")]
    Hash {
        fields: Vec<String>,
        algorithm: HashAlgorithm,
        encoding: HashEncoding,
        salt: Option<String>,
        into: Option<String>,
    },

    /// Parse a stringified-JSON field into a real nested JSON value.
    ///
    /// Fields whose value is already an object/array (or any non-string) are
    /// left unchanged (idempotent). Missing fields are silently skipped. Parse
    /// failures are governed by [`JsonParseOnError`].
    ///
    /// When `into` is `Some`, exactly one field is allowed and the parsed value
    /// is written to `into`; when `None`, each field is replaced in place.
    ///
    /// _Requires feature `transform-json-parse`._
    #[cfg(feature = "transform-json-parse")]
    JsonParse {
        fields: Vec<String>,
        on_error: JsonParseOnError,
        into: Option<String>,
    },

    /// Fill a missing or null field with a default — either a literal value, or
    /// the first non-null value among a list of fallback keys.
    ///
    /// Exactly one of `default` / `from` must be set. The target is written
    /// only when it is absent or JSON `null` (or, when
    /// `treat_empty_string_as_null` is true, an empty string); a present,
    /// non-null target is left unchanged (idempotent).
    ///
    /// _Requires feature `transform-coalesce`._
    #[cfg(feature = "transform-coalesce")]
    Coalesce {
        field: String,
        /// Literal fallback value. Mutually exclusive with `from`.
        default: Option<Value>,
        /// Fallback keys; the first non-null wins. Mutually exclusive with
        /// `default`.
        from: Vec<String>,
        /// Treat an empty string as null for both the target and `from` keys.
        treat_empty_string_as_null: bool,
    },

    /// Split a string field into an array on `delimiter`.
    ///
    /// Non-string / absent fields are left unchanged. With `trim`, each element
    /// is whitespace-trimmed; empty segments are kept. When `into` is `Some`
    /// the array is written there (overwriting), else in place.
    ///
    /// _Requires feature `transform-split-join`._
    #[cfg(feature = "transform-split-join")]
    Split {
        field: String,
        delimiter: String,
        trim: bool,
        into: Option<String>,
    },

    /// Join an array field into a string with `delimiter`.
    ///
    /// Non-array / absent fields are left unchanged. Non-string elements are
    /// rendered via their JSON scalar form (strings without quotes, everything
    /// else as compact JSON). When `into` is `Some` the string is written
    /// there, else in place.
    ///
    /// _Requires feature `transform-split-join`._
    #[cfg(feature = "transform-split-join")]
    Join {
        field: String,
        delimiter: String,
        into: Option<String>,
    },

    /// Serialize a nested field to a JSON **string** (the inverse of
    /// [`JsonParse`](RecordTransform::JsonParse)).
    ///
    /// Each named field whose value is an object or array is replaced in place
    /// with its compact JSON-string form — the common shaping step for landing
    /// nested data as a flat `STRING` column. Scalar (already-flat) values and
    /// absent fields are left unchanged (idempotent).
    ///
    /// _Requires feature `transform-json-encode`._
    #[cfg(feature = "transform-json-encode")]
    JsonEncode { fields: Vec<String> },

    /// Enrich each record by joining it against a small in-memory reference
    /// table — a code→label lookup, without SQL.
    ///
    /// The record's `on_record` value is matched against the reference rows'
    /// `on_ref` value; on a hit, each `(output_column, reference_column)` pair in
    /// `add` is written onto the record. Keys are compared by their scalar
    /// string form so `42` matches `"42"`. Behaviour on a miss is governed by
    /// [`LookupOnMissing`]. This is a 1→1 enrichment (it never drops rows).
    ///
    /// The reference rows are resolved at config-load time (inline `values`, or
    /// a `csv`/`jsonl` file loaded by the CLI) and carried here verbatim.
    ///
    /// _Requires feature `transform-lookup`._
    #[cfg(feature = "transform-lookup")]
    Lookup {
        /// Resolved reference rows.
        reference: Vec<Map<String, Value>>,
        /// Field on the incoming record to match on.
        on_record: String,
        /// Field on the reference rows to match against.
        on_ref: String,
        /// `(output column on the record, source column on the reference row)`.
        add: Vec<(String, String)>,
        /// What to do when no reference row matches.
        on_missing: LookupOnMissing,
    },

    /// A user-supplied transformation function.
    ///
    /// The function receives each record as a [`Value`] and returns the
    /// (possibly modified) record.  Construct one with [`RecordTransform::custom`].
    ///
    /// Always available — not guarded by any feature flag.
    Custom(Arc<dyn Fn(Value) -> Value + Send + Sync>),
}

/// Behaviour when a [`RecordTransform::Lookup`] finds no matching reference row.
#[cfg(feature = "transform-lookup")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum LookupOnMissing {
    /// Write each `add` output column as JSON `null` (default).
    #[default]
    Null,
    /// Leave the record unchanged (add no columns).
    Keep,
    /// Fail the batch with a [`FaucetError::Transform`].
    Error,
}

impl fmt::Debug for RecordTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "transform-flatten")]
            Self::Flatten { separator } => f
                .debug_struct("Flatten")
                .field("separator", separator)
                .finish(),
            #[cfg(feature = "transform-rename-keys")]
            Self::RenameKeys {
                pattern,
                replacement,
            } => f
                .debug_struct("RenameKeys")
                .field("pattern", pattern)
                .field("replacement", replacement)
                .finish(),
            #[cfg(feature = "transform-keys-case")]
            Self::KeysCase { mode, on_collision } => f
                .debug_struct("KeysCase")
                .field("mode", mode)
                .field("on_collision", on_collision)
                .finish(),
            #[cfg(feature = "transform-select")]
            Self::Select { fields } => f.debug_struct("Select").field("fields", fields).finish(),
            #[cfg(feature = "transform-drop")]
            Self::Drop { fields } => f.debug_struct("Drop").field("fields", fields).finish(),
            #[cfg(feature = "transform-set")]
            Self::Set { values } => f.debug_struct("Set").field("values", values).finish(),
            #[cfg(feature = "transform-rename-field")]
            Self::RenameField { fields } => f
                .debug_struct("RenameField")
                .field("fields", fields)
                .finish(),
            #[cfg(feature = "transform-cast")]
            Self::Cast { fields, on_error } => f
                .debug_struct("Cast")
                .field("fields", fields)
                .field("on_error", on_error)
                .finish(),
            #[cfg(feature = "transform-redact")]
            Self::Redact { fields, mask } => f
                .debug_struct("Redact")
                .field("fields", fields)
                .field("mask", mask)
                .finish(),
            #[cfg(feature = "transform-value-case")]
            Self::ValueCase { fields, mode } => f
                .debug_struct("ValueCase")
                .field("fields", fields)
                .field("mode", mode)
                .finish(),
            #[cfg(feature = "transform-spell-symbols")]
            Self::SpellSymbols { extra, separator } => f
                .debug_struct("SpellSymbols")
                .field("extra", extra)
                .field("separator", separator)
                .finish(),
            #[cfg(feature = "transform-hash")]
            Self::Hash {
                fields,
                algorithm,
                encoding,
                salt,
                into,
            } => f
                .debug_struct("Hash")
                .field("fields", fields)
                .field("algorithm", algorithm)
                .field("encoding", encoding)
                // Never print salt material.
                .field("salt", &salt.as_ref().map(|_| "<redacted>"))
                .field("into", into)
                .finish(),
            #[cfg(feature = "transform-json-parse")]
            Self::JsonParse {
                fields,
                on_error,
                into,
            } => f
                .debug_struct("JsonParse")
                .field("fields", fields)
                .field("on_error", on_error)
                .field("into", into)
                .finish(),
            #[cfg(feature = "transform-coalesce")]
            Self::Coalesce {
                field,
                default,
                from,
                treat_empty_string_as_null,
            } => f
                .debug_struct("Coalesce")
                .field("field", field)
                .field("default", default)
                .field("from", from)
                .field("treat_empty_string_as_null", treat_empty_string_as_null)
                .finish(),
            #[cfg(feature = "transform-split-join")]
            Self::Split {
                field,
                delimiter,
                trim,
                into,
            } => f
                .debug_struct("Split")
                .field("field", field)
                .field("delimiter", delimiter)
                .field("trim", trim)
                .field("into", into)
                .finish(),
            #[cfg(feature = "transform-split-join")]
            Self::Join {
                field,
                delimiter,
                into,
            } => f
                .debug_struct("Join")
                .field("field", field)
                .field("delimiter", delimiter)
                .field("into", into)
                .finish(),
            #[cfg(feature = "transform-json-encode")]
            Self::JsonEncode { fields } => f
                .debug_struct("JsonEncode")
                .field("fields", fields)
                .finish(),
            #[cfg(feature = "transform-lookup")]
            Self::Lookup {
                reference,
                on_record,
                on_ref,
                add,
                on_missing,
            } => f
                .debug_struct("Lookup")
                .field("reference_rows", &reference.len())
                .field("on_record", on_record)
                .field("on_ref", on_ref)
                .field("add", add)
                .field("on_missing", on_missing)
                .finish(),
            Self::Custom(_) => write!(f, "Custom(<fn>)"),
        }
    }
}

// Arc<dyn Fn> is Clone (bumps refcount) but #[derive(Clone)] can't see that,
// so we implement Clone manually.
impl Clone for RecordTransform {
    fn clone(&self) -> Self {
        match self {
            #[cfg(feature = "transform-flatten")]
            Self::Flatten { separator } => Self::Flatten {
                separator: separator.clone(),
            },
            #[cfg(feature = "transform-rename-keys")]
            Self::RenameKeys {
                pattern,
                replacement,
            } => Self::RenameKeys {
                pattern: pattern.clone(),
                replacement: replacement.clone(),
            },
            #[cfg(feature = "transform-keys-case")]
            Self::KeysCase { mode, on_collision } => Self::KeysCase {
                mode: *mode,
                on_collision: *on_collision,
            },
            #[cfg(feature = "transform-select")]
            Self::Select { fields } => Self::Select {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-drop")]
            Self::Drop { fields } => Self::Drop {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-set")]
            Self::Set { values } => Self::Set {
                values: values.clone(),
            },
            #[cfg(feature = "transform-rename-field")]
            Self::RenameField { fields } => Self::RenameField {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-cast")]
            Self::Cast { fields, on_error } => Self::Cast {
                fields: fields.clone(),
                on_error: *on_error,
            },
            #[cfg(feature = "transform-redact")]
            Self::Redact { fields, mask } => Self::Redact {
                fields: fields.clone(),
                mask: mask.clone(),
            },
            #[cfg(feature = "transform-value-case")]
            Self::ValueCase { fields, mode } => Self::ValueCase {
                fields: fields.clone(),
                mode: *mode,
            },
            #[cfg(feature = "transform-spell-symbols")]
            Self::SpellSymbols { extra, separator } => Self::SpellSymbols {
                extra: extra.clone(),
                separator: separator.clone(),
            },
            #[cfg(feature = "transform-hash")]
            Self::Hash {
                fields,
                algorithm,
                encoding,
                salt,
                into,
            } => Self::Hash {
                fields: fields.clone(),
                algorithm: *algorithm,
                encoding: *encoding,
                salt: salt.clone(),
                into: into.clone(),
            },
            #[cfg(feature = "transform-json-parse")]
            Self::JsonParse {
                fields,
                on_error,
                into,
            } => Self::JsonParse {
                fields: fields.clone(),
                on_error: *on_error,
                into: into.clone(),
            },
            #[cfg(feature = "transform-coalesce")]
            Self::Coalesce {
                field,
                default,
                from,
                treat_empty_string_as_null,
            } => Self::Coalesce {
                field: field.clone(),
                default: default.clone(),
                from: from.clone(),
                treat_empty_string_as_null: *treat_empty_string_as_null,
            },
            #[cfg(feature = "transform-split-join")]
            Self::Split {
                field,
                delimiter,
                trim,
                into,
            } => Self::Split {
                field: field.clone(),
                delimiter: delimiter.clone(),
                trim: *trim,
                into: into.clone(),
            },
            #[cfg(feature = "transform-split-join")]
            Self::Join {
                field,
                delimiter,
                into,
            } => Self::Join {
                field: field.clone(),
                delimiter: delimiter.clone(),
                into: into.clone(),
            },
            #[cfg(feature = "transform-json-encode")]
            Self::JsonEncode { fields } => Self::JsonEncode {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-lookup")]
            Self::Lookup {
                reference,
                on_record,
                on_ref,
                add,
                on_missing,
            } => Self::Lookup {
                reference: reference.clone(),
                on_record: on_record.clone(),
                on_ref: on_ref.clone(),
                add: add.clone(),
                on_missing: *on_missing,
            },
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

// Arc<dyn Fn> is Clone (bumps refcount) but #[derive(Clone)] can't see that,
// so we implement Clone manually.
impl Clone for CompiledTransform {
    fn clone(&self) -> Self {
        match self {
            #[cfg(feature = "transform-flatten")]
            Self::Flatten { separator } => Self::Flatten {
                separator: separator.clone(),
            },
            #[cfg(feature = "transform-rename-keys")]
            Self::RenameKeys { re, replacement } => Self::RenameKeys {
                re: re.clone(),
                replacement: replacement.clone(),
            },
            #[cfg(feature = "transform-keys-case")]
            Self::KeysCase { mode, on_collision } => Self::KeysCase {
                mode: *mode,
                on_collision: *on_collision,
            },
            #[cfg(feature = "transform-select")]
            Self::Select { fields } => Self::Select {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-drop")]
            Self::Drop { fields } => Self::Drop {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-set")]
            Self::Set { values } => Self::Set {
                values: values.clone(),
            },
            #[cfg(feature = "transform-rename-field")]
            Self::RenameField { fields } => Self::RenameField {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-cast")]
            Self::Cast { fields, on_error } => Self::Cast {
                fields: fields.clone(),
                on_error: *on_error,
            },
            #[cfg(feature = "transform-redact")]
            Self::Redact { fields, mask } => Self::Redact {
                fields: fields.clone(),
                mask: mask.clone(),
            },
            #[cfg(feature = "transform-value-case")]
            Self::ValueCase { fields, mode } => Self::ValueCase {
                fields: fields.clone(),
                mode: *mode,
            },
            #[cfg(feature = "transform-spell-symbols")]
            Self::SpellSymbols {
                replacements,
                separator,
            } => Self::SpellSymbols {
                replacements: replacements.clone(),
                separator: separator.clone(),
            },
            #[cfg(feature = "transform-hash")]
            Self::Hash {
                fields,
                algorithm,
                encoding,
                salt,
                into,
            } => Self::Hash {
                fields: fields.clone(),
                algorithm: *algorithm,
                encoding: *encoding,
                salt: salt.clone(),
                into: into.clone(),
            },
            #[cfg(feature = "transform-json-parse")]
            Self::JsonParse {
                fields,
                on_error,
                into,
            } => Self::JsonParse {
                fields: fields.clone(),
                on_error: *on_error,
                into: into.clone(),
            },
            #[cfg(feature = "transform-coalesce")]
            Self::Coalesce {
                field,
                default,
                from,
                treat_empty_string_as_null,
            } => Self::Coalesce {
                field: field.clone(),
                default: default.clone(),
                from: from.clone(),
                treat_empty_string_as_null: *treat_empty_string_as_null,
            },
            #[cfg(feature = "transform-split-join")]
            Self::Split {
                field,
                delimiter,
                trim,
                into,
            } => Self::Split {
                field: field.clone(),
                delimiter: delimiter.clone(),
                trim: *trim,
                into: into.clone(),
            },
            #[cfg(feature = "transform-split-join")]
            Self::Join {
                field,
                delimiter,
                into,
            } => Self::Join {
                field: field.clone(),
                delimiter: delimiter.clone(),
                into: into.clone(),
            },
            #[cfg(feature = "transform-json-encode")]
            Self::JsonEncode { fields } => Self::JsonEncode {
                fields: fields.clone(),
            },
            #[cfg(feature = "transform-lookup")]
            Self::Lookup {
                index,
                on_record,
                add,
                on_missing,
            } => Self::Lookup {
                index: index.clone(),
                on_record: on_record.clone(),
                add: add.clone(),
                on_missing: *on_missing,
            },
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

impl RecordTransform {
    /// Create a custom transform from any function or closure.
    ///
    /// The closure receives each record as a [`Value`] and must return a
    /// [`Value`] (the transformed record).  It is called once per record and
    /// may perform any manipulation — adding fields, removing fields, renaming,
    /// type coercion, etc.
    ///
    /// Custom transforms are always available regardless of feature flags.
    ///
    /// # Example
    ///
    /// ```rust
    /// use faucet_core::RecordTransform;
    /// use serde_json::{Value, json};
    ///
    /// // Inject a constant "source" field into every record.
    /// let stamp = RecordTransform::custom(|mut record| {
    ///     if let Value::Object(ref mut map) = record {
    ///         map.insert("_source".to_string(), json!("my-api"));
    ///     }
    ///     record
    /// });
    /// ```
    pub fn custom<F>(f: F) -> Self
    where
        F: Fn(Value) -> Value + Send + Sync + 'static,
    {
        Self::Custom(Arc::new(f))
    }
}

// ── Internal compiled representation ─────────────────────────────────────────

/// Pre-compiled form of a [`RecordTransform`].
///
/// Stored inside a source (e.g. the REST source's `RestStream`) so that regex
/// patterns are compiled exactly once (at construction time) rather than once
/// per record.
///
/// Non-exhaustive for the same reason as [`RecordTransform`]: new transforms
/// are added additively.
#[non_exhaustive]
pub enum CompiledTransform {
    #[cfg(feature = "transform-flatten")]
    Flatten {
        separator: String,
    },
    #[cfg(feature = "transform-rename-keys")]
    RenameKeys {
        re: Regex,
        replacement: String,
    },
    #[cfg(feature = "transform-keys-case")]
    KeysCase {
        mode: KeyCaseMode,
        on_collision: KeyCollision,
    },
    #[cfg(feature = "transform-select")]
    Select {
        fields: Vec<String>,
    },
    #[cfg(feature = "transform-drop")]
    Drop {
        fields: Vec<String>,
    },
    #[cfg(feature = "transform-set")]
    Set {
        values: Map<String, Value>,
    },
    #[cfg(feature = "transform-rename-field")]
    RenameField {
        /// `(from, to)` pairs sorted by `from`, so application is deterministic
        /// regardless of the source `HashMap`'s iteration order.
        fields: Vec<(String, String)>,
    },
    #[cfg(feature = "transform-cast")]
    Cast {
        fields: HashMap<String, CastType>,
        on_error: CastOnError,
    },
    #[cfg(feature = "transform-redact")]
    Redact {
        fields: Vec<String>,
        mask: Value,
    },
    #[cfg(feature = "transform-value-case")]
    ValueCase {
        fields: Vec<String>,
        mode: ValueCaseMode,
    },
    #[cfg(feature = "transform-spell-symbols")]
    SpellSymbols {
        /// `(from, to)` pairs sorted by descending `from.len()` so longer
        /// patterns win when prefixes overlap (e.g. `"<="` before `"<"`).
        replacements: Vec<(String, String)>,
        separator: String,
    },
    #[cfg(feature = "transform-hash")]
    Hash {
        fields: Vec<String>,
        algorithm: HashAlgorithm,
        encoding: HashEncoding,
        salt: Option<String>,
        into: Option<String>,
    },
    #[cfg(feature = "transform-json-parse")]
    JsonParse {
        fields: Vec<String>,
        on_error: JsonParseOnError,
        into: Option<String>,
    },
    #[cfg(feature = "transform-coalesce")]
    Coalesce {
        field: String,
        default: Option<Value>,
        from: Vec<String>,
        treat_empty_string_as_null: bool,
    },
    #[cfg(feature = "transform-split-join")]
    Split {
        field: String,
        delimiter: String,
        trim: bool,
        into: Option<String>,
    },
    #[cfg(feature = "transform-split-join")]
    Join {
        field: String,
        delimiter: String,
        into: Option<String>,
    },
    #[cfg(feature = "transform-json-encode")]
    JsonEncode {
        fields: Vec<String>,
    },
    #[cfg(feature = "transform-lookup")]
    Lookup {
        /// Reference rows indexed by the scalar-string form of `on_ref`
        /// (first row wins on duplicate keys).
        index: HashMap<String, Map<String, Value>>,
        on_record: String,
        add: Vec<(String, String)>,
        on_missing: LookupOnMissing,
    },
    Custom(Arc<dyn Fn(Value) -> Value + Send + Sync>),
}

/// Compile a [`RecordTransform`] into its [`CompiledTransform`] form.
///
/// Returns [`FaucetError::Transform`] if a regex pattern is invalid.
pub fn compile(t: &RecordTransform) -> Result<CompiledTransform, FaucetError> {
    match t {
        #[cfg(feature = "transform-flatten")]
        RecordTransform::Flatten { separator } => Ok(CompiledTransform::Flatten {
            separator: separator.clone(),
        }),
        #[cfg(feature = "transform-rename-keys")]
        RecordTransform::RenameKeys {
            pattern,
            replacement,
        } => {
            let re = Regex::new(pattern)
                .map_err(|e| FaucetError::Transform(format!("invalid regex '{pattern}': {e}")))?;
            Ok(CompiledTransform::RenameKeys {
                re,
                replacement: replacement.clone(),
            })
        }
        #[cfg(feature = "transform-keys-case")]
        RecordTransform::KeysCase { mode, on_collision } => Ok(CompiledTransform::KeysCase {
            mode: *mode,
            on_collision: *on_collision,
        }),
        #[cfg(feature = "transform-select")]
        RecordTransform::Select { fields } => Ok(CompiledTransform::Select {
            fields: fields.clone(),
        }),
        #[cfg(feature = "transform-drop")]
        RecordTransform::Drop { fields } => Ok(CompiledTransform::Drop {
            fields: fields.clone(),
        }),
        #[cfg(feature = "transform-set")]
        RecordTransform::Set { values } => Ok(CompiledTransform::Set {
            values: values.clone(),
        }),
        #[cfg(feature = "transform-rename-field")]
        RecordTransform::RenameField { fields } => {
            // Materialize into a stable, sorted order so renames apply
            // deterministically — a `HashMap`'s iteration order is randomized,
            // which made interacting renames (chains/swaps) produce unstable or
            // corrupted output and intermittent collision errors.
            let mut fields: Vec<(String, String)> =
                fields.iter().map(|(f, t)| (f.clone(), t.clone())).collect();
            fields.sort();
            Ok(CompiledTransform::RenameField { fields })
        }
        #[cfg(feature = "transform-cast")]
        RecordTransform::Cast { fields, on_error } => Ok(CompiledTransform::Cast {
            fields: fields.clone(),
            on_error: *on_error,
        }),
        #[cfg(feature = "transform-redact")]
        RecordTransform::Redact { fields, mask } => Ok(CompiledTransform::Redact {
            fields: fields.clone(),
            mask: mask.clone(),
        }),
        #[cfg(feature = "transform-value-case")]
        RecordTransform::ValueCase { fields, mode } => Ok(CompiledTransform::ValueCase {
            fields: fields.clone(),
            mode: *mode,
        }),
        #[cfg(feature = "transform-spell-symbols")]
        RecordTransform::SpellSymbols { extra, separator } => {
            // Merge defaults + user overrides into a single ordered list,
            // sorted longest-first so `"<="` beats `"<"` etc.
            let mut merged = default_symbol_map();
            for (k, v) in extra {
                merged.insert(k.clone(), v.clone());
            }
            let mut replacements: Vec<(String, String)> = merged.into_iter().collect();
            replacements.sort_by_key(|b| std::cmp::Reverse(b.0.len()));
            Ok(CompiledTransform::SpellSymbols {
                replacements,
                separator: separator.clone(),
            })
        }
        #[cfg(feature = "transform-hash")]
        RecordTransform::Hash {
            fields,
            algorithm,
            encoding,
            salt,
            into,
        } => {
            if fields.is_empty() {
                return Err(FaucetError::Config(
                    "hash: `fields` must not be empty".to_owned(),
                ));
            }
            if into.is_some() && fields.len() != 1 {
                return Err(FaucetError::Config(
                    "hash: `into` is only valid with exactly one field".to_owned(),
                ));
            }
            Ok(CompiledTransform::Hash {
                fields: fields.clone(),
                algorithm: *algorithm,
                encoding: *encoding,
                salt: salt.clone(),
                into: into.clone(),
            })
        }
        #[cfg(feature = "transform-json-parse")]
        RecordTransform::JsonParse {
            fields,
            on_error,
            into,
        } => {
            if fields.is_empty() {
                return Err(FaucetError::Config(
                    "json_parse: `fields` must not be empty".to_owned(),
                ));
            }
            if into.is_some() && fields.len() != 1 {
                return Err(FaucetError::Config(
                    "json_parse: `into` is only valid with exactly one field".to_owned(),
                ));
            }
            Ok(CompiledTransform::JsonParse {
                fields: fields.clone(),
                on_error: *on_error,
                into: into.clone(),
            })
        }
        #[cfg(feature = "transform-coalesce")]
        RecordTransform::Coalesce {
            field,
            default,
            from,
            treat_empty_string_as_null,
        } => {
            match (default.is_some(), from.is_empty()) {
                // default set, from empty → ok
                (true, true) => {}
                // default unset, from non-empty → ok
                (false, false) => {}
                (true, false) => {
                    return Err(FaucetError::Config(
                        "coalesce: set exactly one of `default` or `from`, not both".to_owned(),
                    ));
                }
                (false, true) => {
                    return Err(FaucetError::Config(
                        "coalesce: set exactly one of `default` or `from`".to_owned(),
                    ));
                }
            }
            Ok(CompiledTransform::Coalesce {
                field: field.clone(),
                default: default.clone(),
                from: from.clone(),
                treat_empty_string_as_null: *treat_empty_string_as_null,
            })
        }
        #[cfg(feature = "transform-split-join")]
        RecordTransform::Split {
            field,
            delimiter,
            trim,
            into,
        } => Ok(CompiledTransform::Split {
            field: field.clone(),
            delimiter: delimiter.clone(),
            trim: *trim,
            into: into.clone(),
        }),
        #[cfg(feature = "transform-split-join")]
        RecordTransform::Join {
            field,
            delimiter,
            into,
        } => Ok(CompiledTransform::Join {
            field: field.clone(),
            delimiter: delimiter.clone(),
            into: into.clone(),
        }),
        #[cfg(feature = "transform-json-encode")]
        RecordTransform::JsonEncode { fields } => Ok(CompiledTransform::JsonEncode {
            fields: fields.clone(),
        }),
        #[cfg(feature = "transform-lookup")]
        RecordTransform::Lookup {
            reference,
            on_record,
            on_ref,
            add,
            on_missing,
        } => {
            if on_record.trim().is_empty() || on_ref.trim().is_empty() {
                return Err(FaucetError::Transform(
                    "lookup: `on_record` and `on_ref` must be non-empty".into(),
                ));
            }
            if add.is_empty() {
                return Err(FaucetError::Transform(
                    "lookup: `add` must name at least one output column".into(),
                ));
            }
            // Index the reference rows by the scalar-string form of `on_ref`;
            // the first row for a given key wins.
            let mut index = HashMap::with_capacity(reference.len());
            for row in reference {
                if let Some(k) = row.get(on_ref).map(value_to_key) {
                    index.entry(k).or_insert_with(|| row.clone());
                }
            }
            Ok(CompiledTransform::Lookup {
                index,
                on_record: on_record.clone(),
                add: add.clone(),
                on_missing: *on_missing,
            })
        }
        RecordTransform::Custom(f) => Ok(CompiledTransform::Custom(Arc::clone(f))),
    }
}

/// Scalar-string key form for [`RecordTransform::Lookup`] matching, so `42`
/// matches `"42"`. `null` maps to the empty string.
#[cfg(feature = "transform-lookup")]
fn value_to_key(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Apply a slice of pre-compiled transforms to a record, in order.
///
/// Returns [`FaucetError::Transform`] if a transform would silently lose data
/// — currently when `flatten`, `keys_case`, or `spell_symbols` collapse two
/// distinct fields to the same key (#78/#28).
pub fn apply_all(record: Value, transforms: &[CompiledTransform]) -> Result<Value, FaucetError> {
    let mut acc = record;
    for t in transforms {
        acc = apply_one(acc, t)?;
    }
    Ok(acc)
}

fn apply_one(value: Value, t: &CompiledTransform) -> Result<Value, FaucetError> {
    match t {
        #[cfg(feature = "transform-flatten")]
        CompiledTransform::Flatten { separator } => flatten(value, separator),
        #[cfg(feature = "transform-rename-keys")]
        CompiledTransform::RenameKeys { re, replacement } => {
            Ok(rename_keys(value, re, replacement))
        }
        #[cfg(feature = "transform-keys-case")]
        CompiledTransform::KeysCase { mode, on_collision } => {
            keys_case(value, *mode, *on_collision)
        }
        #[cfg(feature = "transform-select")]
        CompiledTransform::Select { fields } => Ok(select_fields(value, fields)),
        #[cfg(feature = "transform-drop")]
        CompiledTransform::Drop { fields } => Ok(drop_fields(value, fields)),
        #[cfg(feature = "transform-set")]
        CompiledTransform::Set { values } => Ok(set_fields(value, values)),
        #[cfg(feature = "transform-rename-field")]
        CompiledTransform::RenameField { fields } => rename_field(value, fields),
        #[cfg(feature = "transform-cast")]
        CompiledTransform::Cast { fields, on_error } => cast_fields(value, fields, *on_error),
        #[cfg(feature = "transform-redact")]
        CompiledTransform::Redact { fields, mask } => Ok(redact_fields(value, fields, mask)),
        #[cfg(feature = "transform-value-case")]
        CompiledTransform::ValueCase { fields, mode } => Ok(value_case(value, fields, *mode)),
        #[cfg(feature = "transform-spell-symbols")]
        CompiledTransform::SpellSymbols {
            replacements,
            separator,
        } => spell_symbols(value, replacements, separator),
        #[cfg(feature = "transform-hash")]
        CompiledTransform::Hash {
            fields,
            algorithm,
            encoding,
            salt,
            into,
        } => Ok(hash_fields(
            value,
            fields,
            *algorithm,
            *encoding,
            salt.as_deref(),
            into.as_deref(),
        )),
        #[cfg(feature = "transform-json-parse")]
        CompiledTransform::JsonParse {
            fields,
            on_error,
            into,
        } => json_parse_fields(value, fields, *on_error, into.as_deref()),
        #[cfg(feature = "transform-coalesce")]
        CompiledTransform::Coalesce {
            field,
            default,
            from,
            treat_empty_string_as_null,
        } => Ok(coalesce_field(
            value,
            field,
            default.as_ref(),
            from,
            *treat_empty_string_as_null,
        )),
        #[cfg(feature = "transform-split-join")]
        CompiledTransform::Split {
            field,
            delimiter,
            trim,
            into,
        } => Ok(split_field(value, field, delimiter, *trim, into.as_deref())),
        #[cfg(feature = "transform-split-join")]
        CompiledTransform::Join {
            field,
            delimiter,
            into,
        } => Ok(join_field(value, field, delimiter, into.as_deref())),
        #[cfg(feature = "transform-json-encode")]
        CompiledTransform::JsonEncode { fields } => Ok(json_encode_fields(value, fields)),
        #[cfg(feature = "transform-lookup")]
        CompiledTransform::Lookup {
            index,
            on_record,
            add,
            on_missing,
        } => lookup_field(value, index, on_record, add, *on_missing),
        CompiledTransform::Custom(f) => Ok(f(value)),
    }
}

// ── Flatten ───────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-flatten")]
fn flatten(value: Value, separator: &str) -> Result<Value, FaucetError> {
    match value {
        Value::Object(_) => {
            let mut out = Map::new();
            flatten_into(value, "", separator, &mut out)?;
            Ok(Value::Object(out))
        }
        other => Ok(other),
    }
}

#[cfg(feature = "transform-flatten")]
fn flatten_into(
    value: Value,
    prefix: &str,
    separator: &str,
    out: &mut Map<String, Value>,
) -> Result<(), FaucetError> {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let key = if prefix.is_empty() {
                    k
                } else {
                    format!("{prefix}{separator}{k}")
                };
                flatten_into(v, &key, separator, out)?;
            }
        }
        other => {
            // Erroring (rather than last-wins) avoids silently dropping a value
            // when a nested path and a literal key collide, e.g.
            // `{"a__b":1,"a":{"b":2}}` both map to `a__b` (#78/#28).
            if out.contains_key(prefix) {
                return Err(FaucetError::Transform(format!(
                    "flatten produced a duplicate key '{prefix}'; two distinct fields collapse \
                     to the same flattened key (separator '{separator}')"
                )));
            }
            out.insert(prefix.to_string(), other);
        }
    }
    Ok(())
}

// ── Rename keys ───────────────────────────────────────────────────────────────

#[cfg(feature = "transform-rename-keys")]
fn rename_keys(value: Value, re: &Regex, replacement: &str) -> Value {
    match value {
        Value::Object(map) => {
            let new_map: Map<String, Value> = map
                .into_iter()
                .map(|(k, v)| {
                    let new_k = re.replace_all(&k, replacement).into_owned();
                    (new_k, rename_keys(v, re, replacement))
                })
                .collect();
            Value::Object(new_map)
        }
        Value::Array(arr) => Value::Array(
            arr.into_iter()
                .map(|v| rename_keys(v, re, replacement))
                .collect(),
        ),
        other => other,
    }
}

// ── KeysCase ──────────────────────────────────────────────────────────────────

/// Recursively re-case every key in the record according to `mode`.
///
/// When two distinct keys within the same object re-case to the same name,
/// `on_collision` decides: [`KeyCollision::Error`] fails the record (default,
/// never drops a column); [`KeyCollision::Suffix`] appends `_2`, `_3`, … in
/// original key order to make each unique (deterministic + order-stable).
#[cfg(feature = "transform-keys-case")]
fn keys_case(
    value: Value,
    mode: KeyCaseMode,
    on_collision: KeyCollision,
) -> Result<Value, FaucetError> {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map {
                let tokens = tokenize_key(&k);
                let recased = if tokens.is_empty() {
                    // An all-symbol key tokenises to nothing — keep the
                    // original key instead of producing a blank one.
                    k
                } else {
                    apply_key_case(tokens, mode)
                };
                let new_v = keys_case(v, mode, on_collision)?;
                let final_key = if new_map.contains_key(&recased) {
                    match on_collision {
                        KeyCollision::Error => {
                            return Err(FaucetError::Transform(format!(
                                "keys_case produced a duplicate key '{recased}'; two distinct \
                                 keys re-case to the same name under mode {mode:?} (set \
                                 on_collision: suffix to disambiguate)"
                            )));
                        }
                        KeyCollision::Suffix => {
                            // Bump the numeric suffix until the name is free —
                            // guards against colliding with a real `name_2` key.
                            let mut n = 2usize;
                            let mut candidate = format!("{recased}_{n}");
                            while new_map.contains_key(&candidate) {
                                n += 1;
                                candidate = format!("{recased}_{n}");
                            }
                            candidate
                        }
                    }
                } else {
                    recased
                };
                new_map.insert(final_key, new_v);
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(keys_case(v, mode, on_collision)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other),
    }
}

/// Split a key into word tokens.  Boundaries: whitespace, `_`, `-`, any
/// other non-alphanumeric char, and lower→upper transitions (so
/// `firstName` splits as `["first", "Name"]`).  Multi-char uppercase runs
/// are left as one token (`"XMLParser"` → `["XMLParser"]`); document the
/// limitation in the cookbook rather than complicating the tokeniser.
#[cfg(feature = "transform-keys-case")]
fn tokenize_key(key: &str) -> Vec<String> {
    // Single source of truth (shared with connector code that must snake_case
    // column names identically — see `crate::util::tokenize_identifier`).
    crate::util::tokenize_identifier(key)
}

#[cfg(feature = "transform-keys-case")]
fn apply_key_case(tokens: Vec<String>, mode: KeyCaseMode) -> String {
    match mode {
        KeyCaseMode::Snake => tokens
            .iter()
            .map(|t| t.to_lowercase())
            .collect::<Vec<_>>()
            .join("_"),
        KeyCaseMode::ScreamingSnake => tokens
            .iter()
            .map(|t| t.to_uppercase())
            .collect::<Vec<_>>()
            .join("_"),
        KeyCaseMode::Kebab => tokens
            .iter()
            .map(|t| t.to_lowercase())
            .collect::<Vec<_>>()
            .join("-"),
        KeyCaseMode::Dot => tokens
            .iter()
            .map(|t| t.to_lowercase())
            .collect::<Vec<_>>()
            .join("."),
        KeyCaseMode::Camel => {
            let mut iter = tokens.into_iter();
            match iter.next() {
                None => String::new(),
                Some(first) => {
                    let mut out = first.to_lowercase();
                    for t in iter {
                        out.push_str(&capitalize_token(&t));
                    }
                    out
                }
            }
        }
        KeyCaseMode::Pascal => tokens
            .into_iter()
            .map(|t| capitalize_token(&t))
            .collect::<String>(),
    }
}

/// Lowercase the input then uppercase the first char.
#[cfg(feature = "transform-keys-case")]
fn capitalize_token(s: &str) -> String {
    let lower = s.to_lowercase();
    let mut chars = lower.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
    }
}

// ── Select ────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-select")]
fn select_fields(value: Value, fields: &[String]) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::with_capacity(fields.len().min(map.len()));
            // Preserve `fields` order so downstream consumers get a stable layout.
            for f in fields {
                if let Some(v) = map.get(f) {
                    out.insert(f.clone(), v.clone());
                }
            }
            Value::Object(out)
        }
        other => other,
    }
}

// ── Drop ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-drop")]
fn drop_fields(value: Value, fields: &[String]) -> Value {
    match value {
        Value::Object(mut map) => {
            for f in fields {
                map.remove(f);
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── Set ───────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-set")]
fn set_fields(value: Value, values: &Map<String, Value>) -> Value {
    match value {
        Value::Object(mut map) => {
            for (k, v) in values {
                map.insert(k.clone(), v.clone());
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── RenameField ───────────────────────────────────────────────────────────────

#[cfg(feature = "transform-rename-field")]
fn rename_field(value: Value, fields: &[(String, String)]) -> Result<Value, FaucetError> {
    match value {
        Value::Object(mut map) => {
            // Apply every rename against the ORIGINAL record (a snapshot), not
            // sequentially against a mutating map. This makes interacting renames
            // — chains (`{a:b, b:c}`) and swaps (`{a:b, b:a}`) — deterministic and
            // order-independent: each source's value moves to its target as it was
            // before any rename, and sources are removed atomically.
            let renames: Vec<(&str, &str)> = fields
                .iter()
                .filter(|(from, to)| from != to && map.contains_key(from))
                .map(|(from, to)| (from.as_str(), to.as_str()))
                .collect();
            let sources: std::collections::HashSet<&str> =
                renames.iter().map(|(from, _)| *from).collect();

            // Validate before mutating.
            let mut seen_targets: std::collections::HashSet<&str> =
                std::collections::HashSet::new();
            for (from, to) in &renames {
                if !seen_targets.insert(to) {
                    return Err(FaucetError::Transform(format!(
                        "rename_field: two fields rename to the same target key '{to}'"
                    )));
                }
                // A target collides only if a *surviving* key (one not being
                // renamed away) already occupies it — mirrors the collision
                // semantics in `flatten` / `keys_case`.
                if map.contains_key(*to) && !sources.contains(to) {
                    return Err(FaucetError::Transform(format!(
                        "rename_field: target key '{to}' already exists on the record \
                         (renaming from '{from}')"
                    )));
                }
            }

            let staged: Vec<(String, Value)> = renames
                .iter()
                .map(|(from, to)| {
                    let v = map.remove(*from).expect("source presence checked above");
                    (to.to_string(), v)
                })
                .collect();
            for (to, v) in staged {
                map.insert(to, v);
            }
            Ok(Value::Object(map))
        }
        other => Ok(other),
    }
}

// ── Cast ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-cast")]
fn cast_fields(
    value: Value,
    fields: &HashMap<String, CastType>,
    on_error: CastOnError,
) -> Result<Value, FaucetError> {
    match value {
        Value::Object(mut map) => {
            for (field, target) in fields {
                let Some(current) = map.get(field) else {
                    continue;
                };
                match cast_value(current, *target) {
                    Ok(new_val) => {
                        map.insert(field.clone(), new_val);
                    }
                    Err(msg) => match on_error {
                        CastOnError::Error => {
                            return Err(FaucetError::Transform(format!(
                                "cast: field '{field}' to {target:?} failed: {msg}"
                            )));
                        }
                        CastOnError::Null => {
                            map.insert(field.clone(), Value::Null);
                        }
                        CastOnError::Skip => { /* leave as-is */ }
                    },
                }
            }
            Ok(Value::Object(map))
        }
        other => Ok(other),
    }
}

/// Try to coerce a single [`Value`] to `target`.  Returns a human-readable
/// reason string on failure (the caller wraps it in `FaucetError::Transform`).
#[cfg(feature = "transform-cast")]
fn cast_value(v: &Value, target: CastType) -> Result<Value, String> {
    match target {
        CastType::Int => match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    return Ok(Value::Number(i.into()));
                }
                // A float-backed number only converts when it is a whole
                // number within i64 range. A fractional or out-of-range float
                // is an error rather than a silent truncate/saturate — so
                // `on_error` (error/null/skip) governs it as documented.
                // `2^63` is the exact f64 just above i64::MAX; `[-2^63, 2^63)`
                // with a zero fractional part round-trips losslessly.
                match n.as_f64() {
                    Some(f)
                        if f.fract() == 0.0 && (-(2f64.powi(63))..2f64.powi(63)).contains(&f) =>
                    {
                        Ok(Value::Number((f as i64).into()))
                    }
                    Some(f) => Err(format!(
                        "float '{f}' is not a whole number representable as i64"
                    )),
                    None => Err(format!("number '{n}' is not representable as i64")),
                }
            }
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(|i| Value::Number(i.into()))
                .map_err(|e| format!("'{s}' is not an integer: {e}")),
            Value::Bool(b) => Ok(Value::Number(i64::from(*b).into())),
            Value::Null => Err("null cannot be cast to int".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to int".to_owned())
            }
        },
        CastType::Float => match v {
            Value::Number(n) => n
                .as_f64()
                .and_then(|f| serde_json::Number::from_f64(f).map(Value::Number))
                .ok_or_else(|| format!("number '{n}' is not representable as f64")),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .ok()
                .and_then(|f| serde_json::Number::from_f64(f).map(Value::Number))
                .ok_or_else(|| format!("'{s}' is not a float")),
            Value::Bool(b) => serde_json::Number::from_f64(if *b { 1.0 } else { 0.0 })
                .map(Value::Number)
                .ok_or_else(|| "could not encode bool as f64".to_owned()),
            Value::Null => Err("null cannot be cast to float".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to float".to_owned())
            }
        },
        CastType::Bool => match v {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    match i {
                        0 => Ok(Value::Bool(false)),
                        1 => Ok(Value::Bool(true)),
                        _ => Err(format!("integer {i} is not 0 or 1")),
                    }
                } else {
                    Err(format!("number '{n}' is not 0 or 1"))
                }
            }
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "y" => Ok(Value::Bool(true)),
                "false" | "0" | "no" | "n" => Ok(Value::Bool(false)),
                other => Err(format!("'{other}' is not a recognised boolean")),
            },
            Value::Null => Err("null cannot be cast to bool".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to bool".to_owned())
            }
        },
        CastType::String => match v {
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Number(n) => Ok(Value::String(n.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            Value::Null => Err("null cannot be cast to string".to_owned()),
            Value::Array(_) | Value::Object(_) => {
                Err("composite values cannot be cast to string".to_owned())
            }
        },
        CastType::Timestamp => match v {
            Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
                .map(|dt| Value::String(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)))
                .map_err(|e| format!("'{s}' is not a valid RFC 3339 timestamp: {e}")),
            other => Err(format!(
                "cannot cast {} to timestamp (expected RFC 3339 string)",
                value_type_name(other)
            )),
        },
    }
}

#[cfg(feature = "transform-cast")]
fn value_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

// ── Redact ────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-redact")]
fn redact_fields(value: Value, fields: &[String], mask: &Value) -> Value {
    match value {
        Value::Object(mut map) => {
            for f in fields {
                if map.contains_key(f) {
                    map.insert(f.clone(), mask.clone());
                }
            }
            Value::Object(map)
        }
        other => other,
    }
}

// ── ValueCase ─────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-value-case")]
fn value_case(value: Value, fields: &[String], mode: ValueCaseMode) -> Value {
    match value {
        Value::Object(mut map) => {
            for f in fields {
                if let Some(Value::String(s)) = map.get(f) {
                    let new_s = match mode {
                        ValueCaseMode::Lower => s.to_lowercase(),
                        ValueCaseMode::Upper => s.to_uppercase(),
                        ValueCaseMode::Trim => s.trim().to_owned(),
                        ValueCaseMode::Title => title_case(s),
                        ValueCaseMode::Capitalize => capitalize_str(s),
                    };
                    map.insert(f.clone(), Value::String(new_s));
                }
            }
            Value::Object(map)
        }
        other => other,
    }
}

/// Title-case: upper-case the first letter of each whitespace-delimited word,
/// lower-case the rest. Word boundaries are ASCII/Unicode whitespace only
/// (punctuation and underscores do NOT start a new word — `"o'brien"` →
/// `"O'brien"`). Uses `char::to_uppercase` semantics.
#[cfg(feature = "transform-value-case")]
fn title_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut at_word_start = true;
    for ch in s.chars() {
        if ch.is_whitespace() {
            at_word_start = true;
            out.push(ch);
        } else if at_word_start {
            out.extend(ch.to_uppercase());
            at_word_start = false;
        } else {
            out.extend(ch.to_lowercase());
        }
    }
    out
}

/// Capitalize: upper-case only the first character of the whole string,
/// lower-case the rest.
#[cfg(feature = "transform-value-case")]
fn capitalize_str(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => {
            let mut out: String = first.to_uppercase().collect();
            out.push_str(&chars.as_str().to_lowercase());
            out
        }
    }
}

// ── SpellSymbols ──────────────────────────────────────────────────────────────

/// Built-in symbol → word map used by [`RecordTransform::SpellSymbols`].
///
/// The defaults cover the common ASCII symbols that downstream identifier
/// rules (`snake_case`, SQL column naming, JSON pointer paths) typically
/// strip or reject.  Symbols that are already identifier-safe (`_`, `-`,
/// `.`) are intentionally left alone; symbols that `keys_case` strips
/// outright (`(`, `)`, `[`, `]`, `:`, `,` …) are also omitted — chain
/// `keys_case` after `spell_symbols` if you need them removed.
#[cfg(feature = "transform-spell-symbols")]
pub fn default_symbol_map() -> HashMap<String, String> {
    let pairs: &[(&str, &str)] = &[
        ("%", "percent"),
        ("#", "number"),
        ("$", "dollar"),
        ("&", "and"),
        ("@", "at"),
        ("+", "plus"),
        ("*", "star"),
        ("=", "equals"),
        ("<", "lt"),
        (">", "gt"),
        ("/", "slash"),
        ("\\", "backslash"),
        ("|", "pipe"),
        ("^", "caret"),
        ("~", "tilde"),
    ];
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect()
}

#[cfg(feature = "transform-spell-symbols")]
fn spell_symbols(
    value: Value,
    replacements: &[(String, String)],
    separator: &str,
) -> Result<Value, FaucetError> {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::with_capacity(map.len());
            for (k, v) in map {
                let new_k = spell_symbols_in_key(&k, replacements, separator);
                let new_v = spell_symbols(v, replacements, separator)?;
                // Erroring (rather than last-wins) avoids silently dropping a
                // value when two distinct keys spell to the same name — same
                // contract as `flatten` / `keys_case` (#78/#28).
                if new_map.contains_key(&new_k) {
                    return Err(FaucetError::Transform(format!(
                        "spell_symbols produced a duplicate key '{new_k}'; two distinct keys \
                         expand to the same name"
                    )));
                }
                new_map.insert(new_k, new_v);
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(spell_symbols(v, replacements, separator)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other),
    }
}

/// Apply the (longest-first) `replacements` to a single key string,
/// inserting `separator` around each substitution so word boundaries
/// survive a downstream `keys_case` step.
#[cfg(feature = "transform-spell-symbols")]
fn spell_symbols_in_key(key: &str, replacements: &[(String, String)], separator: &str) -> String {
    // Walk the input left-to-right; at each position try the longest
    // replacement first. This avoids `"<="` being split by the shorter
    // `"<"` substitution.
    let bytes = key.as_bytes();
    let mut out = String::with_capacity(key.len());
    let mut i = 0;
    while i < bytes.len() {
        let mut matched = false;
        for (from, to) in replacements {
            let f = from.as_bytes();
            if !f.is_empty() && bytes[i..].starts_with(f) {
                out.push_str(separator);
                out.push_str(to);
                out.push_str(separator);
                i += f.len();
                matched = true;
                break;
            }
        }
        if !matched {
            // Step by one UTF-8 char. We have to walk the &str slice (not
            // the byte buffer) to respect codepoint boundaries.
            let ch = key[i..]
                .chars()
                .next()
                .expect("non-empty slice yields at least one char");
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

// ── Hash ──────────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-hash")]
fn hash_fields(
    value: Value,
    fields: &[String],
    algorithm: HashAlgorithm,
    encoding: HashEncoding,
    salt: Option<&str>,
    into: Option<&str>,
) -> Value {
    match value {
        Value::Object(mut map) => {
            for field in fields {
                let Some(current) = map.get(field) else {
                    continue;
                };
                // Null is left alone, matching `faucet_core::masking` (whose
                // `hash`/`tokenize` actions skip null for the same reasons).
                // Hashing it would (a) destroy nullability — a NOT NULL column
                // silently accepts the digest and `IS NULL` stops matching
                // downstream — and (b) give *every* null-valued row the same
                // digest, so hashing a nullable field and keying an upsert or a
                // join on it collapses all of those rows into one (#456 M3).
                if current.is_null() {
                    continue;
                }
                // String values hash over their raw UTF-8 bytes; every other
                // JSON value hashes over its canonical serialization.
                let input = match current {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let digest = hash_string(&input, algorithm, encoding, salt);
                let target = into.unwrap_or(field.as_str());
                map.insert(target.to_owned(), Value::String(digest));
            }
            Value::Object(map)
        }
        other => other,
    }
}

#[cfg(feature = "transform-hash")]
fn hash_string(
    input: &str,
    algorithm: HashAlgorithm,
    encoding: HashEncoding,
    salt: Option<&str>,
) -> String {
    // Salt is prepended before the value bytes.
    let mut bytes: Vec<u8> = Vec::with_capacity(salt.map_or(0, str::len) + input.len());
    if let Some(s) = salt {
        bytes.extend_from_slice(s.as_bytes());
    }
    bytes.extend_from_slice(input.as_bytes());
    let digest: Vec<u8> = match algorithm {
        HashAlgorithm::Sha256 => {
            use sha2::{Digest, Sha256};
            let mut h = Sha256::new();
            h.update(&bytes);
            h.finalize().to_vec()
        }
        HashAlgorithm::Blake3 => blake3::hash(&bytes).as_bytes().to_vec(),
    };
    match encoding {
        HashEncoding::Hex => hex_encode(&digest),
        HashEncoding::Base64 => {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&digest)
        }
    }
}

#[cfg(feature = "transform-hash")]
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0x0f) as usize] as char);
    }
    s
}

// ── JsonEncode / Lookup (#516) ───────────────────────────────────────────────

/// [`RecordTransform::JsonEncode`] — replace each named object/array field with
/// its compact JSON-string form. Scalars and absent fields are left unchanged.
#[cfg(feature = "transform-json-encode")]
fn json_encode_fields(mut value: Value, fields: &[String]) -> Value {
    if let Value::Object(map) = &mut value {
        for f in fields {
            if let Some(v) = map.get_mut(f)
                && matches!(v, Value::Object(_) | Value::Array(_))
            {
                let s = serde_json::to_string(v).unwrap_or_else(|_| "null".to_string());
                *v = Value::String(s);
            }
        }
    }
    value
}

/// [`RecordTransform::Lookup`] — enrich a record from an indexed reference set.
#[cfg(feature = "transform-lookup")]
fn lookup_field(
    mut value: Value,
    index: &HashMap<String, Map<String, Value>>,
    on_record: &str,
    add: &[(String, String)],
    on_missing: LookupOnMissing,
) -> Result<Value, FaucetError> {
    let Value::Object(map) = &mut value else {
        return Ok(value);
    };
    let key = map.get(on_record).map(value_to_key);
    let matched = key.as_deref().and_then(|k| index.get(k));
    match matched {
        Some(row) => {
            for (out, src) in add {
                let v = row.get(src).cloned().unwrap_or(Value::Null);
                map.insert(out.clone(), v);
            }
        }
        None => match on_missing {
            LookupOnMissing::Null => {
                for (out, _) in add {
                    map.insert(out.clone(), Value::Null);
                }
            }
            LookupOnMissing::Keep => {}
            LookupOnMissing::Error => {
                return Err(FaucetError::Transform(format!(
                    "lookup: no reference row for {on_record}={:?}",
                    key.unwrap_or_default()
                )));
            }
        },
    }
    Ok(value)
}

// ── JsonParse ───────────────────────────────────────────────────────────────

#[cfg(feature = "transform-json-parse")]
fn json_parse_fields(
    value: Value,
    fields: &[String],
    on_error: JsonParseOnError,
    into: Option<&str>,
) -> Result<Value, FaucetError> {
    match value {
        Value::Object(mut map) => {
            for field in fields {
                // Only string values are candidates; a non-string (already
                // parsed) value passes through untouched — idempotent.
                let Some(Value::String(s)) = map.get(field) else {
                    continue;
                };
                let s = s.clone();
                match serde_json::from_str::<Value>(&s) {
                    Ok(parsed) => {
                        let target = into.unwrap_or(field.as_str());
                        map.insert(target.to_owned(), parsed);
                    }
                    Err(e) => match on_error {
                        JsonParseOnError::Keep => { /* leave the string as-is */ }
                        JsonParseOnError::Null => {
                            let target = into.unwrap_or(field.as_str());
                            map.insert(target.to_owned(), Value::Null);
                        }
                        JsonParseOnError::Error => {
                            return Err(FaucetError::Transform(format!(
                                "json_parse: field '{field}' is not valid JSON: {e}"
                            )));
                        }
                    },
                }
            }
            Ok(Value::Object(map))
        }
        other => Ok(other),
    }
}

// ── Coalesce ──────────────────────────────────────────────────────────────────

#[cfg(feature = "transform-coalesce")]
fn coalesce_field(
    value: Value,
    field: &str,
    default: Option<&Value>,
    from: &[String],
    treat_empty_string_as_null: bool,
) -> Value {
    match value {
        Value::Object(mut map) => {
            if is_nullish(map.get(field), treat_empty_string_as_null) {
                let replacement: Option<Value> = match default {
                    Some(d) => Some(d.clone()),
                    None => from.iter().find_map(|k| {
                        let v = map.get(k);
                        if is_nullish(v, treat_empty_string_as_null) {
                            None
                        } else {
                            v.cloned()
                        }
                    }),
                };
                if let Some(v) = replacement {
                    map.insert(field.to_owned(), v);
                }
            }
            Value::Object(map)
        }
        other => other,
    }
}

/// A value counts as "nullish" (eligible for coalescing) when it is absent or
/// JSON `null`, or — when `treat_empty_string_as_null` — an empty string.
#[cfg(feature = "transform-coalesce")]
fn is_nullish(v: Option<&Value>, treat_empty_string_as_null: bool) -> bool {
    match v {
        None | Some(Value::Null) => true,
        Some(Value::String(s)) => treat_empty_string_as_null && s.is_empty(),
        _ => false,
    }
}

// ── Split / Join ────────────────────────────────────────────────────────────

#[cfg(feature = "transform-split-join")]
fn split_field(
    value: Value,
    field: &str,
    delimiter: &str,
    trim: bool,
    into: Option<&str>,
) -> Value {
    match value {
        Value::Object(mut map) => {
            let Some(Value::String(s)) = map.get(field) else {
                return Value::Object(map);
            };
            let s = s.clone();
            // An empty delimiter is treated as "no split" — one element holding
            // the whole (optionally trimmed) string — rather than the surprising
            // std behaviour of splitting between every char.
            let parts: Vec<Value> = if delimiter.is_empty() {
                vec![Value::String(if trim { s.trim().to_owned() } else { s })]
            } else {
                s.split(delimiter)
                    .map(|part| {
                        let p = if trim { part.trim() } else { part };
                        Value::String(p.to_owned())
                    })
                    .collect()
            };
            let target = into.unwrap_or(field);
            map.insert(target.to_owned(), Value::Array(parts));
            Value::Object(map)
        }
        other => other,
    }
}

#[cfg(feature = "transform-split-join")]
fn join_field(value: Value, field: &str, delimiter: &str, into: Option<&str>) -> Value {
    match value {
        Value::Object(mut map) => {
            let Some(Value::Array(arr)) = map.get(field) else {
                return Value::Object(map);
            };
            let joined = arr
                .iter()
                .map(scalar_to_string)
                .collect::<Vec<_>>()
                .join(delimiter);
            let target = into.unwrap_or(field);
            map.insert(target.to_owned(), Value::String(joined));
            Value::Object(map)
        }
        other => other,
    }
}

/// Render a JSON array element for `join`: strings emit their raw value, null
/// emits an empty string, everything else its compact JSON scalar form.
#[cfg(feature = "transform-split-join")]
fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Test-only wrapper that shadows [`super::apply_all`] and unwraps, so the
    /// many existing success-path tests need no changes now that `apply_all`
    /// returns `Result`. Collision tests call `super::apply_all` for the
    /// `Result` directly.
    fn apply_all(record: Value, transforms: &[CompiledTransform]) -> Value {
        super::apply_all(record, transforms).expect("transform should succeed in this test")
    }

    fn compiled(transforms: &[RecordTransform]) -> Vec<CompiledTransform> {
        transforms.iter().map(|t| compile(t).unwrap()).collect()
    }

    // ── Custom (always available) ─────────────────────────────────────────────

    #[test]
    fn test_custom_adds_field() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::custom(|mut v| {
                if let Value::Object(ref mut m) = v {
                    m.insert("added".to_string(), json!(true));
                }
                v
            })]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["added"], true);
    }

    #[test]
    fn test_custom_removes_field() {
        let record = json!({"id": 1, "secret": "drop_me"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::custom(|mut v| {
                if let Value::Object(ref mut m) = v {
                    m.remove("secret");
                }
                v
            })]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("secret").is_none());
    }

    #[test]
    fn test_no_transforms_is_identity() {
        let record = json!({"id": 1, "name": "Alice"});
        let result = apply_all(record.clone(), &[]);
        assert_eq!(result, record);
    }

    // ── Flatten ───────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_nested_object() {
        let record = json!({"a": {"b": 1, "c": {"d": 2}}, "e": 3});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        );
        assert_eq!(result["a__b"], 1);
        assert_eq!(result["a__c__d"], 2);
        assert_eq!(result["e"], 3);
        assert!(result.get("a").is_none(), "nested key should be removed");
    }

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_leaves_arrays_intact() {
        let record = json!({"tags": ["rust", "api"], "meta": {"count": 2}});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: ".".into(),
            }]),
        );
        assert_eq!(result["tags"], json!(["rust", "api"]));
        assert_eq!(result["meta.count"], 2);
    }

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_already_flat() {
        let record = json!({"id": 1, "name": "Alice"});
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn test_flatten_empty_separator() {
        let record = json!({"a": {"b": 1}});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: "".into(),
            }]),
        );
        assert_eq!(result["ab"], 1);
    }

    // ── RenameKeys ────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_strips_prefix() {
        let record = json!({"_prefix_id": 1, "_prefix_name": "Alice"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameKeys {
                pattern: r"^_prefix_".into(),
                replacement: "".into(),
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_uppercase_to_placeholder() {
        let record = json!({"OUTER": {"INNER": 42}});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameKeys {
                pattern: r"[A-Z]+".into(),
                replacement: "x".into(),
            }]),
        );
        assert_eq!(result["x"]["x"], 42);
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_in_array_elements() {
        let record = json!({"items": [{"KEY": 1}, {"KEY": 2}]});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameKeys {
                pattern: r"KEY".into(),
                replacement: "key".into(),
            }]),
        );
        assert_eq!(result["items"][0]["key"], 1);
        assert_eq!(result["items"][1]["key"], 2);
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_invalid_regex_errors_at_compile() {
        let err = compile(&RecordTransform::RenameKeys {
            pattern: "[invalid".into(),
            replacement: "".into(),
        });
        assert!(err.is_err());
        assert!(matches!(err, Err(FaucetError::Transform(_))));
    }

    #[cfg(feature = "transform-rename-keys")]
    #[test]
    fn test_rename_keys_chained() {
        let record = json!({"__camelCase__": 1});
        let result = apply_all(
            record,
            &compiled(&[
                RecordTransform::RenameKeys {
                    pattern: r"^_+|_+$".into(),
                    replacement: "".into(),
                },
                RecordTransform::RenameKeys {
                    pattern: r"[A-Z]".into(),
                    replacement: "_".into(),
                },
            ]),
        );
        let key = result.as_object().unwrap().keys().next().unwrap().clone();
        assert_eq!(key, "camel_ase");
    }

    // ── Chaining ──────────────────────────────────────────────────────────────

    #[cfg(all(feature = "transform-keys-case", feature = "transform-flatten"))]
    #[test]
    fn test_keys_case_then_flatten() {
        let record = json!({"User Info": {"First Name": "Alice", "Last Name": "Smith"}});
        let result = apply_all(
            record,
            &compiled(&[
                RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                    on_collision: KeyCollision::Error,
                },
                RecordTransform::Flatten {
                    separator: "_".into(),
                },
            ]),
        );
        assert_eq!(result["user_info_first_name"], "Alice");
        assert_eq!(result["user_info_last_name"], "Smith");
    }

    #[test]
    fn test_custom_chained_with_builtin() {
        // Custom runs before (or after) built-ins — ordering is preserved.
        let record = json!({"id": 1, "raw_value": 100});
        let result = apply_all(
            record,
            &compiled(&[
                // Step 1: custom — double raw_value
                RecordTransform::custom(|mut v| {
                    if let Some(n) = v.get("raw_value").and_then(|n| n.as_i64())
                        && let Value::Object(ref mut m) = v
                    {
                        m.insert("raw_value".to_string(), json!(n * 2));
                    }
                    v
                }),
                // Step 2: custom — rename raw_value → value
                RecordTransform::custom(|mut v| {
                    if let Value::Object(ref mut m) = v
                        && let Some(val) = m.remove("raw_value")
                    {
                        m.insert("value".to_string(), val);
                    }
                    v
                }),
            ]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["value"], 200);
        assert!(result.get("raw_value").is_none());
    }

    // ── #78/#28: collisions must error, not silently drop ──────────────────

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn flatten_key_collision_errors() {
        // `a__b` (literal) and `a.b` (nested) both flatten to `a__b`.
        let record = json!({"a__b": 1, "a": {"b": 2}});
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        )
        .expect_err("colliding flattened keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("a__b"), "{err}");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_collision_errors_by_default() {
        // `AccountId` and `account_id` both snake-case to `account_id`.
        let record = json!({"AccountId": 1, "account_id": 2});
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: KeyCollision::Error,
            }]),
        )
        .expect_err("colliding re-cased keys must error by default, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("account_id"), "{err}");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_collision_suffix_disambiguates() {
        let record = json!({"AccountId": 1, "account_id": 2});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: KeyCollision::Suffix,
            }]),
        );
        let obj = result.as_object().unwrap();
        // Both columns survive under unique names (order/backend-independent).
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["account_id", "account_id_2"].into_iter().collect(),
            "got {keys:?}"
        );
        let vals: std::collections::BTreeSet<i64> =
            obj.values().filter_map(Value::as_i64).collect();
        assert_eq!(vals, [1, 2].into_iter().collect(), "both values preserved");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_collision_suffix_three_way() {
        // `Foo`, `foo`, `FOO` all snake-case to `foo`.
        let record = json!({"Foo": 1, "foo": 2, "FOO": 3});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
                on_collision: KeyCollision::Suffix,
            }]),
        );
        let obj = result.as_object().unwrap();
        let keys: std::collections::BTreeSet<&str> = obj.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            ["foo", "foo_2", "foo_3"].into_iter().collect(),
            "got {keys:?}"
        );
        let vals: std::collections::BTreeSet<i64> =
            obj.values().filter_map(Value::as_i64).collect();
        assert_eq!(vals, [1, 2, 3].into_iter().collect());
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_collision_suffix_is_order_stable() {
        let record = json!({"AccountId": 1, "account_id": 2, "Account_Id": 3});
        let run = || {
            apply_all(
                record.clone(),
                &compiled(&[RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                    on_collision: KeyCollision::Suffix,
                }]),
            )
        };
        // Deterministic: same input yields byte-identical output across runs.
        assert_eq!(run(), run());
    }

    // ── Select ────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_keeps_only_listed_fields() {
        let record = json!({"id": 1, "name": "Alice", "secret": "drop"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Select {
                fields: vec!["id".into(), "name".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
        assert!(result.get("secret").is_none());
    }

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_missing_field_is_no_op() {
        // Listed field is absent — must not introduce a null.
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Select {
                fields: vec!["id".into(), "missing".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("missing").is_none());
    }

    #[cfg(feature = "transform-select")]
    #[test]
    fn select_passes_through_non_object() {
        let record = json!([1, 2, 3]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Select {
                fields: vec!["id".into()],
            }]),
        );
        assert_eq!(result, record);
    }

    // ── Drop ──────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-drop")]
    #[test]
    fn drop_removes_listed_fields() {
        let record = json!({"id": 1, "ssn": "111-22-3333", "name": "Alice"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Drop {
                fields: vec!["ssn".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["name"], "Alice");
        assert!(result.get("ssn").is_none());
    }

    #[cfg(feature = "transform-drop")]
    #[test]
    fn drop_missing_field_is_no_op() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Drop {
                fields: vec!["missing".into()],
            }]),
        );
        assert_eq!(result["id"], 1);
    }

    // ── Set ───────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_inserts_new_fields() {
        let record = json!({"id": 1});
        let mut values = Map::new();
        values.insert("_source".into(), json!("api"));
        values.insert("ingested_at".into(), json!("2026-01-01"));
        let result = apply_all(record, &compiled(&[RecordTransform::Set { values }]));
        assert_eq!(result["id"], 1);
        assert_eq!(result["_source"], "api");
        assert_eq!(result["ingested_at"], "2026-01-01");
    }

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_overwrites_existing_field() {
        let record = json!({"_source": "old", "id": 1});
        let mut values = Map::new();
        values.insert("_source".into(), json!("new"));
        let result = apply_all(record, &compiled(&[RecordTransform::Set { values }]));
        assert_eq!(result["_source"], "new");
        assert_eq!(result["id"], 1);
    }

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_supports_any_json_value() {
        let record = json!({});
        let mut values = Map::new();
        values.insert("n".into(), json!(42));
        values.insert("b".into(), json!(true));
        values.insert("arr".into(), json!([1, 2]));
        values.insert("obj".into(), json!({"k": "v"}));
        values.insert("null".into(), Value::Null);
        let result = apply_all(record, &compiled(&[RecordTransform::Set { values }]));
        assert_eq!(result["n"], 42);
        assert_eq!(result["b"], true);
        assert_eq!(result["arr"], json!([1, 2]));
        assert_eq!(result["obj"]["k"], "v");
        assert_eq!(result["null"], Value::Null);
    }

    // ── RenameField ───────────────────────────────────────────────────────────

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_renames_exact_key() {
        let record = json!({"old_name": 1, "keep": 2});
        let mut fields = HashMap::new();
        fields.insert("old_name".to_owned(), "new_name".to_owned());
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        );
        assert_eq!(result["new_name"], 1);
        assert_eq!(result["keep"], 2);
        assert!(result.get("old_name").is_none());
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_missing_source_is_no_op() {
        let record = json!({"id": 1});
        let mut fields = HashMap::new();
        fields.insert("missing".to_owned(), "renamed".to_owned());
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("renamed").is_none());
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_target_collision_errors() {
        let record = json!({"a": 1, "b": 2});
        let mut fields = HashMap::new();
        fields.insert("a".to_owned(), "b".to_owned());
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        )
        .expect_err("collision must error, not overwrite");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("'b'"), "{err}");
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_swap_is_deterministic() {
        // A swap {a:b, b:a} must exchange the two values, never error or corrupt,
        // and must be stable across HashMap iteration orders (run repeatedly).
        for _ in 0..50 {
            let record = json!({"a": 1, "b": 2, "keep": 3});
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), "b".to_owned());
            fields.insert("b".to_owned(), "a".to_owned());
            let result = apply_all(
                record,
                &compiled(&[RecordTransform::RenameField { fields }]),
            );
            assert_eq!(result["a"], 2, "{result}");
            assert_eq!(result["b"], 1, "{result}");
            assert_eq!(result["keep"], 3);
        }
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_chain_applies_against_original_snapshot() {
        // A chain {a:b, b:c} renames against the ORIGINAL record: a→b and b→c
        // both read pre-rename values, deterministically, for any iteration order.
        for _ in 0..50 {
            let record = json!({"a": 1, "b": 2});
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), "b".to_owned());
            fields.insert("b".to_owned(), "c".to_owned());
            let result = apply_all(
                record,
                &compiled(&[RecordTransform::RenameField { fields }]),
            );
            assert_eq!(result["b"], 1, "{result}");
            assert_eq!(result["c"], 2, "{result}");
            assert!(result.get("a").is_none(), "{result}");
        }
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_two_sources_one_target_errors() {
        let record = json!({"a": 1, "b": 2});
        let mut fields = HashMap::new();
        fields.insert("a".to_owned(), "c".to_owned());
        fields.insert("b".to_owned(), "c".to_owned());
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        )
        .expect_err("two renames to the same target must error");
        assert!(format!("{err}").contains("same target"), "{err}");
    }

    // ── Cast ──────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-cast")]
    fn cast_specs(field: &str, ty: CastType, on_error: CastOnError) -> Vec<RecordTransform> {
        let mut fields = HashMap::new();
        fields.insert(field.to_owned(), ty);
        vec![RecordTransform::Cast { fields, on_error }]
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_int() {
        let record = json!({"age": "42"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result["age"], 42);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_whole_number_float_to_int_succeeds() {
        // A float with no fractional part and within i64 range converts.
        let record = json!({"n": 5.0});
        let result = apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result["n"], 5);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_fractional_float_to_int_errors_under_on_error_error() {
        // A fractional float must surface an error, not silently truncate to 3.
        let record = json!({"n": 3.9});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Error)),
        )
        .expect_err("a fractional float must not silently truncate to int");
        assert!(matches!(err, FaucetError::Transform(_)), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_out_of_range_float_to_int_errors_under_on_error_error() {
        // A float beyond i64 range must error, not silently saturate to i64::MAX.
        let record = json!({"n": 1e30});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Error)),
        )
        .expect_err("an out-of-range float must not silently saturate to i64::MAX");
        assert!(matches!(err, FaucetError::Transform(_)), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_fractional_float_to_int_nulls_under_on_error_null() {
        let record = json!({"n": 3.9});
        let result = apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Null)),
        );
        assert_eq!(result["n"], Value::Null);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_float() {
        let record = json!({"price": "9.99"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("price", CastType::Float, CastOnError::Error)),
        );
        assert_eq!(result["price"], 9.99);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_bool() {
        for input in ["true", "TRUE", "1", "yes"] {
            let record = json!({"flag": input});
            let result = apply_all(
                record,
                &compiled(&cast_specs("flag", CastType::Bool, CastOnError::Error)),
            );
            assert_eq!(result["flag"], true, "input was {input:?}");
        }
        for input in ["false", "0", "no"] {
            let record = json!({"flag": input});
            let result = apply_all(
                record,
                &compiled(&cast_specs("flag", CastType::Bool, CastOnError::Error)),
            );
            assert_eq!(result["flag"], false, "input was {input:?}");
        }
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_number_to_string() {
        let record = json!({"id": 42});
        let result = apply_all(
            record,
            &compiled(&cast_specs("id", CastType::String, CastOnError::Error)),
        );
        assert_eq!(result["id"], "42");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_timestamp_normalises() {
        let record = json!({"ts": "2026-05-28T12:34:56+00:00"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("ts", CastType::Timestamp, CastOnError::Error)),
        );
        // `+00:00` normalises to `Z` via chrono's RFC 3339 emitter.
        assert_eq!(result["ts"], "2026-05-28T12:34:56Z");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_on_error_error_propagates() {
        let record = json!({"age": "not a number"});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Error)),
        )
        .expect_err("uncastable value must error under on_error=error");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("'age'"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_on_error_null_replaces() {
        let record = json!({"age": "not a number"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Null)),
        );
        assert_eq!(result["age"], Value::Null);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_on_error_skip_leaves_value() {
        let record = json!({"age": "not a number"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("age", CastType::Int, CastOnError::Skip)),
        );
        assert_eq!(result["age"], "not a number");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_missing_field_is_no_op() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&cast_specs("missing", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("missing").is_none());
    }

    // ── Redact ────────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_replaces_value_with_mask() {
        let record = json!({"id": 1, "ssn": "111-22-3333", "email": "x@y.z"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Redact {
                fields: vec!["ssn".into(), "email".into()],
                mask: json!("***"),
            }]),
        );
        assert_eq!(result["id"], 1);
        assert_eq!(result["ssn"], "***");
        assert_eq!(result["email"], "***");
    }

    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_missing_field_does_not_insert_mask() {
        let record = json!({"id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Redact {
                fields: vec!["ssn".into()],
                mask: json!("***"),
            }]),
        );
        assert_eq!(result["id"], 1);
        assert!(result.get("ssn").is_none());
    }

    // ── ValueCase ─────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_lower() {
        let record = json!({"email": "User@Example.COM", "id": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["email".into()],
                mode: ValueCaseMode::Lower,
            }]),
        );
        assert_eq!(result["email"], "user@example.com");
        assert_eq!(result["id"], 1);
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_upper() {
        let record = json!({"code": "abc"});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["code".into()],
                mode: ValueCaseMode::Upper,
            }]),
        );
        assert_eq!(result["code"], "ABC");
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_trim() {
        let record = json!({"name": "  Alice  "});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["name".into()],
                mode: ValueCaseMode::Trim,
            }]),
        );
        assert_eq!(result["name"], "Alice");
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_passes_non_string_through() {
        let record = json!({"id": 42});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["id".into()],
                mode: ValueCaseMode::Upper,
            }]),
        );
        assert_eq!(result["id"], 42);
    }

    // ── SpellSymbols ──────────────────────────────────────────────────────────

    #[cfg(feature = "transform-spell-symbols")]
    fn spell_default() -> Vec<RecordTransform> {
        vec![RecordTransform::SpellSymbols {
            extra: HashMap::new(),
            separator: " ".into(),
        }]
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_replaces_common_symbols() {
        let record = json!({"%sold": 1, "C#course": 2, "$amount": 3});
        let result = apply_all(record, &compiled(&spell_default()));
        // Defaults insert " " around each replacement so a downstream
        // snake_case picks up the word boundary.
        assert!(result.get(" percent sold").is_some());
        assert!(result.get("C number course").is_some());
        assert!(result.get(" dollar amount").is_some());
    }

    #[cfg(all(feature = "transform-spell-symbols", feature = "transform-keys-case"))]
    #[test]
    fn spell_symbols_then_keys_case_pipeline() {
        let record = json!({"% sold": 10, "C# courses": 20});
        let result = super::apply_all(
            record,
            &compiled(&[
                RecordTransform::SpellSymbols {
                    extra: HashMap::new(),
                    separator: " ".into(),
                },
                RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                    on_collision: KeyCollision::Error,
                },
            ]),
        )
        .expect("pipeline must succeed");
        assert_eq!(result["percent_sold"], 10);
        assert_eq!(result["c_number_courses"], 20);
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_extra_overrides_defaults() {
        let mut extra = HashMap::new();
        extra.insert("#".to_owned(), "hash".to_owned());
        extra.insert("©".to_owned(), "copyright".to_owned());
        let record = json!({"#tag": 1, "©2026": 2});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::SpellSymbols {
                extra,
                separator: " ".into(),
            }]),
        );
        // `#` override beats the default `"number"`.
        assert!(result.get(" hash tag").is_some());
        // `©` is not in the default map but the user added it.
        assert!(result.get(" copyright 2026").is_some());
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_longest_match_wins() {
        // Without longest-first ordering, `"<"` would shadow `"<="`.
        let mut extra = HashMap::new();
        extra.insert("<=".to_owned(), "lte".to_owned());
        let record = json!({"a<=b": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::SpellSymbols {
                extra,
                separator: " ".into(),
            }]),
        );
        assert!(result.get("a lte b").is_some());
        // Confirm `<` alone was NOT applied separately.
        assert!(result.get("a lt = b").is_none());
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_recursive_into_objects_and_arrays() {
        let record = json!({"outer&": {"inner%": [{"deep#": 1}]}});
        let result = apply_all(record, &compiled(&spell_default()));
        let outer_key = result.as_object().unwrap().keys().next().unwrap().clone();
        assert!(outer_key.contains("and"), "outer key was {outer_key:?}");
        let inner = &result[&outer_key];
        let inner_key = inner.as_object().unwrap().keys().next().unwrap().clone();
        assert!(inner_key.contains("percent"), "inner key was {inner_key:?}");
        let deep = &inner[&inner_key][0];
        let deep_key = deep.as_object().unwrap().keys().next().unwrap().clone();
        assert!(deep_key.contains("number"), "deep key was {deep_key:?}");
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_key_collision_errors() {
        // With separator "" both keys collapse to "percent".
        let record = json!({"%": 1, "percent": 2});
        let err = super::apply_all(
            record,
            &compiled(&[RecordTransform::SpellSymbols {
                extra: HashMap::new(),
                separator: "".into(),
            }]),
        )
        .expect_err("colliding spelled keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("percent"), "{err}");
    }

    // ── KeysCase ──────────────────────────────────────────────────────────────

    #[cfg(feature = "transform-keys-case")]
    fn keys_case_specs(mode: KeyCaseMode) -> Vec<RecordTransform> {
        vec![RecordTransform::KeysCase {
            mode,
            on_collision: KeyCollision::Error,
        }]
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_snake() {
        let record = json!({"First Name": 1, "last-name": 2, "ID": 3});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        assert_eq!(result["first_name"], 1);
        assert_eq!(result["last_name"], 2);
        assert_eq!(result["id"], 3);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_camel_from_various_inputs() {
        // snake → camel
        let record = json!({"first_name": 1, "User ID": 2, "kebab-case": 3, "PascalCase": 4});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Camel)));
        assert_eq!(result["firstName"], 1);
        assert_eq!(result["userId"], 2);
        assert_eq!(result["kebabCase"], 3);
        assert_eq!(result["pascalCase"], 4);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_pascal() {
        let record = json!({"first_name": 1, "second name": 2});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Pascal)));
        assert_eq!(result["FirstName"], 1);
        assert_eq!(result["SecondName"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_kebab() {
        let record = json!({"firstName": 1, "second_name": 2});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Kebab)));
        assert_eq!(result["first-name"], 1);
        assert_eq!(result["second-name"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_screaming_snake() {
        let record = json!({"firstName": 1, "second name": 2});
        let result = apply_all(
            record,
            &compiled(&keys_case_specs(KeyCaseMode::ScreamingSnake)),
        );
        assert_eq!(result["FIRST_NAME"], 1);
        assert_eq!(result["SECOND_NAME"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_recursive_into_nested() {
        let record = json!({"User Info": {"First Name": "Alice", "items": [{"Tag Name": "x"}]}});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        assert_eq!(result["user_info"]["first_name"], "Alice");
        assert_eq!(result["user_info"]["items"][0]["tag_name"], "x");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_collision_errors() {
        // "firstName" and "first_name" both snake_case to "first_name".
        let record = json!({"firstName": 1, "first_name": 2});
        let err = super::apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)))
            .expect_err("colliding re-cased keys must error, not drop a value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("first_name"), "{err}");
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_all_symbol_key_kept_as_is() {
        // A key that tokenises to nothing must keep its original form rather
        // than producing an empty-string key.
        let record = json!({"!@#": 1, "id": 2});
        let result = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        assert_eq!(result["!@#"], 1);
        assert_eq!(result["id"], 2);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_idempotent_in_target_mode() {
        // Re-running the transform should be a no-op once keys are already in
        // the target shape.
        let record = json!({"first_name": 1});
        let once = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        let twice = apply_all(
            once.clone(),
            &compiled(&keys_case_specs(KeyCaseMode::Snake)),
        );
        assert_eq!(once, twice);
    }

    #[cfg(feature = "transform-spell-symbols")]
    #[test]
    fn spell_symbols_handles_unicode_keys() {
        // A non-ASCII char with a UTF-8 length > 1 must not corrupt the walk.
        let record = json!({"café%": 1});
        let result = apply_all(record, &compiled(&spell_default()));
        let key = result.as_object().unwrap().keys().next().unwrap().clone();
        assert!(key.contains("café"), "key was {key:?}");
        assert!(key.contains("percent"), "key was {key:?}");
    }

    // ── Debug formatting for every RecordTransform variant ─────────────────────

    #[test]
    fn debug_record_transform_all_variants() {
        // Custom is always available.
        let dbg = format!("{:?}", RecordTransform::custom(|v| v));
        assert_eq!(dbg, "Custom(<fn>)");

        #[cfg(feature = "transform-flatten")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Flatten {
                    separator: "__".into()
                }
            );
            assert!(dbg.starts_with("Flatten"), "{dbg}");
            assert!(dbg.contains("separator"), "{dbg}");
            assert!(dbg.contains("__"), "{dbg}");
        }
        #[cfg(feature = "transform-rename-keys")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::RenameKeys {
                    pattern: "p".into(),
                    replacement: "r".into(),
                }
            );
            assert!(dbg.starts_with("RenameKeys"), "{dbg}");
            assert!(dbg.contains("pattern"), "{dbg}");
            assert!(dbg.contains("replacement"), "{dbg}");
        }
        #[cfg(feature = "transform-keys-case")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                    on_collision: Default::default(),
                }
            );
            assert!(dbg.starts_with("KeysCase"), "{dbg}");
            assert!(dbg.contains("Snake"), "{dbg}");
        }
        #[cfg(feature = "transform-select")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Select {
                    fields: vec!["a".into()]
                }
            );
            assert!(dbg.starts_with("Select"), "{dbg}");
            assert!(dbg.contains("fields"), "{dbg}");
        }
        #[cfg(feature = "transform-drop")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Drop {
                    fields: vec!["a".into()]
                }
            );
            assert!(dbg.starts_with("Drop"), "{dbg}");
        }
        #[cfg(feature = "transform-set")]
        {
            let mut values = Map::new();
            values.insert("k".into(), json!("v"));
            let dbg = format!("{:?}", RecordTransform::Set { values });
            assert!(dbg.starts_with("Set"), "{dbg}");
            assert!(dbg.contains("values"), "{dbg}");
        }
        #[cfg(feature = "transform-rename-field")]
        {
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), "b".to_owned());
            let dbg = format!("{:?}", RecordTransform::RenameField { fields });
            assert!(dbg.starts_with("RenameField"), "{dbg}");
        }
        #[cfg(feature = "transform-cast")]
        {
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), CastType::Int);
            let dbg = format!(
                "{:?}",
                RecordTransform::Cast {
                    fields,
                    on_error: CastOnError::Error,
                }
            );
            assert!(dbg.starts_with("Cast"), "{dbg}");
            assert!(dbg.contains("on_error"), "{dbg}");
        }
        #[cfg(feature = "transform-redact")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Redact {
                    fields: vec!["a".into()],
                    mask: json!("***"),
                }
            );
            assert!(dbg.starts_with("Redact"), "{dbg}");
            assert!(dbg.contains("mask"), "{dbg}");
        }
        #[cfg(feature = "transform-value-case")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::ValueCase {
                    fields: vec!["a".into()],
                    mode: ValueCaseMode::Lower,
                }
            );
            assert!(dbg.starts_with("ValueCase"), "{dbg}");
            assert!(dbg.contains("mode"), "{dbg}");
        }
        #[cfg(feature = "transform-spell-symbols")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::SpellSymbols {
                    extra: HashMap::new(),
                    separator: " ".into(),
                }
            );
            assert!(dbg.starts_with("SpellSymbols"), "{dbg}");
            assert!(dbg.contains("separator"), "{dbg}");
        }
        #[cfg(feature = "transform-hash")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Hash {
                    fields: vec!["a".into()],
                    algorithm: HashAlgorithm::Blake3,
                    encoding: HashEncoding::Base64,
                    salt: Some("s".into()),
                    into: Some("a_hash".into()),
                }
            );
            assert!(dbg.starts_with("Hash"), "{dbg}");
            assert!(dbg.contains("algorithm"), "{dbg}");
            assert!(dbg.contains("encoding"), "{dbg}");
        }
        #[cfg(feature = "transform-json-parse")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::JsonParse {
                    fields: vec!["a".into()],
                    on_error: JsonParseOnError::Null,
                    into: None,
                }
            );
            assert!(dbg.starts_with("JsonParse"), "{dbg}");
            assert!(dbg.contains("on_error"), "{dbg}");
        }
        #[cfg(feature = "transform-coalesce")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Coalesce {
                    field: "a".into(),
                    default: Some(json!("x")),
                    from: vec![],
                    treat_empty_string_as_null: true,
                }
            );
            assert!(dbg.starts_with("Coalesce"), "{dbg}");
            assert!(dbg.contains("treat_empty_string_as_null"), "{dbg}");
        }
        #[cfg(feature = "transform-split-join")]
        {
            let dbg = format!(
                "{:?}",
                RecordTransform::Split {
                    field: "a".into(),
                    delimiter: ",".into(),
                    trim: true,
                    into: None,
                }
            );
            assert!(dbg.starts_with("Split"), "{dbg}");
            assert!(dbg.contains("delimiter"), "{dbg}");
            let dbg = format!(
                "{:?}",
                RecordTransform::Join {
                    field: "a".into(),
                    delimiter: ",".into(),
                    into: None,
                }
            );
            assert!(dbg.starts_with("Join"), "{dbg}");
        }
    }

    // ── Clone for every RecordTransform variant (refcount bump on Custom) ──────

    #[test]
    fn clone_record_transform_custom_preserves_behaviour() {
        let original = RecordTransform::custom(|mut v| {
            if let Value::Object(ref mut m) = v {
                m.insert("cloned".into(), json!(true));
            }
            v
        });
        let cloned = original.clone();
        assert_eq!(format!("{cloned:?}"), "Custom(<fn>)");
        let out = apply_all(json!({"id": 1}), &compiled(&[cloned]));
        assert_eq!(out["cloned"], true);
        assert_eq!(out["id"], 1);
    }

    #[test]
    // Every push below is #[cfg(feature)]-gated, so a vec![] literal can't
    // express this; suppress the vec-init-then-push lint for the whole test.
    #[allow(clippy::vec_init_then_push)]
    fn clone_record_transform_all_builtin_variants() {
        let mut variants: Vec<RecordTransform> = Vec::new();
        #[cfg(feature = "transform-flatten")]
        variants.push(RecordTransform::Flatten {
            separator: "__".into(),
        });
        #[cfg(feature = "transform-rename-keys")]
        variants.push(RecordTransform::RenameKeys {
            pattern: "p".into(),
            replacement: "r".into(),
        });
        #[cfg(feature = "transform-keys-case")]
        variants.push(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
            on_collision: KeyCollision::Error,
        });
        #[cfg(feature = "transform-select")]
        variants.push(RecordTransform::Select {
            fields: vec!["a".into()],
        });
        #[cfg(feature = "transform-drop")]
        variants.push(RecordTransform::Drop {
            fields: vec!["a".into()],
        });
        #[cfg(feature = "transform-set")]
        {
            let mut values = Map::new();
            values.insert("k".into(), json!("v"));
            variants.push(RecordTransform::Set { values });
        }
        #[cfg(feature = "transform-rename-field")]
        {
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), "b".to_owned());
            variants.push(RecordTransform::RenameField { fields });
        }
        #[cfg(feature = "transform-cast")]
        {
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), CastType::Int);
            variants.push(RecordTransform::Cast {
                fields,
                on_error: CastOnError::Error,
            });
        }
        #[cfg(feature = "transform-redact")]
        variants.push(RecordTransform::Redact {
            fields: vec!["a".into()],
            mask: json!("***"),
        });
        #[cfg(feature = "transform-value-case")]
        variants.push(RecordTransform::ValueCase {
            fields: vec!["a".into()],
            mode: ValueCaseMode::Lower,
        });
        #[cfg(feature = "transform-spell-symbols")]
        variants.push(RecordTransform::SpellSymbols {
            extra: HashMap::new(),
            separator: " ".into(),
        });
        #[cfg(feature = "transform-hash")]
        variants.push(RecordTransform::Hash {
            fields: vec!["a".into()],
            algorithm: HashAlgorithm::Sha256,
            encoding: HashEncoding::Hex,
            salt: Some("s".into()),
            into: None,
        });
        #[cfg(feature = "transform-json-parse")]
        variants.push(RecordTransform::JsonParse {
            fields: vec!["a".into()],
            on_error: JsonParseOnError::Keep,
            into: Some("b".into()),
        });
        #[cfg(feature = "transform-coalesce")]
        variants.push(RecordTransform::Coalesce {
            field: "a".into(),
            default: None,
            from: vec!["b".into()],
            treat_empty_string_as_null: false,
        });
        #[cfg(feature = "transform-split-join")]
        {
            variants.push(RecordTransform::Split {
                field: "a".into(),
                delimiter: ",".into(),
                trim: true,
                into: None,
            });
            variants.push(RecordTransform::Join {
                field: "a".into(),
                delimiter: ",".into(),
                into: Some("b".into()),
            });
        }

        // The clone's Debug must match the original's Debug exactly.
        for v in &variants {
            let cloned = v.clone();
            assert_eq!(format!("{v:?}"), format!("{cloned:?}"));
        }
    }

    #[test]
    fn clone_compiled_transform_all_variants() {
        let mut specs: Vec<RecordTransform> = vec![RecordTransform::custom(|v| v)];
        #[cfg(feature = "transform-flatten")]
        specs.push(RecordTransform::Flatten {
            separator: "__".into(),
        });
        #[cfg(feature = "transform-rename-keys")]
        specs.push(RecordTransform::RenameKeys {
            pattern: "p".into(),
            replacement: "r".into(),
        });
        #[cfg(feature = "transform-keys-case")]
        specs.push(RecordTransform::KeysCase {
            mode: KeyCaseMode::Camel,
            on_collision: KeyCollision::Error,
        });
        #[cfg(feature = "transform-select")]
        specs.push(RecordTransform::Select {
            fields: vec!["a".into()],
        });
        #[cfg(feature = "transform-drop")]
        specs.push(RecordTransform::Drop {
            fields: vec!["a".into()],
        });
        #[cfg(feature = "transform-set")]
        {
            let mut values = Map::new();
            values.insert("k".into(), json!("v"));
            specs.push(RecordTransform::Set { values });
        }
        #[cfg(feature = "transform-rename-field")]
        {
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), "b".to_owned());
            specs.push(RecordTransform::RenameField { fields });
        }
        #[cfg(feature = "transform-cast")]
        {
            let mut fields = HashMap::new();
            fields.insert("a".to_owned(), CastType::Int);
            specs.push(RecordTransform::Cast {
                fields,
                on_error: CastOnError::Null,
            });
        }
        #[cfg(feature = "transform-redact")]
        specs.push(RecordTransform::Redact {
            fields: vec!["a".into()],
            mask: json!("***"),
        });
        #[cfg(feature = "transform-value-case")]
        specs.push(RecordTransform::ValueCase {
            fields: vec!["a".into()],
            mode: ValueCaseMode::Upper,
        });
        #[cfg(feature = "transform-spell-symbols")]
        specs.push(RecordTransform::SpellSymbols {
            extra: HashMap::new(),
            separator: " ".into(),
        });
        #[cfg(feature = "transform-hash")]
        specs.push(RecordTransform::Hash {
            fields: vec!["a".into()],
            algorithm: HashAlgorithm::Blake3,
            encoding: HashEncoding::Base64,
            salt: None,
            into: None,
        });
        #[cfg(feature = "transform-json-parse")]
        specs.push(RecordTransform::JsonParse {
            fields: vec!["a".into()],
            on_error: JsonParseOnError::Error,
            into: None,
        });
        #[cfg(feature = "transform-coalesce")]
        specs.push(RecordTransform::Coalesce {
            field: "a".into(),
            default: Some(json!("x")),
            from: vec![],
            treat_empty_string_as_null: true,
        });
        #[cfg(feature = "transform-split-join")]
        {
            specs.push(RecordTransform::Split {
                field: "a".into(),
                delimiter: ",".into(),
                trim: false,
                into: None,
            });
            specs.push(RecordTransform::Join {
                field: "a".into(),
                delimiter: ",".into(),
                into: None,
            });
        }

        // Compile each, clone the compiled form, and confirm the cloned slice
        // still transforms a record identically to the original slice.
        let original = compiled(&specs);
        let cloned: Vec<CompiledTransform> = original.to_vec();
        assert_eq!(original.len(), cloned.len());
        let record = json!({"a": "1", "k": "x"});
        let out_orig = super::apply_all(record.clone(), &original);
        let out_clone = super::apply_all(record, &cloned);
        assert_eq!(
            out_orig.is_ok(),
            out_clone.is_ok(),
            "clone must transform identically"
        );
        if let (Ok(a), Ok(b)) = (out_orig, out_clone) {
            assert_eq!(a, b);
        }
    }

    // ── Non-object records pass through every object-only transform ────────────

    #[cfg(feature = "transform-flatten")]
    #[test]
    fn flatten_passes_through_non_object() {
        let record = json!([1, 2, 3]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        );
        assert_eq!(result, record);
        // A bare scalar too.
        let scalar = json!(42);
        let result = apply_all(
            scalar.clone(),
            &compiled(&[RecordTransform::Flatten {
                separator: "__".into(),
            }]),
        );
        assert_eq!(result, scalar);
    }

    #[cfg(feature = "transform-drop")]
    #[test]
    fn drop_passes_through_non_object() {
        let record = json!([1, 2]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Drop {
                fields: vec!["a".into()],
            }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-set")]
    #[test]
    fn set_passes_through_non_object() {
        let mut values = Map::new();
        values.insert("k".into(), json!("v"));
        let record = json!("scalar");
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Set { values }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_passes_through_non_object() {
        let mut fields = HashMap::new();
        fields.insert("a".to_owned(), "b".to_owned());
        let record = json!([1, 2]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::RenameField { fields }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-rename-field")]
    #[test]
    fn rename_field_same_name_is_skipped() {
        // from == to short-circuits (continue) and leaves the field intact.
        let mut fields = HashMap::new();
        fields.insert("a".to_owned(), "a".to_owned());
        let record = json!({"a": 1});
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::RenameField { fields }]),
        );
        assert_eq!(result["a"], 1);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_passes_through_non_object() {
        let record = json!([1, 2]);
        let result = apply_all(
            record.clone(),
            &compiled(&cast_specs("a", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-redact")]
    #[test]
    fn redact_passes_through_non_object() {
        let record = json!("scalar");
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Redact {
                fields: vec!["a".into()],
                mask: json!("***"),
            }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_passes_through_non_object() {
        let record = json!([1, 2]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["a".into()],
                mode: ValueCaseMode::Lower,
            }]),
        );
        assert_eq!(result, record);
    }

    // ── Cast: exhaustive per-type / per-source-value matrix ────────────────────

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_integer_number_to_int_is_identity() {
        // Number that is already an i64 takes the `as_i64()` Some branch.
        let record = json!({"n": 7});
        let result = apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Error)),
        );
        assert_eq!(result["n"], 7);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_bool_to_int() {
        let record = json!({"t": true, "f": false});
        let mut fields = HashMap::new();
        fields.insert("t".to_owned(), CastType::Int);
        fields.insert("f".to_owned(), CastType::Int);
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Cast {
                fields,
                on_error: CastOnError::Error,
            }]),
        );
        assert_eq!(result["t"], 1);
        assert_eq!(result["f"], 0);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_null_to_int_errors() {
        let record = json!({"n": null});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Error)),
        )
        .expect_err("null cannot become int");
        assert!(
            format!("{err}").contains("null cannot be cast to int"),
            "{err}"
        );
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_composite_to_int_errors() {
        let record = json!({"n": [1, 2]});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Int, CastOnError::Error)),
        )
        .expect_err("array cannot become int");
        assert!(format!("{err}").contains("composite"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_number_to_float() {
        let record = json!({"n": 5});
        let result = apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Float, CastOnError::Error)),
        );
        assert_eq!(result["n"], 5.0);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_bool_to_float() {
        let record = json!({"t": true, "f": false});
        let mut fields = HashMap::new();
        fields.insert("t".to_owned(), CastType::Float);
        fields.insert("f".to_owned(), CastType::Float);
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Cast {
                fields,
                on_error: CastOnError::Error,
            }]),
        );
        assert_eq!(result["t"], 1.0);
        assert_eq!(result["f"], 0.0);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_null_to_float_errors() {
        let record = json!({"n": null});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Float, CastOnError::Error)),
        )
        .expect_err("null cannot become float");
        assert!(
            format!("{err}").contains("null cannot be cast to float"),
            "{err}"
        );
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_composite_to_float_errors() {
        let record = json!({"n": {"x": 1}});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Float, CastOnError::Error)),
        )
        .expect_err("object cannot become float");
        assert!(format!("{err}").contains("composite"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_float_invalid_errors() {
        let record = json!({"n": "not a float"});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Float, CastOnError::Error)),
        )
        .expect_err("non-numeric string cannot become float");
        assert!(format!("{err}").contains("is not a float"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_bool_to_bool_is_identity() {
        let record = json!({"b": true});
        let result = apply_all(
            record,
            &compiled(&cast_specs("b", CastType::Bool, CastOnError::Error)),
        );
        assert_eq!(result["b"], true);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_number_to_bool() {
        let record = json!({"on": 1, "off": 0});
        let mut fields = HashMap::new();
        fields.insert("on".to_owned(), CastType::Bool);
        fields.insert("off".to_owned(), CastType::Bool);
        let result = apply_all(
            record,
            &compiled(&[RecordTransform::Cast {
                fields,
                on_error: CastOnError::Error,
            }]),
        );
        assert_eq!(result["on"], true);
        assert_eq!(result["off"], false);
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_integer_other_than_zero_one_to_bool_errors() {
        let record = json!({"n": 7});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Bool, CastOnError::Error)),
        )
        .expect_err("only 0/1 convert to bool");
        assert!(format!("{err}").contains("not 0 or 1"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_float_number_to_bool_errors() {
        // A fractional number takes the non-i64 branch ("number ... is not 0 or 1").
        let record = json!({"n": 1.5});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("n", CastType::Bool, CastOnError::Error)),
        )
        .expect_err("fractional number cannot become bool");
        assert!(format!("{err}").contains("not 0 or 1"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_unrecognised_string_to_bool_errors() {
        let record = json!({"flag": "maybe"});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("flag", CastType::Bool, CastOnError::Error)),
        )
        .expect_err("'maybe' is not a boolean");
        assert!(
            format!("{err}").contains("not a recognised boolean"),
            "{err}"
        );
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_null_to_bool_errors() {
        let record = json!({"b": null});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("b", CastType::Bool, CastOnError::Error)),
        )
        .expect_err("null cannot become bool");
        assert!(
            format!("{err}").contains("null cannot be cast to bool"),
            "{err}"
        );
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_composite_to_bool_errors() {
        let record = json!({"b": [true]});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("b", CastType::Bool, CastOnError::Error)),
        )
        .expect_err("array cannot become bool");
        assert!(format!("{err}").contains("composite"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_string_to_string_is_identity() {
        let record = json!({"s": "hello"});
        let result = apply_all(
            record,
            &compiled(&cast_specs("s", CastType::String, CastOnError::Error)),
        );
        assert_eq!(result["s"], "hello");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_bool_to_string() {
        let record = json!({"b": true});
        let result = apply_all(
            record,
            &compiled(&cast_specs("b", CastType::String, CastOnError::Error)),
        );
        assert_eq!(result["b"], "true");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_null_to_string_errors() {
        let record = json!({"s": null});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("s", CastType::String, CastOnError::Error)),
        )
        .expect_err("null cannot become string");
        assert!(
            format!("{err}").contains("null cannot be cast to string"),
            "{err}"
        );
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_composite_to_string_errors() {
        let record = json!({"s": {"a": 1}});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("s", CastType::String, CastOnError::Error)),
        )
        .expect_err("object cannot become string");
        assert!(format!("{err}").contains("composite"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_invalid_timestamp_string_errors() {
        let record = json!({"ts": "not a date"});
        let err = super::apply_all(
            record,
            &compiled(&cast_specs("ts", CastType::Timestamp, CastOnError::Error)),
        )
        .expect_err("invalid timestamp string");
        assert!(format!("{err}").contains("RFC 3339"), "{err}");
    }

    #[cfg(feature = "transform-cast")]
    #[test]
    fn cast_non_string_to_timestamp_names_the_type() {
        // Each non-string source exercises a distinct arm of value_type_name.
        for (val, ty_name) in [
            (json!(null), "null"),
            (json!(true), "bool"),
            (json!(42), "number"),
            (json!([1, 2]), "array"),
            (json!({"a": 1}), "object"),
        ] {
            let record = json!({ "ts": val });
            let err = super::apply_all(
                record,
                &compiled(&cast_specs("ts", CastType::Timestamp, CastOnError::Error)),
            )
            .expect_err("non-string cannot become timestamp");
            let msg = format!("{err}");
            assert!(msg.contains("timestamp"), "{msg}");
            assert!(
                msg.contains(ty_name),
                "expected type name {ty_name:?} in: {msg}"
            );
        }
    }

    // ── Hash (#403) ─────────────────────────────────────────────────────────

    #[cfg(feature = "transform-hash")]
    fn hash_spec(fields: &[&str], enc: HashEncoding, salt: Option<&str>) -> Vec<RecordTransform> {
        vec![RecordTransform::Hash {
            fields: fields.iter().map(|s| (*s).to_owned()).collect(),
            algorithm: HashAlgorithm::Sha256,
            encoding: enc,
            salt: salt.map(str::to_owned),
            into: None,
        }]
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_replaces_in_place_and_is_stable() {
        let a = apply_all(
            json!({"email": "a@b.com", "id": 1}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        let b = apply_all(
            json!({"email": "a@b.com", "id": 1}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        // Deterministic (same input → same token) and id untouched.
        assert_eq!(a["email"], b["email"]);
        assert_eq!(a["id"], 1);
        // Known SHA-256 hex of "a@b.com".
        assert_eq!(a["email"].as_str().unwrap().len(), 64);
        assert_ne!(a["email"], json!("a@b.com"));
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_salt_changes_output() {
        let unsalted = apply_all(
            json!({"email": "a@b.com"}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        let salted = apply_all(
            json!({"email": "a@b.com"}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, Some("pepper"))),
        );
        assert_ne!(unsalted["email"], salted["email"]);
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_hex_vs_base64_differ_and_both_decode() {
        let hex = apply_all(
            json!({"v": "x"}),
            &compiled(&hash_spec(&["v"], HashEncoding::Hex, None)),
        );
        let b64 = apply_all(
            json!({"v": "x"}),
            &compiled(&hash_spec(&["v"], HashEncoding::Base64, None)),
        );
        assert_ne!(hex["v"], b64["v"]);
        // hex is 64 chars; base64 of 32 bytes is 44 chars incl. padding.
        assert_eq!(hex["v"].as_str().unwrap().len(), 64);
        assert_eq!(b64["v"].as_str().unwrap().len(), 44);
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_into_preserves_source() {
        let out = apply_all(
            json!({"email": "a@b.com"}),
            &compiled(&[RecordTransform::Hash {
                fields: vec!["email".into()],
                algorithm: HashAlgorithm::Sha256,
                encoding: HashEncoding::Hex,
                salt: None,
                into: Some("email_hash".into()),
            }]),
        );
        assert_eq!(out["email"], "a@b.com");
        assert_eq!(out["email_hash"].as_str().unwrap().len(), 64);
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_missing_field_is_no_op() {
        let out = apply_all(
            json!({"id": 1}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        assert_eq!(out, json!({"id": 1}));
    }

    /// #456 M3: null must stay null. Hashing it destroys nullability *and* gives
    /// every null-valued row the same digest — so hashing a nullable column and
    /// keying an upsert/join on it would collapse all of those rows into one.
    /// `faucet_core::masking` skips null for the same reason.
    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_leaves_null_alone() {
        let out = apply_all(
            json!({"email": null, "other": "x"}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        assert_eq!(out["email"], Value::Null, "null must not be hashed");
        assert_eq!(out["other"], json!("x"));

        // Two distinct records with a null in the hashed field must not become
        // indistinguishable.
        let a = apply_all(
            json!({"id": 1, "email": null}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        let b = apply_all(
            json!({"id": 2, "email": null}),
            &compiled(&hash_spec(&["email"], HashEncoding::Hex, None)),
        );
        assert_ne!(a, b);
        assert!(a["email"].is_null() && b["email"].is_null());
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_non_string_hashes_canonical_json() {
        // A number hashes over its canonical JSON serialization ("42").
        let out = apply_all(
            json!({"n": 42}),
            &compiled(&hash_spec(&["n"], HashEncoding::Hex, None)),
        );
        let expected = hash_string("42", HashAlgorithm::Sha256, HashEncoding::Hex, None);
        assert_eq!(out["n"], Value::String(expected));
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_blake3_differs_from_sha256() {
        let sha = apply_all(
            json!({"v": "x"}),
            &compiled(&[RecordTransform::Hash {
                fields: vec!["v".into()],
                algorithm: HashAlgorithm::Sha256,
                encoding: HashEncoding::Hex,
                salt: None,
                into: None,
            }]),
        );
        let b3 = apply_all(
            json!({"v": "x"}),
            &compiled(&[RecordTransform::Hash {
                fields: vec!["v".into()],
                algorithm: HashAlgorithm::Blake3,
                encoding: HashEncoding::Hex,
                salt: None,
                into: None,
            }]),
        );
        assert_ne!(sha["v"], b3["v"]);
        assert_eq!(b3["v"].as_str().unwrap().len(), 64);
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_empty_fields_is_config_error() {
        let res = compile(&RecordTransform::Hash {
            fields: vec![],
            algorithm: HashAlgorithm::Sha256,
            encoding: HashEncoding::Hex,
            salt: None,
            into: None,
        });
        assert!(matches!(res, Err(FaucetError::Config(_))));
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_into_with_multiple_fields_is_config_error() {
        let res = compile(&RecordTransform::Hash {
            fields: vec!["a".into(), "b".into()],
            algorithm: HashAlgorithm::Sha256,
            encoding: HashEncoding::Hex,
            salt: None,
            into: Some("x".into()),
        });
        assert!(matches!(res, Err(FaucetError::Config(_))));
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_debug_redacts_salt() {
        let dbg = format!(
            "{:?}",
            RecordTransform::Hash {
                fields: vec!["a".into()],
                algorithm: HashAlgorithm::Sha256,
                encoding: HashEncoding::Hex,
                salt: Some("supersecret".into()),
                into: None,
            }
        );
        assert!(!dbg.contains("supersecret"), "{dbg}");
        assert!(dbg.contains("redacted"), "{dbg}");
    }

    // ── JsonParse (#404) ──────────────────────────────────────────────────────

    #[cfg(feature = "transform-json-parse")]
    fn json_parse_spec(field: &str, on_error: JsonParseOnError) -> Vec<RecordTransform> {
        vec![RecordTransform::JsonParse {
            fields: vec![field.to_owned()],
            on_error,
            into: None,
        }]
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_object_string_becomes_object() {
        let out = apply_all(
            json!({"payload": "{\"a\":1,\"b\":[2,3]}"}),
            &compiled(&json_parse_spec("payload", JsonParseOnError::Keep)),
        );
        assert_eq!(out["payload"], json!({"a": 1, "b": [2, 3]}));
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_already_parsed_is_no_op() {
        let record = json!({"payload": {"a": 1}});
        let out = apply_all(
            record.clone(),
            &compiled(&json_parse_spec("payload", JsonParseOnError::Error)),
        );
        assert_eq!(out, record);
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_missing_field_is_no_op() {
        let out = apply_all(
            json!({"id": 1}),
            &compiled(&json_parse_spec("payload", JsonParseOnError::Error)),
        );
        assert_eq!(out, json!({"id": 1}));
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_invalid_keep_leaves_string() {
        let out = apply_all(
            json!({"payload": "not json"}),
            &compiled(&json_parse_spec("payload", JsonParseOnError::Keep)),
        );
        assert_eq!(out["payload"], "not json");
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_invalid_null_replaces() {
        let out = apply_all(
            json!({"payload": "not json"}),
            &compiled(&json_parse_spec("payload", JsonParseOnError::Null)),
        );
        assert_eq!(out["payload"], Value::Null);
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_invalid_error_propagates() {
        let err = super::apply_all(
            json!({"payload": "not json"}),
            &compiled(&json_parse_spec("payload", JsonParseOnError::Error)),
        )
        .expect_err("invalid JSON under on_error=error must fail");
        assert!(matches!(err, FaucetError::Transform(_)), "{err}");
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_into_writes_target() {
        let out = apply_all(
            json!({"payload": "{\"a\":1}"}),
            &compiled(&[RecordTransform::JsonParse {
                fields: vec!["payload".into()],
                on_error: JsonParseOnError::Error,
                into: Some("parsed".into()),
            }]),
        );
        assert_eq!(out["payload"], "{\"a\":1}");
        assert_eq!(out["parsed"], json!({"a": 1}));
    }

    // ── Coalesce (#405) ──────────────────────────────────────────────────────

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_default_fills_null_and_absent() {
        let spec = |field: &str| {
            vec![RecordTransform::Coalesce {
                field: field.to_owned(),
                default: Some(json!("unknown")),
                from: vec![],
                treat_empty_string_as_null: false,
            }]
        };
        // null
        let a = apply_all(json!({"status": null}), &compiled(&spec("status")));
        assert_eq!(a["status"], "unknown");
        // absent
        let b = apply_all(json!({"id": 1}), &compiled(&spec("status")));
        assert_eq!(b["status"], "unknown");
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_non_null_target_untouched() {
        let out = apply_all(
            json!({"status": "active"}),
            &compiled(&[RecordTransform::Coalesce {
                field: "status".into(),
                default: Some(json!("unknown")),
                from: vec![],
                treat_empty_string_as_null: false,
            }]),
        );
        assert_eq!(out["status"], "active");
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_from_picks_first_non_null() {
        let out = apply_all(
            json!({"status": null, "state": null, "phase": "running"}),
            &compiled(&[RecordTransform::Coalesce {
                field: "status".into(),
                default: None,
                from: vec!["status".into(), "state".into(), "phase".into()],
                treat_empty_string_as_null: false,
            }]),
        );
        assert_eq!(out["status"], "running");
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_empty_string_toggle() {
        let spec = |treat: bool| {
            vec![RecordTransform::Coalesce {
                field: "status".into(),
                default: Some(json!("unknown")),
                from: vec![],
                treat_empty_string_as_null: treat,
            }]
        };
        // Off: "" is a real value, left alone.
        let off = apply_all(json!({"status": ""}), &compiled(&spec(false)));
        assert_eq!(off["status"], "");
        // On: "" counts as null and is filled.
        let on = apply_all(json!({"status": ""}), &compiled(&spec(true)));
        assert_eq!(on["status"], "unknown");
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_from_all_null_leaves_target() {
        let out = apply_all(
            json!({"status": null, "state": null}),
            &compiled(&[RecordTransform::Coalesce {
                field: "status".into(),
                default: None,
                from: vec!["status".into(), "state".into()],
                treat_empty_string_as_null: false,
            }]),
        );
        assert_eq!(out["status"], Value::Null);
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_both_default_and_from_is_config_error() {
        let res = compile(&RecordTransform::Coalesce {
            field: "status".into(),
            default: Some(json!("x")),
            from: vec!["state".into()],
            treat_empty_string_as_null: false,
        });
        assert!(matches!(res, Err(FaucetError::Config(_))));
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_neither_default_nor_from_is_config_error() {
        let res = compile(&RecordTransform::Coalesce {
            field: "status".into(),
            default: None,
            from: vec![],
            treat_empty_string_as_null: false,
        });
        assert!(matches!(res, Err(FaucetError::Config(_))));
    }

    // ── Split / Join (#406) ──────────────────────────────────────────────────

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_basic_no_trim() {
        let out = apply_all(
            json!({"tags": "a, b ,c"}),
            &compiled(&[RecordTransform::Split {
                field: "tags".into(),
                delimiter: ",".into(),
                trim: false,
                into: None,
            }]),
        );
        assert_eq!(out["tags"], json!(["a", " b ", "c"]));
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_with_trim_keeps_empty_segments() {
        let out = apply_all(
            json!({"tags": "a, ,c,"}),
            &compiled(&[RecordTransform::Split {
                field: "tags".into(),
                delimiter: ",".into(),
                trim: true,
                into: None,
            }]),
        );
        // Empty segments are kept (documented).
        assert_eq!(out["tags"], json!(["a", "", "c", ""]));
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_empty_input_yields_single_empty() {
        let out = apply_all(
            json!({"tags": ""}),
            &compiled(&[RecordTransform::Split {
                field: "tags".into(),
                delimiter: ",".into(),
                trim: false,
                into: None,
            }]),
        );
        assert_eq!(out["tags"], json!([""]));
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_non_string_is_no_op() {
        let record = json!({"tags": [1, 2]});
        let out = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Split {
                field: "tags".into(),
                delimiter: ",".into(),
                trim: false,
                into: None,
            }]),
        );
        assert_eq!(out, record);
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_into_writes_target() {
        let out = apply_all(
            json!({"csv": "a,b"}),
            &compiled(&[RecordTransform::Split {
                field: "csv".into(),
                delimiter: ",".into(),
                trim: false,
                into: Some("arr".into()),
            }]),
        );
        assert_eq!(out["csv"], "a,b");
        assert_eq!(out["arr"], json!(["a", "b"]));
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn join_basic_and_non_string_elements() {
        let out = apply_all(
            json!({"parts": ["a", 2, true, null]}),
            &compiled(&[RecordTransform::Join {
                field: "parts".into(),
                delimiter: ",".into(),
                into: None,
            }]),
        );
        // strings raw, numbers/bools as JSON scalars, null as empty.
        assert_eq!(out["parts"], "a,2,true,");
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn join_non_array_is_no_op() {
        let record = json!({"parts": "already a string"});
        let out = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Join {
                field: "parts".into(),
                delimiter: ",".into(),
                into: None,
            }]),
        );
        assert_eq!(out, record);
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_then_join_round_trips() {
        let out = apply_all(
            json!({"tags": "a,b,c"}),
            &compiled(&[
                RecordTransform::Split {
                    field: "tags".into(),
                    delimiter: ",".into(),
                    trim: false,
                    into: None,
                },
                RecordTransform::Join {
                    field: "tags".into(),
                    delimiter: ",".into(),
                    into: None,
                },
            ]),
        );
        assert_eq!(out["tags"], "a,b,c");
    }

    // ── ValueCase: Title / Capitalize (#407) ──────────────────────────────────

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_title() {
        let out = apply_all(
            json!({"city": "new york", "id": 1}),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["city".into()],
                mode: ValueCaseMode::Title,
            }]),
        );
        assert_eq!(out["city"], "New York");
        assert_eq!(out["id"], 1);
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_title_lowercases_rest_of_word() {
        let out = apply_all(
            json!({"s": "hELLO WORLD"}),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["s".into()],
                mode: ValueCaseMode::Title,
            }]),
        );
        assert_eq!(out["s"], "Hello World");
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_capitalize() {
        let out = apply_all(
            json!({"s": "hELLO wORLD"}),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["s".into()],
                mode: ValueCaseMode::Capitalize,
            }]),
        );
        assert_eq!(out["s"], "Hello world");
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_title_is_idempotent() {
        let once = apply_all(
            json!({"s": "new york"}),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["s".into()],
                mode: ValueCaseMode::Title,
            }]),
        );
        let twice = apply_all(
            once.clone(),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["s".into()],
                mode: ValueCaseMode::Title,
            }]),
        );
        assert_eq!(once, twice);
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_title_non_string_no_op() {
        let out = apply_all(
            json!({"n": 42}),
            &compiled(&[RecordTransform::ValueCase {
                fields: vec!["n".into()],
                mode: ValueCaseMode::Title,
            }]),
        );
        assert_eq!(out["n"], 42);
    }

    // ── KeysCase: Dot (#408) ──────────────────────────────────────────────────

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_dot() {
        let out = apply_all(
            json!({"userId": 1, "First Name": 2, "kebab-case": 3}),
            &compiled(&keys_case_specs(KeyCaseMode::Dot)),
        );
        assert_eq!(out["user.id"], 1);
        assert_eq!(out["first.name"], 2);
        assert_eq!(out["kebab.case"], 3);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_dot_is_idempotent() {
        let once = apply_all(
            json!({"userId": 1}),
            &compiled(&keys_case_specs(KeyCaseMode::Dot)),
        );
        let twice = apply_all(once.clone(), &compiled(&keys_case_specs(KeyCaseMode::Dot)));
        assert_eq!(once, twice);
        assert_eq!(twice["user.id"], 1);
    }

    #[cfg(feature = "transform-keys-case")]
    #[test]
    fn keys_case_dot_matches_snake_tokenization() {
        // Dot must tokenize identically to snake/kebab — only the join differs.
        let record = json!({"XMLHttpRequest": 1, "second name": 2});
        let dot = apply_all(
            record.clone(),
            &compiled(&keys_case_specs(KeyCaseMode::Dot)),
        );
        let snake = apply_all(record, &compiled(&keys_case_specs(KeyCaseMode::Snake)));
        // Same token boundaries → snake key with '_' replaced by '.' equals dot key.
        let dot_keys: Vec<String> = dot.as_object().unwrap().keys().cloned().collect();
        let snake_keys: Vec<String> = snake.as_object().unwrap().keys().cloned().collect();
        let converted: Vec<String> = snake_keys.iter().map(|k| k.replace('_', ".")).collect();
        assert_eq!(dot_keys, converted);
    }

    // ── Coverage completeness for the new transforms ─────────────────────────

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_empty_fields_is_config_error() {
        let res = compile(&RecordTransform::JsonParse {
            fields: vec![],
            on_error: JsonParseOnError::Keep,
            into: None,
        });
        assert!(matches!(res, Err(FaucetError::Config(_))));
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_into_with_multiple_fields_is_config_error() {
        let res = compile(&RecordTransform::JsonParse {
            fields: vec!["a".into(), "b".into()],
            on_error: JsonParseOnError::Keep,
            into: Some("x".into()),
        });
        assert!(matches!(res, Err(FaucetError::Config(_))));
    }

    #[cfg(feature = "transform-hash")]
    #[test]
    fn hash_passes_through_non_object() {
        let record = json!([1, 2, 3]);
        let result = apply_all(
            record.clone(),
            &compiled(&hash_spec(&["v"], HashEncoding::Hex, None)),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-json-parse")]
    #[test]
    fn json_parse_passes_through_non_object() {
        let record = json!("scalar");
        let result = apply_all(
            record.clone(),
            &compiled(&json_parse_spec("v", JsonParseOnError::Error)),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-json-encode")]
    #[test]
    fn json_encode_stringifies_nested_only() {
        let out = apply_all(
            json!({"id": 1, "addr": {"city": "NYC"}, "tags": [1, 2], "name": "a"}),
            &compiled(&[RecordTransform::JsonEncode {
                fields: vec![
                    "addr".into(),
                    "tags".into(),
                    "name".into(),
                    "missing".into(),
                ],
            }]),
        );
        assert_eq!(out["addr"], json!("{\"city\":\"NYC\"}"));
        assert_eq!(out["tags"], json!("[1,2]"));
        assert_eq!(out["name"], json!("a")); // scalar left untouched (idempotent)
        assert_eq!(out["id"], json!(1));
    }

    #[cfg(feature = "transform-lookup")]
    fn ref_rows(v: Value) -> Vec<Map<String, Value>> {
        v.as_array()
            .unwrap()
            .iter()
            .map(|r| r.as_object().unwrap().clone())
            .collect()
    }

    #[cfg(feature = "transform-lookup")]
    #[test]
    fn lookup_enriches_on_hit_and_nulls_on_miss() {
        let spec = RecordTransform::Lookup {
            reference: ref_rows(json!([{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}])),
            on_record: "user_id".into(),
            on_ref: "id".into(),
            add: vec![("user_name".into(), "name".into())],
            on_missing: LookupOnMissing::Null,
        };
        let hit = apply_all(
            json!({"user_id": 1}),
            &compiled(std::slice::from_ref(&spec)),
        );
        assert_eq!(hit["user_name"], json!("Alice"));
        let miss = apply_all(json!({"user_id": 9}), &compiled(&[spec]));
        assert_eq!(miss["user_name"], json!(null));
    }

    #[cfg(feature = "transform-lookup")]
    #[test]
    fn lookup_matches_number_against_string_key() {
        let spec = RecordTransform::Lookup {
            reference: ref_rows(json!([{"code": "42", "label": "answer"}])),
            on_record: "code".into(),
            on_ref: "code".into(),
            add: vec![("label".into(), "label".into())],
            on_missing: LookupOnMissing::Keep,
        };
        // Record carries numeric 42; reference key is "42" — matched by scalar form.
        let out = apply_all(json!({"code": 42}), &compiled(&[spec]));
        assert_eq!(out["label"], json!("answer"));
    }

    #[cfg(feature = "transform-lookup")]
    #[test]
    fn lookup_on_missing_error_fails() {
        let c = compile(&RecordTransform::Lookup {
            reference: Vec::new(),
            on_record: "k".into(),
            on_ref: "k".into(),
            add: vec![("x".into(), "y".into())],
            on_missing: LookupOnMissing::Error,
        })
        .unwrap();
        let res = super::apply_all(json!({"k": "z"}), std::slice::from_ref(&c));
        assert!(res.is_err());
    }

    #[cfg(feature = "transform-lookup")]
    #[test]
    fn lookup_empty_add_is_rejected_at_compile() {
        assert!(
            compile(&RecordTransform::Lookup {
                reference: Vec::new(),
                on_record: "k".into(),
                on_ref: "k".into(),
                add: Vec::new(),
                on_missing: LookupOnMissing::Null,
            })
            .is_err()
        );
    }

    #[cfg(all(feature = "transform-json-encode", feature = "transform-lookup"))]
    #[test]
    fn json_encode_and_lookup_debug_and_clone() {
        // RecordTransform Debug + Clone for both new variants.
        let je = RecordTransform::JsonEncode {
            fields: vec!["meta".into()],
        };
        assert!(format!("{je:?}").contains("JsonEncode"));
        assert!(format!("{:?}", je.clone()).contains("JsonEncode"));

        let mut row = serde_json::Map::new();
        row.insert("id".into(), Value::String("1".into()));
        let lk = RecordTransform::Lookup {
            reference: vec![row],
            on_record: "rid".into(),
            on_ref: "id".into(),
            add: vec![("name".into(), "name".into())],
            on_missing: LookupOnMissing::Null,
        };
        assert!(format!("{lk:?}").contains("Lookup"));
        assert!(format!("{:?}", lk.clone()).contains("Lookup"));

        // CompiledTransform Clone for both new variants.
        let _ = compile(&je).unwrap().clone();
        let _ = compile(&lk).unwrap().clone();
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_non_null_non_string_target_untouched() {
        // A numeric (non-null, non-string) target is not nullish, so it is left
        // as-is regardless of `treat_empty_string_as_null`.
        let out = apply_all(
            json!({"n": 0}),
            &compiled(&[RecordTransform::Coalesce {
                field: "n".into(),
                default: Some(json!(99)),
                from: vec![],
                treat_empty_string_as_null: true,
            }]),
        );
        assert_eq!(out["n"], 0);
    }

    #[cfg(feature = "transform-coalesce")]
    #[test]
    fn coalesce_passes_through_non_object() {
        let record = json!([1, 2]);
        let result = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Coalesce {
                field: "a".into(),
                default: Some(json!("x")),
                from: vec![],
                treat_empty_string_as_null: false,
            }]),
        );
        assert_eq!(result, record);
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_and_join_pass_through_non_object() {
        let record = json!(42);
        let split = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Split {
                field: "a".into(),
                delimiter: ",".into(),
                trim: false,
                into: None,
            }]),
        );
        assert_eq!(split, record);
        let join = apply_all(
            record.clone(),
            &compiled(&[RecordTransform::Join {
                field: "a".into(),
                delimiter: ",".into(),
                into: None,
            }]),
        );
        assert_eq!(join, record);
    }

    #[cfg(feature = "transform-split-join")]
    #[test]
    fn split_empty_delimiter_yields_single_element() {
        let out = apply_all(
            json!({"s": "  hi  "}),
            &compiled(&[RecordTransform::Split {
                field: "s".into(),
                delimiter: String::new(),
                trim: true,
                into: None,
            }]),
        );
        // Empty delimiter → one trimmed element (no per-char split).
        assert_eq!(out["s"], json!(["hi"]));
    }

    #[cfg(feature = "transform-value-case")]
    #[test]
    fn value_case_title_and_capitalize_handle_empty_string() {
        for mode in [ValueCaseMode::Title, ValueCaseMode::Capitalize] {
            let out = apply_all(
                json!({"s": ""}),
                &compiled(&[RecordTransform::ValueCase {
                    fields: vec!["s".into()],
                    mode,
                }]),
            );
            assert_eq!(out["s"], "");
        }
    }
}

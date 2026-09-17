//! Config-shaped types for the data-quality layer. Pure declarations — no
//! evaluation logic (that lives in `record.rs` / `batch.rs`) and no
//! compilation (that lives in `compile.rs`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What to do when a check fails. The allowed subset is validated per check
/// at compile time (see `compile.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Route the specific offending row(s) to the DLQ; keep the rest.
    Quarantine,
    /// Route all survivors of the page to the DLQ; write nothing this page.
    QuarantineBatch,
    /// Surface `FaucetError::QualityFailure` and fail the run.
    Abort,
}

/// Ordering / equality operator for the `compare` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    /// Greater than: `field > value`. Both must be JSON numbers.
    Gt,
    /// Greater than or equal: `field >= value`. Both must be JSON numbers.
    Gte,
    /// Less than: `field < value`. Both must be JSON numbers.
    Lt,
    /// Less than or equal: `field <= value`. Both must be JSON numbers.
    Lte,
    /// JSON equality. Two numbers compare by numeric value (`1` == `1.0`, and
    /// large 64-bit integers compare exactly); all other types compare
    /// structurally with no cross-type coercion (string `"5"` != number `5`).
    Eq,
    /// JSON inequality — the negation of [`CompareOp::Eq`] (numbers by value,
    /// other types structurally).
    Ne,
}

impl std::fmt::Display for CompareOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            CompareOp::Gt => "gt",
            CompareOp::Gte => "gte",
            CompareOp::Lt => "lt",
            CompareOp::Lte => "lte",
            CompareOp::Eq => "eq",
            CompareOp::Ne => "ne",
        })
    }
}

/// Expected JSON type for the `type_is` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JsonType {
    /// JSON boolean (`true` / `false`).
    Boolean,
    /// JSON number (integer or float).
    Number,
    /// JSON string.
    String,
    /// JSON array.
    Array,
    /// JSON object.
    Object,
    /// JSON null. Note: a *missing* field is distinct from an explicit `null`.
    Null,
}

impl std::fmt::Display for JsonType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            JsonType::Boolean => "boolean",
            JsonType::Number => "number",
            JsonType::String => "string",
            JsonType::Array => "array",
            JsonType::Object => "object",
            JsonType::Null => "null",
        })
    }
}

fn default_true() -> bool {
    true
}

/// The `quality:` config block. Per-record checks run first (partitioning the
/// page into survivors + quarantined); per-batch checks then run over the
/// survivors.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QualitySpec {
    /// Per-record checks, evaluated in declared order (first failure wins).
    #[serde(default)]
    pub record: Vec<RecordCheck>,
    /// Per-batch checks, evaluated per page over the survivors.
    #[serde(default)]
    pub batch: Vec<BatchCheck>,
}

/// A per-record check. Addressed field accepts the filter/explode path subset
/// (bare key, `dot.path`, `$['bracketed']`).
///
/// Every variant carries `field` (the path to check) and `on_failure` (what a
/// failure does). Per-record checks accept only [`OnFailure::Quarantine`] or
/// [`OnFailure::Abort`] — `quarantine_batch` is rejected at compile time
/// because a per-record failure is always attributable to one row.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum RecordCheck {
    /// Field present and non-null.
    NotNull {
        /// Path to the checked field.
        field: String,
        /// When `true` (default) a missing field fails; when `false` only an
        /// explicit JSON `null` fails.
        #[serde(default = "default_true")]
        treat_missing_as_null: bool,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field is a string, non-empty after `trim()`.
    NotEmpty {
        /// Path to the checked field. A missing field, a `null`, or a
        /// non-string value all fail.
        field: String,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field is a string matching `pattern`.
    RegexMatch {
        /// Path to the checked field. A missing field or a non-string value
        /// fails.
        field: String,
        /// Rust `regex`-crate pattern. Unanchored — it only has to match
        /// *somewhere* in the value, so anchor with `^…$` for a full match.
        /// An invalid pattern is rejected at config load, never mid-run.
        pattern: String,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field value is a member of `values` (exact JSON equality).
    ValueInSet {
        /// Path to the checked field. A missing field fails.
        field: String,
        /// The allowed values. Must be non-empty. Compared by JSON equality
        /// with no type coercion, so the string `"5"` does not match the
        /// number `5`.
        values: Vec<Value>,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field value is NOT a member of `values` (exact JSON equality).
    NotInSet {
        /// Path to the checked field. A **missing** field passes — there is no
        /// value to forbid.
        field: String,
        /// The forbidden values. Must be non-empty. Compared by JSON equality
        /// with no type coercion.
        values: Vec<Value>,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field value compares against `value` under `op`.
    Compare {
        /// Path to the checked field. A missing field fails.
        field: String,
        /// The comparison to apply, as `field <op> value`.
        op: CompareOp,
        /// The right-hand side of the comparison. The ordering ops
        /// (`gt`/`gte`/`lt`/`lte`) require a JSON number here — anything else
        /// is rejected at config load — and also require the record's value to
        /// be a number; `eq`/`ne` accept any JSON value.
        value: Value,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field's JSON type equals `expected`.
    TypeIs {
        /// Path to the checked field. A missing field fails, which is distinct
        /// from an `expected: null` match on a present `null`.
        field: String,
        /// The required JSON type.
        expected: JsonType,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// Field is a string whose char count is within `[min, max]`.
    StringLength {
        /// Path to the checked field. A missing field or a non-string value
        /// fails.
        field: String,
        /// Inclusive minimum length in Unicode **characters** (not bytes).
        /// Omit for no lower bound.
        #[serde(default)]
        min: Option<usize>,
        /// Inclusive maximum length in Unicode **characters** (not bytes).
        /// Omit for no upper bound. At least one of `min`/`max` is required,
        /// and `min <= max`, both enforced at config load.
        #[serde(default)]
        max: Option<usize>,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
    /// The whole record validates against a JSON Schema document.
    #[cfg(feature = "quality-jsonschema")]
    JsonSchema {
        /// The JSON Schema document, inline. Compiled once at config load —
        /// an invalid schema fails there, not on the first page. The first
        /// validation error becomes the DLQ/abort message.
        schema: Value,
        /// What a failure does: `quarantine` (row to the DLQ) or `abort`.
        on_failure: OnFailure,
    },
}

/// A per-batch check, evaluated per page over the survivors of the per-record
/// pass.
///
/// **Scope is one page, not the whole run** — with a source `batch_size` of
/// 1000, `row_count` sees 1000 rows at a time. Set the source's
/// `batch_size: 0` to evaluate these over the entire result set instead.
///
/// The aggregate checks (`row_count` / `null_rate` / `distinct_count`) cannot
/// blame an individual row, so they accept only [`OnFailure::Abort`] or
/// [`OnFailure::QuarantineBatch`]; `unique` *is* row-attributable and so
/// accepts `quarantine` or `abort`. Either way the wrong choice is rejected at
/// config load.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum BatchCheck {
    /// Survivor count is within `[min, max]` (at least one bound required).
    RowCount {
        /// Inclusive minimum surviving rows in the page. Omit for no lower
        /// bound.
        #[serde(default)]
        min: Option<usize>,
        /// Inclusive maximum surviving rows in the page. Omit for no upper
        /// bound. At least one of `min`/`max` is required, and `min <= max`,
        /// both enforced at config load.
        #[serde(default)]
        max: Option<usize>,
        /// What a failure does: `abort`, or `quarantine_batch` (every survivor
        /// in the page goes to the DLQ and nothing is written).
        on_failure: OnFailure,
    },
    /// Null-or-missing rate of `field` across survivors is `<= max`.
    NullRate {
        /// Path to the measured field. A missing field counts the same as an
        /// explicit `null`.
        field: String,
        /// Maximum allowed null-or-missing proportion, in `[0.0, 1.0]`. Out-of-range values are rejected at compile time.
        max: f64,
        /// What a failure does: `abort`, or `quarantine_batch` (every survivor
        /// in the page goes to the DLQ and nothing is written).
        on_failure: OnFailure,
    },
    /// The composite `fields` tuple is unique across survivors.
    Unique {
        /// Paths forming the uniqueness key, in order. One path for a simple
        /// key, several for a composite one. Must be non-empty. A missing
        /// field is a distinct key value from an explicit `null`.
        fields: Vec<String>,
        /// What a failure does. This check names the offending rows, so
        /// `quarantine` sends the **duplicate occurrences** to the DLQ (the
        /// first occurrence of each key is kept); `abort` fails the run.
        on_failure: OnFailure,
    },
    /// Distinct values of `field` across survivors is within `[min, max]`.
    DistinctCount {
        /// Path to the counted field. A missing field counts as its own
        /// distinct value, separate from an explicit `null`.
        field: String,
        /// Inclusive minimum number of distinct values. Omit for no lower
        /// bound.
        #[serde(default)]
        min: Option<usize>,
        /// Inclusive maximum number of distinct values. Omit for no upper
        /// bound. At least one of `min`/`max` is required, and `min <= max`,
        /// both enforced at config load.
        #[serde(default)]
        max: Option<usize>,
        /// What a failure does: `abort`, or `quarantine_batch` (every survivor
        /// in the page goes to the DLQ and nothing is written).
        on_failure: OnFailure,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_failure_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&OnFailure::QuarantineBatch).unwrap(),
            "\"quarantine_batch\""
        );
    }

    #[test]
    fn compare_op_round_trips() {
        let op: CompareOp = serde_json::from_str("\"gte\"").unwrap();
        assert_eq!(op, CompareOp::Gte);
    }

    #[test]
    fn json_type_round_trips() {
        let t: JsonType = serde_json::from_str("\"boolean\"").unwrap();
        assert_eq!(t, JsonType::Boolean);
    }

    #[test]
    fn parses_full_quality_block() {
        let spec: QualitySpec = serde_json::from_value(serde_json::json!({
            "record": [
                { "type": "not_null", "field": "user_id", "on_failure": "quarantine" },
                { "type": "compare", "field": "age", "op": "gte", "value": 0, "on_failure": "abort" },
                { "type": "string_length", "field": "name", "min": 1, "max": 256, "on_failure": "quarantine" }
            ],
            "batch": [
                { "type": "row_count", "min": 1, "max": 100000, "on_failure": "abort" },
                { "type": "unique", "fields": ["id"], "on_failure": "quarantine" }
            ]
        }))
        .unwrap();
        assert_eq!(spec.record.len(), 3);
        assert_eq!(spec.batch.len(), 2);
        assert!(matches!(spec.record[0], RecordCheck::NotNull { .. }));
        assert!(matches!(spec.batch[1], BatchCheck::Unique { .. }));
        if let RecordCheck::NotNull {
            treat_missing_as_null,
            ..
        } = &spec.record[0]
        {
            assert!(
                *treat_missing_as_null,
                "treat_missing_as_null defaults to true"
            );
        } else {
            panic!("expected first record check to be NotNull");
        }
    }

    #[test]
    fn empty_quality_block_defaults_to_no_checks() {
        let spec: QualitySpec = serde_json::from_str("{}").unwrap();
        assert!(spec.record.is_empty());
        assert!(spec.batch.is_empty());
    }
}

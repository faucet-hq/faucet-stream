//! Compile a `QualitySpec` into a `CompiledQuality`: parse paths/regexes/
//! schemas once, validate the `on_failure` subset and numeric bounds. Failures
//! surface as `FaucetError::Config`.

use crate::error::FaucetError;
use crate::quality::config::{
    BatchCheck, CompareOp, JsonType, OnFailure, QualitySpec, RecordCheck,
};
use crate::quality::record::ValueSetIndex;
use crate::stage::CompiledPath;
use serde_json::Value;

/// A fully compiled quality spec. Built once via [`CompiledQuality::compile`].
#[derive(Debug)]
pub struct CompiledQuality {
    pub record: Vec<CompiledRecordCheck>,
    pub batch: Vec<CompiledBatchCheck>,
}

/// One compiled per-record check: the check kind plus its failure policy and
/// labelling metadata.
#[derive(Debug)]
pub struct CompiledRecordCheck {
    pub kind: CompiledRecordKind,
    pub on_failure: OnFailure,
    /// Stable metric-label name, e.g. `"not_null"`.
    pub name: &'static str,
    /// Addressed field (for envelope/metric `field` label); `None` for whole-
    /// record checks like `json_schema`.
    pub field: Option<String>,
}

/// Compiled form of a per-record check, keyed by check kind.
#[derive(Debug)]
pub enum CompiledRecordKind {
    NotNull {
        path: CompiledPath,
        treat_missing_as_null: bool,
    },
    NotEmpty {
        path: CompiledPath,
    },
    RegexMatch {
        path: CompiledPath,
        re: regex::Regex,
    },
    ValueInSet {
        path: CompiledPath,
        values: Vec<Value>,
        /// Hashed lookup over `values`; build it with [`ValueSetIndex::new`].
        index: ValueSetIndex,
    },
    NotInSet {
        path: CompiledPath,
        values: Vec<Value>,
        /// Hashed lookup over `values`; build it with [`ValueSetIndex::new`].
        index: ValueSetIndex,
    },
    Compare {
        path: CompiledPath,
        op: CompareOp,
        value: Value,
    },
    TypeIs {
        path: CompiledPath,
        expected: JsonType,
    },
    StringLength {
        path: CompiledPath,
        min: Option<usize>,
        max: Option<usize>,
    },
    #[cfg(feature = "quality-jsonschema")]
    JsonSchema {
        validator: jsonschema::Validator,
    },
}

/// One compiled per-batch check.
#[derive(Debug)]
pub struct CompiledBatchCheck {
    pub kind: CompiledBatchKind,
    pub on_failure: OnFailure,
    pub name: &'static str,
    pub field: Option<String>,
}

/// Compiled form of a per-batch check, keyed by check kind.
#[derive(Debug)]
pub enum CompiledBatchKind {
    RowCount {
        min: Option<usize>,
        max: Option<usize>,
    },
    NullRate {
        path: CompiledPath,
        max: f64,
    },
    Unique {
        paths: Vec<CompiledPath>,
    },
    DistinctCount {
        path: CompiledPath,
        min: Option<usize>,
        max: Option<usize>,
    },
}

fn config_err(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("quality: {}", msg.into()))
}

fn compile_path(field: &str) -> Result<CompiledPath, FaucetError> {
    CompiledPath::compile(field).map_err(|e| config_err(format!("invalid field '{field}': {e}")))
}

/// Per-record checks accept only `quarantine` / `abort`.
fn check_record_policy(name: &str, on_failure: OnFailure) -> Result<(), FaucetError> {
    match on_failure {
        OnFailure::Quarantine | OnFailure::Abort => Ok(()),
        OnFailure::QuarantineBatch => Err(config_err(format!(
            "check '{name}': on_failure 'quarantine_batch' is only valid on aggregate batch checks"
        ))),
    }
}

impl CompiledQuality {
    /// Compile and validate a [`QualitySpec`]. Returns [`FaucetError::Config`]
    /// on any invalid path, regex, schema, bound, or `on_failure` choice.
    pub fn compile(spec: &QualitySpec) -> Result<Self, FaucetError> {
        let record = spec
            .record
            .iter()
            .map(compile_record_check)
            .collect::<Result<_, _>>()?;
        let batch = spec
            .batch
            .iter()
            .map(compile_batch_check)
            .collect::<Result<_, _>>()?;
        Ok(Self { record, batch })
    }

    /// True if any check would route records to the DLQ (so a DLQ sink is
    /// required). Checked by the pipeline at run start.
    pub fn requires_dlq(&self) -> bool {
        self.record
            .iter()
            .any(|c| matches!(c.on_failure, OnFailure::Quarantine))
            || self.batch.iter().any(|c| {
                matches!(
                    c.on_failure,
                    OnFailure::Quarantine | OnFailure::QuarantineBatch
                )
            })
    }
}

fn compile_record_check(c: &RecordCheck) -> Result<CompiledRecordCheck, FaucetError> {
    Ok(match c {
        RecordCheck::NotNull {
            field,
            treat_missing_as_null,
            on_failure,
        } => {
            check_record_policy("not_null", *on_failure)?;
            CompiledRecordCheck {
                kind: CompiledRecordKind::NotNull {
                    path: compile_path(field)?,
                    treat_missing_as_null: *treat_missing_as_null,
                },
                on_failure: *on_failure,
                name: "not_null",
                field: Some(field.clone()),
            }
        }
        RecordCheck::NotEmpty { field, on_failure } => {
            check_record_policy("not_empty", *on_failure)?;
            CompiledRecordCheck {
                kind: CompiledRecordKind::NotEmpty {
                    path: compile_path(field)?,
                },
                on_failure: *on_failure,
                name: "not_empty",
                field: Some(field.clone()),
            }
        }
        RecordCheck::RegexMatch {
            field,
            pattern,
            on_failure,
        } => {
            check_record_policy("regex_match", *on_failure)?;
            let re = regex::Regex::new(pattern).map_err(|e| {
                config_err(format!("regex_match: invalid pattern '{pattern}': {e}"))
            })?;
            CompiledRecordCheck {
                kind: CompiledRecordKind::RegexMatch {
                    path: compile_path(field)?,
                    re,
                },
                on_failure: *on_failure,
                name: "regex_match",
                field: Some(field.clone()),
            }
        }
        RecordCheck::ValueInSet {
            field,
            values,
            on_failure,
        } => {
            check_record_policy("value_in_set", *on_failure)?;
            if values.is_empty() {
                return Err(config_err("value_in_set: `values` must be non-empty"));
            }
            CompiledRecordCheck {
                kind: CompiledRecordKind::ValueInSet {
                    path: compile_path(field)?,
                    values: values.clone(),
                    index: ValueSetIndex::new(values),
                },
                on_failure: *on_failure,
                name: "value_in_set",
                field: Some(field.clone()),
            }
        }
        RecordCheck::NotInSet {
            field,
            values,
            on_failure,
        } => {
            check_record_policy("not_in_set", *on_failure)?;
            if values.is_empty() {
                return Err(config_err("not_in_set: `values` must be non-empty"));
            }
            CompiledRecordCheck {
                kind: CompiledRecordKind::NotInSet {
                    path: compile_path(field)?,
                    values: values.clone(),
                    index: ValueSetIndex::new(values),
                },
                on_failure: *on_failure,
                name: "not_in_set",
                field: Some(field.clone()),
            }
        }
        RecordCheck::Compare {
            field,
            op,
            value,
            on_failure,
        } => {
            check_record_policy("compare", *on_failure)?;
            // Ordering ops require a numeric configured value.
            if matches!(
                op,
                CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte
            ) && !value.is_number()
            {
                return Err(config_err(format!(
                    "compare: op '{op}' requires a numeric `value`"
                )));
            }
            CompiledRecordCheck {
                kind: CompiledRecordKind::Compare {
                    path: compile_path(field)?,
                    op: *op,
                    value: value.clone(),
                },
                on_failure: *on_failure,
                name: "compare",
                field: Some(field.clone()),
            }
        }
        RecordCheck::TypeIs {
            field,
            expected,
            on_failure,
        } => {
            check_record_policy("type_is", *on_failure)?;
            CompiledRecordCheck {
                kind: CompiledRecordKind::TypeIs {
                    path: compile_path(field)?,
                    expected: *expected,
                },
                on_failure: *on_failure,
                name: "type_is",
                field: Some(field.clone()),
            }
        }
        RecordCheck::StringLength {
            field,
            min,
            max,
            on_failure,
        } => {
            check_record_policy("string_length", *on_failure)?;
            if min.is_none() && max.is_none() {
                return Err(config_err(
                    "string_length: at least one of `min`/`max` is required",
                ));
            }
            if let (Some(lo), Some(hi)) = (min, max)
                && lo > hi
            {
                return Err(config_err("string_length: `min` must be <= `max`"));
            }
            CompiledRecordCheck {
                kind: CompiledRecordKind::StringLength {
                    path: compile_path(field)?,
                    min: *min,
                    max: *max,
                },
                on_failure: *on_failure,
                name: "string_length",
                field: Some(field.clone()),
            }
        }
        #[cfg(feature = "quality-jsonschema")]
        RecordCheck::JsonSchema { schema, on_failure } => {
            check_record_policy("json_schema", *on_failure)?;
            let validator = jsonschema::validator_for(schema)
                .map_err(|e| config_err(format!("json_schema: invalid schema: {e}")))?;
            CompiledRecordCheck {
                kind: CompiledRecordKind::JsonSchema { validator },
                on_failure: *on_failure,
                name: "json_schema",
                field: None,
            }
        }
    })
}

fn compile_batch_check(c: &BatchCheck) -> Result<CompiledBatchCheck, FaucetError> {
    // Aggregate batch checks accept only `abort` / `quarantine_batch`.
    fn check_aggregate_policy(name: &str, on_failure: OnFailure) -> Result<(), FaucetError> {
        match on_failure {
            OnFailure::Abort | OnFailure::QuarantineBatch => Ok(()),
            OnFailure::Quarantine => Err(config_err(format!(
                "check '{name}': on_failure 'quarantine' is not row-attributable here; \
                 use 'quarantine_batch' or 'abort'"
            ))),
        }
    }

    Ok(match c {
        BatchCheck::RowCount {
            min,
            max,
            on_failure,
        } => {
            check_aggregate_policy("row_count", *on_failure)?;
            if min.is_none() && max.is_none() {
                return Err(config_err(
                    "row_count: at least one of `min`/`max` is required",
                ));
            }
            if let (Some(lo), Some(hi)) = (min, max)
                && lo > hi
            {
                return Err(config_err("row_count: `min` must be <= `max`"));
            }
            CompiledBatchCheck {
                kind: CompiledBatchKind::RowCount {
                    min: *min,
                    max: *max,
                },
                on_failure: *on_failure,
                name: "row_count",
                field: None,
            }
        }
        BatchCheck::NullRate {
            field,
            max,
            on_failure,
        } => {
            check_aggregate_policy("null_rate", *on_failure)?;
            if !(0.0..=1.0).contains(max) {
                return Err(config_err("null_rate: `max` must be within [0.0, 1.0]"));
            }
            CompiledBatchCheck {
                kind: CompiledBatchKind::NullRate {
                    path: compile_path(field)?,
                    max: *max,
                },
                on_failure: *on_failure,
                name: "null_rate",
                field: Some(field.clone()),
            }
        }
        BatchCheck::Unique { fields, on_failure } => {
            // Unique IS row-attributable: accept quarantine / abort.
            check_record_policy("unique", *on_failure)?;
            if fields.is_empty() {
                return Err(config_err("unique: `fields` must be non-empty"));
            }
            let paths = fields
                .iter()
                .map(|f| compile_path(f))
                .collect::<Result<_, _>>()?;
            CompiledBatchCheck {
                kind: CompiledBatchKind::Unique { paths },
                on_failure: *on_failure,
                name: "unique",
                field: Some(fields.join(",")),
            }
        }
        BatchCheck::DistinctCount {
            field,
            min,
            max,
            on_failure,
        } => {
            check_aggregate_policy("distinct_count", *on_failure)?;
            if min.is_none() && max.is_none() {
                return Err(config_err(
                    "distinct_count: at least one of `min`/`max` is required",
                ));
            }
            if let (Some(lo), Some(hi)) = (min, max)
                && lo > hi
            {
                return Err(config_err("distinct_count: `min` must be <= `max`"));
            }
            CompiledBatchCheck {
                kind: CompiledBatchKind::DistinctCount {
                    path: compile_path(field)?,
                    min: *min,
                    max: *max,
                },
                on_failure: *on_failure,
                name: "distinct_count",
                field: Some(field.clone()),
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quality::config::{BatchCheck, CompareOp, OnFailure, QualitySpec, RecordCheck};
    use serde_json::json;

    fn compile(spec: QualitySpec) -> Result<CompiledQuality, crate::FaucetError> {
        CompiledQuality::compile(&spec)
    }

    #[test]
    fn compiles_valid_spec() {
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "user_id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![BatchCheck::RowCount {
                min: Some(1),
                max: Some(10),
                on_failure: OnFailure::Abort,
            }],
        };
        let c = compile(spec).unwrap();
        assert_eq!(c.record.len(), 1);
        assert_eq!(c.batch.len(), 1);
        assert!(c.requires_dlq());
    }

    #[test]
    fn rejects_bad_regex_at_compile() {
        let spec = QualitySpec {
            record: vec![RecordCheck::RegexMatch {
                field: "email".into(),
                pattern: "[invalid".into(),
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn rejects_disallowed_on_failure_for_record_check() {
        // quarantine_batch is not valid on a per-record check.
        let spec = QualitySpec {
            record: vec![RecordCheck::NotEmpty {
                field: "name".into(),
                on_failure: OnFailure::QuarantineBatch,
            }],
            batch: vec![],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn rejects_disallowed_on_failure_for_aggregate_batch_check() {
        // quarantine (row-attributable) is not valid on row_count.
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::RowCount {
                min: Some(1),
                max: None,
                on_failure: OnFailure::Quarantine,
            }],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn rejects_empty_bounds() {
        let spec = QualitySpec {
            record: vec![RecordCheck::StringLength {
                field: "name".into(),
                min: None,
                max: None,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn rejects_non_numeric_compare_value_for_ordering_op() {
        let spec = QualitySpec {
            record: vec![RecordCheck::Compare {
                field: "age".into(),
                op: CompareOp::Gte,
                value: json!("not a number"),
                on_failure: OnFailure::Abort,
            }],
            batch: vec![],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn rejects_null_rate_out_of_range() {
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::NullRate {
                field: "name".into(),
                max: 1.5,
                on_failure: OnFailure::Abort,
            }],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn requires_dlq_false_when_only_abort() {
        let spec = QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Abort,
            }],
            batch: vec![],
        };
        assert!(!compile(spec).unwrap().requires_dlq());
    }

    #[test]
    fn rejects_row_count_min_gt_max() {
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::RowCount {
                min: Some(100),
                max: Some(1),
                on_failure: OnFailure::Abort,
            }],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }

    #[test]
    fn rejects_distinct_count_min_gt_max() {
        let spec = QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::DistinctCount {
                field: "x".into(),
                min: Some(5),
                max: Some(2),
                on_failure: OnFailure::Abort,
            }],
        };
        assert!(matches!(compile(spec), Err(crate::FaucetError::Config(_))));
    }
}

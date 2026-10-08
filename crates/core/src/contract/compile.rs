//! Compile a `ContractSpec` into a `CompiledContract`: validate the version,
//! field-name uniqueness, per-type constraint compatibility, and bounds, and
//! compile regexes/enum sets once so the per-record pass is cheap. Failures
//! surface as `FaucetError::Config` at config-load time, never mid-run.

use crate::contract::config::{ContractFieldType, ContractSpec, FieldContract, OnBreach};
use crate::error::FaucetError;
use serde_json::Value;
use std::collections::HashSet;

/// A fully compiled contract. Built once via [`CompiledContract::compile`].
#[derive(Debug)]
pub struct CompiledContract {
    /// Contract version (carried into breach errors and DLQ envelopes).
    pub version: String,
    /// Breach policy.
    pub on_breach: OnBreach,
    /// When `false`, undeclared top-level keys breach the contract.
    pub allow_extra_fields: bool,
    /// Compiled per-field rules, in declared order (first breach wins).
    pub fields: Vec<CompiledField>,
    /// Declared field names, for the O(1) extra-field membership test.
    pub declared: HashSet<String>,
}

/// One compiled field rule.
#[derive(Debug)]
pub struct CompiledField {
    pub name: String,
    pub field_type: ContractFieldType,
    pub required: bool,
    pub nullable: bool,
    pub allowed_values: Option<Vec<Value>>,
    pub pattern: Option<regex::Regex>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
}

fn config_err(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("contract: {}", msg.into()))
}

/// Does `value` conform to the declared contract type? `integer` means a JSON
/// number with no fractional part (`1` and `1.0` both conform).
pub(crate) fn type_matches(value: &Value, ty: ContractFieldType) -> bool {
    match ty {
        ContractFieldType::String => value.is_string(),
        ContractFieldType::Integer => {
            value.is_i64()
                || value.is_u64()
                || value
                    .as_f64()
                    .is_some_and(|f| f.is_finite() && f.fract() == 0.0)
        }
        ContractFieldType::Number => value.is_number(),
        ContractFieldType::Boolean => value.is_boolean(),
        ContractFieldType::Object => value.is_object(),
        ContractFieldType::Array => value.is_array(),
    }
}

impl CompiledContract {
    /// Compile and validate a [`ContractSpec`]. Returns [`FaucetError::Config`]
    /// on a malformed contract: empty version, no fields, duplicate/empty
    /// field names, an invalid regex, an empty or type-mismatched `enum`,
    /// constraints that don't apply to the declared type, or `min > max`.
    pub fn compile(spec: &ContractSpec) -> Result<Self, FaucetError> {
        if spec.version.trim().is_empty() {
            return Err(config_err("`version` must be non-empty"));
        }
        if spec.fields.is_empty() {
            return Err(config_err("`fields` must be non-empty"));
        }
        let mut declared: HashSet<String> = HashSet::with_capacity(spec.fields.len());
        let mut fields = Vec::with_capacity(spec.fields.len());
        for f in &spec.fields {
            if f.name.trim().is_empty() {
                return Err(config_err("field names must be non-empty"));
            }
            if !declared.insert(f.name.clone()) {
                return Err(config_err(format!("duplicate field '{}'", f.name)));
            }
            fields.push(compile_field(f)?);
        }
        Ok(Self {
            version: spec.version.clone(),
            on_breach: spec.on_breach,
            allow_extra_fields: spec.allow_extra_fields,
            fields,
            declared,
        })
    }

    /// True when breaches route records to the DLQ (so a DLQ sink is
    /// required). Checked by the pipeline at run start and by the CLI at
    /// config-load time.
    pub fn requires_dlq(&self) -> bool {
        self.on_breach == OnBreach::Quarantine
    }
}

fn compile_field(f: &FieldContract) -> Result<CompiledField, FaucetError> {
    let is_string = f.field_type == ContractFieldType::String;
    let is_numeric = matches!(
        f.field_type,
        ContractFieldType::Integer | ContractFieldType::Number
    );

    let pattern = match &f.pattern {
        Some(p) => {
            if !is_string {
                return Err(config_err(format!(
                    "field '{}': `pattern` applies only to string fields, not {}",
                    f.name, f.field_type
                )));
            }
            Some(regex::Regex::new(p).map_err(|e| {
                config_err(format!("field '{}': invalid pattern '{p}': {e}", f.name))
            })?)
        }
        None => None,
    };

    if (f.min.is_some() || f.max.is_some()) && !is_numeric {
        return Err(config_err(format!(
            "field '{}': `min`/`max` apply only to integer/number fields, not {}",
            f.name, f.field_type
        )));
    }
    if let (Some(lo), Some(hi)) = (f.min, f.max)
        && lo > hi
    {
        return Err(config_err(format!(
            "field '{}': `min` must be <= `max`",
            f.name
        )));
    }

    if (f.min_length.is_some() || f.max_length.is_some()) && !is_string {
        return Err(config_err(format!(
            "field '{}': `min_length`/`max_length` apply only to string fields, not {}",
            f.name, f.field_type
        )));
    }
    if let (Some(lo), Some(hi)) = (f.min_length, f.max_length)
        && lo > hi
    {
        return Err(config_err(format!(
            "field '{}': `min_length` must be <= `max_length`",
            f.name
        )));
    }

    if let Some(values) = &f.allowed_values {
        if values.is_empty() {
            return Err(config_err(format!(
                "field '{}': `enum` must be non-empty",
                f.name
            )));
        }
        for v in values {
            if v.is_null() {
                return Err(config_err(format!(
                    "field '{}': `enum` must not contain null — use `nullable: true`",
                    f.name
                )));
            }
            if !type_matches(v, f.field_type) {
                return Err(config_err(format!(
                    "field '{}': enum value {v} does not match declared type {}",
                    f.name, f.field_type
                )));
            }
        }
    }

    Ok(CompiledField {
        name: f.name.clone(),
        field_type: f.field_type,
        required: f.required,
        nullable: f.nullable,
        allowed_values: f.allowed_values.clone(),
        pattern,
        min: f.min,
        max: f.max,
        min_length: f.min_length,
        max_length: f.max_length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(v: Value) -> ContractSpec {
        serde_json::from_value(v).unwrap()
    }

    fn compile(v: Value) -> Result<CompiledContract, FaucetError> {
        CompiledContract::compile(&spec(v))
    }

    fn assert_config_err(r: Result<CompiledContract, FaucetError>, needle: &str) {
        match r {
            Err(FaucetError::Config(msg)) => {
                assert!(msg.contains(needle), "expected '{needle}' in: {msg}")
            }
            other => panic!("expected Config error containing '{needle}', got {other:?}"),
        }
    }

    #[test]
    fn compiles_valid_contract() {
        let c = compile(json!({
            "version": "1.0.0",
            "on_breach": "quarantine",
            "fields": [
                { "name": "id", "type": "integer", "min": 0 },
                { "name": "status", "type": "string", "enum": ["a", "b"] }
            ]
        }))
        .unwrap();
        assert_eq!(c.version, "1.0.0");
        assert_eq!(c.fields.len(), 2);
        assert!(c.requires_dlq());
        assert!(c.declared.contains("status"));
    }

    #[test]
    fn requires_dlq_only_for_quarantine() {
        for (policy, expected) in [("fail", false), ("warn", false), ("quarantine", true)] {
            let c = compile(json!({
                "version": "1", "on_breach": policy,
                "fields": [{ "name": "id", "type": "string" }]
            }))
            .unwrap();
            assert_eq!(c.requires_dlq(), expected, "policy {policy}");
        }
    }

    #[test]
    fn rejects_empty_version() {
        assert_config_err(
            compile(json!({"version": "  ", "fields": [{"name": "x", "type": "string"}]})),
            "`version` must be non-empty",
        );
    }

    #[test]
    fn rejects_empty_fields() {
        assert_config_err(
            compile(json!({"version": "1", "fields": []})),
            "`fields` must be non-empty",
        );
    }

    #[test]
    fn rejects_duplicate_field_names() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "id", "type": "string"},
                {"name": "id", "type": "integer"}
            ]})),
            "duplicate field 'id'",
        );
    }

    #[test]
    fn rejects_empty_field_name() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [{"name": " ", "type": "string"}]})),
            "field names must be non-empty",
        );
    }

    #[test]
    fn rejects_bad_regex() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "email", "type": "string", "pattern": "[invalid"}
            ]})),
            "invalid pattern",
        );
    }

    #[test]
    fn rejects_pattern_on_non_string() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "n", "type": "integer", "pattern": "^\\d+$"}
            ]})),
            "`pattern` applies only to string fields",
        );
    }

    #[test]
    fn rejects_min_max_on_non_numeric() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "s", "type": "string", "min": 1.0}
            ]})),
            "`min`/`max` apply only to integer/number fields",
        );
    }

    #[test]
    fn rejects_min_gt_max() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "n", "type": "number", "min": 5.0, "max": 1.0}
            ]})),
            "`min` must be <= `max`",
        );
    }

    #[test]
    fn rejects_length_on_non_string() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "n", "type": "number", "min_length": 1}
            ]})),
            "`min_length`/`max_length` apply only to string fields",
        );
    }

    #[test]
    fn rejects_min_length_gt_max_length() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "s", "type": "string", "min_length": 9, "max_length": 3}
            ]})),
            "`min_length` must be <= `max_length`",
        );
    }

    #[test]
    fn rejects_empty_enum() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "s", "type": "string", "enum": []}
            ]})),
            "`enum` must be non-empty",
        );
    }

    #[test]
    fn rejects_null_in_enum() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "s", "type": "string", "enum": ["a", null]}
            ]})),
            "must not contain null",
        );
    }

    #[test]
    fn rejects_enum_value_type_mismatch() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "n", "type": "integer", "enum": [1, "two"]}
            ]})),
            "does not match declared type",
        );
    }

    #[test]
    fn integer_type_rejects_float_enum_value() {
        assert_config_err(
            compile(json!({"version": "1", "fields": [
                {"name": "n", "type": "integer", "enum": [1.5]}
            ]})),
            "does not match declared type",
        );
    }

    #[test]
    fn number_type_accepts_integer_enum_value() {
        assert!(
            compile(json!({"version": "1", "fields": [
                {"name": "n", "type": "number", "enum": [1, 2.5]}
            ]}))
            .is_ok()
        );
    }

    #[test]
    fn type_matches_covers_all_types() {
        use ContractFieldType::*;
        assert!(type_matches(&json!("s"), String));
        assert!(!type_matches(&json!(1), String));
        assert!(type_matches(&json!(1), Integer));
        assert!(!type_matches(&json!(1.5), Integer));
        assert!(type_matches(&json!(1.5), Number));
        assert!(type_matches(&json!(1), Number));
        assert!(type_matches(&json!(true), Boolean));
        assert!(type_matches(&json!({}), Object));
        assert!(type_matches(&json!([]), Array));
        assert!(!type_matches(&json!(null), Boolean));
    }
}

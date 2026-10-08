//! Data contracts (issue #204): a declarative, versioned schema + semantic
//! contract for a pipeline's output — required fields, types, nullability,
//! enum sets, regex patterns, numeric/length bounds — enforced per page after
//! transforms and quality checks and before the sink write. Breaches `fail`,
//! `quarantine` (via the DLQ), or `warn` per the contract-level policy.
//!
//! Pure evaluation; the pipeline wires DLQ routing and metrics in
//! `run_stream` / `instrumented_apply_contract`.

pub mod compile;
pub mod config;
pub mod export;

pub use compile::{CompiledContract, CompiledField};
pub use config::{ContractFieldType, ContractSpec, FieldContract, OnBreach};
pub use export::{OPENLINEAGE_SCHEMA_FACET_URL, to_json_schema, to_openlineage_facet};

use crate::contract::compile::type_matches;
use crate::error::FaucetError;
use serde_json::Value;

/// One observed contract breach.
#[derive(Debug, Clone)]
pub struct ContractViolation {
    /// Stable rule label (closed set, safe as a metric label): `not_object`,
    /// `missing`, `null`, `type`, `enum`, `pattern`, `range`, `length`,
    /// `extra_field`.
    pub rule: &'static str,
    /// Breaching field name; `None` for whole-record rules (`not_object`).
    pub field: Option<String>,
    /// Human-readable breach message (goes into errors / DLQ envelopes).
    pub message: String,
    /// 0-based position within the original page — the DLQ envelope's
    /// `record_index` contract (position within the page that failed).
    pub page_index: usize,
}

impl ContractViolation {
    /// One-line human description: `field 'x': <message> (<rule>)`.
    pub fn describe(&self) -> String {
        match &self.field {
            Some(f) => format!("field '{f}': {} ({})", self.message, self.rule),
            None => format!("{} ({})", self.message, self.rule),
        }
    }
}

/// A record removed from the page by a `quarantine` breach, destined for the
/// DLQ.
#[derive(Debug, Clone)]
pub struct ViolatingRecord {
    pub record: Value,
    pub violation: ContractViolation,
}

/// Result of applying the contract pass to one page.
#[derive(Debug, Clone, Default)]
pub struct ContractOutcome {
    /// Records that conform (plus, under `warn`, records that breached).
    pub survivors: Vec<Value>,
    /// Breaching records routed to the DLQ (`quarantine` only).
    pub quarantined: Vec<ViolatingRecord>,
    /// Breaches observed under `warn` (their records stay in `survivors`).
    pub warned: Vec<ContractViolation>,
}

/// Apply the contract to one page. Pure: no metrics, no DLQ I/O.
///
/// Records are evaluated in order; per record, the first breach wins (fields
/// in declared order, then the extra-field check). Policy:
/// - `fail` — return [`FaucetError::ContractViolation`] on the first breach.
/// - `quarantine` — move breaching records to `quarantined`.
/// - `warn` — record the breach in `warned`, keep the record in `survivors`.
pub fn apply_contract(
    records: Vec<Value>,
    contract: &CompiledContract,
) -> Result<ContractOutcome, FaucetError> {
    let mut out = ContractOutcome::default();
    for (page_index, rec) in records.into_iter().enumerate() {
        match evaluate_record(&rec, contract, page_index) {
            None => out.survivors.push(rec),
            Some(violation) => match contract.on_breach {
                OnBreach::Fail => {
                    return Err(FaucetError::ContractViolation {
                        version: contract.version.clone(),
                        message: violation.describe(),
                    });
                }
                OnBreach::Quarantine => out.quarantined.push(ViolatingRecord {
                    record: rec,
                    violation,
                }),
                OnBreach::Warn => {
                    out.warned.push(violation);
                    out.survivors.push(rec);
                }
            },
        }
    }
    Ok(out)
}

/// Evaluate one record against the contract. Returns the first breach, or
/// `None` when the record conforms.
fn evaluate_record(
    rec: &Value,
    contract: &CompiledContract,
    page_index: usize,
) -> Option<ContractViolation> {
    let violation = |rule: &'static str, field: Option<&str>, message: String| ContractViolation {
        rule,
        field: field.map(str::to_string),
        message,
        page_index,
    };

    let Some(obj) = rec.as_object() else {
        return Some(violation(
            "not_object",
            None,
            format!("record is not a JSON object (got {})", json_type_name(rec)),
        ));
    };

    for f in &contract.fields {
        let value = match obj.get(&f.name) {
            None => {
                if f.required {
                    return Some(violation(
                        "missing",
                        Some(&f.name),
                        "required field is missing".into(),
                    ));
                }
                continue; // absent optional field: nothing else to check
            }
            Some(v) => v,
        };
        if value.is_null() {
            if f.nullable {
                continue; // explicit null allowed: skip the value checks
            }
            return Some(violation(
                "null",
                Some(&f.name),
                "field is null but not nullable".into(),
            ));
        }
        if !type_matches(value, f.field_type) {
            return Some(violation(
                "type",
                Some(&f.name),
                format!("expected {}, got {}", f.field_type, json_type_name(value)),
            ));
        }
        if let Some(values) = &f.allowed_values
            && !enum_contains(values, value)
        {
            return Some(violation(
                "enum",
                Some(&f.name),
                "value is not in the allowed set".into(),
            ));
        }
        if let Some(re) = &f.pattern {
            // compile() guarantees pattern ⇒ string type, checked above.
            let s = value.as_str().unwrap_or_default();
            if !re.is_match(s) {
                return Some(violation(
                    "pattern",
                    Some(&f.name),
                    format!("value does not match pattern '{re}'"),
                ));
            }
        }
        if (f.min.is_some() || f.max.is_some())
            && let Value::Number(n) = value
        {
            if let Some(min) = f.min
                && cmp_number_to_bound(n, min) == Some(std::cmp::Ordering::Less)
            {
                return Some(violation(
                    "range",
                    Some(&f.name),
                    format!("value is below the minimum {min}"),
                ));
            }
            if let Some(max) = f.max
                && cmp_number_to_bound(n, max) == Some(std::cmp::Ordering::Greater)
            {
                return Some(violation(
                    "range",
                    Some(&f.name),
                    format!("value is above the maximum {max}"),
                ));
            }
        }
        if f.min_length.is_some() || f.max_length.is_some() {
            let len = value.as_str().map(|s| s.chars().count()).unwrap_or(0);
            if let Some(min) = f.min_length
                && len < min
            {
                return Some(violation(
                    "length",
                    Some(&f.name),
                    format!("string length {len} is below the minimum {min}"),
                ));
            }
            if let Some(max) = f.max_length
                && len > max
            {
                return Some(violation(
                    "length",
                    Some(&f.name),
                    format!("string length {len} is above the maximum {max}"),
                ));
            }
        }
    }

    if !contract.allow_extra_fields {
        for key in obj.keys() {
            if !contract.declared.contains(key) {
                return Some(violation(
                    "extra_field",
                    Some(key),
                    "field is not declared in the contract and allow_extra_fields is false".into(),
                ));
            }
        }
    }

    None
}

/// Order a JSON number against an `f64` bound without rounding an integer
/// operand above 2^53 through `f64` (CORE-86).
fn cmp_number_to_bound(n: &serde_json::Number, bound: f64) -> Option<std::cmp::Ordering> {
    let int = n
        .as_i64()
        .map(i128::from)
        .or_else(|| n.as_u64().map(i128::from));
    match int {
        Some(i) => cmp_int_to_f64(i, bound),
        None => n.as_f64()?.partial_cmp(&bound),
    }
}

fn cmp_int_to_f64(i: i128, bound: f64) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if bound.is_nan() {
        return None;
    }
    let floor = bound.floor();
    if floor >= 1.0e20 {
        return Some(Ordering::Less);
    }
    if floor <= -1.0e20 {
        return Some(Ordering::Greater);
    }
    match i.cmp(&(floor as i128)) {
        Ordering::Equal if bound > floor => Some(Ordering::Less),
        other => Some(other),
    }
}

fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Enum membership with numeric-aware equality: two JSON numbers compare by
/// value (so an integer-spelled enum member `1` matches a conforming `1.0` on a
/// `number` field), everything else by exact structural equality. `compile`
/// already accepts an integer enum on a number field, and the per-record type
/// check runs before this, so no cross-type coercion sneaks in (audit #321 M2).
fn enum_contains(allowed: &[Value], value: &Value) -> bool {
    allowed.iter().any(|v| match (v, value) {
        (Value::Number(a), Value::Number(b)) => {
            if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                x == y
            } else if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
                x == y
            } else {
                matches!((a.as_f64(), b.as_f64()), (Some(x), Some(y)) if x == y)
            }
        }
        _ => v == value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn compiled(v: Value) -> CompiledContract {
        let spec: ContractSpec = serde_json::from_value(v).unwrap();
        CompiledContract::compile(&spec).unwrap()
    }

    fn basic(on_breach: &str) -> CompiledContract {
        compiled(json!({
            "version": "1.0.0",
            "on_breach": on_breach,
            "fields": [
                { "name": "id", "type": "integer", "min": 0 },
                { "name": "status", "type": "string", "enum": ["open", "closed"] },
                { "name": "email", "type": "string", "required": false,
                  "nullable": true, "pattern": "^.+@.+$", "min_length": 3 }
            ]
        }))
    }

    #[test]
    fn breach_messages_never_carry_the_value() {
        let c = compiled(json!({
            "version": "1.0.0",
            "on_breach": "fail",
            "fields": [
                { "name": "s", "type": "string", "required": false, "enum": ["a"] },
                { "name": "p", "type": "string", "required": false, "pattern": "^x$" },
                { "name": "n", "type": "integer", "required": false, "min": 0, "max": 5 }
            ]
        }));
        for rec in [
            json!({"s": "secret-enum"}),
            json!({"p": "secret-pattern"}),
            json!({"n": 987654321}),
            json!({"n": -987654321}),
        ] {
            let err = apply_contract(vec![rec], &c).unwrap_err().to_string();
            assert!(
                !err.contains("secret") && !err.contains("987654321"),
                "{err}"
            );
        }
    }

    #[test]
    fn integer_accepts_whole_floats_and_bounds_compare_exactly() {
        let c = compiled(json!({
            "version": "1.0.0",
            "on_breach": "fail",
            "fields": [ { "name": "n", "type": "integer", "max": 9007199254740992.0 } ]
        }));
        assert!(apply_contract(vec![json!({"n": 1.0})], &c).is_ok());
        assert!(apply_contract(vec![json!({"n": 1.5})], &c).is_err());
        assert!(apply_contract(vec![json!({"n": 9007199254740992u64})], &c).is_ok());
        // 2^53 + 1 rounds to 2^53 through f64 and used to pass.
        assert!(apply_contract(vec![json!({"n": 9007199254740993u64})], &c).is_err());
    }

    #[test]
    fn int_to_f64_bound_ordering_edges() {
        use std::cmp::Ordering;
        assert_eq!(cmp_int_to_f64(1, 1.5), Some(Ordering::Less));
        assert_eq!(cmp_int_to_f64(2, 1.5), Some(Ordering::Greater));
        assert_eq!(cmp_int_to_f64(1, 1.0), Some(Ordering::Equal));
        assert_eq!(cmp_int_to_f64(1, f64::NAN), None);
        assert_eq!(
            cmp_int_to_f64(i128::from(u64::MAX), 1e30),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp_int_to_f64(i128::from(i64::MIN), -1e30),
            Some(Ordering::Greater)
        );
        assert_eq!(
            cmp_number_to_bound(&serde_json::Number::from_f64(0.5).unwrap(), 1.0),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn conforming_page_passes_untouched() {
        let page = vec![
            json!({"id": 1, "status": "open", "email": "a@b.c"}),
            json!({"id": 2, "status": "closed"}), // optional email absent
            json!({"id": 3, "status": "open", "email": null}), // nullable
        ];
        let out = apply_contract(page.clone(), &basic("fail")).unwrap();
        assert_eq!(out.survivors, page);
        assert!(out.quarantined.is_empty());
        assert!(out.warned.is_empty());
    }

    #[test]
    fn numeric_enum_matches_across_int_float_spelling() {
        // #321 M2: an integer-spelled enum on a `number` field must accept a
        // conforming float value (and vice-versa) instead of spuriously breaching.
        let c = compiled(json!({
            "version": "1.0.0",
            "on_breach": "fail",
            "fields": [ { "name": "rate", "type": "number", "enum": [1, 2] } ]
        }));
        // 1.0 conforms to enum {1, 2} — must pass (was a false breach before).
        let out = apply_contract(vec![json!({"rate": 1.0})], &c).unwrap();
        assert_eq!(out.survivors.len(), 1);
        assert!(out.quarantined.is_empty());
        // A genuinely out-of-set value still breaches.
        let err = apply_contract(vec![json!({"rate": 3.0})], &c).unwrap_err();
        assert!(matches!(err, FaucetError::ContractViolation { .. }));
    }

    #[test]
    fn fail_raises_typed_error_with_version_and_field() {
        let page = vec![
            json!({"id": 1, "status": "open"}),
            json!({"id": 2, "status": "bogus"}),
        ];
        let err = apply_contract(page, &basic("fail")).unwrap_err();
        match err {
            FaucetError::ContractViolation { version, message } => {
                assert_eq!(version, "1.0.0");
                assert!(message.contains("status"), "message: {message}");
                assert!(message.contains("enum"), "message: {message}");
            }
            other => panic!("expected ContractViolation, got {other:?}"),
        }
    }

    #[test]
    fn quarantine_partitions_page_and_keeps_page_index() {
        let page = vec![
            json!({"id": 1, "status": "open"}),
            json!({"id": -5, "status": "open"}), // range breach at index 1
            json!({"id": 3, "status": "open"}),
            json!({"id": 4, "status": "nope"}), // enum breach at index 3
        ];
        let out = apply_contract(page, &basic("quarantine")).unwrap();
        assert_eq!(out.survivors.len(), 2);
        let idx: Vec<usize> = out
            .quarantined
            .iter()
            .map(|q| q.violation.page_index)
            .collect();
        assert_eq!(idx, vec![1, 3], "page_index must be the original position");
        assert_eq!(out.quarantined[0].violation.rule, "range");
        assert_eq!(out.quarantined[1].violation.rule, "enum");
    }

    #[test]
    fn warn_keeps_all_records_and_reports_breaches() {
        let page = vec![
            json!({"id": 1, "status": "open"}),
            json!({"id": "not-an-int", "status": "open"}),
        ];
        let out = apply_contract(page.clone(), &basic("warn")).unwrap();
        assert_eq!(out.survivors, page, "warn must not remove records");
        assert!(out.quarantined.is_empty());
        assert_eq!(out.warned.len(), 1);
        assert_eq!(out.warned[0].rule, "type");
        assert_eq!(out.warned[0].field.as_deref(), Some("id"));
        assert_eq!(out.warned[0].page_index, 1);
    }

    #[test]
    fn missing_required_field_breaches() {
        let out = apply_contract(vec![json!({"status": "open"})], &basic("warn")).unwrap();
        assert_eq!(out.warned[0].rule, "missing");
        assert_eq!(out.warned[0].field.as_deref(), Some("id"));
    }

    #[test]
    fn null_on_non_nullable_breaches() {
        let out =
            apply_contract(vec![json!({"id": null, "status": "open"})], &basic("warn")).unwrap();
        assert_eq!(out.warned[0].rule, "null");
    }

    #[test]
    fn pattern_and_length_breaches() {
        let c = basic("warn");
        let out = apply_contract(
            vec![json!({"id": 1, "status": "open", "email": "nope"})],
            &c,
        )
        .unwrap();
        assert_eq!(out.warned[0].rule, "pattern");
        let out =
            apply_contract(vec![json!({"id": 1, "status": "open", "email": "@b"})], &c).unwrap();
        // "@b" fails min_length 3? len 2 < 3 → but pattern checked first: "@b"
        // does not match ^.+@.+$ either. Pattern wins (declared order of rules).
        assert_eq!(out.warned[0].rule, "pattern");
        let out =
            apply_contract(vec![json!({"id": 1, "status": "open", "email": "a@c"})], &c).unwrap();
        assert!(out.warned.is_empty(), "a@c matches pattern and min_length");
    }

    #[test]
    fn min_length_breach_reported() {
        let c = compiled(json!({
            "version": "1", "on_breach": "warn",
            "fields": [{ "name": "s", "type": "string", "min_length": 3, "max_length": 5 }]
        }));
        let out = apply_contract(vec![json!({"s": "ab"})], &c).unwrap();
        assert_eq!(out.warned[0].rule, "length");
        let out = apply_contract(vec![json!({"s": "abcdef"})], &c).unwrap();
        assert_eq!(out.warned[0].rule, "length");
        // char count, not byte count
        let out = apply_contract(vec![json!({"s": "héllo"})], &c).unwrap();
        assert!(out.warned.is_empty());
    }

    #[test]
    fn integer_type_rejects_float_value() {
        let out =
            apply_contract(vec![json!({"id": 1.5, "status": "open"})], &basic("warn")).unwrap();
        assert_eq!(out.warned[0].rule, "type");
    }

    #[test]
    fn extra_field_breach_when_disallowed() {
        let c = compiled(json!({
            "version": "1", "on_breach": "warn", "allow_extra_fields": false,
            "fields": [{ "name": "id", "type": "integer" }]
        }));
        let out = apply_contract(vec![json!({"id": 1, "surprise": true})], &c).unwrap();
        assert_eq!(out.warned[0].rule, "extra_field");
        assert_eq!(out.warned[0].field.as_deref(), Some("surprise"));
    }

    #[test]
    fn extra_field_allowed_by_default() {
        let c = compiled(json!({
            "version": "1", "on_breach": "warn",
            "fields": [{ "name": "id", "type": "integer" }]
        }));
        let out = apply_contract(vec![json!({"id": 1, "surprise": true})], &c).unwrap();
        assert!(out.warned.is_empty());
    }

    #[test]
    fn non_object_record_breaches() {
        let out = apply_contract(vec![json!([1, 2, 3])], &basic("warn")).unwrap();
        assert_eq!(out.warned[0].rule, "not_object");
        assert!(out.warned[0].field.is_none());
    }

    #[test]
    fn first_breach_wins_in_declared_field_order() {
        // Both `id` (type) and `status` (enum) breach; `id` is declared first.
        let out =
            apply_contract(vec![json!({"id": "x", "status": "nope"})], &basic("warn")).unwrap();
        assert_eq!(out.warned.len(), 1, "one breach per record");
        assert_eq!(out.warned[0].field.as_deref(), Some("id"));
    }

    #[test]
    fn range_bounds_inclusive() {
        let c = compiled(json!({
            "version": "1", "on_breach": "warn",
            "fields": [{ "name": "n", "type": "number", "min": 0.0, "max": 10.0 }]
        }));
        let out = apply_contract(vec![json!({"n": 0.0}), json!({"n": 10.0})], &c).unwrap();
        assert!(out.warned.is_empty(), "bounds are inclusive");
        let out = apply_contract(vec![json!({"n": 10.001})], &c).unwrap();
        assert_eq!(out.warned[0].rule, "range");
    }

    #[test]
    fn violation_describe_includes_field_and_rule() {
        let v = ContractViolation {
            rule: "enum",
            field: Some("status".into()),
            message: "value \"x\" is not in the allowed set".into(),
            page_index: 0,
        };
        let d = v.describe();
        assert!(d.contains("field 'status'"));
        assert!(d.contains("(enum)"));
        let v2 = ContractViolation {
            rule: "not_object",
            field: None,
            message: "record is not a JSON object (got array)".into(),
            page_index: 0,
        };
        assert!(!v2.describe().contains("field"));
    }
}

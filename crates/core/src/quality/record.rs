//! Per-record check evaluation. Pure functions over `&Value`. A check returns
//! `Ok(())` on pass or `Err(message)` on fail; the message is surfaced in the
//! DLQ envelope or the abort error.

use crate::quality::compile::{CompiledRecordCheck, CompiledRecordKind};
use crate::quality::config::{CompareOp, JsonType};
use serde_json::Value;

/// Evaluate one compiled per-record check against a record. `Ok(())` = pass;
/// `Err(message)` = fail (message is human-readable, used in DLQ/abort text).
pub fn evaluate_record_check(c: &CompiledRecordCheck, rec: &Value) -> Result<(), String> {
    match &c.kind {
        CompiledRecordKind::NotNull {
            path,
            treat_missing_as_null,
        } => match path.resolve(rec).ok().flatten() {
            Some(Value::Null) => Err("value was null".into()),
            Some(_) => Ok(()),
            None => {
                if *treat_missing_as_null {
                    Err("field was missing".into())
                } else {
                    Ok(())
                }
            }
        },
        CompiledRecordKind::NotEmpty { path } => match path.resolve(rec).ok().flatten() {
            Some(Value::String(s)) if !s.trim().is_empty() => Ok(()),
            Some(Value::String(_)) => Err("string was empty/whitespace".into()),
            Some(Value::Null) => Err("value was null".into()),
            Some(_) => Err("value was not a string".into()),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::RegexMatch { path, re } => match path.resolve(rec).ok().flatten() {
            Some(Value::String(s)) if re.is_match(s) => Ok(()),
            Some(Value::String(_)) => Err("value did not match pattern".into()),
            Some(_) => Err("value was not a string".into()),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::ValueInSet { path, index, .. } => {
            match path.resolve(rec).ok().flatten() {
                Some(v) if index.contains(v) => Ok(()),
                Some(_) => Err("value not in allowed set".into()),
                None => Err("field was missing".into()),
            }
        }
        CompiledRecordKind::NotInSet { path, index, .. } => {
            match path.resolve(rec).ok().flatten() {
                Some(v) if index.contains(v) => Err("value is in the forbidden set".into()),
                // present-and-not-in-set OR missing -> pass
                _ => Ok(()),
            }
        }
        CompiledRecordKind::Compare { path, op, value } => {
            let resolved = path.resolve(rec).ok().flatten();
            let Some(actual) = resolved else {
                return Err("field was missing".into());
            };
            evaluate_compare(*op, actual, value)
        }
        CompiledRecordKind::TypeIs { path, expected } => match path.resolve(rec).ok().flatten() {
            Some(v) if json_type_matches(v, *expected) => Ok(()),
            Some(_) => Err(format!("value was not of type {expected}")),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::StringLength { path, min, max } => {
            match path.resolve(rec).ok().flatten() {
                Some(Value::String(s)) => {
                    let len = s.chars().count();
                    if let Some(lo) = min
                        && len < *lo
                    {
                        return Err(format!("string length {len} < min {lo}"));
                    }
                    if let Some(hi) = max
                        && len > *hi
                    {
                        return Err(format!("string length {len} > max {hi}"));
                    }
                    Ok(())
                }
                Some(_) => Err("value was not a string".into()),
                None => Err("field was missing".into()),
            }
        }
        #[cfg(feature = "quality-jsonschema")]
        CompiledRecordKind::JsonSchema { validator } => {
            if validator.is_valid(rec) {
                Ok(())
            } else {
                let msg = validator
                    .iter_errors(rec)
                    .next()
                    .map(|e| {
                        format!(
                            "record did not validate against schema at `{}` (rule `{}`)",
                            e.instance_path(),
                            e.schema_path()
                        )
                    })
                    .unwrap_or_else(|| "record did not validate against schema".into());
                Err(msg)
            }
        }
    }
}

/// Order two JSON numbers without lossy `f64` conversion when both are
/// integers. `serde_json::Number` may hold an `i64`, a `u64` (for magnitudes
/// above `i64::MAX`), or an `f64`; converting an integer above 2^53 to `f64`
/// rounds it, so a naive `as_f64()` comparison can decide the wrong ordering
/// for large 64-bit integers. We compare integer operands exactly (handling
/// the mixed signed/unsigned case) and only fall back to `f64` when at least
/// one operand is genuinely a floating-point value. Returns `None` only when a
/// float operand is non-finite (not representable for JSON numbers).
fn cmp_numbers(a: &serde_json::Number, b: &serde_json::Number) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Some(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_u64(), b.as_u64()) {
        return Some(x.cmp(&y));
    }
    // Mixed integer signedness: an `i64`-only operand is negative (so it does
    // not fit `u64`) while a `u64`-only operand exceeds `i64::MAX` — the
    // negative is always the smaller of the two.
    if a.as_i64().is_some() && b.as_u64().is_some() {
        return Some(Ordering::Less);
    }
    if a.as_u64().is_some() && b.as_i64().is_some() {
        return Some(Ordering::Greater);
    }
    // At least one operand is a float: lossy comparison is unavoidable here.
    a.as_f64()?.partial_cmp(&b.as_f64()?)
}

/// JSON equality used by `compare` `eq`/`ne`. For two JSON numbers this is
/// *numeric* equality (so `1` and `1.0` are equal, and large 64-bit integers
/// compare exactly via [`cmp_numbers`] without the `f64` rounding that
/// structural `Value` equality would sidestep but that a naive `as_f64`
/// wouldn't) — a number's int-vs-float spelling should not change a value
/// comparison (F48). All other types fall back to exact structural equality, so
/// there is still no cross-type coercion (string `"5"` ≠ number `5`).
fn values_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            cmp_numbers(x, y) == Some(std::cmp::Ordering::Equal)
        }
        _ => a == b,
    }
}

/// Set membership for `value_in_set` / `not_in_set` using numeric-aware
/// equality ([`values_equal`]) rather than structural `Vec::contains`, so an
/// int/float spelling difference (`0` vs `0.0`) does not bypass the check or
/// cause a false quarantine (audit #321 M1).
#[cfg(test)]
fn set_contains(values: &[Value], v: &Value) -> bool {
    values.iter().any(|candidate| values_equal(candidate, v))
}

/// Membership index for `value_in_set` / `not_in_set`, with the same
/// numeric-aware equality as a linear scan: strings hash exactly, numbers are
/// bucketed by their `f64` value and confirmed exactly, and the remaining
/// values (booleans, null, objects, arrays) are scanned.
#[derive(Debug, Clone, Default)]
pub struct ValueSetIndex {
    strings: std::collections::HashSet<String>,
    numbers: std::collections::HashMap<u64, Vec<serde_json::Number>>,
    rest: Vec<Value>,
}

fn number_bucket(n: &serde_json::Number) -> Option<u64> {
    let f = n.as_f64()?;
    Some(if f == 0.0 {
        0.0f64.to_bits()
    } else {
        f.to_bits()
    })
}

impl ValueSetIndex {
    /// Index `values`.
    pub fn new(values: &[Value]) -> Self {
        let mut idx = Self::default();
        for v in values {
            match v {
                Value::String(s) => {
                    idx.strings.insert(s.clone());
                }
                Value::Number(n) => match number_bucket(n) {
                    Some(b) => idx.numbers.entry(b).or_default().push(n.clone()),
                    None => idx.rest.push(v.clone()),
                },
                other => idx.rest.push(other.clone()),
            }
        }
        idx
    }

    /// Whether `v` equals a member of the set.
    pub fn contains(&self, v: &Value) -> bool {
        match v {
            Value::String(s) => self.strings.contains(s.as_str()),
            Value::Number(n) => {
                let in_bucket = number_bucket(n)
                    .and_then(|b| self.numbers.get(&b))
                    .is_some_and(|ns| {
                        ns.iter()
                            .any(|c| cmp_numbers(c, n) == Some(std::cmp::Ordering::Equal))
                    });
                in_bucket || self.rest.iter().any(|c| values_equal(c, v))
            }
            other => self.rest.iter().any(|c| values_equal(c, other)),
        }
    }
}

/// An exact decimal: `±digits × 10^exp`, `digits` without leading zeros
/// (empty for zero).
#[derive(Debug, PartialEq, Eq)]
struct Decimal {
    negative: bool,
    digits: Vec<u8>,
    exp: i64,
}

impl Decimal {
    fn parse(s: &str) -> Option<Self> {
        let (negative, rest) = match s.as_bytes().first()? {
            b'-' => (true, &s[1..]),
            b'+' => (false, &s[1..]),
            _ => (false, s),
        };
        let (mantissa, exp) = match rest.find(['e', 'E']) {
            Some(i) => (&rest[..i], rest[i + 1..].parse::<i64>().ok()?),
            None => (rest, 0),
        };
        let (int, frac) = match mantissa.split_once('.') {
            Some((i, f)) => (i, f),
            None => (mantissa, ""),
        };
        if int.is_empty() && frac.is_empty() {
            return None;
        }
        if !int.bytes().chain(frac.bytes()).all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut digits: Vec<u8> = int.bytes().chain(frac.bytes()).map(|b| b - b'0').collect();
        let mut exp = exp.checked_sub(i64::try_from(frac.len()).ok()?)?;
        while digits.last() == Some(&0) {
            digits.pop();
            exp = exp.checked_add(1)?;
        }
        let lead = digits.iter().take_while(|d| **d == 0).count();
        digits.drain(..lead);
        let negative = negative && !digits.is_empty();
        Some(Self {
            negative,
            digits,
            exp,
        })
    }

    fn magnitude_cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self.digits.is_empty(), other.digits.is_empty()) {
            (true, true) => return Ordering::Equal,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            _ => {}
        }
        let top = |d: &Self| d.exp as i128 + d.digits.len() as i128;
        top(self)
            .cmp(&top(other))
            .then_with(|| self.digits.cmp(&other.digits))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.negative, other.negative) {
            (false, true) => std::cmp::Ordering::Greater,
            (true, false) => std::cmp::Ordering::Less,
            (false, false) => self.magnitude_cmp(other),
            (true, true) => other.magnitude_cmp(self),
        }
    }
}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// A `compare` ordering operand as an exact decimal: a JSON number, or a
/// string holding one (how exact decimals and big integers are rendered).
fn decimal_operand(v: &Value) -> Option<Decimal> {
    match v {
        Value::Number(n) => Decimal::parse(&n.to_string()),
        Value::String(s) => Decimal::parse(s),
        _ => None,
    }
}

/// Evaluate a `compare` check. Ordering ops (`gt`/`gte`/`lt`/`lte`) compare
/// integer operands exactly via [`cmp_numbers`] (no precision loss above
/// 2^53), and a string holding an exact decimal compares by its exact value; `eq`/`ne` compare numbers numerically (`1` == `1.0`) and all other
/// types by exact structural JSON equality via [`values_equal`].
fn evaluate_compare(op: CompareOp, actual: &Value, expected: &Value) -> Result<(), String> {
    use std::cmp::Ordering;
    match op {
        CompareOp::Eq => {
            if values_equal(actual, expected) {
                Ok(())
            } else {
                Err("values were not equal".into())
            }
        }
        CompareOp::Ne => {
            if !values_equal(actual, expected) {
                Ok(())
            } else {
                Err("values were equal".into())
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            let ord = match (actual, expected) {
                (Value::Number(a), Value::Number(b)) => cmp_numbers(a, b),
                _ => match (decimal_operand(actual), decimal_operand(expected)) {
                    (Some(a), Some(b)) => Some(a.cmp(&b)),
                    _ => None,
                },
            };
            let Some(ord) = ord else {
                return Err("value was not numeric".into());
            };
            let ok = match op {
                CompareOp::Gt => ord == Ordering::Greater,
                CompareOp::Gte => ord != Ordering::Less,
                CompareOp::Lt => ord == Ordering::Less,
                CompareOp::Lte => ord != Ordering::Greater,
                _ => unreachable!(),
            };
            if ok {
                Ok(())
            } else {
                Err(format!("comparison {op} {expected} failed"))
            }
        }
    }
}

fn json_type_matches(v: &Value, expected: JsonType) -> bool {
    matches!(
        (v, expected),
        (Value::Bool(_), JsonType::Boolean)
            | (Value::Number(_), JsonType::Number)
            | (Value::String(_), JsonType::String)
            | (Value::Array(_), JsonType::Array)
            | (Value::Object(_), JsonType::Object)
            | (Value::Null, JsonType::Null)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quality::compile::CompiledQuality;
    use crate::quality::config::{CompareOp, JsonType, OnFailure, QualitySpec, RecordCheck};
    use serde_json::json;

    #[test]
    fn ordering_compares_decimal_strings_exactly() {
        let ok = |op, a: Value, b: Value| evaluate_compare(op, &a, &b).is_ok();
        assert!(ok(CompareOp::Gte, json!("12.50"), json!(0)));
        assert!(ok(CompareOp::Lt, json!("-0.01"), json!(0)));
        assert!(!ok(CompareOp::Gt, json!("0.00"), json!(0)));
        assert!(ok(CompareOp::Gte, json!("-0"), json!(0)));
        assert!(ok(
            CompareOp::Gt,
            json!("100000000000000000000.000000001"),
            json!("1e20")
        ));
        assert!(ok(
            CompareOp::Lt,
            json!("12345678901234567890123"),
            json!("12345678901234567890124")
        ));
        assert!(ok(CompareOp::Lte, json!(2.5), json!("2.50")));
        assert!(ok(CompareOp::Gt, json!("-1.5"), json!("-1.51")));
        assert!(ok(CompareOp::Lt, json!(".5"), json!("1")));
        for bad in [
            json!("abc"),
            json!("1.2.3"),
            json!(""),
            json!("-"),
            json!(" 1"),
            json!("1e"),
            json!(true),
        ] {
            assert_eq!(
                evaluate_compare(CompareOp::Gt, &bad, &json!(0)),
                Err("value was not numeric".to_string()),
                "{bad}"
            );
        }
    }

    #[test]
    fn set_index_agrees_with_a_linear_scan() {
        let members = vec![
            json!("a"),
            json!(1),
            json!(2.5),
            json!(0.0),
            json!(9007199254740993u64),
            json!(-3),
            json!(true),
            json!(null),
            json!({"k": [1, 2]}),
            json!([1, "x"]),
        ];
        let idx = ValueSetIndex::new(&members);
        let probes = [
            json!("a"),
            json!("b"),
            json!(1.0),
            json!(1),
            json!(2.5),
            json!(-0.0),
            json!(0),
            json!(9007199254740993u64),
            json!(9007199254740992u64),
            json!(-3.0),
            json!(true),
            json!(false),
            json!(null),
            json!({"k": [1, 2]}),
            json!({"k": [2, 1]}),
            json!([1, "x"]),
            json!("1"),
        ];
        for p in &probes {
            assert_eq!(idx.contains(p), set_contains(&members, p), "{p}");
        }
    }

    fn one(check: RecordCheck) -> crate::quality::compile::CompiledRecordCheck {
        let spec = QualitySpec {
            record: vec![check],
            batch: vec![],
        };
        CompiledQuality::compile(&spec)
            .unwrap()
            .record
            .pop()
            .unwrap()
    }

    #[test]
    fn not_null_passes_present_fails_missing_and_null() {
        let c = one(RecordCheck::NotNull {
            field: "id".into(),
            treat_missing_as_null: true,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"id": 1})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"id": null})).is_err());
        assert!(evaluate_record_check(&c, &json!({})).is_err());
    }

    #[test]
    fn not_null_treat_missing_false_only_explicit_null_fails() {
        let c = one(RecordCheck::NotNull {
            field: "id".into(),
            treat_missing_as_null: false,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({})).is_ok()); // missing -> pass
        assert!(evaluate_record_check(&c, &json!({"id": null})).is_err());
    }

    #[test]
    fn not_empty() {
        let c = one(RecordCheck::NotEmpty {
            field: "name".into(),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"name": "x"})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"name": "  "})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": ""})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": 5})).is_err()); // non-string
        assert!(evaluate_record_check(&c, &json!({})).is_err()); // missing
    }

    #[test]
    fn regex_match() {
        let c = one(RecordCheck::RegexMatch {
            field: "email".into(),
            pattern: r"^[^@]+@[^@]+\.[^@]+$".into(),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"email": "a@b.com"})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"email": "nope"})).is_err());
        assert!(evaluate_record_check(&c, &json!({"email": 1})).is_err());
        assert!(evaluate_record_check(&c, &json!({})).is_err());
    }

    #[test]
    fn value_in_set_and_not_in_set_missing_handling() {
        let in_set = one(RecordCheck::ValueInSet {
            field: "status".into(),
            values: vec![json!("active"), json!("closed")],
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&in_set, &json!({"status": "active"})).is_ok());
        assert!(evaluate_record_check(&in_set, &json!({"status": "x"})).is_err());
        assert!(evaluate_record_check(&in_set, &json!({})).is_err()); // missing -> fail

        let not_in = one(RecordCheck::NotInSet {
            field: "status".into(),
            values: vec![json!("banned")],
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&not_in, &json!({"status": "active"})).is_ok());
        assert!(evaluate_record_check(&not_in, &json!({"status": "banned"})).is_err());
        assert!(evaluate_record_check(&not_in, &json!({})).is_ok()); // missing -> pass
    }

    #[test]
    fn set_checks_match_numerically_not_structurally() {
        // #321 M1: int/float spelling must not bypass the set checks.
        let in_set = one(RecordCheck::ValueInSet {
            field: "n".into(),
            values: vec![json!(5)],
            on_failure: OnFailure::Quarantine,
        });
        // 5.0 is in {5} numerically → must pass (was wrongly quarantined before).
        assert!(evaluate_record_check(&in_set, &json!({"n": 5.0})).is_ok());

        let not_in = one(RecordCheck::NotInSet {
            field: "n".into(),
            values: vec![json!(0)],
            on_failure: OnFailure::Quarantine,
        });
        // 0.0 is in the forbidden set {0} numerically → must fail (was bypassed).
        assert!(evaluate_record_check(&not_in, &json!({"n": 0.0})).is_err());
    }

    #[test]
    fn compare_ordering_and_equality() {
        let gte = one(RecordCheck::Compare {
            field: "age".into(),
            op: CompareOp::Gte,
            value: json!(0),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&gte, &json!({"age": 0})).is_ok());
        assert!(evaluate_record_check(&gte, &json!({"age": -1})).is_err());
        assert!(evaluate_record_check(&gte, &json!({"age": "x"})).is_err()); // non-numeric
        assert!(evaluate_record_check(&gte, &json!({})).is_err()); // missing

        let eq = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Eq,
            value: json!(5),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&eq, &json!({"v": 5})).is_ok());
        assert!(evaluate_record_check(&eq, &json!({"v": "5"})).is_err()); // no coercion

        let ne = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Ne,
            value: json!(5),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&ne, &json!({"v": 6})).is_ok()); // 6 != 5 -> pass
        assert!(evaluate_record_check(&ne, &json!({"v": 5})).is_err()); // 5 == 5 -> fail
    }

    #[test]
    fn compare_eq_ne_match_numbers_by_value_not_spelling() {
        // `eq`/`ne` on numbers compare numerically, so an integer-vs-float
        // spelling difference (5 vs 5.0) does not flip the result (F48).
        let eq = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Eq,
            value: json!(5), // serde parses the literal as an integer
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&eq, &json!({"v": 5.0})).is_ok()); // float 5.0 == int 5
        assert!(evaluate_record_check(&eq, &json!({"v": 5})).is_ok());
        assert!(evaluate_record_check(&eq, &json!({"v": 5.5})).is_err());
        // Cross-type is still NOT coerced: string "5" != number 5.
        assert!(evaluate_record_check(&eq, &json!({"v": "5"})).is_err());

        let ne = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Ne,
            value: json!(5),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&ne, &json!({"v": 5.0})).is_err()); // 5.0 == 5 -> ne fails
        assert!(evaluate_record_check(&ne, &json!({"v": 6})).is_ok());

        // Large 64-bit integers compare exactly (no f64 collapse): 2^53 and
        // 2^53+1 are distinct under `eq` even though they share an f64.
        let big = 9_007_199_254_740_992i64; // 2^53
        let eq_big = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Eq,
            value: json!(big),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&eq_big, &json!({"v": big})).is_ok());
        assert!(evaluate_record_check(&eq_big, &json!({"v": big + 1})).is_err());

        // Non-number operands still use structural equality.
        let eq_obj = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Eq,
            value: json!({"a": 1}),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&eq_obj, &json!({"v": {"a": 1}})).is_ok());
        assert!(evaluate_record_check(&eq_obj, &json!({"v": {"a": 2}})).is_err());
    }

    #[test]
    fn compare_ordering_is_exact_above_2_pow_53() {
        // 2^53 and 2^53+1 are distinct integers but collapse to the same f64.
        // A lossy comparison would treat `gt(2^53)` as failing for 2^53+1.
        let threshold = 9_007_199_254_740_992i64; // 2^53
        let gt = one(RecordCheck::Compare {
            field: "id".into(),
            op: CompareOp::Gt,
            value: json!(threshold),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&gt, &json!({"id": threshold + 1})).is_ok());
        assert!(evaluate_record_check(&gt, &json!({"id": threshold})).is_err());

        // Two distinct u64 values above i64::MAX that round to the same f64.
        let big = 18_446_744_073_709_551_614u64; // u64::MAX - 1
        let lte = one(RecordCheck::Compare {
            field: "id".into(),
            op: CompareOp::Lte,
            value: json!(big),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&lte, &json!({"id": big})).is_ok());
        assert!(evaluate_record_check(&lte, &json!({"id": u64::MAX})).is_err());
    }

    #[test]
    fn compare_ordering_handles_mixed_sign_and_floats() {
        let lt = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Lt,
            value: json!(u64::MAX),
            on_failure: OnFailure::Abort,
        });
        // negative i64 vs huge u64: -1 < u64::MAX
        assert!(evaluate_record_check(&lt, &json!({"v": -1})).is_ok());

        let gt = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Gt,
            value: json!(-1),
            on_failure: OnFailure::Abort,
        });
        // huge u64 vs negative i64: u64::MAX > -1
        assert!(evaluate_record_check(&gt, &json!({"v": u64::MAX})).is_ok());

        // float operands still compare
        let gte = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Gte,
            value: json!(1.5),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&gte, &json!({"v": 1.5})).is_ok());
        assert!(evaluate_record_check(&gte, &json!({"v": 1.4})).is_err());
    }

    #[test]
    fn not_empty_null_reports_null() {
        let c = one(RecordCheck::NotEmpty {
            field: "name".into(),
            on_failure: OnFailure::Quarantine,
        });
        let err = evaluate_record_check(&c, &json!({"name": null})).unwrap_err();
        assert!(err.contains("null"));
    }

    #[test]
    fn type_is() {
        let b = one(RecordCheck::TypeIs {
            field: "active".into(),
            expected: JsonType::Boolean,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&b, &json!({"active": true})).is_ok());
        assert!(evaluate_record_check(&b, &json!({"active": 1})).is_err());
        assert!(evaluate_record_check(&b, &json!({})).is_err());

        let n = one(RecordCheck::TypeIs {
            field: "x".into(),
            expected: JsonType::Null,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&n, &json!({"x": null})).is_ok());
        assert!(evaluate_record_check(&n, &json!({})).is_err()); // missing != null
    }

    #[test]
    fn string_length() {
        let c = one(RecordCheck::StringLength {
            field: "name".into(),
            min: Some(1),
            max: Some(3),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"name": "ab"})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"name": ""})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": "abcd"})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": "é"})).is_ok()); // 1 char
        assert!(evaluate_record_check(&c, &json!({"name": 5})).is_err());
    }

    #[cfg(feature = "quality-jsonschema")]
    #[test]
    fn json_schema() {
        let c = one(RecordCheck::JsonSchema {
            schema: json!({"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"id": 1})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"id": "x"})).is_err());
        assert!(evaluate_record_check(&c, &json!({})).is_err());
        let msg = evaluate_record_check(&c, &json!({"id": "secret-value"})).unwrap_err();
        assert!(
            !msg.contains("secret-value") && msg.contains("/id"),
            "{msg}"
        );
    }

    #[test]
    fn compare_failure_message_omits_the_record_value() {
        let gt = one(RecordCheck::Compare {
            field: "id".into(),
            op: CompareOp::Lt,
            value: json!(10),
            on_failure: OnFailure::Abort,
        });
        let msg = evaluate_record_check(&gt, &json!({"id": 4242})).unwrap_err();
        assert!(!msg.contains("4242") && msg.contains("10"), "{msg}");
    }
}

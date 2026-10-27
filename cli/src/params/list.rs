//! `type: list` params (#952): a caller-chosen list of scalars or flat
//! objects, held to an `items:` spec and size limits.
//!
//! A list is what a source template's stream repeats over (`for_each:`), so
//! every element is coerced and checked *before* anything is expanded: a
//! wrong-typed element, one outside its closed `values:` set, a missing
//! required field or a list over `max_items` is refused naming the element.

use std::collections::BTreeSet;

use serde_json::{Map, Value};

use super::spec::{
    ParamSpec, ParamType, coerce as coerce_kind, refuse_list_fields, validate_name_of,
    validate_scalar, values_match,
};
use crate::error::{CliError, CliResult};
use crate::interpolate::value_to_string;

/// The fewest items a list holds when the spec does not say: an empty list is
/// refused, because it would run a pipeline with no rows.
pub const DEFAULT_MIN_ITEMS: usize = 1;

fn items_of<'a>(name: &str, p: &'a ParamSpec) -> CliResult<&'a ParamSpec> {
    p.items.as_deref().ok_or_else(|| {
        CliError::Config(format!(
            "param '{name}': a `type: list` param needs `items:` — the type of each element \
             (`items: {{ type: string }}`, or `{{ type: object, properties: {{ … }} }}`)"
        ))
    })
}

/// Load-time checks of a `type: list` param.
pub fn validate(name: &str, p: &ParamSpec) -> CliResult<()> {
    if p.computed.is_some() {
        return Err(CliError::Config(format!(
            "param '{name}': a list cannot be `computed`"
        )));
    }
    if !p.values.is_empty() {
        return Err(CliError::Config(format!(
            "param '{name}': a list takes no `values:` — put the closed set on its `items:`"
        )));
    }
    if !p.properties.is_empty() {
        return Err(CliError::Config(format!(
            "param '{name}': `properties` belong under `items:` (`items: {{ type: object, \
             properties: {{ … }} }}`)"
        )));
    }
    if p.required && p.default.is_some() {
        return Err(CliError::Config(format!(
            "param '{name}' is both `required: true` and has a `default` — drop one"
        )));
    }
    let items = items_of(name, p)?;
    let label = format!("{name}[]");
    for (field, set) in [
        ("required", items.required),
        ("default", items.default.is_some()),
        ("computed", items.computed.is_some()),
        ("min_items", items.min_items.is_some()),
        ("max_items", items.max_items.is_some()),
    ] {
        if set {
            return Err(CliError::Config(format!(
                "param '{name}': `items.{field}` does not apply to list items"
            )));
        }
    }
    match items.kind {
        ParamType::List => {
            return Err(CliError::Config(format!(
                "param '{name}': a list of lists is not supported — use `items: {{ type: \
                 object, properties: {{ … }} }}` for structured items"
            )));
        }
        ParamType::Object => {
            if items.items.is_some() {
                return Err(CliError::Config(format!(
                    "param '{name}': object items take `properties`, not `items`"
                )));
            }
            if !items.values.is_empty() {
                return Err(CliError::Config(format!(
                    "param '{name}': object items take no `values:` — put a closed set on a \
                     property"
                )));
            }
            if items.properties.is_empty() {
                return Err(CliError::Config(format!(
                    "param '{name}': `items: {{ type: object }}` needs `properties`"
                )));
            }
            for (field, fp) in &items.properties {
                validate_name_of("field", field)?;
                let flabel = format!("{name}[].{field}");
                if !fp.kind.is_scalar() {
                    return Err(CliError::Config(format!(
                        "param '{flabel}': an item field must be a scalar (string, int, float, \
                         bool), not a {}",
                        fp.kind.as_str()
                    )));
                }
                refuse_list_fields(&flabel, fp)?;
                if fp.computed.is_some() {
                    return Err(CliError::Config(format!(
                        "param '{flabel}': an item field cannot be `computed`"
                    )));
                }
                validate_scalar(&flabel, fp)?;
            }
        }
        _ => {
            refuse_list_fields(&label, items)?;
            validate_scalar(&label, items)?;
        }
    }
    let min = p.min_items.unwrap_or(DEFAULT_MIN_ITEMS);
    if let Some(max) = p.max_items {
        if max == 0 {
            return Err(CliError::Config(format!(
                "param '{name}': `max_items: 0` allows no value at all"
            )));
        }
        if min > max {
            return Err(CliError::Config(format!(
                "param '{name}': `min_items` ({min}) is above `max_items` ({max})"
            )));
        }
    }
    if let Some(d) = &p.default {
        // A `${…}` default resolves before binding; any other must be a valid list.
        let deferred = d.as_str().is_some_and(|s| s.contains("${"));
        if !deferred {
            coerce(name, p, d).map_err(|e| {
                CliError::Config(format!("param '{name}': the default is not valid: {e}"))
            })?;
        }
    }
    Ok(())
}

/// The elements a raw value names: a JSON array; a string holding one (the
/// CLI's `--param tables='["a","b"]'`); or, for a list of strings, a
/// comma-separated `a,b`.
fn elements(name: &str, items: &ParamSpec, raw: &Value) -> CliResult<Vec<Value>> {
    let expected = || {
        CliError::Config(format!(
            "param '{name}': expected a list — a JSON array (`[\"a\", \"b\"]`){}",
            if items.kind == ParamType::String {
                ", or comma-separated text (`a,b`)"
            } else {
                ""
            }
        ))
    };
    match raw {
        Value::Array(a) => Ok(a.clone()),
        Value::String(s) => {
            let t = s.trim();
            if t.starts_with('[') {
                match serde_json::from_str::<Value>(t) {
                    Ok(Value::Array(a)) => Ok(a),
                    Ok(_) => Err(expected()),
                    Err(e) => Err(CliError::Config(format!(
                        "param '{name}': the value is not a JSON array: {e}"
                    ))),
                }
            } else if items.kind == ParamType::String {
                Ok(if t.is_empty() {
                    Vec::new()
                } else {
                    t.split(',')
                        .map(|x| Value::String(x.trim().to_string()))
                        .collect()
                })
            } else {
                Err(expected())
            }
        }
        _ => Err(expected()),
    }
}

/// A float element: a JSON number as given, or text read exactly through
/// `faucet_core::fidelity` (so `0.1` stays `0.1`, never a binary double's
/// expansion).
fn coerce_float(label: &str, v: &Value) -> CliResult<Value> {
    let bad = || {
        CliError::Config(format!(
            "param '{label}': expected a finite number, got {v}"
        ))
    };
    match v {
        Value::Number(n) if n.as_f64().is_some_and(f64::is_finite) => Ok(v.clone()),
        Value::String(s) => match faucet_core::fidelity::number_text(s) {
            Ok(Value::Null) | Err(_) => Err(bad()),
            Ok(x) => Ok(x),
        },
        _ => Err(bad()),
    }
}

/// One scalar against its spec: the type, then the closed `values:` set.
fn coerce_scalar(label: &str, spec: &ParamSpec, v: &Value) -> CliResult<Value> {
    let coerced = match spec.kind {
        ParamType::Float => coerce_float(label, v)?,
        kind => coerce_kind(label, kind, v)?,
    };
    if !spec.values.is_empty()
        && !spec
            .values
            .iter()
            .any(|allowed| values_match(spec.kind, allowed, &coerced))
    {
        let allowed: Vec<String> = spec.values.iter().map(value_to_string).collect();
        let shown = if spec.secret {
            "the supplied value".to_string()
        } else {
            value_to_string(&coerced)
        };
        return Err(CliError::Config(format!(
            "param '{label}': {shown} is not one of the allowed values ({})",
            allowed.join(", ")
        )));
    }
    Ok(coerced)
}

fn coerce_object(label: &str, items: &ParamSpec, v: &Value) -> CliResult<Value> {
    let Value::Object(fields) = v else {
        return Err(CliError::Config(format!(
            "param '{label}': expected an object with fields {}, got {v}",
            items
                .properties
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        )));
    };
    for k in fields.keys() {
        if !items.properties.contains_key(k) {
            return Err(CliError::Config(format!(
                "param '{label}': unknown field '{k}' (fields: {})",
                items
                    .properties
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }
    let mut out = Map::new();
    for (field, spec) in &items.properties {
        let flabel = format!("{label}.{field}");
        match (fields.get(field).filter(|x| !x.is_null()), &spec.default) {
            (Some(x), _) => {
                out.insert(field.clone(), coerce_scalar(&flabel, spec, x)?);
            }
            (None, Some(d)) => {
                out.insert(field.clone(), coerce_scalar(&flabel, spec, d)?);
            }
            (None, None) if spec.required => {
                return Err(CliError::Config(format!(
                    "param '{label}': missing required field '{field}'"
                )));
            }
            (None, None) => {}
        }
    }
    Ok(Value::Object(out))
}

/// Coerce a list value: every element against `items`, then the size limits.
pub fn coerce(name: &str, p: &ParamSpec, raw: &Value) -> CliResult<Value> {
    let items = items_of(name, p)?;
    let elems = elements(name, items, raw)?;
    let min = p.min_items.unwrap_or(DEFAULT_MIN_ITEMS);
    if elems.len() < min {
        return Err(CliError::Config(format!(
            "param '{name}': needs at least {min} item(s), got {}",
            elems.len()
        )));
    }
    if let Some(max) = p.max_items
        && elems.len() > max
    {
        return Err(CliError::Config(format!(
            "param '{name}': holds {} items, more than `max_items: {max}`",
            elems.len()
        )));
    }
    let mut out = Vec::with_capacity(elems.len());
    for (i, e) in elems.iter().enumerate() {
        let label = format!("{name}[{i}]");
        if matches!(e, Value::Array(_)) {
            return Err(CliError::Config(format!(
                "param '{label}': an element cannot be a list — lists of lists are not supported"
            )));
        }
        out.push(match items.kind {
            ParamType::Object => coerce_object(&label, items, e)?,
            _ => coerce_scalar(&label, items, e)?,
        });
    }
    Ok(Value::Array(out))
}

/// The fields of an object item marked `secret: true`.
pub fn secret_fields(p: &ParamSpec) -> BTreeSet<String> {
    p.items
        .as_deref()
        .map(|i| {
            i.properties
                .iter()
                .filter(|(_, f)| f.secret)
                .map(|(n, _)| n.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Every scalar of a value as text — what the redaction registry learns.
pub fn leaves(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Array(a) => a.iter().for_each(|x| leaves(x, out)),
        Value::Object(o) => o.values().for_each(|x| leaves(x, out)),
        Value::Null => {}
        other => out.push(value_to_string(other)),
    }
}

/// `v` with each named field of every element replaced by `"***"`.
pub fn redact_fields(v: &Value, fields: &BTreeSet<String>) -> Value {
    match v {
        Value::Array(a) => Value::Array(a.iter().map(|x| redact_fields(x, fields)).collect()),
        Value::Object(o) => Value::Object(
            o.iter()
                .map(|(k, x)| {
                    if fields.contains(k) {
                        (k.clone(), Value::String("***".into()))
                    } else {
                        (k.clone(), x.clone())
                    }
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

/// A placeholder for item `i`: a declared value when there is a closed set,
/// else a distinct type-shaped value.
fn item_placeholder(spec: &ParamSpec, i: usize) -> Value {
    if !spec.values.is_empty() {
        let v = &spec.values[i % spec.values.len()];
        return super::spec::coerce("_", spec.kind, v).unwrap_or_else(|_| v.clone());
    }
    match spec.kind {
        ParamType::String => Value::String(format!("p{}", i + 1)),
        ParamType::Int => Value::from(i as i64 + 1),
        ParamType::Float => Value::from(i as f64 + 1.0),
        ParamType::Bool => Value::Bool(i % 2 == 1),
        ParamType::List | ParamType::Object => Value::Null,
    }
}

/// How many distinct placeholder items `items` can hold (at most `want`).
fn distinct_capacity(items: &ParamSpec, want: usize) -> usize {
    let one = |s: &ParamSpec| match s.kind {
        ParamType::Bool if s.values.is_empty() => 2,
        _ if !s.values.is_empty() => s.values.len(),
        _ => usize::MAX,
    };
    let cap = match items.kind {
        ParamType::Object => items.properties.values().map(one).max().unwrap_or(1),
        _ => one(items),
    };
    want.min(cap).max(1)
}

/// A placeholder list: two distinct items (one when `max_items: 1` or the
/// items cannot differ), at least `min_items` long, so a structural check sees
/// a stream repeated and strict coercion accepts it.
pub fn placeholder(p: &ParamSpec) -> Value {
    let Some(items) = p.items.as_deref() else {
        return Value::Array(Vec::new());
    };
    let max = p.max_items.unwrap_or(usize::MAX);
    let want = 2.min(max);
    let n = distinct_capacity(items, want)
        .max(p.min_items.unwrap_or(1))
        .min(max.max(1));
    Value::Array(
        (0..n)
            .map(|i| match items.kind {
                ParamType::Object => Value::Object(
                    items
                        .properties
                        .iter()
                        .map(|(k, f)| (k.clone(), item_placeholder(f, i)))
                        .collect(),
                ),
                _ => item_placeholder(items, i),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::ParamsSpec;
    use serde_json::json;

    fn list(yaml: &str) -> ParamSpec {
        crate::yaml::from_str(yaml).expect("param spec")
    }

    fn tables() -> ParamSpec {
        list(
            "type: list\nmax_items: 3\nitems:\n  type: object\n  properties:\n    schema: { type: string, default: public }\n    table: { type: string, required: true }\n    token: { type: string, secret: true }\n",
        )
    }

    #[test]
    fn the_placeholder_meets_min_items_and_coerces() {
        for (yaml, len) in [
            ("type: list\nmin_items: 3\nitems: { type: string }\n", 3),
            (
                "type: list\nmin_items: 3\nmax_items: 4\nitems: { type: int }\n",
                3,
            ),
            ("type: list\nitems: { type: string }\n", 2),
            ("type: list\nmax_items: 1\nitems: { type: string }\n", 1),
            ("type: list\nmin_items: 0\nitems: { type: string }\n", 2),
        ] {
            let p = list(yaml);
            let v = placeholder(&p);
            assert_eq!(v.as_array().unwrap().len(), len, "{yaml}");
            coerce("t", &p, &v).unwrap();
        }
    }

    #[test]
    fn scalar_lists_accept_arrays_json_text_and_comma_text() {
        let p = list("type: list\nitems: { type: string }\n");
        validate("t", &p).unwrap();
        assert_eq!(
            coerce("t", &p, &json!(["a", "b"])).unwrap(),
            json!(["a", "b"])
        );
        assert_eq!(
            coerce("t", &p, &json!(r#"["a","b"]"#)).unwrap(),
            json!(["a", "b"])
        );
        assert_eq!(
            coerce("t", &p, &json!(" a , b ")).unwrap(),
            json!(["a", "b"])
        );
        let ints = list("type: list\nitems: { type: int }\n");
        assert_eq!(coerce("n", &ints, &json!("[1, 2]")).unwrap(), json!([1, 2]));
        assert_eq!(coerce("n", &ints, &json!(["3"])).unwrap(), json!([3]));
        let err = coerce("n", &ints, &json!("1,2")).unwrap_err().to_string();
        assert!(
            err.contains("expected a list") && !err.contains("comma"),
            "{err}"
        );
        let err = coerce("n", &ints, &json!("[1,")).unwrap_err().to_string();
        assert!(err.contains("not a JSON array"), "{err}");
        let err = coerce("n", &ints, &json!("{\"a\":1}"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("expected a list"), "{err}");
        let err = coerce("n", &ints, &json!(5)).unwrap_err().to_string();
        assert!(err.contains("expected a list"), "{err}");
    }

    #[test]
    fn integers_above_2_53_stay_exact_and_floats_go_through_fidelity() {
        let ints = list("type: list\nitems: { type: int }\n");
        let big = "[9007199254740993, \"9007199254740995\"]";
        let v = coerce("n", &ints, &json!(big)).unwrap();
        assert_eq!(v[0].to_string(), "9007199254740993");
        assert_eq!(v[1].to_string(), "9007199254740995");
        let floats = list("type: list\nitems: { type: float }\n");
        let v = coerce("f", &floats, &json!(["0.1", 2.5, "1e3"])).unwrap();
        assert_eq!(value_to_string(&v[0]), "0.1");
        assert_eq!(value_to_string(&v[1]), "2.5");
        assert_eq!(value_to_string(&v[2]), "1000.0");
        for bad in [json!(["NaN"]), json!(["x"]), json!([true])] {
            let err = coerce("f", &floats, &bad).unwrap_err().to_string();
            assert!(
                err.contains("f[0]") && err.contains("finite number"),
                "{err}"
            );
        }
    }

    #[test]
    fn a_wrong_element_is_refused_by_index() {
        let ints = list("type: list\nitems: { type: int, values: [1, 2] }\n");
        validate("n", &ints).unwrap();
        let err = coerce("n", &ints, &json!([1, "abc"]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("param 'n[1]'") && err.contains("integer"),
            "{err}"
        );
        let err = coerce("n", &ints, &json!([1, 3])).unwrap_err().to_string();
        assert!(
            err.contains("n[1]") && err.contains("allowed values (1, 2)"),
            "{err}"
        );
        let err = coerce("n", &ints, &json!([[1]])).unwrap_err().to_string();
        assert!(err.contains("cannot be a list"), "{err}");
    }

    #[test]
    fn size_limits_and_the_empty_list() {
        let p = tables();
        let err = coerce("t", &p, &json!([])).unwrap_err().to_string();
        assert!(err.contains("at least 1 item"), "{err}");
        let four = json!([{"table": "a"}, {"table": "b"}, {"table": "c"}, {"table": "d"}]);
        let err = coerce("t", &p, &four).unwrap_err().to_string();
        assert!(err.contains("more than `max_items: 3`"), "{err}");
        let empty_ok = list("type: list\nmin_items: 0\nitems: { type: string }\n");
        assert_eq!(coerce("t", &empty_ok, &json!("")).unwrap(), json!([]));
    }

    #[test]
    fn object_items_fill_defaults_and_refuse_unknown_or_missing_fields() {
        let p = tables();
        validate("t", &p).unwrap();
        let v = coerce("t", &p, &json!([{"table": "orders"}])).unwrap();
        assert_eq!(v, json!([{"schema": "public", "table": "orders"}]));
        let err = coerce("t", &p, &json!([{"schema": "x"}]))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("t[0]") && err.contains("missing required field 'table'"),
            "{err}"
        );
        let err = coerce("t", &p, &json!([{"table": "a", "x": 1}]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("unknown field 'x'"), "{err}");
        let err = coerce("t", &p, &json!(["a"])).unwrap_err().to_string();
        assert!(err.contains("expected an object"), "{err}");
        assert_eq!(secret_fields(&p), BTreeSet::from(["token".to_string()]));
        let redacted = redact_fields(
            &json!([{"table": "a", "token": "s3cret"}]),
            &secret_fields(&p),
        );
        assert_eq!(redacted, json!([{"table": "a", "token": "***"}]));
        let mut out = Vec::new();
        leaves(&json!([{"a": 1, "b": null}, "x"]), &mut out);
        assert_eq!(out, vec!["1", "x"]);
    }

    #[test]
    fn declarations_are_checked_at_load() {
        for (yaml, want) in [
            ("type: list\n", "needs `items:`"),
            (
                "type: list\nitems: { type: list, items: { type: string } }\n",
                "list of lists",
            ),
            (
                "type: list\nitems: { type: object }\n",
                "needs `properties`",
            ),
            (
                "type: list\nitems: { type: object, properties: { a: { type: list } } }\n",
                "must be a scalar",
            ),
            (
                "type: list\nitems: { type: object, properties: { a-b: { type: string } } }\n",
                "invalid field name",
            ),
            (
                "type: list\nitems: { type: string, required: true }\n",
                "items.required",
            ),
            (
                "type: list\nitems: { type: string }\nvalues: [a]\n",
                "no `values:`",
            ),
            (
                "type: list\nitems: { type: string }\nmax_items: 0\n",
                "max_items: 0",
            ),
            (
                "type: list\nitems: { type: string }\nmin_items: 3\nmax_items: 2\n",
                "above `max_items`",
            ),
            (
                "type: list\nitems: { type: int }\ndefault: [x]\n",
                "default is not valid",
            ),
            (
                "type: list\nitems: { type: string }\ncomputed: \"${param.a}\"\n",
                "cannot be `computed`",
            ),
            (
                "type: list\nitems: { type: string }\nrequired: true\ndefault: [a]\n",
                "drop one",
            ),
            (
                "type: list\nitems: { type: object, properties: { a: { type: string } }, values: [x] }\n",
                "object items take no `values:`",
            ),
            (
                "type: list\nitems: { type: string, min_items: 1 }\n",
                "items.min_items",
            ),
        ] {
            let err = validate("t", &list(yaml)).unwrap_err().to_string();
            assert!(err.contains(want), "{yaml} → {err}");
        }
        validate(
            "t",
            &list("type: list\nitems: { type: string }\ndefault: \"${env:T}\"\n"),
        )
        .unwrap();
        // The block-level validation dispatches to it and refuses stray fields.
        let mut spec = ParamsSpec::new();
        spec.insert(
            "o".into(),
            list("type: object\nproperties: { a: { type: string } }\n"),
        );
        let err = crate::params::spec::validate(&spec)
            .unwrap_err()
            .to_string();
        assert!(err.contains("only valid as a list's `items`"), "{err}");
        let mut spec = ParamsSpec::new();
        spec.insert("s".into(), list("type: string\nmax_items: 2\n"));
        let err = crate::params::spec::validate(&spec)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("`max_items` applies to a `type: list`"),
            "{err}"
        );
    }

    #[test]
    fn placeholders_are_distinct_items() {
        assert_eq!(
            placeholder(&list("type: list\nitems: { type: string }\n")),
            json!(["p1", "p2"])
        );
        assert_eq!(
            placeholder(&list("type: list\nmax_items: 1\nitems: { type: int }\n")),
            json!([1])
        );
        assert_eq!(
            placeholder(&list(
                "type: list\nitems: { type: string, values: [only] }\n"
            )),
            json!(["only"])
        );
        assert_eq!(
            placeholder(&list("type: list\nitems: { type: bool }\n")),
            json!([false, true])
        );
        assert_eq!(
            placeholder(&list("type: list\nitems: { type: float }\n")),
            json!([1.0, 2.0])
        );
        assert_eq!(placeholder(&tables())[1]["table"], json!("p2"));
        assert_eq!(placeholder(&ParamSpec::default()), json!([]));
        assert!(tables().placeholder().is_array());
        assert!(list("type: list\nitems: { type: string, secret: true }\n").is_secret());
    }
}

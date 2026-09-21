//! Serde types + validation for the top-level `params:` block (#444).
//!
//! `params:` declares a config's **trigger-time override surface**: a typed,
//! named set of values a caller supplies when the pipeline is run (via
//! `faucet run --param`, `faucet template run --param`, or
//! `POST /v1/templates/{id}/runs`). Declared params are referenced in the
//! config as `${param.NAME}` and bound by [`crate::params::bind`].
//!
//! Everything here is pure data + pure validation — no I/O, no interpolation.

use crate::error::{CliError, CliResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// The scalar type a param carries. Params are deliberately scalar-only: a
/// param substitutes into a config *value* position, and structured overrides
/// are what named templates / matrix rows are for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ParamType {
    #[default]
    String,
    Int,
    Float,
    Bool,
}

impl ParamType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
        }
    }

    /// A type-shaped stand-in used when validating a config whose required
    /// params have no value yet (template registration, `faucet validate`).
    /// Never reaches a real connector — registration/validation only checks
    /// structure, it never builds a source or sink.
    pub fn placeholder(self) -> Value {
        match self {
            Self::String => Value::String("<param>".into()),
            Self::Int => Value::from(0i64),
            Self::Float => Value::from(0.0f64),
            Self::Bool => Value::Bool(false),
        }
    }
}

/// One declared parameter.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ParamSpec {
    /// Value type. Governs coercion of the supplied value and the type
    /// substituted when `${param.NAME}` is a config value's *entire* text.
    #[serde(rename = "type", default)]
    pub kind: ParamType,

    /// When true the caller MUST supply a value; there is no fallback.
    /// Mutually exclusive with `default` (a defaulted param is by definition
    /// optional).
    #[serde(default)]
    pub required: bool,

    /// Value used when the caller supplies none. Resolved like any other config
    /// scalar first, so `default: "${env:SINCE}"` works.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,

    /// Marks the value as sensitive: it is registered with the redaction
    /// registry the moment it is bound, so it never reaches logs, error
    /// strings, API responses, or the audit log in clear.
    #[serde(default)]
    pub secret: bool,

    /// Human-readable purpose, surfaced by `faucet template list/show`, the
    /// MCP `get_template` tool, and `GET /v1/templates/{id}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// A **derived** value: this param is not user-supplied but computed from
    /// other params via an interpolation expression — `${param.NAME}` and the
    /// `${map:NAME|case=value|*=default}` lookup — resolved *after* the ordinary
    /// params bind (#573). A computed param is excluded from the trigger surface
    /// (supplying a value for it is an error) and is mutually exclusive with
    /// `required`, `default`, and `secret`. Example:
    /// `accounts_domain: { computed: "${map:region|ca=zohocloud|*=zoho}" }`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub computed: Option<String>,

    /// Closed set of acceptable values (#648).
    ///
    /// Without this a typo'd param value — `sink: jsonlines` — binds happily
    /// and fails somewhere downstream, or worse produces a config that is
    /// merely *wrong*. With it, the bind refuses and names the alternatives.
    ///
    /// It is also what makes a template's parameter space enumerable: the
    /// `auto.enum_coverage` case generator in `faucet template test` derives
    /// one case per listed value, so "does this template still work for every
    /// sink it advertises?" is a red/green check rather than a manual sweep.
    ///
    /// Values are held to the declared `type` and compared after the same
    /// coercion a supplied value gets, so `values: [1, 2]` on an `int` param
    /// accepts the string `"1"` from a query string. Empty means unconstrained.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<Value>,
}

impl ParamSpec {
    /// A plain optional string param with the given default.
    #[cfg(test)]
    pub fn string_default(default: &str) -> Self {
        Self {
            kind: ParamType::String,
            required: false,
            default: Some(Value::String(default.into())),
            secret: false,
            description: None,
            computed: None,
            values: Vec::new(),
        }
    }
}

/// The whole `params:` block — declaration order is irrelevant, so a sorted map
/// keeps every rendering (schema, list output, audit) deterministic.
pub type ParamsSpec = BTreeMap<String, ParamSpec>;

/// A param name must be an identifier: `^[A-Za-z_][A-Za-z0-9_]*$`. Dots are
/// excluded because `${param.a.b}` would be ambiguous with a nested lookup, and
/// dashes because they read as arithmetic in some config editors.
fn validate_name(name: &str) -> CliResult<()> {
    let mut chars = name.chars();
    let ok = match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    };
    if !ok {
        return Err(CliError::Config(format!(
            "invalid param name '{name}' — names must match ^[A-Za-z_][A-Za-z0-9_]*$"
        )));
    }
    Ok(())
}

/// Whether `value` is an acceptable literal for `kind` (used for `default`,
/// which is authored in the config and therefore held to the declared type
/// rather than leniently coerced like a caller-supplied value).
fn default_matches(kind: ParamType, value: &Value) -> bool {
    match kind {
        // A string default may legitimately still hold an unresolved
        // interpolation token; anything scalar is accepted and stringified.
        ParamType::String => value.is_string() || value.is_number() || value.is_boolean(),
        ParamType::Int => value.as_i64().is_some(),
        ParamType::Float => value.as_f64().is_some(),
        ParamType::Bool => value.is_boolean(),
    }
}

/// Fail-fast validation of a whole `params:` block, run at every entry point
/// that touches params (config load, template registration, trigger).
pub fn validate(spec: &ParamsSpec) -> CliResult<()> {
    for (name, p) in spec {
        validate_name(name)?;
        if p.computed.is_some() {
            if p.required {
                return Err(CliError::Config(format!(
                    "param '{name}' is `computed` and cannot be `required` — a computed param is \
                     derived, never supplied"
                )));
            }
            if p.default.is_some() {
                return Err(CliError::Config(format!(
                    "param '{name}' is `computed` and cannot have a `default` — its value is the \
                     computed expression"
                )));
            }
            if p.secret {
                return Err(CliError::Config(format!(
                    "param '{name}' is `computed` and cannot be `secret` — a derived value is not \
                     a secret source; reference the secret directly where it is used"
                )));
            }
        }
        if p.required && p.default.is_some() {
            return Err(CliError::Config(format!(
                "param '{name}' is both `required: true` and has a `default` — a param with a \
                 default is optional; drop one"
            )));
        }
        if let Some(d) = &p.default {
            if d.is_null() {
                return Err(CliError::Config(format!(
                    "param '{name}': `default: null` is not a value — omit `default` instead"
                )));
            }
            if !default_matches(p.kind, d) {
                return Err(CliError::Config(format!(
                    "param '{name}': default {d} is not a valid {} value",
                    p.kind.as_str()
                )));
            }
        }
        if !p.values.is_empty() {
            if p.computed.is_some() {
                return Err(CliError::Config(format!(
                    "param '{name}' is `computed` and cannot also declare `values` — its value \
                     comes from the expression, not from a caller"
                )));
            }
            // Every listed value must be a legal literal for the declared
            // type, and the default (when there is one) must be in the set —
            // otherwise the all-defaults case would fail its own constraint.
            for v in &p.values {
                if !default_matches(p.kind, v) {
                    return Err(CliError::Config(format!(
                        "param '{name}': value {v} in `values` is not a valid {} value",
                        p.kind.as_str()
                    )));
                }
            }
            if let Some(d) = &p.default
                && !p.values.iter().any(|v| values_match(p.kind, v, d))
            {
                return Err(CliError::Config(format!(
                    "param '{name}': default {d} is not one of the declared `values`"
                )));
            }
        }
    }
    Ok(())
}

/// Whether two literals name the same value for `kind`, after the coercion a
/// caller-supplied value would get.
///
/// Compared post-coercion so `values: [1, 2]` on an `int` param accepts the
/// string `"1"` a query string or `--param` delivers — otherwise the
/// constraint would reject exactly the inputs it exists to guard.
pub(crate) fn values_match(kind: ParamType, a: &Value, b: &Value) -> bool {
    match (coerce("_", kind, a), coerce("_", kind, b)) {
        (Ok(x), Ok(y)) => x == y,
        _ => a == b,
    }
}

/// Coerce a caller-supplied value to the declared type.
///
/// Deliberately lenient about *representation* (a CLI `--param n=5` and an HTTP
/// `{"n": 5}` must behave identically) and strict about *type* (`int` never
/// silently accepts `1.5`). Never accepts `null`: a param either has a value or
/// falls back to its default.
pub fn coerce(name: &str, kind: ParamType, value: &Value) -> CliResult<Value> {
    let bad = |expected: &str| {
        CliError::Config(format!(
            "param '{name}': expected {expected}, got {}",
            match value {
                Value::Null => "null".to_string(),
                other => other.to_string(),
            }
        ))
    };
    match kind {
        ParamType::String => match value {
            Value::String(s) => Ok(Value::String(s.clone())),
            Value::Number(n) => Ok(Value::String(n.to_string())),
            Value::Bool(b) => Ok(Value::String(b.to_string())),
            _ => Err(bad("a string")),
        },
        ParamType::Int => match value {
            Value::Number(n) => n.as_i64().map(Value::from).ok_or_else(|| bad("an integer")),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::from)
                .map_err(|_| bad("an integer")),
            _ => Err(bad("an integer")),
        },
        ParamType::Float => match value {
            Value::Number(n) => n.as_f64().map(Value::from).ok_or_else(|| bad("a number")),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::from)
                .map_err(|_| bad("a number")),
            _ => Err(bad("a number")),
        },
        ParamType::Bool => match value {
            Value::Bool(b) => Ok(Value::Bool(*b)),
            Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
                "true" | "yes" | "1" => Ok(Value::Bool(true)),
                "false" | "no" | "0" => Ok(Value::Bool(false)),
                _ => Err(bad("a boolean (true/false)")),
            },
            _ => Err(bad("a boolean (true/false)")),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn p(kind: ParamType, required: bool, default: Option<Value>) -> ParamSpec {
        ParamSpec {
            kind,
            required,
            default,
            secret: false,
            description: None,
            computed: None,
            values: Vec::new(),
        }
    }

    #[test]
    fn computed_param_cannot_be_required_default_or_secret() {
        for yaml in [
            "a: { computed: \"${param.x}\", required: true }\n",
            "a: { computed: \"${param.x}\", default: y }\n",
            "a: { computed: \"${param.x}\", secret: true }\n",
        ] {
            let spec: ParamsSpec = serde_yaml::from_str(yaml).unwrap();
            assert!(
                matches!(validate(&spec), Err(CliError::Config(m)) if m.contains("computed")),
                "expected a computed-conflict error for: {yaml}"
            );
        }
        // A plain computed param validates.
        let spec: ParamsSpec = serde_yaml::from_str("a: { computed: \"${param.x}\" }\n").unwrap();
        assert!(validate(&spec).is_ok());
    }

    #[test]
    fn parses_block_with_defaults() {
        let spec: ParamsSpec = serde_yaml::from_str(
            "tenant_id: { type: string, required: true, description: Tenant }\n\
             since: { default: \"1970-01-01\" }\n\
             page_size: { type: int, default: 500 }\n\
             api_token: { required: true, secret: true }\n",
        )
        .unwrap();
        validate(&spec).unwrap();
        assert_eq!(spec["tenant_id"].kind, ParamType::String);
        assert!(spec["tenant_id"].required);
        assert_eq!(spec["tenant_id"].description.as_deref(), Some("Tenant"));
        // `type` defaults to string.
        assert_eq!(spec["since"].kind, ParamType::String);
        assert_eq!(spec["page_size"].default, Some(json!(500)));
        assert!(spec["api_token"].secret);
    }

    #[test]
    fn rejects_unknown_field() {
        let err = serde_yaml::from_str::<ParamsSpec>("a: { typo: 1 }").unwrap_err();
        assert!(err.to_string().contains("typo"), "{err}");
    }

    #[test]
    fn rejects_bad_names() {
        for bad in ["", "1abc", "a.b", "a-b", "a b"] {
            let spec: ParamsSpec = [(bad.to_string(), p(ParamType::String, false, None))].into();
            assert!(validate(&spec).is_err(), "name {bad:?} should be rejected");
        }
        for good in ["a", "_a", "tenant_id", "A1"] {
            let spec: ParamsSpec = [(good.to_string(), p(ParamType::String, false, None))].into();
            validate(&spec).unwrap();
        }
    }

    #[test]
    fn rejects_required_with_default() {
        let spec: ParamsSpec = [(
            "a".to_string(),
            p(ParamType::String, true, Some(json!("x"))),
        )]
        .into();
        let err = validate(&spec).unwrap_err().to_string();
        assert!(err.contains("required"), "{err}");
    }

    #[test]
    fn rejects_null_and_mistyped_defaults() {
        let spec: ParamsSpec = [(
            "a".to_string(),
            p(ParamType::String, false, Some(Value::Null)),
        )]
        .into();
        assert!(validate(&spec).unwrap_err().to_string().contains("null"));

        let spec: ParamsSpec = [(
            "n".to_string(),
            p(ParamType::Int, false, Some(json!("not-a-number"))),
        )]
        .into();
        assert!(validate(&spec).unwrap_err().to_string().contains("int"));

        let spec: ParamsSpec =
            [("f".to_string(), p(ParamType::Bool, false, Some(json!(1))))].into();
        assert!(validate(&spec).is_err());

        // An int default is a valid float.
        let spec: ParamsSpec =
            [("f".to_string(), p(ParamType::Float, false, Some(json!(1))))].into();
        validate(&spec).unwrap();
    }

    #[test]
    fn coerce_accepts_both_wire_shapes() {
        // HTTP JSON shape.
        assert_eq!(coerce("n", ParamType::Int, &json!(5)).unwrap(), json!(5));
        assert_eq!(
            coerce("f", ParamType::Float, &json!(1.5)).unwrap(),
            json!(1.5)
        );
        assert_eq!(
            coerce("b", ParamType::Bool, &json!(true)).unwrap(),
            json!(true)
        );
        // CLI `--param k=v` shape (always a string).
        assert_eq!(coerce("n", ParamType::Int, &json!("5")).unwrap(), json!(5));
        assert_eq!(
            coerce("f", ParamType::Float, &json!(" 1.5 ")).unwrap(),
            json!(1.5)
        );
        for truthy in ["true", "TRUE", "yes", "1"] {
            assert_eq!(
                coerce("b", ParamType::Bool, &json!(truthy)).unwrap(),
                json!(true)
            );
        }
        for falsy in ["false", "No", "0"] {
            assert_eq!(
                coerce("b", ParamType::Bool, &json!(falsy)).unwrap(),
                json!(false)
            );
        }
        // A scalar into a string param stringifies.
        assert_eq!(
            coerce("s", ParamType::String, &json!(7)).unwrap(),
            json!("7")
        );
    }

    #[test]
    fn coerce_rejects_type_errors_and_null() {
        for (kind, v) in [
            (ParamType::Int, json!(1.5)),
            (ParamType::Int, json!("x")),
            (ParamType::Int, json!(null)),
            (ParamType::Float, json!("x")),
            (ParamType::Bool, json!("maybe")),
            (ParamType::Bool, json!(1)),
            (ParamType::String, json!(null)),
            (ParamType::String, json!({"a": 1})),
            (ParamType::Int, json!([1])),
        ] {
            let err = coerce("p", kind, &v).unwrap_err().to_string();
            assert!(err.contains("param 'p'"), "{kind:?} {v}: {err}");
        }
    }

    #[test]
    fn placeholders_are_type_shaped() {
        assert!(ParamType::String.placeholder().is_string());
        assert!(ParamType::Int.placeholder().is_i64());
        assert!(ParamType::Float.placeholder().is_f64());
        assert!(ParamType::Bool.placeholder().is_boolean());
        assert_eq!(ParamType::Float.as_str(), "float");
    }

    #[test]
    fn schema_generates() {
        let schema = schemars::schema_for!(ParamSpec);
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["properties"]["type"].is_object());
        assert!(v["properties"]["secret"].is_object());
    }

    #[test]
    fn string_default_helper_builds_optional_param() {
        let s = ParamSpec::string_default("v");
        assert!(!s.required);
        assert_eq!(s.default, Some(json!("v")));
    }

    // ── closed value sets (#648) ──────────────────────────────────────────

    fn spec_of(name: &str, value: Value) -> ParamsSpec {
        let p: ParamSpec = serde_json::from_value(value).expect("param spec");
        let mut m = ParamsSpec::new();
        m.insert(name.into(), p);
        m
    }

    #[test]
    fn a_closed_value_set_accepts_a_matching_default() {
        let spec = spec_of(
            "region",
            json!({"type": "string", "default": "us", "values": ["us", "eu"]}),
        );
        validate(&spec).expect("a default inside the set is fine");
    }

    #[test]
    fn a_default_outside_the_closed_set_is_rejected() {
        // Otherwise the all-defaults case would fail its own constraint — the
        // one combination every caller hits.
        let spec = spec_of(
            "region",
            json!({"type": "string", "default": "mars", "values": ["us", "eu"]}),
        );
        let err = validate(&spec).expect_err("default outside the set");
        assert!(
            err.to_string().contains("not one of the declared `values`"),
            "{err}"
        );
    }

    #[test]
    fn a_listed_value_of_the_wrong_type_is_rejected() {
        let spec = spec_of("n", json!({"type": "int", "values": [1, "not-an-int"]}));
        let err = validate(&spec).expect_err("bad literal");
        assert!(
            err.to_string().contains("is not a valid int value"),
            "{err}"
        );
    }

    #[test]
    fn values_and_computed_are_mutually_exclusive() {
        // A computed param's value comes from the expression, so a caller-facing
        // constraint on it would never be checked against anything.
        let spec = spec_of(
            "derived",
            json!({"type": "string", "computed": "${param.other}", "values": ["a"]}),
        );
        let err = validate(&spec).expect_err("computed + values");
        assert!(
            err.to_string().contains("cannot also declare `values`"),
            "{err}"
        );
    }

    #[test]
    fn membership_is_decided_after_coercion() {
        // `values: [1, 2]` on an `int` param must accept the string "1" that a
        // query string or `--param` delivers, or the constraint would reject
        // exactly the inputs it exists to guard.
        assert!(values_match(ParamType::Int, &json!(1), &json!("1")));
        assert!(values_match(ParamType::Float, &json!(1.5), &json!("1.5")));
        assert!(values_match(ParamType::Bool, &json!(true), &json!("true")));
        assert!(!values_match(ParamType::Int, &json!(1), &json!(2)));
        // Uncoercible on both sides falls back to structural equality.
        assert!(values_match(ParamType::Int, &json!("x"), &json!("x")));
    }

    #[test]
    fn an_int_default_that_is_not_an_int_is_rejected() {
        let spec = spec_of("n", json!({"type": "int", "default": "abc"}));
        let err = validate(&spec).expect_err("bad default");
        assert!(
            err.to_string().contains("is not a valid int value"),
            "{err}"
        );
    }
}

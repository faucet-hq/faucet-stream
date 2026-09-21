//! Binding `${param.NAME}` references to supplied values (#444).
//!
//! Binding is a **pre-parse pass over the untyped config document**, run after
//! `${env:}` / `${file:}` / `${secret:}` interpolation and before the typed
//! `PipelineConfig` deserialise. Two consequences worth stating, because both
//! are load-bearing:
//!
//! 1. **Structure safety.** Substitution happens per JSON/YAML scalar, exactly
//!    like [`crate::interpolate::interpolate_value`] — a supplied value holding
//!    `:`, a newline, or `-` stays the single scalar it replaced and can never
//!    inject a key or an array element. Downstream, SQL-bound and JSON-safe
//!    substitution paths (`substitute_context_bind_params` /
//!    `substitute_context_json` in `faucet_core::util`) are untouched, so the
//!    existing SQL/JSON-injection guarantees still hold for param-derived text.
//! 2. **No re-interpolation of caller input.** Env/file/secret directives are
//!    resolved *before* binding, so a supplied value is never itself scanned for
//!    directives. Belt and braces, a supplied value containing `${` is rejected
//!    outright: params are data, not directives.
//!
//! When `${param.NAME}` is a scalar's *entire* text the declared type is
//! preserved (an `int` param lands as a JSON number, not `"5"`); embedded in a
//! longer string it is stringified, like every other interpolation namespace.

use super::spec::{self, ParamsSpec};
use crate::error::{CliError, CliResult};
use crate::interpolate::{
    Directive, classify_directive, iter_directives, rewrite, value_to_string,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The config key holding the declaration block.
pub const PARAMS_KEY: &str = "params";

/// The interpolation namespace params live in (`${param.NAME}`).
pub const PARAM_ID: &str = "param";

/// What to do with a `required` param the caller did not supply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindMode {
    /// A missing required param is an error. Used on every path that actually
    /// runs a pipeline.
    Strict,
    /// A missing required param is filled with a type-shaped placeholder. Used
    /// to structurally validate a config whose values arrive later — template
    /// registration and `faucet validate` on a parameterized config.
    Placeholder,
}

/// Caller-supplied values, keyed by param name. Values arrive either as real
/// JSON (HTTP) or as strings (`--param k=v`); [`spec::coerce`] normalizes both.
pub type SuppliedParams = BTreeMap<String, Value>;

/// The result of a bind: every declared param's effective value, plus which of
/// them are sensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundParams {
    pub values: BTreeMap<String, Value>,
    pub secret_names: BTreeSet<String>,
}

impl BoundParams {
    /// The bound values with every `secret: true` entry replaced by `"***"` —
    /// the only form safe to echo into an API response, the audit log, or a run
    /// record.
    pub fn redacted(&self) -> BTreeMap<String, Value> {
        self.values
            .iter()
            .map(|(k, v)| {
                if self.secret_names.contains(k) {
                    (k.clone(), Value::String("***".into()))
                } else {
                    (k.clone(), v.clone())
                }
            })
            .collect()
    }

    /// Whether any bound param is marked secret (drives the cluster-persistence
    /// guard in the template-trigger path).
    pub fn has_secrets(&self) -> bool {
        !self.secret_names.is_empty()
    }
}

/// Read the `params:` block out of an untyped config document, validating it.
/// A document with no block yields an empty spec.
pub fn declared(doc: &Value) -> CliResult<ParamsSpec> {
    let Some(raw) = doc.get(PARAMS_KEY) else {
        return Ok(ParamsSpec::new());
    };
    if raw.is_null() {
        return Ok(ParamsSpec::new());
    }
    let parsed: ParamsSpec = serde_json::from_value(raw.clone())
        .map_err(|e| CliError::Config(format!("invalid `params:` block: {e}")))?;
    spec::validate(&parsed)?;
    Ok(parsed)
}

/// Resolve every declared param to a value, without touching the document.
///
/// Precedence: supplied → `default` → (`Placeholder` mode) type placeholder →
/// error. Supplied names that are not declared are rejected so a typo can never
/// be silently ignored.
pub fn resolve(
    spec: &ParamsSpec,
    supplied: &SuppliedParams,
    mode: BindMode,
) -> CliResult<BoundParams> {
    for name in supplied.keys() {
        if !spec.contains_key(name) {
            return Err(CliError::UnknownParam {
                name: name.clone(),
                known: spec.keys().cloned().collect(),
            });
        }
    }

    // A computed param is derived, never supplied — reject a value for one.
    for (name, p) in spec {
        if p.computed.is_some() && supplied.contains_key(name) {
            return Err(CliError::Config(format!(
                "param '{name}' is `computed` and cannot be supplied a value — it is derived from \
                 other params"
            )));
        }
    }

    let mut bound = BoundParams::default();
    for (name, p) in spec {
        // Computed params are resolved in a second pass (they may reference
        // other params, including each other), after the supplied/default ones.
        if p.computed.is_some() {
            continue;
        }
        let value = match supplied.get(name) {
            Some(raw) => {
                reject_directives(name, raw)?;
                let coerced = spec::coerce(name, p.kind, raw)?;
                // A closed `values:` set is checked here, at bind, so a typo'd
                // value fails naming the alternatives rather than producing a
                // config that is merely wrong (#648).
                if !p.values.is_empty()
                    && !p
                        .values
                        .iter()
                        .any(|v| spec::values_match(p.kind, v, &coerced))
                {
                    let allowed: Vec<String> = p.values.iter().map(value_to_string).collect();
                    return Err(CliError::Config(format!(
                        "param '{name}': {} is not one of the allowed values ({})",
                        value_to_string(&coerced),
                        allowed.join(", ")
                    )));
                }
                coerced
            }
            None => match &p.default {
                // A default is authored in the config and already went through
                // env/file/secret resolution, so it is coerced but not
                // directive-checked.
                Some(d) => spec::coerce(name, p.kind, d)?,
                None => match mode {
                    BindMode::Placeholder => p.kind.placeholder(),
                    BindMode::Strict => {
                        return Err(CliError::MissingParam {
                            name: name.clone(),
                            description: p.description.clone(),
                        });
                    }
                },
            },
        };
        if p.secret {
            // Register before the value can reach any log line, error string, or
            // API body. `register` no-ops below the registry's minimum length.
            crate::secrets::registry::register(&value_to_string(&value));
            bound.secret_names.insert(name.clone());
        }
        bound.values.insert(name.clone(), value);
    }

    resolve_computed(spec, &mut bound)?;
    Ok(bound)
}

/// Resolve every `computed` param, in dependency order, into `bound.values`.
///
/// A computed param's expression may reference regular params and other
/// computed params via `${param.NAME}` and the `${map:NAME|case=value|*=default}`
/// lookup. We loop, resolving any computed param all of whose referenced params
/// are already bound, until none remain. If a round makes no progress, the
/// leftover set either references an undeclared param (→ `UnknownParamRef`) or
/// forms a cycle (→ `InterpolationCycle`).
fn resolve_computed(spec: &ParamsSpec, bound: &mut BoundParams) -> CliResult<()> {
    let computed_names: BTreeSet<String> = spec
        .iter()
        .filter(|(_, p)| p.computed.is_some())
        .map(|(n, _)| n.clone())
        .collect();
    let mut remaining: Vec<(String, String)> = spec
        .iter()
        .filter_map(|(n, p)| p.computed.as_ref().map(|c| (n.clone(), c.clone())))
        .collect();

    while !remaining.is_empty() {
        let mut progressed = false;
        let mut still = Vec::new();
        for (name, expr) in remaining {
            let refs = referenced_params(&expr);
            if refs.iter().all(|r| bound.values.contains_key(r)) {
                let value = eval_computed_expr(&name, &expr, &bound.values)?;
                bound.values.insert(name, Value::String(value));
                progressed = true;
            } else {
                still.push((name, expr));
            }
        }
        if !progressed {
            // No computed param could resolve. Distinguish an undeclared
            // reference from a cycle among the remaining computed params.
            for (name, expr) in &still {
                for r in referenced_params(expr) {
                    if !bound.values.contains_key(&r) && !computed_names.contains(&r) {
                        return Err(CliError::UnknownParamRef {
                            name: r,
                            token: format!("computed param '{name}'"),
                        });
                    }
                }
            }
            let mut chain: Vec<String> = still.into_iter().map(|(n, _)| n).collect();
            chain.sort();
            return Err(CliError::InterpolationCycle { chain });
        }
        remaining = still;
    }
    Ok(())
}

/// Param names referenced by a computed expression — the `NAME` in every
/// `${param.NAME}` and the switch `NAME` in every `${map:NAME|…}`.
fn referenced_params(expr: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = expr;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else { break };
        let body = &after[..end];
        if let Some(name) = body.strip_prefix("param.") {
            out.push(name.trim().to_string());
        } else if let Some(spec) = body.strip_prefix("map:")
            && let Some(input) = spec.split('|').next()
        {
            out.push(input.trim().to_string());
        }
        rest = &after[end + 1..];
    }
    out
}

/// Evaluate a computed expression: substitute `${param.NAME}` (stringified bound
/// value) and `${map:NAME|case=value|*=default}` (lookup on the bound value of
/// `NAME`). Any other `${…}` directive is rejected — a computed value is derived
/// from other params only, never from the environment/secrets.
fn eval_computed_expr(
    name: &str,
    expr: &str,
    bound: &BTreeMap<String, Value>,
) -> CliResult<String> {
    let mut out = String::new();
    let mut rest = expr;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after.find('}').ok_or_else(|| {
            CliError::Config(format!(
                "computed param '{name}': unterminated `${{` in expression '{expr}'"
            ))
        })?;
        let body = &after[..end];
        out.push_str(&resolve_computed_token(name, body, bound)?);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Resolve one `${…}` body inside a computed expression to its string value.
fn resolve_computed_token(
    owner: &str,
    body: &str,
    bound: &BTreeMap<String, Value>,
) -> CliResult<String> {
    if let Some(pname) = body.strip_prefix("param.") {
        let pname = pname.trim();
        let v = bound.get(pname).ok_or_else(|| CliError::UnknownParamRef {
            name: pname.to_string(),
            token: format!("computed param '{owner}'"),
        })?;
        return Ok(value_to_string(v));
    }
    if let Some(spec) = body.strip_prefix("map:") {
        return resolve_map(owner, spec, bound);
    }
    Err(CliError::Config(format!(
        "computed param '{owner}': `${{{body}}}` is not allowed — a computed expression may only \
         reference `${{param.NAME}}` or `${{map:NAME|case=value|*=default}}`"
    )))
}

/// Resolve a `map:NAME|case=value|…|*=default` body: look up the bound value of
/// `NAME`, return the value of the matching case, or the `*` default. No match
/// and no `*` is a typed load-time error.
fn resolve_map(owner: &str, spec: &str, bound: &BTreeMap<String, Value>) -> CliResult<String> {
    let mut parts = spec.split('|');
    let input_name = parts
        .next()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            CliError::Config(format!(
                "computed param '{owner}': `${{map:…}}` is missing the switch param name — write \
                 `${{map:NAME|case=value|*=default}}`"
            ))
        })?;
    let input = bound
        .get(input_name)
        .ok_or_else(|| CliError::UnknownParamRef {
            name: input_name.to_string(),
            token: format!("computed param '{owner}'"),
        })?;
    let input_str = value_to_string(input);

    let mut default: Option<String> = None;
    let mut matched: Option<String> = None;
    for pair in parts {
        let (case, value) = pair.split_once('=').ok_or_else(|| {
            CliError::Config(format!(
                "computed param '{owner}': map case '{pair}' is not `case=value`"
            ))
        })?;
        let case = case.trim();
        if case == "*" {
            default = Some(value.to_string());
        } else if case == input_str {
            matched = Some(value.to_string());
        }
    }
    matched.or(default).ok_or_else(|| {
        CliError::Config(format!(
            "computed param '{owner}': map has no case for '{input_name}' = '{input_str}' and no \
             `*` default"
        ))
    })
}

/// Bind params in an untyped config document, in place.
///
/// Reads and validates the document's own `params:` block, resolves each param
/// against `supplied`, then substitutes `${param.NAME}` everywhere **except**
/// inside the `params:` block itself (a default is a literal, not a target).
/// Returns the bound values so the caller can echo/audit them (redacted).
pub fn bind_document(
    doc: &mut Value,
    supplied: &SuppliedParams,
    mode: BindMode,
) -> CliResult<BoundParams> {
    let spec = declared(doc)?;
    let bound = resolve(&spec, supplied, mode)?;

    // Lift the declaration block out so defaults are never rewritten, then put
    // it back byte-identical — the block is part of the config and is persisted
    // with a registered template.
    let stashed = doc.get_mut(PARAMS_KEY).map(std::mem::take);
    let result = substitute(doc, &bound.values);
    if let (Some(block), Some(map)) = (stashed, doc.as_object_mut()) {
        map.insert(PARAMS_KEY.to_string(), block);
    }
    result?;
    Ok(bound)
}

/// Reject an interpolation directive inside a caller-supplied value. Supplied
/// params are data: allowing `${vault:…}` / `${env:…}` through would let a
/// caller read the *server's* secrets and environment by way of a param.
fn reject_directives(name: &str, raw: &Value) -> CliResult<()> {
    if let Value::String(s) = raw
        && s.contains("${")
    {
        return Err(CliError::Config(format!(
            "param '{name}': value contains an interpolation directive (`${{`). Param values are \
             literal data — put the directive in the config's `params:` default or in the config \
             body instead"
        )));
    }
    Ok(())
}

/// Substitute `${param.NAME}` throughout `v`.
fn substitute(v: &mut Value, bound: &BTreeMap<String, Value>) -> CliResult<()> {
    if let Value::String(s) = v {
        let replaced = match whole_token(s, bound)? {
            Some(typed) => typed,
            None => Value::String(rewrite_text(s, bound)?),
        };
        *v = replaced;
        return Ok(());
    }
    match v {
        Value::Array(items) => {
            for item in items.iter_mut() {
                substitute(item, bound)?;
            }
        }
        Value::Object(map) => {
            // Keys may carry tokens too (a param-named header, say). Rebuild the
            // map so a rewritten key is honoured — mirrors `interpolate_value`.
            let entries: Vec<(String, Value)> = std::mem::take(map).into_iter().collect();
            for (key, mut val) in entries {
                substitute(&mut val, bound)?;
                map.insert(rewrite_text(&key, bound)?, val);
            }
        }
        _ => {}
    }
    Ok(())
}

/// If `s` is *exactly* one `${param.NAME}` token, return that param's value with
/// its declared type intact. Anything else (extra text, several tokens, an
/// escaped `$${param.x}`) returns `None` for textual rewriting.
fn whole_token(s: &str, bound: &BTreeMap<String, Value>) -> CliResult<Option<Value>> {
    let mut tokens = iter_directives(s);
    let Some((token, dir)) = tokens.next() else {
        return Ok(None);
    };
    if tokens.next().is_some() || token != s {
        return Ok(None);
    }
    match dir {
        Directive::Deferred { id, path } if id == PARAM_ID => {
            Ok(Some(lookup(path, token, bound)?.clone()))
        }
        Directive::LoadTime {
            prefix: "map",
            body,
        } => Ok(Some(Value::String(resolve_map(token, body, bound)?))),
        _ => Ok(None),
    }
}

/// Textual rewrite: every `${param.NAME}` becomes the stringified value; every
/// other directive survives verbatim for its own resolution stage.
fn rewrite_text(s: &str, bound: &BTreeMap<String, Value>) -> CliResult<String> {
    rewrite(s, |body| match classify_directive(body) {
        Directive::Deferred { id, path } if id == PARAM_ID => {
            let token = format!("${{{body}}}");
            Ok(Some(value_to_string(lookup(path, &token, bound)?)))
        }
        Directive::LoadTime {
            prefix: "map",
            body: map_body,
        } => {
            let token = format!("${{{body}}}");
            Ok(Some(resolve_map(&token, map_body, bound)?))
        }
        _ => Ok(None),
    })
}

/// Resolve the `NAME` in `${param.NAME}`. The path must be a bare name — nested
/// lookups (`${param.a.b}`) are not a thing, since params are scalars.
fn lookup<'a>(path: &str, token: &str, bound: &'a BTreeMap<String, Value>) -> CliResult<&'a Value> {
    if path.is_empty() {
        return Err(CliError::Config(format!(
            "interpolation '{token}' is missing a param name — write `${{param.NAME}}`"
        )));
    }
    if path.contains('.') {
        return Err(CliError::Config(format!(
            "interpolation '{token}' is not a valid param reference — params are scalars, so \
             `${{param.NAME}}` takes a bare name"
        )));
    }
    bound.get(path).ok_or_else(|| CliError::UnknownParamRef {
        name: path.to_string(),
        token: token.to_string(),
    })
}

/// Parse a `--param key=value` CLI argument. The value is kept as a JSON string;
/// [`spec::coerce`] converts it to the declared type at bind time.
pub fn parse_cli_param(arg: &str) -> CliResult<(String, Value)> {
    let (key, value) = arg.split_once('=').ok_or_else(|| {
        CliError::Config(format!("invalid --param '{arg}' — expected `name=value`"))
    })?;
    let key = key.trim();
    if key.is_empty() {
        return Err(CliError::Config(format!(
            "invalid --param '{arg}' — the name is empty"
        )));
    }
    Ok((key.to_string(), Value::String(value.to_string())))
}

/// Collect a `--param name=value` list into a [`SuppliedParams`] map, rejecting
/// a repeated name (silently keeping the last would be a footgun).
pub fn collect_cli_params(args: &[String]) -> CliResult<SuppliedParams> {
    let mut out = SuppliedParams::new();
    for arg in args {
        let (k, v) = parse_cli_param(arg)?;
        if out.insert(k.clone(), v).is_some() {
            return Err(CliError::Config(format!(
                "--param '{k}' was given more than once"
            )));
        }
    }
    Ok(out)
}

/// Collect a `--param-env NAME[=VALUE]` list into an env overlay. A bare `NAME`
/// takes the value from the caller's own environment (so a secret never appears
/// in the process arguments); `NAME=VALUE` sets it explicitly.
pub fn collect_env_overrides(args: &[String]) -> CliResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for arg in args {
        let (name, value) = match arg.split_once('=') {
            Some((n, v)) => (n.trim().to_string(), v.to_string()),
            None => {
                let n = arg.trim().to_string();
                let v = std::env::var(&n).map_err(|_| {
                    CliError::Config(format!(
                        "--param-env '{n}' has no value and '{n}' is not set in the environment"
                    ))
                })?;
                (n, v)
            }
        };
        if name.is_empty() {
            return Err(CliError::Config(format!(
                "invalid --param-env '{arg}' — the variable name is empty"
            )));
        }
        if out.insert(name.clone(), value).is_some() {
            return Err(CliError::Config(format!(
                "--param-env '{name}' was given more than once"
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec_of(yaml: &str) -> ParamsSpec {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn supplied(pairs: &[(&str, Value)]) -> SuppliedParams {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn resolves_supplied_default_and_placeholder() {
        let spec = spec_of(
            "tenant: { required: true }\n\
             since: { default: \"1970-01-01\" }\n\
             page: { type: int, required: true }\n",
        );
        let bound = resolve(
            &spec,
            &supplied(&[("tenant", json!("acme")), ("page", json!("50"))]),
            BindMode::Strict,
        )
        .unwrap();
        assert_eq!(bound.values["tenant"], json!("acme"));
        assert_eq!(bound.values["since"], json!("1970-01-01"));
        // Coerced to the declared int type even though it arrived as a string.
        assert_eq!(bound.values["page"], json!(50));

        // Placeholder mode fills the required ones.
        let bound = resolve(&spec, &SuppliedParams::new(), BindMode::Placeholder).unwrap();
        assert_eq!(bound.values["tenant"], json!("<param>"));
        assert_eq!(bound.values["page"], json!(0));
        assert_eq!(bound.values["since"], json!("1970-01-01"));
    }

    #[test]
    fn computed_param_map_default_and_match() {
        let spec = spec_of(
            "region: { default: com }\n\
             accounts_domain: { computed: \"${map:region|ca=zohocloud|*=zoho}\" }\n",
        );
        // Default region → `*` default value.
        let bound = resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(bound.values["accounts_domain"], json!("zoho"));
        assert_eq!(bound.values["region"], json!("com"));
        // region=ca → matching case.
        let bound = resolve(
            &spec,
            &supplied(&[("region", json!("ca"))]),
            BindMode::Strict,
        )
        .unwrap();
        assert_eq!(bound.values["accounts_domain"], json!("zohocloud"));
    }

    #[test]
    fn computed_param_rejected_when_supplied() {
        let spec = spec_of(
            "region: { default: com }\n\
             accounts_domain: { computed: \"${map:region|*=zoho}\" }\n",
        );
        let err = resolve(
            &spec,
            &supplied(&[("accounts_domain", json!("hacked"))]),
            BindMode::Strict,
        )
        .unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("computed") && m.contains("cannot be supplied")),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_param_chain_resolves_in_dependency_order() {
        // b depends on a (also computed); resolution order is derived, not lexical.
        let spec = spec_of(
            "env: { default: prod }\n\
             a: { computed: \"${map:env|prod=live|*=test}\" }\n\
             b: { computed: \"tier-${param.a}\" }\n",
        );
        let bound = resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(bound.values["a"], json!("live"));
        assert_eq!(bound.values["b"], json!("tier-live"));
    }

    #[test]
    fn computed_param_cycle_is_detected() {
        let spec = spec_of(
            "a: { computed: \"${param.b}\" }\n\
             b: { computed: \"${param.a}\" }\n",
        );
        match resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err() {
            CliError::InterpolationCycle { chain } => {
                assert_eq!(chain, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected InterpolationCycle, got {other:?}"),
        }
    }

    #[test]
    fn computed_param_unknown_reference_is_typed() {
        let spec = spec_of("a: { computed: \"${param.nope}\" }\n");
        match resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err() {
            CliError::UnknownParamRef { name, .. } => assert_eq!(name, "nope"),
            other => panic!("expected UnknownParamRef, got {other:?}"),
        }
    }

    #[test]
    fn map_with_no_match_and_no_default_errors() {
        let spec = spec_of(
            "region: { default: xx }\n\
             d: { computed: \"${map:region|ca=zohocloud}\" }\n",
        );
        let err = resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("no case for") && m.contains("no")),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_unterminated_brace_errors() {
        // No closing `}` → referenced_params finds nothing, eval hits the guard.
        let spec = spec_of("a: { computed: \"x-${param.region\" }\n");
        let err = resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("unterminated")),
            "got {err:?}"
        );
    }

    #[test]
    fn computed_disallowed_directive_errors() {
        // A computed expression may only reference ${param.*} / ${map:…}.
        let spec = spec_of("a: { computed: \"${env:SECRET}\" }\n");
        let err = resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("may only reference")),
            "got {err:?}"
        );
    }

    #[test]
    fn map_case_not_key_value_errors() {
        let spec = spec_of(
            "region: { default: com }\n\
             d: { computed: \"${map:region|badcase}\" }\n",
        );
        let err = resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("is not `case=value`")),
            "got {err:?}"
        );
    }

    #[test]
    fn standalone_map_missing_switch_name_errors() {
        // `${map:|a=b}` — empty switch name.
        let mut doc = json!({
            "pipeline": { "source": { "config": { "h": "${map:|a=b}" } } }
        });
        let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("missing the switch param name")),
            "got {err:?}"
        );
    }

    #[test]
    fn standalone_map_unknown_input_errors() {
        let mut doc = json!({
            "pipeline": { "source": { "config": { "h": "${map:nope|*=x}" } } }
        });
        let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(
            matches!(&err, CliError::UnknownParamRef { name, .. } if name == "nope"),
            "got {err:?}"
        );
    }

    #[test]
    fn whole_token_map_resolves_with_declared_type() {
        // The entire value is a single `${map:…}` token → whole_token path.
        let mut doc = json!({
            "params": { "region": { "default": "com" } },
            "pipeline": { "source": { "config": {
                "domain": "${map:region|ca=zohocloud|*=zoho}"
            } } }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["source"]["config"]["domain"], json!("zoho"));
    }

    #[test]
    fn standalone_map_token_resolves_in_document() {
        // `${map:…}` used directly in a config value (not via a computed param).
        let mut doc = json!({
            "params": { "region": { "default": "ca" } },
            "pipeline": { "source": { "config": {
                "host": "accounts.${map:region|ca=zohocloud|*=zoho}.${param.region}"
            } } }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(
            doc["pipeline"]["source"]["config"]["host"],
            json!("accounts.zohocloud.ca")
        );
    }

    #[test]
    fn bind_document_substitutes_computed_param() {
        let mut doc = json!({
            "params": {
                "region": { "default": "com" },
                "accounts_domain": { "computed": "${map:region|ca=zohocloud|*=zoho}" }
            },
            "pipeline": { "source": { "config": {
                "base_url": "https://accounts.${param.accounts_domain}.${param.region}"
            } } }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(
            doc["pipeline"]["source"]["config"]["base_url"],
            json!("https://accounts.zoho.com")
        );
        // The declaration block is preserved byte-identical (computed expr intact).
        assert_eq!(
            doc["params"]["accounts_domain"]["computed"],
            json!("${map:region|ca=zohocloud|*=zoho}")
        );
    }

    #[test]
    fn missing_required_param_is_a_typed_error() {
        let spec = spec_of("tenant: { required: true, description: Tenant to sync }\n");
        match resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err() {
            CliError::MissingParam { name, description } => {
                assert_eq!(name, "tenant");
                assert_eq!(description.as_deref(), Some("Tenant to sync"));
            }
            other => panic!("expected MissingParam, got {other:?}"),
        }
    }

    #[test]
    fn unknown_supplied_param_is_rejected() {
        let spec = spec_of("tenant: { required: true }\n");
        match resolve(
            &spec,
            &supplied(&[("tenant", json!("a")), ("tenatn", json!("b"))]),
            BindMode::Strict,
        )
        .unwrap_err()
        {
            CliError::UnknownParam { name, known } => {
                assert_eq!(name, "tenatn");
                assert_eq!(known, vec!["tenant".to_string()]);
            }
            other => panic!("expected UnknownParam, got {other:?}"),
        }
    }

    #[test]
    fn supplied_value_may_not_carry_a_directive() {
        let spec = spec_of("t: { required: true }\n");
        let err = resolve(
            &spec,
            &supplied(&[("t", json!("${vault:secret/data/db#password}"))]),
            BindMode::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("literal data"), "{err}");
    }

    #[test]
    fn binds_document_typed_and_textual() {
        let mut doc = json!({
            "version": 1,
            "params": {
                "tenant": { "required": true },
                "page": { "type": "int", "default": 500 },
                "live": { "type": "bool", "default": true }
            },
            "pipeline": {
                "source": {
                    "type": "rest",
                    "config": {
                        "url": "https://api.example.com/${param.tenant}/events",
                        "page_size": "${param.page}",
                        "streaming": "${param.live}"
                    }
                }
            }
        });
        let bound = bind_document(
            &mut doc,
            &supplied(&[("tenant", json!("acme"))]),
            BindMode::Strict,
        )
        .unwrap();
        let cfg = &doc["pipeline"]["source"]["config"];
        assert_eq!(cfg["url"], "https://api.example.com/acme/events");
        // Whole-scalar tokens keep the declared type.
        assert_eq!(cfg["page_size"], json!(500));
        assert_eq!(cfg["streaming"], json!(true));
        // The declaration block survives untouched.
        assert_eq!(doc["params"]["page"]["default"], json!(500));
        assert_eq!(bound.values["tenant"], json!("acme"));
    }

    #[test]
    fn defaults_inside_the_params_block_are_not_substituted() {
        // A default that *looks* like a param reference stays literal — the block
        // is lifted out before substitution.
        let mut doc = json!({
            "params": { "a": { "default": "${param.a}" } },
            "pipeline": { "x": "ok" }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["params"]["a"]["default"], "${param.a}");
    }

    #[test]
    fn undeclared_reference_is_rejected() {
        let mut doc = json!({
            "params": { "a": { "default": "1" } },
            "pipeline": { "url": "${param.b}" }
        });
        match bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err() {
            CliError::UnknownParamRef { name, token } => {
                assert_eq!(name, "b");
                assert_eq!(token, "${param.b}");
            }
            other => panic!("expected UnknownParamRef, got {other:?}"),
        }
    }

    #[test]
    fn reference_without_a_params_block_is_rejected() {
        // No `params:` at all — a `${param.x}` token must not silently survive
        // into a connector config.
        let mut doc = json!({ "pipeline": { "url": "${param.x}" } });
        let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(matches!(err, CliError::UnknownParamRef { .. }), "{err:?}");
    }

    #[test]
    fn supplying_a_param_with_no_block_is_rejected() {
        let mut doc = json!({ "pipeline": {} });
        match bind_document(&mut doc, &supplied(&[("x", json!("1"))]), BindMode::Strict)
            .unwrap_err()
        {
            CliError::UnknownParam { known, .. } => assert!(known.is_empty()),
            other => panic!("expected UnknownParam, got {other:?}"),
        }
    }

    #[test]
    fn malformed_references_are_rejected() {
        for bad in ["${param}", "${param.a.b}"] {
            let mut doc = json!({
                "params": { "a": { "default": "1" } },
                "pipeline": { "url": bad }
            });
            let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict)
                .unwrap_err()
                .to_string();
            assert!(err.contains("param"), "{bad}: {err}");
        }
    }

    #[test]
    fn escaped_token_stays_literal() {
        let mut doc = json!({
            "params": { "a": { "default": "v" } },
            "pipeline": { "note": "$${param.a}" }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["note"], "${param.a}");
    }

    #[test]
    fn other_namespaces_survive_binding() {
        let mut doc = json!({
            "params": { "a": { "default": "v" } },
            "pipeline": { "url": "${param.a}/${now.date}/${users.id}" }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["url"], "v/${now.date}/${users.id}");
    }

    #[test]
    fn substitutes_into_keys_and_arrays() {
        let mut doc = json!({
            "params": { "h": { "default": "X-Tenant" }, "n": { "type": "int", "default": 2 } },
            "pipeline": {
                "headers": { "${param.h}": "v" },
                "list": ["${param.n}", "n=${param.n}"]
            }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["headers"]["X-Tenant"], "v");
        assert_eq!(doc["pipeline"]["list"][0], json!(2));
        assert_eq!(doc["pipeline"]["list"][1], json!("n=2"));
    }

    #[test]
    fn secret_params_are_tracked_and_redacted() {
        let spec = spec_of("token: { required: true, secret: true }\nuser: { default: bob }\n");
        let bound = resolve(
            &spec,
            &supplied(&[("token", json!("s3cret-value-long-enough"))]),
            BindMode::Strict,
        )
        .unwrap();
        assert!(bound.has_secrets());
        let red = bound.redacted();
        assert_eq!(red["token"], json!("***"));
        assert_eq!(red["user"], json!("bob"));
        // Registered for redaction, so it can never reach a log line in clear.
        assert_eq!(
            crate::secrets::registry::redact("token=s3cret-value-long-enough"),
            "token=***"
        );
    }

    #[test]
    fn invalid_params_block_is_a_config_error() {
        let doc = json!({ "params": { "a": { "type": "date" } } });
        let err = declared(&doc).unwrap_err().to_string();
        assert!(err.contains("`params:` block"), "{err}");
        // A null block is simply absent.
        assert!(declared(&json!({ "params": null })).unwrap().is_empty());
        assert!(declared(&json!({})).unwrap().is_empty());
    }

    #[test]
    fn cli_param_parsing() {
        let (k, v) = parse_cli_param("tenant=acme").unwrap();
        assert_eq!(k, "tenant");
        assert_eq!(v, json!("acme"));
        // Values may contain '='.
        let (_, v) = parse_cli_param("q=a=b").unwrap();
        assert_eq!(v, json!("a=b"));
        // Empty value is allowed (an intentional blank).
        let (_, v) = parse_cli_param("q=").unwrap();
        assert_eq!(v, json!(""));
        assert!(parse_cli_param("noequals").is_err());
        assert!(parse_cli_param("=v").is_err());

        let map = collect_cli_params(&["a=1".into(), "b=2".into()]).unwrap();
        assert_eq!(map.len(), 2);
        let err = collect_cli_params(&["a=1".into(), "a=2".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than once"), "{err}");
    }

    #[test]
    fn env_override_parsing() {
        let map = collect_env_overrides(&["A=1".into()]).unwrap();
        assert_eq!(map["A"], "1");
        // Bare NAME reads the caller's environment.
        unsafe { std::env::set_var("FAUCET_PARAM_ENV_TEST", "from-env") };
        let map = collect_env_overrides(&["FAUCET_PARAM_ENV_TEST".into()]).unwrap();
        assert_eq!(map["FAUCET_PARAM_ENV_TEST"], "from-env");
        unsafe { std::env::remove_var("FAUCET_PARAM_ENV_TEST") };
        assert!(collect_env_overrides(&["FAUCET_PARAM_ENV_TEST".into()]).is_err());
        assert!(collect_env_overrides(&["=1".into()]).is_err());
        assert!(collect_env_overrides(&["A=1".into(), "A=2".into()]).is_err());
    }

    #[test]
    fn placeholder_mode_leaves_a_bindable_document() {
        // The point of Placeholder mode: a config with required params still
        // parses + expands for structural validation.
        let mut doc = json!({
            "params": { "t": { "required": true }, "n": { "type": "int", "required": true } },
            "pipeline": { "source": { "config": { "url": "https://x/${param.t}", "n": "${param.n}" } } }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Placeholder).unwrap();
        let cfg = &doc["pipeline"]["source"]["config"];
        assert_eq!(cfg["url"], "https://x/<param>");
        assert_eq!(cfg["n"], json!(0));
    }

    #[test]
    fn bound_params_default_is_empty() {
        let b = BoundParams::default();
        assert!(!b.has_secrets());
        assert!(b.redacted().is_empty());
    }

    #[test]
    fn binding_a_non_object_document_is_a_no_op() {
        // A scalar / array document has no `params:` key; binding must not panic.
        let mut doc = json!(["${param.a}"]);
        let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(matches!(err, CliError::UnknownParamRef { .. }));
        let mut doc = json!(7);
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc, json!(7));
    }
}

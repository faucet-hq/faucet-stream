//! Typed config validation of hub templates (#823).
//!
//! A template's own `validate()` checks its shape, and the publishability lint
//! checks its policy, but neither deserializes the connector configs the
//! template produces. So a template whose `type: int` param lands where a
//! connector expects a string passed `hub lint` / `hub check` and failed for
//! every user at `faucet validate --source … --sink …`. These checks run the
//! same typed validation `faucet validate` runs — expand, each connector's
//! config into its typed struct, each transform chain compiled — over a
//! placeholder-bound composition, and name the param responsible.
//!
//! A row whose connector or transform this binary does not include is
//! skipped: a slim build must not flag a template for a connector it cannot
//! see.

use serde_json::{Value, json};

use super::compose::{Composition, compose_with};
use super::rows::{placeholder_nodes, source_template_nodes};
use super::spec::{SinkTemplate, SourceTemplate};
use crate::commands::validate::{check_sink_configs, check_source_configs, check_transforms};
use crate::error::{CliError, CliResult};
use crate::expand::ExpandedNode;
use crate::params::{ParamType, ParamsSpec};
use faucet_core::WriteMode;

/// Rows whose transform chain uses only transforms this binary has.
fn compiled_transform_rows(nodes: &[ExpandedNode]) -> Vec<ExpandedNode> {
    let available = crate::transforms::available_transforms();
    nodes
        .iter()
        .filter(|n| {
            n.transforms
                .iter()
                .all(|t| available.contains(&t.kind.as_str()))
        })
        .cloned()
        .collect()
}

fn check_source_template(t: &SourceTemplate) -> CliResult<()> {
    let nodes = source_template_nodes(t)?;
    check_source_configs(&nodes, true)?;
    check_transforms(&compiled_transform_rows(&nodes))
}

/// A one-stream source template whose source is never validated — only the
/// sink side of the composition is checked.
fn probe_source() -> SourceTemplate {
    serde_json::from_value(json!({
        "kind": "source-template",
        "name": "lint-probe",
        "source": { "type": "lint-probe", "config": {} },
        "streams": [{ "name": "probe" }],
    }))
    .expect("static probe source template")
}

fn check_sink_template(t: &SinkTemplate) -> CliResult<()> {
    let c = compose_with(&probe_source(), t, &[WriteMode::Append])?;
    let (_, nodes) = placeholder_nodes(&c.document)?;
    check_sink_configs(&nodes.unwrap_or_default(), true)
}

/// The full typed check of a composed pairing — what `faucet validate
/// --source X --sink Y` runs, offline and placeholder-bound.
pub fn check_composition(c: &Composition) -> CliResult<()> {
    // A composition is always a matrix config, never a topology.
    let (_, nodes) = placeholder_nodes(&c.document)?;
    let nodes = nodes.unwrap_or_default();
    check_source_configs(&nodes, true)?;
    check_sink_configs(&nodes, true)?;
    check_transforms(&compiled_transform_rows(&nodes))
}

/// `params` with `name` re-declared as a plain string (default stringified,
/// value set dropped) — the probe that tells whether the param's type is
/// what breaks the config.
fn as_string_param(params: &ParamsSpec, name: &str) -> ParamsSpec {
    let mut out = params.clone();
    if let Some(p) = out.get_mut(name) {
        p.kind = ParamType::String;
        p.values.clear();
        p.default = p.default.take().map(|d| match d {
            Value::String(s) => Value::String(s),
            other => Value::String(other.to_string()),
        });
    }
    out
}

/// The non-string params whose type alone makes the template fail.
fn culprit_params<T: Clone>(
    params: &ParamsSpec,
    template: &T,
    with_params: impl Fn(&mut T, ParamsSpec),
    check: impl Fn(&T) -> CliResult<()>,
) -> Vec<String> {
    params
        .iter()
        .filter(|(_, p)| p.kind != ParamType::String && p.computed.is_none())
        .filter_map(|(name, p)| {
            let mut probe = template.clone();
            with_params(&mut probe, as_string_param(params, name));
            check(&probe)
                .is_ok()
                .then(|| format!("`{name}` (type: {})", p.kind.as_str()))
        })
        .collect()
}

fn finding(e: &CliError, culprits: &[String]) -> String {
    let mut msg = format!("config does not validate (`faucet validate` would reject it): {e}");
    for c in culprits {
        msg.push_str(&format!(
            "\n    param {c} is substituted where the connector expects another type — \
             declare it `type: string`, or use it inside a longer string"
        ));
    }
    msg
}

/// Typed-validation findings for a source template (empty when it passes).
pub fn source_findings(t: &SourceTemplate) -> Vec<String> {
    match check_source_template(t) {
        Ok(()) => Vec::new(),
        Err(e) => {
            let culprits = culprit_params(&t.params, t, |t, p| t.params = p, check_source_template);
            vec![finding(&e, &culprits)]
        }
    }
}

/// Typed-validation findings for a sink template (empty when it passes).
pub fn sink_findings(t: &SinkTemplate) -> Vec<String> {
    match check_sink_template(t) {
        Ok(()) => Vec::new(),
        Err(e) => {
            let culprits = culprit_params(&t.params, t, |t, p| t.params = p, check_sink_template);
            vec![finding(&e, &culprits)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source(yaml: &str) -> SourceTemplate {
        serde_yaml::from_str(yaml).expect("source template")
    }

    fn sink(yaml: &str) -> SinkTemplate {
        serde_yaml::from_str(yaml).expect("sink template")
    }

    /// The #823 shape: an `int` param as a whole `query_params` value.
    const INT_QUERY_PARAM: &str = r#"
kind: source-template
name: acme
description: Acme API
params:
  page_size: { type: int, default: 100 }
source:
  type: rest
  config:
    base_url: "https://api.example.com"
    path: /items
    query_params:
      limit: "${param.page_size}"
streams: [{ name: items }]
"#;

    #[cfg(feature = "source-rest")]
    #[test]
    fn an_int_param_as_a_whole_query_value_is_reported_by_name() {
        let f = source_findings(&source(INT_QUERY_PARAM));
        assert_eq!(f.len(), 1, "{f:?}");
        assert!(f[0].contains("does not validate"), "{}", f[0]);
        assert!(
            f[0].contains("param `page_size` (type: int)"),
            "the param is named: {}",
            f[0]
        );
    }

    #[cfg(feature = "source-rest")]
    #[test]
    fn a_string_param_or_an_embedded_int_passes() {
        assert!(
            source_findings(&source(
                &INT_QUERY_PARAM.replace("type: int", "type: string")
            ))
            .is_empty()
        );
        let embedded = INT_QUERY_PARAM.replace("\"${param.page_size}\"", "\"n${param.page_size}\"");
        assert!(source_findings(&source(&embedded)).is_empty());
    }

    #[cfg(feature = "source-rest")]
    #[test]
    fn a_failure_no_param_explains_names_no_param() {
        let bad = INT_QUERY_PARAM.replace("path: /items", "path: /items\n    no_such_field: 1");
        let f = source_findings(&source(&bad));
        assert_eq!(f.len(), 1, "{f:?}");
        assert!(!f[0].contains("param `"), "{}", f[0]);
    }

    #[test]
    fn a_connector_this_build_lacks_is_skipped() {
        let t = INT_QUERY_PARAM.replace("type: rest", "type: not-compiled-here");
        assert!(source_findings(&source(&t)).is_empty());
    }

    #[test]
    fn as_string_param_stringifies_the_default_and_drops_the_value_set() {
        let mut params = ParamsSpec::new();
        params.insert(
            "n".into(),
            serde_json::from_value(json!({"type": "int", "default": 5, "values": [5, 6]})).unwrap(),
        );
        params.insert(
            "s".into(),
            serde_json::from_value(json!({"type": "int", "required": true})).unwrap(),
        );
        let out = as_string_param(&params, "n");
        assert_eq!(out["n"].kind, ParamType::String);
        assert_eq!(out["n"].default, Some(json!("5")));
        assert!(out["n"].values.is_empty());
        assert_eq!(out["s"].kind, ParamType::Int, "other params untouched");
        let s = as_string_param(&params, "s");
        assert_eq!(s["s"].default, None);
        let str_default: ParamsSpec = [(
            "x".to_string(),
            serde_json::from_value(json!({"type": "string", "default": "a"})).unwrap(),
        )]
        .into();
        assert_eq!(
            as_string_param(&str_default, "x")["x"].default,
            Some(json!("a"))
        );
    }

    const BOOL_SINK: &str = r#"
kind: sink-template
name: files
description: JSON Lines files
params:
  flag: { type: bool, default: true }
sink:
  type: jsonl
  config: { append: "${param.flag}" }
per_stream: { path: "./out/${stream}.jsonl" }
"#;

    #[cfg(feature = "sink-jsonl")]
    #[test]
    fn a_sink_template_is_checked_through_a_probe_source() {
        assert!(sink_findings(&sink(BOOL_SINK)).is_empty());
        let bad = BOOL_SINK.replace(
            "append: \"${param.flag}\"",
            "append: \"${param.flag}\", nope: 1",
        );
        let f = sink_findings(&sink(&bad));
        assert_eq!(f.len(), 1, "{f:?}");
        assert!(f[0].contains("does not validate"), "{}", f[0]);
    }

    #[cfg(all(feature = "source-rest", feature = "sink-jsonl"))]
    #[test]
    fn a_composed_pairing_is_checked_whole() {
        let k = sink(BOOL_SINK);
        let ok = super::super::compose::compose(
            &source(&INT_QUERY_PARAM.replace("type: int", "type: string")),
            &k,
        )
        .unwrap();
        check_composition(&ok).expect("a valid pairing passes");
        let bad = super::super::compose::compose(&source(INT_QUERY_PARAM), &k).unwrap();
        let e = check_composition(&bad).expect_err("the int query param is rejected");
        assert!(e.to_string().contains("source"), "{e}");
    }
}

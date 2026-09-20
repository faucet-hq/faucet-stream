//! Running a template test suite (#648).
//!
//! Two tiers, both offline:
//!
//! 1. **Validation** — materialize the template for a param combination
//!    exactly as a real trigger would, then `expand` it and compile each row's
//!    transform chain (or validate the graph, in topology mode). This catches
//!    the whole "param X breaks the config" class with no data and no network,
//!    which is why it is the default and why it is cheap enough to run in CI on
//!    every change.
//! 2. **Behavioural** — feed fixture records through the real pipeline via the
//!    existing `faucet test` harness, with the same matchers.
//!
//! Neither tier touches a real endpoint. A template whose behaviour depends on
//! a live system is still tested for *config validity* across its whole
//! parameter space, which is the part that silently rots.

use serde_json::Value;
use std::collections::BTreeMap;

use super::combine::{GeneratedCase, generate};
use super::spec::{Behavioral, SuiteFile};
use crate::error::{CliError, CliResult};
use crate::params::SuppliedParams;
use crate::templates::{Materialize, TemplateStore, materialize, resolve_version};

/// What one case did.
#[derive(Debug, Clone, PartialEq)]
pub struct CaseOutcome {
    pub name: String,
    pub origin: &'static str,
    pub params: BTreeMap<String, Value>,
    pub passed: bool,
    /// Why it failed, or `None` when it passed.
    pub failure: Option<String>,
}

/// The whole suite's result.
#[derive(Debug, Clone, Default)]
pub struct SuiteOutcome {
    pub cases: Vec<CaseOutcome>,
}

impl SuiteOutcome {
    pub fn failed(&self) -> usize {
        self.cases.iter().filter(|c| !c.passed).count()
    }

    pub fn passed(&self) -> usize {
        self.cases.iter().filter(|c| c.passed).count()
    }
}

/// Where the template under test comes from.
pub enum Target<'a> {
    /// A registered template, resolved through the store.
    Registered {
        store: &'a TemplateStore,
        id: &'a str,
        version: u32,
    },
    /// A config file on disk — lets a template be tested **before** it is
    /// registered, which is when these failures are cheapest to fix.
    Document { body: String },
}

/// Run every case in `file` against `target`, optionally filtered by name.
pub async fn run(
    file: &SuiteFile,
    target: Target<'_>,
    filter: Option<&str>,
) -> CliResult<SuiteOutcome> {
    let params_spec = declared_params(&target).await?;
    let cases = generate(&file.suite, &params_spec)?;

    let mut outcome = SuiteOutcome::default();
    for case in &cases {
        if !name_matches(&case.name, filter) {
            continue;
        }
        outcome.cases.push(run_validation_case(case, &target).await);
    }
    for b in &file.suite.behavioral {
        if !name_matches(&b.name, filter) {
            continue;
        }
        outcome.cases.push(run_behavioral_case(b, &target).await);
    }
    Ok(outcome)
}

/// Resolve a version selector against the store, so a suite can say
/// `select: stable` rather than pinning a number that goes stale.
pub async fn resolve_target_version(
    store: &TemplateStore,
    id: &str,
    select: Option<&str>,
) -> CliResult<u32> {
    let selector = match select {
        Some(s) => crate::serve::history::templates::VersionSelector::parse(s)?,
        None => Default::default(),
    };
    resolve_version(store, id, selector).await
}

/// Glob-ish filter: `*` matches any run of characters. A plain name is an
/// exact match, so `--filter auto:defaults` runs one case.
fn name_matches(name: &str, filter: Option<&str>) -> bool {
    let Some(pat) = filter else { return true };
    let mut rest = name;
    let mut parts = pat.split('*').peekable();
    let first = parts.next().unwrap_or("");
    if !rest.starts_with(first) {
        return false;
    }
    rest = &rest[first.len()..];
    let mut last_empty = pat.ends_with('*');
    while let Some(part) = parts.next() {
        if part.is_empty() {
            last_empty = true;
            continue;
        }
        last_empty = parts.peek().is_some() || pat.ends_with('*');
        match rest.find(part) {
            Some(i) => rest = &rest[i + part.len()..],
            None => return false,
        }
    }
    last_empty || rest.is_empty()
}

/// The template's `params:` declaration, needed by the `auto:` generator.
async fn declared_params(target: &Target<'_>) -> CliResult<crate::params::ParamsSpec> {
    let body = body_of(target).await?;
    let doc: Value = serde_yaml::from_str(&body)
        .map_err(|e| CliError::Config(format!("template test: parsing the template: {e}")))?;
    crate::params::declared(&doc)
}

async fn body_of(target: &Target<'_>) -> CliResult<String> {
    match target {
        Target::Document { body } => Ok(body.clone()),
        Target::Registered { store, id, version } => {
            let rec = store
                .template_get(id, Some(*version))
                .await
                .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
                .ok_or_else(|| CliError::UnknownPipelineTemplate {
                    id: (*id).to_string(),
                    version: Some(*version),
                })?;
            Ok(rec.body)
        }
    }
}

/// Materialize one combination and check it expands and compiles.
async fn run_validation_case(case: &GeneratedCase, target: &Target<'_>) -> CaseOutcome {
    let supplied: SuppliedParams = case.params.clone().into_iter().collect();
    let result = materialize_and_check(&supplied, target).await;

    let (passed, failure) = match (&result, case.expect.expects_failure()) {
        // Expected to work, and did.
        (Ok(()), false) => (true, None),
        // Expected to work, didn't.
        (Err(e), false) => (false, Some(e.to_string())),
        // Expected to fail, and did — check the message when one was named.
        (Err(e), true) => match &case.expect.error {
            Some(want) if !e.to_string().contains(want.as_str()) => (
                false,
                Some(format!(
                    "failed as expected, but the error did not mention '{want}': {e}"
                )),
            ),
            _ => (true, None),
        },
        // Expected to fail, didn't. This is the direction that matters most:
        // a negative case silently turning green means the guard it pins is
        // gone.
        (Ok(()), true) => (
            false,
            Some("expected this combination to fail, but it materialized cleanly".into()),
        ),
    };

    CaseOutcome {
        name: case.name.clone(),
        origin: case.origin.as_str(),
        params: case.params.clone(),
        passed,
        failure,
    }
}

/// Materialize for `supplied`, then expand + compile.
async fn materialize_and_check(supplied: &SuppliedParams, target: &Target<'_>) -> CliResult<()> {
    let body = materialize_body(supplied, target).await?;

    let cfg = crate::config::PipelineConfig::from_text(&body, std::path::Path::new("suite.json"))?;
    // Topology mode has no matrix to expand — its graph checks are the
    // equivalent gate, and skipping them would let a broken graph pass.
    if cfg.pipeline.nodes.is_empty() {
        let nodes = crate::expand::expand(&cfg)?;
        for n in &nodes {
            if !n.transforms.is_empty() {
                crate::transforms::compile_transforms(&n.transforms).map_err(|e| {
                    CliError::Config(format!("row '{}': transform chain: {e}", n.id))
                })?;
            }
        }
    } else {
        crate::topology::validate_topology_spec(&cfg)?;
    }
    Ok(())
}

/// Run one behavioural case through the fixture harness.
async fn run_behavioral_case(b: &Behavioral, target: &Target<'_>) -> CaseOutcome {
    let supplied: SuppliedParams = b.params.clone().into_iter().collect();
    let failure = match behavioral_inner(b, &supplied, target).await {
        Ok(None) => None,
        Ok(Some(msg)) => Some(msg),
        Err(e) => Some(e.to_string()),
    };
    CaseOutcome {
        name: b.name.clone(),
        origin: "behavioral",
        params: b.params.clone(),
        passed: failure.is_none(),
        failure,
    }
}

async fn behavioral_inner(
    b: &Behavioral,
    supplied: &SuppliedParams,
    target: &Target<'_>,
) -> CliResult<Option<String>> {
    // Materialize first so the case tests the *combination*, not an unbound
    // template — which is the whole point of attaching fixtures to a param
    // set rather than to the template as a whole.
    let body = materialize_body(supplied, target).await?;
    let cfg = crate::config::PipelineConfig::from_text(&body, std::path::Path::new("suite.json"))?;
    let nodes = crate::expand::expand(&cfg)?;
    let node = nodes.first().ok_or_else(|| {
        CliError::Config(format!(
            "behavioral case '{}': the materialized config expands to no rows",
            b.name
        ))
    })?;

    let input = crate::pipeline_test::fixtures::load_input(
        std::path::Path::new("."),
        &parse_input(&b.input)?,
    )?;
    let expect: crate::pipeline_test::spec::Expectation = serde_json::from_value(b.expect.clone())
        .map_err(|e| CliError::Config(format!("behavioral case '{}': `expect`: {e}", b.name)))?;

    // The same resolved-case shape `faucet test` runs, so the matchers and
    // their semantics are shared rather than reimplemented here.
    let resolved = crate::pipeline_test::runner::ResolvedCase {
        name: b.name.clone(),
        transforms: node.transforms.clone(),
        #[cfg(feature = "quality")]
        quality: node.quality.clone(),
        #[cfg(feature = "contract")]
        contract: node.contract.clone(),
        #[cfg(feature = "masking")]
        masking: node.masking.clone(),
        input,
        page_size: b.page_size.unwrap_or(0),
        clock: chrono::Utc::now().fixed_offset(),
    };
    let run = crate::pipeline_test::runner::run_case(&resolved).await?;
    let failures = crate::pipeline_test::diff::evaluate(&expect, &run);
    Ok(failures.first().cloned())
}

/// Materialize the template body for one param combination.
async fn materialize_body(supplied: &SuppliedParams, target: &Target<'_>) -> CliResult<String> {
    match target {
        // `Materialize::Local` (not `Persisted`): a suite runs in this
        // process, so load-time directives resolve here exactly as they would
        // on a `faucet template run`.
        Target::Registered { store, id, version } => Ok(materialize(
            store,
            id,
            *version,
            supplied,
            &BTreeMap::new(),
            Materialize::Local,
        )
        .await?
        .body),
        Target::Document { body } => {
            let mut doc: Value = serde_yaml::from_str(body)
                .map_err(|e| CliError::Config(format!("template test: {e}")))?;
            crate::params::bind_document(&mut doc, supplied, crate::params::BindMode::Strict)?;
            if let Some(map) = doc.as_object_mut() {
                map.remove("params");
            }
            serde_json::to_string(&doc)
                .map_err(|e| CliError::Internal(format!("template test: {e}")))
        }
    }
}

/// Accept the same `input:` shapes `faucet test` does — an inline array or a
/// path — by round-tripping through its own spec type.
fn parse_input(v: &Value) -> CliResult<crate::pipeline_test::spec::InputSpec> {
    serde_json::from_value(v.clone())
        .map_err(|e| CliError::Config(format!("behavioral case `input`: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_matches_exactly_or_by_glob() {
        assert!(name_matches("auto:defaults", None));
        assert!(name_matches("auto:defaults", Some("auto:defaults")));
        assert!(!name_matches("auto:defaults", Some("auto:missing")));
        assert!(name_matches("auto:missing-tenant", Some("auto:*")));
        assert!(name_matches("sink=jsonl,object=A", Some("*object=A")));
        assert!(name_matches("sink=jsonl,object=A", Some("sink=*,object=A")));
        assert!(!name_matches("sink=bigquery", Some("sink=jsonl*")));
    }

    #[test]
    fn a_bare_prefix_is_not_a_prefix_match() {
        // Without this, `--filter auto` would silently run every auto case
        // when the author meant one named `auto`.
        assert!(!name_matches("auto:defaults", Some("auto")));
    }
}

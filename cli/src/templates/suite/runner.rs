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
use crate::templates::{Materialize, TemplateStore, resolve_version};

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
    /// A registered template, resolved through the store. `sink` is the
    /// registered sink template (id, version) a `source-template` composes
    /// with; ignored by a `pipeline` template.
    Registered {
        store: &'a TemplateStore,
        id: &'a str,
        version: u32,
        sink: Option<(&'a str, u32)>,
    },
    /// A config file on disk — lets a template be tested **before** it is
    /// registered, which is when these failures are cheapest to fix.
    /// `sink_body` is the sink template document a `source-template` file
    /// composes with.
    Document {
        body: String,
        sink_body: Option<String>,
    },
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
/// For a source template this is the **composed** declaration (source + sink
/// params merged), because that is the surface a trigger binds.
async fn declared_params(target: &Target<'_>) -> CliResult<crate::params::ParamsSpec> {
    let doc = effective_document(target).await?;
    crate::params::declared(&doc)
}

fn parse_template_text(text: &str, what: &str) -> CliResult<Value> {
    serde_yaml::from_str(text)
        .map_err(|e| CliError::Config(format!("template test: parsing the {what}: {e}")))
}

/// The pipeline document the cases run against: a `pipeline` template's body
/// as-is, or a `source-template` composed with its sink. Composition here is
/// exactly what a trigger does, so a suite tests the real thing.
async fn effective_document(target: &Target<'_>) -> CliResult<Value> {
    match target {
        Target::Document { body, sink_body } => {
            let doc = parse_template_text(body, "template")?;
            match crate::hub::detect_kind(&doc) {
                Some(crate::hub::TemplateKind::SourceTemplate) => {
                    let sink_text = sink_body.as_deref().ok_or_else(|| {
                        CliError::Config(
                            "template test: the template is a source-template — set `sink:` to the \
                             sink template (a path here) it should compose with"
                                .into(),
                        )
                    })?;
                    let source: crate::hub::SourceTemplate =
                        serde_json::from_value(doc).map_err(|e| {
                            CliError::Config(format!("template test: source-template: {e}"))
                        })?;
                    let sink: crate::hub::SinkTemplate =
                        serde_json::from_value(parse_template_text(sink_text, "sink template")?)
                            .map_err(|e| {
                                CliError::Config(format!("template test: sink-template: {e}"))
                            })?;
                    Ok(crate::hub::compose(&source, &sink)?.document)
                }
                Some(crate::hub::TemplateKind::SinkTemplate) => Err(CliError::Config(
                    "template test: a sink-template has no streams to test on its own — point \
                     `template:` at a source template and name this one under `sink:`"
                        .into(),
                )),
                _ => Ok(doc),
            }
        }
        Target::Registered {
            store,
            id,
            version,
            sink,
        } => {
            let rec = store
                .template_get(id, Some(*version))
                .await
                .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
                .ok_or_else(|| CliError::UnknownPipelineTemplate {
                    id: (*id).to_string(),
                    version: Some(*version),
                })?;
            match rec.kind {
                crate::hub::TemplateKind::Pipeline => parse_template_text(&rec.body, "template"),
                crate::hub::TemplateKind::SourceTemplate => {
                    let (sink_id, sink_version) = sink.ok_or_else(|| {
                        CliError::Config(format!(
                            "template test: '{id}' is a source-template — set `sink:` to the registered \
                             sink template it should compose with"
                        ))
                    })?;
                    let sink_rec = store
                        .template_get(sink_id, Some(sink_version))
                        .await
                        .map_err(|e| CliError::Internal(format!("template registry read: {e}")))?
                        .ok_or_else(|| CliError::UnknownPipelineTemplate {
                            id: sink_id.to_string(),
                            version: Some(sink_version),
                        })?;
                    let source: crate::hub::SourceTemplate =
                        serde_json::from_value(parse_template_text(&rec.body, "source-template")?)
                            .map_err(|e| {
                                CliError::Internal(format!("stored source-template '{id}': {e}"))
                            })?;
                    let sink_t: crate::hub::SinkTemplate = serde_json::from_value(
                        parse_template_text(&sink_rec.body, "sink-template")?,
                    )
                    .map_err(|e| {
                        CliError::Internal(format!("stored sink-template '{sink_id}': {e}"))
                    })?;
                    Ok(crate::hub::compose(&source, &sink_t)?.document)
                }
                crate::hub::TemplateKind::SinkTemplate => Err(CliError::Config(format!(
                    "template test: '{id}' is a sink-template and has no streams to test on its own"
                ))),
            }
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
        Target::Registered {
            store,
            id,
            version,
            sink,
        } => {
            let choice = crate::templates::SinkChoice {
                id: sink.map(|(s, _)| s.to_string()),
                version: match sink {
                    Some((_, v)) => crate::serve::history::templates::VersionSelector::Pinned(*v),
                    None => Default::default(),
                },
            };
            Ok(crate::templates::materialize_for_run(
                store,
                id,
                *version,
                &choice,
                supplied,
                &BTreeMap::new(),
                Materialize::Local,
            )
            .await?
            .body)
        }
        Target::Document { .. } => {
            let mut doc = effective_document(target).await?;
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

    // ── the runner against a document target ──────────────────────────────
    //
    // `Target::Document` needs no registry, so the whole materialize → expand
    // → compile path is exercised here with nothing but a config string.

    /// A template with one required param, one defaulted, and one closed set.
    fn template() -> String {
        r#"version: 1
name: suite-fixture
params:
  tenant:
    type: string
    required: true
  region:
    type: string
    default: us
    values: [us, eu]
  page_size:
    type: int
    default: 100
pipeline:
  source:
    type: rest
    config:
      base_url: "https://${param.region}.example.com"
      path: "/t/${param.tenant}"
  sink:
    type: jsonl
    config:
      path: "./out/${param.tenant}.jsonl"
"#
        .to_string()
    }

    fn doc() -> Target<'static> {
        Target::Document {
            body: template(),
            sink_body: None,
        }
    }

    fn suite_from(yaml: &str) -> SuiteFile {
        SuiteFile::parse(yaml).expect("suite parses")
    }

    #[tokio::test]
    async fn an_explicit_case_that_materializes_cleanly_passes() {
        let file = suite_from(
            r#"
version: 1
template: ignored-for-document-targets
suite:
  cases:
    - name: eu
      params: { tenant: acme, region: eu }
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert_eq!(out.passed(), 1, "{:?}", out.cases);
        assert_eq!(out.failed(), 0);
        assert_eq!(out.cases[0].origin, "explicit");
        assert!(out.cases[0].failure.is_none());
    }

    #[tokio::test]
    async fn a_value_outside_the_closed_set_fails_the_case() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  cases:
    - name: bad-region
      params: { tenant: acme, region: antarctica }
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert_eq!(out.failed(), 1);
        let msg = out.cases[0].failure.as_deref().unwrap_or_default();
        assert!(msg.contains("region"), "{msg}");
    }

    #[tokio::test]
    async fn a_negative_case_passes_when_the_error_matches_and_fails_when_it_does_not() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  cases:
    - name: names-the-param
      params: { tenant: acme, region: antarctica }
      expect: { error: region }
    - name: names-the-wrong-thing
      params: { tenant: acme, region: antarctica }
      expect: { error: "some unrelated phrase" }
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert!(out.cases[0].passed, "{:?}", out.cases[0]);
        assert!(!out.cases[1].passed, "a wrong `error:` must not pass");
        let msg = out.cases[1].failure.as_deref().unwrap_or_default();
        assert!(msg.contains("did not mention"), "{msg}");
    }

    /// The direction that matters most: a negative case going green means the
    /// guard it pins is gone.
    #[tokio::test]
    async fn a_case_expected_to_fail_that_succeeds_is_reported() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  cases:
    - name: should-have-failed
      params: { tenant: acme }
      expect: { valid: false }
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert_eq!(out.failed(), 1);
        let msg = out.cases[0].failure.as_deref().unwrap_or_default();
        assert!(msg.contains("expected this combination to fail"), "{msg}");
    }

    /// A missing required param is a materialize failure, not a panic.
    #[tokio::test]
    async fn omitting_a_required_param_fails_the_case_naming_it() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  cases:
    - name: no-tenant
      params: { region: eu }
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert_eq!(out.failed(), 1);
        assert!(
            out.cases[0]
                .failure
                .as_deref()
                .unwrap_or_default()
                .contains("tenant"),
            "{:?}",
            out.cases[0].failure
        );
    }

    #[tokio::test]
    async fn auto_cases_are_derived_from_the_templates_own_params() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  auto:
    enum_coverage: true
    required_omitted: true
    defaults_baseline: true
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        let names: Vec<&str> = out.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"auto:defaults"), "{names:?}");
        assert!(names.contains(&"auto:region=us"), "{names:?}");
        assert!(names.contains(&"auto:region=eu"), "{names:?}");
        assert!(names.contains(&"auto:missing-tenant"), "{names:?}");
        assert_eq!(out.failed(), 0, "{:?}", out.cases);
        assert!(out.cases.iter().all(|c| c.origin == "auto"));
    }

    #[tokio::test]
    async fn combine_sweeps_the_axes_and_fills_the_required_params() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  combine:
    params:
      region: [us, eu]
      page_size: [1, 50]
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert_eq!(out.cases.len(), 4, "2 x 2");
        assert_eq!(out.failed(), 0, "{:?}", out.cases);
        assert!(out.cases.iter().all(|c| c.origin == "combine"));
    }

    #[tokio::test]
    async fn the_filter_selects_a_subset_by_glob() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  cases:
    - name: keep-me
      params: { tenant: a }
    - name: drop-me
      params: { tenant: b }
"#,
        );
        let out = run(&file, doc(), Some("keep-*")).await.expect("runs");
        assert_eq!(out.cases.len(), 1);
        assert_eq!(out.cases[0].name, "keep-me");
    }

    /// The behavioural tier runs fixture records through the real pipeline and
    /// reports a mismatch as a case failure rather than an error.
    #[tokio::test]
    async fn a_behavioural_case_runs_fixtures_and_reports_a_mismatch() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  behavioral:
    - name: counts-the-rows
      params: { tenant: acme }
      input: [{ "id": "1" }, { "id": "2" }]
      expect: { records_written: 2 }
    - name: wrong-count
      params: { tenant: acme }
      input: [{ "id": "1" }]
      expect: { records_written: 99 }
"#,
        );
        let out = run(&file, doc(), None).await.expect("runs");
        assert_eq!(out.cases.len(), 2);
        assert!(out.cases[0].passed, "{:?}", out.cases[0].failure);
        assert_eq!(out.cases[0].origin, "behavioral");
        assert!(!out.cases[1].passed, "a wrong expectation must fail");
    }

    #[tokio::test]
    async fn a_malformed_behavioural_expectation_fails_the_case_not_the_run() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  behavioral:
    - name: bad-expect
      params: { tenant: acme }
      input: []
      expect: { not_a_real_key: 1 }
"#,
        );
        let out = run(&file, doc(), None)
            .await
            .expect("the run itself survives");
        assert_eq!(out.failed(), 1);
        assert!(out.cases[0].failure.is_some());
    }

    /// A template that is not parseable is a run-level error, not a silent
    /// zero-case pass.
    #[tokio::test]
    async fn an_unparseable_template_is_an_error() {
        let file = suite_from(
            r#"
version: 1
template: t
suite:
  cases:
    - name: a
      params: {}
"#,
        );
        let target = Target::Document {
            body: "this: is: not: valid: yaml:
"
            .into(),
            sink_body: None,
        };
        assert!(run(&file, target, None).await.is_err());
    }

    // ── the registry target ───────────────────────────────────────────────

    /// Register the same fixture into an in-process store so the
    /// `Target::Registered` path — version resolution and the registry read —
    /// is exercised without a database.
    async fn registered() -> crate::templates::TemplateStore {
        let store = crate::templates::resolve_store_url("memory")
            .await
            .expect("memory store");
        crate::templates::register(
            &store,
            crate::templates::RegisterRequest {
                id: Some("suite-fixture".into()),
                body: template(),
                format: crate::serve::load::ConfigFormat::Yaml,
                description: None,
                tags: Vec::new(),
                launch: true,
                created_by: None,
            },
        )
        .await
        .expect("register");
        store
    }

    #[tokio::test]
    async fn a_registered_template_resolves_and_runs() {
        let store = registered().await;
        let version = resolve_target_version(&store, "suite-fixture", None)
            .await
            .expect("stable resolves after a launch");
        assert_eq!(version, 1);

        let file = suite_from(
            r#"
version: 1
template: suite-fixture
suite:
  cases:
    - name: eu
      params: { tenant: acme, region: eu }
"#,
        );
        let out = run(
            &file,
            Target::Registered {
                store: &store,
                id: "suite-fixture",
                version,
                sink: None,
            },
            None,
        )
        .await
        .expect("runs");
        assert_eq!(out.passed(), 1, "{:?}", out.cases);
    }

    #[tokio::test]
    async fn an_explicit_selector_is_honoured() {
        let store = registered().await;
        assert_eq!(
            resolve_target_version(&store, "suite-fixture", Some("1"))
                .await
                .expect("pinned"),
            1
        );
        assert_eq!(
            resolve_target_version(&store, "suite-fixture", Some("newest"))
                .await
                .expect("channel"),
            1
        );
        assert!(
            resolve_target_version(&store, "suite-fixture", Some("latest"))
                .await
                .is_err(),
            "`latest` is deliberately not a channel"
        );
    }

    #[tokio::test]
    async fn an_unknown_registered_template_is_an_error() {
        let store = registered().await;
        let file = suite_from(
            "version: 1\ntemplate: nope\nsuite:\n  cases:\n    - name: a\n      params: {}\n",
        );
        let err = run(
            &file,
            Target::Registered {
                store: &store,
                id: "nope",
                version: 1,
                sink: None,
            },
            None,
        )
        .await
        .expect_err("unknown id");
        assert!(err.to_string().contains("nope"), "{err}");
    }

    // ── source × sink suites ────────────────────────────────────────────────

    fn hub_pair(dir: &std::path::Path) -> (String, String) {
        std::fs::write(dir.join("orders.csv"), "id,total\n1,10\n").unwrap();
        let source = format!(
            "kind: source-template\nname: acme-exports\ndescription: Acme exports\nparams:\n  data_dir: {{ type: string, default: {} }}\n  region: {{ type: string, values: [eu, us], default: eu }}\nsource:\n  type: csv\n  config:\n    path: \"${{param.data_dir}}/orders.csv\"\nstreams:\n  - {{ name: orders, primary_keys: [id], write: [overwrite, upsert] }}\n",
            dir.display()
        );
        let sink = "kind: sink-template\nname: local-jsonl\ndescription: Local files\nparams:\n  out_dir: { type: string, required: true }\nsink:\n  type: jsonl\n  config: { append: false }\nper_stream:\n  path: \"${param.out_dir}/${source}/${stream}.jsonl\"\nwrite_mode_aliases: { overwrite: append }\n".to_string();
        (source, sink)
    }

    #[tokio::test]
    async fn a_source_template_file_composes_with_a_sink_file() {
        let dir = tempfile::tempdir().unwrap();
        let (source, sink) = hub_pair(dir.path());
        // The composed param surface is the union: `out_dir` comes from the sink.
        let file = suite_from(
            r#"
version: 1
template: acme-exports
suite:
  cases:
    - name: eu
      params: { region: eu, out_dir: ./out }
    - name: missing-sink-param
      params: { region: eu }
      expect: { error: "out_dir" }
"#,
        );
        let out = run(
            &file,
            Target::Document {
                body: source.clone(),
                sink_body: Some(sink.clone()),
            },
            None,
        )
        .await
        .expect("runs");
        assert_eq!(out.passed(), 2, "{:?}", out.cases);

        // Without a sink the file is not testable, and the message says which key to set.
        let err = run(
            &file,
            Target::Document {
                body: source,
                sink_body: None,
            },
            None,
        )
        .await
        .expect_err("needs a sink")
        .to_string();
        assert!(err.contains("`sink:`"), "{err}");

        // A sink template has no streams to test.
        let err = run(
            &file,
            Target::Document {
                body: sink,
                sink_body: None,
            },
            None,
        )
        .await
        .expect_err("sink alone")
        .to_string();
        assert!(err.contains("no streams"), "{err}");
    }

    #[tokio::test]
    async fn a_registered_source_template_composes_with_a_registered_sink() {
        let dir = tempfile::tempdir().unwrap();
        let (source, sink) = hub_pair(dir.path());
        let store = crate::templates::resolve_store_url("memory").await.unwrap();
        for body in [source, sink] {
            crate::templates::register(
                &store,
                crate::templates::RegisterRequest {
                    id: None,
                    body,
                    format: crate::serve::load::ConfigFormat::Yaml,
                    description: None,
                    tags: Vec::new(),
                    launch: true,
                    created_by: None,
                },
            )
            .await
            .expect("register");
        }
        let file = suite_from(
            r#"
version: 1
template: acme-exports
sink: local-jsonl
suite:
  auto: { enum_coverage: true }
  cases:
    - name: eu
      params: { region: eu, out_dir: ./out }
"#,
        );
        let out = run(
            &file,
            Target::Registered {
                store: &store,
                id: "acme-exports",
                version: 1,
                sink: Some(("local-jsonl", 1)),
            },
            None,
        )
        .await
        .expect("runs");
        // auto enum coverage fills `out_dir` (required, from the sink) with a placeholder.
        assert!(out.passed() >= 1, "{:?}", out.cases);
        assert_eq!(out.failed(), 0, "{:?}", out.cases);

        let err = run(
            &file,
            Target::Registered {
                store: &store,
                id: "acme-exports",
                version: 1,
                sink: None,
            },
            None,
        )
        .await
        .expect_err("needs a sink")
        .to_string();
        assert!(err.contains("`sink:`"), "{err}");

        let err = run(
            &file,
            Target::Registered {
                store: &store,
                id: "local-jsonl",
                version: 1,
                sink: None,
            },
            None,
        )
        .await
        .expect_err("sink alone")
        .to_string();
        assert!(err.contains("no streams"), "{err}");
    }

    #[test]
    fn outcome_counts_split_passed_and_failed() {
        let outcome = SuiteOutcome {
            cases: vec![
                CaseOutcome {
                    name: "a".into(),
                    origin: "explicit",
                    params: BTreeMap::new(),
                    passed: true,
                    failure: None,
                },
                CaseOutcome {
                    name: "b".into(),
                    origin: "auto",
                    params: BTreeMap::new(),
                    passed: false,
                    failure: Some("nope".into()),
                },
            ],
        };
        assert_eq!(outcome.passed(), 1);
        assert_eq!(outcome.failed(), 1);
    }
}

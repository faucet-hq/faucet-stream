//! Running a registered version's test bundle, recording the result, and the
//! launch gate over the registry (#856).

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::{CliError, CliResult};
use crate::hub::TemplateKind;
use crate::serve::history::templates::{
    RESULTS_RETAIN, TemplateRecord, TemplateTestResult, VersionSelector,
};
use crate::template_tests::bundle::{SuiteRequirement, TestBundle, TestSuiteTemplate, bundle_of};
use crate::template_tests::gate::{TestStatus, VersionStatus, status_of};
use crate::template_tests::result::BundleOutcome;
use crate::template_tests::run::{SharedSuite, pick, run_bundle};
use crate::template_tests::runner::Target;
use crate::templates::{OverlayChoice, TemplateStore, parse_body};

fn read_err(e: crate::serve::history::HistoryError) -> CliError {
    crate::templates::store::registry_err("template registry read", e)
}

/// sha256 of a stored body, exactly as stored.
pub fn body_sha256(body: &str) -> String {
    format!("{:x}", Sha256::digest(body.as_bytes()))
}

/// Whether a document carries a `tests:` block (unparseable = no).
pub fn document_has_tests(body: &str, format: crate::serve::load::ConfigFormat) -> bool {
    parse_body(body, format).is_ok_and(|d| d.get("tests").is_some())
}

/// The `tests:` block of a stored version, if it has one.
pub fn bundle_in(rec: &TemplateRecord) -> CliResult<Option<TestBundle>> {
    if rec.kind == TemplateKind::TestSuite {
        return Ok(None);
    }
    bundle_of(&parse_body(&rec.body, rec.format)?, rec.kind)
}

/// Every registered, non-deprecated release of the shared suite `name`.
async fn candidates(
    store: &TemplateStore,
    name: &str,
) -> CliResult<Vec<(Option<u32>, TestSuiteTemplate)>> {
    let state = store.template_state(name).await.map_err(read_err)?;
    let mut out = Vec::new();
    for v in &state.versions {
        if state.version_deprecation(*v).is_some() {
            continue;
        }
        let Some(rec) = store.template_get(name, Some(*v)).await.map_err(read_err)? else {
            continue;
        };
        if rec.kind != TemplateKind::TestSuite {
            return Err(CliError::Config(format!(
                "requires_suites: '{name}' is a {}, not a test-suite",
                rec.kind
            )));
        }
        let doc = parse_body(&rec.body, rec.format)?;
        out.push((
            Some(*v),
            crate::template_tests::bundle::parse_test_suite(doc)?,
        ));
    }
    Ok(out)
}

/// Resolve every requirement against the registry: the highest registered
/// release in range, or [`CliError::UnsatisfiedSuiteRequirement`].
pub async fn resolve_shared(
    store: &TemplateStore,
    reqs: &[SuiteRequirement],
) -> CliResult<Vec<SharedSuite>> {
    let mut out = Vec::with_capacity(reqs.len());
    for r in reqs {
        out.push(pick(r, candidates(store, &r.name).await?)?);
    }
    Ok(out)
}

/// The release `release` of `id` already registered, if any.
pub async fn registered_release(
    store: &TemplateStore,
    id: &str,
    release: &semver::Version,
) -> CliResult<Option<u32>> {
    let state = store.template_state(id).await.map_err(read_err)?;
    for v in state.versions {
        let Some(rec) = store.template_get(id, Some(v)).await.map_err(read_err)? else {
            continue;
        };
        if rec.kind != TemplateKind::TestSuite {
            continue;
        }
        let t =
            crate::template_tests::bundle::parse_test_suite(parse_body(&rec.body, rec.format)?)?;
        if t.release_version().ok().as_ref() == Some(release) {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

async fn select(store: &TemplateStore, id: &str, sel: Option<&str>) -> CliResult<u32> {
    crate::template_tests::resolve_target_version(store, id, sel).await
}

/// Run `id`@`version`'s bundle. `Ok(None)` when the version carries no
/// `tests:` block. Setup failures (a companion that does not resolve, a shared
/// suite out of range) are part of the outcome, so they are recorded as a red
/// run rather than lost.
pub async fn run_version(
    store: &TemplateStore,
    id: &str,
    version: u32,
    filter: Option<&str>,
) -> CliResult<Option<(TemplateRecord, BundleOutcome)>> {
    let rec = store
        .template_get(id, Some(version))
        .await
        .map_err(read_err)?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })?;
    if rec.kind == TemplateKind::TestSuite {
        return Err(CliError::Config(format!(
            "'{id}' is a test-suite — it runs as part of the bundle of every template that \
             requires it, not on its own"
        )));
    }
    let Some(bundle) = bundle_in(&rec)? else {
        return Ok(None);
    };
    let outcome = match prepare(store, &rec, &bundle).await {
        Ok((companions, shared)) => {
            let target = companions.target(store, &rec);
            run_bundle(&bundle, &target, &shared, filter).await
        }
        Err(e) => BundleOutcome {
            error: Some(e.to_string()),
            ..Default::default()
        },
    };
    Ok(Some((rec, outcome)))
}

/// The resolved companions of a registered subject.
struct Companions {
    source: Option<(String, u32)>,
    sink: Option<(String, u32)>,
    overlay: Option<(String, u32)>,
}

impl Companions {
    fn target<'a>(&'a self, store: &'a TemplateStore, rec: &'a TemplateRecord) -> Target<'a> {
        let reg = |id: &str, v: u32| OverlayChoice::Registered {
            id: id.to_string(),
            version: VersionSelector::Pinned(v),
        };
        let pair = |p: &'a Option<(String, u32)>| p.as_ref().map(|(i, v)| (i.as_str(), *v));
        match rec.kind {
            TemplateKind::SinkTemplate => {
                let (sid, sv) = pair(&self.source).unwrap_or(("", 0));
                Target::Registered {
                    store,
                    id: sid,
                    version: sv,
                    sink: Some((rec.id.as_str(), rec.version)),
                    overlay: None,
                }
            }
            TemplateKind::Deployment => {
                let (sid, sv) = pair(&self.source).unwrap_or(("", 0));
                Target::Registered {
                    store,
                    id: sid,
                    version: sv,
                    sink: pair(&self.sink),
                    overlay: Some(reg(&rec.id, rec.version)),
                }
            }
            _ => Target::Registered {
                store,
                id: rec.id.as_str(),
                version: rec.version,
                sink: pair(&self.sink),
                overlay: self.overlay.as_ref().map(|(i, v)| reg(i, *v)),
            },
        }
    }
}

async fn prepare(
    store: &TemplateStore,
    _rec: &TemplateRecord,
    bundle: &TestBundle,
) -> CliResult<(Companions, Vec<SharedSuite>)> {
    let mut c = Companions {
        source: None,
        sink: None,
        overlay: None,
    };
    if let Some(s) = &bundle.source {
        c.source = Some((
            s.clone(),
            select(store, s, bundle.source_select.as_deref()).await?,
        ));
    }
    if let Some(s) = &bundle.sink {
        c.sink = Some((
            s.clone(),
            select(store, s, bundle.sink_select.as_deref()).await?,
        ));
    }
    if let Some(s) = &bundle.overlay {
        c.overlay = Some((
            s.clone(),
            select(store, s, bundle.overlay_select.as_deref()).await?,
        ));
    }
    let shared = resolve_shared(store, &bundle.requires_suites).await?;
    Ok((c, shared))
}

/// Run `id`@`version`'s bundle and record the result on the version.
pub async fn test_version(
    store: &TemplateStore,
    id: &str,
    version: u32,
    recorded_by: Option<&str>,
) -> CliResult<TemplateTestResult> {
    let (rec, outcome) = run_version(store, id, version, None)
        .await?
        .ok_or_else(|| no_bundle(id, version))?;
    let result = TemplateTestResult::new(
        id,
        version,
        body_sha256(&rec.body),
        outcome,
        recorded_by.map(str::to_string),
    );
    store
        .template_record_test(&result)
        .await
        .map_err(|e| crate::templates::store::registry_err("template test result write", e))?;
    tracing::info!(
        template = %id,
        version,
        passed = result.passed,
        cases = result.cases.len(),
        "recorded template test run"
    );
    Ok(result)
}

fn no_bundle(id: &str, version: u32) -> CliError {
    CliError::Config(format!(
        "v{version} of template '{id}' has no `tests:` block — add one and register a new version"
    ))
}

/// A version's test status with its recent runs.
#[derive(Debug, Clone, Serialize)]
pub struct VersionTests {
    pub version: u32,
    pub has_tests: bool,
    #[serde(flatten)]
    pub status: VersionStatus,
    /// Recent runs, newest first.
    pub results: Vec<TemplateTestResult>,
}

async fn record(store: &TemplateStore, id: &str, version: u32) -> CliResult<TemplateRecord> {
    store
        .template_get(id, Some(version))
        .await
        .map_err(read_err)?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })
}

/// `id`@`version`'s test status and up to `limit` recent runs.
pub async fn version_tests(
    store: &TemplateStore,
    id: &str,
    version: u32,
    limit: usize,
) -> CliResult<VersionTests> {
    let rec = record(store, id, version).await?;
    let has_tests = bundle_in(&rec)?.is_some();
    let results = store
        .template_test_results(id, Some(version), limit.max(1))
        .await
        .map_err(read_err)?;
    Ok(VersionTests {
        version,
        has_tests,
        status: status_of(id, version, has_tests, &body_sha256(&rec.body), &results),
        results,
    })
}

/// `id`@`version`'s test status.
pub async fn version_status(
    store: &TemplateStore,
    id: &str,
    version: u32,
) -> CliResult<VersionStatus> {
    Ok(version_tests(store, id, version, RESULTS_RETAIN).await?.status)
}

/// Refuse `action` unless `id`@`version` is `passing` — the one check every
/// label move (launch, rollback, a channel) goes through. An `untested`
/// version has its bundle run first, through the normal record path, and is
/// then decided on that result; a `failing` one is never re-run here.
pub async fn require_passing(
    store: &TemplateStore,
    id: &str,
    version: u32,
    action: &str,
) -> CliResult<VersionStatus> {
    let mut st = version_status(store, id, version).await?;
    if st.status == TestStatus::Untested {
        test_version(store, id, version, None).await?;
        st = version_status(store, id, version).await?;
    }
    if st.status.can_hold_label() {
        Ok(st)
    } else {
        Err(st.refusal(id, version, action))
    }
}

/// Fill in each version's test status, whether the template's newest
/// version has tests, the stable version's status, and — once the template
/// has been launched — make `newest` skip versions that are not `passing`.
pub async fn annotate(
    store: &TemplateStore,
    id: &str,
    state: &mut crate::serve::history::templates::TemplateState,
) -> CliResult<()> {
    for v in state.versions.clone() {
        let st = version_status(store, id, v).await?;
        state.test_status.insert(v, st.status);
    }
    state.has_tests = state
        .versions
        .first()
        .and_then(|v| state.test_status.get(v))
        .is_some_and(|s| *s != TestStatus::NoTests);
    state.stable_tests = state.stable.and_then(|v| state.test_status.get(&v).copied());
    if state.stable.is_some() {
        let retired = |v: u32| state.version_deprecation(v).is_some();
        state.newest = state.versions.iter().copied().find(|v| {
            !retired(*v) && state.test_status.get(v) == Some(&TestStatus::Passing)
        });
    }
    Ok(())
}

/// The newest stored version of `id` carrying a `tests:` block, with that block.
pub async fn inheritable_tests(
    store: &TemplateStore,
    id: &str,
) -> CliResult<Option<(u32, serde_json::Value)>> {
    for v in store.template_versions(id).await.map_err(read_err)? {
        let rec = record(store, id, v).await?;
        if bundle_in(&rec)?.is_some()
            && let Some(t) = parse_body(&rec.body, rec.format)?.get("tests")
        {
            return Ok(Some((v, t.clone())));
        }
    }
    Ok(None)
}

/// `body` with a top-level `tests:` block added. A YAML document keeps its
/// text and comments, with the block appended; a document that does not read
/// back identically that way (flow style) is re-emitted.
pub fn with_tests(
    body: &str,
    format: crate::serve::load::ConfigFormat,
    tests: &serde_json::Value,
) -> CliResult<String> {
    let mut doc = parse_body(body, format)?;
    let obj = doc
        .as_object_mut()
        .ok_or_else(|| CliError::Config("a template must be a mapping".into()))?;
    obj.insert("tests".into(), tests.clone());
    let emit_err = |e: String| CliError::Internal(format!("adding inherited tests: {e}"));
    match format {
        crate::serve::load::ConfigFormat::Json => {
            serde_json::to_string_pretty(&doc).map_err(|e| emit_err(e.to_string()))
        }
        crate::serve::load::ConfigFormat::Yaml => {
            let block = serde_yaml::to_string(&serde_json::json!({ "tests": tests }))
                .map_err(|e| emit_err(e.to_string()))?;
            let appended = format!("{}\n{block}", body.trim_end());
            if parse_body(&appended, format).ok().as_ref() == Some(&doc) {
                Ok(appended)
            } else {
                serde_yaml::to_string(&doc).map_err(|e| emit_err(e.to_string()))
            }
        }
    }
}

/// Test support: `body` with a minimal `tests:` block (the all-defaults
/// case) when it is a pipeline document without one, so tests about other
/// behaviour can launch it.
#[cfg(test)]
pub(crate) fn smoke(body: &str) -> String {
    let doc: serde_json::Value = serde_yaml::from_str(body).unwrap_or_default();
    let pipeline = doc.get("pipeline").is_some()
        && doc.get("kind").and_then(|k| k.as_str()).is_none_or(|k| k == "pipeline");
    if !pipeline || doc.get("tests").is_some() {
        return body.to_string();
    }
    format!(
        "{}\ntests:\n  suite:\n    auto: {{ defaults_baseline: true }}\n",
        body.trim_end()
    )
}

/// Test support: run `id`'s newest version's bundle again and record it — for
/// hub-kind pairs whose companions were registered after them.
#[cfg(test)]
pub(crate) async fn retest(store: &TemplateStore, id: &str) -> TemplateTestResult {
    let v = store.template_versions(id).await.unwrap()[0];
    test_version(store, id, v, None).await.unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::templates::VersionChannel;
    use crate::template_tests::TestStatus;
    use crate::templates::{RegisterRequest, register, register_tested};

    pub(crate) fn pipeline(name: &str, expect_written: usize) -> String {
        format!(
            r#"kind: pipeline
version: 1
name: {name}
params:
  region:
    type: string
    default: us
    values: [us, eu]
pipeline:
  source:
    type: rest
    config:
      base_url: "https://${{param.region}}.example.com"
      path: /rows
  sink:
    type: file
    config:
      path: ./out.jsonl
tests:
  suite:
    auto: {{ enum_coverage: true, defaults_baseline: true }}
  fixtures:
    - name: rows
      input: [{{ "id": 1 }}, {{ "id": 2 }}]
      expect: {{ records_written: {expect_written} }}
"#
        )
    }

    fn untested_body(name: &str) -> String {
        let body = pipeline(name, 2);
        body[..body.find("tests:").unwrap()].to_string()
    }

    fn req(body: String) -> RegisterRequest {
        RegisterRequest {
            id: None,
            body,
            format: crate::serve::load::ConfigFormat::Yaml,
            description: None,
            tags: Vec::new(),
            launch: false,
            created_by: Some("ci".into()),
        }
    }

    async fn store() -> TemplateStore {
        crate::templates::resolve_store_url("memory").await.unwrap()
    }

    async fn status(s: &TemplateStore, id: &str, v: u32) -> TestStatus {
        version_status(s, id, v).await.unwrap().status
    }

    fn pin(v: u32) -> VersionSelector {
        VersionSelector::Pinned(v)
    }

    #[tokio::test]
    async fn register_runs_the_bundle_and_a_passing_version_launches() {
        let s = store().await;
        let reg = register_tested(&s, req(pipeline("orders", 2))).await.unwrap();
        let t = reg.tests.expect("ran on register");
        assert!(t.passed, "{t:#?}");
        assert_eq!(t.body_sha256, body_sha256(&reg.record.body));
        let names: Vec<&str> = t.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"auto:region=eu") && names.contains(&"rows"), "{names:?}");
        let vt = version_tests(&s, "orders", 1, 5).await.unwrap();
        assert!(vt.has_tests);
        assert_eq!(vt.status.status, TestStatus::Passing);
        let out = crate::templates::launch(&s, "orders", pin(1), None).await.unwrap();
        assert_eq!(out.tests.unwrap().status, TestStatus::Passing);
        let again = crate::templates::launch(&s, "orders", pin(1), None).await.unwrap();
        assert!(again.already_launched && again.tests.is_none());
        let st = crate::templates::template_state(&s, "orders").await.unwrap();
        assert!(st.has_tests);
        assert_eq!(st.stable_tests, Some(TestStatus::Passing));
    }

    #[tokio::test]
    async fn a_failing_version_registers_but_takes_no_label() {
        let s = store().await;
        let reg = register_tested(&s, req(pipeline("orders", 9))).await.unwrap();
        assert!(!reg.tests.unwrap().passed, "registered, and marked failing");
        assert_eq!(status(&s, "orders", 1).await, TestStatus::Failing);
        let err = crate::templates::launch(&s, "orders", pin(1), None).await.unwrap_err();
        let CliError::VersionNotPassing { status, failing, .. } = &err else {
            panic!("{err}")
        };
        assert_eq!(status, "failing");
        assert!(failing.iter().any(|f| f.starts_with("rows:")), "{failing:?}");
        for tag in [VersionChannel::Dev, VersionChannel::Prod] {
            let err = crate::templates::promote(&s, "orders", tag, pin(1)).await.unwrap_err();
            assert!(err.to_string().contains("needs a passing version"), "{err}");
        }
        let mut r = req(pipeline("orders", 9));
        r.launch = true;
        let err = register_tested(&s, r).await.unwrap_err();
        assert!(err.to_string().contains("registered as v2, but not labelled"), "{err}");
        let mut r = req(pipeline("orders", 9));
        r.tags = vec![VersionChannel::Dev];
        assert!(register_tested(&s, r).await.is_err());
        let st = s.template_state("orders").await.unwrap();
        assert_eq!((st.versions.len(), st.stable), (3, None));
        assert!(st.tags.is_empty());
    }

    #[tokio::test]
    async fn a_draft_runs_and_registers_anything_but_labels_only_passing() {
        let s = store().await;
        register(&s, req(untested_body("orders"))).await.unwrap();
        assert_eq!(status(&s, "orders", 1).await, TestStatus::NoTests);
        let err = crate::templates::launch(&s, "orders", pin(1), None).await.unwrap_err();
        assert!(err.to_string().contains("no `tests:` block"), "{err}");
        let m = crate::templates::materialize_for_run(
            &s,
            "orders",
            1,
            &Default::default(),
            &Default::default(),
            &Default::default(),
            crate::templates::Materialize::Local,
        )
        .await
        .expect("a draft runs any version");
        assert!(m.untested.is_none());
        let st = crate::templates::template_state(&s, "orders").await.unwrap();
        assert!(!st.has_tests);
        assert_eq!(st.newest, Some(st.versions[0]), "a draft's newest is the highest");
    }

    #[tokio::test]
    async fn an_empty_tests_block_counts_as_no_tests() {
        let s = store().await;
        register(&s, req(untested_body("orders") + "tests: {}\n")).await.unwrap();
        assert_eq!(status(&s, "orders", 1).await, TestStatus::NoTests);
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        crate::templates::launch(&s, "orders", pin(2), None).await.unwrap();
        let reg = register_tested(&s, req(untested_body("orders") + "tests: {}\n"))
            .await
            .unwrap();
        assert_eq!(reg.tests_inherited_from, Some(2), "after a launch, empty = missing");
    }

    #[tokio::test]
    async fn an_untested_version_is_tested_before_a_label_moves_onto_it() {
        let s = store().await;
        let draft = TemplateDraftFor::new("orders", &pipeline("orders", 2));
        s.template_register(&draft.0).await.unwrap();
        assert_eq!(status(&s, "orders", 1).await, TestStatus::Untested);
        crate::templates::promote(&s, "orders", VersionChannel::Dev, pin(1))
            .await
            .expect("an untested version is run first, then labelled");
        assert_eq!(s.template_test_results("orders", Some(1), 5).await.unwrap().len(), 1);
        assert_eq!(status(&s, "orders", 1).await, TestStatus::Passing);

        let bad = TemplateDraftFor::new("orders", &pipeline("orders", 9));
        s.template_register(&bad.0).await.unwrap();
        let err = crate::templates::launch(&s, "orders", pin(2), None).await.unwrap_err();
        assert!(err.to_string().contains("is failing"), "{err}");
        assert_eq!(s.template_test_results("orders", Some(2), 5).await.unwrap().len(), 1);
        let err = crate::templates::launch(&s, "orders", pin(2), None).await.unwrap_err();
        assert!(err.to_string().contains("is failing"), "{err}");
        assert_eq!(
            s.template_test_results("orders", Some(2), 5).await.unwrap().len(),
            1,
            "a failing version is never re-run by a label move"
        );
    }

    struct TemplateDraftFor(crate::serve::history::templates::TemplateDraft);

    impl TemplateDraftFor {
        fn new(id: &str, body: &str) -> Self {
            Self(crate::serve::history::templates::TemplateDraft {
                id: crate::serve::history::templates::TemplateId::parse(id).unwrap(),
                kind: TemplateKind::Pipeline,
                name: Some(id.into()),
                description: None,
                body: body.into(),
                format: crate::serve::load::ConfigFormat::Yaml,
                params: Default::default(),
                created_by: None,
            })
        }
    }

    #[tokio::test]
    async fn a_launched_template_inherits_tests_into_new_versions() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        let body = format!("# keep this comment\n{}", untested_body("orders"));
        let reg = register_tested(&s, req(body)).await.unwrap();
        assert_eq!(reg.tests_inherited_from, Some(1));
        assert!(reg.record.body.contains("# keep this comment"));
        assert!(reg.record.body.contains("tests:"));
        assert!(reg.tests.unwrap().passed);
        assert_eq!(status(&s, "orders", 2).await, TestStatus::Passing);
        assert!(document_has_tests(&reg.record.body, crate::serve::load::ConfigFormat::Yaml));
    }

    #[tokio::test]
    async fn a_template_launched_without_tests_must_add_them() {
        let s = store().await;
        register(&s, req(untested_body("legacy"))).await.unwrap();
        s.template_launch("legacy", 1, None).await.unwrap();
        let err = register(&s, req(untested_body("legacy"))).await.unwrap_err();
        assert!(matches!(err, CliError::TestsRequired { .. }), "{err}");
        assert!(err.to_string().contains("add a `tests:` block"), "{err}");
        let st = crate::templates::template_state(&s, "legacy").await.unwrap();
        assert_eq!(st.stable, Some(1), "an existing label stays");
        assert_eq!(st.stable_tests, Some(TestStatus::NoTests));
        assert_eq!(st.newest, None, "newest skips a version that is not passing");
        register(&s, req(pipeline("legacy", 2))).await.unwrap();
        crate::templates::launch(&s, "legacy", pin(2), None).await.unwrap();
        let err = crate::templates::rollback(&s, "legacy", None).await.unwrap_err();
        assert!(err.to_string().contains("adds one"), "{err}");
    }

    #[tokio::test]
    async fn a_status_change_never_moves_a_label() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        r.tags = vec![VersionChannel::Prod];
        register_tested(&s, r).await.unwrap();
        let mut failed = s.template_test_results("orders", Some(1), 1).await.unwrap()[0].clone();
        failed.passed = false;
        failed.recorded_at = chrono::Utc::now() + chrono::Duration::seconds(5);
        s.template_record_test(&failed).await.unwrap();
        let st = crate::templates::template_state(&s, "orders").await.unwrap();
        assert_eq!(st.stable, Some(1));
        assert_eq!(st.tags.get("prod"), Some(&1));
        assert_eq!(st.stable_tests, Some(TestStatus::Failing));
        let err = crate::templates::promote(&s, "orders", VersionChannel::Dev, pin(1))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is failing"), "{err}");
        crate::templates::promote(&s, "orders", VersionChannel::Prod, pin(1))
            .await
            .expect("re-pointing a channel where it already is moves nothing");
    }

    #[tokio::test]
    async fn after_a_launch_newest_and_runs_skip_or_refuse_non_passing_versions() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        register(&s, req(pipeline("orders", 9))).await.unwrap();
        let st = crate::templates::template_state(&s, "orders").await.unwrap();
        assert_eq!(st.newest, Some(1));
        assert_eq!(st.test_status.get(&2), Some(&TestStatus::Failing));
        let run = |allow: bool| {
            let s = s.clone();
            async move {
                crate::templates::materialize_for_run(
                    &s,
                    "orders",
                    2,
                    &crate::templates::SinkChoice {
                        allow_untested: allow,
                        ..Default::default()
                    },
                    &Default::default(),
                    &Default::default(),
                    crate::templates::Materialize::Local,
                )
                .await
            }
        };
        let err = run(false).await.unwrap_err();
        assert!(err.to_string().contains("allow_untested"), "{err}");
        let m = run(true).await.unwrap();
        assert_eq!(m.untested, Some(TestStatus::Failing));
        assert!(
            crate::templates::run_gate(&s, "orders", 1, false)
                .await
                .unwrap()
                .is_none()
        );
    }

    fn suite_doc(release: &str) -> String {
        format!(
            "kind: test-suite\nname: conformance\nrelease: {release}\nsuite:\n  auto: {{ defaults_baseline: true }}\n"
        )
    }

    fn requiring(range: &str) -> String {
        pipeline("orders", 2).replace(
            "tests:\n",
            &format!(
                "tests:\n  requires_suites:\n    - {{ name: conformance, version: \"{range}\" }}\n"
            ),
        )
    }

    #[tokio::test]
    async fn shared_suites_resolve_by_range_or_fail_registration_with_a_typed_error() {
        let s = store().await;
        let err = register(&s, req(requiring(">=1.2,<2"))).await.unwrap_err();
        assert!(matches!(err, CliError::UnsatisfiedSuiteRequirement { .. }), "{err}");
        assert!(err.to_string().contains("none registered"), "{err}");

        register(&s, req(suite_doc("1.1.0"))).await.unwrap();
        register(&s, req(suite_doc("1.3.0"))).await.unwrap();
        register(&s, req(suite_doc("2.0.0"))).await.unwrap();
        let dup = register(&s, req(suite_doc("1.3.0"))).await.unwrap_err();
        assert!(dup.to_string().contains("already registered as v2"), "{dup}");

        let rec = register(&s, req(requiring(">=1.2,<2"))).await.unwrap();
        let t = test_version(&s, "orders", rec.version, None).await.unwrap();
        assert!(t.passed, "{t:#?}");
        assert_eq!(t.suites[0].release, "1.3.0");
        assert_eq!(t.suites[0].version, Some(2));
        assert!(t.cases.iter().any(|c| c.source == "suite:conformance@1.3.0"));

        crate::templates::set_version_deprecated(&s, "conformance", 2, None, None, true)
            .await
            .unwrap();
        let t = test_version(&s, "orders", rec.version, None).await.unwrap();
        let e = t.error.expect("1.3.0 retired, nothing else in range");
        assert!(e.contains("1.1.0, 2.0.0"), "{e}");

        assert!(
            run_version(&s, "conformance", 1, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("is a test-suite")
        );
        register(&s, req(pipeline("x", 2))).await.unwrap();
        let wrong = resolve_shared(
            &s,
            &[SuiteRequirement {
                name: "x".into(),
                version: "1".into(),
            }],
        )
        .await
        .unwrap_err();
        assert!(wrong.to_string().contains("not a test-suite"), "{wrong}");
    }

    #[tokio::test]
    async fn a_test_suite_is_never_runnable() {
        let s = store().await;
        register(&s, req(suite_doc("1.0.0"))).await.unwrap();
        let err = crate::templates::materialize_for_run(
            &s,
            "conformance",
            1,
            &Default::default(),
            &Default::default(),
            &Default::default(),
            crate::templates::Materialize::Local,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not runnable"), "{err}");
        let st = s.template_get("conformance", None).await.unwrap().unwrap();
        assert!(bundle_in(&st).unwrap().is_none());
    }

    #[tokio::test]
    async fn a_bundle_change_is_a_new_version_and_a_new_content_hash() {
        let s = store().await;
        let a = register(&s, req(pipeline("orders", 2))).await.unwrap();
        let b = register(&s, req(pipeline("orders", 3))).await.unwrap();
        assert_eq!((a.version, b.version), (1, 2));
        assert_ne!(body_sha256(&a.body), body_sha256(&b.body));
        #[cfg(feature = "templates-sync")]
        {
            let plan_hash = crate::templates::sync::plan::body_hash;
            assert_ne!(plan_hash(&a.body).unwrap(), plan_hash(&b.body).unwrap());
            assert_eq!(
                plan_hash(&a.body).unwrap(),
                plan_hash(&format!("# comment\n{}", a.body)).unwrap(),
                "identical content hashes alike, so a sync registers nothing"
            );
        }
        assert!(!document_has_tests("{", crate::serve::load::ConfigFormat::Yaml));
    }

    #[test]
    fn inherited_tests_are_appended_to_yaml_or_re_emitted() {
        use crate::serve::load::ConfigFormat;
        let tests = serde_json::json!({"suite": {"auto": {"defaults_baseline": true}}});
        let y = with_tests("# c\nname: a\n", ConfigFormat::Yaml, &tests).unwrap();
        assert!(y.starts_with("# c\nname: a\n") && y.contains("defaults_baseline"), "{y}");
        let flow = with_tests("{name: a}", ConfigFormat::Yaml, &tests).unwrap();
        assert!(parse_body(&flow, ConfigFormat::Yaml).unwrap().get("tests").is_some());
        let j = with_tests("{\"name\": \"a\"}", ConfigFormat::Json, &tests).unwrap();
        assert!(parse_body(&j, ConfigFormat::Json).unwrap().get("tests").is_some());
        assert!(with_tests("[1]", ConfigFormat::Json, &tests).is_err());
    }

    fn source(sink_line: &str) -> String {
        format!(
            r#"kind: source-template
name: acme
description: acme
source:
  type: rest
  config: {{ base_url: "https://example.com" }}
streams:
  - name: orders
    source: {{ config: {{ path: /orders }} }}
tests:
  {sink_line}
  suite:
    auto: {{ defaults_baseline: true }}
"#
        )
    }

    const SINK: &str = r#"kind: sink-template
name: files
description: files
sink: { type: file, config: {} }
per_stream: { path: "./out/${stream}.jsonl" }
tests:
  source: acme
  source_select: newest
  suite:
    auto: { defaults_baseline: true }
"#;

    #[tokio::test]
    async fn hub_kind_bundles_compose_with_their_registered_companions() {
        let s = store().await;
        let reg = register_tested(&s, req(SINK.into())).await.unwrap();
        let e = reg.tests.unwrap().error.expect("acme is not registered yet");
        assert!(e.contains("acme"), "{e}");

        let src = register_tested(&s, req(source("sink: files\n  sink_select: newest")))
            .await
            .unwrap();
        let t = src.tests.unwrap();
        assert!(t.passed, "{t:?}");
        assert!(test_version(&s, "files", 1, None).await.unwrap().passed);

        let ops = register_tested(
            &s,
            req("kind: deployment\nname: ops\nstate: { type: memory }\ntests:\n  source: acme\n  source_select: newest\n  sink: files\n  sink_select: newest\n  suite:\n    auto: { defaults_baseline: true }\n".into()),
        )
        .await
        .unwrap();
        assert!(ops.tests.unwrap().passed);

        let with_overlay = register_tested(
            &s,
            req(source("sink: files\n  sink_select: newest\n  overlay: ops\n  overlay_select: newest")),
        )
        .await
        .unwrap();
        assert!(with_overlay.tests.unwrap().passed);
    }

    #[tokio::test]
    async fn version_tests_and_run_version_name_unknown_versions() {
        let s = store().await;
        assert!(matches!(
            version_tests(&s, "nope", 1, 1).await.unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
        assert!(matches!(
            run_version(&s, "nope", 1, None).await.unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        assert!(require_passing(&s, "orders", 1, "launch").await.is_ok());
        assert!(
            registered_release(&s, "orders", &semver::Version::new(1, 0, 0))
                .await
                .unwrap()
                .is_none()
        );
        assert!(inheritable_tests(&s, "orders").await.unwrap().is_some());
    }
}

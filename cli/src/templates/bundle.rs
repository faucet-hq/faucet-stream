//! Running a registered version's test bundle, recording the result as
//! evidence, and the one check every label move goes through (#856).

use serde::Serialize;

use crate::error::{CliError, CliResult};
use crate::hub::TemplateKind;
use crate::serve::history::templates::{
    EvidenceClaim, RESULTS_RETAIN, TemplateRecord, TemplateTestResult, VersionSelector,
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

pub use crate::serve::history::templates::body_sha256;

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
/// suite out of range, a bundle over the size limit) are part of the outcome,
/// so they are recorded as a red run rather than lost.
pub async fn run_version(
    store: &TemplateStore,
    id: &str,
    version: u32,
    filter: Option<&str>,
) -> CliResult<Option<(TemplateRecord, BundleOutcome)>> {
    Ok(run_version_traced(store, id, version, filter)
        .await?
        .map(|(rec, outcome, _)| (rec, outcome)))
}

/// [`run_version`], also naming the companions it ran with (`id@version`).
async fn run_version_traced(
    store: &TemplateStore,
    id: &str,
    version: u32,
    filter: Option<&str>,
) -> CliResult<Option<(TemplateRecord, BundleOutcome, Vec<String>)>> {
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
    let Some(mut bundle) = bundle_in(&rec)? else {
        return Ok(None);
    };
    let limits = crate::template_tests::trust::limits();
    let bytes = parse_body(&rec.body, rec.format)?
        .get("tests")
        .map(|t| serde_json::to_vec(t).map(|v| v.len()).unwrap_or(usize::MAX))
        .unwrap_or(0);
    if bytes > limits.max_bundle_bytes {
        let e = CliError::TestBundleTooLarge {
            id: id.to_string(),
            version,
            bytes,
            limit: limits.max_bundle_bytes,
        };
        return Ok(Some((
            rec,
            BundleOutcome {
                error: Some(e.to_string()),
                ..Default::default()
            },
            Vec::new(),
        )));
    }
    if bundle.is_structural(rec.kind) {
        bundle.suite = crate::template_tests::bundle::structural_suite();
    }
    let (outcome, companions) = match prepare(store, &rec, &bundle).await {
        Ok((companions, shared)) => {
            let target = companions.target(store, &rec);
            let outcome = Box::pin(crate::template_tests::runner::with_run_limits(
                limits,
                run_bundle(&bundle, &target, &shared, filter),
            ))
            .await;
            (outcome, companions.names())
        }
        Err(e) => (
            BundleOutcome {
                error: Some(e.to_string()),
                ..Default::default()
            },
            Vec::new(),
        ),
    };
    Ok(Some((rec, outcome, companions)))
}

/// The resolved companions of a registered subject.
struct Companions {
    source: Option<(String, u32)>,
    sink: Option<(String, u32)>,
    overlay: Option<(String, u32)>,
}

impl Companions {
    fn names(&self) -> Vec<String> {
        [&self.source, &self.sink, &self.overlay]
            .into_iter()
            .flatten()
            .map(|(i, v)| format!("{i}@{v}"))
            .collect()
    }

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
    let shared = if bundle.fully_locked() {
        unlock(bundle)?
    } else {
        resolve_shared(store, &bundle.requires_suites).await?
    };
    Ok((c, shared))
}

/// The shared suites a version locked at registration, as runnable suites.
fn unlock(bundle: &TestBundle) -> CliResult<Vec<SharedSuite>> {
    bundle
        .requires_suites
        .iter()
        .map(|r| {
            let l = bundle.locked(r).expect("fully locked");
            Ok(SharedSuite {
                requirement: r.clone(),
                version: l.registry_version,
                suite: crate::template_tests::bundle::parse_test_suite(l.suite.clone())?,
            })
        })
        .collect()
}

/// Resolve `bundle`'s shared suites against the registry and return them as
/// the lock written into a new version.
pub async fn lock_suites(
    store: &TemplateStore,
    bundle: &TestBundle,
) -> CliResult<Vec<crate::template_tests::bundle::LockedSuite>> {
    resolve_shared(store, &bundle.requires_suites)
        .await?
        .into_iter()
        .map(|s| {
            Ok(crate::template_tests::bundle::LockedSuite {
                name: s.requirement.name.clone(),
                range: s.requirement.version.clone(),
                release: s.suite.release.clone(),
                registry_version: s.version,
                suite: serde_json::to_value(&s.suite)
                    .map_err(|e| CliError::Internal(format!("locking a shared suite: {e}")))?,
            })
        })
        .collect()
}

/// Run `id`@`version`'s bundle and record the result on the version — the
/// only way evidence is produced.
pub async fn test_version(
    store: &TemplateStore,
    id: &str,
    version: u32,
    recorded_by: Option<&str>,
) -> CliResult<TemplateTestResult> {
    let (rec, outcome, companions) = Box::pin(run_version_traced(store, id, version, None))
        .await?
        .ok_or_else(|| no_bundle(id, version))?;
    let mut result = TemplateTestResult::new(
        id,
        version,
        body_sha256(&rec.body),
        outcome,
        recorded_by.map(str::to_string),
    );
    result.provenance = crate::template_tests::result::Provenance {
        build_features: crate::template_tests::trust::build_features(),
        plugins: crate::registry::plugin_connectors(),
        companions,
        instance: crate::template_tests::trust::server_policy().map(|p| p.instance),
    };
    store
        .template_record_test(&result)
        .await
        .map_err(|e| crate::templates::store::registry_err("template test result write", e))?;
    crate::templates::metrics::record_test(result.passed);
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

/// A version's test status from what is stored about it (`results` are its
/// own, newest first; `undecodable` when some of them could not be read):
/// whether it has tests, its status, and whether it is eligible for `newest`.
/// A `tests:` block or results that cannot be read make it `unknown` instead
/// of failing every reader of the template.
pub fn judge(
    rec: &TemplateRecord,
    results: &[TemplateTestResult],
    undecodable: bool,
) -> (bool, VersionStatus, bool) {
    let has_tests = match bundle_in(rec) {
        Ok(b) => b.is_some(),
        Err(e) => {
            return (
                true,
                crate::template_tests::gate::unknown(format!(
                    "v{}'s `tests:` block cannot be read: {e}",
                    rec.version
                )),
                false,
            );
        }
    };
    if undecodable {
        return (
            has_tests,
            crate::template_tests::gate::unknown(format!(
                "v{}'s recorded test results cannot be read",
                rec.version
            )),
            false,
        );
    }
    let sha = body_sha256(&rec.body);
    let status = status_of(&rec.id, rec.version, has_tests, &sha, results);
    let eligible = has_tests && crate::template_tests::gate::ever_passed(results, &sha);
    (has_tests, status, eligible)
}

/// `id`@`version`'s test status and up to `limit` recent runs.
pub async fn version_tests(
    store: &TemplateStore,
    id: &str,
    version: u32,
    limit: usize,
) -> CliResult<VersionTests> {
    let rec = record(store, id, version).await?;
    let (all, broken) = store
        .template_test_results_lenient(id)
        .await
        .map_err(read_err)?;
    let results: Vec<TemplateTestResult> =
        all.into_iter().filter(|r| r.version == version).collect();
    let (has_tests, status, _) = judge(&rec, &results, broken.contains(&version));
    Ok(VersionTests {
        version,
        has_tests,
        status,
        results: results.into_iter().take(limit.max(1)).collect(),
    })
}

/// `id`@`version`'s test status.
pub async fn version_status(
    store: &TemplateStore,
    id: &str,
    version: u32,
) -> CliResult<VersionStatus> {
    Ok(version_tests(store, id, version, RESULTS_RETAIN)
        .await?
        .status)
}

/// Refuse `action` unless `id`@`version` is `passing` — the one check every
/// label move (launch, rollback, a channel) goes through. An `untested`
/// version has its bundle run first, through the normal record path, and is
/// then decided on that result; a `failing` one is never re-run here.
///
/// Returns the evidence the move relies on; the store re-checks it in the
/// same transaction as the move.
pub async fn require_passing(
    store: &TemplateStore,
    id: &str,
    version: u32,
    action: &str,
    by: Option<&str>,
) -> CliResult<(VersionStatus, EvidenceClaim)> {
    let rec = record(store, id, version).await?;
    let mut st = version_status(store, id, version).await?;
    if st.status == TestStatus::Untested {
        Box::pin(test_version(store, id, version, by)).await?;
        st = version_status(store, id, version).await?;
    }
    match (&st.evidence, st.status.can_hold_label()) {
        (Some(e), true) => {
            let claim = EvidenceClaim {
                result_id: e.clone(),
                body_sha256: body_sha256(&rec.body),
            };
            Ok((st, claim))
        }
        _ => {
            crate::templates::metrics::record_refusal(match st.status {
                TestStatus::Failing => "failing",
                TestStatus::Untested => "untested",
                TestStatus::NoTests => "no_tests",
                TestStatus::Unknown => "unknown",
                TestStatus::Passing => "stale_evidence",
            });
            Err(st.refusal(id, version, action))
        }
    }
}

/// A label move the store refused because its evidence changed underneath it.
pub fn stale_evidence(id: &str, version: u32, action: &str, why: String) -> CliError {
    crate::templates::metrics::record_refusal("stale_evidence");
    CliError::VersionNotPassing {
        id: id.to_string(),
        version,
        status: TestStatus::Untested,
        action: action.to_string(),
        reason: format!("{why} — check the version's test results and try again"),
        failing: Vec::new(),
    }
}

/// Fill in each version's test status, whether the template's newest
/// version has tests and the stable version's status, from two reads for the
/// whole template. Once the template has been launched, `newest` is the
/// highest live version whose exact body has ever passed
/// ([`ever_passed`](crate::template_tests::gate::ever_passed)) — sticky, so a
/// later failure or a stale release line never moves it.
pub async fn annotate(
    store: &TemplateStore,
    id: &str,
    state: &mut crate::serve::history::templates::TemplateState,
) -> CliResult<()> {
    let records = store.template_records(id).await.map_err(read_err)?;
    let (results, broken) = store
        .template_test_results_lenient(id)
        .await
        .map_err(read_err)?;
    let mut eligible = std::collections::BTreeSet::new();
    for rec in &records {
        let own: Vec<TemplateTestResult> = results
            .iter()
            .filter(|r| r.version == rec.version)
            .cloned()
            .collect();
        let (_, status, ever) = judge(rec, &own, broken.contains(&rec.version));
        if ever {
            eligible.insert(rec.version);
        }
        state.test_status.insert(rec.version, status.status);
    }
    for t in store.template_tag_records(id).await.map_err(read_err)? {
        if let Some(e) = t.evidence {
            state.evidence.insert(t.tag, e);
        }
    }
    state.has_tests = state
        .versions
        .first()
        .and_then(|v| state.test_status.get(v))
        .is_some_and(|s| *s != TestStatus::NoTests);
    state.stable_tests = state
        .stable
        .and_then(|v| state.test_status.get(&v).copied());
    if state.stable.is_some() {
        // Sticky: the highest live version whose body ever passed.
        let retired = |v: u32| state.version_deprecation(v).is_some();
        state.newest = state
            .versions
            .iter()
            .copied()
            .find(|v| !retired(*v) && eligible.contains(v));
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

/// `body` without the block of top-level key `key` (the line `key:` at
/// column 0 and every indented, blank or comment line after it).
fn without_top_level(body: &str, key: &str) -> String {
    let mut out = Vec::new();
    let mut skipping = false;
    for line in body.lines() {
        let top = !line.is_empty() && !line.starts_with([' ', '\t', '#']);
        if top {
            skipping = line
                .strip_prefix(key)
                .is_some_and(|rest| rest.trim_start().starts_with(':'));
        }
        if !skipping {
            out.push(line);
        }
    }
    out.join("\n")
}

/// `body` with its top-level `tests:` block set to `tests`. A YAML document keeps its
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
            let appended = format!("{}\n{block}", without_top_level(body, "tests").trim_end());
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
        && doc
            .get("kind")
            .and_then(|k| k.as_str())
            .is_none_or(|k| k == "pipeline");
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
        let reg = register_tested(&s, req(pipeline("orders", 2)))
            .await
            .unwrap();
        let t = reg.tests.expect("ran on register");
        assert!(t.passed, "{t:#?}");
        assert_eq!(t.body_sha256, body_sha256(&reg.record.body));
        let names: Vec<&str> = t.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(
            names.contains(&"auto:region=eu") && names.contains(&"rows"),
            "{names:?}"
        );
        let vt = version_tests(&s, "orders", 1, 5).await.unwrap();
        assert!(vt.has_tests);
        assert_eq!(vt.status.status, TestStatus::Passing);
        let out = crate::templates::launch(&s, "orders", pin(1), None)
            .await
            .unwrap();
        assert_eq!(out.tests.unwrap().status, TestStatus::Passing);
        let again = crate::templates::launch(&s, "orders", pin(1), None)
            .await
            .unwrap();
        assert!(again.already_launched && again.tests.is_none());
        let st = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert!(st.has_tests);
        assert_eq!(st.stable_tests, Some(TestStatus::Passing));
    }

    #[tokio::test]
    async fn a_failing_version_registers_but_takes_no_label() {
        let s = store().await;
        let reg = register_tested(&s, req(pipeline("orders", 9)))
            .await
            .unwrap();
        assert!(!reg.tests.unwrap().passed, "registered, and marked failing");
        assert_eq!(status(&s, "orders", 1).await, TestStatus::Failing);
        let err = crate::templates::launch(&s, "orders", pin(1), None)
            .await
            .unwrap_err();
        let CliError::VersionNotPassing {
            status, failing, ..
        } = &err
        else {
            panic!("{err}")
        };
        assert_eq!(*status, TestStatus::Failing);
        assert!(
            failing.iter().any(|f| f.starts_with("rows:")),
            "{failing:?}"
        );
        for tag in [VersionChannel::Dev, VersionChannel::Prod] {
            let err = crate::templates::promote(&s, "orders", tag, pin(1))
                .await
                .unwrap_err();
            assert!(err.to_string().contains("needs a passing version"), "{err}");
        }
        let mut r = req(pipeline("orders", 9));
        r.launch = true;
        let err = register_tested(&s, r).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("registered as v2, but not labelled"),
            "{err}"
        );
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
        let err = crate::templates::launch(&s, "orders", pin(1), None)
            .await
            .unwrap_err();
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
        let st = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert!(!st.has_tests);
        assert_eq!(
            st.newest,
            Some(st.versions[0]),
            "a draft's newest is the highest"
        );
    }

    #[tokio::test]
    async fn an_empty_tests_block_counts_as_no_tests() {
        let s = store().await;
        register(&s, req(untested_body("orders") + "tests: {}\n"))
            .await
            .unwrap();
        assert_eq!(status(&s, "orders", 1).await, TestStatus::NoTests);
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        crate::templates::launch(&s, "orders", pin(2), None)
            .await
            .unwrap();
        let reg = register_tested(&s, req(untested_body("orders") + "tests: {}\n"))
            .await
            .unwrap();
        assert_eq!(
            reg.tests_inherited_from,
            Some(2),
            "after a launch, empty = missing"
        );
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
        assert_eq!(
            s.template_test_results("orders", Some(1), 5)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(status(&s, "orders", 1).await, TestStatus::Passing);

        let bad = TemplateDraftFor::new("orders", &pipeline("orders", 9));
        s.template_register(&bad.0).await.unwrap();
        let err = crate::templates::launch(&s, "orders", pin(2), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is failing"), "{err}");
        assert_eq!(
            s.template_test_results("orders", Some(2), 5)
                .await
                .unwrap()
                .len(),
            1
        );
        let err = crate::templates::launch(&s, "orders", pin(2), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is failing"), "{err}");
        assert_eq!(
            s.template_test_results("orders", Some(2), 5)
                .await
                .unwrap()
                .len(),
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
        assert!(document_has_tests(
            &reg.record.body,
            crate::serve::load::ConfigFormat::Yaml
        ));
    }

    #[tokio::test]
    async fn a_template_launched_without_tests_must_add_them() {
        let s = store().await;
        register(&s, req(untested_body("legacy"))).await.unwrap();
        s.template_launch("legacy", 1, None, None).await.unwrap();
        let err = register(&s, req(untested_body("legacy")))
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::TestsRequired { .. }), "{err}");
        assert!(err.to_string().contains("add a `tests:` block"), "{err}");
        let st = crate::templates::template_state(&s, "legacy")
            .await
            .unwrap();
        assert_eq!(st.stable, Some(1), "an existing label stays");
        assert_eq!(st.stable_tests, Some(TestStatus::NoTests));
        assert_eq!(
            st.newest, None,
            "newest skips a version that is not passing"
        );
        register(&s, req(pipeline("legacy", 2))).await.unwrap();
        crate::templates::launch(&s, "legacy", pin(2), None)
            .await
            .unwrap();
        let err = crate::templates::rollback(&s, "legacy", None)
            .await
            .unwrap_err();
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
        let st = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
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
        let st = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
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
        assert!(
            matches!(err, CliError::UnsatisfiedSuiteRequirement { .. }),
            "{err}"
        );
        assert!(err.to_string().contains("none registered"), "{err}");

        register(&s, req(suite_doc("1.1.0"))).await.unwrap();
        register(&s, req(suite_doc("1.3.0"))).await.unwrap();
        register(&s, req(suite_doc("2.0.0"))).await.unwrap();
        let dup = register(&s, req(suite_doc("1.3.0"))).await.unwrap_err();
        assert!(
            dup.to_string().contains("already registered as v2"),
            "{dup}"
        );

        let rec = register(&s, req(requiring(">=1.2,<2"))).await.unwrap();
        let t = test_version(&s, "orders", rec.version, None).await.unwrap();
        assert!(t.passed, "{t:#?}");
        assert_eq!(t.suites[0].release, "1.3.0");
        assert_eq!(t.suites[0].version, Some(2));
        assert!(
            t.cases
                .iter()
                .any(|c| c.source == "suite:conformance@1.3.0")
        );

        // The version holds its own copy of 1.3.0: a newer release, a retired
        // release or a deleted suite changes nothing about what it tests.
        let stored = s
            .template_get("orders", Some(rec.version))
            .await
            .unwrap()
            .unwrap();
        assert!(stored.body.contains("locked_suites"), "{}", stored.body);
        register(&s, req(suite_doc("1.4.0"))).await.unwrap();
        crate::templates::set_version_deprecated(&s, "conformance", 2, None, None, true)
            .await
            .unwrap();
        let t = test_version(&s, "orders", rec.version, None).await.unwrap();
        assert!(t.passed, "{t:#?}");
        assert_eq!(t.suites[0].release, "1.3.0");

        assert!(
            run_version(&s, "conformance", 1, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("is a test-suite")
        );
        s.template_delete("conformance", None).await.unwrap();
        let t = test_version(&s, "orders", rec.version, None).await.unwrap();
        assert!(t.passed && t.suites[0].release == "1.3.0", "{t:#?}");
        // A new registration resolves against what is registered now.
        let err = register(&s, req(requiring(">=1.2,<2"))).await.unwrap_err();
        assert!(
            matches!(err, CliError::UnsatisfiedSuiteRequirement { .. }),
            "{err}"
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
        assert!(!document_has_tests(
            "{",
            crate::serve::load::ConfigFormat::Yaml
        ));
    }

    #[test]
    fn inherited_tests_are_appended_to_yaml_or_re_emitted() {
        use crate::serve::load::ConfigFormat;
        let tests = serde_json::json!({"suite": {"auto": {"defaults_baseline": true}}});
        let y = with_tests("# c\nname: a\n", ConfigFormat::Yaml, &tests).unwrap();
        assert!(
            y.starts_with("# c\nname: a\n") && y.contains("defaults_baseline"),
            "{y}"
        );
        let flow = with_tests("{name: a}", ConfigFormat::Yaml, &tests).unwrap();
        assert!(
            parse_body(&flow, ConfigFormat::Yaml)
                .unwrap()
                .get("tests")
                .is_some()
        );
        let j = with_tests("{\"name\": \"a\"}", ConfigFormat::Json, &tests).unwrap();
        assert!(
            parse_body(&j, ConfigFormat::Json)
                .unwrap()
                .get("tests")
                .is_some()
        );
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
        let e = reg
            .tests
            .unwrap()
            .error
            .expect("acme is not registered yet");
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
            req(source(
                "sink: files\n  sink_select: newest\n  overlay: ops\n  overlay_select: newest",
            )),
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
        assert!(
            require_passing(&s, "orders", 1, "launch", None)
                .await
                .is_ok()
        );
        assert!(
            registered_release(&s, "orders", &semver::Version::new(1, 0, 0))
                .await
                .unwrap()
                .is_none()
        );
        assert!(inheritable_tests(&s, "orders").await.unwrap().is_some());
    }

    /// Record a result for `id`@`v` as if a run under `faucet` had produced it.
    async fn inject(s: &TemplateStore, id: &str, v: u32, passed: bool, faucet: &str) -> String {
        let rec = s.template_get(id, Some(v)).await.unwrap().unwrap();
        let mut r = TemplateTestResult::new(
            id,
            v,
            body_sha256(&rec.body),
            crate::template_tests::result::BundleOutcome {
                cases: vec![crate::template_tests::result::CaseResult {
                    name: "c".into(),
                    source: "bundle".into(),
                    origin: "explicit".into(),
                    passed,
                    attempts: 1,
                    duration_ms: 0,
                    failure: (!passed).then(|| "boom".to_string()),
                }],
                ..Default::default()
            },
            None,
        );
        r.faucet_version = faucet.into();
        r.recorded_at += chrono::Duration::seconds(1);
        s.template_record_test(&r).await.unwrap();
        r.result_id
    }

    fn next_minor() -> String {
        let (major, minor) = crate::template_tests::result::release_line(
            crate::template_tests::result::faucet_version(),
        )
        .unwrap();
        format!("{major}.{}.0", minor + 1)
    }

    #[tokio::test]
    async fn a_label_authorizes_runs_through_it_even_after_its_evidence_goes_bad() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        crate::templates::promote(&s, "orders", VersionChannel::Prod, pin(1))
            .await
            .unwrap();
        register_tested(&s, req(pipeline("orders", 9)))
            .await
            .unwrap();
        // v1's newest evidence now fails (a flake, or an upgrade's re-run).
        inject(
            &s,
            "orders",
            1,
            false,
            crate::template_tests::result::faucet_version(),
        )
        .await;
        assert_eq!(status(&s, "orders", 1).await, TestStatus::Failing);
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!((state.stable, state.tags.get("prod")), (Some(1), Some(&1)));
        // Through `stable`, `prod`, or pinned while it holds them: it runs.
        assert_eq!(
            crate::templates::run_gate(&s, "orders", 1, false)
                .await
                .unwrap(),
            None
        );
        // v2 holds no label and fails: pinned, it needs `allow_untested`.
        let err = crate::templates::run_gate(&s, "orders", 2, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("allow_untested"), "{err}");
        assert_eq!(
            crate::templates::run_gate(&s, "orders", 2, true)
                .await
                .unwrap(),
            Some(TestStatus::Failing)
        );
        // But no new label moves onto the failing v1.
        let err = crate::templates::promote(&s, "orders", VersionChannel::Dev, pin(1))
            .await
            .unwrap_err();
        assert!(matches!(err, CliError::VersionNotPassing { .. }), "{err}");
    }

    #[tokio::test]
    async fn stale_evidence_keeps_newest_but_moving_a_label_retests() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        // v2 registered straight into the store, its only evidence from
        // another faucet release line.
        let body = pipeline("orders", 2).replace("rows", "rows2");
        let draft = crate::serve::history::templates::TemplateDraft {
            id: crate::serve::history::templates::TemplateId::parse("orders").unwrap(),
            kind: TemplateKind::Pipeline,
            name: Some("orders".into()),
            description: None,
            body,
            format: crate::serve::load::ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
        };
        let v = s.template_register(&draft).await.unwrap().version;
        inject(&s, "orders", v, true, &next_minor()).await;
        assert_eq!(status(&s, "orders", v).await, TestStatus::Untested);
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!(state.newest, Some(v), "a stale pass keeps `newest`");
        assert_eq!(
            crate::templates::run_gate(&s, "orders", v, false)
                .await
                .unwrap(),
            None
        );
        // A label move onto it re-runs its tests first, under this release.
        let out = crate::templates::launch(&s, "orders", pin(v), None)
            .await
            .unwrap();
        assert_eq!(out.tests.unwrap().status, TestStatus::Passing);
    }

    #[tokio::test]
    async fn every_label_move_records_its_evidence_and_a_stale_claim_is_refused() {
        let s = store().await;
        register_tested(&s, req(pipeline("orders", 2)))
            .await
            .unwrap();
        let out = crate::templates::launch(&s, "orders", pin(1), Some("ann"))
            .await
            .unwrap();
        let evidence = out.tests.unwrap().evidence.expect("launch evidence");
        let log = s.template_launches("orders").await.unwrap();
        assert_eq!(log[0].evidence.as_deref(), Some(evidence.as_str()));
        crate::templates::promote(&s, "orders", VersionChannel::Canary, pin(1))
            .await
            .unwrap();
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!(state.evidence.get("stable"), Some(&evidence));
        assert_eq!(state.evidence.get("canary"), Some(&evidence));
        // A claim a newer result superseded is refused inside the write.
        let rec = s.template_get("orders", Some(1)).await.unwrap().unwrap();
        let claim = EvidenceClaim {
            result_id: evidence.clone(),
            body_sha256: body_sha256(&rec.body),
        };
        let newer = inject(
            &s,
            "orders",
            1,
            true,
            crate::template_tests::result::faucet_version(),
        )
        .await;
        let err = s
            .template_set_tag("orders", "dev", 1, Some(&claim))
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::serve::history::HistoryError::StaleEvidence(_)),
            "{err}"
        );
        let fresh = EvidenceClaim {
            result_id: newer,
            ..claim
        };
        s.template_set_tag("orders", "dev", 1, Some(&fresh))
            .await
            .unwrap();
        let fail = inject(
            &s,
            "orders",
            1,
            false,
            crate::template_tests::result::faucet_version(),
        )
        .await;
        let failing = EvidenceClaim {
            result_id: fail,
            body_sha256: body_sha256(&rec.body),
        };
        assert!(
            s.template_set_tag("orders", "test", 1, Some(&failing))
                .await
                .is_err()
        );
        // The store's stale-evidence refusal reads as a refused label move.
        let e = crate::templates::bundle::stale_evidence("orders", 1, "launch", "changed".into());
        assert!(e.to_string().contains("try again"), "{e}");
    }

    #[tokio::test]
    async fn results_carry_their_provenance() {
        let s = store().await;
        let reg = register_tested(&s, req(pipeline("orders", 2)))
            .await
            .unwrap();
        let t = reg.tests.unwrap();
        assert_eq!(t.schema, crate::template_tests::result::RESULT_SCHEMA);
        assert!(!t.result_id.is_empty());
        assert!(!t.provenance.build_features.is_empty());
        assert_eq!(t.recorded_by.as_deref(), Some("ci"));
        assert_eq!(
            t.faucet_version,
            crate::template_tests::result::faucet_version()
        );
    }

    #[tokio::test]
    async fn a_deployment_bundle_is_structural() {
        let s = store().await;
        register(&s, req(SINK.into())).await.unwrap();
        register(&s, req(source("sink: files\n  sink_select: newest")))
            .await
            .unwrap();
        let ops = register_tested(
            &s,
            req("kind: deployment\nname: ops\nstate: { type: memory }\ntests:\n  source: acme\n  source_select: newest\n  sink: files\n  sink_select: newest\n".into()),
        )
        .await
        .unwrap();
        let t = ops.tests.expect("a structural bundle runs");
        assert!(t.passed, "{t:#?}");
        assert!(!t.cases.is_empty());
        assert_eq!(
            t.provenance.companions,
            vec!["acme@1".to_string(), "files@1".to_string()]
        );
        // Without both companions there is nothing to compose: no tests.
        let bare = register_tested(
            &s,
            req(
                "kind: deployment\nname: ops2\nstate: { type: memory }\ntests:\n  source: acme\n"
                    .into(),
            ),
        )
        .await;
        assert!(bare.is_err() || bare.unwrap().tests.is_none());
    }

    #[tokio::test]
    async fn an_oversized_bundle_records_a_failure_with_the_reason() {
        let s = store().await;
        let rows: Vec<String> = (0..60_000)
            .map(|i| format!("{{\"id\": {i}, \"pad\": \"xxxxxxxx\"}}"))
            .collect();
        let body = pipeline("big", 60_000).replace(
            "input: [{ \"id\": 1 }, { \"id\": 2 }]",
            &format!("input: [{}]", rows.join(", ")),
        );
        let reg = register_tested(&s, req(body)).await.unwrap();
        let t = reg.tests.unwrap();
        assert!(!t.passed);
        assert!(
            t.error.as_deref().unwrap_or("").contains("byte"),
            "{:?}",
            t.error
        );
    }

    #[tokio::test]
    async fn deleting_a_template_makes_the_next_registration_a_draft_without_evidence() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        s.template_delete("orders", None).await.unwrap();
        assert!(
            s.template_test_results("orders", None, 50)
                .await
                .unwrap()
                .is_empty()
        );
        let again = register_tested(&s, req(untested_body("orders")))
            .await
            .unwrap();
        assert!(again.tests.is_none() && again.tests_inherited_from.is_none());
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert!(state.stable.is_none(), "a draft again");
        assert_eq!(
            crate::templates::run_gate(&s, "orders", 1, false)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn revalidation_retests_label_holders_and_rollback_targets_once_per_release() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        let mut r = req(pipeline("orders", 2).replace("rows", "rows-b"));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        crate::templates::promote(&s, "orders", VersionChannel::Prod, pin(2))
            .await
            .unwrap();
        register_tested(&s, req(pipeline("orders", 9)))
            .await
            .unwrap();
        let before = s
            .template_test_results("orders", None, 100)
            .await
            .unwrap()
            .len();
        let report = crate::templates::revalidate::revalidate_if_new_release(&s)
            .await
            .unwrap()
            .expect("never revalidated");
        let mut seen: Vec<(u32, Vec<String>)> = report
            .versions
            .iter()
            .map(|v| (v.version, v.labels.clone()))
            .collect();
        seen.sort();
        assert_eq!(
            seen,
            vec![
                (1, vec!["previous".to_string()]),
                (
                    2,
                    vec![
                        "stable".to_string(),
                        "newest".to_string(),
                        "prod".to_string()
                    ]
                )
            ]
        );
        assert_eq!(report.failed(), 0);
        assert!(report.render_human().contains("orders@2"));
        assert_eq!(
            s.template_test_results("orders", None, 100)
                .await
                .unwrap()
                .len(),
            before + 2
        );
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!(state.stable, Some(2), "labels never move");
        assert!(
            crate::templates::revalidate::revalidate_if_new_release(&s)
                .await
                .unwrap()
                .is_none(),
            "once per release line"
        );
        assert_eq!(
            s.meta_get(crate::templates::revalidate::META_KEY)
                .await
                .unwrap(),
            Some(crate::template_tests::result::running_line())
        );
    }

    #[tokio::test]
    async fn a_required_secret_is_missing_unless_a_case_names_it_and_its_value_is_redacted() {
        let s = store().await;
        let secret = pipeline("vaulted", 2).replace(
            "params:\n",
            "params:\n  token: { type: string, required: true, secret: true }\n",
        );
        // The fixture leaves the required secret unset: it fails by name, as
        // a real trigger would (no placeholder without a default to protect).
        let reg = register_tested(&s, req(secret.clone())).await.unwrap();
        let t = reg.tests.unwrap();
        assert!(!t.passed);
        assert!(
            t.cases
                .iter()
                .any(|c| c.name == "rows" && c.failure.as_deref().unwrap_or("").contains("token")),
            "{t:#?}"
        );
        // Named by the fixture, the value is used and never recorded.
        let named = secret.replacen(
            "    - name: rows\n",
            "    - name: rows\n      params: { token: hunter2-856 }\n",
            1,
        );
        let reg = register_tested(&s, req(named)).await.unwrap();
        let t = reg.tests.unwrap();
        assert!(t.passed, "{t:#?}");
        assert!(
            !serde_json::to_string(&t).unwrap().contains("hunter2-856"),
            "the recorded result never carries the value"
        );
    }

    #[tokio::test]
    async fn the_run_budget_and_the_case_timeout_fail_cases_with_the_reason() {
        use crate::template_tests::trust::{DEFAULT_LIMITS, TestLimits};
        let s = store().await;
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        let rec = s.template_get("orders", Some(1)).await.unwrap().unwrap();
        let bundle = bundle_in(&rec).unwrap().unwrap();
        let target = crate::template_tests::Target::Registered {
            store: &s,
            id: "orders",
            version: 1,
            sink: None,
            overlay: None,
        };
        let spent = TestLimits {
            run_budget: std::time::Duration::ZERO,
            ..DEFAULT_LIMITS
        };
        let out = crate::template_tests::runner::with_run_limits(
            spent,
            run_bundle(&bundle, &target, &[], None),
        )
        .await;
        assert!(!out.passed());
        assert!(
            out.cases
                .iter()
                .all(|c| c.failure.as_deref().unwrap_or("").contains("time budget"))
        );
        let tight = TestLimits {
            case_timeout: std::time::Duration::ZERO,
            ..DEFAULT_LIMITS
        };
        let out = crate::template_tests::runner::with_run_limits(
            tight,
            run_bundle(&bundle, &target, &[], None),
        )
        .await;
        assert!(out.cases.iter().all(|c| {
            c.failure
                .as_deref()
                .unwrap_or("")
                .contains("per-case limit")
        }));
    }

    #[test]
    fn an_existing_tests_block_is_replaced_in_place() {
        use crate::serve::load::ConfigFormat;
        let body = "# keep me\nname: a\ntests:\n  # old\n  suite: { auto: { defaults_baseline: true } }\nafter: 1\n";
        let tests = serde_json::json!({"suite": {"auto": {"enum_coverage": true}}});
        let out = with_tests(body, ConfigFormat::Yaml, &tests).unwrap();
        assert!(
            out.starts_with("# keep me\nname: a\n") && out.contains("after: 1"),
            "{out}"
        );
        assert!(
            !out.contains("# old") && out.contains("enum_coverage"),
            "{out}"
        );
    }

    async fn launched(s: &TemplateStore, body: String) {
        let mut r = req(body);
        r.launch = true;
        register_tested(s, r).await.unwrap();
    }

    #[tokio::test]
    async fn newest_is_sticky_through_a_flake_a_failed_revalidation_and_a_stale_line() {
        let s = store().await;
        launched(&s, pipeline("orders", 2)).await;
        register_tested(&s, req(pipeline("orders", 2).replace("rows", "rows-v2")))
            .await
            .unwrap();
        let newest = |s: TemplateStore| async move {
            crate::templates::template_state(&s, "orders")
                .await
                .unwrap()
                .newest
        };
        assert_eq!(newest(s.clone()).await, Some(2));
        // A flake: v2's newest result fails.
        inject(
            &s,
            "orders",
            2,
            false,
            crate::template_tests::result::faucet_version(),
        )
        .await;
        assert_eq!(status(&s, "orders", 2).await, TestStatus::Failing);
        assert_eq!(
            newest(s.clone()).await,
            Some(2),
            "a later failure never moves it"
        );
        // A stale line: its only evidence is from another release.
        inject(&s, "orders", 2, false, &next_minor()).await;
        assert_eq!(newest(s.clone()).await, Some(2));
        // A newer version that never passed does not take it.
        register_tested(&s, req(pipeline("orders", 9)))
            .await
            .unwrap();
        assert_eq!(newest(s.clone()).await, Some(2));
        // Deprecating the holder does.
        crate::templates::set_version_deprecated(&s, "orders", 2, None, None, true)
            .await
            .unwrap();
        assert_eq!(newest(s.clone()).await, Some(1));
    }

    #[tokio::test]
    async fn a_failed_revalidation_keeps_newest_and_keeps_the_marker_unwritten_on_error() {
        let s = store().await;
        // v1 passed once (under an older faucet, say) and was launched; its
        // tests fail when re-run now.
        register_tested(&s, req(pipeline("orders", 9)))
            .await
            .unwrap();
        inject(&s, "orders", 1, true, &next_minor()).await;
        s.template_launch("orders", 1, None, None).await.unwrap();
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!((state.stable, state.newest), (Some(1), Some(1)));
        // An `error` outcome (here: a version whose stored body does not
        // parse its tests) leaves the marker for the next start.
        let mut broken = req(pipeline("broken", 2));
        broken.launch = true;
        register_tested(&s, broken).await.unwrap();
        let rec = s.template_get("broken", Some(1)).await.unwrap().unwrap();
        s.template_delete("broken", Some(1)).await.unwrap();
        let mut draft = crate::serve::history::templates::TemplateDraft {
            id: crate::serve::history::templates::TemplateId::parse("broken").unwrap(),
            kind: TemplateKind::Pipeline,
            name: Some("broken".into()),
            description: None,
            body: rec
                .body
                .replace("records_written: 2", "records_written: not-a-number"),
            format: crate::serve::load::ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
        };
        let v = s.template_register(&draft).await.unwrap().version;
        s.template_launch("broken", v, None, None).await.unwrap();
        draft.created_by = None;
        let r = crate::templates::revalidate::revalidate_if_new_release(&s)
            .await
            .unwrap()
            .expect("due");
        assert!(r.versions.iter().any(|v| v.outcome == "error"), "{r:?}");
        let marker = s
            .meta_get(crate::templates::revalidate::META_KEY)
            .await
            .unwrap();
        assert_ne!(
            marker.as_deref(),
            Some(crate::template_tests::result::running_line().as_str()),
            "an error leaves revalidation due"
        );
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!(
            state.newest,
            Some(1),
            "a failed revalidation never moves newest"
        );
        assert_eq!(state.test_status.get(&1), Some(&TestStatus::Failing));
        assert_eq!(
            crate::templates::template_state(&s, "broken")
                .await
                .unwrap()
                .test_status
                .get(&v),
            Some(&TestStatus::Unknown)
        );
    }

    #[tokio::test]
    async fn the_marker_is_claimed_once_and_an_older_line_never_takes_it_back() {
        use crate::templates::revalidate::{META_KEY, is_newer, revalidate_if_new_release};
        let s = store().await;
        let line = crate::template_tests::result::running_line();
        let (a, b) = tokio::join!(revalidate_if_new_release(&s), revalidate_if_new_release(&s));
        assert_eq!(
            [a.unwrap().is_some(), b.unwrap().is_some()]
                .iter()
                .filter(|x| **x)
                .count(),
            1,
            "one claim"
        );
        assert_eq!(s.meta_get(META_KEY).await.unwrap(), Some(line.clone()));
        s.meta_put(META_KEY, "999.0").await.unwrap();
        assert!(revalidate_if_new_release(&s).await.unwrap().is_none());
        assert_eq!(
            s.meta_get(META_KEY).await.unwrap().as_deref(),
            Some("999.0")
        );
        assert!(is_newer("1.14", Some("1.13")) && !is_newer("1.13", Some("1.14")));
        assert!(is_newer("1.13", Some("")) && is_newer("1.13", None));
        assert!(
            !s.meta_compare_and_set(META_KEY, Some("1.0"), "2.0")
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_dead_instances_claim_expires_and_a_live_one_holds() {
        use crate::templates::revalidate::{CLAIM_KEY, META_KEY, revalidate_if_new_release};
        let s = store().await;
        let line = crate::template_tests::result::running_line();
        let live = format!("{line}@{}", chrono::Utc::now().timestamp() + 3600);
        s.meta_put(CLAIM_KEY, &live).await.unwrap();
        assert!(revalidate_if_new_release(&s).await.unwrap().is_none());
        assert_eq!(s.meta_get(META_KEY).await.unwrap(), None);
        s.meta_put(CLAIM_KEY, &format!("{line}@1")).await.unwrap();
        assert!(revalidate_if_new_release(&s).await.unwrap().is_some());
        assert_eq!(s.meta_get(META_KEY).await.unwrap(), Some(line));
        assert_eq!(s.meta_get(CLAIM_KEY).await.unwrap().as_deref(), Some(""));
    }

    #[tokio::test]
    async fn pruning_keeps_the_newest_holder() {
        use crate::serve::history::templates::{VERSION_RETAIN, sticky_newest};
        let s = store().await;
        launched(&s, pipeline("orders", 2)).await;
        register_tested(&s, req(pipeline("orders", 2).replace("rows", "rows-v2")))
            .await
            .unwrap();
        for i in 0..(VERSION_RETAIN + 2) {
            let body = pipeline("orders", 9).replace("rows", &format!("rows-x{i}"));
            register(&s, req(body)).await.unwrap();
        }
        let versions = s.template_versions("orders").await.unwrap();
        assert!(
            versions.contains(&2),
            "the newest holder survives the prune: {versions:?}"
        );
        let (results, _) = s.template_test_results_lenient("orders").await.unwrap();
        assert_eq!(
            sticky_newest(
                &s.template_records("orders").await.unwrap(),
                &results,
                &Default::default()
            ),
            Some(2)
        );
    }

    #[tokio::test]
    async fn pruning_keeps_a_version_a_pending_launch_request_names() {
        use crate::serve::history::templates::VERSION_RETAIN;
        let s = store().await;
        launched(&s, pipeline("orders", 2)).await;
        register(
            &s,
            req(pipeline("orders", 9).replace("rows", "rows-pinned")),
        )
        .await
        .unwrap();
        let now = chrono::Utc::now().to_rfc3339();
        let change: crate::serve::changes::ChangeRequest =
            serde_json::from_value(serde_json::json!({
                "id": "chg-pin", "kind": "template_launch", "status": "pending",
                "requester": "bob", "requester_role": "operator",
                "payload": {"id": "orders", "version": 2}, "required_approvals": 1,
                "approvals": [], "created_at": now, "updated_at": now, "expires_at": now
            }))
            .unwrap();
        s.change_upsert(&change).await.unwrap();
        for i in 0..(VERSION_RETAIN + 2) {
            let body = pipeline("orders", 9).replace("rows", &format!("rows-y{i}"));
            register(&s, req(body)).await.unwrap();
        }
        let versions = s.template_versions("orders").await.unwrap();
        assert!(
            versions.contains(&2),
            "the pinned version survives: {versions:?}"
        );
    }

    #[tokio::test]
    async fn an_unreadable_bundle_or_results_make_one_version_unknown_not_the_template() {
        let s = store().await;
        launched(&s, pipeline("orders", 2)).await;
        let rec = s.template_get("orders", Some(1)).await.unwrap().unwrap();
        let mut bad = rec.clone();
        bad.body = rec
            .body
            .replace("records_written: 2", "records_written: [nope]");
        let (has, st, eligible) = judge(&bad, &[], false);
        assert!(has && !eligible);
        assert_eq!(st.status, TestStatus::Unknown);
        assert!(st.reason.unwrap().contains("cannot be read"));
        let (_, st, eligible) = judge(&rec, &[], true);
        assert_eq!(st.status, TestStatus::Unknown);
        assert!(!eligible && !st.status.can_hold_label());
        // A version stored with an unreadable bundle: the state still reads,
        // runs through `stable` still go ahead, and no label moves onto it.
        let draft = crate::serve::history::templates::TemplateDraft {
            id: crate::serve::history::templates::TemplateId::parse("orders").unwrap(),
            kind: TemplateKind::Pipeline,
            name: Some("orders".into()),
            description: None,
            body: bad.body.clone(),
            format: crate::serve::load::ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
        };
        let v = s.template_register(&draft).await.unwrap().version;
        let state = crate::templates::template_state(&s, "orders")
            .await
            .unwrap();
        assert_eq!(state.test_status.get(&v), Some(&TestStatus::Unknown));
        assert_eq!(state.newest, Some(1));
        assert_eq!(
            crate::templates::run_gate(&s, "orders", 1, false)
                .await
                .unwrap(),
            None
        );
        assert!(
            crate::templates::launch(&s, "orders", pin(v), None)
                .await
                .is_err()
        );
        assert!(
            crate::templates::promote(&s, "orders", VersionChannel::Dev, pin(v))
                .await
                .is_err()
        );
        let vt = version_tests(&s, "orders", v, 5).await.unwrap();
        assert_eq!(vt.status.status, TestStatus::Unknown);
    }

    #[tokio::test]
    async fn a_result_for_a_deleted_version_is_refused() {
        let s = store().await;
        register_tested(&s, req(pipeline("orders", 2)))
            .await
            .unwrap();
        let rec = s.template_get("orders", Some(1)).await.unwrap().unwrap();
        s.template_delete("orders", Some(1)).await.unwrap();
        let r = TemplateTestResult::new(
            "orders",
            1,
            body_sha256(&rec.body),
            Default::default(),
            None,
        );
        let err = s.template_record_test(&r).await.unwrap_err();
        assert!(err.to_string().contains("no longer exists"), "{err}");
    }

    #[tokio::test]
    async fn registry_written_test_fields_are_refused_in_a_submitted_document() {
        let s = store().await;
        for extra in ["  inherited_from: 1\n", "  locked_suites: []\n"] {
            let body = pipeline("orders", 2).replace("tests:\n", &format!("tests:\n{extra}"));
            let err = register(&s, req(body)).await.unwrap_err();
            assert!(err.to_string().contains("written by the registry"), "{err}");
        }
    }

    #[tokio::test]
    async fn a_bundle_that_generates_no_cases_is_no_tests() {
        let s = store().await;
        // `enum_coverage` over a template with no closed-set params.
        let body = pipeline("orders", 2)
            .replace("    values: [us, eu]\n", "")
            .replace(
                "    auto: { enum_coverage: true, defaults_baseline: true }\n  fixtures:\n    - name: rows\n      input: [{ \"id\": 1 }, { \"id\": 2 }]\n      expect: { records_written: 2 }\n",
                "    auto: { enum_coverage: true }\n",
            );
        assert!(!body.contains("fixtures"), "{body}");
        let reg = register_tested(&s, req(body)).await.unwrap();
        assert!(reg.tests.unwrap().cases.is_empty());
        assert_eq!(status(&s, "orders", 1).await, TestStatus::NoTests);
        let err = crate::templates::launch(&s, "orders", pin(1), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("generates no cases"), "{err}");
    }

    #[tokio::test]
    async fn a_launched_template_refuses_a_version_whose_bundle_generates_no_cases() {
        let s = store().await;
        launched(&s, pipeline("orders", 2)).await;
        let caseless = pipeline("orders", 2)
            .replace("    values: [us, eu]\n", "")
            .replace(
                "    auto: { enum_coverage: true, defaults_baseline: true }\n  fixtures:\n    - name: rows\n      input: [{ \"id\": 1 }, { \"id\": 2 }]\n      expect: { records_written: 2 }\n",
                "    auto: { enum_coverage: true }\n",
            );
        let err = register_tested(&s, req(caseless)).await.unwrap_err();
        assert!(err.to_string().contains("generates no cases"), "{err}");
        assert_eq!(s.template_versions("orders").await.unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn a_launch_that_lands_mid_registration_refuses_the_untested_version() {
        let s = store().await;
        let rec = register(&s, req(untested_body("orders"))).await.unwrap();
        // The launch lands after the registration's read of the launch log.
        s.template_launch("orders", 1, None, None).await.unwrap();
        let err = crate::templates::store::refuse_if_launched_meanwhile(&s, &rec, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("register it again"), "{err}");
        assert!(s.template_get("orders", Some(1)).await.unwrap().is_none());
        crate::templates::store::refuse_if_launched_meanwhile(&s, &rec, true)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn an_auto_run_records_who_asked() {
        let s = store().await;
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        // Make it untested: only stale evidence.
        let rec = s.template_get("orders", Some(1)).await.unwrap().unwrap();
        s.template_delete("orders", Some(1)).await.unwrap();
        let draft = crate::serve::history::templates::TemplateDraft {
            id: crate::serve::history::templates::TemplateId::parse("orders").unwrap(),
            kind: TemplateKind::Pipeline,
            name: Some("orders".into()),
            description: None,
            body: rec.body,
            format: crate::serve::load::ConfigFormat::Yaml,
            params: Default::default(),
            created_by: None,
        };
        let v = s.template_register(&draft).await.unwrap().version;
        crate::templates::launch(&s, "orders", pin(v), Some("ann"))
            .await
            .unwrap();
        let r = s.template_test_results("orders", Some(v), 1).await.unwrap();
        assert_eq!(r[0].recorded_by.as_deref(), Some("ann"));
    }

    #[tokio::test]
    async fn a_secret_params_default_never_resolves_in_a_test_run() {
        let s = store().await;
        let body = pipeline("vaulted", 2)
            .replace(
                "params:\n",
                "params:\n  token: { type: string, secret: true, default: \"${env:FAUCET_856_NEVER_SET}\" }\n",
            )
            .replace("path: /rows", "path: \"/rows/${param.token}\"");
        let reg = register_tested(&s, req(body)).await.unwrap();
        let t = reg.tests.unwrap();
        assert!(t.passed, "the default was never resolved: {t:#?}");
    }
}

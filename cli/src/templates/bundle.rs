//! Running a registered version's test bundle, recording the result, and the
//! launch gate over the registry (#856).

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::error::{CliError, CliResult};
use crate::hub::TemplateKind;
use crate::serve::history::templates::{TemplateRecord, TemplateTestResult, VersionSelector};
use crate::template_tests::bundle::{SuiteRequirement, TestBundle, TestSuiteTemplate, bundle_of};
use crate::template_tests::gate::{GateVerdict, LaunchGate, evaluate};
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

/// A version's standing against the gate, with its recent runs.
#[derive(Debug, Clone, Serialize)]
pub struct VersionTests {
    pub version: u32,
    pub has_tests: bool,
    pub gate: GateVerdict,
    /// Recent runs, newest first.
    pub results: Vec<TemplateTestResult>,
}

/// Evaluate `gate` for `id`@`version` against what the registry holds.
pub async fn version_tests(
    store: &TemplateStore,
    id: &str,
    version: u32,
    gate: &LaunchGate,
    limit: usize,
) -> CliResult<VersionTests> {
    let rec = store
        .template_get(id, Some(version))
        .await
        .map_err(read_err)?
        .ok_or_else(|| CliError::UnknownPipelineTemplate {
            id: id.to_string(),
            version: Some(version),
        })?;
    let has_tests = bundle_in(&rec)?.is_some();
    let results = store
        .template_test_results(id, Some(version), limit.max(1))
        .await
        .map_err(read_err)?;
    let verdict = evaluate(
        gate,
        id,
        version,
        has_tests,
        &body_sha256(&rec.body),
        &results,
    );
    Ok(VersionTests {
        version,
        has_tests,
        gate: verdict,
        results,
    })
}

/// The gate's decision for launching `id`@`version`; `Err` when refused.
pub async fn check_gate(
    store: &TemplateStore,
    id: &str,
    version: u32,
    gate: &LaunchGate,
) -> CliResult<GateVerdict> {
    let t = version_tests(
        store,
        id,
        version,
        gate,
        crate::serve::history::templates::RESULTS_RETAIN,
    )
    .await?;
    if t.gate.allowed {
        Ok(t.gate)
    } else {
        Err(t.gate.refusal(id, version))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template_tests::{GateStatus, LaunchGate};
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
    type: jsonl
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

    fn req(body: String) -> RegisterRequest {
        RegisterRequest {
            id: None,
            body,
            format: crate::serve::load::ConfigFormat::Yaml,
            description: None,
            tags: Vec::new(),
            launch: false,
            created_by: Some("ci".into()),
            test: false,
            gate: LaunchGate::default(),
        }
    }

    async fn store() -> TemplateStore {
        crate::templates::resolve_store_url("memory").await.unwrap()
    }

    fn required() -> LaunchGate {
        LaunchGate::new(true)
    }

    #[tokio::test]
    async fn a_passing_bundle_is_recorded_and_unlocks_a_gated_launch() {
        let s = store().await;
        let mut r = req(pipeline("orders", 2));
        r.test = true;
        let reg = register_tested(&s, r).await.unwrap();
        let t = reg.tests.expect("ran");
        assert!(t.passed, "{t:#?}");
        assert_eq!(t.body_sha256, body_sha256(&reg.record.body));
        let names: Vec<&str> = t.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(
            names.contains(&"auto:region=eu") && names.contains(&"rows"),
            "{names:?}"
        );
        let vt = version_tests(&s, "orders", 1, &required(), 5)
            .await
            .unwrap();
        assert!(vt.has_tests && vt.gate.allowed);
        assert_eq!(vt.gate.status, GateStatus::Passed);
        let out = crate::templates::launch_gated(
            &s,
            "orders",
            VersionSelector::Pinned(1),
            None,
            &required(),
        )
        .await
        .unwrap();
        assert_eq!(out.tests.unwrap().status, GateStatus::Passed);
        let again = crate::templates::launch_gated(
            &s,
            "orders",
            VersionSelector::Pinned(1),
            None,
            &required(),
        )
        .await
        .unwrap();
        assert!(again.already_launched && again.tests.is_none());
    }

    #[tokio::test]
    async fn a_failing_or_missing_result_refuses_the_launch_until_an_admin_overrides() {
        let s = store().await;
        register(&s, req(pipeline("orders", 9))).await.unwrap();
        let err = crate::templates::launch_gated(
            &s,
            "orders",
            VersionSelector::Pinned(1),
            None,
            &required(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CliError::LaunchGated { .. }));
        assert!(err.to_string().contains("never run"), "{err}");

        let t = test_version(&s, "orders", 1, Some("ci")).await.unwrap();
        assert!(!t.passed);
        let err = crate::templates::launch_gated(
            &s,
            "orders",
            VersionSelector::Pinned(1),
            None,
            &required(),
        )
        .await
        .unwrap_err();
        let CliError::LaunchGated { failing, .. } = &err else {
            panic!("{err}")
        };
        assert!(
            failing.iter().any(|f| f.starts_with("rows:")),
            "{failing:?}"
        );

        let gate = required().with_skip(Some("hotfix".into())).unwrap();
        let out = crate::templates::launch_gated(
            &s,
            "orders",
            VersionSelector::Pinned(1),
            Some("root"),
            &gate,
        )
        .await
        .unwrap();
        assert_eq!(out.tests.unwrap().skipped.as_deref(), Some("hotfix"));
        let log = s.template_launches("orders").await.unwrap();
        assert_eq!(log[0].tests_skipped.as_deref(), Some("hotfix"));

        crate::templates::launch(&s, "orders", VersionSelector::Pinned(1), None)
            .await
            .expect("an ungated launch keeps today's behaviour");
    }

    #[tokio::test]
    async fn a_version_without_tests_launches_only_while_tests_are_not_required() {
        let s = store().await;
        let body = pipeline("plain", 2);
        let body = body[..body.find("tests:").unwrap()].to_string();
        register(&s, req(body.clone())).await.unwrap();
        let v = version_tests(&s, "plain", 1, &required(), 5).await.unwrap();
        assert_eq!(v.gate.status, GateStatus::NoTests);
        assert!(!v.gate.allowed);
        assert!(
            crate::templates::launch_gated(
                &s,
                "plain",
                VersionSelector::Pinned(1),
                None,
                &required()
            )
            .await
            .is_err()
        );
        crate::templates::launch_gated(
            &s,
            "plain",
            VersionSelector::Pinned(1),
            None,
            &LaunchGate::new(false),
        )
        .await
        .unwrap();
        assert!(
            test_version(&s, "plain", 1, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("no `tests:`")
        );
        let mut r = req(body.clone());
        r.test = true;
        assert!(
            register(&s, r)
                .await
                .unwrap_err()
                .to_string()
                .contains("no `tests:` block")
        );
        let mut r = req(body);
        r.launch = true;
        r.gate = required();
        assert!(
            register(&s, r)
                .await
                .unwrap_err()
                .to_string()
                .contains("cannot be registered and launched")
        );
        assert_eq!(s.template_state("plain").await.unwrap().versions, vec![1]);
    }

    #[tokio::test]
    async fn register_and_launch_under_the_gate_tests_first_and_reports_a_refusal() {
        let s = store().await;
        let mut r = req(pipeline("orders", 9));
        r.launch = true;
        r.gate = required();
        let err = register_tested(&s, r).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("registered as v1 but not launched"),
            "{err}"
        );
        let st = s.template_state("orders").await.unwrap();
        assert_eq!((st.versions.clone(), st.stable), (vec![1], None));
        assert_eq!(
            s.template_test_results("orders", Some(1), 5)
                .await
                .unwrap()
                .len(),
            1
        );

        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        r.gate = required();
        let reg = register_tested(&s, r).await.unwrap();
        assert!(reg.tests.unwrap().passed);
        assert_eq!(reg.launch.unwrap().version, 2);
    }

    #[tokio::test]
    async fn rollback_goes_through_the_same_gate() {
        let s = store().await;
        let mut r = req(pipeline("orders", 9));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        let mut r = req(pipeline("orders", 2));
        r.launch = true;
        register_tested(&s, r).await.unwrap();
        let err = crate::templates::rollback_gated(&s, "orders", None, &required())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("v1"), "{err}");
        crate::templates::rollback_gated(
            &s,
            "orders",
            None,
            &required().with_skip(Some("incident".into())).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(s.template_state("orders").await.unwrap().stable, Some(1));
    }

    #[tokio::test]
    async fn a_result_from_another_body_or_major_is_stale() {
        let s = store().await;
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        let mut t = test_version(&s, "orders", 1, None).await.unwrap();
        t.faucet_version = "0.0.1".into();
        t.recorded_at = chrono::Utc::now() + chrono::Duration::seconds(5);
        s.template_record_test(&t).await.unwrap();
        let v = version_tests(&s, "orders", 1, &required(), 5)
            .await
            .unwrap();
        assert_eq!(
            v.gate.status,
            GateStatus::Passed,
            "an older valid pass still counts"
        );

        s.template_delete("orders", None).await.unwrap();
        assert!(
            s.template_test_results("orders", None, 5)
                .await
                .unwrap()
                .is_empty()
        );
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        t.faucet_version = crate::template_tests::result::faucet_version().into();
        t.body_sha256 = "stale-body".into();
        s.template_record_test(&t).await.unwrap();
        let v = version_tests(&s, "orders", 1, &required(), 5)
            .await
            .unwrap();
        assert_eq!(v.gate.status, GateStatus::Stale);
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
        let not_suite = register(
            &s,
            req(pipeline("conf2", 2).replace("name: conf2", "name: x")),
        )
        .await;
        assert!(not_suite.is_ok());
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
        assert!(document_has_tests(
            &a.body,
            crate::serve::load::ConfigFormat::Yaml
        ));
        assert!(!document_has_tests(
            "{",
            crate::serve::load::ConfigFormat::Yaml
        ));
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
sink: { type: jsonl, config: {} }
per_stream: { path: "./out/${stream}.jsonl" }
write_mode_aliases: { overwrite: append }
tests:
  source: acme
  suite:
    auto: { defaults_baseline: true }
"#;

    #[tokio::test]
    async fn hub_kind_bundles_compose_with_their_registered_companions() {
        let s = store().await;
        register(&s, req(SINK.into())).await.unwrap();
        crate::templates::launch(&s, "files", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        let t = test_version(&s, "files", 1, None).await.unwrap();
        let e = t.error.expect("acme is not registered yet");
        assert!(e.contains("acme"), "{e}");

        register(&s, req(source("sink: files"))).await.unwrap();
        crate::templates::launch(&s, "acme", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        assert!(test_version(&s, "acme", 1, None).await.unwrap().passed);
        assert!(test_version(&s, "files", 1, None).await.unwrap().passed);

        register(
            &s,
            req("kind: deployment\nname: ops\nstate: { type: memory }\ntests:\n  source: acme\n  sink: files\n  suite:\n    auto: { defaults_baseline: true }\n".into()),
        )
        .await
        .unwrap();
        assert!(test_version(&s, "ops", 1, None).await.unwrap().passed);

        register(&s, req(source("sink: files\n  overlay: ops")))
            .await
            .unwrap();
        crate::templates::launch(&s, "ops", VersionSelector::Pinned(1), None)
            .await
            .unwrap();
        assert!(test_version(&s, "acme", 2, None).await.unwrap().passed);
    }

    #[tokio::test]
    async fn check_gate_and_version_tests_name_unknown_versions() {
        let s = store().await;
        assert!(matches!(
            version_tests(&s, "nope", 1, &required(), 1)
                .await
                .unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
        assert!(matches!(
            run_version(&s, "nope", 1, None).await.unwrap_err(),
            CliError::UnknownPipelineTemplate { .. }
        ));
        register(&s, req(pipeline("orders", 2))).await.unwrap();
        assert!(
            check_gate(&s, "orders", 1, &LaunchGate::new(false))
                .await
                .unwrap()
                .allowed
        );
        assert!(
            registered_release(&s, "orders", &semver::Version::new(1, 0, 0))
                .await
                .unwrap()
                .is_none()
        );
    }
}

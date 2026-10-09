//! Running a whole test bundle (#856): the version's own suite and fixtures,
//! then every shared suite it requires, against one target.

use super::bundle::{SuiteRequirement, TestBundle, TestSuiteTemplate, parse_range};
use super::result::{BundleOutcome, CaseResult, ResolvedSuiteRef};
use super::runner::{SuiteOutcome, Target, run_fixtures, run_suite};
use crate::error::{CliError, CliResult};

/// A shared suite picked for one requirement.
#[derive(Debug, Clone)]
pub struct SharedSuite {
    pub requirement: SuiteRequirement,
    /// Registry version, when resolved from a registry.
    pub version: Option<u32>,
    pub suite: TestSuiteTemplate,
}

impl SharedSuite {
    fn source(&self) -> String {
        format!("suite:{}@{}", self.suite.id(), self.suite.release)
    }
}

/// Pick, for `req`, the highest candidate whose release satisfies its range.
/// `candidates` are `(registry version, suite)`. Fails with
/// [`CliError::UnsatisfiedSuiteRequirement`] naming the suite, the range and
/// what is available.
pub fn pick(
    req: &SuiteRequirement,
    candidates: Vec<(Option<u32>, TestSuiteTemplate)>,
) -> CliResult<SharedSuite> {
    let range = parse_range(req)?;
    let mut available = Vec::new();
    let mut best: Option<(semver::Version, Option<u32>, TestSuiteTemplate)> = None;
    for (version, suite) in candidates {
        let Ok(release) = suite.release_version() else {
            continue;
        };
        available.push(release.to_string());
        if range.matches(&release) && best.as_ref().is_none_or(|(b, _, _)| release > *b) {
            best = Some((release, version, suite));
        }
    }
    match best {
        Some((_, version, suite)) => Ok(SharedSuite {
            requirement: req.clone(),
            version,
            suite,
        }),
        None => {
            available.sort();
            available.dedup();
            Err(CliError::UnsatisfiedSuiteRequirement {
                suite: req.name.clone(),
                range: req.version.clone(),
                available,
            })
        }
    }
}

fn collect(out: &mut BundleOutcome, part: SuiteOutcome, source: &str) {
    out.cases.extend(
        part.cases
            .iter()
            .map(|c| CaseResult::from_outcome(c, source)),
    );
}

/// Run `bundle` (and `shared`) against `target`. Never fails: a setup error
/// lands in [`BundleOutcome::error`], so it is recorded like any red case.
pub async fn run_bundle(
    bundle: &TestBundle,
    target: &Target<'_>,
    shared: &[SharedSuite],
    filter: Option<&str>,
) -> BundleOutcome {
    let started = std::time::Instant::now();
    let mut out = BundleOutcome {
        suites: shared
            .iter()
            .map(|s| ResolvedSuiteRef {
                name: s.requirement.name.clone(),
                range: s.requirement.version.clone(),
                version: s.version,
                release: s.suite.release.clone(),
            })
            .collect(),
        ..Default::default()
    };
    let mut errors = Vec::new();
    if !bundle.suite.is_empty() {
        match run_suite(&bundle.suite, target, filter, None, "").await {
            Ok(part) => collect(&mut out, part, "bundle"),
            Err(e) => errors.push(format!("suite: {e}")),
        }
    }
    collect(
        &mut out,
        run_fixtures(&bundle.fixtures, target, filter, "").await,
        "bundle",
    );
    for s in shared {
        let source = s.source();
        let prefix = format!("{}@{}:", s.suite.id(), s.suite.release);
        if !s.suite.suite.is_empty() {
            match run_suite(&s.suite.suite, target, filter, None, &prefix).await {
                Ok(part) => collect(&mut out, part, &source),
                Err(e) => errors.push(format!("{source}: {e}")),
            }
        }
        collect(
            &mut out,
            run_fixtures(&s.suite.fixtures, target, filter, &prefix).await,
            &source,
        );
    }
    if !errors.is_empty() {
        out.error = Some(errors.join("; "));
    } else if out.cases.is_empty() {
        out.error = Some(match filter {
            Some(f) => format!("no case matches the filter '{f}'"),
            None => "the bundle produced no cases".into(),
        });
    }
    out.duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::template_tests::bundle::parse_test_suite;

    fn suite(name: &str, release: &str) -> TestSuiteTemplate {
        parse_test_suite(serde_json::json!({
            "kind": "test-suite",
            "name": name,
            "release": release,
            "suite": {"auto": {"defaults_baseline": true}},
            "fixtures": [{"name": "rows", "pipeline": {"transforms": []}, "input": [{"a": 1}], "expect": {"records_written": 1}}],
        }))
        .unwrap()
    }

    fn req(range: &str) -> SuiteRequirement {
        SuiteRequirement {
            name: "conf".into(),
            version: range.into(),
        }
    }

    #[test]
    fn the_highest_satisfying_release_wins() {
        let picked = pick(
            &req(">=1.2,<2"),
            vec![
                (Some(1), suite("conf", "1.1.0")),
                (Some(2), suite("conf", "1.4.0")),
                (Some(3), suite("conf", "1.2.5")),
                (Some(4), suite("conf", "2.0.0")),
            ],
        )
        .unwrap();
        assert_eq!(picked.suite.release, "1.4.0");
        assert_eq!(picked.version, Some(2));
    }

    #[test]
    fn an_unsatisfiable_range_names_the_suite_the_range_and_what_exists() {
        let err = pick(
            &req(">=3"),
            vec![(None, suite("conf", "1.0.0")), (None, suite("conf", "2.1.0"))],
        )
        .unwrap_err();
        assert!(matches!(err, CliError::UnsatisfiedSuiteRequirement { .. }));
        let msg = err.to_string();
        assert!(msg.contains("conf") && msg.contains(">=3"), "{msg}");
        assert!(msg.contains("1.0.0, 2.1.0"), "{msg}");
        let none = pick(&req(">=1"), Vec::new()).unwrap_err().to_string();
        assert!(none.contains("none registered"), "{none}");
        assert!(pick(&req("nope"), Vec::new()).is_err());
    }

    fn pipeline_doc() -> String {
        r#"kind: pipeline
version: 1
name: bundle-fixture
params:
  out:
    type: string
    default: ./out.jsonl
pipeline:
  source:
    type: rest
    config:
      base_url: "https://example.com"
      path: "/rows"
  sink:
    type: jsonl
    config:
      path: "${param.out}"
  transforms:
    - type: set
      config:
        values:
          tagged: true
"#
        .to_string()
    }

    fn doc_target() -> Target<'static> {
        Target::Document {
            body: pipeline_doc(),
            sink_body: None,
            overlay: None,
        }
    }

    #[tokio::test]
    async fn a_bundle_runs_its_suite_fixtures_and_shared_suites() {
        let bundle: TestBundle = serde_yaml::from_str(
            r#"
suite:
  auto: { defaults_baseline: true }
fixtures:
  - name: through-the-template
    input: [{ "id": 1 }]
    expect: { records: [{ "id": 1, "tagged": true }] }
  - name: inline
    pipeline: { transforms: [] }
    input: [{ "id": 1 }, { "id": 2 }]
    expect: { records_written: 2 }
"#,
        )
        .unwrap();
        let shared = vec![pick(&req(">=1"), vec![(Some(7), suite("conf", "1.0.0"))]).unwrap()];
        let out = run_bundle(&bundle, &doc_target(), &shared, None).await;
        assert!(out.passed(), "{:#?}", out);
        let names: Vec<&str> = out.cases.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"auto:defaults"), "{names:?}");
        assert!(names.contains(&"through-the-template"), "{names:?}");
        assert!(names.contains(&"conf@1.0.0:auto:defaults"), "{names:?}");
        assert!(names.contains(&"conf@1.0.0:rows"), "{names:?}");
        assert!(out.cases.iter().any(|c| c.source == "suite:conf@1.0.0"));
        assert_eq!(out.suites[0].version, Some(7));
    }

    #[tokio::test]
    async fn a_failing_fixture_is_retried_then_reported() {
        let bundle: TestBundle = serde_yaml::from_str(
            r#"
fixtures:
  - name: wrong
    retries: 2
    input: [{ "id": 1 }]
    expect: { records_written: 5 }
"#,
        )
        .unwrap();
        let out = run_bundle(&bundle, &doc_target(), &[], None).await;
        assert!(!out.passed());
        assert_eq!(out.cases[0].attempts, 3);
        assert!(out.cases[0].failure.is_some());
    }

    #[tokio::test]
    async fn a_setup_error_and_an_empty_filter_are_recorded_failures() {
        let bundle: TestBundle = serde_yaml::from_str(
            "suite:\n  combine:\n    params: { nope: [1] }\n    exclude: [{ other: 2 }]\n",
        )
        .unwrap();
        let out = run_bundle(&bundle, &doc_target(), &[], None).await;
        assert!(out.error.as_deref().unwrap().contains("suite:"), "{out:?}");
        assert!(!out.passed());

        let ok: TestBundle =
            serde_yaml::from_str("suite: { auto: { defaults_baseline: true } }\n").unwrap();
        let out = run_bundle(&ok, &doc_target(), &[], Some("missing")).await;
        assert!(out.error.unwrap().contains("no case matches"));
        let empty: TestBundle =
            serde_yaml::from_str("requires_suites: [{ name: c, version: '1' }]\n").unwrap();
        let out = run_bundle(&empty, &doc_target(), &[], None).await;
        assert!(out.error.unwrap().contains("produced no cases"));
    }

    #[tokio::test]
    async fn a_shared_suite_that_cannot_generate_is_an_error_naming_it() {
        let mut broken = suite("conf", "1.0.0");
        broken.suite.combine = Some(crate::template_tests::spec::Combine {
            params: [("x".to_string(), vec![serde_json::json!(1)])].into(),
            exclude: vec![[("y".to_string(), serde_json::json!(1))].into()],
            pairwise: false,
            expect: Default::default(),
            retries: 0,
        });
        let shared = vec![SharedSuite {
            requirement: req("1"),
            version: None,
            suite: broken,
        }];
        let bundle: TestBundle = serde_yaml::from_str("requires_suites: [{ name: conf, version: '1' }]\n").unwrap();
        let out = run_bundle(&bundle, &doc_target(), &shared, None).await;
        assert!(out.error.unwrap().contains("suite:conf@1.0.0"));
    }
}

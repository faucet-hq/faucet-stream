//! A template version's **test bundle** (#856): the `tests:` block a template
//! document carries, and the `kind: test-suite` documents it can require by
//! version range.
//!
//! The bundle lives in the document, so it is stored verbatim with the version
//! and covered by the version's content hash: changing a test is a new version,
//! exactly like changing the config. Everything in it runs offline — inline
//! fixtures only, no live endpoint — so a passing result can gate a launch.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

use super::spec::{Suite, check_retries};
use crate::error::{CliError, CliResult};
use crate::hub::TemplateKind;
use crate::pipeline_test::spec::{Expectation, InlinePipeline, InputSpec};

/// The `tests:` block of a template document.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TestBundle {
    /// For a `sink-template` or `deployment`: the source template the bundle
    /// composes with (a registered id, or a hub id when run from a hub).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Version of `source`: a number or a channel. Default `stable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_select: Option<String>,
    /// For a `source-template` or `deployment`: the sink template to compose
    /// with.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    /// Version of `sink`. Default `stable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_select: Option<String>,
    /// For a `source-template`: a deployment overlay applied over the
    /// composition.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay: Option<String>,
    /// Version of `overlay`. Default `stable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_select: Option<String>,
    /// Parameter-space cases — the `faucet template test` grammar (`cases`,
    /// `combine`, `auto`, `behavioral`).
    #[serde(default, skip_serializing_if = "Suite::is_empty")]
    pub suite: Suite,
    /// Fixture cases — the `faucet test` grammar, run through this template's
    /// own config (or an inline `pipeline:`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixtures: Vec<Fixture>,
    /// Shared suites (`kind: test-suite`) that run as part of this bundle,
    /// each picked by a semver range over its `release:`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires_suites: Vec<SuiteRequirement>,
}

/// A shared suite named by version range.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuiteRequirement {
    /// The suite's id (`name`, or `owner/name`).
    pub name: String,
    /// A semver range over the suite's `release:` (`">=1.2,<2"`).
    pub version: String,
}

/// One fixture case: records through the pipeline, then `faucet test`'s
/// expectations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// Case name, unique within the bundle's fixtures.
    pub name: String,
    /// Inline transforms / quality / contract / masking. Omitted = the
    /// template's own materialized config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<InlinePipeline>,
    /// Which row of the template's config to run (when it has several).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    /// Param values bound before the template is materialized.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, Value>,
    /// Inline records. A path is refused: the version stores the document
    /// only, so a fixture file would not travel with it.
    pub input: InputSpec,
    /// Records per page (`0` = one page).
    #[serde(default)]
    pub page_size: usize,
    /// Fixed `${now.*}` clock (RFC 3339 or a date).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clock: Option<String>,
    pub expect: Expectation,
    /// Extra attempts before the case is reported failed.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

impl PartialEq for InlinePipeline {
    fn eq(&self, other: &Self) -> bool {
        serde_json::to_value(self).ok() == serde_json::to_value(other).ok()
    }
}

impl PartialEq for Expectation {
    fn eq(&self, other: &Self) -> bool {
        serde_json::to_value(self).ok() == serde_json::to_value(other).ok()
    }
}

impl PartialEq for InputSpec {
    fn eq(&self, other: &Self) -> bool {
        serde_json::to_value(self).ok() == serde_json::to_value(other).ok()
    }
}

/// `kind: test-suite` — a suite many templates share, versioned by `release`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TestSuiteTemplate {
    /// Must be `test-suite`.
    pub kind: TemplateKind,
    /// The faucet versions this suite is written for (a semver requirement).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requires_faucet: Option<String>,
    /// Document version; must be `1`.
    #[serde(default = "default_version")]
    pub version: u32,
    /// Short name; with `owner`, the id is `owner/name`.
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// The suite's own semver release (`1.2.0`) — what `requires_suites`
    /// ranges select. One registered version per release.
    pub release: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Parameter-space cases run against the requiring template.
    #[serde(default, skip_serializing_if = "Suite::is_empty")]
    pub suite: Suite,
    /// Fixture cases run against the requiring template.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fixtures: Vec<Fixture>,
}

fn default_version() -> u32 {
    1
}

impl TestSuiteTemplate {
    /// `owner/name`, or `name`.
    pub fn id(&self) -> String {
        crate::hub::spec::hub_id(self.owner.as_deref(), &self.name)
    }

    /// The parsed `release`.
    pub fn release_version(&self) -> CliResult<semver::Version> {
        semver::Version::parse(self.release.trim()).map_err(|e| {
            CliError::Config(format!(
                "test-suite '{}': `release: {}` is not a semver version (e.g. 1.2.0): {e}",
                self.name, self.release
            ))
        })
    }

    pub fn validate(&self) -> CliResult<()> {
        if self.kind != TemplateKind::TestSuite {
            return Err(CliError::Config(format!(
                "'{}' is a {}, not a test-suite",
                self.name, self.kind
            )));
        }
        if self.version != 1 {
            return Err(CliError::Config(format!(
                "test-suite '{}': unsupported version {} (expected 1)",
                self.name, self.version
            )));
        }
        crate::hub::spec::check_slug("test-suite name", &self.name, true)?;
        if let Some(o) = &self.owner {
            crate::hub::spec::check_slug("test-suite owner", o, true)?;
        }
        self.release_version()?;
        let what = format!("test-suite '{}'", self.name);
        if self.suite.is_empty() && self.fixtures.is_empty() {
            return Err(CliError::Config(format!(
                "{what}: no cases — add `suite:` cases or `fixtures:`"
            )));
        }
        self.suite.validate(&what)?;
        check_inline_behavioral(&what, &self.suite)?;
        check_fixtures(&what, &self.fixtures)
    }
}

/// Parse an untyped `kind: test-suite` document.
pub fn parse_test_suite(value: Value) -> CliResult<TestSuiteTemplate> {
    crate::requires::check_document(&value, "test suite")?;
    let t: TestSuiteTemplate =
        serde_json::from_value(value).map_err(|e| CliError::Config(format!("test-suite: {e}")))?;
    t.validate()?;
    Ok(t)
}

/// The semver range of a requirement.
pub fn parse_range(req: &SuiteRequirement) -> CliResult<semver::VersionReq> {
    semver::VersionReq::parse(req.version.trim()).map_err(|e| {
        CliError::Config(format!(
            "requires_suites: '{}' has an invalid version range '{}' (e.g. \">=1.2,<2\"): {e}",
            req.name, req.version
        ))
    })
}

impl TestBundle {
    /// True when the bundle declares nothing to run.
    pub fn is_empty(&self) -> bool {
        self.suite.is_empty() && self.fixtures.is_empty() && self.requires_suites.is_empty()
    }

    /// Structural checks for a bundle carried by a document of `kind`.
    pub fn validate(&self, kind: TemplateKind) -> CliResult<()> {
        let what = "tests";
        if self.is_empty() {
            return Err(CliError::Config(
                "tests: the block declares no cases — add `suite:`, `fixtures:` or \
                 `requires_suites:`. An empty bundle would record a green result for nothing."
                    .into(),
            ));
        }
        let (needs_source, needs_sink, takes_overlay) = match kind {
            TemplateKind::Pipeline => (false, false, false),
            TemplateKind::SourceTemplate => (false, true, true),
            TemplateKind::SinkTemplate => (true, false, false),
            TemplateKind::Deployment => (true, true, false),
            TemplateKind::TestSuite => {
                return Err(CliError::Config(
                    "a test-suite cannot carry a `tests:` block — its cases are the `suite:` and \
                     `fixtures:` themselves"
                        .into(),
                ));
            }
        };
        let companion = |field: &str, set: bool, needed: bool, allowed: bool| -> CliResult<()> {
            if needed && !set {
                return Err(CliError::Config(format!(
                    "tests: a {kind} bundle needs `{field}:` — the template it composes with to \
                     form a runnable pipeline"
                )));
            }
            if set && !needed && !allowed {
                return Err(CliError::Config(format!(
                    "tests: a {kind} bundle takes no `{field}:`"
                )));
            }
            Ok(())
        };
        companion("source", self.source.is_some(), needs_source, false)?;
        companion("sink", self.sink.is_some(), needs_sink, false)?;
        companion("overlay", self.overlay.is_some(), false, takes_overlay)?;
        for (sel, base) in [
            (&self.source_select, &self.source),
            (&self.sink_select, &self.sink),
            (&self.overlay_select, &self.overlay),
        ] {
            if let Some(s) = sel {
                if base.is_none() {
                    return Err(CliError::Config(format!(
                        "tests: `{s}` selects a version of a companion the bundle does not name"
                    )));
                }
                crate::template_tests::selector::check(s)?;
            }
        }
        self.suite.validate(what)?;
        check_inline_behavioral(what, &self.suite)?;
        check_fixtures(what, &self.fixtures)?;
        let mut seen = BTreeSet::new();
        for r in &self.requires_suites {
            if r.name.trim().is_empty() {
                return Err(CliError::Config(
                    "tests: every `requires_suites` entry needs a `name`".into(),
                ));
            }
            if !seen.insert(r.name.as_str()) {
                return Err(CliError::Config(format!(
                    "tests: suite '{}' is required twice",
                    r.name
                )));
            }
            parse_range(r)?;
        }
        Ok(())
    }
}

/// A behavioural case in a bundle must carry its records inline.
fn check_inline_behavioral(what: &str, suite: &Suite) -> CliResult<()> {
    for b in &suite.behavioral {
        if !b.input.is_array() {
            return Err(CliError::Config(format!(
                "{what}: behavioral case '{}' reads its input from a file — a bundled case must \
                 carry its records inline, because the version stores only the document",
                b.name
            )));
        }
    }
    Ok(())
}

fn check_fixtures(what: &str, fixtures: &[Fixture]) -> CliResult<()> {
    let mut seen = BTreeSet::new();
    for f in fixtures {
        let name = f.name.trim();
        if name.is_empty() {
            return Err(CliError::Config(format!(
                "{what}: every fixture needs a non-empty `name`"
            )));
        }
        if !seen.insert(name) {
            return Err(CliError::Config(format!(
                "{what}: duplicate fixture name '{name}'"
            )));
        }
        if matches!(f.input, InputSpec::Path(_)) {
            return Err(CliError::Config(format!(
                "{what}: fixture '{name}' reads its input from a file — a bundled fixture must \
                 carry its records inline, because the version stores only the document"
            )));
        }
        if f.pipeline.is_some() && (f.row.is_some() || !f.params.is_empty()) {
            return Err(CliError::Config(format!(
                "{what}: fixture '{name}' has an inline `pipeline:`, so `row:` / `params:` (which \
                 pick and bind the template's own config) do not apply"
            )));
        }
        if !f.expect.has_any() {
            return Err(CliError::Config(format!(
                "{what}: fixture '{name}': `expect` must set at least one of records / dlq / \
                 records_written / dlq_count / error"
            )));
        }
        faucet_core::validate_batch_size(f.page_size)
            .map_err(|e| CliError::Config(format!("{what}: fixture '{name}': page_size: {e}")))?;
        check_retries(what, name, f.retries)?;
    }
    Ok(())
}

/// Read the `tests:` block of an untyped template document, validated against
/// the document's kind. `None` when the document has none.
pub fn bundle_of(doc: &Value, kind: TemplateKind) -> CliResult<Option<TestBundle>> {
    let Some(raw) = doc.get("tests") else {
        return Ok(None);
    };
    let bundle: TestBundle = serde_json::from_value(raw.clone())
        .map_err(|e| CliError::Config(format!("tests: {e}")))?;
    bundle.validate(kind)?;
    Ok(Some(bundle))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bundle(yaml: &str) -> TestBundle {
        serde_yaml::from_str(yaml).expect("bundle parses")
    }

    #[test]
    fn a_pipeline_bundle_validates() {
        let b = bundle(
            r#"
suite:
  auto: { defaults_baseline: true }
fixtures:
  - name: passes
    input: [{ "id": 1 }]
    expect: { records_written: 1 }
requires_suites:
  - { name: conformance, version: ">=1.2,<2" }
"#,
        );
        b.validate(TemplateKind::Pipeline).expect("valid");
        assert!(!b.is_empty());
    }

    #[test]
    fn an_empty_bundle_is_refused() {
        let err = TestBundle::default()
            .validate(TemplateKind::Pipeline)
            .expect_err("empty");
        assert!(err.to_string().contains("declares no cases"), "{err}");
    }

    #[test]
    fn companions_follow_the_kind() {
        let base = "suite: { auto: { defaults_baseline: true } }\n";
        let err = bundle(base)
            .validate(TemplateKind::SourceTemplate)
            .expect_err("source needs sink");
        assert!(err.to_string().contains("needs `sink:`"), "{err}");
        bundle(&format!("sink: jsonl\noverlay: ops\n{base}"))
            .validate(TemplateKind::SourceTemplate)
            .expect("source + sink + overlay");
        let err = bundle(&format!("source: erp\n{base}"))
            .validate(TemplateKind::Pipeline)
            .expect_err("pipeline takes none");
        assert!(err.to_string().contains("takes no `source:`"), "{err}");
        let err = bundle(&format!("sink: jsonl\n{base}"))
            .validate(TemplateKind::SinkTemplate)
            .expect_err("sink-template needs a source");
        assert!(err.to_string().contains("needs `source:`"), "{err}");
        bundle(&format!("source: erp\nsink: jsonl\n{base}"))
            .validate(TemplateKind::Deployment)
            .expect("deployment");
        let err = bundle(&format!("source: erp\nsink: jsonl\noverlay: o\n{base}"))
            .validate(TemplateKind::Deployment)
            .expect_err("deployment takes no overlay");
        assert!(err.to_string().contains("takes no `overlay:`"), "{err}");
        let err = bundle(base)
            .validate(TemplateKind::TestSuite)
            .expect_err("suite");
        assert!(err.to_string().contains("cannot carry"), "{err}");
    }

    #[test]
    fn a_selector_without_its_companion_or_with_a_bad_value_is_refused() {
        let err = bundle("sink_select: stable\nsuite: { auto: { defaults_baseline: true } }\n")
            .validate(TemplateKind::Pipeline)
            .expect_err("dangling selector");
        assert!(err.to_string().contains("does not name"), "{err}");
        let err = bundle(
            "sink: jsonl\nsink_select: latest\nsuite: { auto: { defaults_baseline: true } }\n",
        )
        .validate(TemplateKind::SourceTemplate)
        .expect_err("latest");
        assert!(err.to_string().contains("latest"), "{err}");
    }

    #[test]
    fn fixtures_must_be_inline_named_and_asserting() {
        let err = bundle(
            "fixtures:\n  - { name: a, input: rows.jsonl, expect: { records_written: 1 } }\n",
        )
        .validate(TemplateKind::Pipeline)
        .expect_err("path");
        assert!(err.to_string().contains("inline"), "{err}");
        let err = bundle(
            "fixtures:\n  - { name: a, input: [], expect: { records_written: 0 } }\n  - { name: a, input: [], expect: { records_written: 0 } }\n",
        )
        .validate(TemplateKind::Pipeline)
        .expect_err("dup");
        assert!(err.to_string().contains("duplicate fixture"), "{err}");
        let err = bundle("fixtures:\n  - { name: ' ', input: [], expect: { records_written: 0 } }\n")
            .validate(TemplateKind::Pipeline)
            .expect_err("blank");
        assert!(err.to_string().contains("non-empty `name`"), "{err}");
        let err = bundle("fixtures:\n  - { name: a, input: [], expect: {} }\n")
            .validate(TemplateKind::Pipeline)
            .expect_err("no expectation");
        assert!(err.to_string().contains("at least one"), "{err}");
        let err = bundle(
            "fixtures:\n  - { name: a, input: [], retries: 9, expect: { records_written: 0 } }\n",
        )
        .validate(TemplateKind::Pipeline)
        .expect_err("retries");
        assert!(err.to_string().contains("at most"), "{err}");
        let err = bundle(
            "fixtures:\n  - name: a\n    pipeline: { transforms: [] }\n    row: r\n    input: []\n    expect: { records_written: 0 }\n",
        )
        .validate(TemplateKind::Pipeline)
        .expect_err("row with inline pipeline");
        assert!(err.to_string().contains("do not apply"), "{err}");
        let err = bundle(
            "fixtures:\n  - { name: a, input: [], page_size: 99999999, expect: { records_written: 0 } }\n",
        )
        .validate(TemplateKind::Pipeline)
        .expect_err("page size");
        assert!(err.to_string().contains("page_size"), "{err}");
    }

    #[test]
    fn behavioral_input_must_be_inline() {
        let err = bundle(
            "suite:\n  behavioral:\n    - { name: b, input: rows.jsonl, expect: { records_written: 1 } }\n",
        )
        .validate(TemplateKind::Pipeline)
        .expect_err("path");
        assert!(err.to_string().contains("inline"), "{err}");
    }

    #[test]
    fn requirements_need_a_name_a_range_and_no_repeats() {
        let err = bundle("requires_suites: [{ name: c, version: 'not a range' }]\n")
            .validate(TemplateKind::Pipeline)
            .expect_err("range");
        assert!(err.to_string().contains("invalid version range"), "{err}");
        let err = bundle("requires_suites: [{ name: c, version: '1' }, { name: c, version: '2' }]\n")
            .validate(TemplateKind::Pipeline)
            .expect_err("twice");
        assert!(err.to_string().contains("required twice"), "{err}");
        let err = bundle("requires_suites: [{ name: '', version: '1' }]\n")
            .validate(TemplateKind::Pipeline)
            .expect_err("name");
        assert!(err.to_string().contains("needs a `name`"), "{err}");
    }

    #[test]
    fn bundle_of_reads_and_validates_the_block() {
        let doc = serde_json::json!({"tests": {"suite": {"auto": {"defaults_baseline": true}}}});
        assert!(bundle_of(&doc, TemplateKind::Pipeline).unwrap().is_some());
        assert!(
            bundle_of(&serde_json::json!({}), TemplateKind::Pipeline)
                .unwrap()
                .is_none()
        );
        let err = bundle_of(&serde_json::json!({"tests": {"nope": 1}}), TemplateKind::Pipeline)
            .expect_err("unknown key");
        assert!(err.to_string().contains("nope"), "{err}");
    }

    fn suite_doc(release: &str) -> Value {
        serde_json::json!({
            "kind": "test-suite",
            "name": "conformance",
            "release": release,
            "suite": {"auto": {"defaults_baseline": true}},
        })
    }

    #[test]
    fn a_test_suite_parses_and_names_its_release() {
        let t = parse_test_suite(suite_doc("1.2.0")).expect("parses");
        assert_eq!(t.id(), "conformance");
        assert_eq!(t.release_version().unwrap(), semver::Version::new(1, 2, 0));
        let err = parse_test_suite(suite_doc("1.2")).expect_err("not semver");
        assert!(err.to_string().contains("not a semver"), "{err}");
    }

    #[test]
    fn a_test_suite_without_cases_or_with_the_wrong_kind_is_refused() {
        let mut doc = suite_doc("1.0.0");
        doc.as_object_mut().unwrap().remove("suite");
        let err = parse_test_suite(doc).expect_err("empty");
        assert!(err.to_string().contains("no cases"), "{err}");
        let mut doc = suite_doc("1.0.0");
        doc["kind"] = "pipeline".into();
        let err = parse_test_suite(doc).expect_err("kind");
        assert!(err.to_string().contains("not a test-suite"), "{err}");
        let mut doc = suite_doc("1.0.0");
        doc["version"] = 2.into();
        let err = parse_test_suite(doc).expect_err("doc version");
        assert!(err.to_string().contains("unsupported version"), "{err}");
        let mut doc = suite_doc("1.0.0");
        doc["owner"] = "Bad Owner".into();
        assert!(parse_test_suite(doc).is_err());
    }

    #[test]
    fn equality_compares_the_serialized_shape() {
        let a = bundle("fixtures:\n  - { name: a, input: [1], expect: { records_written: 1 } }\n");
        let b = bundle("fixtures:\n  - { name: a, input: [2], expect: { records_written: 1 } }\n");
        assert_ne!(a, b);
        assert_eq!(a, a.clone());
    }
}

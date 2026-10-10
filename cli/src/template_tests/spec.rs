//! Declarative parameter-combination test suites for pipeline templates
//! (#648).
//!
//! A template is a versioned, parameterized pipeline that fans out across a
//! parameter space. A change to one version can silently break one corner of
//! that space — a param that no longer interpolates, a value that produces an
//! invalid config, a `sink` variant whose companion param went missing — and
//! the only way to notice used to be triggering each combination by hand.
//!
//! This is the grammar for turning that sweep into a red/green artifact.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

use crate::error::{CliError, CliResult};

/// A suite file: `version: 1`, the template under test, and the cases.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SuiteFile {
    /// Schema version. Only `1` is accepted.
    pub version: u32,
    /// Registered template id, or a path to a config file. A path lets a
    /// template be tested **before** it is registered, which is the point at
    /// which most of these failures are cheapest to fix.
    pub template: String,
    /// Which version to test. A number, or a channel name (`stable`,
    /// `newest`, `prod`, …). Ignored when `template` is a path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub select: Option<String>,
    /// For a `source-template`: the sink template to compose with — a
    /// registered id when `template` is an id, a path when `template` is a
    /// path. The composed pipeline is what every case exercises.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    /// Version of the registered sink template. Default `stable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink_select: Option<String>,
    /// A deployment overlay applied over the composition (#679) — a registered
    /// id when `template` is an id, a path when `template` is a path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay: Option<String>,
    /// Version of the registered overlay. Default `stable`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay_select: Option<String>,
    /// The cases.
    pub suite: Suite,
    /// The suite file's directory, which relative fixture paths resolve
    /// against (set by [`SuiteFile::from_path`]; the working directory when
    /// the suite was parsed from text).
    #[serde(skip)]
    #[schemars(skip)]
    pub base_dir: Option<std::path::PathBuf>,
}

/// The three ways cases come into existence, applied in this order:
/// explicit, generated, derived.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Suite {
    /// Named cases, written out in full.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cases: Vec<Case>,
    /// A generated cartesian product over named param values.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combine: Option<Combine>,
    /// Cases derived from the template's own `params:` declaration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto: Option<Auto>,
    /// Behavioural cases: fixture records through the real pipeline.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub behavioral: Vec<Behavioral>,
}

/// One explicitly-written case.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Case {
    /// Case name, used in the report and by `--filter`.
    pub name: String,
    /// Param values for this combination.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
    /// What the case asserts.
    #[serde(default)]
    pub expect: Expect,
    /// Extra attempts before the case is reported failed. Explicit and
    /// recorded: a case that needed a retry says so in the result.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: u32,
}

/// What a validation-tier case asserts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    /// The combination must materialize into a config that expands and whose
    /// transforms compile. Defaults to `true`.
    #[serde(default = "default_true")]
    pub valid: bool,
    /// The combination must **fail**, with an error containing this substring.
    /// Setting it implies `valid: false` — a case cannot assert both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn default_true() -> bool {
    true
}

impl Default for Expect {
    /// Mirrors the serde default, so an omitted `expect:` and an empty
    /// `expect: {}` assert the same thing.
    fn default() -> Self {
        Self {
            valid: default_true(),
            error: None,
        }
    }
}

impl Expect {
    /// Whether this case expects failure.
    pub fn expects_failure(&self) -> bool {
        self.error.is_some() || !self.valid
    }
}

/// A generated cartesian product.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Combine {
    /// Param name → the values to sweep. The generated case count is the
    /// product of the list lengths (before `exclude` and `pairwise`).
    pub params: BTreeMap<String, Vec<Value>>,
    /// Combinations to drop — each entry is a partial assignment, and any
    /// generated combination that matches all of its keys is excluded. This is
    /// how a genuinely-invalid pairing (a sink that cannot take an option) is
    /// kept out without hand-writing every valid one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<BTreeMap<String, Value>>,
    /// Reduce to an **all-pairs** set rather than the full product.
    ///
    /// Most param-interaction bugs involve two params, so all-pairs keeps the
    /// coverage that matters while turning a multiplicative case count into
    /// roughly the product of the two largest lists.
    #[serde(default)]
    pub pairwise: bool,
    /// What every generated case asserts.
    #[serde(default)]
    pub expect: Expect,
    /// Extra attempts for every generated case.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: u32,
}

/// Cases derived from the template's own `params:` block.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Auto {
    /// One case per declared value of each param carrying a closed `values:`
    /// set — "does this template still work for every sink it advertises?".
    #[serde(default)]
    pub enum_coverage: bool,
    /// One case per `required` param, omitting it, expecting a failure. Pins
    /// that a missing required param fails *cleanly* rather than producing a
    /// config with a hole in it.
    #[serde(default)]
    pub required_omitted: bool,
    /// The all-defaults case — the combination most callers actually use, and
    /// the one most easily forgotten in a hand-written suite.
    #[serde(default)]
    pub defaults_baseline: bool,
    /// Extra attempts for every derived case.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: u32,
}

/// A behavioural case: fixture records through the real pipeline.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Behavioral {
    /// Case name.
    pub name: String,
    /// Param values for the combination under test.
    #[serde(default)]
    pub params: BTreeMap<String, Value>,
    /// Records per page fed to the pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,
    /// Which row (stream) of the materialized config the case runs. Required
    /// when it expands to more than one root row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row: Option<String>,
    /// Inline records, or a path to a `.jsonl` / `.json` / `.yaml` fixture —
    /// the same shapes `faucet test` accepts.
    pub input: Value,
    /// Expectations, in `faucet test`'s grammar.
    pub expect: Value,
    /// Extra attempts before the case is reported failed.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub retries: u32,
}

fn is_zero(n: &u32) -> bool {
    *n == 0
}

/// The most extra attempts a case may ask for. A case that needs more is
/// broken, and a long retry loop would hide it.
pub const MAX_RETRIES: u32 = 5;

/// Hard ceiling on generated cases.
///
/// A cartesian product explodes quietly: four params with five values each is
/// 625 cases, each materializing and expanding a config. The cap exists so a
/// suite fails loudly rather than appearing to hang — and it is an **error**,
/// not a silent truncation, because silently testing a third of the space is
/// worse than not testing it (the report would read green).
pub const MAX_CASES: usize = 512;

impl SuiteFile {
    /// Parse a suite from YAML or JSON text.
    pub fn parse(text: &str) -> CliResult<Self> {
        let file: SuiteFile = serde_yaml::from_str(text)
            .map_err(|e| CliError::Config(format!("template test suite: {e}")))?;
        file.validate()?;
        Ok(file)
    }

    /// Load a suite from a file.
    pub fn from_path(path: &std::path::Path) -> CliResult<Self> {
        let text = std::fs::read_to_string(path).map_err(|e| {
            CliError::Config(format!("template test suite {}: {e}", path.display()))
        })?;
        let mut file = Self::parse(&text)?;
        file.base_dir = path.parent().map(std::path::Path::to_path_buf);
        Ok(file)
    }

    fn validate(&self) -> CliResult<()> {
        if self.version != 1 {
            return Err(CliError::Config(format!(
                "template test suite: unsupported version {} (expected 1)",
                self.version
            )));
        }
        if self.template.trim().is_empty() {
            return Err(CliError::Config(
                "template test suite: `template` must name a registered id or a config path".into(),
            ));
        }
        if self.suite.is_empty() {
            return Err(CliError::Config(
                "template test suite: no cases — add `cases:`, `combine:`, `auto:` or \
                 `behavioral:`. An empty suite reports green, which is worse than no suite."
                    .into(),
            ));
        }
        self.suite.validate("template test suite")
    }
}

impl Suite {
    /// True when the suite would generate no case at all.
    pub fn is_empty(&self) -> bool {
        let auto_on = self
            .auto
            .as_ref()
            .is_some_and(|a| a.enum_coverage || a.required_omitted || a.defaults_baseline);
        self.cases.is_empty() && self.combine.is_none() && self.behavioral.is_empty() && !auto_on
    }

    /// Structural checks shared by a suite file, a template's `tests:` block
    /// and a shared suite. `what` prefixes every message.
    pub fn validate(&self, what: &str) -> CliResult<()> {
        for c in &self.cases {
            if c.name.trim().is_empty() {
                return Err(CliError::Config(format!(
                    "{what}: every case needs a non-empty `name`"
                )));
            }
            check_retries(what, &c.name, c.retries)?;
        }
        if let Some(cb) = &self.combine {
            if cb.params.is_empty() {
                return Err(CliError::Config(format!(
                    "{what}: `combine.params` is empty — nothing to generate"
                )));
            }
            for (name, values) in &cb.params {
                if values.is_empty() {
                    return Err(CliError::Config(format!(
                        "{what}: `combine.params.{name}` is an empty list, which \
                         would generate zero cases for every other param too"
                    )));
                }
            }
            check_retries(what, "combine", cb.retries)?;
        }
        if let Some(a) = &self.auto {
            check_retries(what, "auto", a.retries)?;
        }
        for b in &self.behavioral {
            if b.name.trim().is_empty() {
                return Err(CliError::Config(format!(
                    "{what}: every behavioral case needs a non-empty `name`"
                )));
            }
            check_retries(what, &b.name, b.retries)?;
        }
        Ok(())
    }
}

pub(crate) fn check_retries(what: &str, case: &str, retries: u32) -> CliResult<()> {
    if retries > MAX_RETRIES {
        return Err(CliError::Config(format!(
            "{what}: case '{case}' asks for {retries} retries; at most {MAX_RETRIES} are allowed"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_suite() {
        let s = SuiteFile::parse(
            r#"
version: 1
template: sf-to-bq
select: newest
suite:
  cases:
    - name: jsonl-account
      params: { sink: jsonl, object: Account }
  combine:
    params:
      sink: [jsonl, bigquery]
      object: [Account, Contact]
    exclude:
      - { sink: bigquery, object: Contact }
    pairwise: false
  auto:
    enum_coverage: true
    required_omitted: true
    defaults_baseline: true
  behavioral:
    - name: flow
      params: { sink: jsonl }
      input: [{ "Id": "1" }]
      expect: { records_written: 1 }
"#,
        )
        .expect("parses");
        assert_eq!(s.template, "sf-to-bq");
        assert_eq!(s.suite.cases.len(), 1);
        assert_eq!(s.suite.combine.as_ref().unwrap().params.len(), 2);
        assert!(s.suite.auto.as_ref().unwrap().enum_coverage);
        assert_eq!(s.suite.behavioral.len(), 1);
    }

    #[test]
    fn an_empty_suite_is_rejected() {
        // A suite with no cases reports green, which is worse than no suite at
        // all — it looks like coverage.
        let err = SuiteFile::parse("version: 1\ntemplate: t\nsuite: {}\n")
            .expect_err("empty suite must be rejected");
        assert!(err.to_string().contains("no cases"), "{err}");
    }

    #[test]
    fn an_unknown_version_is_rejected() {
        let err = SuiteFile::parse("version: 2\ntemplate: t\nsuite: { cases: [{ name: a }] }\n")
            .expect_err("version 2");
        assert!(err.to_string().contains("version 2"), "{err}");
    }

    #[test]
    fn an_empty_combine_list_is_rejected() {
        // A single empty list makes the whole cartesian product empty, so the
        // suite would silently test nothing.
        let err = SuiteFile::parse(
            "version: 1\ntemplate: t\nsuite:\n  combine:\n    params:\n      sink: []\n",
        )
        .expect_err("empty list");
        assert!(err.to_string().contains("empty list"), "{err}");
    }

    #[test]
    fn an_unknown_key_is_rejected() {
        let err =
            SuiteFile::parse("version: 1\ntemplate: t\nsuit: { cases: [] }\n").expect_err("typo");
        assert!(err.to_string().contains("suit"), "{err}");
    }

    #[test]
    fn expect_defaults_to_valid() {
        let e = Expect::default();
        assert!(e.valid);
        assert!(!e.expects_failure());
        let failing = Expect {
            valid: true,
            error: Some("required".into()),
        };
        assert!(
            failing.expects_failure(),
            "an `error:` expectation means the case must fail"
        );
    }

    /// A suite pointed at nothing cannot resolve a template, and reporting
    /// green for zero cases is the failure this whole feature exists to
    /// prevent — so it is refused at parse time.
    #[test]
    fn an_empty_template_reference_is_rejected() {
        let err =
            SuiteFile::parse("version: 1\ntemplate: \"   \"\nsuite:\n  cases:\n    - name: a\n")
                .expect_err("blank template");
        assert!(err.to_string().contains("`template` must name"), "{err}");
    }

    /// An unnamed case makes `--filter` ambiguous and the report unreadable.
    #[test]
    fn a_case_without_a_name_is_rejected() {
        let err =
            SuiteFile::parse("version: 1\ntemplate: t\nsuite:\n  cases:\n    - name: \"  \"\n")
                .expect_err("blank case name");
        assert!(err.to_string().contains("non-empty `name`"), "{err}");
    }

    /// `combine:` present but with no axes generates nothing, which would
    /// again read as a green suite that tested nothing.
    #[test]
    fn an_empty_combine_params_map_is_rejected() {
        let err = SuiteFile::parse("version: 1\ntemplate: t\nsuite:\n  combine:\n    params: {}\n")
            .expect_err("no axes");
        assert!(err.to_string().contains("nothing to generate"), "{err}");
    }
}

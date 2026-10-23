//! Serde types for the `faucet test` spec file.
//!
//! A spec file declares fixture-based, fully-offline test cases for a
//! pipeline's deterministic path (transforms → quality → contract). No real
//! source or sink is ever built: fixture records are streamed through an
//! in-memory source and captured by an in-memory sink + DLQ.

use crate::config::TransformSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Top-level shape of a `faucet test` spec file (YAML or JSON).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TestSpecFile {
    /// Spec-format version. Must be `1`.
    pub version: u32,
    /// The test cases, run in declared order.
    pub tests: Vec<TestCase>,
}

/// One fixture-based test case.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    /// Human-readable case name. Must be unique within the spec file.
    pub name: String,

    /// Path to a pipeline config file (relative paths resolve against the
    /// spec file's directory). The case runs that config's transforms,
    /// `quality:`, and `contract:` blocks against the fixture input.
    /// Mutually exclusive with `pipeline`.
    #[serde(default)]
    pub config: Option<String>,

    /// Inline pipeline logic (transforms / quality / contract) — no config
    /// file needed. Mutually exclusive with `config`.
    #[serde(default)]
    pub pipeline: Option<InlinePipeline>,

    /// Matrix row id to test when the referenced config expands to more than
    /// one invocation. Defaults to the sole invocation; an error names the
    /// available ids when the config has several and `row` is omitted.
    #[serde(default)]
    pub row: Option<String>,

    /// Fixture input: an inline array of JSON records, or a path (string) to
    /// a `.jsonl` / `.json` / `.yaml` fixture file (relative to the spec
    /// file's directory).
    pub input: InputSpec,

    /// Chunk the fixture input into pages of this many records before feeding
    /// the pipeline. `0` (default) feeds everything as a single page —
    /// matching `batch_size: 0` semantics for per-page checks (batch quality
    /// checks and aggregating SQL transforms see the whole input at once).
    #[serde(default)]
    pub page_size: usize,

    /// Fixed `${now.*}` clock for this case (RFC3339 like
    /// `2026-01-31T00:00:00Z`, or a date `2026-01-31`). Overrides the
    /// command-level `--clock`; defaults to process start (UTC). Set this
    /// whenever a transform stamps `${now.*}` so the case is deterministic.
    #[serde(default)]
    pub clock: Option<String>,

    /// What the case asserts about the run's outcome.
    pub expect: Expectation,
}

/// Inline pipeline logic for a test case that doesn't reference a config file.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InlinePipeline {
    /// Transform chain, identical to `pipeline.transforms` in a config file.
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,

    /// Quality checks, identical to `pipeline.quality` in a config file.
    /// Quarantined records are capturable via `expect.dlq` / `dlq_count`.
    #[cfg(feature = "quality")]
    #[serde(default)]
    pub quality: Option<faucet_core::QualitySpec>,

    /// Data contract, identical to `pipeline.contract` in a config file.
    #[cfg(feature = "contract")]
    #[serde(default)]
    pub contract: Option<faucet_core::ContractSpec>,

    /// PII masking policy, identical to `pipeline.masking` in a config file.
    /// Offline there is no destination sink, so every rule applies regardless
    /// of its `applies_to` scoping.
    #[cfg(feature = "masking")]
    #[serde(default)]
    pub masking: Option<faucet_core::MaskingSpec>,
}

/// Fixture input — inline records or a fixture-file path.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum InputSpec {
    /// Inline JSON records.
    Inline(Vec<Value>),
    /// Path to a `.jsonl` (one record per line) or `.json` / `.yaml` /
    /// `.yml` (top-level array) fixture file, relative to the spec file.
    Path(String),
}

/// Expected outcome of a test case. Every field is optional but at least one
/// must be set; all set fields are asserted together.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Expectation {
    /// The exact records the sink must receive, in order (set
    /// `unordered: true` to compare as a multiset). Compared per `match`.
    #[serde(default)]
    pub records: Option<Vec<Value>>,

    /// The original record payloads that must land in the DLQ (quality /
    /// contract quarantines), in order. Envelope metadata (timestamps, error
    /// messages) is not compared — only the quarantined payload itself.
    #[serde(default)]
    pub dlq: Option<Vec<Value>>,

    /// Total records the sink must receive (a count-only alternative to
    /// `records`).
    #[serde(default)]
    pub records_written: Option<usize>,

    /// Total DLQ envelopes (a count-only alternative to `dlq`).
    #[serde(default)]
    pub dlq_count: Option<usize>,

    /// The run must FAIL, and the error message must contain this substring
    /// (e.g. a quality `abort` or contract `on_breach: fail`). Without this
    /// field, a failing run fails the case.
    #[serde(default)]
    pub error: Option<String>,

    /// Compare `records` / `dlq` as multisets instead of ordered lists.
    #[serde(default)]
    pub unordered: bool,

    /// How individual records are compared.
    #[serde(default, rename = "match")]
    pub match_mode: MatchMode,
}

/// Record-comparison mode for `expect.records` / `expect.dlq`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    /// Expected and actual records must be deeply equal.
    #[default]
    Exact,
    /// Each expected record must be a recursive subset of the actual record:
    /// every expected object field must be present and match, but the actual
    /// record may carry extra fields. Arrays still compare element-by-element
    /// (same length), applying subset semantics to nested objects.
    Subset,
}

impl Expectation {
    /// True when at least one assertion field is set.
    pub fn has_any(&self) -> bool {
        self.records.is_some()
            || self.dlq.is_some()
            || self.records_written.is_some()
            || self.dlq_count.is_some()
            || self.error.is_some()
    }
}

impl TestSpecFile {
    /// Structural validation, run right after parsing. Fail-fast so a broken
    /// spec never reaches the runner: version, unique non-empty names, the
    /// config/pipeline exclusivity, a non-empty expectation, and page-size
    /// bounds all surface here with the spec path in the message.
    pub fn validate(&self, spec_path: &std::path::Path) -> crate::error::CliResult<()> {
        let at =
            |msg: String| crate::error::CliError::Config(format!("{}: {msg}", spec_path.display()));
        if self.version != 1 {
            return Err(at(format!(
                "unsupported test-spec version {} (expected 1)",
                self.version
            )));
        }
        if self.tests.is_empty() {
            return Err(at("spec declares no tests".to_string()));
        }
        let mut seen = std::collections::HashSet::new();
        for case in &self.tests {
            let name = case.name.trim();
            if name.is_empty() {
                return Err(at("test case with an empty name".to_string()));
            }
            if !seen.insert(name) {
                return Err(at(format!("duplicate test name '{name}'")));
            }
            match (&case.config, &case.pipeline) {
                (Some(_), Some(_)) => {
                    return Err(at(format!(
                        "test '{name}': `config` and `pipeline` are mutually exclusive — pick one"
                    )));
                }
                (None, None) => {
                    return Err(at(format!(
                        "test '{name}': one of `config` (a pipeline config path) or `pipeline` \
                         (inline transforms/quality/contract) is required"
                    )));
                }
                _ => {}
            }
            if case.row.is_some() && case.config.is_none() {
                return Err(at(format!(
                    "test '{name}': `row` selects a matrix row and requires `config`"
                )));
            }
            if !case.expect.has_any() {
                return Err(at(format!(
                    "test '{name}': `expect` must set at least one of records / dlq / \
                     records_written / dlq_count / error"
                )));
            }
            faucet_core::validate_batch_size(case.page_size)
                .map_err(|e| at(format!("test '{name}': page_size: {e}")))?;
        }
        Ok(())
    }
}

/// Parse a spec file (YAML or JSON by extension) and validate it.
pub fn load_spec(path: &std::path::Path) -> crate::error::CliResult<TestSpecFile> {
    use crate::error::CliError;
    let text = std::fs::read_to_string(path).map_err(|source| CliError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    let spec: TestSpecFile = match ext.as_deref() {
        Some("yaml" | "yml") => {
            serde_yaml_bw::from_str(&text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })?
        }
        Some("json") => serde_json::from_str(&text).map_err(|e| CliError::ParseConfig {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?,
        _ => {
            return Err(CliError::UnknownExtension {
                path: path.to_path_buf(),
            });
        }
    };
    spec.validate(path)?;
    Ok(spec)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::path::Path;

    fn write_spec(dir: &tempfile::TempDir, name: &str, body: &str) -> std::path::PathBuf {
        let p = dir.path().join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn parses_minimal_inline_spec() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_spec(
            &dir,
            "t.yaml",
            r#"
version: 1
tests:
  - name: passthrough
    pipeline: {}
    input: [ { a: 1 } ]
    expect: { records: [ { a: 1 } ] }
"#,
        );
        let spec = load_spec(&p).unwrap();
        assert_eq!(spec.tests.len(), 1);
        assert_eq!(spec.tests[0].name, "passthrough");
        assert!(matches!(spec.tests[0].input, InputSpec::Inline(ref v) if v.len() == 1));
        assert_eq!(spec.tests[0].expect.records, Some(vec![json!({"a": 1})]));
        assert_eq!(spec.tests[0].expect.match_mode, MatchMode::Exact);
        assert!(!spec.tests[0].expect.unordered);
    }

    #[test]
    fn parses_json_spec() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_spec(
            &dir,
            "t.json",
            r#"{ "version": 1, "tests": [ { "name": "n", "pipeline": {},
                 "input": [], "expect": { "records_written": 0 } } ] }"#,
        );
        assert_eq!(load_spec(&p).unwrap().tests.len(), 1);
    }

    #[test]
    fn rejects_unknown_extension_and_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_spec(&dir, "t.toml", "version = 1");
        assert!(matches!(
            load_spec(&p),
            Err(crate::error::CliError::UnknownExtension { .. })
        ));
        assert!(matches!(
            load_spec(Path::new("/nonexistent/spec.yaml")),
            Err(crate::error::CliError::ReadConfig { .. })
        ));
    }

    #[test]
    fn rejects_bad_version_empty_tests_and_duplicates() {
        let dir = tempfile::tempdir().unwrap();
        let bad_version = write_spec(
            &dir,
            "v.yaml",
            "version: 2\ntests: [ { name: x, pipeline: {}, input: [], expect: { records_written: 0 } } ]",
        );
        let err = load_spec(&bad_version).unwrap_err().to_string();
        assert!(err.contains("version 2"), "{err}");

        let empty = write_spec(&dir, "e.yaml", "version: 1\ntests: []");
        assert!(
            load_spec(&empty)
                .unwrap_err()
                .to_string()
                .contains("no tests")
        );

        let dup = write_spec(
            &dir,
            "d.yaml",
            r#"
version: 1
tests:
  - { name: same, pipeline: {}, input: [], expect: { records_written: 0 } }
  - { name: same, pipeline: {}, input: [], expect: { records_written: 0 } }
"#,
        );
        assert!(
            load_spec(&dup)
                .unwrap_err()
                .to_string()
                .contains("duplicate")
        );
    }

    #[test]
    fn rejects_config_pipeline_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let both = write_spec(
            &dir,
            "b.yaml",
            r#"
version: 1
tests:
  - { name: x, config: p.yaml, pipeline: {}, input: [], expect: { records_written: 0 } }
"#,
        );
        assert!(
            load_spec(&both)
                .unwrap_err()
                .to_string()
                .contains("mutually exclusive")
        );

        let neither = write_spec(
            &dir,
            "n.yaml",
            "version: 1\ntests: [ { name: x, input: [], expect: { records_written: 0 } } ]",
        );
        assert!(
            load_spec(&neither)
                .unwrap_err()
                .to_string()
                .contains("is required")
        );
    }

    #[test]
    fn rejects_row_without_config_and_empty_expect() {
        let dir = tempfile::tempdir().unwrap();
        let row = write_spec(
            &dir,
            "r.yaml",
            "version: 1\ntests: [ { name: x, pipeline: {}, row: a, input: [], expect: { records_written: 0 } } ]",
        );
        assert!(
            load_spec(&row)
                .unwrap_err()
                .to_string()
                .contains("requires `config`")
        );

        let empty_expect = write_spec(
            &dir,
            "x.yaml",
            "version: 1\ntests: [ { name: x, pipeline: {}, input: [], expect: {} } ]",
        );
        assert!(
            load_spec(&empty_expect)
                .unwrap_err()
                .to_string()
                .contains("at least one")
        );
    }

    #[test]
    fn rejects_oversized_page_size_and_empty_name() {
        let dir = tempfile::tempdir().unwrap();
        let big = write_spec(
            &dir,
            "p.yaml",
            "version: 1\ntests: [ { name: x, pipeline: {}, input: [], page_size: 2000000, expect: { records_written: 0 } } ]",
        );
        assert!(
            load_spec(&big)
                .unwrap_err()
                .to_string()
                .contains("page_size")
        );

        let unnamed = write_spec(
            &dir,
            "u.yaml",
            "version: 1\ntests: [ { name: '  ', pipeline: {}, input: [], expect: { records_written: 0 } } ]",
        );
        assert!(
            load_spec(&unnamed)
                .unwrap_err()
                .to_string()
                .contains("empty name")
        );
    }

    #[test]
    fn input_path_variant_parses() {
        let dir = tempfile::tempdir().unwrap();
        let p = write_spec(
            &dir,
            "f.yaml",
            r#"
version: 1
tests:
  - name: from-file
    pipeline: {}
    input: fixtures/records.jsonl
    expect: { records_written: 2 }
"#,
        );
        let spec = load_spec(&p).unwrap();
        assert!(
            matches!(spec.tests[0].input, InputSpec::Path(ref s) if s == "fixtures/records.jsonl")
        );
    }

    #[test]
    fn schema_generates() {
        let schema = schemars::schema_for!(TestSpecFile);
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["properties"]["tests"].is_object());
    }
}

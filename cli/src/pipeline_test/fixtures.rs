//! Fixture-input loading for `faucet test`.
//!
//! A case's `input` is either an inline array of records or a path to a
//! fixture file. Files resolve relative to the spec file's directory and may
//! be `.jsonl` (one JSON record per line, blank lines skipped) or `.json` /
//! `.yaml` / `.yml` (a top-level array).

use crate::error::{CliError, CliResult};
use crate::pipeline_test::spec::InputSpec;
use serde_json::Value;
use std::path::Path;

/// Materialize a case's fixture records.
pub fn load_input(spec_dir: &Path, input: &InputSpec) -> CliResult<Vec<Value>> {
    match input {
        InputSpec::Inline(records) => Ok(records.clone()),
        InputSpec::Path(rel) => load_fixture_file(&spec_dir.join(rel)),
    }
}

fn load_fixture_file(path: &Path) -> CliResult<Vec<Value>> {
    let text = std::fs::read_to_string(path).map_err(|source| CliError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase);
    match ext.as_deref() {
        Some("jsonl" | "ndjson") => text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty())
            .map(|(i, line)| {
                serde_json::from_str(line).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: format!("line {}: {e}", i + 1),
                })
            })
            .collect(),
        Some("json") => as_array(
            serde_json::from_str(&text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })?,
            path,
        ),
        Some("yaml" | "yml") => as_array(
            serde_yaml_bw::from_str(&text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })?,
            path,
        ),
        _ => Err(CliError::Config(format!(
            "fixture file '{}' must be .jsonl, .ndjson, .json, .yaml, or .yml",
            path.display()
        ))),
    }
}

fn as_array(v: Value, path: &Path) -> CliResult<Vec<Value>> {
    match v {
        Value::Array(records) => Ok(records),
        other => Err(CliError::Config(format!(
            "fixture file '{}' must hold a top-level array of records, got {}",
            path.display(),
            type_name(&other)
        ))),
    }
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inline_records_pass_through() {
        let dir = tempfile::tempdir().unwrap();
        let records = load_input(
            dir.path(),
            &InputSpec::Inline(vec![json!({"a": 1}), json!({"a": 2})]),
        )
        .unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn jsonl_fixture_skips_blank_lines() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.jsonl"), "{\"a\":1}\n\n{\"a\":2}\n").unwrap();
        let records = load_input(dir.path(), &InputSpec::Path("f.jsonl".into())).unwrap();
        assert_eq!(records, vec![json!({"a": 1}), json!({"a": 2})]);
    }

    #[test]
    fn jsonl_bad_line_reports_line_number() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.jsonl"), "{\"a\":1}\nnot-json\n").unwrap();
        let err = load_input(dir.path(), &InputSpec::Path("f.jsonl".into()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("line 2"), "{err}");
    }

    #[test]
    fn json_and_yaml_arrays_load() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.json"), r#"[{"a":1}]"#).unwrap();
        std::fs::write(dir.path().join("f.yaml"), "- a: 1\n- a: 2\n").unwrap();
        assert_eq!(
            load_input(dir.path(), &InputSpec::Path("f.json".into())).unwrap(),
            vec![json!({"a": 1})]
        );
        assert_eq!(
            load_input(dir.path(), &InputSpec::Path("f.yaml".into()))
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn non_array_json_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.json"), r#"{"a":1}"#).unwrap();
        let err = load_input(dir.path(), &InputSpec::Path("f.json".into()))
            .unwrap_err()
            .to_string();
        assert!(err.contains("top-level array"), "{err}");
        assert!(err.contains("an object"), "{err}");
    }

    #[test]
    fn unknown_extension_and_missing_file_rejected() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("f.csv"), "a\n1\n").unwrap();
        assert!(
            load_input(dir.path(), &InputSpec::Path("f.csv".into()))
                .unwrap_err()
                .to_string()
                .contains("must be .jsonl")
        );
        assert!(matches!(
            load_input(dir.path(), &InputSpec::Path("missing.jsonl".into())),
            Err(CliError::ReadConfig { .. })
        ));
    }
}

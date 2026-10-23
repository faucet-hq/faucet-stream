//! Config composition front pre-pass: `extends` (base inheritance), `profiles`
//! (named overlays selected via `--profile`/`FAUCET_PROFILE`), and `!include`
//! (YAML fragment substitution). Runs BEFORE `${...}` interpolation and the
//! secrets pass, reusing [`crate::merge::merge_value`].
//!
//! [`compose`] returns the merged document as **text** (in the entry file's
//! format) so the unchanged `interpolate → from_text` pipeline runs verbatim.
//! When no composition is in play it returns the raw file text byte-identical.

use crate::error::{CliError, CliResult};
use crate::merge::merge_value;
use serde_json::Value as JsonValue;
use std::path::{Path, PathBuf};

/// Hard cap on extends/!include nesting depth (loop backstop).
const MAX_COMPOSE_DEPTH: usize = 32;

/// File format, chosen by extension.
#[derive(Clone, Copy)]
enum Format {
    Yaml,
    Json,
}

fn format_of(path: &Path) -> CliResult<Format> {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("yaml" | "yml") => Ok(Format::Yaml),
        Some("json") => Ok(Format::Json),
        _ => Err(CliError::UnknownExtension {
            path: path.to_path_buf(),
        }),
    }
}

/// Resolve a (possibly relative) `extends`/`!include` target against the
/// directory of the file that referenced it.
fn resolve_rel(dir: &Path, rel: &str) -> PathBuf {
    let p = Path::new(rel);
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        dir.join(p)
    }
}

/// Canonicalize for cycle detection; fall back to the raw path if the file
/// cannot be canonicalized (callers verify existence before recursing).
fn cycle_key(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

/// Cheap substring pre-check: are there any composition markers in the text?
/// A false positive only forces the (still-correct) merge path; never a false
/// negative for real directives, so the fast path stays safe.
fn has_composition_markers(raw: &str) -> bool {
    raw.contains("extends") || raw.contains("profiles") || raw.contains("!include")
}

/// Public entry: returns text ready to feed into `interpolate` + `from_text`.
pub fn compose(entry: &Path, profile: Option<&str>) -> CliResult<String> {
    let raw = std::fs::read_to_string(entry).map_err(|source| CliError::ReadConfig {
        path: entry.to_path_buf(),
        source,
    })?;
    // Fast path: no directives + no profile selected → byte-identical passthrough.
    if profile.is_none() && !has_composition_markers(&raw) {
        return Ok(raw);
    }
    let mut visited: Vec<PathBuf> = Vec::new();
    let mut merged = compose_document(entry, &mut visited, 0)?;
    apply_profile(&mut merged, profile)?;
    // Strip composition metadata so PipelineConfig (deny_unknown_fields) never sees it.
    if let JsonValue::Object(map) = &mut merged {
        map.remove("profiles");
        map.remove("extends");
    }
    serialize_for(entry, &merged)
}

/// Load a document, then resolve its `extends` chain. Returns the deep-merged
/// `serde_json::Value` (profiles preserved; extends stripped). Used for the
/// entry file and every `extends` target.
fn compose_document(path: &Path, visited: &mut Vec<PathBuf>, depth: usize) -> CliResult<JsonValue> {
    if depth > MAX_COMPOSE_DEPTH {
        return Err(CliError::CompositionDepthExceeded {
            max: MAX_COMPOSE_DEPTH,
        });
    }
    let key = cycle_key(path);
    if visited.contains(&key) {
        let mut chain: Vec<String> = visited.iter().map(|p| p.display().to_string()).collect();
        chain.push(key.display().to_string());
        return Err(CliError::CompositionCycle { chain });
    }
    visited.push(key);

    let mut doc = load_value(path, visited, depth)?;
    let bases = take_extends(&mut doc, path)?;
    let result = if bases.is_empty() {
        doc
    } else {
        let dir = path.parent().unwrap_or_else(|| Path::new("."));
        let mut acc = JsonValue::Object(serde_json::Map::new());
        for base_rel in bases {
            let base_path = resolve_rel(dir, &base_rel);
            if !base_path.exists() {
                // No mid-fn pop: any Err aborts the whole traversal and `visited`
                // (a `compose`-local) is discarded, so the stack is only meaningful
                // on the Ok path.
                return Err(CliError::IncludeNotFound {
                    path: base_path,
                    referenced_by: path.to_path_buf(),
                });
            }
            let base_doc = compose_document(&base_path, visited, depth + 1)?;
            merge_value(&mut acc, base_doc);
        }
        merge_value(&mut acc, doc); // child wins over its bases
        acc
    };

    visited.pop();
    Ok(result)
}

/// Parse one file into a `serde_json::Value`, resolving any `!include` tags
/// (YAML only) before conversion. Shares the caller's `visited`/`depth` cycle
/// stack so a mixed `extends`+`!include` loop is caught as a `CompositionCycle`
/// rather than terminating with a confusing leftover-key error.
fn load_value(path: &Path, visited: &mut Vec<PathBuf>, depth: usize) -> CliResult<JsonValue> {
    let text = std::fs::read_to_string(path).map_err(|source| CliError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    match format_of(path)? {
        Format::Yaml => {
            let mut yv: serde_yaml_bw::Value =
                serde_yaml_bw::from_str(&text).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })?;
            resolve_includes(&mut yv, path, visited, depth)?;
            yaml_to_json(yv, path)
        }
        Format::Json => serde_json::from_str(&text).map_err(|e| CliError::ParseConfig {
            path: path.to_path_buf(),
            message: e.to_string(),
        }),
    }
}

/// Convert a (fully include-resolved) YAML value to JSON. Non-string map keys
/// or leftover unsupported tags surface as a parse error against `path`.
fn yaml_to_json(yv: serde_yaml_bw::Value, path: &Path) -> CliResult<JsonValue> {
    serde_json::to_value(yv).map_err(|e| CliError::ParseConfig {
        path: path.to_path_buf(),
        message: format!(
            "could not convert YAML to JSON (non-string keys or unsupported tag?): {e}"
        ),
    })
}

/// Remove and return the top-level `extends` entry as a list of path strings.
fn take_extends(doc: &mut JsonValue, path: &Path) -> CliResult<Vec<String>> {
    let JsonValue::Object(map) = doc else {
        return Ok(Vec::new());
    };
    let Some(ext) = map.remove("extends") else {
        return Ok(Vec::new());
    };
    match ext {
        JsonValue::String(s) => Ok(vec![s]),
        JsonValue::Array(arr) => arr
            .into_iter()
            .map(|v| match v {
                JsonValue::String(s) => Ok(s),
                other => Err(CliError::Config(format!(
                    "`extends` list entries must be strings, got {other} in '{}'",
                    path.display()
                ))),
            })
            .collect(),
        other => Err(CliError::Config(format!(
            "`extends` must be a string or list of strings, got {other} in '{}'",
            path.display()
        ))),
    }
}

/// Deep-merge `profiles[name]` over `merged` when a profile is selected.
fn apply_profile(merged: &mut JsonValue, profile: Option<&str>) -> CliResult<()> {
    let Some(name) = profile else {
        return Ok(());
    };
    let profiles = merged.get("profiles").and_then(|p| p.as_object());
    let known: Vec<String> = profiles
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    let overlay = profiles.and_then(|m| m.get(name)).cloned();
    match overlay {
        Some(ov) => {
            merge_value(merged, ov);
            Ok(())
        }
        None => Err(CliError::UnknownProfile {
            name: name.to_string(),
            known,
        }),
    }
}

/// Serialize the merged document back to the entry file's format.
///
/// The output is canonicalized, not a faithful re-render: `serde_json`'s map is
/// a `BTreeMap`, so keys come out sorted and comments/formatting are dropped.
/// That's fine — the result is re-parsed by `from_text` downstream, and the fast
/// path keeps non-composed configs byte-identical.
fn serialize_for(entry: &Path, merged: &JsonValue) -> CliResult<String> {
    match format_of(entry)? {
        Format::Yaml => serde_yaml_bw::to_string(merged)
            .map_err(|e| CliError::Internal(format!("re-serialize composed config to YAML: {e}"))),
        Format::Json => serde_json::to_string_pretty(merged)
            .map_err(|e| CliError::Internal(format!("re-serialize composed config to JSON: {e}"))),
    }
}

/// Resolve `!include <path>` tags throughout a YAML value in place. Each tag's
/// payload must be a string path, resolved relative to `including`'s directory.
/// Recurses into the included fragment (which may itself contain `!include`s).
fn resolve_includes(
    yv: &mut serde_yaml_bw::Value,
    including: &Path,
    visited: &mut Vec<PathBuf>,
    depth: usize,
) -> CliResult<()> {
    use serde_yaml_bw::Value as Y;
    match yv {
        Y::Tagged(tagged) => {
            if tagged.tag == "include" {
                let Y::String(rel, _) = &tagged.value else {
                    return Err(CliError::BadInclude {
                        path: including.to_path_buf(),
                        reason: "`!include` payload must be a string path".into(),
                    });
                };
                let dir = including.parent().unwrap_or_else(|| Path::new("."));
                let target = resolve_rel(dir, rel);
                if !target.exists() {
                    return Err(CliError::IncludeNotFound {
                        path: target,
                        referenced_by: including.to_path_buf(),
                    });
                }
                *yv = load_fragment(&target, visited, depth + 1)?;
            } else {
                return Err(CliError::BadInclude {
                    path: including.to_path_buf(),
                    reason: format!(
                        "unsupported YAML tag '{}' (only `!include` is supported)",
                        tagged.tag
                    ),
                });
            }
        }
        Y::Mapping(map) => {
            for v in map.values_mut() {
                resolve_includes(v, including, visited, depth)?;
            }
        }
        Y::Sequence(seq) => {
            for v in seq.iter_mut() {
                resolve_includes(v, including, visited, depth)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Load an `!include` target into a YAML value, resolving its own nested
/// includes. Fragments are raw nodes — `extends`/`profiles` are NOT processed.
/// A fragment's own top-level `extends`/`profiles` keys are therefore passed
/// through as literal data (not stripped, not followed); if such a fragment is
/// spliced where `PipelineConfig` is parsed, the leftover key surfaces as a
/// `deny_unknown_fields` error downstream.
fn load_fragment(
    path: &Path,
    visited: &mut Vec<PathBuf>,
    depth: usize,
) -> CliResult<serde_yaml_bw::Value> {
    if depth > MAX_COMPOSE_DEPTH {
        return Err(CliError::CompositionDepthExceeded {
            max: MAX_COMPOSE_DEPTH,
        });
    }
    let key = cycle_key(path);
    if visited.contains(&key) {
        let mut chain: Vec<String> = visited.iter().map(|p| p.display().to_string()).collect();
        chain.push(key.display().to_string());
        return Err(CliError::CompositionCycle { chain });
    }
    visited.push(key);

    let text = std::fs::read_to_string(path).map_err(|source| CliError::ReadConfig {
        path: path.to_path_buf(),
        source,
    })?;
    let mut yv: serde_yaml_bw::Value = match format_of(path)? {
        Format::Yaml => serde_yaml_bw::from_str(&text).map_err(|e| CliError::ParseConfig {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?,
        Format::Json => {
            let jv: JsonValue = serde_json::from_str(&text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })?;
            serde_yaml_bw::to_value(jv).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: format!("could not convert JSON fragment to YAML: {e}"),
            })?
        }
    };
    resolve_includes(&mut yv, path, visited, depth)?;
    visited.pop();
    Ok(yv)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    #[test]
    fn fast_path_returns_raw_text_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let body = "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n";
        let p = write(dir.path(), "p.yaml", body);
        let out = compose(&p, None).unwrap();
        assert_eq!(
            out, body,
            "no directives + no profile must be byte-identical"
        );
    }

    #[test]
    fn single_extends_child_wins_and_base_keys_survive() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "base.yaml",
            "version: 1\npipeline:\n  source: { type: csv, config: { path: BASE.csv } }\n  sink: { type: jsonl, config: { path: base.jsonl } }\n",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: ./base.yaml\npipeline:\n  source: { config: { path: APP.csv } }\n",
        );
        let text = compose(&app, None).unwrap();
        let v: JsonValue = serde_yaml_bw::from_str(&text).unwrap();
        assert_eq!(v["pipeline"]["source"]["config"]["path"], "APP.csv"); // child wins
        assert_eq!(v["pipeline"]["sink"]["config"]["path"], "base.jsonl"); // base survives
        assert!(v.get("extends").is_none(), "extends must be stripped");
    }

    #[test]
    fn extends_list_merges_left_to_right() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "a.yaml",
            "version: 1\npipeline: { transforms: [] }\nshared: { a: 1, both: \"from-a\" }\n",
        );
        write(dir.path(), "b.yaml", "shared: { b: 2, both: \"from-b\" }\n");
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: [./a.yaml, ./b.yaml]\nshared: { c: 3 }\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, None).unwrap()).unwrap();
        assert_eq!(v["shared"]["a"], 1);
        assert_eq!(v["shared"]["b"], 2);
        assert_eq!(v["shared"]["c"], 3);
        assert_eq!(
            v["shared"]["both"], "from-b",
            "later base wins over earlier"
        );
    }

    #[test]
    fn profile_overlay_beats_extended_base() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "base.yaml",
            "version: 1\npipeline:\n  sink: { type: jsonl, config: { path: base.jsonl } }\n",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: ./base.yaml\nprofiles:\n  prod:\n    pipeline:\n      sink: { config: { path: prod.jsonl } }\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, Some("prod")).unwrap()).unwrap();
        assert_eq!(v["pipeline"]["sink"]["config"]["path"], "prod.jsonl");
        assert!(v.get("profiles").is_none(), "profiles must be stripped");
    }

    #[test]
    fn profiles_stripped_when_no_profile_selected() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            "p.yaml",
            "version: 1\npipeline:\n  sink: { type: jsonl, config: { path: base.jsonl } }\nprofiles:\n  prod: { pipeline: { sink: { config: { path: prod.jsonl } } } }\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&p, None).unwrap()).unwrap();
        assert_eq!(v["pipeline"]["sink"]["config"]["path"], "base.jsonl");
        assert!(v.get("profiles").is_none());
    }

    #[test]
    fn unknown_profile_errors_with_known_list() {
        let dir = tempfile::tempdir().unwrap();
        let p = write(
            dir.path(),
            "p.yaml",
            "version: 1\nprofiles:\n  dev: {}\n  prod: {}\npipeline: {}\n",
        );
        match compose(&p, Some("staging")).unwrap_err() {
            CliError::UnknownProfile { name, known } => {
                assert_eq!(name, "staging");
                assert!(known.contains(&"dev".to_string()) && known.contains(&"prod".to_string()));
            }
            other => panic!("expected UnknownProfile, got {other:?}"),
        }
    }

    #[test]
    fn missing_extends_base_errors() {
        let dir = tempfile::tempdir().unwrap();
        let app = write(dir.path(), "app.yaml", "extends: ./nope.yaml\nversion: 1\n");
        assert!(matches!(
            compose(&app, None).unwrap_err(),
            CliError::IncludeNotFound { .. }
        ));
    }

    #[test]
    fn extends_cycle_errors() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.yaml", "extends: ./b.yaml\nversion: 1\n");
        write(dir.path(), "b.yaml", "extends: ./a.yaml\nversion: 1\n");
        let a = dir.path().join("a.yaml");
        assert!(matches!(
            compose(&a, None).unwrap_err(),
            CliError::CompositionCycle { .. }
        ));
    }

    #[test]
    fn diamond_extends_does_not_false_trigger_cycle() {
        // app extends [b, c]; both b and c extend d. d is reached twice on
        // different branches — that is NOT a cycle.
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "d.yaml",
            "version: 1\nshared: { from_d: true }\n",
        );
        write(
            dir.path(),
            "b.yaml",
            "extends: ./d.yaml\nshared: { from_b: true }\n",
        );
        write(
            dir.path(),
            "c.yaml",
            "extends: ./d.yaml\nshared: { from_c: true }\n",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: [./b.yaml, ./c.yaml]\nshared: { from_app: true }\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, None).unwrap()).unwrap();
        assert_eq!(v["shared"]["from_d"], true);
        assert_eq!(v["shared"]["from_b"], true);
        assert_eq!(v["shared"]["from_c"], true);
        assert_eq!(v["shared"]["from_app"], true);
    }

    #[test]
    fn missing_base_after_successful_base_errors() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "good.yaml", "version: 1\n");
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: [./good.yaml, ./nope.yaml]\n",
        );
        assert!(matches!(
            compose(&app, None).unwrap_err(),
            CliError::IncludeNotFound { .. }
        ));
    }

    #[test]
    fn self_extends_is_a_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(dir.path(), "a.yaml", "extends: ./a.yaml\nversion: 1\n");
        assert!(matches!(
            compose(&a, None).unwrap_err(),
            CliError::CompositionCycle { .. }
        ));
    }

    #[test]
    fn yaml_can_extend_json_base() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "base.json",
            "{ \"version\": 1, \"pipeline\": { \"sink\": { \"type\": \"jsonl\", \"config\": { \"path\": \"base.jsonl\" } } } }",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: ./base.json\npipeline:\n  source: { type: csv, config: { path: a.csv } }\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, None).unwrap()).unwrap();
        assert_eq!(v["pipeline"]["sink"]["config"]["path"], "base.jsonl");
        assert_eq!(v["pipeline"]["source"]["config"]["path"], "a.csv");
    }

    #[test]
    fn include_substitutes_at_nested_map_position() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "auth.yaml",
            "type: bearer\nconfig: { token: T }\n",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "version: 1\npipeline:\n  source:\n    type: rest\n    config: { base_url: https://x }\n    auth: !include ./auth.yaml\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, None).unwrap()).unwrap();
        assert_eq!(v["pipeline"]["source"]["auth"]["type"], "bearer");
        assert_eq!(v["pipeline"]["source"]["auth"]["config"]["token"], "T");
    }

    #[test]
    fn include_substitutes_a_sequence_fragment() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "tx.yaml",
            "- { type: flatten }\n- { type: redact, config: { fields: [ssn] } }\n",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "version: 1\npipeline:\n  source: { type: csv, config: { path: x.csv } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n  transforms: !include ./tx.yaml\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, None).unwrap()).unwrap();
        assert_eq!(v["pipeline"]["transforms"][0]["type"], "flatten");
        assert_eq!(v["pipeline"]["transforms"][1]["type"], "redact");
    }

    #[test]
    fn include_combined_with_extends() {
        let dir = tempfile::tempdir().unwrap();
        write(
            dir.path(),
            "base.yaml",
            "version: 1\npipeline:\n  sink: { type: jsonl, config: { path: base.jsonl } }\n",
        );
        write(
            dir.path(),
            "src.yaml",
            "type: csv\nconfig: { path: from-include.csv }\n",
        );
        let app = write(
            dir.path(),
            "app.yaml",
            "extends: ./base.yaml\npipeline:\n  source: !include ./src.yaml\n",
        );
        let v: JsonValue = serde_yaml_bw::from_str(&compose(&app, None).unwrap()).unwrap();
        assert_eq!(
            v["pipeline"]["source"]["config"]["path"],
            "from-include.csv"
        );
        assert_eq!(v["pipeline"]["sink"]["config"]["path"], "base.jsonl");
    }

    #[test]
    fn include_non_string_payload_errors() {
        let dir = tempfile::tempdir().unwrap();
        let app = write(
            dir.path(),
            "app.yaml",
            "version: 1\nbad: !include { not: a-path }\n",
        );
        assert!(matches!(
            compose(&app, None).unwrap_err(),
            CliError::BadInclude { .. }
        ));
    }

    #[test]
    fn include_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let app = write(
            dir.path(),
            "app.yaml",
            "version: 1\nx: !include ./nope.yaml\n",
        );
        assert!(matches!(
            compose(&app, None).unwrap_err(),
            CliError::IncludeNotFound { .. }
        ));
    }

    #[test]
    fn include_cycle_errors() {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.yaml", "x: !include ./b.yaml\n");
        write(dir.path(), "b.yaml", "y: !include ./a.yaml\n");
        let a = dir.path().join("a.yaml");
        assert!(matches!(
            compose(&a, None).unwrap_err(),
            CliError::CompositionCycle { .. }
        ));
    }

    #[test]
    fn cross_mechanism_extends_include_cycle_errors() {
        // `a extends b`, `b !includes a` — the extends and include stacks are
        // shared, so the loop surfaces as a CompositionCycle (not a confusing
        // leftover-key parse error or an infinite loop).
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "a.yaml", "extends: ./b.yaml\nversion: 1\n");
        write(dir.path(), "b.yaml", "x: !include ./a.yaml\n");
        let a = dir.path().join("a.yaml");
        assert!(matches!(
            compose(&a, None).unwrap_err(),
            CliError::CompositionCycle { .. }
        ));
    }
}

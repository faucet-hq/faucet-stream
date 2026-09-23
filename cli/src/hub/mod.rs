//! Template Hub (#571): source/sink-split pipeline templates composed at run
//! time.
//!
//! - [`spec`] — the two document kinds (`kind: source-template` with
//!   `streams[]` + per-stream `write` preferences; `kind: sink-template` with
//!   `per_stream` addressing) and their validation.
//! - [`mod@compose`] — pure `source × sink → PipelineConfig` composition with
//!   ordered write-mode fallback and per-stream incompatibility errors.
//! - [`catalog`] — a hub directory (`source-templates/`, `sink-templates/`),
//!   its compatibility matrix, the publishability lint, and the renderers
//!   behind the docs page and `index.json`.
//!
//! This module is glue over existing machinery — `pipeline.sources/sinks` +
//! `ref:`, `matrix:` fan-out, transform layering, the `params:` pass, and the
//! registry's write-mode capabilities — so a composed pipeline runs through
//! `expand` / `run_expanded` untouched and inherits every gate they enforce.
//! Zero `faucet-core` changes.

pub mod catalog;
pub mod compose;
pub mod spec;

use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::config::{PipelineConfig, RunInputs};
use crate::error::{CliError, CliResult};

pub use catalog::Catalog;
pub use compose::{Composition, compose};
pub use spec::{SinkTemplate, SourceTemplate, Stream, TemplateKind, WriteChoice};

/// Default hub directory (relative to the working directory) when neither
/// `--hub` nor `FAUCET_HUB` is set.
pub const DEFAULT_HUB_DIR: &str = "hub";

/// Resolve the hub directory: an explicit flag, else `FAUCET_HUB`, else
/// [`DEFAULT_HUB_DIR`].
pub fn hub_dir(flag: Option<&Path>) -> PathBuf {
    if let Some(p) = flag {
        return p.to_path_buf();
    }
    if let Ok(env) = std::env::var("FAUCET_HUB")
        && !env.trim().is_empty()
    {
        return PathBuf::from(env);
    }
    PathBuf::from(DEFAULT_HUB_DIR)
}

/// Read the `kind:` of a hub document without parsing the rest, so the
/// ordinary loaders can recognise a template handed to them by mistake.
pub fn detect_kind(value: &Value) -> Option<TemplateKind> {
    TemplateKind::parse(value.get("kind")?.as_str()?)
}

/// Peek at a config file: if it is a hub template, return its kind so the
/// caller can point the user at `--source` / `--sink` instead of failing on
/// "unknown field `kind`".
pub fn detect_kind_in_file(path: &Path) -> Option<TemplateKind> {
    let text = std::fs::read_to_string(path).ok()?;
    let value = parse_untyped(&text, path).ok()?;
    detect_kind(&value)
}

fn parse_untyped(text: &str, path: &Path) -> CliResult<Value> {
    let is_json = path
        .extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    if is_json {
        serde_json::from_str(text)
            .map_err(|e| CliError::Config(format!("{}: invalid JSON: {e}", path.display())))
    } else {
        serde_yaml::from_str(text)
            .map_err(|e| CliError::Config(format!("{}: invalid YAML: {e}", path.display())))
    }
}

fn read(path: &Path) -> CliResult<String> {
    std::fs::read_to_string(path)
        .map_err(|e| CliError::Config(format!("reading hub template {}: {e}", path.display())))
}

/// Parse + validate a `source-template` file.
pub fn parse_source_file(path: &Path) -> CliResult<SourceTemplate> {
    let value = parse_untyped(&read(path)?, path)?;
    match detect_kind(&value) {
        Some(TemplateKind::SourceTemplate) => {}
        Some(other) => {
            return Err(CliError::Config(format!(
                "{}: is a {}; `--source` needs a source-template",
                path.display(),
                other.as_str()
            )));
        }
        None => {
            return Err(CliError::Config(format!(
                "{}: not a hub template — a source template starts with `kind: source-template`",
                path.display()
            )));
        }
    }
    let t: SourceTemplate = serde_json::from_value(value)
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    t.validate()
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    Ok(t)
}

/// Parse + validate a `sink-template` file.
pub fn parse_sink_file(path: &Path) -> CliResult<SinkTemplate> {
    let value = parse_untyped(&read(path)?, path)?;
    match detect_kind(&value) {
        Some(TemplateKind::SinkTemplate) => {}
        Some(other) => {
            return Err(CliError::Config(format!(
                "{}: is a {}; `--sink` needs a sink-template",
                path.display(),
                other.as_str()
            )));
        }
        None => {
            return Err(CliError::Config(format!(
                "{}: not a hub template — a sink template starts with `kind: sink-template`",
                path.display()
            )));
        }
    }
    let t: SinkTemplate = serde_json::from_value(value)
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    t.validate()
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?;
    Ok(t)
}

/// Turn a `--source` / `--sink` value into a file: an existing path is used
/// as-is; otherwise it is a hub id looked up as `<hub>/<subdir>/<id>.{yaml,yml,json}`.
pub fn resolve_locator(locator: &str, hub: &Path, subdir: &str) -> CliResult<PathBuf> {
    let as_path = Path::new(locator);
    if as_path.is_file() {
        return Ok(as_path.to_path_buf());
    }
    let base = hub.join(subdir);
    for ext in ["yaml", "yml", "json"] {
        let candidate = base.join(format!("{locator}.{ext}"));
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    let known: Vec<String> = std::fs::read_dir(&base)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| {
                    e.path()
                        .file_stem()
                        .map(|s| s.to_string_lossy().into_owned())
                })
                .filter(|s| !s.starts_with('.'))
                .collect()
        })
        .unwrap_or_default();
    let mut known = known;
    known.sort();
    Err(CliError::Config(format!(
        "no hub template '{locator}': not a file, and {} has no {locator}.yaml{}",
        base.display(),
        if known.is_empty() {
            " (set --hub / FAUCET_HUB, or pass a path)".to_string()
        } else {
            format!(" (known: {})", known.join(", "))
        }
    )))
}

/// Load the source template named or pathed by `locator`.
pub fn load_source(locator: &str, hub: &Path) -> CliResult<SourceTemplate> {
    parse_source_file(&resolve_locator(locator, hub, catalog::SOURCE_DIR)?)
}

/// Load the sink template named or pathed by `locator`.
pub fn load_sink(locator: &str, hub: &Path) -> CliResult<SinkTemplate> {
    parse_sink_file(&resolve_locator(locator, hub, catalog::SINK_DIR)?)
}

/// Compose two locators into a [`Composition`].
pub fn compose_locators(source: &str, sink: &str, hub: &Path) -> CliResult<Composition> {
    let s = load_source(source, hub)?;
    let k = load_sink(sink, hub)?;
    compose(&s, &k)
}

/// The synthetic path a composed document is loaded under (error messages
/// and the YAML parser selection).
pub fn composed_path(source: &str, sink: &str) -> PathBuf {
    PathBuf::from(format!("<hub:{source}+{sink}>.yaml"))
}

/// Load a composition as a runnable [`PipelineConfig`] — the same
/// interpolation + params binding a file goes through, plus async secret
/// resolution — so `faucet run --source X --sink Y` is the ordinary run path
/// from here on.
pub async fn load_composed_async(c: &Composition, inputs: &RunInputs) -> CliResult<PipelineConfig> {
    let path = composed_path(&c.source, &c.sink);
    PipelineConfig::from_text_async_with(&c.to_yaml()?, &path, inputs).await
}

/// Offline variant (`faucet validate` / the catalog test): secret directives
/// are rejected rather than resolved.
pub fn load_composed(c: &Composition, inputs: &RunInputs) -> CliResult<PipelineConfig> {
    let path = composed_path(&c.source, &c.sink);
    PipelineConfig::from_text_with(&c.to_yaml()?, &path, inputs)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SRC: &str = "kind: source-template\nname: acme\nsource: {type: rest, config: {base_url: \"https://a\", path: /}}\nstreams: [{name: t}]\n";
    const SINK: &str = "kind: sink-template\nname: files\nsink: {type: jsonl, config: {}}\nper_stream: {path: \"./out/${stream}.jsonl\"}\n";

    fn hub() -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("source-templates")).unwrap();
        std::fs::create_dir_all(d.path().join("sink-templates")).unwrap();
        std::fs::write(d.path().join("source-templates/acme.yaml"), SRC).unwrap();
        std::fs::write(d.path().join("sink-templates/files.yml"), SINK).unwrap();
        d
    }

    #[test]
    fn hub_dir_precedence_flag_env_default() {
        // SAFETY (test): env var private to this test binary.
        unsafe { std::env::remove_var("FAUCET_HUB") };
        assert_eq!(hub_dir(None), PathBuf::from("hub"));
        assert_eq!(hub_dir(Some(Path::new("/x"))), PathBuf::from("/x"));
        unsafe { std::env::set_var("FAUCET_HUB", "/from-env") };
        assert_eq!(hub_dir(None), PathBuf::from("/from-env"));
        assert_eq!(hub_dir(Some(Path::new("/flag"))), PathBuf::from("/flag"));
        unsafe { std::env::remove_var("FAUCET_HUB") };
    }

    #[test]
    fn locators_accept_paths_and_ids_and_name_known_ids_on_miss() {
        let d = hub();
        let by_id = resolve_locator("acme", d.path(), "source-templates").unwrap();
        assert!(by_id.ends_with("source-templates/acme.yaml"));
        let by_yml = resolve_locator("files", d.path(), "sink-templates").unwrap();
        assert!(by_yml.ends_with("files.yml"));
        let by_path = resolve_locator(by_id.to_str().unwrap(), Path::new("/nowhere"), "x").unwrap();
        assert_eq!(by_path, by_id);
        let err = resolve_locator("nope", d.path(), "source-templates")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("no hub template 'nope'") && err.contains("known: acme"),
            "{err}"
        );
        let err = resolve_locator("nope", Path::new("/nowhere"), "source-templates")
            .unwrap_err()
            .to_string();
        assert!(err.contains("set --hub / FAUCET_HUB"), "{err}");
    }

    #[test]
    fn parsers_check_kind_and_validate() {
        let d = hub();
        assert_eq!(load_source("acme", d.path()).unwrap().name, "acme");
        assert_eq!(load_sink("files", d.path()).unwrap().name, "files");
        // Wrong kind for the slot.
        let err = parse_source_file(&d.path().join("sink-templates/files.yml"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("is a sink-template; `--source` needs a source-template"),
            "{err}"
        );
        let err = parse_sink_file(&d.path().join("source-templates/acme.yaml"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("is a source-template; `--sink`"), "{err}");
        // Not a template at all.
        let plain = d.path().join("plain.yaml");
        std::fs::write(&plain, "version: 1\npipeline: {}\n").unwrap();
        assert!(
            parse_source_file(&plain)
                .unwrap_err()
                .to_string()
                .contains("not a hub template")
        );
        assert!(
            parse_sink_file(&plain)
                .unwrap_err()
                .to_string()
                .contains("not a hub template")
        );
        assert_eq!(detect_kind_in_file(&plain), None);
        assert_eq!(
            detect_kind_in_file(&d.path().join("source-templates/acme.yaml")),
            Some(TemplateKind::SourceTemplate)
        );
        assert_eq!(detect_kind_in_file(Path::new("/nope.yaml")), None);
        // Broken YAML and JSON both surface the path.
        std::fs::write(&plain, ": : :").unwrap();
        assert!(
            parse_source_file(&plain)
                .unwrap_err()
                .to_string()
                .contains("invalid YAML")
        );
        let j = d.path().join("t.json");
        std::fs::write(&j, "{").unwrap();
        assert!(
            parse_sink_file(&j)
                .unwrap_err()
                .to_string()
                .contains("invalid JSON")
        );
        std::fs::write(&j, r#"{"kind":"sink-template","name":"j","sink":{"type":"jsonl","config":{}},"per_stream":{"path":"${stream}"}}"#).unwrap();
        assert_eq!(parse_sink_file(&j).unwrap().name, "j");
        assert!(
            parse_source_file(Path::new("/nope.yaml"))
                .unwrap_err()
                .to_string()
                .contains("reading hub template")
        );
    }

    #[test]
    fn compose_locators_yields_a_loadable_pipeline() {
        let d = hub();
        let c = compose_locators("acme", "files", d.path()).unwrap();
        assert_eq!(c.name, "acme");
        let cfg = load_composed(&c, &RunInputs::default()).unwrap();
        assert_eq!(cfg.name.as_deref(), Some("acme"));
        assert_eq!(cfg.matrix.len(), 1);
        assert_eq!(composed_path("a", "b"), PathBuf::from("<hub:a+b>.yaml"));
    }
}

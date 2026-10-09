//! Running a hub template's test bundle (#856) from a catalog directory: the
//! companions it names and the shared suites it requires are read from the
//! same hub, so `faucet hub check` / `lint` (and a hub's CI) run exactly what
//! a registry would after a sync.

use std::path::Path;

use serde_json::Value;

use super::catalog::{SINK_DIR, SOURCE_DIR, load_test_suites};
use super::{DEPLOYMENT_DIR, TemplateKind, detect_kind, resolve_locator};
use crate::error::{CliError, CliResult};
use crate::template_tests::bundle::{TestBundle, bundle_of};
use crate::template_tests::result::BundleOutcome;
use crate::template_tests::run::{SharedSuite, pick, run_bundle};
use crate::template_tests::runner::Target;

fn read(path: &Path) -> CliResult<String> {
    std::fs::read_to_string(path)
        .map_err(|e| CliError::Config(format!("reading {}: {e}", path.display())))
}

fn parse(text: &str, path: &Path) -> CliResult<Value> {
    serde_yaml::from_str(text)
        .map_err(|e| CliError::Config(format!("{}: invalid YAML: {e}", path.display())))
}

/// The bundle a template file carries, with its kind and text.
pub fn file_bundle(path: &Path) -> CliResult<Option<(TemplateKind, String, TestBundle)>> {
    let text = read(path)?;
    let doc = parse(&text, path)?;
    let kind = detect_kind(&doc).unwrap_or(TemplateKind::Pipeline);
    if kind == TemplateKind::TestSuite {
        return Ok(None);
    }
    Ok(bundle_of(&doc, kind)
        .map_err(|e| CliError::Config(format!("{}: {e}", path.display())))?
        .map(|b| (kind, text, b)))
}

/// Resolve the shared suites `bundle` requires from `hub/test-suites/`.
pub fn hub_shared(bundle: &TestBundle, hub: &Path) -> CliResult<Vec<SharedSuite>> {
    if bundle.requires_suites.is_empty() {
        return Ok(Vec::new());
    }
    let all = load_test_suites(hub)?;
    bundle
        .requires_suites
        .iter()
        .map(|r| {
            let official = format!("{}/{}", super::spec::OFFICIAL_OWNER, r.name);
            let candidates = all
                .iter()
                .filter(|(_, t)| t.id() == r.name || t.id() == official)
                .map(|(_, t)| (None, t.clone()))
                .collect();
            pick(r, candidates)
        })
        .collect()
}

fn companion(hub: &Path, id: &str, dir: &str) -> CliResult<String> {
    read(&resolve_locator(id, hub, dir)?)
}

/// The document target for a bundle of `kind` whose subject text is `text`.
pub fn document_target(
    kind: TemplateKind,
    text: &str,
    bundle: &TestBundle,
    hub: &Path,
) -> CliResult<Target<'static>> {
    let overlay_value = |id: &str| -> CliResult<Value> {
        let p = resolve_locator(id, hub, DEPLOYMENT_DIR)?;
        parse(&read(&p)?, &p)
    };
    let need = |field: &Option<String>, what: &str| -> CliResult<String> {
        field
            .clone()
            .ok_or_else(|| CliError::Config(format!("tests: a {kind} bundle needs `{what}:`")))
    };
    Ok(match kind {
        TemplateKind::Pipeline => Target::Document {
            body: text.to_string(),
            sink_body: None,
            overlay: None,
        },
        TemplateKind::SourceTemplate => Target::Document {
            body: text.to_string(),
            sink_body: Some(companion(hub, &need(&bundle.sink, "sink")?, SINK_DIR)?),
            overlay: bundle.overlay.as_deref().map(overlay_value).transpose()?,
        },
        TemplateKind::SinkTemplate => Target::Document {
            body: companion(hub, &need(&bundle.source, "source")?, SOURCE_DIR)?,
            sink_body: Some(text.to_string()),
            overlay: None,
        },
        TemplateKind::Deployment => Target::Document {
            body: companion(hub, &need(&bundle.source, "source")?, SOURCE_DIR)?,
            sink_body: Some(companion(hub, &need(&bundle.sink, "sink")?, SINK_DIR)?),
            overlay: Some(parse(text, Path::new("deployment"))?),
        },
        TemplateKind::TestSuite => {
            return Err(CliError::Config(
                "a test-suite has no bundle of its own".into(),
            ));
        }
    })
}

/// The hub a template file lives in: the parent of its `source-templates/`
/// (or `sink-templates/`, `deployments/`, `test-suites/`) directory, else
/// `fallback`.
pub fn hub_root_for(path: &Path, fallback: &Path) -> std::path::PathBuf {
    const DIRS: &[&str] = &[
        SOURCE_DIR,
        SINK_DIR,
        DEPLOYMENT_DIR,
        super::catalog::TEST_SUITE_DIR,
    ];
    path.ancestors()
        .find(|a| {
            a.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| DIRS.contains(&n))
        })
        .and_then(Path::parent)
        .map_or_else(|| fallback.to_path_buf(), Path::to_path_buf)
}

/// Run the bundle of the template at `path`, resolving companions and shared
/// suites from `hub`. `Ok(None)` when the file carries no `tests:` block; a
/// companion or suite that does not resolve is a failed outcome.
pub async fn run_file(path: &Path, hub: &Path) -> CliResult<Option<BundleOutcome>> {
    let Some((kind, text, bundle)) = file_bundle(path)? else {
        return Ok(None);
    };
    Ok(Some(run_parsed(kind, &text, &bundle, hub, None).await))
}

/// Run an already-read bundle of a `kind` document whose text is `text`.
pub async fn run_parsed(
    kind: TemplateKind,
    text: &str,
    bundle: &TestBundle,
    hub: &Path,
    filter: Option<&str>,
) -> BundleOutcome {
    let prepared = document_target(kind, text, bundle, hub)
        .and_then(|t| hub_shared(bundle, hub).map(|s| (t, s)));
    match prepared {
        Ok((t, shared)) => run_bundle(bundle, &t, &shared, filter).await,
        Err(e) => BundleOutcome {
            error: Some(e.to_string()),
            ..Default::default()
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, rel: &str, body: &str) -> std::path::PathBuf {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, body).unwrap();
        p
    }

    const SOURCE: &str = r#"kind: source-template
name: acme
description: acme exports
params:
  region: { type: string, default: us, values: [us, eu] }
source:
  type: rest
  config:
    base_url: "https://${param.region}.example.com"
streams:
  - name: orders
    source: { config: { path: /orders } }
tests:
  sink: files
  suite:
    auto: { enum_coverage: true, defaults_baseline: true }
  requires_suites:
    - { name: rest-conformance, version: ">=1.0,<2" }
"#;

    const SINK: &str = r#"kind: sink-template
name: files
description: local files
sink:
  type: jsonl
  config: {}
per_stream:
  path: "./out/${source}/${stream}.jsonl"
write_mode_aliases: { overwrite: append }
tests:
  source: acme
  fixtures:
    - name: rows-pass-through
      input: [{ "id": 1 }]
      expect: { records_written: 1 }
"#;

    const SUITE: &str = r#"kind: test-suite
name: rest-conformance
release: 1.2.0
suite:
  auto: { defaults_baseline: true }
"#;

    fn hub() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        write(dir.path(), "source-templates/acme.yaml", SOURCE);
        write(dir.path(), "sink-templates/files.yaml", SINK);
        write(dir.path(), "test-suites/rest-conformance.yaml", SUITE);
        dir
    }

    #[tokio::test]
    async fn a_source_bundle_composes_with_its_sink_and_runs_the_shared_suite() {
        let dir = hub();
        let out = run_file(&dir.path().join("source-templates/acme.yaml"), dir.path())
            .await
            .unwrap()
            .expect("has a bundle");
        assert!(out.passed(), "{out:#?}");
        assert_eq!(out.suites[0].release, "1.2.0");
        assert!(
            out.cases
                .iter()
                .any(|c| c.name == "rest-conformance@1.2.0:auto:defaults")
        );
    }

    #[tokio::test]
    async fn a_sink_bundle_runs_against_its_source() {
        let dir = hub();
        let out = run_file(&dir.path().join("sink-templates/files.yaml"), dir.path())
            .await
            .unwrap()
            .unwrap();
        assert!(out.passed(), "{out:#?}");
    }

    #[tokio::test]
    async fn a_range_no_hub_suite_satisfies_is_a_failed_outcome_naming_it() {
        let dir = hub();
        write(
            dir.path(),
            "test-suites/rest-conformance.yaml",
            &SUITE.replace("1.2.0", "2.0.0"),
        );
        let out = run_file(&dir.path().join("source-templates/acme.yaml"), dir.path())
            .await
            .unwrap()
            .unwrap();
        let e = out.error.expect("unsatisfied");
        assert!(e.contains("rest-conformance") && e.contains(">=1.0,<2"), "{e}");
    }

    #[tokio::test]
    async fn a_missing_companion_is_a_failed_outcome() {
        let dir = hub();
        std::fs::remove_file(dir.path().join("sink-templates/files.yaml")).unwrap();
        let out = run_file(&dir.path().join("source-templates/acme.yaml"), dir.path())
            .await
            .unwrap()
            .unwrap();
        assert!(out.error.is_some());
    }

    #[tokio::test]
    async fn a_file_without_tests_or_a_suite_file_has_no_bundle() {
        let dir = hub();
        let plain = write(
            dir.path(),
            "plain.yaml",
            "kind: pipeline\nversion: 1\npipeline: {}\n",
        );
        assert!(run_file(&plain, dir.path()).await.unwrap().is_none());
        assert!(
            run_file(&dir.path().join("test-suites/rest-conformance.yaml"), dir.path())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_deployment_bundle_applies_itself_over_the_pairing() {
        let dir = hub();
        let p = write(
            dir.path(),
            "deployments/ops.yaml",
            r#"kind: deployment
name: ops
state: { type: memory }
tests:
  source: acme
  sink: files
  suite:
    auto: { defaults_baseline: true }
"#,
        );
        let out = run_file(&p, dir.path()).await.unwrap().unwrap();
        assert!(out.passed(), "{out:#?}");
    }

    #[test]
    fn the_hub_root_is_found_from_a_template_path() {
        let fb = Path::new("/fallback");
        assert_eq!(
            hub_root_for(Path::new("/h/source-templates/acme/x.yaml"), fb),
            Path::new("/h")
        );
        assert_eq!(hub_root_for(Path::new("/elsewhere/x.yaml"), fb), fb);
    }

    #[test]
    fn a_target_refuses_a_test_suite_and_a_missing_companion() {
        let dir = hub();
        let b = TestBundle::default();
        assert!(document_target(TemplateKind::TestSuite, "", &b, dir.path()).is_err());
        let err = document_target(TemplateKind::SinkTemplate, "", &b, dir.path())
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("needs `source:`"), "{err}");
        assert!(matches!(
            document_target(TemplateKind::Pipeline, "x", &b, dir.path()).unwrap(),
            Target::Document { .. }
        ));
    }
}

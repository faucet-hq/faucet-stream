//! Verify every shipped example YAML loads and expands cleanly.
//!
//! Guards against accidental schema-changes (named-templates feature, etc.)
//! breaking real-world configs. Any example that fails this test is a
//! backwards-compatibility regression.
//!
//! Examples that reference `${env:VAR}` or `${file:PATH}` placeholders
//! require credentials that are not available in test environments. Those
//! load errors are treated as "skipped" — the test only fails on structural
//! errors (bad YAML, unknown fields, expand failures) that indicate a
//! backwards-compatibility regression.

use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::expand::expand;
use std::path::PathBuf;

#[path = "support/example_features.rs"]
mod example_features;

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples")
}

/// Returns `true` when the error is a missing credential (env var / file) or a
/// secrets-manager directive that the synchronous loader cannot resolve — both
/// are expected in environments without those resources and should be skipped.
/// (Examples that reference `${vault:…}` etc. are still structurally validated
/// by the `shipped_example_yamls_pass_validate` test via `validate --no-secrets`.)
fn is_credential_error(e: &CliError) -> bool {
    matches!(
        e,
        CliError::MissingEnvVar { .. }
            | CliError::ReadInterpolatedFile { .. }
            | CliError::SecretsRequireAsyncLoad
    )
}

#[test]
fn every_example_loads_and_expands() {
    let dir = examples_dir();
    let mut count = 0;
    let mut skipped = 0;
    let mut failures: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("examples dir exists") {
        let path = entry.expect("readable dir entry").path();
        let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
            continue;
        };
        if !matches!(ext, "yaml" | "yml" | "json") {
            continue;
        }
        // serve_minimal.yaml is a `faucet serve --default-config` partial
        // (workspace defaults only, no source/sink), so it does not expand on
        // its own — it is merged under each submitted run at request time.
        if path.file_name().and_then(|f| f.to_str()) == Some("serve_minimal.yaml") {
            skipped += 1;
            continue;
        }
        // Skip examples that rely on a feature not compiled into this test
        // binary (shared with `cli_end_to_end`). In CI `--all-features`
        // covers everything.
        if example_features::missing_feature(&std::fs::read_to_string(&path).unwrap_or_default())
            .is_some()
        {
            skipped += 1;
            continue;
        }
        count += 1;
        // Load exactly as `faucet validate` does: an example may declare typed
        // `params:` whose values arrive at trigger time, so required params bind
        // to type-shaped placeholders rather than failing the structural check
        // (#444).
        let cfg = match PipelineConfig::from_path_with(
            &path,
            None,
            &faucet_cli::config::RunInputs::placeholders(),
        ) {
            Ok(c) => c,
            Err(ref e) if is_credential_error(e) => {
                // Credential not available in this environment — skip.
                skipped += 1;
                continue;
            }
            Err(e) => {
                failures.push(format!("load {}: {e}", path.display()));
                continue;
            }
        };
        // Topology-mode examples (`pipeline.nodes`) are not matrix rows, so
        // `expand` (matrix-only) does not apply. The parse above already
        // validated their structure; graph validation is covered by
        // `topology_end_to_end.rs`.
        if !cfg.pipeline.nodes.is_empty() {
            continue;
        }
        if let Err(e) = expand(&cfg) {
            failures.push(format!("expand {}: {e}", path.display()));
        }
    }
    assert!(count > 10, "expected to find >10 examples, found {count}");
    eprintln!(
        "examples: {count} total, {skipped} skipped (missing credentials), {} checked",
        count - skipped
    );
    assert!(
        failures.is_empty(),
        "{} example(s) failed:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The `serve --default-config` partial is excluded from the expand loop above
/// (no source/sink), so check it parses as a structurally valid `PipelineConfig`
/// here — this still catches bad YAML / unknown fields in the example.
#[test]
fn serve_minimal_default_config_parses() {
    let path = examples_dir().join("serve_minimal.yaml");
    PipelineConfig::from_path(&path, None)
        .expect("serve_minimal.yaml must parse as a valid PipelineConfig");
}

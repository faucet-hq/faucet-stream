//! `faucet completions <shell>` — emit a static shell-completion script — plus
//! the **dynamic** (runtime) candidate providers used by `clap_complete`'s
//! `CompleteEnv` hook (#383).
//!
//! Two layers:
//!
//! - **Static**: [`run`] writes a completion script for the requested shell to
//!   stdout via [`clap_complete::aot::generate`]. It covers subcommand names,
//!   flag names, fixed-choice value enums, and file-path arguments.
//! - **Dynamic**: the `*_candidates` functions are attached to specific args
//!   with `#[arg(add = ArgValueCandidates::new(...))]` in [`crate::cli`]. When
//!   the shell calls back into the binary at completion time (the `COMPLETE`
//!   env hook wired in [`crate::run_main`]), these compute candidates from the
//!   **compiled registry** (connector kinds) and the **config on disk** (matrix
//!   row ids / tags).
//!
//! **Safety contract:** every dynamic provider must be fast, panic-free, and
//! side-effect-free — no network, no state writes, no connector construction.
//! Connector kinds are compile-time. The config-derived providers only *read*
//! the local config and run the pure [`crate::expand::expand`] pass (which does
//! not build sources or resolve `${secret:}` / `${env:}` directives), returning
//! an empty list on any error.

use crate::CliResult;
use crate::cli::Cli;
use clap::CommandFactory;
use clap_complete::aot::{Shell, generate};
use clap_complete::engine::CompletionCandidate;
use std::io;

/// Emit a static completion script for `shell` to stdout.
pub fn run(shell: Shell) -> CliResult<()> {
    write_completions(shell, &mut io::stdout());
    Ok(())
}

/// Write the completion script for `shell` into `out`. Split from [`run`] so it
/// is testable against an in-memory buffer (rather than the process's stdout).
fn write_completions(shell: Shell, out: &mut impl io::Write) {
    let mut cmd = Cli::command();
    generate(shell, &mut cmd, "faucet", out);
}

// ── Dynamic candidate providers ─────────────────────────────────────────────

/// Compiled-in source connector kinds (e.g. `rest`, `postgres`, `s3`).
pub(crate) fn source_kind_candidates() -> Vec<CompletionCandidate> {
    crate::registry::source_kinds()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

/// Compiled-in sink connector kinds (e.g. `jsonl`, `bigquery`, `s3`).
pub(crate) fn sink_kind_candidates() -> Vec<CompletionCandidate> {
    crate::registry::sink_kinds()
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

/// Compiled-in transform kinds, with their one-line descriptions as help.
pub(crate) fn transform_candidates() -> Vec<CompletionCandidate> {
    crate::transforms::transform_descriptions()
        .into_iter()
        .map(|(kind, desc)| CompletionCandidate::new(kind).help(Some(desc.into())))
        .collect()
}

/// The source-readiness ladder used by `--status` (#371).
pub(crate) fn status_candidates() -> Vec<CompletionCandidate> {
    ["mandatory", "active", "available", "draft", "archived"]
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

/// Matrix row ids from the config discovered in the current directory, for
/// `--select` / `--only` / `--skip`. Empty when no config is found or it does
/// not parse/expand.
pub(crate) fn matrix_id_candidates() -> Vec<CompletionCandidate> {
    let dir = match std::env::current_dir() {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    expanded_ids_from_dir(&dir)
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

/// Distinct tags present across the discovered config's rows, for `--tag`
/// (#376). Empty when no config is found or it does not parse/expand.
pub(crate) fn tag_candidates() -> Vec<CompletionCandidate> {
    let dir = match std::env::current_dir() {
        Ok(d) => d,
        Err(_) => return Vec::new(),
    };
    expanded_tags_from_dir(&dir)
        .into_iter()
        .map(CompletionCandidate::new)
        .collect()
}

/// Best-effort: discover + parse + expand the config in `dir` and return its
/// node ids. Any failure (no config, parse error, expand error) yields an empty
/// list — completion must never error.
fn expanded_ids_from_dir(dir: &std::path::Path) -> Vec<String> {
    load_expanded_from_dir(dir)
        .map(|nodes| nodes.into_iter().map(|n| n.id).collect())
        .unwrap_or_default()
}

/// Best-effort distinct, sorted tags across `dir`'s config rows.
fn expanded_tags_from_dir(dir: &std::path::Path) -> Vec<String> {
    let mut tags: Vec<String> = load_expanded_from_dir(dir)
        .map(|nodes| nodes.into_iter().flat_map(|n| n.tags).collect())
        .unwrap_or_default();
    tags.sort();
    tags.dedup();
    tags
}

/// Discover the config in `dir`, parse it, and run the pure expansion pass.
/// Returns `None` on any error (completion is best-effort). Does no I/O beyond
/// reading the config file and does not resolve secrets/env or build sources.
///
/// Takes an explicit `dir` (rather than reading `$PWD` internally) so it is
/// testable without mutating the process-global current directory.
fn load_expanded_from_dir(dir: &std::path::Path) -> Option<Vec<crate::expand::ExpandedNode>> {
    let path = crate::env_loader::discover_config_path(dir)?;
    let text = std::fs::read_to_string(&path).ok()?;
    let cfg = crate::config::PipelineConfig::from_text(&text, &path).ok()?;
    crate::expand::expand(&cfg).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(cands: &[CompletionCandidate]) -> Vec<String> {
        cands
            .iter()
            .map(|c| c.get_value().to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn source_and_sink_kinds_match_registry() {
        let src = labels(&source_kind_candidates());
        assert_eq!(src, crate::registry::source_kinds());
        let sink = labels(&sink_kind_candidates());
        assert_eq!(sink, crate::registry::sink_kinds());
        // The default build always compiles the REST source + jsonl sink.
        assert!(src.contains(&"rest".to_string()));
        assert!(sink.contains(&"jsonl".to_string()));
    }

    #[test]
    fn transform_candidates_cover_registry() {
        let got = labels(&transform_candidates());
        let expected: Vec<String> = crate::transforms::transform_descriptions()
            .into_iter()
            .map(|(k, _)| k.to_string())
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn status_candidates_are_the_readiness_ladder() {
        assert_eq!(
            labels(&status_candidates()),
            vec!["mandatory", "active", "available", "draft", "archived"]
        );
    }

    #[test]
    fn config_providers_are_empty_without_a_config() {
        // A scratch dir with no faucet.{yaml,yml,json}: both config-derived
        // providers return empty rather than erroring. Uses an explicit dir
        // (not `set_current_dir`) so it is race-free under parallel tests.
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(expanded_ids_from_dir(dir.path()).is_empty());
        assert!(expanded_tags_from_dir(dir.path()).is_empty());
    }

    #[test]
    fn matrix_ids_and_tags_from_a_config_fixture() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cfg = r#"
version: 1
name: demo
pipeline:
  source: { type: rest, config: { url: "https://example.com" } }
  sink: { type: jsonl, config: { path: out.jsonl, append: true } }
matrix:
  - id: alpha
    tags: [daily, us]
  - id: beta
    tags: [daily]
"#;
        std::fs::write(dir.path().join("faucet.yaml"), cfg).expect("write cfg");
        let ids = expanded_ids_from_dir(dir.path());
        let tags = expanded_tags_from_dir(dir.path());

        assert!(ids.contains(&"alpha".to_string()), "ids: {ids:?}");
        assert!(ids.contains(&"beta".to_string()), "ids: {ids:?}");
        // Tags are the distinct, sorted union across rows.
        assert_eq!(tags, vec!["daily", "us"]);
    }

    #[test]
    fn static_generation_produces_a_nonempty_script() {
        // Exercise the static generator for every supported shell, into a
        // buffer (no stdout), via the same helper `run` delegates to.
        for shell in [
            Shell::Bash,
            Shell::Zsh,
            Shell::Fish,
            Shell::PowerShell,
            Shell::Elvish,
        ] {
            let mut buf: Vec<u8> = Vec::new();
            write_completions(shell, &mut buf);
            let script = String::from_utf8(buf).expect("utf8 script");
            assert!(
                script.contains("faucet"),
                "{shell} script should mention the binary name"
            );
            assert!(!script.is_empty());
        }
    }

    #[test]
    fn run_emits_a_script_and_succeeds() {
        // Covers the stdout wrapper `run` itself; the script is written to the
        // test harness's captured stdout.
        run(Shell::Bash).expect("completions run should succeed");
    }

    #[test]
    fn cwd_providers_are_best_effort_and_never_panic() {
        // The public providers read the process's current directory. From an
        // arbitrary cwd (no fixture) they must return a (possibly empty)
        // candidate list without panicking — exercising the `current_dir()`
        // path + the map that `#[arg(add = …)]` calls at completion time.
        // Running these to completion without panicking IS the assertion (the
        // documented best-effort contract); binding the results keeps the calls
        // from being optimized away.
        let _ids = matrix_id_candidates();
        let _tags = tag_candidates();
    }
}

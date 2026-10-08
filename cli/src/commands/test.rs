//! `faucet test` — run fixture-based offline pipeline tests (#210).
//!
//! Loads one or more spec files, resolves each case's pipeline logic (from a
//! referenced config file or the inline `pipeline:` block), streams the
//! fixture records through the real pipeline pass chain with in-memory
//! source/sink/DLQ, and reports pass/fail per case. Exits non-zero (the
//! failed-case count) when any case fails, so CI can gate on it.

use crate::cli::TestArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::expand;
use crate::pipeline_test::report::{CaseOutcome, TestReport};
use crate::pipeline_test::runner::{ResolvedCase, run_case};
use crate::pipeline_test::spec::{TestCase, load_spec};
use crate::pipeline_test::{diff, fixtures};
use chrono::{DateTime, FixedOffset};
use std::path::Path;

/// Execute the `test` subcommand.
pub async fn run(args: TestArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let default_clock = crate::commands::run::resolve_run_clock(args.clock.as_deref())?;

    let mut outcomes: Vec<CaseOutcome> = Vec::new();
    for spec_path in &args.specs {
        let spec = load_spec(spec_path)?;
        let spec_dir = spec_path.parent().unwrap_or(Path::new("."));
        for case in &spec.tests {
            if let Some(f) = &args.filter
                && !case.name.contains(f.as_str())
            {
                continue;
            }
            let resolved = resolve_case(case, spec_path, spec_dir, default_clock, &args).await?;
            let run = run_case(&resolved).await?;
            let failures = diff::evaluate(&case.expect, &run);
            outcomes.push(CaseOutcome::new(
                case.name.clone(),
                spec_path.display().to_string(),
                failures,
            ));
        }
    }

    if outcomes.is_empty() {
        return Err(CliError::Config(match &args.filter {
            Some(f) => format!("no test cases match --filter '{f}'"),
            None => "no test cases found in the given spec file(s)".to_string(),
        }));
    }

    let report = TestReport::new(outcomes);
    if args.json {
        println!("{}", report.render_json());
    } else {
        print!("{}", report.render_human());
    }
    if report.failed > 0 {
        return Err(CliError::TestsFailed {
            failed: report.failed,
        });
    }
    Ok(())
}

/// Resolve a case's pipeline logic + fixtures into the runner's input.
async fn resolve_case(
    case: &TestCase,
    spec_path: &Path,
    spec_dir: &Path,
    default_clock: DateTime<FixedOffset>,
    args: &TestArgs,
) -> CliResult<ResolvedCase> {
    let at = |msg: String| {
        CliError::Config(format!(
            "{}: test '{}': {msg}",
            spec_path.display(),
            case.name
        ))
    };
    let clock = match &case.clock {
        Some(s) => crate::commands::run::resolve_run_clock(Some(s))
            .map_err(|e| at(format!("clock: {e}")))?,
        None => default_clock,
    };
    let input = fixtures::load_input(spec_dir, &case.input)?;

    let resolved = match (&case.config, &case.pipeline) {
        (Some(config_rel), None) => {
            let config_path = spec_dir.join(config_rel);
            // Offline by default: leave `${vault:…}`-style directives
            // unresolved — the source/sink configs that hold them are
            // replaced by fixtures anyway. `--resolve-secrets` opts into the
            // real (network) resolution for the rare secret inside a
            // transform/quality/contract block.
            let cfg = if args.resolve_secrets {
                PipelineConfig::from_path_async(&config_path, args.profile.as_deref()).await?
            } else {
                PipelineConfig::from_path_tolerating_secrets(&config_path, args.profile.as_deref())?
            };
            let nodes = expand(&crate::partition::offline(&cfg))?;
            let node = match &case.row {
                Some(row) => nodes.iter().find(|n| &n.id == row).ok_or_else(|| {
                    at(format!(
                        "row '{row}' not found in '{}' — available rows: {}",
                        config_path.display(),
                        ids(&nodes)
                    ))
                })?,
                None if nodes.len() == 1 => &nodes[0],
                None => {
                    return Err(at(format!(
                        "'{}' expands to {} invocations — set `row` to one of: {}",
                        config_path.display(),
                        nodes.len(),
                        ids(&nodes)
                    )));
                }
            };
            if node.schema.is_some() {
                tracing::warn!(
                    test = %case.name,
                    "the config's `schema:` (drift) block is inert in `faucet test` — \
                     there is no destination schema offline"
                );
            }
            ResolvedCase {
                name: case.name.clone(),
                transforms: node.transforms.clone(),
                #[cfg(feature = "quality")]
                quality: node.quality.clone(),
                #[cfg(feature = "contract")]
                contract: node.contract.clone(),
                #[cfg(feature = "masking")]
                masking: node.masking.clone(),
                input,
                page_size: case.page_size,
                clock,
            }
        }
        (None, Some(inline)) => ResolvedCase {
            name: case.name.clone(),
            transforms: inline.transforms.clone(),
            #[cfg(feature = "quality")]
            quality: inline.quality.clone(),
            #[cfg(feature = "contract")]
            contract: inline.contract.clone(),
            #[cfg(feature = "masking")]
            masking: inline.masking.clone(),
            input,
            page_size: case.page_size,
            clock,
        },
        // Spec validation guarantees exactly one of config/pipeline is set.
        _ => unreachable!("spec validation enforces config XOR pipeline"),
    };
    Ok(resolved)
}

fn ids(nodes: &[crate::expand::ExpandedNode]) -> String {
    nodes
        .iter()
        .map(|n| n.id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

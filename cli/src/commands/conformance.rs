//! `faucet conformance` — score connectors against the faucet SDK contract and
//! report a maturity tier + capabilities (#330).

use crate::cli::ConformanceArgs;
use crate::conformance::{Report, Tier, build_reports};
use crate::error::{CliError, CliResult};

/// Execute the `conformance` command.
pub async fn run(args: ConformanceArgs) -> CliResult<()> {
    // `--matrix` prints the generated capability matrix and exits — the scoring
    // flags do not apply.
    if args.export {
        return export(args.require_all);
    }
    if args.matrix {
        print!("{}", crate::conformance::capability_matrix_markdown());
        return Ok(());
    }
    let kind_filter = match args.kind.as_deref() {
        None => None,
        Some("source") => Some("source"),
        Some("sink") => Some("sink"),
        Some(other) => {
            return Err(CliError::Config(format!(
                "--kind must be 'source' or 'sink' (got '{other}')"
            )));
        }
    };

    // Parse the optional `--min-tier` gate up front so a typo fails fast.
    let min_tier = match args.min_tier.as_deref() {
        None => None,
        Some(s) => Some(Tier::parse(s).ok_or_else(|| {
            CliError::Config(format!(
                "--min-tier must be one of stable/experimental/beta/draft (got '{s}')"
            ))
        })?),
    };

    let mut reports: Vec<Report> = build_reports()
        .into_iter()
        .filter(|r| kind_filter.is_none_or(|k| r.kind == k))
        .filter(|r| args.name.as_deref().is_none_or(|n| r.name == n))
        .collect();
    reports.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.name.cmp(&b.name)));

    if reports.is_empty() {
        if let Some(name) = &args.name {
            return Err(CliError::Config(format!(
                "no connector named '{name}' is compiled into this binary (try `faucet list`)"
            )));
        }
        return Err(CliError::Config(
            "no connectors matched the filter".to_string(),
        ));
    }

    if args.json {
        let json = serde_json::to_string_pretty(&reports)
            .map_err(|e| CliError::Config(format!("serialize conformance report: {e}")))?;
        println!("{json}");
    } else if args.name.is_some() {
        // A single named connector → detailed scorecard.
        for r in &reports {
            println!(
                "{} {} ({}) — {} · {}/100",
                r.tier.badge(),
                r.name,
                r.kind,
                r.tier.label(),
                r.score
            );
            for d in &r.dimensions {
                println!(
                    "  {} {:<24} (+{:>2})  {}",
                    if d.met { "✓" } else { "·" },
                    d.name,
                    d.points,
                    d.note
                );
            }
            if !r.badges.is_empty() {
                println!("  capabilities: {}", r.badges.join(", "));
            }
            println!("  badge: {}", r.tier.badge_url());
        }
    } else {
        // All connectors → one line each, highest score first.
        println!(
            "faucet connector conformance  ({} connectors)\n",
            reports.len()
        );
        for r in &reports {
            let caps = if r.badges.is_empty() {
                String::new()
            } else {
                format!("  · {}", r.badges.join(", "))
            };
            println!(
                "{} {:<15} {:<7} {:>3}/100  {}{}",
                r.tier.badge(),
                r.name,
                r.kind,
                r.score,
                r.tier.label(),
                caps
            );
        }
        let stable = reports
            .iter()
            .filter(|r| matches!(r.tier, Tier::Stable))
            .count();
        println!("\n{}/{} connectors at Stable.", stable, reports.len());
    }

    // `--min-tier` gate: fail if any scored connector is below the bar.
    if let Some(min) = min_tier {
        let below: Vec<&Report> = reports
            .iter()
            .filter(|r| r.tier.rank() < min.rank())
            .collect();
        if !below.is_empty() {
            let names: Vec<String> = below
                .iter()
                .map(|r| format!("{} {} ({})", r.name, r.kind, r.tier.label()))
                .collect();
            return Err(CliError::Config(format!(
                "{} connector(s) below the required `{}` tier: {}",
                below.len(),
                min.label(),
                names.join(", ")
            )));
        }
    }

    Ok(())
}

/// `faucet conformance --export`: print the Connector Hub export.
fn export(require_all: bool) -> CliResult<()> {
    let doc =
        crate::connector_export::build_export(&crate::registry_index::RegistryIndex::embedded());
    let missing = crate::connector_export::missing(&doc);
    if require_all && !missing.is_empty() {
        return Err(CliError::Config(format!(
            "{} registry connector(s) are not compiled into this binary: {}",
            missing.len(),
            missing.join(", ")
        )));
    }
    let json = serde_json::to_string_pretty(&doc)
        .map_err(|e| CliError::Config(format!("serialize connector export: {e}")))?;
    println!("{json}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(name: Option<&str>, kind: Option<&str>, json: bool) -> ConformanceArgs {
        ConformanceArgs {
            name: name.map(str::to_string),
            kind: kind.map(str::to_string),
            all: false,
            json,
            min_tier: None,
            matrix: false,
            export: false,
            require_all: false,
        }
    }

    #[tokio::test]
    async fn matrix_flag_prints_and_exits_ok() {
        let mut a = args(None, None, false);
        a.matrix = true;
        assert!(run(a).await.is_ok());
    }

    #[tokio::test]
    async fn export_flag_prints_and_exits_ok() {
        let mut a = args(None, None, false);
        a.export = true;
        assert!(run(a).await.is_ok());
    }

    #[test]
    fn generated_matrix_has_expected_cells() {
        let m = crate::conformance::capability_matrix_markdown();
        assert!(m.contains("# Connector capability matrix"));
        // A capable sink row and an append-only sink's absence from the sink rows.
        assert!(
            m.contains("| `postgres` | ✓ | ✓ | ✓ |"),
            "postgres row: {m}"
        );
        assert!(
            m.contains("| `iceberg` | ✓ |  | ✓ |"),
            "iceberg is append-only: {m}"
        );
        // jsonl is not a capable sink, so it never appears as a matrix row.
        assert!(!m.contains("| `jsonl` |"), "jsonl has no capability: {m}");
    }

    #[tokio::test]
    async fn runs_over_all_connectors() {
        assert!(run(args(None, None, false)).await.is_ok());
    }

    #[tokio::test]
    async fn runs_json_with_source_filter() {
        assert!(run(args(None, Some("source"), true)).await.is_ok());
    }

    #[tokio::test]
    async fn single_connector_detailed_view() {
        // postgres is compiled into the default test build.
        assert!(run(args(Some("postgres"), None, false)).await.is_ok());
    }

    #[tokio::test]
    async fn unknown_connector_is_an_error() {
        assert!(
            run(args(Some("definitely-not-a-connector"), None, false))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn bad_kind_is_an_error() {
        assert!(run(args(None, Some("neither"), false)).await.is_err());
    }

    #[tokio::test]
    async fn min_tier_stable_passes_for_builtins() {
        // Every built-in scores Stable, so the strictest gate must pass.
        let mut a = args(None, None, false);
        a.min_tier = Some("stable".into());
        assert!(run(a).await.is_ok());
    }

    #[tokio::test]
    async fn bad_min_tier_is_an_error() {
        let mut a = args(None, None, true);
        a.min_tier = Some("platinum".into());
        assert!(run(a).await.is_err());
    }
}

//! `faucet hub` — the Template Hub CLI (#571): compose a source template
//! with a sink template, check a pairing, list a catalog, render its
//! compatibility matrix, and lint templates for publication.

use std::path::Path;

use crate::cli::{
    HubArgs, HubCheckArgs, HubCommand, HubComposeArgs, HubLintArgs, HubListArgs, HubMatrixArgs,
    MatrixFormat,
};
use crate::error::{CliError, CliResult};
use crate::hub::{self, Catalog};

pub async fn run(args: HubArgs) -> CliResult<()> {
    match args.command {
        HubCommand::Compose(a) => compose(a),
        HubCommand::Check(a) => check(a),
        HubCommand::List(a) => list(a),
        HubCommand::Matrix(a) => matrix(a),
        HubCommand::Lint(a) => lint(a),
    }
}

fn pretty<T: serde::Serialize>(v: &T) -> CliResult<String> {
    serde_json::to_string_pretty(v)
        .map_err(|e| CliError::Internal(format!("hub: rendering JSON: {e}")))
}

/// `faucet hub compose --source X --sink Y [--out F] [--json]` — print (or
/// write) the composed pipeline config. The output is an ordinary config:
/// `faucet validate` / `run` / `template register` all take it.
fn compose(a: HubComposeArgs) -> CliResult<()> {
    let hub_dir = hub::hub_dir(a.pair.hub.as_deref());
    let c = hub::compose_locators(&a.pair.source, &a.pair.sink, &hub_dir)?;
    if a.json {
        println!("{}", pretty(&c)?);
        return Ok(());
    }
    let yaml = c.to_yaml()?;
    match &a.out {
        Some(p) => {
            std::fs::write(p, &yaml)
                .map_err(|e| CliError::Config(format!("writing {}: {e}", p.display())))?;
            eprintln!(
                "composed {} × {} → {} ({} stream(s))",
                c.source,
                c.sink,
                p.display(),
                c.streams.len()
            );
        }
        None => print!("{yaml}"),
    }
    Ok(())
}

/// `faucet hub check --source X --sink Y` — the per-stream write-mode
/// resolution for one pairing; exits non-zero when any stream is
/// incompatible.
fn check(a: HubCheckArgs) -> CliResult<()> {
    let hub_dir = hub::hub_dir(a.pair.hub.as_deref());
    let s = hub::load_source(&a.pair.source, &hub_dir)?;
    let k = hub::load_sink(&a.pair.sink, &hub_dir)?;
    let cell = hub::catalog::cell(&s, &k);
    if a.json {
        println!("{}", pretty(&cell)?);
    } else {
        println!(
            "{} × {} ({}): {}",
            cell.source,
            cell.sink,
            cell.sink_kind,
            if cell.compatible {
                "compatible"
            } else {
                "INCOMPATIBLE"
            }
        );
        for p in &cell.streams {
            println!(
                "  ✓ {:<32} {}{}",
                p.stream,
                p.describe(),
                if p.key.is_empty() {
                    String::new()
                } else {
                    format!(" (key: {})", p.key.join(", "))
                }
            );
        }
        for i in &cell.incompatible {
            println!("  ✗ {:<32} {}", i.stream, i.reason);
        }
        if cell.compatible {
            println!("\n{}", hub::catalog::run_command(&s, &k));
        }
    }
    if cell.compatible {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "{} stream(s) of '{}' have no write mode sink '{}' supports",
            cell.incompatible.len(),
            cell.source,
            cell.sink
        )))
    }
}

fn load_catalog(hub_flag: Option<&Path>) -> CliResult<Catalog> {
    Catalog::load(&hub::hub_dir(hub_flag))
}

/// `faucet hub list` — every source and sink template in the catalog.
fn list(a: HubListArgs) -> CliResult<()> {
    let cat = load_catalog(a.hub.as_deref())?;
    if a.json {
        println!("{}", pretty(&hub::catalog::index_json(&cat))?);
        return Ok(());
    }
    println!("source templates ({}):", cat.sources.len());
    for (_, s) in &cat.sources {
        println!(
            "  {:<24} {:<10} {:>3} stream(s)  {}",
            s.name,
            s.source.kind,
            s.streams.len(),
            s.description.as_deref().unwrap_or("")
        );
    }
    println!("sink templates ({}):", cat.sinks.len());
    for (_, k) in &cat.sinks {
        println!(
            "  {:<24} {:<10} {:<28} {}",
            k.name,
            k.sink.kind,
            crate::registry::sink_supported_write_modes(&k.sink.kind)
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>()
                .join("|"),
            k.description.as_deref().unwrap_or("")
        );
    }
    Ok(())
}

/// `faucet hub matrix [--format table|markdown|json]` — the source × sink
/// compatibility matrix. `markdown` is the docs page; `json` is `index.json`.
fn matrix(a: HubMatrixArgs) -> CliResult<()> {
    let cat = load_catalog(a.hub.as_deref())?;
    let out = match a.format {
        MatrixFormat::Json => pretty(&hub::catalog::index_json(&cat))?,
        MatrixFormat::Markdown => hub::catalog::render_markdown(&cat),
        MatrixFormat::Table => {
            let cells = cat.matrix();
            let mut s = String::new();
            s.push_str(&format!("{:<24}", "source \\ sink"));
            for (_, k) in &cat.sinks {
                s.push_str(&format!(" {:>12}", k.name));
            }
            s.push('\n');
            for (_, src) in &cat.sources {
                s.push_str(&format!("{:<24}", src.name));
                for (_, k) in &cat.sinks {
                    let c = cells
                        .iter()
                        .find(|c| c.source == src.name && c.sink == k.name)
                        .expect("cell");
                    let mark = if c.compatible {
                        "✓".to_string()
                    } else {
                        format!("{}/{}", c.streams.len(), src.streams.len())
                    };
                    s.push_str(&format!(" {mark:>12}"));
                }
                s.push('\n');
            }
            s.push_str("\n✓ = every stream compatible; n/m = compatible streams out of m\n");
            s
        }
    };
    match &a.out {
        Some(p) => std::fs::write(p, &out)
            .map_err(|e| CliError::Config(format!("writing {}: {e}", p.display()))),
        None => {
            print!("{out}");
            Ok(())
        }
    }
}

/// `faucet hub lint [--hub DIR] [FILE…]` — publishability lint. With files,
/// lint just those (kind detected from `kind:`); otherwise the whole catalog.
/// Exit non-zero on any finding.
fn lint(a: HubLintArgs) -> CliResult<()> {
    let mut findings: Vec<(String, Vec<String>)> = Vec::new();
    if a.files.is_empty() {
        let cat = load_catalog(a.hub.as_deref())?;
        findings = hub::catalog::lint_catalog(&cat);
        if !a.json {
            println!(
                "linted {} source + {} sink template(s)",
                cat.sources.len(),
                cat.sinks.len()
            );
        }
    } else {
        for f in &a.files {
            match hub::detect_kind_in_file(f) {
                Some(hub::TemplateKind::SourceTemplate) => {
                    let t = hub::parse_source_file(f)?;
                    let r = hub::catalog::lint_source(&t);
                    if !r.is_empty() {
                        findings.push((f.display().to_string(), r));
                    }
                }
                Some(hub::TemplateKind::SinkTemplate) => {
                    let t = hub::parse_sink_file(f)?;
                    let r = hub::catalog::lint_sink(&t);
                    if !r.is_empty() {
                        findings.push((f.display().to_string(), r));
                    }
                }
                Some(hub::TemplateKind::Pipeline) | None => {
                    return Err(CliError::Config(format!(
                        "{}: not a hub template (no `kind: source-template` / `sink-template`)",
                        f.display()
                    )));
                }
            }
        }
    }
    if a.json {
        println!(
            "{}",
            pretty(&serde_json::json!({
                "ok": findings.is_empty(),
                "findings": findings.iter().map(|(t, f)| serde_json::json!({"template": t, "findings": f})).collect::<Vec<_>>(),
            }))?
        );
    } else {
        for (t, f) in &findings {
            println!("{t}:");
            for line in f {
                println!("  - {line}");
            }
        }
        if findings.is_empty() {
            println!("ok — no findings");
        }
    }
    if findings.is_empty() {
        Ok(())
    } else {
        Err(CliError::Config(format!(
            "hub lint: {} template(s) with findings",
            findings.len()
        )))
    }
}

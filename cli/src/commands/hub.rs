//! `faucet hub` — the Template Hub CLI (#571): compose a source template
//! with a sink template, check a pairing, list a catalog, render its
//! compatibility matrix, and lint templates for publication.

use crate::cli::{
    HubArgs, HubCheckArgs, HubCommand, HubComposeArgs, HubLintArgs, HubListArgs, HubMatrixArgs,
    MatrixFormat,
};
use crate::error::{CliError, CliResult};
use crate::hub::{self, Catalog};

pub async fn run(args: HubArgs) -> CliResult<()> {
    match args.command {
        HubCommand::Compose(a) => compose(a).await,
        HubCommand::Check(a) => check(a).await,
        HubCommand::List(a) => list(a).await,
        HubCommand::Matrix(a) => matrix(a).await,
        HubCommand::Lint(a) => lint(a).await,
    }
}

fn pretty<T: serde::Serialize>(v: &T) -> CliResult<String> {
    serde_json::to_string_pretty(v)
        .map_err(|e| CliError::Internal(format!("hub: rendering JSON: {e}")))
}

/// `faucet hub compose --source X --sink Y [--out F] [--json]` — print (or
/// write) the composed pipeline config. The output is an ordinary config:
/// `faucet validate` / `run` / `template register` all take it.
async fn compose(a: HubComposeArgs) -> CliResult<()> {
    let hub_dir = hub::resolve_hub(a.pair.hub.as_deref()).await?;
    let c = hub::compose_locators(&a.pair.source, &a.pair.sink, &hub_dir).await?;
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
async fn check(a: HubCheckArgs) -> CliResult<()> {
    let hub_dir = hub::resolve_hub(a.pair.hub.as_deref()).await?;
    let s = hub::load_source(&a.pair.source, &hub_dir).await?;
    let k = hub::load_sink(&a.pair.sink, &hub_dir).await?;
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

async fn load_catalog(hub_flag: Option<&str>) -> CliResult<Catalog> {
    Catalog::load(&hub::resolve_hub(hub_flag).await?)
}

/// `faucet hub list` — every source and sink template in the catalog.
async fn list(a: HubListArgs) -> CliResult<()> {
    let cat = load_catalog(a.hub.as_deref()).await?;
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
async fn matrix(a: HubMatrixArgs) -> CliResult<()> {
    let cat = load_catalog(a.hub.as_deref()).await?;
    let out = match a.format {
        MatrixFormat::Json => pretty(&hub::catalog::index_json(&cat))?,
        MatrixFormat::Markdown => hub::catalog::render_markdown(&cat),
        MatrixFormat::Table => {
            let cells = cat.matrix();
            let src_w = cat
                .sources
                .iter()
                .map(|(_, t)| t.id().len())
                .max()
                .unwrap_or(0)
                .max(13)
                + 2;
            let mut s = String::new();
            s.push_str(&format!("{:<src_w$}", "source \\ sink"));
            for (_, k) in &cat.sinks {
                s.push_str(&format!(" {:>w$}", k.id(), w = k.id().len().max(5)));
            }
            s.push('\n');
            for (_, src) in &cat.sources {
                let sid = src.id();
                s.push_str(&format!("{sid:<src_w$}"));
                for (_, k) in &cat.sinks {
                    let kid = k.id();
                    let c = cells
                        .iter()
                        .find(|c| c.source == sid && c.sink == kid)
                        .expect("cell");
                    let mark = if c.compatible {
                        "✓".to_string()
                    } else {
                        format!("{}/{}", c.streams.len(), src.streams.len())
                    };
                    s.push_str(&format!(" {mark:>w$}", w = kid.len().max(5)));
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
async fn lint(a: HubLintArgs) -> CliResult<()> {
    let mut findings: Vec<(String, Vec<String>)> = Vec::new();
    if a.files.is_empty() {
        let cat = load_catalog(a.hub.as_deref()).await?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{HubPairArgs, MatrixFormat};
    use std::path::PathBuf;

    fn repo_hub() -> String {
        // The engine's own `hub/` — the catalog the docs and the hub tests use.
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("hub");
        root.canonicalize().unwrap().display().to_string()
    }

    fn pair(source: &str, sink: &str) -> HubPairArgs {
        HubPairArgs {
            source: source.into(),
            sink: sink.into(),
            hub: Some(repo_hub()),
        }
    }

    #[tokio::test]
    async fn every_hub_verb_runs_against_a_directory_hub() {
        let out = tempfile::tempdir().unwrap();
        let composed = out.path().join("composed.yaml");
        run(HubArgs {
            command: HubCommand::Compose(HubComposeArgs {
                pair: pair("example-csv", "jsonl"),
                out: Some(composed.clone()),
                json: false,
            }),
        })
        .await
        .expect("compose to a file");
        assert!(
            std::fs::read_to_string(&composed)
                .unwrap()
                .contains("name: faucet-hq/example-csv")
        );
        run(HubArgs {
            command: HubCommand::Compose(HubComposeArgs {
                pair: pair("example-csv", "sqlite"),
                out: None,
                json: true,
            }),
        })
        .await
        .expect("compose as json");

        run(HubArgs {
            command: HubCommand::Check(HubCheckArgs {
                pair: pair("example-csv", "sqlite"),
                json: false,
            }),
        })
        .await
        .expect("compatible pairing");
        run(HubArgs {
            command: HubCommand::Check(HubCheckArgs {
                pair: pair("example-rest-api", "bigquery"),
                json: true,
            }),
        })
        .await
        .expect("compatible pairing as json");

        for json in [false, true] {
            run(HubArgs {
                command: HubCommand::List(HubListArgs {
                    hub: Some(repo_hub()),
                    json,
                }),
            })
            .await
            .expect("list");
        }
        for format in [
            MatrixFormat::Table,
            MatrixFormat::Markdown,
            MatrixFormat::Json,
        ] {
            run(HubArgs {
                command: HubCommand::Matrix(HubMatrixArgs {
                    hub: Some(repo_hub()),
                    format,
                    out: None,
                }),
            })
            .await
            .expect("matrix");
        }
        let matrix_file = out.path().join("index.json");
        run(HubArgs {
            command: HubCommand::Matrix(HubMatrixArgs {
                hub: Some(repo_hub()),
                format: MatrixFormat::Json,
                out: Some(matrix_file.clone()),
            }),
        })
        .await
        .expect("matrix to a file");
        assert!(matrix_file.is_file());

        run(HubArgs {
            command: HubCommand::Lint(HubLintArgs {
                hub: Some(repo_hub()),
                files: vec![],
                json: false,
            }),
        })
        .await
        .expect("the shipped catalog lints clean");
        run(HubArgs {
            command: HubCommand::Lint(HubLintArgs {
                hub: Some(repo_hub()),
                files: vec![PathBuf::from(repo_hub()).join("sink-templates/faucet-hq/jsonl.yaml")],
                json: true,
            }),
        })
        .await
        .expect("one file lints clean");
    }

    #[tokio::test]
    async fn an_unknown_pairing_and_a_missing_hub_are_errors() {
        let err = run(HubArgs {
            command: HubCommand::Check(HubCheckArgs {
                pair: pair("nope", "jsonl"),
                json: false,
            }),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("no hub template 'nope'"), "{err}");

        let err = run(HubArgs {
            command: HubCommand::List(HubListArgs {
                hub: Some("/definitely/not/a/hub".into()),
                json: false,
            }),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(!err.is_empty());
    }
}

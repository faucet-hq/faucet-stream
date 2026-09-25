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
    let c = hub::compose_locators_overlaid(
        &a.pair.source,
        &a.pair.sink,
        a.pair.overlay.as_deref(),
        &hub_dir,
    )
    .await?;
    for w in &c.warnings {
        eprintln!("warning: {w}");
    }
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
    // An overlay is checked against the pairing it would be applied to: a
    // stream it names must exist, and its params must not clash.
    let overlaid = match (&a.pair.overlay, cell.compatible) {
        (Some(o), true) => {
            Some(hub::compose(&s, &k)?.apply_overlay(&hub::load_deployment(o, &hub_dir)?)?)
        }
        _ => None,
    };
    if a.json {
        let mut v = serde_json::to_value(&cell)
            .map_err(|e| CliError::Internal(format!("hub: rendering JSON: {e}")))?;
        if let (Some(c), Some(obj)) = (&overlaid, v.as_object_mut()) {
            obj.insert("overlay".into(), serde_json::json!(c.overlay));
            obj.insert(
                "overlay_contributes".into(),
                serde_json::json!(c.overlay_contributes),
            );
            obj.insert("warnings".into(), serde_json::json!(c.warnings));
        }
        println!("{}", pretty(&v)?);
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
        if let Some(c) = &overlaid {
            println!(
                "  overlay '{}' sets: {}",
                c.overlay.as_deref().unwrap_or_default(),
                c.overlay_contributes.join(", ")
            );
            for w in &c.warnings {
                println!("  warning: {w}");
            }
        }
        if cell.compatible {
            let mut cmd = hub::catalog::run_command(&s, &k);
            if let Some(o) = &a.pair.overlay {
                cmd.push_str(&format!(" --overlay {o}"));
            }
            println!("\n{cmd}");
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

/// `faucet hub list` — every source and sink template in the catalog, with
/// the trust signals its `index.json` records (#685).
async fn list(a: HubListArgs) -> CliResult<()> {
    let dir = hub::resolve_hub(a.hub.as_deref()).await?;
    let cat = Catalog::load(&dir)?;
    let index = hub::IndexVersions::load(&dir);
    print!(
        "{}",
        render_list(&cat, index.as_ref(), a.sort.into(), a.json)?
    );
    Ok(())
}

fn render_list(
    cat: &Catalog,
    index: Option<&hub::IndexVersions>,
    sort: hub::trust::SortBy,
    json: bool,
) -> CliResult<String> {
    use hub::catalog::{SINK_DIR, SOURCE_DIR};
    use hub::trust::{Candidate, TrustSignals, order};
    let trust_of = |subdir: &str, id: &str| -> Option<TrustSignals> {
        index
            .and_then(|i| i.entry(subdir, id))
            .and_then(|e| e.trust.clone())
    };
    let src_ids: Vec<String> = cat.sources.iter().map(|(_, t)| t.id()).collect();
    let snk_ids: Vec<String> = cat.sinks.iter().map(|(_, t)| t.id()).collect();
    let src_trust: Vec<Option<TrustSignals>> =
        src_ids.iter().map(|id| trust_of(SOURCE_DIR, id)).collect();
    let snk_trust: Vec<Option<TrustSignals>> =
        snk_ids.iter().map(|id| trust_of(SINK_DIR, id)).collect();
    let src_order = order(
        &src_ids
            .iter()
            .zip(&cat.sources)
            .zip(&src_trust)
            .map(|((id, (_, t)), tr)| Candidate {
                id,
                official: t.is_official(),
                trust: tr.as_ref(),
            })
            .collect::<Vec<_>>(),
        sort,
    );
    let snk_order = order(
        &snk_ids
            .iter()
            .zip(&cat.sinks)
            .zip(&snk_trust)
            .map(|((id, (_, t)), tr)| Candidate {
                id,
                official: t.is_official(),
                trust: tr.as_ref(),
            })
            .collect::<Vec<_>>(),
        sort,
    );

    if json {
        let mut v = hub::catalog::index_json(cat);
        for (key, ids, trust, ord) in [
            ("sources", &src_ids, &src_trust, &src_order),
            ("sinks", &snk_ids, &snk_trust, &snk_order),
        ] {
            let list = v[key].as_array().cloned().unwrap_or_default();
            let sorted: Vec<serde_json::Value> = ord
                .iter()
                .filter_map(|&i| {
                    let mut e = list.iter().find(|e| e["id"] == ids[i].as_str())?.clone();
                    if let Some(t) = &trust[i] {
                        e["trust"] = serde_json::to_value(t).unwrap_or_default();
                    }
                    Some(e)
                })
                .collect();
            v[key] = serde_json::Value::Array(sorted);
        }
        return Ok(format!("{}\n", pretty(&v)?));
    }

    let stars = |t: &Option<TrustSignals>| {
        t.as_ref()
            .and_then(|t| t.stars)
            .map(|n| format!("★ {n}"))
            .unwrap_or_default()
    };
    let updated = |t: &Option<TrustSignals>| {
        t.as_ref()
            .and_then(|t| t.updated.clone())
            .unwrap_or_default()
    };
    let w = src_ids
        .iter()
        .chain(&snk_ids)
        .map(String::len)
        .max()
        .unwrap_or(0)
        .max(12);
    let mut out = format!("source templates ({}):\n", cat.sources.len());
    for &i in &src_order {
        let s = &cat.sources[i].1;
        out.push_str(&format!(
            "  {:<w$} {:<10} {:>3} stream(s) {:>7} {:<10}  {}\n",
            src_ids[i],
            s.source.kind,
            s.streams.len(),
            stars(&src_trust[i]),
            updated(&src_trust[i]),
            s.description.as_deref().unwrap_or("")
        ));
    }
    out.push_str(&format!("sink templates ({}):\n", cat.sinks.len()));
    for &i in &snk_order {
        let k = &cat.sinks[i].1;
        out.push_str(&format!(
            "  {:<w$} {:<10} {:<28} {:>7} {:<10}  {}\n",
            snk_ids[i],
            k.sink.kind,
            crate::registry::sink_supported_write_modes(&k.sink.kind)
                .iter()
                .map(|m| m.as_str())
                .collect::<Vec<_>>()
                .join("|"),
            stars(&snk_trust[i]),
            updated(&snk_trust[i]),
            k.description.as_deref().unwrap_or("")
        ));
    }
    Ok(out)
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
                Some(hub::TemplateKind::Deployment) => {
                    let t = hub::parse_deployment_file(f)?;
                    let r = hub::catalog::lint_deployment(&t);
                    if !r.is_empty() {
                        findings.push((f.display().to_string(), r));
                    }
                }
                Some(hub::TemplateKind::Pipeline) | None => {
                    return Err(CliError::Config(format!(
                        "{}: not a hub template (no `kind: source-template` / `sink-template` / `deployment`)",
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
            overlay: None,
            hub: Some(repo_hub()),
        }
    }

    #[tokio::test]
    async fn compose_check_and_lint_take_a_deployment_overlay() {
        let dir = tempfile::tempdir().unwrap();
        let overlay = dir.path().join("ops.yaml");
        std::fs::write(
            &overlay,
            "kind: deployment\nname: ops\ndescription: ops\nstate: { type: memory }\n",
        )
        .unwrap();
        let with = |source: &str, sink: &str| HubPairArgs {
            overlay: Some(overlay.display().to_string()),
            ..pair(source, sink)
        };
        let composed = dir.path().join("composed.yaml");
        run(HubArgs {
            command: HubCommand::Compose(HubComposeArgs {
                pair: with("example-csv", "sqlite"),
                out: Some(composed.clone()),
                json: false,
            }),
        })
        .await
        .expect("compose with an overlay");
        assert!(
            std::fs::read_to_string(&composed)
                .unwrap()
                .contains("type: memory")
        );
        for json in [false, true] {
            run(HubArgs {
                command: HubCommand::Check(HubCheckArgs {
                    pair: with("example-csv", "sqlite"),
                    json,
                }),
            })
            .await
            .expect("check with an overlay");
        }
        // An overlay that names a stream the source lacks fails the check.
        let bad = dir.path().join("bad.yaml");
        std::fs::write(
            &bad,
            "kind: deployment\nname: bad\nstreams:\n  nope: { delivery: at_least_once }\n",
        )
        .unwrap();
        let err = run(HubArgs {
            command: HubCommand::Check(HubCheckArgs {
                pair: HubPairArgs {
                    overlay: Some(bad.display().to_string()),
                    ..pair("example-csv", "sqlite")
                },
                json: false,
            }),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("names no stream"), "{err}");
        // `hub lint` accepts a deployment file and flags a literal credential.
        let leaky = dir.path().join("leaky.yaml");
        std::fs::write(
            &leaky,
            "kind: deployment\nname: leaky\nstate: { type: postgres, config: { url: \"postgres://u:pw@h/db\" } }\n",
        )
        .unwrap();
        let err = run(HubArgs {
            command: HubCommand::Lint(HubLintArgs {
                files: vec![leaky],
                hub: None,
                json: false,
            }),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(!err.is_empty());
        run(HubArgs {
            command: HubCommand::Lint(HubLintArgs {
                files: vec![overlay.clone()],
                hub: None,
                json: false,
            }),
        })
        .await
        .expect("a clean overlay lints clean");
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
                    sort: crate::cli::HubSort::Stars,
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

    #[test]
    fn list_shows_and_sorts_by_the_index_trust_signals() {
        let dir = std::path::Path::new(&repo_hub()).to_path_buf();
        let cat = Catalog::load(&dir).unwrap();
        let index: hub::IndexVersions = serde_json::from_value(serde_json::json!({
            "sources": [
                {"id": "faucet-hq/example-rest-api", "trust": {"stars": 7, "updated": "2026-09-01"}},
                {"id": "faucet-hq/example-csv", "trust": {"stars": 2, "updated": "2026-09-20"}}
            ],
            "sinks": [{"id": "faucet-hq/sqlite", "trust": {"stars": 1}}]
        }))
        .unwrap();
        let by_stars = render_list(&cat, Some(&index), hub::trust::SortBy::Stars, false).unwrap();
        let rest = by_stars.find("faucet-hq/example-rest-api").unwrap();
        let csv = by_stars.find("faucet-hq/example-csv").unwrap();
        assert!(rest < csv, "most starred first:\n{by_stars}");
        assert!(
            by_stars.contains("★ 7") && by_stars.contains("2026-09-20"),
            "{by_stars}"
        );
        let sqlite = by_stars.find("faucet-hq/sqlite").unwrap();
        assert!(
            sqlite < by_stars.find("faucet-hq/bigquery").unwrap(),
            "{by_stars}"
        );

        let by_date = render_list(&cat, Some(&index), hub::trust::SortBy::Updated, false).unwrap();
        assert!(
            by_date.find("faucet-hq/example-csv").unwrap()
                < by_date.find("faucet-hq/example-rest-api").unwrap()
        );

        let json: serde_json::Value = serde_json::from_str(
            &render_list(&cat, Some(&index), hub::trust::SortBy::Stars, true).unwrap(),
        )
        .unwrap();
        assert_eq!(json["sources"][0]["id"], "faucet-hq/example-rest-api");
        assert_eq!(json["sources"][0]["trust"]["stars"], 7);
        assert!(
            json["sinks"]
                .as_array()
                .unwrap()
                .iter()
                .any(|k| k.get("trust").is_none())
        );

        // No index: plain listing, alphabetical.
        let plain = render_list(&cat, None, hub::trust::SortBy::Name, false).unwrap();
        assert!(!plain.contains('★'));
        assert!(
            plain.find("faucet-hq/example-csv").unwrap()
                < plain.find("faucet-hq/example-rest-api").unwrap()
        );
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
                sort: Default::default(),
            }),
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(!err.is_empty());
    }
}

//! The shipped Template Hub catalog (`hub/`, #571 — the sink templates plus
//! two example source templates; real source templates live in a separately
//! contributed catalog) is kept honest here: every template parses,
//! validates, and passes the publishability lint; every source composes with
//! at least two sinks; every compatible pairing loads
//! as a real `PipelineConfig`, expands, type-checks each compiled-in
//! connector's config, and compiles its transforms; and the generated matrix
//! page + `index.json` are up to date.
//!
//! Regenerate the committed artifacts after a catalog change:
//!
//! ```bash
//! cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate
//! ```

use std::path::{Path, PathBuf};

use faucet_cli::config::RunInputs;
use faucet_cli::hub::catalog::{self, Catalog};
use faucet_cli::hub::{self, TemplateKind};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("cli/ has a parent")
        .to_path_buf()
}

fn hub_dir() -> PathBuf {
    repo_root().join("hub")
}

const MATRIX_PAGE: &str = "docs/book/src/reference/template-hub-matrix.md";
const INDEX_JSON: &str = "hub/index.json";

fn catalog() -> Catalog {
    Catalog::load(&hub_dir()).expect("hub/ loads")
}

#[test]
fn every_template_parses_validates_and_passes_lint() {
    let cat = catalog();
    assert!(
        cat.sources.len() >= 2,
        "the shipped catalog carries the example source templates, got {}",
        cat.sources.len()
    );
    assert!(
        cat.sinks.len() >= 3,
        "≥3 sink templates ship, got {}",
        cat.sinks.len()
    );
    for name in ["faucet-hq/bigquery", "faucet-hq/jsonl"] {
        assert!(cat.sink(name).is_some(), "sink-template '{name}' must ship");
    }
    // The maintained set is the faucet-hq namespace, and bare names alias it.
    assert!(cat.sink("jsonl").is_some_and(|k| k.is_official()));
    for (p, s) in &cat.sources {
        assert!(
            s.is_official(),
            "{}: shipped templates live under faucet-hq/",
            p.display()
        );
    }
    for (p, k) in &cat.sinks {
        assert!(
            k.is_official(),
            "{}: shipped templates live under faucet-hq/",
            p.display()
        );
    }
    let findings = catalog::lint_catalog(&cat);
    assert!(findings.is_empty(), "lint findings:\n{findings:#?}");
    // Kind detection on every file, so `faucet run <template>` can hint.
    for (p, _) in &cat.sources {
        assert_eq!(
            hub::detect_kind_in_file(p),
            Some(TemplateKind::SourceTemplate),
            "{}",
            p.display()
        );
    }
    for (p, _) in &cat.sinks {
        assert_eq!(
            hub::detect_kind_in_file(p),
            Some(TemplateKind::SinkTemplate),
            "{}",
            p.display()
        );
    }
}

/// No template may ship a literal credential, a private hostname, or a
/// leftover placeholder — a second, text-level net under the structured lint.
#[test]
fn no_template_carries_private_text() {
    let banned = [
        "REPLACE_ME",
        ".rds.amazonaws.com",
        "internal/",
        "Source of truth",
        "localhost:",
    ];
    for dir in [catalog::SOURCE_DIR, catalog::SINK_DIR] {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(hub_dir().join(dir)).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                for inner in std::fs::read_dir(&p).unwrap() {
                    files.push(inner.unwrap().path());
                }
            } else {
                files.push(p);
            }
        }
        for p in files {
            if !p.is_file() {
                continue;
            }
            let text = std::fs::read_to_string(&p).unwrap();
            for b in banned {
                assert!(!text.contains(b), "{}: contains {b:?}", p.display());
            }
        }
    }
}

/// Every source × sink cell either composes fully or names the streams that
/// cannot run; every source must have at least two fully compatible sinks
/// (the acceptance bar), and every compatible pairing must be a config the
/// ordinary run path accepts — through `expand`, with each compiled-in
/// connector's typed config validated and every transform chain compiled.
#[test]
fn every_compatible_pairing_is_a_runnable_pipeline() {
    let cat = catalog();
    let sink_kinds = faucet_cli::registry::sink_kinds();
    let source_kinds = faucet_cli::registry::source_kinds();
    let mut checked = 0usize;
    let mut skipped_kinds = std::collections::BTreeSet::new();
    for (_, s) in &cat.sources {
        let mut compatible = 0usize;
        for (_, k) in &cat.sinks {
            let cell = catalog::cell(s, k);
            if !cell.compatible {
                assert!(
                    !cell.incompatible.is_empty(),
                    "{} × {}: incompatible without a reason",
                    s.name,
                    k.name
                );
                continue;
            }
            compatible += 1;
            let c = hub::compose(s, k).unwrap_or_else(|e| panic!("{} × {}: {e}", s.name, k.name));
            assert_eq!(c.streams.len(), s.streams.len());
            // Placeholder binding, like `faucet validate` with no --param.
            let inputs = RunInputs {
                params: Default::default(),
                env: Default::default(),
                mode: faucet_cli::params::BindMode::Placeholder,
            };
            let cfg = hub::load_composed(&c, &inputs)
                .unwrap_or_else(|e| panic!("{} × {}: load: {e}", s.name, k.name));
            assert_eq!(
                cfg.name.as_deref(),
                Some(s.id().as_str()),
                "state keys are `{{source id}}::{{stream}}`"
            );
            let nodes = faucet_cli::expand::expand(&cfg)
                .unwrap_or_else(|e| panic!("{} × {}: expand: {e}", s.name, k.name));
            assert!(!nodes.is_empty());
            for node in &nodes {
                if source_kinds.contains(&node.source.kind.as_str()) {
                    faucet_cli::registry::validate_source_config(
                        &node.source.kind,
                        &node.id,
                        node.source.config.clone(),
                    )
                    .unwrap_or_else(|e| {
                        panic!("{} × {} row '{}' source: {e}", s.name, k.name, node.id)
                    });
                } else {
                    skipped_kinds.insert(format!("source {}", node.source.kind));
                }
                if !matches!(node.role, faucet_cli::expand::NodeRole::Discovery { .. }) {
                    if sink_kinds.contains(&node.sink.kind.as_str()) {
                        faucet_cli::registry::validate_sink_config(
                            &node.sink.kind,
                            &node.id,
                            node.sink.config.clone(),
                        )
                        .unwrap_or_else(|e| {
                            panic!("{} × {} row '{}' sink: {e}", s.name, k.name, node.id)
                        });
                    } else {
                        skipped_kinds.insert(format!("sink {}", node.sink.kind));
                    }
                }
                if !node.transforms.is_empty() {
                    faucet_cli::transforms::compile_transforms(&node.transforms).unwrap_or_else(
                        |e| panic!("{} × {} row '{}': {e}", s.name, k.name, node.id),
                    );
                }
            }
            checked += 1;
        }
        assert!(
            compatible >= 2,
            "source-template '{}' composes with only {compatible} sink(s)",
            s.name
        );
    }
    assert!(checked >= cat.sources.len() * 2);
    // Under `--all-features` nothing is skipped; a slimmer build reports what it
    // could not type-check rather than silently passing.
    if !skipped_kinds.is_empty() {
        eprintln!("connector kinds not compiled in (config not type-checked): {skipped_kinds:?}");
    }
}

/// The `${stream}` / `${source}` addressing in every sink template renders a
/// distinct destination per stream — two streams may never collide.
#[test]
fn per_stream_addressing_never_collides() {
    let cat = catalog();
    for (_, s) in &cat.sources {
        for (_, k) in &cat.sinks {
            let Ok(c) = hub::compose(s, k) else { continue };
            let mut seen = std::collections::HashSet::new();
            for row in c.document["matrix"].as_array().unwrap() {
                let cfg = row["sink"]["config"].to_string();
                assert!(
                    seen.insert(cfg.clone()),
                    "{} × {}: two streams share sink config {cfg}",
                    s.name,
                    k.name
                );
            }
        }
    }
}

fn generated_page(cat: &Catalog) -> String {
    catalog::render_markdown(cat)
}

fn generated_index(cat: &Catalog) -> String {
    let mut s = serde_json::to_string_pretty(&catalog::index_json(cat)).unwrap();
    s.push('\n');
    s
}

#[test]
fn committed_matrix_page_and_index_are_current() {
    let cat = catalog();
    let page = std::fs::read_to_string(repo_root().join(MATRIX_PAGE)).expect("matrix page exists");
    assert_eq!(
        page,
        generated_page(&cat),
        "{MATRIX_PAGE} is stale — regenerate with `cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate`"
    );
    let index = std::fs::read_to_string(repo_root().join(INDEX_JSON)).expect("index.json exists");
    assert_eq!(
        index,
        generated_index(&cat),
        "{INDEX_JSON} is stale — regenerate with `cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate`"
    );
}

/// `cargo test -p faucet-cli --test hub_catalog -- --ignored regenerate`
#[test]
#[ignore]
fn regenerate() {
    let cat = catalog();
    std::fs::write(repo_root().join(MATRIX_PAGE), generated_page(&cat)).unwrap();
    std::fs::write(repo_root().join(INDEX_JSON), generated_index(&cat)).unwrap();
}

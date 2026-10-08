//! Every complete config in the docs site validates (#821).
//!
//! A fenced ```yaml block under `docs/book/src/` whose document has top-level
//! `version:` and `pipeline:` keys is a config a reader will copy. It goes
//! through the same path as `faucet validate --no-secrets`: required params
//! bind to placeholders, secret-manager directives are tolerated, and
//! `${env:}` / `${file:}` directives are replaced by placeholder text first
//! (the test does not own the reader's environment or files). Docs paths are
//! relative to the repo root.
//!
//! A block that is deliberately partial or wrong (an example of a rejected
//! config, say) opts out with `<!-- faucet:no-validate -->` on the line before
//! its fence.
#![cfg(all(unix, feature = "full"))]

use clap::Parser;
use faucet_cli::cli::Cli;
use std::path::{Path, PathBuf};

const OPT_OUT: &str = "<!-- faucet:no-validate -->";

#[derive(Debug, PartialEq)]
struct Block {
    line: usize,
    text: String,
    opted_out: bool,
}

fn yaml_blocks(markdown: &str) -> Vec<Block> {
    let lines: Vec<&str> = markdown.lines().collect();
    let mut blocks = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim_start();
        let indent = lines[i].len() - trimmed.len();
        let Some(lang) = trimmed.strip_prefix("```").map(str::trim) else {
            i += 1;
            continue;
        };
        if !matches!(lang, "yaml" | "yml") {
            i += 1;
            while i < lines.len() && !lines[i].trim_start().starts_with("```") {
                i += 1;
            }
            i += 1;
            continue;
        }
        let opted_out = lines[..i]
            .iter()
            .rev()
            .find(|l| !l.trim().is_empty())
            .is_some_and(|l| l.trim() == OPT_OUT);
        let start = i;
        let mut body = Vec::new();
        i += 1;
        while i < lines.len() && !lines[i].trim_start().starts_with("```") {
            let l = lines[i];
            body.push(if l.len() >= indent && l[..indent].trim().is_empty() {
                &l[indent..]
            } else {
                l.trim_start()
            });
            i += 1;
        }
        blocks.push(Block {
            line: start + 1,
            text: body.join("\n") + "\n",
            opted_out,
        });
        i += 1;
    }
    blocks
}

fn is_full_config(text: &str) -> bool {
    match serde_yaml::from_str::<serde_yaml::Value>(text) {
        Ok(serde_yaml::Value::Mapping(m)) => {
            m.contains_key("version") && m.contains_key("pipeline")
        }
        _ => false,
    }
}

/// Replace `${env:NAME}` / `${file:PATH}` with text the config can parse.
fn neutralize_directives(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("${") {
        let (head, tail) = rest.split_at(at);
        out.push_str(head);
        if head.ends_with('$') {
            out.push_str("${");
            rest = &tail[2..];
            continue;
        }
        let Some(end) = tail.find('}') else {
            out.push_str(tail);
            return out;
        };
        let inner = &tail[2..end];
        match inner.split_once(':') {
            Some(("env", name)) => out.push_str(&env_placeholder(name)),
            Some(("file", _)) => out.push_str("placeholder"),
            _ => out.push_str(&tail[..=end]),
        }
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    out
}

fn env_placeholder(name: &str) -> String {
    let name = name.split(":-").next().unwrap_or(name).to_ascii_uppercase();
    if name.ends_with("PORT") {
        "5432".into()
    } else if name.contains("PG") || name.contains("POSTGRES") || name.contains("DATABASE_URL") {
        "postgres://user:pass@localhost:5432/db".into()
    } else if name.contains("MYSQL") {
        "mysql://user:pass@localhost:3306/db".into()
    } else if name.ends_with("URL") || name.ends_with("ENDPOINT") || name.ends_with("HOST") {
        "https://placeholder.invalid".into()
    } else {
        "placeholder".into()
    }
}

fn markdown_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            markdown_files(&p, out);
        } else if p.extension().is_some_and(|e| e == "md") {
            out.push(p);
        }
    }
}

async fn validate(path: &Path) -> Result<(), String> {
    let cli = Cli::try_parse_from([
        "faucet",
        "validate",
        "--no-secrets",
        "--no-env-file",
        "--json",
        path.to_str().unwrap(),
    ])
    .map_err(|e| e.to_string())?;
    Box::pin(faucet_cli::run_command(cli))
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn every_complete_config_in_the_docs_validates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/book/src");
    if !root.is_dir() {
        return;
    }
    // Docs paths (a wasm module, a reference CSV) are relative to the repo root,
    // where a reader runs them. Run from a scratch directory that links those
    // trees, so anything validation writes (a topology's SQLite file) stays out
    // of the checkout.
    let repo = root.join("../../..").canonicalize().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    for tree in ["cli", "examples"] {
        std::os::unix::fs::symlink(repo.join(tree), tmp.path().join(tree)).unwrap();
    }
    std::env::set_current_dir(tmp.path()).unwrap();
    let mut files = Vec::new();
    markdown_files(&root, &mut files);
    let mut checked = 0;
    let mut failures = Vec::new();
    for file in &files {
        let markdown = std::fs::read_to_string(file).unwrap();
        for block in yaml_blocks(&markdown) {
            if block.opted_out || !is_full_config(&block.text) {
                continue;
            }
            checked += 1;
            let path = tmp.path().join(format!("block-{checked}.yaml"));
            std::fs::write(&path, neutralize_directives(&block.text)).unwrap();
            if let Err(e) = validate(&path).await {
                let shown = file.strip_prefix(&root).unwrap_or(file);
                failures.push(format!("{}:{}: {e}", shown.display(), block.line));
            }
        }
    }
    assert!(
        checked > 0,
        "no complete configs found under {}",
        root.display()
    );
    assert!(
        failures.is_empty(),
        "{} of {checked} docs config(s) fail `faucet validate --no-secrets` \
         (fix them, or mark a deliberately invalid one with `{OPT_OUT}`):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn blocks_are_found_with_their_line_indent_and_opt_out() {
    let md = "intro\n\n```yaml\na: 1\n```\n\n<!-- faucet:no-validate -->\n\n```yml\nb: 2\n```\n\n\
              - item\n\n  ```yaml\n  c:\n    d: 3\n  ```\n\n```bash\n```yaml\n```\n";
    assert_eq!(
        yaml_blocks(md),
        vec![
            Block {
                line: 3,
                text: "a: 1\n".into(),
                opted_out: false
            },
            Block {
                line: 9,
                text: "b: 2\n".into(),
                opted_out: true
            },
            Block {
                line: 15,
                text: "c:\n  d: 3\n".into(),
                opted_out: false
            },
        ]
    );
}

#[test]
fn only_documents_with_version_and_pipeline_are_configs() {
    assert!(is_full_config("version: 1\npipeline:\n  source: {}\n"));
    assert!(!is_full_config("pipeline:\n  source: {}\n"));
    assert!(!is_full_config("version: 1\nmatrix: []\n"));
    assert!(!is_full_config("- version: 1\n"));
    assert!(!is_full_config("version: [\n"));
}

#[test]
fn env_and_file_directives_become_placeholders() {
    assert_eq!(
        neutralize_directives(
            "a: ${env:PG_URL}\nb: ${env:API_PORT}\nc: ${file:./k.pem}\nd: ${vault:x#y}\n\
             e: $${literal}\nf: ${env:TOKEN}\ng: ${env:MYSQL_DSN}\nh: ${env:API_HOST}\nopen: ${"
        ),
        "a: postgres://user:pass@localhost:5432/db\nb: 5432\nc: placeholder\nd: ${vault:x#y}\n\
         e: $${literal}\nf: placeholder\ng: mysql://user:pass@localhost:3306/db\n\
         h: https://placeholder.invalid\nopen: ${"
    );
}

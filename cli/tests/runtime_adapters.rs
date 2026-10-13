//! `faucet serve` is a client of one runtime API (#887): the HTTP handlers,
//! the MCP tools, the triggers and change-request execution load and run a
//! config only through `faucet_cli::runtime`, which applies load → trust →
//! policy → budget → tenant admission → governance → execution in one order.
//! A direct call to the loader, the executor or one of the gates from an
//! adapter is a second assembly path that can skip a stage. This scans the
//! non-test code of the CLI for one.

use std::path::{Path, PathBuf};

/// The adapter trees: they call `crate::runtime`, nothing below it.
const ADAPTERS: [&str; 4] = [
    "cli/src/serve/handlers",
    "cli/src/mcp",
    "cli/src/serve/triggers",
    "cli/src/serve/changes",
];

/// Calls an adapter may not make, with the runtime operation to use instead.
const ADAPTER_FORBIDDEN: [(&str, &str); 9] = [
    ("run_expanded", "runtime::run"),
    ("load_submission", "runtime::server::load / admit"),
    ("load_selected(", "runtime::server::load / admit"),
    ("runner::load_for(", "runtime::server::load / admit"),
    ("policy_gate(", "runtime::server::admit (Gates::policy)"),
    ("apply_request_budget(", "runtime::server::submit"),
    ("tenants::admit", "runtime::server::admit (Gates::admit)"),
    ("runner::submit(", "runtime::server::submit"),
    ("submit_gated(", "runtime::server::submit"),
];

/// Where each entry point may be called from anywhere in the CLI: its own
/// definition and the runtime.
const OWNED: [(&str, &[&str]); 3] = [
    (
        "run_expanded",
        &["cli/src/executor.rs", "cli/src/runtime/mod.rs"],
    ),
    (
        "load_submission",
        &["cli/src/serve/load.rs", "cli/src/runtime/server.rs"],
    ),
    (
        "admit_inline(",
        &["cli/src/serve/tenants/mod.rs", "cli/src/runtime/server.rs"],
    ),
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The part of a file before its unit-test module.
fn non_test(text: &str) -> &str {
    let mut cut = text.len();
    for marker in ["#[cfg(test)]\n", "#[cfg(all(test"] {
        for (i, _) in text.match_indices(marker) {
            let after = &text[i..];
            let mut rest = after.split_once('\n').map_or("", |(_, r)| r);
            while rest.starts_with("#[") {
                rest = rest.split_once('\n').map_or("", |(_, r)| r);
            }
            if rest.starts_with("mod ") && i < cut {
                cut = i;
            }
        }
    }
    &text[..cut]
}

/// Lines of `text` that call `needle`, skipping comments.
fn calls(text: &str, needle: &str) -> Vec<usize> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| {
            let code = l.split("//").next().unwrap_or("");
            code.contains(needle)
        })
        .map(|(i, _)| i + 1)
        .collect()
}

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

fn rel(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap()
        .to_string_lossy()
        .replace('\\', "/")
}

#[test]
fn adapters_go_through_the_runtime() {
    let root = root();
    let mut bad = Vec::new();
    for dir in ADAPTERS {
        let mut files = Vec::new();
        rust_files(&root.join(dir), &mut files);
        assert!(!files.is_empty(), "{dir} has no source files");
        for path in files {
            let text = std::fs::read_to_string(&path).unwrap();
            for (needle, instead) in ADAPTER_FORBIDDEN {
                for line in calls(non_test(&text), needle) {
                    bad.push(format!(
                        "{}:{line}: `{needle}` — call {instead}",
                        rel(&root, &path)
                    ));
                }
            }
        }
    }
    assert!(
        bad.is_empty(),
        "serve adapters must load and run configs through `faucet_cli::runtime` (#887):\n{}",
        bad.join("\n")
    );
}

#[test]
fn only_the_runtime_calls_the_loader_the_executor_and_tenant_admission() {
    let root = root();
    let mut files = Vec::new();
    rust_files(&root.join("cli/src"), &mut files);
    let mut bad = Vec::new();
    for path in files {
        let rel = rel(&root, &path);
        let text = std::fs::read_to_string(&path).unwrap();
        for (needle, owners) in OWNED {
            if owners.contains(&rel.as_str()) {
                continue;
            }
            for line in calls(non_test(&text), needle) {
                bad.push(format!("{rel}:{line}: `{needle}`"));
            }
        }
    }
    assert!(
        bad.is_empty(),
        "these entry points belong to the runtime (#887):\n{}",
        bad.join("\n")
    );
}

#[test]
fn the_scanner_sees_a_violation_and_skips_tests() {
    let src = "fn a() { runner::submit(x); }\n// run_expanded in a comment\n#[cfg(test)]\nmod tests { fn t() { run_expanded(); } }\n";
    assert_eq!(calls(non_test(src), "runner::submit("), vec![1]);
    assert!(calls(non_test(src), "run_expanded").is_empty());
    let gated = "fn a() {}\n#[cfg(all(test, feature = \"x\"))]\n#[allow(dead_code)]\nmod tests { load_submission(); }\n";
    assert!(calls(non_test(gated), "load_submission").is_empty());
}

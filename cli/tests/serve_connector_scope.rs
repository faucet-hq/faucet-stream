//! Every connector `faucet serve` builds is built inside a principal's scope
//! (`runtime::under`, or an explicit `with_host_credentials`), because the
//! server refuses the host's own identity to a task with no scope. A
//! connector built straight from the registry under `cli/src/serve` or
//! `cli/src/runtime` must be listed here with the scope it runs in; the
//! executor's and the runtime operations' own builds are already scoped.

use std::path::{Path, PathBuf};

/// `(file, calls, the scope they run in)`.
const SCOPED: [(&str, usize, &str); 2] = [
    (
        "cli/src/serve/runner.rs",
        1,
        "coordinate_sharded_run: resume_claimed runs shard_with_retry under runtime::under",
    ),
    (
        "cli/src/serve/preview/engine.rs",
        2,
        "local-output preview: handlers::preview scopes read_capped with the server's \
         identity refused",
    ),
];

const ROOTS: [&str; 2] = ["cli/src/serve", "cli/src/runtime"];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs")
            && path.file_name().is_none_or(|n| n != "tests.rs")
        {
            out.push(path);
        }
    }
}

/// The part of a file before its unit-test module.
fn non_test(text: &str) -> &str {
    let mut cut = text.len();
    for marker in ["#[cfg(test)]\n", "#[cfg(all(test"] {
        for (i, _) in text.match_indices(marker) {
            let mut rest = text[i..].split_once('\n').map_or("", |(_, r)| r);
            while rest.starts_with("#[") {
                rest = rest.split_once('\n').map_or("", |(_, r)| r);
            }
            if rest.trim_start().starts_with("mod ") && i < cut {
                cut = i;
            }
        }
    }
    &text[..cut]
}

/// Calls of `build_source(` / `build_sink(` (not their definitions), skipping
/// comments.
fn builds(text: &str) -> usize {
    text.lines()
        .map(|l| l.split("//").next().unwrap_or(""))
        .map(|code| {
            ["build_source(", "build_sink("]
                .iter()
                .map(|needle| {
                    code.match_indices(needle)
                        .filter(|(i, _)| !code[..*i].trim_end().ends_with("fn"))
                        .count()
                })
                .sum::<usize>()
        })
        .sum()
}

#[test]
fn connectors_serve_builds_directly_are_scoped() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut found = Vec::new();
    for dir in ROOTS {
        let mut files = Vec::new();
        rust_files(&root.join(dir), &mut files);
        for f in files {
            let text = std::fs::read_to_string(&f).unwrap();
            let n = builds(non_test(&text));
            if n > 0 {
                let rel = f
                    .strip_prefix(&root)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/");
                found.push((rel, n));
            }
        }
    }
    found.sort();
    let mut expected: Vec<(String, usize)> =
        SCOPED.iter().map(|(f, n, _)| (f.to_string(), *n)).collect();
    expected.sort();
    assert_eq!(
        found, expected,
        "a connector built under cli/src/serve or cli/src/runtime must run in a principal's \
         scope (runtime::under or with_host_credentials) and be listed in SCOPED"
    );
}

#[test]
fn the_scan_counts_calls_not_definitions_or_comments() {
    assert_eq!(builds("let s = build_source(k, c).await?;"), 1);
    assert_eq!(builds("async fn build_source(kind: &str) {}"), 0);
    assert_eq!(builds("// build_sink(x)"), 0);
    assert_eq!(builds("crate::registry::build_sink(a); build_source(b)"), 2);
    assert_eq!(
        non_test("a\n#[cfg(test)]\nmod tests {\nbuild_source(x)\n}"),
        "a\n"
    );
}

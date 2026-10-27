//! #789 CLI-146: a string literal wrapped without a trailing `\` keeps the next
//! line's indentation, so the message shows a long run of spaces. Scan the
//! CLI's non-test sources for literals that carry one.
//!
//! The scan tokenizes string literals (skipping comments, char literals and
//! raw strings, which keep their layout on purpose) so a literal that spans
//! lines is seen whole: a line break not escaped with `\` followed by
//! indentation is the wrapped-message defect. `#[cfg(test)]` modules are
//! skipped — their multi-line YAML fixtures are indented on purpose.

use std::path::Path;

#[path = "support/rust_source.rs"]
mod rust_source;
use rust_source::{literals, non_test};

/// Whether a literal's contents carry wrapped indentation: an unescaped line
/// break followed by indentation, or a long run of spaces on one line.
fn garbled(lit: &str) -> bool {
    let wrapped = lit
        .match_indices('\n')
        .any(|(k, _)| !lit[..k].ends_with('\\') && lit[k + 1..].starts_with("    "));
    let run = lit
        .split('\n')
        .any(|l| !l.contains("\\n") && l.trim().contains("        "));
    wrapped || run
}

fn scan(dir: &Path, hits: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            scan(&path, hits);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for (line, lit) in literals(non_test(&text)) {
            if garbled(lit) {
                let first = lit.lines().next().unwrap_or_default();
                hits.push(format!("{}:{line}: \"{first}…", path.display()));
            }
        }
    }
}

#[test]
fn no_message_literal_carries_a_run_of_wrapped_indentation() {
    let mut hits = Vec::new();
    scan(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut hits,
    );
    assert!(hits.is_empty(), "garbled literals:\n{}", hits.join("\n"));
}

/// The scan catches the wrapped-without-`\` shape it exists for, and leaves
/// properly continued literals, raw strings, char literals, comments and test
/// modules alone.
#[test]
fn the_scan_detects_a_literal_wrapped_without_a_backslash() {
    let wrapped = "fn f() -> String {\n    \"refusing this\n            config\".into()\n}\n";
    let hits: Vec<_> = literals(wrapped)
        .into_iter()
        .filter(|(_, l)| garbled(l))
        .collect();
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0].0, 2);

    let fine = concat!(
        "fn f() {\n",
        "    let a = \"refusing this \\\n             config\";\n",
        "    let b = r#\"yaml:\n        indented: on purpose\"#;\n",
        "    let c = '\"';\n",
        "    // \"a comment\n",
        "    let d = \"after\";\n",
        "}\n",
    );
    let lits = literals(fine);
    assert!(lits.iter().all(|(_, l)| !garbled(l)), "{lits:?}");
    assert!(lits.iter().any(|(_, l)| *l == "after"), "{lits:?}");

    let tests_only = "fn f() {}\n#[cfg(test)]\n#[allow(dead_code)]\nmod tests {\n    const Y: &str = \"a\n        b\";\n}\n";
    assert!(literals(non_test(tests_only)).is_empty());
}

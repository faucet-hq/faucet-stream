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

/// Every non-raw string literal in `src` as `(line, contents)`.
fn literals(src: &str) -> Vec<(usize, &str)> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let line_of = |at: usize| src[..at].matches('\n').count() + 1;
    while i < b.len() {
        if src[i..].starts_with("//") {
            i = src[i..].find('\n').map_or(b.len(), |j| i + j);
            continue;
        }
        if src[i..].starts_with("/*") {
            i = src[i + 2..].find("*/").map_or(b.len(), |j| i + 2 + j + 2);
            continue;
        }
        let ident_before = i > 0 && (b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'_');
        if b[i] == b'r' && !ident_before && matches!(b.get(i + 1), Some(b'"' | b'#')) {
            let mut j = i + 1;
            let mut hashes = 0;
            while b.get(j) == Some(&b'#') {
                hashes += 1;
                j += 1;
            }
            if b.get(j) == Some(&b'"') {
                let close = format!("\"{}", "#".repeat(hashes));
                i = src[j + 1..]
                    .find(&close)
                    .map_or(b.len(), |k| j + 1 + k + close.len());
                continue;
            }
        }
        if b[i] == b'\'' {
            // A char literal (`'"'`, `'\''`) or a lifetime.
            if b.get(i + 1) == Some(&b'\\') {
                i = src[i + 2..].find('\'').map_or(b.len(), |k| i + 2 + k + 1);
            } else if let Some(c) = src[i + 1..].chars().next()
                && src[i + 1 + c.len_utf8()..].starts_with('\'')
            {
                i += 1 + c.len_utf8() + 1;
            } else {
                i += 1;
            }
            continue;
        }
        if b[i] == b'"' {
            let start = i + 1;
            let mut j = start;
            while j < b.len() && b[j] != b'"' {
                j += if b[j] == b'\\' { 2 } else { 1 };
            }
            let end = j.min(b.len());
            out.push((line_of(i), &src[start..end]));
            i = end + 1;
            continue;
        }
        i += 1;
    }
    out
}

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

/// The part of a source file before its first `#[cfg(test)]` module.
fn non_test(src: &str) -> &str {
    const ATTR: &str = "#[cfg(test)]";
    let mut from = 0;
    while let Some(k) = src[from..].find(ATTR) {
        let at = from + k;
        let mut rest = src[at + ATTR.len()..].trim_start();
        // Further attributes may sit between `#[cfg(test)]` and `mod`.
        while rest.starts_with("#[") {
            rest = rest.find(']').map_or("", |e| rest[e + 1..].trim_start());
        }
        if rest.starts_with("mod ") {
            return &src[..at];
        }
        from = at + ATTR.len();
    }
    src
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

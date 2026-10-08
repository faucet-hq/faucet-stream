//! #789 CLI-146: a string literal wrapped without a trailing `\` keeps the next
//! line's indentation, so the message shows a long run of spaces. Scan the
//! CLI's sources for literals that still carry one.

use std::path::Path;

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
        for (i, line) in text.lines().enumerate() {
            let t = line.trim_start();
            if !t.starts_with('"') || t.contains("\\n") {
                continue;
            }
            let garbled = t[1..].trim().contains("        ");
            if garbled {
                hits.push(format!("{}:{}: {}", path.display(), i + 1, t));
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

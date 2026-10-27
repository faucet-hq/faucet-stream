//! Every state key is built through `faucet_core::StateKey` (#882). A key
//! assembled with `format!` bypasses its rendering and its migrations, so a
//! later change to either silently moves that key — a full re-sync or a
//! skipped range on the next run. This scans the non-test code of the CLI and
//! core for the two shapes such a key takes.

use std::path::{Path, PathBuf};

#[path = "support/rust_source.rs"]
mod rust_source;

const MARKERS: [&str; 8] = [
    "__sla__",
    "__profiling__",
    "__rollback__",
    "__status__",
    "__lease__",
    "__replication__",
    "__backfill__",
    "__serve_backfill__",
];

/// `{…}::{…}` keys that are not state keys.
const NOT_STATE_KEYS: [(&str, &str); 3] = [
    (
        "cli/src/notify/dispatch.rs",
        "notification dedupe / incident keys",
    ),
    ("cli/src/serve/registry.rs", "in-memory cancel-token keys"),
    ("cli/src/usage/model.rs", "usage report group labels"),
];

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
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
    for (i, _) in text.match_indices("#[cfg(test)]\n") {
        let mut rest = &text[i + "#[cfg(test)]\n".len()..];
        while rest.starts_with("#[") {
            rest = rest.split_once('\n').map_or("", |(_, r)| r);
        }
        if rest.starts_with("mod ") {
            return &text[..i];
        }
    }
    text
}

/// String literals (raw ones too) passed to something that builds a string
/// from parts: `format!` / `format_args!` / `concat!` (also when rustfmt moves
/// the literal to the next line), `push_str`, or `+`.
fn format_literals(text: &str) -> Vec<(usize, String)> {
    const BUILDERS: [&str; 5] = ["format!(", "format_args!(", "concat!(", "push_str(", "+"];
    rust_source::literal_spans(text, true)
        .into_iter()
        .filter(|(at, _)| {
            let before = text[..*at].trim_end();
            BUILDERS.iter().any(|b| before.ends_with(b))
        })
        .map(|(at, lit)| (text[..at].matches('\n').count() + 1, lit.to_string()))
        .collect()
}

fn offenders() -> Vec<String> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
    let mut files = Vec::new();
    rust_files(&root.join("cli/src"), &mut files);
    rust_files(&root.join("crates/core/src"), &mut files);
    let mut bad = Vec::new();
    for path in files {
        let rel = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        if rel == "crates/core/src/state_key.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for (line, lit) in format_literals(non_test(&text)) {
            let marker = MARKERS.iter().any(|m| lit.contains(m));
            let joined = lit.contains("}::{") && !NOT_STATE_KEYS.iter().any(|(f, _)| *f == rel);
            if marker || (joined && looks_like_key(&text, line)) {
                bad.push(format!("{rel}:{line}: format!(\"{lit}\")"));
            }
        }
    }
    bad
}

/// Whether the source around `line` binds, returns or passes something named
/// like a state key.
fn looks_like_key(text: &str, line: usize) -> bool {
    let from = line.saturating_sub(8);
    text.lines()
        .skip(from)
        .take(line - from)
        .map(str::to_lowercase)
        .any(|src| src.contains("state") || src.contains("key") || src.contains("base"))
}

#[test]
fn no_state_key_is_built_with_format() {
    let bad = offenders();
    assert!(
        bad.is_empty(),
        "build these state keys through faucet_core::StateKey:\n{}",
        bad.join("\n")
    );
}

/// Every key the CLI derives renders byte-for-byte as it did before #882 — a
/// moved key is a lost bookmark.
#[test]
fn every_cli_key_keeps_its_rendering() {
    use faucet_cli::executor::StateScope;
    let scope = StateScope {
        namespace: Some("acme".into()),
        on_key: None,
    };
    let base = "orders::lines::42";
    let cases: Vec<(String, &str)> = vec![
        (StateScope::default().prefix("orders"), "orders"),
        (scope.prefix("orders"), "acme::orders"),
        (
            scope.key("orders").with_row("lines").render(),
            "acme::orders::lines",
        ),
        (
            faucet_cli::sla::sla_state_key(base),
            "orders::lines::42::__sla__",
        ),
        (
            faucet_cli::profiling::profiling_state_key(base),
            "orders::lines::42::__profiling__",
        ),
        (
            faucet_cli::rollback::state::index_key(base),
            "orders::lines::42::__rollback__",
        ),
        (
            faucet_cli::rollback::state::marker_key(base, "r1"),
            "orders::lines::42::__rollback__::r1",
        ),
        (
            faucet_cli::pipeline_state::keys::status_key(base),
            "orders::lines::42::__status__",
        ),
        (
            faucet_cli::pipeline_state::keys::lease_key(base),
            "orders::lines::42::__lease__",
        ),
        (
            faucet_cli::replication::state::marker_key("orders"),
            "orders::__replication__",
        ),
        (
            faucet_cli::replication::state::cdc_state_key("orders"),
            "orders::cdc",
        ),
        (
            faucet_cli::backfill::state::marker_key("orders", "0123"),
            "orders::__backfill__::0123",
        ),
        (
            faucet_cli::backfill::state::unit_state_key("orders", "0123", "2026-01-01"),
            "orders::backfill::0123::2026-01-01",
        ),
        (
            faucet_cli::discovery_matrix::state_key_component("a b", true),
            "ab.he63f991904833892",
        ),
        // A child's parent key is typed (key scheme 3): a number and a
        // string of the same text get different keys.
        (
            scope
                .key("orders")
                .with_row("lines")
                .with_parent_value(&serde_json::json!(42))
                .render(),
            "acme::orders::lines::_n.42",
        ),
        (
            scope
                .key("orders")
                .with_row("lines")
                .with_parent_value(&serde_json::json!("42"))
                .render(),
            "acme::orders::lines::42",
        ),
    ];
    for (got, want) in cases {
        assert_eq!(got, want);
    }
}

#[test]
fn the_scan_finds_a_raw_key() {
    let lits = format_literals("let k = format!(\"{base}::__sla__\");\nformat!(\"{a}::{b}\")");
    assert_eq!(lits[0], (1, "{base}::__sla__".to_string()));
    assert_eq!(lits[1].0, 2);
    assert!(looks_like_key("let state_key = x;", 1));
    assert!(!looks_like_key("let label = x;", 1));
    assert_eq!(non_test("a\n#[cfg(test)]\nmod tests {}"), "a\n");
    assert_eq!(
        non_test("a\n#[cfg(test)]\n#[allow(dead_code)] // why\nmod tests {}"),
        "a\n"
    );
}

/// The scan sees a key however the string is built: a rustfmt-wrapped
/// `format!`, a raw string, `concat!`, `push_str` and `+`.
#[test]
fn the_scan_sees_every_way_a_key_string_is_built() {
    let src = concat!(
        "fn a(p: &str, r: &str) -> String {\n",
        "    let key = format!(\n        \"{}::{}::__status__\",\n        p, r\n    );\n",
        "    let k2 = format!(r\"{}::__lease__\", p);\n",
        "    let k3 = concat!(\"::__sla__\", \"x\");\n",
        "    let mut k4 = String::new();\n    k4.push_str(\"::__rollback__\");\n",
        "    let k5 = p.to_string() + \"::__profiling__\";\n",
        "    let note = \"__status__ is a marker\";\n",
        "}\n",
    );
    let found: Vec<String> = format_literals(src).into_iter().map(|(_, l)| l).collect();
    assert_eq!(
        found,
        vec![
            "{}::{}::__status__",
            "{}::__lease__",
            "::__sla__",
            "::__rollback__",
            "::__profiling__",
        ],
        "a bare literal that is not built into a string is not a key"
    );
}

//! The prebuilt binaries' feature set (`dist-workspace.toml`) stays a real set
//! of CLI features, reaches every one of them (#941) and matches what the
//! installation page promises (#820).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

fn repo_file(rel: &str) -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    std::fs::read_to_string(root.join(rel)).unwrap_or_else(|e| panic!("read {rel}: {e}"))
}

/// The quoted names in the `features = [...]` array that follows `key`.
fn quoted_list_after(text: &str, key: &str) -> Vec<String> {
    let start = text
        .lines()
        .position(|l| l.trim_start().starts_with(key))
        .unwrap_or_else(|| panic!("no `{key}` line"));
    let line = text.lines().nth(start).unwrap();
    let inner = &line[line.find('[').expect("[") + 1..line.rfind(']').expect("]")];
    inner
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn shipped() -> Vec<String> {
    quoted_list_after(&repo_file("dist-workspace.toml"), "features =")
}

#[test]
fn shipped_features_are_declared_cli_features() {
    let cargo = repo_file("cli/Cargo.toml");
    let features = &cargo[cargo.find("[features]").expect("[features]")..];
    for f in shipped() {
        assert!(
            features
                .lines()
                .any(|l| l.trim_start().starts_with(&format!("{f} ="))),
            "dist feature `{f}` is not a faucet-cli feature"
        );
    }
}

/// `cli/Cargo.toml`'s `[features]` table: name → the features it enables
/// (dependency forwards and `dep:` entries dropped).
fn cli_features() -> BTreeMap<String, Vec<String>> {
    let cargo = repo_file("cli/Cargo.toml");
    let table = &cargo[cargo.find("\n[features]").expect("[features]") + 1..];
    let mut out = BTreeMap::new();
    let mut current: Option<(String, String)> = None;
    for line in table.lines().skip(1) {
        let line = line.split('#').next().unwrap_or("").trim();
        if current.is_none() {
            if line.starts_with('[') {
                break;
            }
            let Some((name, rest)) = line.split_once('=') else {
                continue;
            };
            current = Some((name.trim().to_string(), String::new()));
            current.as_mut().unwrap().1.push_str(rest);
        } else {
            current.as_mut().unwrap().1.push_str(line);
        }
        if current.as_ref().is_some_and(|(_, body)| body.contains(']')) {
            let (name, body) = current.take().unwrap();
            let inner = &body[body.find('[').expect("[") + 1..body.rfind(']').expect("]")];
            let enables = inner
                .split(',')
                .map(|e| e.trim().trim_matches('"').to_string())
                .filter(|e| !e.is_empty() && !e.contains('/') && !e.starts_with("dep:"))
                .collect();
            out.insert(name, enables);
        }
    }
    out
}

fn reachable(features: &BTreeMap<String, Vec<String>>, roots: &[String]) -> BTreeSet<String> {
    let mut seen = BTreeSet::new();
    let mut stack: Vec<String> = roots.to_vec();
    while let Some(name) = stack.pop() {
        if let Some(enables) = features.get(&name) {
            if seen.insert(name) {
                stack.extend(enables.iter().cloned());
            }
        }
    }
    seen
}

#[test]
fn every_cli_feature_ships_prebuilt() {
    let features = cli_features();
    assert!(
        features.len() > 50,
        "parsed only {} features",
        features.len()
    );
    // cargo-dist builds the default features on top of the listed ones.
    let mut roots = shipped();
    roots.push("default".to_string());
    let shipped = reachable(&features, &roots);
    let missing: Vec<_> = features.keys().filter(|f| !shipped.contains(*f)).collect();
    assert!(
        missing.is_empty(),
        "the prebuilt binaries miss CLI features: {missing:?}"
    );
}

#[test]
fn the_feature_walk_reports_what_a_hand_picked_set_misses() {
    let features = cli_features();
    let picked = reachable(&features, &["serve".to_string(), "mcp".to_string()]);
    assert!(picked.contains("serve") && picked.contains("mcp"));
    assert!(!picked.contains("triggers") && !picked.contains("source-oracle"));
}

#[test]
fn installation_page_names_every_shipped_feature() {
    let page = repo_file("docs/book/src/getting-started/installation.md");
    let start = page
        .find("The prebuilt binary includes")
        .expect("prebuilt feature paragraph");
    let paragraph = &page[start..start + page[start..].find("\n\n").unwrap()];
    for f in shipped() {
        assert!(
            paragraph.contains(&format!("`{f}`")),
            "installation page does not mention shipped feature `{f}`:\n{paragraph}"
        );
    }
}

#[test]
fn windows_job_builds_the_same_set() {
    let ci = repo_file(".github/workflows/ci.yml");
    let line = ci
        .lines()
        .find(|l| l.trim_start().starts_with("SHIPPED_FEATURES:"))
        .expect("SHIPPED_FEATURES");
    let value = line.split_once(':').unwrap().1.trim().trim_matches('"');
    let ci_set: Vec<&str> = value.split(',').collect();
    assert_eq!(ci_set, shipped(), "ci.yml SHIPPED_FEATURES drifted");
}

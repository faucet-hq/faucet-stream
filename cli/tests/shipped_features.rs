//! The prebuilt binaries' feature set (`dist-workspace.toml`) stays a real set
//! of CLI features and matches what the installation page promises (#820).

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

#[test]
fn advertised_runtime_features_ship_prebuilt() {
    let shipped = shipped();
    for f in ["mcp", "notify", "secrets", "catalog", "serve", "templates"] {
        assert!(
            shipped.iter().any(|s| s == f),
            "`{f}` is advertised but missing from the prebuilt binaries: {shipped:?}"
        );
    }
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

//! Every YAML parse and render in the CLI goes through `faucet_cli::yaml`
//! (#929), so swapping the YAML library changes one file.

use std::path::Path;

const LIBRARY: &str = "serde_yaml";
const BOUNDARY: &str = "src/yaml.rs";

fn scan(dir: &Path, root: &Path, offenders: &mut Vec<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            scan(&path, root, offenders);
            continue;
        }
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let rel = path.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
        if rel == BOUNDARY || rel == "tests/yaml_boundary.rs" {
            continue;
        }
        let text = std::fs::read_to_string(&path).unwrap();
        for (n, line) in text.lines().enumerate() {
            if line.contains(LIBRARY) {
                offenders.push(format!("{rel}:{}: {}", n + 1, line.trim()));
            }
        }
    }
}

#[test]
fn the_yaml_library_is_named_only_in_the_yaml_module() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut offenders = Vec::new();
    for dir in ["src", "tests", "examples", "benches"] {
        let dir = root.join(dir);
        if dir.is_dir() {
            scan(&dir, root, &mut offenders);
        }
    }
    assert!(
        offenders.is_empty(),
        "use faucet_cli::yaml (crate::yaml) instead of {LIBRARY}:\n{}",
        offenders.join("\n")
    );
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    let dev = manifest.split("[dev-dependencies]").nth(1).unwrap_or("");
    let dev = dev.split("\n[").next().unwrap_or("");
    assert!(
        !dev.contains(LIBRARY),
        "tests reach YAML through faucet_cli::yaml, not a {LIBRARY} dev-dependency"
    );
}

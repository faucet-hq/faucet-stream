//! The format-v3 fixture tables under `fixtures/v3/warehouse` are generated
//! files (`fixtures/v3/generate.sh`: Spark 4 + Apache Iceberg). This checks
//! that every one of them is the file the generator wrote, and that none was
//! added or removed by hand.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/v3")
}

fn files_under(dir: &Path, root: &Path, out: &mut BTreeSet<String>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            files_under(&path, root, out);
        } else {
            let relative = path.strip_prefix(root).unwrap();
            out.insert(relative.to_string_lossy().replace('\\', "/"));
        }
    }
}

#[test]
fn the_fixture_files_are_the_generated_ones() {
    let root = fixture_dir();
    let sums = std::fs::read_to_string(root.join("SHA256SUMS")).unwrap();
    let mut listed = BTreeSet::new();
    for line in sums.lines() {
        let (want, file) = line.split_once("  ").expect("`<sha256>  <path>`");
        let bytes = std::fs::read(root.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
        let got: String = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(got, want, "{file} is not the generated file");
        listed.insert(file.to_string());
    }
    let mut present = BTreeSet::new();
    files_under(&root.join("warehouse"), &root, &mut present);
    assert_eq!(present, listed, "files on disk against SHA256SUMS");
    for table in ["dv", "upgraded", "variant"] {
        assert!(
            listed
                .iter()
                .any(|f| f.starts_with(&format!("warehouse/db/{table}/metadata/"))),
            "table {table} is missing"
        );
    }
    assert!(
        listed.iter().any(|f| f.ends_with("-deletes.puffin")),
        "no deletion vector in the fixture"
    );
}

//! A host's path guard is asked about every file a listing finds, before any
//! is read; a refused file fails the read with the guard's reason.

use faucet_core::Source;
use faucet_source_file::{FileSource, FileSourceConfig};
use std::collections::HashMap;
use std::path::Path;

fn install() {
    faucet_core::path_guard::install(Box::new(|p: &Path| {
        p.file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("guarded-by-test"))
            .then(|| "is refused by the test guard".to_string())
    }));
}

#[tokio::test]
async fn a_listed_file_the_guard_refuses_fails_the_read() {
    install();
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("a.jsonl"), "{\"id\":1}\n").unwrap();
    std::fs::write(dir.path().join("guarded-by-test.jsonl"), "{\"id\":2}\n").unwrap();
    for path in [
        dir.path().display().to_string(),
        format!("{}/*.jsonl", dir.path().display()),
    ] {
        let src = FileSource::new(FileSourceConfig::new(&path)).unwrap();
        let err = src.fetch_with_context(&HashMap::new()).await.unwrap_err();
        assert!(
            err.to_string().contains("refused by the test guard"),
            "{path}: {err}"
        );
    }
    let src = FileSource::new(FileSourceConfig::new(
        dir.path().join("a.jsonl").display().to_string(),
    ))
    .unwrap();
    assert_eq!(
        src.fetch_with_context(&HashMap::new()).await.unwrap().len(),
        1
    );
}

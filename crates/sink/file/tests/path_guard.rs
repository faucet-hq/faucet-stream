//! A host's path guard is asked before the file sink creates a directory or
//! moves a finished file into place; a refused destination is left as it was.

use faucet_core::Sink;
use faucet_sink_file::FileSink;
use serde_json::json;
use std::path::Path;

fn install() {
    faucet_core::path_guard::install(Box::new(|p: &Path| {
        p.file_name()
            .is_some_and(|n| n.to_string_lossy().starts_with("guarded-by-test"))
            .then(|| "is refused by the test guard".to_string())
    }));
}

fn sink(path: &Path) -> FileSink {
    FileSink::new(serde_json::from_value(json!({"path": path.display().to_string()})).unwrap())
        .unwrap()
}

#[tokio::test]
async fn a_destination_the_guard_refuses_is_not_replaced() {
    install();
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("guarded-by-test.jsonl");
    std::fs::write(&dest, "kept\n").unwrap();
    let s = sink(&dest);
    let written = match s.write_batch(&[json!({"id": 1})]).await {
        Ok(_) => s.flush().await,
        Err(e) => Err(e),
    };
    let err = written.unwrap_err();
    assert!(
        err.to_string().contains("refused by the test guard"),
        "{err}"
    );
    assert_eq!(std::fs::read_to_string(&dest).unwrap(), "kept\n");
}

#[tokio::test]
async fn a_directory_the_guard_refuses_is_not_created() {
    install();
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("guarded-by-test-dir");
    let s = sink(&nested.join("out.jsonl"));
    let written = match s.write_batch(&[json!({"id": 1})]).await {
        Ok(_) => s.flush().await,
        Err(e) => Err(e),
    };
    let err = written.unwrap_err();
    assert!(
        err.to_string().contains("refused by the test guard"),
        "{err}"
    );
    assert!(!nested.exists());
    let ok = sink(&dir.path().join("out.jsonl"));
    ok.write_batch(&[json!({"id": 1})]).await.unwrap();
    ok.flush().await.unwrap();
    assert!(dir.path().join("out.jsonl").exists());
}

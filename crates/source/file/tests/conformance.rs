//! `faucet-conformance` battery for the file source: schema validity (1),
//! bounded-memory streaming (2), bookmark round-trip (3), errors-not-panics
//! (6), `batch_size: 0` (9), connector name (10), preflight (11) and the
//! discover round-trip.

use faucet_core::Source;
use faucet_source_file::{FileSource, FileSourceConfig, IncrementalBy};
use std::io::Write;

fn fixture(total: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for part in 0..2 {
        let mut f = std::fs::File::create(dir.path().join(format!("part-{part}.jsonl"))).unwrap();
        for i in 0..total / 2 {
            writeln!(f, "{{\"id\":{},\"part\":{part}}}", i + part * total / 2).unwrap();
        }
    }
    dir
}

fn path(dir: &tempfile::TempDir) -> String {
    dir.path().to_string_lossy().into_owned()
}

#[test]
fn conformance_config_schema_valid() {
    let source = FileSource::new(FileSourceConfig::new("/tmp/x")).unwrap();
    faucet_conformance::assert_config_schema_valid(&source);
}

#[test]
fn conformance_connector_name_nonempty() {
    let source = FileSource::new(FileSourceConfig::new("/tmp/x")).unwrap();
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "file");
}

#[tokio::test]
async fn conformance_bounded_memory() {
    let dir = fixture(4_000);
    let source = FileSource::new(FileSourceConfig::new(path(&dir)).with_batch_size(250)).unwrap();
    faucet_conformance::assert_bounded_memory(&source, 250, 4_000).await;
}

#[tokio::test]
async fn conformance_bookmark_roundtrip() {
    let dir = fixture(10);
    let source =
        FileSource::new(FileSourceConfig::new(path(&dir)).incremental(IncrementalBy::Name))
            .unwrap();
    faucet_conformance::assert_bookmark_roundtrip(&source).await;
}

#[tokio::test]
async fn conformance_batch_size_zero_single_page() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("one.jsonl"), "{\"a\":1}\n{\"a\":2}\n").unwrap();
    let source = FileSource::new(FileSourceConfig::new(path(&dir)).with_batch_size(0)).unwrap();
    faucet_conformance::assert_batch_size_zero_single_page(&source).await;
}

#[tokio::test]
async fn conformance_preflight_check_wellformed() {
    let dir = fixture(4);
    let source = FileSource::new(FileSourceConfig::new(path(&dir))).unwrap();
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    let source =
        FileSource::new(FileSourceConfig::new("/nonexistent/faucet-conformance/in")).unwrap();
    faucet_conformance::assert_errors_not_panics(&source).await;
}

#[tokio::test]
async fn conformance_discover_roundtrips() {
    let dir = fixture(4);
    let base = serde_json::to_value(FileSourceConfig::new(path(&dir))).unwrap();
    let source = FileSource::new(FileSourceConfig::new(path(&dir))).unwrap();
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let cfg = faucet_conformance::merge_config_patch(base.clone(), &patch);
        async move {
            let cfg: FileSourceConfig = serde_json::from_value(cfg).unwrap();
            Box::new(FileSource::new(cfg).unwrap()) as Box<dyn Source>
        }
    })
    .await;
}

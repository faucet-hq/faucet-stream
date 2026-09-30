//! The file source never reads the file sink's unfinished output (#783 C2):
//! scratch files (`*.faucet-tmp`, `*.faucet-tmp-body`, …) and the swap area
//! of an overwrite run that has not committed are skipped, whether the path
//! is a directory, a recursive directory or a glob.

use faucet_core::Source;
use faucet_source_file::{FileSource, FileSourceConfig, FileSourceFormat};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;

fn write(dir: &Path, name: &str, body: &str) {
    let p = dir.join(name);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, body).unwrap();
}

async fn read(cfg: FileSourceConfig) -> Vec<Value> {
    let src = FileSource::new(cfg).unwrap();
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.extend(p.unwrap().records);
    }
    out
}

fn output_with_scratch(dir: &Path) {
    write(dir, "x.jsonl", "{\"n\":1}\n");
    for scratch in [
        "x.jsonl.faucet-tmp",
        "y.jsonl.faucet-tmp-body",
        "z.jsonl.faucet-tmp-old",
        "z.jsonl.faucet-tmp-seal",
        "z.jsonl.faucet-tmp-prev",
    ] {
        write(dir, scratch, "{\"n\":\"partial\"}\n");
    }
    write(
        dir,
        ".faucet-overwrite-x.jsonl/x.jsonl",
        "{\"n\":\"uncommitted\"}\n",
    );
}

#[tokio::test]
async fn a_directory_read_skips_scratch_and_swap_files() {
    let dir = tempfile::tempdir().unwrap();
    output_with_scratch(dir.path());
    for recursive in [false, true] {
        let mut cfg = FileSourceConfig::new(dir.path().to_string_lossy());
        cfg.format = FileSourceFormat::JsonLines;
        cfg.recursive = recursive;
        assert_eq!(
            read(cfg).await,
            vec![json!({"n": 1})],
            "recursive: {recursive}"
        );
    }
}

#[tokio::test]
async fn a_glob_read_skips_scratch_and_swap_files() {
    let dir = tempfile::tempdir().unwrap();
    output_with_scratch(dir.path());
    let mut cfg = FileSourceConfig::new(format!("{}/**/*", dir.path().to_string_lossy()));
    cfg.format = FileSourceFormat::JsonLines;
    assert_eq!(read(cfg).await, vec![json!({"n": 1})]);
}

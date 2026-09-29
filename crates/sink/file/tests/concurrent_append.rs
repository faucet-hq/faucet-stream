//! Many writers appending to one file at once (#779): every record lands
//! exactly once and every line stays whole, for plain, compressed and
//! encrypted JSON Lines and for CSV.
#![cfg(all(feature = "file-formats", feature = "encryption"))]

use faucet_core::{Sink, Source};
use faucet_sink_file::FileSink;
use faucet_source_file::{FileSource, FileSourceConfig};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const WRITERS: usize = 16;
const PAGES: usize = 5;
const PER_PAGE: usize = 20;

fn text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

async fn read(path: &str, extra: &Value) -> Vec<Value> {
    let mut cfg = json!({ "path": path });
    for (k, v) in extra.as_object().unwrap() {
        cfg[k] = v.clone();
    }
    let cfg: FileSourceConfig = serde_json::from_value(cfg).unwrap();
    FileSource::new(cfg).unwrap().fetch_all().await.unwrap()
}

async fn hammer(name: &str, extra: Value) {
    let dir = tempfile::tempdir().unwrap();
    let path = format!("{}/{name}", dir.path().display());
    let mut tasks = Vec::new();
    for w in 0..WRITERS {
        let mut cfg = json!({ "path": path, "mode": "append" });
        for (k, v) in extra.as_object().unwrap() {
            cfg[k] = v.clone();
        }
        tasks.push(tokio::spawn(async move {
            let sink = FileSink::new(serde_json::from_value(cfg).unwrap()).unwrap();
            for p in 0..PAGES {
                let page: Vec<Value> = (0..PER_PAGE)
                    .map(|i| json!({ "w": w, "i": p * PER_PAGE + i, "pad": "x".repeat(40) }))
                    .collect();
                sink.write_batch(&page).await.unwrap();
                sink.flush().await.unwrap();
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let back = read(&path, &extra).await;
    let mut seen: BTreeMap<(String, String), usize> = BTreeMap::new();
    for r in &back {
        assert_eq!(text(&r["pad"]), "x".repeat(40), "{name}: a torn record {r}");
        *seen.entry((text(&r["w"]), text(&r["i"]))).or_default() += 1;
    }
    assert_eq!(back.len(), WRITERS * PAGES * PER_PAGE, "{name}");
    assert!(seen.values().all(|n| *n == 1), "{name}: duplicates");
    assert_eq!(seen.len(), WRITERS * PAGES * PER_PAGE, "{name}");
    let leftovers: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n != name)
        .collect();
    assert!(leftovers.is_empty(), "{name}: scratch left behind {leftovers:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appenders_to_json_lines_keep_every_record() {
    hammer("out.jsonl", json!({})).await;
    hammer("out.jsonl.gz", json!({})).await;
    hammer("out.jsonl.zst", json!({})).await;
    hammer("sealed.jsonl", json!({ "encryption": { "key": "append-key" } })).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_appenders_to_csv_write_one_header_and_every_row() {
    hammer("out.csv", json!({})).await;
    hammer("out.csv.gz", json!({})).await;
    let dir = tempfile::tempdir().unwrap();
    let path = format!("{}/h.csv", dir.path().display());
    let one = FileSink::new(serde_json::from_value(json!({"path": path, "mode": "append"})).unwrap())
        .unwrap();
    one.write_batch(&[json!({"a": 1})]).await.unwrap();
    one.flush().await.unwrap();
    let body = std::fs::read_to_string(&path).unwrap();
    assert_eq!(body, "a\n1\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_appender_with_a_new_column_widens_the_csv_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = format!("{}/w.csv", dir.path().display());
    let open = |extra: Value| {
        let mut cfg = json!({ "path": path, "mode": "append" });
        for (k, v) in extra.as_object().unwrap() {
            cfg[k] = v.clone();
        }
        FileSink::new(serde_json::from_value(cfg).unwrap()).unwrap()
    };
    let a = open(json!({}));
    let b = open(json!({}));
    a.write_batch(&[json!({"id": 1, "x": "a"})]).await.unwrap();
    b.write_batch(&[json!({"id": 2, "y": "b"})]).await.unwrap();
    a.flush().await.unwrap();
    b.flush().await.unwrap();
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "id,x,y\n1,a,\n2,,b\n");
    let warn = open(json!({"csv": {"on_unknown_field": "warn"}}));
    warn.write_batch(&[json!({"id": 3, "z": "dropped", "x": "c"})])
        .await
        .unwrap();
    warn.flush().await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "id,x,y\n1,a,\n2,,b\n3,c,\n"
    );
    let strict = open(json!({"csv": {"on_unknown_field": "error"}}));
    let refused = match strict.write_batch(&[json!({"id": 4, "q": 1})]).await {
        Err(_) => true,
        Ok(_) => strict.flush().await.is_err(),
    };
    assert!(refused, "`on_unknown_field: error` refuses a column the file lacks");
}

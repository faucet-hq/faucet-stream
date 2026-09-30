#![cfg(feature = "file-formats")]

//! End-to-end: a REST source (wiremock) through the real pipeline into the
//! file sink, read back with the file source.

use async_trait::async_trait;
use faucet_core::{FaucetError, Pipeline, Sink, Source, StreamPage};
use faucet_sink_file::FileSink;
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::stream::BoxStream;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn rows(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| json!({ "id": i, "name": format!("n{i}"), "ok": i % 2 == 0 }))
        .collect()
}

async fn rest(n: usize) -> (MockServer, RestStream) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": rows(n) })))
        .mount(&server)
        .await;
    let src = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/items")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap();
    (server, src)
}

fn sink(v: Value) -> FileSink {
    FileSink::new(serde_json::from_value(v).unwrap()).unwrap()
}

fn p(dir: &Path, rel: &str) -> String {
    format!("{}/{rel}", dir.display())
}

/// Every scalar as text, so formats that type values differently compare.
fn norm(records: &[Value]) -> Vec<BTreeMap<String, String>> {
    let mut out: Vec<_> = records
        .iter()
        .map(|r| {
            r.as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| {
                    let s = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), s)
                })
                .collect()
        })
        .collect();
    out.sort_by_key(|m: &BTreeMap<String, String>| m["id"].parse::<i64>().unwrap());
    out
}

#[tokio::test]
async fn rest_to_every_format_round_trips_through_the_file_source() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "x.jsonl",
        "x.jsonl.gz",
        "x.csv",
        "x.csv.zst",
        "x.json",
        "x.xml",
        "x.xlsx",
        "x.avro",
        "x.parquet",
    ] {
        let (_server, src) = rest(25).await;
        let out = p(dir.path(), name);
        let s = sink(json!({ "path": out }));
        let result = Pipeline::new(&src, &s).run().await.unwrap();
        assert_eq!(result.records_written, 25, "{name}");
        let back = faucet_source_file::read_records(out.clone()).await.unwrap();
        assert_eq!(norm(&back), norm(&rows(25)), "{name}");
        assert!(!Path::new(&format!("{out}.faucet-tmp")).exists(), "{name}");
    }
}

#[tokio::test]
async fn typed_formats_keep_types() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["t.jsonl", "t.json", "t.avro", "t.parquet"] {
        let (_server, src) = rest(4).await;
        let out = p(dir.path(), name);
        Pipeline::new(&src, &sink(json!({ "path": out })))
            .run()
            .await
            .unwrap();
        let mut back = faucet_source_file::read_records(out).await.unwrap();
        back.sort_by_key(|r| r["id"].as_i64().unwrap());
        assert_eq!(back, rows(4), "{name}");
    }
}

#[tokio::test]
async fn rollover_by_records_names_parts_by_the_template() {
    let dir = tempfile::tempdir().unwrap();
    let (_server, src) = rest(10).await;
    let s = sink(json!({
        "path": p(dir.path(), "out/2026-09-27/part-{part}.csv"),
        "max_records_per_file": 4,
    }));
    Pipeline::new(&src, &s).run().await.unwrap();
    drop(s);
    let day = dir.path().join("out/2026-09-27");
    let mut names: Vec<String> = std::fs::read_dir(&day)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(
        names,
        ["part-00001.csv", "part-00002.csv", "part-00003.csv"]
    );
    let back = faucet_source_file::read_records(format!("{}/", day.display()))
        .await
        .unwrap();
    assert_eq!(norm(&back), norm(&rows(10)));
}

#[tokio::test]
async fn parquet_rollover_and_schema_widening() {
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "w.parquet");
    let s = sink(json!({ "path": out, "parquet": {"compression": "zstd"} }));
    s.write_batch(&[json!({"a": 1})]).await.unwrap();
    s.flush().await.unwrap();
    s.write_batch(&[json!({"a": 2, "b": "x"})]).await.unwrap();
    s.write_batch(&[json!({"a": 3})]).await.unwrap();
    s.flush().await.unwrap();
    let back = faucet_source_file::read_records(out.clone()).await.unwrap();
    assert_eq!(
        back,
        vec![
            json!({"a": 1, "b": null}),
            json!({"a": 2, "b": "x"}),
            json!({"a": 3, "b": null})
        ]
    );
    let e = s.write_batch(&[json!({"a": "text"})]).await.unwrap_err();
    assert!(e.to_string().contains("changed type"), "{e}");

    let rolled = sink(json!({
        "path": p(dir.path(), "r/"),
        "format": "parquet",
        "max_records_per_file": 3,
    }));
    let (_server, src) = rest(7).await;
    Pipeline::new(&src, &rolled).run().await.unwrap();
    assert_eq!(std::fs::read_dir(dir.path().join("r")).unwrap().count(), 3);
}

/// A source that stalls forever after its first page, like a run that is
/// killed mid-way.
struct Stalls;

#[async_trait]
impl Source for Stalls {
    async fn fetch_with_context(
        &self,
        _ctx: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        unreachable!()
    }

    fn config_schema(&self) -> Value {
        json!({})
    }

    fn stream_pages<'a>(
        &'a self,
        _ctx: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> BoxStream<'a, Result<StreamPage, FaucetError>> {
        Box::pin(async_stream::stream! {
            yield Ok(StreamPage { records: vec![json!({"id": 0})], bookmark: None });
            futures::future::pending::<()>().await;
        })
    }
}

#[tokio::test]
async fn a_killed_run_leaves_no_final_file_and_a_resumed_run_completes_it() {
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "k.jsonl");
    let s = sink(json!({ "path": out }));
    let pipeline = Pipeline::new(&Stalls, &s);
    let timed_out = tokio::time::timeout(std::time::Duration::from_millis(300), pipeline.run())
        .await
        .is_err();
    assert!(timed_out);
    drop(pipeline);
    // A hard kill never runs destructors.
    std::mem::forget(s);
    assert!(!Path::new(&out).exists(), "no final file after a kill");
    assert!(Path::new(&format!("{out}.faucet-tmp")).exists());

    let (_server, src) = rest(3).await;
    Pipeline::new(&src, &sink(json!({ "path": out })))
        .run()
        .await
        .unwrap();
    let back = faucet_source_file::read_records(out.clone()).await.unwrap();
    assert_eq!(back.len(), 3);
    assert!(!Path::new(&format!("{out}.faucet-tmp")).exists());
}

#[tokio::test]
async fn overwrite_run_replaces_atomically_and_a_failed_one_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = json!({
        "path": p(dir.path(), "o/rows-{part}.jsonl"),
        "write_mode": "overwrite",
        "max_records_per_file": 2,
    });
    let (_s1, src) = rest(5).await;
    Pipeline::new(&src, &sink(cfg.clone())).run().await.unwrap();
    assert_eq!(std::fs::read_dir(dir.path().join("o")).unwrap().count(), 3);
    let (_s2, src) = rest(3).await;
    Pipeline::new(&src, &sink(cfg.clone())).run().await.unwrap();
    let names: Vec<String> = std::fs::read_dir(dir.path().join("o"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(names.len(), 2, "{names:?}");
    let before = faucet_source_file::read_records(p(dir.path(), "o/"))
        .await
        .unwrap();
    assert_eq!(before.len(), 3);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    let failing = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/items")
            .pagination(PaginationStyle::None)
            .max_retries(0),
    )
    .unwrap();
    assert!(Pipeline::new(&failing, &sink(cfg)).run().await.is_err());
    let after = faucet_source_file::read_records(p(dir.path(), "o/"))
        .await
        .unwrap();
    assert_eq!(after, before);
    assert_eq!(std::fs::read_dir(dir.path().join("o")).unwrap().count(), 2);
}

#[tokio::test]
async fn append_mode_builds_a_multi_member_gzip_file() {
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "a.jsonl.gz");
    for _ in 0..3 {
        let s = sink(json!({ "path": out, "mode": "append" }));
        let (_server, src) = rest(2).await;
        Pipeline::new(&src, &s).run().await.unwrap();
    }
    let back = faucet_source_file::read_records(out).await.unwrap();
    assert_eq!(back.len(), 6);
}

#[tokio::test]
async fn csv_append_keeps_one_header_and_widens_columns() {
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "c.csv");
    let s = sink(json!({ "path": out }));
    s.write_batch(&[json!({"a": "1"})]).await.unwrap();
    s.flush().await.unwrap();
    s.write_batch(&[json!({"a": "2", "b": "x"})]).await.unwrap();
    s.flush().await.unwrap();
    drop(s);
    let s = sink(json!({ "path": out, "mode": "append" }));
    s.write_batch(&[json!({"b": "y", "a": "3"})]).await.unwrap();
    s.flush().await.unwrap();
    let text = std::fs::read_to_string(&out).unwrap();
    assert_eq!(text.lines().next(), Some("a,b"), "{text}");
    assert_eq!(text.lines().count(), 4, "{text}");
    let back = faucet_source_file::read_records(out).await.unwrap();
    assert_eq!(back[2], json!({"a": "3", "b": "y"}));
    let e = sink(json!({ "path": p(dir.path(), "n.csv") }))
        .write_batch(&[json!([1, 2])])
        .await
        .unwrap_err();
    assert!(e.to_string().contains("not an object"), "{e}");
}

#[tokio::test]
async fn usable_as_the_dlq_sink() {
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "dlq/dead.jsonl");
    let dlq_sink = sink(json!({ "path": out }));
    dlq_sink
        .write_batch(&[json!({"payload": {"id": 1}, "error": {"message": "x"}})])
        .await
        .unwrap();
    dlq_sink.flush().await.unwrap();
    assert_eq!(
        faucet_source_file::read_records(out).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn columnar_batches_write_parquet_and_avro() {
    let dir = tempfile::tempdir().unwrap();
    let batch = faucet_core::columnar::values_to_record_batch_inferred(&rows(6)).unwrap();
    for name in ["c.parquet", "c.avro"] {
        let out = p(dir.path(), name);
        let s = sink(json!({ "path": out, "max_records_per_file": 4 }));
        assert!(s.supports_columnar());
        assert_eq!(s.write_batch_columnar(&batch).await.unwrap(), 6);
        assert_eq!(s.write_batch_columnar(&batch.slice(0, 0)).await.unwrap(), 0);
        s.flush().await.unwrap();
        let glob = p(dir.path(), &name.replace("c.", "c-*."));
        let back = faucet_source_file::read_records(glob).await.unwrap();
        assert_eq!(norm(&back), norm(&rows(6)), "{name}");
    }
    let s = sink(json!({ "path": p(dir.path(), "b.parquet"), "max_bytes_per_file": 1 }));
    s.write_batch_columnar(&batch).await.unwrap();
    s.flush().await.unwrap();
    assert!(!sink(json!({ "path": p(dir.path(), "x.csv") })).supports_columnar());
}

#![allow(deprecated)]

//! End-to-end reads through `FileSource` against real files on disk and a
//! wiremock HTTP server (#720, #719).
#![cfg(feature = "file-formats")]

use faucet_core::check::CheckContext;
use faucet_core::file_format::{FileFormat, FormatOptions, encode};
use faucet_core::{MemoryStateStore, Pipeline, Sink, Source, StateStore};
use faucet_source_file::{FileSource, FileSourceConfig, FileSourceFormat, IncrementalBy};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

fn write(dir: &Path, name: &str, bytes: &[u8]) -> String {
    let p = dir.join(name);
    if let Some(parent) = p.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&p, bytes).unwrap();
    p.to_string_lossy().into_owned()
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(bytes).unwrap();
    e.finish().unwrap()
}

fn dir_str(dir: &Path) -> String {
    dir.to_string_lossy().into_owned()
}

async fn read_all(src: &FileSource) -> Result<Vec<Value>, String> {
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.extend(p.map_err(|e| e.to_string())?.records);
    }
    Ok(out)
}

/// A sink that keeps what it is handed.
#[derive(Default, Clone)]
struct Capture(Arc<Mutex<Vec<Value>>>);

#[async_trait::async_trait]
impl Sink for Capture {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, faucet_core::FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

#[tokio::test]
async fn a_directory_of_mixed_formats_reads_in_one_run() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    write(
        d,
        "a.jsonl",
        b"{\"src\":\"jsonl\",\"n\":1}\n\n{\"src\":\"jsonl\",\"n\":2}\n",
    );
    write(d, "b.json", br#"[{"src":"json","n":3}]"#);
    write(d, "c.csv.gz", &gzip(b"src,n\ncsv,4\n"));
    let xlsx = encode(
        &[json!({"src": "xlsx", "n": 5})],
        FileFormat::Xlsx,
        &FormatOptions::default(),
    )
    .unwrap();
    write(d, "d.xlsx", &xlsx);
    write(d, "notes.md", b"# not data");
    write(d, "e.txt", b"hello");

    let src = FileSource::new(FileSourceConfig::new(dir_str(d)).with_batch_size(2)).unwrap();
    let rows = read_all(&src).await.unwrap();
    let srcs: Vec<&str> = rows.iter().filter_map(|r| r["src"].as_str()).collect();
    assert_eq!(srcs, ["jsonl", "jsonl", "json", "csv", "xlsx"]);
    assert_eq!(rows[3]["n"], json!("4"), "CSV cells are text");
    assert_eq!(rows[5]["content"], json!("hello"));
    assert!(rows[5]["path"].as_str().unwrap().ends_with("e.txt"));
    assert_eq!(src.fetch_with_context(&HashMap::new()).await.unwrap(), rows);

    let mut strict = FileSourceConfig::new(dir_str(d));
    strict.strict = true;
    let err = read_all(&FileSource::new(strict).unwrap())
        .await
        .unwrap_err();
    assert!(err.contains("notes.md") && err.contains("strict"), "{err}");
}

#[tokio::test]
async fn globs_recursion_explicit_formats_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    write(d, "top.jsonl", b"{\"a\":1}\n");
    write(d, "nested/deep/x.jsonl", b"{\"a\":2}\n");
    write(d, "nested/y.data", b"{\"a\":3}\n");

    let flat = FileSource::new(FileSourceConfig::new(dir_str(d))).unwrap();
    assert_eq!(read_all(&flat).await.unwrap().len(), 1);
    let deep = FileSource::new(FileSourceConfig::new(dir_str(d)).recursive(true)).unwrap();
    assert_eq!(
        read_all(&deep).await.unwrap().len(),
        2,
        "the .data file is skipped under auto"
    );
    let explicit = FileSource::new(
        FileSourceConfig::new(format!("{}/**/*", dir_str(d))).format(FileSourceFormat::JsonLines),
    )
    .unwrap();
    assert_eq!(read_all(&explicit).await.unwrap().len(), 3);
    let mut capped = FileSourceConfig::new(dir_str(d)).recursive(true);
    capped.max_files = Some(1);
    assert_eq!(
        read_all(&FileSource::new(capped).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );

    let two = write(d, "two.jsonl", b"{\"a\":1}\n{\"a\":2}\n");
    let bad = FileSourceConfig::new(two).format(FileSourceFormat::JsonArray);
    let err = read_all(&FileSource::new(bad).unwrap()).await.unwrap_err();
    assert!(err.contains("two.jsonl"), "{err}");
    let broken = write(d, "broken.jsonl", b"{nope\n");
    let err = read_all(&FileSource::new(FileSourceConfig::new(broken)).unwrap())
        .await
        .unwrap_err();
    assert!(err.contains("line 1"), "{err}");
    let missing =
        FileSource::new(FileSourceConfig::new(format!("{}/missing", dir_str(d)))).unwrap();
    assert!(read_all(&missing).await.unwrap_err().contains("missing"));
    let raw = write(d, "bin.txt", &[0xff, 0xfe]);
    assert!(
        read_all(&FileSource::new(FileSourceConfig::new(raw)).unwrap())
            .await
            .unwrap_err()
            .contains("UTF-8")
    );
}

#[tokio::test]
async fn incremental_by_mtime_picks_up_only_a_new_file_through_the_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    write(d, "one.jsonl", b"{\"n\":1}\n");
    write(d, "two.jsonl", b"{\"n\":2}\n");
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let config = FileSourceConfig::new(dir_str(d)).incremental(IncrementalBy::Mtime);

    let run = |store: Arc<dyn StateStore>, config: FileSourceConfig| async move {
        let src = FileSource::new(config).unwrap();
        let sink = Capture::default();
        Pipeline::new(&src, &sink)
            .with_state_store(store)
            .run()
            .await
            .unwrap();
        sink.0.lock().unwrap().clone()
    };
    assert_eq!(
        run(store.clone(), config.clone()).await,
        vec![json!({"n": 1}), json!({"n": 2})]
    );
    assert!(
        run(store.clone(), config.clone()).await.is_empty(),
        "nothing new"
    );
    std::thread::sleep(std::time::Duration::from_millis(20));
    write(d, "three.jsonl", b"{\"n\":3}\n");
    assert_eq!(
        run(store.clone(), config.clone()).await,
        vec![json!({"n": 3})]
    );

    let key = FileSource::new(config.clone())
        .unwrap()
        .state_key()
        .unwrap();
    let stored =
        faucet_core::state_version::peel_versioned(&store.get(&key).await.unwrap().unwrap());
    assert_eq!(stored["by"], json!("mtime"));
    let wrong_mode = FileSource::new(config.incremental(IncrementalBy::Name)).unwrap();
    assert!(wrong_mode.apply_start_bookmark(stored).await.is_err());
}

#[tokio::test]
async fn incremental_by_name_resumes_after_the_last_file() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    write(d, "2026-01.jsonl", b"{\"m\":1}\n");
    write(d, "2026-02.jsonl", b"{\"m\":2}\n");
    let src = FileSource::new(FileSourceConfig::new(dir_str(d)).incremental(IncrementalBy::Name))
        .unwrap();
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut marks = Vec::new();
    while let Some(p) = pages.next().await {
        if let Some(b) = p.unwrap().bookmark {
            marks.push(b);
        }
    }
    drop(pages);
    assert_eq!(marks.len(), 2, "one bookmark per file");
    write(d, "2026-03.jsonl", b"{\"m\":3}\n");
    let next = FileSource::new(FileSourceConfig::new(dir_str(d)).incremental(IncrementalBy::Name))
        .unwrap();
    next.apply_start_bookmark(marks[1].clone()).await.unwrap();
    next.apply_start_bookmark(Value::Null).await.unwrap();
    assert_eq!(read_all(&next).await.unwrap(), vec![json!({"m": 3})]);
    let plain = FileSource::new(FileSourceConfig::new(dir_str(d))).unwrap();
    assert!(plain.state_key().is_none());
    plain
        .apply_start_bookmark(json!({"ignored": true}))
        .await
        .unwrap();
}

#[tokio::test]
async fn files_still_being_written_are_left_for_a_later_run() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "fresh.jsonl", b"{\"n\":1}\n");
    let mut cfg = FileSourceConfig::new(dir_str(dir.path()));
    cfg.stable_for_secs = Some(3600);
    assert!(
        read_all(&FileSource::new(cfg.clone()).unwrap())
            .await
            .unwrap()
            .is_empty()
    );
    cfg.stable_for_secs = Some(0);
    assert_eq!(
        read_all(&FileSource::new(cfg).unwrap())
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn avro_round_trip_preserves_logical_types() {
    let dir = tempfile::tempdir().unwrap();
    let schema = json!({"type": "record", "name": "r", "fields": [
        {"name": "id", "type": "long"},
        {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "at", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "uid", "type": {"type": "string", "logicalType": "uuid"}},
        {"name": "note", "type": ["null", "string"], "default": null}
    ]});
    let rows = vec![
        json!({"id": 1, "amount": "10.50", "day": "2024-01-31", "at": "2024-01-31T10:00:00.000001Z", "uid": "00000000-0000-4000-8000-000000000001", "note": null}),
        json!({"id": 2, "amount": "-0.01", "day": "1999-12-31", "at": "1999-12-31T23:59:59.999999Z", "uid": "00000000-0000-4000-8000-000000000002", "note": "x"}),
    ];
    let mut opts = FormatOptions::default();
    opts.avro.schema = Some(schema);
    opts.avro.codec = faucet_core::AvroCodec::Snappy;
    let path = write(
        dir.path(),
        "pay.avro",
        &encode(&rows, FileFormat::Avro, &opts).unwrap(),
    );
    write(
        dir.path(),
        "pay2.avro.gz",
        &gzip(&encode(&rows[..1], FileFormat::Avro, &opts).unwrap()),
    );

    let src = FileSource::new(FileSourceConfig::new(path).with_batch_size(1)).unwrap();
    assert_eq!(read_all(&src).await.unwrap(), rows);
    let both = FileSource::new(FileSourceConfig::new(dir_str(dir.path()))).unwrap();
    assert_eq!(
        read_all(&both).await.unwrap().len(),
        3,
        "a gzipped OCF decodes too"
    );
}

const ORC: &[u8] = include_bytes!("../../../core/tests/fixtures/orc/people.orc");

#[tokio::test]
async fn orc_from_a_reference_writer_is_read_with_projection() {
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), "people.orc", ORC);
    let mut cfg = FileSourceConfig::new(path.clone()).with_batch_size(2);
    cfg.orc.columns = Some(vec!["name".into(), "amount".into()]);
    let rows = read_all(&FileSource::new(cfg).unwrap()).await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["name"], json!("ada"));
    assert!(rows[0].get("id").is_none(), "projected away");

    let mut cfg = FileSourceConfig::new(path).format(FileSourceFormat::Orc);
    cfg.orc.columns = Some(vec!["id".into()]);
    let src = FileSource::new(cfg).unwrap();
    let ctx = HashMap::new();
    let mut batches = src.stream_batches(&ctx, 0);
    let mut n = 0;
    while let Some(b) = batches.next().await {
        let b = b.unwrap();
        assert_eq!(b.batch.num_columns(), 1);
        n += b.num_rows();
    }
    assert_eq!(n, 3);
}

#[tokio::test]
async fn parquet_files_stream_and_conflicting_schemas_name_both_files() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let dir = tempfile::tempdir().unwrap();
    let put = |name: &str, field: Field, col: arrow::array::ArrayRef| {
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = arrow::array::RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
        let path = dir.path().join(name);
        let mut w = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema,
            None,
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    };
    put(
        "a.parquet",
        Field::new("id", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![1, 2])),
    );
    put(
        "b.parquet",
        Field::new("id", DataType::Int64, false),
        Arc::new(Int64Array::from(vec![3])),
    );
    let src = FileSource::new(FileSourceConfig::new(dir_str(dir.path()))).unwrap();
    assert_eq!(
        read_all(&src).await.unwrap(),
        vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]
    );

    put(
        "c.parquet",
        Field::new("id", DataType::Utf8, false),
        Arc::new(StringArray::from(vec!["x"])),
    );
    let err = read_all(&src).await.unwrap_err();
    assert!(
        err.contains("c.parquet") && err.contains("a.parquet"),
        "{err}"
    );
    let columnar = FileSource::new(
        FileSourceConfig::new(dir_str(dir.path())).format(FileSourceFormat::Parquet),
    )
    .unwrap();
    let ctx = HashMap::new();
    let mut batches = columnar.stream_batches(&ctx, 0);
    let mut failed = false;
    while let Some(b) = batches.next().await {
        failed |= b.is_err();
    }
    assert!(failed);
    let row_only = FileSource::new(FileSourceConfig::new(dir_str(dir.path()))).unwrap();
    assert!(!row_only.supports_columnar());
    let mut batches = row_only.stream_batches(&ctx, 0);
    assert!(batches.next().await.unwrap().is_err());
}

#[tokio::test]
async fn columnar_incremental_pages_carry_bookmarks() {
    let dir = tempfile::tempdir().unwrap();
    let mut opts = FormatOptions::default();
    opts.avro.codec = faucet_core::AvroCodec::Null;
    write(
        dir.path(),
        "a.avro",
        &encode(&[json!({"i": 1})], FileFormat::Avro, &opts).unwrap(),
    );
    write(
        dir.path(),
        "b.avro",
        &encode(&[], FileFormat::Avro, &opts).unwrap(),
    );
    let src = FileSource::new(
        FileSourceConfig::new(dir_str(dir.path()))
            .format(FileSourceFormat::Avro)
            .incremental(IncrementalBy::Name),
    )
    .unwrap();
    let ctx = HashMap::new();
    let mut batches = src.stream_batches(&ctx, 0);
    let mut marks = 0;
    while let Some(b) = batches.next().await {
        if b.unwrap().bookmark.is_some() {
            marks += 1;
        }
    }
    assert_eq!(marks, 2, "an empty file still advances the bookmark");
}

#[tokio::test]
async fn shards_partition_files_and_discover_lists_them() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..8 {
        write(
            dir.path(),
            &format!("f{i}.jsonl"),
            format!("{{\"i\":{i}}}\n").as_bytes(),
        );
    }
    write(dir.path(), "skip.bin", b"");
    let cfg = FileSourceConfig::new(dir_str(dir.path()));
    let src = FileSource::new(cfg.clone()).unwrap();
    assert!(src.is_shardable());
    let shards = src.enumerate_shards(3).await.unwrap();
    let mut total = 0;
    for s in &shards {
        let part = FileSource::new(cfg.clone()).unwrap();
        part.apply_shard(s).await.unwrap();
        total += read_all(&part).await.unwrap().len();
    }
    assert_eq!(total, 8);
    let bad = faucet_core::shard::ShardSpec::new("x", json!({"nope": 1}));
    assert!(src.apply_shard(&bad).await.is_err());

    assert!(src.supports_discover());
    let found = src.discover().await.unwrap();
    assert_eq!(found.len(), 8, "the unrecognised file is not a dataset");
    assert_eq!(found[0].kind, "file");
    assert_eq!(found[0].config_patch["format"], json!("json_lines"));
    assert_eq!(found[0].name, "f0.jsonl");
    assert_eq!(src.connector_name(), "file");
    assert!(src.dataset_uri().starts_with("file://"));
    assert!(
        FileSource::new(FileSourceConfig::new("rel/x.csv"))
            .unwrap()
            .dataset_uri()
            .starts_with("file:///")
    );
    assert!(src.config_schema()["properties"].get("path").is_some());

    let ok = src.check(&CheckContext::default()).await.unwrap();
    assert_eq!(ok.failed_count(), 0);
    let empty = tempfile::tempdir().unwrap();
    let none = FileSource::new(FileSourceConfig::new(dir_str(empty.path()))).unwrap();
    assert_eq!(
        none.check(&CheckContext::default())
            .await
            .unwrap()
            .failed_count(),
        1
    );
    let gone = FileSource::new(FileSourceConfig::new("/definitely/not/here")).unwrap();
    assert_eq!(
        gone.check(&CheckContext::default())
            .await
            .unwrap()
            .failed_count(),
        1
    );
}

#[tokio::test]
async fn a_zero_batch_size_emits_one_page_per_file() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path(), "a.jsonl", b"{\"a\":1}\n{\"a\":2}\n");
    write(dir.path(), "b.json", br#"[{"a":3}]"#);
    write(dir.path(), "c.orc", ORC);
    let src =
        FileSource::new(FileSourceConfig::new(dir_str(dir.path())).with_batch_size(0)).unwrap();
    let ctx = HashMap::new();
    let sizes: Vec<usize> = src
        .stream_pages(&ctx, 0)
        .map(|p| p.unwrap().records.len())
        .collect()
        .await;
    assert_eq!(sizes, vec![2, 1, 3]);
}

#[tokio::test]
async fn a_whole_file_past_max_object_bytes_fails_instead_of_exhausting_memory() {
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<Value> = (0..2_000)
        .map(|i| json!({"padding": "x".repeat(50), "i": i}))
        .collect();
    let body = serde_json::to_vec(&rows).unwrap();
    let path = write(dir.path(), "bomb.json.gz", &gzip(&body));
    let mut cfg = FileSourceConfig::new(path.clone());
    cfg.max_object_bytes = 10_000;
    let err = read_all(&FileSource::new(cfg.clone()).unwrap())
        .await
        .unwrap_err();
    assert!(err.contains("max_object_bytes"), "{err}");
    assert!(err.contains("bomb.json.gz"), "{err}");
    cfg.max_object_bytes = body.len() as u64;
    assert_eq!(
        read_all(&FileSource::new(cfg.clone()).unwrap())
            .await
            .unwrap()
            .len(),
        2_000
    );
    cfg.max_object_bytes = 0;
    assert!(FileSource::new(cfg).is_err());
}

mod http {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn a_remote_gzipped_jsonl_file_streams_with_retries_and_headers() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/data/rows.jsonl.gz"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/data/rows.jsonl.gz"))
            .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/data/rows.jsonl.gz"))
            .and(header("authorization", "Bearer t"))
            .respond_with(
                ResponseTemplate::new(200).set_body_bytes(gzip(b"{\"r\":1}\n{\"r\":2}\n")),
            )
            .mount(&server)
            .await;
        let mut cfg = FileSourceConfig::new(format!("{}/data/rows.jsonl.gz?sig=abc", server.uri()));
        cfg.headers
            .insert("Authorization".into(), "Bearer t".into());
        let src = FileSource::new(cfg).unwrap();
        assert_eq!(
            read_all(&src).await.unwrap(),
            vec![json!({"r": 1}), json!({"r": 2})]
        );
        assert!(!src.is_shardable());
        assert_eq!(src.enumerate_shards(4).await.unwrap().len(), 1);
        assert!(src.dataset_uri().starts_with("http://"));
        let found = src.discover().await.unwrap();
        assert_eq!(found[0].name, "rows.jsonl.gz");
    }

    #[tokio::test]
    async fn remote_failures_are_typed_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/gone.csv"))
            .respond_with(ResponseTemplate::new(404).set_body_string("missing"))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/down.csv"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let gone =
            FileSource::new(FileSourceConfig::new(format!("{}/gone.csv", server.uri()))).unwrap();
        assert!(read_all(&gone).await.unwrap_err().contains("404"));
        let mut cfg = FileSourceConfig::new(format!("{}/down.csv", server.uri()));
        cfg.http_retries = 1;
        let down = FileSource::new(cfg).unwrap();
        let err = read_all(&down).await.unwrap_err();
        assert!(err.contains("2 attempt"), "{err}");
        let mut cfg = FileSourceConfig::new("http://127.0.0.1:1/x.csv");
        cfg.http_retries = 0;
        assert!(read_all(&FileSource::new(cfg).unwrap()).await.is_err());
    }

    #[tokio::test]
    async fn a_stalled_server_times_out_and_is_retried() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/slow.jsonl"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"s\":1}\n")
                    .set_delay(std::time::Duration::from_secs(30)),
            )
            .expect(2)
            .mount(&server)
            .await;
        let mut cfg = FileSourceConfig::new(format!("{}/slow.jsonl", server.uri()));
        cfg.http_retries = 1;
        cfg.http_read_timeout_secs = 1;
        let started = std::time::Instant::now();
        let err = read_all(&FileSource::new(cfg.clone()).unwrap())
            .await
            .unwrap_err();
        assert!(err.contains("2 attempt"), "{err}");
        assert!(started.elapsed() < std::time::Duration::from_secs(20));
        cfg.http_read_timeout_secs = 0;
        assert!(FileSource::new(cfg).is_err());
    }

    #[tokio::test]
    async fn last_modified_drives_incremental_and_check_uses_head() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/feed.json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("Last-Modified", "Tue, 01 Sep 2026 10:00:00 GMT"),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/feed.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(r#"[{"f":1}]"#))
            .mount(&server)
            .await;
        Mock::given(method("HEAD"))
            .and(path("/undated.json"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let url = format!("{}/feed.json", server.uri());
        let cfg = FileSourceConfig::new(url).incremental(IncrementalBy::Mtime);
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        for expected in [1usize, 0] {
            let src = FileSource::new(cfg.clone()).unwrap();
            let sink = Capture::default();
            Pipeline::new(&src, &sink)
                .with_state_store(store.clone())
                .run()
                .await
                .unwrap();
            assert_eq!(sink.0.lock().unwrap().len(), expected);
        }
        let src = FileSource::new(cfg).unwrap();
        assert_eq!(
            src.check(&CheckContext::default())
                .await
                .unwrap()
                .failed_count(),
            0
        );

        let undated = FileSource::new(
            FileSourceConfig::new(format!("{}/undated.json", server.uri()))
                .incremental(IncrementalBy::Mtime),
        )
        .unwrap();
        assert!(
            read_all(&undated)
                .await
                .unwrap_err()
                .contains("Last-Modified")
        );
        let missing = FileSource::new(FileSourceConfig::new(format!(
            "{}/nothing.json",
            server.uri()
        )))
        .unwrap();
        assert_eq!(
            missing
                .check(&CheckContext::default())
                .await
                .unwrap()
                .failed_count(),
            1
        );
    }

    #[tokio::test]
    async fn round_trips_are_counted_on_the_recorder() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/a.jsonl"))
            .respond_with(ResponseTemplate::new(200).set_body_string("{\"a\":1}\n"))
            .mount(&server)
            .await;
        let src =
            FileSource::new(FileSourceConfig::new(format!("{}/a.jsonl", server.uri()))).unwrap();
        let sink = Capture::default();
        Pipeline::new(&src, &sink).run().await.unwrap();
        assert_eq!(sink.0.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn read_records_reads_a_path_back_with_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let p = write(dir.path(), "x.csv", b"a\n1\n");
    assert_eq!(
        faucet_source_file::read_records(p).await.unwrap(),
        vec![json!({"a": "1"})]
    );
    assert!(faucet_source_file::read_records(" ").await.is_err());
}

#[tokio::test]
async fn compressed_and_corrupt_parquet_and_unreadable_files() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    let dir = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = arrow::array::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![7]))],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut w = parquet::arrow::ArrowWriter::try_new(&mut buf, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
    let gz = write(dir.path(), "a.parquet.gz", &gzip(&buf));
    assert_eq!(
        read_all(&FileSource::new(FileSourceConfig::new(gz)).unwrap())
            .await
            .unwrap(),
        vec![json!({"id": 7})]
    );
    let bad = write(dir.path(), "bad.parquet", b"not parquet at all");
    let err = read_all(&FileSource::new(FileSourceConfig::new(bad)).unwrap())
        .await
        .unwrap_err();
    assert!(err.contains("bad.parquet"), "{err}");

    let locked = write(dir.path(), "locked.jsonl", b"{}\n");
    std::fs::set_permissions(&locked, std::os::unix::fs::PermissionsExt::from_mode(0o000)).unwrap();
    let res = read_all(&FileSource::new(FileSourceConfig::new(locked.clone())).unwrap()).await;
    std::fs::set_permissions(&locked, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
    assert!(res.unwrap_err().contains("locked.jsonl"));

    let single = FileSource::new(FileSourceConfig::new(gz_path(&dir))).unwrap();
    let found = single.discover().await.unwrap();
    assert_eq!(found.len(), 1);
    assert!(
        found[0].name.ends_with("a.parquet.gz"),
        "a single-file path names the dataset by its path"
    );
}

fn gz_path(dir: &tempfile::TempDir) -> String {
    dir.path()
        .join("a.parquet.gz")
        .to_string_lossy()
        .into_owned()
}

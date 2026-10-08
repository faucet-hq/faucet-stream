use super::*;
use crate::write::LocalBackend;
use crate::write::remote::tests::{Mem, MemParts};
use crate::write::{RemoteBackend, object_layout};
use serde_json::json;
use std::sync::atomic::Ordering::SeqCst;

fn local(dir: &Path, path: &str, s: WriteSettings) -> FileWriter {
    let full = dir.join(path);
    let (d, t) =
        NameTemplate::from_path(full.to_str().unwrap(), s.format, s.codec, s.rolls_over()).unwrap();
    let b = LocalBackend::new(&d, &t, true);
    FileWriter::new(s, t, Arc::new(b)).unwrap()
}

fn remote(mem: &Arc<Mem>, path: Option<&str>, s: WriteSettings, uploads: usize) -> FileWriter {
    let (base, t) =
        object_layout("pre/", path, ".jsonl", s.format, s.codec, s.rolls_over()).unwrap();
    let b = RemoteBackend::new(mem.clone(), base, &t, None)
        .unwrap()
        .with_upload_concurrency(uploads);
    FileWriter::new(s, t, Arc::new(b)).unwrap()
}

fn jsonl() -> WriteSettings {
    WriteSettings::new(FileFormat::JsonLines, Compression::None)
}

fn per_flush() -> WriteSettings {
    let mut s = jsonl();
    s.object_per_flush = true;
    s
}

fn rows(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({ "i": i })).collect()
}

#[test]
fn estimate_is_the_json_length_plus_a_newline() {
    assert_eq!(estimate(&json!({"a": 1})), 8);
}

#[test]
fn settings_report_encryption_line_formats_and_the_parquet_codec() {
    let s = jsonl();
    assert!(!s.encrypted());
    assert!(s.line_based());
    assert!(s.prune_stale);
    assert_eq!(s.parquet_codec(), ParquetCodec::Snappy);
    assert!(WriteSettings::new(FileFormat::RawText, Compression::None).line_based());
    assert!(!WriteSettings::new(FileFormat::Csv, Compression::None).line_based());
    #[cfg(feature = "encryption")]
    {
        let mut e = s.clone();
        e.encryption = Some(serde_json::from_value(json!({"key": "k"})).unwrap());
        assert!(e.encrypted());
    }
}

#[test]
fn orc_output_and_bad_combinations_are_refused() {
    let e = WriteSettings::new(FileFormat::Orc, Compression::None)
        .validate()
        .unwrap_err()
        .to_string();
    assert!(e.contains("`orc` is read-only"), "{e}");
    let mut s = jsonl();
    s.write_mode = FileWriteMode::Overwrite;
    s.if_exists = IfExists::Append;
    let e = s.validate().unwrap_err().to_string();
    assert!(e.contains("`if_exists` must be `replace`"), "{e}");
    let mut s = jsonl();
    s.max_bytes_per_file = Some(0);
    assert!(s.validate().is_err());
}

#[test]
fn config_text_keeps_a_config_message_and_renders_anything_else() {
    assert_eq!(config_text(FaucetError::Config("bad".into())), "bad");
    assert!(config_text(FaucetError::Sink("io".into())).contains("io"));
}

#[test]
fn a_missing_feature_is_named() {
    let e = missing_feature(FileFormat::Parquet, "file-format-parquet").to_string();
    assert!(
        e.contains("`parquet` needs the `file-format-parquet` build feature"),
        "{e}"
    );
    assert!(require_feature(FileFormat::JsonLines).is_ok());
}

#[tokio::test]
async fn a_failed_flush_poisons_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("sub");
    let w = local(
        &out,
        "doc.json",
        WriteSettings::new(FileFormat::JsonArray, Compression::None),
    );
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    std::fs::remove_dir_all(&out).unwrap();
    assert!(w.flush().await.is_err());
    let e = w
        .write_rows(&[json!({"a": 2})])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("cannot continue"), "{e}");
    let e = w.flush().await.unwrap_err().to_string();
    assert!(e.contains("cannot continue"), "{e}");
}

/// #783 C1: a rollover whose publish fails loses the records of earlier
/// calls held in that file, so the writer refuses everything after it.
#[tokio::test]
async fn a_failed_rollover_that_loses_earlier_records_poisons_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("sub");
    let mut s = jsonl();
    s.max_records_per_file = Some(3);
    let w = local(&out, "r.jsonl", s);
    w.write_rows(&rows(2)).await.unwrap();
    std::fs::remove_dir_all(&out).unwrap();
    assert!(w.write_rows(&rows(2)).await.is_err());
    std::fs::create_dir_all(&out).unwrap();
    let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
    assert!(
        e.contains("records already reported as written were lost"),
        "{e}"
    );
    assert!(w.flush().await.is_err());
    assert_eq!(std::fs::read_dir(&out).unwrap().count(), 0);
}

#[tokio::test]
async fn a_failed_rollover_of_only_this_calls_records_leaves_the_writer_usable() {
    let mem = Arc::new(Mem::default());
    let mut s = per_flush();
    s.max_records_per_file = Some(1);
    let w = remote(&mem, Some("o/"), s, 2);
    *mem.fail_key.lock().unwrap() = Some("pre/o/part-00001.jsonl".into());
    let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
    assert!(e.contains("refused"), "{e}");
    w.write_rows(&rows(1)).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(mem.keys(), ["pre/o/part-00002.jsonl"]);
}

#[tokio::test]
async fn a_failed_background_upload_of_earlier_records_poisons_the_writer() {
    let mem = Arc::new(Mem::default());
    let mut s = per_flush();
    s.max_records_per_file = Some(3);
    let w = remote(&mem, Some("o/"), s, 4);
    w.write_rows(&rows(2)).await.unwrap();
    *mem.fail_key.lock().unwrap() = Some("pre/o/part-00001.jsonl".into());
    assert!(w.write_rows(&rows(2)).await.is_err());
    *mem.fail_key.lock().unwrap() = None;
    let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
    assert!(e.contains("lost"), "{e}");
}

/// #783 H1: without `path` every name is unique to the run, so the end of
/// the run lists nothing.
#[tokio::test]
async fn a_run_without_a_path_never_lists_the_destination() {
    let mem = Arc::new(Mem::default());
    let mut s = per_flush();
    s.prune_stale = false;
    let w = remote(&mem, None, s, 1);
    w.write_rows(&rows(2)).await.unwrap();
    w.flush().await.unwrap();
    w.complete().await.unwrap();
    assert_eq!(mem.lists.load(SeqCst), 0);
    let keys = mem.keys();
    assert_eq!(keys.len(), 1);
    assert!(
        keys[0].starts_with("pre/") && keys[0].ends_with("-00001.jsonl"),
        "{keys:?}"
    );
}

#[tokio::test]
async fn replace_removes_parts_of_a_longer_earlier_run() {
    let mem = Arc::new(Mem::default());
    for n in 1..=3 {
        mem.objects
            .lock()
            .unwrap()
            .insert(format!("pre/o/part-0000{n}.jsonl"), b"{}\n".to_vec());
    }
    let mut s = per_flush();
    s.max_records_per_file = Some(1);
    let w = remote(&mem, Some("o/"), s, 1);
    w.write_rows(&rows(1)).await.unwrap();
    w.flush().await.unwrap();
    w.complete().await.unwrap();
    assert_eq!(mem.keys(), ["pre/o/part-00001.jsonl"]);
}

#[tokio::test]
async fn a_numbered_template_publishes_one_object_per_flush() {
    let mem = Arc::new(Mem::default());
    let w = remote(&mem, Some("out/part-{part}.jsonl"), per_flush(), 1);
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    assert!(mem.keys().is_empty(), "nothing before a flush");
    w.flush().await.unwrap();
    w.flush().await.unwrap();
    w.write_rows(&[json!({"a": 2})]).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(
        mem.keys(),
        ["pre/out/part-00001.jsonl", "pre/out/part-00002.jsonl"]
    );
    assert_eq!(mem.text("pre/out/part-00002.jsonl"), "{\"a\":2}\n");
}

#[tokio::test]
async fn a_single_object_is_extended_from_a_kept_copy() {
    let mem = Arc::new(Mem::default());
    let w = remote(&mem, Some("one.jsonl"), per_flush(), 1);
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    w.flush().await.unwrap();
    mem.objects
        .lock()
        .unwrap()
        .insert("pre/one.jsonl".into(), b"tampered\n".to_vec());
    w.write_rows(&[json!({"a": 2})]).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(
        mem.text("pre/one.jsonl"),
        "{\"a\":1}\n{\"a\":2}\n",
        "continued from the local copy, not a download"
    );
}

#[tokio::test]
async fn a_published_object_that_vanished_is_not_silently_restarted() {
    let mem = Arc::new(Mem::default());
    let mut s = per_flush();
    s.format = FileFormat::JsonLines;
    let (base, t) = object_layout("pre/", Some("one.jsonl"), "", s.format, s.codec, false).unwrap();
    let b = RemoteBackend::new(mem.clone(), base, &t, None)
        .unwrap()
        .with_multipart(Arc::new(MemParts {
            mem: mem.clone(),
            size: 4,
        }));
    let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
    w.write_rows(&rows(3)).await.unwrap();
    w.flush().await.unwrap();
    mem.objects.lock().unwrap().clear();
    let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
    assert!(e.contains("is gone"), "{e}");
}

/// #783 H3: a line format goes up in parts while it is written, so neither
/// memory nor scratch disk grows with the object.
#[tokio::test(flavor = "multi_thread")]
async fn json_lines_stream_to_a_store_that_takes_parts() {
    let mem = Arc::new(Mem::default());
    let s = per_flush();
    let (base, t) = object_layout("pre/", None, ".jsonl", s.format, s.codec, false).unwrap();
    let b = RemoteBackend::new(mem.clone(), base, &t, None)
        .unwrap()
        .with_upload_concurrency(4)
        .with_multipart(Arc::new(MemParts {
            mem: mem.clone(),
            size: 64,
        }));
    let scratch = b.scratch_dir().to_path_buf();
    let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
    for _ in 0..10 {
        w.write_rows(&rows(1000)).await.unwrap();
        let on_disk: u64 = std::fs::read_dir(&scratch)
            .unwrap()
            .flatten()
            .map(|e| e.metadata().unwrap().len())
            .sum();
        assert_eq!(on_disk, 0, "no scratch file for a streamed object");
    }
    assert!(
        mem.parts_put.load(SeqCst) > 10,
        "parts went up during the writes"
    );
    assert!(mem.keys().is_empty(), "nothing is visible before the flush");
    w.flush().await.unwrap();
    let keys = mem.keys();
    assert_eq!(keys.len(), 1);
    let text = mem.text(&keys[0]);
    assert_eq!(text.lines().count(), 10_000);
    assert_eq!(text.lines().next().unwrap(), "{\"i\":0}");
}

#[tokio::test]
async fn a_small_streamed_object_is_one_upload() {
    let mem = Arc::new(Mem::default());
    let s = per_flush();
    let (base, t) = object_layout(
        "pre/",
        Some("s.jsonl.gz"),
        "",
        s.format,
        Compression::Gzip,
        false,
    )
    .unwrap();
    let mut s = s;
    s.codec = Compression::Gzip;
    let b = RemoteBackend::new(mem.clone(), base, &t, None)
        .unwrap()
        .with_multipart(Arc::new(MemParts {
            mem: mem.clone(),
            size: 1 << 20,
        }));
    let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
    w.write_rows(&rows(2)).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(mem.parts_put.load(SeqCst), 0);
    let gz = mem.objects.lock().unwrap()["pre/s.jsonl.gz"].clone();
    let mut plain = String::new();
    std::io::Read::read_to_string(&mut flate2_reader(gz), &mut plain).unwrap();
    assert_eq!(plain, "{\"i\":0}\n{\"i\":1}\n");
}

fn flate2_reader(gz: Vec<u8>) -> impl std::io::Read {
    faucet_core::compression::wrap_sync_reader(std::io::Cursor::new(gz), Compression::Gzip)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failed_part_loses_the_object_and_nothing_is_published() {
    let mem = Arc::new(Mem::default());
    let s = per_flush();
    let (base, t) = object_layout("pre/", Some("p.jsonl"), "", s.format, s.codec, false).unwrap();
    let b = RemoteBackend::new(mem.clone(), base, &t, None)
        .unwrap()
        .with_multipart(Arc::new(MemParts {
            mem: mem.clone(),
            size: 8,
        }));
    let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
    *mem.fail_key.lock().unwrap() = Some("pre/p.jsonl#1".into());
    w.write_rows(&rows(1)).await.unwrap();
    let second = w.write_rows(&rows(2000)).await;
    let flushed = w.flush().await.unwrap_err().to_string();
    match second {
        Err(e) => assert!(e.to_string().contains("refused pre/p.jsonl#1"), "{e}"),
        Ok(_) => assert!(flushed.contains("refused pre/p.jsonl#1"), "{flushed}"),
    }
    let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
    assert!(e.contains("lost"), "the first call's record was lost: {e}");
    assert!(mem.keys().is_empty());
}

/// #783 H2: storage I/O is awaited, so a hung upload does not stop a
/// caller's timeout (or cancel) from taking effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hung_upload_can_be_abandoned() {
    let mem = Arc::new(Mem::default());
    mem.delay_ms.store(10_000, SeqCst);
    let w = remote(&mem, Some("h.jsonl"), per_flush(), 1);
    w.write_rows(&rows(1)).await.unwrap();
    let started = std::time::Instant::now();
    let r = tokio::time::timeout(std::time::Duration::from_millis(200), w.flush()).await;
    assert!(r.is_err(), "the flush is still waiting on the upload");
    assert!(started.elapsed() < std::time::Duration::from_secs(2));
}

#[tokio::test]
async fn object_per_write_closes_an_object_at_the_end_of_each_batch_write() {
    let mem = Arc::new(Mem::default());
    let mut s = per_flush();
    s.object_per_write = true;
    let w = remote(&mem, None, s, 1);
    w.write_rows(&rows(3)).await.unwrap();
    assert_eq!(mem.keys().len(), 1, "published at write end");
    w.write_rows(&rows(2)).await.unwrap();
    w.flush().await.unwrap();
    let o = mem.objects.lock().unwrap();
    let bodies: Vec<usize> = o
        .values()
        .map(|b| b.iter().filter(|c| **c == b'\n').count())
        .collect();
    assert_eq!(bodies, [3, 2]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_page_that_rolls_into_many_objects_uploads_them_concurrently() {
    let mem = Arc::new(Mem::default());
    mem.delay_ms.store(100, SeqCst);
    let mut s = per_flush();
    s.max_records_per_file = Some(1);
    let w = remote(&mem, Some("o/"), s, 5);
    let started = std::time::Instant::now();
    w.write_rows(&rows(20)).await.unwrap();
    w.flush().await.unwrap();
    assert_eq!(mem.keys().len(), 20);
    assert_eq!(mem.peak.load(SeqCst), 5);
    assert!(started.elapsed() < std::time::Duration::from_millis(1000));
}

fn overwrite_settings() -> WriteSettings {
    let mut s = per_flush();
    s.write_mode = FileWriteMode::Overwrite;
    s.max_records_per_file = Some(1);
    s
}

fn overwrite_remote(mem: &Arc<Mem>) -> FileWriter {
    remote(mem, Some("d/"), overwrite_settings(), 3)
}

#[tokio::test]
async fn overwrite_moves_the_swap_area_into_place_and_removes_stale_parts() {
    let mem = Arc::new(Mem::default());
    mem.objects
        .lock()
        .unwrap()
        .insert("pre/d/part-00009.jsonl".into(), b"old\n".to_vec());
    let w = overwrite_remote(&mem);
    assert!(!w.swap_area_exists().await.unwrap());
    w.begin_overwrite().await.unwrap();
    assert!(w.swap_area_exists().await.unwrap());
    w.write_rows(&rows(2)).await.unwrap();
    w.flush().await.unwrap();
    assert!(mem.keys().contains(&"pre/d/part-00009.jsonl".to_string()));
    w.commit_overwrite().await.unwrap();
    assert_eq!(
        mem.keys(),
        ["pre/d/part-00001.jsonl", "pre/d/part-00002.jsonl"]
    );
    assert!(!w.swap_area_exists().await.unwrap());
    let e = overwrite_remote(&mem).commit_overwrite().await.unwrap_err();
    assert!(e.to_string().contains("missing"), "{e}");
}

/// #783 C5: a move into place that stopped half-way is finished by the next
/// run instead of being thrown away with the swap area.
#[tokio::test]
async fn an_interrupted_move_into_place_is_finished_by_the_next_run() {
    let mem = Arc::new(Mem::default());
    for n in [1, 2, 3] {
        mem.objects.lock().unwrap().insert(
            format!("pre/d/part-0000{n}.jsonl"),
            b"{\"old\":1}\n".to_vec(),
        );
    }
    let w = overwrite_remote(&mem);
    w.begin_overwrite().await.unwrap();
    w.write_rows(&rows(2)).await.unwrap();
    w.flush().await.unwrap();
    let swap = format!("pre/d/{}/part-00002.jsonl", w.template().swap_dir_name());
    *mem.fail_rename_key.lock().unwrap() = Some(swap);
    assert!(w.commit_overwrite().await.is_err());
    *mem.fail_rename_key.lock().unwrap() = None;

    overwrite_remote(&mem).begin_overwrite().await.unwrap();
    let keys: Vec<String> = mem
        .keys()
        .into_iter()
        .filter(|k| !k.contains(".faucet-"))
        .collect();
    assert_eq!(keys, ["pre/d/part-00001.jsonl", "pre/d/part-00002.jsonl"]);
    assert_eq!(mem.text("pre/d/part-00002.jsonl"), "{\"i\":1}\n");
}

#[tokio::test]
async fn an_abort_after_the_move_started_finishes_it() {
    let mem = Arc::new(Mem::default());
    let w = overwrite_remote(&mem);
    w.begin_overwrite().await.unwrap();
    w.write_rows(&rows(2)).await.unwrap();
    w.flush().await.unwrap();
    let swap = format!("pre/d/{}/part-00002.jsonl", w.template().swap_dir_name());
    *mem.fail_rename_key.lock().unwrap() = Some(swap);
    assert!(w.commit_overwrite().await.is_err());
    *mem.fail_rename_key.lock().unwrap() = None;
    overwrite_remote(&mem).abort_overwrite().await.unwrap();
    assert_eq!(
        mem.keys(),
        ["pre/d/part-00001.jsonl", "pre/d/part-00002.jsonl"]
    );
    overwrite_remote(&mem).commit_overwrite().await.unwrap_err();
}

#[tokio::test]
async fn an_abort_discards_the_swap_area_and_what_was_in_flight() {
    let mem = Arc::new(Mem::default());
    mem.objects
        .lock()
        .unwrap()
        .insert("pre/d/part-00001.jsonl".into(), b"keep\n".to_vec());
    let w = overwrite_remote(&mem);
    w.begin_overwrite().await.unwrap();
    w.write_rows(&rows(4)).await.unwrap();
    w.abort_overwrite().await.unwrap();
    assert_eq!(mem.keys(), ["pre/d/part-00001.jsonl"]);
    assert_eq!(mem.text("pre/d/part-00001.jsonl"), "keep\n");
}

#[tokio::test]
async fn the_move_into_place_runs_concurrently() {
    let mem = Arc::new(Mem::default());
    let w = overwrite_remote(&mem);
    w.begin_overwrite().await.unwrap();
    w.write_rows(&rows(20)).await.unwrap();
    w.flush().await.unwrap();
    w.commit_overwrite().await.unwrap();
    assert_eq!(mem.renames.load(SeqCst), 20);
    assert_eq!(mem.keys().len(), 20);
}

#[tokio::test]
async fn overwrite_on_a_local_directory_leaves_no_swap_area() {
    let dir = tempfile::tempdir().unwrap();
    for n in 1..=3 {
        std::fs::write(dir.path().join(format!("o-0000{n}.jsonl")), "{\"old\":1}\n").unwrap();
    }
    let mut s = jsonl();
    s.write_mode = FileWriteMode::Overwrite;
    s.max_records_per_file = Some(1);
    let w = local(dir.path(), "o-{part}.jsonl", s);
    w.begin_overwrite().await.unwrap();
    w.write_rows(&rows(1)).await.unwrap();
    w.flush().await.unwrap();
    w.commit_overwrite().await.unwrap();
    let mut names: Vec<String> = std::fs::read_dir(dir.path())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    assert_eq!(names, ["o-00001.jsonl"]);
}

#[cfg(feature = "file-format-parquet")]
fn parquet_shape(bytes: Vec<u8>) -> (i64, usize) {
    use parquet::file::reader::FileReader;
    let r = parquet::file::reader::SerializedFileReader::new(bytes::Bytes::from(bytes)).unwrap();
    let m = r.metadata().file_metadata();
    (m.num_rows(), m.schema_descr().num_columns())
}

#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn a_compressed_parquet_file_is_continued_after_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let w = local(
        dir.path(),
        "out.parquet.gz",
        WriteSettings::new(FileFormat::Parquet, Compression::Gzip),
    );
    w.write_rows(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .unwrap();
    w.flush().await.unwrap();
    w.write_rows(&[json!({"a": 3})]).await.unwrap();
    w.flush().await.unwrap();
    let gz = std::fs::read(dir.path().join("out.parquet.gz")).unwrap();
    let mut plain = Vec::new();
    std::io::Read::read_to_end(&mut flate2_reader(gz), &mut plain).unwrap();
    assert_eq!(parquet_shape(plain), (3, 1));
}

#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn a_widened_parquet_schema_carries_the_open_file_and_leaves_no_scratch() {
    let dir = tempfile::tempdir().unwrap();
    let w = local(
        dir.path(),
        "w.parquet",
        WriteSettings::new(FileFormat::Parquet, Compression::None),
    );
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    w.write_rows(&[json!({"a": 2, "b": "x"})]).await.unwrap();
    w.flush().await.unwrap();
    let bytes = std::fs::read(dir.path().join("w.parquet")).unwrap();
    assert_eq!(parquet_shape(bytes), (2, 2));
    assert!(
        std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .all(|e| !crate::write::is_scratch_name(&e.file_name().to_string_lossy()))
    );
}

#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn a_parquet_type_change_is_refused_without_losing_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let w = local(
        dir.path(),
        "t.parquet",
        WriteSettings::new(FileFormat::Parquet, Compression::None),
    );
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    let e = w
        .write_rows(&[json!({"a": "x"})])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("changed type"), "{e}");
    w.write_rows(&[json!({"a": 2})]).await.unwrap();
    w.flush().await.unwrap();
    let bytes = std::fs::read(dir.path().join("t.parquet")).unwrap();
    assert_eq!(parquet_shape(bytes), (2, 1));
}

#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn a_rejected_parquet_page_leaves_nothing_to_publish() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = WriteSettings::new(FileFormat::Parquet, Compression::None);
    s.parquet.schema = Some(vec![super::super::options::ParquetField {
        name: "a".into(),
        data_type: super::super::options::ParquetType::Int64,
        nullable: true,
    }]);
    let w = local(dir.path(), "r.parquet", s);
    let e = w
        .write_rows(&[json!({"b": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("not in `parquet.schema`"), "{e}");
    w.flush().await.unwrap();
    assert!(!dir.path().join("r.parquet").exists());
}

#[cfg(feature = "file-format-parquet")]
fn batch() -> arrow::array::RecordBatch {
    faucet_core::columnar::values_to_record_batch(
        &[json!({"a": 1}), json!({"a": 2})],
        faucet_core::columnar::infer_arrow_schema(&[json!({"a": 1})]).unwrap(),
    )
    .unwrap()
}

#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn a_batch_for_a_row_format_is_written_as_rows() {
    let dir = tempfile::tempdir().unwrap();
    let w = local(dir.path(), "b.jsonl", jsonl());
    assert_eq!(w.write_batch(&batch()).await.unwrap(), 2);
    w.flush().await.unwrap();
    let text = std::fs::read_to_string(dir.path().join("b.jsonl")).unwrap();
    assert_eq!(text, "{\"a\":1}\n{\"a\":2}\n");
}

/// #783 L8: a slice of a large batch counts its share of the batch's
/// memory, not the whole buffer it points into.
#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn the_columnar_byte_cap_counts_each_rows_share() {
    let dir = tempfile::tempdir().unwrap();
    let many: Vec<Value> = (0..1000).map(|i| json!({ "a": i })).collect();
    let big = faucet_core::columnar::values_to_record_batch_inferred(&many).unwrap();
    let per_row = big.get_array_memory_size().div_ceil(1000);
    let mut s = WriteSettings::new(FileFormat::Parquet, Compression::None);
    s.max_bytes_per_file = Some(per_row * 400);
    let w = local(dir.path(), "c.parquet", s);
    w.write_batch(&big).await.unwrap();
    w.flush().await.unwrap();
    let files = std::fs::read_dir(dir.path()).unwrap().count();
    assert_eq!(files, 3, "400 + 400 + 200 rows");
}

#[cfg(feature = "file-format-parquet")]
#[tokio::test]
async fn a_poisoned_writer_refuses_batches() {
    let dir = tempfile::tempdir().unwrap();
    let out = dir.path().join("sub");
    let w = local(
        &out,
        "p.parquet",
        WriteSettings::new(FileFormat::Parquet, Compression::None),
    );
    w.write_batch(&batch()).await.unwrap();
    std::fs::remove_dir_all(&out).unwrap();
    assert!(w.flush().await.is_err());
    let e = w.write_batch(&batch()).await.unwrap_err().to_string();
    assert!(e.contains("cannot continue"), "{e}");
}

#[cfg(all(feature = "file-format-csv", feature = "encryption"))]
#[tokio::test]
async fn an_encrypted_csv_file_is_reopened_after_a_flush() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = WriteSettings::new(FileFormat::Csv, Compression::None);
    let spec: faucet_core::EncryptionSpec = serde_json::from_value(json!({"key": "k"})).unwrap();
    s.encryption = Some(spec.clone());
    let w = local(dir.path(), "e.csv", s);
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    w.flush().await.unwrap();
    w.write_rows(&[json!({"a": 2, "b": 3})]).await.unwrap();
    w.flush().await.unwrap();
    let sealed = std::fs::read(dir.path().join("e.csv")).unwrap();
    let plain = faucet_core::CompiledEncryption::compile(&spec)
        .unwrap()
        .decrypt(&sealed)
        .unwrap();
    let text = String::from_utf8(plain).unwrap();
    assert_eq!(text.lines().collect::<Vec<_>>(), ["a,b", "1,", "2,3"]);
}

#[cfg(all(feature = "file-format-csv", feature = "encryption"))]
#[tokio::test]
async fn an_encrypted_remote_csv_is_continued_from_its_kept_copy() {
    let mem = Arc::new(Mem::default());
    let mut s = WriteSettings::new(FileFormat::Csv, Compression::None);
    let spec: faucet_core::EncryptionSpec = serde_json::from_value(json!({"key": "k"})).unwrap();
    s.encryption = Some(spec.clone());
    s.object_per_flush = true;
    let w = remote(&mem, Some("e.csv"), s, 1);
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    w.flush().await.unwrap();
    w.write_rows(&[json!({"a": 2})]).await.unwrap();
    w.flush().await.unwrap();
    let sealed = mem.objects.lock().unwrap()["pre/e.csv"].clone();
    let plain = faucet_core::CompiledEncryption::compile(&spec)
        .unwrap()
        .decrypt(&sealed)
        .unwrap();
    assert_eq!(String::from_utf8(plain).unwrap(), "a\n1\n2\n");
}

#[cfg(feature = "file-format-csv")]
#[tokio::test]
async fn appending_to_an_unreadable_csv_file_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("x.csv"), b"a\n\xff\xfe\n").unwrap();
    let mut s = WriteSettings::new(FileFormat::Csv, Compression::None);
    s.if_exists = IfExists::Append;
    let w = local(dir.path(), "x.csv", s);
    let e = w
        .write_rows(&[json!({"a": 1})])
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("reading existing"), "{e}");
}

#[cfg(feature = "file-format-csv")]
#[tokio::test]
async fn a_csv_record_that_is_not_an_object_is_refused_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let w = local(
        dir.path(),
        "c.csv",
        WriteSettings::new(FileFormat::Csv, Compression::None),
    );
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    assert!(w.write_rows(&[json!(3)]).await.is_err());
    w.write_rows(&[json!({"a": 2})]).await.unwrap();
    w.flush().await.unwrap();
    let text = std::fs::read_to_string(dir.path().join("c.csv")).unwrap();
    assert_eq!(text, "a\n1\n2\n");
}

#[cfg(feature = "file-format-csv")]
#[tokio::test]
async fn a_corrupt_csv_body_fails_the_flush() {
    let dir = tempfile::tempdir().unwrap();
    let w = local(
        dir.path(),
        "c.csv",
        WriteSettings::new(FileFormat::Csv, Compression::None),
    );
    w.write_rows(&[json!({"a": 1})]).await.unwrap();
    w.write_rows(&[json!({"a": 2, "b": 3})]).await.unwrap();
    let body = dir.path().join("c.csv.faucet-tmp-body");
    std::fs::write(&body, [0xff; 64]).unwrap();
    let e = w.flush().await.unwrap_err().to_string();
    assert!(e.contains("file sink: writing"), "{e}");
}

#[cfg(feature = "encryption")]
mod sealing {
    use super::*;

    fn sealed(path: &str, codec: Compression) -> WriteSettings {
        let format = if path.contains(".csv") {
            FileFormat::Csv
        } else {
            FileFormat::JsonLines
        };
        let mut s = WriteSettings::new(format, codec);
        s.if_exists = IfExists::Append;
        s.encryption = Some(serde_json::from_value(json!({"key": "k"})).unwrap());
        s
    }

    fn plain(s: &WriteSettings) -> WriteSettings {
        let mut p = s.clone();
        p.encryption = None;
        p
    }

    /// #783 M14: appending under `encryption` to a file that is not sealed
    /// the same way (or plaintext to a sealed one) is refused.
    #[tokio::test]
    async fn appending_across_a_sealing_mismatch_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        for (name, codec) in [
            ("l.jsonl", Compression::None),
            ("g.jsonl.gz", Compression::Gzip),
        ] {
            let s = sealed(name, codec);
            let w = local(dir.path(), name, plain(&s));
            w.write_rows(&rows(1)).await.unwrap();
            w.flush().await.unwrap();
            let w = local(dir.path(), name, s.clone());
            let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
            assert!(
                e.contains("is not encrypted but `encryption` is set"),
                "{name}: {e}"
            );

            let other = format!("s-{name}");
            let w = local(dir.path(), &other, s.clone());
            w.write_rows(&rows(1)).await.unwrap();
            w.flush().await.unwrap();
            let w = local(dir.path(), &other, plain(&s));
            let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
            assert!(
                e.contains("is encrypted but `encryption` is not set"),
                "{name}: {e}"
            );
            let w = local(dir.path(), &other, s.clone());
            w.write_rows(&rows(1)).await.unwrap();
            w.flush().await.unwrap();
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn plaintext_scratch_of_an_encrypted_output_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let mut s = sealed("x.jsonl.gz", Compression::Gzip);
        s.if_exists = IfExists::Replace;
        let w = local(dir.path(), "x.jsonl.gz", s);
        w.write_rows(&rows(1)).await.unwrap();
        let tmp = dir.path().join("x.jsonl.gz.faucet-tmp");
        let mode = std::fs::metadata(&tmp).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
        w.flush().await.unwrap();
        let mode = std::fs::metadata(dir.path().join("x.jsonl.gz"))
            .unwrap()
            .permissions()
            .mode();
        assert_ne!(
            mode & 0o044,
            0,
            "the sealed output keeps default permissions"
        );
    }
}

#[tokio::test]
async fn error_if_exists_and_append_numbering() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("e.jsonl"), "").unwrap();
    let mut s = jsonl();
    s.if_exists = IfExists::Error;
    let w = local(dir.path(), "e.jsonl", s);
    let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
    assert!(e.contains("`if_exists` is `error`"), "{e}");
    std::fs::write(dir.path().join("p-00004.jsonl"), "{}\n").unwrap();
    let mut s = jsonl();
    s.if_exists = IfExists::Append;
    s.max_records_per_file = Some(10);
    let w = local(dir.path(), "p-{part}.jsonl", s);
    w.write_rows(&rows(1)).await.unwrap();
    w.flush().await.unwrap();
    assert!(dir.path().join("p-00005.jsonl").exists());
    assert_eq!(w.local_outputs().len(), 1);
    w.discard().await;
}

/// #789 FILE-06: a store whose uploads go through a temporary name has the
/// stale ones of this output removed when a run starts; others are kept.
#[tokio::test]
async fn a_run_removes_its_own_stale_upload_scratch() {
    let mem = Arc::new(Mem::default());
    mem.upload_scratch.store(true, SeqCst);
    for k in [
        "pre/o/part-00001.jsonl.faucet-tmp-upload-dead",
        "pre/o/other.csv.faucet-tmp-upload-live",
    ] {
        mem.objects.lock().unwrap().insert(k.into(), b"x".to_vec());
    }
    let mut s = per_flush();
    s.max_records_per_file = Some(1);
    let w = remote(&mem, Some("o/"), s, 1);
    w.write_rows(&rows(1)).await.unwrap();
    w.flush().await.unwrap();
    w.complete().await.unwrap();
    assert_eq!(
        mem.keys(),
        [
            "pre/o/other.csv.faucet-tmp-upload-live",
            "pre/o/part-00001.jsonl"
        ]
    );
}

#[cfg(feature = "encryption")]
mod sealed_line_integrity {
    use super::*;
    use crate::sealed_lines::{Opened, open, plaintext};

    fn enc() -> faucet_core::CompiledEncryption {
        faucet_core::CompiledEncryption::compile(
            &serde_json::from_value(json!({"key": "k"})).unwrap(),
        )
        .unwrap()
    }

    fn sealed_jsonl() -> WriteSettings {
        let mut s = jsonl();
        s.if_exists = IfExists::Append;
        s.encryption = Some(serde_json::from_value(json!({"key": "k"})).unwrap());
        s
    }

    #[tokio::test]
    async fn a_file_continued_across_flushes_and_runs_verifies_whole() {
        let dir = tempfile::tempdir().unwrap();
        let w = local(dir.path(), "e.jsonl", sealed_jsonl());
        w.write_rows(&rows(2)).await.unwrap();
        w.flush().await.unwrap();
        w.write_rows(&rows(1)).await.unwrap();
        w.flush().await.unwrap();
        let again = local(dir.path(), "e.jsonl", sealed_jsonl());
        again.write_rows(&rows(1)).await.unwrap();
        again.flush().await.unwrap();
        let raw = std::fs::read(dir.path().join("e.jsonl")).unwrap();
        assert!(matches!(open(&raw, &enc()).unwrap(), Opened::Sealed { .. }));
        let text = String::from_utf8(plaintext(&raw, &enc()).unwrap()).unwrap();
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            ["{\"i\":0}", "{\"i\":1}", "{\"i\":0}", "{\"i\":0}"]
        );
        let cut: Vec<&[u8]> = raw.split_inclusive(|b| *b == b'\n').collect();
        assert!(open(&cut[..cut.len() - 1].concat(), &enc()).is_err());
    }

    #[tokio::test]
    async fn a_streamed_remote_file_carries_header_and_trailer() {
        let mem = Arc::new(Mem::default());
        let mut s = sealed_jsonl();
        s.if_exists = IfExists::Replace;
        let w = remote(&mem, Some("e.jsonl"), s, 1);
        w.write_rows(&rows(3)).await.unwrap();
        w.flush().await.unwrap();
        let raw = mem.objects.lock().unwrap()["pre/e.jsonl"].clone();
        assert_eq!(
            String::from_utf8(plaintext(&raw, &enc()).unwrap())
                .unwrap()
                .lines()
                .count(),
            3
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_file_streamed_in_parts_carries_header_and_trailer() {
        let mem = Arc::new(Mem::default());
        let mut s = per_flush();
        s.encryption = Some(serde_json::from_value(json!({"key": "k"})).unwrap());
        let (base, t) = object_layout("pre/", None, ".jsonl", s.format, s.codec, false).unwrap();
        let b = RemoteBackend::new(mem.clone(), base, &t, None)
            .unwrap()
            .with_multipart(Arc::new(MemParts {
                mem: mem.clone(),
                size: 64,
            }));
        let w = FileWriter::new(s, t, Arc::new(b)).unwrap();
        w.write_rows(&rows(50)).await.unwrap();
        w.flush().await.unwrap();
        let keys = mem.keys();
        let raw = mem.objects.lock().unwrap()[&keys[0]].clone();
        assert!(matches!(open(&raw, &enc()).unwrap(), Opened::Sealed { .. }));
        assert_eq!(
            String::from_utf8(plaintext(&raw, &enc()).unwrap())
                .unwrap()
                .lines()
                .count(),
            50
        );
    }

    #[tokio::test]
    async fn a_file_from_before_whole_file_integrity_continues_line_by_line() {
        use base64::Engine as _;
        let dir = tempfile::tempdir().unwrap();
        let line = |r: &str| {
            format!(
                "{}\n",
                base64::engine::general_purpose::STANDARD.encode(enc().encrypt(r.as_bytes()))
            )
        };
        std::fs::write(dir.path().join("old.jsonl"), line("{\"i\":9}")).unwrap();
        let w = local(dir.path(), "old.jsonl", sealed_jsonl());
        w.write_rows(&rows(1)).await.unwrap();
        w.flush().await.unwrap();
        let raw = std::fs::read(dir.path().join("old.jsonl")).unwrap();
        assert!(matches!(open(&raw, &enc()).unwrap(), Opened::Legacy { .. }));
        assert_eq!(plaintext(&raw, &enc()).unwrap(), b"{\"i\":9}\n{\"i\":0}\n");

        std::fs::write(dir.path().join("empty.jsonl"), "\n").unwrap();
        let w = local(dir.path(), "empty.jsonl", sealed_jsonl());
        w.write_rows(&rows(1)).await.unwrap();
        w.flush().await.unwrap();
        let raw = std::fs::read(dir.path().join("empty.jsonl")).unwrap();
        assert!(matches!(open(&raw, &enc()).unwrap(), Opened::Sealed { .. }));

        let w = local(dir.path(), "t.jsonl", sealed_jsonl());
        w.write_rows(&rows(2)).await.unwrap();
        w.flush().await.unwrap();
        let mut lines: Vec<Vec<u8>> = std::fs::read(dir.path().join("t.jsonl"))
            .unwrap()
            .split_inclusive(|b| *b == b'\n')
            .map(<[u8]>::to_vec)
            .collect();
        lines.swap(1, 2);
        std::fs::write(dir.path().join("t.jsonl"), lines.concat()).unwrap();
        let w = local(dir.path(), "t.jsonl", sealed_jsonl());
        let e = w.write_rows(&rows(1)).await.unwrap_err().to_string();
        assert!(e.contains("integrity"), "{e}");
    }
}

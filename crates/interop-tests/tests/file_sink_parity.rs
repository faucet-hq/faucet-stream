#![allow(deprecated)]
#![cfg(feature = "file-formats")]

//! Parity with the `csv`, `jsonl` and `parquet` sinks (#777): the same
//! records written through the old sink and through the file sink with the
//! equivalent options produce the same file.

use faucet_core::Sink;
use faucet_sink_file::FileSink;
use serde_json::{Value, json};
use std::path::Path;

fn sink(v: Value) -> FileSink {
    FileSink::new(serde_json::from_value(v).unwrap()).unwrap()
}

fn sink_err(v: Value) -> String {
    match serde_json::from_value(v) {
        Ok(cfg) => FileSink::new(cfg).err().expect("rejected").to_string(),
        Err(e) => e.to_string(),
    }
}

fn p(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

async fn pages(s: &dyn Sink, pages: &[Vec<Value>]) -> Result<(), String> {
    for page in pages {
        s.write_batch(page).await.map_err(|e| e.to_string())?;
    }
    s.flush().await.map_err(|e| e.to_string())
}

fn people() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "Smith; J", "tags": ["a"], "ok": true, "note": null}),
        json!({"id": 2, "name": "Doe", "tags": [], "ok": false, "note": "x\"y"}),
    ]
}

#[tokio::test]
async fn csv_output_matches_the_csv_sink() {
    let dir = tempfile::tempdir().unwrap();
    for (old_cfg, new_cfg) in [
        (
            faucet_sink_csv::CsvSinkConfig::new(p(dir.path(), "old1.csv")),
            json!({"path": p(dir.path(), "new1.csv")}),
        ),
        (
            faucet_sink_csv::CsvSinkConfig::new(p(dir.path(), "old2.csv"))
                .delimiter(b';')
                .write_headers(false),
            json!({"path": p(dir.path(), "new2.csv"), "csv": {"delimiter": ";", "write_headers": false}}),
        ),
    ] {
        let old_path = old_cfg.path.clone();
        pages(
            &faucet_sink_csv::CsvSink::new(old_cfg),
            &[people(), people()],
        )
        .await
        .unwrap();
        let new_path = new_cfg["path"].as_str().unwrap().to_string();
        pages(&sink(new_cfg), &[people(), people()]).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(&new_path).unwrap(),
            std::fs::read_to_string(&old_path).unwrap()
        );
    }
}

#[tokio::test]
async fn csv_on_unknown_field_matches_the_csv_sink() {
    use faucet_sink_csv::config::OnUnknownField;
    use faucet_sink_csv::{CsvSink, CsvSinkConfig};
    let dir = tempfile::tempdir().unwrap();
    let first = vec![json!({"a": 1, "b": 2})];
    let later = vec![json!({"a": 3, "b": 4, "c": 5})];

    let old = p(dir.path(), "old.csv");
    pages(
        &CsvSink::new(CsvSinkConfig::new(&old)),
        &[first.clone(), later.clone()],
    )
    .await
    .unwrap();
    let new = p(dir.path(), "new.csv");
    pages(
        &sink(json!({"path": new, "csv": {"on_unknown_field": "warn"}})),
        &[first.clone(), later.clone()],
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(&new).unwrap(),
        std::fs::read_to_string(&old).unwrap()
    );
    assert_eq!(std::fs::read_to_string(&new).unwrap(), "a,b\n1,2\n3,4\n");

    let old_err = pages(
        &CsvSink::new(
            CsvSinkConfig::new(p(dir.path(), "oe.csv")).on_unknown_field(OnUnknownField::Error),
        ),
        &[first.clone(), later.clone()],
    )
    .await
    .unwrap_err();
    let new_err = pages(
        &sink(json!({"path": p(dir.path(), "ne.csv"), "csv": {"on_unknown_field": "error"}})),
        &[first.clone(), later.clone()],
    )
    .await
    .unwrap_err();
    assert!(old_err.contains("[c]"), "{old_err}");
    assert!(
        new_err.contains("[c]") && new_err.contains("on_unknown_field"),
        "{new_err}"
    );

    let widened = p(dir.path(), "w.csv");
    pages(&sink(json!({"path": widened})), &[first, later])
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&widened).unwrap(),
        "a,b,c\n1,2,\n3,4,5\n"
    );
    let strict =
        faucet_source_file::FileSource::new(faucet_source_file::FileSourceConfig::new(&widened))
            .unwrap();
    use faucet_core::Source;
    assert_eq!(strict.fetch_all().await.unwrap().len(), 2);
}

#[tokio::test]
async fn csv_quote_applies_on_write() {
    let dir = tempfile::tempdir().unwrap();
    let out = p(dir.path(), "q.csv");
    pages(
        &sink(json!({"path": out, "csv": {"quote": "'"}})),
        &[vec![json!({"a": "x,y", "b": "it's"})]],
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(&out).unwrap(),
        "a,b\n'x,y','it''s'\n"
    );
    let back = faucet_source_file::FileSource::new({
        let mut c = faucet_source_file::FileSourceConfig::new(&out);
        c.csv.quote = "'".into();
        c
    })
    .unwrap();
    use faucet_core::Source;
    assert_eq!(
        back.fetch_all().await.unwrap(),
        vec![json!({"a": "x,y", "b": "it's"})]
    );
    assert!(sink_err(json!({"path": out, "csv": {"quote": "ab"}})).contains("csv.quote"));
}

#[tokio::test]
async fn json_lines_output_matches_the_jsonl_sink() {
    use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};
    let dir = tempfile::tempdir().unwrap();
    for pretty in [false, true] {
        let old = dir.path().join(format!("old-{pretty}.jsonl"));
        pages(
            &JsonlSink::new(JsonlSinkConfig::new(&old).pretty(pretty)),
            &[people(), people()],
        )
        .await
        .unwrap();
        let new = p(dir.path(), &format!("new-{pretty}.jsonl"));
        pages(
            &sink(json!({"path": new, "json_lines": {"pretty": pretty}})),
            &[people(), people()],
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(&new).unwrap(),
            std::fs::read(&old).unwrap(),
            "pretty={pretty}"
        );
    }
    let old = dir.path().join("old.jsonl.gz");
    pages(&JsonlSink::new(JsonlSinkConfig::new(&old)), &[people()])
        .await
        .unwrap();
    let new = p(dir.path(), "new.jsonl.gz");
    pages(&sink(json!({"path": new})), &[people()])
        .await
        .unwrap();
    let gunzip = |b: Vec<u8>| {
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut flate2::read::MultiGzDecoder::new(&b[..]), &mut out)
            .unwrap();
        out
    };
    assert_eq!(
        gunzip(std::fs::read(&new).unwrap()),
        gunzip(std::fs::read(&old).unwrap())
    );
}

fn read_parquet(path: &str) -> (Vec<Value>, parquet::file::metadata::ParquetMetaData) {
    let file = std::fs::File::open(path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let meta = builder.metadata().as_ref().clone();
    let mut out = Vec::new();
    for b in builder.build().unwrap() {
        out.extend(faucet_core::columnar::record_batch_to_values(&b.unwrap()).unwrap());
    }
    (out, meta)
}

fn metrics(n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| json!({"id": i, "name": format!("n{i}"), "score": i as f64 / 2.0}))
        .collect()
}

#[tokio::test]
async fn parquet_output_matches_the_parquet_sink() {
    use faucet_sink_parquet::{ParquetCompression, ParquetSink, ParquetSinkConfig};
    let dir = tempfile::tempdir().unwrap();
    let old = p(dir.path(), "old.parquet");
    let old_sink = ParquetSink::new(
        ParquetSinkConfig::local(&old)
            .row_group_size(3)
            .compression(ParquetCompression::Lz4),
    )
    .await
    .unwrap();
    pages(&old_sink, &[metrics(5), metrics(5)]).await.unwrap();
    drop(old_sink);
    let new = p(dir.path(), "new.parquet");
    pages(
        &sink(json!({"path": new, "parquet": {"row_group_size": 3, "compression": "lz4"}})),
        &[metrics(5), metrics(5)],
    )
    .await
    .unwrap();
    let (old_rows, old_meta) = read_parquet(&old);
    let (new_rows, new_meta) = read_parquet(&new);
    assert_eq!(new_rows, old_rows);
    assert_eq!(new_meta.num_row_groups(), old_meta.num_row_groups());
    assert_eq!(new_meta.num_row_groups(), 4);
    assert_eq!(
        new_meta.row_group(0).column(0).compression(),
        parquet::basic::Compression::LZ4_RAW
    );

    let none = p(dir.path(), "u.parquet");
    pages(
        &sink(json!({"path": none, "parquet": {"compression": "uncompressed"}})),
        &[metrics(2)],
    )
    .await
    .unwrap();
    assert_eq!(
        read_parquet(&none).1.row_group(0).column(0).compression(),
        parquet::basic::Compression::UNCOMPRESSED
    );
    assert!(
        sink_err(json!({"path": none, "parquet": {"row_group_size": 0}}))
            .contains("row_group_size")
    );
}

#[cfg(feature = "encryption")]
mod encryption {
    use super::*;
    use faucet_core::Source;
    use faucet_source_file::{FileSource, FileSourceConfig};

    fn spec(key: &str) -> Value {
        json!({"key": key})
    }

    async fn read_back(path: &str, key: &str) -> Result<Vec<Value>, String> {
        let mut cfg = FileSourceConfig::new(path);
        cfg.encryption = Some(serde_json::from_value(spec(key)).unwrap());
        FileSource::new(cfg)
            .map_err(|e| e.to_string())?
            .fetch_all()
            .await
            .map_err(|e| e.to_string())
    }

    #[tokio::test]
    async fn json_lines_are_sealed_per_line_like_the_jsonl_sink() {
        use base64::Engine as _;
        let dir = tempfile::tempdir().unwrap();
        let new = p(dir.path(), "new.jsonl");
        pages(
            &sink(json!({"path": new, "encryption": spec("k")})),
            &[people()],
        )
        .await
        .unwrap();
        let old = dir.path().join("old.jsonl");
        pages(
            &faucet_sink_jsonl::JsonlSink::new(
                faucet_sink_jsonl::JsonlSinkConfig::new(&old)
                    .encryption(serde_json::from_value(spec("k")).unwrap()),
            ),
            &[people()],
        )
        .await
        .unwrap();
        let enc =
            faucet_core::CompiledEncryption::compile(&serde_json::from_value(spec("k")).unwrap())
                .unwrap();
        let open = |path: &Path| -> Vec<Vec<u8>> {
            std::fs::read_to_string(path)
                .unwrap()
                .lines()
                .map(|l| {
                    enc.decrypt(&base64::engine::general_purpose::STANDARD.decode(l).unwrap())
                        .unwrap()
                })
                .collect()
        };
        assert_eq!(open(Path::new(&new)), open(&old));
        assert!(!std::fs::read_to_string(&new).unwrap().contains("Doe"));
        assert_eq!(read_back(&new, "k").await.unwrap(), people());

        let appended = sink(json!({"path": new, "mode": "append", "encryption": spec("k")}));
        pages(&appended, &[vec![json!({"id": 3})]]).await.unwrap();
        assert_eq!(read_back(&new, "k").await.unwrap().len(), 3);
        assert!(read_back(&new, "other").await.is_err());

        let gz = p(dir.path(), "z.jsonl.gz");
        pages(
            &sink(json!({"path": gz, "encryption": spec("k")})),
            &[people()],
        )
        .await
        .unwrap();
        assert!(faucet_core::encryption::is_encrypted(
            &std::fs::read(&gz).unwrap()
        ));
        pages(
            &sink(json!({"path": gz, "mode": "append", "encryption": spec("k")})),
            &[people()],
        )
        .await
        .unwrap();
        assert_eq!(read_back(&gz, "k").await.unwrap().len(), 4);
        assert!(
            !sink_err(json!({"path": p(dir.path(), "b.jsonl"), "encryption": spec(" ")}))
                .is_empty()
        );
    }

    #[tokio::test]
    async fn whole_file_formats_are_sealed_after_compression() {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "a.csv.gz",
            "b.json",
            "c.parquet",
            "d.avro",
            "e.xml",
            "f.xlsx",
            "g.txt",
        ] {
            let out = p(dir.path(), name);
            let recs = if name.ends_with(".txt") {
                vec![json!({"text": "line one"})]
            } else {
                vec![json!({"id": "1", "name": "x"})]
            };
            pages(
                &sink(json!({"path": out, "encryption": spec("k")})),
                std::slice::from_ref(&recs),
            )
            .await
            .unwrap();
            let raw = std::fs::read(&out).unwrap();
            if !name.ends_with(".txt") {
                assert!(faucet_core::encryption::is_encrypted(&raw), "{name}");
            }
            let back = read_back(&out, "k").await.unwrap();
            assert_eq!(back.len(), 1, "{name}");
        }
    }

    #[tokio::test]
    async fn a_sealed_file_is_decrypted_when_a_run_writes_to_it_again() {
        let dir = tempfile::tempdir().unwrap();
        let pq = p(dir.path(), "r.parquet");
        let s = sink(json!({"path": pq, "encryption": spec("k")}));
        pages(&s, &[vec![json!({"id": 1})]]).await.unwrap();
        pages(&s, &[vec![json!({"id": 2, "more": "m"})]])
            .await
            .unwrap();
        let back = read_back(&pq, "k").await.unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[1], json!({"id": 2, "more": "m"}));

        let csv = p(dir.path(), "r.csv");
        pages(
            &sink(json!({"path": csv, "encryption": spec("k")})),
            &[vec![json!({"a": "1"})]],
        )
        .await
        .unwrap();
        pages(
            &sink(json!({"path": csv, "mode": "append", "encryption": spec("k")})),
            &[vec![json!({"a": "2"})]],
        )
        .await
        .unwrap();
        assert_eq!(
            read_back(&csv, "k").await.unwrap(),
            vec![json!({"a": "1"}), json!({"a": "2"})]
        );
        let wrong = sink(json!({"path": csv, "mode": "append", "encryption": spec("other")}));
        assert!(pages(&wrong, &[vec![json!({"a": "3"})]]).await.is_err());
    }
}

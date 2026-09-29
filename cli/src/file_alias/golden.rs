#![allow(deprecated)]
//! Golden tests: a config run through the deprecated kind (built as `file`)
//! gives the same output as the old crate on the same input.

use crate::auth_catalog::AuthCatalog;
use crate::registry::{build_sink, build_source};
use faucet_core::{Sink, Source};
use serde_json::{Value, json};
use std::path::Path;

fn p(dir: &Path, name: &str) -> String {
    dir.join(name).to_string_lossy().into_owned()
}

async fn alias_source(kind: &str, cfg: Value) -> Box<dyn Source> {
    build_source(kind, cfg, &AuthCatalog::new(), None)
        .await
        .unwrap()
}

async fn alias_sink(kind: &str, cfg: Value) -> Box<dyn Sink> {
    build_sink(kind, cfg, &AuthCatalog::new()).await.unwrap()
}

async fn write(s: &dyn Sink, pages: &[Vec<Value>]) {
    for page in pages {
        s.write_batch(page).await.unwrap();
    }
    s.flush().await.unwrap();
}

fn people() -> Vec<Value> {
    vec![
        json!({"id": 1, "name": "Smith; J", "ok": true, "note": "a\"b"}),
        json!({"id": 2, "name": "Doe", "ok": false, "note": ""}),
    ]
}

#[cfg(feature = "source-csv")]
#[tokio::test]
async fn csv_source_reads_the_same_records() {
    let d = tempfile::tempdir().unwrap();
    let path = p(d.path(), "in.csv");
    std::fs::write(&path, "id;name;note\n1;\"Smith; J\";\n2;Doe;NULL\n").unwrap();
    let single = p(d.path(), "single.csv");
    std::fs::write(&single, "1;'q;x';z\n2;y;\n").unwrap();
    for cfg in [
        json!({"path": path, "delimiter": 59, "null_values": ["", "NULL"]}),
        json!({"path": single, "delimiter": 59, "quote": 39, "has_headers": false}),
    ] {
        let old: faucet_source_csv::CsvSourceConfig = serde_json::from_value(cfg.clone()).unwrap();
        let want = faucet_source_csv::CsvSource::new(old)
            .fetch_all()
            .await
            .unwrap();
        let src = alias_source("csv", cfg).await;
        assert_eq!(src.connector_name(), "csv");
        assert_eq!(src.fetch_all().await.unwrap(), want);
    }
}

#[cfg(feature = "source-csv")]
#[tokio::test]
async fn csv_source_ragged_rows_fail_as_before() {
    let d = tempfile::tempdir().unwrap();
    let path = p(d.path(), "in.csv");
    std::fs::write(&path, "a,b\n1,2\n3\n").unwrap();
    let src = alias_source("csv", json!({"path": path})).await;
    assert!(src.fetch_all().await.is_err());
    let flexible = alias_source("csv", json!({"path": path, "flexible": true})).await;
    assert_eq!(flexible.fetch_all().await.unwrap().len(), 2);
}

#[cfg(feature = "sink-csv")]
#[tokio::test]
async fn csv_sink_writes_the_same_bytes() {
    let d = tempfile::tempdir().unwrap();
    let later = vec![json!({"id": 3, "name": "Roe", "ok": true, "note": "n", "extra": 1})];
    for (i, extra) in [
        json!({}),
        json!({"delimiter": 59, "write_headers": false}),
        json!({"append": true}),
    ]
    .into_iter()
    .enumerate()
    {
        let mk = |name: &str| {
            let mut c = extra.clone();
            c["path"] = json!(p(d.path(), &format!("{i}-{name}.csv")));
            c
        };
        for _run in 0..2 {
            let old: faucet_sink_csv::CsvSinkConfig = serde_json::from_value(mk("old")).unwrap();
            write(
                &faucet_sink_csv::CsvSink::new(old),
                &[people(), later.clone()],
            )
            .await;
            let s = alias_sink("csv", mk("new")).await;
            assert_eq!(s.connector_name(), "csv");
            write(s.as_ref(), &[people(), later.clone()]).await;
        }
        let read = |name: &str| std::fs::read_to_string(p(d.path(), &format!("{i}-{name}.csv")));
        assert_eq!(read("new").unwrap(), read("old").unwrap(), "case {i}");
    }
}

#[cfg(feature = "sink-jsonl")]
#[tokio::test]
async fn jsonl_sink_writes_the_same_bytes() {
    let d = tempfile::tempdir().unwrap();
    for (i, extra) in [json!({}), json!({"pretty": true}), json!({"append": true})]
        .into_iter()
        .enumerate()
    {
        let mk = |name: &str| {
            let mut c = extra.clone();
            c["path"] = json!(p(d.path(), &format!("{i}-{name}.json")));
            c
        };
        for _run in 0..2 {
            let old: faucet_sink_jsonl::JsonlSinkConfig =
                serde_json::from_value(mk("old")).unwrap();
            write(
                &faucet_sink_jsonl::JsonlSink::new(old),
                &[people(), people()],
            )
            .await;
            let s = alias_sink("jsonl", mk("new")).await;
            assert_eq!(s.connector_name(), "jsonl");
            write(s.as_ref(), &[people(), people()]).await;
        }
        let read = |name: &str| std::fs::read(p(d.path(), &format!("{i}-{name}.json")));
        assert_eq!(read("new").unwrap(), read("old").unwrap(), "case {i}");
    }
}

#[cfg(all(feature = "sink-parquet", feature = "source-parquet"))]
async fn read_parquet(location: Value) -> Vec<Value> {
    let cfg: faucet_source_parquet::ParquetSourceConfig =
        serde_json::from_value(json!({ "source": location })).unwrap();
    let mut rows = faucet_source_parquet::ParquetSource::new(cfg)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    rows.sort_by_key(|r| r["id"].as_i64());
    rows
}

#[cfg(all(feature = "sink-parquet", feature = "source-parquet"))]
#[tokio::test]
async fn parquet_sink_and_source_match_the_old_crates() {
    let d = tempfile::tempdir().unwrap();
    let dest = |name: &str| json!({"type": "local_path", "path": p(d.path(), name)});
    let old: faucet_sink_parquet::ParquetSinkConfig =
        serde_json::from_value(json!({"destination": dest("old.parquet"), "compression": "zstd"}))
            .unwrap();
    {
        let s = faucet_sink_parquet::ParquetSink::new(old).await.unwrap();
        write(&s, &[people()]).await;
    }
    let s = alias_sink(
        "parquet",
        json!({"destination": dest("new.parquet"), "compression": "zstd"}),
    )
    .await;
    assert_eq!(s.connector_name(), "parquet");
    write(s.as_ref(), &[people()]).await;
    drop(s);
    let want = read_parquet(dest("old.parquet")).await;
    assert_eq!(want.len(), 2);
    assert_eq!(read_parquet(dest("new.parquet")).await, want);

    for cfg in [
        json!({"source": dest("old.parquet")}),
        json!({"source": dest("old.parquet"), "columns": ["id", "name"]}),
        json!({"source": {"type": "glob", "pattern": p(d.path(), "*.parquet")}}),
    ] {
        let old: faucet_source_parquet::ParquetSourceConfig =
            serde_json::from_value(cfg.clone()).unwrap();
        let want = faucet_source_parquet::ParquetSource::new(old)
            .await
            .unwrap()
            .fetch_all()
            .await
            .unwrap();
        let src = alias_source("parquet", cfg).await;
        assert_eq!(src.connector_name(), "parquet");
        assert_eq!(src.fetch_all().await.unwrap(), want);
    }
}

#[cfg(all(feature = "sink-parquet", feature = "source-parquet"))]
#[tokio::test]
async fn parquet_sink_directory_runs_add_files() {
    let d = tempfile::tempdir().unwrap();
    let dir = p(d.path(), "out");
    for _run in 0..2 {
        let s = alias_sink(
            "parquet",
            json!({"destination": {"type": "local_path", "path": dir}, "max_rows_per_file": 1}),
        )
        .await;
        write(s.as_ref(), &[people()]).await;
    }
    let files = std::fs::read_dir(&dir).unwrap().count();
    assert_eq!(files, 4);
    let rows = read_parquet(json!({"type": "glob", "pattern": format!("{dir}/*.parquet")})).await;
    assert_eq!(rows.len(), 4);
}

#[cfg(feature = "sink-parquet")]
#[tokio::test]
async fn parquet_sink_refuses_what_the_old_sink_refused() {
    let err = build_sink(
        "parquet",
        json!({"destination": {"type": "local_path", "path": "/tmp/x/"}, "schema": {"type": "explicit"}}),
        &AuthCatalog::new(),
    )
    .await
    .err()
    .unwrap();
    assert!(err.to_string().contains("explicit"), "{err}");
    let err = build_sink(
        "parquet",
        json!({"destination": {"type": "local_path", "path": ""}}),
        &AuthCatalog::new(),
    )
    .await
    .err()
    .unwrap();
    assert!(err.to_string().contains("destination.path"), "{err}");
    let err = build_sink(
        "parquet",
        json!({"destination": {"type": "local_path", "path": "o/"}, "bogus": 1}),
        &AuthCatalog::new(),
    )
    .await
    .err()
    .unwrap();
    assert!(err.to_string().contains("bogus"), "{err}");
}

#[cfg(all(feature = "source-csv", feature = "sink-csv"))]
#[tokio::test]
async fn csv_bytes_above_ascii_stay_on_the_old_crate() {
    let d = tempfile::tempdir().unwrap();
    let path = p(d.path(), "in.csv");
    std::fs::write(&path, "a\u{a7}b\n1\u{a7}2\n").unwrap();
    let src = alias_source("csv", json!({"path": path, "quote": 200})).await;
    assert_eq!(src.fetch_all().await.unwrap().len(), 1);
    let out = p(d.path(), "out.csv");
    let s = alias_sink("csv", json!({"path": out, "delimiter": 167})).await;
    write(s.as_ref(), &[people()]).await;
    assert!(std::fs::read(&out).unwrap().contains(&167));
}

#[test]
fn validate_checks_the_file_config_it_will_build() {
    use crate::registry::{validate_sink_config, validate_source_config};
    #[cfg(feature = "source-csv")]
    validate_source_config("csv", "row", json!({"path": "a.csv", "delimiter": 59})).unwrap();
    #[cfg(feature = "source-parquet")]
    {
        let s3 = json!({"source": {"type": "s3", "bucket": "b", "key": "k.parquet"}});
        validate_source_config("parquet", "row", s3).unwrap();
        let local = json!({"source": {"type": "local_path", "path": "a.parquet"}});
        validate_source_config("parquet", "row", local).unwrap();
    }
    #[cfg(feature = "sink-jsonl")]
    validate_sink_config("jsonl", "row", json!({"path": "o.jsonl", "append": true})).unwrap();
    #[cfg(feature = "sink-csv")]
    validate_sink_config("csv", "row", json!({"path": "o.csv"})).unwrap();
    #[cfg(feature = "sink-parquet")]
    {
        let explicit = json!({"destination": {"type": "local_path", "path": "o/"}, "schema": {"type": "explicit"}});
        let err = validate_sink_config("parquet", "row", explicit).unwrap_err();
        assert!(err.to_string().contains("explicit"), "{err}");
        let s3 = json!({"destination": {"type": "s3", "bucket": "b"}});
        validate_sink_config("parquet", "row", s3).unwrap();
    }
}

#[cfg(feature = "source-csv")]
#[test]
fn schemas_are_marked_deprecated() {
    let s = crate::registry::source_schema("csv").unwrap();
    assert_eq!(s["deprecated"], true);
    assert!(
        s["description"]
            .as_str()
            .unwrap()
            .starts_with("Deprecated alias")
    );
}

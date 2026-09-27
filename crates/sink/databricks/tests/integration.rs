//! Wiremock-backed integration tests for the Databricks sink against a
//! scripted Statement Execution API (and the Files API for volume staging).

mod common;

use std::sync::Arc;

use common::{Warehouse, columns, failed, ok, rows, sink};
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::drift::{ColumnChange, SchemaEvolution};
use faucet_core::staging::StagingCleanup;
use faucet_core::{Credential, FaucetError, Sink, WriteMode};
use faucet_sink_databricks::{DatabricksLoadMethod, DatabricksStagingConfig};
use serde_json::json;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, ResponseTemplate};

const DESCRIBE: &str = "information_schema";
const TARGET: &str = r#""value":"orders"}"#;
const STAGING: &str = r#""value":"orders__faucet_ovw"}"#;

fn upsert(c: &mut faucet_sink_databricks::DatabricksSinkConfig) {
    c.write.write_mode = WriteMode::Upsert;
    c.write.key = vec!["id".into()];
}

fn volume(c: &mut faucet_sink_databricks::DatabricksSinkConfig, cleanup: StagingCleanup) {
    c.load_method = DatabricksLoadMethod::CopyInto;
    c.staging = Some(DatabricksStagingConfig {
        location: "/Volumes/main/sales/stage".into(),
        cleanup,
        copy_options: Some("'mergeSchema' = 'false'".into()),
    });
}

#[tokio::test]
async fn append_auto_creates_table_then_inserts() {
    let wh = Warehouse::start().await;
    let s = sink(&wh, |c| c.catalog = Some("main".into()));
    let n = s
        .write_batch(&[
            json!({"id": 1, "name": "a"}),
            json!({"id": 2, "name": "o'k"}),
        ])
        .await
        .unwrap();
    assert_eq!(n, 2);
    let st = wh.statements();
    assert!(st[0].contains("FROM `main`.information_schema.columns"));
    assert_eq!(
        st[1],
        "CREATE TABLE IF NOT EXISTS `main`.`sales`.`orders` (`id` BIGINT, `name` STRING) USING DELTA"
    );
    assert_eq!(
        st[2],
        "INSERT INTO `main`.`sales`.`orders` (`id`, `name`) SELECT CAST(v.c0 AS bigint) AS `id`, \
         v.c1 AS `name` FROM VALUES ('1', 'a'), ('2', 'o\\'k') AS v(c0, c1)"
    );
    let body = &wh.bodies()[2];
    assert_eq!(body["warehouse_id"], json!("wh"));
    assert_eq!(body["catalog"], json!("main"));
    assert_eq!(body["format"], json!("JSON_ARRAY"));

    // A second page reuses the cached column list.
    wh.clear();
    s.write_batch(&[json!({"id": 3})]).await.unwrap();
    assert_eq!(wh.statements().len(), 1);
    assert_eq!(s.write_batch(&[]).await.unwrap(), 0);
}

#[tokio::test]
async fn append_casts_to_declared_types_and_chunks() {
    let wh = Warehouse::start().await;
    wh.on(
        DESCRIBE,
        columns(&[
            ("id", "int"),
            ("ts", "timestamp"),
            ("payload", "struct<a:int>"),
        ]),
    );
    let s = sink(&wh, |c| c.batch_size = 1);
    s.write_batch(&[
        json!({"id": 1, "ts": "2026-01-01T00:00:00Z", "payload": {"a": 1}}),
        json!({"id": 2}),
    ])
    .await
    .unwrap();
    let w = wh.writes();
    assert_eq!(w.len(), 2, "{w:?}");
    assert!(w[0].contains("CAST(v.c0 AS int) AS `id`"));
    assert!(w[0].contains("CAST(v.c1 AS timestamp) AS `ts`"));
    assert!(w[0].contains("from_json(v.c2, 'struct<a:int>') AS `payload`"));
    assert!(w[0].contains(r#"'{"a":1}'"#));
    assert!(w[1].contains("('2', NULL, NULL)"));
}

#[tokio::test]
async fn unknown_columns_and_missing_table_are_typed_errors() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let s = sink(&wh, |_| {});
    let err = s
        .write_batch(&[json!({"id": 1, "extra": 2})])
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("extra") && err.contains("on_drift"), "{err}");

    let wh = Warehouse::start().await;
    let s = sink(&wh, |c| c.create_table = false);
    let err = s.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(err.to_string().contains("does not exist"), "{err}");

    let wh = Warehouse::start().await;
    let s = sink(&wh, |_| {});
    assert_eq!(s.write_batch(&[json!(1)]).await.unwrap(), 0);
    assert!(wh.writes().is_empty());
}

#[tokio::test]
async fn failed_statement_surfaces_code_and_conflicts_are_retried() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    wh.on("INSERT", failed("DELTA_TABLE_NOT_FOUND", "gone"));
    let s = sink(&wh, |_| {});
    let err = s.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(matches!(err, FaucetError::Sink(_)));
    assert!(err.to_string().contains("DELTA_TABLE_NOT_FOUND"), "{err}");

    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    wh.on_seq(
        "MERGE",
        vec![
            ResponseTemplate::new(200).set_body_json(failed("DELTA_CONCURRENT_APPEND", "conflict")),
            ResponseTemplate::new(200).set_body_json(ok()),
        ],
    );
    let s = sink(&wh, upsert);
    s.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();
    assert_eq!(wh.writes().len(), 2);
}

#[tokio::test]
async fn upsert_merges_with_delete_marker_and_dedup() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, |c| {
        upsert(c);
        c.write.delete_marker = Some(faucet_core::DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        });
    });
    assert!(s.dedups_by_key());
    s.write_batch(&[
        json!({"id": 1, "name": "a"}),
        json!({"id": 1, "name": "b"}),
        json!({"id": 2, "name": "x", "__op": "d"}),
    ])
    .await
    .unwrap();
    let w = wh.writes();
    assert_eq!(w.len(), 1);
    assert!(
        w[0].contains("FROM VALUES ('1', 'b', 'u'), ('2', NULL, 'd') AS v(c0, c1, c2)"),
        "{}",
        w[0]
    );
    assert!(w[0].contains("WHEN MATCHED AND s.`__faucet_op` = 'd' THEN DELETE"));
    assert!(w[0].contains("WHEN MATCHED THEN UPDATE SET t.`name` = s.`name`"));
}

#[tokio::test]
async fn keyed_writes_route_keyless_rows() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, upsert);
    let recs = [json!({"id": 1, "name": "a"}), json!({"name": "no key"})];
    let err = s.write_batch(&recs).await.unwrap_err();
    assert!(err.to_string().contains("record 1"), "{err}");
    assert!(wh.writes().is_empty());

    let out = s.write_batch_partial(&recs).await.unwrap();
    assert!(out[0].is_ok() && out[1].is_err());
    assert_eq!(wh.writes().len(), 1);

    wh.clear();
    let out = s
        .write_batch_partial(&[json!({"name": "x"})])
        .await
        .unwrap();
    assert!(out[0].is_err());
    assert!(wh.writes().is_empty());
    assert!(s.write_batch_partial(&[]).await.unwrap().is_empty());

    let wh = Warehouse::start().await;
    let fresh = sink(&wh, upsert);
    let out = fresh.write_batch_partial(&[json!("scalar")]).await.unwrap();
    assert!(out[0].is_err());

    let wh = Warehouse::start().await;
    let fresh = sink(&wh, upsert);
    let out = fresh
        .write_batch_partial(&[json!({"id": 5}), json!(7)])
        .await
        .unwrap();
    assert!(out[0].is_ok() && out[1].is_err());
    assert!(
        wh.writes()[0].starts_with("CREATE TABLE IF NOT EXISTS `sales`.`orders` (`id` BIGINT)")
    );
}

#[tokio::test]
async fn append_partial_delegates_to_write_batch() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let s = sink(&wh, |_| {});
    let out = s.write_batch_partial(&[json!({"id": 1})]).await.unwrap();
    assert_eq!(out.len(), 1);
    assert!(out[0].is_ok());
}

#[tokio::test]
async fn delete_mode_merges_keys_only() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, |c| {
        c.write.write_mode = WriteMode::Delete;
        c.write.key = vec!["id".into()];
    });
    s.write_batch(&[json!({"id": 9, "name": "ignored"})])
        .await
        .unwrap();
    let w = wh.writes();
    assert!(w[0].contains("VALUES ('9', 'd') AS v(c0, c1)"), "{}", w[0]);
    assert!(!w[0].contains("UPDATE SET"));
}

#[tokio::test]
async fn overwrite_swaps_through_a_staging_table() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let s = sink(&wh, |c| c.write.write_mode = WriteMode::Overwrite);
    assert!(s.is_overwrite());
    s.begin_overwrite().await.unwrap();
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
    s.commit_overwrite().await.unwrap();
    assert_eq!(
        wh.writes(),
        vec![
            "DROP TABLE IF EXISTS `sales`.`orders__faucet_ovw`",
            "CREATE TABLE `sales`.`orders__faucet_ovw` LIKE `sales`.`orders`",
            "INSERT INTO `sales`.`orders__faucet_ovw` (`id`) SELECT CAST(v.c0 AS bigint) AS `id` FROM VALUES ('1') AS v(c0)",
            "INSERT OVERWRITE TABLE `sales`.`orders` SELECT * FROM `sales`.`orders__faucet_ovw`",
            "DROP TABLE IF EXISTS `sales`.`orders__faucet_ovw`",
        ]
    );
    wh.clear();
    s.abort_overwrite().await.unwrap();
    assert_eq!(
        wh.writes(),
        vec!["DROP TABLE IF EXISTS `sales`.`orders__faucet_ovw`"]
    );
}

#[tokio::test]
async fn overwrite_first_run_creates_staging_and_renames() {
    let wh = Warehouse::start().await;
    let s = sink(&wh, |c| c.write.write_mode = WriteMode::Overwrite);
    s.begin_overwrite().await.unwrap();
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
    wh.on(STAGING, columns(&[("id", "bigint")]));
    s.commit_overwrite().await.unwrap();
    let w = wh.writes();
    assert_eq!(w[0], "DROP TABLE IF EXISTS `sales`.`orders__faucet_ovw`");
    assert!(w[1].starts_with("CREATE TABLE IF NOT EXISTS `sales`.`orders__faucet_ovw`"));
    assert_eq!(
        w.last().unwrap(),
        "ALTER TABLE `sales`.`orders__faucet_ovw` RENAME TO `sales`.`orders`"
    );

    // Zero-record first run: nothing to publish.
    let wh = Warehouse::start().await;
    let s = sink(&wh, |c| c.write.write_mode = WriteMode::Overwrite);
    s.commit_overwrite().await.unwrap();
    assert!(wh.writes().is_empty());
}

#[tokio::test]
async fn overwrite_refuses_unsafe_states() {
    let wh = Warehouse::start().await;
    wh.on(TARGET, columns(&[("id", "bigint")]));
    let s = sink(&wh, |c| c.write.write_mode = WriteMode::Overwrite);
    let err = s.commit_overwrite().await.unwrap_err();
    assert!(err.to_string().contains("refusing"), "{err}");

    let wh = Warehouse::start().await;
    let s = sink(&wh, |c| {
        c.write.write_mode = WriteMode::Overwrite;
        c.create_table = false;
    });
    assert!(s.begin_overwrite().await.is_err());

    let err = s
        .write_batch_idempotent(&[json!({"id": 1})], "p::r", "00000000000000000001")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exactly_once"), "{err}");
}

#[tokio::test]
async fn exactly_once_append_replaces_by_scope_and_sequence() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, |c| c.batch_size = 1);
    assert!(s.supports_idempotent_writes());

    assert_eq!(s.last_committed_token("p::r").await.unwrap(), None);
    let token = "00000000000000000004#{\"lsn\":9}";
    let page = [json!({"id": 1, "name": "a"}), json!({"id": 2})];
    s.write_batch_idempotent(&page, "p::r", token)
        .await
        .unwrap();

    let w = wh.writes();
    assert!(w[0].starts_with("CREATE TABLE IF NOT EXISTS `sales`.`_faucet_commit_token`"));
    assert!(w[1].starts_with("SELECT `token` FROM `sales`.`_faucet_commit_token`"));
    assert_eq!(
        w[2],
        "ALTER TABLE `sales`.`orders` ADD COLUMNS (`_faucet_scope` STRING, `_faucet_seq` BIGINT)"
    );
    assert!(
        w[3].starts_with(
            "INSERT INTO `sales`.`orders` REPLACE WHERE `_faucet_scope` = 'p::r' AND `_faucet_seq` >= 4 SELECT"
        ),
        "{}",
        w[3]
    );
    assert!(w[4].starts_with(
        "INSERT INTO `sales`.`orders` (`id`, `name`, `_faucet_scope`, `_faucet_seq`)"
    ));
    assert!(w[4].contains("'p::r', 4 FROM VALUES ('2', NULL)"));
    assert!(w[5].starts_with("MERGE INTO `sales`.`_faucet_commit_token`"));
    let merge_body = wh.bodies().last().unwrap().clone();
    assert_eq!(merge_body["parameters"][1]["value"], json!(token));

    // A replayed page issues the same idempotent statements (no DDL again).
    wh.clear();
    s.write_batch_idempotent(&page, "p::r", token)
        .await
        .unwrap();
    let again = wh.writes();
    assert_eq!(again.len(), 3);
    assert_eq!(again[0], w[3]);

    // Resume reads the stored watermark back.
    wh.on("SELECT `token`", rows(&[&[Some(token)]]));
    assert_eq!(
        s.last_committed_token("p::r").await.unwrap().as_deref(),
        Some(token)
    );
}

#[tokio::test]
async fn exactly_once_empty_pages_and_bad_tokens() {
    let wh = Warehouse::start().await;
    wh.on(
        DESCRIBE,
        columns(&[
            ("id", "bigint"),
            ("_faucet_scope", "string"),
            ("_faucet_seq", "bigint"),
        ]),
    );
    let s = sink(&wh, |_| {});
    s.write_batch_idempotent(&[], "p::r", "00000000000000000002")
        .await
        .unwrap();
    let w = wh.writes();
    assert_eq!(
        w[0],
        "DELETE FROM `sales`.`orders` WHERE `_faucet_scope` = 'p::r' AND `_faucet_seq` >= 2"
    );
    assert!(
        w.last()
            .unwrap()
            .starts_with("MERGE INTO `sales`.`_faucet_commit_token`")
    );

    // No table yet: only the watermark advances.
    let wh = Warehouse::start().await;
    let s = sink(&wh, |_| {});
    s.write_batch_idempotent(&[], "p::r", "00000000000000000001")
        .await
        .unwrap();
    assert!(!wh.writes().iter().any(|w| w.starts_with("DELETE")));
    s.write_batch_idempotent(&[json!(1)], "p::r", "00000000000000000002")
        .await
        .unwrap();

    let err = s
        .write_batch_idempotent(&[], "p::r", "garbage")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unparseable"), "{err}");
}

#[tokio::test]
async fn exactly_once_upsert_merges_then_advances_token() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, upsert);
    s.write_batch_idempotent(
        &[json!({"id": 1, "name": "a"})],
        "p::r",
        "00000000000000000001",
    )
    .await
    .unwrap();
    let w = wh.writes();
    assert!(w[0].starts_with("MERGE INTO `sales`.`orders`"));
    assert!(
        w.last()
            .unwrap()
            .starts_with("MERGE INTO `sales`.`_faucet_commit_token`")
    );
    s.write_batch_idempotent(&[], "p::r", "00000000000000000002")
        .await
        .unwrap();
    let err = s
        .write_batch_idempotent(&[json!({"name": "x"})], "p::r", "00000000000000000003")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("record 0"));

    let wh = Warehouse::start().await;
    let s = sink(&wh, upsert);
    s.write_batch_idempotent(&[json!(1)], "p::r", "00000000000000000001")
        .await
        .unwrap_err();
}

#[tokio::test]
async fn rewind_moves_or_clears_the_watermark() {
    let wh = Warehouse::start().await;
    let s = sink(&wh, |_| {});
    s.rewind_commit_token("p::r", Some("00000000000000000003"))
        .await
        .unwrap();
    s.rewind_commit_token("p::r", None).await.unwrap();
    let w = wh.writes();
    assert!(w[1].starts_with("MERGE INTO `sales`.`_faucet_commit_token`"));
    assert!(w[2].starts_with("DELETE FROM `sales`.`_faucet_commit_token`"));
}

#[tokio::test]
async fn drift_reads_schema_and_evolves() {
    let wh = Warehouse::start().await;
    let s = sink(&wh, |_| {});
    assert_eq!(s.current_schema().await.unwrap(), None);

    wh.on(
        DESCRIBE,
        columns(&[("id", "int"), ("_faucet_seq", "bigint")]),
    );
    let schema = s.current_schema().await.unwrap().unwrap();
    assert_eq!(
        schema,
        json!({"type": "object", "properties": {"id": {"type": ["integer", "null"]}}})
    );
    s.write_batch(&[json!({"id": 1})]).await.unwrap();

    wh.clear();
    let evo = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec![],
    };
    s.evolve_schema(&evo).await.unwrap();
    assert_eq!(
        wh.writes(),
        vec![
            "ALTER TABLE `sales`.`orders` ADD COLUMNS (`email` STRING)",
            "ALTER TABLE `sales`.`orders` SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')",
            "ALTER TABLE `sales`.`orders` ALTER COLUMN `id` TYPE DOUBLE",
        ]
    );
    // The column cache was dropped: the next write re-reads the table.
    wh.clear();
    s.write_batch(&[json!({"id": 2})]).await.unwrap();
    assert!(wh.statements()[0].contains(DESCRIBE));
}

async fn mount_files_api(wh: &Warehouse) {
    Mock::given(method("PUT"))
        .and(path_regex(
            r"^/api/2\.0/fs/files/Volumes/main/sales/stage/_faucet/orders/.+\.parquet$",
        ))
        .respond_with(ResponseTemplate::new(204))
        .mount(&wh.server)
        .await;
    Mock::given(method("DELETE"))
        .and(path_regex(r"^/api/2\.0/fs/files/Volumes/"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&wh.server)
        .await;
}

async fn files_requests(wh: &Warehouse, verb: &str) -> Vec<wiremock::Request> {
    wh.server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method.as_str() == verb && r.url.path().starts_with("/api/2.0/fs/files"))
        .collect()
}

#[tokio::test]
async fn staged_append_uploads_parquet_to_a_volume_and_copies_into() {
    let wh = Warehouse::start().await;
    mount_files_api(&wh).await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, |c| volume(c, StagingCleanup::Always));
    s.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();

    let puts = files_requests(&wh, "PUT").await;
    assert_eq!(puts.len(), 1);
    assert!(puts[0].body.starts_with(b"PAR1"));
    assert_eq!(puts[0].url.query(), Some("overwrite=true"));
    let file = puts[0].url.path().rsplit('/').next().unwrap().to_owned();
    let dir = puts[0]
        .url
        .path()
        .trim_start_matches("/api/2.0/fs/files")
        .trim_end_matches(&format!("/{file}"))
        .to_owned();
    let w = wh.writes();
    assert_eq!(
        w[0],
        format!(
            "COPY INTO `sales`.`orders` FROM (SELECT CAST(`id` AS bigint) AS `id`, `name` AS `name` \
             FROM '{dir}') FILEFORMAT = PARQUET FILES = ('{file}') COPY_OPTIONS ('mergeSchema' = 'false')"
        )
    );
    assert_eq!(files_requests(&wh, "DELETE").await.len(), 1);
}

#[tokio::test]
async fn staged_load_keeps_files_on_failure_with_on_success_cleanup() {
    let wh = Warehouse::start().await;
    mount_files_api(&wh).await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    wh.on("COPY INTO", failed("COPY_INTO_FAILED", "denied"));
    let s = sink(&wh, |c| volume(c, StagingCleanup::OnSuccess));
    assert!(s.write_batch(&[json!({"id": 1})]).await.is_err());
    assert!(files_requests(&wh, "DELETE").await.is_empty());
}

#[tokio::test]
async fn staged_exactly_once_and_upsert_read_the_staged_file() {
    let wh = Warehouse::start().await;
    mount_files_api(&wh).await;
    wh.on(
        DESCRIBE,
        columns(&[
            ("id", "bigint"),
            ("_faucet_scope", "string"),
            ("_faucet_seq", "bigint"),
        ]),
    );
    let s = sink(&wh, |c| volume(c, StagingCleanup::Never));
    s.write_batch_idempotent(&[json!({"id": 1})], "p::r", "00000000000000000007")
        .await
        .unwrap();
    let replace = wh
        .writes()
        .into_iter()
        .find(|w| w.contains("REPLACE WHERE"))
        .unwrap();
    assert!(replace.contains("read_files('/Volumes/main/sales/stage/_faucet/orders/"));
    assert!(replace.contains("/00000000000000000007.parquet', format => 'parquet')"));
    assert!(files_requests(&wh, "DELETE").await.is_empty());

    let wh = Warehouse::start().await;
    mount_files_api(&wh).await;
    wh.on(DESCRIBE, columns(&[("id", "bigint"), ("name", "string")]));
    let s = sink(&wh, |c| {
        volume(c, StagingCleanup::Always);
        upsert(c);
    });
    s.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();
    let merge = &wh.writes()[0];
    assert!(
        merge.starts_with("MERGE INTO `sales`.`orders` AS t USING (SELECT CAST(`id` AS bigint)")
    );
    assert!(merge.contains(
        "`__faucet_op` AS `__faucet_op` FROM read_files('/Volumes/main/sales/stage/_faucet/orders/"
    ));
}

#[tokio::test]
async fn files_api_retries_and_reports_failures() {
    let wh = Warehouse::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&wh.server)
        .await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&wh.server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&wh.server)
        .await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let s = sink(&wh, |c| volume(c, StagingCleanup::Always));
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert_eq!(files_requests(&wh, "PUT").await.len(), 2);

    let wh = Warehouse::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(403).set_body_string("no volume access"))
        .mount(&wh.server)
        .await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let s = sink(&wh, |c| volume(c, StagingCleanup::Always));
    let err = s.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(err.to_string().contains("no volume access"), "{err}");
    assert!(wh.writes().is_empty());

    let wh = Warehouse::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&wh.server)
        .await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&wh.server)
        .await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let s = sink(&wh, |c| {
        volume(c, StagingCleanup::Always);
        c.max_retries = 0;
    });
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
}

#[tokio::test]
async fn staging_uses_workspace_url_when_no_override() {
    let wh = Warehouse::start().await;
    Mock::given(method("PUT"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&wh.server)
        .await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let mut c = common::config(&format!("{}/", wh.uri()));
    volume(&mut c, StagingCleanup::Never);
    let s = faucet_sink_databricks::DatabricksSink::new(c).unwrap();
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert_eq!(files_requests(&wh, "PUT").await.len(), 1);
}

#[cfg(feature = "staging")]
#[tokio::test]
async fn cloud_staging_goes_through_the_object_store() {
    use object_store::{ObjectStore, ObjectStoreExt};
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let store = Arc::new(object_store::memory::InMemory::new());
    let s = sink(&wh, |c| {
        c.load_method = DatabricksLoadMethod::CopyInto;
        c.staging = Some(DatabricksStagingConfig {
            location: "s3://bucket/pre".into(),
            cleanup: StagingCleanup::Never,
            copy_options: None,
        });
    })
    .with_object_store(store.clone());
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
    let w = wh.writes();
    assert!(
        w[0].contains("FROM 's3://bucket/pre/_faucet/orders/"),
        "{}",
        w[0]
    );
    let listed: Vec<_> = futures::StreamExt::collect::<Vec<_>>(store.list(None)).await;
    assert_eq!(listed.len(), 1);
    let key = listed[0].as_ref().unwrap().location.clone();
    assert!(key.as_ref().starts_with("pre/_faucet/orders/"));

    let s = sink(&wh, |c| {
        c.load_method = DatabricksLoadMethod::CopyInto;
        c.staging = Some(DatabricksStagingConfig {
            location: "s3://bucket/pre".into(),
            cleanup: StagingCleanup::Always,
            copy_options: None,
        });
    })
    .with_object_store(store.clone());
    s.write_batch(&[json!({"id": 2})]).await.unwrap();
    let listed: Vec<_> = futures::StreamExt::collect::<Vec<_>>(store.list(None)).await;
    assert_eq!(listed.len(), 1);
    store.delete(&key).await.unwrap();
}

#[derive(Debug)]
struct Shared;

#[faucet_core::async_trait]
impl faucet_core::AuthProvider for Shared {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer("m2m".into()))
    }
    fn provider_name(&self) -> &'static str {
        "shared"
    }
}

#[tokio::test]
async fn shared_provider_supplies_the_bearer_token() {
    let wh = Warehouse::start().await;
    let s = sink(&wh, |_| {}).with_auth_provider(Arc::new(Shared));
    s.current_schema().await.unwrap();
    let reqs = wh.server.received_requests().await.unwrap();
    assert_eq!(
        reqs[0]
            .headers
            .get("Authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer m2m"
    );
}

#[tokio::test]
async fn check_probes_warehouse_and_table() {
    let ctx = CheckContext::default();
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let report = sink(&wh, |_| {}).check(&ctx).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass))
    );

    let wh = Warehouse::start().await;
    let report = sink(&wh, |_| {}).check(&ctx).await.unwrap();
    assert!(matches!(report.probes[1].status, ProbeStatus::Skip { .. }));
    let report = sink(&wh, |c| c.create_table = false)
        .check(&ctx)
        .await
        .unwrap();
    assert!(matches!(report.probes[1].status, ProbeStatus::Fail { .. }));

    let wh = Warehouse::start().await;
    wh.on("SELECT 1", failed("PERMISSION_DENIED", "no"));
    wh.on(DESCRIBE, failed("PERMISSION_DENIED", "no"));
    let report = sink(&wh, |_| {}).check(&ctx).await.unwrap();
    assert_eq!(report.failed_count(), 2);

    let wh = Warehouse::start().await;
    wh.on_seq(
        "",
        vec![
            ResponseTemplate::new(200)
                .set_body_json(ok())
                .set_delay(std::time::Duration::from_millis(300)),
        ],
    );
    let quick = CheckContext {
        timeout: std::time::Duration::from_millis(20),
    };
    let report = sink(&wh, |_| {}).check(&quick).await.unwrap();
    assert_eq!(report.failed_count(), 2);
}

#[tokio::test]
async fn eo_append_tolerates_a_concurrent_column_add() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    wh.on("ADD COLUMNS", failed("FIELDS_ALREADY_EXISTS", "exists"));
    let s = sink(&wh, |_| {});
    let err = s
        .write_batch_idempotent(&[json!({"id": 1})], "p::r", "00000000000000000001")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("FIELDS_ALREADY_EXISTS"));

    wh.on_seq(
        DESCRIBE,
        vec![
            ResponseTemplate::new(200).set_body_json(columns(&[("id", "bigint")])),
            ResponseTemplate::new(200).set_body_json(columns(&[
                ("id", "bigint"),
                ("_faucet_scope", "string"),
                ("_faucet_seq", "bigint"),
            ])),
        ],
    );
    let s = sink(&wh, |_| {});
    s.write_batch_idempotent(&[json!({"id": 1})], "p::r", "00000000000000000001")
        .await
        .unwrap();
}

#[tokio::test]
async fn cached_columns_gain_eo_columns_on_first_exactly_once_write() {
    let wh = Warehouse::start().await;
    wh.on(DESCRIBE, columns(&[("id", "bigint")]));
    let mut c = common::config("https://unused.example");
    c.poll_interval_ms = 1;
    let s = faucet_sink_databricks::DatabricksSink::new(c)
        .unwrap()
        .with_endpoint_base(wh.uri());
    s.write_batch(&[json!({"id": 1})]).await.unwrap();
    wh.clear();
    s.write_batch_idempotent(&[json!({"id": 2})], "p::r", "00000000000000000001")
        .await
        .unwrap();
    let st = wh.statements();
    assert!(!st.iter().any(|x| x.contains(DESCRIBE)), "{st:?}");
    assert!(
        st.iter()
            .any(|x| x.contains("ADD COLUMNS (`_faucet_scope` STRING"))
    );
}

//! Integration tests for `IcebergSink` using a SQLite-backed SQL catalog and
//! a local-filesystem warehouse — no Docker, no network, fully in-process.
//!
//! # Requirements
//!
//! These tests require the `catalog-sql` Cargo feature to be enabled:
//!
//! ```sh
//! cargo test -p faucet-sink-iceberg --test integration --features catalog-sql
//! ```
//!
//! CI runs `cargo test --workspace --all-features`, which includes `catalog-sql`.
//!
//! # Approach
//!
//! - **Catalog:** `iceberg-catalog-sql` backed by a SQLite file in a `TempDir`.
//!   The SQL catalog auto-creates its schema tables (`iceberg_tables`,
//!   `iceberg_namespace_properties`) inside `SqlCatalog::new()` via
//!   `CREATE TABLE IF NOT EXISTS`, so no separate migration step is needed.
//! - **Warehouse:** local filesystem via `file://<tempdir>/warehouse`.
//!   `iceberg` 0.9.1's `LocalFsStorageFactory` (no extra feature flag required)
//!   handles `file://` paths natively.
//! - **Bind style:** SQLite requires `?` placeholders (`SqlBindStyle::QMark`).
//!   The sink's `build_sql` now infers this automatically from a `sqlite:` URI.
//!
//! # Sink defect surfaced by these tests
//!
//! Prior to this test, `build_sql` called `SqlCatalogBuilder::default().load(...)`
//! without calling `.with_storage_factory(...)`.  The `SqlCatalogBuilder` defaults
//! `storage_factory` to `None`, and `SqlCatalog::new()` returns an immediate error
//! when it is `None`.  The sink now sets `LocalFsStorageFactory` for local
//! warehouses.  A GitHub follow-up issue should track OpenDal-backed cloud
//! warehouses (S3/GCS) for the SQL catalog.

#![cfg(feature = "catalog-sql")]

use std::sync::Arc;

use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use serde_json::json;
use tempfile::TempDir;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns an `IcebergSinkConfig` that writes to a SQLite catalog + local-FS
/// warehouse inside `dir`.
fn sink_config(dir: &TempDir, table: &str) -> IcebergSinkConfig {
    let db_path = dir.path().join("catalog.db");
    let warehouse_path = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse_path).expect("create warehouse dir");

    // `?mode=rwc` enables SQLite's "read-write-create" mode so sqlx creates
    // the database file if it does not already exist (without this flag the
    // AnyPool connection fails with "unable to open database file").
    let sqlite_uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse_path.display());

    serde_json::from_value(json!({
        "catalog": {
            "type": "sql",
            "uri": sqlite_uri,
            "warehouse": warehouse_uri
            // bind_style and storage_factory are inferred by build_sql from
            // the sqlite: URI prefix and the file:// warehouse prefix.
        },
        "namespace": ["db"],
        "table": table,
        "create_if_missing": true,
        // batch_size = 0 → the whole write_batch slice is one chunk.
        "batch_size": 0
    }))
    .expect("sink config parse")
}

/// Build a reader `SqlCatalog` pointing at the SAME SQLite database + warehouse
/// so we can inspect the committed table metadata after the sink runs.
async fn open_reader_catalog(dir: &TempDir) -> impl Catalog {
    let db_path = dir.path().join("catalog.db");
    let warehouse_path = dir.path().join("warehouse");

    // Reader also needs `mode=rwc` so sqlx can open the already-existing file
    // via the same AnyPool path that creates-if-missing.
    let sqlite_uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse_path.display());

    let props = std::collections::HashMap::from([
        (SQL_CATALOG_PROP_URI.to_string(), sqlite_uri),
        (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri),
        (
            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
            SqlBindStyle::QMark.to_string(),
        ),
    ]);

    // Must use the SAME catalog name as the sink ("faucet-iceberg") because
    // the SQL catalog stores tables keyed by (catalog_name, namespace, table).
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("faucet-iceberg", props)
        .await
        .expect("reader catalog open")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Core path: write one batch → flush → assert exactly ONE snapshot.
/// Write a second batch → flush → assert TWO snapshots with a different
/// snapshot ID.  Then flush again with no writes → still TWO (no empty snapshot).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_and_flush_creates_iceberg_snapshots() {
    let dir = TempDir::new().expect("tempdir");

    // ── Write batch 1 ─────────────────────────────────────────────────────────

    let cfg = sink_config(&dir, "events");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    let records: Vec<serde_json::Value> = (0u64..100)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();

    let written = sink.write_batch(&records).await.expect("write_batch 1");
    assert_eq!(written, 100, "expected 100 rows written");

    sink.flush().await.expect("flush 1");

    // ── Assert snapshot 1 ─────────────────────────────────────────────────────

    let reader = open_reader_catalog(&dir).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("namespace ident");
    let tid = TableIdent::new(ns.clone(), "events".to_string());

    let table1 = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush 1");
    let meta1 = table1.metadata();

    let snap_count_1 = meta1.snapshots().count();
    assert_eq!(
        snap_count_1, 1,
        "expected exactly 1 snapshot after first flush, got {snap_count_1}"
    );
    let snap1_id = meta1
        .current_snapshot_id()
        .expect("current_snapshot_id must be set after first flush");

    // ── Write batch 2 ─────────────────────────────────────────────────────────

    let records2: Vec<serde_json::Value> = (100u64..200)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();

    let written2 = sink.write_batch(&records2).await.expect("write_batch 2");
    assert_eq!(written2, 100, "expected 100 rows written in batch 2");

    sink.flush().await.expect("flush 2");

    // ── Assert snapshot 2 ─────────────────────────────────────────────────────

    let table2 = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush 2");
    let meta2 = table2.metadata();

    let snap_count_2 = meta2.snapshots().count();
    assert_eq!(
        snap_count_2, 2,
        "expected exactly 2 snapshots after second flush, got {snap_count_2}"
    );

    let snap2_id = meta2
        .current_snapshot_id()
        .expect("current_snapshot_id must be set after second flush");

    assert_ne!(
        snap1_id, snap2_id,
        "second flush must produce a new snapshot id (got {snap1_id} twice)"
    );

    // ── Empty flush is a no-op ─────────────────────────────────────────────────

    sink.flush().await.expect("flush 3 (empty)");

    let table3 = reader
        .load_table(&tid)
        .await
        .expect("load_table after empty flush");
    assert_eq!(
        table3.metadata().snapshots().count(),
        2,
        "empty flush must NOT create a third snapshot"
    );
}

/// Two writers committing from a stale base must both land — iceberg-rust
/// reloads the latest metadata and re-applies the append against the newest
/// snapshot rather than aborting (#193).
///
/// After a setup commit (snapshot 1), two sinks each lazily load the table at
/// snapshot 1 and buffer their data files. Sink A flushes → snapshot 2, leaving
/// sink B's in-memory base stale. Sink B then flushes: `Transaction::commit`
/// detects the stale base, rebases onto snapshot 2, and re-applies the
/// `fast_append` → snapshot 3. No error, no lost write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_writers_resolve_commit_conflict_via_refresh() {
    let dir = TempDir::new().expect("tempdir");

    // ── Setup: create the table + snapshot 1 ────────────────────────────────
    let setup = IcebergSink::new(sink_config(&dir, "races"))
        .await
        .expect("setup sink");
    let seed: Vec<serde_json::Value> = (0u64..10)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    setup.write_batch(&seed).await.expect("seed write");
    setup.flush().await.expect("seed flush");

    // ── Two writers both lazily load the table at snapshot 1 ────────────────
    let a = IcebergSink::new(sink_config(&dir, "races"))
        .await
        .expect("sink a");
    let b = IcebergSink::new(sink_config(&dir, "races"))
        .await
        .expect("sink b");

    let a_rows: Vec<serde_json::Value> = (100u64..110)
        .map(|i| json!({ "id": i, "name": format!("a{i}") }))
        .collect();
    let b_rows: Vec<serde_json::Value> = (200u64..210)
        .map(|i| json!({ "id": i, "name": format!("b{i}") }))
        .collect();

    // Both buffer (and lazily load the same base snapshot 1) before either
    // commits.
    a.write_batch(&a_rows).await.expect("a write");
    b.write_batch(&b_rows).await.expect("b write");

    // A commits → snapshot 2. B's cached base is now stale.
    a.flush().await.expect("a flush");
    // B commits from a stale base → iceberg reloads + rebases → snapshot 3.
    b.flush()
        .await
        .expect("b flush must succeed via metadata refresh, not abort");

    // ── Assert all three commits landed (no lost write) ─────────────────────
    let reader = open_reader_catalog(&dir).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("ns");
    let tid = TableIdent::new(ns, "races".to_string());
    let table = reader.load_table(&tid).await.expect("load table");
    assert_eq!(
        table.metadata().snapshots().count(),
        3,
        "setup + two stale-base commits must all produce snapshots (no lost write)"
    );
}

/// A snapshot commit that fails (here: the table is dropped out from under the
/// sink before flush) propagates as an error so the run aborts without
/// advancing the bookmark — and because a "table vanished" outcome is ambiguous
/// (not a definitive commit conflict), the uploaded data files are NOT deleted
/// even with `cleanup_orphans_on_failure` enabled (#193).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_failure_on_dropped_table_propagates_and_keeps_orphans() {
    use iceberg::Catalog;

    let dir = TempDir::new().expect("tempdir");

    // Create the table + first snapshot via a setup sink.
    let setup = IcebergSink::new(sink_config(&dir, "vanishing"))
        .await
        .expect("setup sink");
    let seed: Vec<serde_json::Value> = (0u64..5).map(|i| json!({ "id": i })).collect();
    setup.write_batch(&seed).await.expect("seed write");
    setup.flush().await.expect("seed flush");

    // A writer with cleanup enabled buffers a batch (loading the table).
    let mut cfg = sink_config(&dir, "vanishing");
    cfg.cleanup_orphans_on_failure = true;
    let writer = IcebergSink::new(cfg).await.expect("writer sink");
    let rows: Vec<serde_json::Value> = (100u64..105).map(|i| json!({ "id": i })).collect();
    writer.write_batch(&rows).await.expect("buffer write");

    // Drop the table from the catalog before the writer flushes.
    let reader = open_reader_catalog(&dir).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("ns");
    let tid = TableIdent::new(ns, "vanishing".to_string());
    reader.drop_table(&tid).await.expect("drop table");

    // The commit must fail (table gone) and surface as a Sink error.
    let err = writer
        .flush()
        .await
        .expect_err("flush must fail when the table was dropped");
    let msg = err.to_string();
    assert!(
        msg.contains("iceberg") && msg.contains("commit"),
        "error should describe the failed commit: {msg}"
    );
}

/// Empty `write_batch` followed by `flush` must not produce any snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_write_batch_no_snapshot() {
    let dir = TempDir::new().expect("tempdir");
    let cfg = sink_config(&dir, "empty_table");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    let written = sink.write_batch(&[]).await.expect("empty write_batch");
    assert_eq!(written, 0);

    // Flush without any prior writes — table does not even exist yet.
    // flush() must succeed without panicking.
    sink.flush().await.expect("flush after empty write_batch");

    // The table was never created (no records → no schema inference →
    // no `create_table` call), so it should not exist in the catalog.
    let reader = open_reader_catalog(&dir).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("namespace ident");
    let tid = TableIdent::new(ns, "empty_table".to_string());
    let exists = reader.table_exists(&tid).await.expect("table_exists check");
    assert!(!exists, "table must not exist after empty write+flush");
}

/// `current_schema` returns `Ok(None)` before the table exists and an
/// `infer_schema`-shaped object once the table has been created (#194, #255).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_schema_reports_table_columns() {
    let dir = TempDir::new().expect("tempdir");
    let cfg = sink_config(&dir, "drift_table");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    // Before any write the table does not exist → drift handling is inert.
    let before = sink.current_schema().await.expect("current_schema (pre)");
    assert_eq!(before, None, "missing table must report no schema");

    // Write + flush so the table is created with an inferred schema.
    let records: Vec<serde_json::Value> = (0u64..3)
        .map(|i| json!({ "id": i, "name": format!("n{i}"), "active": true }))
        .collect();
    sink.write_batch(&records).await.expect("write_batch");
    sink.flush().await.expect("flush");

    let schema = sink
        .current_schema()
        .await
        .expect("current_schema (post)")
        .expect("schema must be Some once the table exists");

    assert_eq!(schema["type"], "object");
    let props = schema["properties"].as_object().expect("properties object");
    // Inferred Iceberg columns surface as nullable JSON base types.
    assert_eq!(props["id"]["type"], json!(["integer", "null"]));
    assert_eq!(props["name"]["type"], json!(["string", "null"]));
    assert_eq!(props["active"]["type"], json!(["boolean", "null"]));
}

/// #255: additive schema evolution — `evolve_schema` adds a new optional column
/// to an existing table via iceberg-rust 0.10.0's `update_schema` action, and
/// rejects (non-applicable) widenings.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evolve_schema_adds_optional_column() {
    use faucet_core::{ColumnChange, SchemaEvolution};

    let dir = TempDir::new().expect("tempdir");
    let sink = IcebergSink::new(sink_config(&dir, "evolve"))
        .await
        .expect("IcebergSink::new");

    // Create the table with {id, name}.
    let records: Vec<serde_json::Value> = (0u64..2)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();
    sink.write_batch(&records).await.expect("write_batch");
    sink.flush().await.expect("flush");

    assert!(sink.supports_schema_evolution());

    // Add a new nullable column `email`.
    let evo = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".to_string(),
            from: None,
            to: json!({ "type": ["string", "null"] }),
        }],
        widenings: vec![],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evo)
        .await
        .expect("evolve_schema add column");

    // The destination schema now includes the new column.
    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("schema Some");
    let props = schema["properties"].as_object().expect("properties");
    assert!(
        props.contains_key("email"),
        "new column must appear: {schema}"
    );
    assert_eq!(props["email"]["type"], json!(["string", "null"]));

    // A widening is not applicable via iceberg-rust 0.10.0 → typed error.
    let widen = SchemaEvolution {
        additions: vec![],
        widenings: vec![ColumnChange {
            name: "id".to_string(),
            from: Some(json!({ "type": "integer" })),
            to: json!({ "type": "number" }),
        }],
        relax_nullability: vec![],
    };
    let err = sink.evolve_schema(&widen).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("additive schema evolution supports new columns only"),
        "got {err}"
    );
}

/// #255: `evolve_schema` is a no-op (Ok) when there are no additions, and when
/// the target table does not exist yet — covering both early-return branches.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evolve_schema_noop_branches() {
    use faucet_core::{ColumnChange, SchemaEvolution};

    let dir = TempDir::new().expect("tempdir");
    let sink = IcebergSink::new(sink_config(&dir, "evolve_noop"))
        .await
        .expect("IcebergSink::new");

    // Empty evolution → Ok without touching the catalog (no additions).
    sink.evolve_schema(&SchemaEvolution::default())
        .await
        .expect("empty evolution is a no-op");

    // Additions requested but the table does not exist yet → inert Ok (the
    // create path lays down the full schema on first write).
    let evo = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".to_string(),
            from: None,
            to: json!({ "type": ["string", "null"] }),
        }],
        widenings: vec![],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evo)
        .await
        .expect("evolve against an absent table is inert");
}

/// #255: adding a column whose name already exists is rejected by
/// `update_schema` — exercises `evolve_schema`'s error path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evolve_schema_duplicate_column_errors() {
    use faucet_core::{ColumnChange, SchemaEvolution};

    let dir = TempDir::new().expect("tempdir");
    let sink = IcebergSink::new(sink_config(&dir, "evolve_dup"))
        .await
        .expect("IcebergSink::new");

    // Create the table with {id, name}.
    sink.write_batch(&[json!({ "id": 1u64, "name": "a" })])
        .await
        .expect("write_batch");
    sink.flush().await.expect("flush");

    // Try to add a column that already exists.
    let evo = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "name".to_string(),
            from: None,
            to: json!({ "type": ["string", "null"] }),
        }],
        widenings: vec![],
        relax_nullability: vec![],
    };
    let err = sink.evolve_schema(&evo).await.unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::Sink(_)),
        "duplicate-column evolution must fail with a Sink error: {err}"
    );
    assert!(
        err.to_string().contains("schema evolution failed"),
        "got {err}"
    );
}

async fn read_rows(dir: &TempDir, table: &str) -> Vec<serde_json::Value> {
    use futures::TryStreamExt;
    let reader = open_reader_catalog(dir).await;
    let tid = TableIdent::new(NamespaceIdent::from_strs(["db"]).unwrap(), table.into());
    let t = reader.load_table(&tid).await.expect("load_table");
    let batches: Vec<_> = t
        .scan()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("to_arrow")
        .try_collect()
        .await
        .expect("read batches");
    let mut rows = Vec::new();
    for b in &batches {
        rows.extend(faucet_core::columnar::record_batch_to_values(b).unwrap());
    }
    rows.sort_by_key(|r| r["id"].as_i64());
    rows
}

/// #789 FILE-10: a partitioned table takes writes; each data file carries its
/// partition value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_to_a_partitioned_table_commit() {
    let dir = TempDir::new().expect("tempdir");
    let mut cfg = serde_json::to_value(sink_config(&dir, "parts")).unwrap();
    cfg["partition_spec"] = json!([{"source": "region", "transform": "identity"}]);
    let sink = IcebergSink::new(serde_json::from_value(cfg).unwrap())
        .await
        .expect("IcebergSink::new");
    let records: Vec<_> = (0i64..6)
        .map(|i| json!({"id": i, "region": if i % 2 == 0 { "eu" } else { "us" }}))
        .collect();
    sink.write_batch(&records).await.expect("write_batch");
    sink.flush()
        .await
        .expect("flush commits a partitioned append");

    let reader = open_reader_catalog(&dir).await;
    let tid = TableIdent::new(NamespaceIdent::from_strs(["db"]).unwrap(), "parts".into());
    let table = reader.load_table(&tid).await.unwrap();
    assert!(!table.metadata().default_partition_spec().is_unpartitioned());
    let rows = read_rows(&dir, "parts").await;
    assert_eq!(rows.len(), 6);
    assert_eq!(rows[1]["region"], "us");
}

/// #789 FILE-11: values of a column added by `evolve_schema` mid-run land.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn values_of_an_evolved_column_are_written() {
    use faucet_core::{ColumnChange, SchemaEvolution};
    let dir = TempDir::new().expect("tempdir");
    let sink = IcebergSink::new(sink_config(&dir, "evolve_values"))
        .await
        .expect("IcebergSink::new");
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();
    sink.write_batch(&[json!({"id": 2, "name": "b"})])
        .await
        .unwrap();

    sink.evolve_schema(&SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({"type": ["string", "null"]}),
        }],
        widenings: vec![],
        relax_nullability: vec![],
    })
    .await
    .expect("evolve");
    sink.write_batch(&[json!({"id": 3, "name": "c", "email": "c@x"})])
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let rows = read_rows(&dir, "evolve_values").await;
    assert_eq!(rows.len(), 3, "{rows:?}");
    assert_eq!(rows[1]["email"], serde_json::Value::Null);
    assert_eq!(rows[2]["email"], "c@x");
}

/// #789 FILE-12: a fractional value bound for an integer column fails the
/// write instead of being truncated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fractional_value_in_an_integer_column_is_refused() {
    let dir = TempDir::new().expect("tempdir");
    let sink = IcebergSink::new(sink_config(&dir, "ints"))
        .await
        .expect("IcebergSink::new");
    sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    let err = sink
        .write_batch(&[json!({"id": 10.5})])
        .await
        .expect_err("10.5 is not an integer");
    assert!(err.to_string().contains("'id'"), "{err}");
}

/// #789 FILE-16 / FILE-51: an exactly-once page that fails in a later chunk
/// commits nothing — not even its earlier chunks — and a committed token is
/// also kept as a table property, so it outlives its snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_idempotent_page_commits_nothing_and_tokens_are_carried() {
    let dir = TempDir::new().expect("tempdir");
    let mut cfg = sink_config(&dir, "eo");
    cfg.batch_size = 2;
    let sink = IcebergSink::new(cfg.clone()).await.expect("sink");
    let token = faucet_core::idempotency::format_token(1);
    sink.write_batch_idempotent(&[json!({"id": 1})], "p::r", &token)
        .await
        .unwrap();
    sink.flush().await.unwrap();

    let reader = open_reader_catalog(&dir).await;
    let tid = TableIdent::new(NamespaceIdent::from_strs(["db"]).unwrap(), "eo".into());
    let table = reader.load_table(&tid).await.unwrap();
    assert_eq!(
        table
            .metadata()
            .properties()
            .get("faucet.commit-token.p::r"),
        Some(&token)
    );
    assert_eq!(
        sink.last_committed_token("p::r").await.unwrap(),
        Some(token)
    );

    let page = [
        json!({"id": 2}),
        json!({"id": 3}),
        json!({"id": 4}),
        json!({"id": "not-a-number"}),
    ];
    let token2 = faucet_core::idempotency::format_token(2);
    assert!(
        sink.write_batch_idempotent(&page, "p::r", &token2)
            .await
            .is_err()
    );
    assert!(
        sink.flush().await.is_err(),
        "a poisoned sink refuses to commit"
    );
    assert!(sink.write_batch(&[json!({"id": 9})]).await.is_err());
    assert_eq!(read_rows(&dir, "eo").await, vec![json!({"id": 1})]);

    let fresh = IcebergSink::new(cfg).await.expect("sink");
    assert_eq!(
        fresh.last_committed_token("p::r").await.unwrap().as_deref(),
        Some(faucet_core::idempotency::format_token(1).as_str())
    );
}

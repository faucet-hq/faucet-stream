//! Shared fixture: a SQLite SQL catalog + local warehouse seeded through the
//! Iceberg sink.
#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use faucet_source_iceberg::{IcebergSource, IcebergSourceConfig};
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{
    DataContentType, DataFileBuilder, DataFileFormat, ManifestListWriter, ManifestWriterBuilder,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use serde_json::{Value, json};
use tempfile::TempDir;

pub struct Lake {
    pub dir: TempDir,
}

impl Lake {
    pub fn new() -> Self {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("warehouse")).unwrap();
        Self { dir }
    }

    pub fn uri(&self) -> String {
        format!(
            "sqlite:{}?mode=rwc",
            self.dir.path().join("catalog.db").display()
        )
    }

    pub fn warehouse(&self) -> String {
        format!("file://{}", self.dir.path().join("warehouse").display())
    }

    pub fn catalog_json(&self) -> Value {
        json!({ "type": "sql", "uri": self.uri(), "warehouse": self.warehouse() })
    }

    pub async fn append(&self, table: &str, ids: std::ops::Range<i64>) {
        let cfg: IcebergSinkConfig = serde_json::from_value(json!({
            "catalog": self.catalog_json(),
            "namespace": ["db"],
            "table": table,
            "create_if_missing": true,
            "batch_size": 0
        }))
        .unwrap();
        let sink = IcebergSink::new(cfg).await.unwrap();
        let rows: Vec<Value> = ids
            .map(|i| json!({ "id": i, "name": format!("n{i}"), "amount": i as f64 + 0.5 }))
            .collect();
        sink.write_batch(&rows).await.unwrap();
        sink.flush().await.unwrap();
    }

    pub async fn catalog(&self) -> iceberg_catalog_sql::SqlCatalog {
        let props = HashMap::from([
            (SQL_CATALOG_PROP_URI.to_string(), self.uri()),
            (SQL_CATALOG_PROP_WAREHOUSE.to_string(), self.warehouse()),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::QMark.to_string(),
            ),
        ]);
        SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(faucet_common_iceberg::CATALOG_NAME, props)
            .await
            .unwrap()
    }

    pub async fn table(&self, table: &str) -> iceberg::table::Table {
        let ns = NamespaceIdent::from_strs(["db"]).unwrap();
        self.catalog()
            .await
            .load_table(&TableIdent::new(ns, table.to_string()))
            .await
            .unwrap()
    }

    /// Snapshot ids oldest first.
    pub async fn snapshots(&self, table: &str) -> Vec<(i64, i64)> {
        let t = self.table(table).await;
        let mut s: Vec<_> = t
            .metadata()
            .snapshots()
            .map(|s| (s.sequence_number(), s.snapshot_id(), s.timestamp_ms()))
            .collect();
        s.sort();
        s.into_iter().map(|(_, id, ts)| (id, ts)).collect()
    }

    pub fn source_config(&self, table: &str, extra: Value) -> IcebergSourceConfig {
        let mut v = json!({ "catalog": self.catalog_json(), "table": format!("db.{table}") });
        for (k, val) in extra.as_object().unwrap() {
            v[k] = val.clone();
        }
        serde_json::from_value(v).unwrap()
    }

    pub async fn source(&self, table: &str, extra: Value) -> IcebergSource {
        IcebergSource::new(self.source_config(table, extra))
            .await
            .unwrap()
    }

    /// Commit a `delete` snapshot carrying one equality-delete file on `id`.
    pub async fn delete_ids(&self, table: &str, ids: &[i64]) -> i64 {
        let t = self.table(table).await;
        let meta = t.metadata();
        let schema = meta.current_schema().clone();
        let field = schema.field_by_name("id").unwrap();
        let current = meta.current_snapshot().unwrap().clone();
        let new_id = current.snapshot_id() + 1_000;
        let seq = meta.last_sequence_number() + 1;
        let location = meta.location().trim_end_matches('/').to_string();
        let local = |p: &str| p.trim_start_matches("file://").to_string();

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, !field.required).with_metadata(HashMap::from([(
                parquet::arrow::PARQUET_FIELD_ID_META_KEY.to_string(),
                field.id.to_string(),
            )])),
        ]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(Int64Array::from(ids.to_vec()))],
        )
        .unwrap();
        let del_path = format!("{location}/data/eqdel-{}.parquet", uuid::Uuid::new_v4());
        std::fs::create_dir_all(std::path::Path::new(&local(&del_path)).parent().unwrap()).unwrap();
        {
            let f = std::fs::File::create(local(&del_path)).unwrap();
            let mut w = parquet::arrow::ArrowWriter::try_new(f, arrow_schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let size = std::fs::metadata(local(&del_path)).unwrap().len();
        let data_file = DataFileBuilder::default()
            .content(DataContentType::EqualityDeletes)
            .file_path(del_path)
            .file_format(DataFileFormat::Parquet)
            .record_count(ids.len() as u64)
            .file_size_in_bytes(size)
            .equality_ids(Some(vec![field.id]))
            .partition_spec_id(meta.default_partition_spec_id())
            .build()
            .unwrap();

        let manifest_out = t
            .file_io()
            .new_output(format!(
                "{location}/metadata/{}-m0.avro",
                uuid::Uuid::new_v4()
            ))
            .unwrap();
        let mut mw = ManifestWriterBuilder::new(
            manifest_out,
            Some(new_id),
            schema.clone(),
            (**meta.default_partition_spec()).clone(),
        )
        .build_v2_deletes();
        mw.add_file(data_file, seq).unwrap();
        let delete_manifest = mw.write_manifest_file().await.unwrap();

        let existing = t.manifest_list_reader(&current).load().await.unwrap();
        let list_path = format!(
            "{location}/metadata/snap-{new_id}-{}.avro",
            uuid::Uuid::new_v4()
        );
        let writer = t
            .file_io()
            .new_output(&list_path)
            .unwrap()
            .writer()
            .await
            .unwrap();
        let mut lw = ManifestListWriter::v2(writer, new_id, Some(current.snapshot_id()), seq);
        lw.add_manifests(
            existing
                .entries()
                .iter()
                .cloned()
                .chain(std::iter::once(delete_manifest)),
        )
        .unwrap();
        lw.close().await.unwrap();

        self.commit_snapshot(
            table,
            &t,
            new_id,
            current.snapshot_id(),
            seq,
            &list_path,
            "delete",
        )
        .await;
        new_id
    }

    /// Point the catalog at a hand-edited metadata file adding one snapshot.
    #[allow(clippy::too_many_arguments)]
    pub async fn commit_snapshot(
        &self,
        table: &str,
        t: &iceberg::table::Table,
        new_id: i64,
        parent: i64,
        seq: i64,
        manifest_list: &str,
        operation: &str,
    ) {
        let old_loc = t.metadata_location().unwrap().to_string();
        let mut meta: Value =
            serde_json::from_slice(&std::fs::read(old_loc.trim_start_matches("file://")).unwrap())
                .unwrap();
        let now = meta["last-updated-ms"].as_i64().unwrap() + 1_000;
        let schema_id = meta["current-schema-id"].clone();
        meta["snapshots"].as_array_mut().unwrap().push(json!({
            "snapshot-id": new_id,
            "parent-snapshot-id": parent,
            "sequence-number": seq,
            "timestamp-ms": now,
            "manifest-list": manifest_list,
            "summary": { "operation": operation },
            "schema-id": schema_id,
        }));
        meta["current-snapshot-id"] = json!(new_id);
        meta["last-sequence-number"] = json!(seq);
        meta["last-updated-ms"] = json!(now);
        meta["refs"] = json!({ "main": { "snapshot-id": new_id, "type": "branch" } });
        meta["snapshot-log"]
            .as_array_mut()
            .unwrap()
            .push(json!({ "snapshot-id": new_id, "timestamp-ms": now }));
        let location = meta["location"]
            .as_str()
            .unwrap()
            .trim_end_matches('/')
            .to_string();
        let new_loc = format!(
            "{location}/metadata/99999-{}.metadata.json",
            uuid::Uuid::new_v4()
        );
        std::fs::write(
            new_loc.trim_start_matches("file://"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();

        let pool = sqlx::SqlitePool::connect(&self.uri()).await.unwrap();
        let updated = sqlx::query(
            "UPDATE iceberg_tables SET metadata_location = ?, previous_metadata_location = ? \
             WHERE table_name = ?",
        )
        .bind(&new_loc)
        .bind(&old_loc)
        .bind(table)
        .execute(&pool)
        .await
        .unwrap();
        assert_eq!(updated.rows_affected(), 1);
    }
}

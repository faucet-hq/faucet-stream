//! Shared by the catalog round trips: write two snapshots through the sink,
//! then reopen the table through a fresh catalog and scan every row back.

use faucet_common_iceberg::{CatalogConfig, build_catalog};
use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use futures::TryStreamExt;
use iceberg::spec::Operation;
use iceberg::{NamespaceIdent, TableIdent};
use serde_json::{Value, json};

#[allow(dead_code)]
pub async fn create_s3_bucket(endpoint: &str, access_key: &str, secret_key: &str, bucket: &str) {
    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new(access_key, secret_key, None, None, "test"))
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(config)
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("create bucket");
}

/// OpenDAL S3 properties pointing at an S3-compatible endpoint.
#[allow(dead_code)]
pub fn s3_props(endpoint: &str, access_key: &str, secret_key: &str) -> Value {
    json!({
        "s3.endpoint": endpoint,
        "s3.access-key-id": access_key,
        "s3.secret-access-key": secret_key,
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
        "s3.disable-config-load": "true",
        "s3.disable-ec2-metadata": "true"
    })
}

/// Write 25 then 15 rows (one snapshot each) into `db.<table>` and check the
/// table lives under `location_prefix`, has two appends and scans 40 rows.
pub async fn assert_round_trip(catalog: Value, table: &str, location_prefix: &str) {
    let cfg: IcebergSinkConfig = serde_json::from_value(json!({
        "catalog": catalog,
        "namespace": ["db"],
        "table": table,
        "create_if_missing": true,
        "batch_size": 0
    }))
    .expect("sink config");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");
    for ids in [0u64..25, 25..40] {
        let records: Vec<Value> = ids
            .map(|i| json!({ "id": i, "name": format!("n{i}") }))
            .collect();
        let n = records.len();
        assert_eq!(sink.write_batch(&records).await.expect("write_batch"), n);
        sink.flush().await.expect("flush");
    }

    let reader_cfg: CatalogConfig = serde_json::from_value(catalog).expect("catalog config");
    let reader = build_catalog(&reader_cfg).await.expect("reader catalog");
    let loaded = reader
        .load_table(&TableIdent::new(
            NamespaceIdent::from_strs(["db"]).unwrap(),
            table.to_string(),
        ))
        .await
        .expect("load_table");
    let meta = loaded.metadata();
    assert!(
        meta.location().starts_with(location_prefix),
        "{} not under {location_prefix}",
        meta.location()
    );
    let appends = meta
        .snapshots()
        .filter(|s| s.summary().operation == Operation::Append)
        .count();
    assert_eq!(appends, 2, "one append snapshot per flush");

    let batches: Vec<_> = loaded
        .scan()
        .select_all()
        .build()
        .expect("scan")
        .to_arrow()
        .await
        .expect("arrow stream")
        .try_collect()
        .await
        .expect("scan rows");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 40, "every written row scans back");
}

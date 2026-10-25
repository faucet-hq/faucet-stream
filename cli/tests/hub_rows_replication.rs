//! The rows report names an incremental read whichever config key turns it on.
#![cfg(all(feature = "source-postgres", feature = "sink-jsonl"))]

use serde_json::json;

#[test]
fn sql_incremental_row_is_reported_incremental_and_resumable() {
    let doc = json!({
        "version": 1,
        "name": "orders",
        "pipeline": {
            "source": {"type": "postgres", "config": {
                "connection_url": "postgres://localhost/db",
                "query": "SELECT * FROM orders WHERE updated_at >= ${bookmark}",
                "replication": {"type": "incremental", "column": "updated_at", "initial_value": "1970-01-01"}
            }},
            "sink": {"type": "jsonl", "config": {"path": "out.jsonl"}},
            "state": {"type": "file", "config": {"dir": "state"}}
        }
    });
    let rows = faucet_cli::hub::rows::rows_for_pipeline(&doc).expect("rows");
    let row = &rows.report.rows[0];
    assert_eq!(row.read.replication.method, "incremental");
    assert_eq!(row.read.replication.field.as_deref(), Some("updated_at"));
    assert!(row.read.resumable);
}

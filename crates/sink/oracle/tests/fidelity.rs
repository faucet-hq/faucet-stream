//! Typed round-trip fidelity: the shared corpus written by the Oracle sink
//! into an auto-created table and read back by the Oracle source. Skips
//! cleanly without Docker or Oracle Instant Client.
//!
//! Tolerances are Oracle limitations, not conveniences:
//! - a zero-length string is stored as `NULL`;
//! - booleans land in `NUMBER(1)` (no SQL `BOOLEAN` before 23ai) and read back
//!   as `1` / `0`;
//! - Oracle does not distinguish negative zero: `-0.0` is stored as `0`.

mod common;

use faucet_conformance::fidelity::{self, Tolerance};
use faucet_core::{Sink, Source};
use faucet_sink_oracle::{OracleSink, OracleSinkConfig};
use faucet_source_oracle::{OracleSource, OracleSourceConfig};

#[tokio::test(flavor = "multi_thread")]
async fn oracle_round_trip_fidelity() {
    let Some((_container, conn)) = common::start_oracle().await else {
        return;
    };
    let sent = fidelity::corpus();
    let sink = OracleSink::new(OracleSinkConfig::new(conn.clone(), "fidelity"))
        .await
        .expect("sink");
    sink.write_batch(&sent).await.expect("write corpus");

    let mut read = OracleSourceConfig::new(conn, "SELECT * FROM \"fidelity\"");
    read.json_columns = ["object", "array", "empty_object", "empty_array"]
        .map(String::from)
        .to_vec();
    let landed = OracleSource::new(read)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    let landed: Vec<serde_json::Value> = landed
        .into_iter()
        .map(|mut r| {
            if let Some(o) = r.as_object_mut() {
                o.retain(|_, v| !v.is_null());
            }
            r
        })
        .collect();

    fidelity::assert_round_trip(
        &sent,
        &landed,
        Tolerance::exact()
            .empty_string_becomes_null()
            .skipping("true_val")
            .skipping("false_val")
            .skipping("negative_zero"),
    );
}

//! Internal change-record types and the parsers that build them.
//!
//! Spanner returns three kinds of change record from a change-stream query: a
//! `data_change_record`, a `heartbeat_record` and a `child_partitions_record`.
//! GoogleSQL packs them as one `ChangeRecord` column holding a one-element
//! array whose struct has all three as arrays (exactly one non-empty). The
//! PostgreSQL JSON form carries one record per row. Both reduce to
//! [`ChangeRecord`] here, so everything downstream is dialect-agnostic.

use chrono::{DateTime, Utc};
use faucet_core::FaucetError;
use serde_json::{Map, Value};

use crate::config::parse_timestamp;

/// One parsed change record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ChangeRecord {
    /// Boxed: a data change is far larger than the other variants.
    Data(Box<DataChange>),
    /// Everything in the partition up to this time has been returned.
    Heartbeat(DateTime<Utc>),
    /// The partition ended; these partitions continue it.
    Children(Vec<ChildPartition>),
}

/// A partition announced by a `child_partitions_record`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ChildPartition {
    pub token: String,
    /// Partitions that must be fully read before this one starts. Empty for the
    /// partitions returned by the root query.
    pub parents: Vec<String>,
    /// When this partition starts (read it from here, never earlier).
    pub start: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ModType {
    Insert,
    Update,
    Delete,
}

impl ModType {
    /// The envelope `op` value (`c`, `u`, `d`).
    pub fn op(self) -> &'static str {
        match self {
            Self::Insert => "c",
            Self::Update => "u",
            Self::Delete => "d",
        }
    }
}

/// One modified row inside a data change record.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Mod {
    pub keys: Map<String, Value>,
    pub new_values: Map<String, Value>,
    pub old_values: Map<String, Value>,
}

/// A `data_change_record`: the changes one transaction made to one table in one
/// partition.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DataChange {
    pub table: String,
    pub commit_timestamp: DateTime<Utc>,
    pub server_transaction_id: String,
    pub record_sequence: String,
    pub is_last_in_transaction: bool,
    pub is_system_transaction: bool,
    pub transaction_tag: String,
    pub value_capture_type: String,
    pub mod_type: ModType,
    /// Column name -> Spanner type JSON (`{"code":"INT64"}`).
    pub column_types: Map<String, Value>,
    pub mods: Vec<Mod>,
}

fn bad(what: &str) -> FaucetError {
    FaucetError::Source(format!("spanner-cdc: malformed change record: {what}"))
}

fn str_field<'a>(v: &'a Value, key: &str) -> Result<&'a str, FaucetError> {
    v.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| bad(&format!("missing string field `{key}`")))
}

fn ts_field(v: &Value, key: &str) -> Result<DateTime<Utc>, FaucetError> {
    let s = str_field(v, key)?;
    parse_timestamp(s).map_err(|e| bad(&format!("`{key}` {s:?} is not a timestamp: {e}")))
}

fn object_field(v: &Value, key: &str) -> Map<String, Value> {
    match v.get(key) {
        Some(Value::Object(m)) => m.clone(),
        // Some clients hand JSON columns back as text.
        Some(Value::String(s)) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Object(m)) => m,
            _ => Map::new(),
        },
        _ => Map::new(),
    }
}

fn array_field<'a>(v: &'a Value, key: &str) -> &'a [Value] {
    v.get(key)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

/// Parse one `data_change_record` object.
pub(crate) fn parse_data_change(v: &Value) -> Result<DataChange, FaucetError> {
    let mod_type = match str_field(v, "mod_type")? {
        "INSERT" => ModType::Insert,
        "UPDATE" => ModType::Update,
        "DELETE" => ModType::Delete,
        other => return Err(bad(&format!("unknown mod_type {other:?}"))),
    };
    let mut column_types = Map::new();
    for col in array_field(v, "column_types") {
        if let (Some(name), Some(ty)) = (col.get("name").and_then(Value::as_str), col.get("type")) {
            column_types.insert(name.to_string(), ty.clone());
        }
    }
    let mods = array_field(v, "mods")
        .iter()
        .map(|m| Mod {
            keys: object_field(m, "keys"),
            new_values: object_field(m, "new_values"),
            old_values: object_field(m, "old_values"),
        })
        .collect();
    Ok(DataChange {
        table: str_field(v, "table_name")?.to_string(),
        commit_timestamp: ts_field(v, "commit_timestamp")?,
        server_transaction_id: str_field(v, "server_transaction_id")?.to_string(),
        record_sequence: str_field(v, "record_sequence")?.to_string(),
        is_last_in_transaction: v
            .get("is_last_record_in_transaction_in_partition")
            .and_then(Value::as_bool)
            .unwrap_or(true),
        is_system_transaction: v
            .get("is_system_transaction")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        transaction_tag: v
            .get("transaction_tag")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        value_capture_type: v
            .get("value_capture_type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        mod_type,
        column_types,
        mods,
    })
}

/// Parse one `heartbeat_record` object.
pub(crate) fn parse_heartbeat(v: &Value) -> Result<DateTime<Utc>, FaucetError> {
    ts_field(v, "timestamp")
}

/// Parse one `child_partitions_record` object.
pub(crate) fn parse_children(v: &Value) -> Result<Vec<ChildPartition>, FaucetError> {
    let start = ts_field(v, "start_timestamp")?;
    array_field(v, "child_partitions")
        .iter()
        .map(|c| {
            Ok(ChildPartition {
                token: str_field(c, "token")?.to_string(),
                parents: array_field(c, "parent_partition_tokens")
                    .iter()
                    .filter_map(|p| p.as_str().map(str::to_string))
                    .collect(),
                start,
            })
        })
        .collect()
}

/// Parse one `{data_change_record: [..], heartbeat_record: [..],
/// child_partitions_record: [..]}` struct (the GoogleSQL shape).
pub(crate) fn parse_record_set(set: &Value) -> Result<Vec<ChangeRecord>, FaucetError> {
    let mut out = Vec::new();
    for d in array_field(set, "data_change_record") {
        out.push(ChangeRecord::Data(Box::new(parse_data_change(d)?)));
    }
    for h in array_field(set, "heartbeat_record") {
        out.push(ChangeRecord::Heartbeat(parse_heartbeat(h)?));
    }
    for c in array_field(set, "child_partitions_record") {
        out.push(ChangeRecord::Children(parse_children(c)?));
    }
    Ok(out)
}

/// Parse one decoded GoogleSQL result row (`{"ChangeRecord": [ {..} ]}`).
pub(crate) fn parse_googlesql_row(row: &Value) -> Result<Vec<ChangeRecord>, FaucetError> {
    let sets = row
        .get("ChangeRecord")
        .and_then(Value::as_array)
        .ok_or_else(|| bad("row has no `ChangeRecord` array"))?;
    let mut out = Vec::new();
    for set in sets {
        out.extend(parse_record_set(set)?);
    }
    Ok(out)
}

/// Parse one decoded PostgreSQL result row.
///
/// The PostgreSQL change-stream function (`spanner.read_json_<stream>`) returns
/// one JSONB column per row, holding exactly one externally tagged record:
/// `{"data_change_record": {..}}`, `{"heartbeat_record": {..}}` or
/// `{"child_partitions_record": {..}}`. The column is named after the function,
/// so the first (only) column is used rather than a fixed name.
pub(crate) fn parse_pg_row(row: &Value) -> Result<Vec<ChangeRecord>, FaucetError> {
    let cell = row
        .as_object()
        .and_then(|cols| cols.values().next())
        .ok_or_else(|| bad("row has no JSON column"))?;
    // The cell is a JSONB value; tolerate a client that hands it back as text.
    let parsed;
    let record = match cell {
        Value::String(text) => {
            parsed = serde_json::from_str::<Value>(text)
                .map_err(|e| bad(&format!("record column is not JSON: {e}")))?;
            &parsed
        }
        other => other,
    };
    let obj = record
        .as_object()
        .ok_or_else(|| bad("record column is not a JSON object"))?;
    let mut out = Vec::new();
    for (tag, body) in obj {
        match tag.as_str() {
            "data_change_record" => {
                out.push(ChangeRecord::Data(Box::new(parse_data_change(body)?)))
            }
            "heartbeat_record" => out.push(ChangeRecord::Heartbeat(parse_heartbeat(body)?)),
            "child_partitions_record" => out.push(ChangeRecord::Children(parse_children(body)?)),
            other => return Err(bad(&format!("unknown record kind `{other}`"))),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data(mod_type: &str, last: bool) -> Value {
        json!({
            "table_name": "t",
            "commit_timestamp": "2026-10-12T08:05:15.454853Z",
            "server_transaction_id": "tx1",
            "record_sequence": "00000000",
            "is_last_record_in_transaction_in_partition": last,
            "is_system_transaction": false,
            "transaction_tag": "",
            "value_capture_type": "OLD_AND_NEW_VALUES",
            "mod_type": mod_type,
            "column_types": [
                {"name": "id", "type": {"code": "INT64"}, "is_primary_key": true},
                {"name": "name", "type": {"code": "STRING"}, "is_primary_key": false}
            ],
            "mods": [{"keys": {"id": "1"}, "new_values": {"name": "a"}, "old_values": {}}]
        })
    }

    #[test]
    fn parses_a_data_change_record() {
        let d = parse_data_change(&data("INSERT", true)).unwrap();
        assert_eq!(d.table, "t");
        assert_eq!(d.mod_type, ModType::Insert);
        assert!(d.is_last_in_transaction);
        assert_eq!(d.mods.len(), 1);
        assert_eq!(d.mods[0].keys["id"], json!("1"));
        assert_eq!(d.column_types["id"], json!({"code": "INT64"}));
        assert_eq!(d.commit_timestamp.timestamp_subsec_micros(), 454_853);
    }

    #[test]
    fn mod_types_map_to_ops() {
        for (mt, op) in [("INSERT", "c"), ("UPDATE", "u"), ("DELETE", "d")] {
            assert_eq!(
                parse_data_change(&data(mt, true)).unwrap().mod_type.op(),
                op
            );
        }
    }

    #[test]
    fn unknown_mod_type_is_a_typed_error() {
        let err = parse_data_change(&data("MERGE", true)).unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
        assert!(err.to_string().contains("MERGE"));
    }

    #[test]
    fn missing_required_fields_are_typed_errors() {
        let mut v = data("INSERT", true);
        v.as_object_mut().unwrap().remove("commit_timestamp");
        assert!(matches!(parse_data_change(&v), Err(FaucetError::Source(_))));
        let mut v = data("INSERT", true);
        v["commit_timestamp"] = json!("not a time");
        assert!(parse_data_change(&v).is_err());
    }

    #[test]
    fn json_columns_delivered_as_text_are_accepted() {
        let mut v = data("INSERT", true);
        v["mods"][0]["new_values"] = json!("{\"name\":\"b\"}");
        let d = parse_data_change(&v).unwrap();
        assert_eq!(d.mods[0].new_values["name"], json!("b"));
    }

    #[test]
    fn parses_heartbeat_and_children() {
        let hb = parse_heartbeat(&json!({"timestamp": "2026-10-12T08:05:25.54883Z"})).unwrap();
        assert_eq!(hb.timestamp_subsec_micros(), 548_830);

        let kids = parse_children(&json!({
            "start_timestamp": "2026-10-12T08:05:35.439975Z",
            "record_sequence": "00000000",
            "child_partitions": [
                {"token": "A", "parent_partition_tokens": []},
                {"token": "B", "parent_partition_tokens": ["P", "Q"]}
            ]
        }))
        .unwrap();
        assert_eq!(kids.len(), 2);
        assert!(kids[0].parents.is_empty());
        assert_eq!(kids[1].parents, vec!["P".to_string(), "Q".to_string()]);
        assert_eq!(kids[1].start, kids[0].start);
    }

    #[test]
    fn googlesql_row_with_one_populated_array() {
        let row = json!({"ChangeRecord": [{
            "data_change_record": [],
            "heartbeat_record": [{"timestamp": "2026-10-12T08:05:25Z"}],
            "child_partitions_record": []
        }]});
        let recs = parse_googlesql_row(&row).unwrap();
        assert_eq!(recs.len(), 1);
        assert!(matches!(recs[0], ChangeRecord::Heartbeat(_)));
    }

    #[test]
    fn googlesql_row_with_data_record() {
        let row = json!({"ChangeRecord": [{
            "data_change_record": [data("DELETE", false)],
            "heartbeat_record": [],
            "child_partitions_record": []
        }]});
        let recs = parse_googlesql_row(&row).unwrap();
        match &recs[0] {
            ChangeRecord::Data(d) => {
                assert_eq!(d.mod_type, ModType::Delete);
                assert!(!d.is_last_in_transaction);
            }
            other => panic!("expected data record, got {other:?}"),
        }
    }

    #[test]
    fn row_without_change_record_column_is_an_error() {
        assert!(parse_googlesql_row(&json!({"other": 1})).is_err());
    }

    #[test]
    fn parses_each_postgresql_record_kind() {
        let row = json!({"read_json_orders_cs": {"data_change_record": data("UPDATE", true)}});
        let recs = parse_pg_row(&row).unwrap();
        assert!(matches!(&recs[..], [ChangeRecord::Data(d)] if d.mod_type == ModType::Update));

        let row = json!({"read_json_orders_cs": {
            "heartbeat_record": {"timestamp": "2026-10-12T08:05:15.454853+00:00"}
        }});
        assert!(matches!(
            &parse_pg_row(&row).unwrap()[..],
            [ChangeRecord::Heartbeat(_)]
        ));

        let row = json!({"read_json_orders_cs": {"child_partitions_record": {
            "start_timestamp": "2026-10-12T08:05:15.454853+00:00",
            "record_sequence": "00000000",
            "child_partitions": [{"token": "c1", "parent_partition_tokens": []}]
        }}});
        match &parse_pg_row(&row).unwrap()[..] {
            [ChangeRecord::Children(c)] => assert_eq!(c[0].token, "c1"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn postgresql_record_may_arrive_as_json_text() {
        let text = json!({"heartbeat_record": {"timestamp": "2026-10-12T08:05:15Z"}}).to_string();
        let row = json!({"read_json_x": text});
        assert!(matches!(
            &parse_pg_row(&row).unwrap()[..],
            [ChangeRecord::Heartbeat(_)]
        ));
    }

    #[test]
    fn malformed_postgresql_rows_are_typed_errors() {
        assert!(parse_pg_row(&json!({})).is_err());
        assert!(parse_pg_row(&json!({"c": 5})).is_err());
        assert!(parse_pg_row(&json!({"c": {"mystery_record": {}}})).is_err());
        assert!(parse_pg_row(&json!({"c": "not json"})).is_err());
    }
}

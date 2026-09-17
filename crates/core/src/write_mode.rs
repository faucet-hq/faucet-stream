//! Unified write-mode types + planner shared by every upsert-capable sink.

use crate::error::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Write semantics for a sink. Serialized snake_case. Default `Append`.
// `#[non_exhaustive]`: this is a deliberate extension point — adding a write
// mode (as `Overwrite` was, #492) is an additive change that ships as a minor
// release. Downstream connectors that `match` on it must carry a wildcard arm;
// the built-in sinks already gate on the specific modes they implement. Kept a
// plain comment (not rustdoc) so the schema/rustdoc description is unchanged.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, schemars::JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum WriteMode {
    /// Insert every record (today's behaviour).
    #[default]
    Append,
    /// Insert-or-update by `key`; optionally route delete-marked rows to deletes.
    Upsert,
    /// Delete by `key` for every record.
    Delete,
    /// Replace the entire destination with this run's records (truncate-load /
    /// full refresh). The old contents are swapped out atomically only after the
    /// run completes successfully, so a mid-run failure leaves them intact. No
    /// `key` is required — it is a whole-dataset operation, not a keyed one.
    Overwrite,
}

impl WriteMode {
    /// Lowercase wire name, for error messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::Append => "append",
            WriteMode::Upsert => "upsert",
            WriteMode::Delete => "delete",
            WriteMode::Overwrite => "overwrite",
        }
    }
}

/// Identifies a record as a delete (vs. an upsert) by a marker field's value.
/// e.g. `{ field: "__op", values: ["d", "delete"] }`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
pub struct DeleteMarker {
    /// Field name whose value flags a delete.
    pub field: String,
    /// Values of `field` that mean "this row is a delete".
    pub values: Vec<String>,
}

/// Scope for a **scoped/windowed overwrite** (#518): with `write_mode:
/// overwrite`, replace only the destination rows matching this scope instead of
/// truncating the whole table. The sink-side sibling of scoped cleanup (#478).
///
/// v1 supports a half-open date/number **window** on a single column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OverwriteScope {
    /// Replace rows where `column` is in the half-open range `[from, to)`.
    Window {
        /// The destination column to window on.
        column: String,
        /// Inclusive lower bound.
        from: Value,
        /// Exclusive upper bound.
        to: Value,
    },
}

impl OverwriteScope {
    /// The scoped column name.
    pub fn column(&self) -> &str {
        match self {
            OverwriteScope::Window { column, .. } => column,
        }
    }

    /// Validate the scope at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        match self {
            OverwriteScope::Window { column, from, to } => {
                if column.trim().is_empty() {
                    return Err(FaucetError::Config(
                        "overwrite scope: window `column` must not be empty".into(),
                    ));
                }
                if from.is_null() || to.is_null() {
                    return Err(FaucetError::Config(
                        "overwrite scope: window `from`/`to` must not be null".into(),
                    ));
                }
                Ok(())
            }
        }
    }

    /// Render a SQL `WHERE` predicate over the (already-quoted) column using
    /// escaped SQL **literals** for the bounds. Literals (not bind params) are
    /// used so the engine coerces a string bound to the column's real type
    /// (`date >= '2024-06-01'` works; a text-typed bind param would not). String
    /// literals have their single quotes doubled, so a crafted bound cannot
    /// break out of the quotes.
    pub fn render_where_literal(&self, quoted_col: &str) -> String {
        match self {
            OverwriteScope::Window { from, to, .. } => format!(
                "{quoted_col} >= {} AND {quoted_col} < {}",
                sql_literal(from),
                sql_literal(to)
            ),
        }
    }
}

/// Render a JSON scalar as an **ANSI-standard** SQL literal: strings are
/// single-quoted with embedded quotes doubled (`''`), numbers and booleans pass
/// through in their canonical form, and anything else becomes `NULL`.
///
/// This is the house escaper for every dialect that follows the ANSI rule —
/// Postgres (with `standard_conforming_strings`, the default since 9.1), BigQuery
/// string literals built by the overwrite/cleanup planners, MySQL under
/// `NO_BACKSLASH_ESCAPES`, SQL Server, and SQLite. **Public so connectors escape
/// through one audited implementation instead of hand-rolling a third
/// convention** (#654 M14): the copies that existed before it was exported each
/// invented their own rules, so a fix to one reached none of the others.
///
/// A dialect whose string literals treat some *other* character as a
/// meta-character cannot use this function — doubling quotes alone does not
/// neutralise a trailing escape character. Such a dialect must keep its own
/// escaper, and that escaper's doc comment must state which extra characters it
/// escapes and why (see `faucet_common_clickhouse::sql_literal`, where a
/// backslash is a literal meta-character).
pub fn sql_literal(v: &Value) -> String {
    match v {
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        // Validated non-null upstream; any residual maps to NULL (never matches
        // a range comparison, so the delete is a safe no-op rather than wrong).
        _ => "NULL".to_owned(),
    }
}

/// Shared write-mode config, embedded in each upsert-capable sink config via
/// `#[serde(flatten)]` so `write_mode` / `key` / `delete_marker` appear at the
/// sink-config top level.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WriteSpec {
    /// Append (default), upsert, or delete.
    #[serde(default)]
    pub write_mode: WriteMode,
    /// Key columns. Required and non-empty for upsert/delete; ignored for append.
    #[serde(default)]
    pub key: Vec<String>,
    /// Optional. Upsert only: rows whose `field` matches one of `values` are
    /// deletes; all others are upserts. The marker field is stripped from
    /// upsert rows before writing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_marker: Option<DeleteMarker>,
}

impl WriteSpec {
    /// Validate internal consistency at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if matches!(self.write_mode, WriteMode::Upsert | WriteMode::Delete) && self.key.is_empty() {
            return Err(FaucetError::Config(format!(
                "write_mode: {} requires a non-empty `key`",
                self.write_mode.as_str()
            )));
        }
        Ok(())
    }

    /// Whether this spec makes writes converge by key — `write_mode: upsert`
    /// or `delete` with a non-empty `key`. The canonical implementation of
    /// [`Sink::dedups_by_key`](crate::Sink::dedups_by_key) for sinks that
    /// flatten a `WriteSpec` into their config.
    pub fn dedups_by_key(&self) -> bool {
        matches!(self.write_mode, WriteMode::Upsert | WriteMode::Delete) && !self.key.is_empty()
    }

    /// Whether this spec requests full-destination replacement
    /// ([`WriteMode::Overwrite`]). The canonical implementation of
    /// [`Sink::is_overwrite`](crate::Sink::is_overwrite) for sinks that flatten
    /// a `WriteSpec` into their config.
    pub fn is_overwrite(&self) -> bool {
        matches!(self.write_mode, WriteMode::Overwrite)
    }
}

/// Ordered key column → value pairs, in `key` declaration order.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyTuple(pub Vec<(String, Value)>);

/// The partition of a page by write mode. Infallible to build — per-row
/// failures (missing/null key) land in `failed` with their original page index
/// so the caller can route them to a DLQ or abort.
#[derive(Debug, Default)]
pub struct WritePlan {
    /// Rows to insert-or-update, deduped (last-write-wins), marker stripped.
    pub upserts: Vec<Value>,
    /// Key tuples to delete, deduped.
    pub deletes: Vec<KeyTuple>,
    /// `(page_index, message)` for rows whose key could not be extracted.
    pub failed: Vec<(usize, String)>,
}

#[derive(Clone)]
enum Action {
    Upsert(Value),
    Delete(KeyTuple),
}

/// Partition `page` into upserts + deletes per `spec`. The single place all six
/// sinks share. `WriteMode::Append` should never reach here (callers route
/// append separately); if it does, every row is treated as an upsert.
pub fn plan_writes(page: &[Value], spec: &WriteSpec) -> WritePlan {
    debug_assert!(
        matches!(spec.write_mode, WriteMode::Upsert | WriteMode::Delete),
        "plan_writes is only for Upsert/Delete — Append and Overwrite are routed separately"
    );
    let mut plan = WritePlan::default();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut order: Vec<Action> = Vec::new();

    for (i, rec) in page.iter().enumerate() {
        let key_tuple = match extract_key(rec, &spec.key) {
            Ok(k) => k,
            Err(msg) => {
                plan.failed.push((i, msg));
                continue;
            }
        };
        let canon = canonical(&key_tuple);

        let is_delete = match spec.write_mode {
            WriteMode::Delete => true,
            WriteMode::Upsert => is_delete_marked(rec, spec.delete_marker.as_ref()),
            WriteMode::Append | WriteMode::Overwrite => false,
        };

        let action = if is_delete {
            Action::Delete(key_tuple)
        } else {
            Action::Upsert(strip_marker(rec.clone(), spec.delete_marker.as_ref()))
        };

        match index.get(&canon) {
            Some(&slot) => order[slot] = action,
            None => {
                index.insert(canon, order.len());
                order.push(action);
            }
        }
    }

    for action in order {
        match action {
            Action::Upsert(v) => plan.upserts.push(v),
            Action::Delete(k) => plan.deletes.push(k),
        }
    }
    plan
}

/// Pull the key columns out of a record in `key` order. Missing key or null
/// key value is an error.
fn extract_key(rec: &Value, key: &[String]) -> Result<KeyTuple, String> {
    let obj = rec
        .as_object()
        .ok_or_else(|| "record is not a JSON object".to_string())?;
    let mut out = Vec::with_capacity(key.len());
    for col in key {
        match obj.get(col) {
            None => return Err(format!("missing key column '{col}'")),
            Some(Value::Null) => return Err(format!("null value for key column '{col}'")),
            Some(v) => out.push((col.clone(), v.clone())),
        }
    }
    Ok(KeyTuple(out))
}

fn is_delete_marked(rec: &Value, marker: Option<&DeleteMarker>) -> bool {
    let Some(dm) = marker else { return false };
    let Some(v) = rec.get(&dm.field) else {
        return false;
    };
    let Some(s) = v.as_str() else { return false };
    dm.values.iter().any(|m| m == s)
}

fn strip_marker(mut rec: Value, marker: Option<&DeleteMarker>) -> Value {
    if let (Some(dm), Value::Object(map)) = (marker, &mut rec) {
        map.remove(&dm.field);
    }
    rec
}

/// Stable canonical string for a key tuple, for dedup.
fn canonical(k: &KeyTuple) -> String {
    let arr: Vec<&Value> = k.0.iter().map(|(_, v)| v).collect();
    serde_json::to_string(&arr).expect("a Vec<&serde_json::Value> always serializes")
}

/// Render a key tuple into a single document id (Elasticsearch `_id`).
///
/// A single-column key is rendered as its plain string / JSON form (no
/// separator can collide). A **composite** key is rendered as a canonical JSON
/// array of its values rather than a separator-join: a plain join is not
/// injective — e.g. `["a_", "b"]` and `["a", "_b"]` both collapse to `"a__b"`
/// under separator `"_"`, silently overwriting two distinct rows with one. JSON
/// encoding escapes any separator-like characters in the values, so distinct key
/// tuples always map to distinct ids.
///
/// Assumes each key column has a consistent JSON type across records (the
/// normal case for SQL and CDC sources); it does not disambiguate, e.g., the
/// integer `7` from the string `"7"` in the same column.
pub fn key_to_doc_id(k: &KeyTuple, separator: &str) -> String {
    let _ = separator; // retained for API stability; no separator can collide now
    if k.0.len() == 1 {
        return match &k.0[0].1 {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
    }
    let values: Vec<&Value> = k.0.iter().map(|(_, v)| v).collect();
    serde_json::to_string(&values).expect("a Vec<&serde_json::Value> always serializes")
}

/// Build a Mongo/ES filter document `{ col: value, … }` from a key tuple.
pub fn key_to_filter(k: &KeyTuple) -> Map<String, Value> {
    k.0.iter().map(|(c, v)| (c.clone(), v.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn upsert_spec(keys: &[&str]) -> WriteSpec {
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: keys.iter().map(|s| s.to_string()).collect(),
            delete_marker: None,
        }
    }

    #[test]
    fn upsert_extracts_key_and_keeps_row() {
        let plan = plan_writes(&[json!({"id": 1, "name": "a"})], &upsert_spec(&["id"]));
        assert_eq!(plan.upserts, vec![json!({"id": 1, "name": "a"})]);
        assert!(plan.deletes.is_empty());
        assert!(plan.failed.is_empty());
    }

    #[test]
    fn key_to_doc_id_single_key_is_plain() {
        let k = KeyTuple(vec![("id".into(), json!(7))]);
        assert_eq!(key_to_doc_id(&k, "_"), "7");
        let k = KeyTuple(vec![("name".into(), json!("alice"))]);
        assert_eq!(key_to_doc_id(&k, "_"), "alice");
    }

    #[test]
    fn key_to_doc_id_composite_is_injective() {
        // ["a_", "b"] and ["a", "_b"] must NOT collide (the F13 separator bug).
        let k1 = KeyTuple(vec![("x".into(), json!("a_")), ("y".into(), json!("b"))]);
        let k2 = KeyTuple(vec![("x".into(), json!("a")), ("y".into(), json!("_b"))]);
        let id1 = key_to_doc_id(&k1, "_");
        let id2 = key_to_doc_id(&k2, "_");
        assert_ne!(id1, id2, "distinct composite keys must map to distinct ids");
        // Mixed types also stay distinct.
        let k3 = KeyTuple(vec![("x".into(), json!(1)), ("y".into(), json!("2"))]);
        let k4 = KeyTuple(vec![("x".into(), json!("1")), ("y".into(), json!(2))]);
        assert_ne!(key_to_doc_id(&k3, "_"), key_to_doc_id(&k4, "_"));
    }

    #[test]
    fn missing_key_goes_to_failed_with_original_index() {
        let plan = plan_writes(
            &[json!({"id": 1}), json!({"name": "no-key"})],
            &upsert_spec(&["id"]),
        );
        assert_eq!(plan.upserts.len(), 1);
        assert_eq!(plan.failed.len(), 1);
        assert_eq!(plan.failed[0].0, 1, "failed row keeps its page index");
    }

    #[test]
    fn null_key_value_is_a_failure() {
        let plan = plan_writes(&[json!({"id": null})], &upsert_spec(&["id"]));
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.failed.len(), 1);
    }

    #[test]
    fn delete_marker_routes_to_deletes_and_strips_marker() {
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
        };
        let plan = plan_writes(
            &[
                json!({"id": 1, "name": "a", "__op": "u"}),
                json!({"id": 2, "__op": "d"}),
            ],
            &spec,
        );
        assert_eq!(plan.upserts, vec![json!({"id": 1, "name": "a"})]);
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.deletes[0].0, vec![("id".to_string(), json!(2))]);
    }

    #[test]
    fn last_write_wins_dedup_keeps_final_upsert() {
        let plan = plan_writes(
            &[json!({"id": 1, "v": "old"}), json!({"id": 1, "v": "new"})],
            &upsert_spec(&["id"]),
        );
        assert_eq!(plan.upserts, vec![json!({"id": 1, "v": "new"})]);
    }

    #[test]
    fn last_write_wins_delete_after_upsert_is_a_delete() {
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
        };
        let plan = plan_writes(
            &[json!({"id": 1, "__op": "u"}), json!({"id": 1, "__op": "d"})],
            &spec,
        );
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.deletes.len(), 1);
    }

    #[test]
    fn delete_mode_routes_every_row_to_deletes() {
        let spec = WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".into()],
            delete_marker: None,
        };
        let plan = plan_writes(&[json!({"id": 1}), json!({"id": 2})], &spec);
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.deletes.len(), 2);
    }

    #[test]
    fn composite_key_tuple_is_ordered() {
        let plan = plan_writes(
            &[json!({"a": 1, "b": 2, "v": 9})],
            &upsert_spec(&["a", "b"]),
        );
        assert_eq!(plan.upserts.len(), 1);
        let plan2 = plan_writes(
            &[
                json!({"a": 1, "b": 2, "v": "x"}),
                json!({"a": 1, "b": 3, "v": "y"}),
            ],
            &upsert_spec(&["a", "b"]),
        );
        assert_eq!(plan2.upserts.len(), 2, "(1,2) and (1,3) are distinct keys");
    }

    #[test]
    fn validate_rejects_upsert_without_key() {
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec![],
            delete_marker: None,
        };
        assert!(spec.validate().is_err());
    }

    #[test]
    fn validate_allows_append_without_key() {
        assert!(WriteSpec::default().validate().is_ok());
    }

    #[test]
    fn dedups_by_key_requires_keyed_upsert_or_delete() {
        assert!(!WriteSpec::default().dedups_by_key());
        let upsert = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: None,
        };
        assert!(upsert.dedups_by_key());
        let delete = WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".into()],
            delete_marker: None,
        };
        assert!(delete.dedups_by_key());
        // An (invalid) keyless upsert never claims keyed dedup.
        let keyless = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec![],
            delete_marker: None,
        };
        assert!(!keyless.dedups_by_key());
    }

    #[test]
    fn last_write_wins_upsert_after_delete_is_an_upsert() {
        // Inverse of the delete-after-upsert case: [delete, upsert] → upsert wins.
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
        };
        let plan = plan_writes(
            &[
                json!({"id": 1, "__op": "d"}),
                json!({"id": 1, "v": 9, "__op": "u"}),
            ],
            &spec,
        );
        assert!(plan.deletes.is_empty());
        assert_eq!(plan.upserts, vec![json!({"id": 1, "v": 9})]);
    }

    #[test]
    fn overwrite_mode_flags_and_needs_no_key() {
        let spec = WriteSpec {
            write_mode: WriteMode::Overwrite,
            ..Default::default()
        };
        assert!(spec.is_overwrite());
        assert!(!spec.dedups_by_key());
        // Overwrite is a whole-dataset op — no key required, so validate passes.
        assert!(spec.validate().is_ok());
        assert_eq!(WriteMode::Overwrite.as_str(), "overwrite");
        // Non-overwrite specs report false.
        assert!(!WriteSpec::default().is_overwrite());
        assert!(!upsert_spec(&["id"]).is_overwrite());
    }

    #[test]
    fn overwrite_scope_window_validates_and_renders() {
        let scope = OverwriteScope::Window {
            column: "posting_date".into(),
            from: json!("2024-06-01"),
            to: json!("2024-07-01"),
        };
        assert_eq!(scope.column(), "posting_date");
        assert!(scope.validate().is_ok());
        let whr = scope.render_where_literal("\"posting_date\"");
        assert_eq!(
            whr,
            "\"posting_date\" >= '2024-06-01' AND \"posting_date\" < '2024-07-01'"
        );

        // Empty column / null bounds are rejected.
        assert!(
            OverwriteScope::Window {
                column: " ".into(),
                from: json!(1),
                to: json!(2)
            }
            .validate()
            .is_err()
        );
        assert!(
            OverwriteScope::Window {
                column: "c".into(),
                from: json!(null),
                to: json!(2)
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn scope_window_number_bounds() {
        let scope = OverwriteScope::Window {
            column: "seq".into(),
            from: json!(100),
            to: json!(200),
        };
        assert!(scope.validate().is_ok());
        assert_eq!(
            scope.render_where_literal("`seq`"),
            "`seq` >= 100 AND `seq` < 200"
        );
    }

    #[test]
    fn sql_literal_renders_each_scalar_kind_by_the_ansi_rule() {
        // Strings: single-quoted, embedded quotes doubled — and *only* quotes,
        // since a backslash is an ordinary character under the ANSI rule.
        assert_eq!(sql_literal(&json!("plain")), "'plain'");
        assert_eq!(sql_literal(&json!("O'Brien")), "'O''Brien'");
        assert_eq!(sql_literal(&json!("a\\b")), "'a\\b'");
        // Every quote is doubled, not just the first: 5 interior quotes → 10.
        assert_eq!(sql_literal(&json!("'' ' ''")), "''''' '' '''''");
        // An empty string stays a valid (empty) literal, not bare quotes-less text.
        assert_eq!(sql_literal(&json!("")), "''");
        // Numbers and booleans pass through in canonical form.
        assert_eq!(sql_literal(&json!(42)), "42");
        assert_eq!(sql_literal(&json!(-1.5)), "-1.5");
        assert_eq!(sql_literal(&json!(true)), "true");
        assert_eq!(sql_literal(&json!(false)), "false");
        // Null and non-scalars degrade to NULL rather than injecting structure.
        assert_eq!(sql_literal(&Value::Null), "NULL");
        assert_eq!(sql_literal(&json!({"a": 1})), "NULL");
        assert_eq!(sql_literal(&json!([1, 2])), "NULL");
    }

    #[test]
    fn sql_literal_neutralises_a_quote_breakout_attempt() {
        // The whole point of the escaper: a crafted value must stay one literal.
        let out = sql_literal(&json!("x' OR 1=1 --"));
        assert_eq!(out, "'x'' OR 1=1 --'");
        // Exactly one opening and one closing quote delimit the literal: every
        // interior quote is part of a doubled pair.
        let interior = &out[1..out.len() - 1];
        assert!(
            interior.matches('\'').count().is_multiple_of(2),
            "unpaired quote inside literal: {out}"
        );
    }

    #[test]
    fn scope_literal_escapes_quotes() {
        // A crafted bound cannot break out of the string literal.
        let scope = OverwriteScope::Window {
            column: "c".into(),
            from: json!("x' OR '1'='1"),
            to: json!("z"),
        };
        let whr = scope.render_where_literal("\"c\"");
        assert!(whr.contains("'x'' OR ''1''=''1'"), "{whr}");
    }

    #[test]
    fn overwrite_deserializes_from_wire() {
        let spec: WriteSpec = serde_json::from_value(json!({"write_mode": "overwrite"})).unwrap();
        assert_eq!(spec.write_mode, WriteMode::Overwrite);
        assert!(spec.is_overwrite());
    }

    #[test]
    fn empty_page_produces_empty_plan() {
        let plan = plan_writes(&[], &upsert_spec(&["id"]));
        assert!(plan.upserts.is_empty());
        assert!(plan.deletes.is_empty());
        assert!(plan.failed.is_empty());
    }

    #[test]
    fn delete_mode_dedups_repeated_key() {
        // Same key deleted twice in one page collapses to a single delete.
        let spec = WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".into()],
            delete_marker: None,
        };
        let plan = plan_writes(&[json!({"id": 1}), json!({"id": 1})], &spec);
        assert_eq!(plan.deletes.len(), 1);
    }
}

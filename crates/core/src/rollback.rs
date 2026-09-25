//! Undo a run (#706): the shared types behind `faucet rollback`.
//!
//! A run that should not have happened — a wrong parameter, a corrupt upstream
//! extract, a bad deploy — is reverted by the sink that wrote it, one dataset at
//! a time, through the defaulted [`Sink::rollback_run`](crate::Sink::rollback_run)
//! hook. What "revert" means depends on how the run wrote:
//!
//! - **append** — delete the rows stamped with the run id (the `_faucet_run_id`
//!   metadata column, which the `rollback:` block turns on);
//! - **upsert / delete** — deleting is not enough, because the run overwrote
//!   older values; with the opt-in **before-image journal**
//!   ([`RUN_JOURNAL_TABLE`]) the sink recorded the prior row of every key it
//!   changed, in the same transaction as the page, and rollback restores it;
//! - **overwrite** — with `keep_previous`, the replaced table is kept as
//!   `<table>__faucet_prev` ([`PREVIOUS_TABLE_SUFFIX`]) and rollback swaps it back.
//!
//! The pipeline's state bookmark and exactly-once commit token are rewound by
//! the CLI from the pre-run marker it recorded, so the next run re-reads what
//! the reverted run consumed. Everything here is pure data; the sinks and the
//! CLI carry the I/O.

use serde::{Deserialize, Serialize};

/// The before-image journal table a journaling sink maintains next to its
/// data tables: `(run_id, table_name, key_json, before_json)`; a `NULL`
/// before-image means the key did not exist before the run.
pub const RUN_JOURNAL_TABLE: &str = "_faucet_run_journal";

/// Suffix of the table an overwrite run keeps its replaced contents in when
/// `keep_previous` is on.
pub const PREVIOUS_TABLE_SUFFIX: &str = "__faucet_prev";

/// Default name of the run-id column rollback keys on
/// (`{metadata prefix}_{run_id}` with the default prefix).
pub const DEFAULT_RUN_ID_COLUMN: &str = "_faucet_run_id";

/// Runtime rollback settings the executor hands a sink through its
/// [`WriteSpec`](crate::write_mode::WriteSpec) (`rollback:` field). Not user
/// config: the top-level `rollback:` block decides these, and the CLI injects
/// them per invocation.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RollbackWriteSpec {
    /// The run id this invocation writes under.
    pub run_id: String,
    /// The destination column carrying the run id (stamped by the metadata
    /// decorator).
    pub run_id_column: String,
    /// Record before-images of every upserted / deleted key.
    #[serde(default)]
    pub journal: bool,
    /// Keep the replaced table of an overwrite as `<table>__faucet_prev`.
    #[serde(default)]
    pub keep_previous: bool,
}

/// How the run being rolled back wrote, which decides the undo strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackMode {
    /// Delete the run's rows by run id.
    Append,
    /// Restore before-images from the journal.
    Upsert,
    /// Swap the kept previous table back.
    Overwrite,
}

impl RollbackMode {
    /// The undo strategy for a write mode.
    pub fn for_write_mode(mode: crate::write_mode::WriteMode) -> Self {
        match mode {
            crate::write_mode::WriteMode::Append => RollbackMode::Append,
            crate::write_mode::WriteMode::Upsert | crate::write_mode::WriteMode::Delete => {
                RollbackMode::Upsert
            }
            crate::write_mode::WriteMode::Overwrite => RollbackMode::Overwrite,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            RollbackMode::Append => "append",
            RollbackMode::Upsert => "upsert",
            RollbackMode::Overwrite => "overwrite",
        }
    }
}

/// What a sink is asked to undo.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackOptions {
    /// The destination column carrying the run id.
    pub run_id_column: String,
    pub mode: RollbackMode,
    /// Restore a key even when a later run changed it since (the row's run id
    /// no longer matches). Off by default: such a key is a **conflict** and the
    /// whole dataset is left untouched.
    pub force: bool,
    /// Count what would change without changing anything.
    pub dry_run: bool,
}

/// What a sink did (or would do) for one dataset.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RollbackOutcome {
    /// Rows deleted (append: the run's rows; upsert: keys the run created).
    pub deleted: u64,
    /// Rows restored to their before-image (upsert), or rows swapped back
    /// (overwrite).
    pub restored: u64,
    /// Keys a later run changed since; without `force` these block the whole
    /// dataset.
    pub conflicts: u64,
    /// Whether changes were applied. `false` for a dry run, or when conflicts
    /// blocked the rollback.
    pub applied: bool,
    /// A human note (e.g. why nothing was applied).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl RollbackOutcome {
    /// Nothing to undo: the run left no rows / journal / previous table.
    pub fn nothing(note: impl Into<String>) -> Self {
        Self {
            applied: true,
            note: Some(note.into()),
            ..Default::default()
        }
    }

    /// Blocked by conflicts (no change made).
    pub fn blocked(conflicts: u64) -> Self {
        Self {
            conflicts,
            applied: false,
            note: Some(format!(
                "{conflicts} key(s) were changed by a later run; pass --force to restore them anyway"
            )),
            ..Default::default()
        }
    }
}

/// The canonical JSON text of a key tuple — the journal's row identity.
/// Columns are emitted in the configured key order, so the same key always
/// serialises identically.
pub fn key_json(tuple: &crate::write_mode::KeyTuple) -> String {
    // Built by hand rather than through a `Map`: the map's key order depends
    // on the `preserve_order` feature, and this text is a primary key.
    let parts: Vec<String> = tuple
        .0
        .iter()
        .map(|(k, v)| format!("{}:{}", serde_json::Value::String(k.clone()), v))
        .collect();
    format!("{{{}}}", parts.join(","))
}

/// A key tuple with every scalar rendered as its text form, so a key read
/// back from the database (`7`) and the same key in a source record (`"7"`)
/// journal identically. Nulls stay null; the SQL sinks bind the text through
/// the key column's own type, so the restore addresses the right row.
pub fn canonical_key(tuple: &crate::write_mode::KeyTuple) -> crate::write_mode::KeyTuple {
    crate::write_mode::KeyTuple(
        tuple
            .0
            .iter()
            .map(|(k, v)| {
                let v = match v {
                    serde_json::Value::Null => serde_json::Value::Null,
                    serde_json::Value::String(s) => serde_json::Value::String(s.clone()),
                    serde_json::Value::Bool(b) => serde_json::Value::String(b.to_string()),
                    serde_json::Value::Number(n) => serde_json::Value::String(n.to_string()),
                    other => serde_json::Value::String(other.to_string()),
                };
                (k.clone(), v)
            })
            .collect(),
    )
}

/// Every key a planned page touches — upserts and deletes — as canonical
/// tuples, deduplicated, in page order. What a sink journals before it
/// applies the plan.
pub fn plan_keys(
    plan: &crate::write_mode::WritePlan,
    key: &[String],
) -> Vec<crate::write_mode::KeyTuple> {
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(plan.upserts.len() + plan.deletes.len());
    let upserts = plan
        .upserts
        .iter()
        .filter_map(|r| crate::write_mode::record_key(r, key));
    for t in upserts.chain(plan.deletes.iter().cloned()) {
        let c = canonical_key(&t);
        if seen.insert(key_json(&c)) {
            out.push(c);
        }
    }
    out
}

/// Dialect-neutral SQL for the before-image journal and the run-scoped delete.
///
/// Each builder takes the dialect's identifier quoting and placeholder style
/// as function pointers (plain `fn`, so a sink can hold one across an await
/// in a `Send` future), so the SQL sinks share one tested generator instead of three
/// hand-written copies. `before_type` is the dialect's JSON-or-text column type
/// (`JSONB`, `TEXT`, `JSON`); `insert_ignore` is its "skip an existing key"
/// spelling (`INSERT … ON CONFLICT DO NOTHING`, `INSERT OR IGNORE`,
/// `INSERT IGNORE`).
#[derive(Clone, Copy)]
pub struct JournalSql {
    pub quote: fn(&str) -> String,
    pub placeholder: fn(usize) -> String,
    pub before_type: &'static str,
    pub insert_prefix: &'static str,
    pub insert_suffix: &'static str,
    pub now: &'static str,
    /// The `key_json` column definition (plus any helper column). A dialect
    /// with a byte-limited index (MySQL: 3072 bytes) keys on a stored hash of
    /// the JSON instead of the text itself.
    pub key_column: &'static str,
    /// The primary-key clause.
    pub primary_key: &'static str,
}

/// The `key_json` definition for dialects that can index the text directly.
pub const KEY_COLUMN_TEXT: &str = "key_json TEXT NOT NULL";
/// The primary key over the JSON text itself.
pub const PRIMARY_KEY_TEXT: &str = "PRIMARY KEY (run_id, table_name, key_json)";

impl JournalSql {
    fn t(&self) -> String {
        (self.quote)(RUN_JOURNAL_TABLE)
    }

    /// `CREATE TABLE IF NOT EXISTS _faucet_run_journal (…)`.
    pub fn create(&self) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {t} (run_id VARCHAR(64) NOT NULL, table_name VARCHAR(255) NOT NULL, \
             {key}, before_json {bt}, recorded_at TIMESTAMP DEFAULT {now}, {pk})",
            t = self.t(),
            key = self.key_column,
            bt = self.before_type,
            now = self.now,
            pk = self.primary_key,
        )
    }

    /// Insert `rows` journal rows of `(run_id, table_name, key_json,
    /// before_json)`, skipping keys already journaled for the run — the first
    /// page to touch a key holds its true pre-run image.
    pub fn insert(&self, rows: usize) -> String {
        let mut n = 0usize;
        let tuples: Vec<String> = (0..rows)
            .map(|_| {
                let ph: Vec<String> = (0..4)
                    .map(|_| {
                        n += 1;
                        (self.placeholder)(n)
                    })
                    .collect();
                format!("({})", ph.join(", "))
            })
            .collect();
        format!(
            "{prefix} INTO {t} (run_id, table_name, key_json, before_json) VALUES {v}{suffix}",
            prefix = self.insert_prefix,
            t = self.t(),
            v = tuples.join(", "),
            suffix = self.insert_suffix,
        )
    }

    /// Every journal row of one run against one table.
    pub fn select(&self) -> String {
        format!(
            "SELECT key_json, before_json FROM {t} WHERE run_id = {p1} AND table_name = {p2}",
            t = self.t(),
            p1 = (self.placeholder)(1),
            p2 = (self.placeholder)(2),
        )
    }

    /// Drop one run's journal rows for one table (after a restore).
    pub fn delete_table(&self) -> String {
        format!(
            "DELETE FROM {t} WHERE run_id = {p1} AND table_name = {p2}",
            t = self.t(),
            p1 = (self.placeholder)(1),
            p2 = (self.placeholder)(2),
        )
    }

    /// `(k1, k2) IN ((p, p), (p, p), …)` over `tuples` key tuples, placeholders
    /// numbered from `start + 1`. Returns the predicate and the next free
    /// placeholder index.
    pub fn keys_in(&self, key: &[String], tuples: usize, start: usize) -> (String, usize) {
        let cols: Vec<String> = key.iter().map(|k| (self.quote)(k)).collect();
        let mut n = start;
        let groups: Vec<String> = (0..tuples)
            .map(|_| {
                let ph: Vec<String> = key
                    .iter()
                    .map(|_| {
                        n += 1;
                        (self.placeholder)(n)
                    })
                    .collect();
                format!("({})", ph.join(", "))
            })
            .collect();
        (
            format!("({}) IN ({})", cols.join(", "), groups.join(", ")),
            n,
        )
    }

    /// `DELETE FROM <table> WHERE <run_id_col> = p1` — the append undo.
    pub fn delete_by_run(&self, table_ref: &str, run_id_col: &str) -> String {
        format!(
            "DELETE FROM {table_ref} WHERE {c} = {p}",
            c = (self.quote)(run_id_col),
            p = (self.placeholder)(1)
        )
    }

    /// `SELECT count(*) FROM <table> WHERE <run_id_col> = p1`.
    pub fn count_by_run(&self, table_ref: &str, run_id_col: &str) -> String {
        format!(
            "SELECT count(*) FROM {table_ref} WHERE {c} = {p}",
            c = (self.quote)(run_id_col),
            p = (self.placeholder)(1)
        )
    }
}

/// One journaled key, decoded: the key object and its before-image (`None`
/// when the key did not exist before the run).
#[derive(Debug, Clone, PartialEq)]
pub struct JournalEntry {
    pub key: serde_json::Map<String, serde_json::Value>,
    pub before: Option<serde_json::Value>,
}

impl JournalEntry {
    /// Decode a journal row's two text columns.
    pub fn decode(key_json: &str, before_json: Option<&str>) -> Result<Self, crate::FaucetError> {
        let key: serde_json::Value = serde_json::from_str(key_json).map_err(|e| {
            crate::FaucetError::Sink(format!("rollback: malformed journal key {key_json:?}: {e}"))
        })?;
        let key = match key {
            serde_json::Value::Object(m) => m,
            other => {
                return Err(crate::FaucetError::Sink(format!(
                    "rollback: journal key is not an object: {other}"
                )));
            }
        };
        let before = match before_json {
            None => None,
            Some(b) => Some(serde_json::from_str(b).map_err(|e| {
                crate::FaucetError::Sink(format!("rollback: malformed before-image: {e}"))
            })?),
        };
        Ok(Self { key, before })
    }

    /// The key as a [`KeyTuple`](crate::write_mode::KeyTuple) in `key` order.
    pub fn tuple(&self, key: &[String]) -> crate::write_mode::KeyTuple {
        crate::write_mode::KeyTuple(
            key.iter()
                .map(|k| {
                    (
                        k.clone(),
                        self.key.get(k).cloned().unwrap_or(serde_json::Value::Null),
                    )
                })
                .collect(),
        )
    }
}

/// Split journal entries into what a restore does: keys to **delete** (the
/// run created them) and before-images to **write back** (the run changed or
/// deleted them). Pure.
pub fn plan_restore(entries: &[JournalEntry]) -> (Vec<&JournalEntry>, Vec<serde_json::Value>) {
    let mut deletes = Vec::new();
    let mut restores = Vec::new();
    for e in entries {
        match &e.before {
            None => deletes.push(e),
            Some(b) => restores.push(b.clone()),
        }
    }
    (deletes, restores)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write_mode::WriteMode;

    fn q(s: &str) -> String {
        format!("\"{s}\"")
    }
    fn ph(n: usize) -> String {
        format!("${n}")
    }
    fn sql() -> JournalSql {
        JournalSql {
            quote: q,
            placeholder: ph,
            before_type: "JSONB",
            insert_prefix: "INSERT",
            insert_suffix: " ON CONFLICT DO NOTHING",
            now: "now()",
            key_column: KEY_COLUMN_TEXT,
            primary_key: PRIMARY_KEY_TEXT,
        }
    }

    #[test]
    fn journal_sql_shapes() {
        let j = sql();
        assert!(
            j.create()
                .starts_with("CREATE TABLE IF NOT EXISTS \"_faucet_run_journal\"")
        );
        assert!(j.create().contains("before_json JSONB"));
        assert!(j.create().contains("key_json TEXT NOT NULL"));
        assert!(
            j.create()
                .ends_with("PRIMARY KEY (run_id, table_name, key_json))"),
            "{}",
            j.create()
        );
        let ins = j.insert(2);
        assert!(
            ins.starts_with("INSERT INTO \"_faucet_run_journal\""),
            "{ins}"
        );
        assert!(ins.contains("($1, $2, $3, $4), ($5, $6, $7, $8)"), "{ins}");
        assert!(ins.ends_with(" ON CONFLICT DO NOTHING"));
        assert_eq!(
            j.select(),
            "SELECT key_json, before_json FROM \"_faucet_run_journal\" WHERE run_id = $1 AND table_name = $2"
        );
        assert!(
            j.delete_table()
                .starts_with("DELETE FROM \"_faucet_run_journal\" WHERE run_id = $1")
        );
        let (pred, next) = j.keys_in(&["a".into(), "b".into()], 2, 1);
        assert_eq!(pred, "(\"a\", \"b\") IN (($2, $3), ($4, $5))");
        assert_eq!(next, 5);
        assert_eq!(
            j.delete_by_run("\"t\"", "_faucet_run_id"),
            "DELETE FROM \"t\" WHERE \"_faucet_run_id\" = $1"
        );
        assert_eq!(
            j.count_by_run("\"t\"", "_faucet_run_id"),
            "SELECT count(*) FROM \"t\" WHERE \"_faucet_run_id\" = $1"
        );
    }

    #[test]
    fn journal_entries_decode_and_plan() {
        let created = JournalEntry::decode("{\"id\":1}", None).unwrap();
        let changed = JournalEntry::decode("{\"id\":2}", Some("{\"id\":2,\"v\":\"old\"}")).unwrap();
        assert_eq!(
            created.tuple(&["id".into()]).0,
            vec![("id".to_string(), serde_json::json!(1))]
        );
        assert_eq!(
            created.tuple(&["id".into(), "missing".into()]).0[1],
            ("missing".to_string(), serde_json::Value::Null)
        );
        let entries = [created.clone(), changed.clone()];
        let (deletes, restores) = plan_restore(&entries);
        assert_eq!(deletes, vec![&created]);
        assert_eq!(restores, vec![serde_json::json!({"id": 2, "v": "old"})]);
        assert!(JournalEntry::decode("nope", None).is_err());
        assert!(JournalEntry::decode("[1]", None).is_err());
        assert!(JournalEntry::decode("{\"id\":1}", Some("{broken")).is_err());
        let kt = crate::write_mode::KeyTuple(vec![
            ("b".into(), serde_json::json!(2)),
            ("a".into(), serde_json::json!("x")),
        ]);
        assert_eq!(
            key_json(&kt),
            "{\"b\":2,\"a\":\"x\"}",
            "configured key order, not sorted"
        );
    }

    #[test]
    fn plan_keys_are_canonical_and_deduplicated() {
        let spec = crate::write_mode::WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            ..Default::default()
        };
        let page = vec![
            serde_json::json!({"id": 7, "v": 1}),
            serde_json::json!({"id": "7", "v": 2}),
            serde_json::json!({"id": true}),
            serde_json::json!({"id": null}),
        ];
        let plan = crate::write_mode::plan_writes(&page, &spec);
        let keys = plan_keys(&plan, &spec.key);
        // 7 and "7" journal as one key; the null-key row is a plan failure.
        assert_eq!(keys.len(), 2);
        assert_eq!(key_json(&keys[0]), "{\"id\":\"7\"}");
        assert_eq!(key_json(&keys[1]), "{\"id\":\"true\"}");
        let nested = canonical_key(&crate::write_mode::KeyTuple(vec![
            ("k".into(), serde_json::json!([1])),
            ("n".into(), serde_json::Value::Null),
        ]));
        assert_eq!(nested.0[0].1, serde_json::json!("[1]"));
        assert_eq!(nested.0[1].1, serde_json::Value::Null);
        // Deletes are journaled too.
        let del = crate::write_mode::WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".into()],
            ..Default::default()
        };
        let plan = crate::write_mode::plan_writes(&[serde_json::json!({"id": 3})], &del);
        assert_eq!(key_json(&plan_keys(&plan, &del.key)[0]), "{\"id\":\"3\"}");
    }

    #[test]
    fn mode_follows_the_write_mode() {
        assert_eq!(
            RollbackMode::for_write_mode(WriteMode::Append),
            RollbackMode::Append
        );
        assert_eq!(
            RollbackMode::for_write_mode(WriteMode::Upsert),
            RollbackMode::Upsert
        );
        assert_eq!(
            RollbackMode::for_write_mode(WriteMode::Delete),
            RollbackMode::Upsert
        );
        assert_eq!(
            RollbackMode::for_write_mode(WriteMode::Overwrite),
            RollbackMode::Overwrite
        );
        assert_eq!(RollbackMode::Upsert.as_str(), "upsert");
        assert_eq!(
            serde_json::to_string(&RollbackMode::Overwrite).unwrap(),
            "\"overwrite\""
        );
    }

    #[test]
    fn outcomes_carry_their_reason() {
        let n = RollbackOutcome::nothing("no rows");
        assert!(n.applied && n.deleted == 0);
        let b = RollbackOutcome::blocked(3);
        assert!(!b.applied && b.conflicts == 3);
        assert!(b.note.unwrap().contains("--force"));
        let spec: RollbackWriteSpec = serde_json::from_value(serde_json::json!({
            "run_id": "r1", "run_id_column": "_faucet_run_id"
        }))
        .unwrap();
        assert!(!spec.journal && !spec.keep_previous);
        assert!(
            serde_json::from_value::<RollbackWriteSpec>(serde_json::json!({
                "run_id": "r1", "run_id_column": "c", "bogus": 1
            }))
            .is_err()
        );
    }
}

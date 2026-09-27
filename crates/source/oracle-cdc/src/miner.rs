//! Transaction assembly over LogMiner rows. Pure.
//!
//! Rows arrive in SCN order without `COMMITTED_DATA_ONLY`, so changes are
//! buffered per transaction and released only when its `COMMIT` arrives; a
//! `ROLLBACK` discards the buffer. Within a transaction, the follow-up
//! `UPDATE` / `LOB_WRITE` rows LogMiner emits for LOB columns are folded into
//! the row's earlier change, and savepoint-rollback undo rows remove the
//! change they undo.

use std::collections::HashMap;

use faucet_common_oracle::{TypeFamily, normalize_datetime_text, typed_text_to_json};
use faucet_core::FaucetError;
use serde_json::{Map, Value, json};

use crate::config::OnUnsupported;
use crate::redo::{DmlKind, Image, LobChange, Resolved, apply_lob, parse_dml, parse_lob_change};

/// `V$LOGMNR_CONTENTS.OPERATION_CODE` values the miner acts on.
pub mod op {
    /// `INSERT`.
    pub const INSERT: i64 = 1;
    /// `DELETE`.
    pub const DELETE: i64 = 2;
    /// `UPDATE`.
    pub const UPDATE: i64 = 3;
    /// `DDL`.
    pub const DDL: i64 = 5;
    /// `START`.
    pub const START: i64 = 6;
    /// `COMMIT`.
    pub const COMMIT: i64 = 7;
    /// `LOB_WRITE`.
    pub const LOB_WRITE: i64 = 10;
    /// `LOB_TRIM`.
    pub const LOB_TRIM: i64 = 11;
    /// `ROLLBACK`.
    pub const ROLLBACK: i64 = 36;
    /// `UNSUPPORTED`.
    pub const UNSUPPORTED: i64 = 255;
}

/// One (continuation-assembled) `V$LOGMNR_CONTENTS` row.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogRow {
    /// `SCN`.
    pub scn: u64,
    /// `OPERATION_CODE`.
    pub op: i64,
    /// `RAWTOHEX(XID)`.
    pub xid: String,
    /// `SEG_OWNER`.
    pub owner: Option<String>,
    /// `TABLE_NAME`.
    pub table: Option<String>,
    /// `SQL_REDO`, with `CSF` continuation rows joined.
    pub redo: String,
    /// `ROW_ID`.
    pub row_id: Option<String>,
    /// `ROLLBACK = 1`: the row undoes an earlier change (savepoint rollback).
    pub rollback: bool,
    /// `TIMESTAMP`, rendered by the session NLS format.
    pub timestamp: Option<String>,
    /// `INFO`.
    pub info: Option<String>,
}

/// Joins `CSF = 1` continuation rows (a `SQL_REDO` longer than 4000 bytes is
/// split across consecutive rows) into one [`LogRow`].
#[derive(Debug, Default)]
pub struct Assembler {
    pending: Option<LogRow>,
}

impl Assembler {
    /// Feed a raw row; returns the completed row once its last piece arrives.
    pub fn push(&mut self, row: LogRow, continues: bool) -> Option<LogRow> {
        let row = match self.pending.take() {
            Some(mut head) => {
                head.redo.push_str(&row.redo);
                head
            }
            None => row,
        };
        if continues {
            self.pending = Some(row);
            None
        } else {
            Some(row)
        }
    }
}

/// Column type families per `(owner, table)`, for typing redo literals.
pub type TableMeta = HashMap<(String, String), HashMap<String, TypeFamily>>;

/// The kind of a captured change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventOp {
    /// Row inserted.
    Insert,
    /// Row updated.
    Update,
    /// Row deleted.
    Delete,
    /// DDL on a captured table.
    Ddl,
    /// `TRUNCATE` of a captured table.
    Truncate,
}

impl EventOp {
    fn code(self) -> &'static str {
        match self {
            EventOp::Insert => "i",
            EventOp::Update => "u",
            EventOp::Delete => "d",
            EventOp::Ddl => "ddl",
            EventOp::Truncate => "truncate",
        }
    }
}

/// One change inside a transaction.
#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    /// Identity within the miner (for savepoint undo).
    pub id: u64,
    /// Kind.
    pub op: EventOp,
    /// Owner.
    pub owner: String,
    /// Table.
    pub table: String,
    /// Before image (`update` / `delete`).
    pub before: Option<Image>,
    /// After image (`insert` / `update`).
    pub after: Option<Image>,
    /// `ROW_ID`, for folding follow-up rows.
    pub row_id: Option<String>,
    /// SCN of the change.
    pub scn: u64,
    /// Statement text (`ddl` / `truncate`).
    pub sql: Option<String>,
}

/// A committed transaction ready to emit.
#[derive(Debug, Clone, PartialEq)]
pub struct Committed {
    /// Transaction id.
    pub xid: String,
    /// Commit SCN.
    pub commit_scn: u64,
    /// Commit timestamp.
    pub timestamp: Option<String>,
    /// Captured changes, in order.
    pub events: Vec<Event>,
}

/// How one redo row changed the transaction's events, so a savepoint
/// rollback's undo row can revert exactly that step.
#[derive(Debug, Clone)]
enum PieceKind {
    Created(DmlKind),
    Folded { prev_after: Image },
}

#[derive(Debug, Clone)]
struct Piece {
    event_id: u64,
    row_id: Option<String>,
    kind: PieceKind,
}

#[derive(Debug, Default)]
struct Txn {
    first_scn: u64,
    events: Vec<Event>,
    pieces: Vec<Piece>,
}

impl Txn {
    /// Revert the latest change of `original` kind on row `rid`.
    fn undo(&mut self, rid: &str, original: DmlKind) {
        let events = &self.events;
        let targets_row = |p: &Piece| {
            p.row_id.as_deref() == Some(rid)
                || events
                    .iter()
                    .any(|e| e.id == p.event_id && e.row_id.as_deref() == Some(rid))
        };
        let compatible = |p: &Piece| match (&p.kind, original) {
            (PieceKind::Created(k), o) => *k == o,
            (PieceKind::Folded { .. }, DmlKind::Update) => true,
            _ => false,
        };
        let Some(i) = self
            .pieces
            .iter()
            .rposition(|p| targets_row(p) && compatible(p))
        else {
            return;
        };
        let piece = self.pieces.remove(i);
        match piece.kind {
            PieceKind::Created(_) => {
                self.events.retain(|e| e.id != piece.event_id);
                self.pieces.retain(|p| p.event_id != piece.event_id);
            }
            PieceKind::Folded { prev_after } => {
                if let Some(e) = self.events.iter_mut().find(|e| e.id == piece.event_id) {
                    e.after = Some(prev_after);
                }
            }
        }
    }
}

/// Buffers in-flight transactions.
#[derive(Debug)]
pub struct Miner {
    open: HashMap<String, Txn>,
    on_unsupported: OnUnsupported,
    max_staged: Option<usize>,
    ddl_seen: Vec<(String, String)>,
    next_id: u64,
}

fn valid_row_id(r: &Option<String>) -> Option<&str> {
    r.as_deref()
        .filter(|s| !s.is_empty() && !s.chars().all(|c| c == 'A'))
}

fn overlay(base: &mut Image, set: &Image) {
    for (col, v) in set {
        match base.iter_mut().find(|(c, _)| c == col) {
            Some(slot) => slot.1 = v.clone(),
            None => base.push((col.clone(), v.clone())),
        }
    }
}

fn matches(image: &Image, conditions: &Image) -> bool {
    conditions
        .iter()
        .all(|(c, v)| image.iter().any(|(ic, iv)| ic == c && iv == v))
}

impl Miner {
    /// A miner with the given policies.
    pub fn new(on_unsupported: OnUnsupported, max_staged: Option<usize>) -> Self {
        Self {
            open: HashMap::new(),
            on_unsupported,
            max_staged,
            ddl_seen: Vec::new(),
            next_id: 0,
        }
    }

    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// First SCN of the oldest transaction still buffered.
    pub fn oldest_open(&self) -> Option<u64> {
        self.open.values().map(|t| t.first_scn).min()
    }

    /// Tables whose DDL was seen since the last call (their column metadata
    /// must be reloaded).
    pub fn take_ddl_tables(&mut self) -> Vec<(String, String)> {
        std::mem::take(&mut self.ddl_seen)
    }

    fn txn(&mut self, xid: &str, scn: u64) -> &mut Txn {
        self.open.entry(xid.to_string()).or_insert_with(|| Txn {
            first_scn: scn,
            ..Txn::default()
        })
    }

    fn unsupported(&self, row: &LogRow, reason: &str) -> Result<Option<Committed>, FaucetError> {
        let table = format!(
            "{}.{}",
            row.owner.as_deref().unwrap_or("?"),
            row.table.as_deref().unwrap_or("?")
        );
        match self.on_unsupported {
            OnUnsupported::Fail => Err(FaucetError::Source(format!(
                "oracle-cdc: cannot decode the change at SCN {} on {table}: {reason}. A \
                 dictionary mismatch means the table's structure changed after this redo was \
                 written (mining uses the current dictionary); re-snapshot the table and restart \
                 capture, or set `on_unsupported: skip` to drop such changes",
                row.scn
            ))),
            OnUnsupported::Skip => {
                tracing::warn!(scn = row.scn, table = %table, reason, "oracle-cdc: skipping undecodable change");
                Ok(None)
            }
        }
    }

    fn check_staged(&self, xid: &str) -> Result<(), FaucetError> {
        if let (Some(max), Some(t)) = (self.max_staged, self.open.get(xid))
            && t.events.len() > max
        {
            return Err(FaucetError::Source(format!(
                "oracle-cdc: transaction {xid} exceeded max_staged_records ({max}); raise the \
                 limit or split the source transaction"
            )));
        }
        Ok(())
    }

    /// Feed one row; returns a transaction when this row committed it.
    pub fn apply(&mut self, row: &LogRow) -> Result<Option<Committed>, FaucetError> {
        match row.op {
            op::START => {
                self.txn(&row.xid, row.scn);
                Ok(None)
            }
            op::COMMIT => Ok(self.open.remove(&row.xid).map(|t| Committed {
                xid: row.xid.clone(),
                commit_scn: row.scn,
                timestamp: row.timestamp.clone(),
                events: t.events,
            })),
            op::ROLLBACK => {
                self.open.remove(&row.xid);
                Ok(None)
            }
            op::INSERT | op::DELETE | op::UPDATE => self.dml(row),
            op::DDL => {
                let (owner, table) = (
                    row.owner.clone().unwrap_or_default(),
                    row.table.clone().unwrap_or_default(),
                );
                let kind = if row
                    .redo
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("truncate")
                {
                    EventOp::Truncate
                } else {
                    EventOp::Ddl
                };
                self.ddl_seen.push((owner.clone(), table.clone()));
                let event = Event {
                    id: self.id(),
                    op: kind,
                    owner,
                    table,
                    before: None,
                    after: None,
                    row_id: None,
                    scn: row.scn,
                    sql: Some(row.redo.trim().to_string()),
                };
                self.txn(&row.xid, row.scn).events.push(event);
                self.check_staged(&row.xid)?;
                Ok(None)
            }
            op::LOB_WRITE | op::LOB_TRIM => match parse_lob_change(&row.redo) {
                Ok(change) if !is_mismatch(row) => {
                    self.lob(row, change);
                    self.check_staged(&row.xid)?;
                    Ok(None)
                }
                Ok(_) => self.unsupported(row, "dictionary mismatch"),
                Err(e) => self.unsupported(row, &e),
            },
            op::UNSUPPORTED => {
                let info = row
                    .info
                    .clone()
                    .unwrap_or_else(|| "unsupported operation".into());
                self.unsupported(row, &info)
            }
            _ => Ok(None),
        }
    }

    fn dml(&mut self, row: &LogRow) -> Result<Option<Committed>, FaucetError> {
        if row.rollback {
            let original = match row.op {
                op::DELETE => DmlKind::Insert,
                op::INSERT => DmlKind::Delete,
                _ => DmlKind::Update,
            };
            if let Some(rid) = valid_row_id(&row.row_id).map(str::to_string) {
                self.txn(&row.xid, row.scn).undo(&rid, original);
            }
            return Ok(None);
        }
        if is_mismatch(row) {
            return self.unsupported(row, "dictionary mismatch");
        }
        let dml = match parse_dml(&row.redo) {
            Ok(d) => d,
            Err(e) => return self.unsupported(row, &e),
        };
        if dml.kind != DmlKind::Insert && dml.conditions.is_empty() {
            return self.unsupported(
                row,
                "no key columns are logged for this row — enable supplemental logging for the \
                 table (ALTER TABLE <table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS)",
            );
        }
        let row_id = valid_row_id(&row.row_id).map(str::to_string);
        let id = self.id();
        let txn = self.txn(&row.xid, row.scn);
        if dml.kind == DmlKind::Update
            && let Some(prev) = txn.events.iter_mut().rev().find(|e| {
                matches!(e.op, EventOp::Insert | EventOp::Update)
                    && e.owner == dml.owner
                    && e.table == dml.table
                    && same_row(e, row_id.as_deref(), &dml.conditions)
            })
        {
            let after = prev.after.get_or_insert_with(Vec::new);
            let prev_after = after.clone();
            overlay(after, &dml.set);
            if prev.row_id.is_none() {
                prev.row_id.clone_from(&row_id);
            }
            txn.pieces.push(Piece {
                event_id: prev.id,
                row_id,
                kind: PieceKind::Folded { prev_after },
            });
            return Ok(None);
        }
        let base = base_event(id, &dml.owner, &dml.table, row_id.clone(), row.scn);
        let event = match dml.kind {
            DmlKind::Insert => Event {
                op: EventOp::Insert,
                after: Some(dml.set),
                ..base
            },
            DmlKind::Update => {
                let mut after = dml.conditions.clone();
                overlay(&mut after, &dml.set);
                Event {
                    op: EventOp::Update,
                    before: Some(dml.conditions),
                    after: Some(after),
                    ..base
                }
            }
            DmlKind::Delete => Event {
                op: EventOp::Delete,
                before: Some(dml.conditions),
                ..base
            },
        };
        txn.pieces.push(Piece {
            event_id: id,
            row_id,
            kind: PieceKind::Created(dml.kind),
        });
        txn.events.push(event);
        self.check_staged(&row.xid)?;
        Ok(None)
    }

    fn lob(&mut self, row: &LogRow, change: LobChange) {
        let id = self.id();
        let txn = self.txn(&row.xid, row.scn);
        let target = txn.events.iter_mut().rev().find(|e| {
            matches!(e.op, EventOp::Insert | EventOp::Update)
                && e.owner == change.owner
                && e.table == change.table
                && e.after
                    .as_ref()
                    .is_some_and(|a| matches(a, &change.conditions))
        });
        match target {
            Some(e) => {
                let after = e.after.get_or_insert_with(Vec::new);
                let current = after
                    .iter()
                    .find(|(c, _)| c == &change.column)
                    .map(|(_, v)| v.clone())
                    .unwrap_or(Resolved::Null);
                overlay(
                    after,
                    &vec![(change.column.clone(), apply_lob(&current, &change))],
                );
            }
            None => {
                let mut after = change.conditions.clone();
                overlay(
                    &mut after,
                    &vec![(change.column.clone(), apply_lob(&Resolved::Null, &change))],
                );
                txn.events.push(Event {
                    op: EventOp::Update,
                    before: Some(change.conditions.clone()),
                    after: Some(after),
                    ..base_event(id, &change.owner, &change.table, None, row.scn)
                });
            }
        }
    }
}

/// Whether an update addresses the row an earlier change in the same
/// transaction produced: by `ROW_ID` when both carry one, else by its `where`
/// clause matching that change's after image (inserts carry no `ROW_ID`).
fn same_row(prev: &Event, row_id: Option<&str>, conditions: &Image) -> bool {
    match (prev.row_id.as_deref(), row_id) {
        (Some(a), Some(b)) => a == b,
        _ => !conditions.is_empty() && prev.after.as_ref().is_some_and(|a| matches(a, conditions)),
    }
}

fn base_event(id: u64, owner: &str, table: &str, row_id: Option<String>, scn: u64) -> Event {
    Event {
        id,
        op: EventOp::Insert,
        owner: owner.to_string(),
        table: table.to_string(),
        before: None,
        after: None,
        row_id,
        scn,
        sql: None,
    }
}

fn is_mismatch(row: &LogRow) -> bool {
    row.info.as_deref().is_some_and(|i| {
        i.to_ascii_lowercase().contains("dictionary") && i.to_ascii_lowercase().contains("mismatch")
    })
}

/// Shape an image as JSON, typing each literal by its column's family.
pub fn image_to_json(image: &Image, families: Option<&HashMap<String, TypeFamily>>) -> Value {
    let mut out = Map::with_capacity(image.len());
    for (col, v) in image {
        let family = families
            .and_then(|f| f.get(col))
            .copied()
            .unwrap_or(TypeFamily::Other);
        let json = match v {
            Resolved::Null | Resolved::EmptyLob => Value::Null,
            Resolved::Text(t) => typed_text_to_json(t, family),
            Resolved::Bytes(b) => match family {
                TypeFamily::Clob | TypeFamily::Text | TypeFamily::NationalText => {
                    Value::String(String::from_utf8_lossy(b).into_owned())
                }
                _ => {
                    use base64::Engine;
                    Value::String(base64::engine::general_purpose::STANDARD.encode(b))
                }
            },
        };
        out.insert(col.clone(), json);
    }
    Value::Object(out)
}

/// The change envelope for one event (`cdc_unwrap`-compatible).
pub fn to_envelope(event: &Event, txn: &Committed, meta: &TableMeta) -> Value {
    let families = meta.get(&(event.owner.clone(), event.table.clone()));
    let img = |i: &Option<Image>| {
        i.as_ref()
            .map_or(Value::Null, |i| image_to_json(i, families))
    };
    let mut env = json!({
        "op": event.op.code(),
        "schema": event.owner,
        "table": event.table,
        "before": img(&event.before),
        "after": img(&event.after),
        "scn": event.scn,
        "commit_scn": txn.commit_scn,
        "xid": txn.xid,
        "ts": txn.timestamp.as_deref().map(normalize_datetime_text),
    });
    if let Some(sql) = &event.sql {
        env["sql"] = Value::String(sql.clone());
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(scn: u64, op: i64, xid: &str, redo: &str) -> LogRow {
        LogRow {
            scn,
            op,
            xid: xid.into(),
            owner: Some("APP".into()),
            table: Some("T".into()),
            redo: redo.into(),
            row_id: Some(format!("ROW{scn}")),
            ..Default::default()
        }
    }

    fn miner() -> Miner {
        Miner::new(OnUnsupported::Fail, None)
    }

    #[test]
    fn continuation_rows_are_joined() {
        let mut a = Assembler::default();
        assert!(
            a.push(
                row(
                    1,
                    op::INSERT,
                    "X",
                    "insert into \"A\".\"T\"(\"C\") values ('ab"
                ),
                true
            )
            .is_none()
        );
        assert!(a.push(row(1, op::INSERT, "X", "cd"), true).is_none());
        let done = a.push(row(1, op::INSERT, "X", "ef')"), false).unwrap();
        assert!(done.redo.ends_with("'abcdef')"), "{}", done.redo);
        let single = a.push(row(2, op::COMMIT, "X", "commit"), false).unwrap();
        assert_eq!(single.redo, "commit");
    }

    #[test]
    fn commit_releases_buffered_changes_in_order() {
        let mut m = miner();
        assert!(
            m.apply(&row(10, op::START, "X", "set transaction read write"))
                .unwrap()
                .is_none()
        );
        m.apply(&row(
            11,
            op::INSERT,
            "X",
            r#"insert into "APP"."T"("ID","V") values ('1','a')"#,
        ))
        .unwrap();
        m.apply(&row(
            12,
            op::UPDATE,
            "X",
            r#"update "APP"."T" set "V" = 'b' where "ID" = '2' and "V" = 'z'"#,
        ))
        .unwrap();
        m.apply(&row(
            13,
            op::DELETE,
            "X",
            r#"delete from "APP"."T" where "ID" = '3'"#,
        ))
        .unwrap();
        assert_eq!(m.oldest_open(), Some(10));
        let mut commit = row(14, op::COMMIT, "X", "commit");
        commit.timestamp = Some("2024-01-02 03:04:05".into());
        let c = m.apply(&commit).unwrap().unwrap();
        assert_eq!(m.oldest_open(), None);
        assert_eq!(c.commit_scn, 14);
        let ops: Vec<EventOp> = c.events.iter().map(|e| e.op).collect();
        assert_eq!(ops, vec![EventOp::Insert, EventOp::Update, EventOp::Delete]);
        let upd = &c.events[1];
        assert_eq!(
            upd.before.as_ref().unwrap()[1].1,
            Resolved::Text("z".into())
        );
        assert_eq!(upd.after.as_ref().unwrap()[1].1, Resolved::Text("b".into()));

        let meta: TableMeta = HashMap::from([(
            ("APP".into(), "T".into()),
            HashMap::from([("ID".to_string(), TypeFamily::Integer)]),
        )]);
        let env = to_envelope(&c.events[0], &c, &meta);
        assert_eq!(env["op"], "i");
        assert_eq!(env["after"], json!({"ID": 1, "V": "a"}));
        assert_eq!(env["before"], Value::Null);
        assert_eq!(env["commit_scn"], 14);
        assert_eq!(env["ts"], "2024-01-02T03:04:05");
        let del = to_envelope(&c.events[2], &c, &meta);
        assert_eq!(del["op"], "d");
        assert_eq!(del["before"], json!({"ID": 3}));
    }

    #[test]
    fn rollback_discards_and_unknown_commit_is_empty() {
        let mut m = miner();
        m.apply(&row(
            1,
            op::INSERT,
            "Y",
            r#"insert into "APP"."T"("ID") values ('1')"#,
        ))
        .unwrap();
        assert_eq!(m.oldest_open(), Some(1), "a DML row opens its transaction");
        m.apply(&row(2, op::ROLLBACK, "Y", "rollback")).unwrap();
        assert_eq!(m.oldest_open(), None);
        assert!(
            m.apply(&row(3, op::COMMIT, "Z", "commit"))
                .unwrap()
                .is_none()
        );
        assert!(m.apply(&row(4, 99, "Z", "")).unwrap().is_none());
    }

    fn with_rid(mut r: LogRow, rid: &str) -> LogRow {
        r.row_id = Some(rid.into());
        r
    }

    fn undo(mut r: LogRow, rid: &str) -> LogRow {
        r.row_id = Some(rid.into());
        r.rollback = true;
        r
    }

    #[test]
    fn savepoint_undo_reverts_folds_and_removes_created_rows() {
        let mut m = miner();
        m.apply(&with_rid(
            row(
                1,
                op::INSERT,
                "X",
                r#"insert into "APP"."T"("ID","V") values ('1','a')"#,
            ),
            "R1",
        ))
        .unwrap();
        m.apply(&with_rid(
            row(
                2,
                op::INSERT,
                "X",
                r#"insert into "APP"."T"("ID","V") values ('2','b')"#,
            ),
            "R2",
        ))
        .unwrap();
        m.apply(&with_rid(
            row(
                3,
                op::UPDATE,
                "X",
                r#"update "APP"."T" set "V" = 'c' where "ID" = '1' and "V" = 'a'"#,
            ),
            "R1",
        ))
        .unwrap();
        m.apply(&with_rid(
            row(
                4,
                op::DELETE,
                "X",
                r#"delete from "APP"."T" where "ID" = '5'"#,
            ),
            "R5",
        ))
        .unwrap();
        m.apply(&undo(
            row(
                5,
                op::INSERT,
                "X",
                r#"insert into "APP"."T"("ID") values ('5')"#,
            ),
            "R5",
        ))
        .unwrap();
        m.apply(&undo(
            row(6, op::UPDATE, "X", r#"update "APP"."T" set "V" = 'a'"#),
            "R1",
        ))
        .unwrap();
        m.apply(&undo(
            row(
                7,
                op::DELETE,
                "X",
                r#"delete from "APP"."T" where ROWID = 'R2'"#,
            ),
            "R2",
        ))
        .unwrap();
        m.apply(&undo(row(8, op::DELETE, "X", ""), "R9")).unwrap();
        m.apply(&undo(row(9, op::DELETE, "X", ""), "AAAAAAAAAAAAAAAAAA"))
            .unwrap();
        let c = m
            .apply(&row(10, op::COMMIT, "X", "commit"))
            .unwrap()
            .unwrap();
        assert_eq!(c.events.len(), 1);
        assert_eq!(
            image_to_json(c.events[0].after.as_ref().unwrap(), None),
            json!({"ID": "1", "V": "a"})
        );
    }

    #[test]
    fn lob_insert_undo_uses_the_adopted_row_id() {
        let mut m = miner();
        m.apply(&row(
            1,
            op::INSERT,
            "X",
            r#"insert into "APP"."T"("ID") values ('4')"#,
        ))
        .unwrap();
        let mut ins = row(
            2,
            op::INSERT,
            "X",
            r#"insert into "APP"."T"("ID","NOTE") values ('91',EMPTY_CLOB())"#,
        );
        ins.row_id = Some("AAAAAAAAAAAAAAAAAA".into());
        m.apply(&ins).unwrap();
        m.apply(&with_rid(
            row(
                3,
                op::UPDATE,
                "X",
                r#"update "APP"."T" set "NOTE" = NULL where "ID" = '91'"#,
            ),
            "R91",
        ))
        .unwrap();
        m.apply(&undo(row(4, op::DELETE, "X", ""), "R91")).unwrap();
        let c = m
            .apply(&row(5, op::COMMIT, "X", "commit"))
            .unwrap()
            .unwrap();
        assert_eq!(c.events.len(), 1);
        assert_eq!(
            image_to_json(c.events[0].after.as_ref().unwrap(), None),
            json!({"ID": "4"})
        );
    }

    #[test]
    fn keyless_updates_are_unsupported() {
        let err = miner()
            .apply(&row(
                1,
                op::UPDATE,
                "X",
                r#"update "APP"."T" set "V" = 'x'"#,
            ))
            .unwrap_err();
        assert!(err.to_string().contains("no key columns"), "{err}");
    }

    #[test]
    fn follow_up_update_without_row_id_folds_by_key() {
        let mut m = miner();
        let mut ins = row(
            1,
            op::INSERT,
            "X",
            r#"insert into "APP"."T"("ID","NOTE") values ('1',EMPTY_CLOB())"#,
        );
        ins.row_id = Some("AAAAAAAAAAAAAAAAAA".into());
        m.apply(&ins).unwrap();
        let upd = row(
            2,
            op::UPDATE,
            "X",
            r#"update "APP"."T" set "NOTE" = 'n' where "ID" = '1'"#,
        );
        m.apply(&upd).unwrap();
        let other = row(
            3,
            op::UPDATE,
            "X",
            r#"update "APP"."T" set "NOTE" = 'o' where "ID" = '2'"#,
        );
        m.apply(&other).unwrap();
        let c = m
            .apply(&row(4, op::COMMIT, "X", "commit"))
            .unwrap()
            .unwrap();
        assert_eq!(c.events.len(), 2);
        assert_eq!(
            image_to_json(c.events[0].after.as_ref().unwrap(), None),
            json!({"ID": "1", "NOTE": "n"})
        );
        assert_eq!(c.events[1].op, EventOp::Update);
    }

    #[test]
    fn lob_writes_fill_the_row_or_become_updates() {
        let mut m = miner();
        m.apply(&row(
            1,
            op::INSERT,
            "X",
            r#"insert into "APP"."T"("ID","NOTE","B") values ('1',EMPTY_CLOB(),EMPTY_BLOB())"#,
        ))
        .unwrap();
        let write = |col: &str, buf: &str, off: u64| {
            format!(
                "DECLARE loc_c CLOB; BEGIN select \"{col}\" into loc_c from \"APP\".\"T\" where \"ID\" = '1' for update; buf_c := {buf}; dbms_lob.write(loc_c, 3, {off}, buf_c); END;"
            )
        };
        m.apply(&row(2, op::LOB_WRITE, "X", &write("NOTE", "'abc'", 1)))
            .unwrap();
        m.apply(&row(3, op::LOB_WRITE, "X", &write("NOTE", "'def'", 4)))
            .unwrap();
        m.apply(&row(
            4,
            op::LOB_WRITE,
            "X",
            &write("B", "HEXTORAW('0102')", 1),
        ))
        .unwrap();
        let orphan = "DECLARE loc_c CLOB; BEGIN select \"NOTE\" into loc_c from \"APP\".\"T\" where \"ID\" = '9' for update; buf_c := 'zz'; dbms_lob.write(loc_c, 2, 1, buf_c); END;";
        m.apply(&row(5, op::LOB_WRITE, "X", orphan)).unwrap();
        let c = m
            .apply(&row(6, op::COMMIT, "X", "commit"))
            .unwrap()
            .unwrap();
        assert_eq!(c.events.len(), 2);
        let meta: TableMeta = HashMap::from([(
            ("APP".into(), "T".into()),
            HashMap::from([
                ("ID".to_string(), TypeFamily::Integer),
                ("NOTE".to_string(), TypeFamily::Clob),
                ("B".to_string(), TypeFamily::Blob),
            ]),
        )]);
        let env = to_envelope(&c.events[0], &c, &meta);
        assert_eq!(
            env["after"],
            json!({"ID": 1, "NOTE": "abcdef", "B": "AQI="})
        );
        let upd = to_envelope(&c.events[1], &c, &meta);
        assert_eq!(upd["op"], "u");
        assert_eq!(upd["after"], json!({"ID": 9, "NOTE": "zz"}));
        assert_eq!(upd["before"], json!({"ID": 9}));
    }

    #[test]
    fn ddl_and_truncate_events() {
        let mut m = miner();
        m.apply(&row(1, op::DDL, "D", "alter table app.t add (x number)"))
            .unwrap();
        m.apply(&row(2, op::DDL, "D", "truncate table app.t"))
            .unwrap();
        assert_eq!(
            m.take_ddl_tables(),
            vec![("APP".into(), "T".into()), ("APP".into(), "T".into())]
        );
        assert!(m.take_ddl_tables().is_empty());
        let c = m
            .apply(&row(3, op::COMMIT, "D", "commit"))
            .unwrap()
            .unwrap();
        let env = to_envelope(&c.events[1], &c, &TableMeta::new());
        assert_eq!(env["op"], "truncate");
        assert_eq!(env["sql"], "truncate table app.t");
        assert_eq!(
            to_envelope(&c.events[0], &c, &TableMeta::new())["op"],
            "ddl"
        );
    }

    #[test]
    fn unsupported_rows_fail_or_skip() {
        let mut m = miner();
        let mut bad = row(1, op::UNSUPPORTED, "U", "");
        bad.info = Some("Unsupported datatype".into());
        let err = m.apply(&bad).unwrap_err();
        assert!(err.to_string().contains("Unsupported datatype"), "{err}");
        let mut mismatch = row(
            2,
            op::INSERT,
            "U",
            r#"insert into "APP"."T"("COL 1") values (HEXTORAW('c102'))"#,
        );
        mismatch.info = Some("Dictionary Mismatch".into());
        assert!(m.apply(&mismatch).is_err());
        let mut lob_mismatch = row(
            3,
            op::LOB_WRITE,
            "U",
            "DECLARE BEGIN select \"N\" into loc_c from \"APP\".\"T\" for update; END;",
        );
        lob_mismatch.info = Some("Dictionary Version Mismatch".into());
        assert!(m.apply(&lob_mismatch).is_err());
        assert!(m.apply(&row(4, op::LOB_WRITE, "U", "garbage(")).is_err());
        assert!(m.apply(&row(5, op::UPDATE, "U", "not sql")).is_err());

        let mut skip = Miner::new(OnUnsupported::Skip, None);
        assert!(skip.apply(&bad).unwrap().is_none());
        let mut no_owner = row(6, op::UNSUPPORTED, "U", "");
        no_owner.owner = None;
        no_owner.table = None;
        assert!(skip.apply(&no_owner).unwrap().is_none());
    }

    #[test]
    fn max_staged_bounds_memory() {
        let mut m = Miner::new(OnUnsupported::Fail, Some(1));
        let ins = |scn| {
            row(
                scn,
                op::INSERT,
                "M",
                r#"insert into "APP"."T"("ID") values ('1')"#,
            )
        };
        m.apply(&ins(1)).unwrap();
        let err = m.apply(&ins(2)).unwrap_err();
        assert!(err.to_string().contains("max_staged_records"), "{err}");
        let mut d = Miner::new(OnUnsupported::Fail, Some(0));
        assert!(d.apply(&row(1, op::DDL, "Q", "alter table x")).is_err());
        let lob = "DECLARE BEGIN select \"N\" into loc_c from \"APP\".\"T\" where \"ID\" = '1' for update; END;";
        assert!(
            Miner::new(OnUnsupported::Fail, Some(0))
                .apply(&row(1, op::LOB_WRITE, "Q", lob))
                .is_err()
        );
    }

    #[test]
    fn image_bytes_follow_the_column_family() {
        let img = vec![
            ("R".to_string(), Resolved::Bytes(vec![0xff])),
            ("S".to_string(), Resolved::Bytes(b"hi".to_vec())),
            ("E".to_string(), Resolved::EmptyLob),
        ];
        let fam = HashMap::from([("S".to_string(), TypeFamily::Text)]);
        assert_eq!(
            image_to_json(&img, Some(&fam)),
            json!({"R": "/w==", "S": "hi", "E": null})
        );
    }
}

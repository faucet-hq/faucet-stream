//! Pure page-assembly state machine.
//!
//! Extracted from the stream so the RECORD/STATE/EOF → [`StreamPage`] logic is
//! unit-testable without spawning a process. The stream feeds parsed messages
//! in; the process/error paths (idle timeout, non-zero exit, malformed-fail)
//! stay in `stream.rs`.

use faucet_core::{StreamPage, Value};

/// Accumulates RECORDs into pages and attaches the latest STATE as the page
/// bookmark, per the v0 single-stream rules.
pub struct PageAssembler {
    target_stream: String,
    /// Other names the target stream goes by (its catalog `stream` /
    /// `tap_stream_id` pair).
    aliases: Vec<String>,
    batch_size: usize,
    flush_on_state: bool,
    buffer: Vec<Value>,
    /// Latest STATE `value` seen but not yet attached to a flushed page.
    pending_state: Option<Value>,
    target_records: usize,
    /// RECORD counts for streams that are not the target.
    other_streams: std::collections::BTreeMap<String, usize>,
}

impl PageAssembler {
    /// `batch_size == 0` disables size-based flushing (flush only on STATE/EOF).
    pub fn new(target_stream: impl Into<String>, batch_size: usize, flush_on_state: bool) -> Self {
        Self {
            target_stream: target_stream.into(),
            aliases: Vec::new(),
            batch_size,
            flush_on_state,
            buffer: Vec::new(),
            pending_state: None,
            target_records: 0,
            other_streams: std::collections::BTreeMap::new(),
        }
    }

    /// Also accept RECORDs carrying `name` (the other of the target's catalog
    /// `stream` / `tap_stream_id` names).
    pub fn with_alias(mut self, name: impl Into<String>) -> Self {
        self.aliases.push(name.into());
        self
    }

    /// Feed a RECORD. Records for a different stream are ignored (single-stream
    /// v0) and warned about once per stream. Returns a page when the
    /// `batch_size` threshold is reached.
    pub fn on_record(&mut self, stream: &str, record: Value) -> Option<StreamPage> {
        if stream != self.target_stream && !self.aliases.iter().any(|a| a == stream) {
            let seen = self.other_streams.entry(stream.to_string()).or_insert(0);
            if *seen == 0 {
                tracing::warn!(
                    stream = %stream,
                    target = %self.target_stream,
                    "singer RECORD for a stream other than the configured one; dropping"
                );
            }
            *seen += 1;
            return None;
        }
        self.target_records += 1;
        self.buffer.push(record);
        if self.batch_size != 0 && self.buffer.len() >= self.batch_size {
            Some(self.flush())
        } else {
            None
        }
    }

    /// Feed a STATE. Always becomes the pending checkpoint; flushes immediately
    /// when `flush_on_state` (yielding a page — possibly empty — that carries
    /// the STATE as its bookmark).
    pub fn on_state(&mut self, value: Value) -> Option<StreamPage> {
        self.pending_state = Some(value);
        if self.flush_on_state {
            Some(self.flush())
        } else {
            None
        }
    }

    /// An error when the tap has emitted RECORDs only for other streams: the
    /// configured `stream` almost certainly names the catalog entry by the
    /// name the tap does not use on RECORD, and checkpointing its STATE would
    /// skip those rows for good (API-26).
    pub fn stream_mismatch(&self) -> Option<String> {
        if self.target_records > 0 || self.other_streams.is_empty() {
            return None;
        }
        let seen: Vec<String> = self
            .other_streams
            .iter()
            .map(|(s, n)| format!("{s} ({n})"))
            .collect();
        Some(format!(
            "the tap emitted RECORDs only for other streams [{}] and none for `{}`; refusing \
             to checkpoint its STATE — set `stream` to the name the tap uses on RECORD",
            seen.join(", "),
            self.target_stream
        ))
    }

    /// Flush any trailing buffer + pending checkpoint at end-of-stream. Returns
    /// `None` when there is nothing to emit.
    pub fn on_eof(&mut self) -> Option<StreamPage> {
        if self.buffer.is_empty() && self.pending_state.is_none() {
            None
        } else {
            Some(self.flush())
        }
    }

    /// Drain the buffer + pending checkpoint into a page.
    fn flush(&mut self) -> StreamPage {
        StreamPage {
            records: std::mem::take(&mut self.buffer),
            bookmark: self.pending_state.take(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rec(id: i64) -> Value {
        json!({ "id": id })
    }

    #[test]
    fn flushes_at_batch_size_with_no_bookmark() {
        let mut a = PageAssembler::new("s", 2, true);
        assert!(a.on_record("s", rec(1)).is_none());
        let page = a.on_record("s", rec(2)).expect("flush at size 2");
        assert_eq!(page.records, vec![rec(1), rec(2)]);
        assert!(
            page.bookmark.is_none(),
            "no STATE covers a size-based flush"
        );
        // third record buffered until EOF
        assert!(a.on_record("s", rec(3)).is_none());
        let tail = a.on_eof().unwrap();
        assert_eq!(tail.records, vec![rec(3)]);
    }

    #[test]
    fn state_flush_attaches_bookmark() {
        let mut a = PageAssembler::new("s", 1000, true);
        assert!(a.on_record("s", rec(1)).is_none());
        assert!(a.on_record("s", rec(2)).is_none());
        let page = a.on_state(json!({"last_id": 2})).expect("flush on state");
        assert_eq!(page.records, vec![rec(1), rec(2)]);
        assert_eq!(page.bookmark, Some(json!({"last_id": 2})));
        // nothing left
        assert!(a.on_eof().is_none());
    }

    #[test]
    fn empty_run_with_trailing_state_yields_empty_page_with_bookmark() {
        let mut a = PageAssembler::new("s", 1000, true);
        let page = a.on_state(json!({"last_id": 0})).unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.bookmark, Some(json!({"last_id": 0})));
    }

    #[test]
    fn records_for_other_streams_are_ignored() {
        let mut a = PageAssembler::new("wanted", 1, true);
        assert!(a.on_record("other", rec(1)).is_none());
        assert!(a.on_record("other", rec(2)).is_none());
        // only the wanted stream flushes
        let page = a.on_record("wanted", rec(3)).unwrap();
        assert_eq!(page.records, vec![rec(3)]);
    }

    #[test]
    fn no_flush_on_state_defers_bookmark_to_next_flush() {
        let mut a = PageAssembler::new("s", 2, false);
        // STATE arrives first; not flushed because flush_on_state == false
        assert!(a.on_state(json!({"last_id": 0})).is_none());
        assert!(a.on_record("s", rec(1)).is_none());
        // batch flush picks up the pending bookmark
        let page = a.on_record("s", rec(2)).unwrap();
        assert_eq!(page.records, vec![rec(1), rec(2)]);
        assert_eq!(page.bookmark, Some(json!({"last_id": 0})));
    }

    #[test]
    fn eof_with_empty_buffer_and_no_state_yields_nothing() {
        let mut a = PageAssembler::new("s", 1000, true);
        assert!(a.on_eof().is_none());
    }

    #[test]
    fn an_alias_matches_and_a_mismatch_is_reported() {
        let mut a = PageAssembler::new("public-users", 0, true).with_alias("users");
        assert!(a.on_record("users", rec(1)).is_none());
        assert!(a.stream_mismatch().is_none());
        let page = a.on_state(json!({"b": 1})).unwrap();
        assert_eq!(page.records, vec![rec(1)]);

        let mut b = PageAssembler::new("public-users", 0, true);
        assert!(
            b.stream_mismatch().is_none(),
            "no records at all is a quiet run"
        );
        b.on_record("users", rec(1));
        b.on_record("users", rec(2));
        let msg = b.stream_mismatch().unwrap();
        assert!(
            msg.contains("users (2)") && msg.contains("public-users"),
            "{msg}"
        );
    }
}

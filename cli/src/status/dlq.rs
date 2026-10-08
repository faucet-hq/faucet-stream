//! DLQ backlog for `faucet status`: how many envelopes a row's DLQ holds and
//! how old the oldest is — read from a local JSON Lines DLQ sink's files
//! (`file` writing uncompressed JSON Lines, or the deprecated `jsonl`).

use crate::config::DlqSpec;
use crate::dlq_replay::plan::{dlq_encryption_value, writes_json_lines};
use crate::dlq_replay::reader::{DlqDecryptor, expand_location, scan_files};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// A row's DLQ backlog.
#[derive(Debug, Clone, Default, Serialize)]
pub struct DlqStatus {
    pub configured: bool,
    /// Whether the backlog could be read (local JSON Lines DLQs only).
    pub readable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Envelopes attributed to this row (or unattributed).
    pub count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest: Option<DateTime<Utc>>,
    /// Lines that could not be read as envelopes (malformed / sealed).
    #[serde(skip_serializing_if = "is_zero")]
    pub unreadable_lines: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

fn is_zero(n: &usize) -> bool {
    *n == 0
}

/// `${…}` tokens (a dated DLQ path) become `*`, so every dated file counts.
pub fn location_glob(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut rest = path;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        match rest[start..].find('}') {
            Some(end) => {
                out.push('*');
                rest = &rest[start + end + 1..];
            }
            None => {
                out.push_str(&rest[start..]);
                rest = "";
            }
        }
    }
    out.push_str(rest);
    out
}

/// Count the backlog of `spec` for `pipeline` / `row`.
pub fn backlog(spec: Option<&DlqSpec>, pipeline: &str, row: &str) -> DlqStatus {
    let Some(spec) = spec else {
        return DlqStatus::default();
    };
    let mut out = DlqStatus {
        configured: true,
        ..Default::default()
    };
    let path = spec.sink.config.get("path").and_then(|p| p.as_str());
    let (true, Some(path)) = (writes_json_lines(&spec.sink), path) else {
        out.note = Some(format!(
            "backlog not readable for a `{}` DLQ sink — only local JSON Lines DLQs \
             (`file` writing uncompressed JSON Lines, or `jsonl`) are counted; \
             inspect it at its destination",
            spec.sink.kind
        ));
        return out;
    };
    let glob = location_glob(path);
    out.location = Some(glob.clone());
    let files = match expand_location(&glob) {
        Ok(f) => f
            .into_iter()
            .filter(|p| !crate::dlq_replay::plan::is_replay_failure(p))
            .collect::<Vec<_>>(),
        Err(_) => {
            // Nothing written yet is an empty backlog, not an error.
            out.readable = true;
            return out;
        }
    };
    let dec = match DlqDecryptor::from_config_value(dlq_encryption_value(Some(spec))) {
        Ok(d) => d,
        Err(e) => {
            out.note = Some(format!("DLQ encryption config unusable: {e}"));
            return out;
        }
    };
    match scan_files(&files, &dec) {
        Ok(scan) => {
            out.readable = true;
            out.unreadable_lines = scan.malformed + scan.undecryptable;
            let mine = scan.envelopes.iter().filter(|e| {
                e.pipeline.as_deref().is_none_or(|p| p == pipeline)
                    && e.row.as_deref().is_none_or(|r| r.is_empty() || r == row)
            });
            for e in mine {
                out.count += 1;
                if let Some(ts) = e.ts_ms.and_then(DateTime::from_timestamp_millis)
                    && out.oldest.is_none_or(|o| ts < o)
                {
                    out.oldest = Some(ts);
                }
            }
        }
        Err(e) => out.note = Some(format!("DLQ unreadable: {e}")),
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::PipelineConfig;
    use serde_json::json;
    use std::path::Path;

    fn spec(kind: &str, config: serde_json::Value) -> DlqSpec {
        let text = format!(
            "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: a }} }}\n  sink: {{ type: jsonl, config: {{ path: b }} }}\n  dlq:\n    sink: {{ type: {kind}, config: {config} }}\n"
        );
        PipelineConfig::from_text(&text, Path::new("t.yaml"))
            .unwrap()
            .pipeline
            .dlq
            .unwrap()
    }

    fn envelope(pipeline: &str, row: &str, ts: i64) -> String {
        json!({
            "payload": {"id": 1},
            "reason": "sink",
            "error": {"kind": "Sink", "message": "no"},
            "pipeline": pipeline,
            "row": row,
            "ts_ms": ts,
        })
        .to_string()
    }

    #[test]
    fn globs_tokens() {
        assert_eq!(
            location_glob("./dlq/${now.date}/x.jsonl"),
            "./dlq/*/x.jsonl"
        );
        assert_eq!(location_glob("a${b}c${d}"), "a*c*");
        assert_eq!(location_glob("plain.jsonl"), "plain.jsonl");
        assert_eq!(location_glob("bad${open"), "bad${open");
    }

    #[test]
    fn counts_this_rows_envelopes_and_the_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.jsonl");
        let lines = [
            envelope("orders", "a", 2_000),
            envelope("orders", "a", 1_000),
            envelope("orders", "b", 500),
            envelope("other", "a", 10),
            "not json".to_string(),
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();
        let s = spec("jsonl", json!({"path": path.to_str().unwrap()}));
        let st = backlog(Some(&s), "orders", "a");
        assert!(st.configured && st.readable);
        assert_eq!(st.count, 2);
        assert_eq!(st.oldest.unwrap().timestamp_millis(), 1_000);
        assert_eq!(st.unreadable_lines, 1);
    }

    /// A dated DLQ glob skips replay-failure files and discarded envelopes,
    /// so a handled backlog reads as empty (#789 CLI-166).
    #[test]
    fn a_dated_backlog_skips_replay_failures_and_discards() {
        let dir = tempfile::tempdir().unwrap();
        let day = dir.path().join("2026-10-01.jsonl");
        std::fs::write(&day, envelope("orders", "a", 1_000)).unwrap();
        std::fs::write(
            dir.path().join("2026-10-01.replay-failed.jsonl"),
            envelope("orders", "a", 2_000),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("replay-failed.jsonl"),
            envelope("orders", "a", 3_000),
        )
        .unwrap();
        let pattern = format!("{}/${{now.date}}.jsonl", dir.path().display());
        let s = spec("jsonl", json!({ "path": pattern }));
        assert_eq!(backlog(Some(&s), "orders", "a").count, 1);
        crate::dlq_replay::discard(
            day.to_str().unwrap(),
            None,
            None,
            true,
            &crate::dlq_replay::reader::DlqDecryptor::default(),
        )
        .unwrap();
        assert_eq!(backlog(Some(&s), "orders", "a").count, 0);
    }

    #[test]
    fn counts_a_json_lines_file_dlq() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dlq.jsonl");
        let lines = [
            envelope("orders", "a", 3_000),
            envelope("orders", "a", 2_500),
        ];
        std::fs::write(&path, lines.join("\n")).unwrap();
        for config in [
            json!({"path": path.to_str().unwrap()}),
            json!({"path": path.to_str().unwrap(), "format": "json_lines"}),
        ] {
            let st = backlog(Some(&spec("file", config)), "orders", "a");
            assert!(st.configured && st.readable, "{:?}", st.note);
            assert_eq!(st.count, 2);
            assert_eq!(st.oldest.unwrap().timestamp_millis(), 2_500);
        }
        let csv = dir.path().join("dlq.csv");
        let st = backlog(
            Some(&spec("file", json!({"path": csv.to_str().unwrap()}))),
            "orders",
            "a",
        );
        assert!(!st.readable);
        assert!(st.note.unwrap().contains("`file`"));
    }

    #[test]
    fn missing_files_are_an_empty_backlog_and_other_kinds_are_noted() {
        assert!(!backlog(None, "p", "r").configured);
        let s = spec(
            "jsonl",
            json!({"path": "/nonexistent/dlq-${now.date}.jsonl"}),
        );
        let st = backlog(Some(&s), "p", "r");
        assert!(st.readable && st.count == 0);
        let st = backlog(Some(&spec("stdout", json!({}))), "p", "r");
        assert!(!st.readable);
        assert!(st.note.unwrap().contains("`stdout`"));
    }

    #[test]
    fn unreadable_files_and_bad_encryption_are_noted() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let f = dir.path().join("locked.jsonl");
            std::fs::write(&f, "{}").unwrap();
            std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o000)).unwrap();
            let st = backlog(
                Some(&spec("jsonl", json!({"path": f.to_str().unwrap()}))),
                "p",
                "r",
            );
            std::fs::set_permissions(&f, std::fs::Permissions::from_mode(0o600)).unwrap();
            if !st.readable {
                assert!(st.note.unwrap().contains("DLQ unreadable"));
            }
        }
        let f = dir.path().join("e.jsonl");
        std::fs::write(&f, "{}").unwrap();
        let st = backlog(
            Some(&spec(
                "jsonl",
                json!({"path": f.to_str().unwrap(), "encryption": {"key": ""}}),
            )),
            "p",
            "r",
        );
        assert!(st.note.unwrap().contains("encryption"));
    }
}

//! Shared log-shipping data: a buffered line, per-run delivery state, and the
//! `log_export` status reported on run records and `faucet run` output.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Longest log line kept in the buffer (bytes). A longer line is cut at a char
/// boundary and suffixed with the number of bytes removed.
pub const MAX_LINE_BYTES: usize = 16 * 1024;

/// Cut `line` to [`MAX_LINE_BYTES`].
pub fn truncate_line(line: String) -> String {
    if line.len() <= MAX_LINE_BYTES {
        return line;
    }
    let mut end = MAX_LINE_BYTES;
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    let removed = line.len() - end;
    format!("{}…[truncated {removed} bytes]", &line[..end])
}

/// One buffered log line, ready to export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShipLine {
    /// Per-run, strictly increasing sequence number.
    pub seq: u64,
    /// RFC 3339 capture time.
    pub ts: String,
    /// `TRACE` / `DEBUG` / `INFO` / `WARN` / `ERROR`.
    pub level: String,
    /// The message (and any event fields).
    pub body: String,
    /// Line-level attributes (`target`, `row`, `connector`, `trace_id`, …).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attrs: BTreeMap<String, String>,
}

impl ShipLine {
    /// Rebuild a line from the serve log store, whose `line` is
    /// `<ts> <LEVEL> <target>: <body>`.
    pub fn from_rendered(
        seq: u64,
        ts: &str,
        level: &str,
        line: &str,
        attrs: BTreeMap<String, String>,
    ) -> Self {
        let body = match attrs.get("target") {
            Some(target) => line
                .strip_prefix(&format!("{ts} {level} {target}: "))
                .unwrap_or(line),
            None => line,
        };
        Self {
            seq,
            ts: ts.to_string(),
            level: level.to_string(),
            body: body.to_string(),
            attrs,
        }
    }

    /// Approximate buffered size of the line (bytes).
    pub fn size(&self) -> u64 {
        (self.body.len()
            + self.ts.len()
            + self.level.len()
            + self
                .attrs
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()) as u64
    }
}

/// Whether a run's logs reached the log service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogExportStatus {
    /// Log shipping is not configured.
    NotConfigured,
    /// Lines are buffered and not yet acknowledged.
    Pending,
    /// Every captured line was acknowledged by the collector.
    Exported,
    /// The last export attempt failed; lines are still buffered.
    Failed,
    /// Some lines were dropped by a buffer bound or the per-run cap.
    PartiallyDropped,
}

impl LogExportStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotConfigured => "not_configured",
            Self::Pending => "pending",
            Self::Exported => "exported",
            Self::Failed => "failed",
            Self::PartiallyDropped => "partially_dropped",
        }
    }
}

/// The delivery bookkeeping a buffer keeps per run.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryState {
    /// Highest sequence the collector acknowledged.
    pub delivered_seq: Option<u64>,
    /// Highest sequence captured.
    pub total_seq: Option<u64>,
    /// Lines buffered and not yet acknowledged.
    pub pending_lines: u64,
    /// Lines dropped by a bound or the per-run cap.
    pub dropped_lines: u64,
    pub last_error: Option<String>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    /// When the current run of failed attempts began.
    pub failing_since: Option<DateTime<Utc>>,
    /// When the last batch was acknowledged.
    pub delivered_at: Option<DateTime<Utc>>,
}

/// The `log_export` object on a run record / `faucet run` summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogExportView {
    pub status: LogExportStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_seq: Option<u64>,
    pub pending_lines: u64,
    pub dropped_lines: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivered_at: Option<DateTime<Utc>>,
    /// Link to the run's logs in the log service (`link_template`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link: Option<String>,
}

/// The view for a run's delivery state.
pub fn derive_view(configured: bool, st: &DeliveryState) -> LogExportView {
    let status = if !configured {
        LogExportStatus::NotConfigured
    } else if st.dropped_lines > 0 {
        LogExportStatus::PartiallyDropped
    } else if st.pending_lines > 0 && st.last_error.is_some() {
        LogExportStatus::Failed
    } else if st.pending_lines > 0 {
        LogExportStatus::Pending
    } else {
        LogExportStatus::Exported
    };
    LogExportView {
        status,
        delivered_seq: st.delivered_seq,
        total_seq: st.total_seq,
        pending_lines: st.pending_lines,
        dropped_lines: st.dropped_lines,
        last_error: st.last_error.clone(),
        last_attempt_at: st.last_attempt_at,
        delivered_at: st.delivered_at,
        link: None,
    }
}

/// Whether a run has been failing long enough to notify.
pub fn failing_past(st: &DeliveryState, threshold: Duration, now: DateTime<Utc>) -> bool {
    st.pending_lines > 0
        && st
            .failing_since
            .is_some_and(|t| now - t >= chrono::Duration::from_std(threshold).unwrap_or_default())
}

impl LogExportView {
    /// The one-line `faucet run` summary.
    pub fn summary_line(&self) -> String {
        match self.status {
            LogExportStatus::NotConfigured => "logs: not shipped (not configured)".into(),
            LogExportStatus::Exported => "logs: exported".into(),
            LogExportStatus::Pending | LogExportStatus::Failed => {
                let mut s = format!(
                    "logs: {} line{} pending — run \"faucet logs ship\"",
                    crate::logship::record::group(self.pending_lines),
                    if self.pending_lines == 1 { "" } else { "s" }
                );
                if let Some(e) = &self.last_error {
                    s.push_str(&format!(" (last error: {e})"));
                }
                s
            }
            LogExportStatus::PartiallyDropped => format!(
                "logs: partially dropped — {} line{} dropped, {} pending",
                group(self.dropped_lines),
                if self.dropped_lines == 1 { "" } else { "s" },
                group(self.pending_lines)
            ),
        }
    }
}

/// `1204` → `1,204`.
pub fn group(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// Values for a `link_template`.
#[derive(Debug, Clone, Default)]
pub struct LinkVars {
    pub run_id: String,
    pub pipeline: String,
    pub row: String,
    pub tenant: String,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
}

/// Fill `template`'s placeholders, URL-encoding each value.
pub fn render_link(template: &str, v: &LinkVars) -> String {
    let ts = |t: Option<DateTime<Utc>>| {
        t.map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true))
            .unwrap_or_default()
    };
    template
        .replace("{run_id}", &encode(&v.run_id))
        .replace("{pipeline}", &encode(&v.pipeline))
        .replace("{row}", &encode(&v.row))
        .replace("{tenant}", &encode(&v.tenant))
        .replace("{started_at}", &encode(&ts(v.started_at)))
        .replace("{ended_at}", &encode(&ts(v.ended_at)))
}

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// OTLP severity number for a tracing level.
pub fn severity_number(level: &str) -> i32 {
    match level {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_cuts_at_a_char_boundary() {
        assert_eq!(truncate_line("short".into()), "short");
        let long = "é".repeat(MAX_LINE_BYTES);
        let t = truncate_line(long.clone());
        assert!(t.len() < long.len());
        assert!(t.ends_with(&format!("[truncated {} bytes]", long.len() - MAX_LINE_BYTES)));
    }

    #[test]
    fn rendered_lines_lose_their_prefix() {
        let mut attrs = BTreeMap::new();
        attrs.insert("target".to_string(), "faucet_core::pipeline".to_string());
        let l = ShipLine::from_rendered(
            7,
            "2026-01-01T00:00:00.000Z",
            "INFO",
            "2026-01-01T00:00:00.000Z INFO faucet_core::pipeline: hello k=1",
            attrs.clone(),
        );
        assert_eq!(l.body, "hello k=1");
        assert!(l.size() > 0);
        let raw = ShipLine::from_rendered(1, "t", "INFO", "free text", BTreeMap::new());
        assert_eq!(raw.body, "free text");
        let other = ShipLine::from_rendered(1, "t", "INFO", "no prefix", attrs);
        assert_eq!(other.body, "no prefix");
    }

    #[test]
    fn status_precedence() {
        let mut st = DeliveryState::default();
        assert_eq!(derive_view(false, &st).status, LogExportStatus::NotConfigured);
        assert_eq!(derive_view(true, &st).status, LogExportStatus::Exported);
        st.pending_lines = 3;
        assert_eq!(derive_view(true, &st).status, LogExportStatus::Pending);
        st.last_error = Some("down".into());
        assert_eq!(derive_view(true, &st).status, LogExportStatus::Failed);
        st.dropped_lines = 1;
        assert_eq!(
            derive_view(true, &st).status,
            LogExportStatus::PartiallyDropped
        );
        for s in [
            LogExportStatus::NotConfigured,
            LogExportStatus::Pending,
            LogExportStatus::Exported,
            LogExportStatus::Failed,
            LogExportStatus::PartiallyDropped,
        ] {
            assert_eq!(
                serde_json::to_value(s).unwrap(),
                serde_json::Value::String(s.as_str().into())
            );
        }
    }

    #[test]
    fn summary_lines() {
        let mut st = DeliveryState::default();
        assert_eq!(derive_view(true, &st).summary_line(), "logs: exported");
        assert!(derive_view(false, &st).summary_line().contains("not configured"));
        st.pending_lines = 1204;
        assert_eq!(
            derive_view(true, &st).summary_line(),
            "logs: 1,204 lines pending — run \"faucet logs ship\""
        );
        st.pending_lines = 1;
        st.last_error = Some("refused".into());
        assert!(derive_view(true, &st).summary_line().contains("1 line pending"));
        assert!(derive_view(true, &st).summary_line().contains("refused"));
        st.dropped_lines = 1;
        assert!(derive_view(true, &st).summary_line().contains("1 line dropped"));
        st.dropped_lines = 2;
        assert!(derive_view(true, &st).summary_line().contains("2 lines dropped"));
        assert_eq!(group(0), "0");
        assert_eq!(group(999), "999");
        assert_eq!(group(1_000_000), "1,000,000");
    }

    #[test]
    fn failing_past_needs_pending_and_age() {
        let now = Utc::now();
        let mut st = DeliveryState {
            pending_lines: 1,
            failing_since: Some(now - chrono::Duration::seconds(400)),
            ..Default::default()
        };
        assert!(failing_past(&st, Duration::from_secs(300), now));
        assert!(!failing_past(&st, Duration::from_secs(500), now));
        st.pending_lines = 0;
        assert!(!failing_past(&st, Duration::from_secs(1), now));
    }

    #[test]
    fn link_template_encodes_values() {
        let t0 = DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
            .unwrap()
            .with_timezone(&Utc);
        let v = LinkVars {
            run_id: "r 1".into(),
            pipeline: "a/b".into(),
            row: "x".into(),
            tenant: "t&u".into(),
            started_at: Some(t0),
            ended_at: None,
        };
        assert_eq!(
            render_link(
                "https://g/explore?q={run_id}&p={pipeline}&r={row}&t={tenant}&s={started_at}&e={ended_at}",
                &v
            ),
            "https://g/explore?q=r%201&p=a%2Fb&r=x&t=t%26u&s=2026-01-02T03%3A04%3A05.000Z&e="
        );
    }

    #[test]
    fn severity_numbers() {
        assert_eq!(severity_number("TRACE"), 1);
        assert_eq!(severity_number("DEBUG"), 5);
        assert_eq!(severity_number("INFO"), 9);
        assert_eq!(severity_number("WARN"), 13);
        assert_eq!(severity_number("ERROR"), 17);
        assert_eq!(severity_number("x"), 0);
    }
}

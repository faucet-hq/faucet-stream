//! Best-effort bookmark-lag gauge. When a pipeline's bookmark is a
//! `Value::String` that parses as RFC3339 — bare, or as the `value` of a
//! cursor bookmark `{"value": …, "boundary": …}` — we update two gauges:
//! `faucet_pipeline_last_bookmark_unix_seconds` (epoch seconds of the
//! bookmark) and `faucet_pipeline_seconds_since_last_bookmark` (now - that).
//! Non-string / non-RFC3339 bookmarks leave the gauges untouched.
//!
//! **Intentional staleness of `faucet_pipeline_seconds_since_last_bookmark`.**
//! That gauge is computed *only at bookmark-write time*, so it captures the lag
//! observed at the moment the last bookmark was persisted — it does **not**
//! advance on its own between writes. When a pipeline stalls (no new bookmarks),
//! this gauge freezes at its last value rather than climbing, by design: it
//! reports recency-at-write, not live lag, and recomputing it on every scrape
//! would require a background ticker this layer intentionally avoids. For live
//! stall detection, alert on the freshness of the absolute timestamp instead —
//! `time() - faucet_pipeline_last_bookmark_unix_seconds` grows continuously
//! while a pipeline is wedged (Prometheus evaluates `time()` at scrape time, so
//! no in-process ticker is needed).

use crate::observability::labels::Labels;
use metrics::{Label, SharedString, gauge};
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

/// Update the bookmark-lag gauges if the bookmark is a parseable RFC3339
/// timestamp. Returns `true` if the gauges were updated, `false` otherwise.
pub fn update_bookmark_lag(bookmark: &Value, labels: &Labels) -> bool {
    let s = match bookmark {
        Value::String(s) => s,
        Value::Object(o) => match o.get("value") {
            Some(Value::String(s)) => s,
            _ => return false,
        },
        _ => return false,
    };
    let ts = match parse_rfc3339_to_unix_seconds(s) {
        Some(v) => v,
        None => return false,
    };
    let label_vec = vec![
        Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
        Label::new("row", SharedString::from(labels.row.to_string())),
    ];
    gauge!(
        "faucet_pipeline_last_bookmark_unix_seconds",
        label_vec.clone()
    )
    .set(ts);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(ts);
    gauge!("faucet_pipeline_seconds_since_last_bookmark", label_vec).set((now - ts).max(0.0));
    true
}

/// Strict RFC3339 parser yielding a unix-seconds `f64`. Accepts
/// `YYYY-MM-DDTHH:MM:SS(.frac)?(Z|±HH:MM)`. Returns `None` on any
/// deviation, including invalid dates (e.g. Feb 30, month > 12).
///
/// Hand-rolled to avoid pulling in `chrono` or `time` for a single parse.
fn parse_rfc3339_to_unix_seconds(s: &str) -> Option<f64> {
    let bytes = s.as_bytes();
    if bytes.len() < 20 {
        return None;
    }
    let year: i64 = s.get(0..4)?.parse().ok()?;
    if !(1970..=9999).contains(&year) {
        return None;
    }
    let month: u32 = s.get(5..7)?.parse().ok()?;
    let day: u32 = s.get(8..10)?.parse().ok()?;
    if &bytes[4..5] != b"-" || &bytes[7..8] != b"-" || (bytes[10] != b'T' && bytes[10] != b't') {
        return None;
    }
    let hour: u32 = s.get(11..13)?.parse().ok()?;
    let minute: u32 = s.get(14..16)?.parse().ok()?;
    let second: u32 = s.get(17..19)?.parse().ok()?;
    if &bytes[13..14] != b":" || &bytes[16..17] != b":" {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        // 60 allowed for leap second; treat as 59 below.
        return None;
    }
    let mut idx = 19;
    let mut frac = 0.0_f64;
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == start {
            return None;
        }
        let f: f64 = s.get(start..idx)?.parse().ok()?;
        frac = f / 10f64.powi((idx - start) as i32);
    }
    if idx >= bytes.len() {
        return None;
    }
    let (offset_seconds, idx) = match bytes[idx] {
        b'Z' | b'z' => (0i64, idx + 1),
        b'+' | b'-' => {
            let sign = if bytes[idx] == b'-' { -1i64 } else { 1i64 };
            if idx + 6 > bytes.len() {
                return None;
            }
            let oh: i64 = s.get(idx + 1..idx + 3)?.parse().ok()?;
            let om: i64 = s.get(idx + 4..idx + 6)?.parse().ok()?;
            if &bytes[idx + 3..idx + 4] != b":" {
                return None;
            }
            (sign * (oh * 3600 + om * 60), idx + 6)
        }
        _ => return None,
    };
    if idx != bytes.len() {
        return None;
    }
    let days = days_from_ymd(year, month, day)?;
    let clipped_second = second.min(59);
    let secs =
        days * 86_400 + (hour as i64) * 3600 + (minute as i64) * 60 + (clipped_second as i64)
            - offset_seconds;
    Some(secs as f64 + frac)
}

/// Days since 1970-01-01 for the given Y/M/D. Returns `None` for invalid dates.
fn days_from_ymd(year: i64, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || day < 1 {
        return None;
    }
    const DAYS_IN_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let is_leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let max = if month == 2 && is_leap {
        29
    } else {
        DAYS_IN_MONTH[(month - 1) as usize]
    };
    if day > max {
        return None;
    }
    let mut days: i64 = 0;
    for y in 1970..year {
        days += if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
            366
        } else {
            365
        };
    }
    for m in 1..month {
        days += if m == 2 && is_leap {
            29
        } else {
            DAYS_IN_MONTH[(m - 1) as usize]
        } as i64;
    }
    days += (day - 1) as i64;
    Some(days)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_utc_z() {
        let v = parse_rfc3339_to_unix_seconds("2026-05-23T10:00:00Z").unwrap();
        // 2026-05-23T10:00:00Z = 1779530400 (verified via python's datetime).
        assert!((v - 1779530400.0).abs() < 1.0);
    }

    #[test]
    fn parses_with_offset() {
        let v_utc = parse_rfc3339_to_unix_seconds("2026-05-23T10:00:00Z").unwrap();
        let v_pst = parse_rfc3339_to_unix_seconds("2026-05-23T03:00:00-07:00").unwrap();
        assert!((v_utc - v_pst).abs() < 1.0);
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_rfc3339_to_unix_seconds("not-a-date").is_none());
        assert!(parse_rfc3339_to_unix_seconds("2026-13-01T00:00:00Z").is_none());
        assert!(parse_rfc3339_to_unix_seconds("2026-02-30T00:00:00Z").is_none());
        assert!(parse_rfc3339_to_unix_seconds("").is_none());
    }

    #[test]
    fn update_skips_non_string_bookmarks() {
        let l = Labels::new("p", "r", "rid");
        assert!(!update_bookmark_lag(&Value::from(42), &l));
        assert!(!update_bookmark_lag(&Value::Null, &l));
    }

    #[test]
    fn update_returns_true_on_timestamp() {
        let l = Labels::new("p", "r", "rid");
        assert!(update_bookmark_lag(
            &Value::String("2026-05-23T10:00:00Z".to_string()),
            &l,
        ));
    }

    #[test]
    fn update_reads_a_cursor_bookmark_value() {
        let l = Labels::new("p", "r", "rid");
        assert!(update_bookmark_lag(
            &serde_json::json!({"value": "2026-10-01T00:00:00Z", "boundary": ["abc"]}),
            &l,
        ));
        assert!(!update_bookmark_lag(
            &serde_json::json!({"value": 17, "boundary": null}),
            &l,
        ));
        assert!(!update_bookmark_lag(&serde_json::json!({"lsn": "0/1"}), &l));
    }
}

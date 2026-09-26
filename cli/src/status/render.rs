//! The human `faucet status` screen.

use super::{Health, RowStatus, StatusReport, bookmark_text};
use chrono::{DateTime, Utc};

/// `2h`, `3d`, `45m`, `12s`.
pub fn age(secs: i64) -> String {
    let secs = secs.max(0);
    match secs {
        s if s < 60 => format!("{s}s"),
        s if s < 3_600 => format!("{}m", s / 60),
        s if s < 86_400 => format!("{}h", s / 3_600),
        s => format!("{}d", s / 86_400),
    }
}

fn clip(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

fn when(at: DateTime<Utc>, now: DateTime<Utc>) -> String {
    format!(
        "{} ({})",
        at.format("%Y-%m-%d %H:%M"),
        age((now - at).num_seconds())
    )
}

fn health_label(h: Health) -> String {
    match h {
        Health::Failed | Health::Degraded => h.as_str().to_ascii_uppercase(),
        other => other.as_str().to_string(),
    }
}

fn cells(r: &RowStatus, now: DateTime<Utc>) -> [String; 7] {
    let dlq = if !r.dlq.configured {
        "—".to_string()
    } else if r.dlq.readable {
        r.dlq.count.to_string()
    } else {
        "?".to_string()
    };
    [
        r.row.clone(),
        health_label(r.health),
        r.last_success
            .as_ref()
            .map(|s| when(s.at, now))
            .unwrap_or_else(|| "never".into()),
        r.bookmark
            .as_ref()
            .map(|b| clip(&bookmark_text(b), 28))
            .unwrap_or_else(|| "—".into()),
        r.lag
            .as_ref()
            .map(|l| clip(&l.to_string(), 12))
            .unwrap_or_else(|| "—".into()),
        dlq,
        clip(&r.resume, 60),
    ]
}

/// Render the report as the one-screen table.
pub fn render(report: &StatusReport) -> String {
    let now = report.generated_at;
    let header = [
        "row",
        "status",
        "last success",
        "bookmark",
        "lag",
        "dlq",
        "next run resumes at",
    ];
    let rows: Vec<[String; 7]> = report.rows.iter().map(|r| cells(r, now)).collect();
    let mut widths: Vec<usize> = header.iter().map(|h| h.chars().count()).collect();
    for r in &rows {
        for (i, c) in r.iter().enumerate() {
            widths[i] = widths[i].max(c.chars().count());
        }
    }
    let line = |cols: Vec<&str>| -> String {
        let mut s = String::from("  ");
        for (i, c) in cols.iter().enumerate() {
            if i + 1 == cols.len() {
                s.push_str(c);
            } else {
                s.push_str(&format!("{c:<w$}  ", w = widths[i]));
            }
        }
        s.trim_end().to_string() + "\n"
    };
    let state = if report.state.kinds.is_empty() {
        "none".to_string()
    } else {
        report.state.kinds.join(", ")
    };
    let mut out = format!(
        "pipeline {} ({} {}{}) — {}    state: {}\n",
        report.pipeline,
        report.rows.len(),
        if report.topology { "sink node" } else { "row" },
        if report.rows.len() == 1 { "" } else { "s" },
        health_label(report.health),
        state
    );
    if let Some(note) = &report.state.note {
        out.push_str(&format!("  note: {note}\n"));
    }
    out.push_str(&line(header.to_vec()));
    let indent = " ".repeat(2 + widths[0] + 2);
    for (r, c) in report.rows.iter().zip(&rows) {
        out.push_str(&line(c.iter().map(String::as_str).collect()));
        for d in details(r, now) {
            out.push_str(&format!("{indent}└ {d}\n"));
        }
    }
    out
}

/// The indented lines under a row.
fn details(r: &RowStatus, now: DateTime<Utc>) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(l) = &r.running {
        out.push(format!(
            "running: run {} (pid {}{}) since {}",
            l.run_id,
            l.pid,
            l.host
                .as_deref()
                .map(|h| format!(" on {h}"))
                .unwrap_or_default(),
            when(l.acquired_at, now)
        ));
    }
    if let Some(f) = &r.last_failure
        && r.health == Health::Failed
    {
        out.push(format!(
            "last error: {}{} ({}{}){}",
            f.error_kind
                .as_deref()
                .map(|k| format!("{k}: "))
                .unwrap_or_default(),
            clip(f.error.as_deref().unwrap_or("failed"), 160),
            f.at.format("%Y-%m-%d %H:%M"),
            f.run_id
                .as_deref()
                .map(|id| format!(", run {}", clip(id, 12)))
                .unwrap_or_default(),
            if r.consecutive_failures > 1 {
                format!(" · {} consecutive failures", r.consecutive_failures)
            } else {
                String::new()
            }
        ));
    }
    let mut marks = Vec::new();
    for v in &r.sla {
        marks.push(format!("SLA {}: {}", v.kind, v.message));
    }
    if let Some(eo) = &r.exactly_once {
        let sink = eo
            .sink
            .as_ref()
            .and_then(|s| s.seq)
            .map(|s| s.to_string())
            .unwrap_or_else(|| "—".into());
        marks.push(format!(
            "exactly-once: state seq {} · sink seq {} · {:?} → next run trusts the {}",
            eo.state_seq, sink, eo.agreement, eo.trusted
        ));
        if let Some(e) = &eo.probe_error {
            marks.push(format!("watermark probe failed: {e}"));
        }
    }
    if r.dlq.count > 0 {
        marks.push(format!(
            "DLQ: {} record(s){}",
            r.dlq.count,
            r.dlq
                .oldest
                .map(|o| format!(", oldest {}", when(o, now)))
                .unwrap_or_default()
        ));
    }
    if let Some(n) = &r.dlq.note {
        marks.push(format!("DLQ: {n}"));
    }
    if let Some(p) = &r.profiling
        && p.drift > 0
    {
        marks.push(format!(
            "profiling: {} drift finding(s) in the latest run",
            p.drift
        ));
    }
    if r.rollback.undoable_runs > 0 {
        marks.push(format!(
            "rollback: {} undoable run(s), newest {}",
            r.rollback.undoable_runs,
            r.rollback.newest.as_deref().unwrap_or("?")
        ));
    }
    if let Some(o) = &r.overwrite_staging {
        marks.push(format!("overwrite staging: {} — {}", o.state, o.note));
    }
    for c in &r.children {
        marks.push(format!(
            "children '{}': {} bookmark(s), {} failed — worst {}",
            c.row,
            c.bookmarks,
            c.failed,
            c.worst.as_str()
        ));
    }
    for reason in r.reasons.iter().filter(|x| {
        x.contains("unknown between runs")
            || x.contains("without releasing")
            || x.contains("in flight")
    }) {
        marks.push(reason.clone());
    }
    for e in &r.errors {
        marks.push(format!("unreadable: {e}"));
    }
    out.extend(marks);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ages_and_clipping() {
        assert_eq!(age(-5), "0s");
        assert_eq!(age(59), "59s");
        assert_eq!(age(120), "2m");
        assert_eq!(age(7_200), "2h");
        assert_eq!(age(200_000), "2d");
        assert_eq!(clip("abcdef", 4), "abc…");
        assert_eq!(clip("abc", 4), "abc");
    }
}

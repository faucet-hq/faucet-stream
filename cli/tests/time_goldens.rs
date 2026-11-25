//! Frozen renderings of the CLI's dates, clocks, windows and zones (#1093
//! stage 0: F3 to F8 and F14). They must pass unchanged when the library under
//! `faucet_core::time` changes. Schedule ticks on DST days are not frozen here:
//! today's are wrong there (#1111), so this file pins only the days croner 3
//! and 5 agree on.

use chrono::{DateTime, FixedOffset};
use serde_json::json;

fn snapshot(lines: &[String], expected: &str) {
    let got = lines.join("\n");
    assert_eq!(got, expected.trim(), "actual:\n{got}");
}

fn clock(text: &str) -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339(text).unwrap()
}

const NAMED: &[&str] = &[
    "date", "datetime", "iso", "year", "month", "day", "hour", "minute", "second", "unix",
];

#[test]
fn f5_now_named_tokens() {
    let mut out = Vec::new();
    for c in [
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.5+05:45",
        "2026-01-02T03:04:05.123456789-03:00",
        "0999-02-03T04:05:06+00:00",
        "1969-12-31T23:59:59.999-12:00",
    ] {
        for t in NAMED {
            let r = faucet_cli::interpolate::resolve_now(&format!("${{now.{t}}}"), clock(c));
            out.push(format!("{c} now.{t} = {:?}", r.map_err(|e| e.to_string())));
        }
    }
    out.push(format!(
        "path = {:?}",
        faucet_cli::interpolate::resolve_now(
            "s3://b/dt=${now.date}/h=${now.hour}/part.jsonl",
            clock("2026-11-25T10:00:00+01:00")
        )
        .map_err(|e| e.to_string())
    ));
    out.push(format!(
        "unknown = {:?}",
        faucet_cli::interpolate::resolve_now("${now.week}", clock("2026-11-25T10:00:00Z"))
            .map_err(|e| e.to_string())
    ));
    snapshot(&out, include_str!("fixtures/time_goldens/f5_now.txt"));
}

#[test]
fn f6_now_strftime_grammar() {
    let mut out = Vec::new();
    let specs = [
        "%Y",
        "%C",
        "%y",
        "%m",
        "%b",
        "%B",
        "%h",
        "%d",
        "%e",
        "%a",
        "%A",
        "%w",
        "%u",
        "%U",
        "%W",
        "%G",
        "%g",
        "%V",
        "%j",
        "%D",
        "%x",
        "%F",
        "%v",
        "%H",
        "%k",
        "%I",
        "%l",
        "%P",
        "%p",
        "%M",
        "%S",
        "%f",
        "%.f",
        "%.3f",
        "%.6f",
        "%.9f",
        "%3f",
        "%6f",
        "%9f",
        "%R",
        "%T",
        "%X",
        "%r",
        "%Z",
        "%z",
        "%:z",
        "%::z",
        "%:::z",
        "%#z",
        "%c",
        "%+",
        "%s",
        "%t",
        "%n",
        "%%",
        "%-d",
        "%_d",
        "%0d",
        "%-m",
        "%_H",
        "%-j",
        "%Q",
        "%N",
        "%E",
        "%O",
        "%q",
        "%i",
        "%J",
        "%L",
        "%K",
        "%o",
        "%1",
        "%",
        "%y%m%dT%H",
        "literal",
    ];
    for c in ["2026-11-05T07:08:09.5-03:00", "2026-11-25T23:00:00Z"] {
        for s in specs {
            let r =
                faucet_cli::interpolate::resolve_now(&format!("${{now.strftime.{s}}}"), clock(c));
            out.push(format!("{c} {s:?} = {:?}", r.map_err(|e| e.to_string())));
        }
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f6_strftime.txt"));
}

#[test]
fn f8_backfill_windows_across_dst() {
    use faucet_cli::backfill::plan::{
        parse_boundary, parse_window, plan_windows, range_hash, substitute_unit_tokens,
    };
    use faucet_cli::backfill::spec::parse_timezone;
    let mut out = Vec::new();
    let cases = [
        ("Australia/Lord_Howe", "2025-04-05", "2025-04-08", "1d"),
        ("Australia/Lord_Howe", "2025-10-04", "2025-10-06", "6h"),
        ("America/Santiago", "2025-04-05", "2025-04-07", "1d"),
        ("America/Santiago", "2025-09-06", "2025-09-08", "1d"),
        (
            "America/Santiago",
            "2025-09-06T22:00:00-04:00",
            "2025-09-07T03:00:00-03:00",
            "1h",
        ),
        ("Europe/London", "2025-03-29", "2025-04-01", "1d"),
        ("Europe/London", "2025-10-25", "2025-10-28", "24h"),
        ("America/New_York", "2025-11-01", "2025-11-15", "1w"),
    ];
    for (zone, from, to, window) in cases {
        let tz = parse_timezone(zone).unwrap();
        let units = plan_windows(
            parse_boundary(from, tz).unwrap(),
            parse_boundary(to, tz).unwrap(),
            Some(parse_window(window).unwrap()),
            tz,
        )
        .unwrap();
        out.push(format!(
            "{zone} {from}..{to} by {window}: {} units",
            units.len()
        ));
        for u in &units {
            let mut v = json!(
                "${backfill.unit}|${backfill.start}|${backfill.end}|${backfill.start_date}|${backfill.end_date}|${backfill.start_unix}|${backfill.end_unix}"
            );
            substitute_unit_tokens(&mut v, u).unwrap();
            out.push(format!("  {}", v.as_str().unwrap()));
        }
    }
    out.push(format!(
        "hash = {}",
        range_hash("orders|2025-01-01|2025-02-01|86400|UTC")
    ));
    snapshot(&out, include_str!("fixtures/time_goldens/f8_windows.txt"));
}

#[test]
fn f14_accepted_zone_names() {
    let mut out = Vec::new();
    for name in [
        "UTC",
        "Etc/UTC",
        "utc",
        "EST",
        "US/Eastern",
        "Europe/Kyiv",
        "Europe/Kiev",
        "Asia/Calcutta",
        "Asia/Kolkata",
        "europe/london",
        "Europe/London ",
        "GMT+5",
        "Etc/GMT+5",
        "Mars/Olympus",
        "",
    ] {
        let ok = faucet_cli::backfill::spec::parse_timezone(name).is_ok();
        out.push(format!("{name:?} = {ok}"));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f14_zones.txt"));
}

#[cfg(feature = "schedule")]
#[test]
fn f7_schedule_ticks_on_days_croner_3_and_5_agree() {
    use faucet_cli::schedule::compiled::CompiledSchedule;
    let mut out = Vec::new();
    let cases = [
        ("0 0 * * *", "America/New_York", "2025-03-08T12:00:00Z", 4),
        ("30 2 * * *", "America/New_York", "2025-03-08T12:00:00Z", 3),
        ("30 1 * * *", "America/New_York", "2025-11-01T12:00:00Z", 3),
        ("0 * * * *", "Europe/London", "2025-03-30T00:00:00Z", 4),
        (
            "0 0 * * *",
            "Australia/Lord_Howe",
            "2025-04-04T12:00:00Z",
            4,
        ),
        (
            "0 0 * * *",
            "Australia/Lord_Howe",
            "2025-10-03T12:00:00Z",
            4,
        ),
        ("0 0 * * *", "America/Santiago", "2025-09-05T12:00:00Z", 4),
        ("*/30 * * * *", "Asia/Kolkata", "2025-03-30T00:00:00Z", 4),
        ("0 0 30 * *", "UTC", "2025-01-01T00:00:00Z", 3),
        ("15 9 * * 1-5", "Europe/Berlin", "2025-11-21T00:00:00Z", 3),
    ];
    for (cron, tz, after, n) in cases {
        let spec: faucet_cli::schedule::spec::ScheduleSpec =
            serde_json::from_value(json!({ "cron": cron, "timezone": tz })).unwrap();
        let c = CompiledSchedule::compile(&spec).unwrap();
        let start: chrono::DateTime<chrono::Utc> = after.parse().unwrap();
        let ticks: Vec<String> = std::iter::successors(c.next_after(start), |t| c.next_after(*t))
            .take(n)
            .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
            .collect();
        out.push(format!("{cron:?} {tz} after {after}: {}", ticks.join(" ")));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f7_ticks.txt"));
}

#[cfg(feature = "serve")]
#[test]
fn f3_history_timestamps() {
    use faucet_cli::serve::history::sql::{fmt_ts, parse_ts};
    let mut out = Vec::new();
    for t in [
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.5Z",
        "2026-11-25T10:00:00.123456789+02:00",
        "0001-01-01T00:00:00Z",
        "9999-12-31T23:59:59.999999999Z",
    ] {
        let s = fmt_ts(DateTime::parse_from_rfc3339(t).unwrap().to_utc());
        out.push(format!("fmt_ts {t} = {s} ({} chars)", s.len()));
        out.push(format!("parse_ts back = {}", fmt_ts(parse_ts(&s))));
    }
    for raw in [
        "2026-11-25T10:00:00+02:00",
        "2026-11-25t10:00:00z",
        "2026-11-25 10:00:00Z",
    ] {
        out.push(format!("parse_ts {raw} = {}", fmt_ts(parse_ts(raw))));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f3_history.txt"));
    let before = chrono::Utc::now();
    let fallback = parse_ts("not a timestamp");
    assert!(
        fallback >= before && fallback <= chrono::Utc::now(),
        "parse_ts falls back to now today (#1112)"
    );
}

#[cfg(feature = "serve")]
#[test]
fn f4_stored_records_keep_their_timestamp_spelling() {
    use faucet_cli::serve::history::{AuditEntry, RunRecord};
    let mut out = Vec::new();
    let spellings = [
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.500Z",
        "2026-11-25T10:00:00.123456Z",
        "2026-11-25T10:00:00.123456789Z",
        "2026-11-25T12:00:00+02:00",
        "2026-11-25 10:00:00Z",
        "2026-11-25t10:00:00z",
    ];
    let run: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/history/run-record-1.17.0.json")).unwrap();
    for s in spellings {
        let mut v = run.clone();
        v["submitted_at"] = json!(s);
        v["started_at"] = json!(s);
        v["finished_at"] = json!(s);
        let rec: RunRecord = serde_json::from_value(v).unwrap();
        let back = serde_json::to_value(&rec).unwrap();
        out.push(format!(
            "RunRecord {s} -> {} {} {}",
            back["submitted_at"], back["started_at"], back["finished_at"]
        ));
        let audit = json!({
            "id": "0193a6f2-5c1e-7b2a-9c4d-2f1e0a9b8c7d",
            "timestamp": s,
            "principal": "p",
            "role": "admin",
            "action": "run.submit",
            "result": "ok"
        });
        let entry: AuditEntry = serde_json::from_value(audit).unwrap();
        out.push(format!(
            "AuditEntry {s} -> {}",
            serde_json::to_value(&entry).unwrap()["timestamp"]
        ));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f4_records.txt"));
}

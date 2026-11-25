//! Frozen renderings of every date and time faucet-core prints or parses
//! (#1093 stage 0, items F1, F11, F12). They must pass unchanged when the
//! library under `faucet_core::time` changes.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use faucet_core::fidelity::{self, TimePrecision};
use faucet_core::replication::{BindFormat, format_instant, parse_instant};
use serde_json::json;

const PRECISIONS: [TimePrecision; 4] = [
    TimePrecision::Seconds,
    TimePrecision::Millis,
    TimePrecision::Micros,
    TimePrecision::Nanos,
];

fn utc(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text).unwrap().to_utc()
}

fn snapshot(lines: &[String], expected: &str) {
    let got = lines.join("\n");
    assert_eq!(got, expected.trim(), "actual:\n{got}");
}

#[test]
fn f1_fidelity_renderings() {
    let mut out = Vec::new();
    let instants = [
        "0001-01-01T00:00:00Z",
        "1969-12-31T23:59:59.999999999Z",
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.000000001Z",
        "2026-11-25T10:00:00.000001Z",
        "2026-11-25T10:00:00.001Z",
        "2016-12-31T23:59:60.5Z",
        "9999-12-31T23:59:59.999999999Z",
    ];
    let offsets = [0, 14 * 3600, -12 * 3600, 5 * 3600 + 45 * 60];
    for text in instants {
        let t = utc(text);
        for p in PRECISIONS {
            out.push(format!(
                "timestamp {text} {p:?} = {}",
                fidelity::timestamp(&t, p)
            ));
            out.push(format!(
                "naive {text} {p:?} = {}",
                fidelity::timestamp_naive(&t.naive_utc(), p)
            ));
            out.push(format!(
                "time {text} {p:?} = {}",
                fidelity::time(&t.time(), p)
            ));
        }
        for off in offsets {
            let z = FixedOffset::east_opt(off).unwrap();
            out.push(format!(
                "zoned {text} {off} = {}",
                fidelity::timestamp(&t.with_timezone(&z), TimePrecision::Micros)
            ));
        }
        out.push(format!("date {text} = {}", fidelity::date(&t.date_naive())));
    }
    let far = [
        NaiveDate::from_ymd_opt(10000, 1, 1).unwrap(),
        NaiveDate::from_ymd_opt(-1, 6, 1).unwrap(),
        NaiveDate::from_ymd_opt(0, 1, 1).unwrap(),
    ];
    for d in far {
        let dt = NaiveDateTime::new(d, NaiveTime::MIN);
        out.push(format!("far date {d:?} = {}", fidelity::date(&d)));
        out.push(format!(
            "far naive {d:?} = {}",
            fidelity::timestamp_naive(&dt, TimePrecision::Seconds)
        ));
        out.push(format!(
            "far timestamp {d:?} = {}",
            fidelity::timestamp(&dt.and_utc(), TimePrecision::Seconds)
        ));
    }
    for (value, unit) in [
        (0, TimePrecision::Seconds),
        (-1, TimePrecision::Millis),
        (1_795_600_800_123_456, TimePrecision::Micros),
        (1_795_600_800_123_456_789, TimePrecision::Nanos),
        (i64::MAX, TimePrecision::Seconds),
    ] {
        out.push(format!(
            "epoch {value} {unit:?} naive = {:?}",
            fidelity::timestamp_from_epoch(value, unit, None).map_err(|e| e.to_string())
        ));
        out.push(format!(
            "epoch {value} {unit:?} +05:30 = {:?}",
            fidelity::timestamp_from_epoch(value, unit, FixedOffset::east_opt(19_800))
                .map_err(|e| e.to_string())
        ));
    }
    for s in [
        "2026-11-25T10:00:00+02:00",
        "2026-11-25 10:00:00.5-03:30",
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00",
        "garbage",
    ] {
        out.push(format!(
            "offset_instant_utc {s:?} = {:?}",
            fidelity::offset_instant_utc(s)
        ));
    }
    for s in ["10:00:00+02:00", "23:30:00-01:00", "10:00:00", "x"] {
        out.push(format!(
            "offset_time_utc {s:?} = {:?}",
            fidelity::offset_time_utc(s)
        ));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f1_fidelity.txt"));
}

#[test]
fn f11_window_binds_and_instants() {
    let mut out = Vec::new();
    for text in [
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.123456789Z",
        "1969-12-31T23:59:59.5Z",
    ] {
        let t = utc(text);
        for f in [
            BindFormat::Raw,
            BindFormat::Iso8601,
            BindFormat::EpochS,
            BindFormat::EpochMs,
            BindFormat::Date,
        ] {
            out.push(format!(
                "format_instant {text} {f:?} = {}",
                format_instant(t, f)
            ));
        }
    }
    for v in [
        json!("2026-11-25T10:00:00Z"),
        json!("2026-11-25T10:00:00.5+02:00"),
        json!("2026-11-25"),
        json!("2026-11-25 10:00:00"),
        json!(1_795_600_800),
        json!(1_795_600_800_123i64),
        json!("1795600800"),
        json!("not a time"),
        json!(null),
    ] {
        out.push(format!(
            "parse_instant {v} = {:?}",
            parse_instant(&v)
                .map(|d| d.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
                .map_err(|e| e.to_string())
        ));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f11_binds.txt"));
}

#[cfg(feature = "transform-cast")]
#[test]
fn f12_parse_sides() {
    let mut out = Vec::new();
    let cast = faucet_core::transform::compile(&faucet_core::transform::RecordTransform::Cast {
        fields: [(
            "ts".to_string(),
            faucet_core::transform::CastType::Timestamp,
        )]
        .into(),
        on_error: faucet_core::transform::CastOnError::Error,
    })
    .unwrap();
    for s in [
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.5Z",
        "2026-11-25T10:00:00.120+02:00",
        "2026-11-25t10:00:00z",
        "2026-11-25 10:00:00Z",
        "2026-11-25T10:00:00",
        "2026-11-25",
    ] {
        let r = faucet_core::transform::apply_all(json!({ "ts": s }), std::slice::from_ref(&cast));
        out.push(format!(
            "cast {s:?} = {:?}",
            r.map(|v| v["ts"].clone()).map_err(|e| e.to_string())
        ));
    }
    let norm = faucet_core::diff::Normalizer::default();
    let canon = |v: &str| {
        faucet_core::diff::row_hash(&json!({ "k": 1, "t": v }), &["k".into()], None, &[], &norm)
    };
    let base = canon("2026-11-25T10:00:00Z");
    for s in [
        "2026-11-25T11:00:00+01:00",
        "2026-11-25T10:00:00.000Z",
        "2026-11-25 10:00:00",
        "2026-11-25T10:00:00",
        "2026-11-25 10:00:00+00",
        "1795600800",
        "2026-11-25",
    ] {
        out.push(format!("normalizer {s:?} same_as_z = {}", canon(s) == base));
    }
    let mut headers = reqwest::header::HeaderMap::new();
    for raw in [
        "120",
        "1.5",
        "Wed, 21 Oct 2015 07:28:00 GMT",
        "Wednesday, 21-Oct-15 07:28:00 GMT",
        "Wed Oct 21 07:28:00 2015",
        "garbage",
        "-5",
    ] {
        headers.insert(reqwest::header::RETRY_AFTER, raw.parse().unwrap());
        out.push(format!(
            "retry_after {raw:?} = {:?}",
            faucet_core::util::retry_after(&headers)
        ));
    }
    snapshot(&out, include_str!("fixtures/time_goldens/f12_parse.txt"));
}

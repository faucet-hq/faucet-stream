//! Golden tick tables for #1111: hand-computed UTC instants, plus a
//! differential check against the brute-force oracle in `dst_oracle`. Both go
//! through `CompiledSchedule` only, never the cron library, so a change of
//! library or backend reuses them unchanged.

use super::compiled::CompiledSchedule;
use super::dst_oracle::{self, Pattern};
use chrono::{DateTime, Utc};
use faucet_core::time::ChronoZone as Tz;

fn compiled(cron: &str, tz: &str) -> CompiledSchedule {
    let spec = crate::yaml::from_str(&format!("cron: \"{cron}\"\ntimezone: \"{tz}\"\n")).unwrap();
    CompiledSchedule::compile(&spec).unwrap()
}

fn at(s: &str) -> DateTime<Utc> {
    s.parse().unwrap()
}

fn ticks(cron: &str, tz: &str, after: &str, n: usize) -> Vec<String> {
    let c = compiled(cron, tz);
    let mut out = Vec::new();
    let mut t = at(after);
    while out.len() < n {
        t = c.try_next_after(t).unwrap().unwrap();
        out.push(t.format("%Y-%m-%dT%H:%MZ").to_string());
    }
    out
}

/// (cron, zone, start, expected UTC ticks). Comments give the local times.
const GOLDEN: &[(&str, &str, &str, &[&str])] = &[
    // London spring-forward 2025-03-30 01:00 GMT -> 02:00 BST: midnight runs once.
    (
        "0 0 * * *",
        "Europe/London",
        "2025-03-28T12:00:00Z",
        &[
            "2025-03-29T00:00Z",
            "2025-03-30T00:00Z",
            "2025-03-30T23:00Z",
            "2025-03-31T23:00Z",
        ],
    ),
    // Day-of-month landing on that day.
    (
        "0 0 30 * *",
        "Europe/London",
        "2025-03-29T00:00:00Z",
        &["2025-03-30T00:00Z", "2025-04-29T23:00Z"],
    ),
    // New York fall-back 2025-11-02 02:00 EDT -> 01:00 EST; 01:xx repeats
    // (first pass 05:xxZ, second 06:xxZ). Step and `*` jobs run in both passes.
    (
        "*/30 * * * *",
        "America/New_York",
        "2025-11-02T04:45:00Z",
        &[
            "2025-11-02T05:00Z",
            "2025-11-02T05:30Z",
            "2025-11-02T06:00Z",
            "2025-11-02T06:30Z",
            "2025-11-02T07:00Z",
            "2025-11-02T07:30Z",
        ],
    ),
    (
        "0 * * * *",
        "America/New_York",
        "2025-11-02T04:30:00Z",
        &[
            "2025-11-02T05:00Z",
            "2025-11-02T06:00Z",
            "2025-11-02T07:00Z",
            "2025-11-02T08:00Z",
        ],
    ),
    // Lists and ranges run once, in the first pass.
    (
        "0 1,13 * * *",
        "America/New_York",
        "2025-11-02T04:00:00Z",
        &[
            "2025-11-02T05:00Z",
            "2025-11-02T18:00Z",
            "2025-11-03T06:00Z",
        ],
    ),
    (
        "0,30 1 * * *",
        "America/New_York",
        "2025-11-02T04:00:00Z",
        &[
            "2025-11-02T05:00Z",
            "2025-11-02T05:30Z",
            "2025-11-03T06:00Z",
            "2025-11-03T06:30Z",
        ],
    ),
    (
        "15 1,2 * * *",
        "America/New_York",
        "2025-11-02T04:00:00Z",
        &[
            "2025-11-02T05:15Z",
            "2025-11-02T07:15Z",
            "2025-11-03T06:15Z",
        ],
    ),
    // Starting inside the second pass (01:10 EST = 06:10Z).
    (
        "0,30 1 * * *",
        "America/New_York",
        "2025-11-02T06:10:00Z",
        &["2025-11-03T06:00Z", "2025-11-03T06:30Z"],
    ),
    (
        "30 1 * * *",
        "America/New_York",
        "2025-11-02T06:10:00Z",
        &["2025-11-03T06:30Z", "2025-11-04T06:30Z"],
    ),
    (
        "*/30 * * * *",
        "America/New_York",
        "2025-11-02T06:10:00Z",
        &["2025-11-02T06:30Z", "2025-11-02T07:00Z"],
    ),
    // New York spring-forward 2025-03-09 02:00 EST -> 03:00 EDT.
    // Interval jobs skip the missing times.
    (
        "0 2,14 * * *",
        "America/New_York",
        "2025-03-09T05:00:00Z",
        &[
            "2025-03-09T18:00Z",
            "2025-03-10T06:00Z",
            "2025-03-10T18:00Z",
        ],
    ),
    (
        "*/30 2 * * *",
        "America/New_York",
        "2025-03-08T05:00:00Z",
        &[
            "2025-03-08T07:00Z",
            "2025-03-08T07:30Z",
            "2025-03-10T06:00Z",
            "2025-03-10T06:30Z",
        ],
    ),
    (
        "58 1-2 * * *",
        "America/New_York",
        "2025-03-09T05:00:00Z",
        &[
            "2025-03-09T06:58Z",
            "2025-03-10T05:58Z",
            "2025-03-10T06:58Z",
            "2025-03-11T05:58Z",
        ],
    ),
    // A fixed-time job in the gap runs once at its edge, 03:00 EDT.
    (
        "30 2 * * *",
        "America/New_York",
        "2025-03-09T05:00:00Z",
        &["2025-03-09T07:00Z", "2025-03-10T06:30Z"],
    ),
    // Sydney fall-back 2025-04-06 03:00 AEDT -> 02:00 AEST (02:xx at 15:xxZ, then 16:xxZ).
    (
        "30 2 * * *",
        "Australia/Sydney",
        "2025-04-05T14:00:00Z",
        &["2025-04-05T15:30Z", "2025-04-06T16:30Z"],
    ),
    (
        "0 2,14 * * *",
        "Australia/Sydney",
        "2025-04-05T14:00:00Z",
        &[
            "2025-04-05T15:00Z",
            "2025-04-06T04:00Z",
            "2025-04-06T16:00Z",
        ],
    ),
    // Lord Howe falls back 30 minutes, 02:00 (+11) -> 01:30 (+10:30) at 15:00Z.
    (
        "*/15 * * * *",
        "Australia/Lord_Howe",
        "2025-04-05T14:00:00Z",
        &[
            "2025-04-05T14:15Z",
            "2025-04-05T14:30Z",
            "2025-04-05T14:45Z",
            "2025-04-05T15:00Z",
            "2025-04-05T15:15Z",
            "2025-04-05T15:30Z",
            "2025-04-05T15:45Z",
            "2025-04-05T16:00Z",
        ],
    ),
    // Santiago (southern hemisphere): the gap is at midnight on 2025-09-07
    // (00:00 -04 -> 01:00 -03); 23:xx repeats on 2025-04-05.
    (
        "0 0 * * *",
        "America/Santiago",
        "2025-09-05T12:00:00Z",
        &[
            "2025-09-06T04:00Z",
            "2025-09-07T04:00Z",
            "2025-09-08T03:00Z",
        ],
    ),
    (
        "30 23 * * *",
        "America/Santiago",
        "2025-04-05T12:00:00Z",
        &["2025-04-06T02:30Z", "2025-04-07T03:30Z"],
    ),
    // Apia skipped 2011-12-30. A day-of-month job for the 30th waits for
    // January; a daily job runs at the gap's edge (00:00 on the 31st) and at
    // its own time that day.
    (
        "0 12 30 * *",
        "Pacific/Apia",
        "2011-12-28T00:00:00Z",
        &[
            "2012-01-29T22:00Z",
            "2012-03-29T22:00Z",
            "2012-04-29T23:00Z",
        ],
    ),
    (
        "0 12 * * *",
        "Pacific/Apia",
        "2011-12-28T00:00:00Z",
        &[
            "2011-12-28T22:00Z",
            "2011-12-29T22:00Z",
            "2011-12-30T10:00Z",
            "2011-12-30T22:00Z",
        ],
    ),
    // No daylight saving.
    (
        "0 * * * *",
        "Asia/Kolkata",
        "2026-01-01T00:00:00Z",
        &[
            "2026-01-01T00:30Z",
            "2026-01-01T01:30Z",
            "2026-01-01T02:30Z",
        ],
    ),
    (
        "0 0 31 * *",
        "UTC",
        "2026-01-01T00:00:00Z",
        &[
            "2026-01-31T00:00Z",
            "2026-03-31T00:00Z",
            "2026-05-31T00:00Z",
        ],
    ),
    // Steps without a range (accepted as croner 3 did).
    (
        "/5 * * * *",
        "UTC",
        "2026-11-01T00:00:00Z",
        &[
            "2026-11-01T00:05Z",
            "2026-11-01T00:10Z",
            "2026-11-01T00:15Z",
        ],
    ),
    (
        "5/5 * * * *",
        "UTC",
        "2026-11-01T00:56:00Z",
        &["2026-11-01T01:05Z", "2026-11-01T01:10Z"],
    ),
    (
        "0 3/4 * * *",
        "UTC",
        "2026-11-01T20:00:00Z",
        &[
            "2026-11-01T23:00Z",
            "2026-11-02T03:00Z",
            "2026-11-02T07:00Z",
        ],
    ),
    (
        "0 0 /5 * *",
        "UTC",
        "2026-11-25T00:00:00Z",
        &[
            "2026-11-26T00:00Z",
            "2026-12-01T00:00Z",
            "2026-12-06T00:00Z",
        ],
    ),
    (
        "0 0 1 /2 *",
        "UTC",
        "2026-11-01T00:00:00Z",
        &["2027-01-01T00:00Z", "2027-03-01T00:00Z"],
    ),
    // 2026-11-01 is a Sunday: /2 is Sun, Tue, Thu, Sat.
    (
        "0 0 * * /2",
        "UTC",
        "2026-11-01T00:00:00Z",
        &[
            "2026-11-03T00:00Z",
            "2026-11-05T00:00Z",
            "2026-11-07T00:00Z",
            "2026-11-08T00:00Z",
        ],
    ),
    (
        "0 0 * * SAT-SUN",
        "UTC",
        "2026-11-01T00:00:00Z",
        &[
            "2026-11-07T00:00Z",
            "2026-11-08T00:00Z",
            "2026-11-14T00:00Z",
        ],
    ),
    (
        "0 0 * * 7-7",
        "UTC",
        "2026-11-01T00:00:00Z",
        &["2026-11-08T00:00Z", "2026-11-15T00:00Z"],
    ),
];

#[test]
fn golden_ticks_match() {
    let mut wrong = Vec::new();
    for (cron, tz, after, want) in GOLDEN {
        assert!(!dst_oracle::excluded(tz));
        let got = ticks(cron, tz, after, want.len());
        if got != *want {
            wrong.push(format!(
                "{cron} in {tz} after {after}:\n  want {want:?}\n  got  {got:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn golden_rows_agree_with_the_oracle() {
    for (cron, tz, after, want) in GOLDEN {
        if cron
            .split_whitespace()
            .any(|f| f.chars().any(|c| c.is_ascii_alphabetic()))
            || cron
                .split_whitespace()
                .any(|f| f.starts_with('/') || (f.contains('/') && !f.starts_with('*')))
        {
            continue;
        }
        let zone: Tz = tz.parse().unwrap();
        let from = at(after) + chrono::Duration::minutes(1);
        let last = at(&want[want.len() - 1].replace('Z', ":00Z"));
        let mins = dst_oracle::minutes(zone, from, last + chrono::Duration::minutes(1));
        let got: Vec<String> = dst_oracle::ticks(&Pattern::parse(cron), zone, &mins)
            .iter()
            .map(|t| t.format("%Y-%m-%dT%H:%MZ").to_string())
            .collect();
        assert_eq!(got, *want, "oracle: {cron} in {tz} after {after}");
    }
}

/// `faucet schedule`'s loop through a fall-back and a spring-forward with an
/// injected clock: each tick fires a few seconds late, as dispatch does.
#[test]
fn the_schedule_loop_fires_the_golden_ticks() {
    for (cron, tz, start, want) in [
        (
            "*/30 * * * *",
            "America/New_York",
            "2025-11-02T04:45:00Z",
            [
                "2025-11-02T05:00Z",
                "2025-11-02T05:30Z",
                "2025-11-02T06:00Z",
                "2025-11-02T06:30Z",
            ],
        ),
        (
            "0 1,13 * * *",
            "America/New_York",
            "2025-11-02T04:00:00Z",
            [
                "2025-11-02T05:00Z",
                "2025-11-02T18:00Z",
                "2025-11-03T06:00Z",
                "2025-11-03T18:00Z",
            ],
        ),
        (
            "0 0 * * *",
            "Europe/London",
            "2025-03-28T12:00:00Z",
            [
                "2025-03-29T00:00Z",
                "2025-03-30T00:00Z",
                "2025-03-30T23:00Z",
                "2025-03-31T23:00Z",
            ],
        ),
    ] {
        let c = compiled(cron, tz);
        let mut due = c.try_next_after(at(start)).unwrap().unwrap();
        let mut fired = Vec::new();
        for _ in 0..4 {
            fired.push(due.format("%Y-%m-%dT%H:%MZ").to_string());
            let now = due + chrono::Duration::seconds(3);
            due = c.try_next_due_after_tick(due, now).unwrap().unwrap();
        }
        assert_eq!(fired, want, "{cron} in {tz}");
    }
}

/// Ticks always move forward, whatever the start: every second-pass start of
/// a fall-back included.
#[test]
fn ticks_are_strictly_increasing_from_any_start() {
    for (cron, tz) in [
        ("0,30 1 * * *", "America/New_York"),
        ("*/15 * * * *", "America/New_York"),
        ("30 1 * * *", "America/New_York"),
        ("*/15 * * * *", "Australia/Lord_Howe"),
        ("0 2,14 * * *", "Australia/Sydney"),
    ] {
        let c = compiled(cron, tz);
        let mut start = at("2025-11-02T04:00:00Z");
        if tz.starts_with("Australia") {
            start = at("2025-04-05T13:00:00Z");
        }
        for step in 0..(4 * 60) {
            let after = start + chrono::Duration::minutes(step) + chrono::Duration::seconds(17);
            let next = c.try_next_after(after).unwrap().unwrap();
            assert!(next > after, "{cron} in {tz}: {next} after {after}");
            assert!(c.try_next_after(next).unwrap().unwrap() > next);
        }
    }
}

/// The two runs of a step job in a repeated hour share the local date and
/// hour; `${now.iso}` (offset) and `${now.unix}` tell them apart.
#[test]
fn now_tokens_on_the_two_passes_of_a_repeated_hour() {
    let c = compiled("*/30 * * * *", "America/New_York");
    let first = c.clock_at(at("2025-11-02T05:30:00Z"));
    let second = c.clock_at(at("2025-11-02T06:30:00Z"));
    let r = |s: &str, k| crate::interpolate::resolve_now(s, k).unwrap();
    for token in ["${now.date}", "${now.hour}", "${now.minute}"] {
        assert_eq!(r(token, first), r(token, second), "{token}");
    }
    assert_eq!(r("${now.iso}", first), "2025-11-02T01:30:00-04:00");
    assert_eq!(r("${now.iso}", second), "2025-11-02T01:30:00-05:00");
    assert_ne!(r("${now.unix}", first), r("${now.unix}", second));
}

/// The minimum-interval measure does not depend on the date or the zone.
#[test]
fn the_nominal_gap_is_date_and_zone_independent() {
    let h = |n: u64| Some(std::time::Duration::from_secs(n));
    for (cron, tz, gap) in [
        ("0 1,13 * * *", "America/New_York", h(12 * 3600)),
        ("0,55 1 * * *", "America/New_York", h(55 * 60)),
        ("0 0 * * *", "Europe/London", h(24 * 3600)),
        ("*/30 * * * *", "Australia/Lord_Howe", h(30 * 60)),
        ("*/58 1 * * *", "America/New_York", h(58 * 60)),
        ("58 1-2 * * *", "America/New_York", h(60 * 60)),
        ("0,55 0,23 1,31 1,12 *", "UTC", h(5 * 60)),
        ("0 0 29 2 *", "UTC", h(1461 * 86_400)),
    ] {
        let c = compiled(cron, tz);
        assert_eq!(c.nominal_shortest_gap(), gap, "{cron} in {tz}");
    }
}

fn sweep(zones: &[&str], from: DateTime<Utc>, to: DateTime<Utc>) -> usize {
    const PATTERNS: &[&str] = &[
        "0 0 * * *",
        "30 1 * * *",
        "30 2 * * *",
        "0 2 * * *",
        "15 0 * * *",
        "30 23 * * *",
        "0 * * * *",
        "*/30 * * * *",
        "*/15 * * * *",
        "0 1,13 * * *",
        "0,30 1 * * *",
        "15 1,2 * * *",
        "58 1-2 * * *",
        "0 2,14 * * *",
        "*/30 2 * * *",
        "0 0 30 * *",
    ];
    let patterns: Vec<(&str, Pattern)> = PATTERNS.iter().map(|p| (*p, Pattern::parse(p))).collect();
    let mut wrong = Vec::new();
    let mut checked = 0usize;
    for name in zones {
        if dst_oracle::excluded(name) {
            continue;
        }
        let zone: Tz = name.parse().unwrap();
        let offset = |t: DateTime<Utc>| {
            use chrono::Offset;
            t.with_timezone(&zone).offset().fix().local_minus_utc()
        };
        let mut t = from;
        while t < to {
            let next = t + chrono::Duration::hours(6);
            if offset(next) != offset(t) {
                let w_from = t - chrono::Duration::hours(30);
                let w_to = next + chrono::Duration::hours(30);
                let mins = dst_oracle::minutes(zone, w_from, w_to);
                for (cron, p) in &patterns {
                    let want = dst_oracle::ticks(p, zone, &mins);
                    let c = compiled(cron, name);
                    let mut got = Vec::new();
                    let mut a = w_from - chrono::Duration::seconds(1);
                    while let Some(n) = c
                        .try_next_after(a)
                        .unwrap_or_else(|e| panic!("{cron} in {name} after {a}: {e}"))
                    {
                        if n >= w_to {
                            break;
                        }
                        got.push(n);
                        a = n;
                    }
                    checked += 1;
                    if got != want {
                        wrong.push(format!(
                            "{cron} in {name} around {t}:\n  want {want:?}\n  got  {got:?}"
                        ));
                    }
                }
            }
            t = next;
        }
    }
    assert!(
        wrong.is_empty(),
        "{} of {checked} differ:\n{}",
        wrong.len(),
        wrong
            .iter()
            .take(10)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    );
    checked
}

/// Every zone's transitions in 2024 and 2025 (about 5 s in a debug build).
#[test]
fn engine_matches_the_oracle_in_every_zone() {
    let zones: Vec<&str> = faucet_core::time::Zone::names().collect();
    let checked = sweep(
        &zones,
        at("2024-01-01T00:00:00Z"),
        at("2026-01-01T00:00:00Z"),
    );
    assert!(checked > 5_000, "only {checked} windows checked");
}

/// Zones tzdata 2026 changed (#1116): ticks follow the current rules.
#[test]
fn ticks_follow_tzdata_2026_changes() {
    assert_eq!(
        ticks("0 9 * * *", "America/Vancouver", "2026-11-01T12:00:00Z", 2),
        ["2026-11-01T16:00Z", "2026-11-02T16:00Z"]
    );
    assert_eq!(
        ticks("0 9 * * *", "America/Havana", "2026-11-01T12:00:00Z", 2),
        ["2026-11-01T13:00Z", "2026-11-02T13:00Z"]
    );
    assert_eq!(
        ticks("0 3 * * *", "Africa/Casablanca", "2026-09-20T12:00:00Z", 1),
        ["2026-09-21T03:00Z"]
    );
}

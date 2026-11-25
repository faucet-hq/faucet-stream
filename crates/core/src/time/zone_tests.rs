use chrono::{Offset, TimeZone};

use super::*;

#[test]
fn names_are_exact_case() {
    assert!(Zone::parse("europe/london").is_err());
    assert!(
        jiff::tz::TimeZoneDatabase::bundled()
            .get("europe/london")
            .is_ok(),
        "jiff alone matches names ignoring case"
    );
    for tz in chrono_tz::TZ_VARIANTS {
        assert_eq!(Zone::parse(tz.name()).unwrap().name(), tz.name());
    }
    assert_eq!(Zone::names().next(), Some("UTC"));
    assert_eq!(
        "Asia/Kolkata".parse::<ChronoZone>().unwrap().name(),
        "Asia/Kolkata"
    );
    assert!("asia/kolkata".parse::<ChronoZone>().is_err());
    assert_eq!(
        format!("{} {:?}", ChronoZone::UTC, Zone::UTC),
        "UTC Zone(\"UTC\")"
    );
}

#[test]
fn fixed_zones() {
    assert!(Zone::UTC.is_fixed());
    assert!(Zone::parse("Etc/GMT+5").unwrap().is_fixed());
    assert!(!Zone::parse("Europe/London").unwrap().is_fixed());
}

#[test]
fn chrono_adapter_round_trips() {
    let z = Zone::parse("America/New_York").unwrap().chrono();
    let utc = chrono::NaiveDate::from_ymd_opt(2026, 7, 1)
        .unwrap()
        .and_hms_opt(12, 0, 0)
        .unwrap();
    let local = z.from_utc_datetime(&utc);
    assert_eq!(local.offset().fix().local_minus_utc(), -14_400);
    assert_eq!(local.timezone(), z);
    assert_eq!(local.to_string(), "2026-07-01 08:00:00 -04:00");
    assert_eq!(format!("{:?}", local.offset()), "-04:00");
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
    assert_eq!(
        z.offset_from_utc_date(&date).fix().local_minus_utc(),
        -18_000
    );
    assert!(matches!(
        z.offset_from_local_date(&date),
        chrono::MappedLocalTime::Single(_)
    ));
    let far = chrono::NaiveDate::from_ymd_opt(100_000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    assert!(matches!(
        z.offset_from_local_datetime(&far),
        chrono::MappedLocalTime::None
    ));
    assert_eq!(
        z.offset_from_utc_datetime(&far).fix().local_minus_utc(),
        -18_000
    );
}

/// Every offset the adapter reports for a civil time is the one in force at
/// the instant it names, and a fold has two such offsets, at every
/// transition edge of every zone, 1970 to 2099.
#[test]
fn every_local_answer_is_an_offset_in_force() {
    let start = jiff::Timestamp::from_second(0).unwrap();
    let end = jiff::Timestamp::from_second(4_102_444_800).unwrap();
    let mut checked = 0usize;
    for name in Zone::names() {
        let zone = Zone::parse(name).unwrap();
        let tz = zone.tz();
        for tr in tz.following(start) {
            if tr.timestamp() >= end {
                break;
            }
            let before = tz.to_offset(tr.timestamp() - jiff::SignedDuration::from_secs(1));
            for edge in [
                before.to_datetime(tr.timestamp()),
                tr.offset().to_datetime(tr.timestamp()),
            ] {
                for minutes in [-120i64, -60, -30, -1, 0, 1, 30, 60, 120] {
                    let Ok(dt) = edge.checked_add(jiff::SignedDuration::from_secs(minutes * 60))
                    else {
                        continue;
                    };
                    let naive = chrono::NaiveDate::from_ymd_opt(
                        dt.year().into(),
                        dt.month() as u32,
                        dt.day() as u32,
                    )
                    .unwrap()
                    .and_hms_opt(dt.hour() as u32, dt.minute() as u32, dt.second() as u32)
                    .unwrap();
                    let offsets = zone.local_offsets(&naive);
                    for o in &offsets {
                        let at = jiff::tz::Offset::from_seconds(o.local_minus_utc())
                            .unwrap()
                            .to_timestamp(dt)
                            .unwrap();
                        assert_eq!(
                            tz.to_offset(at).seconds(),
                            o.local_minus_utc(),
                            "{name} local {dt}"
                        );
                    }
                    let in_force = [before, tr.offset()]
                        .into_iter()
                        .filter(|o| o.to_timestamp(dt).is_ok_and(|at| tz.to_offset(at) == *o))
                        .map(|o| o.seconds())
                        .collect::<std::collections::BTreeSet<_>>();
                    if in_force.len() == 2 {
                        assert_eq!(offsets.len(), 2, "{name} local {dt}: a fold");
                    }
                    checked += 1;
                }
            }
        }
    }
    assert!(checked > 500_000, "only {checked} civil times checked");
}

fn pinned_tzdata() -> String {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../supply-chain/tzdata-version"
    );
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("the tzdata pin {path}: {e}"))
        .trim()
        .to_string()
}

/// One tzdata release everywhere (#1116): ours (jiff-tzdb), Arrow's and
/// ORC's (the vendored chrono-tz) and the pin in supply-chain.
#[test]
fn both_databases_are_the_pinned_release() {
    let pin = pinned_tzdata();
    assert_eq!(jiff_tzdb::VERSION, pin, "jiff-tzdb differs from the pin");
    assert_eq!(
        chrono_tz::IANA_TZDB_VERSION,
        pin,
        "vendor/chrono-tz differs from the pin: regenerate it from tzdata {pin}"
    );
    assert_eq!(crate::time::TZDATA_VERSION, pin);
}

#[test]
fn both_databases_know_the_same_zones() {
    let ours: std::collections::BTreeSet<&str> =
        Zone::names().filter(|n| *n != "Factory").collect();
    let theirs: std::collections::BTreeSet<&str> =
        chrono_tz::TZ_VARIANTS.iter().map(|z| z.name()).collect();
    assert_eq!(ours, theirs);
}

/// Equal UTC offsets 1970-2099: at every transition of either database (one
/// second before, at, one second after) and on a daily grid. A change on
/// the chrono-tz side is found by bisecting the grid to the second.
#[test]
fn both_databases_give_the_same_offsets() {
    const FROM: i64 = 0;
    const UNTIL: i64 = 4_102_444_800;
    const DAY: i64 = 86_400;
    let mut wrong = Vec::new();
    let mut points = 0usize;
    for tz in chrono_tz::TZ_VARIANTS {
        let zone = Zone::parse(tz.name()).unwrap();
        let theirs = |t: i64| {
            tz.offset_from_utc_datetime(
                &chrono::DateTime::from_timestamp(t, 0).unwrap().naive_utc(),
            )
            .fix()
            .local_minus_utc()
        };
        let mut check = |t: i64| {
            points += 1;
            let (a, b) = (zone.offset_secs_at(t), theirs(t));
            if a != b && wrong.len() < 20 {
                wrong.push(format!("{} at {t}: ours {a}, chrono-tz {b}", tz.name()));
            }
        };
        let start = jiff::Timestamp::from_second(FROM).unwrap();
        for tr in zone.tz().following(start) {
            let at = tr.timestamp().as_second();
            if at >= UNTIL {
                break;
            }
            for t in [at - 1, at, at + 1] {
                check(t);
            }
        }
        let mut t = FROM;
        let mut prev = theirs(t);
        while t < UNTIL {
            let next = t + DAY;
            let o = theirs(next);
            check(next);
            if o != prev {
                let (mut lo, mut hi) = (t, next);
                while hi - lo > 1 {
                    let mid = lo + (hi - lo) / 2;
                    if theirs(mid) == prev {
                        lo = mid
                    } else {
                        hi = mid
                    }
                }
                for p in [lo, hi, hi + 1] {
                    check(p);
                }
            }
            prev = o;
            t = next;
        }
    }
    assert!(points > 25_000_000, "only {points} instants checked");
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Equal local-time resolution at every transition edge, 1970-2099.
#[test]
fn both_databases_resolve_local_times_alike() {
    let start = jiff::Timestamp::from_second(0).unwrap();
    let end = jiff::Timestamp::from_second(4_102_444_800).unwrap();
    let fixes = |r: chrono::MappedLocalTime<FixedOffset>| match r {
        chrono::MappedLocalTime::Single(a) => vec![a.local_minus_utc()],
        chrono::MappedLocalTime::Ambiguous(a, b) => vec![a.local_minus_utc(), b.local_minus_utc()],
        chrono::MappedLocalTime::None => vec![],
    };
    let mut wrong = Vec::new();
    for tz in chrono_tz::TZ_VARIANTS {
        let zone = Zone::parse(tz.name()).unwrap().chrono();
        let jtz = zone.zone().tz();
        for tr in jtz.following(start) {
            if tr.timestamp() >= end {
                break;
            }
            let before = jtz.to_offset(tr.timestamp() - jiff::SignedDuration::from_secs(1));
            let edge = before.to_datetime(tr.timestamp());
            for minutes in [-120i64, -60, -30, -1, 0, 1, 30, 60, 120] {
                let dt = edge + jiff::SignedDuration::from_secs(minutes * 60);
                let naive = chrono::NaiveDate::from_ymd_opt(
                    dt.year().into(),
                    dt.month() as u32,
                    dt.day() as u32,
                )
                .unwrap()
                .and_hms_opt(dt.hour() as u32, dt.minute() as u32, dt.second() as u32)
                .unwrap();
                let ours = fixes(zone.offset_from_local_datetime(&naive).map(|o| o.fix()));
                let theirs = fixes(tz.offset_from_local_datetime(&naive).map(|o| o.fix()));
                if ours != theirs && wrong.len() < 20 {
                    wrong.push(format!(
                        "{} local {naive}: ours {ours:?}, chrono-tz {theirs:?}",
                        tz.name()
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

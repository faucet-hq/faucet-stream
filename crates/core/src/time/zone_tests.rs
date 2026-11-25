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

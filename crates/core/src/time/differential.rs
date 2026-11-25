//! The same operations on chrono (what ships) and jiff 1.x (the planned
//! replacement, a dev-dependency only). A difference must be listed with its
//! reason or the test fails, so the swap (#1093 stage B) is a known quantity.

use super::*;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn range(&mut self, lo: i64, hi: i64) -> i64 {
        lo + (self.next() % (hi - lo + 1) as u64) as i64
    }
}

const JIFF_MIN: i64 = -377_705_023_201;
const JIFF_MAX: i64 = 253_402_207_200;
const YEAR_0: i64 = -62_167_219_200;
const SAMPLES: usize = 20_000;

fn nanos_of(t: Timestamp) -> i128 {
    t.unix_secs() as i128 * 1_000_000_000 + t.subsec_nanos() as i128
}

#[test]
fn unix_constructors_agree_in_jiffs_range() {
    let mut rng = Rng(0x1093_1093_1093_1093);
    for _ in 0..SAMPLES {
        let secs = rng.range(JIFF_MIN, JIFF_MAX - 1);
        let nanos = rng.range(0, 999_999_999) as u32;
        let ours = Timestamp::from_unix_parts(secs, nanos).unwrap();
        let theirs = jiff::Timestamp::new(secs, nanos as i32).unwrap();
        assert_eq!(nanos_of(ours), theirs.as_nanosecond(), "{secs}.{nanos}");
        if secs >= YEAR_0 {
            assert_eq!(
                ours.to_rfc3339(Precision::Nanos),
                format!("{theirs:.9}"),
                "{secs}.{nanos}"
            );
        }

        let millis = secs * 1000 + (nanos / 1_000_000) as i64;
        let ours = Timestamp::from_unix_millis(millis).unwrap();
        let theirs = jiff::Timestamp::from_millisecond(millis).unwrap();
        assert_eq!(nanos_of(ours), theirs.as_nanosecond(), "millis {millis}");
        assert_eq!(
            ours.unix_millis(),
            theirs.as_millisecond(),
            "millis {millis}"
        );

        let micros = secs
            .saturating_mul(1_000_000)
            .saturating_add((nanos / 1000) as i64);
        if let (Some(ours), Ok(theirs)) = (
            Timestamp::from_unix_micros(micros),
            jiff::Timestamp::from_microsecond(micros),
        ) {
            assert_eq!(nanos_of(ours), theirs.as_nanosecond(), "micros {micros}");
        }

        let ours = Timestamp::from_unix_secs(secs).unwrap();
        let theirs = jiff::Timestamp::from_second(secs).unwrap();
        assert_eq!(ours.unix_secs(), theirs.as_second());
    }
}

#[test]
fn chrono_accepts_instants_jiff_refuses_outside_its_range() {
    for secs in [JIFF_MIN - 1, JIFF_MAX + 1, 253_402_300_799] {
        assert!(Timestamp::from_unix_secs(secs).is_some(), "{secs}");
        assert!(jiff::Timestamp::from_second(secs).is_err(), "{secs}");
    }
}

#[test]
fn negative_years_print_differently() {
    let secs = -83_868_181_061;
    let ours = Timestamp::from_unix_secs(secs).unwrap();
    let theirs = jiff::Timestamp::from_second(secs).unwrap();
    assert_eq!(ours.to_rfc3339(Precision::Secs), "-0688-04-28T11:02:19Z");
    assert_eq!(theirs.to_string(), "-000688-04-28T11:02:19Z");
}

/// Inputs both libraries parse to the same instant.
const RFC3339_SAME: &[&str] = &[
    "2026-11-25T10:00:00Z",
    "2026-11-25T10:00:00.5Z",
    "2026-11-25T10:00:00.123456789Z",
    "2026-11-25T10:00:00+05:45",
    "2026-11-25T10:00:00-12:00",
    "2026-11-25T10:00:00+14:00",
    "2026-11-25t10:00:00z",
    "2026-11-25 10:00:00Z",
    "0001-01-01T00:00:00Z",
    "2024-02-29T00:00:00Z",
];

/// Inputs on which they differ, and why.
const RFC3339_DIFFERENT: &[(&str, &str)] = &[
    (
        "2016-12-31T23:59:60Z",
        "leap second: chrono keeps :60, jiff clamps to :59",
    ),
    (
        "2026-11-25T10:00:00.1234567891Z",
        "chrono truncates past 9 digits, jiff refuses",
    ),
    (
        "9999-12-31T23:59:59.999999999Z",
        "jiff's instants end at 9999-12-30T22:00:00Z",
    ),
];

/// Inputs both refuse.
const RFC3339_REFUSED: &[&str] = &["", "x", "2026-13-01T00:00:00Z", "2023-02-29T00:00:00Z"];

#[test]
fn rfc3339_parsing() {
    for text in RFC3339_SAME {
        let ours = Timestamp::parse_rfc3339(text).unwrap();
        let theirs: jiff::Timestamp = text.parse().unwrap();
        assert_eq!(nanos_of(ours), theirs.as_nanosecond(), "{text}");
    }
    for (text, why) in RFC3339_DIFFERENT {
        let ours = Timestamp::parse_rfc3339(text).map(nanos_of).ok();
        let theirs = text
            .parse::<jiff::Timestamp>()
            .map(|t| t.as_nanosecond())
            .ok();
        assert_ne!(
            ours, theirs,
            "{text} no longer differs ({why}); move it to RFC3339_SAME"
        );
    }
    for text in RFC3339_REFUSED {
        assert!(Timestamp::parse_rfc3339(text).is_err(), "{text}");
        assert!(text.parse::<jiff::Timestamp>().is_err(), "{text}");
    }
}

#[test]
fn rfc2822_parsing() {
    let parser = jiff::fmt::rfc2822::DateTimeParser::new();
    for text in [
        "Wed, 25 Nov 2026 10:00:00 GMT",
        "Wed, 25 Nov 2026 12:00:00 +0200",
        "25 Nov 2026 10:00:00 -0000",
        "Wed, 25 Nov 2026 10:00:00 UT",
    ] {
        let ours = Timestamp::parse_rfc2822(text).unwrap();
        let theirs = parser.parse_timestamp(text).unwrap();
        assert_eq!(nanos_of(ours), theirs.as_nanosecond(), "{text}");
    }
    for text in ["", "2026-11-25", "Wed, 31 Nov 2026 10:00:00 GMT"] {
        assert!(Timestamp::parse_rfc2822(text).is_err(), "{text}");
        assert!(parser.parse_timestamp(text).is_err(), "{text}");
    }
}

#[cfg(feature = "time-zones")]
mod zones {
    use super::*;

    const FROM: i64 = 0;
    const UNTIL: i64 = 2_208_988_800;

    fn jiff_db() -> jiff::tz::TimeZoneDatabase {
        jiff::tz::TimeZoneDatabase::bundled()
    }

    fn classify(z: &Zone, civil: jiff::civil::DateTime) -> (u8, Vec<i32>) {
        let c = CivilDate::new(
            civil.year().into(),
            civil.month() as u32,
            civil.day() as u32,
        )
        .unwrap()
        .at(CivilTime::new(
            civil.hour() as u32,
            civil.minute() as u32,
            civil.second() as u32,
            0,
        )
        .unwrap());
        match z.resolve_local(c) {
            LocalTime::Single(o) => (0, vec![o.offset_secs()]),
            LocalTime::Fold { earlier, later } => {
                (1, vec![earlier.offset_secs(), later.offset_secs()])
            }
            LocalTime::Gap => (2, vec![]),
        }
    }

    fn classify_jiff(tz: &jiff::tz::TimeZone, civil: jiff::civil::DateTime) -> (u8, Vec<i32>) {
        match tz.to_ambiguous_timestamp(civil).offset() {
            jiff::tz::AmbiguousOffset::Unambiguous { offset } => (0, vec![offset.seconds()]),
            jiff::tz::AmbiguousOffset::Fold { before, after } => {
                (1, vec![before.seconds(), after.seconds()])
            }
            jiff::tz::AmbiguousOffset::Gap { .. } => (2, vec![]),
        }
    }

    /// Zones whose rules in 1970-2039 differ between chrono-tz 0.10.4's
    /// database (tzdata 2025b) and jiff-tzdb 1.0.0's (tzdata 2026f). First
    /// differing transition in the comment.
    const TZDATA_DIFFERS: &[&str] = &[
        "Africa/Casablanca",    // 2026-09-20
        "Africa/El_Aaiun",      // 2026-09-20
        "America/Bogota",       // 1992-05-02
        "America/Edmonton",     // 2026-11-01
        "America/Ensenada",     // 1970-04-26
        "America/Godthab",      // 2023-10-29
        "America/Inuvik",       // 2026-11-01
        "America/Nuuk",         // 2023-10-29
        "America/Rainy_River",  // 2026-11-01
        "America/Santa_Isabel", // 1970-04-26
        "America/Tijuana",      // 1970-04-26
        "America/Vancouver",    // 2026-11-01
        "America/Winnipeg",     // 2026-11-01
        "America/Yellowknife",  // 2026-11-01
        "Asia/Tehran",          // 1979-05-25
        "Canada/Central",       // 2026-11-01
        "Canada/Mountain",      // 2026-11-01
        "Canada/Pacific",       // 2026-11-01
        "Europe/Chisinau",      // 2022-03-27
        "Europe/Tiraspol",      // 2022-03-27
        "Iran",                 // 1979-05-25
        "Mexico/BajaNorte",     // 1970-04-26
    ];

    #[test]
    fn every_zone_agrees_at_every_transition_1970_to_2039() {
        let db = jiff_db();
        let mut differing = std::collections::BTreeSet::new();
        let mut transitions = 0usize;
        for tz in chrono_tz::TZ_VARIANTS {
            let name = tz.name();
            let ours = Zone::from_chrono(tz);
            let Ok(theirs) = db.get(name) else {
                differing.insert(name.to_string());
                continue;
            };
            let start = jiff::Timestamp::from_second(FROM).unwrap();
            for tr in theirs.following(start) {
                let at = tr.timestamp();
                if at.as_second() >= UNTIL {
                    break;
                }
                transitions += 1;
                let mut ok = true;
                for probe in [at.as_second() - 1, at.as_second(), at.as_second() + 1] {
                    let o = ours
                        .to_local(Timestamp::from_unix_secs(probe).unwrap())
                        .offset_secs();
                    let j = theirs
                        .to_offset(jiff::Timestamp::from_second(probe).unwrap())
                        .seconds();
                    ok &= o == j;
                }
                let before =
                    theirs.to_offset(jiff::Timestamp::from_second(at.as_second() - 1).unwrap());
                let after = tr.offset();
                let delta = (after.seconds() - before.seconds()).abs() as i64;
                let edge = before.to_datetime(at);
                for minutes in [0, delta / 120, delta / 60 - 1] {
                    let civil = edge
                        .checked_add(jiff::SignedDuration::from_secs(minutes.max(0) * 60))
                        .unwrap();
                    ok &= classify(&ours, civil) == classify_jiff(&theirs, civil);
                }
                if !ok {
                    differing.insert(name.to_string());
                }
            }
        }
        assert!(
            transitions > 10_000,
            "only {transitions} transitions checked"
        );
        let allowed: std::collections::BTreeSet<String> =
            TZDATA_DIFFERS.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            differing, allowed,
            "zones that differ between the two databases"
        );
    }

    #[test]
    fn names_jiff_accepts_that_we_refuse() {
        let db = jiff_db();
        assert!(Zone::parse("europe/london").is_err());
        assert!(
            db.get("europe/london").is_ok(),
            "jiff matches zone names ignoring case"
        );
    }
}

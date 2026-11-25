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

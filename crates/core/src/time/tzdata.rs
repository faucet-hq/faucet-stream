use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

use super::TimeError;

/// The IANA tzdata release of the zone database compiled into this binary.
pub const TZDATA_VERSION: &str = jiff_tzdb::VERSION;

/// An IANA tzdata release name such as `2026f`, ordered by year then letter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TzdataVersion {
    year: u16,
    letter: u8,
}

impl TzdataVersion {
    /// Parse `YYYYl` (four digits and one lowercase letter); refuse anything else.
    pub fn parse(text: &str) -> Result<Self, TimeError> {
        let bad = || {
            TimeError::new(
                "tzdata version",
                format!("'{text}' is not YYYY plus a letter"),
            )
        };
        let bytes = text.as_bytes();
        if bytes.len() != 5 || !bytes[..4].iter().all(u8::is_ascii_digit) {
            return Err(bad());
        }
        let letter = bytes[4];
        if !letter.is_ascii_lowercase() {
            return Err(bad());
        }
        let year = text[..4].parse().map_err(|_| bad())?;
        Ok(Self { year, letter })
    }

    /// The release compiled into this binary.
    pub fn current() -> Self {
        Self::parse(TZDATA_VERSION).expect("jiff-tzdb names its release YYYYl")
    }
}

impl fmt::Display for TzdataVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}{}", self.year, self.letter as char)
    }
}

/// The last tzdata release faucet shipped before it moved off chrono-tz's
/// bundled data (#1116). Replicas that do not advertise a version run it.
pub const TZDATA_BEFORE_ADVERTISING: &str = "2025b";

#[derive(serde::Deserialize)]
struct Fixture {
    max_delta_secs: i64,
    zones: BTreeMap<String, Vec<(i64, Option<i64>)>>,
}

fn fixture() -> &'static Fixture {
    static F: OnceLock<Fixture> = OnceLock::new();
    F.get_or_init(|| {
        serde_json::from_str(include_str!("fixtures/changed_since_2025b.json"))
            .expect("the checked-in fixture parses")
    })
}

/// Where tzdata 2025b and the compiled release give a zone different UTC
/// offsets (#1116): half-open unix-second intervals, `None` end = open.
/// Frozen: it describes what releases built on 2025b computed.
pub mod changed_since_2025b {
    use super::fixture;

    /// The largest offset difference in any interval, in seconds.
    pub fn max_delta_secs() -> i64 {
        fixture().max_delta_secs
    }

    /// The zones that changed.
    pub fn zones() -> impl Iterator<Item = &'static str> {
        fixture().zones.keys().map(String::as_str)
    }

    /// A zone's intervals (empty when it did not change).
    pub fn intervals(zone: &str) -> &'static [(i64, Option<i64>)] {
        fixture().zones.get(zone).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Whether the instant `at` lies in one of the zone's intervals.
    pub fn contains(zone: &str, at: i64) -> bool {
        intervals(zone)
            .iter()
            .any(|(s, e)| *s <= at && e.is_none_or(|e| at < e))
    }

    /// Whether `[start, end)` overlaps one of the zone's intervals.
    pub fn overlaps(zone: &str, start: i64, end: i64) -> bool {
        intervals(zone)
            .iter()
            .any(|(s, e)| *s < end && e.is_none_or(|e| start < e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versions_parse_and_order() {
        let a = TzdataVersion::parse("2025b").unwrap();
        let b = TzdataVersion::parse("2026a").unwrap();
        let c = TzdataVersion::parse("2026f").unwrap();
        assert!(a < b && b < c);
        assert_eq!(c.to_string(), "2026f");
        for bad in ["", "2026", "2026F", "26f", "2026fg", "x026f", "2026-"] {
            assert!(TzdataVersion::parse(bad).is_err(), "{bad}");
        }
        assert_eq!(TzdataVersion::current().to_string(), TZDATA_VERSION);
        assert!(
            TzdataVersion::parse(TZDATA_BEFORE_ADVERTISING).unwrap() < TzdataVersion::current()
        );
    }

    fn secs(text: &str) -> i64 {
        crate::time::Timestamp::parse_rfc3339(text)
            .unwrap()
            .unix_secs()
    }

    /// The fixture agrees with the NEWS rows: each changed row inside, each
    /// row both releases agree on outside.
    #[test]
    fn fixture_matches_the_news_rows() {
        assert_eq!(changed_since_2025b::max_delta_secs(), 3600);
        assert_eq!(changed_since_2025b::zones().count(), 22);
        for (zone, at) in [
            ("America/Vancouver", "2026-11-24T10:13:20Z"),
            ("Canada/Pacific", "2026-12-15T12:00:00Z"),
            ("America/Edmonton", "2026-12-15T12:00:00Z"),
            ("America/Inuvik", "2026-12-15T12:00:00Z"),
            ("America/Winnipeg", "2099-12-15T12:00:00Z"),
            ("America/Havana", "2026-11-24T10:13:20Z"),
            ("Cuba", "2027-02-01T12:00:00Z"),
            ("Africa/Casablanca", "2026-09-20T01:30:00Z"),
            ("Africa/El_Aaiun", "2026-12-15T12:00:00Z"),
            ("Europe/Chisinau", "2026-03-29T00:30:00Z"),
            ("Europe/Tiraspol", "2026-10-25T00:30:00Z"),
            ("America/Bogota", "1992-05-02T12:00:00Z"),
            ("Iran", "1979-05-26T00:00:00Z"),
            ("Mexico/BajaNorte", "1972-07-01T12:00:00Z"),
        ] {
            assert!(changed_since_2025b::contains(zone, secs(at)), "{zone} {at}");
        }
        for (zone, at) in [
            ("America/Vancouver", "2026-07-01T12:00:00Z"),
            ("America/Havana", "2026-10-31T12:00:00Z"),
            ("America/Havana", "2027-12-15T12:00:00Z"),
            ("Africa/Casablanca", "2026-09-20T00:30:00Z"),
            ("Europe/Chisinau", "2026-03-29T01:00:00Z"),
            ("America/Bogota", "1992-05-02T04:59:59Z"),
            ("America/Tijuana", "1972-01-15T12:00:00Z"),
            ("America/Nuuk", "2023-10-29T01:00:00Z"),
            ("Europe/London", "2026-07-01T12:00:00Z"),
        ] {
            assert!(
                !changed_since_2025b::contains(zone, secs(at)),
                "{zone} {at}"
            );
        }
        let casablanca_day = (secs("2026-09-19T23:00:00Z"), secs("2026-09-21T00:00:00Z"));
        assert!(changed_since_2025b::overlaps(
            "Africa/Casablanca",
            casablanca_day.0,
            casablanca_day.1
        ));
        assert!(!changed_since_2025b::overlaps(
            "Africa/Casablanca",
            secs("2026-09-01T00:00:00Z"),
            secs("2026-09-20T01:00:00Z")
        ));
        assert!(changed_since_2025b::intervals("UTC").is_empty());
    }
}

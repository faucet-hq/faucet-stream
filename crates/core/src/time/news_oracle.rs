//! Zone answers checked against the IANA NEWS file, not against a library
//! (#1116). Each row names the release whose NEWS entry fixes its value.

use super::*;

/// (zone, UTC instant, UTC offset in seconds, NEWS release).
const OFFSETS: &[(&str, &str, i32, &str)] = &[
    (
        "America/Vancouver",
        "2026-03-08T09:59:59Z",
        -28_800,
        "2026b",
    ),
    (
        "America/Vancouver",
        "2026-07-01T12:00:00Z",
        -25_200,
        "2026b",
    ),
    (
        "America/Vancouver",
        "2026-11-24T10:13:20Z",
        -25_200,
        "2026b",
    ),
    (
        "America/Vancouver",
        "2027-01-15T12:00:00Z",
        -25_200,
        "2026b",
    ),
    ("Canada/Pacific", "2026-12-15T12:00:00Z", -25_200, "2026b"),
    ("America/Edmonton", "2026-03-08T08:59:59Z", -25_200, "2026c"),
    ("America/Edmonton", "2026-12-15T12:00:00Z", -21_600, "2026c"),
    (
        "America/Yellowknife",
        "2026-12-15T12:00:00Z",
        -21_600,
        "2026c",
    ),
    ("Canada/Mountain", "2026-12-15T12:00:00Z", -21_600, "2026c"),
    ("America/Inuvik", "2026-12-15T12:00:00Z", -21_600, "2026d"),
    ("America/Winnipeg", "2026-12-15T12:00:00Z", -18_000, "2026e"),
    (
        "America/Rainy_River",
        "2026-12-15T12:00:00Z",
        -18_000,
        "2026e",
    ),
    ("Canada/Central", "2026-12-15T12:00:00Z", -18_000, "2026e"),
    ("America/Havana", "2026-10-31T12:00:00Z", -14_400, "2026f"),
    ("America/Havana", "2026-11-24T10:13:20Z", -14_400, "2026f"),
    ("America/Havana", "2027-02-01T12:00:00Z", -14_400, "2026f"),
    ("America/Havana", "2027-12-15T12:00:00Z", -18_000, "2026f"),
    ("Cuba", "2026-11-24T10:13:20Z", -14_400, "2026f"),
    ("Africa/Casablanca", "2026-09-20T00:30:00Z", 3_600, "2026c"),
    ("Africa/Casablanca", "2026-09-20T01:30:00Z", 0, "2026c"),
    ("Africa/Casablanca", "2026-12-15T12:00:00Z", 0, "2026c"),
    ("Africa/Casablanca", "2027-06-15T12:00:00Z", 0, "2026c"),
    ("Africa/El_Aaiun", "2026-12-15T12:00:00Z", 0, "2026c"),
    ("Europe/Chisinau", "2026-03-29T00:30:00Z", 7_200, "2026a"),
    ("Europe/Chisinau", "2026-03-29T01:00:00Z", 10_800, "2026a"),
    ("Europe/Chisinau", "2026-10-25T00:30:00Z", 10_800, "2026a"),
    ("Europe/Chisinau", "2026-10-25T01:00:00Z", 7_200, "2026a"),
    ("Europe/Tiraspol", "2026-03-29T00:30:00Z", 7_200, "2026a"),
    ("America/Bogota", "1992-05-02T04:59:59Z", -18_000, "2026d"),
    ("America/Bogota", "1992-05-02T12:00:00Z", -14_400, "2026d"),
    ("Asia/Tehran", "1979-05-25T20:29:59Z", 12_600, "2026d"),
    ("Asia/Tehran", "1979-05-26T00:00:00Z", 16_200, "2026d"),
    ("Iran", "1979-05-26T00:00:00Z", 16_200, "2026d"),
    ("America/Tijuana", "1972-01-15T12:00:00Z", -28_800, "2025c"),
    ("America/Tijuana", "1972-07-01T12:00:00Z", -25_200, "2025c"),
    ("America/Ensenada", "1972-07-01T12:00:00Z", -25_200, "2025c"),
    (
        "America/Santa_Isabel",
        "1972-07-01T12:00:00Z",
        -25_200,
        "2025c",
    ),
    ("Mexico/BajaNorte", "1972-07-01T12:00:00Z", -25_200, "2025c"),
    ("America/Nuuk", "2023-07-15T12:00:00Z", -7_200, "2022g"),
    ("America/Nuuk", "2023-12-15T12:00:00Z", -7_200, "2022g"),
    ("America/Nuuk", "2024-07-15T12:00:00Z", -3_600, "2022g"),
    ("America/Nuuk", "2026-01-15T12:00:00Z", -7_200, "2022g"),
    ("Europe/London", "2026-01-15T12:00:00Z", 0, "-"),
    ("Europe/London", "2026-07-01T12:00:00Z", 3_600, "-"),
    ("America/New_York", "2026-01-15T12:00:00Z", -18_000, "-"),
    ("America/New_York", "2026-07-01T12:00:00Z", -14_400, "-"),
    ("Australia/Lord_Howe", "2026-01-15T12:00:00Z", 39_600, "-"),
    ("Australia/Lord_Howe", "2026-07-15T12:00:00Z", 37_800, "-"),
    ("Asia/Kolkata", "2026-07-01T12:00:00Z", 19_800, "-"),
    ("Pacific/Apia", "2011-12-30T09:59:59Z", -36_000, "2011n"),
    ("Pacific/Apia", "2011-12-30T10:00:00Z", 50_400, "2011n"),
];

#[derive(Debug, PartialEq)]
enum Want {
    Single(i32),
    Fold(i32, i32),
    Gap,
}

/// (zone, local civil time, resolution, NEWS release).
const LOCAL: &[(&str, &str, Want, &str)] = &[
    (
        "America/Nuuk",
        "2023-10-28 23:00",
        Want::Single(-7_200),
        "2022g",
    ),
    (
        "Europe/Chisinau",
        "2026-03-29 02:30",
        Want::Single(7_200),
        "2026a",
    ),
    ("Europe/Chisinau", "2026-03-29 03:00", Want::Gap, "2026a"),
    ("Europe/Chisinau", "2026-03-29 03:59", Want::Gap, "2026a"),
    (
        "Europe/Chisinau",
        "2026-03-29 04:00",
        Want::Single(10_800),
        "2026a",
    ),
    (
        "America/Havana",
        "2026-11-01 00:30",
        Want::Single(-14_400),
        "2026f",
    ),
    (
        "America/Havana",
        "2026-11-01 01:00",
        Want::Single(-14_400),
        "2026f",
    ),
    (
        "America/Vancouver",
        "2026-11-01 01:30",
        Want::Single(-25_200),
        "2026b",
    ),
    (
        "Africa/Casablanca",
        "2026-09-20 00:30",
        Want::Single(3_600),
        "2026c",
    ),
    (
        "Africa/Casablanca",
        "2026-09-20 01:30",
        Want::Fold(3_600, 0),
        "2026c",
    ),
    (
        "Africa/Casablanca",
        "2026-09-20 02:30",
        Want::Single(0),
        "2026c",
    ),
    ("Europe/London", "2026-03-29 01:30", Want::Gap, "-"),
    (
        "Europe/London",
        "2026-10-25 01:30",
        Want::Fold(3_600, 0),
        "-",
    ),
    ("America/New_York", "2026-03-08 02:30", Want::Gap, "-"),
    (
        "America/New_York",
        "2026-11-01 01:30",
        Want::Fold(-14_400, -18_000),
        "-",
    ),
];

fn civil(text: &str) -> CivilDateTime {
    CivilDateTime::from_chrono(NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M").unwrap())
}

#[test]
fn offsets_follow_news() {
    let mut wrong = Vec::new();
    for (zone, at, want, release) in OFFSETS {
        let got = Zone::parse(zone)
            .unwrap()
            .to_local(Timestamp::parse_rfc3339(at).unwrap())
            .offset_secs();
        if got != *want {
            wrong.push(format!(
                "{zone} at {at}: got {got}, NEWS {release} says {want}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn local_times_follow_news() {
    let mut wrong = Vec::new();
    for (zone, at, want, release) in LOCAL {
        let got = match Zone::parse(zone).unwrap().resolve_local(civil(at)) {
            LocalTime::Single(o) => Want::Single(o.offset_secs()),
            LocalTime::Fold { earlier, later } => {
                Want::Fold(earlier.offset_secs(), later.offset_secs())
            }
            LocalTime::Gap => Want::Gap,
        };
        if got != *want {
            wrong.push(format!(
                "{zone} local {at}: got {got:?}, NEWS {release} says {want:?}"
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

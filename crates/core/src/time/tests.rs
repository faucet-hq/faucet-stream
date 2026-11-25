use super::*;
use chrono::TimeZone;

fn ts(text: &str) -> Timestamp {
    Timestamp::parse_rfc3339(text).unwrap()
}

#[test]
fn rfc3339_print_matches_chrono_at_every_precision() {
    let cases = [
        (
            "2026-11-25T10:00:00Z",
            [
                "2026-11-25T10:00:00Z",
                "2026-11-25T10:00:00Z",
                "2026-11-25T10:00:00.000Z",
                "2026-11-25T10:00:00.000000Z",
                "2026-11-25T10:00:00.000000000Z",
            ],
        ),
        (
            "2026-11-25T10:00:00.5Z",
            [
                "2026-11-25T10:00:00.500Z",
                "2026-11-25T10:00:00Z",
                "2026-11-25T10:00:00.500Z",
                "2026-11-25T10:00:00.500000Z",
                "2026-11-25T10:00:00.500000000Z",
            ],
        ),
        (
            "2026-11-25T10:00:00.000001Z",
            [
                "2026-11-25T10:00:00.000001Z",
                "2026-11-25T10:00:00Z",
                "2026-11-25T10:00:00.000Z",
                "2026-11-25T10:00:00.000001Z",
                "2026-11-25T10:00:00.000001000Z",
            ],
        ),
        (
            "2026-11-25T10:00:00.000000001Z",
            [
                "2026-11-25T10:00:00.000000001Z",
                "2026-11-25T10:00:00Z",
                "2026-11-25T10:00:00.000Z",
                "2026-11-25T10:00:00.000000Z",
                "2026-11-25T10:00:00.000000001Z",
            ],
        ),
        (
            "0001-01-01T00:00:00Z",
            [
                "0001-01-01T00:00:00Z",
                "0001-01-01T00:00:00Z",
                "0001-01-01T00:00:00.000Z",
                "0001-01-01T00:00:00.000000Z",
                "0001-01-01T00:00:00.000000000Z",
            ],
        ),
        (
            "9999-12-31T23:59:59.999999999Z",
            [
                "9999-12-31T23:59:59.999999999Z",
                "9999-12-31T23:59:59Z",
                "9999-12-31T23:59:59.999Z",
                "9999-12-31T23:59:59.999999Z",
                "9999-12-31T23:59:59.999999999Z",
            ],
        ),
    ];
    let precisions = [
        Precision::Auto,
        Precision::Secs,
        Precision::Millis,
        Precision::Micros,
        Precision::Nanos,
    ];
    for (input, want) in cases {
        let t = ts(input);
        for (p, w) in precisions.iter().zip(want) {
            assert_eq!(t.to_rfc3339(*p), w, "{input} at {p:?}");
            assert_eq!(
                t.to_rfc3339(*p),
                t.to_chrono().to_rfc3339_opts(p.chrono(), true)
            );
        }
        assert_eq!(
            t.to_rfc3339_offset(Precision::Auto),
            t.to_chrono().to_rfc3339(),
            "{input}"
        );
    }
}

#[test]
fn serde_is_chronos_form() {
    for text in [
        "2026-11-25T10:00:00Z",
        "2026-11-25T10:00:00.500Z",
        "2026-11-25T10:00:00.123456Z",
        "2026-11-25T10:00:00.123456789Z",
    ] {
        let t = ts(text);
        let ours = serde_json::to_string(&t).unwrap();
        assert_eq!(ours, serde_json::to_string(&t.to_chrono()).unwrap());
        assert_eq!(ours, format!("\"{text}\""));
        let back: Timestamp = serde_json::from_str(&ours).unwrap();
        assert_eq!(back, t);
    }
    let offset: Timestamp = serde_json::from_str("\"2026-11-25T12:00:00+02:00\"").unwrap();
    assert_eq!(offset, ts("2026-11-25T10:00:00Z"));
    assert!(serde_json::from_str::<Timestamp>("\"not a time\"").is_err());
}

#[test]
fn parse_rfc3339_accepts_what_chrono_accepts() {
    for text in [
        "2026-11-25T10:00:00Z",
        "2026-11-25t10:00:00z",
        "2026-11-25 10:00:00Z",
        "2026-11-25T10:00:00+05:45",
        "2026-11-25T10:00:00-12:00",
        "2026-11-25T10:00:00+14:00",
        "2026-11-25T10:00:00.1234567891Z",
        "2016-12-31T23:59:60Z",
    ] {
        let ours = Timestamp::parse_rfc3339(text).unwrap();
        let theirs = DateTime::parse_from_rfc3339(text).unwrap().to_utc();
        assert_eq!(ours.to_chrono(), theirs, "{text}");
    }
    for text in [
        "",
        "2026-11-25",
        "2026-11-25T10:00:00",
        "2026-13-01T00:00:00Z",
        "x",
    ] {
        let err = Timestamp::parse_rfc3339(text).unwrap_err();
        assert!(err.to_string().starts_with("RFC 3339 timestamp: "), "{err}");
    }
}

#[test]
fn from_str_is_parse_rfc3339() {
    let t: Timestamp = "2026-11-25T12:00:00+02:00".parse().unwrap();
    assert_eq!(t, ts("2026-11-25T10:00:00Z"));
    assert!("nope".parse::<Timestamp>().is_err());
}

#[test]
fn leap_second_is_kept_as_chrono_keeps_it() {
    let t = ts("2016-12-31T23:59:60.5Z");
    assert_eq!(t.subsec_nanos(), 1_500_000_000);
    assert_eq!(t.to_rfc3339(Precision::Auto), "2016-12-31T23:59:60.500Z");
}

#[test]
fn parse_rfc2822() {
    let t = Timestamp::parse_rfc2822("Wed, 25 Nov 2026 10:00:00 GMT").unwrap();
    assert_eq!(t, ts("2026-11-25T10:00:00Z"));
    let t = Timestamp::parse_rfc2822("Wed, 25 Nov 2026 12:00:00 +0200").unwrap();
    assert_eq!(t, ts("2026-11-25T10:00:00Z"));
    let err = Timestamp::parse_rfc2822("2026-11-25").unwrap_err();
    assert!(err.to_string().starts_with("RFC 2822 date: "), "{err}");
}

#[test]
fn unix_conversions_round_trip_and_bound() {
    let t = ts("2026-11-25T10:00:00.123456789Z");
    assert_eq!(t.unix_secs(), 1_795_600_800);
    assert_eq!(t.unix_millis(), 1_795_600_800_123);
    assert_eq!(t.unix_micros(), 1_795_600_800_123_456);
    assert_eq!(t.unix_nanos(), Some(1_795_600_800_123_456_789));
    assert_eq!(t.subsec_nanos(), 123_456_789);
    assert_eq!(Timestamp::from_unix_nanos(1_795_600_800_123_456_789), t);
    assert_eq!(
        Timestamp::from_unix_parts(1_795_600_800, 123_456_789),
        Some(t)
    );
    assert_eq!(
        Timestamp::from_unix_secs(1_795_600_800),
        Some(ts("2026-11-25T10:00:00Z"))
    );
    assert_eq!(
        Timestamp::from_unix_millis(1_795_600_800_123),
        Some(ts("2026-11-25T10:00:00.123Z"))
    );
    assert_eq!(
        Timestamp::from_unix_micros(1_795_600_800_123_456),
        Some(ts("2026-11-25T10:00:00.123456Z"))
    );
    let before = ts("1969-12-31T23:59:59.5Z");
    assert_eq!(before.unix_secs(), -1);
    assert_eq!(before.unix_millis(), -500);
    assert_eq!(Timestamp::from_unix_secs(i64::MAX), None);
    assert_eq!(Timestamp::from_unix_millis(i64::MAX), None);
    assert_eq!(Timestamp::from_unix_micros(i64::MAX), None);
    assert!(Timestamp::from_unix_micros(i64::MIN / 2).is_some());
    assert_eq!(Timestamp::from_unix_parts(0, 2_000_000_000), None);
    assert_eq!(ts("2300-01-01T00:00:00Z").unix_nanos(), None);
    assert_eq!(Timestamp::UNIX_EPOCH.unix_secs(), 0);
    let now = Timestamp::now();
    assert!(now > ts("2026-01-01T00:00:00Z"));
}

#[test]
fn arithmetic_and_order() {
    let a = ts("2026-11-25T10:00:00Z");
    let hour = Span::from_hours(1).unwrap();
    let b = a.checked_add(hour).unwrap();
    assert_eq!(b, ts("2026-11-25T11:00:00Z"));
    assert_eq!(b.checked_sub(hour), Some(a));
    assert_eq!(b.since(a), hour);
    assert!(a.since(b).is_negative());
    assert!(a < b);
    let far = Timestamp::from_chrono(DateTime::<Utc>::MAX_UTC);
    assert_eq!(far.checked_add(hour), None);
    let near = Timestamp::from_chrono(DateTime::<Utc>::MIN_UTC);
    assert_eq!(near.checked_sub(hour), None);
}

#[test]
fn display_is_chronos() {
    let t = ts("2026-11-25T10:00:00.5Z");
    assert_eq!(t.to_string(), t.to_chrono().to_string());
    assert_eq!(t.to_string(), "2026-11-25 10:00:00.500 UTC");
}

#[test]
fn chrono_conversions_are_identity() {
    let c = Utc.with_ymd_and_hms(2026, 11, 25, 10, 0, 0).unwrap();
    let t: Timestamp = c.into();
    let back: DateTime<Utc> = t.into();
    assert_eq!(back, c);
    assert_eq!(Timestamp::from_chrono(c).to_chrono(), c);
}

#[test]
fn time_error_becomes_faucet_error() {
    let e: crate::FaucetError = Timestamp::parse_rfc3339("x").unwrap_err().into();
    assert!(matches!(e, crate::FaucetError::Custom(_)));
    assert!(e.to_string().contains("RFC 3339 timestamp"), "{e}");
}

#[test]
fn span_constructors_and_reads() {
    assert_eq!(Span::from_secs(90).unwrap().as_secs(), 90);
    assert_eq!(Span::from_millis(1_500).unwrap().as_millis(), 1_500);
    assert_eq!(Span::from_micros(7).as_micros(), Some(7));
    assert_eq!(Span::from_nanos(7).as_nanos(), Some(7));
    assert_eq!(Span::from_minutes(2).unwrap().as_secs(), 120);
    assert_eq!(Span::from_days(1).unwrap().as_secs(), 86_400);
    assert_eq!(Span::from_secs(i64::MAX), None);
    assert!(Span::from_millis(i64::MAX).is_some());
    assert_eq!(Span::from_minutes(i64::MAX), None);
    assert_eq!(Span::from_hours(i64::MAX), None);
    assert_eq!(Span::from_days(i64::MAX), None);
    assert_eq!(Span::from_secs(-3).unwrap().as_secs(), -3);
    assert!(Span::from_secs(-3).unwrap().is_negative());
    assert!(!Span::ZERO.is_negative());
    assert_eq!(Span::default(), Span::ZERO);
    let std = std::time::Duration::from_millis(2_500);
    assert_eq!(Span::from_std(std).unwrap().to_std(), Some(std));
    assert_eq!(Span::from_std(std::time::Duration::MAX), None);
    assert_eq!(Span::from_secs(-1).unwrap().to_std(), None);
    let one = Span::from_secs(1).unwrap();
    assert_eq!(one.checked_add(one), Span::from_secs(2));
    assert_eq!(one.checked_sub(one), Some(Span::ZERO));
    let max = Span::from_chrono(TimeDelta::MAX);
    assert_eq!(max.checked_add(one), None);
    assert_eq!(max.as_nanos(), None);
    assert_eq!(max.as_micros(), None);
    assert_eq!(Span::from_chrono(TimeDelta::MIN).checked_sub(one), None);
    assert_eq!(one.to_chrono(), TimeDelta::seconds(1));
}

#[test]
fn offset_clock() {
    let t = ts("2026-11-25T10:00:00.25Z");
    let c = t.with_offset(5 * 3600 + 45 * 60).unwrap();
    assert_eq!(c.offset_secs(), 20_700);
    assert_eq!(c.timestamp(), t);
    assert_eq!(
        c.to_rfc3339(Precision::Auto),
        "2026-11-25T15:45:00.250+05:45"
    );
    assert_eq!(c.to_rfc3339(Precision::Auto), c.to_chrono().to_rfc3339());
    assert_eq!(c.civil().to_string(), "2026-11-25 15:45:00.250");
    assert!(t.with_offset(86_400).is_none());
    let utc = t.with_offset(0).unwrap();
    assert_eq!(utc.to_rfc3339(Precision::Secs), "2026-11-25T10:00:00+00:00");
    let parsed = OffsetClock::parse_rfc3339("2026-11-25T15:45:00.250+05:45").unwrap();
    assert_eq!(parsed, c);
    assert_eq!(OffsetClock::from_chrono(c.to_chrono()), c);
    assert!(OffsetClock::parse_rfc3339("nope").is_err());
}

#[test]
fn strftime_is_chronos_grammar() {
    let c = OffsetClock::parse_rfc3339("2026-11-05T07:08:09.5-03:00").unwrap();
    for (fmt, want) in [
        ("%Y%m%d", "20261105"),
        ("%-d/%-m", "5/11"),
        ("%_d", " 5"),
        ("%f", "500000000"),
        ("%.f", ".500"),
        ("%3f", "500"),
        ("%:z", "-03:00"),
        ("%Z", "-03:00"),
        ("%+", "2026-11-05T07:08:09.500-03:00"),
        ("%c", "Thu Nov  5 07:08:09 2026"),
        ("%v", " 5-Nov-2026"),
        ("plain", "plain"),
    ] {
        assert_eq!(c.format_chrono_strftime(fmt).unwrap(), want, "{fmt}");
        assert_eq!(
            c.format_chrono_strftime(fmt).unwrap(),
            c.to_chrono().format(fmt).to_string(),
            "{fmt}"
        );
    }
    let bad = c.format_chrono_strftime("%Q").unwrap_err();
    assert_eq!(bad.to_string(), "strftime format: unknown specifier");
    let unrenderable = c.format_chrono_strftime("%#z").unwrap_err();
    assert_eq!(
        unrenderable.to_string(),
        "strftime format: a specifier cannot be rendered"
    );
}

#[test]
fn civil_values() {
    let d = CivilDate::new(2024, 2, 29).unwrap();
    assert_eq!((d.year(), d.month(), d.day()), (2024, 2, 29));
    assert_eq!(d.to_string(), "2024-02-29");
    assert_eq!(d.succ(), CivilDate::new(2024, 3, 1));
    assert_eq!(CivilDate::new(2023, 2, 29), None);
    assert_eq!(CivilDate::parse("2024-02-29").unwrap(), d);
    assert!(
        CivilDate::parse("2024-2-30")
            .unwrap_err()
            .to_string()
            .starts_with("date: ")
    );
    assert_eq!(CivilDate::from_chrono(NaiveDate::MAX).succ(), None);
    assert_eq!(CivilDate::from_chrono(d.to_chrono()), d);
    let t = CivilTime::new(1, 2, 3, 4).unwrap();
    assert_eq!(t.to_string(), "01:02:03.000000004");
    assert_eq!(CivilTime::new(24, 0, 0, 0), None);
    assert_eq!(CivilTime::MIDNIGHT.to_string(), "00:00:00");
    assert_eq!(CivilTime::from_chrono(t.to_chrono()), t);
    let dt = d.at(t);
    assert_eq!(dt.date(), d);
    assert_eq!(dt.time(), t);
    assert_eq!(dt.to_string(), "2024-02-29 01:02:03.000000004");
    assert_eq!(dt.in_utc(), ts("2024-02-29T01:02:03.000000004Z"));
    assert_eq!(dt.in_utc().civil(), dt);
    assert_eq!(CivilDateTime::from_chrono(dt.to_chrono()), dt);
}

#[cfg(feature = "time-zones")]
mod zones {
    use super::*;

    fn civil(text: &str) -> CivilDateTime {
        CivilDateTime::from_chrono(NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M").unwrap())
    }

    #[test]
    fn parse_is_exact() {
        assert_eq!(
            Zone::parse("Europe/London").unwrap().name(),
            "Europe/London"
        );
        assert_eq!(Zone::parse("UTC").unwrap(), Zone::UTC);
        assert!(Zone::parse("Etc/UTC").is_ok());
        assert!(Zone::parse("US/Eastern").is_ok());
        assert!(Zone::parse("Europe/Kiev").is_ok());
        assert!(Zone::parse("Europe/Kyiv").is_ok());
        assert!(Zone::parse("EST").is_ok());
        let err = Zone::parse("utc").unwrap_err();
        assert_eq!(err.to_string(), "time zone: unknown zone 'utc'");
        assert!(Zone::parse("Mars/Olympus").is_err());
        let z = Zone::parse("Asia/Kolkata").unwrap();
        assert_eq!(Zone::from_chrono(z.to_chrono()), z);
    }

    #[test]
    fn to_local_and_resolve_around_transitions() {
        let london = Zone::parse("Europe/London").unwrap();
        assert_eq!(
            london
                .to_local(ts("2025-07-01T12:00:00Z"))
                .to_rfc3339(Precision::Secs),
            "2025-07-01T13:00:00+01:00"
        );
        assert_eq!(
            london.resolve_local(civil("2025-03-30 00:30")),
            LocalTime::Single(OffsetClock::parse_rfc3339("2025-03-30T00:30:00+00:00").unwrap())
        );
        assert_eq!(
            london.resolve_local(civil("2025-03-30 01:30")),
            LocalTime::Gap
        );
        assert_eq!(
            london.resolve_local(civil("2025-10-26 01:30")),
            LocalTime::Fold {
                earlier: OffsetClock::parse_rfc3339("2025-10-26T01:30:00+01:00").unwrap(),
                later: OffsetClock::parse_rfc3339("2025-10-26T01:30:00+00:00").unwrap(),
            }
        );
        let lord_howe = Zone::parse("Australia/Lord_Howe").unwrap();
        assert_eq!(
            lord_howe.resolve_local(civil("2025-10-05 02:15")),
            LocalTime::Gap
        );
        assert!(matches!(
            lord_howe.resolve_local(civil("2025-04-06 01:45")),
            LocalTime::Fold { .. }
        ));
        let santiago = Zone::parse("America/Santiago").unwrap();
        assert_eq!(
            santiago.resolve_local(civil("2025-09-07 00:30")),
            LocalTime::Gap
        );
    }
}

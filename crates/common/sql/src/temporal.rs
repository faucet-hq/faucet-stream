//! Offset-bearing instants bound for columns without a zone.
//!
//! A `TIMESTAMP` (without time zone) or `TIME` column drops the offset of a
//! value like `2026-09-18T12:00:00+02:00` and stores `12:00:00`. Every sink
//! writing such a column converts the value to UTC wall time first, so an
//! instant from a zoned source lands as the same instant in UTC.

/// A column type without a zone that a sink stores instants in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NaiveTemporal {
    /// A date and time of day (`timestamp`, `timestamp without time zone`,
    /// `timestamp(3)`, `datetime`, `datetime2`).
    Timestamp,
    /// A time of day (`time`, `time without time zone`).
    Time,
}

/// Which zone-less temporal type `sql_type` is, if any. Zoned types
/// (`timestamptz`, `timestamp with time zone`, `timetz`) are `None`.
pub fn naive_temporal(sql_type: &str) -> Option<NaiveTemporal> {
    // Lower-cased, `(…)` dropped, whitespace collapsed; the longest name is 27 bytes.
    let mut base = [0u8; 32];
    let mut len = 0usize;
    let mut depth = 0usize;
    let mut gap = false;
    for c in sql_type.chars() {
        match c {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth > 0 => {}
            _ if c.is_whitespace() => gap = len > 0,
            _ => {
                if !c.is_ascii() {
                    return None;
                }
                if gap {
                    *base.get_mut(len)? = b' ';
                    len += 1;
                    gap = false;
                }
                *base.get_mut(len)? = (c as u8).to_ascii_lowercase();
                len += 1;
            }
        }
    }
    match &base[..len] {
        b"timestamp" | b"timestamp without time zone" | b"datetime" | b"datetime2" => {
            Some(NaiveTemporal::Timestamp)
        }
        b"time" | b"time without time zone" => Some(NaiveTemporal::Time),
        _ => None,
    }
}

/// The text to bind for `text` into a column of `sql_type`: the UTC wall time
/// of an offset-bearing value when the column has no zone. `None` when the
/// column is zoned or not temporal, or `text` carries no offset — bind it
/// unchanged then.
pub fn naive_temporal_text(sql_type: &str, text: &str) -> Option<String> {
    match naive_temporal(sql_type)? {
        NaiveTemporal::Timestamp => faucet_core::fidelity::offset_instant_utc(text)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.f").to_string()),
        NaiveTemporal::Time => faucet_core::fidelity::offset_time_utc(text)
            .map(|t| t.format("%H:%M:%S%.f").to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The classification as it was computed with owned strings.
    fn reference(sql_type: &str) -> Option<NaiveTemporal> {
        let lower = sql_type.trim().to_ascii_lowercase();
        let mut base = String::with_capacity(lower.len());
        let mut depth = 0usize;
        for c in lower.chars() {
            match c {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                _ if depth == 0 => base.push(c),
                _ => {}
            }
        }
        let base = base.split_whitespace().collect::<Vec<_>>().join(" ");
        match base.as_str() {
            "timestamp" | "timestamp without time zone" | "datetime" | "datetime2" => {
                Some(NaiveTemporal::Timestamp)
            }
            "time" | "time without time zone" => Some(NaiveTemporal::Time),
            _ => None,
        }
    }

    #[test]
    fn classification_matches_the_string_built_one() {
        let pieces = [
            "timestamp",
            "TIMESTAMP",
            "Time",
            "datetime",
            "DateTime2",
            "without",
            "time",
            "zone",
            "with",
            "tz",
            "timestamptz",
            "timetz",
            "text",
            "(3)",
            "(",
            ")",
            "(6) ",
            " ",
            "  ",
            "\t",
            "\n",
            "\u{a0}",
            "\u{2003}",
            "é",
            "x",
            "",
            "((a)",
            "timestamp without time zone",
            "TIME WITHOUT TIME ZONE",
            "time(3)stamp",
            "a-very-long-type-name-that-overflows-it",
        ];
        let mut checked = 0;
        for a in pieces {
            for b in pieces {
                for c in pieces {
                    for sep in ["", " "] {
                        let t = format!("{a}{sep}{b}{sep}{c}");
                        assert_eq!(naive_temporal(&t), reference(&t), "{t:?}");
                        checked += 1;
                    }
                }
            }
        }
        assert!(checked > 50_000);
        assert_eq!(
            naive_temporal(" Timestamp(3)\u{a0}WITHOUT  time\tzone "),
            Some(NaiveTemporal::Timestamp)
        );
    }

    #[test]
    fn zone_less_types_are_recognised() {
        for t in [
            "timestamp",
            "TIMESTAMP(3)",
            "timestamp without time zone",
            "timestamp(6) without time zone",
            "datetime2(7)",
        ] {
            assert_eq!(naive_temporal(t), Some(NaiveTemporal::Timestamp), "{t}");
        }
        assert_eq!(
            naive_temporal("time without time zone"),
            Some(NaiveTemporal::Time)
        );
        for t in [
            "timestamptz",
            "timestamp with time zone",
            "timetz",
            "date",
            "text",
        ] {
            assert_eq!(naive_temporal(t), None, "{t}");
        }
    }

    #[test]
    fn offset_values_become_utc_wall_time() {
        assert_eq!(
            naive_temporal_text("timestamp", "2026-09-18T12:00:00+02:00").as_deref(),
            Some("2026-09-18 10:00:00")
        );
        assert_eq!(
            naive_temporal_text("timestamp", "2026-09-18T00:30:00.5+01:00").as_deref(),
            Some("2026-09-17 23:30:00.500")
        );
        assert_eq!(
            naive_temporal_text("time", "01:00:00+02:00").as_deref(),
            Some("23:00:00")
        );
        assert_eq!(
            naive_temporal_text("timestamp", "2026-09-18 12:00:00"),
            None
        );
        assert_eq!(
            naive_temporal_text("timestamptz", "2026-09-18T12:00:00+02:00"),
            None
        );
        assert_eq!(
            naive_temporal_text("text", "2026-09-18T12:00:00+02:00"),
            None
        );
    }
}

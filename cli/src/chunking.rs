//! Pure range chunking shared by `faucet backfill` and the `partition:` block
//! (#479). Boundary parsing, timezone/DST-correct time windows, integer-range
//! splitting, and offset/limit splitting. No I/O.
//!
//! Both consumers are CLI-layer — the substitution that uses these chunks walks
//! config strings, which no connector crate participates in — so this lives here
//! rather than in `faucet-core`. That also avoids making `chrono` non-optional
//! and adding `chrono-tz` to core, neither of which a connector author needs.
//!
//! The time half was moved here verbatim from `backfill::plan`, which now
//! re-exports it, so `faucet backfill` keeps byte-identical planning (and its
//! existing tests keep passing against the moved code).
//!
//! ## Bounds are the correctness hazard
//!
//! An integer range can be split two ways, and picking wrong is silent data
//! loss, not an error. With `chunk_size: 10000` from 0:
//!
//! | [`Bounds`] | chunk 1 | chunk 2 | emitted `end` |
//! |---|---|---|---|
//! | `Inclusive` | `[0, 9999]` | `[10000, 19999]` | `9999` |
//! | `HalfOpen`  | `[0, 10000)` | `[10000, 20000)` | `10000` |
//!
//! Half-open chunks against an API whose upper bound is inclusive fetch record
//! 10000 **twice**; inclusive chunks against an exclusive API **never fetch**
//! record 9999. Neither surfaces as a failure, which is why the config field has
//! no default — the user has to state which their source is.

use crate::error::{CliError, CliResult};
use chrono::{DateTime, Duration, FixedOffset, TimeZone, Utc};

/// Hard ceiling on planned chunks — a tiny window over a huge range is a config
/// error, not a workload.
pub const MAX_UNITS: usize = 10_000;
/// Above this many chunks a loud warning is emitted (but planning proceeds).
pub const WARN_UNITS: usize = 1_000;

// ── Time windows ─────────────────────────────────────────────────────────────

/// One independent, resumable slice of a time range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeChunk {
    /// Stable id — the UTC start instant, compact (`20260601T000000Z`).
    /// Doubles as the state-key suffix.
    pub id: String,
    /// Half-open window start (inclusive). Carried in the range's timezone
    /// offset so `${now.*}` renders local dates.
    pub start: DateTime<FixedOffset>,
    /// Half-open window end (exclusive).
    pub end: DateTime<FixedOffset>,
}

/// How a window advances the cursor.
///
/// The distinction matters only in a timezone that observes DST, and only for
/// day/week windows: stepping a "day" by a fixed 24 hours drifts off local
/// midnight after a transition, so the unit labelled `2026-03-08` would cover
/// 00:00 → *next day* 01:00 (25 local hours) and every later unit would start an
/// hour late. Sub-day windows have no such expectation — an hour is an hour — so
/// they stay absolute (#461).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowStep {
    /// A fixed elapsed duration (`s` / `m` / `h`, or a bare integer = seconds).
    Absolute(Duration),
    /// N calendar days in the range's timezone.
    Days(i64),
    /// N calendar weeks in the range's timezone.
    Weeks(i64),
}

impl std::fmt::Display for WindowStep {
    /// Stable form used in the range-hash descriptor that keys a progress
    /// marker.
    ///
    /// An absolute window renders as its **seconds**, exactly as before this type
    /// existed, so a backfill already in flight keeps its marker and resumes. A
    /// calendar window renders as `Nd` / `Nw`, which deliberately hashes
    /// differently: its unit boundaries are not the ones the old plan produced, so
    /// resuming against a marker from that plan would mix two different unit sets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Absolute(d) => write!(f, "{}", d.num_seconds()),
            Self::Days(n) => write!(f, "{n}d"),
            Self::Weeks(n) => write!(f, "{n}w"),
        }
    }
}

impl WindowStep {
    /// The nominal duration, for logging and for the absolute fallback.
    fn nominal(self) -> Duration {
        match self {
            Self::Absolute(d) => d,
            Self::Days(n) => Duration::days(n),
            Self::Weeks(n) => Duration::weeks(n),
        }
    }
}

/// Parse a window duration: `45s`, `30m`, `6h`, `1d`, `1w` (or a bare integer =
/// seconds). Must be positive.
///
/// `d` / `w` yield **calendar** steps; everything else is absolute. So `1d` and
/// `24h` differ across a DST transition, deliberately.
pub fn parse_window(s: &str) -> CliResult<WindowStep> {
    let s = s.trim();
    let err = || {
        CliError::Config(format!(
            "'{s}' is not a valid window — use e.g. 45s, 30m, 6h, 1d, 1w"
        ))
    };
    let (num, unit) = match s.chars().last() {
        Some(c) if c.is_ascii_digit() => (s, "s"),
        Some(c) => (&s[..s.len() - c.len_utf8()], &s[s.len() - c.len_utf8()..]),
        None => return Err(err()),
    };
    let n: i64 = num.parse().map_err(|_| err())?;
    if n <= 0 {
        return Err(CliError::Config(format!(
            "window '{s}' must be a positive duration"
        )));
    }
    let out_of_range = || CliError::Config(format!("window '{s}' is too large"));
    let step = match unit {
        "s" => WindowStep::Absolute(Duration::try_seconds(n).ok_or_else(out_of_range)?),
        "m" => WindowStep::Absolute(Duration::try_minutes(n).ok_or_else(out_of_range)?),
        "h" => WindowStep::Absolute(Duration::try_hours(n).ok_or_else(out_of_range)?),
        "d" => WindowStep::Days(n),
        "w" => WindowStep::Weeks(n),
        _ => return Err(err()),
    };
    Ok(step)
}

/// Advance `cursor` by a calendar amount in `tz`, keeping the local wall-clock
/// time (so a day stays a day and midnight stays midnight across a DST change).
///
/// The naive local time is advanced first — naive arithmetic has no DST, so this
/// *is* calendar arithmetic — then re-resolved in `tz`. A spring-forward gap
/// (the wall-clock time does not exist that day) has no valid instant, so the
/// time is nudged forward an hour at a time until it does; a fall-back
/// (ambiguous, repeated) hour resolves to the **earliest** instant, matching
/// [`parse_boundary`].
fn advance_calendar(cursor: DateTime<Utc>, tz: chrono_tz::Tz, days: i64) -> Option<DateTime<Utc>> {
    let naive = cursor
        .with_timezone(&tz)
        .naive_local()
        .checked_add_signed(Duration::days(days))?;
    for extra_hours in 0..=3 {
        let candidate = naive.checked_add_signed(Duration::hours(extra_hours))?;
        if let Some(local) = tz.from_local_datetime(&candidate).earliest() {
            return Some(local.with_timezone(&Utc));
        }
    }
    None
}

/// Parse a range boundary: RFC3339 (`2026-06-01T00:00:00Z`) or a bare date
/// (`2026-06-01`, interpreted as midnight in `tz`). A midnight that falls in a
/// DST gap resolves to the earliest valid instant after the gap; an ambiguous
/// midnight resolves to the earlier of its two instants.
pub fn parse_boundary(s: &str, tz: chrono_tz::Tz) -> CliResult<DateTime<FixedOffset>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Ok(dt.with_timezone(&tz).fixed_offset());
    }
    if let Ok(date) = s.parse::<chrono::NaiveDate>() {
        let midnight = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::Config(format!("'{s}' has no valid midnight in {tz}")))?;
        let local = (0..=180)
            .filter_map(|m| midnight.checked_add_signed(Duration::minutes(m)))
            .find_map(|t| tz.from_local_datetime(&t).earliest())
            .ok_or_else(|| {
                CliError::Config(format!("'{s}' midnight does not exist in {tz} (DST gap)"))
            })?;
        return Ok(local.fixed_offset());
    }
    Err(CliError::Config(format!(
        "'{s}' is not RFC3339 (2026-06-01T00:00:00Z) or a date (2026-06-01)"
    )))
}

/// Chunk `[from, to)` into contiguous half-open windows of `window` (the last
/// window truncated at `to`). `window: None` = the whole range as one unit.
/// Window arithmetic is absolute (instants), so units never gap or overlap —
/// including across DST transitions; boundaries are re-rendered in `tz` so
/// `${now.*}` tokens see local wall-clock time.
pub fn plan_windows(
    from: DateTime<FixedOffset>,
    to: DateTime<FixedOffset>,
    window: Option<WindowStep>,
    tz: chrono_tz::Tz,
) -> CliResult<Vec<TimeChunk>> {
    if from >= to {
        return Err(CliError::Config(format!(
            "--from ({from}) must be before --to ({to})"
        )));
    }
    let mut units = Vec::new();
    let mut cursor = from.with_timezone(&Utc);
    let end = to.with_timezone(&Utc);
    let step = window.unwrap_or(WindowStep::Absolute(end - cursor));
    while cursor < end {
        if units.len() >= MAX_UNITS {
            return Err(CliError::Config(format!(
                "the range would produce more than {MAX_UNITS} units with this --window — \
                 use a larger window"
            )));
        }
        // Calendar steps keep the local wall clock; absolute steps add elapsed
        // time. Either way the next boundary must be strictly ahead of the
        // cursor, or the loop could not terminate — fall back to the nominal
        // duration if a zone quirk ever produced a non-advancing instant.
        let next = match step {
            WindowStep::Absolute(d) => cursor + d,
            WindowStep::Days(n) => {
                advance_calendar(cursor, tz, n).unwrap_or(cursor + step.nominal())
            }
            WindowStep::Weeks(n) => {
                advance_calendar(cursor, tz, n * 7).unwrap_or(cursor + step.nominal())
            }
        };
        let next = if next > cursor {
            next
        } else {
            cursor + step.nominal()
        };
        let unit_end = next.min(end);
        units.push(TimeChunk {
            id: cursor.format("%Y%m%dT%H%M%SZ").to_string(),
            start: cursor.with_timezone(&tz).fixed_offset(),
            end: unit_end.with_timezone(&tz).fixed_offset(),
        });
        cursor = unit_end;
    }
    Ok(units)
}

// ── Integer ranges ───────────────────────────────────────────────────────────

/// Whether a chunk's upper edge is included. See the module docs — this has no
/// default on purpose.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Bounds {
    /// `end` is the last value in the chunk: `[start, end]`. For a source whose
    /// upper-bound filter is inclusive (`id_to=9999` returns 9999).
    Inclusive,
    /// `end` is the first value *after* the chunk: `[start, end)`. For a source
    /// whose upper-bound filter is exclusive.
    HalfOpen,
}

/// One independent slice of an integer range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntChunk {
    /// Stable id: the chunk's plan index, unpadded so it does not change when
    /// the chunk count crosses a power of ten (it keys the chunk's state).
    pub id: String,
    /// Inclusive lower bound.
    pub start: i64,
    /// Upper bound, interpreted per the [`Bounds`] the plan was built with.
    pub end: i64,
    /// True for the final chunk. A caller that was asked for an open-ended tail
    /// renders this chunk without an upper-bound predicate, so rows appended
    /// above the planned maximum between planning and execution are still read.
    pub is_last: bool,
}

/// Split `[from, to]` (or `[from, to)` per `bounds`) into contiguous chunks of
/// at most `chunk_size` values.
///
/// The union of the returned chunks tiles the range exactly once — no gap, no
/// overlap — under either `bounds`, which is the property the tests pin.
pub fn plan_int_chunks(
    from: i64,
    to: i64,
    chunk_size: u64,
    bounds: Bounds,
) -> CliResult<Vec<IntChunk>> {
    if chunk_size == 0 {
        return Err(CliError::Config(
            "partition.chunk_size must be greater than 0".into(),
        ));
    }
    // Width in values. i128 so a range spanning i64::MIN..i64::MAX cannot wrap.
    let span: i128 = match bounds {
        Bounds::Inclusive => to as i128 - from as i128 + 1,
        Bounds::HalfOpen => to as i128 - from as i128,
    };
    if span <= 0 {
        return Err(CliError::Config(format!(
            "partition range is empty: from ({from}) must be {} to ({to})",
            match bounds {
                Bounds::Inclusive => "less than or equal to",
                Bounds::HalfOpen => "less than",
            }
        )));
    }
    // `span` is positive past the guard above, so unsigned division is safe —
    // and `div_ceil` is stable for unsigned integers only.
    let size = chunk_size as u128;
    let count = (span as u128).div_ceil(size) as i128;
    if count > MAX_UNITS as i128 {
        return Err(CliError::Config(format!(
            "the range would produce {count} chunks with chunk_size {chunk_size} \
             (max {MAX_UNITS}) — use a larger chunk_size"
        )));
    }
    let mut out = Vec::with_capacity(count as usize);
    let mut cursor = from as i128;
    for i in 0..count {
        let next = cursor + size as i128;
        let is_last = i == count - 1;
        // The final chunk is truncated at the requested bound rather than
        // overshooting it.
        let raw_end = match bounds {
            Bounds::Inclusive => (next - 1).min(to as i128),
            Bounds::HalfOpen => next.min(to as i128),
        };
        out.push(IntChunk {
            id: i.to_string(),
            start: cursor as i64,
            end: raw_end as i64,
            is_last,
        });
        cursor = next;
    }
    Ok(out)
}

// ── Offset / limit ───────────────────────────────────────────────────────────

/// One `offset`/`limit` slice of a countable result set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetChunk {
    pub id: String,
    pub offset: u64,
    pub limit: u64,
}

/// Split a result set of `total` rows into `offset`/`limit` chunks.
///
/// This is the parallel form of what a source's serial offset pagination already
/// does; it takes a **count**, never a maximum key. Chunking an id range from a
/// count is wrong the moment ids are sparse — see the `partition` reference.
pub fn plan_offset_chunks(total: u64, chunk_size: u64) -> CliResult<Vec<OffsetChunk>> {
    if chunk_size == 0 {
        return Err(CliError::Config(
            "partition.chunk_size must be greater than 0".into(),
        ));
    }
    if total == 0 {
        return Ok(Vec::new());
    }
    let count = total.div_ceil(chunk_size);
    if count > MAX_UNITS as u64 {
        return Err(CliError::Config(format!(
            "a total of {total} would produce {count} chunks with chunk_size {chunk_size} \
             (max {MAX_UNITS}) — use a larger chunk_size"
        )));
    }
    Ok((0..count)
        .map(|i| OffsetChunk {
            id: i.to_string(),
            offset: i * chunk_size,
            limit: chunk_size.min(total - i * chunk_size),
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    // ── Time windows (moved verbatim from backfill::plan, proving the move
    // did not change planning behaviour) ────────────────────────────────────

    fn tz(name: &str) -> chrono_tz::Tz {
        name.parse().unwrap()
    }

    #[test]
    fn window_durations_parse() {
        // Sub-day units are absolute elapsed time…
        assert_eq!(
            parse_window("45s").unwrap(),
            WindowStep::Absolute(Duration::seconds(45))
        );
        assert_eq!(
            parse_window("30m").unwrap(),
            WindowStep::Absolute(Duration::minutes(30))
        );
        assert_eq!(
            parse_window("6h").unwrap(),
            WindowStep::Absolute(Duration::hours(6))
        );
        assert_eq!(
            parse_window("3600").unwrap(),
            WindowStep::Absolute(Duration::seconds(3600))
        );
        // …while day/week units are calendar steps (#461).
        assert_eq!(parse_window("1d").unwrap(), WindowStep::Days(1));
        assert_eq!(parse_window("2w").unwrap(), WindowStep::Weeks(2));
        assert!(parse_window("0d").is_err());
        assert!(parse_window("-1h").is_err());
        assert!(parse_window("soon").is_err());
        assert!(parse_window("1y").is_err());
    }

    #[test]
    fn boundaries_parse_rfc3339_and_dates() {
        let utc = tz("UTC");
        let dt = parse_boundary("2026-06-01T12:30:00Z", utc).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-06-01T12:30:00+00:00");
        // A bare date is midnight in the given timezone.
        let ny = tz("America/New_York");
        let dt = parse_boundary("2026-06-01", ny).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-06-01T00:00:00-04:00");
        assert!(parse_boundary("yesterday", utc).is_err());
    }

    #[test]
    fn thirty_one_days_one_day_window_is_31_units() {
        // The acceptance-criteria example: a 31-day June-July range with a 1d
        // window plans exactly 31 units.
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-01", utc).unwrap();
        let to = parse_boundary("2026-07-02", utc).unwrap();
        let units = plan_windows(from, to, Some(WindowStep::Days(1)), utc).unwrap();
        assert_eq!(units.len(), 31);
        assert_eq!(units[0].id, "20260601T000000Z");
        assert_eq!(units[0].start.to_rfc3339(), "2026-06-01T00:00:00+00:00");
        assert_eq!(units[0].end.to_rfc3339(), "2026-06-02T00:00:00+00:00");
        // Contiguous half-open windows: each start equals the previous end.
        for w in units.windows(2) {
            assert_eq!(w[0].end, w[1].start);
        }
        assert_eq!(units[30].end.to_rfc3339(), "2026-07-02T00:00:00+00:00");
    }

    #[test]
    fn last_window_truncates_at_to() {
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-01T00:00:00Z", utc).unwrap();
        let to = parse_boundary("2026-06-01T05:30:00Z", utc).unwrap();
        let units = plan_windows(
            from,
            to,
            Some(WindowStep::Absolute(Duration::hours(2))),
            utc,
        )
        .unwrap();
        assert_eq!(units.len(), 3);
        assert_eq!(units[2].start.to_rfc3339(), "2026-06-01T04:00:00+00:00");
        assert_eq!(units[2].end.to_rfc3339(), "2026-06-01T05:30:00+00:00");
    }

    #[test]
    fn no_window_is_a_single_unit() {
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-01", utc).unwrap();
        let to = parse_boundary("2026-07-01", utc).unwrap();
        let units = plan_windows(from, to, None, utc).unwrap();
        assert_eq!(units.len(), 1);
        assert_eq!(units[0].start, from);
        assert_eq!(units[0].end, to);
    }

    /// #461: a calendar day must stay a calendar day. Absolute 24h stepping used
    /// to drift off local midnight after a DST change — the unit labelled
    /// 2026-03-08 covered 00:00 → *next day* 01:00 (25 local hours) and every
    /// later unit started an hour late, so `${backfill.start_date}` no longer
    /// described the window it named.
    #[test]
    fn calendar_day_windows_stay_on_local_midnight_across_dst() {
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-11", ny).unwrap();
        let units = plan_windows(from, to, Some(WindowStep::Days(1)), ny).unwrap();

        assert_eq!(units.len(), 4, "four calendar days");
        for u in &units {
            assert_eq!(
                (u.start.hour(), u.start.minute()),
                (0, 0),
                "unit {} must start at local midnight, got {}",
                u.id,
                u.start
            );
        }
        // Contiguous, and each unit's label matches the day it covers.
        for w in units.windows(2) {
            assert_eq!(w[0].end, w[1].start, "no gap/overlap");
        }
        let dates: Vec<String> = units
            .iter()
            .map(|u| u.start.format("%Y-%m-%d").to_string())
            .collect();
        assert_eq!(
            dates,
            ["2026-03-07", "2026-03-08", "2026-03-09", "2026-03-10"]
        );
        // The spring-forward day is genuinely 23 hours of elapsed time.
        let spring_forward = &units[1];
        assert_eq!(
            (spring_forward.end - spring_forward.start).num_hours(),
            23,
            "2026-03-08 loses an hour"
        );
    }

    /// Fall-back (an hour repeats) must also stay on midnight, at 25 elapsed hours.
    #[test]
    fn calendar_day_windows_handle_fall_back() {
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-10-31", ny).unwrap();
        let to = parse_boundary("2026-11-03", ny).unwrap();
        let units = plan_windows(from, to, Some(WindowStep::Days(1)), ny).unwrap();
        for u in &units {
            assert_eq!((u.start.hour(), u.start.minute()), (0, 0), "{}", u.id);
        }
        // 2026-11-01 is the fall-back day: 25 hours.
        let long_day = units
            .iter()
            .find(|u| u.start.format("%Y-%m-%d").to_string() == "2026-11-01")
            .expect("the fall-back day is planned");
        assert_eq!((long_day.end - long_day.start).num_hours(), 25);
    }

    /// `1d` and `24h` are deliberately different across a transition: one is a
    /// calendar day, the other is elapsed time.
    #[test]
    fn calendar_and_absolute_windows_differ_across_dst() {
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-10", ny).unwrap();
        let cal = plan_windows(from, to, Some(parse_window("1d").unwrap()), ny).unwrap();
        let abs = plan_windows(from, to, Some(parse_window("24h").unwrap()), ny).unwrap();
        assert_eq!(cal[2].start.hour(), 0, "calendar stays on midnight");
        assert_eq!(abs[2].start.hour(), 1, "absolute drifts by the DST delta");
        assert_ne!(cal[2].start, abs[2].start);
    }

    /// The descriptor an absolute window contributes to the range hash is
    /// unchanged, so a backfill already in flight keeps resuming.
    #[test]
    fn window_descriptor_is_stable_for_absolute_and_distinct_for_calendar() {
        assert_eq!(
            WindowStep::Absolute(Duration::hours(6)).to_string(),
            "21600"
        );
        assert_eq!(WindowStep::Absolute(Duration::days(1)).to_string(), "86400");
        assert_eq!(WindowStep::Days(1).to_string(), "1d");
        assert_eq!(WindowStep::Weeks(2).to_string(), "2w");
    }

    #[test]
    fn dst_transition_produces_no_gap_or_overlap() {
        // US spring-forward 2026: March 8, 02:00 EST → 03:00 EDT. Absolute
        // 1-day windows stay contiguous; local render shows the offset flip.
        let ny = tz("America/New_York");
        let from = parse_boundary("2026-03-07", ny).unwrap();
        let to = parse_boundary("2026-03-10T00:00:00-04:00", ny).unwrap();
        let units =
            plan_windows(from, to, Some(WindowStep::Absolute(Duration::days(1))), ny).unwrap();
        for w in units.windows(2) {
            assert_eq!(w[0].end, w[1].start, "no gap/overlap across DST");
        }
        // First window starts EST (-05:00); a later one renders EDT (-04:00).
        assert!(units[0].start.to_rfc3339().ends_with("-05:00"));
        assert!(units.last().unwrap().end.to_rfc3339().ends_with("-04:00"));
    }

    #[test]
    fn rejects_inverted_range_and_unit_explosion() {
        let utc = tz("UTC");
        let from = parse_boundary("2026-06-02", utc).unwrap();
        let to = parse_boundary("2026-06-01", utc).unwrap();
        assert!(plan_windows(from, to, None, utc).is_err());

        let from = parse_boundary("2020-01-01", utc).unwrap();
        let to = parse_boundary("2026-01-01", utc).unwrap();
        let err = plan_windows(
            from,
            to,
            Some(WindowStep::Absolute(Duration::minutes(1))),
            utc,
        )
        .unwrap_err();
        assert!(err.to_string().contains("larger window"), "{err}");
    }

    // ── Integer chunking ─────────────────────────────────────────────────────

    /// The property that matters: the chunks tile the range exactly once. A gap
    /// silently drops records; an overlap silently duplicates them.
    fn covered(chunks: &[IntChunk], bounds: Bounds) -> Vec<i64> {
        let mut seen = Vec::new();
        for c in chunks {
            let last = match bounds {
                Bounds::Inclusive => c.end,
                Bounds::HalfOpen => c.end - 1,
            };
            for v in c.start..=last {
                seen.push(v);
            }
        }
        seen
    }

    #[test]
    fn inclusive_chunks_tile_the_range_exactly_once() {
        let chunks = plan_int_chunks(0, 24, 10, Bounds::Inclusive).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!((chunks[0].start, chunks[0].end), (0, 9));
        assert_eq!((chunks[1].start, chunks[1].end), (10, 19));
        assert_eq!((chunks[2].start, chunks[2].end), (20, 24), "last truncated");
        assert_eq!(
            covered(&chunks, Bounds::Inclusive),
            (0..=24).collect::<Vec<_>>()
        );
    }

    #[test]
    fn half_open_chunks_tile_the_range_exactly_once() {
        let chunks = plan_int_chunks(0, 25, 10, Bounds::HalfOpen).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!((chunks[0].start, chunks[0].end), (0, 10));
        assert_eq!((chunks[1].start, chunks[1].end), (10, 20));
        assert_eq!((chunks[2].start, chunks[2].end), (20, 25));
        assert_eq!(
            covered(&chunks, Bounds::HalfOpen),
            (0..25).collect::<Vec<_>>()
        );
    }

    #[test]
    fn the_two_bounds_differ_by_exactly_one_at_every_boundary() {
        // The concrete failure mode: pick the wrong one and every boundary
        // either duplicates or drops a record.
        let inc = plan_int_chunks(0, 19, 10, Bounds::Inclusive).unwrap();
        let half = plan_int_chunks(0, 20, 10, Bounds::HalfOpen).unwrap();
        assert_eq!(inc[0].end, 9);
        assert_eq!(half[0].end, 10);
        assert_eq!(inc[0].end + 1, half[0].end);
    }

    #[test]
    fn tiles_exactly_once_across_many_sizes_and_ranges() {
        for from in [-7i64, 0, 5, 1000] {
            for span in [1i64, 2, 7, 10, 33, 100] {
                for size in [1u64, 2, 3, 10, 64] {
                    let to = from + span - 1;
                    let chunks = plan_int_chunks(from, to, size, Bounds::Inclusive).unwrap();
                    assert_eq!(
                        covered(&chunks, Bounds::Inclusive),
                        (from..=to).collect::<Vec<_>>(),
                        "inclusive from={from} span={span} size={size}"
                    );
                    let chunks =
                        plan_int_chunks(from, from + span, size, Bounds::HalfOpen).unwrap();
                    assert_eq!(
                        covered(&chunks, Bounds::HalfOpen),
                        (from..from + span).collect::<Vec<_>>(),
                        "half-open from={from} span={span} size={size}"
                    );
                }
            }
        }
    }

    #[test]
    fn a_single_value_range_is_one_chunk_inclusive_and_empty_half_open() {
        let inc = plan_int_chunks(5, 5, 10, Bounds::Inclusive).unwrap();
        assert_eq!(inc.len(), 1);
        assert_eq!((inc[0].start, inc[0].end), (5, 5));
        // Half-open [5,5) contains nothing, so it is an error rather than a
        // silent zero-chunk plan that would fetch nothing.
        assert!(plan_int_chunks(5, 5, 10, Bounds::HalfOpen).is_err());
    }

    #[test]
    fn only_the_final_chunk_is_marked_last() {
        let chunks = plan_int_chunks(0, 29, 10, Bounds::Inclusive).unwrap();
        assert_eq!(
            chunks.iter().filter(|c| c.is_last).count(),
            1,
            "exactly one chunk carries the open-ended tail flag"
        );
        assert!(chunks.last().unwrap().is_last);
    }

    #[test]
    fn ids_do_not_depend_on_the_chunk_count() {
        let nine = plan_int_chunks(0, 8, 1, Bounds::Inclusive).unwrap();
        let hundred = plan_int_chunks(0, 99, 1, Bounds::Inclusive).unwrap();
        for (a, b) in nine.iter().zip(&hundred) {
            assert_eq!(a.id, b.id, "a chunk keeps its id when the plan grows");
        }
        assert_eq!(hundred[42].id, "42");
        let offsets9 = plan_offset_chunks(9, 1).unwrap();
        let offsets11 = plan_offset_chunks(11, 1).unwrap();
        assert_eq!(offsets9[3].id, offsets11[3].id);
        assert_eq!(offsets11[3].id, "3");
    }

    #[test]
    fn rejects_inverted_and_empty_ranges() {
        assert!(plan_int_chunks(10, 5, 10, Bounds::Inclusive).is_err());
        assert!(plan_int_chunks(10, 10, 10, Bounds::HalfOpen).is_err());
    }

    #[test]
    fn a_window_too_large_for_a_duration_is_a_usage_error_not_a_panic() {
        for w in ["99999999999999999s", "999999999999999m", "99999999999999h"] {
            let err = parse_window(w).unwrap_err();
            assert!(err.to_string().contains("too large"), "{w}: {err}");
        }
        assert!(parse_window("5é").is_err());
    }

    #[test]
    fn a_midnight_in_a_dst_gap_resolves_to_the_first_instant_after_it() {
        // Santiago springs forward at 00:00 → 01:00 on 2023-09-03.
        let tz: chrono_tz::Tz = "America/Santiago".parse().unwrap();
        let b = parse_boundary("2023-09-03", tz).unwrap();
        assert_eq!(b.to_rfc3339(), "2023-09-03T01:00:00-03:00");
    }

    #[test]
    fn rejects_zero_chunk_size() {
        // Deliberately different from the `batch_size: 0` sentinel elsewhere —
        // zero here would mean infinite chunks.
        let err = plan_int_chunks(0, 10, 0, Bounds::Inclusive).unwrap_err();
        assert!(err.to_string().contains("greater than 0"), "{err}");
    }

    #[test]
    fn rejects_a_chunk_explosion() {
        let err = plan_int_chunks(0, 10_000_000, 1, Bounds::Inclusive).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("larger chunk_size"), "{msg}");
        assert!(msg.contains(&MAX_UNITS.to_string()), "names the cap: {msg}");
    }

    #[test]
    fn does_not_overflow_near_i64_bounds() {
        let chunks = plan_int_chunks(i64::MAX - 5, i64::MAX, 2, Bounds::Inclusive).unwrap();
        assert_eq!(covered(&chunks, Bounds::Inclusive).len(), 6);
        let chunks = plan_int_chunks(i64::MIN, i64::MIN + 5, 2, Bounds::Inclusive).unwrap();
        assert_eq!(covered(&chunks, Bounds::Inclusive).len(), 6);
    }

    // ── Offset chunking ──────────────────────────────────────────────────────

    #[test]
    fn offset_chunks_cover_the_total_without_overrunning_it() {
        let chunks = plan_offset_chunks(25, 10).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!((chunks[0].offset, chunks[0].limit), (0, 10));
        assert_eq!((chunks[1].offset, chunks[1].limit), (10, 10));
        assert_eq!(
            (chunks[2].offset, chunks[2].limit),
            (20, 5),
            "final limit is trimmed to the remainder"
        );
        assert_eq!(chunks.iter().map(|c| c.limit).sum::<u64>(), 25);
    }

    #[test]
    fn an_exact_multiple_produces_full_chunks() {
        let chunks = plan_offset_chunks(30, 10).unwrap();
        assert_eq!(chunks.len(), 3);
        assert!(chunks.iter().all(|c| c.limit == 10));
    }

    #[test]
    fn a_zero_total_plans_nothing() {
        assert!(plan_offset_chunks(0, 10).unwrap().is_empty());
    }

    #[test]
    fn offset_rejects_zero_chunk_size_and_explosions() {
        assert!(plan_offset_chunks(10, 0).is_err());
        assert!(plan_offset_chunks(10_000_000, 1).is_err());
    }
}

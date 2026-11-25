//! One boundary for dates, times and zones (#1093).
//!
//! Every instant, clock, civil value and zone our own code handles is meant to
//! go through the types here, so the library underneath can change in one
//! place. Today they wrap `chrono` (and `chrono-tz` behind the `time-zones`
//! feature) and call exactly the chrono functions call sites call directly, so
//! output is identical by construction:
//!
//! - [`Timestamp`] serializes `#[serde(transparent)]` over
//!   `chrono::DateTime<Utc>`, so a stored value keeps its spelling.
//! - [`Timestamp::to_rfc3339`] with [`Precision::Auto`] prints 0, 3, 6 or 9
//!   fraction digits, as chrono's `SecondsFormat::AutoSi` does.
//!
//! Range constructors return `Option` (like chrono); parsers return
//! [`TimeError`], which converts into [`FaucetError`](crate::FaucetError).
//!
//! Two uses stay on chrono on purpose: the user-facing `${now.strftime.<fmt>}`
//! grammar is chrono's ([`OffsetClock::format_chrono_strftime`]), and cron
//! ticks come from a library that takes chrono types. The `from_chrono` /
//! `to_chrono` conversions are the seam for code that receives a chrono value
//! from a driver; they are hidden from the docs and not meant for new code.

use std::fmt;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};

/// A text value that did not parse, or a value outside the supported range.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{what}: {reason}")]
pub struct TimeError {
    what: &'static str,
    reason: String,
}

impl TimeError {
    fn new(what: &'static str, reason: impl fmt::Display) -> Self {
        Self {
            what,
            reason: reason.to_string(),
        }
    }
}

impl From<TimeError> for crate::FaucetError {
    fn from(e: TimeError) -> Self {
        crate::FaucetError::Custom(Box::new(e))
    }
}

/// How many fraction digits [`Timestamp::to_rfc3339`] prints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Precision {
    /// 0, 3, 6 or 9 digits: the fewest of those that keep the value exact.
    Auto,
    /// No fraction.
    Secs,
    /// Exactly 3 digits.
    Millis,
    /// Exactly 6 digits.
    Micros,
    /// Exactly 9 digits.
    Nanos,
}

impl Precision {
    fn chrono(self) -> chrono::SecondsFormat {
        match self {
            Precision::Auto => chrono::SecondsFormat::AutoSi,
            Precision::Secs => chrono::SecondsFormat::Secs,
            Precision::Millis => chrono::SecondsFormat::Millis,
            Precision::Micros => chrono::SecondsFormat::Micros,
            Precision::Nanos => chrono::SecondsFormat::Nanos,
        }
    }
}

/// A UTC instant with nanosecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Timestamp {
    /// 1970-01-01T00:00:00Z.
    pub const UNIX_EPOCH: Timestamp = Timestamp(DateTime::<Utc>::UNIX_EPOCH);

    /// The current wall-clock time.
    pub fn now() -> Self {
        Self(Utc::now())
    }

    /// Whole seconds since the Unix epoch; `None` outside the supported range.
    pub fn from_unix_secs(secs: i64) -> Option<Self> {
        DateTime::from_timestamp(secs, 0).map(Self)
    }

    /// Seconds plus nanoseconds (`nanos < 1_000_000_000`) since the epoch.
    pub fn from_unix_parts(secs: i64, nanos: u32) -> Option<Self> {
        DateTime::from_timestamp(secs, nanos).map(Self)
    }

    /// Milliseconds since the epoch; `None` outside the supported range.
    pub fn from_unix_millis(millis: i64) -> Option<Self> {
        DateTime::from_timestamp_millis(millis).map(Self)
    }

    /// Microseconds since the epoch; `None` outside the supported range.
    pub fn from_unix_micros(micros: i64) -> Option<Self> {
        DateTime::from_timestamp_micros(micros).map(Self)
    }

    /// Nanoseconds since the epoch. Every `i64` is in range.
    pub fn from_unix_nanos(nanos: i64) -> Self {
        Self(DateTime::from_timestamp_nanos(nanos))
    }

    /// Whole seconds since the epoch (floor).
    pub fn unix_secs(&self) -> i64 {
        self.0.timestamp()
    }

    /// Milliseconds since the epoch (floor).
    pub fn unix_millis(&self) -> i64 {
        self.0.timestamp_millis()
    }

    /// Microseconds since the epoch (floor).
    pub fn unix_micros(&self) -> i64 {
        self.0.timestamp_micros()
    }

    /// Nanoseconds since the epoch; `None` when it does not fit an `i64`
    /// (before 1677 or after 2262).
    pub fn unix_nanos(&self) -> Option<i64> {
        self.0.timestamp_nanos_opt()
    }

    /// The nanoseconds past the whole second.
    pub fn subsec_nanos(&self) -> u32 {
        chrono::Timelike::nanosecond(&self.0)
    }

    /// Parse RFC 3339 text with any offset; the result is the UTC instant.
    pub fn parse_rfc3339(text: &str) -> Result<Self, TimeError> {
        DateTime::parse_from_rfc3339(text)
            .map(|d| Self(d.to_utc()))
            .map_err(|e| TimeError::new("RFC 3339 timestamp", e))
    }

    /// Parse RFC 2822 text (HTTP and mail dates); the result is the UTC instant.
    pub fn parse_rfc2822(text: &str) -> Result<Self, TimeError> {
        DateTime::parse_from_rfc2822(text)
            .map(|d| Self(d.to_utc()))
            .map_err(|e| TimeError::new("RFC 2822 date", e))
    }

    /// RFC 3339 with a `Z` suffix.
    pub fn to_rfc3339(&self, precision: Precision) -> String {
        self.0.to_rfc3339_opts(precision.chrono(), true)
    }

    /// RFC 3339 with a `+00:00` suffix (chrono's `to_rfc3339` spelling).
    pub fn to_rfc3339_offset(&self, precision: Precision) -> String {
        self.0.to_rfc3339_opts(precision.chrono(), false)
    }

    /// `self + span`; `None` outside the supported range.
    pub fn checked_add(&self, span: Span) -> Option<Self> {
        self.0.checked_add_signed(span.0).map(Self)
    }

    /// `self - span`; `None` outside the supported range.
    pub fn checked_sub(&self, span: Span) -> Option<Self> {
        self.0.checked_sub_signed(span.0).map(Self)
    }

    /// `self - earlier` (negative when `earlier` is later).
    pub fn since(&self, earlier: Timestamp) -> Span {
        Span(self.0.signed_duration_since(earlier.0))
    }

    /// This instant on a clock `offset_secs` east of UTC; `None` when the
    /// offset is not within ±24 h.
    pub fn with_offset(&self, offset_secs: i32) -> Option<OffsetClock> {
        FixedOffset::east_opt(offset_secs).map(|o| OffsetClock(self.0.with_timezone(&o)))
    }

    /// The civil date and time in UTC.
    pub fn civil(&self) -> CivilDateTime {
        CivilDateTime(self.0.naive_utc())
    }

    #[doc(hidden)]
    pub fn from_chrono(value: DateTime<Utc>) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> DateTime<Utc> {
        self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::str::FromStr for Timestamp {
    type Err = TimeError;

    /// Same as [`Timestamp::parse_rfc3339`].
    fn from_str(text: &str) -> Result<Self, TimeError> {
        Self::parse_rfc3339(text)
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for DateTime<Utc> {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

/// A signed duration with nanosecond precision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Span(TimeDelta);

impl Span {
    /// Zero length.
    pub const ZERO: Span = Span(TimeDelta::zero());

    /// Whole seconds; `None` outside the supported range.
    pub fn from_secs(secs: i64) -> Option<Self> {
        TimeDelta::try_seconds(secs).map(Self)
    }

    /// Milliseconds; `None` outside the supported range.
    pub fn from_millis(millis: i64) -> Option<Self> {
        TimeDelta::try_milliseconds(millis).map(Self)
    }

    /// Microseconds. Every `i64` is in range.
    pub fn from_micros(micros: i64) -> Self {
        Self(TimeDelta::microseconds(micros))
    }

    /// Nanoseconds. Every `i64` is in range.
    pub fn from_nanos(nanos: i64) -> Self {
        Self(TimeDelta::nanoseconds(nanos))
    }

    /// Whole minutes; `None` outside the supported range.
    pub fn from_minutes(minutes: i64) -> Option<Self> {
        TimeDelta::try_minutes(minutes).map(Self)
    }

    /// Whole hours; `None` outside the supported range.
    pub fn from_hours(hours: i64) -> Option<Self> {
        TimeDelta::try_hours(hours).map(Self)
    }

    /// Whole days of 24 hours; `None` outside the supported range.
    pub fn from_days(days: i64) -> Option<Self> {
        TimeDelta::try_days(days).map(Self)
    }

    /// From a `std` duration; `None` when it is too long.
    pub fn from_std(d: std::time::Duration) -> Option<Self> {
        TimeDelta::from_std(d).ok().map(Self)
    }

    /// To a `std` duration; `None` when negative.
    pub fn to_std(&self) -> Option<std::time::Duration> {
        self.0.to_std().ok()
    }

    /// Whole seconds, truncated toward zero.
    pub fn as_secs(&self) -> i64 {
        self.0.num_seconds()
    }

    /// Whole milliseconds, truncated toward zero.
    pub fn as_millis(&self) -> i64 {
        self.0.num_milliseconds()
    }

    /// Whole microseconds; `None` on overflow.
    pub fn as_micros(&self) -> Option<i64> {
        self.0.num_microseconds()
    }

    /// Nanoseconds; `None` on overflow.
    pub fn as_nanos(&self) -> Option<i64> {
        self.0.num_nanoseconds()
    }

    /// Whether the span is below zero.
    pub fn is_negative(&self) -> bool {
        self.0 < TimeDelta::zero()
    }

    /// `self + other`; `None` on overflow.
    pub fn checked_add(&self, other: Span) -> Option<Self> {
        self.0.checked_add(&other.0).map(Self)
    }

    /// `self - other`; `None` on overflow.
    pub fn checked_sub(&self, other: Span) -> Option<Self> {
        self.0.checked_sub(&other.0).map(Self)
    }

    #[doc(hidden)]
    pub fn from_chrono(value: TimeDelta) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> TimeDelta {
        self.0
    }
}

/// An instant read on a clock with a fixed UTC offset (the `${now.*}` clock).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OffsetClock(DateTime<FixedOffset>);

impl OffsetClock {
    /// Parse RFC 3339 text, keeping its offset.
    pub fn parse_rfc3339(text: &str) -> Result<Self, TimeError> {
        DateTime::parse_from_rfc3339(text)
            .map(Self)
            .map_err(|e| TimeError::new("RFC 3339 timestamp", e))
    }

    /// The instant.
    pub fn timestamp(&self) -> Timestamp {
        Timestamp(self.0.to_utc())
    }

    /// Seconds east of UTC.
    pub fn offset_secs(&self) -> i32 {
        self.0.offset().local_minus_utc()
    }

    /// The civil date and time on this clock.
    pub fn civil(&self) -> CivilDateTime {
        CivilDateTime(self.0.naive_local())
    }

    /// RFC 3339 with the clock's offset (`+00:00`, never `Z`).
    pub fn to_rfc3339(&self, precision: Precision) -> String {
        self.0.to_rfc3339_opts(precision.chrono(), false)
    }

    /// Render a chrono strftime format. The grammar is chrono's on purpose
    /// (`${now.strftime.<fmt>}` documents it); a specifier chrono refuses, or
    /// one that parses but cannot format, is an error rather than a panic.
    pub fn format_chrono_strftime(&self, fmt: &str) -> Result<String, TimeError> {
        use chrono::format::{Item, StrftimeItems};
        let items: Vec<Item> = StrftimeItems::new(fmt).collect();
        if items.iter().any(|i| matches!(i, Item::Error)) {
            return Err(TimeError::new("strftime format", "unknown specifier"));
        }
        let mut out = String::new();
        fmt::Write::write_fmt(
            &mut out,
            format_args!("{}", self.0.format_with_items(items.iter())),
        )
        .map_err(|_| TimeError::new("strftime format", "a specifier cannot be rendered"))?;
        Ok(out)
    }

    #[doc(hidden)]
    pub fn from_chrono(value: DateTime<FixedOffset>) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> DateTime<FixedOffset> {
        self.0
    }
}

/// A calendar date without a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CivilDate(NaiveDate);

impl CivilDate {
    /// `None` for a date that does not exist.
    pub fn new(year: i32, month: u32, day: u32) -> Option<Self> {
        NaiveDate::from_ymd_opt(year, month, day).map(Self)
    }

    /// Parse `YYYY-MM-DD`.
    pub fn parse(text: &str) -> Result<Self, TimeError> {
        NaiveDate::parse_from_str(text, "%Y-%m-%d")
            .map(Self)
            .map_err(|e| TimeError::new("date", e))
    }

    /// The year.
    pub fn year(&self) -> i32 {
        chrono::Datelike::year(&self.0)
    }

    /// The month, 1 to 12.
    pub fn month(&self) -> u32 {
        chrono::Datelike::month(&self.0)
    }

    /// The day of the month, 1 to 31.
    pub fn day(&self) -> u32 {
        chrono::Datelike::day(&self.0)
    }

    /// The next day; `None` at the end of the supported range.
    pub fn succ(&self) -> Option<Self> {
        self.0.succ_opt().map(Self)
    }

    /// This date at `time`.
    pub fn at(&self, time: CivilTime) -> CivilDateTime {
        CivilDateTime(self.0.and_time(time.0))
    }

    #[doc(hidden)]
    pub fn from_chrono(value: NaiveDate) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> NaiveDate {
        self.0
    }
}

impl fmt::Display for CivilDate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A time of day without a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CivilTime(NaiveTime);

impl CivilTime {
    /// Midnight.
    pub const MIDNIGHT: CivilTime = CivilTime(NaiveTime::MIN);

    /// `None` for a time that does not exist (a leap second is `sec = 59`
    /// with `nanos >= 1_000_000_000`, as in chrono).
    pub fn new(hour: u32, min: u32, sec: u32, nanos: u32) -> Option<Self> {
        NaiveTime::from_hms_nano_opt(hour, min, sec, nanos).map(Self)
    }

    #[doc(hidden)]
    pub fn from_chrono(value: NaiveTime) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> NaiveTime {
        self.0
    }
}

impl fmt::Display for CivilTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// A calendar date and time of day without a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CivilDateTime(NaiveDateTime);

impl CivilDateTime {
    /// The date part.
    pub fn date(&self) -> CivilDate {
        CivilDate(self.0.date())
    }

    /// The time part.
    pub fn time(&self) -> CivilTime {
        CivilTime(self.0.time())
    }

    /// Read this civil value as UTC.
    pub fn in_utc(&self) -> Timestamp {
        Timestamp(self.0.and_utc())
    }

    #[doc(hidden)]
    pub fn from_chrono(value: NaiveDateTime) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> NaiveDateTime {
        self.0
    }
}

impl fmt::Display for CivilDateTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(feature = "time-zones")]
mod zone;
#[cfg(feature = "time-zones")]
pub use zone::{LocalTime, Zone};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod differential;

#[cfg(all(test, feature = "time-zones"))]
mod news_oracle;

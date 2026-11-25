use std::collections::HashMap;
use std::fmt;
use std::sync::OnceLock;

use chrono::{FixedOffset, MappedLocalTime, NaiveDate, NaiveDateTime, TimeZone};

use super::{CivilDateTime, OffsetClock, TimeError, Timestamp};

/// A named IANA time zone from the database compiled into the binary (no file
/// is read at run time, so the host's tzdata does not matter). The database is
/// [`TZDATA_VERSION`](super::TZDATA_VERSION).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Zone(u16);

/// How a civil date and time maps onto a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalTime {
    /// Exactly one instant.
    Single(OffsetClock),
    /// A repeated hour (clocks went back): both instants, earlier first.
    Fold {
        /// The first occurrence.
        earlier: OffsetClock,
        /// The second occurrence.
        later: OffsetClock,
    },
    /// Skipped (clocks went forward): no instant has this civil time.
    Gap,
}

struct Table {
    names: Vec<&'static str>,
    index: HashMap<&'static str, u16>,
    zones: Vec<OnceLock<jiff::tz::TimeZone>>,
}

fn table() -> &'static Table {
    static TABLE: OnceLock<Table> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut names: Vec<&'static str> = jiff_tzdb::available().filter(|n| *n != "UTC").collect();
        names.sort_unstable();
        names.insert(0, "UTC");
        let index = names
            .iter()
            .enumerate()
            .map(|(i, n)| (*n, u16::try_from(i).expect("fewer than 65536 zones")))
            .collect();
        let zones = names.iter().map(|_| OnceLock::new()).collect();
        Table {
            names,
            index,
            zones,
        }
    })
}

fn civil_of(local: &NaiveDateTime) -> Option<jiff::civil::DateTime> {
    use chrono::{Datelike, Timelike};
    jiff::civil::DateTime::new(
        i16::try_from(local.year()).ok()?,
        i8::try_from(local.month()).ok()?,
        i8::try_from(local.day()).ok()?,
        i8::try_from(local.hour()).ok()?,
        i8::try_from(local.minute()).ok()?,
        i8::try_from(local.second().min(59)).ok()?,
        i32::try_from(local.nanosecond().min(999_999_999)).ok()?,
    )
    .ok()
}

fn fixed(seconds: i32) -> FixedOffset {
    FixedOffset::east_opt(seconds).expect("tzdata offsets are under a day")
}

impl Zone {
    /// UTC.
    pub const UTC: Zone = Zone(0);

    /// Parse an IANA name. Matching is exact (case-sensitive).
    pub fn parse(name: &str) -> Result<Self, TimeError> {
        table()
            .index
            .get(name)
            .map(|i| Zone(*i))
            .ok_or_else(|| TimeError::new("time zone", format!("unknown zone '{name}'")))
    }

    /// Every zone name the database knows, `UTC` first, then sorted.
    pub fn names() -> impl Iterator<Item = &'static str> {
        table().names.iter().copied()
    }

    /// The IANA name.
    pub fn name(&self) -> &'static str {
        table().names[usize::from(self.0)]
    }

    fn tz(&self) -> &'static jiff::tz::TimeZone {
        table().zones[usize::from(self.0)].get_or_init(|| {
            jiff::tz::TimeZoneDatabase::bundled()
                .get(self.name())
                .expect("every listed zone is in the bundled database")
        })
    }

    /// Whether the zone has never changed its UTC offset.
    pub fn is_fixed(&self) -> bool {
        let tz = self.tz();
        tz.following(jiff::Timestamp::MIN).next().is_none()
            && tz.preceding(jiff::Timestamp::MAX).next().is_none()
    }

    fn offset_secs_at(&self, unix_secs: i64) -> i32 {
        let secs = unix_secs.clamp(
            jiff::Timestamp::MIN.as_second(),
            jiff::Timestamp::MAX.as_second(),
        );
        let at = jiff::Timestamp::from_second(secs).expect("clamped to jiff's range");
        self.tz().to_offset(at).seconds()
    }

    /// The clock reading of `instant` in this zone.
    pub fn to_local(&self, instant: Timestamp) -> OffsetClock {
        let offset = fixed(self.offset_secs_at(instant.unix_secs()));
        OffsetClock(instant.0.with_timezone(&offset))
    }

    /// Resolve a civil date and time in this zone.
    pub fn resolve_local(&self, civil: CivilDateTime) -> LocalTime {
        let clock = |o: FixedOffset| {
            OffsetClock(
                o.from_local_datetime(&civil.0)
                    .single()
                    .expect("a fixed offset maps every civil time"),
            )
        };
        match self.local_offsets(&civil.0).as_slice() {
            [] => LocalTime::Gap,
            [one] => LocalTime::Single(clock(*one)),
            [earlier, later, ..] => LocalTime::Fold {
                earlier: clock(*earlier),
                later: clock(*later),
            },
        }
    }

    /// Offsets in force at `local`, earlier instant first. A candidate is kept
    /// only if it is the offset in force at the instant it names, so a fold or
    /// offset that never applied is never reported.
    fn local_offsets(&self, local: &NaiveDateTime) -> Vec<FixedOffset> {
        let Some(dt) = civil_of(local) else {
            return Vec::new();
        };
        let tz = self.tz();
        let candidates = match tz.to_ambiguous_timestamp(dt).offset() {
            jiff::tz::AmbiguousOffset::Unambiguous { offset } => [Some(offset), None],
            jiff::tz::AmbiguousOffset::Fold { before, after }
            | jiff::tz::AmbiguousOffset::Gap { before, after } => [Some(before), Some(after)],
        };
        let mut kept: Vec<(jiff::Timestamp, i32)> = Vec::new();
        for o in candidates.into_iter().flatten() {
            let Ok(at) = o.to_timestamp(dt) else {
                continue;
            };
            if tz.to_offset(at) == o && kept.iter().all(|(_, s)| *s != o.seconds()) {
                kept.push((at, o.seconds()));
            }
        }
        kept.sort_by_key(|(at, _)| *at);
        kept.into_iter().map(|(_, s)| fixed(s)).collect()
    }

    /// This zone as a chrono [`TimeZone`], for libraries that take chrono types.
    #[doc(hidden)]
    pub fn chrono(self) -> ChronoZone {
        ChronoZone(self)
    }
}

impl fmt::Debug for Zone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Zone").field(&self.name()).finish()
    }
}

impl fmt::Display for Zone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Zone {
    type Err = TimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Zone::parse(s)
    }
}

/// A [`Zone`] seen as a chrono [`TimeZone`], for code that hands a zone to a
/// library taking chrono types (cron, window planning).
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChronoZone(Zone);

/// The offset type of [`ChronoZone`]: the zone and the offset in force.
#[doc(hidden)]
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ChronoZoneOffset {
    zone: Zone,
    fixed: FixedOffset,
}

impl ChronoZone {
    /// UTC.
    pub const UTC: ChronoZone = ChronoZone(Zone::UTC);

    /// The zone.
    pub fn zone(&self) -> Zone {
        self.0
    }

    /// The IANA name.
    pub fn name(&self) -> &'static str {
        self.0.name()
    }

    fn offset(&self, fixed: FixedOffset) -> ChronoZoneOffset {
        ChronoZoneOffset {
            zone: self.0,
            fixed,
        }
    }
}

impl fmt::Debug for ChronoZone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl fmt::Display for ChronoZone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for ChronoZone {
    type Err = TimeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Zone::parse(s).map(ChronoZone)
    }
}

impl fmt::Debug for ChronoZoneOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.fixed, f)
    }
}

impl fmt::Display for ChronoZoneOffset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.fixed, f)
    }
}

impl chrono::Offset for ChronoZoneOffset {
    fn fix(&self) -> FixedOffset {
        self.fixed
    }
}

impl TimeZone for ChronoZone {
    type Offset = ChronoZoneOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        ChronoZone(offset.zone)
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> MappedLocalTime<Self::Offset> {
        self.offset_from_local_datetime(&local.and_time(chrono::NaiveTime::MIN))
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> MappedLocalTime<Self::Offset> {
        match self.0.local_offsets(local).as_slice() {
            [] => MappedLocalTime::None,
            [one] => MappedLocalTime::Single(self.offset(*one)),
            [earlier, later, ..] => {
                MappedLocalTime::Ambiguous(self.offset(*earlier), self.offset(*later))
            }
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        self.offset_from_utc_datetime(&utc.and_time(chrono::NaiveTime::MIN))
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        self.offset(fixed(self.0.offset_secs_at(utc.and_utc().timestamp())))
    }
}

#[cfg(test)]
#[path = "zone_tests.rs"]
mod tests;

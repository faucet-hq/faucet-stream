use chrono::{LocalResult, Offset, TimeZone};

use super::{CivilDateTime, OffsetClock, TimeError, Timestamp};

/// A named IANA time zone from the database compiled into the binary (no file
/// is read at run time, so the host's tzdata does not matter).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Zone(chrono_tz::Tz);

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

impl Zone {
    /// UTC.
    pub const UTC: Zone = Zone(chrono_tz::UTC);

    /// Parse an IANA name. Matching is exact (case-sensitive), as before.
    pub fn parse(name: &str) -> Result<Self, TimeError> {
        name.parse::<chrono_tz::Tz>()
            .map(Self)
            .map_err(|_| TimeError::new("time zone", format!("unknown zone '{name}'")))
    }

    /// The IANA name.
    pub fn name(&self) -> &'static str {
        self.0.name()
    }

    /// The clock reading of `instant` in this zone.
    pub fn to_local(&self, instant: Timestamp) -> OffsetClock {
        let local = instant.0.with_timezone(&self.0);
        let offset = local.offset().fix();
        OffsetClock(instant.0.with_timezone(&offset))
    }

    /// Resolve a civil date and time in this zone.
    pub fn resolve_local(&self, civil: CivilDateTime) -> LocalTime {
        let fixed = |d: chrono::DateTime<chrono_tz::Tz>| OffsetClock(d.fixed_offset());
        match self.0.from_local_datetime(&civil.0) {
            LocalResult::Single(d) => LocalTime::Single(fixed(d)),
            LocalResult::Ambiguous(a, b) => {
                let (earlier, later) = if a <= b { (a, b) } else { (b, a) };
                LocalTime::Fold {
                    earlier: fixed(earlier),
                    later: fixed(later),
                }
            }
            LocalResult::None => LocalTime::Gap,
        }
    }

    #[doc(hidden)]
    pub fn from_chrono(value: chrono_tz::Tz) -> Self {
        Self(value)
    }

    #[doc(hidden)]
    pub fn to_chrono(self) -> chrono_tz::Tz {
        self.0
    }
}

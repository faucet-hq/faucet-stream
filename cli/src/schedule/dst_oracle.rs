//! A brute-force reference for the daylight-saving rule of
//! `CompiledSchedule::try_next_after` (#1111). It knows nothing of the cron
//! library: it walks every UTC minute, reads the local wall time, and applies
//! the rule as documented. Five-field crons with numbers, `*`, `*/n`, ranges
//! and lists only.

use chrono::{DateTime, Datelike, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc};
use chrono_tz::Tz;

/// Zones whose rules differ between time-zone databases (#1116); kept out of
/// the golden rows and the sweep so a backend change does not trip them.
pub const TZDATA_UNSTABLE: &[&str] = &[
    "America/Vancouver",
    "America/Edmonton",
    "America/Winnipeg",
    "America/Inuvik",
    "America/Yellowknife",
    "America/Rainy_River",
    "Africa/Casablanca",
    "Africa/El_Aaiun",
    "America/Havana",
    "Cuba",
    "Europe/Chisinau",
    "Europe/Tiraspol",
    "America/Bogota",
    "Asia/Tehran",
    "Iran",
    "America/Tijuana",
    "America/Ensenada",
    "America/Santa_Isabel",
    "Mexico/BajaNorte",
    "America/Nuuk",
    "America/Godthab",
];

/// Whether a zone is left out of the golden rows and the sweep.
pub fn excluded(tz: &str) -> bool {
    tz.starts_with("Canada/") || TZDATA_UNSTABLE.contains(&tz)
}

fn field(text: &str, min: u32, max: u32) -> Vec<u32> {
    let mut out = Vec::new();
    for part in text.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((r, s)) => (r, s.parse::<u32>().expect("step")),
            None => (part, 1),
        };
        let (lo, hi) = if range == "*" {
            (min, max)
        } else if let Some((a, b)) = range.split_once('-') {
            (a.parse().expect("number"), b.parse().expect("number"))
        } else {
            let v: u32 = range.parse().expect("number");
            (v, if step > 1 { max } else { v })
        };
        out.extend((lo..=hi).step_by(step as usize));
    }
    out.sort_unstable();
    out.dedup();
    out
}

/// A parsed five-field cron.
pub struct Pattern {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days: Option<Vec<u32>>,
    months: Vec<u32>,
    weekdays: Option<Vec<u32>>,
    fixed: bool,
    once_in_repeat: bool,
}

impl Pattern {
    pub fn parse(cron: &str) -> Self {
        let f: Vec<&str> = cron.split_whitespace().collect();
        assert_eq!(f.len(), 5, "oracle reads five-field crons: {cron}");
        let minutes = field(f[0], 0, 59);
        let hours = field(f[1], 0, 23);
        let days = (f[2] != "*").then(|| field(f[2], 1, 31));
        let weekdays =
            (f[4] != "*").then(|| field(f[4], 0, 7).into_iter().map(|d| d % 7).collect());
        let fixed = minutes.len() == 1 && hours.len() == 1;
        let plain = |s: &str| !s.contains(['*', '/']);
        Self {
            fixed,
            once_in_repeat: fixed || (plain(f[0]) && plain(f[1])),
            minutes,
            hours,
            days,
            months: field(f[3], 1, 12),
            weekdays,
        }
    }

    fn day_ok(&self, d: NaiveDate) -> bool {
        let dom = self.days.as_ref().map(|s| s.contains(&d.day()));
        let dow = self
            .weekdays
            .as_ref()
            .map(|s| s.contains(&d.weekday().num_days_from_sunday()));
        let day = match (dom, dow) {
            (Some(a), Some(b)) => a || b,
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => true,
        };
        day && self.months.contains(&d.month())
    }

    fn matches(&self, w: NaiveDateTime) -> bool {
        self.minutes.contains(&w.minute())
            && self.hours.contains(&w.hour())
            && self.day_ok(w.date())
    }
}

/// One UTC minute of a window with its local wall time.
pub struct Minute {
    utc: DateTime<Utc>,
    wall: NaiveDateTime,
    later_pass: bool,
}

/// Every UTC minute in `[from, to)` read in `tz`. Shared by every pattern
/// checked over the window.
pub fn minutes(tz: Tz, from: DateTime<Utc>, to: DateTime<Utc>) -> Vec<Minute> {
    let mut out = Vec::new();
    let mut t = from;
    while t < to {
        let wall = t.with_timezone(&tz).naive_local();
        let later_pass = matches!(tz.from_local_datetime(&wall), LocalResult::Ambiguous(_, l) if l.with_timezone(&Utc) == t);
        out.push(Minute {
            utc: t,
            wall,
            later_pass,
        });
        t += chrono::Duration::minutes(1);
    }
    out
}

/// The ticks of `p` in the window `mins` covers, by the documented rule.
pub fn ticks(p: &Pattern, tz: Tz, mins: &[Minute]) -> Vec<DateTime<Utc>> {
    let mut out: Vec<DateTime<Utc>> = mins
        .iter()
        .filter(|m| p.matches(m.wall) && !(p.once_in_repeat && m.later_pass))
        .map(|m| m.utc)
        .collect();
    if p.fixed && !mins.is_empty() {
        let first = mins[0].wall.date() - chrono::Duration::days(1);
        let last = mins[mins.len() - 1].wall.date() + chrono::Duration::days(1);
        let mut d = first;
        while d <= last {
            let intended = d
                .and_hms_opt(p.hours[0], p.minutes[0], 0)
                .expect("valid time of day");
            if matches!(tz.from_local_datetime(&intended), LocalResult::None)
                && p.day_ok(d)
                && let Some(edge) = mins.iter().find(|m| m.wall > intended)
                && p.day_ok(edge.wall.date())
                && mins.first().is_some_and(|m| m.wall < intended)
            {
                out.push(edge.utc);
            }
            d += chrono::Duration::days(1);
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

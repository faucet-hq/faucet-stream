//! In-run datetime window slicing for forward incremental (#527).
//!
//! [`ReplicationBind`](crate::ReplicationBind) (#513) binds only a single *lower*
//! bound (`?since=<bookmark>`); [`faucet backfill`](https://…) (#282) windows only
//! a *bounded historical* range. Neither bounds each request of the ordinary
//! forward-incremental run.
//!
//! Many APIs — analytics / ads / reporting feeds especially — require **both** a
//! lower and an upper bound and **cap the span** (e.g. reject a range over 30 or
//! 90 days). Against those, an unbounded `?since=<bookmark>` either errors or,
//! worse, silently truncates. Window slicing bounds each request to a rolling
//! `[start, end)` interval between the persisted bookmark and `now`, iterating the
//! windows within a single run and persisting the window boundary as the bookmark
//! at each step so the run is resumable mid-sweep. This is parity with Airbyte's
//! `DatetimeBasedCursor` (`start_datetime` / `end_datetime` / `step` /
//! `cursor_granularity` / `lookback_window`).
//!
//! The enumeration is a pure function ([`enumerate_windows`]); a source injects
//! the rendered boundaries into its requests via the [`WindowBind`]s (the window
//! analogue of [`ReplicationBind`](crate::ReplicationBind)).

use crate::FaucetError;
use crate::replication::{BindFormat, BindTarget, format_instant};
use chrono::{DateTime, Duration, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The placeholder replaced by the formatted window boundary inside a
/// [`WindowBind::template`]. The `lower` bind renders the window **start**, the
/// `upper` bind renders the window **end**.
pub const WINDOW_PLACEHOLDER: &str = "${window}";

fn default_window_template() -> String {
    WINDOW_PLACEHOLDER.to_owned()
}

/// Default [`WindowSpec::max_windows`]: a runaway backstop, far above any real
/// sweep. On a first run against years of history at a small `step`, the sweep is
/// truncated here and the next run resumes from the last window.
pub const DEFAULT_MAX_WINDOWS: usize = 10_000;

fn default_max_windows() -> usize {
    DEFAULT_MAX_WINDOWS
}

/// One half-open `[start, end)` slice of the replication timeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    /// Inclusive lower bound.
    pub start: DateTime<Utc>,
    /// Exclusive upper bound.
    pub end: DateTime<Utc>,
}

/// Injects a rendered window boundary into the outgoing request — the window
/// analogue of [`ReplicationBind`](crate::ReplicationBind). Reuses the same
/// [`BindTarget`] placement and [`BindFormat`] formatting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WindowBind {
    /// Where to place the rendered boundary (query param / header / body field /
    /// path placeholder).
    #[serde(default)]
    pub into: BindTarget,
    /// The parameter / header / body-field / path-placeholder name. Optional
    /// only for `into: body` with a `path`.
    #[serde(default)]
    pub name: String,
    /// `into: body` only: an RFC 6901 JSON Pointer into the configured `body`
    /// (`/dateRanges/0/startDate`) instead of a top-level `name` (#748).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// JSON type written by a body bind: `string` (default) or `number`.
    #[serde(default)]
    pub value_type: crate::replication::BindValueType,
    /// Template rendered with [`WINDOW_PLACEHOLDER`] (`${window}`) replaced by the
    /// formatted boundary. Defaults to the bare `${window}`; set e.g.
    /// `"gte|${window}"` or `"[${window} TO *]"`.
    #[serde(default = "default_window_template")]
    pub template: String,
    /// How to format the boundary before substitution.
    #[serde(default)]
    pub format: BindFormat,
}

impl WindowBind {
    /// Validate the bind at config-load time. `side` names the field for errors
    /// (`"lower"` / `"upper"`).
    pub fn validate(&self, side: &str) -> Result<(), FaucetError> {
        crate::replication::validate_bind_placement(
            &format!("window slicing `{side}`"),
            self.into,
            &self.name,
            self.path.as_deref(),
        )?;
        if !self.template.contains(WINDOW_PLACEHOLDER) {
            return Err(FaucetError::Config(format!(
                "window slicing: `{side}.template` must contain the `{WINDOW_PLACEHOLDER}` placeholder"
            )));
        }
        Ok(())
    }

    /// Render the bind for a concrete boundary instant.
    pub fn render(&self, boundary: DateTime<Utc>) -> String {
        let formatted = format_instant(boundary, self.format);
        self.template.replace(WINDOW_PLACEHOLDER, &formatted)
    }
}

/// Declarative in-run datetime window slicing (#527).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WindowSpec {
    /// Window size — `45s` / `30m` / `6h` / `30d`, or a bare integer (= seconds).
    /// Absolute UTC durations (`d` = 24h); calendar/DST-correct windows are a
    /// [`faucet backfill`] concern, not the incremental cursor.
    pub step: String,
    /// Lower-bound bind, rendered with the window **start**.
    pub lower: WindowBind,
    /// Upper-bound bind, rendered with the window **end**.
    pub upper: WindowBind,
    /// Subtract this from each window's *rendered* upper bound so `[start, end]`
    /// is non-overlapping for inclusive-inclusive APIs (Airbyte
    /// `cursor_granularity`). Same grammar as `step`. The **persisted bookmark is
    /// always the true half-open boundary**, so resume never gaps or overlaps —
    /// only the value sent to the server is adjusted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub granularity: Option<String>,
    /// Re-scan this much *before* the bookmark on the first window, to catch
    /// late-arriving updates without a full replay. Same grammar as `step`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookback: Option<String>,
    /// Safety cap on the number of windows enumerated in one run. On overflow the
    /// sweep is truncated (logged, never silently), and the next run resumes from
    /// the last window's end.
    #[serde(default = "default_max_windows")]
    pub max_windows: usize,
}

/// Parse a [`WindowSpec`] duration string (`step` / `granularity` / `lookback`)
/// into an absolute [`chrono::Duration`]: `45s` / `30m` / `6h` / `30d` (`d` =
/// 24h), or a bare integer (= seconds). Must be positive.
pub fn parse_step(s: &str) -> Result<Duration, FaucetError> {
    let s = s.trim();
    let err = || {
        FaucetError::Config(format!(
            "window slicing: '{s}' is not a valid duration — use e.g. 45s, 30m, 6h, 30d"
        ))
    };
    let (num, unit) = match s.chars().last() {
        Some(c) if c.is_ascii_digit() => (s, "s"),
        Some(c) => (&s[..s.len() - c.len_utf8()], &s[s.len() - c.len_utf8()..]),
        None => return Err(err()),
    };
    let n: i64 = num.parse().map_err(|_| err())?;
    if n <= 0 {
        return Err(FaucetError::Config(format!(
            "window slicing: duration '{s}' must be positive"
        )));
    }
    Ok(match unit {
        "s" => Duration::seconds(n),
        "m" => Duration::minutes(n),
        "h" => Duration::hours(n),
        "d" => Duration::days(n),
        _ => return Err(err()),
    })
}

impl WindowSpec {
    /// Validate the whole spec at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        parse_step(&self.step)?;
        if let Some(g) = &self.granularity {
            parse_step(g)?;
        }
        if let Some(l) = &self.lookback {
            parse_step(l)?;
        }
        self.lower.validate("lower")?;
        self.upper.validate("upper")?;
        if self.max_windows == 0 {
            return Err(FaucetError::Config(
                "window slicing: `max_windows` must be greater than zero".to_owned(),
            ));
        }
        Ok(())
    }

    /// The parsed `step` duration.
    pub fn step_duration(&self) -> Result<Duration, FaucetError> {
        parse_step(&self.step)
    }

    /// The parsed `granularity` duration, if any.
    pub fn granularity_duration(&self) -> Result<Option<Duration>, FaucetError> {
        self.granularity.as_deref().map(parse_step).transpose()
    }

    /// The parsed `lookback` duration, if any.
    pub fn lookback_duration(&self) -> Result<Option<Duration>, FaucetError> {
        self.lookback.as_deref().map(parse_step).transpose()
    }

    /// The rendered lower-bound value for a window (the window **start**).
    pub fn render_lower(&self, w: &Window) -> String {
        self.lower.render(w.start)
    }

    /// The rendered upper-bound value for a window, applying `granularity` (the
    /// window **end**, minus `granularity` if set, for inclusive-inclusive APIs).
    pub fn render_upper(&self, w: &Window) -> Result<String, FaucetError> {
        let end = match self.granularity_duration()? {
            Some(g) => w.end - g,
            None => w.end,
        };
        Ok(self.upper.render(end))
    }
}

/// Enumerate contiguous half-open `[start, end)` windows from `start` (minus
/// `lookback`) up to `now`, each `step` wide (the last clamped to `now`).
///
/// Returns `(windows, truncated)`: an empty vec when `start >= now` (a no-op
/// run); `truncated = true` when the sweep hit `max_windows` before reaching
/// `now` (the caller logs it — the next run resumes from the last window's end,
/// which is the persisted bookmark).
pub fn enumerate_windows(
    start: DateTime<Utc>,
    now: DateTime<Utc>,
    step: Duration,
    lookback: Option<Duration>,
    max_windows: usize,
) -> (Vec<Window>, bool) {
    let mut cur = match lookback {
        Some(lb) => start - lb,
        None => start,
    };
    let mut out = Vec::new();
    let mut truncated = false;
    while cur < now {
        if out.len() >= max_windows {
            truncated = true;
            break;
        }
        let end = std::cmp::min(cur + step, now);
        // `parse_step` guarantees a positive step, so `cur + step > cur`; the
        // clamp to `now` also keeps `end > cur` because the loop guard is
        // `cur < now`. This guard is belt-and-braces against a degenerate clock.
        if end <= cur {
            break;
        }
        out.push(Window { start: cur, end });
        cur = end;
    }
    (out, truncated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    fn dt(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn parse_step_units() {
        assert_eq!(parse_step("45s").unwrap(), Duration::seconds(45));
        assert_eq!(parse_step("30m").unwrap(), Duration::minutes(30));
        assert_eq!(parse_step("6h").unwrap(), Duration::hours(6));
        assert_eq!(parse_step("30d").unwrap(), Duration::days(30));
        assert_eq!(parse_step("3600").unwrap(), Duration::seconds(3600));
    }

    #[test]
    fn parse_step_rejects_bad() {
        assert!(parse_step("0d").is_err());
        assert!(parse_step("-1h").is_err());
        assert!(parse_step("").is_err());
        assert!(parse_step("10y").is_err());
        assert!(parse_step("abc").is_err());
    }

    #[test]
    fn enumerate_contiguous_half_open() {
        let (ws, trunc) = enumerate_windows(
            dt("2024-01-01T00:00:00Z"),
            dt("2024-01-04T00:00:00Z"),
            Duration::days(1),
            None,
            100,
        );
        assert!(!trunc);
        assert_eq!(ws.len(), 3);
        assert_eq!(ws[0].start, dt("2024-01-01T00:00:00Z"));
        assert_eq!(ws[0].end, dt("2024-01-02T00:00:00Z"));
        // Half-open: window N's end equals window N+1's start (no gap, no overlap).
        assert_eq!(ws[0].end, ws[1].start);
        assert_eq!(ws[2].end, dt("2024-01-04T00:00:00Z"));
    }

    #[test]
    fn last_window_clamps_to_now() {
        let (ws, _) = enumerate_windows(
            dt("2024-01-01T00:00:00Z"),
            dt("2024-01-02T06:00:00Z"),
            Duration::days(1),
            None,
            100,
        );
        assert_eq!(ws.len(), 2);
        assert_eq!(ws[1].start, dt("2024-01-02T00:00:00Z"));
        assert_eq!(ws[1].end, dt("2024-01-02T06:00:00Z")); // clamped, not +1 day
    }

    #[test]
    fn empty_when_start_at_or_after_now() {
        let (ws, trunc) = enumerate_windows(
            dt("2024-06-01T00:00:00Z"),
            dt("2024-06-01T00:00:00Z"),
            Duration::days(1),
            None,
            100,
        );
        assert!(ws.is_empty());
        assert!(!trunc);
    }

    #[test]
    fn lookback_extends_the_first_window_backwards() {
        let (ws, _) = enumerate_windows(
            dt("2024-01-02T00:00:00Z"),
            dt("2024-01-03T00:00:00Z"),
            Duration::days(1),
            Some(Duration::hours(6)),
            100,
        );
        // First window now starts 6h before the bookmark.
        assert_eq!(ws[0].start, dt("2024-01-01T18:00:00Z"));
    }

    #[test]
    fn max_windows_truncates_and_flags() {
        let (ws, trunc) = enumerate_windows(
            dt("2024-01-01T00:00:00Z"),
            dt("2024-12-31T00:00:00Z"),
            Duration::days(1),
            None,
            5,
        );
        assert_eq!(ws.len(), 5);
        assert!(trunc);
        // The next run resumes from the last window's end.
        assert_eq!(ws[4].end, dt("2024-01-06T00:00:00Z"));
    }

    #[test]
    fn render_lower_and_upper_with_granularity() {
        let spec = WindowSpec {
            step: "1d".into(),
            lower: WindowBind {
                into: BindTarget::Query,
                name: "start".into(),
                template: "${window}".into(),
                format: BindFormat::Date,
                path: None,
                value_type: Default::default(),
            },
            upper: WindowBind {
                into: BindTarget::Query,
                name: "end".into(),
                template: "${window}".into(),
                format: BindFormat::Date,
                path: None,
                value_type: Default::default(),
            },
            granularity: Some("1d".into()),
            lookback: None,
            max_windows: DEFAULT_MAX_WINDOWS,
        };
        let w = Window {
            start: dt("2024-01-01T00:00:00Z"),
            end: dt("2024-01-02T00:00:00Z"),
        };
        assert_eq!(spec.render_lower(&w), "2024-01-01");
        // Upper is end - granularity (inclusive-inclusive): 2024-01-01, not -02.
        assert_eq!(spec.render_upper(&w).unwrap(), "2024-01-01");
    }

    #[test]
    fn render_template_and_epoch_format() {
        let bind = WindowBind {
            into: BindTarget::Query,
            name: "since".into(),
            template: "gte|${window}".into(),
            format: BindFormat::EpochS,
            path: None,
            value_type: Default::default(),
        };
        let ts = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        assert_eq!(bind.render(ts), "gte|1700000000");
    }

    #[test]
    fn validate_catches_misconfig() {
        let ok = WindowSpec {
            step: "1d".into(),
            lower: WindowBind {
                into: BindTarget::Query,
                name: "start".into(),
                template: "${window}".into(),
                format: BindFormat::Iso8601,
                path: None,
                value_type: Default::default(),
            },
            upper: WindowBind {
                into: BindTarget::Query,
                name: "end".into(),
                template: "${window}".into(),
                format: BindFormat::Iso8601,
                path: None,
                value_type: Default::default(),
            },
            granularity: None,
            lookback: None,
            max_windows: DEFAULT_MAX_WINDOWS,
        };
        ok.validate().unwrap();

        let mut bad_step = ok.clone();
        bad_step.step = "0d".into();
        assert!(bad_step.validate().is_err());

        let mut empty_name = ok.clone();
        empty_name.lower.name = "  ".into();
        assert!(empty_name.validate().is_err());

        let mut no_placeholder = ok.clone();
        no_placeholder.upper.template = "fixed".into();
        assert!(no_placeholder.validate().is_err());

        let mut zero_windows = ok.clone();
        zero_windows.max_windows = 0;
        assert!(zero_windows.validate().is_err());
    }

    #[test]
    fn spec_deserializes_from_yaml_shape() {
        let v = json!({
            "step": "30d",
            "lower": {"into": "query", "name": "start_date", "format": "date"},
            "upper": {"into": "query", "name": "end_date", "format": "date"},
            "lookback": "1d"
        });
        let spec: WindowSpec = serde_json::from_value(v).unwrap();
        assert_eq!(spec.step, "30d");
        assert_eq!(spec.lower.template, WINDOW_PLACEHOLDER); // defaulted
        assert_eq!(spec.max_windows, DEFAULT_MAX_WINDOWS); // defaulted
        spec.validate().unwrap();
    }
}

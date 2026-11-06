//! Validation of a schedule definition (#1044), shared by the `--schedules`
//! file, the API and change requests. Pure except for the clock used to
//! preview ticks.

use crate::schedule::compiled::CompiledSchedule;
use crate::serve::history::schedules::{ScheduleSpec, ScheduleTemplate};
use chrono::{DateTime, Utc};
use std::time::Duration;

/// Longest schedule name.
pub const MAX_NAME_LEN: usize = 64;
/// Occurrences scanned when checking the shortest gap between ticks.
const GAP_SCAN: usize = 64;
/// The `${schedule.*}` tokens a template param may use.
pub const TOKENS: &[&str] = &["tick", "name", "fired_at"];
/// The shortest gap allowed between any schedule's ticks.
pub const MIN_INTERVAL: Duration = crate::serve::config::MIN_SCHEDULE_INTERVAL;

/// Compile a cron + timezone the way `faucet schedule` does, so both
/// runtimes accept exactly the same expressions.
pub fn compile(cron: &str, timezone: &str) -> Result<CompiledSchedule, String> {
    let spec: crate::schedule::spec::ScheduleSpec =
        serde_json::from_value(serde_json::json!({ "cron": cron, "timezone": timezone }))
            .map_err(|e| e.to_string())?;
    CompiledSchedule::compile(&spec).map_err(|e| {
        e.to_string()
            .trim_start_matches("configuration error: ")
            .trim_start_matches("schedule: ")
            .to_string()
    })
}

/// The next `n` ticks strictly after `after`.
pub fn next_ticks(
    schedule: &CompiledSchedule,
    after: DateTime<Utc>,
    n: usize,
) -> Vec<DateTime<Utc>> {
    std::iter::successors(schedule.next_after(after), |t| schedule.next_after(*t))
        .take(n)
        .collect()
}

/// The shortest gap between consecutive ticks over the next occurrences.
pub fn shortest_gap(schedule: &CompiledSchedule, after: DateTime<Utc>) -> Option<Duration> {
    next_ticks(schedule, after, GAP_SCAN)
        .windows(2)
        .filter_map(|w| (w[1] - w[0]).to_std().ok())
        .min()
}

/// Check a name: letters, digits, `_` and `-`, 1–64 characters (it appears in
/// URLs, metrics and idempotency keys).
pub fn check_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return Err(format!(
            "schedule name must be 1–{MAX_NAME_LEN} characters (got {})",
            name.len()
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(format!(
            "invalid schedule name '{name}' (letters, digits, '_' and '-' only)"
        ));
    }
    Ok(())
}

/// Every `${schedule.<token>}` in `s`; `Err` names an unknown token or an
/// unterminated one.
fn schedule_tokens(s: &str) -> Result<Vec<&str>, String> {
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(start) = rest.find("${schedule.") {
        let after = &rest[start + "${schedule.".len()..];
        let Some(end) = after.find('}') else {
            return Err(format!("unterminated `${{schedule.` in '{s}'"));
        };
        let token = &after[..end];
        if !TOKENS.contains(&token) {
            return Err(format!(
                "unknown token `${{schedule.{token}}}` (known: {})",
                TOKENS
                    .iter()
                    .map(|t| format!("${{schedule.{t}}}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        out.push(token);
        rest = &after[end + 1..];
    }
    Ok(out)
}

/// Replace every `${schedule.*}` token in `s`.
pub fn substitute(s: &str, name: &str, tick: &str, fired_at: &str) -> String {
    s.replace("${schedule.tick}", tick)
        .replace("${schedule.name}", name)
        .replace("${schedule.fired_at}", fired_at)
}

fn check_selector(field: &str, v: &Option<String>) -> Result<(), String> {
    if let Some(s) = v {
        serde_json::from_value::<crate::serve::history::templates::VersionSelector>(
            serde_json::Value::String(s.clone()),
        )
        .map_err(|e| format!("template.{field} '{s}': {e}"))?;
    }
    Ok(())
}

fn check_template(t: &ScheduleTemplate) -> Result<(), String> {
    if t.id.trim().is_empty() {
        return Err("template.id is empty".into());
    }
    check_selector("version", &t.version)?;
    check_selector("sink_version", &t.sink_version)?;
    check_selector("overlay_version", &t.overlay_version)?;
    if t.sink_version.is_some() && t.sink.is_none() {
        return Err("template.sink_version needs template.sink".into());
    }
    if t.overlay_version.is_some() && t.overlay.is_none() {
        return Err("template.overlay_version needs template.overlay".into());
    }
    for (k, v) in &t.params {
        if let serde_json::Value::String(s) = v {
            schedule_tokens(s).map_err(|e| format!("template.params.{k}: {e}"))?;
        }
    }
    Ok(())
}

/// The floor in force: [`MIN_INTERVAL`]. Debug builds (the test suite) may
/// lower it with `FAUCET_SCHEDULE_TEST_MIN_INTERVAL_SECS` so integration tests
/// see ticks in seconds; release binaries never read it.
pub fn min_interval() -> Duration {
    #[cfg(debug_assertions)]
    if let Some(secs) = std::env::var(TEST_MIN_INTERVAL_ENV)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        return Duration::from_secs(secs);
    }
    MIN_INTERVAL
}

/// The debug-build override of [`min_interval`] (tests only).
pub const TEST_MIN_INTERVAL_ENV: &str = "FAUCET_SCHEDULE_TEST_MIN_INTERVAL_SECS";

/// Refuse a cron with a seconds field: schedules use five-field crons.
pub fn check_no_seconds(cron: &str) -> Result<(), String> {
    if min_interval() >= Duration::from_secs(60)
        && !cron.trim_start().starts_with('@')
        && cron.split_whitespace().count() > 5
    {
        return Err(format!(
            "`{cron}` has a seconds field; schedules use five-field crons (minute hour day month \
             weekday) and tick at most every {}",
            human(MIN_INTERVAL)
        ));
    }
    Ok(())
}

/// Validate a definition; returns its compiled cron. `Err` is user-facing.
/// No schedule ticks more often than every [`MIN_INTERVAL`].
pub fn validate(spec: &ScheduleSpec) -> Result<CompiledSchedule, String> {
    check_name(&spec.name)?;
    check_no_seconds(&spec.cron)?;
    let compiled = compile(&spec.cron, &spec.timezone)?;
    check_min_interval(spec, &compiled, min_interval(), None, Utc::now())?;
    check_template(&spec.template)?;
    if let Some(sel) = &spec.tenants {
        if !cfg!(feature = "tenants") {
            return Err("`tenants:` needs a build with the `tenants` feature".into());
        }
        sel.validate()?;
    }
    crate::serve::history::labels::check_submitted(&spec.run.labels, false)
        .map_err(|e| format!("run.labels: {e}"))?;
    if spec.run.timeout_secs == Some(0) {
        return Err("run.timeout_secs must be >= 1 (omit it for no timeout)".into());
    }
    if let Some(name) = &spec.run.name {
        schedule_tokens(name).map_err(|e| format!("run.name: {e}"))?;
    }
    if let Some(d) = &spec.description
        && d.len() > 1000
    {
        return Err("description must be at most 1000 characters".into());
    }
    Ok(compiled)
}

/// Refuse a schedule that ticks more often than `min`: every schedule's rule
/// (`who` = `None`), or a tenant's guardrail.
pub fn check_min_interval(
    spec: &ScheduleSpec,
    schedule: &CompiledSchedule,
    min: Duration,
    who: Option<&str>,
    now: DateTime<Utc>,
) -> Result<(), String> {
    match shortest_gap(schedule, now) {
        Some(gap) if gap < min => Err(format!(
            "`{}` ticks every {}; {} at most every {}",
            spec.cron,
            human(gap),
            match who {
                Some(t) => format!("{t}'s schedules may tick"),
                None => "a schedule may tick".to_string(),
            },
            human(min)
        )),
        _ => Ok(()),
    }
}

/// `90 seconds`, `5 minutes`, `2 hours`.
pub fn human(d: Duration) -> String {
    let s = d.as_secs();
    let (n, unit) = if s.is_multiple_of(3600) && s >= 3600 {
        (s / 3600, "hour")
    } else if s.is_multiple_of(60) && s >= 60 {
        (s / 60, "minute")
    } else {
        (s, "second")
    };
    format!("{n} {unit}{}", if n == 1 { "" } else { "s" })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(v: serde_json::Value) -> ScheduleSpec {
        let mut base = json!({"name": "n", "cron": "0 2 * * *", "template": {"id": "t"}});
        crate::merge::merge_value(&mut base, v);
        serde_json::from_value(base).unwrap()
    }

    fn err(v: serde_json::Value) -> String {
        match validate(&spec(v)) {
            Err(e) => e,
            Ok(_) => panic!("expected an error"),
        }
    }

    #[test]
    fn a_valid_schedule_compiles() {
        assert!(validate(&spec(json!({}))).is_ok());
        assert!(
            validate(&spec(
                json!({"template": {"params": {"since": "${schedule.tick}", "n": 3}}})
            ))
            .is_ok()
        );
    }

    #[test]
    fn every_field_is_checked() {
        assert!(err(json!({"name": "a b"})).contains("invalid schedule name"));
        assert!(err(json!({"name": ""})).contains("1–64"));
        assert!(err(json!({"name": "x".repeat(65)})).contains("1–64"));
        assert!(err(json!({"cron": "0 2 1 13 *"})).contains("invalid cron"));
        assert!(err(json!({"timezone": "Mars/Base"})).contains("unknown timezone"));
        assert!(err(json!({"template": {"id": " "}})).contains("template.id is empty"));
        assert!(err(json!({"template": {"version": "latest"}})).contains("template.version"));
        assert!(err(json!({"template": {"sink_version": "1"}})).contains("needs template.sink"));
        assert!(
            err(json!({"template": {"overlay_version": "1"}})).contains("needs template.overlay")
        );
        assert!(
            err(json!({"template": {"params": {"x": "${schedule.nope}"}}}))
                .contains("unknown token `${schedule.nope}`")
        );
        assert!(
            err(json!({"template": {"params": {"x": "${schedule.tick"}}})).contains("unterminated")
        );
        assert!(err(json!({"run": {"labels": {"template": "x"}}})).contains("run.labels"));
        assert!(err(json!({"run": {"timeout_secs": 0}})).contains("timeout_secs"));
        assert!(err(json!({"run": {"name": "${schedule.when}"}})).contains("run.name"));
        assert!(err(json!({"description": "d".repeat(1001)})).contains("1000"));
        assert!(err(json!({"tenants": []})).contains("tenant"));
        assert!(
            err(json!({"cron": "*/2 * * * *"}))
                .contains("ticks every 2 minutes; a schedule may tick at most every 5 minutes")
        );
        assert!(err(json!({"cron": "0 */5 * * * *"})).contains("seconds field"));
        assert!(err(json!({"cron": "* * * * * *"})).contains("seconds field"));
    }

    #[test]
    fn five_minutes_and_aliases_are_allowed() {
        assert!(validate(&spec(json!({"cron": "*/5 * * * *"}))).is_ok());
        assert!(validate(&spec(json!({"cron": "@hourly"}))).is_ok());
        assert!(check_no_seconds("@daily").is_ok());
    }

    #[test]
    fn substitution_replaces_every_token() {
        assert_eq!(
            substitute(
                "${schedule.name}@${schedule.tick} (${schedule.fired_at})",
                "n",
                "T",
                "F"
            ),
            "n@T (F)"
        );
    }

    #[test]
    fn previews_and_gaps() {
        let now: DateTime<Utc> = "2026-11-05T00:00:30Z".parse().unwrap();
        let every_two = compile("*/2 * * * *", "UTC").unwrap();
        let ticks = next_ticks(&every_two, now, 3);
        assert_eq!(ticks.len(), 3);
        assert_eq!(
            ticks[0],
            "2026-11-05T00:02:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(
            shortest_gap(&every_two, now),
            Some(Duration::from_secs(120))
        );
        let s = spec(json!({"cron": "*/2 * * * *"}));
        let e = check_min_interval(&s, &every_two, Duration::from_secs(300), Some("acme"), now)
            .unwrap_err();
        assert!(
            e.contains("ticks every 2 minutes; acme's schedules may tick at most every 5 minutes"),
            "{e}"
        );
        let daily = compile("0 2 * * *", "UTC").unwrap();
        assert!(
            check_min_interval(&s, &daily, Duration::from_secs(300), Some("acme"), now).is_ok()
        );
    }

    #[test]
    fn human_durations() {
        assert_eq!(human(Duration::from_secs(1)), "1 second");
        assert_eq!(human(Duration::from_secs(90)), "90 seconds");
        assert_eq!(human(Duration::from_secs(60)), "1 minute");
        assert_eq!(human(Duration::from_secs(7200)), "2 hours");
    }
}

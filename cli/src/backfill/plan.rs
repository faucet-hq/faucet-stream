//! Backfill-specific planning: `${backfill.*}` token substitution and the stable
//! range hash the progress marker is keyed by.
//!
//! The window planning itself lives in [`crate::chunking`], shared with the
//! `partition:` block (#479) so both have one implementation and one set of
//! boundary tests. It is re-exported here unchanged, so every existing call site
//! and test keeps working against the moved code — which is what demonstrates
//! the move did not alter backfill's behaviour.

use crate::error::{CliError, CliResult};
use serde_json::Value;

pub use crate::chunking::{
    MAX_UNITS, TimeChunk as BackfillUnit, WARN_UNITS, WindowStep, parse_boundary, parse_window,
    plan_windows,
};

/// Substitute `${backfill.*}` tokens in every string leaf of `value`:
/// `start` / `end` (RFC3339), `start_date` / `end_date` (`YYYY-MM-DD`, local),
/// `start_unix` / `end_unix` (epoch seconds), `unit` (the unit id). An
/// unrecognized `${backfill.*}` token is a typo — typed error.
pub fn substitute_unit_tokens(value: &mut Value, unit: &BackfillUnit) -> CliResult<()> {
    match value {
        Value::String(s) => {
            *s = substitute_in_str(s, unit)?;
            Ok(())
        }
        Value::Array(a) => a
            .iter_mut()
            .try_for_each(|v| substitute_unit_tokens(v, unit)),
        Value::Object(m) => m
            .values_mut()
            .try_for_each(|v| substitute_unit_tokens(v, unit)),
        _ => Ok(()),
    }
}

fn substitute_in_str(input: &str, unit: &BackfillUnit) -> CliResult<String> {
    const PREFIX: &str = "${backfill.";
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = crate::interpolate::find_unescaped(rest, PREFIX) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + PREFIX.len()..];
        let close = after.find('}').ok_or_else(|| {
            CliError::Config(format!("unterminated ${{backfill.…}} token in '{input}'"))
        })?;
        let token = &after[..close];
        let rendered = match token {
            "start" => unit.start.to_rfc3339(),
            "end" => unit.end.to_rfc3339(),
            "start_date" => unit.start.format("%Y-%m-%d").to_string(),
            "end_date" => unit.end.format("%Y-%m-%d").to_string(),
            "start_unix" => unit.start.timestamp().to_string(),
            "end_unix" => unit.end.timestamp().to_string(),
            "unit" => unit.id.clone(),
            other => {
                return Err(CliError::Config(format!(
                    "unknown token ${{backfill.{other}}} — supported: start, end, start_date, \
                     end_date, start_unix, end_unix, unit"
                )));
            }
        };
        out.push_str(&rendered);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Deterministic 64-bit FNV-1a hash of the range descriptor, hex-encoded.
/// Keys the progress marker so `--resume` finds the same backfill across
/// process restarts (std's `DefaultHasher` is randomly seeded — unusable).
pub fn range_hash(descriptor: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for b in descriptor.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tz(name: &str) -> chrono_tz::Tz {
        name.parse().unwrap()
    }

    #[test]
    fn tokens_substitute_in_nested_config() {
        let utc = tz("UTC");
        let unit = BackfillUnit {
            id: "20260601T000000Z".into(),
            start: parse_boundary("2026-06-01T00:00:00Z", utc).unwrap(),
            end: parse_boundary("2026-06-02T00:00:00Z", utc).unwrap(),
        };
        let mut cfg = json!({
            "query": "SELECT * FROM t WHERE ts >= '${backfill.start}' AND ts < '${backfill.end}'",
            "nested": { "path": "dt=${backfill.start_date}/part-${backfill.unit}.jsonl" },
            "unix": ["${backfill.start_unix}", "${backfill.end_unix}"],
            "count": 3,
        });
        substitute_unit_tokens(&mut cfg, &unit).unwrap();
        assert_eq!(
            cfg["query"],
            "SELECT * FROM t WHERE ts >= '2026-06-01T00:00:00+00:00' AND ts < '2026-06-02T00:00:00+00:00'"
        );
        assert_eq!(
            cfg["nested"]["path"],
            "dt=2026-06-01/part-20260601T000000Z.jsonl"
        );
        assert_eq!(cfg["unix"][0], "1780272000");
        assert_eq!(cfg["count"], 3);
    }

    #[test]
    fn unknown_or_unterminated_token_is_a_typed_error() {
        let utc = tz("UTC");
        let unit = BackfillUnit {
            id: "u".into(),
            start: parse_boundary("2026-06-01", utc).unwrap(),
            end: parse_boundary("2026-06-02", utc).unwrap(),
        };
        let mut bad = json!({"q": "${backfill.begin}"});
        let err = substitute_unit_tokens(&mut bad, &unit).unwrap_err();
        assert!(err.to_string().contains("backfill.begin"), "{err}");
        let mut unterminated = json!({"q": "${backfill.start"});
        assert!(substitute_unit_tokens(&mut unterminated, &unit).is_err());
    }

    #[test]
    fn range_hash_is_stable_and_distinct() {
        let a = range_hash("2026-06-01|2026-07-01|1d");
        assert_eq!(a, range_hash("2026-06-01|2026-07-01|1d"), "deterministic");
        assert_ne!(a, range_hash("2026-06-01|2026-07-01|6h"));
        assert_eq!(a.len(), 16);
    }
}

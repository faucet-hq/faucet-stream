//! Turn a [`PartitionSpec`] into concrete chunks and
//! substitute their `${partition.*}` tokens into connector configs (#479).
//!
//! Substitution walks every string leaf of a config `Value`, exactly like
//! `backfill::plan::substitute_unit_tokens` — which is why this works for any
//! source without a line of connector code. A REST URL, a SQL `WHERE`, an object
//! prefix and a Mongo filter are all just strings.
//!
//! ## Injection
//!
//! Tokens land inside SQL and JSON strings, where `substitute_context` is
//! documented as unsafe. What makes it safe here is that every rendered value is
//! **faucet-generated and type-constrained** — an `i64`, a `u64`, or an RFC3339
//! timestamp formatted by chrono — never user data and never a passthrough of
//! anything a remote system said. [`plan()`] is the only place token values are
//! produced, and it produces them from typed fields — an `i64` start/end, a
//! `u64` offset/limit, a chrono-formatted timestamp.

use super::spec::{CountBound, IntBound, PartitionSpec};
use crate::chunking::{self, Bounds};
use crate::error::{CliError, CliResult};
use serde_json::Value;
use std::collections::BTreeMap;

/// The literal prefix a partition token starts with.
const PREFIX: &str = "${partition.";

/// One planned chunk: a stable id plus the tokens its invocation substitutes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionChunk {
    /// Stable id, used as the state-key suffix and in log lines. Unpadded, so
    /// it stays the same when the plan grows past a power of ten.
    pub id: String,
    /// Token name → rendered value, e.g. `start` → `"10000"`.
    pub tokens: BTreeMap<String, String>,
    /// True for the final chunk of an `to_unbounded` integer range: its upper
    /// bound is omitted so late-arriving rows above the planned maximum are
    /// still read.
    pub open_ended: bool,
}

/// The `${partition.end}` an open-ended final chunk renders: the largest
/// value, so its predicate (inclusive or half-open) admits every row above the
/// planned bound.
pub const UNBOUNDED_END: i64 = i64::MAX;

/// Plan every chunk for `spec`.
pub fn plan(spec: &PartitionSpec) -> CliResult<Vec<PartitionChunk>> {
    spec.validate()?;
    match spec {
        PartitionSpec::Integer {
            from,
            to,
            chunk_size,
            bounds,
            to_unbounded,
        } => {
            let to = match to {
                IntBound::Literal(v) => *v,
                IntBound::Discovered(_) => {
                    return Err(CliError::Internal(
                        "partition: an undiscovered bound reached the planner —                          `resolve_bounds` must run before `plan`"
                            .into(),
                    ));
                }
            };
            let chunks = chunking::plan_int_chunks(*from, to, *chunk_size, *bounds)?;
            Ok(chunks
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let open = to_unbounded.unwrap_or(false) && c.is_last;
                    let end = if open { UNBOUNDED_END } else { c.end };
                    let mut tokens = BTreeMap::new();
                    tokens.insert("start".into(), c.start.to_string());
                    tokens.insert("end".into(), end.to_string());
                    tokens.insert("index".into(), i.to_string());
                    tokens.insert("id".into(), c.id.clone());
                    PartitionChunk {
                        id: c.id,
                        tokens,
                        open_ended: open,
                    }
                })
                .collect())
        }

        PartitionSpec::Timestamp {
            from,
            to,
            chunk_size,
            timezone,
        } => {
            let tz: chrono_tz::Tz = timezone
                .as_deref()
                .unwrap_or("UTC")
                .parse()
                .map_err(|_| CliError::Config("invalid partition.timezone".into()))?;
            let step = chunking::parse_window(chunk_size)?;
            let from = chunking::parse_boundary(from, tz)?;
            let to = chunking::parse_boundary(to, tz)?;
            let chunks = chunking::plan_windows(from, to, Some(step), tz)?;
            Ok(chunks
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let mut tokens = BTreeMap::new();
                    tokens.insert("start".into(), c.start.to_rfc3339());
                    tokens.insert("end".into(), c.end.to_rfc3339());
                    tokens.insert("start_date".into(), c.start.format("%Y-%m-%d").to_string());
                    tokens.insert("end_date".into(), c.end.format("%Y-%m-%d").to_string());
                    tokens.insert("start_unix".into(), c.start.timestamp().to_string());
                    tokens.insert("end_unix".into(), c.end.timestamp().to_string());
                    tokens.insert("index".into(), i.to_string());
                    tokens.insert("id".into(), c.id.clone());
                    PartitionChunk {
                        id: c.id,
                        tokens,
                        open_ended: false,
                    }
                })
                .collect())
        }

        PartitionSpec::Offset { total, chunk_size } => {
            let total = match total {
                CountBound::Literal(v) => *v,
                CountBound::Discovered(_) => {
                    return Err(CliError::Internal(
                        "partition: an undiscovered total reached the planner —                          `resolve_bounds` must run before `plan`"
                            .into(),
                    ));
                }
            };
            let chunks = chunking::plan_offset_chunks(total, *chunk_size)?;
            Ok(chunks
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    let mut tokens = BTreeMap::new();
                    tokens.insert("offset".into(), c.offset.to_string());
                    tokens.insert("limit".into(), c.limit.to_string());
                    tokens.insert("index".into(), i.to_string());
                    tokens.insert("id".into(), c.id.clone());
                    PartitionChunk {
                        id: c.id,
                        tokens,
                        open_ended: false,
                    }
                })
                .collect())
        }
    }
}

/// Substitute `${partition.*}` tokens in every string leaf of `value`.
///
/// An unrecognized token is a typo, not a passthrough: it errors, naming the
/// tokens this chunk actually defines. Silently leaving `${partition.strat}` in a
/// URL would send the literal text to the source.
pub fn substitute(value: &mut Value, chunk: &PartitionChunk) -> CliResult<()> {
    match value {
        Value::String(s) => {
            *s = substitute_in_str(s, chunk)?;
            Ok(())
        }
        Value::Array(a) => a.iter_mut().try_for_each(|v| substitute(v, chunk)),
        Value::Object(m) => m.values_mut().try_for_each(|v| substitute(v, chunk)),
        _ => Ok(()),
    }
}

fn substitute_in_str(input: &str, chunk: &PartitionChunk) -> CliResult<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(pos) = rest.find(PREFIX) {
        out.push_str(&rest[..pos]);
        let after = &rest[pos + PREFIX.len()..];
        let close = after.find('}').ok_or_else(|| {
            CliError::Config(format!("unterminated ${{partition.…}} token in '{input}'"))
        })?;
        let token = &after[..close];
        let rendered = chunk.tokens.get(token).ok_or_else(|| {
            CliError::Config(format!(
                "unknown token ${{partition.{token}}} — this partition defines: {}",
                chunk.tokens.keys().cloned().collect::<Vec<_>>().join(", ")
            ))
        })?;
        out.push_str(rendered);
        rest = &after[close + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Whether a serialized config references any `${partition.*}` token.
///
/// A `partition:` block on a row whose source ignores the tokens would run the
/// same query N times, so this gates the config at load time.
pub fn references_partition(serialized: &str) -> bool {
    serialized.contains(PREFIX)
}

/// The `end` predicate for an open-ended final chunk is the caller's problem —
/// this reports whether one was planned so `expand` can require the config to
/// handle it.
pub fn has_open_ended(chunks: &[PartitionChunk]) -> bool {
    chunks.iter().any(|c| c.open_ended)
}

/// Bounds label for log lines.
pub fn bounds_label(b: Bounds) -> &'static str {
    match b {
        Bounds::Inclusive => "inclusive",
        Bounds::HalfOpen => "half_open",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn int_spec(to_unbounded: bool) -> PartitionSpec {
        PartitionSpec::Integer {
            from: 0,
            to: IntBound::Literal(24),
            chunk_size: 10,
            bounds: Bounds::Inclusive,
            to_unbounded: to_unbounded.then_some(true),
        }
    }

    #[test]
    fn integer_chunks_carry_start_end_index_id() {
        let chunks = plan(&int_spec(false)).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].tokens["start"], "0");
        assert_eq!(chunks[0].tokens["end"], "9");
        assert_eq!(chunks[0].tokens["index"], "0");
        assert_eq!(
            chunks[2].tokens["end"], "24",
            "final chunk truncates at `to`"
        );
        assert!(chunks.iter().all(|c| !c.open_ended));
    }

    #[test]
    fn only_the_final_chunk_is_open_ended_and_only_when_asked() {
        let chunks = plan(&int_spec(true)).unwrap();
        assert_eq!(chunks.iter().filter(|c| c.open_ended).count(), 1);
        assert!(chunks.last().unwrap().open_ended);
        assert_eq!(
            chunks.last().unwrap().tokens["end"],
            i64::MAX.to_string(),
            "the open tail renders no real upper bound"
        );
        assert_eq!(chunks[1].tokens["end"], "19", "inner chunks keep theirs");
        assert!(has_open_ended(&chunks));
        assert!(!has_open_ended(&plan(&int_spec(false)).unwrap()));
    }

    #[test]
    fn offset_chunks_carry_offset_and_limit() {
        let chunks = plan(&PartitionSpec::Offset {
            total: CountBound::Literal(25),
            chunk_size: 10,
        })
        .unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].tokens["offset"], "0");
        assert_eq!(chunks[0].tokens["limit"], "10");
        assert_eq!(
            chunks[2].tokens["limit"], "5",
            "final limit is the remainder"
        );
        assert!(
            !chunks[0].tokens.contains_key("start"),
            "no id-range tokens"
        );
    }

    #[test]
    fn timestamp_chunks_carry_the_backfill_token_set() {
        let chunks = plan(&PartitionSpec::Timestamp {
            from: "2026-06-01".into(),
            to: "2026-06-04".into(),
            chunk_size: "1d".into(),
            timezone: None,
        })
        .unwrap();
        assert_eq!(chunks.len(), 3);
        for t in [
            "start",
            "end",
            "start_date",
            "end_date",
            "start_unix",
            "end_unix",
        ] {
            assert!(chunks[0].tokens.contains_key(t), "missing {t}");
        }
        assert_eq!(chunks[0].tokens["start_date"], "2026-06-01");
        assert_eq!(chunks[1].tokens["start_date"], "2026-06-02");
    }

    #[test]
    fn substitutes_into_nested_string_leaves_only() {
        let chunks = plan(&int_spec(false)).unwrap();
        let mut cfg = json!({
            "url": "https://api/x?from=${partition.start}&to=${partition.end}",
            "nested": { "list": ["chunk-${partition.index}", 7, true] },
            "count": 3
        });
        substitute(&mut cfg, &chunks[1]).unwrap();
        assert_eq!(cfg["url"], "https://api/x?from=10&to=19");
        assert_eq!(cfg["nested"]["list"][0], "chunk-1");
        assert_eq!(cfg["nested"]["list"][1], 7, "non-strings untouched");
        assert_eq!(cfg["count"], 3);
    }

    #[test]
    fn substitutes_the_same_token_more_than_once() {
        let chunks = plan(&int_spec(false)).unwrap();
        let mut cfg = json!({ "q": "id >= ${partition.start} AND ${partition.start} > 0" });
        substitute(&mut cfg, &chunks[0]).unwrap();
        assert_eq!(cfg["q"], "id >= 0 AND 0 > 0");
    }

    #[test]
    fn an_unknown_token_errors_and_lists_what_is_available() {
        let chunks = plan(&int_spec(false)).unwrap();
        let mut cfg = json!({ "url": "x?a=${partition.strat}" });
        let err = substitute(&mut cfg, &chunks[0]).unwrap_err().to_string();
        assert!(err.contains("strat"), "{err}");
        assert!(err.contains("start"), "should list the real tokens: {err}");
    }

    #[test]
    fn an_offset_config_cannot_reference_id_range_tokens() {
        // Guards the kind/token mismatch: a config written for an id range but
        // partitioned by offset fails loudly instead of sending literal text.
        let chunks = plan(&PartitionSpec::Offset {
            total: CountBound::Literal(10),
            chunk_size: 5,
        })
        .unwrap();
        let mut cfg = json!({ "url": "x?from=${partition.start}" });
        let err = substitute(&mut cfg, &chunks[0]).unwrap_err().to_string();
        assert!(err.contains("start"), "{err}");
        assert!(err.contains("offset"), "lists the offset tokens: {err}");
    }

    #[test]
    fn an_unterminated_token_is_a_typed_error() {
        let chunks = plan(&int_spec(false)).unwrap();
        let mut cfg = json!({ "url": "x?a=${partition.start" });
        let err = substitute(&mut cfg, &chunks[0]).unwrap_err().to_string();
        assert!(err.contains("unterminated"), "{err}");
    }

    #[test]
    fn detects_whether_a_config_references_the_tokens() {
        assert!(references_partition(r#"{"url":"x?a=${partition.start}"}"#));
        assert!(!references_partition(r#"{"url":"x?a=${now.date}"}"#));
    }

    #[test]
    fn rendered_values_are_numeric_or_rfc3339_never_passthrough() {
        // The injection-safety argument: every value comes from a typed field.
        let chunks = plan(&int_spec(false)).unwrap();
        for c in &chunks {
            for k in ["start", "end", "index"] {
                assert!(
                    c.tokens[k].parse::<i64>().is_ok(),
                    "{k} must render as an integer, got {:?}",
                    c.tokens[k]
                );
            }
        }
        let ts = plan(&PartitionSpec::Timestamp {
            from: "2026-06-01".into(),
            to: "2026-06-02".into(),
            chunk_size: "1d".into(),
            timezone: None,
        })
        .unwrap();
        assert!(chrono::DateTime::parse_from_rfc3339(&ts[0].tokens["start"]).is_ok());
    }
}

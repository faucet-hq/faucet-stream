//! Discover a partition bound by running a source once (#479).
//!
//! An id range is frequently open-ended: you know where to start but not where
//! the data ends. Rather than invent a probe protocol, the bound comes from an
//! ordinary **source config** — so `SELECT MAX(id)`, a `?sort=-id&limit=1`
//! request, or a count endpoint all work through the existing connector
//! registry, auth catalog, and secrets. Discovery stays as source-agnostic as
//! the substitution it feeds.
//!
//! ## The probe/plan race
//!
//! Rows inserted between the probe and the last chunk's execution sit above the
//! discovered maximum and would never be read. `to_unbounded: true` closes that
//! by dropping the final chunk's upper bound, so it is **defaulted on** whenever
//! `to` is discovered — the same reason `plan_pk_shards` marks its last shard
//! `hi_unbounded`. An explicit `to_unbounded: false` still wins, for a range the
//! user knows is closed.
//!
//! ## Injection
//!
//! A probed value comes from outside faucet, so it is parsed into a typed `i64` /
//! `u64` here and re-rendered from that. The raw string a source returned is
//! never substituted into a config — which is what keeps the "every token value
//! is faucet-generated" argument true once discovery is in play.

use super::spec::{BoundProbe, CountBound, IntBound, PartitionSpec};
use crate::auth_catalog::AuthCatalog;
use crate::error::{CliError, CliResult};
use serde_json::Value;

/// Resolve every discoverable bound in `spec`, returning a spec whose bounds are
/// literals. A spec with no probes is returned unchanged and costs nothing.
pub async fn resolve_bounds(spec: &PartitionSpec, auth: &AuthCatalog) -> CliResult<PartitionSpec> {
    Ok(match spec {
        PartitionSpec::Integer {
            from,
            to,
            chunk_size,
            bounds,
            to_unbounded,
        } => match to {
            IntBound::Literal(_) => spec.clone(),
            IntBound::Discovered(p) => {
                let raw = probe_value(p, auth).await?;
                let v = as_i64(&raw, &p.value_path)?;
                if v < *from {
                    return Err(CliError::Config(format!(
                        "partition: the discovered upper bound ({v}) is below `from` ({from}) — \
                         the probe `{}` returned an empty or unexpected result",
                        p.value_path
                    )));
                }
                PartitionSpec::Integer {
                    from: *from,
                    to: IntBound::Literal(v),
                    chunk_size: *chunk_size,
                    bounds: *bounds,
                    // Default the open-ended tail ON for a probed bound: the
                    // probe is stale the instant it returns, so rows appended
                    // between it and the last chunk would otherwise be missed.
                    // An explicit setting still wins.
                    to_unbounded: Some(to_unbounded.unwrap_or(true)),
                }
            }
        },
        PartitionSpec::Offset { total, chunk_size } => match total {
            CountBound::Literal(_) => spec.clone(),
            CountBound::Discovered(p) => {
                let raw = probe_value(p, auth).await?;
                let v = as_u64(&raw, &p.value_path)?;
                PartitionSpec::Offset {
                    total: CountBound::Literal(v),
                    chunk_size: *chunk_size,
                }
            }
        },
        PartitionSpec::Timestamp { .. } => spec.clone(),
    })
}

/// Whether `spec` needs a probe — lets `faucet validate` report that it cannot
/// fully plan offline without pretending it can.
pub fn needs_probe(spec: &PartitionSpec) -> bool {
    matches!(
        spec,
        PartitionSpec::Integer {
            to: IntBound::Discovered(_),
            ..
        } | PartitionSpec::Offset {
            total: CountBound::Discovered(_),
            ..
        }
    )
}

/// Whether a probed integer bound should default `to_unbounded` on.
pub fn probe_implies_unbounded(spec: &PartitionSpec) -> bool {
    matches!(
        spec,
        PartitionSpec::Integer {
            to: IntBound::Discovered(_),
            ..
        }
    )
}

/// Run the probe source and pull `value_path` out of its first record.
async fn probe_value(p: &BoundProbe, auth: &AuthCatalog) -> CliResult<Value> {
    let source = crate::registry::build_source(
        &p.from_source.kind,
        p.from_source.config.clone(),
        auth,
        None,
    )
    .await
    .map_err(|e| {
        CliError::Config(format!(
            "partition bound probe: building source failed: {e}"
        ))
    })?;

    let records = source.fetch_all().await.map_err(|e| {
        CliError::Config(format!(
            "partition bound probe: fetching the bound failed: {e}"
        ))
    })?;

    // Empty is a distinct, actionable outcome — not "assume zero". Zero would
    // silently plan nothing and the run would read no data at all.
    let first = records.first().ok_or_else(|| {
        CliError::Config(format!(
            "partition bound probe: source '{}' returned no records, so the bound could not \
             be determined. A `MAX(id)` over an empty table returns NULL rather than a row — \
             give the range an explicit `to` if the source can legitimately be empty",
            p.from_source.kind
        ))
    })?;

    // Reuse core's JSONPath helper so the path grammar matches `records_path`
    // and every other JSONPath surface in the project.
    let found = faucet_core::util::extract_records(first, Some(&p.value_path)).map_err(|e| {
        CliError::Config(format!(
            "partition bound probe: value_path '{}' is not valid JSONPath: {e}",
            p.value_path
        ))
    })?;
    match found.as_slice() {
        [] => Err(CliError::Config(format!(
            "partition bound probe: value_path '{}' matched nothing in the probe's first \
             record ({})",
            p.value_path,
            crate::secrets::registry::redact(&first.to_string())
        ))),
        [one] => Ok(one.clone()),
        many => Err(CliError::Config(format!(
            "partition bound probe: value_path '{}' matched {} values; it must select exactly \
             one",
            p.value_path,
            many.len()
        ))),
    }
}

/// Parse a probed value as a signed bound. A JSON number or a numeric string are
/// both accepted (a SQL driver may hand back either); anything else is an error
/// rather than a silent zero.
fn as_i64(v: &Value, path: &str) -> CliResult<i64> {
    if v.is_null() {
        return Err(CliError::Config(format!(
            "partition bound probe: '{path}' is null — `MAX(id)` over an empty table returns \
             NULL. Give the range an explicit `to`, or ensure the probe cannot match zero rows"
        )));
    }
    v.as_i64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
        .ok_or_else(|| {
            CliError::Config(format!(
                "partition bound probe: '{path}' is not an integer (got {v})"
            ))
        })
}

fn as_u64(v: &Value, path: &str) -> CliResult<u64> {
    if v.is_null() {
        return Err(CliError::Config(format!(
            "partition bound probe: '{path}' is null — give the range an explicit `total`"
        )));
    }
    v.as_u64()
        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<u64>().ok()))
        .ok_or_else(|| {
            CliError::Config(format!(
                "partition bound probe: '{path}' is not a non-negative integer (got {v})"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn probe() -> BoundProbe {
        BoundProbe {
            from_source: crate::config::ConnectorSpec {
                kind: "csv".into(),
                config: json!({"path": "./x.csv"}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
                attributes: Default::default(),
            },
            value_path: "$.max_id".into(),
        }
    }

    #[test]
    fn needs_probe_only_when_a_bound_is_discovered() {
        let literal = PartitionSpec::Integer {
            from: 0,
            to: IntBound::Literal(10),
            chunk_size: 5,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: None,
        };
        assert!(!needs_probe(&literal));

        let discovered = PartitionSpec::Integer {
            from: 0,
            to: IntBound::Discovered(probe()),
            chunk_size: 5,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: None,
        };
        assert!(needs_probe(&discovered));
        assert!(probe_implies_unbounded(&discovered));
        assert!(!probe_implies_unbounded(&literal));

        // A timestamp range is never probed.
        assert!(!needs_probe(&PartitionSpec::Timestamp {
            from: "a".into(),
            to: "b".into(),
            chunk_size: "1d".into(),
            timezone: None,
        }));
    }

    #[test]
    fn parses_numbers_and_numeric_strings() {
        assert_eq!(as_i64(&json!(42), "$.x").unwrap(), 42);
        // A SQL driver may hand back a big integer as text.
        assert_eq!(as_i64(&json!("  42 "), "$.x").unwrap(), 42);
        assert_eq!(as_u64(&json!(7), "$.x").unwrap(), 7);
        assert_eq!(as_u64(&json!("7"), "$.x").unwrap(), 7);
    }

    #[test]
    fn null_is_refused_with_the_empty_table_explanation() {
        // `MAX(id)` over an empty table returns NULL. Treating that as 0 would
        // plan a single degenerate chunk and read nothing.
        let err = as_i64(&json!(null), "$.max_id").unwrap_err().to_string();
        assert!(err.contains("null"), "{err}");
        assert!(err.contains("empty table"), "explains why: {err}");
        assert!(as_u64(&json!(null), "$.total").is_err());
    }

    #[test]
    fn non_numeric_is_refused_rather_than_defaulted() {
        for v in [json!("abc"), json!(true), json!({"a": 1}), json!([1])] {
            assert!(as_i64(&v, "$.x").is_err(), "{v} must not parse");
            assert!(as_u64(&v, "$.x").is_err(), "{v} must not parse");
        }
        // Negative is a valid i64 bound but never a valid count.
        assert_eq!(as_i64(&json!(-5), "$.x").unwrap(), -5);
        assert!(as_u64(&json!(-5), "$.x").is_err());
    }

    #[tokio::test]
    async fn a_probe_below_from_is_refused() {
        // Guards the "probe returned something unexpected" case: planning
        // [0, -1] would otherwise be an opaque empty-range error.
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("p.csv");
        std::fs::write(&csv, "max_id\n-3\n").unwrap();
        let mut p = probe();
        p.from_source.config = json!({ "path": csv.to_str().unwrap() });

        let spec = PartitionSpec::Integer {
            from: 0,
            to: IntBound::Discovered(p),
            chunk_size: 5,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: None,
        };
        let err = resolve_bounds(&spec, &AuthCatalog::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("below `from`"), "{err}");
    }

    #[tokio::test]
    async fn a_probed_bound_defaults_the_open_tail_on() {
        // The probe is stale the instant it returns, so rows appended between it
        // and the last chunk would be missed without an open final chunk.
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("p.csv");
        std::fs::write(&csv, "max_id\n99\n").unwrap();
        let mut p = probe();
        p.from_source.config = json!({ "path": csv.to_str().unwrap() });

        let mk = |explicit: Option<bool>| PartitionSpec::Integer {
            from: 0,
            to: IntBound::Discovered(p.clone()),
            chunk_size: 50,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: explicit,
        };
        let unset = resolve_bounds(&mk(None), &AuthCatalog::default())
            .await
            .unwrap();
        match unset {
            PartitionSpec::Integer { to_unbounded, .. } => {
                assert_eq!(
                    to_unbounded,
                    Some(true),
                    "unset must default ON when probed"
                )
            }
            o => panic!("{o:?}"),
        }
        // An explicit `false` still wins — the user knows the range is closed.
        let forced = resolve_bounds(&mk(Some(false)), &AuthCatalog::default())
            .await
            .unwrap();
        match forced {
            PartitionSpec::Integer { to_unbounded, .. } => assert_eq!(to_unbounded, Some(false)),
            o => panic!("{o:?}"),
        }
    }

    #[tokio::test]
    async fn a_literal_bound_never_defaults_the_open_tail_on() {
        let spec = PartitionSpec::Integer {
            from: 0,
            to: IntBound::Literal(10),
            chunk_size: 5,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: None,
        };
        let out = resolve_bounds(&spec, &AuthCatalog::default())
            .await
            .unwrap();
        match out {
            PartitionSpec::Integer { to_unbounded, .. } => assert_eq!(to_unbounded, None),
            o => panic!("{o:?}"),
        }
    }

    #[tokio::test]
    async fn a_literal_spec_is_returned_unchanged_without_any_probing() {
        let spec = PartitionSpec::Offset {
            total: CountBound::Literal(10),
            chunk_size: 5,
        };
        let out = resolve_bounds(&spec, &AuthCatalog::default())
            .await
            .unwrap();
        assert_eq!(out, spec);
    }

    #[tokio::test]
    async fn a_discovered_bound_is_resolved_from_the_probe_source() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("p.csv");
        std::fs::write(&csv, "max_id\n99\n").unwrap();
        let mut p = probe();
        p.from_source.config = json!({ "path": csv.to_str().unwrap() });

        let spec = PartitionSpec::Integer {
            from: 0,
            to: IntBound::Discovered(p),
            chunk_size: 50,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: Some(true),
        };
        let out = resolve_bounds(&spec, &AuthCatalog::default())
            .await
            .unwrap();
        match out {
            PartitionSpec::Integer { to, .. } => assert_eq!(to, IntBound::Literal(99)),
            other => panic!("expected an integer spec, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_empty_probe_result_is_actionable_not_zero() {
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("p.csv");
        std::fs::write(&csv, "max_id\n").unwrap(); // header only
        let mut p = probe();
        p.from_source.config = json!({ "path": csv.to_str().unwrap() });

        let spec = PartitionSpec::Integer {
            from: 0,
            to: IntBound::Discovered(p),
            chunk_size: 5,
            bounds: crate::chunking::Bounds::Inclusive,
            to_unbounded: None,
        };
        let err = resolve_bounds(&spec, &AuthCatalog::default())
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("no records"), "{err}");
        assert!(err.contains("explicit `to`"), "suggests the fix: {err}");
    }
}

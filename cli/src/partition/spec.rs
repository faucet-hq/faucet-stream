//! Serde config types for the `partition:` block (#479).
//!
//! A partitioned row is fanned out into N independent invocations, each scoped
//! to one chunk of a range via `${partition.*}` tokens substituted into the
//! connector configs. This is `faucet backfill`'s window mechanism generalized:
//! from time-only to time / integer / offset, and from a separate bounded-replay
//! command to any run.
//!
//! ## Why the kinds are a tagged enum
//!
//! Each kind needs a different set of fields, and two of the mistakes are silent
//! data loss rather than errors:
//!
//! - `bounds` is meaningful for `integer` and required there, but meaningless for
//!   `offset`. As a tagged enum, serde makes it required exactly where it applies
//!   instead of a runtime "you must set this when kind is …" check.
//! - **A count is not a maximum key.** `{"total": 1234567}` equals the largest id
//!   only when ids are dense and 1-based; chunking an *id range* from a count
//!   silently stops early the moment ids are sparse (deletions, sharded id
//!   allocation, non-sequential keys), and every record above it is never
//!   fetched. Because `total` lives only on `offset` and `to` only on `integer`,
//!   that mistake is structurally impossible rather than merely documented.

use crate::chunking::Bounds;
use crate::error::{CliError, CliResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A partitioned range. `kind` selects the variant.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PartitionSpec {
    /// Split an integer range — the shape for an API or table filtered by an id
    /// range (`?id_from=&id_to=`, `WHERE id BETWEEN`).
    Integer {
        /// Inclusive lower bound.
        from: i64,
        /// Upper bound, interpreted per `bounds`. Either a literal, or a probe
        /// that discovers it (`{from_source: …, value_path: …}`).
        to: IntBound,
        /// Values per chunk. Must be > 0.
        chunk_size: u64,
        /// Whether `to` (and each chunk's `end`) is inclusive or exclusive.
        ///
        /// **Required, with no default.** Getting it wrong is silent: half-open
        /// chunks against an inclusive-bound source fetch one record twice per
        /// boundary; inclusive chunks against an exclusive source skip one per
        /// boundary. Neither raises.
        bounds: Bounds,
        /// Render the final chunk without an upper bound, so rows appended above
        /// `to` between planning and execution are still read.
        ///
        /// Unset defaults to **`true` when `to` is discovered** and `false` when
        /// it is a literal: a probed bound is stale the moment it is read, so the
        /// open tail is what keeps late rows from being missed. An explicit value
        /// always wins, for a range the user knows is closed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        to_unbounded: Option<bool>,
    },

    /// Split a time range — the same windowing `faucet backfill` uses, including
    /// its DST-correct calendar steps.
    Timestamp {
        /// RFC3339, or a bare date interpreted as midnight in `timezone`.
        from: String,
        /// Exclusive upper bound; time windows are always half-open.
        to: String,
        /// Window size: `45s`, `30m`, `6h`, `1d`, `1w`, or a bare integer =
        /// seconds. `d`/`w` are calendar steps, so `1d` and `24h` differ across a
        /// DST transition — deliberately.
        chunk_size: String,
        /// IANA timezone for date boundaries and calendar steps. Defaults to UTC.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
    },

    /// Split a countable result set into offset/limit slices — the parallel form
    /// of what a source's serial offset pagination already does.
    Offset {
        /// Total rows in the result set. Either a literal, or a probe that
        /// discovers it.
        total: CountBound,
        /// Rows per chunk. Must be > 0.
        chunk_size: u64,
    },
}

/// A discoverable upper bound for an integer range.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum IntBound {
    /// Known up front.
    Literal(i64),
    /// Discovered by running a probe source once — `SELECT MAX(id)`, or a
    /// `?sort=-id&limit=1` request.
    Discovered(BoundProbe),
}

/// A discoverable row count for an offset range.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum CountBound {
    Literal(u64),
    Discovered(BoundProbe),
}

/// Discover a bound by running a source once and reading one field out of its
/// first record.
///
/// Any source works — the probe is an ordinary connector config, so a SQL
/// `SELECT MAX(id)`, a REST "last record" request, or anything else the registry
/// can build. That keeps discovery source-agnostic for the same reason the
/// substitution is: no connector needs to know this feature exists.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct BoundProbe {
    /// A full source config, `{type, config}`, run once before planning.
    pub from_source: crate::config::ConnectorSpec,
    /// JSONPath into the probe's **first** record, e.g. `$.max_id`.
    pub value_path: String,
}

impl PartitionSpec {
    /// Fail-fast validation, run by `faucet validate` and again before any run.
    /// Range/size errors surface from the planners; this covers what has to be
    /// checked before planning is even attempted.
    pub fn validate(&self) -> CliResult<()> {
        match self {
            Self::Integer { chunk_size, .. } | Self::Offset { chunk_size, .. } => {
                if *chunk_size == 0 {
                    // Deliberately unlike the `batch_size: 0` sentinel elsewhere,
                    // where 0 means "no batching" — here it would mean infinite
                    // chunks, so it is an error rather than a mode.
                    return Err(CliError::Config(
                        "partition.chunk_size must be greater than 0 (unlike `batch_size`, \
                         0 is not a 'no chunking' sentinel here)"
                            .into(),
                    ));
                }
            }
            Self::Timestamp {
                chunk_size,
                timezone,
                ..
            } => {
                crate::chunking::parse_window(chunk_size)?;
                if let Some(tz) = timezone {
                    tz.parse::<chrono_tz::Tz>().map_err(|_| {
                        CliError::Config(format!(
                            "'{tz}' is not a valid IANA timezone (e.g. UTC, America/New_York)"
                        ))
                    })?;
                }
            }
        }
        Ok(())
    }

    /// The token names this spec's chunks will define, for error messages that
    /// tell the user what they *can* reference.
    pub fn token_names(&self) -> &'static [&'static str] {
        match self {
            Self::Integer { .. } => &["start", "end", "index", "id"],
            Self::Timestamp { .. } => &[
                "start",
                "end",
                "start_date",
                "end_date",
                "start_unix",
                "end_unix",
                "index",
                "id",
            ],
            Self::Offset { .. } => &["offset", "limit", "index", "id"],
        }
    }

    /// Human label for logs and errors.
    pub fn kind_str(&self) -> &'static str {
        match self {
            Self::Integer { .. } => "integer",
            Self::Timestamp { .. } => "timestamp",
            Self::Offset { .. } => "offset",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<PartitionSpec, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn integer_requires_bounds() {
        // The whole point of the tagged enum: omitting `bounds` on an integer
        // range is a parse error, not a runtime surprise at the first boundary.
        let err = parse("kind: integer\nfrom: 0\nto: 100\nchunk_size: 10\n")
            .expect_err("bounds must be required");
        assert!(err.to_string().contains("bounds"), "{err}");
    }

    #[test]
    fn integer_parses_with_bounds() {
        let s =
            parse("kind: integer\nfrom: 0\nto: 100\nchunk_size: 10\nbounds: inclusive\n").unwrap();
        assert!(matches!(
            s,
            PartitionSpec::Integer {
                from: 0,
                to: IntBound::Literal(100),
                chunk_size: 10,
                bounds: Bounds::Inclusive,
                to_unbounded: None
            }
        ));
        s.validate().unwrap();
    }

    #[test]
    fn a_count_cannot_bound_an_id_range() {
        // `total` belongs to `offset` only. Chunking an id range from a count is
        // silently wrong when ids are sparse, so the shape forbids it.
        let err = parse("kind: integer\nfrom: 0\ntotal: 1000\nchunk_size: 10\nbounds: inclusive\n")
            .expect_err("total is not an integer-range field");
        assert!(err.to_string().contains("total"), "{err}");
    }

    #[test]
    fn an_id_bound_cannot_be_given_to_an_offset_range() {
        let err = parse("kind: offset\nto: 1000\nchunk_size: 10\n")
            .expect_err("to is not an offset field");
        assert!(err.to_string().contains("to"), "{err}");
    }

    #[test]
    fn offset_parses_and_needs_no_bounds() {
        let s = parse("kind: offset\ntotal: 250\nchunk_size: 100\n").unwrap();
        assert!(matches!(
            s,
            PartitionSpec::Offset {
                total: CountBound::Literal(250),
                chunk_size: 100
            }
        ));
        s.validate().unwrap();
    }

    #[test]
    fn timestamp_parses_and_validates_window_and_timezone() {
        let s = parse(
            "kind: timestamp\nfrom: 2026-01-01\nto: 2026-02-01\nchunk_size: 1d\n\
             timezone: America/New_York\n",
        )
        .unwrap();
        s.validate().unwrap();

        let bad_window = parse("kind: timestamp\nfrom: a\nto: b\nchunk_size: 1y\n").unwrap();
        assert!(bad_window.validate().is_err(), "1y is not a valid window");

        let bad_tz =
            parse("kind: timestamp\nfrom: a\nto: b\nchunk_size: 1d\ntimezone: Mars/Olympus\n")
                .unwrap();
        assert!(
            bad_tz.validate().is_err(),
            "bogus timezone must be rejected"
        );
    }

    #[test]
    fn zero_chunk_size_is_rejected_with_the_batch_size_distinction_spelled_out() {
        let s =
            parse("kind: integer\nfrom: 0\nto: 10\nchunk_size: 0\nbounds: half_open\n").unwrap();
        let err = s.validate().unwrap_err();
        assert!(err.to_string().contains("greater than 0"), "{err}");
        assert!(
            err.to_string().contains("batch_size"),
            "the message should distinguish it from the batch_size sentinel: {err}"
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(
            parse("kind: offset\ntotal: 10\nchunk_size: 5\nchunck_size: 5\n").is_err(),
            "a typo must not be silently ignored"
        );
    }

    #[test]
    fn token_names_match_the_kind() {
        let int =
            parse("kind: integer\nfrom: 0\nto: 1\nchunk_size: 1\nbounds: inclusive\n").unwrap();
        assert!(int.token_names().contains(&"start"));
        assert!(!int.token_names().contains(&"offset"));

        let off = parse("kind: offset\ntotal: 1\nchunk_size: 1\n").unwrap();
        assert!(off.token_names().contains(&"offset"));
        assert!(!off.token_names().contains(&"start"));
    }
}

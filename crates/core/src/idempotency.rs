//! Exactly-once / idempotent delivery primitives.
//!
//! The pipeline issues a monotonic **commit token** for every page that carries
//! a bookmark. The token is persisted in the [`StateStore`](crate::state::StateStore)
//! value next to the bookmark and committed inside the sink's own transaction,
//! so a crash between "sink durably wrote" and "state persisted" is resolved on
//! resume by skipping pages the sink already committed. See
//! `docs/superpowers/specs/2026-06-09-exactly-once-delivery-design.md`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Delivery guarantee for a pipeline run.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMode {
    /// Today's behaviour: a page may be re-delivered after a crash between the
    /// sink write and the bookmark persist. Downstream must tolerate duplicates.
    #[default]
    AtLeastOnce,
    /// The sink durably records a per-page commit token atomically with the
    /// data; on resume the pipeline skips already-committed pages. Requires a
    /// state store, an idempotent sink, and a deterministic-replay source.
    ExactlyOnce,
}

/// How faithfully a [`Source`](crate::Source) **replays** its record stream
/// when resumed from a bookmark.
///
/// This is the source-side capability the effectively-once *atomic-watermark*
/// mechanism depends on: after a crash the pipeline re-anchors the source at a
/// persisted position, and correctness requires that nothing before that
/// position is re-emitted and nothing after it is skipped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayGuarantee {
    /// Resuming from a bookmark may replay a *different* record stream
    /// (query-based sources whose upstream can mutate, sources without
    /// per-page bookmarks). The default.
    #[default]
    NonDeterministic,
    /// The source emits a complete resume position (bookmark) on **every**
    /// page, and resuming from any such bookmark continues the record stream
    /// at exactly that position — no record before the bookmark is re-emitted
    /// and none after it is skipped (immutable-log sources: CDC WAL/binlog/
    /// change streams, Kafka partitions).
    Deterministic,
}

/// The strongest delivery guarantee a [`Sink`](crate::Sink) can uphold.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkGuarantee {
    /// Plain writes: a replayed page is written again. The default.
    #[default]
    AtLeastOnce,
    /// The sink can dedup by key (`write_mode: upsert` with a configured
    /// `key`): re-applying a record with the same key converges instead of
    /// duplicating.
    KeyedUpsert,
    /// The sink can commit a page's rows **and** a commit token in one atomic
    /// transaction ([`Sink::write_batch_idempotent`](crate::Sink)).
    AtomicWatermark,
}

/// The mechanism through which a pipeline achieves effectively-once delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectivelyOnceMechanism {
    /// Deterministic-replay source + sink that commits data and a per-page
    /// commit token atomically; on resume already-committed pages are skipped
    /// (or the stream is re-anchored at the sink's recorded position).
    AtomicWatermark,
    /// The sink dedups by key (`write_mode: upsert`); replayed records
    /// converge on the same keyed row. Works with any source.
    KeyedUpsert,
}

/// The end-to-end guarantee a *pipeline* provides for a given
/// source × sink × config combination.
///
/// Deliberately no `ExactlyOnce` variant — distributed-consensus exactly-once
/// is not achievable here; effectively-once (idempotent at-least-once: each
/// record is *observably applied* once) is the ceiling, and
/// `delivery: exactly_once` in config is precisely documented as requesting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "guarantee", content = "via")]
pub enum DeliveryGuarantee {
    /// A crash between the sink write and the bookmark persist may re-deliver
    /// a page. Downstream must tolerate duplicates.
    AtLeastOnce,
    /// Idempotent at-least-once: each record is observably applied once.
    EffectivelyOnce(EffectivelyOnceMechanism),
}

impl std::fmt::Display for DeliveryGuarantee {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AtLeastOnce => write!(f, "at-least-once"),
            Self::EffectivelyOnce(EffectivelyOnceMechanism::AtomicWatermark) => {
                write!(f, "effectively-once (atomic watermark)")
            }
            Self::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert) => {
                write!(f, "effectively-once (keyed upsert)")
            }
        }
    }
}

/// Inputs to [`derive_delivery_guarantee`] — the facts about a concrete
/// source × sink × config combination the derivation keys off.
#[derive(Debug, Clone, Copy, Default)]
pub struct GuaranteeInputs {
    /// The source's replay capability.
    pub replay: ReplayGuarantee,
    /// Whether the sink commits data + token atomically
    /// (`Sink::supports_idempotent_writes`).
    pub sink_atomic: bool,
    /// Whether the sink is *configured* to dedup by key — `write_mode: upsert`
    /// (or `delete`) with a non-empty `key` (`Sink::dedups_by_key`).
    pub keyed_upsert_configured: bool,
    /// Whether a durable (non-memory) state store is configured. The
    /// atomic-watermark mechanism persists its cross-restart sequence here.
    pub durable_state: bool,
    /// Whether a DLQ is configured (incompatible with the atomic-watermark
    /// mechanism in this version).
    pub dlq: bool,
}

/// Derive the end-to-end [`DeliveryGuarantee`] a pipeline actually provides.
///
/// Preference order: the atomic-watermark mechanism (strongest bookkeeping,
/// no keyed-schema requirement) when the topology supports it, then keyed
/// upsert, then at-least-once. A sink that is both atomic and keyed reports
/// atomic-watermark when the source replays deterministically, and falls back
/// to keyed upsert otherwise.
pub fn derive_delivery_guarantee(i: &GuaranteeInputs) -> DeliveryGuarantee {
    if i.sink_atomic && i.replay == ReplayGuarantee::Deterministic && i.durable_state && !i.dlq {
        return DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::AtomicWatermark);
    }
    if i.keyed_upsert_configured {
        return DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert);
    }
    DeliveryGuarantee::AtLeastOnce
}

/// Reserved key marking the exactly-once state wrapper object.
const EO_MARKER: &str = "__faucet_eo";
const EO_BOOKMARK: &str = "bookmark";
const EO_SEQ: &str = "seq";

/// Width of the zero-padded decimal token. `u64::MAX` is 20 digits, so 20 makes
/// lexicographic order match numeric order for the full `u64` range.
const TOKEN_WIDTH: usize = 20;

/// Separator between the numeric sequence and the embedded resume bookmark in
/// a commit token. The prefix before it is always the fixed-width sequence.
const TOKEN_BOOKMARK_SEP: char = '#';

/// Render a page sequence as a fixed-width, lexicographically-ordered token.
pub fn format_token(seq: u64) -> String {
    format!("{seq:0TOKEN_WIDTH$}")
}

/// Render a commit token that carries the page's **resume bookmark** alongside
/// the sequence: `"{seq:020}#{bookmark-json}"`.
///
/// Sinks store the token opaquely, so the committed watermark doubles as a
/// durable record of *where the stream stood* when the page committed. On
/// resume the pipeline recovers that position from the sink
/// ([`parse_token_parts`]) and re-anchors the source there — closing the
/// crash window between "sink durably committed" and "state store persisted"
/// without requiring the source to replay identical page boundaries.
pub fn format_token_with_bookmark(seq: u64, bookmark: Option<&Value>) -> String {
    match bookmark {
        Some(bm) => format!("{seq:0TOKEN_WIDTH$}{TOKEN_BOOKMARK_SEP}{bm}"),
        None => format_token(seq),
    }
}

/// Parse the numeric sequence from a token produced by [`format_token`] or
/// [`format_token_with_bookmark`]. Returns `None` on garbage.
pub fn parse_token(s: &str) -> Option<u64> {
    let seq = match s.split_once(TOKEN_BOOKMARK_SEP) {
        Some((prefix, _)) => prefix,
        None => s,
    };
    seq.trim().parse::<u64>().ok()
}

/// Parse a stored commit token into `(seq, embedded_bookmark)`.
///
/// Tokens written before bookmarks were embedded (bare `format_token` output)
/// parse with `bookmark = None`. A bookmark suffix that is not valid JSON also
/// yields `None` for the bookmark — the sequence alone still drives the
/// skip-on-resume path.
pub fn parse_token_parts(s: &str) -> Option<(u64, Option<Value>)> {
    match s.split_once(TOKEN_BOOKMARK_SEP) {
        Some((prefix, suffix)) => {
            let seq = prefix.trim().parse::<u64>().ok()?;
            Some((seq, serde_json::from_str(suffix).ok()))
        }
        None => Some((s.trim().parse::<u64>().ok()?, None)),
    }
}

/// Wrap a bookmark + sequence into the exactly-once state value.
pub fn wrap_state(bookmark: Option<&Value>, seq: u64) -> Value {
    serde_json::json!({
        EO_MARKER: 1,
        EO_BOOKMARK: bookmark.cloned().unwrap_or(Value::Null),
        EO_SEQ: seq,
    })
}

/// Unwrap a stored state value into `(bookmark, seq)`.
///
/// A value that is the exactly-once wrapper object unwraps to its inner
/// bookmark + seq. Anything else is treated as a legacy/at-least-once **bare
/// bookmark** with `seq = 0` — so switching an existing pipeline to
/// `exactly_once` resumes cleanly (the sink's own watermark is authoritative).
pub fn unwrap_state(value: &Value) -> (Option<Value>, u64) {
    if let Value::Object(map) = value
        && map.get(EO_MARKER).and_then(Value::as_u64) == Some(1)
    {
        let bookmark = match map.get(EO_BOOKMARK) {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.clone()),
        };
        let seq = map.get(EO_SEQ).and_then(Value::as_u64).unwrap_or(0);
        return (bookmark, seq);
    }
    // Legacy bare bookmark.
    let bookmark = if value.is_null() {
        None
    } else {
        Some(value.clone())
    };
    (bookmark, 0)
}

/// Canonical watermark table the SQL sinks UPSERT the commit token into.
pub const COMMIT_TOKEN_TABLE: &str = "_faucet_commit_token";
/// Watermark column holding the pipeline state-key (`{name}::{row_id}`).
pub const COMMIT_TOKEN_SCOPE_COL: &str = "scope";
/// Watermark column holding the latest committed token.
pub const COMMIT_TOKEN_TOKEN_COL: &str = "token";

/// Iceberg snapshot summary property names.
pub const ICEBERG_SCOPE_PROP: &str = "faucet.commit-scope";
pub const ICEBERG_TOKEN_PROP: &str = "faucet.commit-token";

/// Suffix appended to a destination's name to form the staging relation an
/// in-flight `write_mode: overwrite` run writes into.
///
/// Reserved: the overwrite lifecycle creates, truncates, and **drops** whatever
/// carries this suffix, so a real destination named `<x>__faucet_ovw` would be
/// destroyed by `abort_overwrite`. Every sink derives its staging name from this
/// one constant so the reserved set stays enumerable by a collision check or an
/// orphan sweep (#654 M12).
pub const OVERWRITE_STAGING_SUFFIX: &str = "__faucet_ovw";
/// Suffix of the transient relation the *outgoing* destination is renamed to
/// mid-swap, on backends whose only atomic publish is a rename (MySQL, whose
/// DDL auto-commits so a transaction cannot span the swap).
///
/// Extends [`OVERWRITE_STAGING_SUFFIX`], so a sweep matching the base suffix as
/// a prefix catches this one too.
pub const OVERWRITE_STAGING_OLD_SUFFIX: &str = "__faucet_ovw_old";

/// Fit a watermark scope into a length-capped, indexable key column.
///
/// The scope is the pipeline state key — `{name}::{row}` for a root, plus
/// `::{parent_record_key}` for a child — and a child's key comes from *record
/// data*, so it has no length bound. The SQL sinks store it as a PRIMARY KEY, and
/// a key column cannot be unbounded (MySQL's index limit, SQL Server's 900-byte
/// key budget), so an over-long scope either errors or — under a non-strict MySQL
/// `sql_mode` — is **truncated**, silently collapsing two distinct rows onto one
/// watermark so one row's committed token suppresses the other's pages (#456 L1).
///
/// Scopes at or under `max` are returned verbatim, so every watermark written
/// before this existed still resolves. A longer one is replaced by a
/// deterministic, collision-resistant digest form (`__h:<64-hex>`), which is
/// stable across restarts — the only property the watermark needs.
/// Length of the `__h:` + 16 hex-digit suffix appended to a shortened scope.
const SCOPE_DIGEST_LEN: usize = 4 + 16;

pub fn scope_key(scope: &str, max: usize) -> String {
    if scope.len() <= max {
        return scope.to_owned();
    }
    // FNV-1a rather than a crypto hash: `sha2` is an optional dependency of this
    // crate (masking / transform-hash / encryption) and this module is always
    // compiled. The requirement is determinism, not preimage resistance — the same
    // choice the backfill progress marker makes.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in scope.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    // Keep as much of the readable head as fits, so an operator inspecting the
    // watermark table can still tell which pipeline a row belongs to. Truncate on
    // a char boundary — a scope may hold non-ASCII.
    let room = max.saturating_sub(SCOPE_DIGEST_LEN);
    let mut head = 0usize;
    for (i, _) in scope.char_indices() {
        if i > room {
            break;
        }
        head = i;
    }
    let shortened = format!("{}__h:{h:016x}", &scope[..head]);
    tracing::debug!(
        scope_len = scope.len(),
        max,
        "exactly-once scope exceeds the sink's key-column width; shortening with a digest"
    );
    shortened
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn token_round_trips_and_orders_lexicographically() {
        assert_eq!(format_token(42).len(), TOKEN_WIDTH);
        assert_eq!(parse_token(&format_token(42)), Some(42));
        assert_eq!(parse_token(&format_token(0)), Some(0));
        assert_eq!(parse_token(&format_token(u64::MAX)), Some(u64::MAX));
        assert!(format_token(9) < format_token(10));
        assert!(format_token(2) < format_token(1000));
    }

    #[test]
    fn parse_token_rejects_garbage() {
        assert_eq!(parse_token("abc"), None);
        assert_eq!(parse_token(""), None);
    }

    #[test]
    fn wrap_then_unwrap_preserves_bookmark_and_seq() {
        let bm = json!({"lsn": "0/16B2D58"});
        let wrapped = wrap_state(Some(&bm), 7);
        let (got_bm, got_seq) = unwrap_state(&wrapped);
        assert_eq!(got_bm, Some(bm));
        assert_eq!(got_seq, 7);
    }

    #[test]
    fn wrap_none_bookmark_unwraps_to_none() {
        let wrapped = wrap_state(None, 3);
        let (got_bm, got_seq) = unwrap_state(&wrapped);
        assert_eq!(got_bm, None);
        assert_eq!(got_seq, 3);
    }

    #[test]
    fn legacy_bare_bookmark_unwraps_with_seq_zero() {
        let (bm, seq) = unwrap_state(&json!("2024-12-01"));
        assert_eq!(bm, Some(json!("2024-12-01")));
        assert_eq!(seq, 0);
        let (bm2, seq2) = unwrap_state(&json!({"updated_at": "2024-12-01"}));
        assert_eq!(bm2, Some(json!({"updated_at": "2024-12-01"})));
        assert_eq!(seq2, 0);
    }

    #[test]
    fn object_with_non_sentinel_marker_is_treated_as_bare_bookmark() {
        // A legacy/user object that merely contains the key must NOT be misread
        // as an EO wrapper — only the typed sentinel `1` counts.
        let v = json!({"__faucet_eo": null, "offset": 500});
        let (bm, seq) = unwrap_state(&v);
        assert_eq!(bm, Some(v));
        assert_eq!(seq, 0);
    }

    #[test]
    fn null_value_unwraps_to_none_seq_zero() {
        let (bm, seq) = unwrap_state(&json!(null));
        assert_eq!(bm, None);
        assert_eq!(seq, 0);
    }

    #[test]
    fn token_with_bookmark_round_trips() {
        let bm = json!({"partition_offsets": [{"topic": "t", "partition": 0, "offset": 42}]});
        let token = format_token_with_bookmark(7, Some(&bm));
        assert!(token.starts_with(&format_token(7)));
        assert_eq!(parse_token(&token), Some(7));
        let (seq, parsed_bm) = parse_token_parts(&token).unwrap();
        assert_eq!(seq, 7);
        assert_eq!(parsed_bm, Some(bm));
    }

    #[test]
    fn token_with_no_bookmark_is_bare_and_back_compatible() {
        assert_eq!(format_token_with_bookmark(3, None), format_token(3));
        let (seq, bm) = parse_token_parts(&format_token(3)).unwrap();
        assert_eq!((seq, bm), (3, None));
    }

    #[test]
    fn token_with_bookmark_orders_lexicographically_on_prefix() {
        // The fixed-width numeric prefix keeps lexicographic order meaningful
        // even with an embedded bookmark (kafka side-topic folding compares
        // parsed sequences, but SQL MAX() naturally works too).
        let a = format_token_with_bookmark(9, Some(&json!({"o": 1})));
        let b = format_token_with_bookmark(10, Some(&json!({"o": 2})));
        assert!(a < b);
    }

    #[test]
    fn parse_token_parts_tolerates_garbage() {
        assert_eq!(parse_token_parts("abc"), None);
        assert_eq!(parse_token_parts(""), None);
        // Bad JSON suffix: sequence survives, bookmark is dropped.
        let (seq, bm) = parse_token_parts("00000000000000000005#{not json").unwrap();
        assert_eq!((seq, bm), (5, None));
        // parse_token ignores the suffix entirely.
        assert_eq!(parse_token("00000000000000000005#{not json"), Some(5));
    }

    #[test]
    fn derive_guarantee_prefers_atomic_then_keyed_then_at_least_once() {
        use ReplayGuarantee::*;
        let base = GuaranteeInputs {
            replay: Deterministic,
            sink_atomic: true,
            keyed_upsert_configured: false,
            durable_state: true,
            dlq: false,
        };
        assert_eq!(
            derive_delivery_guarantee(&base),
            DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::AtomicWatermark)
        );
        // Atomic path degrades without deterministic replay…
        let non_det = GuaranteeInputs {
            replay: NonDeterministic,
            ..base
        };
        assert_eq!(
            derive_delivery_guarantee(&non_det),
            DeliveryGuarantee::AtLeastOnce
        );
        // …but keyed upsert rescues it, source-independent.
        let keyed = GuaranteeInputs {
            keyed_upsert_configured: true,
            ..non_det
        };
        assert_eq!(
            derive_delivery_guarantee(&keyed),
            DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert)
        );
        // A DLQ or missing durable state disables atomic; keyed still applies.
        let dlq = GuaranteeInputs {
            dlq: true,
            keyed_upsert_configured: true,
            ..base
        };
        assert_eq!(
            derive_delivery_guarantee(&dlq),
            DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert)
        );
        let mem_state = GuaranteeInputs {
            durable_state: false,
            ..base
        };
        assert_eq!(
            derive_delivery_guarantee(&mem_state),
            DeliveryGuarantee::AtLeastOnce
        );
    }

    #[test]
    fn guarantee_display_is_human_readable() {
        assert_eq!(DeliveryGuarantee::AtLeastOnce.to_string(), "at-least-once");
        assert_eq!(
            DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::AtomicWatermark)
                .to_string(),
            "effectively-once (atomic watermark)"
        );
        assert_eq!(
            DeliveryGuarantee::EffectivelyOnce(EffectivelyOnceMechanism::KeyedUpsert).to_string(),
            "effectively-once (keyed upsert)"
        );
    }

    #[test]
    fn capability_enums_default_to_weakest() {
        assert_eq!(
            ReplayGuarantee::default(),
            ReplayGuarantee::NonDeterministic
        );
        assert_eq!(SinkGuarantee::default(), SinkGuarantee::AtLeastOnce);
    }

    #[test]
    fn delivery_mode_serde_is_snake_case_and_defaults_at_least_once() {
        assert_eq!(DeliveryMode::default(), DeliveryMode::AtLeastOnce);
        assert_eq!(
            serde_json::to_string(&DeliveryMode::ExactlyOnce).unwrap(),
            "\"exactly_once\""
        );
        let m: DeliveryMode = serde_json::from_str("\"at_least_once\"").unwrap();
        assert_eq!(m, DeliveryMode::AtLeastOnce);
    }

    #[test]
    fn overwrite_staging_suffixes_are_pinned() {
        // These are on-the-wire relation names: a live destination staged under
        // the old spelling would be stranded (and the new spelling's relation
        // dropped) if either value ever moved.
        assert_eq!(OVERWRITE_STAGING_SUFFIX, "__faucet_ovw");
        assert_eq!(OVERWRITE_STAGING_OLD_SUFFIX, "__faucet_ovw_old");
        assert_ne!(OVERWRITE_STAGING_SUFFIX, OVERWRITE_STAGING_OLD_SUFFIX);
        // An orphan sweep matching the base suffix as a prefix must also catch
        // the mid-swap relation.
        assert!(OVERWRITE_STAGING_OLD_SUFFIX.starts_with(OVERWRITE_STAGING_SUFFIX));
    }
}

#[cfg(test)]
mod scope_key_tests {
    use super::*;

    /// #456 L1: the SQL sinks store the scope as a length-capped PRIMARY KEY, so
    /// a long child scope (its key comes from record data and has no bound) either
    /// errored or — under a non-strict MySQL sql_mode — truncated, collapsing two
    /// rows onto one watermark.
    #[test]
    fn scope_key_passes_short_scopes_through_and_shortens_long_ones() {
        // Backwards compatible: anything that fit before is returned verbatim, so
        // watermarks written before this existed still resolve.
        assert_eq!(scope_key("pipe::row", 255), "pipe::row");
        let exactly = "x".repeat(255);
        assert_eq!(scope_key(&exactly, 255), exactly);

        // Over the cap: shortened, and within the cap.
        let long = format!("pipe::row::{}", "k".repeat(400));
        let key = scope_key(&long, 255);
        assert!(key.len() <= 255, "len {}", key.len());
        assert_ne!(key, long);
        // Keeps a readable head so the row is still attributable.
        assert!(key.starts_with("pipe::row::"), "{key}");
        assert!(key.contains("__h:"), "{key}");
    }

    #[test]
    fn scope_key_is_deterministic_and_distinguishes_scopes() {
        let a = format!("pipe::row::{}", "a".repeat(400));
        let b = format!("pipe::row::{}", "b".repeat(400));
        // Stable across calls — the watermark must resolve after a restart.
        assert_eq!(scope_key(&a, 255), scope_key(&a, 255));
        // Two distinct scopes must not collide onto one watermark. Under plain
        // truncation both of these would become the same 255-char prefix.
        assert_ne!(scope_key(&a, 255), scope_key(&b, 255));
        assert_eq!(&a[..255], &format!("pipe::row::{}", "a".repeat(400))[..255]);
    }

    #[test]
    fn scope_key_truncates_on_a_char_boundary() {
        // A multi-byte head must not be split mid-character (that would panic).
        let long = format!("pipé::{}", "é".repeat(400));
        let key = scope_key(&long, 255);
        assert!(key.len() <= 255);
        assert!(key.contains("__h:"), "{key}");
    }
}

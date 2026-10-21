//! Source sharding for clustered (Mode B) execution.
//!
//! A *shardable* source can split its work into independent [`ShardSpec`]s that
//! different cluster workers process concurrently — so one logical pipeline over
//! a single large source (a big S3 prefix, a billion-row table) scales
//! horizontally instead of being capped at one worker's throughput.
//!
//! The capability is exposed as **defaulted methods on the
//! [`Source`](crate::Source) trait**
//! ([`enumerate_shards`](crate::Source::enumerate_shards),
//! [`apply_shard`](crate::Source::apply_shard),
//! [`is_shardable`](crate::Source::is_shardable)) rather than a separate trait,
//! so it composes with the existing `Box<dyn Source>` object model with no
//! downcast and stays fully backward compatible: a source that does not override
//! them reports `is_shardable() == false` and enumerates to a single
//! whole-dataset shard, i.e. it behaves exactly as today.
//!
//! ## Lifecycle
//!
//! 1. A coordinator calls [`enumerate_shards`](crate::Source::enumerate_shards)
//!    once per run (idempotently) to discover the shard set.
//! 2. Each shard is persisted and claimed by exactly one worker.
//! 3. The claiming worker calls [`apply_shard`](crate::Source::apply_shard) to
//!    narrow its source instance to that one shard before streaming.
//! 4. Each shard carries its own state-key suffix so resume is per-shard: a
//!    reassigned shard resumes where its dead owner left off.

use serde::{Deserialize, Serialize};

/// One independently-processable slice of a shardable source.
///
/// The [`descriptor`](ShardSpec::descriptor) is opaque to the coordinator — only
/// the producing/consuming connector interprets it (e.g. `{"pk_range":[0,1000]}`
/// for a key-range split, `{"prefix":"dt=2026-06-11/"}` for an object split). It
/// must round-trip through JSON unchanged so the coordinator can persist it and
/// hand it back to [`apply_shard`](crate::Source::apply_shard) on the worker that
/// claims the shard.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardSpec {
    /// Stable identifier, unique within a run's shard set. Also used as the
    /// per-shard state-key suffix (`{run}::{shard.id}`), so it must be a valid
    /// state-key segment (no `:` — see
    /// [`validate_state_key`](crate::state::validate_state_key)).
    pub id: String,

    /// Connector-specific, opaque shard descriptor. Persisted verbatim and
    /// passed back to [`apply_shard`](crate::Source::apply_shard).
    pub descriptor: serde_json::Value,

    /// Optional relative size estimate (rows / bytes / objects — the unit is the
    /// connector's choice, only comparisons within one run's set matter) used
    /// for skew-aware assignment. `None` when the source cannot cheaply
    /// estimate; the coordinator then falls back to count-based balancing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_estimate: Option<u64>,
}

impl ShardSpec {
    /// The single whole-dataset shard a non-shardable source enumerates to.
    ///
    /// Its [`id`](ShardSpec::id) is `"0"` and its descriptor is JSON `null`;
    /// [`apply_shard`](crate::Source::apply_shard)'s default ignores it, so the
    /// source streams its entire dataset unchanged.
    pub fn whole() -> Self {
        Self {
            id: "0".to_string(),
            descriptor: serde_json::Value::Null,
            size_estimate: None,
        }
    }

    /// A shard with the given id and descriptor and no size estimate.
    pub fn new(id: impl Into<String>, descriptor: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            descriptor,
            size_estimate: None,
        }
    }

    /// Attach a relative size estimate (builder style).
    pub fn with_size(mut self, size: u64) -> Self {
        self.size_estimate = Some(size);
        self
    }

    /// Whether this is the whole-dataset shard (id `"0"`, null descriptor) — the
    /// no-op shard a non-shardable source produces.
    pub fn is_whole(&self) -> bool {
        self.id == "0" && self.descriptor.is_null()
    }
}

// ── PK-range sharding (shared by the SQL query sources) ─────────────────────

/// The most shards one source is ever split into. A planner caps its target
/// here, so a mistyped (or hostile) `shard.count` cannot make the coordinator
/// allocate and persist millions of shard rows.
pub const MAX_SHARDS: usize = 4096;

/// Split the integer range observed as `[min, max]` into up to `target`
/// contiguous shards, each described by
/// `{key, lo, hi, lo_unbounded, hi_unbounded, include_null}`. Interior cut
/// points are half-open `[lo, hi)`, but the **boundary shards are open-ended**:
/// the first shard has no lower bound and the last shard has no upper bound, so
/// the union of all shards tiles `(-∞, +∞)`.
///
/// Open-ended boundaries match unsharded semantics: `min`/`max` are captured
/// once at enumeration time, but rows can be inserted below `min` or above `max`
/// (or backfilled) before the workers actually stream their shards. Clamping the
/// boundary shards to the captured `[min, max]` would silently drop those rows
/// (audit F54/F55); leaving them open captures everything.
///
/// The last shard also carries `include_null: true` so that NULL-key rows —
/// invisible to the `MIN`/`MAX` enumeration that produced `[min, max]` — are
/// read by exactly one shard (no loss, no duplication; audit F37). Picking the
/// *last* shard means a single-shard plan still covers NULLs.
///
/// Coverage scheme (proven by `predicate_coverage_*` tests):
/// - non-NULL keys: the first shard owns `key < cut₁`, interior shards own
///   `[cutᵢ, cutᵢ₊₁)`, and the last shard owns `key >= cutₙ` — every value
///   (including below `min` and above `max`) falls in exactly one shard;
/// - NULL keys: matched only by the last shard's `OR key IS NULL` clause —
///   exactly one shard.
///
/// Pure function (no I/O) so it is unit-testable without a database. Shared by
/// every PK-range-sharded SQL source (postgres, mysql, mssql, sqlite) so the
/// subtle boundary/NULL coverage logic lives in exactly one place.
pub fn plan_pk_shards(key: &str, min: i64, max: i64, target: usize) -> Vec<ShardSpec> {
    let target = target.clamp(1, MAX_SHARDS);
    // Range width as u128 to avoid i64 overflow on full-range PKs.
    let width = (max as i128 - min as i128 + 1).max(1) as u128;
    let n = (target as u128).min(width) as usize; // never more shards than values
    let step = width.div_ceil(n as u128); // ceil so shards cover the whole range

    let mut shards = Vec::with_capacity(n);
    let mut lo = min as i128;
    for i in 0..n {
        let mut hi = lo + step as i128;
        let is_first = i == 0;
        let is_last = i == n - 1;
        if is_last || hi > max as i128 {
            hi = max as i128; // last interior cut closes at max (the last shard
            // is unbounded above, so `hi` is unused there)
        }
        let descriptor = serde_json::json!({
            "key": key,
            "lo": lo as i64,
            "hi": hi as i64,
            // Boundary shards are open-ended so the union tiles (-∞, +∞).
            "lo_unbounded": is_first,
            "hi_unbounded": is_last,
            // Exactly one shard (the last) owns the NULL-key rows.
            "include_null": is_last,
        });
        let size = (hi - lo).max(0) as u64 + if is_last { 1 } else { 0 };
        shards.push(ShardSpec::new(i.to_string(), descriptor).with_size(size));
        if is_last {
            break;
        }
        lo = hi;
    }
    shards
}

/// Parsed integer range bounds for an applied PK-range shard, produced by
/// [`plan_pk_shards`] and applied by a SQL source's
/// [`apply_shard`](crate::Source::apply_shard).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkShardBounds {
    /// The shard-key column name, **unquoted** — the consuming dialect quotes
    /// it via the closure handed to [`wrap`](Self::wrap).
    pub key: String,
    /// Inclusive lower cut point (unused when `lo_unbounded`).
    pub lo: i64,
    /// Exclusive upper cut point (unused when `hi_unbounded`).
    pub hi: i64,
    /// When `true` this shard has **no lower bound** — it is the *first* shard,
    /// so it owns every key below `hi` including any below the enumerated `MIN`.
    /// Rows backfilled or inserted with smaller ids during the run are read by
    /// this shard instead of being silently dropped outside `[MIN, MAX]` (F54).
    pub lo_unbounded: bool,
    /// When `true` this shard has **no upper bound** — it is the *last* shard,
    /// so it owns every key at/above `lo` including any above the enumerated
    /// `MAX`. Rows appended above the captured `MAX` between coordination and
    /// shard execution are read by this shard instead of being lost (F55).
    pub hi_unbounded: bool,
    /// When `true` this shard *additionally* matches rows whose `key` is NULL.
    ///
    /// SQL aggregates (`MIN`/`MAX`) ignore NULLs, so a nullable shard key never
    /// produces a `[lo, hi]` range covering NULL-key rows — without this flag
    /// every sharded run would silently drop them (audit F37). Exactly one
    /// shard (the last) carries this flag, so NULL-key rows are read by
    /// precisely one shard: no loss, no duplication.
    pub include_null: bool,
}

impl PkShardBounds {
    /// Parse from a [`ShardSpec`] descriptor produced by [`plan_pk_shards`].
    /// Returns `None` for a malformed descriptor (missing/ill-typed
    /// `key`/`lo`/`hi`) — including the whole-dataset shard's `null`
    /// descriptor, so callers must check [`ShardSpec::is_whole`] first.
    pub fn from_spec(spec: &ShardSpec) -> Option<Self> {
        let d = &spec.descriptor;
        Some(Self {
            key: d.get("key")?.as_str()?.to_string(),
            lo: d.get("lo")?.as_i64()?,
            hi: d.get("hi")?.as_i64()?,
            lo_unbounded: d
                .get("lo_unbounded")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            hi_unbounded: d
                .get("hi_unbounded")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
            include_null: d
                .get("include_null")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        })
    }

    /// Wrap `inner` so only rows whose `key` falls in this shard's range are
    /// returned: `SELECT * FROM (<inner>) AS _faucet_shard WHERE <predicate>`.
    ///
    /// `quote_ident` is the consuming dialect's identifier quoting (ANSI
    /// double quotes for postgres/sqlite, backticks for mysql, brackets for
    /// mssql) — quoting the key makes it injection-safe. The bounds are
    /// inlined as integer literals (safe — they are `i64`s produced by
    /// enumeration).
    ///
    /// The boundary shards are **open-ended** (`lo_unbounded` / `hi_unbounded`)
    /// so the union of all shards tiles `(-∞, +∞)`, matching unsharded
    /// semantics — no row is dropped for sorting outside the `[MIN, MAX]`
    /// captured at enumeration time (F54/F55). The single shard with
    /// `include_null` also matches `key IS NULL` so NULL-key rows (invisible to
    /// the `MIN`/`MAX` enumeration) are still read.
    pub fn wrap(&self, inner: &str, quote_ident: impl Fn(&str) -> String) -> String {
        let key = quote_ident(&self.key);
        let mut parts: Vec<String> = Vec::with_capacity(2);
        if !self.lo_unbounded {
            parts.push(format!("{key} >= {lo}", lo = self.lo));
        }
        if !self.hi_unbounded {
            parts.push(format!("{key} < {hi}", hi = self.hi));
        }
        let range = parts.join(" AND ");
        let predicate = if self.include_null {
            if range.is_empty() {
                // A single fully-unbounded shard owns the whole dataset,
                // NULL-key rows included.
                "1=1".to_string()
            } else {
                // Parenthesize so the OR binds correctly inside the WHERE clause.
                format!("(({range}) OR {key} IS NULL)")
            }
        } else if range.is_empty() {
            "1=1".to_string()
        } else {
            range
        };
        format!("SELECT * FROM ({inner}) AS _faucet_shard WHERE {predicate}")
    }
}

/// Parse + validate an applied shard for a PK-range source.
///
/// Returns `Ok(None)` for the whole-dataset shard (the source should clear any
/// narrowing and stream everything) and a typed [`FaucetError::Source`](crate::FaucetError::Source) naming
/// `connector` for a malformed descriptor. The one-line body every SQL
/// source's [`apply_shard`](crate::Source::apply_shard) delegates to.
pub fn parse_pk_shard(
    shard: &ShardSpec,
    connector: &str,
) -> Result<Option<PkShardBounds>, crate::FaucetError> {
    if shard.is_whole() {
        return Ok(None);
    }
    PkShardBounds::from_spec(shard).map(Some).ok_or_else(|| {
        crate::FaucetError::Source(format!(
            "{connector}: invalid shard descriptor: {}",
            shard.descriptor
        ))
    })
}

/// Build the `MIN`/`MAX` bounds probe a PK-range enumeration runs once:
/// `SELECT CAST(MIN(<key>) AS <int_cast>) AS lo, CAST(MAX(<key>) AS <int_cast>) AS hi
/// FROM (<inner>) AS _faucet_bounds`.
///
/// `quoted_key` must already be dialect-quoted; `int_cast` is the dialect's
/// 64-bit integer cast target (`BIGINT` for postgres/mssql, `SIGNED` for
/// mysql, `INTEGER` for sqlite).
pub fn pk_bounds_query(inner: &str, quoted_key: &str, int_cast: &str) -> String {
    format!(
        "SELECT CAST(MIN({quoted_key}) AS {int_cast}) AS lo, \
         CAST(MAX({quoted_key}) AS {int_cast}) AS hi \
         FROM ({inner}) AS _faucet_bounds"
    )
}

/// Turn the decoded output of a [`pk_bounds_query`] probe into a shard plan: a
/// populated range plans PK-range shards via [`plan_pk_shards`]; an empty
/// result set (`MIN`/`MAX` are NULL) degrades to one whole-dataset shard.
pub fn pk_shards_from_bounds(
    key: &str,
    lo: Option<i64>,
    hi: Option<i64>,
    target: usize,
) -> Vec<ShardSpec> {
    match (lo, hi) {
        (Some(lo), Some(hi)) => plan_pk_shards(key, lo, hi, target),
        _ => vec![ShardSpec::whole()],
    }
}

// ── Hash-of-key sharding (shared by the object-store / file sources) ────────

/// Stable FNV-1a hash of an object key / file path, used to assign keys to
/// shards.
///
/// Deterministic across processes and platforms (all cluster workers run the
/// identical binary and this fixed algorithm), so every worker maps a given key
/// to the same shard index — the partition is disjoint and complete.
pub fn shard_hash(key: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Enumerate `target` hash-modulo shards, each described by
/// `{shards, index}`. Shard `i` owns the keys whose [`shard_hash`] is
/// `i (mod target)`. No I/O: the partition is defined by the hash function,
/// so enumeration is cheap and stays stable as new objects appear.
/// `target <= 1` yields a single whole-dataset shard.
pub fn plan_hash_shards(target: usize) -> Vec<ShardSpec> {
    let target = target.min(MAX_SHARDS);
    if target <= 1 {
        return vec![ShardSpec::whole()];
    }
    (0..target)
        .map(|i| {
            ShardSpec::new(
                i.to_string(),
                serde_json::json!({ "shards": target, "index": i }),
            )
        })
        .collect()
}

/// Parsed `{shards, index}` hash-modulo shard descriptor, produced by
/// [`plan_hash_shards`] and applied by an object-store / file source's
/// [`apply_shard`](crate::Source::apply_shard).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HashShard {
    /// Total shard count in this run's set.
    pub shards: usize,
    /// This shard's index in `0..shards`.
    pub index: usize,
}

impl HashShard {
    /// Parse from a [`ShardSpec`] descriptor produced by [`plan_hash_shards`].
    /// Returns `None` for a malformed descriptor (missing/ill-typed `shards`
    /// or `index`) — including the whole-dataset shard's `null` descriptor,
    /// so callers must check [`ShardSpec::is_whole`] first.
    pub fn from_spec(spec: &ShardSpec) -> Option<Self> {
        let d = &spec.descriptor;
        Some(Self {
            shards: d.get("shards")?.as_u64()? as usize,
            index: d.get("index")?.as_u64()? as usize,
        })
    }

    /// Whether `key` belongs to this shard. A degenerate set of `<= 1` shards
    /// owns every key.
    pub fn contains(&self, key: &str) -> bool {
        self.shards <= 1 || (shard_hash(key) % self.shards as u64) == self.index as u64
    }
}

/// Parse + validate an applied shard for a hash-of-key source.
///
/// Returns `Ok(None)` for the whole-dataset shard (the source should clear any
/// filter and read everything) and a typed [`FaucetError::Source`](crate::FaucetError::Source) naming
/// `connector` for a malformed descriptor. The one-line body every
/// object-store / file source's [`apply_shard`](crate::Source::apply_shard)
/// delegates to.
pub fn parse_hash_shard(
    shard: &ShardSpec,
    connector: &str,
) -> Result<Option<HashShard>, crate::FaucetError> {
    if shard.is_whole() {
        return Ok(None);
    }
    HashShard::from_spec(shard).map(Some).ok_or_else(|| {
        crate::FaucetError::Source(format!(
            "{connector}: invalid shard descriptor: {}",
            shard.descriptor
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn whole_is_the_no_op_shard() {
        let w = ShardSpec::whole();
        assert_eq!(w.id, "0");
        assert!(w.descriptor.is_null());
        assert_eq!(w.size_estimate, None);
        assert!(w.is_whole());
    }

    #[test]
    fn new_and_with_size() {
        let s = ShardSpec::new("3", json!({ "partition": 3 })).with_size(42);
        assert_eq!(s.id, "3");
        assert_eq!(s.descriptor, json!({ "partition": 3 }));
        assert_eq!(s.size_estimate, Some(42));
        assert!(!s.is_whole(), "a real shard is not the whole-dataset shard");
    }

    #[test]
    fn round_trips_through_json_with_size_omitted_when_none() {
        let s = ShardSpec::new("a", json!({ "prefix": "dt=2026-06-11/" }));
        let text = serde_json::to_string(&s).unwrap();
        // size_estimate is omitted from the wire form when None.
        assert!(
            !text.contains("size_estimate"),
            "None size is skipped: {text}"
        );
        let back: ShardSpec = serde_json::from_str(&text).unwrap();
        assert_eq!(back, s);
    }

    #[test]
    fn round_trips_with_size_present() {
        let s = ShardSpec::new("b", json!({ "pk_range": [0, 1000] })).with_size(1000);
        let back: ShardSpec = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back, s);
        assert_eq!(back.size_estimate, Some(1000));
    }

    // A non-whole shard with id "0" but a non-null descriptor is NOT treated as
    // the whole-dataset shard (the descriptor disambiguates).
    #[test]
    fn id_zero_with_descriptor_is_not_whole() {
        let s = ShardSpec::new("0", json!({ "partition": 0 }));
        assert!(!s.is_whole());
    }

    // ── PK-range planning ────────────────────────────────────────────────────

    /// ANSI double-quote identifier quoting, matching
    /// `faucet_core::util::quote_ident` (not imported to keep this module
    /// self-contained).
    fn ansi_quote(name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    #[test]
    fn plan_pk_shards_covers_full_range_without_gaps_or_overlap() {
        let shards = plan_pk_shards("id", 0, 99, 4);
        assert_eq!(shards.len(), 4);
        // Contiguous half-open interior cuts; boundary shards are open-ended.
        let mut expected_lo = 0i64;
        for (i, s) in shards.iter().enumerate() {
            let d = &s.descriptor;
            assert_eq!(d["key"], "id");
            assert_eq!(d["lo"].as_i64().unwrap(), expected_lo);
            let hi = d["hi"].as_i64().unwrap();
            let first = i == 0;
            let last = i == shards.len() - 1;
            assert_eq!(d["lo_unbounded"].as_bool().unwrap(), first);
            assert_eq!(d["hi_unbounded"].as_bool().unwrap(), last);
            expected_lo = hi; // next shard starts where this half-open one ended
        }
    }

    #[test]
    fn plan_pk_shards_never_more_shards_than_values() {
        // Range [5, 7] has 3 values; asking for 10 shards yields at most 3.
        let shards = plan_pk_shards("pk", 5, 7, 10);
        assert!(shards.len() <= 3, "got {} shards", shards.len());
        assert!(
            shards[0].descriptor["lo_unbounded"].as_bool().unwrap(),
            "first shard is unbounded below"
        );
        assert!(
            shards.last().unwrap().descriptor["hi_unbounded"]
                .as_bool()
                .unwrap(),
            "last shard is unbounded above"
        );
    }

    #[test]
    fn plan_pk_shards_single_value_one_shard() {
        let shards = plan_pk_shards("id", 42, 42, 8);
        assert_eq!(shards.len(), 1);
        // A lone shard is open-ended on both sides → the whole dataset.
        assert!(shards[0].descriptor["lo_unbounded"].as_bool().unwrap());
        assert!(shards[0].descriptor["hi_unbounded"].as_bool().unwrap());
    }

    #[test]
    fn plan_pk_shards_target_zero_treated_as_one() {
        let shards = plan_pk_shards("id", 0, 9, 0);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].descriptor["hi"].as_i64().unwrap(), 9);
    }

    #[test]
    fn plan_pk_shards_full_i64_range_does_not_overflow() {
        // A full-range key must not overflow the i128 width computation.
        let shards = plan_pk_shards("id", i64::MIN, i64::MAX, 4);
        assert_eq!(shards.len(), 4);
        assert!(shards[0].descriptor["lo_unbounded"].as_bool().unwrap());
        assert!(
            shards.last().unwrap().descriptor["hi_unbounded"]
                .as_bool()
                .unwrap()
        );
    }

    // ── PK-range bounds / predicate generation ──────────────────────────────

    #[test]
    fn pk_bounds_wrap_builds_half_open_predicate() {
        // An interior shard (bounded both sides) is half-open `[lo, hi)`.
        let spec = ShardSpec::new(
            "1",
            json!({"key": "id", "lo": 100, "hi": 200, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let b = PkShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t", ansi_quote);
        assert!(sql.contains("(SELECT * FROM t) AS _faucet_shard"));
        assert!(sql.contains(r#""id" >= 100"#), "got: {sql}");
        assert!(
            sql.contains(r#""id" < 200"#),
            "half-open upper bound: {sql}"
        );
    }

    #[test]
    fn pk_bounds_wrap_first_shard_has_no_lower_bound() {
        // F54: the first shard omits the `>= lo` floor so keys below the
        // enumerated MIN are still read.
        let spec = ShardSpec::new(
            "0",
            json!({"key": "id", "lo": 0, "hi": 100, "lo_unbounded": true, "hi_unbounded": false}),
        );
        let b = PkShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t", ansi_quote);
        assert!(sql.contains(r#""id" < 100"#), "upper bound present: {sql}");
        assert!(!sql.contains(">="), "first shard has no lower floor: {sql}");
    }

    #[test]
    fn pk_bounds_wrap_last_shard_has_no_upper_bound() {
        // F55: the last shard omits the upper bound so keys above the
        // enumerated MAX are still read.
        let spec = ShardSpec::new(
            "2",
            json!({"key": "id", "lo": 200, "hi": 300, "lo_unbounded": false, "hi_unbounded": true}),
        );
        let b = PkShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t", ansi_quote);
        assert!(sql.contains(r#""id" >= 200"#), "lower bound present: {sql}");
        assert!(
            !sql.contains(" < ") && !sql.contains("<="),
            "last shard has no upper bound: {sql}"
        );
    }

    #[test]
    fn pk_bounds_wrap_uses_the_supplied_dialect_quoting() {
        let spec = ShardSpec::new(
            "0",
            json!({"key": "id", "lo": 0, "hi": 1, "lo_unbounded": false, "hi_unbounded": false}),
        );
        let b = PkShardBounds::from_spec(&spec).unwrap();
        let backtick = b.wrap("SELECT 1", |k| format!("`{k}`"));
        assert!(backtick.contains("`id` >= 0"), "got: {backtick}");
        let bracket = b.wrap("SELECT 1", |k| format!("[{k}]"));
        assert!(bracket.contains("[id] >= 0"), "got: {bracket}");
    }

    #[test]
    fn pk_bounds_from_spec_rejects_malformed_descriptor() {
        let spec = ShardSpec::new("0", json!({"key": "id"})); // no lo/hi
        assert!(PkShardBounds::from_spec(&spec).is_none());
        assert!(PkShardBounds::from_spec(&ShardSpec::whole()).is_none());
    }

    // ── F37: NULL-key shard coverage ────────────────────────────────────────

    #[test]
    fn exactly_one_shard_includes_null() {
        let shards = plan_pk_shards("id", 0, 99, 5);
        let null_owners: Vec<usize> = shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.descriptor["include_null"].as_bool().unwrap_or(false))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            null_owners,
            vec![shards.len() - 1],
            "exactly the last shard owns NULL keys"
        );
    }

    #[test]
    fn single_shard_plan_still_owns_null() {
        // A single value yields one shard; it must still cover NULL keys.
        let shards = plan_pk_shards("id", 7, 7, 4);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].descriptor["include_null"].as_bool().unwrap());
    }

    #[test]
    fn last_shard_wrap_emits_is_null_clause() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        let last = PkShardBounds::from_spec(shards.last().unwrap()).unwrap();
        let sql = last.wrap("SELECT * FROM t", ansi_quote);
        assert!(
            sql.contains(r#""id" IS NULL"#),
            "last shard must match NULL keys: {sql}"
        );
        assert!(sql.contains(" OR "), "NULL clause OR'd with range: {sql}");
    }

    #[test]
    fn non_last_shard_wrap_omits_is_null_clause() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        // First shard is not the last → no NULL clause.
        let first = PkShardBounds::from_spec(&shards[0]).unwrap();
        let sql = first.wrap("SELECT * FROM t", ansi_quote);
        assert!(
            !sql.contains("IS NULL"),
            "non-last shard must not match NULL keys: {sql}"
        );
    }

    /// Property check on the generated predicates: OR-ing every shard's WHERE
    /// predicate must cover (a) every non-NULL key — including values *outside*
    /// the enumerated `[min, max]` (F54/F55) — exactly once and (b) NULL keys
    /// exactly once.
    #[test]
    fn predicate_coverage_complete_and_non_overlapping() {
        let (min, max, target) = (0i64, 19i64, 4usize);
        let bounds: Vec<PkShardBounds> = plan_pk_shards("k", min, max, target)
            .iter()
            .map(|s| PkShardBounds::from_spec(s).unwrap())
            .collect();

        // The boundary shards model SQL membership: open below for the first
        // shard, open above for the last.
        let matches_key = |b: &PkShardBounds, key: i64| -> bool {
            let lower = b.lo_unbounded || key >= b.lo;
            let upper = b.hi_unbounded || key < b.hi;
            lower && upper
        };

        // (a) Every non-NULL key — well below min, in range, and well above max
        // — matches exactly one shard. Keys outside [min, max] model rows
        // inserted/backfilled during the coordinate→execute window.
        for key in (min - 50)..=(max + 50) {
            let matches = bounds.iter().filter(|b| matches_key(b, key)).count();
            assert_eq!(matches, 1, "key {key} matched {matches} shards (want 1)");
        }

        // (b) NULL keys match exactly one shard (the one with include_null).
        let null_matches = bounds.iter().filter(|b| b.include_null).count();
        assert_eq!(null_matches, 1, "NULL keys must match exactly one shard");
    }

    #[test]
    fn single_shard_wrap_selects_whole_dataset_including_null() {
        // A lone open-ended shard must select every row, NULL keys included.
        let shards = plan_pk_shards("id", 7, 7, 1);
        assert_eq!(shards.len(), 1);
        let b = PkShardBounds::from_spec(&shards[0]).unwrap();
        let sql = b.wrap("SELECT * FROM t", ansi_quote);
        assert!(sql.contains("WHERE 1=1"), "whole-dataset predicate: {sql}");
        assert!(!sql.contains(">="), "no bounds on a lone shard: {sql}");
    }

    // ── Hash-of-key sharding ────────────────────────────────────────────────

    #[test]
    fn shard_hash_is_deterministic() {
        assert_eq!(
            shard_hash("data/part-001.jsonl"),
            shard_hash("data/part-001.jsonl")
        );
        assert_ne!(shard_hash("a"), shard_hash("b"));
    }

    #[test]
    fn plan_hash_shards_returns_target_disjoint_shards() {
        let shards = plan_hash_shards(3);
        assert_eq!(shards.len(), 3);
        for (i, s) in shards.iter().enumerate() {
            assert_eq!(s.descriptor["shards"], 3);
            assert_eq!(s.descriptor["index"], i);
            assert_eq!(s.id, i.to_string());
        }
    }

    #[test]
    fn planners_never_exceed_max_shards() {
        assert_eq!(plan_hash_shards(500_000_000).len(), MAX_SHARDS);
        assert_eq!(
            plan_pk_shards("id", 0, i64::MAX, usize::MAX).len(),
            MAX_SHARDS
        );
        assert_eq!(plan_pk_shards("id", 0, 9, 0).len(), 1);
    }

    #[test]
    fn plan_hash_shards_target_one_is_whole() {
        let shards = plan_hash_shards(1);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
        let shards = plan_hash_shards(0);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
    }

    // The union of every shard's owned key set equals the full set, with no
    // key in two shards — the core no-dup / no-loss guarantee.
    #[test]
    fn hash_shards_partition_keys_disjointly_and_completely() {
        let keys: Vec<String> = (0..200).map(|i| format!("data/obj-{i}.jsonl")).collect();
        let n = 4;
        let members: Vec<HashShard> = plan_hash_shards(n)
            .iter()
            .map(|s| HashShard::from_spec(s).unwrap())
            .collect();
        for key in &keys {
            let owners = members.iter().filter(|m| m.contains(key)).count();
            assert_eq!(owners, 1, "key {key} owned by {owners} shards (want 1)");
        }
    }

    #[test]
    fn hash_shard_degenerate_single_shard_owns_everything() {
        let m = HashShard {
            shards: 1,
            index: 0,
        };
        assert!(m.contains("anything"));
    }

    #[test]
    fn hash_shard_from_spec_rejects_malformed_descriptor() {
        assert!(HashShard::from_spec(&ShardSpec::new("0", json!({ "index": 0 }))).is_none());
        assert!(HashShard::from_spec(&ShardSpec::new("0", json!({ "shards": 4 }))).is_none());
        assert!(HashShard::from_spec(&ShardSpec::whole()).is_none());
    }

    #[test]
    fn hash_shard_round_trips_plan_descriptors() {
        for spec in plan_hash_shards(5) {
            let m = HashShard::from_spec(&spec).unwrap();
            assert_eq!(m.shards, 5);
            assert_eq!(m.index, spec.id.parse::<usize>().unwrap());
        }
    }

    // ── Connector-glue helpers (parse / bounds probe / plan-from-bounds) ─────

    #[test]
    fn parse_pk_shard_whole_clears_and_real_parses() {
        assert!(parse_pk_shard(&ShardSpec::whole(), "t").unwrap().is_none());
        let spec = &plan_pk_shards("id", 0, 99, 3)[1];
        let bounds = parse_pk_shard(spec, "t").unwrap().unwrap();
        assert_eq!(bounds.key, "id");
    }

    #[test]
    fn parse_pk_shard_malformed_names_connector() {
        let bad = ShardSpec::new("0", json!({ "key": "id" }));
        let err = parse_pk_shard(&bad, "mysql").unwrap_err();
        assert!(err.to_string().contains("mysql"), "got: {err}");
    }

    #[test]
    fn parse_hash_shard_whole_clears_and_real_parses() {
        assert!(
            parse_hash_shard(&ShardSpec::whole(), "t")
                .unwrap()
                .is_none()
        );
        let spec = &plan_hash_shards(3)[2];
        let m = parse_hash_shard(spec, "t").unwrap().unwrap();
        assert_eq!((m.shards, m.index), (3, 2));
    }

    #[test]
    fn parse_hash_shard_malformed_names_connector() {
        let bad = ShardSpec::new("0", json!({ "index": 0 }));
        let err = parse_hash_shard(&bad, "gcs").unwrap_err();
        assert!(err.to_string().contains("gcs"), "got: {err}");
    }

    #[test]
    fn pk_bounds_query_shapes_the_probe() {
        let sql = pk_bounds_query("SELECT * FROM t", "\"id\"", "BIGINT");
        assert_eq!(
            sql,
            "SELECT CAST(MIN(\"id\") AS BIGINT) AS lo, CAST(MAX(\"id\") AS BIGINT) AS hi \
             FROM (SELECT * FROM t) AS _faucet_bounds"
        );
    }

    #[test]
    fn pk_shards_from_bounds_plans_or_degrades() {
        let shards = pk_shards_from_bounds("id", Some(0), Some(99), 4);
        assert_eq!(shards.len(), 4);
        // NULL MIN/MAX (empty result set) → one whole shard.
        for (lo, hi) in [(None, None), (Some(1), None), (None, Some(1))] {
            let shards = pk_shards_from_bounds("id", lo, hi, 4);
            assert_eq!(shards.len(), 1);
            assert!(shards[0].is_whole());
        }
    }
}

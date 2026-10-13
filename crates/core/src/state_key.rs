//! Typed state keys (#882, RFC 0013).
//!
//! Every key a pipeline reads or writes in its [`StateStore`] is built here
//! from its components and rendered one way:
//!
//! ```text
//! [{tenant}::]{pipeline}::{row}[::{sub}][::{shard}][::{suffix}]
//! [{tenant}::]{pipeline}::{pipeline-level suffix}
//! ```
//!
//! A changed rendering silently looks like a first run — a full re-sync or a
//! skipped incremental range — so the rendering is versioned
//! ([`KEY_SCHEME_VERSION`]) and every key can list what earlier schemes would
//! have rendered for it ([`StateKey::legacy_renderings`]).
//! [`migrate_state_key`] moves a value stored under one of those keys to the
//! current key at run start.

use crate::error::FaucetError;
use crate::state::StateStore;

/// Version of the key rendering this build writes.
///
/// * `1` — a child invocation's parent-key value rendered verbatim.
/// * `2` — the parent-key value is encoded by [`encode_component`] (#840), so a
///   value holding `::`, spaces or non-ASCII cannot split or break the key.
pub const KEY_SCHEME_VERSION: u32 = 2;

/// Separator between key components.
pub const KEY_SEPARATOR: &str = "::";

/// Longest raw value [`encode_component`] keeps verbatim.
pub const COMPONENT_MAX: usize = 64;

/// What a key holds besides a plain resume position.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum KeySuffix {
    /// `::__sla__` — freshness / volume SLA history of an invocation.
    Sla,
    /// `::__profiling__` — column-profile baseline of an invocation.
    Profiling,
    /// `::__rollback__` — the retained undoable runs of an invocation.
    RollbackIndex,
    /// `::__rollback__::{run}` — one undoable run's marker.
    RollbackRun(String),
    /// `::__status__` — the last run outcomes of an invocation.
    Status,
    /// `::__lease__` — held while a run of the invocation is in flight.
    Lease,
    /// `{pipeline}::__replication__` — the snapshot→CDC phase marker.
    Replication,
    /// `{pipeline}::__backfill__::{range hash}` — a backfill's progress marker.
    Backfill(String),
    /// `{pipeline}::__serve_backfill__::{range hash}` — a served backfill range.
    ServeBackfill(String),
}

impl KeySuffix {
    /// Whether the suffix hangs off the pipeline rather than an invocation.
    pub fn is_pipeline_level(&self) -> bool {
        matches!(
            self,
            Self::Replication | Self::Backfill(_) | Self::ServeBackfill(_)
        )
    }

    fn render_into(&self, out: &mut String) {
        let (marker, tail) = match self {
            Self::Sla => ("__sla__", None),
            Self::Profiling => ("__profiling__", None),
            Self::RollbackIndex => ("__rollback__", None),
            Self::RollbackRun(run) => ("__rollback__", Some(run.as_str())),
            Self::Status => ("__status__", None),
            Self::Lease => ("__lease__", None),
            Self::Replication => ("__replication__", None),
            Self::Backfill(h) => ("__backfill__", Some(h.as_str())),
            Self::ServeBackfill(h) => ("__serve_backfill__", Some(h.as_str())),
        };
        push(out, marker);
        if let Some(tail) = tail {
            push(out, tail);
        }
    }

    /// The suffix's rendering, without the leading separator.
    pub fn render(&self) -> String {
        let mut s = String::new();
        self.render_into(&mut s);
        s
    }

    /// The suffix as it ends a key, separator included (`::__lease__`).
    pub fn tail(&self) -> String {
        format!("{KEY_SEPARATOR}{}", self.render())
    }
}

fn push(out: &mut String, seg: &str) {
    if !out.is_empty() {
        out.push_str(KEY_SEPARATOR);
    }
    out.push_str(seg);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Sub {
    /// A child invocation's parent-key value, as the parent produced it.
    Parent(String),
    /// A component already in key form (a product tuple, a parsed segment).
    Encoded(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Head {
    Parts {
        tenant: Option<String>,
        pipeline: String,
        row: Option<String>,
        sub: Option<Sub>,
    },
    /// An invocation key [`StateKey::render`] produced earlier.
    Rendered(String),
}

/// A state key built from its components. See the [module docs](self).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateKey {
    head: Head,
    shard: Option<String>,
    suffix: Option<KeySuffix>,
}

impl StateKey {
    /// The pipeline-level key namespace of `pipeline` (no row yet).
    pub fn pipeline(pipeline: impl Into<String>) -> Self {
        Self {
            head: Head::Parts {
                tenant: None,
                pipeline: pipeline.into(),
                row: None,
                sub: None,
            },
            shard: None,
            suffix: None,
        }
    }

    /// `{pipeline}::{row}` — a matrix row's (or topology sink node's) key.
    pub fn invocation(pipeline: impl Into<String>, row: impl Into<String>) -> Self {
        Self::pipeline(pipeline).with_row(row)
    }

    /// A key derived from an invocation key [`render`](Self::render) already
    /// produced — the base the per-invocation markers hang off.
    pub fn from_rendered(base: impl Into<String>) -> Self {
        Self {
            head: Head::Rendered(base.into()),
            shard: None,
            suffix: None,
        }
    }

    /// Place the key under `tenant` (`{tenant}::{pipeline}::…`, #709).
    pub fn with_tenant(mut self, tenant: Option<impl Into<String>>) -> Self {
        if let Head::Parts { tenant: t, .. } = &mut self.head {
            *t = tenant.map(Into::into);
        }
        self
    }

    /// The matrix row / topology node id.
    pub fn with_row(mut self, id: impl Into<String>) -> Self {
        if let Head::Parts { row, .. } = &mut self.head {
            *row = Some(id.into());
        }
        self
    }

    /// A child invocation's parent-key value, raw; rendered through
    /// [`encode_component`].
    pub fn with_parent_key(mut self, raw: impl Into<String>) -> Self {
        if let Head::Parts { sub, .. } = &mut self.head {
            *sub = Some(Sub::Parent(raw.into()));
        }
        self
    }

    /// A sub-invocation component already in key form (a product tuple suffix).
    pub fn with_sub(mut self, encoded: impl Into<String>) -> Self {
        if let Head::Parts { sub, .. } = &mut self.head {
            *sub = Some(Sub::Encoded(encoded.into()));
        }
        self
    }

    /// A shard of the invocation (`{base}::{shard}`).
    pub fn with_shard(mut self, shard: impl Into<String>) -> Self {
        self.shard = Some(shard.into());
        self
    }

    /// A marker suffix.
    pub fn with_suffix(mut self, suffix: KeySuffix) -> Self {
        self.suffix = Some(suffix);
        self
    }

    /// The tenant, when the key is built from components.
    pub fn tenant(&self) -> Option<&str> {
        match &self.head {
            Head::Parts { tenant, .. } => tenant.as_deref(),
            Head::Rendered(_) => None,
        }
    }

    /// The pipeline name, when the key is built from components.
    pub fn pipeline_name(&self) -> Option<&str> {
        match &self.head {
            Head::Parts { pipeline, .. } => Some(pipeline),
            Head::Rendered(_) => None,
        }
    }

    /// The row id, when the key is built from components.
    pub fn row(&self) -> Option<&str> {
        match &self.head {
            Head::Parts { row, .. } => row.as_deref(),
            Head::Rendered(_) => None,
        }
    }

    /// The rendered sub-invocation component.
    pub fn sub(&self) -> Option<String> {
        match &self.head {
            Head::Parts { sub: Some(s), .. } => Some(render_sub(s, KEY_SCHEME_VERSION)),
            _ => None,
        }
    }

    /// The shard id.
    pub fn shard(&self) -> Option<&str> {
        self.shard.as_deref()
    }

    /// The marker suffix.
    pub fn suffix(&self) -> Option<&KeySuffix> {
        self.suffix.as_ref()
    }

    /// `[{tenant}::]{pipeline}` — the first segments of every key of the
    /// pipeline; for a key built from a rendering, that rendering.
    pub fn namespace(&self) -> String {
        match &self.head {
            Head::Parts {
                tenant, pipeline, ..
            } => {
                let mut s = String::new();
                if let Some(t) = tenant {
                    push(&mut s, t);
                }
                push(&mut s, pipeline);
                s
            }
            Head::Rendered(base) => base.clone(),
        }
    }

    /// The prefix every key under this one starts with (`{key}::`).
    pub fn children_prefix(&self) -> String {
        format!("{}{KEY_SEPARATOR}", self.render())
    }

    /// The key under the current scheme.
    pub fn render(&self) -> String {
        self.render_scheme(KEY_SCHEME_VERSION)
    }

    /// The key as scheme `version` rendered it.
    pub fn render_scheme(&self, version: u32) -> String {
        let mut s = match &self.head {
            Head::Parts { row, sub, .. } => {
                let mut s = self.namespace();
                if let Some(row) = row {
                    push(&mut s, row);
                }
                if let Some(sub) = sub {
                    push(&mut s, &render_sub(sub, version));
                }
                s
            }
            Head::Rendered(base) => base.clone(),
        };
        if let Some(shard) = &self.shard {
            push(&mut s, shard);
        }
        if let Some(suffix) = &self.suffix {
            suffix.render_into(&mut s);
        }
        s
    }

    /// Every distinct, valid key an earlier scheme rendered for this key, newest
    /// scheme first — where [`migrate_state_key`] looks when the current key
    /// holds nothing.
    pub fn legacy_renderings(&self) -> Vec<String> {
        let current = self.render();
        let mut out: Vec<String> = Vec::new();
        for version in (1..KEY_SCHEME_VERSION).rev() {
            let old = self.render_scheme(version);
            if old != current
                && !out.contains(&old)
                && crate::state::validate_state_key(&old).is_ok()
            {
                out.push(old);
            }
        }
        out
    }

    /// Decode `key` under the namespace `[{tenant}::]{pipeline}`; `None` when it
    /// is outside it. A shard id cannot be told from a sub component, so it is
    /// returned as part of [`sub`](Self::sub); the rendering round-trips.
    pub fn parse(tenant: Option<&str>, pipeline: &str, key: &str) -> Option<Self> {
        let ns = StateKey::pipeline(pipeline).with_tenant(tenant).namespace();
        let rest = key.strip_prefix(&ns)?.strip_prefix(KEY_SEPARATOR)?;
        if rest.is_empty() {
            return None;
        }
        let segs: Vec<&str> = rest.split(KEY_SEPARATOR).collect();
        let join = |s: &[&str]| s.join(KEY_SEPARATOR);
        let base = StateKey::pipeline(pipeline).with_tenant(tenant);
        match segs[0] {
            "__replication__" if segs.len() == 1 => {
                return Some(base.with_suffix(KeySuffix::Replication));
            }
            "__backfill__" if segs.len() > 1 => {
                return Some(base.with_suffix(KeySuffix::Backfill(join(&segs[1..]))));
            }
            "__serve_backfill__" if segs.len() > 1 => {
                return Some(base.with_suffix(KeySuffix::ServeBackfill(join(&segs[1..]))));
            }
            _ => {}
        }
        let marker = segs.iter().enumerate().skip(1).find_map(|(i, s)| {
            let suffix = match *s {
                "__sla__" if i + 1 == segs.len() => KeySuffix::Sla,
                "__profiling__" if i + 1 == segs.len() => KeySuffix::Profiling,
                "__status__" if i + 1 == segs.len() => KeySuffix::Status,
                "__lease__" if i + 1 == segs.len() => KeySuffix::Lease,
                "__rollback__" if i + 1 < segs.len() => {
                    KeySuffix::RollbackRun(join(&segs[i + 1..]))
                }
                "__rollback__" => KeySuffix::RollbackIndex,
                _ => return None,
            };
            Some((i, suffix))
        });
        let end = marker.as_ref().map_or(segs.len(), |(i, _)| *i);
        // A backfill unit's row id is `backfill::{range hash}::{unit}`.
        let row_end = if segs[0] == "backfill" && end > 1 {
            end
        } else {
            1
        };
        let mut parsed = base.with_row(join(&segs[..row_end]));
        if end > row_end {
            parsed = parsed.with_sub(join(&segs[row_end..end]));
        }
        if let Some((_, suffix)) = marker {
            parsed = parsed.with_suffix(suffix);
        }
        Some(parsed)
    }
}

fn render_sub(sub: &Sub, version: u32) -> String {
    match sub {
        Sub::Parent(raw) if version >= 2 => encode_component(raw, true),
        Sub::Parent(raw) => raw.clone(),
        Sub::Encoded(s) => s.clone(),
    }
}

impl std::fmt::Display for StateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.render())
    }
}

/// Render one raw value as a state-key component (#789 CLI-52/CLI-53). A value
/// made only of state-key characters is kept as is; anything else (an email, a
/// space, non-ASCII, `::`, an over-long value) becomes a readable prefix plus a
/// stable 64-bit hash of the raw value. `allow_separators` admits `:` and `/`
/// (a child's parent key, which stands alone after `::`).
pub fn encode_component(raw: &str, allow_separators: bool) -> String {
    let plain = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.');
    let legal = |c: char| plain(c) || (allow_separators && matches!(c, ':' | '/'));
    if !raw.is_empty()
        && raw.len() <= COMPONENT_MAX
        && raw.chars().all(legal)
        && !raw.contains(KEY_SEPARATOR)
        && !raw.starts_with(':')
        && !raw.ends_with(':')
    {
        return raw.to_owned();
    }
    let readable: String = raw.chars().filter(|c| plain(*c)).take(32).collect();
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in raw.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{readable}.h{hash:016x}")
}

/// The row id of one backfill unit's invocation, `backfill::{range hash}::{unit}`
/// — namespaced away from every live row's key.
pub fn backfill_unit_row(range_hash: &str, unit: &str) -> String {
    ["backfill", range_hash, unit].join(KEY_SEPARATOR)
}

/// What [`migrate_state_key`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum KeyMigration {
    /// The current key already holds a value; nothing was looked up.
    Current,
    /// No candidate holds a value: a genuine first run.
    Fresh,
    /// The value under `from` now lives under the current key.
    Moved {
        /// The earlier key the value was found under.
        from: String,
    },
}

/// Move a value an earlier key scheme (or a renamed row) left under one of
/// `candidates` to `key`, when `key` holds nothing.
///
/// The first candidate holding a value wins. The value is claimed with
/// [`StateStore::compare_and_put`] against an absent entry, so a run that wrote
/// `key` meanwhile is never overwritten; the old key is deleted afterwards. A
/// crash in between leaves the value under both keys, and the next run sees the
/// current key and moves nothing. Counted in
/// `faucet_state_key_migrations_total{outcome}`.
pub async fn migrate_state_key(
    store: &dyn StateStore,
    key: &str,
    candidates: &[String],
) -> Result<KeyMigration, FaucetError> {
    if candidates.is_empty() {
        return Ok(KeyMigration::Fresh);
    }
    if store.get(key).await?.is_some() {
        return Ok(KeyMigration::Current);
    }
    for old in candidates.iter().filter(|c| c.as_str() != key) {
        let Some(value) = store.get(old).await? else {
            continue;
        };
        if !store.compare_and_put(key, None, &value).await? {
            metrics::counter!("faucet_state_key_migrations_total", "outcome" => "raced")
                .increment(1);
            return Ok(KeyMigration::Current);
        }
        if let Err(e) = store.delete(old).await {
            tracing::warn!(key = %old, error = %e, "legacy state key could not be removed");
        }
        metrics::counter!("faucet_state_key_migrations_total", "outcome" => "moved").increment(1);
        tracing::info!(from = %old, to = key, "moved state from the key an earlier release used");
        return Ok(KeyMigration::Moved { from: old.clone() });
    }
    Ok(KeyMigration::Fresh)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::MemoryStateStore;
    use serde_json::json;

    #[test]
    fn renders_every_kind() {
        let inv = StateKey::invocation("p", "r");
        assert_eq!(inv.render(), "p::r");
        assert_eq!(StateKey::pipeline("p").render(), "p");
        assert_eq!(
            StateKey::invocation("p", "r")
                .with_tenant(Some("acme"))
                .render(),
            "acme::p::r"
        );
        assert_eq!(inv.clone().with_parent_key("42").render(), "p::r::42");
        assert_eq!(inv.clone().with_sub("a:1/b:2").render(), "p::r::a:1/b:2");
        assert_eq!(inv.clone().with_shard("s3").render(), "p::r::s3");
        let cases = [
            (KeySuffix::Sla, "p::r::__sla__"),
            (KeySuffix::Profiling, "p::r::__profiling__"),
            (KeySuffix::RollbackIndex, "p::r::__rollback__"),
            (KeySuffix::RollbackRun("x".into()), "p::r::__rollback__::x"),
            (KeySuffix::Status, "p::r::__status__"),
            (KeySuffix::Lease, "p::r::__lease__"),
        ];
        for (suffix, want) in cases {
            assert!(!suffix.is_pipeline_level());
            assert_eq!(inv.clone().with_suffix(suffix).render(), want);
        }
        for (suffix, want) in [
            (KeySuffix::Replication, "p::__replication__"),
            (KeySuffix::Backfill("h".into()), "p::__backfill__::h"),
            (
                KeySuffix::ServeBackfill("h".into()),
                "p::__serve_backfill__::h",
            ),
        ] {
            assert!(suffix.is_pipeline_level());
            assert_eq!(StateKey::pipeline("p").with_suffix(suffix).render(), want);
        }
        assert_eq!(
            StateKey::from_rendered("p::r::7")
                .with_suffix(KeySuffix::Status)
                .to_string(),
            "p::r::7::__status__"
        );
        assert_eq!(
            KeySuffix::RollbackRun("x".into()).render(),
            "__rollback__::x"
        );
        assert_eq!(inv.children_prefix(), "p::r::");
        assert_eq!(KeySuffix::Lease.tail(), "::__lease__");
    }

    #[test]
    fn accessors_report_components() {
        let k = StateKey::invocation("p", "r")
            .with_tenant(Some("t"))
            .with_parent_key("a b")
            .with_shard("s")
            .with_suffix(KeySuffix::Lease);
        assert_eq!(k.tenant(), Some("t"));
        assert_eq!(k.pipeline_name(), Some("p"));
        assert_eq!(k.row(), Some("r"));
        assert_eq!(k.sub(), Some(encode_component("a b", true)));
        assert_eq!(k.shard(), Some("s"));
        assert_eq!(k.suffix(), Some(&KeySuffix::Lease));
        assert_eq!(k.namespace(), "t::p");
        let r = StateKey::from_rendered("x::y")
            .with_tenant(Some("ignored"))
            .with_row("ignored")
            .with_parent_key("ignored")
            .with_sub("ignored");
        assert_eq!(r.render(), "x::y");
        assert_eq!(
            (r.tenant(), r.pipeline_name(), r.row(), r.sub()),
            (None, None, None, None)
        );
        assert_eq!(r.namespace(), "x::y");
        assert_eq!(StateKey::invocation("p", "r").sub(), None);
    }

    #[test]
    fn parent_keys_are_encoded_and_remember_the_verbatim_scheme() {
        let k = StateKey::invocation("p", "r").with_parent_key("a b");
        assert_ne!(k.render(), "p::r::a b");
        assert_eq!(k.render_scheme(1), "p::r::a b");
        // `a b` is not a valid key, so scheme 1 has nothing to offer.
        assert!(k.legacy_renderings().is_empty());
        let k = StateKey::invocation("p", "r").with_parent_key("x::y");
        assert_eq!(k.legacy_renderings(), vec!["p::r::x::y".to_string()]);
        let k = StateKey::invocation("p", "r").with_parent_key("plain");
        assert!(k.legacy_renderings().is_empty());
    }

    #[test]
    fn encode_component_keeps_legal_values_and_hashes_the_rest() {
        assert_eq!(encode_component("cust-42_a.b", false), "cust-42_a.b");
        assert_eq!(encode_component("a/b:c", true), "a/b:c");
        assert_ne!(encode_component("a/b:c", false), "a/b:c");
        for raw in ["", "x::y", ":x", "y:", "émoji", &"z".repeat(65)] {
            let c = encode_component(raw, true);
            assert!(c.contains(".h"), "{raw:?} → {c}");
            crate::state::validate_state_key(&format!("p::r::{c}")).unwrap();
        }
    }

    #[test]
    fn parse_round_trips_every_kind() {
        let keys = [
            "p::r",
            "p::r::42",
            "p::r::a:1/b:2",
            "p::r::__sla__",
            "p::r::__profiling__",
            "p::r::__rollback__",
            "p::r::__rollback__::run1",
            "p::r::7::__status__",
            "p::r::__lease__",
            "p::__replication__",
            "p::__backfill__::abc",
            "p::__serve_backfill__::abc",
            "p::backfill::h::2026-01-01",
            "p::backfill::h::u1::__status__",
            "p::backfill",
        ];
        for key in keys {
            let k = StateKey::parse(None, "p", key).unwrap_or_else(|| panic!("{key}"));
            assert_eq!(k.render(), key);
        }
        assert_eq!(backfill_unit_row("h", "u"), "backfill::h::u");
        let k = StateKey::parse(None, "p", "p::backfill::h::u1::__lease__").unwrap();
        assert_eq!(k.row(), Some("backfill::h::u1"));
        assert_eq!(k.suffix(), Some(&KeySuffix::Lease));
        let k = StateKey::parse(Some("t"), "p", "t::p::r::9").unwrap();
        assert_eq!(
            (k.tenant(), k.row(), k.sub()),
            (Some("t"), Some("r"), Some("9".into()))
        );
        assert!(StateKey::parse(None, "p", "q::r").is_none());
        assert!(StateKey::parse(None, "p", "p::").is_none());
        assert!(StateKey::parse(None, "p", "px::r").is_none());
    }

    #[tokio::test]
    async fn migration_moves_once_and_never_clobbers() {
        let store = MemoryStateStore::new();
        assert_eq!(
            migrate_state_key(&store, "p::new", &[]).await.unwrap(),
            KeyMigration::Fresh
        );
        assert_eq!(
            migrate_state_key(&store, "p::new", &["p::old".into()])
                .await
                .unwrap(),
            KeyMigration::Fresh
        );
        store.put("p::old", &json!({"page": 3})).await.unwrap();
        assert_eq!(
            migrate_state_key(&store, "p::new", &["p::new".into(), "p::old".into()])
                .await
                .unwrap(),
            KeyMigration::Moved {
                from: "p::old".into()
            }
        );
        assert_eq!(store.get("p::new").await.unwrap(), Some(json!({"page": 3})));
        assert_eq!(store.get("p::old").await.unwrap(), None);
        store.put("p::old", &json!(1)).await.unwrap();
        assert_eq!(
            migrate_state_key(&store, "p::new", &["p::old".into()])
                .await
                .unwrap(),
            KeyMigration::Current
        );
        assert_eq!(store.get("p::new").await.unwrap(), Some(json!({"page": 3})));
    }

    struct Racing(MemoryStateStore);

    #[async_trait::async_trait]
    impl StateStore for Racing {
        async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, FaucetError> {
            self.0.get(key).await
        }
        async fn put(&self, key: &str, value: &serde_json::Value) -> Result<(), FaucetError> {
            self.0.put(key, value).await
        }
        async fn delete(&self, key: &str) -> Result<(), FaucetError> {
            self.0.delete(key).await
        }
        async fn compare_and_put(
            &self,
            _key: &str,
            _expected: Option<&serde_json::Value>,
            _value: &serde_json::Value,
        ) -> Result<bool, FaucetError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn a_concurrent_writer_wins_the_race() {
        let store = Racing(MemoryStateStore::new());
        store.put("p::old", &json!(1)).await.unwrap();
        assert_eq!(
            migrate_state_key(&store, "p::new", &["p::old".into()])
                .await
                .unwrap(),
            KeyMigration::Current
        );
        assert_eq!(store.get("p::old").await.unwrap(), Some(json!(1)));
    }
}

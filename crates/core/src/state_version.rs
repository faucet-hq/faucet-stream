//! Versioned pipeline state (#736): upgrade-safe bookmarks and CDC positions.
//!
//! A bookmark outlives the release that wrote it. Stored bare, a value written
//! by release N is read by N+1 as whatever shape N+1 expects — so a changed
//! bookmark shape turns into a silent full re-sync or a skipped range. The
//! pipeline therefore stores every bookmark inside an envelope naming the
//! connector that owns the shape and that connector's shape version:
//!
//! ```json
//! { "faucet_state": 1, "owner": "postgres-cdc", "schema": 0, "data": { "last_lsn": "0/16B3748" } }
//! ```
//!
//! On read the envelope is checked before the source sees anything:
//!
//! - an un-enveloped value is a legacy bookmark at schema `0` (read as-is and
//!   rewritten in the envelope by the next bookmark write);
//! - an older `schema` is migrated forward by the source
//!   ([`Source::migrate_state`]) — a pure step, so a crash before the next
//!   write leaves the old value valid;
//! - a newer `schema`, a newer envelope `faucet_state`, or another `owner` is
//!   refused with [`FaucetError::StateIncompatible`]: never guessed.
//!
//! The exactly-once wrapper (`{"__faucet_eo": 1, "bookmark", "seq"}`) lives
//! *inside* `data`, and a migration rewrites only its bookmark.

use crate::FaucetError;
use crate::traits::Source;
use serde::Serialize;
use serde_json::{Map, Value};

/// The envelope version this release writes and the newest it reads.
pub const STATE_FORMAT: u32 = 1;

const FORMAT_KEY: &str = "faucet_state";
const OWNER_KEY: &str = "owner";
const SCHEMA_KEY: &str = "schema";
const DATA_KEY: &str = "data";

/// A stored state value, taken apart.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct StoredState {
    /// Envelope version; `0` for a legacy, un-enveloped value.
    pub format: u32,
    /// The connector that owns the shape of `data` (`None` for legacy values).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    /// The owner's shape version (`0` for legacy values).
    pub schema: u32,
    /// The bookmark (or exactly-once wrapper) itself.
    pub data: Value,
}

impl StoredState {
    /// Take a stored value apart. Anything that is not an envelope is a legacy
    /// bookmark at schema 0.
    pub fn parse(stored: &Value) -> Self {
        if let Some(map) = stored.as_object()
            && let Some(format) = map.get(FORMAT_KEY).and_then(Value::as_u64)
            && let Some(data) = map.get(DATA_KEY)
        {
            return Self {
                format: u32::try_from(format).unwrap_or(u32::MAX),
                owner: map
                    .get(OWNER_KEY)
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                schema: map
                    .get(SCHEMA_KEY)
                    .and_then(Value::as_u64)
                    .map(|s| u32::try_from(s).unwrap_or(u32::MAX))
                    .unwrap_or(0),
                data: data.clone(),
            };
        }
        Self {
            format: 0,
            owner: None,
            schema: 0,
            data: stored.clone(),
        }
    }

    /// Whether the value predates the envelope.
    pub fn is_legacy(&self) -> bool {
        self.format == 0
    }
}

/// Wrap `data` in the envelope for `owner` at shape version `schema`.
pub fn wrap_versioned(owner: &str, schema: u32, data: &Value) -> Value {
    let mut map = Map::new();
    map.insert(FORMAT_KEY.into(), Value::from(STATE_FORMAT));
    map.insert(OWNER_KEY.into(), Value::from(owner));
    map.insert(SCHEMA_KEY.into(), Value::from(schema));
    map.insert(DATA_KEY.into(), data.clone());
    Value::Object(map)
}

/// The payload of a stored value — `data` of an envelope, or the value itself.
/// For readers that only display or pass the value through.
pub fn peel_versioned(stored: &Value) -> Value {
    StoredState::parse(stored).data
}

/// Whether a stored value is an envelope.
pub fn is_versioned(stored: &Value) -> bool {
    !StoredState::parse(stored).is_legacy()
}

/// How a stored value relates to what the reading source expects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum StateCompat {
    /// Readable as-is.
    Current,
    /// Readable after the source migrates it from `from` to `to`.
    Migrate { from: u32, to: u32 },
    /// Not readable by this release or this connector.
    Incompatible { found: String, expected: String },
}

/// Compare a stored value with the `owner` and shape version `expected` the
/// reading source declares.
pub fn check_compat(state: &StoredState, owner: &str, expected: u32) -> StateCompat {
    if state.format > STATE_FORMAT {
        return StateCompat::Incompatible {
            found: format!("state format {}", state.format),
            expected: format!("state format {STATE_FORMAT} or older"),
        };
    }
    if let Some(found) = &state.owner
        && found != owner
    {
        return StateCompat::Incompatible {
            found: format!("state owned by '{found}'"),
            expected: format!("state owned by '{owner}'"),
        };
    }
    if state.schema > expected {
        return StateCompat::Incompatible {
            found: format!("'{owner}' state schema {}", state.schema),
            expected: format!("'{owner}' state schema {expected} or older"),
        };
    }
    if state.schema < expected {
        return StateCompat::Migrate {
            from: state.schema,
            to: expected,
        };
    }
    StateCompat::Current
}

/// The refusal for a stored value this release or connector cannot read.
pub fn incompatible(key: &str, found: String, expected: String) -> FaucetError {
    FaucetError::StateIncompatible {
        key: key.to_string(),
        found,
        expected,
    }
}

/// A stored value made readable for the source that will resume from it.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedState {
    /// The payload at the source's current shape — a bare bookmark or an
    /// exactly-once wrapper around one.
    pub data: Value,
    /// The shape version it was migrated from, when a migration ran.
    pub migrated_from: Option<u32>,
    /// Whether the stored value predated the envelope.
    pub legacy: bool,
}

/// Check and, when needed, migrate the value stored under `key` for `source`.
/// Refuses (typed) rather than letting the source misread it.
pub fn resolve_for_source(
    key: &str,
    stored: &Value,
    source: &dyn Source,
) -> Result<ResolvedState, FaucetError> {
    resolve_with(
        key,
        stored,
        source.connector_name(),
        source.state_schema(),
        |from, data| source.migrate_state(from, data),
    )
}

/// [`resolve_for_source`] with the owner, expected version and migration
/// passed explicitly — for callers holding a connector kind rather than a
/// built source.
pub fn resolve_with(
    key: &str,
    stored: &Value,
    owner: &str,
    expected: u32,
    migrate: impl Fn(u32, Value) -> Result<Value, FaucetError>,
) -> Result<ResolvedState, FaucetError> {
    let state = StoredState::parse(stored);
    let legacy = state.is_legacy();
    match check_compat(&state, owner, expected) {
        StateCompat::Current => Ok(ResolvedState {
            data: state.data,
            migrated_from: None,
            legacy,
        }),
        StateCompat::Incompatible { found, expected } => Err(incompatible(key, found, expected)),
        StateCompat::Migrate { from, to } => {
            let data = migrate_payload(state.data, |bm| {
                migrate(from, bm).map_err(|e| {
                    incompatible(
                        key,
                        format!("'{owner}' state schema {from}"),
                        format!("'{owner}' state schema {to} (migration failed: {e})"),
                    )
                })
            })?;
            Ok(ResolvedState {
                data,
                migrated_from: Some(from),
                legacy,
            })
        }
    }
}

/// Apply `migrate` to the bookmark in `data`: inside an exactly-once wrapper
/// when there is one, keeping its sequence. A null bookmark has nothing to
/// migrate.
fn migrate_payload(
    data: Value,
    migrate: impl Fn(Value) -> Result<Value, FaucetError>,
) -> Result<Value, FaucetError> {
    if crate::idempotency::is_eo_envelope(&data) {
        let (bookmark, seq) = crate::idempotency::unwrap_state(&data);
        let migrated = bookmark.map(migrate).transpose()?;
        return Ok(crate::idempotency::wrap_state(migrated.as_ref(), seq));
    }
    if data.is_null() {
        return Ok(data);
    }
    migrate(data)
}

/// The value to store: the envelope, or — when `legacy` (a mixed-version
/// cluster whose older members cannot read the envelope yet) — the bare
/// payload.
pub fn encode_for_write(owner: &str, schema: u32, data: &Value, legacy: bool) -> Value {
    if legacy {
        data.clone()
    } else {
        wrap_versioned(owner, schema, data)
    }
}

/// How one pipeline's bookmark is written (#736): the owner and shape version
/// the envelope carries, or bare for a mixed-version cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateCodec {
    /// The connector that owns the bookmark shape.
    pub owner: String,
    /// Its shape version.
    pub schema: u32,
    /// Write the bare payload (a cluster member that predates the envelope is
    /// live).
    pub legacy: bool,
}

impl StateCodec {
    /// The codec for bookmarks `source` produces.
    pub fn for_source(source: &dyn Source, legacy: bool) -> Self {
        Self {
            owner: source.connector_name().to_string(),
            schema: source.state_schema(),
            legacy,
        }
    }

    /// The stored form of `value`.
    pub fn encode(&self, value: &Value) -> Value {
        encode_for_write(&self.owner, self.schema, value, self.legacy)
    }
}

/// A [`StateStore`](crate::state::StateStore) that stores one key — the
/// pipeline's bookmark — in the versioned envelope and passes everything else
/// through. Reads return the stored value; resolving it is the reader's job
/// ([`resolve_for_source`]).
pub struct VersionedStateStore {
    inner: std::sync::Arc<dyn crate::state::StateStore>,
    key: String,
    codec: StateCodec,
}

impl VersionedStateStore {
    /// Version writes of `key` into `inner` with `codec`.
    pub fn new(
        inner: std::sync::Arc<dyn crate::state::StateStore>,
        key: impl Into<String>,
        codec: StateCodec,
    ) -> Self {
        Self {
            inner,
            key: key.into(),
            codec,
        }
    }

    fn stored(&self, key: &str, value: &Value) -> Value {
        if key == self.key {
            self.codec.encode(value)
        } else {
            value.clone()
        }
    }
}

#[async_trait::async_trait]
impl crate::state::StateStore for VersionedStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        self.inner.get(key).await
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        self.inner.put(key, &self.stored(key, value)).await
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        self.inner.delete(key).await
    }

    async fn check(
        &self,
        ctx: &crate::check::CheckContext,
    ) -> Result<crate::check::CheckReport, FaucetError> {
        self.inner.check(ctx).await
    }

    fn supports_list(&self) -> bool {
        self.inner.supports_list()
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, FaucetError> {
        self.inner.list(prefix).await
    }

    fn supports_atomic_batch(&self) -> bool {
        self.inner.supports_atomic_batch()
    }

    async fn put_batch(&self, entries: &[(String, Value)]) -> Result<(), FaucetError> {
        let entries: Vec<(String, Value)> = entries
            .iter()
            .map(|(k, v)| (k.clone(), self.stored(k, v)))
            .collect();
        self.inner.put_batch(&entries).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn legacy_values_parse_as_schema_zero() {
        let s = StoredState::parse(&json!({"lsn": "0/1"}));
        assert!(s.is_legacy());
        assert_eq!((s.format, s.schema, s.owner.clone()), (0, 0, None));
        assert_eq!(s.data, json!({"lsn": "0/1"}));
        assert!(!is_versioned(&json!(7)));
        assert_eq!(peel_versioned(&json!(7)), json!(7));
        let no_data = json!({"faucet_state": 1, "owner": "x"});
        assert!(StoredState::parse(&no_data).is_legacy());
    }

    #[test]
    fn envelopes_round_trip() {
        let v = wrap_versioned("kafka", 2, &json!({"o": 1}));
        assert_eq!(
            v,
            json!({"faucet_state": 1, "owner": "kafka", "schema": 2, "data": {"o": 1}})
        );
        let s = StoredState::parse(&v);
        assert_eq!(s.owner.as_deref(), Some("kafka"));
        assert_eq!(s.schema, 2);
        assert!(is_versioned(&v));
        assert_eq!(peel_versioned(&v), json!({"o": 1}));
        assert_eq!(encode_for_write("k", 0, &json!(1), true), json!(1));
        assert_eq!(
            encode_for_write("k", 0, &json!(1), false),
            wrap_versioned("k", 0, &json!(1))
        );
        let huge = json!({"faucet_state": u64::MAX, "schema": u64::MAX, "data": 1});
        let s = StoredState::parse(&huge);
        assert_eq!((s.format, s.schema), (u32::MAX, u32::MAX));
    }

    #[test]
    fn compat_decisions() {
        let at = |owner: Option<&str>, schema: u32, format: u32| StoredState {
            format,
            owner: owner.map(str::to_owned),
            schema,
            data: json!(1),
        };
        assert_eq!(
            check_compat(&at(Some("a"), 1, 1), "a", 1),
            StateCompat::Current
        );
        assert_eq!(check_compat(&at(None, 0, 0), "a", 0), StateCompat::Current);
        assert_eq!(
            check_compat(&at(None, 0, 0), "a", 2),
            StateCompat::Migrate { from: 0, to: 2 }
        );
        assert!(matches!(
            check_compat(&at(Some("a"), 3, 1), "a", 2),
            StateCompat::Incompatible { .. }
        ));
        assert!(matches!(
            check_compat(&at(Some("b"), 0, 1), "a", 0),
            StateCompat::Incompatible { .. }
        ));
        assert!(matches!(
            check_compat(&at(Some("a"), 0, STATE_FORMAT + 1), "a", 0),
            StateCompat::Incompatible { .. }
        ));
    }

    #[test]
    fn resolve_migrates_bare_and_exactly_once_payloads() {
        let add_v = |from: u32, v: Value| -> Result<Value, FaucetError> {
            assert_eq!(from, 0);
            let mut m = v.as_object().cloned().unwrap_or_default();
            m.insert("v".into(), json!(1));
            Ok(Value::Object(m))
        };
        let r = resolve_with("k", &json!({"a": 1}), "src", 1, add_v).unwrap();
        assert_eq!(r.data, json!({"a": 1, "v": 1}));
        assert_eq!(r.migrated_from, Some(0));
        assert!(r.legacy);

        let eo = crate::idempotency::wrap_state(Some(&json!({"a": 2})), 9);
        let r = resolve_with("k", &wrap_versioned("src", 0, &eo), "src", 1, add_v).unwrap();
        assert_eq!(
            crate::idempotency::unwrap_state(&r.data),
            (Some(json!({"a": 2, "v": 1})), 9)
        );
        assert!(!r.legacy);

        let empty_eo = crate::idempotency::wrap_state(None, 3);
        let r = resolve_with("k", &empty_eo, "src", 1, add_v).unwrap();
        assert_eq!(crate::idempotency::unwrap_state(&r.data), (None, 3));
        let r = resolve_with("k", &Value::Null, "src", 1, add_v).unwrap();
        assert_eq!(r.data, Value::Null);

        let current =
            resolve_with("k", &wrap_versioned("src", 1, &json!(5)), "src", 1, add_v).unwrap();
        assert_eq!((current.data, current.migrated_from), (json!(5), None));
    }

    #[test]
    fn resolve_refuses_with_a_typed_error() {
        let never = |_: u32, _: Value| -> Result<Value, FaucetError> { unreachable!() };
        let err = resolve_with(
            "p::r",
            &wrap_versioned("other", 0, &json!(1)),
            "src",
            0,
            never,
        )
        .unwrap_err();
        match &err {
            FaucetError::StateIncompatible {
                key,
                found,
                expected,
            } => {
                assert_eq!(key, "p::r");
                assert!(found.contains("'other'") && expected.contains("'src'"));
            }
            other => panic!("{other:?}"),
        }
        assert!(err.to_string().contains("p::r"), "{err}");

        let failing = |_: u32, _: Value| -> Result<Value, FaucetError> {
            Err(FaucetError::State("bad shape".into()))
        };
        let err = resolve_with("k", &json!({"a": 1}), "src", 1, failing).unwrap_err();
        assert!(err.to_string().contains("migration failed"), "{err}");
    }

    #[tokio::test]
    async fn versioned_store_envelopes_only_the_bookmark_key() {
        use crate::state::{MemoryStateStore, StateStore};
        let inner: std::sync::Arc<dyn StateStore> = std::sync::Arc::new(MemoryStateStore::new());
        let codec = StateCodec {
            owner: "src".into(),
            schema: 2,
            legacy: false,
        };
        let store = VersionedStateStore::new(std::sync::Arc::clone(&inner), "p::r", codec.clone());
        store.put("p::r", &json!({"a": 1})).await.unwrap();
        store.put("p::r::__sla__", &json!({"x": 1})).await.unwrap();
        assert_eq!(
            inner.get("p::r").await.unwrap(),
            Some(wrap_versioned("src", 2, &json!({"a": 1})))
        );
        assert_eq!(
            store.get("p::r::__sla__").await.unwrap(),
            Some(json!({"x": 1}))
        );
        store
            .put_batch(&[("p::r".into(), json!(2)), ("other".into(), json!(3))])
            .await
            .unwrap();
        assert_eq!(
            inner.get("p::r").await.unwrap(),
            Some(wrap_versioned("src", 2, &json!(2)))
        );
        assert_eq!(inner.get("other").await.unwrap(), Some(json!(3)));
        assert_eq!(store.supports_list(), inner.supports_list());
        assert_eq!(store.supports_atomic_batch(), inner.supports_atomic_batch());
        assert!(
            store
                .list("p::")
                .await
                .unwrap()
                .contains(&"p::r".to_string())
        );
        store.delete("other").await.unwrap();
        assert_eq!(inner.get("other").await.unwrap(), None);
        assert!(
            store
                .check(&crate::check::CheckContext::default())
                .await
                .is_ok()
        );
        let legacy = StateCodec {
            legacy: true,
            ..codec
        };
        assert_eq!(legacy.encode(&json!(5)), json!(5));
    }
}

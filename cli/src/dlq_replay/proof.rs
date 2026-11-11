//! The signed record of masking on dead-letter envelopes
//! ([`faucet_core::masking::MaskingProof`]): written by a governed DLQ when
//! the server holds a vault key, and checked by a replay a caller started, so
//! only a payload provably masked under the rules in force today is written
//! without being masked again.

use faucet_core::MaskingSpec;
use faucet_core::UnwrappedEnvelope;
use faucet_core::masking::MaskingProof;
use faucet_core::masking::proof::rules_fingerprint;
use serde_json::Value;
use std::sync::{OnceLock, RwLock};

/// The label the signing key is derived under from the server's vault key.
pub const KEY_LABEL: &str = "dlq-masking";

fn slot() -> &'static RwLock<Vec<Vec<u8>>> {
    static KEYS: OnceLock<RwLock<Vec<Vec<u8>>>> = OnceLock::new();
    KEYS.get_or_init(Default::default)
}

/// Sign with the first of `keys` and accept any of them (the others are
/// previous keys). Empty: nothing is signed and no proof is valid.
pub fn install_keys(keys: Vec<Vec<u8>>) {
    if let Ok(mut k) = slot().write() {
        *k = keys;
    }
}

fn keys() -> Vec<Vec<u8>> {
    slot().read().map(|k| k.clone()).unwrap_or_default()
}

/// The rules of `spec` that apply to a destination named by `ids`.
pub fn destination_rules(spec: &MaskingSpec, ids: &[&str]) -> Value {
    Value::Array(
        spec.rules
            .iter()
            .filter(|r| {
                r.applies_to.is_empty() || r.applies_to.iter().any(|t| ids.contains(&t.as_str()))
            })
            .map(|r| serde_json::to_value(r).unwrap_or(Value::Null))
            .collect(),
    )
}

fn proof(key: &[u8], spec: &MaskingSpec, ids: &[&str], tenant: Option<&str>) -> MaskingProof {
    let rules = rules_fingerprint(&destination_rules(spec, ids), spec.key.as_deref(), key);
    MaskingProof::new(key.to_vec(), tenant, rules)
}

/// The signer for a DLQ of the destination `ids` under `spec`, if the server
/// holds a key.
pub fn signer(spec: &MaskingSpec, ids: &[&str], tenant: Option<&str>) -> Option<MaskingProof> {
    keys().first().map(|k| proof(k, spec, ids, tenant))
}

/// Whether `env` carries a valid record of masking under `spec` for the
/// destination `ids`, signed for `tenant` with the current or a previous key.
pub fn verified(
    env: &UnwrappedEnvelope,
    spec: &MaskingSpec,
    ids: &[&str],
    tenant: Option<&str>,
) -> bool {
    verified_with(&keys(), env, spec, ids, tenant)
}

/// What a replay checks a payload's masking record against: the target's
/// masking, its destination ids, the tenant and the keys held when the replay
/// was planned.
#[derive(Clone)]
pub struct ProofCheck {
    spec: MaskingSpec,
    ids: Vec<String>,
    tenant: Option<String>,
    keys: Vec<Vec<u8>>,
}

impl ProofCheck {
    /// Check against `spec` for the destination `ids`, for `tenant`, with
    /// the keys the server holds now.
    pub fn new(spec: MaskingSpec, ids: &[&str], tenant: Option<String>) -> Self {
        Self::with_keys(spec, ids, tenant, keys())
    }

    fn with_keys(
        spec: MaskingSpec,
        ids: &[&str],
        tenant: Option<String>,
        keys: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            spec,
            ids: ids.iter().map(|s| s.to_string()).collect(),
            tenant,
            keys,
        }
    }

    /// Whether `env`'s masking record verifies.
    pub fn holds(&self, env: &UnwrappedEnvelope) -> bool {
        let ids: Vec<&str> = self.ids.iter().map(String::as_str).collect();
        verified_with(&self.keys, env, &self.spec, &ids, self.tenant.as_deref())
    }

    #[cfg(test)]
    pub(crate) fn for_tests(
        spec: MaskingSpec,
        ids: &[&str],
        tenant: Option<&str>,
        keys: Vec<Vec<u8>>,
    ) -> Self {
        Self::with_keys(spec, ids, tenant.map(str::to_string), keys)
    }
}

fn verified_with(
    keys: &[Vec<u8>],
    env: &UnwrappedEnvelope,
    spec: &MaskingSpec,
    ids: &[&str],
    tenant: Option<&str>,
) -> bool {
    let Some(masking) = &env.masking else {
        return false;
    };
    let (pipeline, row) = (
        env.pipeline.as_deref().unwrap_or_default(),
        env.row.as_deref().unwrap_or_default(),
    );
    keys.iter()
        .any(|k| proof(k, spec, ids, tenant).verify(pipeline, row, &env.payload, masking))
}

tokio::task_local! {
    static REPLAY: Option<String>;
}

/// Run `fut` (a DLQ replay) checking masking records for `tenant`: a replay a
/// caller started writes a payload unmasked only when its record verifies.
pub async fn checking<F: std::future::Future>(tenant: Option<String>, fut: F) -> F::Output {
    REPLAY.scope(tenant, fut).await
}

/// `Some(tenant)` inside [`checking`]; `None` for a replay that keeps the
/// writer-name check alone (the local CLI).
pub fn checked_for() -> Option<Option<String>> {
    REPLAY.try_with(Clone::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(rules: Value) -> MaskingSpec {
        serde_json::from_value(json!({ "key": "k", "rules": rules })).unwrap()
    }

    #[test]
    fn records_verify_under_current_and_previous_keys_only() {
        let s = spec(json!([
            {"match": {"fields": ["email"]}, "action": {"type": "hash"}},
            {"match": {"fields": ["x"]}, "action": {"type": "redact"}, "applies_to": ["other"]}
        ]));
        let ids = ["out", "jsonl"];
        let old = faucet_core::masking::proof::derive_key(b"old-secret", KEY_LABEL);
        let new = faucet_core::masking::proof::derive_key(b"new-secret", KEY_LABEL);
        let mut e = json!({"pipeline": "p", "row": "r", "payload": {"email": "h"}});
        proof(&old, &s, &ids, Some("t")).sign(&mut e);
        let env = faucet_core::unwrap_envelope(&e).unwrap();
        assert!(verified_with(
            &[new.clone(), old.clone()],
            &env,
            &s,
            &ids,
            Some("t")
        ));
        assert!(!verified_with(
            std::slice::from_ref(&new),
            &env,
            &s,
            &ids,
            Some("t")
        ));
        assert!(!verified_with(
            std::slice::from_ref(&old),
            &env,
            &s,
            &ids,
            None
        ));
        assert!(!verified_with(&[], &env, &s, &ids, Some("t")));
        assert_eq!(destination_rules(&s, &ids).as_array().unwrap().len(), 1);
        assert_eq!(
            destination_rules(&s, &["other"]).as_array().unwrap().len(),
            2
        );
        let mut bare = env.clone();
        bare.masking = None;
        assert!(!verified_with(&[old], &bare, &s, &ids, Some("t")));
    }

    #[tokio::test]
    async fn the_check_is_scoped_to_the_replay() {
        assert_eq!(checked_for(), None);
        let inside = checking(Some("t".into()), async { checked_for() }).await;
        assert_eq!(inside, Some(Some("t".into())));
    }
}

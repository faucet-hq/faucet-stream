//! Client options faucet sets itself, checked against `extra_client_config`.

use faucet_core::FaucetError;
use std::collections::BTreeMap;

/// One client option faucet derives from a typed config field.
///
/// `keys` are the option's names (aliases included); `accepts` says whether
/// an `extra_client_config` value agrees with what faucet sets; `owner` names
/// the typed field to use instead.
pub struct OwnedKey<'a> {
    /// The option's names, e.g. `["bootstrap.servers", "metadata.broker.list"]`.
    pub keys: &'a [&'a str],
    /// Whether a value (trimmed) agrees with faucet's own.
    pub accepts: &'a (dyn Fn(&str) -> bool + Sync),
    /// What to set instead, e.g. "set `group_id` instead".
    pub owner: &'a str,
}

/// Refuse an `extra_client_config` entry that sets one of `owned` to a value
/// that disagrees with what faucet derives from the typed config. Key names
/// match trimmed and ignoring ASCII case; the error names the key and never
/// its value.
pub fn check_owned_keys(
    connector: &str,
    extra: &BTreeMap<String, String>,
    owned: &[OwnedKey<'_>],
) -> Result<(), FaucetError> {
    for (key, value) in extra {
        let name = key.trim();
        let Some(rule) = owned
            .iter()
            .find(|o| o.keys.iter().any(|k| k.eq_ignore_ascii_case(name)))
        else {
            continue;
        };
        if !(rule.accepts)(value.trim()) {
            return Err(FaucetError::Config(format!(
                "{connector}: extra_client_config.{name} disagrees with what faucet sets: {}",
                rule.owner
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn extra(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn a_disagreeing_value_is_refused_under_any_alias_or_case() {
        let group = |v: &str| v == "g1";
        let commit = |v: &str| v.eq_ignore_ascii_case("false");
        let owned = [
            OwnedKey {
                keys: &["group.id"],
                accepts: &group,
                owner: "set `group_id` instead",
            },
            OwnedKey {
                keys: &["enable.auto.commit"],
                accepts: &commit,
                owner: "faucet commits offsets only after a page is written",
            },
        ];
        assert!(check_owned_keys("kafka source", &extra(&[("group.id", "g1")]), &owned).is_ok());
        assert!(
            check_owned_keys(
                "kafka source",
                &extra(&[("enable.auto.commit", " FALSE ")]),
                &owned
            )
            .is_ok()
        );
        assert!(
            check_owned_keys("kafka source", &extra(&[("fetch.min.bytes", "1")]), &owned).is_ok()
        );
        let err = check_owned_keys("kafka source", &extra(&[(" Group.ID ", "other")]), &owned)
            .unwrap_err()
            .to_string();
        assert!(err.contains("extra_client_config.Group.ID"), "{err}");
        assert!(err.contains("group_id"), "{err}");
        assert!(!err.contains("other"), "{err}");
        assert!(
            check_owned_keys(
                "kafka source",
                &extra(&[("enable.auto.commit", "true")]),
                &owned
            )
            .is_err()
        );
    }
}

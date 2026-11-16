//! What a feed keeps sealed with the server's vault key (#1034): the HMAC
//! secret of a feed made through the API, a delivery's secret template
//! params, and (when the feed says so) event payloads. Each is sealed to the
//! feed's uid and tenant, so it opens for that feed only. Without a vault key
//! such a value is refused. A `--feeds` file is the operator's own file and
//! is not sealed.

use crate::serve::history::feeds::{DeliveryTemplate, FeedSpec};
use crate::serve::schedules::secrets::{SEALED, is_keep_marker, is_sealed};
use crate::serve::state::ServerState;
use serde_json::{Value, json};

/// The text a stored signature secret starts with, and the whole text a view
/// shows in its place; sending it back keeps the stored secret.
pub const SEALED_TEXT: &str = "$sealed";

/// What a sealed value is bound to.
#[derive(Debug, Clone, Copy)]
pub struct Scope<'a> {
    /// The feed's uid.
    pub feed: &'a str,
    pub tenant: Option<&'a str>,
}

impl Scope<'_> {
    fn hmac(&self) -> String {
        format!(
            "feed:{}:tenant:{}:hmac",
            self.feed,
            self.tenant.unwrap_or("-")
        )
    }

    fn param(&self, delivery: &str, template: &str, param: &str) -> String {
        format!(
            "feed:{}:delivery:{delivery}:tenant:{}:template:{template}:param:{param}",
            self.feed,
            self.tenant.unwrap_or("-")
        )
    }
}

#[cfg(feature = "encryption")]
const NO_VAULT: &str =
    "this server has no vault key (start it with --vault-key or FAUCET_VAULT_KEY)";

#[cfg(feature = "encryption")]
fn seal_text(state: &ServerState, text: &str, context: &str) -> Result<String, String> {
    let vault = state.vault().ok_or(NO_VAULT)?;
    Ok(vault.seal_str_for(text, context))
}

#[cfg(not(feature = "encryption"))]
fn seal_text(state: &ServerState, text: &str, context: &str) -> Result<String, String> {
    let _ = (state, text, context);
    Err("this build has no `encryption` feature, so nothing can be sealed".into())
}

#[cfg(feature = "encryption")]
fn open_text(state: &ServerState, sealed: &str, context: &str) -> Result<String, String> {
    let vault = state.vault().ok_or(NO_VAULT)?;
    vault.open_str_for(sealed, context)
}

#[cfg(not(feature = "encryption"))]
fn open_text(state: &ServerState, sealed: &str, context: &str) -> Result<String, String> {
    let _ = (state, sealed, context);
    Err("this build has no `encryption` feature, so nothing sealed can be opened".into())
}

/// Seal `payload` for an event of the feed (`context` is
/// `faucet_common_feed::sealing_context`).
pub fn seal_payload(state: &ServerState, payload: &[u8], context: &str) -> Result<Vec<u8>, String> {
    seal_text(state, &hex(payload), context).map(String::into_bytes)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

#[cfg(feature = "encryption")]
fn unhex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) || !text.is_ascii() {
        return Err("the opened payload is not hex".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&text[i..i + 2], 16)
                .map_err(|_| "the opened payload is not hex".to_string())
        })
        .collect()
}

/// The opener a delivery run's source uses for sealed payloads; `None` when
/// the server has no vault key.
pub fn payload_opener(state: &ServerState) -> Option<faucet_common_feed::PayloadOpener> {
    #[cfg(feature = "encryption")]
    {
        let vault = state.vault()?;
        Some(std::sync::Arc::new(move |sealed: &[u8], context: &str| {
            let text = std::str::from_utf8(sealed).map_err(|_| {
                faucet_common_feed::OpenError("the sealed payload is not text".into())
            })?;
            let opened = vault
                .open_str_for(text, context)
                .map_err(faucet_common_feed::OpenError)?;
            unhex(&opened).map_err(faucet_common_feed::OpenError)
        }))
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = state;
        None
    }
}

/// Seal the HMAC secret of a feed made through the API, in place. The
/// [`SEALED_TEXT`] marker keeps the stored one (`current`); a `${…}`
/// reference is refused (references belong in the `--feeds` file). With
/// `presealed`, a value this server already sealed to `scope` is kept.
pub fn seal_signature(
    state: &ServerState,
    spec: &mut FeedSpec,
    current: Option<&FeedSpec>,
    scope: Scope<'_>,
    presealed: bool,
) -> Result<(), String> {
    let Some(sig) = spec.ingest.auth.signature.as_mut() else {
        return Ok(());
    };
    if presealed
        && let Some(sealed) = sig
            .secret
            .strip_prefix(SEALED_TEXT)
            .and_then(|s| s.strip_prefix(':'))
        && open_text(state, sealed, &scope.hmac()).is_ok()
    {
        return Ok(());
    }
    if sig.secret == SEALED_TEXT {
        let stored = current
            .and_then(|c| c.ingest.auth.signature.as_ref())
            .map(|s| s.secret.clone())
            .filter(|s| s.starts_with(SEALED_TEXT) && s.len() > SEALED_TEXT.len() + 1)
            .ok_or("ingest.auth.signature.secret: there is no stored secret to keep")?;
        open_text(state, &stored[SEALED_TEXT.len() + 1..], &scope.hmac()).map_err(|e| {
            format!("ingest.auth.signature.secret: the stored secret can no longer be opened ({e}); supply it again")
        })?;
        sig.secret = stored;
        return Ok(());
    }
    if sig.secret.starts_with(SEALED_TEXT) {
        return Err(format!(
            "ingest.auth.signature.secret: send the secret itself, or \"{SEALED_TEXT}\" to keep the stored one"
        ));
    }
    if sig.secret.contains("${") {
        return Err(
            "ingest.auth.signature.secret: a `${…}` reference is only resolved in the --feeds \
             file; send the secret itself (it is stored sealed)"
                .into(),
        );
    }
    let sealed = seal_text(state, &sig.secret, &scope.hmac()).map_err(|e| {
        format!(
            "ingest.auth.signature.secret is stored sealed with the server's vault key, and {e}"
        )
    })?;
    crate::secrets::registry::register_server(&sig.secret);
    sig.secret = format!("{SEALED_TEXT}:{sealed}");
    Ok(())
}

/// Open a stored feed's HMAC secret in place, for the server's own use.
pub fn open_signature(state: &ServerState, spec: &mut FeedSpec, feed: &str) -> Result<(), String> {
    let tenant = spec.tenant.clone();
    let Some(sig) = spec.ingest.auth.signature.as_mut() else {
        return Ok(());
    };
    let Some(sealed) = sig
        .secret
        .strip_prefix(SEALED_TEXT)
        .and_then(|s| s.strip_prefix(':'))
    else {
        return Ok(());
    };
    let scope = Scope {
        feed,
        tenant: tenant.as_deref(),
    };
    let opened = open_text(state, sealed, &scope.hmac())
        .map_err(|e| format!("the feed's signature secret could not be opened: {e}"))?;
    crate::secrets::registry::register_server(&opened);
    sig.secret = opened;
    Ok(())
}

/// `spec` as a response shows it: the signature secret and every sealed
/// param replaced by a marker.
pub fn masked(spec: &FeedSpec) -> FeedSpec {
    let mut out = spec.clone();
    if let Some(sig) = out.ingest.auth.signature.as_mut() {
        sig.secret = SEALED_TEXT.to_string();
    }
    for d in &mut out.deliveries {
        if let Some(t) = d.template.as_mut() {
            crate::serve::schedules::secrets::mask_params(&mut t.params);
        }
    }
    out
}

/// Seal a delivery's values for the params `secret` names, in place. A keep
/// marker takes the value from `current`, unless the template changed. A
/// value already sealed is accepted only when `presealed` (a change request
/// this server sealed) and only when it opens under `scope`.
pub fn seal_params(
    state: &ServerState,
    template: &mut DeliveryTemplate,
    delivery: &str,
    secret: &[String],
    current: Option<&DeliveryTemplate>,
    scope: Scope<'_>,
    presealed: bool,
) -> Result<(), String> {
    let id = template.id.clone();
    let moved = current.is_some_and(|c| c.id != id);
    let resupply: Vec<String> = template
        .params
        .iter()
        .filter(|(_, v)| moved && is_keep_marker(v))
        .map(|(k, _)| format!("template.params.{k}"))
        .collect();
    if !resupply.is_empty() {
        return Err(format!(
            "the template changed, so the stored secret values no longer apply; supply {} again",
            resupply.join(", ")
        ));
    }
    for (k, v) in template.params.iter_mut() {
        let context = scope.param(delivery, &id, k);
        if is_keep_marker(v) {
            let stored = current
                .and_then(|c| c.params.get(k))
                .filter(|s| is_sealed(s))
                .ok_or_else(|| format!("template.params.{k}: there is no stored value to keep"))?;
            open_param(state, stored, &context).map_err(|e| {
                format!("template.params.{k}: {e}; the stored value can no longer be opened, so supply it again")
            })?;
            *v = stored.clone();
            continue;
        }
        if is_sealed(v) {
            if presealed && open_param(state, v, &context).is_ok() {
                continue;
            }
            return Err(format!(
                "template.params.{k}: send the secret value itself, or {{\"{SEALED}\": true}} to keep the stored one"
            ));
        }
        if !secret.iter().any(|s| s == k) || v.is_null() {
            continue;
        }
        let text = serde_json::to_string(v).map_err(|e| e.to_string())?;
        let sealed = seal_text(state, &text, &context).map_err(|e| {
            format!(
                "template.params.{k} is a secret param: a delivery stores it sealed with the \
                 server's vault key, and {e}"
            )
        })?;
        *v = json!({ SEALED: sealed });
    }
    Ok(())
}

fn open_param(state: &ServerState, v: &Value, context: &str) -> Result<Value, String> {
    let text = v
        .get(SEALED)
        .and_then(Value::as_str)
        .ok_or("the value is not sealed")?;
    let opened = open_text(state, text, context)?;
    serde_json::from_str(&opened).map_err(|e| e.to_string())
}

/// `template` with its sealed params opened, for one run.
pub fn opened_params(
    state: &ServerState,
    template: &DeliveryTemplate,
    delivery: &str,
    scope: Scope<'_>,
) -> Result<DeliveryTemplate, String> {
    let mut out = template.clone();
    for (k, v) in out.params.iter_mut() {
        if is_sealed(v) {
            *v = open_param(state, v, &scope.param(delivery, &template.id, k)).map_err(|e| {
                format!("template.params.{k} could not be opened: {e}; edit the delivery and supply its secret params again")
            })?;
        }
    }
    Ok(out)
}

#[cfg(all(test, feature = "encryption"))]
mod tests {
    use super::*;
    use crate::serve::test_support::{state_from, test_config, test_state};

    fn vaulted() -> ServerState {
        let state = state_from(&test_config());
        state.set_vault(Some(std::sync::Arc::new(
            crate::serve::vault::Vault::new("test-vault-key-0123456789abcdef0123", &[]).unwrap(),
        )));
        state
    }

    fn spec(secret: &str) -> FeedSpec {
        serde_json::from_value(json!({
            "name": "orders",
            "ingest": {"auth": {"signature": {"header": "X-Sig", "secret": secret}}},
            "deliveries": [{"name": "lake", "template": {"id": "t", "params": {"token": "tok", "n": 3}}}]
        }))
        .unwrap()
    }

    #[test]
    fn a_signature_secret_is_stored_sealed_and_opens_for_its_feed_only() {
        let state = vaulted();
        let scope = Scope {
            feed: "f1",
            tenant: None,
        };
        let mut s = spec("hmac-secret");
        seal_signature(&state, &mut s, None, scope, false).unwrap();
        let stored = s.ingest.auth.signature.as_ref().unwrap().secret.clone();
        assert!(stored.starts_with("$sealed:") && !stored.contains("hmac-secret"));
        assert_eq!(
            masked(&s).ingest.auth.signature.unwrap().secret,
            SEALED_TEXT
        );

        let mut opened = s.clone();
        open_signature(&state, &mut opened, "f1").unwrap();
        assert_eq!(
            opened.ingest.auth.signature.as_ref().unwrap().secret,
            "hmac-secret"
        );
        let mut other = s.clone();
        assert!(
            open_signature(&state, &mut other, "f2")
                .unwrap_err()
                .contains("could not be opened")
        );

        // The marker keeps the stored secret; anything else that looks sealed is refused.
        let mut keep = spec(SEALED_TEXT);
        seal_signature(&state, &mut keep, Some(&s), scope, false).unwrap();
        assert_eq!(keep.ingest.auth.signature.as_ref().unwrap().secret, stored);
        let mut nothing = spec(SEALED_TEXT);
        assert!(
            seal_signature(&state, &mut nothing, None, scope, false)
                .unwrap_err()
                .contains("no stored secret")
        );
        let mut wrong = spec(SEALED_TEXT);
        let elsewhere = Scope {
            feed: "f9",
            tenant: None,
        };
        assert!(
            seal_signature(&state, &mut wrong, Some(&s), elsewhere, false)
                .unwrap_err()
                .contains("can no longer be opened")
        );
        let mut forged = spec("$sealed:abc");
        assert!(
            seal_signature(&state, &mut forged, None, scope, false)
                .unwrap_err()
                .contains("send the secret itself")
        );
        let mut reference = spec("${secret:HMAC}");
        assert!(
            seal_signature(&state, &mut reference, None, scope, false)
                .unwrap_err()
                .contains("--feeds")
        );

        // No vault key: refused; a plain or absent secret is left alone.
        let bare = test_state();
        let mut s2 = spec("hmac-secret");
        assert!(
            seal_signature(&bare, &mut s2, None, scope, false)
                .unwrap_err()
                .contains("vault key")
        );
        let mut plain = spec("plain");
        open_signature(&bare, &mut plain, "f1").unwrap();
        let mut none: FeedSpec =
            serde_json::from_value(json!({"name": "a", "ingest": {"auth": {"tokens": "managed"}}}))
                .unwrap();
        seal_signature(&bare, &mut none, None, scope, false).unwrap();
        open_signature(&bare, &mut none, "f1").unwrap();
    }

    #[test]
    fn secret_params_are_sealed_per_delivery_and_template() {
        let state = vaulted();
        let scope = Scope {
            feed: "f1",
            tenant: Some("acme"),
        };
        let mut t = spec("x").deliveries[0].template.clone().unwrap();
        seal_params(&state, &mut t, "d1", &["token".into()], None, scope, false).unwrap();
        assert!(is_sealed(&t.params["token"]));
        assert_eq!(t.params["n"], json!(3));
        let opened = opened_params(&state, &t, "d1", scope).unwrap();
        assert_eq!(opened.params["token"], json!("tok"));
        assert!(
            opened_params(&state, &t, "d2", scope)
                .unwrap_err()
                .contains("could not be opened")
        );

        // Keep marker: the stored value, unless the template changed.
        let mut edit = t.clone();
        edit.params.insert("token".into(), json!({SEALED: true}));
        seal_params(
            &state,
            &mut edit,
            "d1",
            &["token".into()],
            Some(&t),
            scope,
            false,
        )
        .unwrap();
        assert_eq!(edit.params["token"], t.params["token"]);
        let mut moved = edit.clone();
        moved.id = "other".into();
        moved.params.insert("token".into(), json!({SEALED: true}));
        assert!(
            seal_params(
                &state,
                &mut moved,
                "d1",
                &["token".into()],
                Some(&t),
                scope,
                false
            )
            .unwrap_err()
            .contains("the template changed")
        );
        let mut nothing = t.clone();
        nothing.params.insert("token".into(), json!({SEALED: true}));
        assert!(
            seal_params(
                &state,
                &mut nothing,
                "d1",
                &["token".into()],
                None,
                scope,
                false
            )
            .unwrap_err()
            .contains("no stored value")
        );
        let mut other_delivery = t.clone();
        other_delivery
            .params
            .insert("token".into(), json!({SEALED: true}));
        assert!(
            seal_params(
                &state,
                &mut other_delivery,
                "d2",
                &["token".into()],
                Some(&t),
                scope,
                false
            )
            .unwrap_err()
            .contains("can no longer be opened")
        );

        // A sealed value from the wire is refused; one this server sealed is kept.
        let mut wire = t.clone();
        assert!(
            seal_params(
                &state,
                &mut wire,
                "d1",
                &["token".into()],
                None,
                scope,
                false
            )
            .unwrap_err()
            .contains("send the secret value itself")
        );
        seal_params(
            &state,
            &mut wire,
            "d1",
            &["token".into()],
            None,
            scope,
            true,
        )
        .unwrap();
        let mut null = t.clone();
        null.params.insert("token".into(), Value::Null);
        seal_params(
            &state,
            &mut null,
            "d1",
            &["token".into()],
            None,
            scope,
            false,
        )
        .unwrap();
        assert!(null.params["token"].is_null());

        let masked_spec = {
            let mut s = spec("x");
            s.deliveries[0].template = Some(t.clone());
            masked(&s)
        };
        assert_eq!(
            masked_spec.deliveries[0].template.as_ref().unwrap().params["token"],
            json!({SEALED: true})
        );
        let bare = test_state();
        let mut t2 = spec("x").deliveries[0].template.clone().unwrap();
        assert!(
            seal_params(&bare, &mut t2, "d1", &["token".into()], None, scope, false)
                .unwrap_err()
                .contains("vault key")
        );
    }

    #[test]
    fn a_payload_seals_and_opens_under_the_feeds_context() {
        let state = vaulted();
        let feed = faucet_common_feed::FeedUid::new("f1").unwrap();
        let context = faucet_common_feed::sealing_context(&feed, None);
        let sealed = seal_payload(&state, b"{\"a\":1}\xff", &context).unwrap();
        assert!(!sealed.windows(3).any(|w| w == b"\"a\""));
        let open = payload_opener(&state).unwrap();
        assert_eq!(open(&sealed, &context).unwrap(), b"{\"a\":1}\xff");
        assert!(open(&sealed, "feed:other:tenant:-:event").is_err());
        assert!(
            open(b"\xff\xfe", &context)
                .unwrap_err()
                .to_string()
                .contains("not text")
        );
        assert!(payload_opener(&test_state()).is_none());
        assert!(
            seal_payload(&test_state(), b"x", &context)
                .unwrap_err()
                .contains("vault key")
        );
        assert_eq!(unhex("zz").unwrap_err(), "the opened payload is not hex");
        assert_eq!(unhex("abc").unwrap_err(), "the opened payload is not hex");
    }
}

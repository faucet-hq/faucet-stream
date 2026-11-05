//! Secret template params of a stored schedule (#1044). A schedule made
//! through the API keeps its param values in the run history, so a value
//! for a param its template declares `secret: true` is sealed with the
//! server's vault key before it is stored, and opened only when a tick
//! fires. Responses show `{"$sealed": true}` in its place; an edit that sends
//! that marker back keeps the stored value. Without a vault key such a value
//! is refused. (A `--schedules` file is the operator's own file, like the
//! triggers file, and is not sealed.)

use crate::serve::history::schedules::ScheduleSpec;
use crate::serve::state::ServerState;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// The key of a sealed value's marker object.
pub const SEALED: &str = "$sealed";

fn context(schedule: &str, param: &str) -> String {
    format!("schedule:{schedule}:param:{param}")
}

/// Whether `v` is a sealed value (`{"$sealed": "<ciphertext>"}`).
pub fn is_sealed(v: &Value) -> bool {
    sealed_text(v).is_some()
}

fn sealed_text(v: &Value) -> Option<&str> {
    match v {
        Value::Object(m) if m.len() == 1 => m.get(SEALED).and_then(Value::as_str),
        _ => None,
    }
}

/// Whether `v` is the "keep the stored value" marker a view hands out.
pub fn is_keep_marker(v: &Value) -> bool {
    matches!(v, Value::Object(m) if m.len() == 1 && m.get(SEALED) == Some(&Value::Bool(true)))
}

/// `spec` with every sealed value replaced by the keep marker — what a
/// response shows.
pub fn masked(spec: &ScheduleSpec) -> ScheduleSpec {
    let mut out = spec.clone();
    for v in out.template.params.values_mut() {
        if is_sealed(v) {
            *v = json!({ SEALED: true });
        }
    }
    out
}

/// Seal `spec`'s values for the params `secret` names, in place. A keep
/// marker takes the value from `current` (the stored definition being
/// edited); a value already sealed is kept as it is.
pub fn seal(
    state: &ServerState,
    spec: &mut ScheduleSpec,
    secret: &[String],
    current: Option<&ScheduleSpec>,
) -> Result<(), String> {
    let name = spec.name.clone();
    let params: &mut BTreeMap<String, Value> = &mut spec.template.params;
    for (k, v) in params.iter_mut() {
        if is_keep_marker(v) {
            let stored = current
                .and_then(|c| c.template.params.get(k))
                .filter(|s| is_sealed(s))
                .ok_or_else(|| format!("template.params.{k}: there is no stored value to keep"))?;
            *v = stored.clone();
            continue;
        }
        if is_sealed(v) {
            return Err(format!(
                "template.params.{k}: send the secret value itself, or {{\"{SEALED}\": true}} to keep the stored one"
            ));
        }
        if !secret.iter().any(|s| s == k) || v.is_null() {
            continue;
        }
        *v = seal_value(state, &name, k, v)?;
    }
    Ok(())
}

#[cfg(feature = "encryption")]
fn seal_value(
    state: &ServerState,
    schedule: &str,
    param: &str,
    v: &Value,
) -> Result<Value, String> {
    let vault = state.vault().ok_or_else(|| no_vault(param))?;
    Ok(json!({ SEALED: vault.seal_for(v, &context(schedule, param)) }))
}

#[cfg(not(feature = "encryption"))]
fn seal_value(
    state: &ServerState,
    schedule: &str,
    param: &str,
    v: &Value,
) -> Result<Value, String> {
    let _ = (state, schedule, v, context(schedule, param));
    Err(no_vault(param))
}

fn no_vault(param: &str) -> String {
    format!(
        "template.params.{param} is a secret param: a schedule stores it sealed with the server's \
         vault key, and this server has none (start it with --vault-key or FAUCET_VAULT_KEY)"
    )
}

/// `spec` with its sealed values opened, for one tick.
pub fn opened(state: &ServerState, spec: &ScheduleSpec) -> Result<ScheduleSpec, String> {
    if !spec.template.params.values().any(is_sealed) {
        return Ok(spec.clone());
    }
    let mut out = spec.clone();
    for (k, v) in out.template.params.iter_mut() {
        if let Some(text) = sealed_text(v) {
            *v = open_value(state, &spec.name, k, text)?;
        }
    }
    Ok(out)
}

#[cfg(feature = "encryption")]
fn open_value(
    state: &ServerState,
    schedule: &str,
    param: &str,
    text: &str,
) -> Result<Value, String> {
    let vault = state.vault().ok_or_else(|| {
        format!("template.params.{param} is sealed and this server has no vault key")
    })?;
    vault
        .open_for(text, &context(schedule, param))
        .map_err(|e| format!("template.params.{param} could not be opened: {e}"))
}

#[cfg(not(feature = "encryption"))]
fn open_value(
    state: &ServerState,
    schedule: &str,
    param: &str,
    text: &str,
) -> Result<Value, String> {
    let _ = (state, schedule, text);
    Err(format!(
        "template.params.{param} is sealed and this build has no `encryption` feature"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(params: Value) -> ScheduleSpec {
        serde_json::from_value(json!({
            "name": "n", "cron": "0 * * * *", "template": {"id": "t", "params": params}
        }))
        .unwrap()
    }

    #[test]
    fn markers_and_masking() {
        assert!(is_sealed(&json!({"$sealed": "abc"})));
        assert!(!is_sealed(&json!({"$sealed": true})));
        assert!(is_keep_marker(&json!({"$sealed": true})));
        assert!(!is_keep_marker(&json!({"$sealed": true, "x": 1})));
        let m = masked(&spec(json!({"token": {"$sealed": "abc"}, "region": "eu"})));
        assert_eq!(m.template.params["token"], json!({"$sealed": true}));
        assert_eq!(m.template.params["region"], json!("eu"));
    }

    #[test]
    fn sealing_needs_a_vault_and_keeps_stored_values() {
        let state = crate::serve::test_support::test_state();
        let mut s = spec(json!({"token": "secret", "region": "eu", "none": null}));
        // No vault key on the test server.
        let e = seal(&state, &mut s, &["token".into()], None).unwrap_err();
        assert!(e.contains("--vault-key"), "{e}");
        // Not a secret param: untouched.
        let mut plain = spec(json!({"region": "eu"}));
        seal(&state, &mut plain, &["token".into()], None).unwrap();
        assert_eq!(plain.template.params["region"], json!("eu"));
        // Keep marker: taken from the stored definition.
        let stored = spec(json!({"token": {"$sealed": "cipher"}}));
        let mut edit = spec(json!({"token": {"$sealed": true}}));
        seal(&state, &mut edit, &["token".into()], Some(&stored)).unwrap();
        assert_eq!(edit.template.params["token"], json!({"$sealed": "cipher"}));
        let mut orphan = spec(json!({"token": {"$sealed": true}}));
        assert!(seal(&state, &mut orphan, &["token".into()], None).is_err());
        // A caller may not hand in ciphertext.
        let mut forged = spec(json!({"token": {"$sealed": "cipher"}}));
        assert!(seal(&state, &mut forged, &["token".into()], None).is_err());
        // Opening without a vault fails; nothing sealed opens as is.
        assert!(opened(&state, &stored).is_err());
        assert_eq!(opened(&state, &plain).unwrap(), plain);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn a_sealed_value_round_trips_through_the_vault() {
        let state = crate::serve::test_support::test_state();
        state.set_vault(Some(std::sync::Arc::new(
            crate::serve::vault::Vault::new(&"k".repeat(32), &[]).unwrap(),
        )));
        let mut s = spec(json!({"token": "secret"}));
        seal(&state, &mut s, &["token".into()], None).unwrap();
        assert!(is_sealed(&s.template.params["token"]));
        assert!(!s.template.params["token"].to_string().contains("secret"));
        assert_eq!(
            opened(&state, &s).unwrap().template.params["token"],
            json!("secret")
        );
        // Sealed for another schedule: refused.
        let mut other = s.clone();
        other.name = "other".into();
        assert!(opened(&state, &other).is_err());
    }
}

//! Sealed `secret: true` param values of a clustered template run (#956).
//!
//! A clustered server persists a run's materialized config so whichever
//! member claims it can execute it. Before that, every caller-supplied secret
//! value in the body is replaced by a token (`${sealed:N}`, or
//! `${sealed:N:number}` for a whole numeric value), and the values themselves
//! are sealed under the server's vault key with the run id as associated data
//! and stored apart from the run record (`run_secrets_*`). The claiming member
//! opens them in memory, registers them for redaction and restores the body
//! just before loading it. They are deleted once the run is terminal.

use crate::serve::error::ServeError;
use crate::serve::state::ServerState;
use serde_json::Value;
use std::borrow::Cow;

/// The start of every token this module writes into a persisted body.
pub const TOKEN_PREFIX: &str = "${sealed:";
const NUMBER_SUFFIX: &str = ":number";

/// Why a clustered trigger with `secret: true` params needs the vault key.
pub const NO_VAULT_HINT: &str = "start every cluster member with the same vault key \
     (`--vault-key` / `FAUCET_VAULT_KEY`) so the values are stored encrypted";

/// A persisted body with its secret values replaced by tokens, and the values
/// the tokens stand for. `Debug` never prints the values.
#[derive(Clone, PartialEq, Eq)]
pub struct SealedParams {
    body: String,
    values: Vec<String>,
}

impl std::fmt::Debug for SealedParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedParams")
            .field("body", &self.body)
            .field("values", &format!("[{} sealed]", self.values.len()))
            .finish()
    }
}

impl SealedParams {
    /// The body that is persisted: tokens, never values.
    pub fn body(&self) -> &str {
        &self.body
    }

    /// The values the tokens stand for, by token index.
    pub fn values(&self) -> &[String] {
        &self.values
    }

    /// `text` with every secret value replaced by `***` (a run name or label
    /// derived from the config, which is stored as text, not restored).
    pub fn redact_text(&self, text: &str) -> String {
        let patterns = ordered(&self.values);
        replace_all(text, &patterns, |_| "***".to_string())
    }
}

/// The secret values worth sealing, longest first (so a value containing
/// another is matched whole), with their index in `values`.
fn ordered(values: &[String]) -> Vec<(usize, &str)> {
    let mut v: Vec<(usize, &str)> = values
        .iter()
        .enumerate()
        .map(|(i, s)| (i, s.as_str()))
        .collect();
    v.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then(a.0.cmp(&b.0)));
    v
}

/// One left-to-right pass replacing each occurrence of a pattern; replaced
/// text is never scanned again.
fn replace_all(text: &str, patterns: &[(usize, &str)], token: impl Fn(usize) -> String) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    'scan: while let Some(c) = rest.chars().next() {
        for (i, p) in patterns {
            if rest.starts_with(p) {
                out.push_str(&token(*i));
                rest = &rest[p.len()..];
                continue 'scan;
            }
        }
        out.push(c);
        rest = &rest[c.len_utf8()..];
    }
    out
}

/// Replace every secret value in a JSON `body` by a token. `None` when no
/// value occurs in it (nothing to seal). Empty values, booleans and values
/// that are themselves directives (a deferred `${env:…}` default) are not
/// secret material and are left alone.
pub fn tokenize(body: &str, secrets: &[String]) -> Result<Option<SealedParams>, String> {
    if body.contains(TOKEN_PREFIX) {
        return Err(format!(
            "the materialized config contains the reserved text `{TOKEN_PREFIX}`"
        ));
    }
    let mut values: Vec<String> = Vec::new();
    for s in secrets {
        if s.is_empty() || s == "true" || s == "false" || s.contains("${") {
            continue;
        }
        if !values.contains(s) {
            values.push(s.clone());
        }
    }
    if values.is_empty() {
        return Ok(None);
    }
    let doc: Value =
        serde_json::from_str(body).map_err(|e| format!("materialized config is not JSON: {e}"))?;
    let patterns = ordered(&values);
    let mut changed = false;
    let doc = tokenize_value(doc, &patterns, &mut changed);
    if !changed {
        return Ok(None);
    }
    let body = serde_json::to_string(&doc).map_err(|e| format!("re-serializing config: {e}"))?;
    Ok(Some(SealedParams { body, values }))
}

fn tokenize_str(s: &str, patterns: &[(usize, &str)], changed: &mut bool) -> String {
    let out = replace_all(s, patterns, |i| format!("{TOKEN_PREFIX}{i}}}"));
    if out != s {
        *changed = true;
    }
    out
}

fn tokenize_value(v: Value, patterns: &[(usize, &str)], changed: &mut bool) -> Value {
    match v {
        Value::String(s) => Value::String(tokenize_str(&s, patterns, changed)),
        Value::Number(n) => {
            let text = n.to_string();
            match patterns.iter().find(|(_, p)| *p == text) {
                Some((i, _)) => {
                    *changed = true;
                    Value::String(format!("{TOKEN_PREFIX}{i}{NUMBER_SUFFIX}}}"))
                }
                None => Value::Number(n),
            }
        }
        Value::Array(a) => Value::Array(
            a.into_iter()
                .map(|x| tokenize_value(x, patterns, changed))
                .collect(),
        ),
        Value::Object(o) => Value::Object(
            o.into_iter()
                .map(|(k, x)| {
                    (
                        tokenize_str(&k, patterns, changed),
                        tokenize_value(x, patterns, changed),
                    )
                })
                .collect(),
        ),
        other => other,
    }
}

/// Whether a persisted body carries sealed tokens.
pub fn is_sealed(body: &str) -> bool {
    body.contains(TOKEN_PREFIX)
}

/// The token at the start of `s`: its index and whether it stands for a
/// whole number, plus its length.
fn parse_token(s: &str) -> Result<(usize, bool, usize), String> {
    let malformed = || {
        format!(
            "malformed sealed token in `{}`",
            s.chars().take(40).collect::<String>()
        )
    };
    let rest = s.strip_prefix(TOKEN_PREFIX).ok_or_else(malformed)?;
    let digits = rest.chars().take_while(char::is_ascii_digit).count();
    if digits == 0 {
        return Err(malformed());
    }
    let index: usize = rest[..digits].parse().map_err(|_| malformed())?;
    let tail = &rest[digits..];
    if tail.starts_with('}') {
        Ok((index, false, TOKEN_PREFIX.len() + digits + 1))
    } else if tail.starts_with(NUMBER_SUFFIX) && tail[NUMBER_SUFFIX.len()..].starts_with('}') {
        Ok((
            index,
            true,
            TOKEN_PREFIX.len() + digits + NUMBER_SUFFIX.len() + 1,
        ))
    } else {
        Err(malformed())
    }
}

fn value_at(values: &[String], i: usize) -> Result<&str, String> {
    values.get(i).map(String::as_str).ok_or_else(|| {
        format!(
            "sealed token {i} has no value (the run holds {})",
            values.len()
        )
    })
}

fn restore_str(s: &str, values: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(at) = rest.find(TOKEN_PREFIX) {
        out.push_str(&rest[..at]);
        let (i, _, len) = parse_token(&rest[at..])?;
        out.push_str(value_at(values, i)?);
        rest = &rest[at + len..];
    }
    out.push_str(rest);
    Ok(out)
}

fn restore_value(v: Value, values: &[String]) -> Result<Value, String> {
    Ok(match v {
        Value::String(s) => {
            if s.starts_with(TOKEN_PREFIX)
                && let Ok((i, true, len)) = parse_token(&s)
                && len == s.len()
            {
                let text = value_at(values, i)?;
                let n: serde_json::Number = text
                    .parse()
                    .map_err(|_| format!("sealed token {i} is not a number"))?;
                Value::Number(n)
            } else {
                Value::String(restore_str(&s, values)?)
            }
        }
        Value::Array(a) => Value::Array(
            a.into_iter()
                .map(|x| restore_value(x, values))
                .collect::<Result<_, _>>()?,
        ),
        Value::Object(o) => Value::Object(
            o.into_iter()
                .map(|(k, x)| Ok((restore_str(&k, values)?, restore_value(x, values)?)))
                .collect::<Result<_, String>>()?,
        ),
        other => other,
    })
}

/// The body [`tokenize`] started from: every token replaced by its value.
pub fn restore(body: &str, values: &[String]) -> Result<String, String> {
    let doc: Value =
        serde_json::from_str(body).map_err(|e| format!("sealed config is not JSON: {e}"))?;
    let doc = restore_value(doc, values)?;
    serde_json::to_string(&doc).map_err(|e| format!("re-serializing config: {e}"))
}

/// The associated data a run's sealed values are bound to.
pub fn context(run_id: &str) -> String {
    format!("run:{run_id}:secret-params")
}

/// Whether this server can seal secret params (it has a vault key).
pub fn vault_configured(state: &ServerState) -> bool {
    #[cfg(feature = "encryption")]
    {
        state.vault().is_some()
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = state;
        false
    }
}

fn history_error(e: crate::serve::history::HistoryError) -> ServeError {
    match e {
        crate::serve::history::HistoryError::Degraded(m) => ServeError::Unavailable(m),
        e => ServeError::Internal(e.to_string()),
    }
}

/// Seal `sealed`'s values for `run_id` and store them, before the run record
/// that refers to them is written.
pub async fn store(
    state: &ServerState,
    run_id: &str,
    sealed: &SealedParams,
) -> Result<(), ServeError> {
    #[cfg(feature = "encryption")]
    {
        let vault = state.vault().ok_or_else(|| ServeError::Unprocessable {
            message: format!("this server has no vault key: {NO_VAULT_HINT}"),
            details: None,
        })?;
        let text = vault.seal_for(
            &serde_json::json!({ "values": sealed.values }),
            &context(run_id),
        );
        state
            .history()
            .run_secrets_put(run_id, &text)
            .await
            .map_err(history_error)
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = (state, run_id, sealed);
        Err(ServeError::Unprocessable {
            message: format!(
                "sealing secret params needs a build with the `encryption` feature: {NO_VAULT_HINT}"
            ),
            details: None,
        })
    }
}

/// Drop `run_id`'s sealed values (best-effort: the terminal purge catches
/// what this misses).
pub async fn forget(state: &ServerState, run_id: &str) {
    if let Err(e) = state.history().run_secrets_delete(run_id).await {
        tracing::warn!(run_id, error = %e, "deleting a run's sealed secret params failed");
    }
}

#[cfg(feature = "encryption")]
fn open_stored(state: &ServerState, run_id: &str, stored: &str) -> Result<Value, String> {
    open_with(state.vault().as_deref(), run_id, stored)
}

#[cfg(not(feature = "encryption"))]
fn open_stored(state: &ServerState, run_id: &str, stored: &str) -> Result<Value, String> {
    let _ = (state, run_id, stored);
    Err(format!(
        "this instance was built without the `encryption` feature: {NO_VAULT_HINT}"
    ))
}

/// The body a claiming member loads: `body` itself, or — when it carries
/// sealed tokens — the body with `run_id`'s values opened and restored. A
/// member that cannot open them fails the run rather than run the tokens.
pub async fn unseal<'a>(
    state: &ServerState,
    run_id: &str,
    body: &'a str,
) -> Result<Cow<'a, str>, ServeError> {
    unseal_with(
        state.history().as_ref(),
        |stored| open_stored(state, run_id, stored),
        run_id,
        body,
    )
    .await
}

/// [`unseal`] over a history store and an `open` that decrypts the stored
/// payload (for a component that holds the vault key but no server state).
pub async fn unseal_with<'a>(
    history: &dyn crate::serve::history::RunHistory,
    open: impl FnOnce(&str) -> Result<Value, String>,
    run_id: &str,
    body: &'a str,
) -> Result<Cow<'a, str>, ServeError> {
    if !is_sealed(body) {
        return Ok(Cow::Borrowed(body));
    }
    let refuse = |m: String| ServeError::Unprocessable {
        message: format!("this run's `secret: true` param values are sealed, and {m}"),
        details: None,
    };
    let stored = history
        .run_secrets_get(run_id)
        .await
        .map_err(history_error)?
        .ok_or_else(|| {
            refuse(
                "they are no longer stored (they are deleted once a run finishes) — trigger the \
                 template again"
                    .into(),
            )
        })?;
    let opened = open(&stored).map_err(refuse)?;
    let values: Vec<String> = serde_json::from_value(opened["values"].clone())
        .map_err(|e| refuse(format!("their sealed payload is malformed: {e}")))?;
    for v in &values {
        crate::secrets::registry::register(v);
    }
    restore(body, &values)
        .map(Cow::Owned)
        .map_err(|e| refuse(format!("the stored config does not match them: {e}")))
}

/// Decrypt a run's stored payload with `vault` (`None`: no key here).
#[cfg(feature = "encryption")]
pub fn open_with(
    vault: Option<&crate::serve::vault::Vault>,
    run_id: &str,
    stored: &str,
) -> Result<Value, String> {
    vault
        .ok_or_else(|| format!("this instance was started without a vault key: {NO_VAULT_HINT}"))?
        .open_for(stored, &context(run_id))
        .map_err(|e| format!("this instance cannot open them: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn tokenize_hides_every_occurrence_and_restore_is_exact() {
        let body = serde_json::json!({
            "pipeline": {
                "source": { "config": { "password": "hunter2-pw", "url": "pg://u:hunter2-pw@h/db" } },
                "sink": { "config": { "port": 5432, "pin": 9137, "key": "k-hunter2-pw-k", "flag": true } }
            },
            "hunter2-pw": ["a", "hunter2-pw"],
            "env": "${env:X}"
        })
        .to_string();
        let sealed = tokenize(
            &body,
            &texts(&["hunter2-pw", "9137", "", "true", "${env:X}", "hunter2-pw"]),
        )
        .unwrap()
        .unwrap();
        assert!(!sealed.body().contains("hunter2-pw"), "{}", sealed.body());
        assert!(!sealed.body().contains("9137"), "{}", sealed.body());
        assert!(sealed.body().contains("${env:X}"), "directives stay");
        assert!(sealed.body().contains("5432"));
        assert!(
            sealed.body().contains("${sealed:1:number}"),
            "{}",
            sealed.body()
        );
        assert_eq!(sealed.values(), &texts(&["hunter2-pw", "9137"])[..]);
        let back: Value =
            serde_json::from_str(&restore(sealed.body(), sealed.values()).unwrap()).unwrap();
        let orig: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(back, orig);
        assert!(format!("{sealed:?}").contains("[2 sealed]"));
        assert!(!format!("{sealed:?}").contains("hunter2-pw"));
        assert_eq!(sealed.redact_text("run hunter2-pw-x"), "run ***-x");
    }

    #[test]
    fn the_longest_value_wins_and_tokens_are_never_rescanned() {
        let body = serde_json::json!({ "a": "abcdef", "b": "xab", "c": "sealed" }).to_string();
        let sealed = tokenize(&body, &texts(&["ab", "abcdef", "e"]))
            .unwrap()
            .unwrap();
        let doc: Value = serde_json::from_str(sealed.body()).unwrap();
        assert_eq!(doc["a"], "${sealed:1}");
        assert_eq!(doc["b"], "x${sealed:0}");
        assert_eq!(doc["c"], "s${sealed:2}al${sealed:2}d");
        let back: Value =
            serde_json::from_str(&restore(sealed.body(), sealed.values()).unwrap()).unwrap();
        assert_eq!(back, serde_json::from_str::<Value>(&body).unwrap());
    }

    #[test]
    fn nothing_to_seal_yields_none() {
        let body = r#"{"a":"plain"}"#;
        assert!(tokenize(body, &texts(&["absent"])).unwrap().is_none());
        assert!(tokenize(body, &texts(&["", "false"])).unwrap().is_none());
        assert!(!is_sealed(body));
    }

    #[test]
    fn reserved_text_and_bad_input_are_refused() {
        let err = tokenize(r#"{"a":"${sealed:0}"}"#, &texts(&["x"])).unwrap_err();
        assert!(err.contains("reserved"), "{err}");
        assert!(
            tokenize("not json", &texts(&["x"]))
                .unwrap_err()
                .contains("not JSON")
        );
        assert!(restore("not json", &[]).unwrap_err().contains("not JSON"));
        for bad in [
            r#"{"a":"${sealed:}"}"#,
            r#"{"a":"${sealed:1"}"#,
            r#"{"a":"${sealed:1:numbr}"}"#,
            r#"{"a":"${sealed:x}"}"#,
        ] {
            assert!(
                restore(bad, &texts(&["v", "w"]))
                    .unwrap_err()
                    .contains("malformed"),
                "{bad}"
            );
        }
        assert!(
            restore(r#"{"a":"${sealed:3}"}"#, &texts(&["v"]))
                .unwrap_err()
                .contains("no value")
        );
        assert!(
            restore(r#"{"a":"${sealed:0:number}"}"#, &texts(&["abc"]))
                .unwrap_err()
                .contains("not a number")
        );
        assert_eq!(
            restore(r#"{"a":"x${sealed:0:number}"}"#, &texts(&["12"])).unwrap(),
            r#"{"a":"x12"}"#
        );
        assert_eq!(context("r1"), "run:r1:secret-params");
    }
}

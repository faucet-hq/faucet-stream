//! Conservative secret redaction for text echoed from a Singer subprocess.

use faucet_core::Value;

/// Scrubs the scalar string values found in a Singer tap/target config out of
/// any echoed text (stderr, error messages). Which values are secret is
/// unknowable, so every string leaf of at least four characters is treated as
/// sensitive, as is a number of at least four digits under a secret-looking
/// key (a numeric API key or PIN).
#[derive(Clone, Debug, Default)]
pub struct Redactor {
    secrets: Vec<String>,
}

impl Redactor {
    /// Build a redactor from a config object, collecting its string leaves.
    pub fn from_config(cfg: &Value) -> Self {
        let mut secrets = Vec::new();
        collect_strings(cfg, false, &mut secrets);
        // Longest first so containing values are scrubbed before substrings.
        secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
        secrets.dedup();
        Self { secrets }
    }

    /// Replace any known secret value in `s` with `***`.
    pub fn redact(&self, s: &str) -> String {
        let mut out = s.to_string();
        for secret in &self.secrets {
            if secret.len() >= 4 && out.contains(secret.as_str()) {
                out = out.replace(secret.as_str(), "***");
            }
        }
        out
    }
}

/// String and number leaves of `cfg` stored under a secret-looking key
/// (`password`, `token`, `secret`, `key`, `credential`, …) — the values worth
/// registering with a process-wide log redactor. Values shorter than four
/// characters are skipped.
pub fn secret_like_values(cfg: &Value) -> Vec<String> {
    let mut out = Vec::new();
    collect_secret_like(cfg, false, &mut out);
    out.sort();
    out.dedup();
    out
}

fn is_secret_key(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    [
        "password",
        "passwd",
        "secret",
        "token",
        "key",
        "credential",
        "auth",
        "private",
    ]
    .iter()
    .any(|needle| k.contains(needle))
}

fn collect_secret_like(v: &Value, under_secret: bool, out: &mut Vec<String>) {
    match v {
        Value::String(s) if under_secret && s.len() >= 4 => out.push(s.clone()),
        Value::Number(n) if under_secret => push_number(n, out),
        Value::Array(a) => a
            .iter()
            .for_each(|x| collect_secret_like(x, under_secret, out)),
        Value::Object(o) => o
            .iter()
            .for_each(|(k, x)| collect_secret_like(x, under_secret || is_secret_key(k), out)),
        _ => {}
    }
}

fn collect_strings(v: &Value, under_secret: bool, out: &mut Vec<String>) {
    match v {
        Value::String(s) => out.push(s.clone()),
        Value::Number(n) if under_secret => push_number(n, out),
        Value::Array(a) => a.iter().for_each(|x| collect_strings(x, under_secret, out)),
        Value::Object(o) => o
            .iter()
            .for_each(|(k, x)| collect_strings(x, under_secret || is_secret_key(k), out)),
        _ => {}
    }
}

fn push_number(n: &serde_json::Number, out: &mut Vec<String>) {
    let text = n.to_string();
    if text.len() >= 4 {
        out.push(text);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn redactor_scrubs_config_string_values() {
        let cfg = json!({"token": "supersecrettoken", "n": 5, "nested": {"pw": "hunter2pass"}, "l": ["listvalue"]});
        let r = Redactor::from_config(&cfg);
        let out = r.redact("auth with supersecrettoken and hunter2pass and listvalue ok");
        assert!(!out.contains("supersecrettoken"));
        assert!(!out.contains("hunter2pass"));
        assert!(!out.contains("listvalue"));
        assert!(out.contains("***"));
    }

    #[test]
    fn numeric_secrets_are_redacted_and_other_numbers_are_not() {
        let cfg = json!({"api_key": 81726354, "auth": {"pin": 4321}, "port": 5432, "batch": 1000, "pw": 12});
        let r = Redactor::from_config(&cfg);
        let out = r.redact("401 for key 81726354 pin 4321 on port 5432 batch 1000 n 12");
        assert!(!out.contains("81726354"), "{out}");
        assert!(!out.contains("4321"), "{out}");
        assert!(
            out.contains("5432") && out.contains("1000") && out.contains(" 12"),
            "{out}"
        );
        assert_eq!(
            secret_like_values(&cfg),
            vec!["4321".to_string(), "81726354".to_string()]
        );
    }

    #[test]
    fn redactor_ignores_short_values() {
        let r = Redactor::from_config(&json!({"x": "ab"}));
        assert_eq!(r.redact("value ab here"), "value ab here");
    }

    #[test]
    fn secret_like_values_follow_key_names() {
        let cfg = json!({
            "api_token": "tok-123456",
            "path": "./out",
            "auth": {"user": "someone", "pw": "abc"},
            "keys": ["k-1111", 5],
            "port": 5432
        });
        assert_eq!(
            secret_like_values(&cfg),
            vec![
                "k-1111".to_string(),
                "someone".to_string(),
                "tok-123456".to_string()
            ]
        );
    }
}

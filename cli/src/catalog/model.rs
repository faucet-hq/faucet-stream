//! Pure helpers for building catalog observations (#279): dataset-URI
//! canonicalization (fold `${now.*}`-derived segments back to their tokens,
//! redact credentials) and schema inference over the run's record samples.

use chrono::{DateTime, FixedOffset};
use serde_json::Value;

/// Canonicalize a concrete dataset URI for use as the catalog identity key.
///
/// 1. **`${now.*}` folding** — a dated path like `./out/dt=2026-07-06/x.jsonl`
///    produced by a `${now.date}` template would otherwise mint a new catalog
///    dataset every day (the cardinality trap called out in #279). For every
///    `${now.*}` token that appears in the connector's *raw* (pre-resolution)
///    config, the token's rendered value is replaced back with the token text
///    in the URI, so all runs of the template converge on one dataset.
/// 2. **Credential redaction** — via `faucet_core::redact_uri_credentials`,
///    so a `postgres://user:pass@host/db` URI never lands in the store.
pub fn canonicalize_uri(uri: &str, raw_config: &Value, clock: DateTime<FixedOffset>) -> String {
    let mut ctxs: Vec<TokenCtx> = Vec::new();
    collect_now_token_contexts(raw_config, &mut ctxs);

    // Each token is folded back **anchored to the literal characters that
    // surround it in the config template** — not by a blind global replace of
    // the rendered value (audit #321 L8). A short rendering like `${now.month}`
    // → "07" would otherwise fold a coincidental `bucket-07` on days whose
    // digits collide, splitting one physical sink into several catalog datasets
    // with unstable identity. Anchoring `/07/` (the token's real `.../${now.month}/…`
    // context) leaves `bucket-07` untouched.
    let mut pairs: Vec<(String, String)> = ctxs
        .into_iter()
        .filter_map(|c| {
            let rendered = crate::interpolate::resolve_now(&c.token, clock).ok()?;
            if rendered.is_empty() || rendered == c.token {
                return None;
            }
            let mut search = String::new();
            let mut replace = String::new();
            if let Some(l) = c.left {
                search.push(l);
                replace.push(l);
            }
            search.push_str(&rendered);
            replace.push_str(&c.token);
            if let Some(r) = c.right {
                search.push(r);
                replace.push(r);
            }
            Some((search, replace))
        })
        .collect();
    // Dedup identical (search, replace) folds, then apply longest-search first
    // so `${now.datetime}` wins over the `${now.year}` embedded within it.
    pairs.sort();
    pairs.dedup();
    pairs.sort_by_key(|(search, _)| std::cmp::Reverse(search.len()));

    let mut out = uri.to_string();
    for (search, replace) in pairs {
        out = out.replace(&search, &replace);
    }
    faucet_core::redact_uri_credentials(&out)
}

/// A `${now.*}` token together with the literal characters that immediately
/// precede and follow it in its config string (used as fold anchors). `None`
/// when the token sits at the very start / end of the string.
struct TokenCtx {
    token: String,
    left: Option<char>,
    right: Option<char>,
}

/// Collect every `${now.…}` token in the string values of `v`, each with its
/// surrounding-character context. The context anchors keep a short rendered
/// value from folding coincidental look-alike substrings elsewhere in the URI.
fn collect_now_token_contexts(v: &Value, out: &mut Vec<TokenCtx>) {
    match v {
        Value::String(s) => {
            let mut search_from = 0;
            while let Some(rel) = s[search_from..].find("${now.") {
                let start = search_from + rel;
                // Adjacent tokens (`${now.year}${now.month}`) fold as one run:
                // between them there is no literal character to anchor on.
                let mut end = None;
                let mut cursor = start;
                while s[cursor..].starts_with("${now.") {
                    match s[cursor..].find('}') {
                        Some(end_rel) => {
                            end = Some(cursor + end_rel);
                            cursor += end_rel + 1;
                        }
                        None => break,
                    }
                }
                let Some(end) = end else { break };
                let token = s[start..=end].to_string();
                let left = s[..start].chars().next_back();
                let right = s[end + 1..].chars().next();
                out.push(TokenCtx { token, left, right });
                search_from = end + 1;
            }
        }
        Value::Array(items) => items
            .iter()
            .for_each(|i| collect_now_token_contexts(i, out)),
        Value::Object(map) => map
            .values()
            .for_each(|i| collect_now_token_contexts(i, out)),
        _ => {}
    }
}

/// Infer an `infer_schema`-shaped record schema from a run's sample, `None`
/// when nothing was sampled (empty run, or non-object records only).
pub fn schema_from_samples(samples: &[Value]) -> Option<Value> {
    if samples.is_empty() {
        return None;
    }
    let schema = faucet_core::schema::infer_schema(samples);
    // A sample of non-object records infers no properties — not a schema
    // worth a timeline entry.
    schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|p| !p.is_empty())
        .then_some(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn clock() -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339("2026-07-06T10:30:00Z").unwrap()
    }

    /// Adjacent tokens fold as one unit, so a compact dated path converges
    /// on one dataset (#789 CLI-132).
    #[test]
    fn adjacent_tokens_fold_as_one_run() {
        let raw = json!({"path": "./out/dt=${now.year}${now.month}${now.day}/x.jsonl"});
        let uri = canonicalize_uri("file://./out/dt=20260706/x.jsonl", &raw, clock());
        assert_eq!(uri, "file://./out/dt=${now.year}${now.month}${now.day}/x.jsonl");
        let raw = json!({"path": "./out/${now.year}${now.month}"});
        assert_eq!(
            canonicalize_uri("file://./out/202607", &raw, clock()),
            "file://./out/${now.year}${now.month}"
        );
    }

    #[test]
    fn folds_now_tokens_back_into_the_uri() {
        let raw = json!({"path": "./out/dt=${now.date}/part.jsonl"});
        let uri = canonicalize_uri("file://./out/dt=2026-07-06/part.jsonl", &raw, clock());
        assert_eq!(uri, "file://./out/dt=${now.date}/part.jsonl");
    }

    #[test]
    fn folds_strftime_tokens_and_ignores_unused_ones() {
        let raw = json!({"path": "./out/${now.strftime.%Y/%m}/x.jsonl", "other": "${now.unix}"});
        let uri = canonicalize_uri("file://./out/2026/07/x.jsonl", &raw, clock());
        assert_eq!(uri, "file://./out/${now.strftime.%Y/%m}/x.jsonl");
    }

    #[test]
    fn without_now_tokens_the_uri_is_untouched_except_redaction() {
        let raw = json!({"connection_url": "postgres://u:pw@h:5432/db"});
        let uri = canonicalize_uri("postgres://u:pw@h:5432/db/public.users", &raw, clock());
        assert!(!uri.contains("pw"), "credentials must be redacted: {uri}");
        assert!(uri.contains("h:5432"));
    }

    #[test]
    fn collect_finds_tokens_with_surrounding_context() {
        let mut out = Vec::new();
        collect_now_token_contexts(&json!("a-${now.year}-${now.month}-b"), &mut out);
        let got: Vec<(&str, Option<char>, Option<char>)> = out
            .iter()
            .map(|c| (c.token.as_str(), c.left, c.right))
            .collect();
        assert_eq!(
            got,
            vec![
                ("${now.year}", Some('-'), Some('-')),
                ("${now.month}", Some('-'), Some('-')),
            ]
        );
        // Unterminated token is ignored, not a panic.
        out.clear();
        collect_now_token_contexts(&json!("broken ${now.date"), &mut out);
        assert!(out.is_empty());
        // Tokens inside arrays (e.g. a list-valued config field) are found too.
        out.clear();
        collect_now_token_contexts(&json!({"paths": ["x", "${now.date}"]}), &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].token, "${now.date}");
        assert_eq!((out[0].left, out[0].right), (None, None));
    }

    #[test]
    fn anchored_fold_does_not_touch_coincidental_substrings() {
        // #321 L8: a short `${now.month}` → "07" must fold only the real
        // `.../07/...` path segment, not a coincidental `bucket-07`.
        let raw = json!({"path": "s3://bucket-07/data/${now.month}/part.jsonl"});
        let uri = canonicalize_uri("s3://bucket-07/data/07/part.jsonl", &raw, clock());
        assert_eq!(uri, "s3://bucket-07/data/${now.month}/part.jsonl");
    }

    #[test]
    fn schema_from_samples_requires_object_records() {
        assert!(schema_from_samples(&[]).is_none());
        assert!(schema_from_samples(&[json!("scalar")]).is_none());
        let schema = schema_from_samples(&[json!({"id": 1, "name": "a"})]).unwrap();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["id"].is_object());
    }
}

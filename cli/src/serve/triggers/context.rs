//! The fired event (`TriggerEvent`) and pure derivations from it:
//! `${trigger.*}` text substitution, the deterministic idempotency key, and the
//! auto run-labels. No IO.

use std::collections::BTreeMap;

/// A concrete event that fired a trigger.
#[derive(Debug, Clone)]
pub enum TriggerEvent {
    Object {
        bucket: String,
        key: String,
        size: u64,
        last_modified: String, // RFC3339
    },
    /// One poll-cycle's worth of new objects (batch mode).
    ObjectBatch {
        bucket: String,
        count: usize,
        /// Max last_modified seen this batch (for the idempotency key).
        watermark: String,
    },
    Webhook {
        method: String,
        body: String,
        headers: BTreeMap<String, String>, // lowercased header names
        query: BTreeMap<String, String>,
        /// Pre-resolved idempotency key (dedupe header value or a UUID).
        idem: String,
    },
    QueueDepth {
        queue: String,
        depth: u64,
        /// Rising-edge ordinal (so re-arm + re-cross → distinct run).
        edge: u64,
    },
    /// A cron tick (#709).
    Schedule {
        /// The scheduled instant (RFC 3339) — not the wall-clock fire time,
        /// so every cluster instance derives the same idempotency key.
        tick: String,
    },
}

impl TriggerEvent {
    pub fn type_label(&self) -> &'static str {
        match self {
            TriggerEvent::Object { .. } | TriggerEvent::ObjectBatch { .. } => "object_arrival",
            TriggerEvent::Webhook { .. } => "webhook",
            TriggerEvent::QueueDepth { .. } => "queue_depth",
            TriggerEvent::Schedule { .. } => "schedule",
        }
    }

    /// Map `${trigger.<token>}` → value. `fired_at` is supplied by the caller
    /// (so this stays pure / clock-free).
    fn lookup(&self, token: &str, name: &str, fired_at: &str) -> Option<String> {
        match token {
            "name" => return Some(name.to_string()),
            "type" => return Some(self.type_label().to_string()),
            "fired_at" => return Some(fired_at.to_string()),
            _ => {}
        }
        match self {
            TriggerEvent::Object {
                bucket,
                key,
                size,
                last_modified,
            } => match token {
                "object_key" => Some(key.clone()),
                "bucket" => Some(bucket.clone()),
                "size" => Some(size.to_string()),
                "last_modified" => Some(last_modified.clone()),
                _ => None,
            },
            TriggerEvent::ObjectBatch { bucket, count, .. } => match token {
                "bucket" => Some(bucket.clone()),
                "object_count" => Some(count.to_string()),
                _ => None,
            },
            TriggerEvent::Webhook {
                method,
                body,
                headers,
                query,
                ..
            } => {
                // HTTP header names are case-insensitive (looked up lowercased); URI query
                // keys are case-sensitive per the URI spec and matched verbatim.
                if let Some(h) = token.strip_prefix("header.") {
                    return headers.get(&h.to_ascii_lowercase()).cloned();
                }
                if let Some(q) = token.strip_prefix("query.") {
                    return query.get(q).cloned();
                }
                match token {
                    "method" => Some(method.clone()),
                    "body" => Some(body.clone()),
                    _ => None,
                }
            }
            TriggerEvent::QueueDepth { queue, depth, .. } => match token {
                "queue" => Some(queue.clone()),
                "depth" => Some(depth.to_string()),
                _ => None,
            },
            TriggerEvent::Schedule { tick } => match token {
                "tick" => Some(tick.clone()),
                _ => None,
            },
        }
    }
}

/// Substitute every `${trigger.<token>}` in `text`. Substituted values are
/// YAML-escaped (wrapped + quotes doubled) so a value containing `:`/quotes can
/// land in a scalar position without breaking the document. An unknown token is
/// an error (never silently passed through). Returns the substituted text.
pub fn substitute(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<String, String> {
    substitute_with(text, event, name, fired_at, yaml_escape)
}

/// [`substitute`] without YAML quoting — for a value that is not spliced
/// into a document (a template param).
pub fn substitute_plain(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<String, String> {
    substitute_with(text, event, name, fired_at, str::to_string)
}

fn substitute_with(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
    escape: fn(&str) -> String,
) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${trigger.") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..]; // skip "${"
        let Some(end) = after.find('}') else {
            return Err("unterminated `${trigger.…}` token".into());
        };
        let token = &after[8..end]; // after "trigger."
        match event.lookup(token, name, fired_at) {
            Some(v) => out.push_str(&escape(&v)),
            None => {
                return Err(format!(
                    "unknown `${{trigger.{token}}}` token for {} trigger '{name}'",
                    event.type_label()
                ));
            }
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Quote a value for safe scalar substitution into YAML.
fn yaml_escape(v: &str) -> String {
    format!(
        "\"{}\"",
        v.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t")
    )
}

/// Deterministic idempotency key for the event (see spec §9).
pub fn idempotency_key(name: &str, event: &TriggerEvent) -> String {
    match event {
        TriggerEvent::Object {
            bucket,
            key,
            last_modified,
            ..
        } => {
            format!("trig:{name}:{bucket}:{key}:{last_modified}")
        }
        TriggerEvent::ObjectBatch { watermark, .. } => format!("trig:{name}:{watermark}"),
        TriggerEvent::Webhook { idem, .. } => idem.clone(),
        TriggerEvent::QueueDepth { edge, .. } => format!("trig:{name}:edge:{edge}"),
        TriggerEvent::Schedule { tick } => format!("trig:{name}:{tick}"),
    }
}

/// Auto labels attached to the enqueued run (low-cardinality by construction).
pub fn labels(name: &str, event: &TriggerEvent) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    m.insert("faucet.trigger.name".into(), name.to_string());
    m.insert("faucet.trigger.type".into(), event.type_label().to_string());
    match event {
        TriggerEvent::Object { bucket, key, .. } => {
            m.insert("faucet.trigger.bucket".into(), bucket.clone());
            m.insert("faucet.trigger.object_key".into(), key.clone());
        }
        TriggerEvent::ObjectBatch { bucket, count, .. } => {
            m.insert("faucet.trigger.bucket".into(), bucket.clone());
            m.insert("faucet.trigger.object_count".into(), count.to_string());
        }
        TriggerEvent::QueueDepth { queue, depth, .. } => {
            m.insert("faucet.trigger.queue".into(), queue.clone());
            m.insert("faucet.trigger.depth".into(), depth.to_string());
        }
        TriggerEvent::Webhook { method, .. } => {
            m.insert("faucet.trigger.method".into(), method.clone());
        }
        TriggerEvent::Schedule { tick } => {
            m.insert("faucet.trigger.tick".into(), tick.clone());
        }
    }
    m
}

/// Render a `{field}` run-name template against trigger fields.
pub fn render_name(template: &str, event: &TriggerEvent, name: &str, fired_at: &str) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        if let Some(end) = after.find('}') {
            let token = &after[..end];
            out.push_str(&event.lookup(token, name, fired_at).unwrap_or_default());
            rest = &after[end + 1..];
        } else {
            out.push('{');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_events_substitute_label_and_key_by_tick() {
        let e = TriggerEvent::Schedule {
            tick: "2026-09-26T02:00:00Z".into(),
        };
        assert_eq!(e.type_label(), "schedule");
        assert_eq!(
            substitute_plain("since=${trigger.tick}", &e, "n", "f").unwrap(),
            "since=2026-09-26T02:00:00Z"
        );
        assert_eq!(
            substitute("t: ${trigger.tick}", &e, "n", "f").unwrap(),
            "t: \"2026-09-26T02:00:00Z\""
        );
        assert!(substitute_plain("${trigger.depth}", &e, "n", "f").is_err());
        assert_eq!(idempotency_key("n", &e), "trig:n:2026-09-26T02:00:00Z");
        assert_eq!(labels("n", &e)["faucet.trigger.tick"], "2026-09-26T02:00:00Z");
    }

    fn obj() -> TriggerEvent {
        TriggerEvent::Object {
            bucket: "b".into(),
            key: "incoming/2026/data:set.json".into(),
            size: 42,
            last_modified: "2026-06-12T10:00:00Z".into(),
        }
    }

    #[test]
    fn substitutes_object_key_with_yaml_escaping() {
        let out = substitute(
            "key: ${trigger.object_key}",
            &obj(),
            "t",
            "2026-06-12T10:00:01Z",
        )
        .unwrap();
        // The ':' in the key must be quoted so YAML stays valid.
        assert_eq!(out, "key: \"incoming/2026/data:set.json\"");
    }

    #[test]
    fn unknown_token_errors() {
        let err = substitute("x: ${trigger.nope}", &obj(), "t", "now").unwrap_err();
        assert!(err.contains("trigger.nope"), "{err}");
    }

    #[test]
    fn webhook_header_and_query_tokens() {
        let mut headers = BTreeMap::new();
        headers.insert("x-tenant".into(), "acme".into());
        let mut query = BTreeMap::new();
        query.insert("mode".into(), "full".into());
        let e = TriggerEvent::Webhook {
            method: "POST".into(),
            body: "{}".into(),
            headers,
            query,
            idem: "k1".into(),
        };
        let out = substitute(
            "t: ${trigger.header.X-Tenant} m: ${trigger.query.mode}",
            &e,
            "h",
            "now",
        )
        .unwrap();
        assert_eq!(out, "t: \"acme\" m: \"full\"");
    }

    #[test]
    fn idempotency_keys_are_deterministic() {
        assert_eq!(
            idempotency_key("t", &obj()),
            "trig:t:b:incoming/2026/data:set.json:2026-06-12T10:00:00Z"
        );
        let q = TriggerEvent::QueueDepth {
            queue: "jobs".into(),
            depth: 9,
            edge: 3,
        };
        assert_eq!(idempotency_key("d", &q), "trig:d:edge:3");
    }

    #[test]
    fn renders_name_template() {
        let n = render_name("{name}:{object_key}", &obj(), "t", "now");
        assert_eq!(n, "t:incoming/2026/data:set.json");
    }

    #[test]
    fn substitutes_multiline_value_escapes_newline() {
        let mut headers = BTreeMap::new();
        let mut query = BTreeMap::new();
        headers.insert("x-h".into(), "v".into());
        query.insert("q".into(), "v".into());
        let e = TriggerEvent::Webhook {
            method: "POST".into(),
            body: "line1\nline2".into(),
            headers,
            query,
            idem: "k".into(),
        };
        let out = substitute("b: ${trigger.body}", &e, "h", "now").unwrap();
        // Must contain literal backslash-n, not a raw newline.
        assert_eq!(out, r#"b: "line1\nline2""#);
        assert!(!out.contains('\n'), "raw newline must not appear in output");
    }
}

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

/// Substitute every `${trigger.<token>}` in a config document. The document is
/// parsed and each string scalar is substituted on its own with the raw value,
/// so a token inside a quoted or longer scalar stays a single string with no
/// quotes added, and a value holding `:`, quotes or newlines cannot change the
/// document's shape. An unknown token is an error (never silently passed
/// through). Returns the re-rendered document.
pub fn substitute(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<String, String> {
    substitute_tracked(text, event, name, fired_at)
        .map(|s| s.text)
        .map_err(|e| e.message)
}

/// Why a substitution failed, and whether the event's sender caused it (a
/// value it sent is refused where the config places it, or a header or
/// query value the config needs is missing) rather than the trigger's own
/// configuration (a malformed document, a token the trigger never has).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubstituteError {
    pub message: String,
    pub by_sender: bool,
}

impl SubstituteError {
    fn config(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            by_sender: false,
        }
    }
}

impl std::fmt::Display for SubstituteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// A value the event's sender chose (a webhook body, header or query value,
/// an object key) and where it landed in the config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventValue {
    /// The token, e.g. `body` or `object_key`.
    pub token: String,
    pub value: String,
    /// Whether the value starts its scalar (it decides what kind of path or
    /// URL the scalar is).
    pub leads: bool,
}

/// A substituted config and the sender-chosen values it now carries, for the
/// caller to check against the server's reference policy.
#[derive(Debug, Clone, Default)]
pub struct Substitution {
    pub text: String,
    pub event_values: Vec<EventValue>,
}

/// [`substitute`], also returning the sender-chosen values it placed.
pub fn substitute_tracked(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<Substitution, SubstituteError> {
    if !text.contains("${trigger.") {
        return Ok(Substitution {
            text: text.to_string(),
            event_values: Vec::new(),
        });
    }
    let mut doc: serde_yaml::Value = serde_yaml::from_str(text)
        .map_err(|e| SubstituteError::config(format!("pipeline config is not valid YAML: {e}")))?;
    let mut event_values = Vec::new();
    substitute_value(&mut doc, None, event, name, fired_at, &mut event_values)?;
    let text = serde_yaml::to_string(&doc)
        .map_err(|e| SubstituteError::config(format!("re-rendering pipeline config: {e}")))?;
    Ok(Substitution { text, event_values })
}

fn substitute_value(
    v: &mut serde_yaml::Value,
    key: Option<&str>,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
    seen: &mut Vec<EventValue>,
) -> Result<(), SubstituteError> {
    match v {
        serde_yaml::Value::String(s) if s.contains("${trigger.") => {
            *s = substitute_with(s, event, name, fired_at, Some(Placement { key, seen }))?;
        }
        serde_yaml::Value::Sequence(items) => {
            for item in items {
                substitute_value(item, key, event, name, fired_at, seen)?;
            }
        }
        serde_yaml::Value::Mapping(map) => {
            for (k, item) in map.iter_mut() {
                substitute_value(item, k.as_str(), event, name, fired_at, seen)?;
            }
        }
        serde_yaml::Value::Tagged(tagged) => {
            substitute_value(&mut tagged.value, key, event, name, fired_at, seen)?;
        }
        _ => {}
    }
    Ok(())
}

/// Substitute every `${trigger.<token>}` in one string with the raw value.
pub fn substitute_plain(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<String, String> {
    substitute_with(text, event, name, fired_at, None).map_err(|e| e.message)
}

/// Where a config substitution lands: the mapping key of the scalar, and the
/// sender-chosen values placed so far.
struct Placement<'a> {
    key: Option<&'a str>,
    seen: &'a mut Vec<EventValue>,
}

/// Config keys whose value is SQL.
const SQL_KEYS: &[&str] = &["query", "sql", "statement", "where", "filter"];

/// Whether `token` takes its value from the event's sender rather than the
/// trigger's own configuration.
fn sender_chosen(event: &TriggerEvent, token: &str) -> bool {
    match event {
        TriggerEvent::Object { .. } => token == "object_key",
        TriggerEvent::Webhook { .. } => {
            token == "body" || token.starts_with("header.") || token.starts_with("query.")
        }
        _ => false,
    }
}

/// With a [`Placement`] (a config document), each `${` an event value carries
/// is written as `$${`, so the interpolation pass the config goes through
/// next keeps it literal instead of resolving a directive the event's author
/// smuggled in, and a sender-chosen value is checked against where it lands.
fn substitute_with(
    text: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
    mut placement: Option<Placement<'_>>,
) -> Result<String, SubstituteError> {
    let sql = placement
        .as_ref()
        .and_then(|p| p.key)
        .is_some_and(|k| SQL_KEYS.contains(&k.to_ascii_lowercase().as_str()));
    // A scalar with no whitespace or quotes around its tokens is a path, a URL
    // or a name; anything else (a query, free text) is treated as text a
    // quote or `;` could rewrite.
    let free_text = sql
        || without_tokens(text)
            .chars()
            .any(|c| c.is_whitespace() || "'\"`".contains(c));
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${trigger.") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            return Err(SubstituteError::config("unterminated `${trigger.…}` token"));
        };
        let token = &after[8..end];
        let leads = out.is_empty() && start == 0;
        // The value is the whole scalar: it lands as one string and cannot
        // reach the text around it.
        let whole = leads && after[end + 1..].is_empty();
        let Some(v) = event.lookup(token, name, fired_at) else {
            // A header or query value the request did not carry is the
            // sender's to supply; any other unknown token is the config's.
            return Err(SubstituteError {
                message: format!(
                    "unknown `${{trigger.{token}}}` token for {} trigger '{name}'",
                    event.type_label()
                ),
                by_sender: sender_chosen(event, token),
            });
        };
        match placement.as_mut() {
            Some(p) => {
                if sender_chosen(event, token) {
                    if let Some(why) = placement_refusal(&v, &out, whole, sql, free_text) {
                        return Err(SubstituteError {
                            message: format!(
                                "the `${{trigger.{token}}}` value {why}; the event was not run"
                            ),
                            by_sender: true,
                        });
                    }
                    p.seen.push(EventValue {
                        token: token.to_string(),
                        value: v.clone(),
                        leads,
                    });
                }
                out.push_str(&v.replace("${", "$${"));
            }
            None => out.push_str(&v),
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// `text` with its `${trigger.…}` tokens removed.
fn without_tokens(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(start) = rest.find("${trigger.") {
        out.push_str(&rest[..start]);
        match rest[start..].find('}') {
            Some(end) => rest = &rest[start + end + 1..],
            None => return out,
        }
    }
    out.push_str(rest);
    out
}

/// Why a sender-chosen value may not land where it does, if it may not. `before`
/// is the scalar's text ahead of the value.
fn placement_refusal(
    v: &str,
    before: &str,
    whole: bool,
    sql: bool,
    free_text: bool,
) -> Option<String> {
    // A whole value cannot reach the text around it; one that reads as a
    // path (no whitespace) still may not climb directories.
    if !whole && v.chars().any(char::is_control) {
        return Some("contains a control character".into());
    }
    let path_like = !whole || !v.chars().any(char::is_whitespace);
    if path_like && v.split(['/', '\\']).any(|seg| seg == "..") {
        return Some(
            "contains a `..` path segment, which could climb out of the directory the config names"
                .into(),
        );
    }
    if sql && whole {
        return Some(
            "would be a whole SQL statement; splice it into a query the config writes instead"
                .into(),
        );
    }
    if !whole
        && free_text
        && let Some(c) = v.chars().find(|c| ['\'', '"', '`', ';', '\\'].contains(c))
    {
        return Some(format!(
            "contains `{c}`, and the config splices it into a longer string (a query, free \
             text), where it could change the meaning of the text around it; use the token as \
             a whole value"
        ));
    }
    if let Some(i) = before.rfind("://")
        && !before[i + 3..].contains(['/', '?', '#'])
        && !v
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '-')
    {
        return Some(
            "would be part of a URL's host and holds characters a host name cannot".into(),
        );
    }
    None
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
        TriggerEvent::Webhook { idem, .. } => format!("trig:{name}:{idem}"),
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

    /// An event value spliced into a longer config string may not carry text
    /// that could rewrite it (end a SQL literal, climb out of a directory); as
    /// a whole scalar it may.
    #[test]
    fn a_spliced_event_value_cannot_rewrite_the_text_around_it() {
        let obj = |key: &str| TriggerEvent::Object {
            bucket: "b".into(),
            key: key.into(),
            size: 1,
            last_modified: "t".into(),
        };
        let spliced = "query: \"SELECT * FROM t WHERE k = '${trigger.object_key}'\"\n";
        for bad in ["x' OR '1'='1", "a;b", "../../etc/passwd", "a\nb", "a\\b"] {
            let err = substitute(spliced, &obj(bad), "n", "f").unwrap_err();
            assert!(err.contains("trigger.object_key"), "{bad}: {err}");
        }
        let ok = substitute(spliced, &obj("incoming/2026-10-21.csv"), "n", "f").unwrap();
        assert!(ok.contains("k = 'incoming/2026-10-21.csv'"), "{ok}");
        // A whole-scalar value lands as one string, whatever it holds.
        let whole = substitute("key: ${trigger.object_key}\n", &obj("x' OR 1;"), "n", "f").unwrap();
        assert_eq!(parsed(&whole)["key"], "x' OR 1;");
        let path = substitute("path: out/${trigger.object_key}\n", &obj("../up"), "n", "f");
        assert!(path.is_err());
    }

    /// A value placed in a path, a URL or a name may hold quotes and `;`
    /// (they cannot end anything there); one placed in a URL's host may hold
    /// only host-name characters; a whole SQL statement from the sender is
    /// refused (#934).
    #[test]
    fn a_sender_value_is_checked_against_where_it_lands() {
        let obj = |key: &str| TriggerEvent::Object {
            bucket: "b".into(),
            key: key.into(),
            size: 1,
            last_modified: "t".into(),
        };
        for doc in [
            "prefix: in/${trigger.object_key}\n",
            "path: \"s3://bucket/${trigger.object_key}\"\n",
        ] {
            let sub = substitute_tracked(doc, &obj("O'Brien;2026.csv"), "n", "f").unwrap();
            assert!(sub.text.contains("O'Brien;2026.csv"), "{}", sub.text);
            assert_eq!(
                sub.event_values,
                vec![EventValue {
                    token: "object_key".into(),
                    value: "O'Brien;2026.csv".into(),
                    leads: false,
                }]
            );
        }
        let hook = |h: &str| TriggerEvent::Webhook {
            method: "POST".into(),
            body: "SELECT 1".into(),
            headers: [("x-region".to_string(), h.to_string())].into(),
            query: Default::default(),
            idem: "i".into(),
        };
        let host = "url: \"https://${trigger.header.x-region}.api.example/v1\"\n";
        assert!(substitute(host, &hook("eu-1"), "n", "f").is_ok());
        for bad in ["internal.corp/x#", "a@b", "h:1"] {
            let err = substitute(host, &hook(bad), "n", "f").unwrap_err();
            assert!(err.contains("host"), "{bad}: {err}");
        }
        let err = substitute("query: ${trigger.body}\n", &hook("x"), "n", "f").unwrap_err();
        assert!(err.contains("whole SQL statement"), "{err}");
        // A value from the trigger's own configuration is not the sender's.
        let sub = substitute_tracked("b: ${trigger.bucket}/x\n", &obj("k"), "n", "f").unwrap();
        assert!(sub.event_values.is_empty());
        let sub = substitute_tracked("p: ${trigger.object_key}\n", &obj("/abs"), "n", "f").unwrap();
        assert!(sub.event_values[0].leads);
        assert_eq!(without_tokens("a${trigger.x}b${trigger.y"), "ab");
    }

    #[test]
    fn directives_carried_by_event_values_stay_literal_through_interpolation() {
        let e = TriggerEvent::Object {
            bucket: "b".into(),
            key: "incoming/${env:HOME}/x$${env:HOME}.csv".into(),
            size: 1,
            last_modified: "t".into(),
        };
        let out = substitute("path: data/${trigger.object_key}\n", &e, "n", "f").unwrap();
        let resolved = crate::interpolate::interpolate(&out).unwrap();
        let path = parsed(&resolved)["path"].as_str().unwrap().to_string();
        assert_eq!(
            crate::interpolate::unescape(&path),
            "data/incoming/${env:HOME}/x$${env:HOME}.csv",
            "consumed as the literal key, never resolved"
        );
        assert_eq!(
            substitute_plain("${trigger.object_key}", &e, "n", "f").unwrap(),
            "incoming/${env:HOME}/x$${env:HOME}.csv"
        );
    }

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
            parsed(&substitute("t: ${trigger.tick}", &e, "n", "f").unwrap())["t"],
            "2026-09-26T02:00:00Z"
        );
        assert!(substitute_plain("${trigger.depth}", &e, "n", "f").is_err());
        assert_eq!(idempotency_key("n", &e), "trig:n:2026-09-26T02:00:00Z");
        assert_eq!(
            labels("n", &e)["faucet.trigger.tick"],
            "2026-09-26T02:00:00Z"
        );
    }

    fn obj() -> TriggerEvent {
        TriggerEvent::Object {
            bucket: "b".into(),
            key: "incoming/2026/data:set.json".into(),
            size: 42,
            last_modified: "2026-06-12T10:00:00Z".into(),
        }
    }

    fn parsed(text: &str) -> serde_json::Value {
        serde_yaml::from_str(text).unwrap()
    }

    #[test]
    fn substitutes_object_key_into_bare_quoted_and_embedded_scalars() {
        let out = substitute(
            "key: ${trigger.object_key}\nquoted: \"${trigger.object_key}\"\nprefix: in/${trigger.object_key}\nsize: ${trigger.size}\nlist: ['${trigger.bucket}']\n",
            &obj(),
            "t",
            "2026-06-12T10:00:01Z",
        )
        .unwrap();
        let v = parsed(&out);
        assert_eq!(v["key"], "incoming/2026/data:set.json");
        assert_eq!(v["quoted"], "incoming/2026/data:set.json");
        assert_eq!(v["prefix"], "in/incoming/2026/data:set.json");
        assert_eq!(v["size"], "42", "a whole-scalar token stays a string");
        assert_eq!(v["list"][0], "b");
    }

    #[test]
    fn a_document_without_tokens_passes_through_unchanged() {
        let text = "# keep me\nname: x\n";
        assert_eq!(substitute(text, &obj(), "t", "now").unwrap(), text);
        let tagged = substitute("x: !Keep ${trigger.bucket}\n", &obj(), "t", "now").unwrap();
        assert_eq!(tagged.trim(), "x: !Keep b");
        assert!(substitute("a: [${trigger.bucket}", &obj(), "t", "now").is_err());
        assert!(substitute("a: ${trigger.bucket", &obj(), "t", "now").is_err());
    }

    /// A refusal the sender caused is told apart from a config error, so the
    /// webhook can answer the sender's mistakes with 422 and keep reporting
    /// the trigger's own as failures.
    #[test]
    fn a_substitution_error_says_whether_the_sender_caused_it() {
        let hook = TriggerEvent::Webhook {
            method: "POST".into(),
            body: "a/../b".into(),
            headers: Default::default(),
            query: Default::default(),
            idem: "i".into(),
        };
        let by_sender = |doc: &str| {
            substitute_tracked(doc, &hook, "n", "f")
                .unwrap_err()
                .by_sender
        };
        assert!(by_sender("p: in/${trigger.body}\n"), "a refused value");
        assert!(
            by_sender("p: ${trigger.header.x-missing}\n"),
            "a missing header"
        );
        assert!(
            by_sender("p: ${trigger.query.missing}\n"),
            "a missing query value"
        );
        assert!(
            !by_sender("p: ${trigger.object_key}\n"),
            "a token webhooks never have"
        );
        assert!(!by_sender("p: [${trigger.body}\n"), "a malformed document");
        assert!(!by_sender("p: ${trigger.body\n"), "an unterminated token");
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
            "t: ${trigger.header.X-Tenant}\nm: \"${trigger.query.mode}\"",
            &e,
            "h",
            "now",
        )
        .unwrap();
        let v = parsed(&out);
        assert_eq!(
            (v["t"].as_str(), v["m"].as_str()),
            (Some("acme"), Some("full"))
        );
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
    fn substitutes_a_multiline_value_as_one_scalar() {
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
        let out = substitute("b: ${trigger.body}\nc: 1\n", &e, "h", "now").unwrap();
        let v = parsed(&out);
        assert_eq!(v["b"], "line1\nline2");
        assert_eq!(v["c"], 1, "the value cannot add or move keys");
    }
}

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
    let mut doc: crate::yaml::Value = crate::yaml::from_str(text)
        .map_err(|e| SubstituteError::config(format!("pipeline config is not valid YAML: {e}")))?;
    let mut event_values = Vec::new();
    let root = doc.clone();
    let mut walk = Walk {
        event,
        name,
        fired_at,
        root: &root,
        seen: &mut event_values,
    };
    walk.value(&mut doc, None, &Scope::Unknown)?;
    let text = crate::yaml::to_string(&doc)
        .map_err(|e| SubstituteError::config(format!("re-rendering pipeline config: {e}")))?;
    Ok(Substitution { text, event_values })
}

/// The top-level fields of a connector or transform kind that hold a SQL
/// statement and that name a path or URL, read from its config schema
/// (`x-faucet-sql`, `x-faucet-resource`), so a plugin is judged like a
/// built-in.
#[derive(Debug, Default)]
struct KindFields {
    sql: std::collections::BTreeSet<String>,
    resources: std::collections::BTreeSet<String>,
}

fn kind_fields(kind: &str) -> std::sync::Arc<KindFields> {
    type Cache = std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<KindFields>>>;
    static CACHE: std::sync::OnceLock<Cache> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(hit) = cache.read().ok().and_then(|c| c.get(kind).cloned()) {
        return hit;
    }
    let mut fields = KindFields::default();
    let schemas = [
        crate::registry::source_schema(kind).ok(),
        crate::registry::sink_schema(kind).ok(),
        crate::transforms::transform_schema(kind).ok(),
    ];
    for schema in schemas.iter().flatten() {
        fields.sql.extend(faucet_core::resolve::sql_fields(schema));
        fields.resources.extend(
            faucet_core::resolve::annotated_fields(schema)
                .into_iter()
                .filter(|(path, kind)| {
                    !path.contains(['.', '[', '{'])
                        && *kind != faucet_core::resolve::ResourceKind::Command
                })
                .map(|(path, _)| path),
        );
    }
    let fields = std::sync::Arc::new(fields);
    if let Ok(mut c) = cache.write() {
        c.insert(kind.to_string(), std::sync::Arc::clone(&fields));
    }
    fields
}

/// What the walk knows about the node it is at.
#[derive(Clone)]
enum Scope {
    /// Outside any connector or transform the walk could name.
    Unknown,
    /// The config object of a known kind: its children are its fields.
    Object(std::sync::Arc<KindFields>),
    /// A direct field of a known kind's config.
    Field(std::sync::Arc<KindFields>),
    /// Nested below a known kind's top-level fields.
    Inside,
}

/// What a scalar's placement says about the value landing in it.
#[derive(Clone, Copy)]
enum Landing {
    /// The kind's schema says it: whether the field is a SQL statement, and
    /// whether it names a path or URL.
    Known { sql: bool, path: bool },
    /// No kind to ask: judged by the key's name and the value's shape.
    Unknown,
}

struct Walk<'a> {
    event: &'a TriggerEvent,
    name: &'a str,
    fired_at: &'a str,
    root: &'a crate::yaml::Value,
    seen: &'a mut Vec<EventValue>,
}

impl Walk<'_> {
    fn value(
        &mut self,
        v: &mut crate::yaml::Value,
        key: Option<&str>,
        scope: &Scope,
    ) -> Result<(), SubstituteError> {
        match v {
            crate::yaml::Value::String(s) if s.contains("${trigger.") => {
                let landing = match scope {
                    Scope::Unknown => Landing::Unknown,
                    Scope::Field(f) => Landing::Known {
                        sql: key.is_some_and(|k| f.sql.contains(k)),
                        path: key.is_some_and(|k| f.resources.contains(k)),
                    },
                    Scope::Object(_) | Scope::Inside => Landing::Known {
                        sql: false,
                        path: false,
                    },
                };
                *s = substitute_with(
                    s,
                    self.event,
                    self.name,
                    self.fired_at,
                    Some(Placement {
                        key,
                        landing,
                        seen: self.seen,
                        escape: true,
                    }),
                )?;
            }
            crate::yaml::Value::Sequence(items) => {
                for item in items {
                    self.value(item, key, scope)?;
                }
            }
            crate::yaml::Value::Mapping(map) => {
                let kind = mapping_kind(map, key, self.root);
                for (k, item) in map.iter_mut() {
                    let child = match (&kind, scope) {
                        (Some(kind), _) => {
                            let f = kind_fields(kind);
                            if k.as_str() == Some("config") {
                                Scope::Object(f)
                            } else {
                                Scope::Field(f)
                            }
                        }
                        (None, Scope::Object(f)) => Scope::Field(std::sync::Arc::clone(f)),
                        (None, Scope::Unknown) => Scope::Unknown,
                        (None, _) => Scope::Inside,
                    };
                    self.value(item, k.as_str(), &child)?;
                }
            }
            crate::yaml::Value::Tagged(tagged) => {
                self.value(&mut tagged.value, key, scope)?;
            }
            _ => {}
        }
        Ok(())
    }
}

/// The connector or transform kind a mapping configures: its own `type`, or
/// — for a matrix row's `source:` / `sink:` override — the `type` of the
/// template its `ref` (else the default template) names.
fn mapping_kind(
    map: &crate::yaml::Mapping,
    key: Option<&str>,
    root: &crate::yaml::Value,
) -> Option<String> {
    if let Some(t) = map.get("type").and_then(crate::yaml::Value::as_str) {
        return Some(t.to_string());
    }
    let side = match key {
        Some("source") => "source",
        Some("sink") => "sink",
        _ => return None,
    };
    let pipeline = root.get("pipeline")?;
    let named = pipeline.get(format!("{side}s"));
    let template = match map.get("ref").and_then(crate::yaml::Value::as_str) {
        Some(r) => named.and_then(|n| n.get(r)),
        None => pipeline
            .get(side)
            .or_else(|| named.and_then(|n| n.get("default"))),
    }?;
    template
        .get("type")
        .and_then(crate::yaml::Value::as_str)
        .map(str::to_string)
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

/// Substitute every `${trigger.<token>}` in a template trigger's param
/// `param`, checking each sender-chosen value as config substitution does.
/// Where the template places the param is not known here, so the value is
/// judged by the param's name and its own shape: no `..` segment in a value
/// that reads as a path, no whole statement for a param named like one, and
/// the returned values go through the caller reference policy.
pub fn substitute_param(
    text: &str,
    param: &str,
    event: &TriggerEvent,
    name: &str,
    fired_at: &str,
) -> Result<Substitution, SubstituteError> {
    let mut event_values = Vec::new();
    let text = substitute_with(
        text,
        event,
        name,
        fired_at,
        Some(Placement {
            key: Some(param),
            landing: Landing::Unknown,
            seen: &mut event_values,
            escape: false,
        }),
    )?;
    Ok(Substitution { text, event_values })
}

/// Where a config substitution lands: the mapping key of the scalar, and the
/// sender-chosen values placed so far.
struct Placement<'a> {
    key: Option<&'a str>,
    landing: Landing,
    seen: &'a mut Vec<EventValue>,
    /// Write `${` in a value as `$${` (a config document goes through the
    /// interpolation pass next; a template param does not).
    escape: bool,
}

/// Keys that may hold a statement, for a scalar outside any connector or
/// transform the walk could name.
const SQL_KEYS: &[&str] = &["query", "sql", "statement", "where", "filter"];

/// Whether the field a value lands in runs it as a statement.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Statement {
    No,
    /// The kind's schema marks the field as SQL.
    Sql,
    /// No kind to ask, and the key's name is one a statement often has.
    Maybe,
}

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
    let statement = match placement.as_ref() {
        None => Statement::No,
        Some(Placement {
            landing: Landing::Known { sql, .. },
            ..
        }) => {
            if *sql {
                Statement::Sql
            } else {
                Statement::No
            }
        }
        Some(Placement {
            landing: Landing::Unknown,
            key,
            ..
        }) => {
            if key.is_some_and(|k| SQL_KEYS.contains(&k.to_ascii_lowercase().as_str())) {
                Statement::Maybe
            } else {
                Statement::No
            }
        }
    };
    let sql = statement != Statement::No;
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
                    // A whole value is checked for `..` only where it names a
                    // path or URL: a body parsed as data may hold the text.
                    let path_field = match p.landing {
                        Landing::Known { path, .. } => path,
                        Landing::Unknown => {
                            !v.chars().any(char::is_whitespace)
                                && !v.trim_start().starts_with(['{', '['])
                        }
                    };
                    if let Some(why) =
                        placement_refusal(&v, &out, whole, statement, p.key, path_field, free_text)
                    {
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
                if p.escape {
                    out.push_str(&v.replace("${", "$${"));
                } else {
                    out.push_str(&v);
                }
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
/// is the scalar's text ahead of the value; `path_field` says whether a whole
/// value lands in a field naming a path or URL.
fn placement_refusal(
    v: &str,
    before: &str,
    whole: bool,
    statement: Statement,
    key: Option<&str>,
    path_field: bool,
    free_text: bool,
) -> Option<String> {
    // A whole value cannot reach the text around it; one that names a path
    // still may not climb directories.
    if !whole && v.chars().any(char::is_control) {
        return Some("contains a control character".into());
    }
    let path_like = !whole || path_field;
    if path_like && v.split(['/', '\\']).any(|seg| seg == "..") {
        return Some(
            "contains a `..` path segment, which could climb out of the directory the config names"
                .into(),
        );
    }
    if whole {
        match statement {
            Statement::Sql => {
                return Some(
                    "would be a whole SQL statement; splice it into a query the config writes \
                     instead"
                        .into(),
                );
            }
            Statement::Maybe => {
                return Some(format!(
                    "would be the whole `{}` value, which may be a statement the connector runs; \
                     splice it into one the config writes instead",
                    key.unwrap_or_default()
                ));
            }
            Statement::No => {}
        }
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
        // Outside any connector the key's name is all there is to go on.
        let err = substitute("query: ${trigger.body}\n", &hook("x"), "n", "f").unwrap_err();
        assert!(err.contains("whole `query` value"), "{err}");
        // A value from the trigger's own configuration is not the sender's.
        let sub = substitute_tracked("b: ${trigger.bucket}/x\n", &obj("k"), "n", "f").unwrap();
        assert!(sub.event_values.is_empty());
        let sub = substitute_tracked("p: ${trigger.object_key}\n", &obj("/abs"), "n", "f").unwrap();
        assert!(sub.event_values[0].leads);
        assert_eq!(without_tokens("a${trigger.x}b${trigger.y"), "ab");
    }

    fn body_hook(body: &str) -> TriggerEvent {
        TriggerEvent::Webhook {
            method: "POST".into(),
            body: body.into(),
            headers: Default::default(),
            query: Default::default(),
            idem: "i".into(),
        }
    }

    /// A whole value is refused as a statement only in a field the
    /// connector's schema marks as SQL: a GraphQL document, a search or a
    /// document-store filter is not one (#934).
    #[cfg(all(
        feature = "source-postgres",
        feature = "source-graphql",
        feature = "source-elasticsearch",
        feature = "source-mongodb"
    ))]
    #[test]
    fn a_whole_value_is_a_statement_only_where_the_schema_says_sql() {
        let doc = |kind: &str, key: &str| {
            format!(
                "pipeline:\n  source:\n    type: {kind}\n    config:\n      {key}: ${{trigger.body}}\n"
            )
        };
        let gql = "{ orders(first: 10) { id } }";
        let sub = substitute(&doc("graphql", "query"), &body_hook(gql), "n", "f").unwrap();
        assert!(sub.contains("orders(first: 10)"), "{sub}");
        for (kind, key, body) in [
            ("elasticsearch", "query", r#"{"match":{"a":"b"}}"#),
            ("mongodb", "filter", r#"{"a":1}"#),
        ] {
            substitute(&doc(kind, key), &body_hook(body), "n", "f").unwrap();
        }
        let err =
            substitute(&doc("postgres", "query"), &body_hook("SELECT 1"), "n", "f").unwrap_err();
        assert!(err.contains("whole SQL statement"), "{err}");
        // A matrix row's override is judged by the template it selects.
        let row = "pipeline:\n  sources:\n    pg: {type: postgres, config: {query: x}}\n    gql: {type: graphql, config: {query: x}}\nmatrix:\n  - id: a\n    source: {ref: pg, config: {query: \"${trigger.body}\"}}\n";
        assert!(substitute(row, &body_hook("SELECT 1"), "n", "f").is_err());
        let row = row.replace("ref: pg", "ref: gql");
        substitute(&row, &body_hook(gql), "n", "f").unwrap();
        // Nested below a SQL source's top-level fields is not the statement.
        let nested = "pipeline:\n  source:\n    type: postgres\n    config:\n      replication:\n        query: ${trigger.body}\n";
        substitute(nested, &body_hook("SELECT 1"), "n", "f").unwrap();
    }

    /// A whole value is checked for a `..` segment only where it names a
    /// path or URL; a body parsed as data may hold the text (#934).
    #[cfg(feature = "source-csv")]
    #[test]
    fn a_whole_value_is_checked_for_dot_dot_only_where_it_names_a_path() {
        let json = r#"{"u":"a/../b","v":".."}"#;
        // Outside any connector: compact JSON is not a path.
        let sub = substitute("payload: ${trigger.body}\n", &body_hook(json), "n", "f").unwrap();
        assert_eq!(parsed(&sub)["payload"], json);
        let err = substitute("p: ${trigger.body}\n", &body_hook("a/../b"), "n", "f").unwrap_err();
        assert!(err.contains("`..`"), "{err}");
        // A connector's path field refuses it whole; another field takes it.
        let path =
            "pipeline:\n  source:\n    type: csv\n    config:\n      path: ${trigger.body}\n";
        let err = substitute(path, &body_hook("in/../../etc"), "n", "f").unwrap_err();
        assert!(err.contains("`..`"), "{err}");
        let other =
            "pipeline:\n  source:\n    type: csv\n    config:\n      delimiter: ${trigger.body}\n";
        substitute(other, &body_hook("a/../b"), "n", "f").unwrap();
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
        crate::yaml::from_str(text).unwrap()
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

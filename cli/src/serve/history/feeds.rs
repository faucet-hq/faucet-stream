//! Feeds (#1034): the definition (`FeedSpec`, also the shape of an entry in
//! the `--feeds` file) and the records the run history keeps for a feed made
//! through the API and for its ingest tokens. Pure data; the scheduler, the
//! rules and the stores live in `crate::serve::feeds`.

use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Most edits a record keeps in its change log.
pub const CHANGE_LOG_RETAIN: usize = 50;
/// The store a feed uses when it names none.
pub const DEFAULT_STORE: &str = "default";
/// Most deliveries one feed may have.
pub const MAX_DELIVERIES: usize = 16;
/// Longest feed, delivery or token name.
pub const MAX_NAME_LEN: usize = 64;

/// A length of time: whole seconds as a number, or text with a unit
/// (`90s`, `15m`, `24h`, `7d`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Span(pub u64);

impl Span {
    pub const fn secs(self) -> u64 {
        self.0
    }

    pub fn duration(self) -> std::time::Duration {
        std::time::Duration::from_secs(self.0)
    }

    /// Parse `90`, `90s`, `15m`, `24h` or `7d`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let (digits, unit) = split_unit(text);
        let n: u64 = digits
            .parse()
            .map_err(|_| format!("'{text}' is not a duration (use e.g. 90s, 15m, 24h, 7d)"))?;
        let factor = match unit {
            "" | "s" => 1,
            "m" => 60,
            "h" => 3_600,
            "d" => 86_400,
            _ => {
                return Err(format!(
                    "'{text}' has an unknown duration unit '{unit}' (use s, m, h or d)"
                ));
            }
        };
        n.checked_mul(factor)
            .map(Span)
            .ok_or_else(|| format!("'{text}' is too long a duration"))
    }
}

impl std::fmt::Display for Span {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.0;
        for (unit, factor) in [("d", 86_400), ("h", 3_600), ("m", 60)] {
            if n != 0 && n % factor == 0 {
                return write!(f, "{}{unit}", n / factor);
            }
        }
        write!(f, "{n}s")
    }
}

/// A size in bytes: a number, or text with a unit (`512KiB`, `64MiB`,
/// `50GiB`; `KB` / `MB` / `GB` are powers of 1000).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Size(pub u64);

impl Size {
    pub const fn bytes(self) -> u64 {
        self.0
    }

    /// Parse `1048576`, `512KiB`, `64MiB`, `50GiB`, `10MB`.
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        let (digits, unit) = split_unit(text);
        let n: u64 = digits
            .parse()
            .map_err(|_| format!("'{text}' is not a size (use e.g. 512KiB, 64MiB, 50GiB)"))?;
        let factor: u64 = match unit {
            "" | "B" => 1,
            "KiB" => 1 << 10,
            "MiB" => 1 << 20,
            "GiB" => 1 << 30,
            "TiB" => 1 << 40,
            "KB" => 1_000,
            "MB" => 1_000_000,
            "GB" => 1_000_000_000,
            "TB" => 1_000_000_000_000,
            _ => {
                return Err(format!(
                    "'{text}' has an unknown size unit '{unit}' (use KiB, MiB, GiB, TiB, KB, MB, GB or TB)"
                ));
            }
        };
        n.checked_mul(factor)
            .map(Size)
            .ok_or_else(|| format!("'{text}' is too large a size"))
    }
}

impl std::fmt::Display for Size {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.0;
        for (unit, shift) in [("TiB", 40), ("GiB", 30), ("MiB", 20), ("KiB", 10)] {
            if n != 0 && n % (1u64 << shift) == 0 {
                return write!(f, "{}{unit}", n >> shift);
            }
        }
        write!(f, "{n}")
    }
}

fn split_unit(text: &str) -> (&str, &str) {
    let at = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    let (digits, unit) = text.split_at(at);
    (digits, unit.trim())
}

macro_rules! text_or_number {
    ($ty:ident, $what:literal, $example:literal) => {
        impl Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_str(self)
            }
        }

        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                struct V;
                impl serde::de::Visitor<'_> for V {
                    type Value = $ty;
                    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        f.write_str(concat!($what, " as a number or text such as ", $example))
                    }
                    fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<$ty, E> {
                        $ty::parse(s).map_err(E::custom)
                    }
                    fn visit_u64<E: serde::de::Error>(self, n: u64) -> Result<$ty, E> {
                        Ok($ty(n))
                    }
                    fn visit_i64<E: serde::de::Error>(self, n: i64) -> Result<$ty, E> {
                        u64::try_from(n)
                            .map($ty)
                            .map_err(|_| E::custom(concat!($what, " must not be negative")))
                    }
                }
                d.deserialize_any(V)
            }
        }

        impl JsonSchema for $ty {
            fn schema_name() -> std::borrow::Cow<'static, str> {
                stringify!($ty).into()
            }
            fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
                schemars::json_schema!({
                    "description": concat!($what, ": a number, or text such as ", $example, "."),
                    "type": ["string", "integer"],
                    "minimum": 0
                })
            }
        }
    };
}

text_or_number!(Span, "seconds", "90s, 15m, 24h or 7d");
text_or_number!(Size, "bytes", "512KiB, 64MiB or 50GiB");

/// One feed: how events get in, how long they are kept and where they go.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FeedSpec {
    /// Unique name (per tenant): letters, digits, `_` and `-`, at most 64
    /// characters. Names the feed in URLs, metrics and the runs it starts.
    pub name: String,
    /// What the feed is for, shown in the console.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The tenant the feed belongs to. Its deliveries run on behalf of that
    /// tenant (limits, budget, state namespace).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The server-defined store that holds the feed's events. Default
    /// `default`, which follows the run-history backend.
    #[serde(default = "default_store", skip_serializing_if = "is_default_store")]
    pub store: String,
    /// How senders push events in.
    #[serde(default)]
    pub ingest: IngestSpec,
    /// How long events are kept.
    #[serde(default)]
    pub retention: RetentionSpec,
    /// Seal event payloads with the server's vault key. Default: on for a
    /// tenant feed, off otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seal: Option<bool>,
    /// Where the events go: each delivery runs a pipeline over the feed with
    /// its own position.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub deliveries: Vec<DeliverySpec>,
}

fn default_store() -> String {
    DEFAULT_STORE.to_string()
}

fn is_default_store(s: &str) -> bool {
    s == DEFAULT_STORE
}

impl FeedSpec {
    /// Whether event payloads are sealed.
    pub fn sealed(&self) -> bool {
        self.seal.unwrap_or(self.tenant.is_some())
    }

    /// The delivery named `name`.
    pub fn delivery(&self, name: &str) -> Option<&DeliverySpec> {
        self.deliveries.iter().find(|d| d.name == name)
    }
}

/// How senders push events into a feed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IngestSpec {
    /// What a sender must present.
    #[serde(default)]
    pub auth: IngestAuth,
    /// Accepted request content types.
    #[serde(default = "default_accept")]
    pub accept: Vec<String>,
    /// Largest single event. Default 1 MiB.
    #[serde(default = "default_max_event_bytes")]
    pub max_event_bytes: Size,
    /// Largest request body. Default 10 MiB.
    #[serde(default = "default_max_request_bytes")]
    pub max_request_bytes: Size,
    /// Drop an event a sender retried.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe: Option<DedupeSpec>,
    /// Most events a second this feed accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<RateLimit>,
    /// Accept the feed token in a `?token=` query parameter, for senders
    /// that can only be given a URL. Default off.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub url_token: bool,
}

impl Default for IngestSpec {
    fn default() -> Self {
        Self {
            auth: IngestAuth::default(),
            accept: default_accept(),
            max_event_bytes: default_max_event_bytes(),
            max_request_bytes: default_max_request_bytes(),
            dedupe: None,
            rate_limit: None,
            url_token: false,
        }
    }
}

fn default_accept() -> Vec<String> {
    [
        "application/json",
        "application/x-ndjson",
        "application/x-www-form-urlencoded",
        "text/plain",
    ]
    .map(String::from)
    .to_vec()
}

fn default_max_event_bytes() -> Size {
    Size(1 << 20)
}

fn default_max_request_bytes() -> Size {
    Size(10 << 20)
}

/// What a sender must present to push events.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IngestAuth {
    /// `managed`: write-only feed tokens issued through the API, stored hashed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<TokenMode>,
    /// An HMAC signature over the request body.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<faucet_core::signature::SignatureSpec>,
    /// Sender addresses or CIDR ranges that may push; empty = any.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_addresses: Vec<String>,
}

impl IngestAuth {
    /// Whether a sender can authenticate at all (a token or a signature).
    pub fn has_credential(&self) -> bool {
        self.tokens.is_some() || self.signature.is_some()
    }
}

/// How feed tokens are managed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TokenMode {
    /// Issued and revoked through the API.
    Managed,
}

/// Drop an event whose key was already seen within `window`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DedupeSpec {
    /// The request header that carries the event's key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header: Option<String>,
    /// A path into a JSON event that holds its key (`id`, `event.id`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_path: Option<String>,
    /// How long a key is remembered. Default 24h.
    #[serde(default = "default_dedupe_window")]
    pub window: Span,
    /// What a sender of a duplicate is told. Default `accept` (202 with
    /// `duplicate: true`).
    #[serde(default)]
    pub on_duplicate: OnDuplicate,
}

fn default_dedupe_window() -> Span {
    Span(86_400)
}

/// The answer to a duplicate event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnDuplicate {
    /// `202` with `duplicate: true`.
    #[default]
    Accept,
    /// `409`.
    Conflict,
}

/// A feed's ingest rate limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RateLimit {
    /// Events a second, sustained.
    pub events_per_sec: u32,
    /// Events accepted in a burst. Default: `events_per_sec`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst: Option<u32>,
}

/// How long a feed keeps events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RetentionSpec {
    /// How long a delivered event stays replayable. Default 7d.
    #[serde(default = "default_max_age")]
    pub max_age: Span,
    /// The hard limits for events some delivery has not delivered yet: past
    /// them the feed refuses new events instead of dropping old ones.
    #[serde(default)]
    pub undelivered: UndeliveredSpec,
    /// Fractions of the undelivered limits at which a warning is raised.
    /// Default `[0.5, 0.8]`.
    #[serde(default = "default_warn_at")]
    pub warn_at: Vec<f64>,
}

impl Default for RetentionSpec {
    fn default() -> Self {
        Self {
            max_age: default_max_age(),
            undelivered: UndeliveredSpec::default(),
            warn_at: default_warn_at(),
        }
    }
}

fn default_max_age() -> Span {
    Span(7 * 86_400)
}

fn default_warn_at() -> Vec<f64> {
    vec![0.5, 0.8]
}

/// The hard limits for undelivered events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UndeliveredSpec {
    /// Oldest undelivered event. Default 30d.
    #[serde(default = "default_undelivered_age")]
    pub max_age: Span,
    /// Most undelivered bytes. Default 50 GiB.
    #[serde(default = "default_undelivered_bytes")]
    pub max_bytes: Size,
}

impl Default for UndeliveredSpec {
    fn default() -> Self {
        Self {
            max_age: default_undelivered_age(),
            max_bytes: default_undelivered_bytes(),
        }
    }
}

fn default_undelivered_age() -> Span {
    Span(30 * 86_400)
}

fn default_undelivered_bytes() -> Size {
    Size(50 << 30)
}

/// One delivery: a pipeline that reads the feed with its own position.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliverySpec {
    /// Unique within the feed: letters, digits, `_` and `-`.
    pub name: String,
    /// The registered template each run uses. Its source must be `type: feed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template: Option<DeliveryTemplate>,
    /// A pipeline config file on the server (`--feeds` file only), relative
    /// to the feeds file. Its source must be `type: feed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub pipeline: Option<String>,
    /// When a run starts.
    #[serde(default)]
    pub flush: FlushSpec,
    /// The most one run delivers; a backlog drains in consecutive runs.
    #[serde(default)]
    pub max_batch: MaxBatch,
    /// Where a new delivery starts: `earliest` (default), `latest`,
    /// `seq:<n>` or `time:<RFC 3339>`.
    #[serde(default)]
    pub start: StartSpec,
    /// Failed runs of a single event before the delivery stalls. Default 5.
    #[serde(default = "default_retries")]
    pub retries: u32,
    /// The execution pool the runs go to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool: Option<String>,
    /// Per-run timeout in seconds. Default 3600.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
    /// Static labels added to every run.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// A paused delivery keeps its position but starts no runs.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub paused: bool,
}

fn default_retries() -> u32 {
    5
}

fn default_timeout_secs() -> u64 {
    3_600
}

/// The registered template a delivery runs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DeliveryTemplate {
    /// Template id.
    pub id: String,
    /// Version: a number or a channel. Default `stable`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "version_text"
    )]
    #[schemars(schema_with = "version_schema")]
    pub version: Option<String>,
    /// Values for the template's `params:`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub params: BTreeMap<String, serde_json::Value>,
    /// For a source template: the sink template to compose in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sink: Option<String>,
    /// The sink template's version: a number or a channel.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "version_text"
    )]
    #[schemars(schema_with = "version_schema")]
    pub sink_version: Option<String>,
    /// A registered deployment overlay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlay: Option<String>,
    /// The overlay's version: a number or a channel.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "version_text"
    )]
    #[schemars(schema_with = "version_schema")]
    pub overlay_version: Option<String>,
}

impl DeliveryTemplate {
    /// The first selector that names a channel, not a numbered version: what
    /// a launch can change under an approved delivery.
    pub fn follows_a_channel(&self) -> Option<&'static str> {
        let pinned = |v: &Option<String>| v.as_deref().is_some_and(|s| s.parse::<u32>().is_ok());
        if !pinned(&self.version) {
            return Some("template.version");
        }
        if self.sink.is_some() && !pinned(&self.sink_version) {
            return Some("template.sink_version");
        }
        if self.overlay.is_some() && !pinned(&self.overlay_version) {
            return Some("template.overlay_version");
        }
        None
    }
}

fn version_text<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        Text(String),
        Number(u64),
    }
    Ok(Option::<V>::deserialize(d)?.map(|v| match v {
        V::Text(s) => s,
        V::Number(n) => n.to_string(),
    }))
}

fn version_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "description": "A version number or a channel name.",
        "type": ["string", "integer", "null"],
        "minimum": 0
    })
}

/// When a delivery run starts: as soon as any of the three limits is
/// reached, but no more often than `min_interval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FlushSpec {
    /// Undelivered events. Default 5000.
    #[serde(default = "default_flush_events")]
    pub max_events: u64,
    /// Undelivered bytes. Default 64 MiB.
    #[serde(default = "default_flush_bytes")]
    pub max_bytes: Size,
    /// Age of the oldest undelivered event. Default 60s.
    #[serde(default = "default_flush_age")]
    pub max_age: Span,
    /// Shortest gap between two runs. Default 10s.
    #[serde(default = "default_min_interval")]
    pub min_interval: Span,
}

impl Default for FlushSpec {
    fn default() -> Self {
        Self {
            max_events: default_flush_events(),
            max_bytes: default_flush_bytes(),
            max_age: default_flush_age(),
            min_interval: default_min_interval(),
        }
    }
}

fn default_flush_events() -> u64 {
    5_000
}

fn default_flush_bytes() -> Size {
    Size(64 << 20)
}

fn default_flush_age() -> Span {
    Span(60)
}

fn default_min_interval() -> Span {
    Span(10)
}

/// The most one run delivers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaxBatch {
    /// Events. Default 10000.
    #[serde(default = "default_batch_events")]
    pub events: u64,
    /// Payload bytes. Default 64 MiB.
    #[serde(default = "default_batch_bytes")]
    pub bytes: Size,
}

impl Default for MaxBatch {
    fn default() -> Self {
        Self {
            events: default_batch_events(),
            bytes: default_batch_bytes(),
        }
    }
}

fn default_batch_events() -> u64 {
    10_000
}

fn default_batch_bytes() -> Size {
    Size(64 << 20)
}

/// Where a new delivery starts.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum StartSpec {
    /// The oldest retained event.
    #[default]
    Earliest,
    /// After the last stored event.
    Latest,
    /// At this sequence number.
    Seq(u64),
    /// At the first event stored at or after this time.
    Time(DateTime<Utc>),
}

impl StartSpec {
    /// Parse `earliest`, `latest`, `seq:<n>` or `time:<RFC 3339>`.
    pub fn parse(text: &str) -> Result<Self, String> {
        match text {
            "earliest" => Ok(Self::Earliest),
            "latest" => Ok(Self::Latest),
            other => {
                if let Some(n) = other.strip_prefix("seq:") {
                    return n
                        .parse()
                        .map(Self::Seq)
                        .map_err(|_| format!("start '{other}': '{n}' is not a sequence number"));
                }
                if let Some(t) = other.strip_prefix("time:") {
                    return DateTime::parse_from_rfc3339(t)
                        .map(|t| Self::Time(t.with_timezone(&Utc)))
                        .map_err(|e| format!("start '{other}': {e}"));
                }
                Err(format!(
                    "start '{other}': use earliest, latest, seq:<n> or time:<RFC 3339>"
                ))
            }
        }
    }
}

impl std::fmt::Display for StartSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Earliest => f.write_str("earliest"),
            Self::Latest => f.write_str("latest"),
            Self::Seq(n) => write!(f, "seq:{n}"),
            Self::Time(t) => write!(
                f,
                "time:{}",
                t.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
            ),
        }
    }
}

impl Serialize for StartSpec {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for StartSpec {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let text = String::deserialize(d)?;
        Self::parse(&text).map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for StartSpec {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "StartSpec".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "description": "`earliest`, `latest`, `seq:<n>` or `time:<RFC 3339>`.",
            "type": "string"
        })
    }
}

/// The key a feed is stored under: `<tenant|->/<name>`.
pub fn feed_key(tenant: Option<&str>, name: &str) -> String {
    format!("{}/{name}", tenant.unwrap_or("-"))
}

/// What the server keeps about one delivery of a stored feed.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryMeta {
    /// The delivery's id within the feed, kept across edits.
    pub uid: String,
    /// Who paused it, when and why (while paused).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_by: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
}

/// One edit in a feed's change log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedChange {
    pub at: DateTime<Utc>,
    /// The principal that made it.
    pub by: String,
    /// `created`, `updated`, `delivery.paused`, `replay`, `skip`, …
    pub action: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub detail: Vec<String>,
}

/// A feed made through the API or the console, as the run history keeps it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedRecord {
    /// The feed's id in its store, set by the server and kept across edits.
    pub uid: String,
    pub spec: FeedSpec,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
    /// Bumped on every write.
    pub revision: u64,
    /// Per delivery name: its id and pause state.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub deliveries: BTreeMap<String, DeliveryMeta>,
    /// The current deliveries came through an approved change request: under
    /// `--require-approval run` only such a feed starts runs. Set by the
    /// server, never from the wire.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub approved: bool,
    /// Newest last, at most [`CHANGE_LOG_RETAIN`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<FeedChange>,
}

impl FeedRecord {
    /// A new record for `spec` with a fresh uid and a fresh id per delivery.
    pub fn new(spec: FeedSpec, by: &str, now: DateTime<Utc>) -> Self {
        let mut r = Self {
            uid: uuid::Uuid::new_v4().simple().to_string(),
            deliveries: BTreeMap::new(),
            spec,
            created_at: now,
            created_by: by.to_string(),
            updated_at: now,
            updated_by: by.to_string(),
            revision: 1,
            approved: false,
            changes: Vec::new(),
        };
        r.sync_deliveries(by, now);
        r.log(now, by, "created", Vec::new());
        r
    }

    pub fn name(&self) -> &str {
        &self.spec.name
    }

    pub fn tenant(&self) -> Option<&str> {
        self.spec.tenant.as_deref()
    }

    /// The key the record is stored under.
    pub fn key(&self) -> String {
        feed_key(self.tenant(), self.name())
    }

    /// Give every delivery of the spec an id, record a spec-level pause and
    /// drop the entries of deliveries the spec no longer has. Returns the
    /// names that were dropped with their ids.
    pub fn sync_deliveries(&mut self, by: &str, now: DateTime<Utc>) -> Vec<(String, String)> {
        let wanted: std::collections::BTreeSet<&str> = self
            .spec
            .deliveries
            .iter()
            .map(|d| d.name.as_str())
            .collect();
        let dropped: Vec<(String, String)> = self
            .deliveries
            .iter()
            .filter(|(name, _)| !wanted.contains(name.as_str()))
            .map(|(name, meta)| (name.clone(), meta.uid.clone()))
            .collect();
        self.deliveries
            .retain(|name, _| wanted.contains(name.as_str()));
        for d in &self.spec.deliveries {
            let meta = self
                .deliveries
                .entry(d.name.clone())
                .or_insert_with(|| DeliveryMeta {
                    uid: uuid::Uuid::new_v4().simple().to_string(),
                    ..Default::default()
                });
            if d.paused && meta.paused_at.is_none() {
                meta.paused_at = Some(now);
                meta.paused_by = Some(by.to_string());
            } else if !d.paused {
                meta.paused_at = None;
                meta.paused_by = None;
                meta.pause_reason = None;
            }
        }
        dropped
    }

    /// Append to the change log, keeping the newest [`CHANGE_LOG_RETAIN`].
    pub fn log(&mut self, at: DateTime<Utc>, by: &str, action: &str, detail: Vec<String>) {
        self.changes.push(FeedChange {
            at,
            by: by.to_string(),
            action: action.to_string(),
            detail,
        });
        let over = self.changes.len().saturating_sub(CHANGE_LOG_RETAIN);
        self.changes.drain(..over);
    }

    /// The next revision of this record: `spec` replaced, stamped by `by`.
    /// Returns it with the deliveries the new spec dropped.
    pub fn revised(
        &self,
        spec: FeedSpec,
        by: &str,
        now: DateTime<Utc>,
        action: &str,
        detail: Vec<String>,
    ) -> (Self, Vec<(String, String)>) {
        let mut r = self.clone();
        r.spec = spec;
        r.updated_at = now;
        r.updated_by = by.to_string();
        r.revision += 1;
        let dropped = r.sync_deliveries(by, now);
        r.log(now, by, action, detail);
        (r, dropped)
    }
}

/// The top-level fields that differ between two specs, one line each;
/// deliveries are compared by name.
pub fn spec_diff(before: &FeedSpec, after: &FeedSpec) -> Vec<String> {
    let (Ok(serde_json::Value::Object(mut a)), Ok(serde_json::Value::Object(mut b))) =
        (serde_json::to_value(before), serde_json::to_value(after))
    else {
        return Vec::new();
    };
    a.remove("deliveries");
    b.remove("deliveries");
    let mut keys: Vec<String> = a.keys().chain(b.keys()).cloned().collect();
    keys.sort();
    keys.dedup();
    let mut out: Vec<String> = keys
        .into_iter()
        .filter(|k| a.get(k) != b.get(k))
        .map(|k| format!("{k} changed"))
        .collect();
    for d in &after.deliveries {
        match before.delivery(&d.name) {
            None => out.push(format!("delivery {} added", d.name)),
            Some(old) if old != d => out.push(format!("delivery {} changed", d.name)),
            Some(_) => {}
        }
    }
    for d in &before.deliveries {
        if after.delivery(&d.name).is_none() {
            out.push(format!("delivery {} removed", d.name));
        }
    }
    out
}

/// A feed token as the run history keeps it: the SHA-256 of the token, never
/// the token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedTokenRecord {
    pub id: String,
    /// The uid of the feed it writes to.
    pub feed: String,
    pub name: String,
    /// Lowercase hex SHA-256 of the token text.
    pub hash: String,
    /// The first characters of the token, to tell tokens apart.
    pub prefix: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_by: Option<String>,
    /// Best effort: not written on every request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_used_at: Option<DateTime<Utc>>,
}

impl FeedTokenRecord {
    /// Whether the token is accepted at `now`.
    pub fn usable(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|t| t > now)
    }
}

/// The feed range a delivery run reads, as its run record keeps it: ids and
/// names only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFeedBinding {
    /// The feed's uid.
    pub feed: String,
    /// The delivery's id within the feed.
    pub delivery: String,
    pub epoch: u64,
    pub claim: String,
    pub from: u64,
    pub to: u64,
    /// The name of the server-defined store that holds the feed.
    pub store: String,
}

#[cfg(feature = "source-feed")]
impl RunFeedBinding {
    /// The record form of `binding`.
    pub fn of(binding: &faucet_common_feed::FeedBinding) -> Self {
        Self {
            feed: binding.feed.as_str().to_string(),
            delivery: binding.delivery.id().to_string(),
            epoch: binding.epoch.0,
            claim: binding.claim.as_str().to_string(),
            from: binding.from.get(),
            to: binding.to.get(),
            store: binding.store.clone(),
        }
    }

    /// The binding a `feed` source reads under.
    pub fn binding(&self) -> Result<faucet_common_feed::FeedBinding, String> {
        use faucet_common_feed::{ClaimId, DeliveryUid, Epoch, FeedBinding, FeedUid, Seq};
        let feed = FeedUid::new(self.feed.clone()).map_err(|e| e.to_string())?;
        let delivery = DeliveryUid::new(feed, self.delivery.clone()).map_err(|e| e.to_string())?;
        let claim = ClaimId::new(self.claim.clone()).map_err(|e| e.to_string())?;
        Ok(FeedBinding::new(
            delivery,
            Epoch(self.epoch),
            claim,
            Seq(self.from),
            Seq(self.to),
            self.store.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn spans_and_sizes_read_numbers_and_text_and_write_the_largest_unit() {
        for (text, secs, shown) in [
            ("90", 90, "90s"),
            ("90s", 90, "90s"),
            ("15m", 900, "15m"),
            ("24h", 86_400, "1d"),
            ("7d", 604_800, "7d"),
            ("0", 0, "0s"),
        ] {
            let s = Span::parse(text).unwrap();
            assert_eq!(s.secs(), secs);
            assert_eq!(s.to_string(), shown);
            assert_eq!(Span::parse(&s.to_string()).unwrap(), s);
        }
        assert_eq!(Span(60).duration(), std::time::Duration::from_secs(60));
        assert!(
            Span::parse("1w")
                .unwrap_err()
                .contains("unknown duration unit")
        );
        assert!(Span::parse("soon").unwrap_err().contains("not a duration"));
        assert!(Span::parse("99999999999999999999d").is_err());
        assert!(
            Span::parse(&format!("{}d", u64::MAX))
                .unwrap_err()
                .contains("too long")
        );

        for (text, bytes, shown) in [
            ("1048576", 1 << 20, "1MiB"),
            ("512KiB", 512 << 10, "512KiB"),
            ("64MiB", 64 << 20, "64MiB"),
            ("50GiB", 50 << 30, "50GiB"),
            ("2TiB", 2 << 40, "2TiB"),
            ("10MB", 10_000_000, "10000000"),
            ("3KB", 3_000, "3000"),
            ("1GB", 1_000_000_000, "1000000000"),
            ("1TB", 1_000_000_000_000, "976562500KiB"),
            ("7B", 7, "7"),
        ] {
            let s = Size::parse(text).unwrap();
            assert_eq!(s.bytes(), bytes);
            assert_eq!(s.to_string(), shown);
        }
        assert!(
            Size::parse("5XB")
                .unwrap_err()
                .contains("unknown size unit")
        );
        assert!(Size::parse("big").unwrap_err().contains("not a size"));
        assert!(
            Size::parse(&format!("{}TiB", u64::MAX))
                .unwrap_err()
                .contains("too large")
        );

        let s: Span = serde_json::from_value(json!(30)).unwrap();
        assert_eq!(s, Span(30));
        let s: Span = serde_json::from_value(json!("2h")).unwrap();
        assert_eq!(serde_json::to_value(s).unwrap(), json!("2h"));
        assert!(serde_json::from_value::<Span>(json!(-1)).is_err());
        assert!(serde_json::from_value::<Size>(json!(true)).is_err());
        let z: Size = serde_json::from_value(json!("1MiB")).unwrap();
        assert_eq!(serde_json::to_value(z).unwrap(), json!("1MiB"));
    }

    fn spec() -> FeedSpec {
        serde_json::from_value(json!({
            "name": "orders",
            "ingest": {"auth": {"tokens": "managed"}},
            "deliveries": [
                {"name": "lake", "template": {"id": "acme/orders", "version": 3}},
                {"name": "audit", "template": {"id": "acme/audit"}, "paused": true}
            ]
        }))
        .unwrap()
    }

    #[test]
    fn a_minimal_spec_gets_the_documented_defaults() {
        let s = spec();
        assert_eq!(s.store, DEFAULT_STORE);
        assert!(!s.sealed());
        assert_eq!(s.ingest.max_event_bytes, Size(1 << 20));
        assert_eq!(s.ingest.max_request_bytes, Size(10 << 20));
        assert_eq!(s.ingest.accept.len(), 4);
        assert!(s.ingest.auth.has_credential());
        assert_eq!(s.retention.max_age, Span(604_800));
        assert_eq!(s.retention.undelivered.max_age, Span(30 * 86_400));
        assert_eq!(s.retention.undelivered.max_bytes, Size(50 << 30));
        assert_eq!(s.retention.warn_at, vec![0.5, 0.8]);
        let d = s.delivery("lake").unwrap();
        assert_eq!(d.template.as_ref().unwrap().version.as_deref(), Some("3"));
        assert_eq!(d.flush, FlushSpec::default());
        assert_eq!(
            (d.max_batch.events, d.max_batch.bytes),
            (10_000, Size(64 << 20))
        );
        assert_eq!((d.retries, d.timeout_secs), (5, 3_600));
        assert_eq!(d.start, StartSpec::Earliest);
        assert!(s.delivery("nope").is_none());
        // The default store and unset options stay out of the stored text.
        let text = serde_json::to_string(&s).unwrap();
        assert!(!text.contains("\"store\""), "{text}");
        let back: FeedSpec = serde_json::from_str(&text).unwrap();
        assert_eq!(back, s);
        let mut tenant = spec();
        tenant.tenant = Some("acme".into());
        assert!(tenant.sealed());
        tenant.seal = Some(false);
        assert!(!tenant.sealed());
        assert!(!IngestAuth::default().has_credential());
        assert_eq!(IngestSpec::default().max_event_bytes, Size(1 << 20));
        assert!(serde_json::from_value::<FeedSpec>(json!({"name": "x", "consumers": []})).is_err());
    }

    #[test]
    fn a_start_reads_and_writes_its_four_forms() {
        for text in ["earliest", "latest", "seq:42", "time:2026-11-16T10:00:00Z"] {
            let s = StartSpec::parse(text).unwrap();
            assert_eq!(s.to_string(), text);
            let back: StartSpec = serde_json::from_value(json!(text)).unwrap();
            assert_eq!(back, s);
        }
        assert!(
            StartSpec::parse("seq:x")
                .unwrap_err()
                .contains("not a sequence number")
        );
        assert!(StartSpec::parse("time:yesterday").is_err());
        assert!(
            StartSpec::parse("middle")
                .unwrap_err()
                .contains("use earliest")
        );
        assert!(serde_json::from_value::<StartSpec>(json!("middle")).is_err());
    }

    #[test]
    fn a_template_is_pinned_only_with_numbered_versions() {
        let t = |v: serde_json::Value| -> DeliveryTemplate { serde_json::from_value(v).unwrap() };
        assert_eq!(
            t(json!({"id": "a"})).follows_a_channel(),
            Some("template.version")
        );
        assert_eq!(
            t(json!({"id": "a", "version": "stable"})).follows_a_channel(),
            Some("template.version")
        );
        assert_eq!(
            t(json!({"id": "a", "version": 2})).follows_a_channel(),
            None
        );
        assert_eq!(
            t(json!({"id": "a", "version": "2", "sink": "s"})).follows_a_channel(),
            Some("template.sink_version")
        );
        assert_eq!(
            t(json!({"id": "a", "version": "2", "sink": "s", "sink_version": 1, "overlay": "o"}))
                .follows_a_channel(),
            Some("template.overlay_version")
        );
        assert_eq!(
            t(json!({"id": "a", "version": "2", "overlay": "o", "overlay_version": "4"}))
                .follows_a_channel(),
            None
        );
        assert_eq!(t(json!({"id": "a", "version": null})).version, None);
    }

    #[test]
    fn a_record_keeps_delivery_ids_across_edits_and_reports_dropped_ones() {
        let now = Utc::now();
        let r = FeedRecord::new(spec(), "alice", now);
        assert_eq!(r.uid.len(), 32);
        assert_eq!(r.key(), "-/orders");
        assert_eq!(feed_key(Some("acme"), "orders"), "acme/orders");
        assert_eq!((r.name(), r.tenant(), r.revision), ("orders", None, 1));
        assert_eq!(r.changes[0].action, "created");
        let lake = r.deliveries["lake"].uid.clone();
        assert!(r.deliveries["lake"].paused_at.is_none());
        assert_eq!(r.deliveries["audit"].paused_by.as_deref(), Some("alice"));

        let mut next = spec();
        next.deliveries.remove(1);
        next.deliveries
            .push(serde_json::from_value(json!({"name": "new", "pipeline": "p.yaml"})).unwrap());
        next.description = Some("d".into());
        let detail = spec_diff(&r.spec, &next);
        assert_eq!(
            detail,
            vec![
                "description changed",
                "delivery new added",
                "delivery audit removed"
            ]
        );
        let (r2, dropped) = r.revised(next.clone(), "bob", now, "updated", detail);
        assert_eq!(r2.revision, 2);
        assert_eq!(r2.updated_by, "bob");
        assert_eq!(r2.deliveries["lake"].uid, lake);
        assert_eq!(dropped.len(), 1);
        assert_eq!(dropped[0].0, "audit");
        assert_ne!(r2.deliveries["new"].uid, lake);

        // Resuming through the spec clears the pause record.
        let mut paused = r.spec.clone();
        paused.deliveries[0].paused = true;
        let (r3, _) = r.revised(paused.clone(), "bob", now, "updated", vec![]);
        assert!(r3.deliveries["lake"].paused_at.is_some());
        paused.deliveries[0].paused = false;
        paused.deliveries[0].retries = 9;
        assert_eq!(spec_diff(&r3.spec, &paused), vec!["delivery lake changed"]);
        let (r4, _) = r3.revised(paused, "bob", now, "updated", vec![]);
        assert!(r4.deliveries["lake"].paused_at.is_none());

        let mut long = r.clone();
        for i in 0..CHANGE_LOG_RETAIN + 5 {
            long.log(now, "x", &format!("a{i}"), vec![]);
        }
        assert_eq!(long.changes.len(), CHANGE_LOG_RETAIN);
        assert!(spec_diff(&r.spec, &r.spec).is_empty());
    }

    #[test]
    fn a_token_is_usable_until_it_expires_or_is_revoked() {
        let now = Utc::now();
        let mut t = FeedTokenRecord {
            id: "t1".into(),
            feed: "f".into(),
            name: "ci".into(),
            hash: "00".into(),
            prefix: "fdt_ab".into(),
            created_at: now,
            created_by: "alice".into(),
            expires_at: None,
            revoked_at: None,
            revoked_by: None,
            last_used_at: None,
        };
        assert!(t.usable(now));
        t.expires_at = Some(now + chrono::Duration::seconds(5));
        assert!(t.usable(now));
        assert!(!t.usable(now + chrono::Duration::seconds(5)));
        t.expires_at = None;
        t.revoked_at = Some(now);
        assert!(!t.usable(now));
        let text = serde_json::to_string(&t).unwrap();
        assert!(!text.contains("last_used_at"));
        assert_eq!(serde_json::from_str::<FeedTokenRecord>(&text).unwrap(), t);
    }

    #[test]
    fn the_schema_describes_durations_sizes_and_starts() {
        let schema = serde_json::to_value(schemars::schema_for!(FeedSpec)).unwrap();
        let text = schema.to_string();
        assert!(text.contains("90s, 15m, 24h or 7d"), "{text}");
        assert!(text.contains("512KiB, 64MiB or 50GiB"));
        assert!(text.contains("seq:<n>"));
        assert!(text.contains("x-faucet-resource"));
    }
}

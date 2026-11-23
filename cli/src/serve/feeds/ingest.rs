//! Ingest (#1034): senders push events into a feed over HTTP.
//!
//! `POST /v1/feeds/{name}/events` and `POST /v1/tenants/{tenant}/feeds/{name}/events`
//! sit outside the `/v1` auth layer and authenticate themselves: a feed
//! token, an HMAC signature over the body, or both. A serve bearer token is
//! never accepted here. An unknown feed, a bad credential and a sender
//! address the feed does not allow get the same `401`, after the same steps,
//! so names cannot be enumerated.
//!
//! A body may arrive gzip-encoded. The signature covers the bytes as sent;
//! the body is expanded after it, never past the request limit, and every
//! size limit applies to the expanded bytes.
//!
//! A request is all or nothing: its events are validated at the door,
//! sealed when the feed says so, appended to the feed's store in one
//! transaction (shared with concurrent requests to the same feed), and
//! answered `202` only after that transaction committed. Nothing here reads
//! the run history per request: definitions and token hashes come from the
//! runtime's snapshot.

use super::{Feed, FeedsRuntime, Snapshot, metrics, secrets, store_calls};
use crate::serve::config::FeedSettings;
use crate::serve::guard::RequestGuard;
use crate::serve::history::feeds::{DedupeSpec, FeedTokenRecord, OnDuplicate};
use crate::serve::state::ServerState;
use axum::Router;
use axum::extract::{ConnectInfo, Path, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use faucet_common_feed::{AppendAck, Appended, FeedError, FeedStore, FeedUid, NewEvent, TenantId};
use serde_json::json;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc, oneshot};

/// Most events one request may carry.
pub const MAX_EVENTS_PER_REQUEST: usize = 10_000;
/// The deepest JSON an event may nest; a delivery could not decode deeper.
pub const MAX_JSON_DEPTH: usize = 64;
/// Most requests appended in one store transaction.
pub const MAX_GROUP_REQUESTS: usize = 256;
/// Most events appended in one store transaction.
pub const MAX_GROUP_EVENTS: usize = 10_000;
/// Most payload bytes appended in one store transaction.
pub const MAX_GROUP_BYTES: u64 = 32 * 1024 * 1024;
/// Requests of an HMAC-only feed whose body is being read before they are
/// authenticated, at once.
pub const UNAUTHENTICATED_CONCURRENCY: usize = 32;
/// How long a feed's append task waits for work before it ends.
/// A sealed request at least this large is sealed off the async workers.
pub const SEAL_OFF_THREAD_BYTES: usize = 64 * 1024;
const BATCHER_IDLE: Duration = Duration::from_secs(30);
/// The shortest gap between two writes of one token's `last_used_at`.
pub const TOKEN_TOUCH_INTERVAL: Duration = Duration::from_secs(60);
/// The longest dedupe key stored as sent; a longer one is stored hashed.
const MAX_DEDUPE_KEY: usize = 256;
/// Request headers kept with an event, beside the feed's dedupe header.
const KEPT_HEADERS: &[&str] = &["user-agent", "x-request-id"];
const RETRY_SOON_SECS: u64 = 1;
const RETRY_STORE_SECS: u64 = 5;
const RETRY_FULL_SECS: u64 = 60;

/// The feed token a request carried in `?token=`, taken out of the URL
/// before anything else sees the request.
#[derive(Debug, Clone)]
pub struct UrlToken(pub String);

/// Why a request was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub status: StatusCode,
    pub code: &'static str,
    /// The closed metric label.
    pub reason: &'static str,
    pub message: String,
    pub retry_after: Option<u64>,
}

impl Refusal {
    fn new(
        status: StatusCode,
        code: &'static str,
        reason: &'static str,
        message: impl Into<String>,
    ) -> Self {
        Self {
            status,
            code,
            reason,
            message: message.into(),
            retry_after: None,
        }
    }

    fn retry(mut self, secs: u64) -> Self {
        self.retry_after = Some(secs.max(1));
        self
    }

    /// The one answer to an unknown feed, a bad credential and a sender
    /// address the feed does not allow.
    pub fn unauthorized() -> Self {
        Self::new(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "unauthorized",
            "unknown feed or invalid credential",
        )
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", "malformed", message)
    }

    fn too_large(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "too_large",
            "too_large",
            message,
        )
    }

    fn unavailable(reason: &'static str, message: impl Into<String>, retry: u64) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            reason,
            message,
        )
        .retry(retry)
    }
}

impl IntoResponse for Refusal {
    fn into_response(self) -> Response {
        let body = json!({"error": {"code": self.code, "message": self.message}});
        let mut resp = (self.status, axum::Json(body)).into_response();
        if let Some(secs) = self.retry_after
            && let Ok(v) = header::HeaderValue::from_str(&secs.to_string())
        {
            resp.headers_mut().insert(header::RETRY_AFTER, v);
        }
        resp
    }
}

/// One event of a request, split and checked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Piece {
    pub content_type: String,
    pub payload: Vec<u8>,
}

fn media_of(content_type: &str) -> String {
    content_type
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn is_ndjson(media: &str) -> bool {
    matches!(
        media,
        "application/x-ndjson"
            | "application/ndjson"
            | "application/jsonl"
            | "application/jsonlines"
            | "application/x-jsonlines"
    )
}

fn is_json(media: &str) -> bool {
    media == "application/json" || media.ends_with("+json")
}

/// The deepest nesting of `json`'s arrays and objects.
fn depth(json: &[u8]) -> usize {
    let (mut now, mut deepest, mut in_string, mut escaped) = (0usize, 0usize, false, false);
    for b in json {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' | b'[' => {
                now += 1;
                deepest = deepest.max(now);
            }
            b'}' | b']' => now = now.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// The key a delivery stores an event's metadata under; no event may carry it.
const RESERVED_KEY: &str = faucet_source_feed::METADATA_KEY;

/// Whether a JSON value is an object holding [`RESERVED_KEY`] at its top
/// level, compared after unescaping and counting repeated keys.
struct HoldsReserved;

impl<'de> serde::de::DeserializeSeed<'de> for HoldsReserved {
    type Value = bool;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> Result<bool, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for HoldsReserved {
    type Value = bool;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<bool, A::Error> {
        let mut found = false;
        while let Some(key) = map.next_key::<String>()? {
            found |= key == RESERVED_KEY;
            map.next_value::<serde::de::IgnoredAny>()?;
        }
        Ok(found)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<bool, A::Error> {
        while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
        Ok(false)
    }
    fn visit_bool<E>(self, _: bool) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_i64<E>(self, _: i64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_u64<E>(self, _: u64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_f64<E>(self, _: f64) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_str<E>(self, _: &str) -> Result<bool, E> {
        Ok(false)
    }
    fn visit_unit<E>(self) -> Result<bool, E> {
        Ok(false)
    }
}

/// The events of a JSON body whose array elements are events: the place
/// of the first event holding [`RESERVED_KEY`].
struct FirstReserved;

impl<'de> serde::de::Visitor<'de> for FirstReserved {
    type Value = Option<usize>;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a JSON value")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let (mut i, mut first) = (0, None);
        while let Some(found) = seq.next_element_seed(HoldsReserved)? {
            if found && first.is_none() {
                first = Some(i);
            }
            i += 1;
        }
        Ok(first)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
        Ok(HoldsReserved.visit_map(map)?.then_some(0))
    }
    fn visit_bool<E>(self, _: bool) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_i64<E>(self, _: i64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_u64<E>(self, _: u64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_f64<E>(self, _: f64) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_str<E>(self, _: &str) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
}

/// Parse `json` once: `Ok(Some(i))` names the first event (an array
/// element when `elements_are_events`, else the value itself, `0`) holding
/// the reserved metadata key.
fn parses(json: &[u8], elements_are_events: bool) -> Result<Option<usize>, String> {
    use serde::de::{DeserializeSeed, Deserializer};
    if depth(json) > MAX_JSON_DEPTH {
        return Err(format!("nested deeper than {MAX_JSON_DEPTH} levels"));
    }
    let mut de = serde_json::Deserializer::from_slice(json);
    let found = if elements_are_events {
        de.deserialize_any(FirstReserved)
    } else {
        HoldsReserved
            .deserialize(&mut de)
            .map(|found| found.then_some(0))
    }
    .and_then(|found| de.end().map(|()| found))
    .map_err(|e| e.to_string())?;
    Ok(found)
}

fn reserved(event: usize) -> Refusal {
    Refusal::bad_request(format!(
        "event {event}: the field \"{RESERVED_KEY}\" is reserved for the feed's metadata; \
         rename it (a record taken from another feed must drop \"{RESERVED_KEY}\" first)"
    ))
}

/// The elements of a JSON array, each as it was sent; `None` when `body`
/// (valid JSON) is not an array.
pub fn array_elements(body: &[u8]) -> Option<Vec<&[u8]>> {
    let start = body.iter().position(|b| !b.is_ascii_whitespace())?;
    if body[start] != b'[' {
        return None;
    }
    let mut out = Vec::new();
    let (mut depth, mut in_string, mut escaped) = (0usize, false, false);
    let mut from = start + 1;
    for (i, b) in body.iter().enumerate().skip(start + 1) {
        if in_string {
            match (escaped, b) {
                (true, _) => escaped = false,
                (false, b'\\') => escaped = true,
                (false, b'"') => in_string = false,
                _ => {}
            }
            continue;
        }
        let end = match b {
            b'"' => {
                in_string = true;
                continue;
            }
            b'{' | b'[' => {
                depth += 1;
                continue;
            }
            b'}' | b']' if depth > 0 => {
                depth -= 1;
                continue;
            }
            b']' => true,
            b',' if depth == 0 => false,
            _ => continue,
        };
        let element = body[from..i].trim_ascii();
        if !element.is_empty() {
            out.push(element);
        }
        if end {
            break;
        }
        from = i + 1;
    }
    Some(out)
}

/// Split a request body into its events and check each at the door: JSON
/// and NDJSON must parse, text must be UTF-8, no event may exceed
/// `max_event_bytes`. A JSON array and an NDJSON body are one event per
/// element or line; anything else is one event.
pub fn split(content_type: &str, body: &[u8], max_event_bytes: u64) -> Result<Vec<Piece>, Refusal> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Err(Refusal::bad_request("the request body is empty"));
    }
    let media = media_of(content_type);
    let mut pieces: Vec<Piece> = Vec::new();
    if is_ndjson(&media) {
        for (n, line) in body.split(|b| *b == b'\n').enumerate() {
            let line = line.trim_ascii();
            if line.is_empty() {
                continue;
            }
            if parses(line, false)
                .map_err(|e| Refusal::bad_request(format!("line {}: not JSON: {e}", n + 1)))?
                .is_some()
            {
                return Err(reserved(pieces.len() + 1));
            }
            pieces.push(Piece {
                content_type: "application/json".into(),
                payload: line.to_vec(),
            });
        }
    } else if is_json(&media) {
        if let Some(i) = parses(body, true)
            .map_err(|e| Refusal::bad_request(format!("the body is not JSON: {e}")))?
        {
            return Err(reserved(i + 1));
        }
        match array_elements(body) {
            Some(elements) => pieces.extend(elements.into_iter().map(|e| Piece {
                content_type: media.clone(),
                payload: e.to_vec(),
            })),
            None => pieces.push(Piece {
                content_type: media.clone(),
                payload: body.trim_ascii().to_vec(),
            }),
        }
    } else {
        if media.starts_with("text/") && std::str::from_utf8(body).is_err() {
            return Err(Refusal::bad_request("the body is not UTF-8 text"));
        }
        if media == "application/x-www-form-urlencoded"
            && url::form_urlencoded::parse(body).any(|(k, _)| k == RESERVED_KEY)
        {
            return Err(reserved(1));
        }
        pieces.push(Piece {
            content_type: content_type.trim().to_string(),
            payload: body.to_vec(),
        });
    }
    if pieces.is_empty() {
        return Err(Refusal::bad_request("the request holds no event"));
    }
    if pieces.len() > MAX_EVENTS_PER_REQUEST {
        return Err(Refusal::too_large(format!(
            "the request holds {} events; send at most {MAX_EVENTS_PER_REQUEST} per request",
            pieces.len()
        )));
    }
    if let Some(i) = pieces
        .iter()
        .position(|p| p.payload.len() as u64 > max_event_bytes)
    {
        return Err(Refusal::too_large(format!(
            "event {} is {} bytes; the feed accepts at most {max_event_bytes} per event",
            i + 1,
            pieces[i].payload.len()
        )));
    }
    Ok(pieces)
}

fn fit_key(key: String) -> String {
    if key.len() <= MAX_DEDUPE_KEY && !key.chars().any(char::is_control) {
        return key;
    }
    format!("sha256:{}", super::service::token_hash(&key))
}

/// The dedupe key of event `index` of `count`: the feed's dedupe header
/// (suffixed with the event's place in a request of several), or the value
/// at `body_path` of a JSON event. `None`: the event carries no key and is
/// stored without dedupe.
pub fn dedupe_key(
    spec: &DedupeSpec,
    headers: &HeaderMap,
    piece: &Piece,
    index: usize,
    count: usize,
) -> Option<String> {
    if let Some(name) = &spec.header {
        let value = headers.get(name.as_str())?.to_str().ok()?.trim();
        if value.is_empty() {
            return None;
        }
        return Some(fit_key(if count == 1 {
            value.to_string()
        } else {
            format!("{value}:{index}")
        }));
    }
    let path = spec.body_path.as_deref()?;
    if !is_json(&media_of(&piece.content_type)) {
        return None;
    }
    let value: serde_json::Value = serde_json::from_slice(&piece.payload).ok()?;
    let mut at = &value;
    for part in path.split('.') {
        at = at.get(part)?;
    }
    match at {
        serde_json::Value::String(s) if !s.is_empty() => Some(fit_key(s.clone())),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The key that stands for a request signed with a timestamp, so the store
/// keeps one copy of it on every server while its timestamp is accepted.
/// `None` when the feed's signature has no timestamp header.
pub fn replay_key(
    sig: &faucet_core::signature::SignatureSpec,
    headers: &HeaderMap,
    now_unix: u64,
) -> Option<String> {
    use sha2::Digest;
    let lookup = |h: &str| headers.get(h).and_then(|v| v.to_str().ok());
    let signed = sig.check().signed_request(lookup, now_unix)?;
    let digest = sha2::Sha256::digest(format!("{}.{}", signed.timestamp, signed.signature));
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    Some(format!("signed:{hex}"))
}

/// Whether `ip` is one of `allowed` (addresses or CIDR ranges); an empty
/// list allows any.
pub fn address_allowed(allowed: &[String], ip: IpAddr) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let ip = match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    };
    allowed.iter().any(|entry| {
        let (net, prefix) = match entry.split_once('/') {
            Some((net, prefix)) => match prefix.parse::<u32>() {
                Ok(bits) => (net, Some(bits)),
                Err(_) => return false,
            },
            None => (entry.as_str(), None),
        };
        match (net.parse::<IpAddr>(), ip) {
            (Ok(IpAddr::V4(net)), IpAddr::V4(ip)) => {
                let bits = prefix.unwrap_or(32).min(32);
                let mask = u32::MAX.checked_shl(32 - bits).unwrap_or(0);
                u32::from(net) & mask == u32::from(ip) & mask
            }
            (Ok(IpAddr::V6(net)), IpAddr::V6(ip)) => {
                let bits = prefix.unwrap_or(128).min(128);
                let mask = u128::MAX.checked_shl(128 - bits).unwrap_or(0);
                u128::from(net) & mask == u128::from(ip) & mask
            }
            _ => false,
        }
    })
}

/// A token bucket; it may go into debt, so a request larger than the burst
/// is admitted once the bucket is full and paid for by the wait after it.
#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    at: Instant,
}

impl Bucket {
    fn refill(&mut self, rate: f64, capacity: f64, now: Instant) {
        let gained = now.saturating_duration_since(self.at).as_secs_f64() * rate;
        self.tokens = (self.tokens + gained).min(capacity);
        self.at = now;
    }

    /// Take `n` tokens, or say how many seconds until they could be taken.
    fn take(&mut self, rate: f64, capacity: f64, n: f64, now: Instant) -> Result<(), u64> {
        self.refill(rate, capacity, now);
        let needed = n.min(capacity);
        if self.tokens >= needed {
            self.tokens -= n;
            return Ok(());
        }
        Err(((needed - self.tokens) / rate).ceil().max(1.0) as u64)
    }
}

/// The rate limits of one server (per replica).
#[derive(Debug, Default)]
struct Limiter(DashMap<String, Mutex<Bucket>>);

impl Limiter {
    fn take(&self, key: &str, rate: u32, burst: u32, n: usize) -> Result<(), u64> {
        let (rate, capacity) = (f64::from(rate.max(1)), f64::from(burst.max(1)));
        let now = Instant::now();
        let entry = self.0.entry(key.to_string()).or_insert_with(|| {
            Mutex::new(Bucket {
                tokens: capacity,
                at: now,
            })
        });
        entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take(rate, capacity, n as f64, now)
    }

    fn refund(&self, key: &str, n: usize) {
        if let Some(entry) = self.0.get(key) {
            entry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tokens += n as f64;
        }
    }
}

struct Job {
    events: Vec<NewEvent>,
    reply: oneshot::Sender<Result<AppendAck, FeedError>>,
}

fn ack_of(events: Vec<Appended>) -> AppendAck {
    let stored = || events.iter().filter(|e| !e.duplicate);
    AppendAck {
        first_seq: stored().map(|e| e.seq).min(),
        last_seq: stored().map(|e| e.seq).max(),
        duplicates: events.iter().filter(|e| e.duplicate).count() as u64,
        events,
    }
}

/// Whether `e` proves the group's append stored nothing and may come from
/// one request of it: a limit one request would cross, or a statement the
/// store refused before its commit. A store that failed at or after its
/// commit answers `Unavailable` (whether anything was stored is unknown),
/// and a cancelled or timed-out call is unknown too; those fail the group.
fn retry_alone(e: &FeedError) -> bool {
    matches!(e, FeedError::Full { .. }) || !(e.is_transient() || matches!(e, FeedError::Call(_)))
}

fn group_failed(e: &FeedError) -> FeedError {
    FeedError::Unavailable(format!(
        "the append this request shared failed; whether it was stored is unknown: {e}"
    ))
}

/// Append the requests of `jobs` in one store transaction and answer each.
/// A failure that proves nothing was stored ([`retry_alone`]) is tried
/// again request by request, so one request the store refuses fails alone;
/// any other failure fails the group. Tried alone, a request whose outcome
/// is unknown, or a second refusal in a row (the store refuses everything),
/// fails the requests still waiting without trying them.
async fn commit(store: &dyn FeedStore, uid: &FeedUid, label: &str, jobs: Vec<Job>) {
    let calls = store_calls();
    commit_with(label, jobs, |events| store.append(&calls, uid, events)).await;
}

async fn commit_with<F, Fut>(label: &str, jobs: Vec<Job>, append: F)
where
    F: Fn(Vec<NewEvent>) -> Fut,
    Fut: Future<Output = Result<AppendAck, FeedError>>,
{
    metrics::append_group(label, jobs.len());
    if jobs.len() > 1 {
        let all: Vec<NewEvent> = jobs.iter().flat_map(|j| j.events.iter().cloned()).collect();
        let started = Instant::now();
        match append(all).await {
            Ok(ack) => {
                metrics::append_duration(label, started.elapsed());
                let mut events = ack.events.into_iter();
                for job in jobs {
                    let own: Vec<Appended> = events.by_ref().take(job.events.len()).collect();
                    let _ = job.reply.send(Ok(ack_of(own)));
                }
                return;
            }
            Err(e) if retry_alone(&e) => {}
            Err(e) => {
                for job in jobs {
                    let _ = job.reply.send(Err(group_failed(&e)));
                }
                return;
            }
        }
    }
    let mut broken: Option<FeedError> = None;
    let mut refused_before = false;
    for job in jobs {
        if let Some(e) = &broken {
            let _ = job.reply.send(Err(group_failed(e)));
            continue;
        }
        let started = Instant::now();
        let result = append(job.events).await;
        metrics::append_duration(label, started.elapsed());
        match &result {
            Ok(_) | Err(FeedError::Full { .. }) => refused_before = false,
            Err(e) if retry_alone(e) && !refused_before => refused_before = true,
            Err(e) => broken = Some(group_failed(e)),
        }
        let _ = job.reply.send(result.map(|a| ack_of(a.events)));
    }
}

/// `first` and the requests already waiting behind it, within the group
/// limits.
fn group(first: Job, rx: &mut mpsc::Receiver<Job>) -> Vec<Job> {
    let size = |j: &Job| j.events.iter().map(NewEvent::size).sum::<u64>();
    let (mut events, mut bytes) = (first.events.len(), size(&first));
    let mut jobs = vec![first];
    while jobs.len() < MAX_GROUP_REQUESTS && events < MAX_GROUP_EVENTS && bytes < MAX_GROUP_BYTES {
        let Ok(next) = rx.try_recv() else {
            break;
        };
        events += next.events.len();
        bytes += size(&next);
        jobs.push(next);
    }
    jobs
}

/// Most refusals a server remembers per feed.
pub const REFUSALS_KEPT: usize = 200;

/// One refused request, as the feed's page lists it: never its payload or
/// its credential.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RefusalRecord {
    pub at: chrono::DateTime<chrono::Utc>,
    pub reason: &'static str,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    pub detail: String,
}

/// What a server keeps for ingest: limits, rate limiters and one append
/// task per feed that is receiving events.
pub struct Ingest {
    refusals: DashMap<String, Mutex<std::collections::VecDeque<RefusalRecord>>>,
    max_request_bytes: u64,
    request_timeout: Duration,
    permits: Arc<Semaphore>,
    feeds: Limiter,
    tenants: Limiter,
    unauthenticated: DashMap<String, Arc<Semaphore>>,
    batchers: Arc<DashMap<String, mpsc::Sender<Job>>>,
    touched: DashMap<String, Instant>,
}

impl std::fmt::Debug for Ingest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ingest")
            .field("max_request_bytes", &self.max_request_bytes)
            .field("appending", &self.batchers.len())
            .finish()
    }
}

impl Ingest {
    pub fn new(settings: &FeedSettings) -> Self {
        Self {
            max_request_bytes: settings.max_request_bytes,
            request_timeout: settings.request_timeout,
            permits: Arc::new(Semaphore::new(settings.max_concurrent_requests)),
            feeds: Limiter::default(),
            tenants: Limiter::default(),
            unauthenticated: DashMap::new(),
            batchers: Arc::new(DashMap::new()),
            touched: DashMap::new(),
            refusals: DashMap::new(),
        }
    }

    fn refused(&self, feed_uid: &str, record: RefusalRecord) {
        let ring = self.refusals.entry(feed_uid.to_string()).or_default();
        let mut ring = ring
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if ring.len() == REFUSALS_KEPT {
            ring.pop_front();
        }
        ring.push_back(record);
    }

    /// The requests of `feed_uid` this server refused lately, newest first.
    /// Kept in memory, per server.
    pub fn refusals(&self, feed_uid: &str) -> Vec<RefusalRecord> {
        self.refusals
            .get(feed_uid)
            .map(|ring| {
                ring.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .rev()
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Forget what is kept for a deleted feed.
    pub fn forget_feed(&self, feed_uid: &str) {
        self.feeds.0.remove(feed_uid);
        self.unauthenticated.remove(feed_uid);
        self.batchers.remove(feed_uid);
        self.refusals.remove(feed_uid);
    }

    /// Forget a deleted or revoked token.
    pub fn forget_token(&self, token_id: &str) {
        self.touched.remove(token_id);
    }

    fn sender(&self, store: &Arc<dyn FeedStore>, feed: &Feed) -> mpsc::Sender<Job> {
        let key = feed.uid.as_str().to_string();
        self.batchers
            .entry(key.clone())
            .or_insert_with(|| {
                let (tx, mut rx) = mpsc::channel::<Job>(MAX_GROUP_REQUESTS * 4);
                let (store, uid, label) = (store.clone(), feed.uid.clone(), feed.label());
                let (batchers, own) = (self.batchers.clone(), tx.clone());
                tokio::spawn(async move {
                    loop {
                        let first = match tokio::time::timeout(BATCHER_IDLE, rx.recv()).await {
                            Ok(Some(job)) => job,
                            Ok(None) => return,
                            Err(_) => break,
                        };
                        commit(&*store, &uid, &label, group(first, &mut rx)).await;
                    }
                    // Idle: leave the map, then answer what raced in.
                    batchers.remove_if(&key, |_, tx| tx.same_channel(&own));
                    drop(own);
                    rx.close();
                    while let Some(first) = rx.recv().await {
                        commit(&*store, &uid, &label, group(first, &mut rx)).await;
                    }
                });
                tx
            })
            .clone()
    }

    /// Append `events` to `feed`, sharing the store transaction with the
    /// requests that arrive while the previous one commits. Returns after
    /// the events are durable.
    pub async fn append(
        &self,
        store: &Arc<dyn FeedStore>,
        feed: &Feed,
        mut events: Vec<NewEvent>,
    ) -> Result<AppendAck, FeedError> {
        for _ in 0..3 {
            let (reply, answer) = oneshot::channel();
            match self.sender(store, feed).send(Job { events, reply }).await {
                Ok(()) => {
                    return answer.await.unwrap_or_else(|_| {
                        Err(FeedError::Unavailable(
                            "the append was interrupted; whether it was stored is unknown".into(),
                        ))
                    });
                }
                Err(mpsc::error::SendError(job)) => {
                    // The task ended between the lookup and the send.
                    self.batchers.remove(feed.uid.as_str());
                    events = job.events;
                }
            }
        }
        Err(FeedError::Unavailable(
            "the feed's append task is not running".into(),
        ))
    }

    /// Record that `token` was used, at most once a minute per token and
    /// never on the request's own path.
    fn touch(&self, state: &ServerState, token: &FeedTokenRecord) {
        let now = Instant::now();
        let due = match self.touched.entry(token.id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut seen) => {
                let due = now.duration_since(*seen.get()) >= TOKEN_TOUCH_INTERVAL;
                if due {
                    seen.insert(now);
                }
                due
            }
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(now);
                true
            }
        };
        if !due {
            return;
        }
        let (state, feed, id) = (state.clone(), token.feed.clone(), token.id.clone());
        tokio::spawn(async move {
            let history = state.history();
            let Ok(tokens) = history.feed_token_list(Some(&feed)).await else {
                return;
            };
            if let Some(current) = tokens.into_iter().find(|t| t.id == id) {
                let mut next = current.clone();
                next.last_used_at = Some(chrono::Utc::now());
                let _ = history.feed_token_replace(&current, &next).await;
            }
        });
    }
}

/// A request that may write to a feed.
struct Admitted {
    feed: Arc<Feed>,
    token: Option<FeedTokenRecord>,
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?.trim();
    let token = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
        .unwrap_or(value)
        .trim();
    (!token.is_empty() && !token.eq_ignore_ascii_case("bearer")).then_some(token)
}

/// Whether the request may write to the feed it names, as far as can be
/// told before its body is read: the feed exists, the sender's address is
/// allowed, and the feed's token (when it uses tokens) is a usable one of
/// that feed.
fn admit(
    snapshot: &Snapshot,
    tenant: Option<&str>,
    name: &str,
    peer: Option<IpAddr>,
    presented: Option<&str>,
    url_token: Option<&str>,
) -> Option<Admitted> {
    let hash = |t: &str| super::service::token_hash(t);
    let header_hash = presented.map(hash);
    let url_hash = url_token.map(hash);
    let feed = snapshot.feed(tenant, name)?;
    let auth = &feed.spec.ingest.auth;
    if !auth.allowed_addresses.is_empty()
        && !peer.is_some_and(|ip| address_allowed(&auth.allowed_addresses, ip))
    {
        return None;
    }
    if auth.tokens.is_none() {
        return Some(Admitted { feed, token: None });
    }
    let hash = header_hash.or(url_hash.filter(|_| feed.spec.ingest.url_token))?;
    let token = snapshot.token(&hash)?;
    let same = faucet_core::signature::secrets_equal;
    let matches = same(token.hash.as_bytes(), hash.as_bytes())
        && same(token.feed.as_bytes(), feed.uid.as_str().as_bytes());
    (matches && token.usable(chrono::Utc::now())).then(|| Admitted {
        feed,
        token: Some(token.clone()),
    })
}

fn full_refusal(e: &FeedError) -> Option<Refusal> {
    match e {
        FeedError::Full {
            limit: faucet_common_feed::FullLimit::TenantBytes,
            max,
            actual,
            ..
        } => Some(Refusal::unavailable(
            "full",
            format!(
                "the tenant's quota is reached (tenant_bytes): its feeds in this store would hold \
                 {actual} of the {max} bytes allowed (max_feed_bytes), counting delivered events \
                 kept for replay; room frees up as retention removes events older than \
                 retention.max_age; retry later"
            ),
            RETRY_FULL_SECS,
        )),
        FeedError::Full { limit, .. } => Some(Refusal::unavailable(
            "full",
            format!(
                "the feed is full ({limit}): its deliveries have to catch up before it accepts \
                 more events; retry later"
            ),
            RETRY_FULL_SECS,
        )),
        _ => None,
    }
}

fn store_refusal(feed: &Feed, what: &str, e: &dyn std::fmt::Display) -> Refusal {
    metrics::store_error(&feed.label());
    tracing::warn!(feed = %feed.label(), error = %e, "{what}");
    Refusal::unavailable(
        "store",
        "the feed's store is unavailable; retry shortly",
        RETRY_STORE_SECS,
    )
}

/// The events to append for `pieces`, sealed when the feed says so, each
/// with its dedupe key and the headers a feed keeps; and each one's size as
/// it was sent.
fn build_events(
    state: &ServerState,
    feed: &Feed,
    headers: &HeaderMap,
    pieces: Vec<Piece>,
    replay: Option<&str>,
) -> Result<(Vec<NewEvent>, Vec<u64>), Refusal> {
    let spec = &feed.spec.ingest;
    let count = pieces.len();
    let sealed = feed.spec.sealed();
    let context = sealed.then(|| {
        let tenant = feed.tenant().and_then(|t| TenantId::new(t).ok());
        faucet_common_feed::sealing_context(&feed.uid, tenant.as_ref())
    });
    let received_at = chrono::Utc::now();
    let dedupe_header = spec.dedupe.as_ref().and_then(|d| d.header.as_deref());
    let mut sizes = Vec::with_capacity(count);
    let mut events = Vec::with_capacity(count);
    for (i, piece) in pieces.into_iter().enumerate() {
        let key = spec
            .dedupe
            .as_ref()
            .and_then(|d| dedupe_key(d, headers, &piece, i, count))
            .or_else(|| replay.map(|r| format!("{r}:{i}")));
        sizes.push(piece.payload.len() as u64);
        let payload = match &context {
            Some(context) => {
                secrets::seal_payload(state, &piece.payload, context).map_err(|e| {
                    tracing::error!(feed = %feed.label(), error = %e, "an event could not be sealed");
                    Refusal::unavailable(
                        "seal",
                        format!("the feed's events are sealed and cannot be sealed here: {e}"),
                        RETRY_FULL_SECS,
                    )
                })?
            }
            None => piece.payload,
        };
        let mut event = NewEvent::new(piece.content_type, payload)
            .received_at(received_at)
            .sealed(sealed);
        if sealed {
            event = event.plain_size(sizes[i]);
        }
        for name in KEPT_HEADERS.iter().copied().chain(dedupe_header) {
            if let Some(value) = headers.get(name).and_then(|v| v.to_str().ok()) {
                event = event.header(name.to_ascii_lowercase(), value);
            }
        }
        if let Some(key) = key {
            event = event.dedupe_key(key);
        }
        events.push(event);
    }
    Ok((events, sizes))
}

/// Append `body` to `feed` as the ingest route would, without a request:
/// the same split, checks, dedupe keys and sealing, straight to the feed's
/// store. `dedupe` is the value the feed's dedupe header would carry. For
/// an operator who holds the store's credentials; no sender credential,
/// rate limit or tenant quota applies.
pub async fn push_local(
    state: &ServerState,
    feed: &Feed,
    content_type: &str,
    body: &[u8],
    dedupe: Option<&str>,
) -> Result<AppendAck, Refusal> {
    let spec = &feed.spec.ingest;
    if body.len() as u64 > spec.max_request_bytes.bytes() {
        return Err(Refusal::too_large(format!(
            "the body is larger than the {} bytes the feed accepts per request",
            spec.max_request_bytes.bytes()
        )));
    }
    let mut headers = HeaderMap::new();
    if let (Some(name), Some(value)) = (
        spec.dedupe.as_ref().and_then(|d| d.header.as_deref()),
        dedupe,
    ) && let (Ok(name), Ok(value)) = (
        header::HeaderName::from_bytes(name.as_bytes()),
        header::HeaderValue::from_str(value),
    ) {
        headers.insert(name, value);
    }
    let pieces = split(content_type, body, spec.max_event_bytes.bytes())?;
    let (events, _) = build_events(state, feed, &headers, pieces, None)?;
    let store = state
        .feeds()
        .store_for(feed)
        .await
        .map_err(|e| store_refusal(feed, "the feed's store could not be opened", &e))?;
    store
        .append(&store_calls(), &feed.uid, events)
        .await
        .map_err(|e| match full_refusal(&e) {
            Some(full) => full,
            None => store_refusal(feed, "appending to the feed's store failed", &e),
        })
}

type Refused = (Option<Arc<Feed>>, Refusal);

/// Whether the body is gzip; `Err` for an encoding the ingest route does not
/// decode.
fn content_encoding(headers: &HeaderMap) -> Result<bool, ()> {
    let mut values = headers.get_all(header::CONTENT_ENCODING).iter();
    let Some(first) = values.next() else {
        return Ok(false);
    };
    if values.next().is_some() {
        return Err(());
    }
    match first.to_str().map(|v| v.trim().to_ascii_lowercase()) {
        Ok(v) if v.is_empty() || v == "identity" => Ok(false),
        Ok(v) if v == "gzip" || v == "x-gzip" => Ok(true),
        _ => Err(()),
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Inflate {
    TooLarge,
    Corrupt,
}

/// Expand a gzip body, stopping one byte past `limit`: an expansion is never
/// held beyond the size an uncompressed request may have.
fn gunzip(packed: &[u8], limit: u64) -> Result<axum::body::Bytes, Inflate> {
    use std::io::Read;
    let mut plain = Vec::new();
    flate2::read::MultiGzDecoder::new(packed)
        .take(limit.saturating_add(1))
        .read_to_end(&mut plain)
        .map_err(|_| Inflate::Corrupt)?;
    if plain.len() as u64 > limit {
        return Err(Inflate::TooLarge);
    }
    Ok(plain.into())
}

async fn handle(
    state: ServerState,
    tenant: Option<String>,
    name: String,
    req: Request,
) -> Result<(Arc<Feed>, Response), Refused> {
    let rt: Arc<FeedsRuntime> = state.feeds();
    let ingest = rt.ingest();
    let before = |r: Refusal| (None, r);
    if state.is_draining() {
        return Err(before(Refusal::unavailable(
            "draining",
            "the server is shutting down; retry shortly",
            RETRY_STORE_SECS,
        )));
    }
    let Ok(_permit) = ingest.permits.clone().try_acquire_owned() else {
        return Err(before(Refusal::unavailable(
            "busy",
            "the server is handling as many ingest requests as it allows; retry shortly",
            RETRY_SOON_SECS,
        )));
    };
    let stale = || {
        Refusal::unavailable(
            "definitions_stale",
            "the server cannot read its feed definitions right now; retry shortly",
            RETRY_STORE_SECS,
        )
    };
    let usable = |s: &Snapshot| s.loaded() && s.fresh();
    let mut snapshot = rt.snapshot();
    if !usable(&snapshot) {
        if let Ok(fresh) = rt.refresh_now(&state).await {
            snapshot = fresh;
        }
        if !usable(&snapshot) {
            return Err(before(stale()));
        }
    }
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    let url_token = req.extensions().get::<UrlToken>().map(|t| t.0.clone());
    let (parts, body) = req.into_parts();
    let headers = parts.headers;
    let presented = bearer(&headers);
    let look = |s: &Snapshot| {
        admit(
            s,
            tenant.as_deref(),
            &name,
            peer,
            presented,
            url_token.as_deref(),
        )
    };
    let admitted = match look(&snapshot) {
        Some(a) => Some(a),
        // Made on another server a moment ago? Look once more, whatever was missing.
        None => match rt.refresh_now(&state).await {
            Ok(fresh) => {
                let again = look(&fresh);
                snapshot = fresh;
                again
            }
            // A 401 here could make a sender drop its events.
            Err(_) => return Err(before(stale())),
        },
    };
    let Some(Admitted { feed, token }) = admitted else {
        // The sender is told the same whether the feed exists or not.
        return Err((
            snapshot.feed(tenant.as_deref(), &name),
            Refusal::unauthorized(),
        ));
    };
    let refuse = |r: Refusal| (Some(feed.clone()), r);
    if rt.moved_of(feed.uid.as_str()).is_some() {
        return Err(refuse(Refusal::unavailable(
            "store_moved",
            "the feed's events are in another feed store than the one it names; it accepts \
             nothing until an operator fixes that; retry later",
            RETRY_STORE_SECS,
        )));
    }
    if feed.derived.signature_unreadable.is_some() {
        return Err(refuse(Refusal::unavailable(
            "seal",
            "the feed's signature secret cannot be read on this server; retry later",
            RETRY_STORE_SECS,
        )));
    }
    let spec = &feed.spec.ingest;
    let _reading = match (&token, &spec.auth.signature) {
        (None, Some(_)) => {
            let cap = ingest
                .unauthenticated
                .entry(feed.uid.as_str().to_string())
                .or_insert_with(|| Arc::new(Semaphore::new(UNAUTHENTICATED_CONCURRENCY)))
                .clone();
            match cap.try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    return Err(refuse(Refusal::unavailable(
                        "busy",
                        "the feed is verifying as many requests as it allows; retry shortly",
                        RETRY_SOON_SECS,
                    )));
                }
            }
        }
        _ => None,
    };
    let gzipped = match content_encoding(&headers) {
        Ok(gzipped) => gzipped,
        Err(()) => {
            return Err(refuse(Refusal::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported_media_type",
                "content_encoding",
                "send the body as it is or with Content-Encoding: gzip",
            )));
        }
    };
    let limit = spec.max_request_bytes.bytes().min(ingest.max_request_bytes);
    let too_large = || {
        Refusal::too_large(format!(
            "the request body is larger than the {limit} bytes the feed accepts per request"
        ))
    };
    if headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .is_some_and(|n| n > limit)
    {
        return Err(refuse(too_large()));
    }
    let read = axum::body::to_bytes(body, usize::try_from(limit).unwrap_or(usize::MAX));
    let body = match tokio::time::timeout(ingest.request_timeout, read).await {
        Err(_) => {
            return Err(refuse(Refusal::new(
                StatusCode::REQUEST_TIMEOUT,
                "timeout",
                "timeout",
                "the request body did not arrive in time",
            )));
        }
        Ok(Err(e)) => {
            let over = std::error::Error::source(&e)
                .is_some_and(|s| s.to_string().contains("length limit"));
            return Err(refuse(if over {
                too_large()
            } else {
                Refusal::bad_request("the request body could not be read")
            }));
        }
        Ok(Ok(bytes)) => bytes,
    };
    let mut replay = None;
    if let Some(sig) = &spec.auth.signature {
        let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0);
        let lookup = |h: &str| headers.get(h).and_then(|v| v.to_str().ok());
        if sig.secret.is_empty() || !sig.check().verify(lookup, &body, now) {
            return Err(refuse(Refusal::unauthorized()));
        }
        replay = replay_key(sig, &headers, now);
    }
    if let Some(token) = &token {
        ingest.touch(&state, token);
    }
    let body = if gzipped {
        let packed = body;
        match tokio::task::spawn_blocking(move || gunzip(&packed, limit)).await {
            Ok(Ok(plain)) => plain,
            Ok(Err(Inflate::TooLarge)) => {
                return Err(refuse(Refusal::too_large(format!(
                    "the request body expands to more than the {limit} bytes the feed accepts \
                     per request"
                ))));
            }
            Ok(Err(Inflate::Corrupt)) | Err(_) => {
                return Err(refuse(Refusal::bad_request(
                    "the request body is not valid gzip",
                )));
            }
        }
    } else {
        body
    };
    if !crate::serve::guard::content_type_accepted(&headers, body.is_empty(), &spec.accept) {
        return Err(refuse(Refusal::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            "content_type",
            format!(
                "the feed accepts these content types: {}",
                spec.accept.join(", ")
            ),
        )));
    }
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    let pieces = split(content_type, &body, spec.max_event_bytes.bytes()).map_err(refuse)?;
    let count = pieces.len();
    let uid = feed.uid.as_str();
    if let Some(limit) = &spec.rate_limit {
        let burst = limit.burst.unwrap_or(limit.events_per_sec);
        if let Err(wait) = ingest.feeds.take(uid, limit.events_per_sec, burst, count) {
            return Err(refuse(
                Refusal::new(
                    StatusCode::TOO_MANY_REQUESTS,
                    "rate_limited",
                    "rate_limited",
                    "the feed's rate limit is reached; retry later",
                )
                .retry(wait),
            ));
        }
    }
    if let Some(t) = feed.tenant()
        && let Some(per_sec) = snapshot
            .tenant_limits
            .get(t)
            .and_then(|l| l.max_feed_events_per_sec)
        && let Err(wait) = ingest.tenants.take(t, per_sec, per_sec, count)
    {
        if spec.rate_limit.is_some() {
            ingest.feeds.refund(uid, count);
        }
        return Err(refuse(
            Refusal::new(
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limited",
                "tenant_rate_limited",
                "the tenant's ingest rate limit is reached; retry later",
            )
            .retry(wait),
        ));
    }
    let built = if feed.spec.sealed() && body.len() >= SEAL_OFF_THREAD_BYTES {
        let (state, feed, headers, replay) =
            (state.clone(), feed.clone(), headers.clone(), replay.clone());
        tokio::task::spawn_blocking(move || {
            build_events(&state, &feed, &headers, pieces, replay.as_deref())
        })
        .await
        .unwrap_or_else(|_| {
            Err(Refusal::unavailable(
                "seal",
                "the request could not be sealed; retry shortly",
                RETRY_STORE_SECS,
            ))
        })
    } else {
        build_events(&state, &feed, &headers, pieces, replay.as_deref())
    };
    let (events, sizes) = built.map_err(refuse)?;
    let store = rt.store_for(&feed).await.map_err(|e| {
        refuse(store_refusal(
            &feed,
            "the feed's store could not be opened",
            &e,
        ))
    })?;
    let ack = ingest
        .append(&store, &feed, events)
        .await
        .map_err(|e| match full_refusal(&e) {
            Some(full) => refuse(full),
            None => refuse(store_refusal(
                &feed,
                "appending to the feed's store failed",
                &e,
            )),
        })?;
    let label = feed.label();
    let bytes: u64 = ack
        .events
        .iter()
        .zip(&sizes)
        .filter(|(e, _)| !e.duplicate)
        .map(|(_, n)| n)
        .sum();
    metrics::ingested(&label, ack.stored(), bytes);
    if ack.duplicates > 0 {
        metrics::dedupe_hits(&label, ack.duplicates);
    }
    if replay.is_some() && ack.duplicates as usize == count {
        metrics::replayed(&label);
    }
    let all_duplicates = ack.duplicates as usize == count;
    let conflict = all_duplicates
        && spec
            .dedupe
            .as_ref()
            .is_some_and(|d| d.on_duplicate == OnDuplicate::Conflict);
    let mut body = json!({
        "ids": ack.ids(),
        "first_seq": ack.first_seq.map(|s| s.get()),
        "last_seq": ack.last_seq.map(|s| s.get()),
    });
    if ack.duplicates > 0 {
        body["duplicate"] = all_duplicates.into();
        body["duplicates"] = ack.duplicates.into();
    }
    let status = if conflict {
        StatusCode::CONFLICT
    } else {
        StatusCode::ACCEPTED
    };
    Ok((feed.clone(), (status, axum::Json(body)).into_response()))
}

async fn answer(
    state: ServerState,
    tenant: Option<String>,
    name: String,
    req: Request,
) -> Response {
    let peer = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string());
    let ingest_state = state.clone();
    match handle(state, tenant, name, req).await {
        Ok((feed, response)) => {
            metrics::ingest_request(&feed.label(), response.status().as_u16());
            response
        }
        Err((feed, refusal)) => {
            // An unknown name is never a label: it is the sender's text.
            let label = feed.as_ref().map(|f| f.label()).unwrap_or_default();
            metrics::ingest_request(&label, refusal.status.as_u16());
            metrics::ingest_refused(&label, refusal.reason);
            if let Some(feed) = &feed {
                ingest_state.feeds().ingest().refused(
                    feed.uid.as_str(),
                    RefusalRecord {
                        at: chrono::Utc::now(),
                        reason: refusal.reason,
                        status: refusal.status.as_u16(),
                        sender: peer,
                        detail: refusal.message.clone(),
                    },
                );
            }
            tracing::debug!(feed = %label, reason = refusal.reason, status = refusal.status.as_u16(), "ingest request refused");
            refusal.into_response()
        }
    }
}

/// `POST /v1/feeds/{name}/events`.
pub async fn server_feed(
    State(state): State<ServerState>,
    Path(name): Path<String>,
    req: Request,
) -> Response {
    answer(state, None, name, req).await
}

/// `POST /v1/tenants/{tenant}/feeds/{name}/events`.
pub async fn tenant_feed(
    State(state): State<ServerState>,
    Path((tenant, name)): Path<(String, String)>,
    req: Request,
) -> Response {
    answer(state, Some(tenant), name, req).await
}

/// The first `token` of a query string and the query without any.
fn without_token(query: &str) -> Option<(String, String)> {
    let mut token = None;
    let mut kept = url::form_urlencoded::Serializer::new(String::new());
    for (k, v) in url::form_urlencoded::parse(query.as_bytes()) {
        if k == "token" {
            token.get_or_insert_with(|| v.into_owned());
        } else {
            kept.append_pair(&k, &v);
        }
    }
    token.map(|t| (t, kept.finish()))
}

/// Take `?token=` out of `req`'s URL and return it. Nothing is registered
/// for redaction: the request is not authenticated yet.
fn take_url_token(req: &mut Request) -> Option<String> {
    let (token, kept) = req.uri().query().and_then(without_token)?;
    let path = req.uri().path().to_string();
    let rebuilt = if kept.is_empty() {
        path.clone()
    } else {
        format!("{path}?{kept}")
    };
    let mut parts = req.uri().clone().into_parts();
    parts.path_and_query = rebuilt.parse().ok().or_else(|| path.parse().ok());
    *req.uri_mut() = axum::http::Uri::from_parts(parts)
        .ok()
        .or_else(|| path.parse().ok())
        .unwrap_or_else(|| axum::http::Uri::from_static("/"));
    Some(token)
}

/// Take `?token=` out of the request before anything can log, trace or
/// count its URL; the handler finds it in the request's extensions.
pub async fn strip_url_token(mut req: Request, next: axum::middleware::Next) -> Response {
    if let Some(token) = take_url_token(&mut req) {
        req.extensions_mut().insert(UrlToken(token));
    }
    next.run(req).await
}

/// The ingest routes with their own layers: no shared body limit (each feed
/// sets its own), request metrics, the `guard`'s Host check and the
/// `?token=` strip outermost.
pub fn router(state: ServerState, guard: Arc<RequestGuard>) -> Router {
    use axum::routing::post;
    Router::new()
        .route("/v1/feeds/{name}/events", post(server_feed))
        .route(
            "/v1/tenants/{tenant}/feeds/{name}/events",
            post(tenant_feed),
        )
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer(axum::middleware::from_fn(
            crate::serve::metrics::track_metrics,
        ))
        .layer(axum::middleware::from_fn_with_state(
            guard,
            crate::serve::guard::guard,
        ))
        .layer(axum::middleware::from_fn(strip_url_token))
        .with_state(state)
}

/// The router of the `--feeds-listen` listener: the ingest routes and
/// `/healthz`, answering for the hosts `--feeds-allowed-host` lists.
pub fn listener_router(state: ServerState, settings: &FeedSettings, listen: SocketAddr) -> Router {
    let guard = RequestGuard::new(
        &crate::serve::guard::GuardSettings {
            allowed_hosts: settings.allowed_hosts.clone(),
            allowed_origins: Vec::new(),
        },
        listen,
        false,
        &[],
    );
    Router::new()
        .route(
            "/healthz",
            axum::routing::get(crate::serve::handlers::health::healthz),
        )
        .with_state(state.clone())
        .merge(router(state, Arc::new(guard)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gz(plain: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(plain).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn a_gzip_body_expands_up_to_the_limit_and_no_further() {
        assert_eq!(&gunzip(&gz(b"hello"), 5).unwrap()[..], b"hello");
        assert_eq!(gunzip(&gz(b"hello!"), 5), Err(Inflate::TooLarge));
        let bomb = gz(&vec![0u8; 8 * 1024 * 1024]);
        assert!(bomb.len() < 16 * 1024);
        assert_eq!(gunzip(&bomb, 1024), Err(Inflate::TooLarge));
        assert_eq!(gunzip(b"not gzip", 1024), Err(Inflate::Corrupt));
        let mut torn = gz(b"a body that is cut short");
        torn.truncate(torn.len() - 6);
        assert_eq!(gunzip(&torn, 1024), Err(Inflate::Corrupt));
        let mut two = gz(b"ab");
        two.extend(gz(b"cd"));
        assert_eq!(&gunzip(&two, 4).unwrap()[..], b"abcd");
        assert_eq!(gunzip(&two, 3), Err(Inflate::TooLarge));
    }

    #[test]
    fn only_gzip_and_identity_are_decoded() {
        let with = |values: &[&str]| {
            let mut h = HeaderMap::new();
            for v in values {
                h.append(header::CONTENT_ENCODING, v.parse().unwrap());
            }
            content_encoding(&h)
        };
        assert_eq!(with(&[]), Ok(false));
        assert_eq!(with(&["identity"]), Ok(false));
        assert_eq!(with(&[" GZIP "]), Ok(true));
        assert_eq!(with(&["x-gzip"]), Ok(true));
        for refused in [
            &["br"][..],
            &["zstd"],
            &["deflate"],
            &["gzip, gzip"],
            &["gzip", "gzip"],
        ] {
            assert_eq!(with(refused), Err(()), "{refused:?}");
        }
    }

    fn payloads(pieces: &[Piece]) -> Vec<&str> {
        pieces
            .iter()
            .map(|p| std::str::from_utf8(&p.payload).unwrap())
            .collect()
    }

    #[test]
    fn a_body_splits_into_events_by_its_content_type() {
        let one = split("application/json; charset=utf-8", b" {\"a\": [1, 2]} ", 100).unwrap();
        assert_eq!(payloads(&one), vec!["{\"a\": [1, 2]}"]);
        assert_eq!(one[0].content_type, "application/json");

        let array = split(
            "application/json",
            br#" [ {"a": "x,]\"y"}, [1, [2]], 3 , "s", null ] "#,
            100,
        )
        .unwrap();
        assert_eq!(
            payloads(&array),
            vec![r#"{"a": "x,]\"y"}"#, "[1, [2]]", "3", "\"s\"", "null"]
        );
        let big = split("application/json", b"[123456789012345678901234567890]", 100).unwrap();
        assert_eq!(payloads(&big), vec!["123456789012345678901234567890"]);

        let lines = split(
            "application/x-ndjson",
            b"{\"id\":1}\r\n\n  {\"id\":2}  \n[3]\n",
            100,
        )
        .unwrap();
        assert_eq!(payloads(&lines), vec!["{\"id\":1}", "{\"id\":2}", "[3]"]);
        assert!(lines.iter().all(|p| p.content_type == "application/json"));

        let vendor = split("application/vnd.thing+json", b"{}", 100).unwrap();
        assert_eq!(vendor[0].content_type, "application/vnd.thing+json");
        let text = split("text/plain; charset=utf-8", b"hello\nworld", 100).unwrap();
        assert_eq!(payloads(&text), vec!["hello\nworld"]);
        assert_eq!(text[0].content_type, "text/plain; charset=utf-8");
        let form = split("application/x-www-form-urlencoded", b"a=1&b=2", 100).unwrap();
        assert_eq!(payloads(&form), vec!["a=1&b=2"]);
        let bytes = split("application/octet-stream", &[0xff, 0x00], 100).unwrap();
        assert_eq!(bytes[0].payload, vec![0xff, 0x00]);
    }

    #[test]
    fn a_body_that_cannot_be_stored_is_refused_with_its_reason() {
        let cases: [(&str, &[u8], StatusCode, &str); 9] = [
            (
                "application/json",
                b"  \n",
                StatusCode::BAD_REQUEST,
                "empty",
            ),
            (
                "application/json",
                b"{",
                StatusCode::BAD_REQUEST,
                "not JSON",
            ),
            (
                "application/json",
                b"[]",
                StatusCode::BAD_REQUEST,
                "no event",
            ),
            (
                "application/x-ndjson",
                b"{}\nnope\n",
                StatusCode::BAD_REQUEST,
                "line 2: not JSON",
            ),
            (
                "text/plain",
                &[0xff, 0xfe],
                StatusCode::BAD_REQUEST,
                "UTF-8",
            ),
            (
                "application/json",
                b"{\"k\": \"0123456789\"}",
                StatusCode::PAYLOAD_TOO_LARGE,
                "event 1 is 19 bytes",
            ),
            (
                "application/json",
                b"[1, \"0123456789abcdef\"]",
                StatusCode::PAYLOAD_TOO_LARGE,
                "event 2 is 18 bytes",
            ),
            (
                "application/x-ndjson",
                b"1\n\"0123456789abcdef\"",
                StatusCode::PAYLOAD_TOO_LARGE,
                "event 2",
            ),
            (
                "text/plain",
                b"0123456789abcdefg",
                StatusCode::PAYLOAD_TOO_LARGE,
                "event 1 is 17 bytes",
            ),
        ];
        for (content_type, body, status, want) in cases {
            let refusal = split(content_type, body, 16).unwrap_err();
            assert_eq!(refusal.status, status, "{want}");
            assert!(
                refusal.message.contains(want),
                "{want}: {}",
                refusal.message
            );
        }
        let many = format!("[{}]", vec!["1"; MAX_EVENTS_PER_REQUEST + 1].join(","));
        let refusal = split("application/json", many.as_bytes(), 16).unwrap_err();
        assert_eq!(refusal.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(refusal.message.contains("10001 events"));
        // Deeper than the parser allows is malformed, not a crash.
        let deep = format!("{}{}", "[".repeat(400), "]".repeat(400));
        assert_eq!(
            split("application/json", deep.as_bytes(), 10_000)
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        let nested = |n: usize| format!("{}1{}", "{\"a\":".repeat(n), "}".repeat(n));
        assert!(
            split(
                "application/x-ndjson",
                nested(MAX_JSON_DEPTH).as_bytes(),
                10_000
            )
            .is_ok()
        );
        let too_deep = split(
            "application/x-ndjson",
            nested(MAX_JSON_DEPTH + 1).as_bytes(),
            10_000,
        )
        .unwrap_err();
        assert!(too_deep.message.contains("nested deeper than 64"));
        assert_eq!(depth(br#"{"a": "[[[\"{", "b": [1, {"c": []}]}"#), 4);
        assert_eq!(array_elements(b"   "), None);
        assert_eq!(array_elements(b" {\"a\": []}"), None);
    }

    #[test]
    fn an_event_holding_the_metadata_key_is_refused() {
        for (content_type, body, event) in [
            ("application/json", &br#"{"_feed": 1}"#[..], 1),
            ("application/json", br#"{"\u005ffeed": 1}"#, 1),
            (
                "application/json",
                br#"{"a": 1, "_feed": 2, "_feed": 3}"#,
                1,
            ),
            (
                "application/json",
                br#"[{"a": 1}, 2, {"b": {"_feed": 1}, "_feed": 0}]"#,
                3,
            ),
            ("application/json; charset=utf-8", br#"{"_feed": {}}"#, 1),
            ("application/x-ndjson", b"{\"a\":1}\n\n{\"_feed\":1}\n", 2),
            ("application/x-www-form-urlencoded", b"a=1&_feed=2", 1),
            ("application/x-www-form-urlencoded", b"%5Ffeed=2", 1),
            ("application/x-www-form-urlencoded", b"_fe%65d=2", 1),
            (
                "Application/X-WWW-Form-Urlencoded; charset=utf-8",
                b"_feed=2",
                1,
            ),
        ] {
            let refusal = split(content_type, body, 1_000).unwrap_err();
            assert_eq!(refusal.status, StatusCode::BAD_REQUEST, "{content_type}");
            assert!(
                refusal
                    .message
                    .starts_with(&format!("event {event}: the field \"_feed\"")),
                "{content_type}: {}",
                refusal.message
            );
        }
        for (content_type, body) in [
            ("application/json", &br#"{"a": {"_feed": 1}}"#[..]),
            ("application/json", br#"["_feed", [{"_feed": 1}]]"#),
            ("application/json", br#""_feed""#),
            ("application/x-ndjson", b"[{\"_feed\": 1}]\n"),
            ("application/x-www-form-urlencoded", b"a=_feed&x_feed=1"),
            ("text/plain", b"_feed=1"),
        ] {
            assert!(split(content_type, body, 1_000).is_ok(), "{content_type}");
        }
    }

    fn dedupe(header: Option<&str>, path: Option<&str>) -> DedupeSpec {
        serde_json::from_value(json!({"header": header, "body_path": path})).unwrap()
    }

    #[test]
    fn a_dedupe_key_comes_from_the_header_or_the_event() {
        let mut headers = HeaderMap::new();
        headers.insert("x-event-id", " evt-1 ".parse().unwrap());
        let piece = |body: &str| Piece {
            content_type: "application/json".into(),
            payload: body.as_bytes().to_vec(),
        };
        let by_header = dedupe(Some("X-Event-Id"), None);
        assert_eq!(
            dedupe_key(&by_header, &headers, &piece("{}"), 0, 1).as_deref(),
            Some("evt-1")
        );
        assert_eq!(
            dedupe_key(&by_header, &headers, &piece("{}"), 2, 3).as_deref(),
            Some("evt-1:2")
        );
        assert_eq!(
            dedupe_key(&by_header, &HeaderMap::new(), &piece("{}"), 0, 1),
            None
        );
        let mut blank = HeaderMap::new();
        blank.insert("x-event-id", " ".parse().unwrap());
        assert_eq!(dedupe_key(&by_header, &blank, &piece("{}"), 0, 1), None);

        let by_path = dedupe(None, Some("event.id"));
        for (body, want) in [
            (r#"{"event": {"id": "abc"}}"#, Some("abc")),
            (r#"{"event": {"id": 42}}"#, Some("42")),
            (r#"{"event": {"id": true}}"#, Some("true")),
            (r#"{"event": {"id": ""}}"#, None),
            (r#"{"event": {"id": null}}"#, None),
            (r#"{"event": {"id": [1]}}"#, None),
            (r#"{"event": 1}"#, None),
            (r#"[1]"#, None),
        ] {
            assert_eq!(
                dedupe_key(&by_path, &headers, &piece(body), 0, 1).as_deref(),
                want,
                "{body}"
            );
        }
        let text = Piece {
            content_type: "text/plain".into(),
            payload: b"{\"event\": {\"id\": 1}}".to_vec(),
        };
        assert_eq!(dedupe_key(&by_path, &headers, &text, 0, 1), None);
        let long = "k".repeat(300);
        let key = dedupe_key(
            &dedupe(None, Some("id")),
            &headers,
            &piece(&format!("{{\"id\": \"{long}\"}}")),
            0,
            1,
        )
        .unwrap();
        assert!(key.starts_with("sha256:") && key.len() == 71, "{key}");
    }

    #[test]
    fn an_allow_list_matches_addresses_and_ranges() {
        let list: Vec<String> = ["10.0.0.0/8", "192.0.2.7", "2001:db8::/32", "0.0.0.0/0x"]
            .map(String::from)
            .to_vec();
        for (ip, allowed) in [
            ("10.200.3.4", true),
            ("192.0.2.7", true),
            ("192.0.2.8", false),
            ("2001:db8:1::5", true),
            ("2001:db9::1", false),
            ("::ffff:10.1.2.3", true),
            ("::1", false),
            ("203.0.113.9", false),
        ] {
            assert_eq!(address_allowed(&list, ip.parse().unwrap()), allowed, "{ip}");
        }
        assert!(address_allowed(&[], "203.0.113.9".parse().unwrap()));
        assert!(address_allowed(
            &["0.0.0.0/0".into()],
            "203.0.113.9".parse().unwrap()
        ));
        assert!(address_allowed(
            &["::/0".into()],
            "2001:db8::1".parse().unwrap()
        ));
    }

    #[test]
    fn a_bucket_refills_and_lets_a_large_request_through_on_credit() {
        let start = Instant::now();
        let mut b = Bucket {
            tokens: 10.0,
            at: start,
        };
        assert_eq!(b.take(5.0, 10.0, 10.0, start), Ok(()));
        assert_eq!(b.take(5.0, 10.0, 1.0, start), Err(1));
        assert_eq!(b.take(5.0, 10.0, 5.0, start), Err(1));
        let later = start + Duration::from_secs(1);
        assert_eq!(b.take(5.0, 10.0, 5.0, later), Ok(()));
        // More than the burst: admitted from a full bucket, then repaid.
        let full = later + Duration::from_secs(10);
        assert_eq!(b.take(5.0, 10.0, 30.0, full), Ok(()));
        assert_eq!(b.take(5.0, 10.0, 1.0, full), Err(5));
        assert_eq!(
            b.take(5.0, 10.0, 1.0, full + Duration::from_secs(5)),
            Ok(())
        );

        let limiter = Limiter::default();
        assert_eq!(limiter.take("f", 1, 2, 2), Ok(()));
        assert!(limiter.take("f", 1, 2, 1).is_err());
        limiter.refund("f", 1);
        limiter.refund("unknown", 1);
        assert_eq!(limiter.take("f", 1, 2, 1), Ok(()));
        assert_eq!(limiter.take("other", 1, 2, 1), Ok(()));
    }

    #[test]
    fn a_bearer_token_is_read_with_or_without_its_scheme() {
        let with = |v: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::AUTHORIZATION, v.parse().unwrap());
            h
        };
        assert_eq!(bearer(&with("Bearer fdt_abc")), Some("fdt_abc"));
        assert_eq!(bearer(&with("bearer  fdt_abc ")), Some("fdt_abc"));
        assert_eq!(bearer(&with("fdt_abc")), Some("fdt_abc"));
        assert_eq!(bearer(&with("Bearer ")), None);
        assert_eq!(bearer(&HeaderMap::new()), None);
    }

    #[test]
    fn a_requests_share_of_a_group_ack_is_its_own() {
        use faucet_common_feed::Seq;
        let own = ack_of(vec![
            Appended {
                id: "a".into(),
                seq: Seq(3),
                duplicate: true,
            },
            Appended {
                id: "b".into(),
                seq: Seq(8),
                duplicate: false,
            },
            Appended {
                id: "c".into(),
                seq: Seq(9),
                duplicate: false,
            },
        ]);
        assert_eq!((own.first_seq, own.last_seq), (Some(Seq(8)), Some(Seq(9))));
        assert_eq!((own.duplicates, own.stored()), (1, 2));
        let none = ack_of(vec![Appended {
            id: "a".into(),
            seq: Seq(3),
            duplicate: true,
        }]);
        assert_eq!(
            (none.first_seq, none.last_seq, none.stored()),
            (None, None, 0)
        );
    }

    #[test]
    fn the_token_is_taken_out_of_a_query_string() {
        assert_eq!(
            without_token("a=1&token=fdt_x&b=%20c&token=second"),
            Some(("fdt_x".to_string(), "a=1&b=+c".to_string()))
        );
        assert_eq!(
            without_token("token=fdt%5Fx"),
            Some(("fdt_x".to_string(), String::new()))
        );
        assert_eq!(without_token("a=1&tokens=2"), None);
        assert_eq!(without_token(""), None);
    }

    fn jobs(
        n: usize,
    ) -> (
        Vec<Job>,
        Vec<oneshot::Receiver<Result<AppendAck, FeedError>>>,
    ) {
        (0..n)
            .map(|i| {
                let (reply, answer) = oneshot::channel();
                let events = vec![NewEvent::new("text/plain", format!("e{i}").into_bytes())];
                (Job { events, reply }, answer)
            })
            .unzip()
    }

    fn full() -> FeedError {
        FeedError::Full {
            feed: FeedUid::new("f1").unwrap(),
            limit: faucet_common_feed::FullLimit::UndeliveredEvents,
            max: 1,
            actual: 2,
        }
    }

    #[tokio::test]
    async fn an_ambiguous_group_failure_fails_every_request_and_appends_nothing_again() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        let (group, answers) = jobs(3);
        commit_with("f", group, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Err(FeedError::Unavailable(
                    "connection reset during commit".into(),
                ))
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "nothing is appended again");
        for answer in answers {
            assert!(matches!(
                answer.await.unwrap(),
                Err(FeedError::Unavailable(_))
            ));
        }
    }

    #[tokio::test]
    async fn a_full_group_is_tried_request_by_request_until_a_store_failure() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        let (group, answers) = jobs(4);
        commit_with("f", group, |events| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            async move {
                match n {
                    0 | 2 => Err(full()),
                    1 => Ok(AppendAck {
                        first_seq: None,
                        last_seq: None,
                        duplicates: 0,
                        events: events
                            .iter()
                            .map(|_| Appended {
                                id: "a".into(),
                                seq: faucet_common_feed::Seq(1),
                                duplicate: false,
                            })
                            .collect(),
                    }),
                    _ => Err(FeedError::Unavailable("timeout".into())),
                }
            }
        })
        .await;
        let mut got = Vec::new();
        for answer in answers {
            got.push(answer.await.unwrap());
        }
        assert!(got[0].as_ref().is_ok_and(|a| a.stored() == 1));
        assert!(matches!(got[1], Err(FeedError::Full { .. })));
        assert!(matches!(&got[2], Err(FeedError::Unavailable(m)) if m == "timeout"));
        assert!(matches!(&got[3], Err(FeedError::Unavailable(m)) if m.contains("unknown")));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "the last request is not tried"
        );
    }

    fn stored(events: &[NewEvent]) -> AppendAck {
        AppendAck {
            first_seq: None,
            last_seq: None,
            duplicates: 0,
            events: events
                .iter()
                .map(|_| Appended {
                    id: "a".into(),
                    seq: faucet_common_feed::Seq(1),
                    duplicate: false,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn a_request_the_store_refuses_fails_alone_and_its_neighbours_are_stored_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        let (group, answers) = jobs(3);
        commit_with("f", group, |events| {
            calls.fetch_add(1, Ordering::SeqCst);
            let bad = events.iter().any(|e| e.payload == b"e1");
            async move {
                if bad {
                    Err(FeedError::Backend("invalid byte sequence".into()))
                } else {
                    Ok(stored(&events))
                }
            }
        })
        .await;
        let mut got = Vec::new();
        for answer in answers {
            got.push(answer.await.unwrap());
        }
        assert!(got[0].as_ref().is_ok_and(|a| a.stored() == 1));
        assert!(matches!(got[1], Err(FeedError::Backend(_))));
        assert!(got[2].as_ref().is_ok_and(|a| a.stored() == 1));
        assert_eq!(calls.load(Ordering::SeqCst), 4, "the group, then each once");
    }

    #[tokio::test]
    async fn a_cancelled_or_ambiguous_group_is_never_tried_request_by_request() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        for failure in [
            || {
                FeedError::Call(faucet_core::FaucetError::Cancelled {
                    operation: "append".into(),
                })
            },
            || FeedError::Unavailable("the append's outcome is unknown".into()),
        ] {
            let calls = AtomicUsize::new(0);
            let (group, answers) = jobs(3);
            commit_with("f", group, |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Err(failure()) }
            })
            .await;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            for answer in answers {
                assert!(matches!(
                    answer.await.unwrap(),
                    Err(FeedError::Unavailable(_))
                ));
            }
        }
    }

    #[tokio::test]
    async fn a_store_that_refuses_everything_is_not_tried_for_every_request() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = AtomicUsize::new(0);
        let (group, answers) = jobs(5);
        commit_with("f", group, |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            async { Err(FeedError::Backend("permission denied".into())) }
        })
        .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "the group and two requests"
        );
        for answer in answers {
            assert!(answer.await.unwrap().is_err());
        }
    }

    #[test]
    fn a_key_with_a_control_character_is_stored_hashed() {
        for key in ["a\u{0}b", "tab\there", "\u{1f}", "line\n"] {
            let fitted = fit_key(key.to_string());
            assert!(
                fitted.starts_with("sha256:") && fitted.len() == 71,
                "{key:?}"
            );
            assert_eq!(
                fitted,
                fit_key(key.to_string()),
                "the same key, the same hash"
            );
        }
        assert_eq!(fit_key("plain-key é".into()), "plain-key é");
    }

    #[test]
    fn a_url_token_leaves_the_url_and_is_never_registered_as_a_secret() {
        let word = "unauthenticated-url-token-word";
        let mut req = Request::builder()
            .uri(format!("/v1/feeds/orders/events?a=1&token={word}"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert_eq!(take_url_token(&mut req).as_deref(), Some(word));
        assert_eq!(req.uri().to_string(), "/v1/feeds/orders/events?a=1");
        let line = format!("a log line with {word} in it");
        assert_eq!(crate::secrets::registry::redact(&line), line);

        let mut bare = Request::builder()
            .uri(format!("/v1/feeds/orders/events?token={word}"))
            .body(axum::body::Body::empty())
            .unwrap();
        assert!(take_url_token(&mut bare).is_some());
        assert_eq!(bare.uri().to_string(), "/v1/feeds/orders/events");
    }

    #[test]
    fn a_refusal_answers_with_its_status_and_retry_after() {
        let resp = Refusal::unavailable("store", "down", 0).into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()[header::RETRY_AFTER], "1");
        let resp = Refusal::unauthorized().into_response();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert!(resp.headers().get(header::RETRY_AFTER).is_none());
        let full = full_refusal(&FeedError::Full {
            feed: FeedUid::new("f").unwrap(),
            limit: faucet_common_feed::FullLimit::UndeliveredBytes,
            max: 1,
            actual: 2,
        })
        .unwrap();
        assert_eq!(full.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(full.retry_after, Some(RETRY_FULL_SECS));
        assert!(full.message.contains("(undelivered_bytes)") && full.message.contains("catch up"));
        let quota = full_refusal(&FeedError::Full {
            feed: FeedUid::new("f").unwrap(),
            limit: faucet_common_feed::FullLimit::TenantBytes,
            max: 100,
            actual: 120,
        })
        .unwrap();
        assert_eq!(
            (quota.status, quota.reason),
            (StatusCode::SERVICE_UNAVAILABLE, "full")
        );
        assert!(
            quota.message.contains("(tenant_bytes)"),
            "{}",
            quota.message
        );
        assert!(
            quota.message.contains("120 of the 100 bytes"),
            "{}",
            quota.message
        );
        assert!(
            quota.message.contains("retention.max_age"),
            "{}",
            quota.message
        );
        assert!(!quota.message.contains("catch up"), "{}", quota.message);
        assert!(full_refusal(&FeedError::Unavailable("x".into())).is_none());
    }
}

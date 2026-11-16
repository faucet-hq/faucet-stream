//! `faucet feed` (#1034): manage feeds straight in their stores, the way
//! `faucet template` manages templates. `--store` names the run history a
//! server keeps its feed definitions in; the events are in the feed store
//! that history implies (or a `--feed-store`). Every verb goes through the
//! rules the HTTP API uses (`serve::feeds::service`); nothing here starts
//! delivery runs: a server does. `faucet feed send` is the one verb that
//! talks to a running server: it posts to the ingest route as a sender does.

use crate::cli::{FeedArgs, FeedCommand, FeedSendArgs, FeedTokenCommand};
use crate::error::{CliError, CliResult};
use crate::serve::feeds::service::{self, Position};
use crate::serve::feeds::{FeedsRuntime, ingest, spec, stores};
use crate::serve::history::feeds::FeedSpec;
use crate::serve::rbac::{AuthContext, Role};
use crate::serve::state::ServerState;
use clap::Parser;
use serde::Serialize;
use std::io::Read;

/// Who a `faucet feed` change is made by.
const PRINCIPAL: &str = "cli";

fn failed(e: crate::serve::error::ServeError) -> CliError {
    CliError::Serve(e.api_error().error.message)
}

/// The server-shaped state the feed rules run against: the history at
/// `--store`, the feed stores it implies and the vault key, with no
/// scheduler.
async fn open(args: &FeedArgs) -> CliResult<ServerState> {
    let store = args.store.as_deref().ok_or_else(|| {
        CliError::Config(
            "pass --store <URL> (or FAUCET_FEED_STORE): the run history the server keeps its \
             feeds in, e.g. sqlite:/var/lib/faucet/history.db or postgres://…"
                .into(),
        )
    })?;
    let mut argv: Vec<String> = ["faucet", "serve", "--no-auth", "--no-env-file", "--history"]
        .map(String::from)
        .to_vec();
    argv.push(store.to_string());
    for s in &args.feed_store {
        argv.extend(["--feed-store".to_string(), s.clone()]);
    }
    if let Some(key) = &args.vault_key {
        argv.extend(["--vault-key".to_string(), key.clone()]);
    }
    let cli = crate::cli::Cli::try_parse_from(argv).map_err(|e| CliError::Config(e.to_string()))?;
    let crate::cli::Command::Serve(serve) = cli.command else {
        return Err(CliError::Config(
            "could not build the store settings".into(),
        ));
    };
    let config = crate::serve::config::ServeConfig::from_args(*serve)?;
    let history = crate::serve::history::connect(
        &config.history,
        config.idempotency_retention,
        config.lease_ttl,
        &uuid::Uuid::new_v4().to_string(),
    )
    .await
    .map_err(|e| CliError::Serve(format!("opening --store: {e}")))?;
    let state = ServerState::new(
        &config,
        None,
        tokio_util::sync::CancellationToken::new(),
        history,
        crate::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        crate::serve::triggers::health::TriggersHandle::empty(),
    );
    #[cfg(feature = "encryption")]
    if let Some(v) = &config.vault {
        let vault =
            crate::serve::vault::Vault::new(&v.key, &v.previous).map_err(CliError::Serve)?;
        state.set_vault(Some(std::sync::Arc::new(vault)));
    }
    state.set_feeds(
        FeedsRuntime::new(
            config.feeds.clone(),
            spec::LoadedFile::default(),
            stores::Stores::from_config(&config),
        )
        .offline(),
    );
    Ok(state)
}

fn actor() -> AuthContext {
    AuthContext {
        principal: PRINCIPAL.into(),
        role: Role::Admin,
        source_ip: None,
        tenant: None,
    }
}

fn print<T: Serialize>(json: bool, value: &T, human: impl FnOnce() -> String) -> CliResult<()> {
    if json {
        let text = serde_json::to_string_pretty(value)
            .map_err(|e| CliError::Serve(format!("serializing output: {e}")))?;
        println!("{text}");
    } else {
        println!("{}", human());
    }
    Ok(())
}

async fn read_spec(path: &std::path::Path) -> CliResult<FeedSpec> {
    let text = if path.as_os_str() == "-" {
        let mut text = String::new();
        std::io::stdin()
            .read_to_string(&mut text)
            .map_err(|e| CliError::Config(format!("reading stdin: {e}")))?;
        text
    } else {
        tokio::fs::read_to_string(path)
            .await
            .map_err(|e| CliError::Config(format!("reading {}: {e}", path.display())))?
    };
    let json = path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("json"));
    if json {
        serde_json::from_str(&text).map_err(|e| CliError::Config(format!("feed definition: {e}")))
    } else {
        crate::yaml::from_str(&text).map_err(|e| CliError::Config(format!("feed definition: {e}")))
    }
}

fn feed_line(f: &service::FeedView) -> String {
    let pending = f.deliveries.iter().filter_map(|d| d.lag).max().unwrap_or(0);
    format!(
        "{:<28} {:<10} events {:<8} pending {:<8} deliveries {}",
        service::label_of(f.tenant.as_deref(), &f.name),
        f.status,
        f.watermark.unwrap_or(0),
        pending,
        f.deliveries.len()
    )
}

fn feed_text(d: &service::FeedDetail) -> String {
    let f = &d.feed;
    let mut out = vec![
        format!(
            "feed {} ({})",
            service::label_of(f.tenant.as_deref(), &f.name),
            f.status
        ),
        format!("  store {} · push to {}", f.store, f.ingest_path),
        format!(
            "  events {}..{} · {} stored byte(s) · {} in the last 24h",
            f.oldest.unwrap_or(0),
            f.watermark.unwrap_or(0),
            f.stored_bytes.unwrap_or(0),
            f.events_24h.unwrap_or(0)
        ),
    ];
    for dl in &f.deliveries {
        out.push(format!(
            "  delivery {:<20} {:<14} next event {} · {} pending{}",
            dl.name,
            dl.status,
            dl.position.unwrap_or(0),
            dl.lag.unwrap_or(0),
            dl.last_error
                .as_ref()
                .map(|e| format!(" · {e}"))
                .unwrap_or_default()
        ));
    }
    for g in &d.gaps {
        out.push(format!(
            "  gap {}..{} ({}) by {}",
            g.from, g.to, g.reason, g.by
        ));
    }
    for t in &d.tokens {
        let state = if t.revoked_at.is_some() {
            "revoked"
        } else {
            "active"
        };
        out.push(format!("  token {} {} {} {state}", t.id, t.name, t.prefix));
    }
    out.join("\n")
}

fn read_body(file: Option<&std::path::Path>) -> CliResult<Vec<u8>> {
    match file.filter(|p| p.as_os_str() != "-") {
        Some(path) => std::fs::read(path)
            .map_err(|e| CliError::Config(format!("reading {}: {e}", path.display()))),
        None => {
            let mut body = Vec::new();
            std::io::stdin()
                .read_to_end(&mut body)
                .map_err(|e| CliError::Config(format!("reading stdin: {e}")))?;
            Ok(body)
        }
    }
}

fn event_line(e: &service::EventView) -> String {
    format!(
        "#{:<10} {} {} {:<24} {} B",
        e.seq,
        e.id,
        e.received_at
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        e.content_type,
        e.size
    )
}

/// The events `[from, …)` of a feed in order, at most `limit`, with their
/// payloads when asked, and the last stored event's number.
async fn events_from(
    state: &ServerState,
    who: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    from: Option<u64>,
    limit: u64,
    payload: bool,
) -> CliResult<(Vec<service::EventView>, u64)> {
    let page = service::events(state, who, tenant, name, None, from, Some(limit))
        .await
        .map_err(failed)?;
    let mut events = Vec::with_capacity(page.events.len());
    for e in page.events.into_iter().rev() {
        events.push(if payload {
            service::event(state, who, tenant, name, e.seq)
                .await
                .map_err(failed)?
        } else {
            e
        });
    }
    Ok((events, page.watermark))
}

fn print_events(json: bool, events: &[service::EventView]) -> CliResult<()> {
    for e in events {
        if json {
            let line = serde_json::to_string(e)
                .map_err(|e| CliError::Serve(format!("serializing output: {e}")))?;
            println!("{line}");
            continue;
        }
        println!("{}", event_line(e));
        match (&e.payload, e.payload_encoding, e.payload_withheld) {
            (Some(text), Some("utf8"), _) => println!("  {text}"),
            (Some(text), _, _) => println!("  base64:{text}"),
            (None, _, Some(why)) => println!("  (payload withheld: {why})"),
            _ => {}
        }
    }
    Ok(())
}

async fn tail(
    state: &ServerState,
    who: &AuthContext,
    tenant: Option<&str>,
    name: &str,
    json: bool,
    (lines, follow, interval_secs, payload): (u64, bool, u64, bool),
) -> CliResult<()> {
    let lines = lines.clamp(1, service::MAX_EVENTS_PAGE);
    let (events, watermark) = events_from(state, who, tenant, name, None, lines, payload).await?;
    print_events(json, &events)?;
    if !follow {
        if events.is_empty() && !json {
            println!("no events");
        }
        return Ok(());
    }
    let pause = std::time::Duration::from_secs(interval_secs.max(1));
    let waits = faucet_core::io::CallContext::new();
    let mut next = events.last().map_or(watermark, |e| e.seq) + 1;
    loop {
        let (events, _) = events_from(
            state,
            who,
            tenant,
            name,
            Some(next),
            service::MAX_EVENTS_PAGE,
            payload,
        )
        .await?;
        print_events(json, &events)?;
        match events.last() {
            Some(last) => next = last.seq + 1,
            None => waits
                .wait(pause)
                .await
                .map_err(|e| CliError::Serve(e.to_string()))?,
        }
    }
}

/// `faucet feed stats`: a feed's size, throughput and backlog.
#[derive(Debug, Serialize)]
pub struct Stats {
    pub feed: String,
    pub status: String,
    pub store: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retained_events: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stored_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_24h: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes_24h: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub undelivered_ratio: Option<f64>,
    pub gaps: u64,
    pub deliveries: Vec<DeliveryStats>,
}

/// One delivery in `faucet feed stats`.
#[derive(Debug, Serialize)]
pub struct DeliveryStats {
    pub name: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oldest_undelivered_at: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivered_24h: Option<u64>,
    pub batch_events: u64,
    pub consecutive_failures: u32,
}

fn stats_of(d: &service::FeedDetail) -> Stats {
    let f = &d.feed;
    Stats {
        feed: service::label_of(f.tenant.as_deref(), &f.name),
        status: f.status.clone(),
        store: f.store.clone(),
        oldest: f.oldest,
        watermark: f.watermark,
        retained_events: match (f.oldest, f.watermark) {
            (Some(oldest), Some(watermark)) => Some((watermark + 1).saturating_sub(oldest)),
            _ => None,
        },
        stored_bytes: f.stored_bytes,
        events_24h: f.events_24h,
        bytes_24h: f.bytes_24h,
        undelivered_ratio: f.undelivered_ratio,
        gaps: d.gaps.len() as u64,
        deliveries: f
            .deliveries
            .iter()
            .map(|dl| DeliveryStats {
                name: dl.name.clone(),
                status: dl.status.clone(),
                pending: dl.lag,
                oldest_undelivered_at: dl.oldest_undelivered_at,
                delivered_24h: dl.delivered_24h,
                batch_events: dl.batch_events,
                consecutive_failures: dl.consecutive_failures,
            })
            .collect(),
    }
}

fn count(n: Option<u64>) -> String {
    n.map_or_else(|| "?".to_string(), |n| n.to_string())
}

fn stats_text(s: &Stats) -> String {
    let mut out = vec![
        format!("feed {} ({}) · store {}", s.feed, s.status, s.store),
        format!(
            "  retained  {} event(s), {} byte(s) · events {}..{}",
            count(s.retained_events),
            count(s.stored_bytes),
            count(s.oldest),
            count(s.watermark)
        ),
        format!(
            "  last 24h  {} event(s), {} byte(s)",
            count(s.events_24h),
            count(s.bytes_24h)
        ),
    ];
    if let Some(ratio) = s.undelivered_ratio {
        out.push(format!(
            "  undelivered {:.0}% of the feed's limit",
            ratio * 100.0
        ));
    }
    if s.gaps > 0 {
        out.push(format!("  gaps      {}", s.gaps));
    }
    for d in &s.deliveries {
        out.push(format!(
            "  delivery {:<20} {:<14} {} pending · {} delivered in 24h · next run up to {} event(s){}",
            d.name,
            d.status,
            count(d.pending),
            count(d.delivered_24h),
            d.batch_events,
            match d.oldest_undelivered_at {
                Some(at) => format!(
                    " · oldest pending from {}",
                    at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                ),
                None => String::new(),
            }
        ));
    }
    out.join("\n")
}

/// `faucet feed cursor`: where one delivery reads next.
#[derive(Debug, Serialize)]
pub struct Cursor {
    pub delivery: String,
    pub status: String,
    /// The next event the delivery reads.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<u64>,
    /// The last stored event's number.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim: Option<crate::serve::feeds::status::LiveClaim>,
    pub gaps: Vec<service::GapView>,
}

fn cursors_of(d: &service::FeedDetail, only: Option<&str>) -> CliResult<Vec<Cursor>> {
    let out: Vec<Cursor> = d
        .feed
        .deliveries
        .iter()
        .filter(|dl| only.is_none_or(|name| name == dl.name))
        .map(|dl| Cursor {
            delivery: dl.name.clone(),
            status: dl.status.clone(),
            position: dl.position,
            epoch: dl.epoch,
            pending: dl.lag,
            watermark: d.feed.watermark,
            claim: dl.claim.clone(),
            gaps: d
                .gaps
                .iter()
                .filter(|g| g.delivery.as_deref() == Some(dl.name.as_str()))
                .cloned()
                .collect(),
        })
        .collect();
    match only {
        Some(name) if out.is_empty() => Err(CliError::Config(format!(
            "feed {} has no delivery named {name}",
            d.feed.name
        ))),
        _ => Ok(out),
    }
}

fn cursors_text(cursors: &[Cursor]) -> String {
    if cursors.is_empty() {
        return "no deliveries".to_string();
    }
    let mut out = Vec::new();
    for c in cursors {
        out.push(format!(
            "{:<20} {:<14} next event {} · epoch {} · {} pending of {}",
            c.delivery,
            c.status,
            count(c.position),
            count(c.epoch),
            count(c.pending),
            count(c.watermark)
        ));
        if let Some(claim) = &c.claim {
            out.push(format!(
                "  claimed {}..{}{}",
                claim.from,
                claim.to.saturating_sub(1),
                claim
                    .run_id
                    .as_ref()
                    .map(|r| format!(" by run {r}"))
                    .unwrap_or_default()
            ));
        }
        for g in &c.gaps {
            out.push(format!(
                "  gap {}..{} ({}) by {}",
                g.from, g.to, g.reason, g.by
            ));
        }
    }
    out.join("\n")
}

fn secret_from_env(var: &str, what: &str) -> CliResult<String> {
    match std::env::var(var) {
        Ok(v) if !v.is_empty() => {
            crate::secrets::registry::register(&v);
            Ok(v)
        }
        _ => Err(CliError::Config(format!(
            "{what}: the environment variable {var} is not set"
        ))),
    }
}

fn named<T: serde::de::DeserializeOwned>(flag: &str, value: &str) -> CliResult<T> {
    serde_json::from_value(serde_json::Value::String(value.to_ascii_lowercase()))
        .map_err(|_| CliError::Config(format!("{flag}: '{value}' is not a value it takes")))
}

/// The request `faucet feed send` makes: its headers and the bytes it sends.
pub struct Outgoing {
    pub headers: reqwest::header::HeaderMap,
    pub body: Vec<u8>,
}

/// Build the request of `faucet feed send` over `body`, signed at `now`
/// (Unix seconds).
pub fn outgoing(args: &FeedSendArgs, body: Vec<u8>, now: u64) -> CliResult<Outgoing> {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
    let name = |flag: &str, n: &str| {
        HeaderName::from_bytes(n.trim().as_bytes())
            .map_err(|_| CliError::Config(format!("{flag}: '{n}' is not a header name")))
    };
    let value = |flag: &str, v: &str| {
        HeaderValue::from_str(v)
            .map_err(|_| CliError::Config(format!("{flag}: the value cannot be sent as a header")))
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        reqwest::header::CONTENT_TYPE,
        value("--content-type", &args.content_type)?,
    );
    for pair in &args.headers {
        let (n, v) = pair.split_once('=').ok_or_else(|| {
            CliError::Config(format!("--header '{pair}': write it as NAME=VALUE"))
        })?;
        headers.insert(name("--header", n)?, value("--header", v)?);
    }
    let body = if args.gzip {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(&body)
            .and_then(|()| enc.finish())
            .map_err(|e| CliError::Serve(format!("compressing the body: {e}")))?
    } else {
        body
    };
    if args.gzip {
        headers.insert(
            reqwest::header::CONTENT_ENCODING,
            HeaderValue::from_static("gzip"),
        );
    }
    let token = match (&args.token, &args.token_env) {
        (Some(t), _) => {
            crate::secrets::registry::register(t);
            Some(t.clone())
        }
        (None, Some(var)) => Some(secret_from_env(var, "--token-env")?),
        (None, None) => None,
    };
    if let Some(token) = &token {
        let mut v = value("--token", &format!("Bearer {token}"))?;
        v.set_sensitive(true);
        headers.insert(reqwest::header::AUTHORIZATION, v);
    }
    if let (Some(var), Some(header)) = (&args.signature_secret_env, &args.signature_header) {
        let secret = secret_from_env(var, "--signature-secret-env")?;
        let check = faucet_core::signature::SignatureCheck {
            header,
            secret: secret.as_bytes(),
            algorithm: named("--signature-algorithm", &args.signature_algorithm)?,
            encoding: named("--signature-encoding", &args.signature_encoding)?,
            prefix: args.signature_prefix.as_deref(),
            timestamp_header: args.signature_timestamp_header.as_deref(),
            tolerance_secs: faucet_core::signature::DEFAULT_TOLERANCE_SECS,
        };
        let stamp = check.timestamp_header.map(|_| now);
        let signature = check.sign(&body, stamp).ok_or_else(|| {
            CliError::Config("--signature-secret-env: the secret cannot sign a request".into())
        })?;
        let mut v = value("--signature-header", &signature)?;
        v.set_sensitive(true);
        headers.insert(name("--signature-header", header)?, v);
        if let Some(ts) = check.timestamp_header {
            headers.insert(
                name("--signature-timestamp-header", ts)?,
                value("--signature-timestamp-header", &now.to_string())?,
            );
        }
    } else if token.is_none() {
        return Err(CliError::Config(
            "a sender needs the feed's credential: --token-env (or --token), or \
             --signature-secret-env with --signature-header"
                .into(),
        ));
    }
    Ok(Outgoing { headers, body })
}

/// `faucet feed send`: post events to a running server's ingest route.
async fn send(args: &FeedSendArgs, json: bool) -> CliResult<()> {
    let safe = |text: String| crate::secrets::registry::redact_message(&text);
    let url = url::Url::parse(&args.url)
        .ok()
        .filter(|u| matches!(u.scheme(), "http" | "https"))
        .ok_or_else(|| CliError::Config("--url: give the feed's http(s) ingest URL".into()))?;
    for (k, v) in url.query_pairs() {
        if k == "token" {
            crate::secrets::registry::register(&v);
        }
    }
    let now = u64::try_from(chrono::Utc::now().timestamp()).unwrap_or(0);
    let out = outgoing(args, read_body(args.file.as_deref())?, now)?;
    let timeout = std::time::Duration::from_secs(args.timeout_secs.max(1));
    let client = faucet_core::http::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| CliError::Serve(safe(format!("building the HTTP client: {e}"))))?;
    let calls = faucet_core::io::CallContext::new().with_timeout(timeout);
    let request = client.post(url).headers(out.headers).body(out.body);
    let resp = client
        .call(&calls, "send", request)
        .await
        .map_err(|e| CliError::Serve(safe(format!("the request failed: {e}"))))?;
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let text = client
        .text(resp)
        .await
        .map_err(|e| CliError::Serve(safe(format!("reading the answer failed: {e}"))))?;
    let answer: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
    if !status.is_success() {
        let message = answer["error"]["message"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| text.chars().take(300).collect());
        return Err(CliError::Serve(safe(format!(
            "the server answered {}: {message}{}",
            status.as_u16(),
            retry_after
                .map(|s| format!(" (retry after {s} s)"))
                .unwrap_or_default()
        ))));
    }
    print(json, &answer, || {
        let stored = answer["ids"].as_array().map_or(0, Vec::len) as u64;
        let duplicates = answer["duplicates"].as_u64().unwrap_or(0);
        format!(
            "stored {} event(s){}",
            stored.saturating_sub(duplicates),
            match duplicates {
                0 => String::new(),
                n => format!(", {n} already stored"),
            }
        )
    })
}

/// Run `faucet feed …`.
pub async fn run(args: FeedArgs) -> CliResult<()> {
    if let FeedCommand::Send(send_args) = &args.command {
        return send(send_args, args.json).await;
    }
    let state = open(&args).await?;
    let (who, json) = (actor(), args.json);
    let tenant = args.tenant.as_deref();
    match &args.command {
        FeedCommand::List => {
            let feeds = service::list(&state, &who, tenant).await.map_err(failed)?;
            print(json, &feeds, || {
                if feeds.is_empty() {
                    "no feeds".to_string()
                } else {
                    feeds.iter().map(feed_line).collect::<Vec<_>>().join("\n")
                }
            })
        }
        FeedCommand::Show { name } => {
            let d = service::get(&state, &who, tenant, name)
                .await
                .map_err(failed)?;
            print(json, &d, || feed_text(&d))
        }
        FeedCommand::Create { file } => {
            let spec = read_spec(file).await?;
            let made = service::create(&state, &who, tenant, spec)
                .await
                .map_err(failed)?;
            print(json, &made, || format!("created feed {}", made.name))
        }
        FeedCommand::Apply { file } => {
            let spec = read_spec(file).await?;
            let name = spec.name.clone();
            let exists = match service::find(&state, tenant, &name).await {
                Ok(_) => true,
                Err(crate::serve::error::ServeError::NotFound) => false,
                Err(e) => return Err(failed(e)),
            };
            let view = if exists {
                service::update(&state, &who, tenant, &name, spec).await
            } else {
                service::create(&state, &who, tenant, spec).await
            }
            .map_err(failed)?;
            print(json, &view, || {
                format!(
                    "{} feed {name} (revision {})",
                    if exists { "updated" } else { "created" },
                    view.revision
                )
            })
        }
        FeedCommand::Delete { name } => {
            service::delete(&state, &who, tenant, name)
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"deleted": name}), || {
                format!("deleted feed {name}")
            })
        }
        FeedCommand::Pause {
            name,
            delivery,
            reason,
        } => {
            service::pause(&state, &who, tenant, name, delivery, reason.clone())
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"paused": delivery}), || {
                format!("paused {name}/{delivery}")
            })
        }
        FeedCommand::Resume { name, delivery } => {
            service::resume(&state, &who, tenant, name, delivery)
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"resumed": delivery}), || {
                format!("resumed {name}/{delivery}")
            })
        }
        FeedCommand::DeliverNow { name, delivery } => {
            service::deliver_now(&state, &who, tenant, name, delivery)
                .await
                .map_err(failed)?;
            print(json, &serde_json::json!({"deliver_now": delivery}), || {
                format!("{name}/{delivery}: a server starts its next run without waiting")
            })
        }
        FeedCommand::Replay {
            name,
            delivery,
            to_seq,
            to_time,
        } => {
            let from = match (to_seq, to_time) {
                (Some(seq), None) => Position::Seq(*seq),
                (None, Some(time)) => Position::Time(
                    chrono::DateTime::parse_from_rfc3339(time)
                        .map_err(|e| CliError::Config(format!("--to-time '{time}': {e}")))?
                        .with_timezone(&chrono::Utc),
                ),
                _ => {
                    return Err(CliError::Config(
                        "a replay names where it starts: exactly one of --to-seq and --to-time"
                            .into(),
                    ));
                }
            };
            let moved = service::replay(&state, &who, tenant, name, delivery, from)
                .await
                .map_err(failed)?;
            print(json, &moved, || {
                format!(
                    "{name}/{delivery}: moved back from event {} to {}",
                    moved.from, moved.to
                )
            })
        }
        FeedCommand::Skip {
            name,
            delivery,
            to,
            reason,
        } => {
            let moved = service::skip(&state, &who, tenant, name, delivery, *to, reason)
                .await
                .map_err(failed)?;
            print(json, &moved, || {
                format!(
                    "{name}/{delivery}: skipped events {}..{}",
                    moved.from,
                    moved.to.saturating_sub(1)
                )
            })
        }
        FeedCommand::Recover { name, delivery } => {
            let moved = service::recover(&state, &who, tenant, name, delivery)
                .await
                .map_err(failed)?;
            print(json, &moved, || {
                format!("{name}/{delivery}: continues from event {}", moved.to)
            })
        }
        FeedCommand::Events {
            name,
            before,
            from,
            limit,
        } => {
            let page = service::events(&state, &who, tenant, name, *before, *from, *limit)
                .await
                .map_err(failed)?;
            print(json, &page, || {
                page.events
                    .iter()
                    .map(event_line)
                    .collect::<Vec<_>>()
                    .join("\n")
            })
        }
        FeedCommand::Push {
            name,
            file,
            content_type,
            dedupe_key,
        } => {
            let body = read_body(file.as_deref())?;
            let found = service::find(&state, tenant, name).await.map_err(failed)?;
            let ack = ingest::push_local(
                &state,
                &found.feed,
                content_type,
                &body,
                dedupe_key.as_deref(),
            )
            .await
            .map_err(|r| CliError::Serve(r.message))?;
            let out = serde_json::json!({
                "ids": ack.ids(),
                "first_seq": ack.first_seq.map(|s| s.get()),
                "last_seq": ack.last_seq.map(|s| s.get()),
                "duplicates": ack.duplicates,
            });
            print(json, &out, || {
                format!(
                    "stored {} event(s){}",
                    ack.stored(),
                    match ack.duplicates {
                        0 => String::new(),
                        n => format!(", {n} already stored"),
                    }
                )
            })
        }
        FeedCommand::Tail {
            name,
            lines,
            follow,
            interval_secs,
            payload,
        } => {
            let how = (*lines, *follow, *interval_secs, *payload);
            tail(&state, &who, tenant, name, json, how).await
        }
        FeedCommand::Stats { name } => {
            let d = service::get(&state, &who, tenant, name)
                .await
                .map_err(failed)?;
            let stats = stats_of(&d);
            print(json, &stats, || stats_text(&stats))
        }
        FeedCommand::Cursor { name, delivery } => {
            let d = service::get(&state, &who, tenant, name)
                .await
                .map_err(failed)?;
            let cursors = cursors_of(&d, delivery.as_deref())?;
            print(json, &cursors, || cursors_text(&cursors))
        }
        FeedCommand::Send(send_args) => send(send_args, json).await,
        FeedCommand::Token(token) => match token {
            FeedTokenCommand::Create {
                name,
                token_name,
                expires_at,
            } => {
                let expires = expires_at
                    .as_deref()
                    .map(|t| {
                        chrono::DateTime::parse_from_rfc3339(t)
                            .map(|t| t.with_timezone(&chrono::Utc))
                            .map_err(|e| CliError::Config(format!("--expires-at '{t}': {e}")))
                    })
                    .transpose()?;
                let (view, text) =
                    service::token_create(&state, &who, tenant, name, token_name, expires)
                        .await
                        .map_err(failed)?;
                let mut out =
                    serde_json::to_value(&view).map_err(|e| CliError::Serve(e.to_string()))?;
                out["token"] = text.clone().into();
                print(json, &out, || {
                    format!(
                        "token {} ({}): {text}\nIt is shown once; store it now.",
                        view.name, view.id
                    )
                })
            }
            FeedTokenCommand::List { name } => {
                let tokens = service::token_list(&state, &who, tenant, name)
                    .await
                    .map_err(failed)?;
                print(json, &tokens, || {
                    tokens
                        .iter()
                        .map(|t| {
                            format!(
                                "{} {:<20} {} {}",
                                t.id,
                                t.name,
                                t.prefix,
                                if t.revoked_at.is_some() {
                                    "revoked"
                                } else {
                                    "active"
                                }
                            )
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
            }
            FeedTokenCommand::Revoke { name, id } => {
                let view = service::token_revoke(&state, &who, tenant, name, id)
                    .await
                    .map_err(failed)?;
                print(json, &view, || format!("revoked token {}", view.id))
            }
        },
    }
}

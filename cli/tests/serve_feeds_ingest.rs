//! Feed ingest over HTTP (#1034): senders push events with a feed token or a
//! signature, every refusal has its status, an acknowledged event is in the
//! store, and what was pushed is delivered by a run. In process, over a real
//! SQLite history and feed store and a real listener.
#![cfg(all(
    feature = "feeds",
    feature = "sink-file",
    feature = "encryption",
    feature = "tenants",
    feature = "notify"
))]
#![allow(clippy::disallowed_methods)] // tests pace their own polling and use a plain HTTP client

use clap::Parser;
use faucet_cli::serve::config::ServeConfig;
use faucet_cli::serve::feeds::runner::Scheduler;
use faucet_cli::serve::feeds::{FeedsRuntime, service, spec, store_calls, stores};
use faucet_cli::serve::history::feeds::FeedSpec;
use faucet_cli::serve::rbac::{AuthContext, Role};
use faucet_cli::serve::state::ServerState;
use faucet_common_feed::{FeedStore, FeedUid, Seq};
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

const VAULT_KEY: &str = "test-vault-key-0123456789abcdef0123";

fn admin() -> AuthContext {
    AuthContext {
        principal: "alice".into(),
        role: Role::Admin,
        source_ip: None,
        tenant: None,
    }
}

fn config(dir: &Path, extra: &[&str]) -> ServeConfig {
    config_as(dir, &["--no-auth"], extra)
}

fn config_as(dir: &Path, auth: &[&str], extra: &[&str]) -> ServeConfig {
    let root = dir.display().to_string();
    let history = format!("sqlite:{}", dir.join("history.db").display());
    let mut argv = vec![
        "faucet",
        "serve",
        "--listen",
        "127.0.0.1:0",
        "--no-env-file",
        "--no-ui",
        "--history",
        &history,
        "--vault-key",
        VAULT_KEY,
        "--allow-path-root",
        &root,
    ];
    argv.extend_from_slice(auth);
    argv.extend_from_slice(extra);
    let cli = faucet_cli::cli::Cli::try_parse_from(argv).unwrap();
    let faucet_cli::cli::Command::Serve(args) = cli.command else {
        panic!("not a serve command");
    };
    ServeConfig::from_args(*args).unwrap()
}

struct Srv {
    state: ServerState,
    scheduler: Scheduler,
    addr: SocketAddr,
    client: reqwest::Client,
}

async fn listen(router: axum::Router) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    addr
}

struct Answer {
    status: u16,
    retry_after: Option<String>,
    body: Value,
}

impl Srv {
    async fn state(config: &ServeConfig) -> ServerState {
        let history = faucet_cli::serve::history::connect(
            &config.history,
            config.idempotency_retention,
            config.lease_ttl,
            &uuid::Uuid::new_v4().to_string(),
        )
        .await
        .unwrap();
        let state = ServerState::new(
            config,
            None,
            tokio_util::sync::CancellationToken::new(),
            history,
            faucet_cli::serve::logs::LogHub::new(),
            None,
            #[cfg(feature = "triggers")]
            faucet_cli::serve::triggers::health::TriggersHandle::empty(),
        );
        state.set_vault(Some(Arc::new(
            faucet_cli::serve::vault::Vault::new(VAULT_KEY, &[]).unwrap(),
        )));
        state.set_feeds(FeedsRuntime::new(
            config.feeds.clone(),
            spec::LoadedFile::default(),
            stores::Stores::from_config(config),
        ));
        state
    }

    async fn start(dir: &Path, extra: &[&str]) -> Self {
        Self::over(config(dir, extra)).await
    }

    async fn over(config: ServeConfig) -> Self {
        let state = Self::state(&config).await;
        let router =
            faucet_cli::serve::server::build_router(state.clone(), &config, &Default::default());
        Self {
            addr: listen(router).await,
            state,
            scheduler: Scheduler::default(),
            client: reqwest::Client::new(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> Answer {
        let resp = req.send().await.unwrap();
        let status = resp.status().as_u16();
        let retry_after = resp
            .headers()
            .get("retry-after")
            .map(|v| v.to_str().unwrap().to_string());
        let text = resp.text().await.unwrap();
        Answer {
            status,
            retry_after,
            body: serde_json::from_str(&text).unwrap_or(Value::String(text)),
        }
    }

    /// POST a JSON body to a server feed with a bearer token.
    async fn push(&self, feed: &str, token: &str, body: &str) -> Answer {
        self.send(
            self.client
                .post(self.url(&format!("/v1/feeds/{feed}/events")))
                .bearer_auth(token)
                .header("content-type", "application/json")
                .body(body.to_string()),
        )
        .await
    }

    /// Create a feed and (when it uses tokens) one token.
    async fn feed(&self, spec: Value) -> (FeedUid, String) {
        let spec: FeedSpec = serde_json::from_value(spec).unwrap();
        let (name, tokens) = (spec.name.clone(), spec.ingest.auth.tokens.is_some());
        let made = service::create(&self.state, &admin(), None, spec)
            .await
            .unwrap();
        let token = if tokens {
            service::token_create(&self.state, &admin(), None, &name, "sender", None)
                .await
                .unwrap()
                .1
        } else {
            String::new()
        };
        (FeedUid::new(made.uid).unwrap(), token)
    }

    async fn store(&self, feed: &str) -> Arc<dyn FeedStore> {
        let found = service::find(&self.state, None, feed).await.unwrap();
        self.state.feeds().store_for(&found.feed).await.unwrap()
    }

    async fn watermark(&self, feed: &str, uid: &FeedUid) -> u64 {
        self.store(feed)
            .await
            .heads(&store_calls(), std::slice::from_ref(uid))
            .await
            .unwrap()
            .remove(0)
            .watermark
            .get()
    }

    async fn tick(&mut self) {
        self.scheduler.invalidate();
        self.scheduler.tick(&self.state).await;
    }
}

async fn register(state: &ServerState, id: &str, state_dir: &Path) {
    let body = format!(
        "version: 1\nkind: pipeline\nname: {id}\nparams:\n  out: {{ type: string, required: true }}\npipeline:\n  source: {{ type: feed }}\n  sink: {{ type: file, config: {{ path: \"${{param.out}}\", format: json_lines, if_exists: append }} }}\n  state: {{ type: file, config: {{ path: \"{}\" }} }}\ntests:\n  suite:\n    auto: {{ defaults_baseline: true }}\n",
        state_dir.display()
    );
    faucet_cli::templates::register(
        &state.history(),
        faucet_cli::templates::store::RegisterRequest {
            id: Some(id.into()),
            body,
            format: faucet_cli::serve::load::ConfigFormat::Yaml,
            description: None,
            tags: Vec::new(),
            launch: true,
            created_by: None,
        },
    )
    .await
    .unwrap();
}

fn delivery(name: &str, template: &str, out: &Path) -> Value {
    json!({
        "name": name,
        "template": {"id": template, "params": {"out": out.display().to_string()}},
        "flush": {"max_events": 1, "min_interval": "0s"}
    })
}

fn lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// Tick the scheduler until `out` holds `count` records.
async fn delivered(srv: &mut Srv, out: &Path, count: usize) -> Vec<Value> {
    for _ in 0..600 {
        srv.tick().await;
        let got = lines(out);
        if got.len() >= count {
            return got;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!(
        "timed out waiting for {count} records; got {:?}",
        lines(out)
    );
}

/// Send raw bytes and read the response until the server closes.
async fn raw(addr: SocketAddr, request: &[u8], pause_before_close: Duration) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream.write_all(request).await.unwrap();
    let mut out = Vec::new();
    let _ = tokio::time::timeout(pause_before_close, stream.read_to_end(&mut out)).await;
    String::from_utf8_lossy(&out).into_owned()
}

fn gz(plain: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(plain).unwrap();
    enc.finish().unwrap()
}

fn sign(secret: &str, signed: &[u8]) -> String {
    use hmac::{Hmac, Mac};
    let mut mac =
        <Hmac<sha2::Sha256> as hmac::digest::KeyInit>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signed);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushed_events_are_stored_then_delivered_by_a_run() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut srv = Srv::start(root, &[]).await;
    register(&srv.state, "orders_lake", &root.join("state")).await;
    let out = root.join("lake.jsonl");
    let (uid, token) = srv
        .feed(json!({
            "name": "orders",
            "ingest": {"auth": {"tokens": "managed"}},
            "deliveries": [delivery("lake", "orders_lake", &out)]
        }))
        .await;

    // One event.
    let one = srv
        .push("orders", &token, r#"{"id": 1, "amount": 12.50}"#)
        .await;
    assert_eq!(one.status, 202, "{}", one.body);
    assert_eq!(one.body["first_seq"], 1);
    assert_eq!(one.body["last_seq"], 1);
    assert_eq!(one.body["ids"].as_array().unwrap().len(), 1);
    assert!(one.body.get("duplicate").is_none());
    // The answer came after the commit: the event is in the store.
    assert_eq!(srv.watermark("orders", &uid).await, 1);

    // A JSON array is one event per element, in one atomic append.
    let array = srv
        .push(
            "orders",
            &token,
            r#"[{"id": 2}, {"id": 3}, 123456789012345678901234567890]"#,
        )
        .await;
    assert_eq!(array.status, 202, "{}", array.body);
    assert_eq!(
        (&array.body["first_seq"], &array.body["last_seq"]),
        (&json!(2), &json!(4))
    );
    assert_eq!(array.body["ids"].as_array().unwrap().len(), 3);

    // NDJSON is one event per line; a raw `Authorization` value works too.
    let ndjson = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events"))
                .header("authorization", &token)
                .header("content-type", "application/x-ndjson")
                .header("user-agent", "sender/1.0")
                .header("x-request-id", "req-7")
                .header("cookie", "session=secret-cookie")
                .body("{\"id\": 5}\n\n{\"id\": 6}\n"),
        )
        .await;
    assert_eq!(ndjson.status, 202, "{}", ndjson.body);
    assert_eq!(
        (&ndjson.body["first_seq"], &ndjson.body["last_seq"]),
        (&json!(5), &json!(6))
    );

    // Text and a form are one event each.
    for (content_type, body) in [
        ("text/plain; charset=utf-8", "plain words"),
        ("application/x-www-form-urlencoded", "id=8&name=a+b"),
    ] {
        let a = srv
            .send(
                srv.client
                    .post(srv.url("/v1/feeds/orders/events"))
                    .bearer_auth(&token)
                    .header("content-type", content_type)
                    .body(body),
            )
            .await;
        assert_eq!(a.status, 202, "{content_type}: {}", a.body);
    }
    assert_eq!(srv.watermark("orders", &uid).await, 8);

    // The stored event keeps the allow-listed headers only.
    let store = srv.store("orders").await;
    let batch = store
        .read(&store_calls(), &uid, Seq(5), Seq(6))
        .await
        .unwrap();
    let headers = &batch.events[0].headers;
    assert_eq!(headers["user-agent"], "sender/1.0");
    assert_eq!(headers["x-request-id"], "req-7");
    assert!(!headers.contains_key("authorization") && !headers.contains_key("cookie"));
    assert!(!batch.events[0].sealed);

    // A run delivers what was pushed, in order, each element its own record.
    let got = delivered(&mut srv, &out, 8).await;
    assert_eq!(got[0]["id"], 1);
    assert_eq!(got[0]["amount"], 12.5);
    assert_eq!(got[0]["_feed"]["seq"], 1);
    assert_eq!(got[0]["_feed"]["id"], one.body["ids"][0]);
    assert_eq!(got[1]["id"], 2);
    assert_eq!(got[3]["value"], "123456789012345678901234567890");
    assert_eq!(got[4]["id"], 5);
    assert_eq!(got[4]["_feed"]["headers"]["x-request-id"], "req-7");
    assert_eq!(got[6]["value"], "plain words");
    assert_eq!(got[7]["name"], "a b");

    // The token's use is recorded, off the request's path.
    let mut used = None;
    for _ in 0..100 {
        used = service::token_list(&srv.state, &admin(), None, "orders")
            .await
            .unwrap()[0]
            .last_used_at;
        if used.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(used.is_some(), "last_used_at is written");

    // A revoked token stops working at once on this server.
    let id = service::token_list(&srv.state, &admin(), None, "orders")
        .await
        .unwrap()[0]
        .id
        .clone();
    service::token_revoke(&srv.state, &admin(), None, "orders", &id)
        .await
        .unwrap();
    assert_eq!(srv.push("orders", &token, "{}").await.status, 401);
    // An expired one is refused even before the next refresh drops it.
    let soon = chrono::Utc::now() + chrono::Duration::milliseconds(300);
    let (_, short) =
        service::token_create(&srv.state, &admin(), None, "orders", "short", Some(soon))
            .await
            .unwrap();
    assert_eq!(srv.push("orders", &short, "{}").await.status, 202);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(srv.push("orders", &short, "{}").await.status, 401);

    // A serve bearer token, or none, is not a feed credential.
    let none = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events"))
                .header("content-type", "application/json")
                .body("{}"),
        )
        .await;
    assert_eq!(none.status, 401);

    // While the server drains, senders are told to retry.
    srv.state.set_draining();
    let (_, fresh) = service::token_create(&srv.state, &admin(), None, "orders", "late", None)
        .await
        .unwrap();
    let draining = srv.push("orders", &fresh, "{}").await;
    assert_eq!(draining.status, 503, "{}", draining.body);
    assert!(draining.retry_after.is_some());
}

/// Every refusal has its own status; before a credential is accepted, the
/// answer is the same whatever was wrong.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_refusal_has_its_status_and_an_unknown_feed_looks_like_a_bad_token() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(
        dir.path(),
        &[
            "--feeds-max-request-bytes",
            "4096",
            "--feeds-request-timeout-secs",
            "1",
            "--feeds-max-concurrent-requests",
            "2",
        ],
    )
    .await;
    let (uid, token) = srv
        .feed(json!({
            "name": "orders",
            "ingest": {
                "auth": {"tokens": "managed"},
                "accept": ["application/json", "application/x-ndjson", "text/*"],
                "max_event_bytes": 64,
                "max_request_bytes": "1KiB"
            }
        }))
        .await;
    let (_, other_token) = srv
        .feed(json!({"name": "other", "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    srv.feed(json!({
        "name": "fenced",
        "ingest": {"auth": {"tokens": "managed", "allowed_addresses": ["203.0.113.0/24"]}}
    }))
    .await;
    let fenced_token = service::token_create(&srv.state, &admin(), None, "fenced", "t", None)
        .await
        .unwrap()
        .1;

    // Unknown feed, wrong token, another feed's token, no token, a token in
    // the URL of a feed that does not allow it, a sender address outside the
    // feed's list: one answer.
    let url_token = srv
        .send(
            srv.client
                .post(srv.url(&format!("/v1/feeds/orders/events?token={token}")))
                .header("content-type", "application/json")
                .body("{}"),
        )
        .await;
    let answers = [
        srv.push("no-such-feed", &token, "{}").await,
        srv.push("orders", "fdt_wrong", "{}").await,
        srv.push("orders", &other_token, "{}").await,
        srv.push("orders", "", "{}").await,
        url_token,
        srv.push("fenced", &fenced_token, "{}").await,
        srv.send(
            srv.client
                .post(srv.url("/v1/tenants/acme/feeds/orders/events"))
                .bearer_auth(&token)
                .header("content-type", "application/json")
                .body("{}"),
        )
        .await,
    ];
    for a in &answers {
        assert_eq!(a.status, 401, "{}", a.body);
        assert_eq!(a.body, answers[0].body);
        assert_eq!(a.retry_after, None);
        assert_eq!(
            a.body["error"]["message"],
            "unknown feed or invalid credential"
        );
    }
    // Nothing a refused request carries is checked or stored: an oversized
    // body with a bad token is still just a 401.
    let big = "x".repeat(8192);
    assert_eq!(srv.push("orders", "fdt_wrong", &big).await.status, 401);

    let post = |content_type: &'static str, body: Vec<u8>| {
        let req = srv
            .client
            .post(srv.url("/v1/feeds/orders/events"))
            .bearer_auth(&token)
            .body(body);
        let req = if content_type.is_empty() {
            req
        } else {
            req.header("content-type", content_type)
        };
        srv.send(req)
    };
    let cases: Vec<(&str, Vec<u8>, u16, &str)> = vec![
        ("application/json", b"{".to_vec(), 400, "not JSON"),
        ("application/json", b" ".to_vec(), 400, "empty"),
        ("application/json", b"[]".to_vec(), 400, "no event"),
        ("application/x-ndjson", b"{}\noops".to_vec(), 400, "line 2"),
        ("text/plain", vec![0xff, 0xfe, 0xfd], 400, "UTF-8"),
        (
            "application/xml",
            b"<a/>".to_vec(),
            415,
            "accepts these content types",
        ),
        ("", b"{}".to_vec(), 415, "accepts these content types"),
        (
            "application/json",
            format!("{{\"k\": \"{}\"}}", "v".repeat(80)).into_bytes(),
            413,
            "at most 64 per event",
        ),
        (
            "application/json",
            format!("[{}]", vec!["{\"k\":1}"; 200].join(",")).into_bytes(),
            413,
            "1024 bytes the feed accepts per request",
        ),
    ];
    for (content_type, body, status, want) in cases {
        let a = post(content_type, body).await;
        assert_eq!(a.status, status, "{want}: {}", a.body);
        assert!(
            a.body["error"]["message"].as_str().unwrap().contains(want),
            "{want}: {}",
            a.body
        );
    }
    // An encoding the route does not decode is refused rather than stored
    // unread.
    let br = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events"))
                .bearer_auth(&token)
                .header("content-type", "application/json")
                .header("content-encoding", "br")
                .body("{}"),
        )
        .await;
    assert_eq!(br.status, 415, "{}", br.body);
    // The server's own ceiling applies whatever a feed says.
    let (_, wide_token) = srv
        .feed(json!({
            "name": "wide",
            "ingest": {"auth": {"tokens": "managed"}, "max_request_bytes": "1MiB"}
        }))
        .await;
    let over = srv
        .push("wide", &wide_token, &format!("\"{}\"", "y".repeat(5000)))
        .await;
    assert_eq!(over.status, 413, "{}", over.body);
    assert!(
        over.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("4096 bytes")
    );

    // A body sent in chunks is cut off at the limit too.
    let chunk = "z".repeat(700);
    let chunked = format!(
        "POST /v1/feeds/orders/events HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {token}\r\nContent-Type: text/plain\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n2bc\r\n{chunk}\r\n2bc\r\n{chunk}\r\n0\r\n\r\n",
        srv.addr
    );
    let answer = raw(srv.addr, chunked.as_bytes(), Duration::from_secs(5)).await;
    assert!(answer.starts_with("HTTP/1.1 413"), "{answer}");

    // A body that stalls is given up on, and while requests hold every slot
    // another sender is told to retry.
    let stalled = format!(
        "POST /v1/feeds/orders/events HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: 20\r\n\r\n{{\"a\":",
        srv.addr
    );
    let slow = |addr| {
        let stalled = stalled.clone();
        tokio::spawn(async move { raw(addr, stalled.as_bytes(), Duration::from_secs(5)).await })
    };
    let (first, second) = (slow(srv.addr), slow(srv.addr));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let busy = srv.push("orders", &token, "{}").await;
    assert_eq!(busy.status, 503, "{}", busy.body);
    assert_eq!(busy.retry_after.as_deref(), Some("1"));
    for answer in [first.await.unwrap(), second.await.unwrap()] {
        assert!(answer.starts_with("HTTP/1.1 408"), "{answer}");
    }

    // None of it stored anything.
    assert_eq!(srv.watermark("orders", &uid).await, 0);
    assert_eq!(srv.push("orders", &token, "{}").await.status, 202);
    // The ingest route is POST only, whoever asks.
    let get = srv
        .send(
            srv.client
                .put(srv.url("/v1/feeds/orders/events"))
                .body("{}"),
        )
        .await;
    assert_eq!(get.status, 405);
}

/// A gzip body is stored as what it expands to, under the limits an
/// uncompressed body has; an expansion past the request limit is cut off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gzip_body_is_expanded_under_the_feeds_limits() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &["--feeds-max-request-bytes", "4096"]).await;
    let (uid, token) = srv
        .feed(json!({
            "name": "orders",
            "ingest": {
                "auth": {"tokens": "managed"},
                "accept": ["application/json", "application/x-ndjson", "application/octet-stream"],
                "max_event_bytes": 64,
                "max_request_bytes": "1KiB"
            }
        }))
        .await;
    let (_, other_token) = srv
        .feed(json!({
            "name": "other",
            "ingest": {"auth": {"tokens": "managed"}, "max_request_bytes": "1MiB"}
        }))
        .await;
    let encoded = |encoding: &'static str, content_type: &'static str, body: Vec<u8>| {
        srv.send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events"))
                .bearer_auth(&token)
                .header("content-type", content_type)
                .header("content-encoding", encoding)
                .body(body),
        )
    };
    let before = srv.watermark("orders", &uid).await;
    let packed = encoded(
        "gzip",
        "application/x-ndjson",
        gz(b"{\"z\":1}\n{\"z\":2}\n"),
    )
    .await;
    assert_eq!(packed.status, 202, "{}", packed.body);
    assert_eq!(packed.body["ids"].as_array().unwrap().len(), 2);
    let last = packed.body["last_seq"].as_u64().unwrap();
    let stored = service::event(&srv.state, &admin(), None, "orders", last)
        .await
        .unwrap();
    assert_eq!(stored.payload.as_deref(), Some("{\"z\":2}"));
    assert_eq!(srv.watermark("orders", &uid).await, before + 2);

    let bomb = gz(format!("\"{}\"", "0".repeat(2 * 1024 * 1024)).as_bytes());
    assert!(bomb.len() < 4096, "{}", bomb.len());
    let gz_cases: Vec<(&str, Vec<u8>, u16, &str)> = vec![
        (
            "gzip",
            gz(format!("\"{}\"", "0".repeat(2000)).as_bytes()),
            413,
            "expands to more than the 1024 bytes",
        ),
        (
            "gzip",
            gz(format!("\"{}\"", "0".repeat(512 * 1024)).as_bytes()),
            413,
            "expands to more than the 1024 bytes",
        ),
        (
            "gzip",
            gz(format!("{{\"k\": \"{}\"}}", "v".repeat(80)).as_bytes()),
            413,
            "at most 64 per event",
        ),
        ("gzip", b"{}".to_vec(), 400, "not valid gzip"),
        (
            "gzip",
            {
                let mut torn = gz(b"{\"k\": \"cut short\"}");
                torn.truncate(torn.len() - 5);
                torn
            },
            400,
            "not valid gzip",
        ),
        ("gzip", gz(b"{"), 400, "not JSON"),
        ("br", b"{}".to_vec(), 415, "Content-Encoding: gzip"),
        ("zstd", b"{}".to_vec(), 415, "Content-Encoding: gzip"),
        ("gzip, gzip", gz(&gz(b"{}")), 415, "Content-Encoding: gzip"),
    ];
    for (encoding, body, status, want) in gz_cases {
        let a = encoded(encoding, "application/json", body).await;
        assert_eq!(a.status, status, "{encoding} {want}: {}", a.body);
        assert!(
            a.body["error"]["message"].as_str().unwrap().contains(want),
            "{want}: {}",
            a.body
        );
    }
    assert_eq!(
        encoded("identity", "application/json", b"{}".to_vec())
            .await
            .status,
        202
    );
    assert_eq!(srv.watermark("orders", &uid).await, before + 3);
    // A compressed body larger than the request limit is refused before it is
    // expanded, and one that expands far past the server's ceiling is cut off
    // there.
    let noise: Vec<u8> = (0..6000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    let big = encoded("gzip", "application/octet-stream", gz(&noise)).await;
    assert_eq!(big.status, 413, "{}", big.body);
    let wide_bomb = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/other/events"))
                .bearer_auth(&other_token)
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(bomb),
        )
        .await;
    assert_eq!(wide_bomb.status, 413, "{}", wide_bomb.body);
    assert!(
        wide_bomb.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("expands to more than the 4096 bytes"),
        "{}",
        wide_bomb.body
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_signed_feed_checks_the_signature_and_its_timestamp() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &[]).await;
    srv.feed(json!({
        "name": "hooks",
        "ingest": {"auth": {"signature": {
            "header": "X-Signature", "secret": "hmac-secret-1", "prefix": "sha256=",
            "timestamp_header": "X-Timestamp", "tolerance_secs": 300
        }}}
    }))
    .await;
    let uid = service::find(&srv.state, None, "hooks")
        .await
        .unwrap()
        .feed
        .uid
        .clone();
    let body = r#"{"id": 1}"#;
    let now = chrono::Utc::now().timestamp();
    let signed = |ts: i64, secret: &str, body: &str| {
        format!("sha256={}", sign(secret, format!("{ts}.{body}").as_bytes()))
    };
    let post = |ts: Option<i64>, signature: Option<String>, body: &'static str| {
        let mut req = srv
            .client
            .post(srv.url("/v1/feeds/hooks/events"))
            .header("content-type", "application/json")
            .body(body);
        if let Some(ts) = ts {
            req = req.header("x-timestamp", ts.to_string());
        }
        if let Some(s) = signature {
            req = req.header("x-signature", s);
        }
        srv.send(req)
    };
    let good = post(Some(now), Some(signed(now, "hmac-secret-1", body)), body).await;
    assert_eq!(good.status, 202, "{}", good.body);

    let refused = [
        // The wrong secret, a body changed after signing, no prefix.
        post(Some(now), Some(signed(now, "another-secret", body)), body).await,
        post(
            Some(now),
            Some(signed(now, "hmac-secret-1", body)),
            r#"{"id": 2}"#,
        )
        .await,
        post(
            Some(now),
            Some(sign("hmac-secret-1", format!("{now}.{body}").as_bytes())),
            body,
        )
        .await,
        // A request replayed after its tolerance, and one from the future.
        post(
            Some(now - 900),
            Some(signed(now - 900, "hmac-secret-1", body)),
            body,
        )
        .await,
        post(
            Some(now + 900),
            Some(signed(now + 900, "hmac-secret-1", body)),
            body,
        )
        .await,
        // A timestamp other than the signed one, none, no signature at all.
        post(
            Some(now - 1),
            Some(signed(now, "hmac-secret-1", body)),
            body,
        )
        .await,
        post(None, Some(signed(now, "hmac-secret-1", body)), body).await,
        post(Some(now), None, body).await,
    ];
    let unknown = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/nope/events"))
                .header("content-type", "application/json")
                .body(body),
        )
        .await;
    for a in &refused {
        assert_eq!(a.status, 401, "{}", a.body);
        assert_eq!(
            a.body, unknown.body,
            "a bad signature reads like an unknown feed"
        );
    }
    assert_eq!(srv.watermark("hooks", &uid).await, 1);

    // With tokens too, a feed needs both.
    let (_, token) = srv
        .feed(json!({
            "name": "both",
            "ingest": {"auth": {"tokens": "managed", "signature": {"header": "X-Sig", "secret": "s2"}}}
        }))
        .await;
    let both = |token: Option<&str>, sig: Option<String>| {
        let mut req = srv
            .client
            .post(srv.url("/v1/feeds/both/events"))
            .header("content-type", "application/json")
            .body(body);
        if let Some(t) = token {
            req = req.bearer_auth(t);
        }
        if let Some(s) = sig {
            req = req.header("x-sig", s);
        }
        srv.send(req)
    };
    assert_eq!(
        both(Some(&token), Some(sign("s2", body.as_bytes())))
            .await
            .status,
        202
    );
    assert_eq!(both(Some(&token), None).await.status, 401);
    assert_eq!(
        both(None, Some(sign("s2", body.as_bytes()))).await.status,
        401
    );
    assert_eq!(
        both(Some(&token), Some(sign("wrong", body.as_bytes())))
            .await
            .status,
        401
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicates_rate_limits_and_a_full_feed_are_answered_as_the_feed_says() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &[]).await;

    // A dedupe header: a retried request is stored once and still accepted.
    let (uid, token) = srv
        .feed(json!({
            "name": "once",
            "ingest": {"auth": {"tokens": "managed"}, "dedupe": {"header": "X-Event-Id"}}
        }))
        .await;
    let with_key = |key: &'static str, body: &'static str| {
        srv.send(
            srv.client
                .post(srv.url("/v1/feeds/once/events"))
                .bearer_auth(&token)
                .header("content-type", "application/json")
                .header("x-event-id", key)
                .body(body),
        )
    };
    let first = with_key("evt-1", r#"{"id": 1}"#).await;
    assert_eq!((first.status, &first.body["first_seq"]), (202, &json!(1)));
    let again = with_key("evt-1", r#"{"id": 1}"#).await;
    assert_eq!(again.status, 202, "{}", again.body);
    assert_eq!(again.body["duplicate"], true);
    assert_eq!(again.body["duplicates"], 1);
    assert_eq!(
        again.body["ids"], first.body["ids"],
        "the stored event's id"
    );
    assert_eq!(again.body["first_seq"], Value::Null);
    // A batch under one key is keyed per event; a retry of it stores nothing.
    let batch = with_key("evt-2", r#"[{"id": 2}, {"id": 3}]"#).await;
    assert_eq!(
        (&batch.body["first_seq"], &batch.body["last_seq"]),
        (&json!(2), &json!(3))
    );
    let retried = with_key("evt-2", r#"[{"id": 2}, {"id": 3}]"#).await;
    assert_eq!(retried.body["duplicates"], 2);
    assert_eq!(srv.watermark("once", &uid).await, 3);
    // No key, no dedupe.
    assert_eq!(srv.push("once", &token, "{}").await.status, 202);
    assert_eq!(srv.push("once", &token, "{}").await.status, 202);
    assert_eq!(srv.watermark("once", &uid).await, 5);
    assert_eq!(
        srv.store("once")
            .await
            .read(&store_calls(), &uid, Seq(1), Seq(2))
            .await
            .unwrap()
            .events[0]
            .headers["x-event-id"],
        "evt-1"
    );

    // A key in the body, and a 409 for senders that want one.
    let (strict_uid, strict) = srv
        .feed(json!({
            "name": "strict",
            "ingest": {"auth": {"tokens": "managed"},
                       "dedupe": {"body_path": "event.id", "on_duplicate": "conflict"}}
        }))
        .await;
    let event = r#"{"event": {"id": "e-1"}, "n": 1}"#;
    assert_eq!(srv.push("strict", &strict, event).await.status, 202);
    let dup = srv.push("strict", &strict, event).await;
    assert_eq!(dup.status, 409, "{}", dup.body);
    assert_eq!(dup.body["duplicate"], true);
    // One new event in the batch: accepted, and the duplicate is counted.
    let mixed = srv
        .push(
            "strict",
            &strict,
            r#"[{"event": {"id": "e-1"}}, {"event": {"id": "e-2"}}]"#,
        )
        .await;
    assert_eq!(mixed.status, 202, "{}", mixed.body);
    assert_eq!(
        (&mixed.body["duplicate"], &mixed.body["duplicates"]),
        (&json!(false), &json!(1))
    );
    assert_eq!(srv.watermark("strict", &strict_uid).await, 2);

    // A rate limit: the burst passes, the next request waits.
    let (limited_uid, limited) = srv
        .feed(json!({
            "name": "limited",
            "ingest": {"auth": {"tokens": "managed"}, "rate_limit": {"events_per_sec": 1, "burst": 3}}
        }))
        .await;
    assert_eq!(srv.push("limited", &limited, "[1, 2, 3]").await.status, 202);
    let slow_down = srv.push("limited", &limited, "[4, 5]").await;
    assert_eq!(slow_down.status, 429, "{}", slow_down.body);
    assert_eq!(slow_down.retry_after.as_deref(), Some("2"));
    assert_eq!(
        srv.watermark("limited", &limited_uid).await,
        3,
        "a refused request stores nothing"
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(srv.push("limited", &limited, "[4]").await.status, 202);

    // A full feed refuses instead of dropping: nothing of the request is kept.
    register(&srv.state, "sink", &dir.path().join("state")).await;
    let mut paused = delivery("held", "sink", &dir.path().join("never.jsonl"));
    paused["paused"] = json!(true);
    let (full_uid, full) = srv
        .feed(json!({
            "name": "small",
            "ingest": {"auth": {"tokens": "managed"}},
            "retention": {"undelivered": {"max_bytes": 50}},
            "deliveries": [paused]
        }))
        .await;
    let twenty = r#"{"k": "0123456789ab"}"#;
    assert_eq!(srv.push("small", &full, twenty).await.status, 202);
    assert_eq!(srv.push("small", &full, twenty).await.status, 202);
    let refused = srv
        .push("small", &full, &format!("[{twenty}, {twenty}]"))
        .await;
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert_eq!(refused.retry_after.as_deref(), Some("60"));
    assert!(
        refused.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("the feed is full")
    );
    assert_eq!(srv.watermark("small", &full_uid).await, 2);
}

/// A sealed feed's store holds ciphertext; a delivery run reads the events
/// as they were sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_feed_stores_ciphertext_and_delivers_what_was_sent() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut srv = Srv::start(root, &[]).await;
    register(&srv.state, "lake", &root.join("state")).await;
    let out = root.join("sealed.jsonl");
    let (uid, token) = srv
        .feed(json!({
            "name": "private",
            "seal": true,
            "ingest": {"auth": {"tokens": "managed"}},
            "deliveries": [delivery("lake", "lake", &out)]
        }))
        .await;
    let a = srv
        .push(
            "private",
            &token,
            r#"[{"card": "4111-secret-pan"}, {"card": "5500-other-pan"}]"#,
        )
        .await;
    assert_eq!(a.status, 202, "{}", a.body);
    let stored = srv
        .store("private")
        .await
        .read(&store_calls(), &uid, Seq(1), Seq(3))
        .await
        .unwrap();
    assert_eq!(stored.events.len(), 2);
    for e in &stored.events {
        assert!(e.sealed);
        let text = String::from_utf8_lossy(&e.payload);
        assert!(
            !text.contains("secret-pan") && !text.contains("other-pan") && !text.contains("card")
        );
    }
    assert_ne!(stored.events[0].payload, stored.events[1].payload);
    let file = std::fs::read(root.join("feeds.db")).unwrap();
    assert!(!String::from_utf8_lossy(&file).contains("4111-secret-pan"));
    let got = delivered(&mut srv, &out, 2).await;
    assert_eq!(got[0]["card"], "4111-secret-pan");
    assert_eq!(got[1]["card"], "5500-other-pan");
}

/// With `--feeds-listen` the ingest routes are on their own listener, with
/// `/healthz` and nothing else; the main listener serves no ingest route.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_feeds_listener_serves_ingest_and_nothing_else() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(
        dir.path(),
        &[
            "--feeds-listen",
            "127.0.0.1:0",
            "--feeds-allowed-host",
            "feeds.example.test",
        ],
    );
    assert!(cfg.feeds.listen.is_some());
    let state = Srv::state(&cfg).await;
    let main = listen(faucet_cli::serve::server::build_router(
        state.clone(),
        &cfg,
        &Default::default(),
    ))
    .await;
    let ingest = listen(faucet_cli::serve::feeds::ingest::listener_router(
        state.clone(),
        &cfg.feeds,
        cfg.feeds.listen.unwrap(),
    ))
    .await;
    let srv = Srv {
        state,
        scheduler: Scheduler::default(),
        addr: ingest,
        client: reqwest::Client::new(),
    };
    let (uid, token) = srv
        .feed(json!({"name": "orders", "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    assert_eq!(srv.push("orders", &token, "{}").await.status, 202);
    let get = |addr: SocketAddr, path: &'static str| {
        let client = srv.client.clone();
        async move {
            client
                .get(format!("http://{addr}{path}"))
                .send()
                .await
                .unwrap()
                .status()
                .as_u16()
        }
    };
    assert_eq!(get(ingest, "/healthz").await, 200);
    for path in ["/readyz", "/metrics", "/v1/runs", "/v1/whoami", "/"] {
        assert_eq!(get(ingest, path).await, 404, "{path}");
    }
    // The main listener still runs the API, and takes no events.
    assert_eq!(get(main, "/v1/runs").await, 200);
    let on_main = srv
        .client
        .post(format!("http://{main}/v1/feeds/orders/events"))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .body("{}")
        .send()
        .await
        .unwrap()
        .status()
        .as_u16();
    assert!(matches!(on_main, 404 | 405), "{on_main}");
    assert_eq!(srv.watermark("orders", &uid).await, 1);

    // The listener answers for the hosts it was given.
    let host = |name: &'static str| {
        srv.send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events"))
                .bearer_auth(&token)
                .header("host", name)
                .header("content-type", "application/json")
                .body("{}"),
        )
    };
    assert_eq!(host("feeds.example.test").await.status, 202);
    assert_eq!(host("elsewhere.example.test").await.status, 403);

    // The flags that only make sense together are refused apart.
    let alone = faucet_cli::cli::Cli::try_parse_from([
        "faucet",
        "serve",
        "--no-auth",
        "--feeds-allowed-host",
        "x.test",
    ])
    .unwrap();
    let faucet_cli::cli::Command::Serve(args) = alone.command else {
        panic!("not serve");
    };
    let err = ServeConfig::from_args(*args).unwrap_err().to_string();
    assert!(err.contains("only applies with --feeds-listen"), "{err}");
    for bad in [
        ["--feeds-listen", "nowhere"],
        ["--feeds-max-request-bytes", "0"],
        ["--feeds-request-timeout-secs", "0"],
        ["--feeds-max-concurrent-requests", "0"],
    ] {
        let cli = faucet_cli::cli::Cli::try_parse_from(
            ["faucet", "serve", "--no-auth"].into_iter().chain(bad),
        )
        .unwrap();
        let faucet_cli::cli::Command::Serve(args) = cli.command else {
            panic!("not serve");
        };
        assert!(ServeConfig::from_args(*args).is_err(), "{bad:?}");
    }
}

/// While the store cannot commit, the sender is told to retry and nothing is
/// acknowledged; once it can, the retry is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_store_that_cannot_commit_answers_503_and_acknowledges_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &[]).await;
    let (uid, token) = srv
        .feed(json!({"name": "orders", "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    assert_eq!(srv.push("orders", &token, r#"{"id": 1}"#).await.status, 202);
    // Another writer holds the store's write lock.
    let db = dir.path().join("feeds.db");
    let mut blocker =
        <sqlx::SqliteConnection as sqlx::Connection>::connect(&format!("sqlite:{}", db.display()))
            .await
            .unwrap();
    sqlx::query("BEGIN EXCLUSIVE")
        .execute(&mut blocker)
        .await
        .unwrap();
    let started = std::time::Instant::now();
    let (a, b) = tokio::join!(
        srv.push("orders", &token, r#"{"id": 2}"#),
        srv.push("orders", &token, r#"[{"id": 3}, {"id": 4}]"#)
    );
    for answer in [&a, &b] {
        assert_eq!(answer.status, 503, "{}", answer.body);
        assert_eq!(answer.retry_after.as_deref(), Some("5"));
        assert!(answer.body.get("ids").is_none());
        assert!(
            answer.body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("store is unavailable")
        );
    }
    assert!(
        started.elapsed() >= Duration::from_secs(4),
        "it waited for the lock"
    );
    sqlx::query("ROLLBACK").execute(&mut blocker).await.unwrap();
    drop(blocker);
    assert_eq!(srv.watermark("orders", &uid).await, 1, "nothing was stored");
    let retry = srv.push("orders", &token, r#"{"id": 2}"#).await;
    assert_eq!((retry.status, &retry.body["first_seq"]), (202, &json!(2)));
}

/// Many senders at once: every acknowledged event is stored exactly once,
/// each request gets its own consecutive range, and requests share commits.
/// While the run history is unreachable a server keeps accepting events
/// from the definitions it holds, for as long as they are fresh; once they
/// are older than the maximum age it answers `503` and stores nothing, and
/// it accepts again when the history is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stale_definitions_snapshot_answers_503_while_the_history_is_unreachable() {
    use faucet_cli::serve::feeds::MAX_SNAPSHOT_AGE;
    use faucet_cli::serve::history::fallback::FallbackHistory;
    use faucet_cli::serve::history::sqlite::SqliteHistory;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), &[]);
    let primary = SqliteHistory::connect(
        &format!("sqlite:{}", dir.path().join("history.db").display()),
        cfg.idempotency_retention,
        cfg.lease_ttl,
        uuid::Uuid::new_v4().to_string(),
    )
    .await
    .unwrap();
    let history = Arc::new(FallbackHistory::healthy(
        Box::new(primary),
        cfg.idempotency_retention,
        "sqlite",
    ));
    let state = ServerState::new(
        &cfg,
        None,
        tokio_util::sync::CancellationToken::new(),
        history.clone(),
        faucet_cli::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        faucet_cli::serve::triggers::health::TriggersHandle::empty(),
    );
    state.set_feeds(FeedsRuntime::new(
        cfg.feeds.clone(),
        spec::LoadedFile::default(),
        stores::Stores::from_config(&cfg),
    ));
    let router = faucet_cli::serve::server::build_router(state.clone(), &cfg, &Default::default());
    let srv = Srv {
        addr: listen(router).await,
        state,
        scheduler: Scheduler::default(),
        client: reqwest::Client::new(),
    };
    let (uid, token) = srv
        .feed(json!({"name": "orders", "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    assert_eq!(srv.push("orders", &token, r#"{"n": 1}"#).await.status, 202);
    let rt = srv.state.feeds();
    let store = srv.store("orders").await;
    let watermark = async || {
        store
            .heads(&store_calls(), std::slice::from_ref(&uid))
            .await
            .unwrap()
            .remove(0)
            .watermark
            .get()
    };

    // Unreachable, definitions still fresh: events are accepted and stored.
    history.mark_unreachable();
    assert!(rt.refresh(&srv.state).await.is_err());
    let held = srv.push("orders", &token, r#"{"n": 2}"#).await;
    assert_eq!(held.status, 202, "{}", held.body);
    assert_eq!(watermark().await, 2);
    rt.backdate_snapshot(MAX_SNAPSHOT_AGE - Duration::from_secs(5));
    assert_eq!(srv.push("orders", &token, r#"{"n": 3}"#).await.status, 202);

    // Past the maximum age: every sender is told to retry, whatever it sent,
    // and nothing is acknowledged or appended.
    rt.backdate_snapshot(Duration::from_secs(10));
    assert!(!rt.snapshot().fresh());
    for (name, credential, body) in [
        ("orders", token.as_str(), r#"{"n": 4}"#),
        ("orders", "fdt_wrong", "{}"),
        ("no-such-feed", token.as_str(), "{}"),
        ("orders", token.as_str(), "{"),
    ] {
        let stale = srv.push(name, credential, body).await;
        assert_eq!(stale.status, 503, "{name}: {}", stale.body);
        assert!(stale.retry_after.is_some(), "{name}");
        assert_eq!(stale.body["error"]["code"], "unavailable");
        assert!(stale.body.get("ids").is_none(), "{}", stale.body);
    }
    assert_eq!(watermark().await, 3);
    let read = store
        .read(&store_calls(), &uid, Seq(1), Seq(10))
        .await
        .unwrap();
    assert_eq!(read.events.len(), 3);

    // The history answers again: the next request reloads and is stored.
    assert!(history.try_recover().await);
    let back = srv.push("orders", &token, r#"{"n": 4}"#).await;
    assert_eq!(back.status, 202, "{}", back.body);
    assert_eq!(back.body["first_seq"], 4);
    assert!(rt.snapshot().fresh());
}

/// A server that starts while the run history is unreachable has never read
/// its feed definitions: it answers `503` with a retry time, never `401`,
/// which could make a sender drop its events. With the definitions read, a
/// credential it cannot look up afresh is answered `503` too, and once the
/// history is back an unknown feed and a bad token get the same `401`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_server_that_never_read_its_definitions_answers_503_not_401() {
    use faucet_cli::serve::history::fallback::FallbackHistory;
    use faucet_cli::serve::history::sqlite::SqliteHistory;
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path(), &[]);
    let primary = SqliteHistory::connect(
        &format!("sqlite:{}", dir.path().join("history.db").display()),
        cfg.idempotency_retention,
        cfg.lease_ttl,
        uuid::Uuid::new_v4().to_string(),
    )
    .await
    .unwrap();
    let history = Arc::new(FallbackHistory::healthy(
        Box::new(primary),
        cfg.idempotency_retention,
        "sqlite",
    ));
    let state = ServerState::new(
        &cfg,
        None,
        tokio_util::sync::CancellationToken::new(),
        history.clone(),
        faucet_cli::serve::logs::LogHub::new(),
        None,
        #[cfg(feature = "triggers")]
        faucet_cli::serve::triggers::health::TriggersHandle::empty(),
    );
    state.set_feeds(FeedsRuntime::new(
        cfg.feeds.clone(),
        spec::LoadedFile::default(),
        stores::Stores::from_config(&cfg),
    ));
    let router = faucet_cli::serve::server::build_router(state.clone(), &cfg, &Default::default());
    let srv = Srv {
        addr: listen(router).await,
        state,
        scheduler: Scheduler::default(),
        client: reqwest::Client::new(),
    };
    let (_, token) = srv
        .feed(json!({"name": "orders", "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    assert_eq!(srv.push("orders", &token, r#"{"n": 1}"#).await.status, 202);

    // The history goes away and the server starts over knowing nothing.
    history.mark_unreachable();
    srv.state.feeds().forget_snapshot();
    let rt = srv.state.feeds();
    assert!(rt.snapshot().fresh() && !rt.snapshot().loaded());
    for (name, credential) in [
        ("orders", token.as_str()),
        ("orders", "fdt_wrong"),
        ("no-such-feed", token.as_str()),
    ] {
        let cold = srv.push(name, credential, r#"{"n": 2}"#).await;
        assert_eq!(cold.status, 503, "{name}: {}", cold.body);
        assert!(cold.retry_after.is_some(), "{name}");
        assert_eq!(cold.body["error"]["code"], "unavailable");
    }

    assert!(history.try_recover().await);
    let back = srv.push("orders", &token, r#"{"n": 2}"#).await;
    assert_eq!(back.status, 202, "{}", back.body);
    assert!(rt.snapshot().loaded());

    // Read once, then unreachable: a known token is still accepted, and a
    // credential the server cannot look up afresh is told to retry.
    history.mark_unreachable();
    assert_eq!(srv.push("orders", &token, r#"{"n": 3}"#).await.status, 202);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    for (name, credential) in [("orders", "fdt_wrong"), ("no-such-feed", token.as_str())] {
        let unknown = srv.push(name, credential, "{}").await;
        assert_eq!(unknown.status, 503, "{name}: {}", unknown.body);
        assert!(unknown.retry_after.is_some(), "{name}");
    }

    assert!(history.try_recover().await);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let bad_token = srv.push("orders", "fdt_wrong", "{}").await;
    let no_feed = srv.push("no-such-feed", &token, "{}").await;
    assert_eq!(bad_token.status, 401, "{}", bad_token.body);
    assert_eq!(no_feed.status, 401, "{}", no_feed.body);
    assert_eq!(bad_token.body, no_feed.body);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_requests_share_commits_and_lose_or_repeat_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Arc::new(Srv::start(dir.path(), &[]).await);
    let (uid, token) = srv
        .feed(json!({"name": "busy", "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    const SENDERS: u64 = 120;
    const PER_REQUEST: u64 = 3;
    let mut tasks = Vec::new();
    for sender in 0..SENDERS {
        let (srv, token) = (srv.clone(), token.clone());
        tasks.push(tokio::spawn(async move {
            let body = format!(
                "[{}]",
                (0..PER_REQUEST)
                    .map(|i| format!("{{\"sender\": {sender}, \"i\": {i}}}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            (sender, srv.push("busy", &token, &body).await)
        }));
    }
    let mut ranges = Vec::new();
    let mut ids = std::collections::HashSet::new();
    for task in tasks {
        let (sender, answer) = task.await.unwrap();
        assert_eq!(answer.status, 202, "{}", answer.body);
        let (first, last) = (
            answer.body["first_seq"].as_u64().unwrap(),
            answer.body["last_seq"].as_u64().unwrap(),
        );
        assert_eq!(
            last - first + 1,
            PER_REQUEST,
            "a request's events are consecutive"
        );
        for id in answer.body["ids"].as_array().unwrap() {
            assert!(ids.insert(id.as_str().unwrap().to_string()));
        }
        ranges.push((first, last, sender));
    }
    ranges.sort_unstable();
    for (i, (first, _, _)) in ranges.iter().enumerate() {
        assert_eq!(*first, i as u64 * PER_REQUEST + 1, "ranges tile the log");
    }
    let total = SENDERS * PER_REQUEST;
    assert_eq!(srv.watermark("busy", &uid).await, total);
    let stored = srv
        .store("busy")
        .await
        .read(&store_calls(), &uid, Seq(1), Seq(total + 1))
        .await
        .unwrap();
    assert_eq!(stored.events.len() as u64, total);
    // Each request's range holds that request's events, in its order.
    for (first, _, sender) in &ranges {
        for i in 0..PER_REQUEST {
            let event = &stored.events[(first - 1 + i) as usize];
            let payload: Value = serde_json::from_slice(&event.payload).unwrap();
            assert_eq!(
                (&payload["sender"], &payload["i"]),
                (&json!(sender), &json!(i))
            );
            assert!(ids.contains(&event.id));
        }
    }
}

/// A tenant's feed is pushed to under the tenant's path, limited by the
/// tenant's events-per-second ceiling and sealed by default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenant_feed_has_its_own_path_and_the_tenants_rate_limit() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &[]).await;
    let now = chrono::Utc::now();
    let rec = serde_json::from_value(json!({
        "id": "acme", "limits": {"max_feed_events_per_sec": 2},
        "created_at": now, "updated_at": now, "created_by": "alice"
    }))
    .unwrap();
    srv.state.history().tenant_upsert(&rec).await.unwrap();
    let acme = AuthContext {
        principal: "acme-app".into(),
        role: Role::Operator,
        source_ip: None,
        tenant: Some("acme".into()),
    };
    let spec: FeedSpec = serde_json::from_value(json!({
        "name": "orders",
        "ingest": {"auth": {"tokens": "managed"}, "url_token": true,
                   "rate_limit": {"events_per_sec": 100}}
    }))
    .unwrap();
    let made = service::create(&srv.state, &acme, None, spec)
        .await
        .unwrap();
    let uid = FeedUid::new(made.uid).unwrap();
    let (_, token) = service::token_create(&srv.state, &acme, None, "orders", "app", None)
        .await
        .unwrap();
    let push = |path: String, body: &'static str| {
        srv.send(
            srv.client
                .post(srv.url(&path))
                .header("content-type", "application/json")
                .body(body),
        )
    };
    // The token in the URL, for a sender that can only be given a URL.
    let tenant_path = format!("/v1/tenants/acme/feeds/orders/events?token={token}&source=crm");
    let first = push(tenant_path.clone(), "[1, 2]").await;
    assert_eq!(first.status, 202, "{}", first.body);
    // The same name without the tenant, or under another, is not this feed.
    for path in [
        format!("/v1/feeds/orders/events?token={token}"),
        format!("/v1/tenants/globex/feeds/orders/events?token={token}"),
    ] {
        assert_eq!(push(path, "{}").await.status, 401);
    }
    // The tenant's ceiling applies across its feeds, and the feed's own
    // allowance is given back when the tenant's refuses.
    let limited = push(tenant_path.clone(), "[3]").await;
    assert_eq!(limited.status, 429, "{}", limited.body);
    assert!(
        limited.body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("tenant's ingest rate limit")
    );
    let found = service::find(&srv.state, Some("acme"), "orders")
        .await
        .unwrap();
    let store = srv.state.feeds().store_for(&found.feed).await.unwrap();
    let head = store
        .heads(&store_calls(), std::slice::from_ref(&uid))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(head.watermark, Seq(2));
    let events = store
        .read(&store_calls(), &uid, Seq(1), Seq(3))
        .await
        .unwrap();
    assert!(
        events.events.iter().all(|e| e.sealed),
        "a tenant feed seals by default"
    );
}

/// A token sent in the URL is taken out of the request before anything can
/// log it: no line written while the request is handled holds it.
#[tokio::test]
async fn a_token_in_the_url_reaches_no_log_line() {
    #[derive(Clone, Default)]
    struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);
    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Captured {
        type Writer = Captured;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(captured.clone())
        .with_ansi(false)
        .finish();
    // Process-wide: a per-thread subscriber misses call sites another test
    // thread reached first. No other test here installs one.
    tracing::subscriber::set_global_default(subscriber).unwrap();
    tracing::info!("probe line");
    assert!(
        String::from_utf8_lossy(&captured.0.lock().unwrap()).contains("probe line"),
        "the capture is installed"
    );

    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &[]).await;
    let (uid, token) = srv
        .feed(json!({
            "name": "orders",
            "ingest": {"auth": {"tokens": "managed"}, "url_token": true, "max_event_bytes": 16}
        }))
        .await;
    let url = srv.url(&format!("/v1/feeds/orders/events?a=1&token={token}"));
    let post = |body: &'static str| {
        srv.send(
            srv.client
                .post(&url)
                .header("content-type", "application/json")
                .body(body),
        )
    };
    assert_eq!(post(r#"{"id": 1}"#).await.status, 202);
    assert_eq!(srv.watermark("orders", &uid).await, 1);
    // A refusal is logged with its reason, and still without the token.
    assert_eq!(post(r#"{"too": "long for this feed"}"#).await.status, 413);
    assert_eq!(post("{").await.status, 400);
    let bad = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events?token=fdt_not-a-real-token-value"))
                .header("content-type", "application/json")
                .body("{}"),
        )
        .await;
    assert_eq!(bad.status, 401);
    let logged = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    assert!(
        logged.contains("ingest request refused"),
        "the capture works: {logged}"
    );
    assert!(logged.contains("too_large") && logged.contains("unauthorized"));
    assert!(!logged.contains(&token), "the token was logged");
    assert!(!logged.contains("fdt_not-a-real-token-value"));
    assert!(
        !logged.contains("long for this feed"),
        "a payload was logged"
    );
}

const AUTH_CONFIG: &str = "principals:\n  - { name: alice, token: admin-tok, role: admin }\n  - { name: bob, token: op-tok, role: operator }\n  - { name: carol, token: view-tok, role: viewer }\n  - { name: acme-app, token: acme-tok, role: operator, tenant: acme }\n";

/// The management API: who may do what, what each answer holds, and the
/// audit entry every change leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_management_api_serves_feeds_to_each_role_and_audits_every_change() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let auth = root.join("auth.yaml");
    std::fs::write(&auth, AUTH_CONFIG).unwrap();
    let auth_arg = auth.display().to_string();
    let srv = Srv::over(config_as(root, &["--auth-config", &auth_arg], &[])).await;
    register(&srv.state, "lake", &root.join("state")).await;
    let now = chrono::Utc::now();
    let rec = serde_json::from_value(json!({
        "id": "acme", "limits": {"max_feeds": 3, "max_feed_bytes": 1_000_000, "max_feed_events_per_sec": 50},
        "created_at": now, "updated_at": now, "created_by": "alice"
    }))
    .unwrap();
    srv.state.history().tenant_upsert(&rec).await.unwrap();

    let call = |method: reqwest::Method, path: &str, token: &str, body: Option<Value>| {
        let mut req = srv.client.request(method, srv.url(path)).bearer_auth(token);
        if let Some(body) = body {
            req = req.json(&body);
        }
        srv.send(req)
    };
    use reqwest::Method as M;
    let out = root.join("lake.jsonl");
    // Deliveries that wait to be told: the scheduler runs in this server.
    let lazy = |name: &str, out: &Path| {
        let mut d = delivery(name, "lake", out);
        d["flush"] = json!({"max_events": 100_000, "max_bytes": "1GiB", "max_age": "1d", "min_interval": "0s"});
        d
    };

    // Nothing yet; what a new feed may name; who may ask.
    let empty = call(M::GET, "/v1/feeds", "view-tok", None).await;
    assert_eq!(empty.status, 200, "{}", empty.body);
    assert_eq!(empty.body["feeds"], json!([]));
    assert_eq!(empty.body["stores"], json!(["default"]));
    assert_eq!(empty.body["approval"], "never");
    assert!(empty.body.get("quotas").is_none());
    assert_eq!(call(M::GET, "/v1/feeds", "nope", None).await.status, 401);
    for (token, has) in [
        ("view-tok", [true, false, false]),
        ("op-tok", [true, true, false]),
        ("admin-tok", [true, true, true]),
    ] {
        let me = call(M::GET, "/v1/whoami", token, None).await;
        let perms = me.body["permissions"].as_array().unwrap();
        for (perm, want) in ["feed_read", "feed_write", "feed_admin"].iter().zip(has) {
            assert_eq!(perms.contains(&json!(perm)), want, "{token} {perm}");
        }
    }

    // Create: an operator may, a viewer may not; the definition is checked.
    let spec = json!({
        "name": "orders",
        "description": "orders from the shop",
        "ingest": {"auth": {"tokens": "managed", "signature": {"header": "X-Sig", "secret": "hmac-s3cret"}}},
        "deliveries": [lazy("lake", &out)]
    });
    assert_eq!(
        call(M::POST, "/v1/feeds", "view-tok", Some(spec.clone()))
            .await
            .status,
        403
    );
    let made = call(M::POST, "/v1/feeds", "op-tok", Some(spec.clone())).await;
    assert_eq!(made.status, 201, "{}", made.body);
    assert_eq!(made.body["name"], "orders");
    assert_eq!(made.body["status"], "receiving");
    assert_eq!(made.body["ingest_path"], "/v1/feeds/orders/events");
    assert_eq!(
        made.body["spec"]["ingest"]["auth"]["signature"]["secret"],
        "$sealed"
    );
    assert_eq!(made.body["deliveries"][0]["status"], "ok");
    assert_eq!(
        call(M::POST, "/v1/feeds", "op-tok", Some(spec.clone()))
            .await
            .status,
        409
    );
    let bad = call(
        M::POST,
        "/v1/feeds",
        "op-tok",
        Some(json!({"name": "Bad Name"})),
    )
    .await;
    assert_eq!(bad.status, 422, "{}", bad.body);
    let unknown_template = call(
        M::POST,
        "/v1/feeds",
        "op-tok",
        Some(
            json!({"name": "x", "ingest": {"auth": {"tokens": "managed"}},
                    "deliveries": [{"name": "d", "template": {"id": "not-registered"}}]}),
        ),
    )
    .await;
    assert!(
        matches!(unknown_template.status, 404 | 422),
        "{}",
        unknown_template.body
    );
    // A store other than `default` is the server admin's to choose.
    let other_store = call(
        M::POST,
        "/v1/feeds",
        "op-tok",
        Some(json!({"name": "y", "store": "big", "ingest": {"auth": {"tokens": "managed"}}})),
    )
    .await;
    assert_eq!(other_store.status, 403, "{}", other_store.body);

    // Update keeps the stored secret behind its marker.
    let mut edited = made.body["spec"].clone();
    edited["description"] = json!("edited");
    edited["ingest"]["auth"]["signature"] = Value::Null;
    edited["ingest"]["auth"]
        .as_object_mut()
        .unwrap()
        .remove("signature");
    let updated = call(M::PUT, "/v1/feeds/orders", "op-tok", Some(edited)).await;
    assert_eq!(updated.status, 200, "{}", updated.body);
    assert_eq!(updated.body["revision"], 2);
    assert_eq!(
        call(M::PUT, "/v1/feeds/nope", "op-tok", Some(spec.clone()))
            .await
            .status,
        404
    );

    // Tokens: the text is shown once.
    let token = call(
        M::POST,
        "/v1/feeds/orders/tokens",
        "op-tok",
        Some(json!({"name": "shop"})),
    )
    .await;
    assert_eq!(token.status, 201, "{}", token.body);
    let text = token.body["token"].as_str().unwrap().to_string();
    assert!(text.starts_with("fdt_"));
    let listed = call(M::GET, "/v1/feeds/orders/tokens", "view-tok", None).await;
    assert_eq!(listed.body.as_array().unwrap().len(), 1);
    let shown = serde_json::to_string(&listed.body).unwrap();
    assert!(!shown.contains(&text) && !shown.contains("hash"), "{shown}");
    assert_eq!(listed.body[0]["name"], "shop");
    assert_eq!(
        call(
            M::POST,
            "/v1/feeds/orders/tokens",
            "view-tok",
            Some(json!({"name": "x"}))
        )
        .await
        .status,
        403
    );
    let past = call(
        M::POST,
        "/v1/feeds/orders/tokens",
        "op-tok",
        Some(json!({"name": "old", "expires_at": "2020-01-01T00:00:00Z"})),
    )
    .await;
    assert_eq!(past.status, 422);

    // Events arrive; the browser pages them newest first, without payloads.
    assert_eq!(
        srv.push(
            "orders",
            &text,
            r#"[{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]"#
        )
        .await
        .status,
        202
    );
    assert_eq!(srv.push("orders", "fdt_wrong", "{}").await.status, 401);
    assert_eq!(srv.push("orders", &text, "{").await.status, 400);
    let page = call(M::GET, "/v1/feeds/orders/events?limit=2", "view-tok", None).await;
    assert_eq!(page.status, 200, "{}", page.body);
    let seqs = |v: &Value| -> Vec<u64> {
        v["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["seq"].as_u64().unwrap())
            .collect()
    };
    assert_eq!(seqs(&page.body), vec![5, 4]);
    assert_eq!(
        (
            &page.body["oldest"],
            &page.body["watermark"],
            &page.body["next_before"]
        ),
        (&json!(1), &json!(5), &json!(4))
    );
    assert!(page.body["events"][0].get("payload").is_none());
    assert_eq!(page.body["events"][0]["content_type"], "application/json");
    assert_eq!(page.body["events"][0]["delivered_by"], json!([]));
    let older = call(
        M::GET,
        "/v1/feeds/orders/events?limit=10&before=4",
        "view-tok",
        None,
    )
    .await;
    assert_eq!(seqs(&older.body), vec![3, 2, 1]);
    assert!(older.body.get("next_before").is_none());
    let forward = call(
        M::GET,
        "/v1/feeds/orders/events?from=2&limit=2",
        "view-tok",
        None,
    )
    .await;
    assert_eq!(seqs(&forward.body), vec![3, 2]);
    let past_end = call(M::GET, "/v1/feeds/orders/events?from=99", "view-tok", None).await;
    assert_eq!(seqs(&past_end.body), Vec::<u64>::new());
    // A payload is an admin's to read, and the read is audited.
    assert_eq!(
        call(M::GET, "/v1/feeds/orders/events/2", "op-tok", None)
            .await
            .status,
        403
    );
    let one = call(M::GET, "/v1/feeds/orders/events/2", "admin-tok", None).await;
    assert_eq!(one.status, 200, "{}", one.body);
    assert_eq!(
        (&one.body["payload"], &one.body["payload_encoding"]),
        (&json!(r#"{"id": 2}"#), &json!("utf8"))
    );
    assert_eq!(
        call(M::GET, "/v1/feeds/orders/events/99", "admin-tok", None)
            .await
            .status,
        404
    );

    // What was refused: reason, status and sender, never a payload.
    let refusals = call(M::GET, "/v1/feeds/orders/refusals", "view-tok", None).await;
    assert_eq!(refusals.body["scope"], "this_server");
    let reasons: Vec<&str> = refusals.body["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["reason"].as_str().unwrap())
        .collect();
    assert_eq!(reasons, vec!["malformed", "unauthorized"]);
    assert_eq!(refusals.body["refusals"][0]["status"], 400);
    assert_eq!(refusals.body["refusals"][0]["sender"], "127.0.0.1");
    assert_eq!(
        call(
            M::GET,
            "/v1/feeds/orders/refusals?limit=1",
            "view-tok",
            None
        )
        .await
        .body["refusals"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // The feed's page: position, the last day, tokens, the change log.
    let detail = call(M::GET, "/v1/feeds/orders", "view-tok", None).await;
    assert_eq!(detail.status, 200, "{}", detail.body);
    assert_eq!(detail.body["watermark"], 5);
    assert_eq!(detail.body["events_24h"], 5);
    assert_eq!(detail.body["bytes_24h"], 45);
    assert_eq!(detail.body["refused_recent"], 2);
    assert_eq!(detail.body["deliveries"][0]["lag"], 5);
    assert_eq!(detail.body["deliveries"][0]["delivered_24h"], 0);
    assert_eq!(detail.body["tokens"][0]["name"], "shop");
    assert_eq!(detail.body["changes"].as_array().unwrap().len(), 2);
    assert_eq!(
        call(M::GET, "/v1/feeds", "view-tok", None).await.body["feeds"][0]["name"],
        "orders"
    );

    // A delivery's moves.
    let d = "/v1/feeds/orders/deliveries/lake";
    let paused = call(
        M::POST,
        &format!("{d}/pause"),
        "op-tok",
        Some(json!({"reason": "maintenance"})),
    )
    .await;
    assert_eq!(paused.status, 200, "{}", paused.body);
    assert_eq!(paused.body["deliveries"][0]["status"], "paused");
    assert_eq!(paused.body["deliveries"][0]["pause_reason"], "maintenance");
    assert_eq!(paused.body["status"], "paused");
    assert_eq!(
        call(M::POST, &format!("{d}/pause"), "view-tok", None)
            .await
            .status,
        403
    );
    assert_eq!(
        call(
            M::POST,
            "/v1/feeds/orders/deliveries/nope/pause",
            "op-tok",
            None
        )
        .await
        .status,
        404
    );
    let resumed = call(M::POST, &format!("{d}/resume"), "op-tok", None).await;
    assert_eq!(resumed.body["deliveries"][0]["status"], "ok");
    for verb in ["replay", "skip", "recover"] {
        let denied = call(
            M::POST,
            &format!("{d}/{verb}"),
            "op-tok",
            Some(json!({"seq": 1, "reason": "x"})),
        )
        .await;
        assert_eq!(denied.status, 403, "{verb}");
    }
    let skipped = call(
        M::POST,
        &format!("{d}/skip"),
        "admin-tok",
        Some(json!({"to": 3, "reason": "bad rows"})),
    )
    .await;
    assert_eq!(skipped.status, 200, "{}", skipped.body);
    assert_eq!(
        (&skipped.body["from"], &skipped.body["to"]),
        (&json!(1), &json!(3))
    );
    let no_reason = call(
        M::POST,
        &format!("{d}/skip"),
        "admin-tok",
        Some(json!({"to": 4, "reason": " "})),
    )
    .await;
    assert_eq!(no_reason.status, 422);
    let both = call(
        M::POST,
        &format!("{d}/replay"),
        "admin-tok",
        Some(json!({"seq": 1, "time": "2026-01-01T00:00:00Z"})),
    )
    .await;
    assert_eq!(both.status, 422, "{}", both.body);
    let replayed = call(
        M::POST,
        &format!("{d}/replay"),
        "admin-tok",
        Some(json!({"seq": 2})),
    )
    .await;
    assert_eq!(
        (&replayed.body["from"], &replayed.body["to"]),
        (&json!(3), &json!(2))
    );
    let by_time = call(
        M::POST,
        &format!("{d}/replay"),
        "admin-tok",
        Some(json!({"time": "2020-01-01T00:00:00Z"})),
    )
    .await;
    assert_eq!(by_time.body["to"], 1, "{}", by_time.body);
    let recovered = call(M::POST, &format!("{d}/recover"), "admin-tok", None).await;
    assert_eq!(recovered.status, 200, "{}", recovered.body);
    let gaps = call(M::GET, "/v1/feeds/orders", "view-tok", None)
        .await
        .body["gaps"]
        .clone();
    assert!(
        gaps.as_array()
            .unwrap()
            .iter()
            .any(|g| g["reason"] == "bad rows" && g["by"] == "alice"),
        "{gaps}"
    );
    assert_eq!(
        call(M::POST, &format!("{d}/deliver-now"), "op-tok", None)
            .await
            .status,
        200
    );
    let mut after = Value::Null;
    for _ in 0..600 {
        after = call(M::GET, "/v1/feeds/orders", "view-tok", None)
            .await
            .body;
        if after["deliveries"][0]["lag"] == 0 && after["deliveries"][0].get("claim").is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(after["deliveries"][0]["lag"], 0, "{after}");
    assert_eq!(after["deliveries"][0]["delivered_24h"], 5);
    assert_eq!(after["deliveries"][0]["last_run"]["outcome"], "delivered");
    assert_eq!(
        call(
            M::GET,
            "/v1/feeds/orders/events?limit=1&before=2",
            "view-tok",
            None
        )
        .await
        .body["events"][0]["delivered_by"],
        json!(["lake"])
    );

    // Deliveries: add, replace, remove.
    let second = lazy("audit", &root.join("audit.jsonl"));
    let added = call(
        M::POST,
        "/v1/feeds/orders/deliveries",
        "op-tok",
        Some(second.clone()),
    )
    .await;
    assert_eq!(added.status, 201, "{}", added.body);
    assert_eq!(added.body["deliveries"].as_array().unwrap().len(), 2);
    assert_eq!(
        call(
            M::POST,
            "/v1/feeds/orders/deliveries",
            "op-tok",
            Some(second.clone())
        )
        .await
        .status,
        409
    );
    let mut slower = second.clone();
    slower["flush"]["max_events"] = json!(10);
    let replaced = call(
        M::PUT,
        "/v1/feeds/orders/deliveries/audit",
        "op-tok",
        Some(slower.clone()),
    )
    .await;
    assert_eq!(replaced.status, 200, "{}", replaced.body);
    assert_eq!(
        call(
            M::PUT,
            "/v1/feeds/orders/deliveries/other",
            "op-tok",
            Some(slower.clone())
        )
        .await
        .status,
        422
    );
    slower["name"] = json!("ghost");
    assert_eq!(
        call(
            M::PUT,
            "/v1/feeds/orders/deliveries/ghost",
            "op-tok",
            Some(slower)
        )
        .await
        .status,
        404
    );
    assert_eq!(
        call(
            M::DELETE,
            "/v1/feeds/orders/deliveries/ghost",
            "op-tok",
            None
        )
        .await
        .status,
        404
    );

    // A run in flight holds a delete, and the answer names it.
    let found = service::find(&srv.state, None, "orders").await.unwrap();
    let audit_uid = found.feed.delivery("audit").unwrap().uid.clone();
    let store = srv.store("orders").await;
    let owner = faucet_common_feed::ClaimOwner::new("serve:test").unwrap();
    let claim = store
        .claim(
            &store_calls(),
            &audit_uid,
            Seq(1),
            Seq(3),
            &owner,
            Duration::from_secs(60),
        )
        .await
        .unwrap()
        .unwrap();
    for path in ["/v1/feeds/orders/deliveries/audit", "/v1/feeds/orders"] {
        let token = if path.ends_with("orders") {
            "admin-tok"
        } else {
            "op-tok"
        };
        let held = call(M::DELETE, path, token, None).await;
        assert_eq!(held.status, 409, "{}", held.body);
        assert_eq!(
            held.body["error"]["details"]["runs"],
            json!([{"delivery": "audit", "run_id": null}])
        );
        assert!(
            held.body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("'audit'")
        );
    }
    store.release(&store_calls(), &claim.id).await.unwrap();
    let removed = call(
        M::DELETE,
        "/v1/feeds/orders/deliveries/audit",
        "op-tok",
        None,
    )
    .await;
    assert_eq!(removed.status, 200, "{}", removed.body);
    assert_eq!(removed.body["deliveries"].as_array().unwrap().len(), 1);

    // Tokens are revoked by those who issue them; a delete is an admin's.
    let id = token.body["id"].as_str().unwrap();
    let revoked = call(
        M::DELETE,
        &format!("/v1/feeds/orders/tokens/{id}"),
        "op-tok",
        None,
    )
    .await;
    assert_eq!(revoked.status, 200, "{}", revoked.body);
    assert!(revoked.body["revoked_at"].is_string());
    assert_eq!(srv.push("orders", &text, "{}").await.status, 401);
    assert_eq!(
        call(M::DELETE, "/v1/feeds/orders/tokens/nope", "op-tok", None)
            .await
            .status,
        404
    );
    assert_eq!(
        call(M::DELETE, "/v1/feeds/orders", "op-tok", None)
            .await
            .status,
        403
    );

    // A tenant's feeds: an operator names the tenant; the tenant's own
    // principal is confined to it; quotas come with the list.
    let tenant_feed = json!({"name": "app", "ingest": {"auth": {"tokens": "managed"}}});
    let made = call(
        M::POST,
        "/v1/feeds?tenant=acme",
        "op-tok",
        Some(tenant_feed.clone()),
    )
    .await;
    assert_eq!(made.status, 201, "{}", made.body);
    assert_eq!(
        made.body["ingest_path"],
        "/v1/tenants/acme/feeds/app/events"
    );
    let theirs = call(M::GET, "/v1/feeds", "acme-tok", None).await;
    assert_eq!(theirs.status, 200, "{}", theirs.body);
    assert_eq!(theirs.body["feeds"].as_array().unwrap().len(), 1);
    assert_eq!(
        theirs.body["quotas"],
        json!({
            "tenant": "acme", "feeds": 1, "max_feeds": 3, "stored_bytes": 0,
            "max_feed_bytes": 1_000_000, "max_feed_events_per_sec": 50
        })
    );
    assert_eq!(
        call(M::GET, "/v1/feeds/app", "acme-tok", None).await.status,
        200
    );
    assert_eq!(
        call(M::GET, "/v1/feeds/orders", "acme-tok", None)
            .await
            .status,
        404
    );
    assert_eq!(
        call(M::GET, "/v1/feeds?tenant=globex", "acme-tok", None)
            .await
            .status,
        404
    );
    assert_eq!(
        call(M::GET, "/v1/feeds/app", "view-tok", None).await.status,
        404,
        "a server feed of that name does not exist"
    );
    assert_eq!(
        call(M::GET, "/v1/feeds/app?tenant=acme", "view-tok", None)
            .await
            .status,
        200
    );
    let own = call(
        M::POST,
        "/v1/feeds/app/tokens",
        "acme-tok",
        Some(json!({"name": "portal"})),
    )
    .await;
    assert_eq!(own.status, 201, "{}", own.body);

    // Every change left its entry, with the feed and what was done.
    let audit = call(M::GET, "/v1/audit?limit=200", "admin-tok", None).await;
    let entries = audit.body["entries"]
        .as_array()
        .cloned()
        .unwrap_or_else(|| audit.body.as_array().cloned().unwrap());
    let find = |action: &str, result: &str| {
        entries
            .iter()
            .find(|e| e["action"] == action && e["result"] == result)
            .unwrap_or_else(|| panic!("no {action}/{result} in {entries:?}"))
            .clone()
    };
    for action in [
        "feed.create",
        "feed.update",
        "feed.token_create",
        "feed.token_revoke",
        "feed.pause",
        "feed.resume",
        "feed.deliver_now",
        "feed.skip",
        "feed.replay",
        "feed.recover",
        "feed.delivery_set",
        "feed.delivery_delete",
        "feed.events_read",
    ] {
        find(action, "ok");
    }
    let skip = find("feed.skip", "ok");
    assert_eq!(
        (&skip["principal"], &skip["target"]),
        (&json!("alice"), &json!("feed:orders/lake"))
    );
    assert_eq!(skip["detail"], "from event 1 to 3; bad rows");
    assert_eq!(find("feed.pause", "ok")["detail"], "maintenance");
    assert_eq!(find("feed.events_read", "ok")["detail"], "event 2");
    assert!(entries.iter().any(|e| {
        e["action"] == "feed.create"
            && e["result"] == "error"
            && e["detail"]
                .as_str()
                .is_some_and(|d| d.contains("already exists"))
    }));
    assert_eq!(find("feed.create", "denied")["principal"], "carol");
    assert!(entries.iter().any(|e| e["action"] == "feed.create"
        && e["tenant"] == "acme"
        && e["target"] == "feed:acme/app"));
    let all = serde_json::to_string(&entries).unwrap();
    assert!(
        !all.contains(&text) && !all.contains("hmac-s3cret"),
        "a secret reached the audit log"
    );

    // Deleted: its URL stops accepting, its page is gone.
    assert_eq!(
        call(M::DELETE, "/v1/feeds/orders", "admin-tok", None)
            .await
            .status,
        204
    );
    assert_eq!(
        call(M::GET, "/v1/feeds/orders", "view-tok", None)
            .await
            .status,
        404
    );
    assert_eq!(
        call(M::DELETE, "/v1/feeds/orders", "admin-tok", None)
            .await
            .status,
        404
    );
}

/// A sealed feed's payload is never returned, and a gated server answers a
/// change with the change request it made.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sealed_payload_is_withheld_and_a_gated_change_answers_202() {
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::over(config(dir.path(), &["--require-approval", "feed"])).await;
    // Made before the gate is asked, straight through the service.
    let (_, token) = srv
        .feed(json!({"name": "private", "seal": true, "ingest": {"auth": {"tokens": "managed"}}}))
        .await;
    assert_eq!(
        srv.push("private", &token, r#"{"card": "4111"}"#)
            .await
            .status,
        202
    );
    let one = srv
        .send(srv.client.get(srv.url("/v1/feeds/private/events/1")))
        .await;
    assert_eq!(one.status, 200, "{}", one.body);
    assert!(one.body.get("payload").is_none());
    assert_eq!(one.body["payload_withheld"], "sealed");
    assert_eq!(one.body["sealed"], true);

    let list = srv.send(srv.client.get(srv.url("/v1/feeds"))).await;
    assert_eq!(list.body["approval"], "always");
    let proposed = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds?reason=new+source"))
                .json(&json!({"name": "later", "ingest": {"auth": {"tokens": "managed"}}})),
        )
        .await;
    assert_eq!(proposed.status, 202, "{}", proposed.body);
    assert_eq!(proposed.body["status"], "pending_approval");
    assert_eq!(proposed.body["change"]["kind"], "feed");
    assert_eq!(proposed.body["change"]["reason"], "new source");
    assert_eq!(
        srv.send(srv.client.get(srv.url("/v1/feeds/later")))
            .await
            .status,
        404
    );
    let deleting = srv
        .send(srv.client.delete(srv.url("/v1/feeds/private")))
        .await;
    assert_eq!(deleting.status, 202, "{}", deleting.body);
    assert_eq!(
        srv.send(srv.client.get(srv.url("/v1/feeds/private")))
            .await
            .status,
        200
    );
}

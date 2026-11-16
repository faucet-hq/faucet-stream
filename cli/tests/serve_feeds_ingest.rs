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
    let root = dir.display().to_string();
    let history = format!("sqlite:{}", dir.join("history.db").display());
    let mut argv = vec![
        "faucet",
        "serve",
        "--listen",
        "127.0.0.1:0",
        "--no-auth",
        "--no-env-file",
        "--no-ui",
        "--history",
        &history,
        "--vault-key",
        VAULT_KEY,
        "--allow-path-root",
        &root,
    ];
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
        let config = config(dir, extra);
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
    // A compressed body is refused rather than stored unread.
    let gz = srv
        .send(
            srv.client
                .post(srv.url("/v1/feeds/orders/events"))
                .bearer_auth(&token)
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body("{}"),
        )
        .await;
    assert_eq!(gz.status, 415, "{}", gz.body);
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

//! `faucet feed` verbs (#1034): `tail`, `stats` and `cursor` read a feed
//! straight from its stores; `send` posts to a running server's ingest route
//! as a sender does.
#![cfg(all(feature = "feeds", feature = "sink-file", feature = "encryption"))]
#![allow(clippy::disallowed_methods)] // the test server is a plain listener

use assert_cmd::Command;
use clap::Parser;
use faucet_cli::serve::config::ServeConfig;
use faucet_cli::serve::feeds::{FeedsRuntime, service, spec, stores};
use faucet_cli::serve::history::feeds::FeedSpec;
use faucet_cli::serve::rbac::{AuthContext, Role};
use faucet_cli::serve::state::ServerState;
use serde_json::{Value, json};
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

const VAULT_KEY: &str = "test-vault-key-0123456789abcdef0123";

fn faucet(dir: &Path) -> Command {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.current_dir(dir)
        .env_remove("FAUCET_FEED_STORE")
        .env_remove("FAUCET_VAULT_KEY");
    cmd
}

struct Ran {
    ok: bool,
    stdout: String,
    stderr: String,
}

fn ran(cmd: &mut Command) -> Ran {
    let out = cmd.output().unwrap();
    Ran {
        ok: out.status.success(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

fn feed(dir: &Path, args: &[&str]) -> Ran {
    let store = format!("sqlite:{}", dir.join("history.db").display());
    ran(faucet(dir).arg("feed").args(args).args(["--store", &store]))
}

fn passed(r: Ran) -> String {
    assert!(r.ok, "stdout: {}\nstderr: {}", r.stdout, r.stderr);
    r.stdout
}

fn json_of(r: Ran) -> Value {
    let text = passed(r);
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{e}: {text}"))
}

#[test]
fn tail_stats_and_cursor_read_a_feed_from_its_stores() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let store = format!("sqlite:{}", dir.join("history.db").display());
    std::fs::write(
        dir.join("archive.yaml"),
        format!(
            "version: 1\nkind: pipeline\nname: archive\nparams:\n  out: {{ type: string, required: true }}\npipeline:\n  source: {{ type: feed }}\n  sink: {{ type: file, config: {{ path: \"${{param.out}}\", format: json_lines, if_exists: append }} }}\n  state: {{ type: file, config: {{ path: \"{}\" }} }}\ntests:\n  suite:\n    auto: {{ defaults_baseline: true }}\n",
            dir.join("state").display()
        ),
    )
    .unwrap();
    passed(ran(faucet(dir).args([
        "template",
        "register",
        "archive.yaml",
        "--id",
        "archive",
        "--launch",
        "--no-env-file",
        "--store",
        &store,
    ])));
    std::fs::write(
        dir.join("feed.yaml"),
        format!(
            "name: orders\ningest:\n  auth: {{ tokens: managed }}\n  accept: [application/json, application/x-ndjson]\ndeliveries:\n  - name: archive\n    template: {{ id: archive, params: {{ out: \"{}\" }} }}\n  - name: audit\n    template: {{ id: archive, params: {{ out: \"{}\" }} }}\n",
            dir.join("out.jsonl").display(),
            dir.join("audit.jsonl").display()
        ),
    )
    .unwrap();
    passed(feed(dir, &["create", "-f", "feed.yaml"]));

    // An empty feed.
    assert_eq!(passed(feed(dir, &["tail", "orders"])).trim(), "no events");
    assert_eq!(passed(feed(dir, &["tail", "orders", "--json"])), "");
    let empty = json_of(feed(dir, &["stats", "orders", "--json"]));
    assert_eq!(empty["retained_events"], 0, "{empty}");
    assert_eq!(empty["watermark"], 0);

    std::fs::write(
        dir.join("events.ndjson"),
        (1..=5)
            .map(|n| format!("{{\"n\": {n}}}\n"))
            .collect::<String>(),
    )
    .unwrap();
    passed(feed(
        dir,
        &[
            "push",
            "orders",
            "events.ndjson",
            "--content-type",
            "application/x-ndjson",
        ],
    ));
    // `send` is the HTTP verb, no longer another name for `push`.
    let aliased = feed(dir, &["send", "orders", "events.ndjson"]);
    assert!(!aliased.ok);
    assert!(aliased.stderr.contains("--url"), "{}", aliased.stderr);

    // tail: the newest events, oldest first, one JSON object per line.
    let text = passed(feed(dir, &["tail", "orders", "-n", "2"]));
    let rows: Vec<&str> = text.lines().collect();
    assert_eq!(rows.len(), 2, "{text}");
    assert!(rows[0].starts_with("#4 "), "{text}");
    assert!(rows[1].starts_with("#5 "), "{text}");
    assert!(!text.contains("\"n\""), "{text}");
    let out = passed(feed(dir, &["tail", "orders", "--json", "--payload"]));
    let events: Vec<Value> = out
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(
        events
            .iter()
            .map(|e| e["seq"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(events[4]["payload"], "{\"n\": 5}");
    assert_eq!(events[4]["payload_encoding"], "utf8");
    let shown = passed(feed(dir, &["tail", "orders", "-n", "1", "--payload"]));
    assert!(
        shown.contains("#5 ") && shown.contains("  {\"n\": 5}"),
        "{shown}"
    );
    let missing = feed(dir, &["tail", "no-such-feed"]);
    assert!(!missing.ok);

    // stats: the feed's size and each delivery's backlog.
    let stats = json_of(feed(dir, &["stats", "orders", "--json"]));
    assert_eq!(stats["feed"], "orders");
    assert_eq!(stats["status"], "receiving");
    assert_eq!(stats["oldest"], 1);
    assert_eq!(stats["watermark"], 5);
    assert_eq!(stats["retained_events"], 5);
    assert_eq!(stats["events_24h"], 5);
    assert_eq!(stats["stored_bytes"], 5 * "{\"n\": 1}".len());
    assert_eq!(stats["bytes_24h"], stats["stored_bytes"]);
    assert_eq!(stats["gaps"], 0);
    let deliveries = stats["deliveries"].as_array().unwrap();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0]["name"], "archive");
    assert_eq!(deliveries[0]["pending"], 5);
    assert_eq!(deliveries[0]["delivered_24h"], 0);
    assert!(deliveries[0]["oldest_undelivered_at"].is_string());
    let text = passed(feed(dir, &["stats", "orders"]));
    assert!(text.contains("retained  5 event(s)"), "{text}");
    assert!(
        text.contains("delivery archive") && text.contains("5 pending"),
        "{text}"
    );

    // cursor: where each delivery reads next, and the gaps a skip left.
    let cursors = json_of(feed(dir, &["cursor", "orders", "--json"]));
    let cursors = cursors.as_array().unwrap();
    assert_eq!(cursors.len(), 2);
    for c in cursors {
        assert_eq!(c["position"], 1, "{c}");
        assert_eq!(c["pending"], 5);
        assert_eq!(c["watermark"], 5);
        assert_eq!(c["gaps"], json!([]));
        assert!(c.get("claim").is_none());
    }
    passed(feed(
        dir,
        &[
            "skip", "orders", "archive", "--to", "3", "--reason", "bad rows",
        ],
    ));
    let one = json_of(feed(dir, &["cursor", "orders", "archive", "--json"]));
    let one = one.as_array().unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0]["delivery"], "archive");
    assert_eq!(one[0]["position"], 3);
    assert_eq!(one[0]["pending"], 3);
    assert_eq!(
        one[0]["epoch"].as_u64().unwrap(),
        cursors[0]["epoch"].as_u64().unwrap() + 1
    );
    assert_eq!(one[0]["gaps"].as_array().unwrap().len(), 1);
    assert_eq!(one[0]["gaps"][0]["reason"], "bad rows");
    let text = passed(feed(dir, &["cursor", "orders"]));
    assert!(
        text.contains("archive") && text.contains("next event 3"),
        "{text}"
    );
    assert!(
        text.contains("gap 1..") && text.contains("bad rows"),
        "{text}"
    );
    assert!(
        text.contains("audit") && text.contains("next event 1"),
        "{text}"
    );
    let other = json_of(feed(dir, &["cursor", "orders", "audit", "--json"]));
    assert_eq!(other[0]["position"], 1);
    assert_eq!(other[0]["gaps"], json!([]));
    let unknown = feed(dir, &["cursor", "orders", "nope"]);
    assert!(!unknown.ok);
    assert!(
        unknown.stderr.contains("no delivery named nope"),
        "{}",
        unknown.stderr
    );
    assert_eq!(
        json_of(feed(dir, &["stats", "orders", "--json"]))["gaps"],
        1
    );
}

fn admin() -> AuthContext {
    AuthContext {
        principal: "alice".into(),
        role: Role::Admin,
        source_ip: None,
        tenant: None,
    }
}

async fn serve(dir: &Path) -> (ServerState, SocketAddr) {
    let history = format!("sqlite:{}", dir.join("history.db").display());
    let cli = faucet_cli::cli::Cli::try_parse_from([
        "faucet",
        "serve",
        "--listen",
        "127.0.0.1:0",
        "--no-env-file",
        "--no-ui",
        "--no-auth",
        "--history",
        &history,
        "--vault-key",
        VAULT_KEY,
    ])
    .unwrap();
    let faucet_cli::cli::Command::Serve(args) = cli.command else {
        unreachable!()
    };
    let config = ServeConfig::from_args(*args).unwrap();
    let store = faucet_cli::serve::history::connect(
        &config.history,
        config.idempotency_retention,
        config.lease_ttl,
        &uuid::Uuid::new_v4().to_string(),
    )
    .await
    .unwrap();
    let state = ServerState::new(
        &config,
        None,
        tokio_util::sync::CancellationToken::new(),
        store,
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
        stores::Stores::from_config(&config),
    ));
    let router =
        faucet_cli::serve::server::build_router(state.clone(), &config, &Default::default());
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
    (state, addr)
}

async fn create(state: &ServerState, spec: Value) {
    let spec: FeedSpec = serde_json::from_value(spec).unwrap();
    service::create(state, &admin(), None, spec).await.unwrap();
}

async fn send(dir: &Path, args: Vec<String>, env: Vec<(&'static str, String)>, stdin: &str) -> Ran {
    let (dir, stdin) = (dir.to_path_buf(), stdin.to_string());
    tokio::task::spawn_blocking(move || {
        let mut cmd = faucet(&dir);
        cmd.args(["feed", "send"]).args(args).write_stdin(stdin);
        for (k, v) in env {
            cmd.env(k, v);
        }
        ran(&mut cmd)
    })
    .await
    .unwrap()
}

async fn payload(state: &ServerState, feed: &str, seq: u64) -> String {
    service::event(state, &admin(), None, feed, seq)
        .await
        .unwrap()
        .payload
        .unwrap()
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| s.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn send_posts_to_a_running_servers_ingest_route() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let (state, addr) = serve(dir).await;
    create(
        &state,
        json!({
            "name": "orders",
            "ingest": {
                "auth": {"tokens": "managed"},
                "accept": ["application/json", "application/x-ndjson"],
                "dedupe": {"header": "X-Event-Id"}
            }
        }),
    )
    .await;
    create(
        &state,
        json!({
            "name": "hooks",
            "ingest": {"auth": {"signature": {
                "header": "X-Signature", "secret": "hmac-secret-1", "prefix": "sha256=",
                "timestamp_header": "X-Timestamp"
            }}}
        }),
    )
    .await;
    create(
        &state,
        json!({
            "name": "plain",
            "ingest": {"auth": {"signature": {
                "header": "X-Sig", "secret": "hmac-secret-2",
                "algorithm": "sha512", "encoding": "base64"
            }}}
        }),
    )
    .await;
    let token = service::token_create(&state, &admin(), None, "orders", "sender", None)
        .await
        .unwrap()
        .1;
    let url = |feed: &str| format!("http://{addr}/v1/feeds/{feed}/events");
    let never_shown = |r: &Ran, secret: &str| {
        assert!(!r.stdout.contains(secret), "{}", r.stdout);
        assert!(!r.stderr.contains(secret), "{}", r.stderr);
    };

    // A token from the environment, the body from stdin.
    let sent = send(
        dir,
        strings(&[
            "--url",
            &url("orders"),
            "--token-env",
            "FEED_TOKEN",
            "--content-type",
            "application/x-ndjson",
        ]),
        vec![("FEED_TOKEN", token.clone())],
        "{\"n\": 1}\n{\"n\": 2}\n",
    )
    .await;
    never_shown(&sent, &token);
    assert_eq!(passed(sent).trim(), "stored 2 event(s)");
    assert_eq!(payload(&state, "orders", 2).await, "{\"n\": 2}");

    // A file, gzip, an extra header and JSON output; the same dedupe key
    // again stores nothing new.
    std::fs::write(dir.join("one.json"), "{\"n\": 3}").unwrap();
    let gz = strings(&[
        "--url",
        &url("orders"),
        "--token-env",
        "FEED_TOKEN",
        "-f",
        "one.json",
        "--gzip",
        "--header",
        "X-Event-Id=evt-3",
        "--json",
    ]);
    let first = json_of(send(dir, gz.clone(), vec![("FEED_TOKEN", token.clone())], "").await);
    assert_eq!(first["first_seq"], 3, "{first}");
    assert_eq!(first["ids"].as_array().unwrap().len(), 1);
    assert_eq!(payload(&state, "orders", 3).await, "{\"n\": 3}");
    let again = send(
        dir,
        gz.into_iter().filter(|a| a != "--json").collect(),
        vec![("FEED_TOKEN", token.clone())],
        "",
    )
    .await;
    assert_eq!(passed(again).trim(), "stored 0 event(s), 1 already stored");

    // A refusal fails the command with the server's answer and no credential.
    let wrong = send(
        dir,
        strings(&[
            "--url",
            &url("orders"),
            "--token",
            "fdt_not-the-feeds-token",
        ]),
        vec![],
        "{}",
    )
    .await;
    assert!(!wrong.ok);
    assert!(
        wrong.stderr.contains("the server answered 401"),
        "{}",
        wrong.stderr
    );
    never_shown(&wrong, "fdt_not-the-feeds-token");
    let bad = send(
        dir,
        strings(&["--url", &url("orders"), "--token-env", "FEED_TOKEN"]),
        vec![("FEED_TOKEN", token.clone())],
        "{",
    )
    .await;
    assert!(!bad.ok);
    assert!(
        bad.stderr.contains("the server answered 400"),
        "{}",
        bad.stderr
    );
    assert!(bad.stderr.contains("not JSON"), "{}", bad.stderr);
    never_shown(&bad, &token);

    // A signed feed: the body is signed as its signature spec says.
    let signed = send(
        dir,
        strings(&[
            "--url",
            &url("hooks"),
            "--signature-secret-env",
            "FEED_SECRET",
            "--signature-header",
            "X-Signature",
            "--signature-prefix",
            "sha256=",
            "--signature-timestamp-header",
            "X-Timestamp",
            "--gzip",
        ]),
        vec![("FEED_SECRET", "hmac-secret-1".into())],
        "{\"hook\": 1}",
    )
    .await;
    never_shown(&signed, "hmac-secret-1");
    assert_eq!(passed(signed).trim(), "stored 1 event(s)");
    assert_eq!(payload(&state, "hooks", 1).await, "{\"hook\": 1}");
    let other = send(
        dir,
        strings(&[
            "--url",
            &url("plain"),
            "--signature-secret-env",
            "FEED_SECRET",
            "--signature-header",
            "X-Sig",
            "--signature-algorithm",
            "sha512",
            "--signature-encoding",
            "base64",
        ]),
        vec![("FEED_SECRET", "hmac-secret-2".into())],
        "{\"hook\": 2}",
    )
    .await;
    assert_eq!(passed(other).trim(), "stored 1 event(s)");
    let mismatched = send(
        dir,
        strings(&[
            "--url",
            &url("plain"),
            "--signature-secret-env",
            "FEED_SECRET",
            "--signature-header",
            "X-Sig",
        ]),
        vec![("FEED_SECRET", "hmac-secret-2".into())],
        "{}",
    )
    .await;
    assert!(!mismatched.ok);
    assert!(mismatched.stderr.contains("401"), "{}", mismatched.stderr);
    never_shown(&mismatched, "hmac-secret-2");

    // What cannot be sent is refused before any request is made.
    let refused = [
        (
            strings(&["--url", &url("orders")]),
            "needs the feed's credential",
        ),
        (
            strings(&["--url", &url("orders"), "--token-env", "FEED_TOKEN_UNSET"]),
            "FEED_TOKEN_UNSET is not set",
        ),
        (
            strings(&["--url", "ftp://example.invalid/x", "--token", "fdt_x"]),
            "http(s) ingest URL",
        ),
        (
            strings(&[
                "--url",
                &url("plain"),
                "--signature-secret-env",
                "FEED_SECRET",
                "--signature-header",
                "X-Sig",
                "--signature-algorithm",
                "md5",
            ]),
            "--signature-algorithm",
        ),
        (
            strings(&[
                "--url",
                &url("orders"),
                "--token",
                "fdt_x",
                "--header",
                "nope",
            ]),
            "NAME=VALUE",
        ),
    ];
    for (args, want) in refused {
        let r = send(
            dir,
            args,
            vec![("FEED_SECRET", "hmac-secret-2".into())],
            "{}",
        )
        .await;
        assert!(!r.ok, "{want}");
        assert!(r.stderr.contains(want), "{want}: {}", r.stderr);
    }
    let orders = service::get(&state, &admin(), None, "orders")
        .await
        .unwrap();
    assert_eq!(orders.feed.watermark, Some(3));
}

/// `tail --follow` prints events another process stores, on a store several
/// processes share.
#[cfg(feature = "serve-history-postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_follow_prints_events_as_they_are_stored() {
    use std::io::BufRead;
    use testcontainers::runners::AsyncRunner;
    let pg = testcontainers_modules::postgres::Postgres::default()
        .start()
        .await
        .unwrap();
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    std::fs::write(
        dir.join("feed.yaml"),
        "name: orders\ningest:\n  auth: { tokens: managed }\n",
    )
    .unwrap();
    let run = |args: Vec<&'static str>, stdin: &'static str| {
        let (dir, url) = (dir.clone(), url.clone());
        tokio::task::spawn_blocking(move || {
            ran(faucet(&dir)
                .arg("feed")
                .args(args)
                .args(["--store", &url])
                .write_stdin(stdin))
        })
    };
    passed(run(vec!["create", "-f", "feed.yaml"], "").await.unwrap());
    passed(run(vec!["push", "orders"], "{\"n\": 1}").await.unwrap());

    let mut child = std::process::Command::new(assert_cmd::cargo::cargo_bin("faucet"))
        .current_dir(&dir)
        .args(["feed", "tail", "orders", "--follow", "--json", "--payload"])
        .args(["--interval-secs", "1", "--store", &url])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Value>();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
        {
            if tx.send(serde_json::from_str(&line).unwrap()).is_err() {
                break;
            }
        }
    });
    let mut next = async || {
        tokio::time::timeout(std::time::Duration::from_secs(60), rx.recv())
            .await
            .expect("tail printed nothing in time")
            .expect("tail ended")
    };
    assert_eq!(next().await["payload"], "{\"n\": 1}");
    passed(
        run(vec!["push", "orders"], "[{\"n\": 2}, {\"n\": 3}]")
            .await
            .unwrap(),
    );
    let (second, third) = (next().await, next().await);
    child.kill().unwrap();
    child.wait().unwrap();
    assert_eq!(
        (second["seq"].as_u64(), third["seq"].as_u64()),
        (Some(2), Some(3))
    );
    assert_eq!(third["payload"], "{\"n\": 3}");
}

/// With `--feeds`, the verbs find the feeds of the server's feeds file; a
/// file feed is changed in the file, never through a verb.
#[test]
fn feeds_of_the_feeds_file_are_found_with_the_feeds_flag() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    std::fs::write(
        dir.join("archive.yaml"),
        "version: 1\nname: orders_archive\npipeline:\n  source: { type: feed }\n  sink: { type: file, config: { path: ./out/orders.jsonl, if_exists: append } }\n  state: { type: file, config: { path: ./state } }\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("feeds.yaml"),
        "version: 1\nfeeds:\n  - name: orders\n    ingest:\n      auth: { tokens: managed }\n      accept: [application/json, application/x-ndjson]\n    deliveries:\n      - name: archive\n        pipeline: archive.yaml\n",
    )
    .unwrap();
    let with = |args: &[&str]| {
        let mut all = args.to_vec();
        all.extend(["--feeds", "feeds.yaml"]);
        feed(dir, &all)
    };

    // Without the file the feed is unknown; with it, it is listed and shown.
    assert!(!feed(dir, &["show", "orders"]).ok);
    let listed = json_of(with(&["list", "--json"]));
    assert_eq!(listed[0]["name"], "orders");
    assert_eq!(listed[0]["source"], "file");
    let shown = json_of(with(&["show", "orders", "--json"]));
    assert_eq!(shown["deliveries"][0]["name"], "archive");

    // Tokens, events and positions work as for a stored feed.
    let token = json_of(with(&["token", "create", "orders", "shop", "--json"]));
    assert!(token["token"].as_str().unwrap().starts_with("fdt_"));
    let tokens = json_of(with(&["token", "list", "orders", "--json"]));
    assert_eq!(tokens.as_array().unwrap().len(), 1);
    passed(ran(faucet(dir)
        .args(["feed", "push", "orders", "--feeds", "feeds.yaml", "--store"])
        .arg(format!("sqlite:{}", dir.join("history.db").display()))
        .write_stdin("[{\"id\": 1}, {\"id\": 2}, {\"id\": 3}]")));
    assert_eq!(
        json_of(with(&["events", "orders", "--json"]))["watermark"],
        3
    );
    assert_eq!(
        passed(with(&["tail", "orders", "-n", "5"])).lines().count(),
        3
    );
    assert_eq!(
        json_of(with(&["stats", "orders", "--json"]))["retained_events"],
        3
    );
    assert_eq!(
        json_of(with(&["cursor", "orders", "archive", "--json"]))[0]["position"],
        1
    );
    passed(with(&[
        "pause",
        "orders",
        "archive",
        "--reason",
        "maintenance",
    ]));
    assert_eq!(
        json_of(with(&["show", "orders", "--json"]))["deliveries"][0]["status"],
        "paused"
    );
    passed(with(&["resume", "orders", "archive"]));
    passed(with(&[
        "skip",
        "orders",
        "archive",
        "--to",
        "3",
        "--reason",
        "test rows",
    ]));
    passed(with(&["replay", "orders", "archive", "--to-seq", "2"]));
    assert_eq!(
        json_of(with(&["cursor", "orders", "--json"]))[0]["position"],
        2
    );
    let id = tokens[0]["id"].as_str().unwrap();
    passed(with(&["token", "revoke", "orders", id]));

    // The file is where a file feed changes.
    std::fs::write(
        dir.join("same-name.yaml"),
        "name: orders\ningest:\n  auth: { tokens: managed }\n",
    )
    .unwrap();
    for (args, want) in [
        (
            &["create", "-f", "same-name.yaml"][..],
            "the --feeds file already defines feed 'orders'",
        ),
        (
            &["apply", "-f", "same-name.yaml"][..],
            "defined in the --feeds file",
        ),
        (&["delete", "orders"][..], "defined in the --feeds file"),
    ] {
        let refused = with(args);
        assert!(!refused.ok, "{args:?}: {}", refused.stdout);
        assert!(
            refused.stderr.contains(want),
            "{args:?}: {}",
            refused.stderr
        );
    }
    assert_eq!(
        json_of(with(&["list", "--json"])).as_array().unwrap().len(),
        1
    );
    let missing = feed(dir, &["list", "--feeds", "no-such-file.yaml"]);
    assert!(!missing.ok);
    assert!(
        missing.stderr.contains("no-such-file.yaml"),
        "{}",
        missing.stderr
    );
}

/// `faucet feed adopt-store`: a feed whose store name now points to another
/// database is started empty there, one feed or every such feed at once.
#[test]
fn adopt_store_starts_a_moved_feed_empty_in_its_new_store() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let named = |db: &str| format!("x=sqlite:{}", dir.join(db).display());
    let spec = dir.join("orders.yaml");
    std::fs::write(
        &spec,
        "name: orders\nstore: x\ningest: { auth: { tokens: managed } }\n",
    )
    .unwrap();
    let spec_arg = spec.display().to_string();
    passed(feed(
        dir,
        &["create", "-f", &spec_arg, "--feed-store", &named("x1.db")],
    ));
    let now = named("x2.db");
    let unmoved = feed(
        dir,
        &["adopt-store", "orders", "--feed-store", &named("x1.db")],
    );
    assert!(
        !unmoved.ok && unmoved.stderr.contains("nothing to adopt"),
        "{}",
        unmoved.stderr
    );
    let one = json_of(feed(
        dir,
        &["adopt-store", "orders", "--feed-store", &now, "--json"],
    ));
    assert_eq!(one["feed"], "orders");
    assert_eq!(one["abandoned_store"], "x");
    assert_ne!(one["store_id"], one["abandoned_store_id"]);
    let none = passed(feed(dir, &["adopt-store", "--all", "--feed-store", &now]));
    assert!(none.contains("no feed"), "{none}");
    let all = json_of(feed(
        dir,
        &[
            "adopt-store",
            "--all",
            "--in",
            "x",
            "--feed-store",
            &named("x3.db"),
            "--json",
        ],
    ));
    assert_eq!(all["adopted"].as_array().unwrap().len(), 1, "{all}");
    let neither = feed(dir, &["adopt-store", "--feed-store", &now]);
    assert!(
        !neither.ok && neither.stderr.contains("--all"),
        "{}",
        neither.stderr
    );
}

//! Feeds end to end, in process (#1034): definitions, the delivery scheduler,
//! the run binding and the operator's moves, over a real SQLite history and
//! feed store. Events are appended straight to the store (the ingest route
//! is a later stage); the scheduler is ticked by hand so the tests do not
//! wait on its timers.
#![cfg(all(
    feature = "feeds",
    feature = "sink-file",
    feature = "encryption",
    feature = "tenants",
    feature = "source-rest",
    feature = "serve-history-postgres"
))]
#![allow(clippy::disallowed_methods)] // tests pace their own polling

use clap::Parser;
use faucet_cli::serve::config::ServeConfig;
use faucet_cli::serve::error::ServeError;
use faucet_cli::serve::feeds::runner::{self, Scheduler};
use faucet_cli::serve::feeds::status::{self, DeliveryStatus, Health};
use faucet_cli::serve::feeds::{FeedsRuntime, service, spec, store_calls, stores};
use faucet_cli::serve::history::feeds::FeedSpec;
use faucet_cli::serve::rbac::{AuthContext, Role};
use faucet_cli::serve::state::ServerState;
use faucet_common_feed::{FeedBinding, FeedStore, FeedUid, NewEvent, Seq};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
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

fn config(dir: &Path, history: &str, extra: &[&str]) -> ServeConfig {
    let root = dir.display().to_string();
    let mut argv = vec![
        "faucet",
        "serve",
        "--listen",
        "127.0.0.1:0",
        "--no-auth",
        "--no-env-file",
        "--no-ui",
        "--history",
        history,
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
}

/// What a test looks at between ticks.
#[derive(Debug)]
struct Pos {
    cursor: u64,
    claimed: bool,
    status: DeliveryStatus,
}

impl Srv {
    async fn over(config: &ServeConfig) -> Self {
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
        if let Some(path) = &config.feeds.path {
            let file = spec::load_file(path).await.unwrap();
            state.set_feeds(FeedsRuntime::new(
                config.feeds.clone(),
                file,
                stores::Stores::from_config(config),
            ));
        }
        Self {
            state,
            scheduler: Scheduler::default(),
        }
    }

    async fn start(dir: &Path, extra: &[&str]) -> Self {
        let history = format!("sqlite:{}", dir.join("history.db").display());
        Self::over(&config(dir, &history, extra)).await
    }

    async fn tick(&mut self) -> runner::TickReport {
        self.scheduler.invalidate();
        self.scheduler.tick(&self.state).await
    }

    async fn store(&self, feed: &str) -> (Arc<dyn FeedStore>, FeedUid) {
        let found = service::find(&self.state, None, feed).await.unwrap();
        let store = self.state.feeds().store_for(&found.feed).await.unwrap();
        (store, found.feed.uid.clone())
    }

    async fn append(&self, feed: &str, ids: std::ops::RangeInclusive<u64>) {
        let (store, uid) = self.store(feed).await;
        let events = ids
            .map(|i| NewEvent::new("application/json", format!("{{\"id\":{i}}}").into_bytes()))
            .collect();
        store.append(&store_calls(), &uid, events).await.unwrap();
    }

    async fn pos(&self, feed: &str, delivery: &str) -> Pos {
        let found = service::find(&self.state, None, feed).await.unwrap();
        let dref = found.feed.delivery(delivery).unwrap().clone();
        let store = self.state.feeds().store_for(&found.feed).await.unwrap();
        let head = store
            .heads(&store_calls(), std::slice::from_ref(&found.feed.uid))
            .await
            .unwrap()
            .remove(0);
        let dh = head
            .deliveries
            .iter()
            .find(|d| d.delivery == dref.uid)
            .unwrap();
        let status = status::read(
            &*self.state.history(),
            found.feed.uid.as_str(),
            dref.uid.id(),
        )
        .await
        .unwrap();
        Pos {
            cursor: dh.cursor.get(),
            claimed: dh.claim.is_some(),
            status,
        }
    }

    /// Tick until `done`; with `no_backoff` a failure's back-off is cleared
    /// between ticks so the test does not wait it out.
    async fn until(
        &mut self,
        feed: &str,
        delivery: &str,
        no_backoff: bool,
        what: &str,
        done: impl Fn(&Pos) -> bool,
    ) -> Pos {
        for _ in 0..600 {
            self.tick().await;
            let pos = self.pos(feed, delivery).await;
            if done(&pos) {
                return pos;
            }
            if no_backoff && pos.status.backoff_until.is_some() {
                let found = service::find(&self.state, None, feed).await.unwrap();
                let id = found.feed.delivery(delivery).unwrap().uid.id().to_string();
                status::update(&*self.state.history(), found.feed.uid.as_str(), &id, |s| {
                    s.backoff_until = None;
                    true
                })
                .await
                .unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "timed out waiting for: {what}; last: {:?}",
            self.pos(feed, delivery).await
        );
    }

    async fn delivered(&mut self, feed: &str, delivery: &str, cursor: u64) -> Pos {
        self.until(feed, delivery, false, &format!("cursor {cursor}"), |p| {
            p.cursor == cursor && !p.claimed && p.status.claim.is_none()
        })
        .await
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

fn feed_spec(name: &str, deliveries: Value) -> FeedSpec {
    serde_json::from_value(json!({
        "name": name,
        "ingest": {"auth": {"tokens": "managed"}},
        "deliveries": deliveries
    }))
    .unwrap()
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

fn ids(path: &Path) -> Vec<u64> {
    lines(path)
        .iter()
        .map(|v| v["id"].as_u64().unwrap())
        .collect()
}

fn message(e: ServeError) -> String {
    e.api_error().error.message
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deliveries_run_move_their_cursors_and_never_share_state() {
    let dir = tempfile::tempdir().unwrap();
    let (root, state_dir) = (dir.path(), dir.path().join("state"));
    let mut srv = Srv::start(root, &[]).await;
    let rt = srv.state.feeds();
    let feeds_db = root.join("feeds.db");

    // Nothing runs, and no store file exists, until a feed does.
    rt.lease_tick(&srv.state).await;
    assert!(!rt.started());
    assert!(!feeds_db.exists());
    assert!(
        service::list(&srv.state, &admin(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(!feeds_db.exists(), "listing opens no store");

    register(&srv.state, "orders_lake", &state_dir).await;
    let (lake, audit) = (root.join("lake.jsonl"), root.join("audit.jsonl"));
    let created = service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec(
            "orders",
            json!([
                delivery("lake", "orders_lake", &lake),
                delivery("audit", "orders_lake", &audit)
            ]),
        ),
    )
    .await
    .unwrap();
    assert!(rt.started(), "the first feed starts the scheduler");
    assert!(feeds_db.exists() && root.join("feeds.db.lock").exists());
    assert_eq!(created.deliveries.len(), 2);
    assert_eq!(created.deliveries[0].position, Some(1));

    // The same name again is refused; so is another server's lock on the file.
    let again = service::create(&srv.state, &admin(), None, feed_spec("orders", json!([]))).await;
    assert!(matches!(again, Err(ServeError::Conflict(_))));

    srv.append("orders", 1..=3).await;
    srv.delivered("orders", "lake", 4).await;
    let after = srv.delivered("orders", "audit", 4).await;
    assert_eq!(after.status.last_run.as_ref().unwrap().outcome, "delivered");
    assert_eq!(after.status.health, Health::Ok);
    assert_eq!(ids(&lake), vec![1, 2, 3]);
    assert_eq!(ids(&audit), vec![1, 2, 3]);
    let first = &lines(&lake)[0];
    assert_eq!(first["_feed"]["seq"], 1);
    assert!(first["_feed"]["id"].is_string());

    // Two deliveries of one template keep separate state: one bookmark each,
    // under a pipeline name that ends in the delivery's short id.
    let found = service::find(&srv.state, None, "orders").await.unwrap();
    let mut states: Vec<String> = std::fs::read_dir(&state_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| !n.contains("__"))
        .collect();
    states.sort();
    assert_eq!(states.len(), 2, "{states:?}");
    for (name, dref) in &found.feed.deliveries {
        let short = dref.short_id();
        assert_eq!(short.len(), 12);
        assert!(
            states
                .iter()
                .any(|s| s.contains(&format!("orders_lake.{short}"))),
            "{name}: {short} not in {states:?}"
        );
    }

    // The runs carry the reserved labels and their binding.
    let run_id = after
        .status
        .last_run
        .as_ref()
        .unwrap()
        .run_id
        .clone()
        .unwrap();
    let rec = srv.state.history().get(&run_id).await.unwrap().unwrap();
    assert_eq!(rec.labels["feed"], "orders");
    assert_eq!(rec.labels["feed_delivery"], "audit");
    assert_eq!(rec.labels["feed_claim"], rec.feed.as_ref().unwrap().claim);
    assert_eq!(rec.pipeline_name_suffix.as_deref().map(str::len), Some(12));
    assert!(rec.idempotency_key.as_deref().unwrap().starts_with("feed:"));
    assert_eq!(rec.submitted_by.as_deref(), Some("feed:orders"));

    // A paused delivery keeps its position; resuming delivers what waited.
    service::pause(
        &srv.state,
        &admin(),
        None,
        "orders",
        "lake",
        Some("maintenance".into()),
    )
    .await
    .unwrap();
    srv.append("orders", 4..=5).await;
    srv.delivered("orders", "audit", 6).await;
    for _ in 0..5 {
        srv.tick().await;
    }
    let paused = srv.pos("orders", "lake").await;
    assert_eq!(paused.cursor, 4);
    assert_eq!(paused.status.paused.as_ref().unwrap().by, "alice");
    let detail = service::get(&srv.state, &admin(), None, "orders")
        .await
        .unwrap();
    assert_eq!(detail.feed.deliveries[0].status, "paused");
    assert_eq!(
        detail.feed.deliveries[0].pause_reason.as_deref(),
        Some("maintenance")
    );
    assert_eq!(detail.feed.deliveries[0].lag, Some(2));
    assert_eq!(detail.feed.watermark, Some(5));
    assert_eq!(detail.changes[0].action, "created");
    service::resume(&srv.state, &admin(), None, "orders", "lake")
        .await
        .unwrap();
    srv.delivered("orders", "lake", 6).await;
    assert_eq!(ids(&lake), vec![1, 2, 3, 4, 5]);

    // A replay goes back and delivers retained events again.
    let moved = service::replay(
        &srv.state,
        &admin(),
        None,
        "orders",
        "lake",
        service::Position::Seq(4),
    )
    .await
    .unwrap();
    assert_eq!((moved.from, moved.to), (6, 4));
    assert!(moved.epoch >= 1, "a cursor move starts a new epoch");
    srv.delivered("orders", "lake", 6).await;
    assert_eq!(ids(&lake), vec![1, 2, 3, 4, 5, 4, 5]);
    let forward = service::replay(
        &srv.state,
        &admin(),
        None,
        "orders",
        "lake",
        service::Position::Seq(9),
    )
    .await;
    assert!(message(forward.unwrap_err()).contains("a replay goes back"));

    // A run asked for now does not wait for the flush policy.
    let mut slow = service::find(&srv.state, None, "orders")
        .await
        .unwrap()
        .record
        .unwrap()
        .spec;
    slow.deliveries[0].flush.max_events = 1_000;
    slow.deliveries[0].flush.max_age = spec::Span(3_600);
    slow.description = Some("orders from the shop".into());
    let updated = service::update(&srv.state, &admin(), None, "orders", slow)
        .await
        .unwrap();
    assert_eq!(updated.revision, 2);
    srv.append("orders", 6..=6).await;
    srv.delivered("orders", "audit", 7).await;
    for _ in 0..5 {
        srv.tick().await;
    }
    assert_eq!(
        srv.pos("orders", "lake").await.cursor,
        6,
        "the flush policy is not met"
    );
    service::deliver_now(&srv.state, &admin(), None, "orders", "lake")
        .await
        .unwrap();
    srv.delivered("orders", "lake", 7).await;

    // Tokens: shown once, listed without their text, revoked for good.
    let (token, text) = service::token_create(&srv.state, &admin(), None, "orders", "ci", None)
        .await
        .unwrap();
    assert!(text.starts_with("fdt_") && text.len() == 68);
    assert_eq!(token.prefix, &text[..10]);
    let snapshot = rt.snapshot();
    let stored = snapshot.token(&service::token_hash(&text)).unwrap();
    assert_eq!(stored.id, token.id);
    let listed = service::token_list(&srv.state, &admin(), None, "orders")
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert!(
        !serde_json::to_string(&listed)
            .unwrap()
            .contains(&text[10..])
    );
    let revoked = service::token_revoke(&srv.state, &admin(), None, "orders", &token.id)
        .await
        .unwrap();
    assert!(revoked.revoked_at.is_some());
    assert!(rt.snapshot().token(&service::token_hash(&text)).is_none());
    let again = service::token_revoke(&srv.state, &admin(), None, "orders", &token.id)
        .await
        .unwrap();
    assert_eq!(again.revoked_at, revoked.revoked_at);
    assert!(matches!(
        service::token_revoke(&srv.state, &admin(), None, "orders", "nope").await,
        Err(ServeError::NotFound)
    ));
    let past = service::token_create(
        &srv.state,
        &admin(),
        None,
        "orders",
        "old",
        Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
    )
    .await;
    assert!(message(past.unwrap_err()).contains("in the past"));

    // Removing a delivery removes its position, status and state.
    let before = service::find(&srv.state, None, "orders").await.unwrap();
    let audit_uid = before.feed.delivery("audit").unwrap().clone();
    let trimmed = service::delete_delivery(&srv.state, &admin(), None, "orders", "audit")
        .await
        .unwrap();
    assert_eq!(trimmed.deliveries.len(), 1);
    let (store, uid) = srv.store("orders").await;
    let head = store
        .heads(&store_calls(), std::slice::from_ref(&uid))
        .await
        .unwrap()
        .remove(0);
    assert!(head.deliveries.iter().all(|d| d.delivery != audit_uid.uid));
    let left: Vec<String> = std::fs::read_dir(&state_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(&audit_uid.short_id()))
        .collect();
    assert!(left.is_empty(), "{left:?}");
    assert!(matches!(
        service::delete_delivery(&srv.state, &admin(), None, "orders", "audit").await,
        Err(ServeError::NotFound)
    ));
    // Adding one back starts it at the feed's oldest retained event.
    let back = service::set_delivery(
        &srv.state,
        &admin(),
        None,
        "orders",
        serde_json::from_value(delivery("audit", "orders_lake", &root.join("audit2.jsonl")))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(back.deliveries.len(), 2);
    srv.delivered("orders", "audit", 7).await;
    assert_eq!(ids(&root.join("audit2.jsonl")), vec![1, 2, 3, 4, 5, 6]);

    // The scheduler reports what it sees.
    let ready = rt.readiness();
    assert!(ready.started && ready.store_ok);
    assert_eq!(
        (ready.feeds, ready.stalled.len(), ready.held.len()),
        (1, 0, 0)
    );

    // A replay by time goes back to the first event stored at or after it.
    let by_time = service::replay(
        &srv.state,
        &admin(),
        None,
        "orders",
        "lake",
        service::Position::Time(chrono::Utc::now() - chrono::Duration::hours(1)),
    )
    .await
    .unwrap();
    assert_eq!((by_time.from, by_time.to), (7, 1));
    service::pause(&srv.state, &admin(), None, "orders", "lake", None)
        .await
        .unwrap();
    service::pause(&srv.state, &admin(), None, "orders", "audit", None)
        .await
        .unwrap();
    srv.until("orders", "lake", false, "no run in flight", |p| {
        !p.claimed && p.status.claim.is_none()
    })
    .await;
    srv.until("orders", "audit", false, "no run in flight", |p| {
        !p.claimed && p.status.claim.is_none()
    })
    .await;

    // Deleting the feed removes its events, tokens and definition.
    service::delete(&srv.state, &admin(), None, "orders")
        .await
        .unwrap();
    assert!(
        store
            .heads(&store_calls(), std::slice::from_ref(&uid))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        srv.state
            .history()
            .feed_token_list(None)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(matches!(
        service::get(&srv.state, &admin(), None, "orders").await,
        Err(ServeError::NotFound)
    ));
    assert_eq!(srv.tick().await.feeds, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_sink_halves_the_batch_until_one_event_stalls_and_a_skip_moves_on() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut srv = Srv::start(root, &[]).await;
    register(&srv.state, "orders_lake", &root.join("state")).await;
    // The sink's directory is a file: every write fails.
    let blocker = root.join("blocked");
    std::fs::write(&blocker, "not a directory").unwrap();
    let out = blocker.join("lake.jsonl");
    let mut d = delivery("lake", "orders_lake", &out);
    d["max_batch"] = json!({"events": 4});
    d["retries"] = json!(2);
    service::create(&srv.state, &admin(), None, feed_spec("orders", json!([d])))
        .await
        .unwrap();
    srv.append("orders", 1..=4).await;

    let failing = srv
        .until("orders", "lake", true, "failing", |p| {
            p.status.health == Health::Failing
        })
        .await;
    assert_eq!(failing.cursor, 1, "nothing was delivered");
    assert!(failing.status.batch_events.unwrap() <= 1);
    assert!(failing.status.consecutive_failures >= 3);

    let stalled = srv
        .until("orders", "lake", true, "stalled", |p| {
            p.status.health == Health::Stalled
        })
        .await;
    assert_eq!((stalled.cursor, stalled.status.failures_at_one), (1, 2));
    let last = stalled.status.last_run.as_ref().unwrap();
    assert_eq!(
        (last.from, last.to, last.outcome.as_str()),
        (1, 2, "failed")
    );
    assert!(stalled.status.last_error.is_some());
    // A stalled delivery starts nothing.
    let stalled_at = srv
        .pos("orders", "lake")
        .await
        .status
        .last_run
        .unwrap()
        .claim;
    for _ in 0..5 {
        let report = srv.tick().await;
        assert_eq!(report.claims, 0);
    }
    assert_eq!(
        srv.pos("orders", "lake")
            .await
            .status
            .last_run
            .unwrap()
            .claim,
        stalled_at
    );
    let ready = srv.state.feeds().readiness();
    assert_eq!(ready.stalled, vec!["orders/lake".to_string()]);
    let view = service::get(&srv.state, &admin(), None, "orders")
        .await
        .unwrap();
    assert_eq!(view.feed.deliveries[0].status, "stalled");

    // The sink is fixed; the poison event is skipped, with a reason.
    assert!(
        message(
            service::skip(&srv.state, &admin(), None, "orders", "lake", None, " ")
                .await
                .unwrap_err()
        )
        .contains("needs a reason")
    );
    std::fs::remove_file(&blocker).unwrap();
    let moved = service::skip(
        &srv.state,
        &admin(),
        None,
        "orders",
        "lake",
        None,
        "event 1 is malformed",
    )
    .await
    .unwrap();
    assert_eq!((moved.from, moved.to), (1, 2));
    assert!(
        message(
            service::skip(&srv.state, &admin(), None, "orders", "lake", Some(99), "x")
                .await
                .unwrap_err()
        )
        .contains("past the feed's end")
    );
    assert!(
        message(
            service::skip(&srv.state, &admin(), None, "orders", "lake", Some(1), "x")
                .await
                .unwrap_err()
        )
        .contains("a skip goes forward")
    );
    let detail = service::get(&srv.state, &admin(), None, "orders")
        .await
        .unwrap();
    assert_eq!(detail.gaps.len(), 1);
    let gap = &detail.gaps[0];
    assert_eq!(
        (gap.from, gap.reason.as_str(), gap.by.as_str()),
        (1, "event 1 is malformed", "alice")
    );
    assert_eq!(gap.delivery.as_deref(), Some("lake"));

    srv.delivered("orders", "lake", 5).await;
    assert_eq!(
        ids(&out),
        vec![2, 3, 4],
        "the skipped event is not delivered"
    );
    let ok = srv.pos("orders", "lake").await;
    assert_eq!(
        (ok.status.health, ok.status.consecutive_failures),
        (Health::Ok, 0)
    );

    // A recover keeps a valid position and records a gap.
    let recovered = service::recover(&srv.state, &admin(), None, "orders", "lake")
        .await
        .unwrap();
    assert_eq!((recovered.from, recovered.to), (5, 5));
    assert!(
        recovered.epoch > moved.epoch,
        "a recover starts a new epoch"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feed_source_runs_only_as_a_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut srv = Srv::start(root, &[]).await;
    register(&srv.state, "orders_lake", &root.join("state")).await;
    let out = root.join("lake.jsonl");

    // A submitted config that reads a feed is refused at once.
    let config = format!(
        "version: 1\nname: sneaky\npipeline:\n  source: {{ type: feed }}\n  sink: {{ type: file, config: {{ path: \"{}\", if_exists: append }} }}\n",
        out.display()
    );
    let req = serde_json::from_value(json!({"config": config})).unwrap();
    let principal = faucet_cli::runtime::Principal::caller(&srv.state, admin());
    let refused = faucet_cli::runtime::server::submit(
        &srv.state,
        &principal,
        req,
        faucet_cli::runtime::server::Approval::Required,
    )
    .await
    .unwrap_err();
    assert!(message(refused).contains("runs only as a delivery of a feed"));

    // A registered template that reads a feed, triggered on its own, is
    // refused when it runs: no delivery bound it to a range.
    let body = faucet_cli::serve::handlers::templates::TriggerBody {
        params: [("out".to_string(), json!(out.display().to_string()))].into(),
        ..Default::default()
    };
    let outcome = faucet_cli::runtime::server::trigger_template(
        srv.state.clone(),
        faucet_cli::runtime::Principal::new(admin(), faucet_cli::runtime::Origin::Trusted),
        "orders_lake".into(),
        body,
    )
    .await
    .unwrap();
    let faucet_cli::runtime::server::TriggerOutcome::Run(run) = outcome else {
        panic!("expected a run");
    };
    let mut rec = None;
    for _ in 0..400 {
        let r = srv
            .state
            .history()
            .get(&run.run.run_id)
            .await
            .unwrap()
            .unwrap();
        if r.status.is_terminal() {
            rec = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let rec = rec.expect("the run ends");
    assert_eq!(rec.status.as_str(), "failed");
    assert!(rec.feed.is_none() && rec.pipeline_name_suffix.is_none());
    let why = format!("{:?} {:?}", rec.error, rec.invocations);
    assert!(why.contains("runs only as a delivery"), "{why}");
    assert!(!out.exists());

    // A delivery whose pipeline does not read the feed is refused when made.
    let plain = "version: 1\nkind: pipeline\nname: plain\nparams:\n  out: { type: string, required: true }\npipeline:\n  source: { type: rest, config: { base_url: \"http://127.0.0.1:9\", path: \"/x\" } }\n  sink: { type: file, config: { path: \"${param.out}\", format: json_lines, if_exists: append } }\ntests:\n  suite:\n    auto: { defaults_baseline: true }\n";
    faucet_cli::templates::register(
        &srv.state.history(),
        faucet_cli::templates::store::RegisterRequest {
            id: Some("plain".into()),
            body: plain.into(),
            format: faucet_cli::serve::load::ConfigFormat::Yaml,
            description: None,
            tags: Vec::new(),
            launch: true,
            created_by: None,
        },
    )
    .await
    .unwrap();
    let wrong = service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec("wrong", json!([delivery("d", "plain", &out)])),
    )
    .await
    .unwrap_err();
    assert!(message(wrong).contains("must read the feed"));
    let missing = service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec("missing", json!([delivery("d", "no_such_template", &out)])),
    )
    .await;
    assert!(missing.is_err());
    // A path outside what a caller may reach is refused as the caller's.
    let outside = service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec(
            "outside",
            json!([delivery(
                "d",
                "orders_lake",
                Path::new("/etc/faucet-feed-out.jsonl")
            )]),
        ),
    )
    .await;
    assert!(outside.is_err());
    assert!(
        service::list(&srv.state, &admin(), None)
            .await
            .unwrap()
            .is_empty()
    );

    // Rules of the definition itself.
    let pipeline_file = service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec("p", json!([{"name": "d", "pipeline": "p.yaml"}])),
    )
    .await
    .unwrap_err();
    assert!(message(pipeline_file).contains("only for a delivery in the --feeds file"));
    let operator = AuthContext {
        role: Role::Operator,
        ..admin()
    };
    let mut other_store = feed_spec("s", json!([]));
    other_store.store = "big".into();
    assert!(matches!(
        service::create(&srv.state, &operator, None, other_store.clone()).await,
        Err(ServeError::Forbidden(_))
    ));
    assert!(
        message(
            service::create(&srv.state, &admin(), None, other_store)
                .await
                .unwrap_err()
        )
        .contains("is not defined on this server")
    );
    let mut tenant_feed = feed_spec("t", json!([]));
    tenant_feed.tenant = Some("acme".into());
    assert!(
        message(
            service::create(&srv.state, &admin(), None, tenant_feed)
                .await
                .unwrap_err()
        )
        .contains("under the tenant")
    );

    // A template that disappears is a refusal a retry cannot fix: it counts.
    service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec("orders", json!([delivery("lake", "orders_lake", &out)])),
    )
    .await
    .unwrap();
    srv.state
        .history()
        .template_delete("orders_lake", None)
        .await
        .unwrap();
    srv.append("orders", 1..=2).await;
    let refused = srv
        .until("orders", "lake", true, "a counted refusal", |p| {
            p.status.consecutive_failures >= 1
        })
        .await;
    assert_eq!(refused.cursor, 1);
    assert!(!refused.claimed, "the claimer released its own claim");
    assert_eq!(refused.status.last_run.as_ref().unwrap().outcome, "failed");
    assert!(refused.status.last_run.as_ref().unwrap().run_id.is_none());

    // An update may not rename the feed or move it to another store.
    let mut renamed = feed_spec("orders2", json!([]));
    renamed.description = None;
    assert!(
        message(
            service::update(&srv.state, &admin(), None, "orders", renamed)
                .await
                .unwrap_err()
        )
        .contains("cannot be renamed")
    );
    let mut moved = feed_spec("orders", json!([]));
    moved.store = "big".into();
    assert!(
        message(
            service::update(&srv.state, &admin(), None, "orders", moved)
                .await
                .unwrap_err()
        )
        .contains("another store")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_submit_and_the_status_write_is_recovered_by_the_claim_label() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let mut srv = Srv::start(root, &[]).await;
    register(&srv.state, "orders_lake", &root.join("state")).await;
    let out = root.join("lake.jsonl");
    service::create(
        &srv.state,
        &admin(),
        None,
        feed_spec("orders", json!([delivery("lake", "orders_lake", &out)])),
    )
    .await
    .unwrap();
    srv.append("orders", 1..=2).await;

    // The claim and the submit happened; the status write did not.
    let rt = srv.state.feeds();
    let found = service::find(&srv.state, None, "orders").await.unwrap();
    let dref = found.feed.delivery("lake").unwrap().clone();
    let (store, _) = srv.store("orders").await;
    let claim = store
        .claim(
            &store_calls(),
            &dref.uid,
            Seq(1),
            Seq(3),
            rt.owner(),
            runner::CLAIM_TTL,
        )
        .await
        .unwrap()
        .unwrap();
    let binding = FeedBinding::new(
        dref.uid.clone(),
        claim.epoch,
        claim.id.clone(),
        claim.from,
        claim.to,
        "default",
    );
    let run_id = runner::submit(
        &srv.state,
        &rt,
        &found.feed,
        &found.feed.spec.deliveries[0],
        &binding,
    )
    .await
    .unwrap();
    assert!(srv.pos("orders", "lake").await.status.claim.is_none());
    // The same claim submitted again is the same run.
    let replay = runner::submit(
        &srv.state,
        &rt,
        &found.feed,
        &found.feed.spec.deliveries[0],
        &binding,
    )
    .await
    .unwrap();
    assert_eq!(replay, run_id);

    let done = srv.delivered("orders", "lake", 3).await;
    let last = done.status.last_run.unwrap();
    assert_eq!(last.outcome, "delivered");
    assert_eq!(last.run_id.as_deref(), Some(run_id.as_str()));
    assert_eq!(last.claim, claim.id.as_str());
    assert_eq!(ids(&out), vec![1, 2]);

    // A claim nobody submitted a run for is left to its time limit, and a
    // drain's claim is never touched.
    srv.append("orders", 3..=3).await;
    let orphan = store
        .claim(
            &store_calls(),
            &dref.uid,
            Seq(3),
            Seq(4),
            rt.owner(),
            Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap();
    for _ in 0..3 {
        srv.tick().await;
    }
    let held = srv.pos("orders", "lake").await;
    assert!(held.claimed && held.cursor == 3, "{held:?}");
    assert_ne!(
        held.status.last_run.as_ref().unwrap().claim,
        orphan.id.as_str()
    );
    // Once it expires the range is claimed again and delivered.
    srv.delivered("orders", "lake", 4).await;
    assert_eq!(ids(&out), vec![1, 2, 3]);

    // Rolling back a delivery run pauses the delivery and says what a resume does.
    let rec = srv.state.history().get(&run_id).await.unwrap().unwrap();
    let bound = rec.feed.clone().unwrap();
    let note = service::pause_for_rollback(&srv.state, &admin(), &run_id, &bound)
        .await
        .unwrap();
    assert!(
        note.contains("delivery 'lake' of feed 'orders' is paused"),
        "{note}"
    );
    assert!(note.contains("replay the delivery from event 1"), "{note}");
    let paused = srv.pos("orders", "lake").await;
    assert!(
        paused
            .status
            .paused
            .as_ref()
            .unwrap()
            .reason
            .as_deref()
            .unwrap()
            .contains("rolled back")
    );
    let mut lost = bound.clone();
    lost.to = 9;
    let note = service::pause_for_rollback(&srv.state, &admin(), &run_id, &lost)
        .await
        .unwrap();
    assert!(
        note.contains("resuming the delivery redelivers these events"),
        "{note}"
    );
    let mut gone = bound.clone();
    gone.delivery = "nope".into();
    assert!(
        service::pause_for_rollback(&srv.state, &admin(), &run_id, &gone)
            .await
            .is_none()
    );
    let audit = srv
        .state
        .history()
        .list_audit(&faucet_cli::serve::history::AuditFilter {
            action: Some("feed.pause".into()),
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(!audit.is_empty(), "the pause is audited");
    srv.append("orders", 4..=4).await;
    for _ in 0..5 {
        srv.tick().await;
    }
    let still = srv.pos("orders", "lake").await;
    assert!(still.cursor == 4 && !still.claimed, "{still:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feeds_file_delivers_a_pipeline_file_and_is_read_only() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let out = root.join("lake.jsonl");
    std::fs::write(
        root.join("lake.yaml"),
        format!(
            "version: 1\nname: file_lake\npipeline:\n  source: {{ type: feed }}\n  sink: {{ type: file, config: {{ path: \"{}\", if_exists: append }} }}\n  state: {{ type: file, config: {{ path: \"{}\" }} }}\n",
            out.display(),
            root.join("state").display()
        ),
    )
    .unwrap();
    let feeds = root.join("feeds.yaml");
    std::fs::write(
        &feeds,
        "version: 1\nfeeds:\n  - name: orders\n    ingest: { auth: { signature: { header: X-Signature, secret: file-hmac-secret } } }\n    deliveries:\n      - name: lake\n        pipeline: lake.yaml\n        flush: { max_events: 1, min_interval: 0s }\n",
    )
    .unwrap();
    let feeds_arg = feeds.display().to_string();
    let history = format!("sqlite:{}", root.join("history.db").display());
    let cfg = config(root, &history, &["--feeds", &feeds_arg]);
    let mut srv = Srv::over(&cfg).await;
    let rt = srv.state.feeds();
    rt.lease_tick(&srv.state).await;
    assert!(
        rt.started(),
        "a feeds file with a feed starts the scheduler"
    );
    srv.tick().await;

    let listed = service::list(&srv.state, &admin(), None).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(serde_json::to_value(listed[0].source).unwrap(), "file");
    assert_eq!(
        listed[0]
            .spec
            .ingest
            .auth
            .signature
            .as_ref()
            .unwrap()
            .secret,
        "$sealed"
    );
    let snapshot = rt.snapshot();
    let feed = snapshot.feed(None, "orders").unwrap();
    assert_eq!(
        feed.spec.ingest.auth.signature.as_ref().unwrap().secret,
        "file-hmac-secret"
    );
    let uid = feed.uid.clone();

    srv.append("orders", 1..=2).await;
    srv.delivered("orders", "lake", 3).await;
    assert_eq!(ids(&out), vec![1, 2]);
    let state_files: Vec<String> = std::fs::read_dir(root.join("state"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    let short = feed.delivery("lake").unwrap().short_id();
    assert!(
        state_files
            .iter()
            .any(|f| f.contains(&format!("file_lake.{short}"))),
        "{state_files:?}"
    );

    // The file's feed is read-only, and its name is taken.
    for refusal in [
        service::update(
            &srv.state,
            &admin(),
            None,
            "orders",
            feed_spec("orders", json!([])),
        )
        .await
        .err(),
        service::delete(&srv.state, &admin(), None, "orders")
            .await
            .err(),
        service::create(&srv.state, &admin(), None, feed_spec("orders", json!([])))
            .await
            .err(),
    ] {
        let m = message(refusal.unwrap());
        assert!(m.contains("--feeds file"), "{m}");
    }
    assert!(
        message(
            service::token_create(&srv.state, &admin(), None, "orders", "ci", None)
                .await
                .unwrap_err()
        )
        .contains("does not use tokens")
    );
    // It can still be paused and resumed at run time.
    service::pause(&srv.state, &admin(), None, "orders", "lake", None)
        .await
        .unwrap();
    srv.append("orders", 3..=3).await;
    for _ in 0..5 {
        srv.tick().await;
    }
    assert_eq!(srv.pos("orders", "lake").await.cursor, 3);
    service::resume(&srv.state, &admin(), None, "orders", "lake")
        .await
        .unwrap();
    srv.delivered("orders", "lake", 4).await;
    // A restart with a changed file keeps the feed's uid and position; a
    // delivery the file no longer lists is removed from the store. (The
    // store handle is carried over: one process holds its lock.)
    let (store, _) = srv.store("orders").await;
    std::fs::write(
        &feeds,
        "version: 1\nfeeds:\n  - name: orders\n    ingest: { auth: { tokens: managed } }\n",
    )
    .unwrap();
    let reloaded = spec::load_file(&feeds).await.unwrap();
    let next = stores::Stores::from_config(&cfg);
    next.install("default", store.clone()).await;
    srv.state
        .set_feeds(FeedsRuntime::new(cfg.feeds.clone(), reloaded, next));
    srv.tick().await;
    let (_, same) = srv.store("orders").await;
    assert_eq!(same, uid);
    let ids_doc = srv
        .state
        .history()
        .meta_get("feed_uid:-/orders")
        .await
        .unwrap()
        .unwrap();
    assert!(
        ids_doc.contains(uid.as_str()) && !ids_doc.contains("lake"),
        "{ids_doc}"
    );
    let head = store
        .heads(&store_calls(), std::slice::from_ref(&same))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(head.watermark, Seq(3));
    assert!(head.deliveries.is_empty(), "the dropped delivery is gone");
    let (_, text) = service::token_create(&srv.state, &admin(), None, "orders", "ci", None)
        .await
        .unwrap();
    assert!(
        srv.state
            .feeds()
            .snapshot()
            .token(&service::token_hash(&text))
            .is_some()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tenants_feeds_are_scoped_limited_sealed_and_purged_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let srv = Srv::start(root, &[]).await;
    let history = srv.state.history();
    let now = chrono::Utc::now();
    for (id, limits) in [
        ("acme", json!({"max_feeds": 1, "max_feed_bytes": 4096})),
        ("globex", json!({})),
    ] {
        let rec = serde_json::from_value(json!({
            "id": id, "limits": limits, "created_at": now, "updated_at": now, "created_by": "alice"
        }))
        .unwrap();
        history.tenant_upsert(&rec).await.unwrap();
    }
    let acme = AuthContext {
        principal: "acme-app".into(),
        role: Role::Operator,
        source_ip: None,
        tenant: Some("acme".into()),
    };
    let signed: FeedSpec = serde_json::from_value(json!({
        "name": "orders",
        "ingest": {"auth": {"tokens": "managed", "signature": {"header": "X-Signature", "secret": "tenant-hmac"}}}
    }))
    .unwrap();
    let made = service::create(&srv.state, &acme, None, signed)
        .await
        .unwrap();
    assert_eq!(made.tenant.as_deref(), Some("acme"));
    assert_eq!(
        made.spec.ingest.auth.signature.as_ref().unwrap().secret,
        "$sealed"
    );
    // Stored sealed; opened only in the server's own snapshot.
    let stored = history.feed_get("acme/orders").await.unwrap().unwrap();
    let secret = &stored.spec.ingest.auth.signature.as_ref().unwrap().secret;
    assert!(secret.starts_with("$sealed:") && !secret.contains("tenant-hmac"));
    let snapshot = srv.state.feeds().snapshot();
    let feed = snapshot.feed(Some("acme"), "orders").unwrap();
    assert_eq!(
        feed.spec.ingest.auth.signature.as_ref().unwrap().secret,
        "tenant-hmac"
    );
    assert!(
        feed.spec.sealed(),
        "a tenant feed seals payloads by default"
    );
    assert_eq!(feed.label(), "acme/orders");

    // The tenant's quota, and its walls.
    let second = service::create(&srv.state, &acme, None, feed_spec("more", json!([]))).await;
    assert!(message(second.unwrap_err()).contains("max_feeds: 1"));
    assert!(matches!(
        service::get(&srv.state, &acme, Some("globex"), "orders").await,
        Err(ServeError::NotFound)
    ));
    assert_eq!(
        service::list(&srv.state, &acme, None).await.unwrap().len(),
        1
    );
    assert!(
        service::list(&srv.state, &admin(), None)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        service::list(&srv.state, &admin(), Some("acme"))
            .await
            .unwrap()
            .len(),
        1
    );
    let mut wrong = feed_spec("x", json!([]));
    wrong.tenant = Some("globex".into());
    assert!(matches!(
        service::create(&srv.state, &acme, None, wrong).await,
        Err(ServeError::Forbidden(_))
    ));
    let ghost = service::create(
        &srv.state,
        &admin(),
        Some("nobody"),
        feed_spec("x", json!([])),
    )
    .await;
    assert!(message(ghost.unwrap_err()).contains("does not exist"));

    // The stored-bytes quota is checked by the store when events arrive.
    let store = srv.state.feeds().store_for(&feed).await.unwrap();
    let big = NewEvent::new("application/json", vec![b'x'; 3000]);
    store
        .append(&store_calls(), &feed.uid, vec![big.clone()])
        .await
        .unwrap();
    let full = store
        .append(&store_calls(), &feed.uid, vec![big])
        .await
        .unwrap_err();
    assert!(full.to_string().contains("is full"), "{full}");
    let (_, token) = service::token_create(&srv.state, &acme, None, "orders", "ci", None)
        .await
        .unwrap();

    // The signature secret is kept across an edit that sends the marker back.
    let mut edit = made.spec.clone();
    edit.description = Some("edited".into());
    let edited = service::update(&srv.state, &acme, None, "orders", edit)
        .await
        .unwrap();
    assert_eq!(edited.revision, 2);
    let kept = history.feed_get("acme/orders").await.unwrap().unwrap();
    assert_eq!(
        &kept.spec.ingest.auth.signature.as_ref().unwrap().secret,
        secret
    );

    // Deleting the tenant purges its feeds: events, tokens, definitions.
    let report = faucet_cli::serve::tenants::delete_tenant(&srv.state, "acme")
        .await
        .unwrap();
    assert_eq!(report.feeds, 1);
    assert!(
        store
            .heads(&store_calls(), std::slice::from_ref(&feed.uid))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(history.feed_get("acme/orders").await.unwrap().is_none());
    assert!(history.feed_token_list(None).await.unwrap().is_empty());
    srv.state.feeds().refresh(&srv.state).await.unwrap();
    assert!(
        srv.state
            .feeds()
            .snapshot()
            .token(&service::token_hash(&token))
            .is_none()
    );
    assert_eq!(
        service::purge_tenant(&srv.state, "globex").await.unwrap(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_feed_definition_and_its_tokens_round_trip_through_sqlite() {
    use faucet_cli::serve::history::feeds::{FeedRecord, FeedTokenRecord};
    let dir = tempfile::tempdir().unwrap();
    let srv = Srv::start(dir.path(), &[]).await;
    let h = srv.state.history();
    let now = chrono::Utc::now();
    assert_eq!(h.feed_count().await.unwrap(), 0);
    let rec = FeedRecord::new(
        feed_spec("orders", json!([{"name": "lake", "template": {"id": "t"}}])),
        "alice",
        now,
    );
    assert!(h.feed_insert(&rec).await.unwrap());
    assert!(!h.feed_insert(&rec).await.unwrap(), "the key is taken");
    let mut tenant = feed_spec("orders", json!([]));
    tenant.tenant = Some("acme".into());
    let other = FeedRecord::new(tenant, "bob", now);
    assert!(
        h.feed_insert(&other).await.unwrap(),
        "a tenant's feed has its own key"
    );
    assert_eq!(h.feed_count().await.unwrap(), 2);
    assert_eq!(
        h.feed_list()
            .await
            .unwrap()
            .iter()
            .map(|r| r.key())
            .collect::<Vec<_>>(),
        ["-/orders", "acme/orders"]
    );

    let mut spec = rec.spec.clone();
    spec.description = Some("d".into());
    let (next, _) = rec.revised(spec, "alice", now, "updated", vec![]);
    assert!(h.feed_replace(&rec, &next).await.unwrap());
    assert!(
        !h.feed_replace(&rec, &next).await.unwrap(),
        "the stored record moved on"
    );
    let moved_key = FeedRecord::new(feed_spec("elsewhere", json!([])), "alice", now);
    assert!(!h.feed_replace(&next, &moved_key).await.unwrap());
    let got = h.feed_get("-/orders").await.unwrap().unwrap();
    assert_eq!(
        (got.revision, got.spec.description.as_deref()),
        (2, Some("d"))
    );
    assert_eq!(got.deliveries["lake"].uid, rec.deliveries["lake"].uid);
    assert!(h.feed_get("-/nope").await.unwrap().is_none());

    let token = |id: &str, feed: &str| FeedTokenRecord {
        id: id.into(),
        feed: feed.into(),
        name: "ci".into(),
        hash: service::token_hash(id),
        prefix: "fdt_".into(),
        created_at: now,
        created_by: "alice".into(),
        expires_at: None,
        revoked_at: None,
        revoked_by: None,
        last_used_at: None,
    };
    let (a, b) = (token("t1", &rec.uid), token("t2", &other.uid));
    assert!(h.feed_token_insert(&a).await.unwrap());
    assert!(!h.feed_token_insert(&a).await.unwrap());
    assert!(h.feed_token_insert(&b).await.unwrap());
    assert_eq!(h.feed_token_list(None).await.unwrap().len(), 2);
    assert_eq!(
        h.feed_token_list(Some(&rec.uid)).await.unwrap(),
        vec![a.clone()]
    );
    let mut used = a.clone();
    used.last_used_at = Some(now);
    assert!(h.feed_token_replace(&a, &used).await.unwrap());
    assert!(!h.feed_token_replace(&a, &used).await.unwrap());
    assert!(
        !h.feed_token_replace(&used, &b).await.unwrap(),
        "a token keeps its id"
    );

    // Deleting a feed takes its tokens; deleting a tenant takes its feeds.
    assert!(h.feed_delete("-/orders").await.unwrap());
    assert!(!h.feed_delete("-/orders").await.unwrap());
    assert_eq!(h.feed_token_list(None).await.unwrap(), vec![b]);
    h.tenant_delete("acme").await.unwrap();
    assert_eq!(h.feed_count().await.unwrap(), 0);
    assert!(h.feed_token_list(None).await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_servers_on_one_postgres_deliver_each_range_once() {
    use testcontainers::runners::AsyncRunner;
    let pg = testcontainers_modules::postgres::Postgres::default()
        .start()
        .await
        .expect("a PostgreSQL container (Docker)");
    let port = pg.get_host_port_ipv4(5432).await.unwrap();
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let cfg = config(root, &url, &[]);
    let mut a = Srv::over(&cfg).await;
    let mut b = Srv::over(&cfg).await;
    register(&a.state, "orders_lake", &root.join("state")).await;
    let out = root.join("lake.jsonl");
    let mut d = delivery("lake", "orders_lake", &out);
    d["max_batch"] = json!({"events": 2});
    service::create(&a.state, &admin(), None, feed_spec("orders", json!([d])))
        .await
        .unwrap();

    // The second server learns of the feed from the shared history, and
    // each server reads the store through its own handle.
    assert!(!b.state.feeds().started());
    b.state.feeds().lease_tick(&b.state).await;
    assert!(b.state.feeds().started());
    b.tick().await;
    let (store_a, uid) = a.store("orders").await;
    let (store_b, _) = b.store("orders").await;
    assert!(!Arc::ptr_eq(&store_a, &store_b));
    assert_eq!(store_a.capabilities().backend, "postgres");
    assert_ne!(a.state.feeds().owner(), b.state.feeds().owner());

    a.append("orders", 1..=7).await;
    for _ in 0..600 {
        a.tick().await;
        b.tick().await;
        let p = a.pos("orders", "lake").await;
        if p.cursor == 8 && !p.claimed && p.status.claim.is_none() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
    assert_eq!(
        ids(&out),
        vec![1, 2, 3, 4, 5, 6, 7],
        "every event once, in order"
    );

    // One run per claimed range, whichever server claimed it.
    let runs = a
        .state
        .history()
        .list(&faucet_cli::serve::history::ListFilter {
            labels: vec![faucet_cli::serve::history::labels::LabelFilter {
                key: "feed".into(),
                value: Some("orders".into()),
            }],
            limit: 100,
            ..Default::default()
        })
        .await
        .unwrap()
        .runs;
    let mut ranges: Vec<(u64, u64)> = runs
        .iter()
        .map(|r| {
            let f = r.feed.as_ref().unwrap();
            (f.from, f.to)
        })
        .collect();
    ranges.sort();
    assert_eq!(ranges, vec![(1, 3), (3, 5), (5, 7), (7, 8)]);
    let head = store_b
        .heads(&store_calls(), std::slice::from_ref(&uid))
        .await
        .unwrap()
        .remove(0);
    assert_eq!(head.deliveries[0].cursor, Seq(8));
}

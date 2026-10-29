
use super::*;

#[test]
fn money_digits_read_the_rendering_or_refuse_it() {
    assert_eq!(money_digits_of("$1.00").unwrap(), 2);
    assert_eq!(money_digits_of("￥1").unwrap(), 0);
    let e = money_digits_of("$2.50").unwrap_err().to_string();
    assert!(
        e.contains("cannot read the money format") && e.contains("$2.50"),
        "{e}"
    );
}

struct FakeSource {
    reads: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    size: usize,
}

impl EventSource for FakeSource {
    fn update_applied(&mut self, _lsn: u64) {}

    async fn recv(&mut self) -> Result<Option<ReplicationEvent>, FaucetError> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Some(ReplicationEvent::XLogData {
            wal_start: Lsn::from_u64(1),
            wal_end: Lsn::from_u64(1),
            server_time_micros: 0,
            data: vec![0u8; self.size].into(),
        }))
    }
}

async fn settle() {
    for _ in 0..50 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
}

#[tokio::test]
async fn read_ahead_pauses_at_the_byte_budget() {
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut duplex = Duplex::spawn_source(
        FakeSource {
            reads: reads.clone(),
            size: 100,
        },
        ReadAhead::new(1_000, 250),
    );
    settle().await;
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert!(matches!(
        duplex.next_event().await,
        Ok(Some(ReplicationEvent::XLogData { .. }))
    ));
    settle().await;
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 4);
}

#[tokio::test]
async fn read_ahead_pauses_at_the_event_budget_and_clamps_oversized_events() {
    let reads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut duplex = Duplex::spawn_source(
        FakeSource {
            reads: reads.clone(),
            size: 10_000,
        },
        ReadAhead::new(2, 100),
    );
    settle().await;
    assert_eq!(reads.load(std::sync::atomic::Ordering::SeqCst), 2);
    duplex.next_event().await.unwrap();
    duplex.next_event().await.unwrap();
    settle().await;
    assert!(reads.load(std::sync::atomic::Ordering::SeqCst) >= 3);
}
use crate::config::CdcTls;
use chrono::{TimeZone, Utc};
use pgwire_replication::SslMode;

// ─── SQLSTATE preservation (#654 H8) ────────────────────────────────────

/// `pg_err` is the whole point of the H8 fix: the SQLSTATE must survive the
/// hop into `FaucetError`, because once the error is stringified the code
/// cannot be recovered. A non-database `sqlx::Error` has no code, and must
/// therefore carry `None` rather than a guess.
#[test]
fn an_active_slot_reports_the_retryable_sqlstate() {
    let err = slot_activity_result("s", Some((true, Some(7)))).unwrap_err();
    assert!(is_slot_active_error(&err));
    assert!(err.to_string().contains("PID 7"), "{err}");
    let err = slot_activity_result("s", Some((true, None))).unwrap_err();
    assert!(err.to_string().contains("PID ?"), "{err}");
    assert!(slot_activity_result("s", Some((false, None))).is_ok());
    assert!(slot_activity_result("s", None).is_ok());
}

#[test]
fn advance_skips_positions_at_or_behind_the_slot() {
    assert_eq!(advance_decision(10, Some(10)), AdvanceDecision::AtSlot);
    assert_eq!(
        advance_decision(5, Some(10)),
        AdvanceDecision::BehindSlot(10)
    );
    assert_eq!(advance_decision(15, Some(10)), AdvanceDecision::Advance);
    assert_eq!(advance_decision(15, None), AdvanceDecision::Advance);
}

#[test]
fn pgwire_server_errors_keep_their_sqlstate() {
    let err = pgwire_err(
        "postgres-cdc start_replication",
        pgwire_replication::PgWireError::Server(
            "replication slot \"s\" is active for PID 7 (SQLSTATE 55006)".into(),
        ),
    );
    assert!(is_slot_active_error(&err));
    let other = pgwire_err(
        "x",
        pgwire_replication::PgWireError::Server("boom (SQLSTATE 42P01)".into()),
    );
    assert!(!is_slot_active_error(&other));
    let io = pgwire_err("x", pgwire_replication::PgWireError::Protocol("eof".into()));
    assert_eq!(sqlstate_of(&io), None);
    assert_eq!(sqlstate_suffix("no code here"), None);
    assert_eq!(sqlstate_suffix("bad (SQLSTATE 55)"), None);
}

#[test]
fn pg_err_preserves_context_and_message_without_a_sqlstate() {
    let err = pg_err("postgres-cdc slot lookup", sqlx::Error::PoolTimedOut);
    let FaucetError::Custom(inner) = &err else {
        panic!("pg_err must produce FaucetError::Custom, got {err:?}");
    };
    let pg = inner
        .downcast_ref::<PostgresError>()
        .expect("the boxed error must still be a PostgresError");
    assert_eq!(pg.context, "postgres-cdc slot lookup");
    assert_eq!(pg.message, sqlx::Error::PoolTimedOut.to_string());
    assert_eq!(
        pg.sqlstate, None,
        "a pool timeout is not a database-reported failure, so it has no SQLSTATE"
    );
    // The rendered form still names both halves, so a log line is useful.
    let rendered = err.to_string();
    assert!(
        rendered.contains("postgres-cdc slot lookup"),
        "context must survive into the display form: {rendered}"
    );
}

/// `sqlstate_of` reads the code back only from a `PostgresError`; every
/// other error — including one whose *text* contains a SQLSTATE — yields
/// `None`, which is what stops `is_slot_active_error` from ever being
/// fooled by prose.
#[test]
fn sqlstate_of_reads_only_the_typed_carrier() {
    let carried = PostgresError::new(
        Some(SQLSTATE_OBJECT_IN_USE.to_string()),
        "acquire slot",
        "replication slot \"s\" is active for PID 42",
    );
    let err = FaucetError::Custom(Box::new(carried));
    assert_eq!(sqlstate_of(&err), Some(SQLSTATE_OBJECT_IN_USE));
    assert!(
        is_slot_active_error(&err),
        "the typed 55006 carrier is what slot-active detection keys off"
    );

    // A PostgresError with no code.
    let codeless = FaucetError::Custom(Box::new(PostgresError::new(
        None,
        "acquire slot",
        "replication slot is active for PID 42",
    )));
    assert_eq!(sqlstate_of(&codeless), None);
    assert!(!is_slot_active_error(&codeless));

    // Not a PostgresError at all, but the message reads exactly like one.
    // Before the fix this class of message drove the retry decision.
    let lookalike =
        FaucetError::Source("replication slot \"s\" is active for PID 42 (SQLSTATE 55006)".into());
    assert_eq!(sqlstate_of(&lookalike), None);
    assert!(
        !is_slot_active_error(&lookalike),
        "prose must never be mistaken for a SQLSTATE"
    );

    // A different real SQLSTATE must not match slot-active.
    let other_code = FaucetError::Custom(Box::new(PostgresError::new(
        Some("42P01".to_string()),
        "acquire slot",
        "relation does not exist",
    )));
    assert_eq!(sqlstate_of(&other_code), Some("42P01"));
    assert!(!is_slot_active_error(&other_code));
}

#[test]
fn apply_cdc_tls_sets_ssl_mode_on_control_plane_options() {
    // #321 M5: the control-plane PgConnectOptions must carry the configured
    // SSL mode, not sqlx's default `Prefer`. Assert via the Debug view.
    let base: PgConnectOptions = "postgres://u:p@h:5432/db".parse().unwrap();
    let dbg = |tls: &CdcTls| format!("{:?}", apply_cdc_tls(base.clone(), tls));
    assert!(
        dbg(&CdcTls::Disable).contains("Disable"),
        "{}",
        dbg(&CdcTls::Disable)
    );
    assert!(dbg(&CdcTls::Require).contains("Require"));
    assert!(dbg(&CdcTls::VerifyCa { ca_path: None }).contains("VerifyCa"));
    assert!(
        dbg(&CdcTls::VerifyFull {
            ca_path: Some("/ca.pem".into())
        })
        .contains("VerifyFull")
    );
}

#[test]
fn tls_config_maps_each_mode() {
    assert_eq!(tls_config(&CdcTls::Disable).mode, SslMode::Disable);
    assert_eq!(tls_config(&CdcTls::Require).mode, SslMode::Require);
    assert_eq!(tls_config(&CdcTls::Prefer).mode, SslMode::Prefer);
    assert_eq!(
        tls_config(&CdcTls::VerifyCa { ca_path: None }).mode,
        SslMode::VerifyCa
    );
    assert_eq!(
        tls_config(&CdcTls::VerifyFull {
            ca_path: Some("/ca.pem".into())
        })
        .mode,
        SslMode::VerifyFull
    );
}

/// Convenience: turn `postgres_clock_to_unix_ms`-compatible math into a
/// `DateTime<Utc>`.  Used by tests only.
fn postgres_clock_to_datetime(ts: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_micros(POSTGRES_EPOCH_MICROS.saturating_add(ts))
        .single()
        .unwrap_or_else(Utc::now)
}

#[test]
fn postgres_clock_round_trip() {
    let dt = Utc.with_ymd_and_hms(2026, 5, 17, 12, 0, 0).unwrap();
    let pg_ts = dt.timestamp_micros() - POSTGRES_EPOCH_MICROS;
    let back = postgres_clock_to_datetime(pg_ts);
    assert_eq!(back, dt);
}

#[test]
fn unix_ms_conversion() {
    let dt = Utc.with_ymd_and_hms(2026, 5, 17, 12, 0, 0).unwrap();
    let pg_ts = dt.timestamp_micros() - POSTGRES_EPOCH_MICROS;
    assert_eq!(postgres_clock_to_unix_ms(pg_ts), 1_779_019_200_000);
}

#[test]
fn quote_slot_simple() {
    assert_eq!(quote_slot("faucet_slot"), "\"faucet_slot\"");
}

#[test]
fn escape_simple_doubles_quotes() {
    assert_eq!(escape_simple("foo'bar"), "foo''bar");
}

#[test]
fn replication_config_starts_and_acknowledges_at_the_resume_lsn() {
    let tls = crate::config::CdcTls::Require;
    let params = ReplicationParams {
        connection_url: "postgres://alice:secret@db.example.com:5544/analytics",
        slot_name: "slot_a",
        publication_name: "pub_a",
        proto_version: 1,
        create_slot_if_missing: false,
        start_lsn: Some(0x16B_3748),
        status_update_interval: Duration::from_secs(3),
        tcp_keepalive: Duration::from_secs(30),
        slot_type: crate::config::SlotType::Permanent,
        tls: &tls,
        max_buffered_events: 10,
        max_buffered_bytes: 1024,
    };
    let cfg = replication_config(parse_url(params.connection_url).unwrap(), &params);
    assert_eq!(cfg.host, "db.example.com");
    assert_eq!(cfg.port, 5544);
    assert_eq!(cfg.user, "alice");
    assert_eq!(cfg.password, "secret");
    assert_eq!(cfg.database, "analytics");
    assert_eq!(cfg.slot, "slot_a");
    assert_eq!(cfg.start_lsn, Lsn::from_u64(0x16B_3748));
    assert_eq!(
        cfg.ack_lsn, None,
        "the acknowledged LSN starts at start_lsn"
    );
    assert_eq!(cfg.stop_at_lsn, None);
    assert_eq!(cfg.status_interval, Duration::from_secs(3));
    assert_eq!(cfg.idle_wakeup_interval, Duration::from_secs(3));
    assert_eq!(cfg.buffer_events, 8192);
    assert!(!cfg.binary);
    assert_eq!(cfg.tls.mode, pgwire_replication::SslMode::Require);

    let from_slot = ReplicationParams {
        start_lsn: None,
        ..params
    };
    let cfg = replication_config(parse_url(from_slot.connection_url).unwrap(), &from_slot);
    assert_eq!(cfg.start_lsn, Lsn::from_u64(0));
}

#[test]
fn parse_url_extracts_all_components() {
    let c = parse_url("postgres://alice:secret@db.example.com:5544/analytics").unwrap();
    assert_eq!(c.host, "db.example.com");
    assert_eq!(c.port, 5544);
    assert_eq!(c.user, "alice");
    assert_eq!(c.password, "secret");
    assert_eq!(c.dbname, "analytics");
}

#[test]
fn parse_url_percent_decodes_and_honours_query_overrides() {
    let c = parse_url("postgres://al%40ice:p%40ss%2Fw@db:5432/my%20db").unwrap();
    assert_eq!(c.user, "al@ice");
    assert_eq!(c.password, "p@ss/w");
    assert_eq!(c.dbname, "my db");
    let c =
        parse_url("postgres://db/x?user=bob&password=s%26t&dbname=other&sslmode=require").unwrap();
    assert_eq!((c.user.as_str(), c.password.as_str()), ("bob", "s&t"));
    assert_eq!(c.dbname, "other");
    assert!(
        parse_url("postgres://u:%FF@db/x").is_err(),
        "invalid UTF-8 is refused"
    );
}

#[test]
fn parse_url_defaults_port_and_dbname() {
    let c = parse_url("postgres://alice@db.example.com").unwrap();
    assert_eq!(c.port, 5432);
    assert_eq!(c.dbname, "postgres");
    assert_eq!(c.password, "");
}

#[test]
fn parse_url_rejects_missing_host() {
    // `postgres:///db` parses with an empty host — must fail fast (#78/#47).
    let err = parse_url("postgres:///analytics").unwrap_err();
    assert!(format!("{err}").contains("missing a host"), "{err}");
}

#[test]
fn parse_url_rejects_missing_user() {
    let err = parse_url("postgres://db.example.com/analytics").unwrap_err();
    assert!(format!("{err}").contains("missing a user"), "{err}");
}

#[test]
fn slot_active_is_classified_by_sqlstate_not_message_text() {
    // SQLSTATE 55006 is the API. The message is not: it is localised and
    // it embeds the slot name, so a slot literally named
    // `orders_is_active` used to match the old substring rule, and a
    // server/driver reword silently removed the restart-race backoff.
    assert!(is_slot_active_error(&slot_in_use()));

    // Same prose, no SQLSTATE → not classified (text alone proves nothing).
    assert!(!is_slot_active_error(&FaucetError::Source(
        "replication slot \"orders_is_active\" is active for PID 1".into()
    )));

    // A different SQLSTATE is not the slot-in-use condition.
    assert!(!is_slot_active_error(&FaucetError::Custom(Box::new(
        PostgresError::new(
            Some("42P01".to_string()),
            "postgres-cdc slot lookup",
            "relation does not exist",
        )
    ))));

    // Unrelated error kinds are untouched.
    assert!(!is_slot_active_error(&FaucetError::Config(
        "bad url".into()
    )));
}

#[test]
fn slot_acquire_backoff_grows_and_is_capped() {
    assert_eq!(slot_acquire_backoff(0), Duration::from_millis(250));
    assert_eq!(slot_acquire_backoff(1), Duration::from_millis(500));
    assert_eq!(slot_acquire_backoff(2), Duration::from_millis(1000));
    // Capped at 4s no matter how large the attempt.
    assert_eq!(slot_acquire_backoff(20), Duration::from_millis(4000));
    assert_eq!(slot_acquire_backoff(64), Duration::from_millis(4000));
}

/// The typed slot-in-use error, as the sqlx boundary now produces it.
fn slot_in_use() -> FaucetError {
    FaucetError::Custom(Box::new(PostgresError::new(
        Some(SQLSTATE_OBJECT_IN_USE.to_string()),
        "postgres-cdc create slot",
        "replication slot \"s\" is active for PID 1",
    )))
}

#[tokio::test]
async fn retry_on_slot_active_retries_then_succeeds() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result = retry_on_slot_active(5, || {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        async move {
            if n < 2 {
                Err(slot_in_use())
            } else {
                Ok::<u32, FaucetError>(42)
            }
        }
    })
    .await;
    assert_eq!(result.unwrap(), 42);
    assert_eq!(calls.load(Ordering::SeqCst), 3, "2 failures + 1 success");
}

#[tokio::test]
async fn retry_on_slot_active_gives_up_after_max_retries() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result: Result<(), _> = retry_on_slot_active(2, || {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Err(slot_in_use()) }
    })
    .await;
    assert!(result.is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "initial attempt + 2 retries"
    );
}

#[tokio::test]
async fn retry_on_slot_active_in_stops_waiting_on_cancel() {
    let token = faucet_core::CancellationToken::new();
    let ctx = faucet_core::CallContext::new()
        .without_timeout()
        .with_cancel(token.clone());
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
    };
    let started = std::time::Instant::now();
    let (result, ()) = futures::future::join(
        retry_on_slot_active_in(&ctx, "slot", 10, || async { Err::<(), _>(slot_in_use()) }),
        cancel,
    )
    .await;
    assert!(
        matches!(result, Err(FaucetError::Cancelled { .. })),
        "{result:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[tokio::test]
async fn retry_on_slot_active_does_not_retry_unrelated_errors() {
    use std::sync::atomic::{AtomicU32, Ordering};
    let calls = AtomicU32::new(0);
    let result: Result<(), _> = retry_on_slot_active(5, || {
        calls.fetch_add(1, Ordering::SeqCst);
        async { Err(FaucetError::Source("connection refused".into())) }
    })
    .await;
    assert!(result.is_err());
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "a non-slot-active error must not be retried"
    );
}

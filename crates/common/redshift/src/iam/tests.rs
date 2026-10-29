
use super::*;

fn conn(config: serde_json::Value) -> RedshiftConnection {
    serde_json::from_value(serde_json::json!({
        "host": "h", "database": "dev", "user": "fallback",
        "credentials": {"type": "iam", "config": config}
    }))
    .unwrap()
}

#[test]
fn a_cluster_takes_db_user_or_the_connection_user() {
    let p = IamCredentials::from_connection(&conn(serde_json::json!({"cluster_identifier": "c"})))
        .unwrap()
        .unwrap();
    assert!(matches!(&p.target, Target::Cluster { db_user, .. } if db_user == "fallback"));
    assert!(format!("{p:?}").contains("fallback"));
    let p = IamCredentials::from_connection(&conn(
            serde_json::json!({"cluster_identifier": "c", "db_user": "analyst", "auto_create": true, "db_groups": ["g"]}),
        ))
        .unwrap()
        .unwrap();
    assert!(
        matches!(&p.target, Target::Cluster { db_user, auto_create: true, db_groups, .. } if db_user == "analyst" && db_groups == &["g".to_string()])
    );
    let p = IamCredentials::from_connection(&conn(serde_json::json!({"workgroup_name": "wg"})))
        .unwrap()
        .unwrap();
    assert!(matches!(&p.target, Target::Workgroup { name } if name == "wg"));
    assert!(
        IamCredentials::from_connection(&RedshiftConnection::new("h", "d", "u", "p"))
            .unwrap()
            .is_none()
    );
}

#[test]
fn invalid_iam_settings_are_refused() {
    let mut no_user = conn(serde_json::json!({"cluster_identifier": "c"}));
    no_user.user = String::new();
    for (c, needle) in [
        (no_user, "db_user"),
        (conn(serde_json::json!({})), "exactly one"),
        (
            conn(serde_json::json!({"cluster_identifier": "c", "workgroup_name": "w"})),
            "exactly one",
        ),
        (
            conn(serde_json::json!({"workgroup_name": "w", "db_user": "u"})),
            "serverless",
        ),
        (
            conn(serde_json::json!({"cluster_identifier": "c", "duration_secs": 60})),
            "duration_secs",
        ),
    ] {
        let e = IamCredentials::from_connection(&c).unwrap_err().to_string();
        assert!(e.contains(needle), "{e}");
    }
}

#[test]
fn refresh_comes_a_margin_before_expiry_or_halfway() {
    let now = SystemTime::now();
    let fetched = Instant::now();
    assert_eq!(
        refresh_at(fetched, now, now + Duration::from_secs(900)),
        fetched + Duration::from_secs(840)
    );
    assert_eq!(
        refresh_at(fetched, now, now + Duration::from_secs(10)),
        fetched + Duration::from_secs(5)
    );
    assert_eq!(
        refresh_at(fetched, now, now - Duration::from_secs(1)),
        fetched + Duration::from_secs(1),
        "already expired by the local clock: not refetched on every use"
    );
    assert_eq!(
        refresh_at(fetched, now, now + Duration::from_secs(1)),
        fetched + Duration::from_secs(1)
    );
}

#[test]
fn a_failed_refresh_backs_off_within_the_remaining_lifetime() {
    let failed = Instant::now();
    let now = SystemTime::now();
    let at = |secs: u64| retry_at(failed, now, now + Duration::from_secs(secs)) - failed;
    assert_eq!(at(60), Duration::from_secs(15));
    assert_eq!(at(3600), Duration::from_secs(60));
    assert_eq!(at(2), Duration::from_secs(1));
    assert_eq!(
        retry_at(failed, now, now + Duration::from_millis(400)) - failed,
        Duration::from_millis(400)
    );
    assert_eq!(retry_at(failed, now, now - Duration::from_secs(1)), failed);
}

#[test]
fn a_response_needs_every_field() {
    let exp = aws_sdk_redshift::primitives::DateTime::from_secs(2_000_000_000);
    let ok = credentials_from(Some("IAM:u".into()), Some("pw".into()), Some(exp)).unwrap();
    assert_eq!(ok.user, "IAM:u");
    assert!(!format!("{ok:?}").contains("pw\""));
    for (u, p, e, needle) in [
        (None, Some("p"), Some(exp), "user"),
        (Some("u"), Some(""), Some(exp), "password"),
        (Some("u"), Some("p"), None, "expiration"),
    ] {
        let err = credentials_from(u.map(Into::into), p.map(Into::into), e)
            .unwrap_err()
            .to_string();
        assert!(err.contains(needle), "{err}");
    }
}

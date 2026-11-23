//! How the server words a lost launch race and a schedule whose pinned
//! template version is gone.
#![cfg(feature = "serve")]

use faucet_cli::serve::error::ServeError;
use faucet_cli::serve::history::HistoryError;

#[test]
fn a_lost_launch_race_is_a_conflict() {
    let e = ServeError::from(HistoryError::LaunchRaced {
        id: "orders".into(),
        expected: Some(2),
        found: Some(3),
    });
    match e {
        ServeError::Conflict(m) => assert!(m.contains("orders"), "{m}"),
        other => panic!("expected a conflict, got {other:?}"),
    }
}

#[cfg(all(feature = "schedule", feature = "templates"))]
#[test]
fn a_missing_pinned_template_names_the_pin() {
    use faucet_cli::serve::schedules::fire::missing_template;
    let pinned =
        serde_json::from_value(serde_json::json!({"id": "orders", "version": "4"})).unwrap();
    let text = missing_template(&pinned);
    assert!(text.contains("v4 of template 'orders'"), "{text}");
    let floating = serde_json::from_value(serde_json::json!({"id": "orders"})).unwrap();
    assert_eq!(missing_template(&floating), "the template was not found");
}

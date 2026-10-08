//! `faucet_build_info` reports the application's version once it sets one
//! (#832). Its own test binary: the version is a process-global first-wins.

use metrics_util::debugging::{DebugValue, DebuggingRecorder};

fn reported_versions(recorder: &DebuggingRecorder, f: impl FnOnce()) -> Vec<String> {
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(recorder, f);
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(key, _, _, v)| {
            key.key().name() == "faucet_build_info"
                && matches!(v, DebugValue::Gauge(g) if (g.into_inner() - 1.0).abs() < f64::EPSILON)
        })
        .flat_map(|(key, _, _, _)| {
            key.key()
                .labels()
                .filter(|l| l.key() == "version")
                .map(|l| l.value().to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

#[test]
fn the_application_version_replaces_cores_and_the_first_one_wins() {
    assert_eq!(faucet_core::build_version(), env!("CARGO_PKG_VERSION"));
    let before = DebuggingRecorder::new();
    assert_eq!(
        reported_versions(&before, faucet_core::register_build_info),
        vec![env!("CARGO_PKG_VERSION").to_string()]
    );

    // Setting the version re-registers the gauge under the current recorder.
    let on_set = DebuggingRecorder::new();
    let mut first = false;
    assert_eq!(
        reported_versions(&on_set, || first = faucet_core::set_build_version("98.7.6")),
        vec!["98.7.6".to_string()]
    );
    assert!(first);
    assert_eq!(faucet_core::build_version(), "98.7.6");

    assert!(!faucet_core::set_build_version("1.2.3"), "first call wins");
    let after = DebuggingRecorder::new();
    assert_eq!(
        reported_versions(&after, faucet_core::register_build_info),
        vec!["98.7.6".to_string()]
    );
}

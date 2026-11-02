//! A `--default-config` whose state store is a local directory: a submitted
//! config that inherits that store loads, and one that reads or writes under
//! the directory as data is refused. Its own test binary: it marks the
//! directory in the process-wide list of the server's own files.
#![cfg(all(feature = "serve", feature = "source-file", feature = "sink-file"))]

use faucet_cli::serve::error::ServeError;
use faucet_cli::serve::load::{BodyOrigin, ConfigFormat, load_submission, protect_default_state};
use serde_json::json;

const CALLER: BodyOrigin = BodyOrigin::Caller {
    allow_subprocess: false,
};

#[tokio::test]
async fn a_run_keeps_the_default_state_store_but_may_not_read_it_as_data() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let input = dir.path().join("in.jsonl");
    std::fs::write(&input, "{\"id\":1}\n").unwrap();
    let base = json!({
        "version": 1,
        "pipeline": {
            "state": {"type": "file", "config": {"path": state.display().to_string()}}
        }
    });
    protect_default_state(&base);

    let body = |source: &std::path::Path| {
        json!({
            "version": 1,
            "name": "inherits",
            "pipeline": {
                "source": {"type": "file", "config": {"path": source.display().to_string()}},
                "sink": {"type": "file", "config": {"path": dir.path().join("out.jsonl").display().to_string()}}
            }
        })
        .to_string()
    };

    if let Err(e) =
        load_submission(&body(&input), ConfigFormat::Json, Some(&base), None, CALLER).await
    {
        panic!("a run inheriting the default state store loads: {e}");
    }

    let err = load_submission(
        &body(&state.join("x")),
        ConfigFormat::Json,
        Some(&base),
        None,
        CALLER,
    )
    .await
    .err()
    .expect("reading the state directory is refused");
    assert!(matches!(err, ServeError::Forbidden(_)), "{err}");
}

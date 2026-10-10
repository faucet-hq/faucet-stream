//! SIGTERM handling (#565, #789 CLI-50). Its own test binary, and one test: it
//! signals its own process, which would cancel any `faucet run` another test
//! had in flight, and a second signal reaching a cancelled `StopSignal` exits.
#![cfg(unix)]

use std::time::Duration;

use faucet_cli::CliError;
use faucet_cli::signals::{StopSignal, Terminate, wait_for_termination};

fn sigterm_self() {
    std::process::Command::new("kill")
        .args(["-TERM", &std::process::id().to_string()])
        .status()
        .unwrap();
}

#[tokio::test]
async fn sigterm_resolves_the_handlers_and_cancels_a_run_cooperatively() {
    let mut t = Terminate::new().expect("install handlers");
    let wait = wait_for_termination();
    sigterm_self();
    tokio::time::timeout(Duration::from_secs(5), t.recv())
        .await
        .expect("recv resolves on SIGTERM");
    tokio::time::timeout(Duration::from_secs(5), wait)
        .await
        .expect("helper resolves on SIGTERM");

    // A one-shot run: the first SIGTERM cancels its token instead of killing
    // the process, and the stopped run reports `Cancelled` (exit 130).
    let stop = StopSignal::install();
    let token = stop.token();
    assert!(!token.is_cancelled());
    sigterm_self();
    tokio::time::timeout(Duration::from_secs(10), token.cancelled())
        .await
        .expect("the token is cancelled by SIGTERM");
    assert!(matches!(stop.finish(Ok(())), Err(CliError::Cancelled)));
}

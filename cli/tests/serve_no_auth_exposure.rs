//! `faucet serve --no-auth` refuses a network address unless the exposure is
//! accepted explicitly (#831), whether the address comes from `--listen` or
//! `FAUCET_SERVE_LISTEN`.
#![cfg(feature = "serve")]

use assert_cmd::Command;
use predicates::prelude::*;

fn faucet() -> Command {
    let mut cmd = Command::cargo_bin("faucet").unwrap();
    cmd.env_remove("FAUCET_SERVE_LISTEN")
        .env_remove("FAUCET_SERVE_AUTH_TOKEN")
        .env_remove("FAUCET_SERVE_ALLOW_UNAUTHENTICATED_NETWORK");
    cmd
}

#[test]
fn no_auth_on_all_interfaces_is_refused() {
    faucet()
        .args(["serve", "--no-auth", "--listen", "0.0.0.0:0"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "refusing to serve without authentication on 0.0.0.0:0",
        ))
        .stderr(predicate::str::contains("--allow-unauthenticated-network"));
}

#[test]
fn no_auth_with_a_network_listen_from_the_environment_is_refused() {
    faucet()
        .env("FAUCET_SERVE_LISTEN", "[::]:0")
        .args(["serve", "--no-auth"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "refusing to serve without authentication",
        ));
}

#[test]
fn the_opt_in_requires_no_auth() {
    faucet()
        .env("FAUCET_SERVE_AUTH_TOKEN", "t")
        .args(["serve", "--allow-unauthenticated-network"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("--no-auth"));
}

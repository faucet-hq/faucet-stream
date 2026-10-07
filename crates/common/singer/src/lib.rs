#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-common-singer
//!
//! Shared [Singer](https://www.singer.io/) protocol pieces for the faucet
//! Singer bridges — `faucet-source-singer` (run a tap) and `faucet-sink-singer`
//! (run a target):
//!
//! - [`message`] — the [`SingerMessage`] model, the stdout line parser, and the
//!   `SCHEMA` / `RECORD` / `STATE` / `ACTIVATE_VERSION` line encoders.
//! - [`redact`] — the conservative config-value [`Redactor`] applied to
//!   anything echoed from the subprocess.
//! - [`temp`] — private (0600) temp files for `--config` and friends.
//! - [`env`](mod@env) — [`InheritEnv`], which of faucet's environment variables the
//!   subprocess receives.

pub mod env;
pub mod lines;
pub mod message;
pub mod redact;
pub mod temp;

pub use env::{BASELINE_ENV, InheritEnv};
pub use lines::{CappedLine, DEFAULT_MAX_LINE_BYTES, read_capped_line};
pub use message::{SingerMessage, parse_line};
pub use redact::{Redactor, secret_like_values};
pub use temp::write_private_json;

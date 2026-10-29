#![cfg_attr(docsrs, feature(doc_cfg))]
//! WebAssembly-as-transform for faucet-stream.
//!
//! Loads a user-provided module (a compiled `.wasm` binary or `.wat` text)
//! through [`wasmtime`] and invokes an exported function once per record. Any
//! language that compiles to
//! core WebAssembly (Rust, TinyGo, AssemblyScript, Zig, C/C++, …) can express
//! arbitrary per-record logic in a faucet pipeline without forking the project.
//!
//! [`WasmTransformConfig`] is the user-facing config; [`WasmTransform`] is the
//! compiled runtime that owns the wasmtime engine + module and runs it per
//! page via [`WasmTransform::into_page_stage`].
//!
//! # Sandbox
//!
//! Modules run in a strict sandbox. Each record call is bounded by
//! [`fuel`](WasmTransformConfig::fuel_limit) (a deterministic CPU limit) and by
//! [`memory`](WasmTransformConfig::memory_limit_mb) (linear-memory cap). The
//! only host imports are `faucet_v1::log` and `faucet_v1::now_ns` — there is no
//! filesystem, network, clock, or environment access in v1.
//!
//! # ABI (v1)
//!
//! The module must export:
//! - `alloc(len: i32) -> i32` — allocate `len` bytes of linear memory, return
//!   the offset. The host writes the input JSON there before each call.
//! - `<function>(ptr: i32, len: i32) -> i64` — the transform entry point
//!   (name from [`WasmTransformConfig::function`], default `"transform"`).
//!   Input is UTF-8 JSON at `[ptr, ptr+len)`. The packed return value is
//!   `(out_ptr as u64) << 32 | (out_len as u64)`:
//!   - `0` → drop the record (filter it out).
//!   - [`u64::MAX`] → error; the host reads the message from the optional
//!     `error_ptr()` / `error_len()` exports.
//!   - otherwise → the output UTF-8 JSON at `[out_ptr, out_ptr+out_len)`.
//! - `memory` — the exported linear memory (standard name).
//!
//! Optional exports: `free(ptr: i32, len: i32)` (host calls it after copying
//! output out), `error_ptr() -> i32` / `error_len() -> i32` (error message).
//!
//! See the crate README and the `docs/book` cookbook page for full details and
//! reference modules.

mod abi;
mod config;
mod engine;
mod instance;
pub(crate) mod metrics;
mod runtime;

pub use config::{WasmOnError, WasmTransformConfig};
pub use runtime::WasmTransform;

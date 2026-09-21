# WASM transform (custom code)

The `wasm` transform runs a **user-provided, precompiled WebAssembly module**
once per record. It is the escape hatch for arbitrary per-record logic in a YAML
pipeline: write the transform in any language that compiles to core wasm — Rust,
TinyGo, AssemblyScript, Zig, C/C++ — compile it to a `.wasm` file, and point the
transform at it. faucet stays a single-binary deploy; you never fork it.

> Needs the `transform-wasm` feature: `cargo install faucet-cli --features transform-wasm`
> (included in `full`). It is **not** in the default build.

## Why WASM

- **Language-agnostic.** Ship logic your team already knows — a JS/TS-to-wasm
  redactor, a Go currency converter, a Rust schema normalizer — without waiting
  on a built-in transform.
- **Sandboxed.** Modules run under wasmtime with a hard **memory cap** and a
  deterministic **fuel** (CPU) budget. A buggy or hostile module cannot crash
  the pipeline, exhaust the host, read the filesystem, or reach the network.
- **Hot-reloadable.** With `reload_on_change`, editing the `.wasm` re-compiles
  and swaps it in at the next page boundary.

## Quick start

```yaml
version: 1
name: csv_to_jsonl_wasm
pipeline:
  source:
    type: csv
    config: { path: cli/examples/data/orders.csv, has_headers: true }
  transforms:
    - type: wasm
      config:
        module: examples/wasm-transforms/add_field.wasm
  sink:
    type: jsonl
    config: { path: /tmp/out.jsonl }
```

The runnable example is `cli/examples/csv_to_jsonl_wasm.yaml`; the reference
module sources live under `examples/wasm-transforms/`.

## Config fields

| Field | Default | Meaning |
|---|---|---|
| `module` | *(required)* | Filesystem path to the precompiled `.wasm` (absolute, or relative to the working directory). URLs are out of scope in v1. |
| `function` | `"transform"` | Exported entry-point function name. |
| `memory_limit_mb` | `16` | Linear-memory cap. A record that grows memory past this fails (see `on_error`). ~8 MB is the practical floor for ~1 KB JSON records. |
| `fuel_limit` | `10_000_000` | wasmtime fuel per record — a deterministic CPU bound. A record that exhausts fuel fails. |
| `on_error` | `fail` | `fail` aborts the run (like every other transform); `skip` drops the failing record; `passthrough` emits it unchanged. |
| `reload_on_change` | `false` | Re-stat the module mtime before each page; recompile + atomically swap if it changed. A failed recompile keeps the last-known-good module and warns. |

## The ABI (v1)

The host passes each record to the module as UTF-8 JSON in the module's own
linear memory and reads the result back. Your module must export:

| Export | Signature | Role |
|---|---|---|
| `alloc` | `(len: i32) -> i32` | Allocate `len` bytes; the host writes the input JSON there. |
| `<function>` | `(ptr: i32, len: i32) -> i64` | Transform the record at `[ptr, ptr+len)`. Return a **packed** result (below). |
| `memory` | *(exported linear memory)* | Standard `memory` export. |

Optional exports:

| Export | Signature | Role |
|---|---|---|
| `free` | `(ptr: i32, len: i32)` | Free a buffer after use — the host calls it for both the input and the output buffer, so a well-behaved module keeps per-page memory bounded. |
| `error_ptr` / `error_len` | `() -> i32` | Location of the last error message (UTF-8) for the error return. |

**Packed return value** (`u64`), high 32 bits = pointer, low 32 bits = length:

- `0` → **drop** the record (filter it out, like the `filter` stage).
- `u64::MAX` → **error**; the host reads the message from `error_ptr()` /
  `error_len()` (or a generic message if those aren't exported), then applies
  `on_error`.
- otherwise → the output UTF-8 JSON at `[ptr, ptr+len)`. If the bytes are not
  valid JSON, that record is treated as an error.

### Host imports (`faucet_v1`)

Two host functions are available for the module to import (both optional):

- `log(level: i32, ptr: i32, len: i32)` — emit a tracing event
  (`0`=trace … `4`=error) with a UTF-8 message from module memory.
- `now_ns() -> i64` — a monotonic clock relative to the page's instance
  (not wall-clock time).

No filesystem, network, environment, or wall-clock access exists in v1.

## Writing a module (Rust)

```rust
use serde_json::Value;
use std::alloc::{alloc as sys_alloc, dealloc, Layout};
use std::slice;

static mut LAST_ERROR: (usize, usize) = (0, 0);

#[no_mangle]
pub extern "C" fn alloc(len: usize) -> *mut u8 {
    if len == 0 { return 1 as *mut u8; }
    unsafe { sys_alloc(Layout::from_size_align_unchecked(len, 1)) }
}

#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut u8, len: usize) {
    if len != 0 && !ptr.is_null() { dealloc(ptr, Layout::from_size_align_unchecked(len, 1)); }
}

#[no_mangle]
pub unsafe extern "C" fn transform(ptr: *const u8, len: usize) -> u64 {
    let mut v: Value = match serde_json::from_slice(slice::from_raw_parts(ptr, len)) {
        Ok(v) => v,
        Err(_) => return u64::MAX, // (set LAST_ERROR for a message)
    };
    if let Some(obj) = v.as_object_mut() {
        obj.insert("wasm_processed".into(), Value::Bool(true));
    }
    let out = serde_json::to_vec(&v).unwrap();
    let out_ptr = alloc(out.len());
    std::ptr::copy_nonoverlapping(out.as_ptr(), out_ptr, out.len());
    ((out_ptr as u64) << 32) | (out.len() as u64)
}
```

Build with `cargo build --release --target wasm32-unknown-unknown`. The full
reference module (with error messages and `error_ptr`/`error_len`) is at
`examples/wasm-transforms/rust/`, alongside TinyGo and AssemblyScript ports.

## Error handling & DLQ

A module failure — a trap, fuel or memory exhaustion, an ABI violation, an
error return, or non-JSON output — is routed by `on_error`:

- `fail` (default) aborts the run with a `Transform` error, matching every other
  transform's fail-fast behaviour.
- `skip` drops the record and logs a warning + increments
  `faucet_wasm_invocations_total{outcome="error"}`.
- `passthrough` emits the record unchanged (and warns).

To quarantine bad records to a dead-letter queue instead of dropping them,
pre-filter the stream, or handle the failure inside the module and emit a
tagged record your downstream can route.

## Performance notes

- The module is **compiled once** and reused across the row's pages; each page
  gets a fresh instance, so linear memory is bounded per page and the module is
  stateless across pages (do not rely on cross-record/cross-page state).
- Export `free` so per-page memory stays flat over large pages.
- First compile of a non-trivial module costs tens to hundreds of milliseconds;
  it is amortised over the whole run. `faucet_wasm_compile_duration_seconds`
  makes hot-reload cost visible.
- A `wasm` stage is `Value`-shaped, so — like every non-columnar stage
  (`filter`, `explode`, `cdc_unwrap`, `sql`) — it drops the pipeline off the
  Arrow columnar fast path onto the JSON `Value` path. This is expected.

## Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `faucet_wasm_invocations_total` | `module`, `outcome=ok\|filter\|error` | One count per record call. |
| `faucet_wasm_invocation_duration_seconds` | `module` | Per-record `transform` wall-clock. |
| `faucet_wasm_fuel_consumed_total` | `module` | Fuel used across records. |
| `faucet_wasm_memory_bytes` | `module` | Peak linear-memory per page instance. |
| `faucet_wasm_compile_duration_seconds` | `module` | Module compile cost (incl. hot reload). |

`module` is the module file basename (low cardinality, user-controlled).

## Out of scope (v1)

- **WASI** (filesystem / env / clock access) and the **component model** —
  planned once wasmtime's component support and demand are clear.
- **Network access** from modules — deliberately disallowed (sandbox + secret
  leakage). A module only ever sees the record it is handed.
- **Module-from-URL** and **inline WASM source** — path to a precompiled `.wasm`
  only. Files are easier to audit; faucet does not bundle a wasm compiler.
- A **wall-clock `timeout`** — `fuel_limit` is the deterministic CPU bound;
  since v1 has no blocking host calls, a separate timeout would be redundant.

# faucet-transform-wasm

WebAssembly-as-transform for [`faucet-stream`](https://crates.io/crates/faucet-stream) —
run a user-provided, sandboxed `.wasm` module over each record.

It loads a precompiled WebAssembly module through [`wasmtime`](https://wasmtime.dev/)
and invokes an exported function once per record. Any language that compiles to
core wasm (Rust, TinyGo, AssemblyScript, Zig, C/C++, …) can express arbitrary
per-record logic in a faucet pipeline — no forking, single-binary deploy.

## CLI usage

Enable the `transform-wasm` feature (included in `full`):

```bash
cargo install faucet-cli --features transform-wasm
```

```yaml
pipeline:
  transforms:
    - type: wasm
      config:
        module: ./transforms/redact.wasm   # path to the precompiled module
        function: transform                 # exported entry point (default)
        memory_limit_mb: 16                 # linear-memory cap per record
        fuel_limit: 10000000                # deterministic CPU bound per record
        on_error: fail                      # fail | skip | passthrough
        reload_on_change: false             # re-stat + hot-swap the module per page
```

## Sandbox

Every record call is bounded by **fuel** (a deterministic CPU limit) and by a
**linear-memory cap**. The only host imports are `faucet_v1::log` and
`faucet_v1::now_ns` — there is no filesystem, network, clock, or environment
access in v1. A module may declare one linear memory and one table (at most
100,000 elements), so `memory_limit_mb` bounds what it can allocate. A trap,
fuel/memory exhaustion, ABI violation, or non-JSON output is routed by
`on_error`; after a trap the instance is discarded and the next record runs on
a fresh one, so a half-updated heap never shapes later output. `log` forwards at
most 100 messages of up to 4 KiB per instance; the rest are counted in one warning.

## ABI (v1)

The module must export:

- `alloc(len: i32) -> i32` — allocate `len` bytes; the host writes the input
  UTF-8 JSON there.
- `<function>(ptr: i32, len: i32) -> i64` — transform the record at
  `[ptr, ptr+len)`. Return a packed `(out_ptr << 32) | out_len`:
  - `0` → drop (filter) the record,
  - `u64::MAX` → error (message from the optional `error_ptr`/`error_len`),
  - otherwise → the output UTF-8 JSON.
- `memory` — the exported linear memory.

Optional: `free(ptr: i32, len: i32)`, `error_ptr() -> i32`, `error_len() -> i32`.

See the [WASM transform cookbook](https://faucet-hq.github.io/faucet-stream/cookbook/wasm-transforms.html)
for the full ABI, host imports, reference modules, and metrics.

## Library usage

```rust,ignore
use faucet_transform_wasm::{WasmTransform, WasmTransformConfig};
use faucet_core::TransformingSource;

let cfg: WasmTransformConfig = serde_json::from_value(serde_json::json!({
    "module": "./transform.wasm",
}))?;
let stage = WasmTransform::compile(&cfg)?.into_page_stage();
let source = TransformingSource::new(inner_source, vec![stage]);
```

## License

Licensed under either of MIT or Apache-2.0 at your option.

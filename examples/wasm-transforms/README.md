# Reference WASM transform modules

These are runnable reference modules for the faucet-stream [`wasm` transform](../../docs/book/src/cookbook/wasm-transforms.md).
Each implements the same v1 ABI in a different source language and does the same
thing: uppercase a top-level `name` field and stamp `wasm_processed: true`.

| Language | Source | Build |
|---|---|---|
| WebAssembly text | [`add_field.wat`](./add_field.wat) | none: faucet loads `.wat` directly |
| Rust | [`rust/`](./rust/) | `cargo build --release --target wasm32-unknown-unknown` |
| AssemblyScript | [`assemblyscript/`](./assemblyscript/) | `npm install && npm run build` |

Any toolchain that emits core wasm exporting the ABI below works (TinyGo,
Zig, C/C++, …); these two are the maintained references.

`add_field.wat` is the module the runnable example
`cli/examples/csv_to_jsonl_wasm.yaml` and the integration tests use. It is
written by hand in the WebAssembly text format, so the repository holds no
compiled binary. It edits the record's JSON bytes rather than parsing them, and
uppercases ASCII letters only; the Rust and AssemblyScript modules use a JSON
library and full Unicode case mapping. To run the example with the Rust build
instead:

```bash
cd rust
cargo build --release --target wasm32-unknown-unknown
# then point `module:` at
# examples/wasm-transforms/rust/target/wasm32-unknown-unknown/release/faucet_wasm_example_add_field.wasm
```

## The ABI in one paragraph

The host writes the record as UTF-8 JSON into memory returned by your exported
`alloc(len) -> ptr`, then calls `transform(ptr, len) -> u64`. The return packs
`(out_ptr << 32) | out_len`; `0` drops the record, `u64::MAX` signals an error
(message via the optional `error_ptr()`/`error_len()` exports), and anything
else points at the output UTF-8 JSON. Export `memory`, and optionally
`free(ptr, len)` so the host can reclaim buffers. Full details:
[cookbook](../../docs/book/src/cookbook/wasm-transforms.md).

## Try it

```bash
# from the repo root
cargo run -p faucet-cli --features transform-wasm -- \
  run cli/examples/csv_to_jsonl_wasm.yaml
cat /tmp/faucet_wasm_demo.jsonl
```

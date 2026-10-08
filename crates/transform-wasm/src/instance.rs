//! One page's live store + instance, and the per-record call path.
//!
//! `TypedFunc` is `Copy`, so the resolved exports are stored by value and copied
//! out before each call — no borrow conflict with `&mut self.store`.

use crate::abi::{self, RawOutcome};
use crate::engine::{self, HostState};
use faucet_core::FaucetError;
use wasmtime::{Instance, Memory, Store, TypedFunc};

/// What the module did with one record.
pub(crate) enum Outcome {
    /// Output JSON bytes to be parsed and emitted.
    Emit(Vec<u8>),
    /// The record was filtered out (module returned the drop sentinel).
    Drop,
    /// The record failed; the string is a host-facing message.
    Error(String),
}

/// The result of one record call, with sandbox telemetry. Peak memory is
/// sampled onto the instance ([`WasmInstance::peak_memory`]) and reported once
/// per page.
pub(crate) struct RunResult {
    pub(crate) outcome: Outcome,
    pub(crate) fuel_consumed: u64,
}

/// A live instance bound to one page's store.
pub(crate) struct WasmInstance {
    store: Store<HostState>,
    memory: Memory,
    alloc: TypedFunc<i32, i32>,
    transform: TypedFunc<(i32, i32), i64>,
    free: Option<TypedFunc<(i32, i32), ()>>,
    error_ptr: Option<TypedFunc<(), i32>>,
    error_len: Option<TypedFunc<(), i32>>,
    fuel_limit: u64,
    peak_memory: u64,
    trapped: bool,
}

impl WasmInstance {
    /// Resolve the ABI exports. Fails if the required `memory`, `alloc`, or the
    /// transform function are absent or mis-typed.
    pub(crate) fn new(
        mut store: Store<HostState>,
        instance: Instance,
        function: &str,
        fuel_limit: u64,
    ) -> Result<Self, FaucetError> {
        let memory = engine::memory_export(&mut store, &instance)?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(|e| {
                FaucetError::Config(format!(
                    "wasm transform: missing or invalid 'alloc(i32) -> i32' export: {e}"
                ))
            })?;
        let transform = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, function)
            .map_err(|e| {
                FaucetError::Config(format!(
                    "wasm transform: missing or invalid '{function}(i32, i32) -> i64' export: {e}"
                ))
            })?;
        let free = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "free")
            .ok();
        let error_ptr = instance
            .get_typed_func::<(), i32>(&mut store, "error_ptr")
            .ok();
        let error_len = instance
            .get_typed_func::<(), i32>(&mut store, "error_len")
            .ok();
        Ok(Self {
            store,
            memory,
            alloc,
            transform,
            free,
            error_ptr,
            error_len,
            fuel_limit,
            peak_memory: 0,
            trapped: false,
        })
    }

    /// Whether a guest call trapped: the heap and globals may be inconsistent,
    /// so the instance must not run another record.
    pub(crate) fn trapped(&self) -> bool {
        self.trapped
    }

    /// `faucet_v1::log` calls dropped by the per-instance cap.
    pub(crate) fn dropped_logs(&self) -> u64 {
        self.store.data().logs_dropped
    }

    /// Peak linear-memory size seen so far on this instance (bytes).
    pub(crate) fn peak_memory(&self) -> u64 {
        self.peak_memory
    }

    /// Run one record through the module. Infrastructure failures surface as
    /// `Err`; module-visible failures (traps, fuel/memory exhaustion, ABI
    /// violations) surface as `Ok(RunResult { outcome: Outcome::Error(..) })`
    /// so the caller can apply its `on_error` policy.
    pub(crate) fn run(&mut self, input: &[u8]) -> Result<RunResult, FaucetError> {
        self.store.set_fuel(self.fuel_limit).map_err(|e| {
            FaucetError::Transform(format!("wasm transform: could not set fuel: {e}"))
        })?;

        let len = match i32::try_from(input.len()) {
            Ok(l) => l,
            Err(_) => {
                return Ok(self.finish(Outcome::Error(
                    "wasm transform: record exceeds the wasm32 4 GiB address space".to_owned(),
                )));
            }
        };

        // Allocate the input buffer inside the module.
        let in_ptr = match self.alloc.call(&mut self.store, len) {
            Ok(p) => p,
            Err(e) => {
                self.trapped = true;
                return Ok(self.finish(Outcome::Error(trap_msg("alloc", &e))));
            }
        };
        if let Err(e) = self.memory.write(&mut self.store, in_ptr as usize, input) {
            self.free_buf(in_ptr, len);
            return Ok(self.finish(Outcome::Error(format!(
                "wasm transform: failed to write input at offset {in_ptr}: {e}"
            ))));
        }

        // Invoke the transform.
        let ret = match self.transform.call(&mut self.store, (in_ptr, len)) {
            Ok(r) => r,
            Err(e) => {
                self.trapped = true;
                return Ok(self.finish(Outcome::Error(trap_msg("transform", &e))));
            }
        };

        // The export returns i64; reinterpret the bits as the packed u64 ABI.
        let outcome = match abi::classify(ret as u64) {
            RawOutcome::Drop => {
                self.free_buf(in_ptr, len);
                Outcome::Drop
            }
            RawOutcome::Error => {
                let msg = self.read_error();
                self.free_buf(in_ptr, len);
                Outcome::Error(msg)
            }
            RawOutcome::Emit { ptr, len: out_len } => match self.read_output(ptr, out_len) {
                Ok(bytes) => {
                    self.free_out(ptr, out_len);
                    self.free_buf(in_ptr, len);
                    Outcome::Emit(bytes)
                }
                Err(msg) => {
                    self.free_buf(in_ptr, len);
                    Outcome::Error(msg)
                }
            },
        };
        Ok(self.finish(outcome))
    }

    /// Build the `RunResult`, sampling fuel + memory telemetry.
    fn finish(&mut self, outcome: Outcome) -> RunResult {
        let fuel_consumed = self
            .fuel_limit
            .saturating_sub(self.store.get_fuel().unwrap_or(0));
        let memory_bytes = self.memory.data_size(&self.store) as u64;
        self.peak_memory = self.peak_memory.max(memory_bytes);
        RunResult {
            outcome,
            fuel_consumed,
        }
    }

    /// Copy `len` bytes of module output out of linear memory, bounds-checked.
    fn read_output(&self, ptr: u32, len: u32) -> Result<Vec<u8>, String> {
        let data = self.memory.data(&self.store);
        let start = ptr as usize;
        let end = start.saturating_add(len as usize);
        data.get(start..end).map(<[u8]>::to_vec).ok_or_else(|| {
            format!(
                "wasm transform: output slice [{start}, {end}) is out of bounds (memory is {} bytes)",
                data.len()
            )
        })
    }

    /// Read the module's error message via the optional `error_ptr` /
    /// `error_len` exports.
    fn read_error(&mut self) -> String {
        let (Some(ep), Some(el)) = (&self.error_ptr, &self.error_len) else {
            return "wasm transform: module signalled an error (no error_ptr/error_len exports)"
                .to_owned();
        };
        // `error_ptr`/`error_len` are *guest* calls and so consume fuel. If the
        // module errored with none left, both trap, we fall back to (0, 0), and the
        // operator gets an empty message instead of the actual diagnosis — so name
        // the real cause up front (#456 L5).
        if let Some(msg) = out_of_fuel_message(self.store.get_fuel().ok(), self.fuel_limit) {
            return msg;
        }
        let ptr = ep.call(&mut self.store, ()).unwrap_or(0);
        let len = el.call(&mut self.store, ()).unwrap_or(0);
        let data = self.memory.data(&self.store);
        let start = ptr as usize;
        let end = start.saturating_add(len as usize);
        match data.get(start..end) {
            Some(slice) => format!(
                "wasm transform: module error: {}",
                String::from_utf8_lossy(slice)
            ),
            None => {
                "wasm transform: module signalled an error (error pointer out of bounds)".to_owned()
            }
        }
    }

    /// Free a host-allocated input buffer if the module exports `free`.
    fn free_buf(&mut self, ptr: i32, len: i32) {
        if let Some(free) = &self.free {
            let _ = free.call(&mut self.store, (ptr, len));
        }
    }

    /// Free a module-allocated output buffer (u32 offsets) if `free` is present.
    fn free_out(&mut self, ptr: u32, len: u32) {
        if let (Some(free), Ok(p), Ok(l)) = (&self.free, i32::try_from(ptr), i32::try_from(len)) {
            let _ = free.call(&mut self.store, (p, l));
        }
    }
}

/// Format a wasmtime trap / call error into a concise host message. The
/// wasmtime error display already carries the reason (e.g. "all fuel consumed
/// by WebAssembly", "out of bounds memory access", "wasm `unreachable`").
fn trap_msg(ctx: &str, err: &wasmtime::Error) -> String {
    // `{:?}` includes the cause chain, so the reason ("all fuel consumed by
    // WebAssembly", "out of bounds memory access", …) is preserved — the bare
    // `{}` display shows only the top-level backtrace line.
    format!("wasm transform: {ctx} failed: {err:?}")
}

/// The message to report when a module signalled an error with no fuel left.
///
/// Pure so the decision is testable: reproducing the state for real needs a
/// module that returns the error sentinel at the exact instant its budget hits
/// zero. `remaining` is `None` when fuel is not being metered (nothing to say).
fn out_of_fuel_message(remaining: Option<u64>, limit: u64) -> Option<String> {
    match remaining {
        Some(0) => Some(format!(
            "wasm transform: module signalled an error with no fuel left (budget {limit} units) — \
             its error message could not be read back, because doing so calls into the module. \
             Raise `fuel_per_record` or simplify the module"
        )),
        _ => None,
    }
}

#[cfg(test)]
mod fuel_message_tests {
    use super::*;

    #[test]
    fn out_of_fuel_message_only_fires_when_the_budget_is_spent() {
        let msg = out_of_fuel_message(Some(0), 10_000).expect("a message");
        assert!(msg.contains("fuel"), "{msg}");
        assert!(msg.contains("10000"), "names the budget: {msg}");

        // Fuel left, or unmetered → let the normal error-export read proceed.
        assert!(out_of_fuel_message(Some(1), 10_000).is_none());
        assert!(out_of_fuel_message(None, 10_000).is_none());
    }
}

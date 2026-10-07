//! The compiled WASM transform: owns the engine/module (shared across pages)
//! and adapts it to a page-level [`TransformStage::PageFn`].

use crate::config::{WasmOnError, WasmTransformConfig};
use crate::engine::WasmEngine;
use crate::instance::Outcome;
use crate::metrics;
use faucet_core::FaucetError;
use faucet_core::stage::TransformStage;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// A compiled WASM transform. The engine + module are compiled once and reused
/// across the row's pages; each page gets a fresh instance.
pub struct WasmTransform {
    engine: Arc<Mutex<WasmEngine>>,
    on_error: WasmOnError,
    module_label: String,
}

impl std::fmt::Debug for WasmTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmTransform")
            .field("module", &self.module_label)
            .field("on_error", &self.on_error)
            .finish_non_exhaustive()
    }
}

impl WasmTransform {
    /// Compile the module, validate its ABI, and build the reusable engine.
    /// Fails fast on a missing file, malformed module, or missing exports.
    pub fn compile(cfg: &WasmTransformConfig) -> Result<Self, FaucetError> {
        let engine = WasmEngine::compile(cfg)?;
        Ok(Self {
            module_label: engine.module_label.clone(),
            engine: Arc::new(Mutex::new(engine)),
            on_error: cfg.on_error,
        })
    }

    /// Consume into a page-level transform stage.
    pub fn into_page_stage(self) -> TransformStage {
        let engine = self.engine;
        let on_error = self.on_error;
        let module_label = self.module_label;
        TransformStage::PageFn(Arc::new(move |records: Vec<Value>| {
            execute_page(&engine, on_error, &module_label, records)
        }))
    }

    #[cfg(test)]
    fn run_page(&self, records: Vec<Value>) -> Result<Vec<Value>, FaucetError> {
        execute_page(&self.engine, self.on_error, &self.module_label, records)
    }
}

fn execute_page(
    engine: &Arc<Mutex<WasmEngine>>,
    on_error: WasmOnError,
    module_label: &str,
    records: Vec<Value>,
) -> Result<Vec<Value>, FaucetError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    let mut eng = engine.lock().unwrap_or_else(|e| e.into_inner());
    eng.reload_if_changed();
    let mut inst = eng.new_page_instance()?;
    let mut peak_memory = 0u64;
    let mut dropped_logs = 0u64;

    let mut out = Vec::with_capacity(records.len());
    for rec in records {
        if inst.trapped() {
            peak_memory = peak_memory.max(inst.peak_memory());
            dropped_logs += inst.dropped_logs();
            inst = eng.new_page_instance()?;
        }
        let input = serde_json::to_vec(&rec).map_err(|e| {
            FaucetError::Transform(format!("wasm transform: serialize record: {e}"))
        })?;
        let start = Instant::now();
        let result = inst.run(&input)?;
        metrics::invocation_duration(module_label, start.elapsed().as_secs_f64());
        metrics::fuel_consumed(module_label, result.fuel_consumed);

        let err_msg: String = match result.outcome {
            Outcome::Emit(bytes) => match serde_json::from_slice::<Value>(&bytes) {
                Ok(v) => {
                    metrics::invocation(module_label, "ok");
                    out.push(v);
                    continue;
                }
                Err(e) => format!("wasm transform: module returned invalid JSON: {e}"),
            },
            Outcome::Drop => {
                metrics::invocation(module_label, "filter");
                continue;
            }
            Outcome::Error(msg) => msg,
        };

        // Error path — apply the on_error policy.
        metrics::invocation(module_label, "error");
        match on_error {
            WasmOnError::Fail => return Err(FaucetError::Transform(err_msg)),
            WasmOnError::Skip => tracing::warn!(
                target: "faucet::transform::wasm",
                module = %module_label,
                error = %err_msg,
                "dropping record after wasm error (on_error: skip)"
            ),
            WasmOnError::Passthrough => {
                tracing::warn!(
                    target: "faucet::transform::wasm",
                    module = %module_label,
                    error = %err_msg,
                    "passing record through unchanged after wasm error (on_error: passthrough)"
                );
                out.push(rec);
            }
        }
    }
    metrics::memory_bytes(module_label, peak_memory.max(inst.peak_memory()));
    dropped_logs += inst.dropped_logs();
    if dropped_logs > 0 {
        tracing::warn!(
            target: "faucet::transform::wasm",
            module = %module_label,
            dropped = dropped_logs,
            "wasm module exceeded its log budget; further log lines were dropped"
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // ── WAT fixture modules implementing the v1 ABI ──────────────────────────

    /// Bump-allocator preamble shared by the fixtures: `memory` + `alloc`.
    const PREAMBLE: &str = r#"
        (memory (export "memory") 1)
        (global $bump (mut i32) (i32.const 1024))
        (func (export "alloc") (param $len i32) (result i32)
            (local $p i32)
            (local.set $p (global.get $bump))
            (global.set $bump (i32.add (global.get $bump) (local.get $len)))
            (local.get $p))
    "#;

    /// Echoes the input JSON back verbatim (proves the ABI round-trip).
    fn identity_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                (local $out i32)
                (local.set $out (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
                (i64.or
                    (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len)))))"#
        )
    }

    /// Returns the drop sentinel (0) — filters every record.
    fn drop_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param i32) (param i32) (result i64)
                (i64.const 0)))"#
        )
    }

    /// Signals an error (returns u64::MAX) with an error message.
    fn error_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (data (i32.const 100) "boom in module")
            (func (export "transform") (param i32) (param i32) (result i64)
                (i64.const -1))
            (func (export "error_ptr") (result i32) (i32.const 100))
            (func (export "error_len") (result i32) (i32.const 14)))"#
        )
    }

    /// Signals an error but exports no `error_ptr`/`error_len`.
    fn error_no_msg_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param i32) (param i32) (result i64)
                (i64.const -1)))"#
        )
    }

    /// Spins forever — exhausts fuel.
    fn fuel_bomb_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param i32) (param i32) (result i64)
                (loop $l (br $l))
                (i64.const 0)))"#
        )
    }

    /// Grows memory far past any cap — traps under trap_on_grow_failure.
    fn mem_bomb_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param i32) (param i32) (result i64)
                (drop (memory.grow (i32.const 100000)))
                (i64.const 0)))"#
        )
    }

    /// Uses the host imports (`now_ns`, `log`) then echoes the record.
    fn host_wat() -> String {
        format!(
            r#"(module
            (import "faucet_v1" "log" (func $log (param i32 i32 i32)))
            (import "faucet_v1" "now_ns" (func $now (result i64)))
            {PREAMBLE}
            (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                (local $out i32)
                (drop (call $now))
                (call $log (i32.const 3) (local.get $ptr) (local.get $len))
                (local.set $out (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
                (i64.or
                    (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len)))))"#
        )
    }

    /// Returns an out-of-bounds output pointer.
    fn bad_ptr_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param i32) (param i32) (result i64)
                (i64.or (i64.shl (i64.const 1000000) (i64.const 32)) (i64.const 10))))"#
        )
    }

    /// Emits bytes that are not valid JSON.
    fn invalid_json_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (data (i32.const 200) "notjson")
            (func (export "transform") (param i32) (param i32) (result i64)
                (i64.or (i64.shl (i64.const 200) (i64.const 32)) (i64.const 7))))"#
        )
    }

    /// Free-exporting echo — exercises the `free` reclamation path.
    fn identity_with_free_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "free") (param i32) (param i32))
            (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                (local $out i32)
                (local.set $out (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
                (i64.or
                    (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len)))))"#
        )
    }

    /// Calls `log` at every level (incl. an out-of-bounds pointer) then echoes.
    fn log_levels_wat() -> String {
        format!(
            r#"(module
            (import "faucet_v1" "log" (func $log (param i32 i32 i32)))
            {PREAMBLE}
            (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                (local $out i32)
                (call $log (i32.const 0) (local.get $ptr) (local.get $len))
                (call $log (i32.const 1) (local.get $ptr) (local.get $len))
                (call $log (i32.const 2) (local.get $ptr) (local.get $len))
                (call $log (i32.const 4) (i32.const 5000000) (i32.const 4))
                (local.set $out (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
                (i64.or
                    (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len)))))"#
        )
    }

    /// `alloc` traps immediately.
    fn alloc_trap_wat() -> &'static str {
        r#"(module
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32) unreachable)
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#
    }

    /// `alloc` returns an out-of-bounds offset, so the host's input write fails.
    fn bad_alloc_ptr_wat() -> &'static str {
        r#"(module
            (memory (export "memory") 1)
            (func (export "alloc") (param i32) (result i32) (i32.const 5000000))
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#
    }

    /// Errors, but points `error_ptr`/`error_len` out of bounds.
    fn error_oob_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const -1))
            (func (export "error_ptr") (result i32) (i32.const 5000000))
            (func (export "error_len") (result i32) (i32.const 10)))"#
        )
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn write_wasm(wat: &str) -> NamedTempFile {
        let wasm = wat::parse_str(wat).expect("valid wat");
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(&wasm).unwrap();
        f.flush().unwrap();
        f
    }

    fn cfg(path: &std::path::Path) -> WasmTransformConfig {
        WasmTransformConfig {
            module: path.to_string_lossy().into_owned(),
            function: "transform".into(),
            memory_limit_mb: 16,
            fuel_limit: 1_000_000,
            on_error: WasmOnError::Fail,
            reload_on_change: false,
        }
    }

    /// Compile a transform from WAT with an optional config tweak. Returns the
    /// transform plus the backing temp file (keep it alive for the test).
    fn build(
        wat: &str,
        tweak: impl FnOnce(&mut WasmTransformConfig),
    ) -> (WasmTransform, NamedTempFile) {
        let f = write_wasm(wat);
        let mut c = cfg(f.path());
        tweak(&mut c);
        let t = WasmTransform::compile(&c).expect("compile");
        (t, f)
    }

    // ── tests ────────────────────────────────────────────────────────────────

    #[test]
    fn identity_round_trips_records() {
        let (t, _f) = build(&identity_wat(), |_| {});
        let out = t
            .run_page(vec![json!({"id": 1, "name": "a"}), json!({"id": 2})])
            .unwrap();
        assert_eq!(out, vec![json!({"id": 1, "name": "a"}), json!({"id": 2})]);
    }

    #[test]
    fn identity_with_free_round_trips() {
        let (t, _f) = build(&identity_with_free_wat(), |_| {});
        let out = t.run_page(vec![json!({"x": 42})]).unwrap();
        assert_eq!(out, vec![json!({"x": 42})]);
    }

    #[test]
    fn empty_page_is_identity() {
        let (t, _f) = build(&identity_wat(), |_| {});
        assert_eq!(t.run_page(vec![]).unwrap(), Vec::<Value>::new());
    }

    #[test]
    fn drop_filters_every_record() {
        let (t, _f) = build(&drop_wat(), |_| {});
        let out = t.run_page(vec![json!({"a": 1}), json!({"a": 2})]).unwrap();
        assert_eq!(out, Vec::<Value>::new());
    }

    #[test]
    fn error_with_message_fails_by_default() {
        let (t, _f) = build(&error_wat(), |_| {});
        let err = t.run_page(vec![json!({"a": 1})]).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("boom in module"), "{msg}");
    }

    #[test]
    fn error_without_message_still_reports() {
        let (t, _f) = build(&error_no_msg_wat(), |_| {});
        let err = t.run_page(vec![json!({"a": 1})]).unwrap_err();
        assert!(format!("{err}").contains("no error_ptr"), "{err}");
    }

    #[test]
    fn on_error_skip_drops_failing_record() {
        let (t, _f) = build(&error_wat(), |c| c.on_error = WasmOnError::Skip);
        let out = t.run_page(vec![json!({"a": 1})]).unwrap();
        assert_eq!(out, Vec::<Value>::new());
    }

    #[test]
    fn on_error_passthrough_keeps_original() {
        let (t, _f) = build(&error_wat(), |c| c.on_error = WasmOnError::Passthrough);
        let out = t.run_page(vec![json!({"a": 1})]).unwrap();
        assert_eq!(out, vec![json!({"a": 1})]);
    }

    #[test]
    fn fuel_exhaustion_is_an_error() {
        let (t, _f) = build(&fuel_bomb_wat(), |c| {
            c.fuel_limit = 10_000;
            c.on_error = WasmOnError::Skip;
        });
        // Skip policy → the failing record is dropped, no records emitted.
        let out = t.run_page(vec![json!({"a": 1})]).unwrap();
        assert_eq!(out, Vec::<Value>::new());
    }

    #[test]
    fn fuel_exhaustion_message_mentions_fuel() {
        let (t, _f) = build(&fuel_bomb_wat(), |c| c.fuel_limit = 10_000);
        let err = t.run_page(vec![json!({"a": 1})]).unwrap_err();
        assert!(format!("{err}").to_lowercase().contains("fuel"), "{err}");
    }

    #[test]
    fn memory_cap_is_an_error() {
        let (t, _f) = build(&mem_bomb_wat(), |c| {
            c.memory_limit_mb = 1;
            c.on_error = WasmOnError::Passthrough;
        });
        let out = t.run_page(vec![json!({"a": 1})]).unwrap();
        // Passthrough → original record survives despite the memory trap.
        assert_eq!(out, vec![json!({"a": 1})]);
    }

    #[test]
    fn host_imports_work() {
        let (t, _f) = build(&host_wat(), |_| {});
        let out = t.run_page(vec![json!({"hello": "world"})]).unwrap();
        assert_eq!(out, vec![json!({"hello": "world"})]);
    }

    #[test]
    fn out_of_bounds_output_is_an_error() {
        let (t, _f) = build(&bad_ptr_wat(), |c| c.on_error = WasmOnError::Skip);
        assert_eq!(
            t.run_page(vec![json!({"a": 1})]).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[test]
    fn invalid_json_output_is_an_error() {
        let (t, _f) = build(&invalid_json_wat(), |_| {});
        let err = t.run_page(vec![json!({"a": 1})]).unwrap_err();
        assert!(format!("{err}").contains("invalid JSON"), "{err}");
    }

    #[test]
    fn missing_module_file_fails_to_compile() {
        let c = WasmTransformConfig {
            module: "/no/such/module.wasm".into(),
            function: "transform".into(),
            memory_limit_mb: 16,
            fuel_limit: 1000,
            on_error: WasmOnError::Fail,
            reload_on_change: false,
        };
        let err = WasmTransform::compile(&c).unwrap_err();
        assert!(format!("{err}").contains("cannot read module"), "{err}");
    }

    #[test]
    fn malformed_module_fails_to_compile() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"not a wasm module").unwrap();
        f.flush().unwrap();
        let err = WasmTransform::compile(&cfg(f.path())).unwrap_err();
        assert!(format!("{err}").contains("compile"), "{err}");
    }

    #[test]
    fn missing_alloc_export_fails_to_compile() {
        let wat = r#"(module
            (memory (export "memory") 1)
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#;
        let f = write_wasm(wat);
        let err = WasmTransform::compile(&cfg(f.path())).unwrap_err();
        assert!(format!("{err}").contains("alloc"), "{err}");
    }

    #[test]
    fn missing_transform_function_fails_to_compile() {
        let f = write_wasm(&drop_wat());
        let mut c = cfg(f.path());
        c.function = "does_not_exist".into();
        let err = WasmTransform::compile(&c).unwrap_err();
        assert!(format!("{err}").contains("does_not_exist"), "{err}");
    }

    #[test]
    fn missing_memory_export_fails_to_compile() {
        let wat = r#"(module
            (func (export "alloc") (param i32) (result i32) (i32.const 0))
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#;
        let f = write_wasm(wat);
        let err = WasmTransform::compile(&cfg(f.path())).unwrap_err();
        assert!(format!("{err}").contains("memory"), "{err}");
    }

    #[test]
    fn into_page_stage_wires_through_core() {
        // Drive the real faucet-core page runner to cover into_page_stage().
        let (t, _f) = build(&identity_wat(), |_| {});
        let stage = t.into_page_stage();
        let compiled = faucet_core::compile_stage(&stage).unwrap();
        let out =
            faucet_core::stage::apply_stages_to_page(vec![json!({"k": "v"})], &[compiled]).unwrap();
        assert_eq!(out, vec![json!({"k": "v"})]);
    }

    #[test]
    fn debug_impl_names_module() {
        let (t, _f) = build(&identity_wat(), |_| {});
        assert!(format!("{t:?}").contains("WasmTransform"));
    }

    #[test]
    fn reload_on_change_swaps_module() {
        // Start as identity, then overwrite with a drop module and confirm the
        // next page reloads it.
        let f = write_wasm(&identity_wat());
        let mut c = cfg(f.path());
        c.reload_on_change = true;
        let t = WasmTransform::compile(&c).unwrap();
        assert_eq!(
            t.run_page(vec![json!({"a": 1})]).unwrap(),
            vec![json!({"a": 1})]
        );

        // Bump mtime and rewrite as a drop module.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(f.path(), wat::parse_str(drop_wat()).unwrap()).unwrap();

        assert_eq!(
            t.run_page(vec![json!({"a": 1})]).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[test]
    fn log_at_all_levels_including_bad_pointer() {
        let (t, _f) = build(&log_levels_wat(), |_| {});
        let out = t.run_page(vec![json!({"msg": "hi"})]).unwrap();
        assert_eq!(out, vec![json!({"msg": "hi"})]);
    }

    #[test]
    fn alloc_trap_is_an_error() {
        let (t, _f) = build(alloc_trap_wat(), |c| c.on_error = WasmOnError::Skip);
        assert_eq!(
            t.run_page(vec![json!({"a": 1})]).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[test]
    fn bad_alloc_pointer_fails_input_write() {
        let (t, _f) = build(bad_alloc_ptr_wat(), |c| c.on_error = WasmOnError::Skip);
        assert_eq!(
            t.run_page(vec![json!({"a": 1})]).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[test]
    fn error_pointer_out_of_bounds_still_reports() {
        let (t, _f) = build(&error_oob_wat(), |_| {});
        let err = t.run_page(vec![json!({"a": 1})]).unwrap_err();
        assert!(format!("{err}").contains("out of bounds"), "{err}");
    }

    #[test]
    fn reload_keeps_last_good_when_file_deleted() {
        let f = write_wasm(&identity_wat());
        let path = f.path().to_path_buf();
        let mut c = cfg(&path);
        c.reload_on_change = true;
        let t = WasmTransform::compile(&c).unwrap();

        // Remove the file; the reload attempt must fail-soft and keep identity.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::remove_file(&path).unwrap();

        assert_eq!(
            t.run_page(vec![json!({"stay": 1})]).unwrap(),
            vec![json!({"stay": 1})]
        );
    }

    #[test]
    fn reload_keeps_last_good_on_bad_module() {
        let f = write_wasm(&identity_wat());
        let mut c = cfg(f.path());
        c.reload_on_change = true;
        let t = WasmTransform::compile(&c).unwrap();

        // Overwrite with garbage; the reload must fail and keep identity.
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(f.path(), b"garbage not wasm").unwrap();

        assert_eq!(
            t.run_page(vec![json!({"keep": true})]).unwrap(),
            vec![json!({"keep": true})]
        );
    }

    #[test]
    fn reload_keeps_last_good_when_the_new_module_breaks_the_abi() {
        let f = write_wasm(&identity_wat());
        let mut c = cfg(f.path());
        c.reload_on_change = true;
        let t = WasmTransform::compile(&c).unwrap();

        std::thread::sleep(std::time::Duration::from_millis(20));
        let no_alloc = r#"(module
            (memory (export "memory") 1)
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#;
        std::fs::write(f.path(), wat::parse_str(no_alloc).unwrap()).unwrap();

        assert_eq!(
            t.run_page(vec![json!({"keep": 1})]).unwrap(),
            vec![json!({"keep": 1})]
        );
    }

    #[test]
    fn the_same_module_compiles_once_across_transforms() {
        let wat = format!(
            r#"(module {PREAMBLE}
            (data (i32.const 50) "module-cache-test")
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#
        );
        let (f1, f2) = (write_wasm(&wat), write_wasm(&wat));
        let a = WasmTransform::compile(&cfg(f1.path())).unwrap();
        let b = WasmTransform::compile(&cfg(f2.path())).unwrap();
        let other = write_wasm(&identity_wat());
        let c = WasmTransform::compile(&cfg(other.path())).unwrap();
        let engine = |t: &WasmTransform| t.engine.lock().unwrap().engine.clone();
        assert!(wasmtime::Engine::same(&engine(&a), &engine(&b)));
        assert!(!wasmtime::Engine::same(&engine(&a), &engine(&c)));
        assert!(crate::engine::MODULES_COMPILED.load(std::sync::atomic::Ordering::Relaxed) >= 2);
    }

    #[test]
    fn compile_inside_a_multi_thread_runtime_runs_off_the_worker() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let f = write_wasm(&identity_wat());
        let c = cfg(f.path());
        let t = rt
            .block_on(async move { tokio::spawn(async move { WasmTransform::compile(&c) }).await });
        assert!(t.unwrap().is_ok());
    }

    /// Traps on a short record after dirtying a global; a dirty instance drops
    /// every later record instead of echoing it.
    fn dirty_trap_wat() -> String {
        format!(
            r#"(module {PREAMBLE}
            (global $dirty (mut i32) (i32.const 0))
            (func (export "transform") (param $ptr i32) (param $len i32) (result i64)
                (local $out i32)
                (if (i32.lt_u (local.get $len) (i32.const 10))
                    (then (global.set $dirty (i32.const 1)) unreachable))
                (if (global.get $dirty) (then (return (i64.const 0))))
                (local.set $out (global.get $bump))
                (global.set $bump (i32.add (global.get $bump) (local.get $len)))
                (memory.copy (local.get $out) (local.get $ptr) (local.get $len))
                (i64.or
                    (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
                    (i64.extend_i32_u (local.get $len)))))"#
        )
    }

    #[test]
    fn a_trapped_instance_is_replaced_before_the_next_record() {
        let long = json!({"aaaaaaaaaaaa": 1});
        let (t, _f) = build(&dirty_trap_wat(), |c| c.on_error = WasmOnError::Skip);
        let out = t.run_page(vec![json!({}), long.clone()]).unwrap();
        assert_eq!(out, vec![long.clone()]);
        let (t, _f) = build(&dirty_trap_wat(), |c| c.on_error = WasmOnError::Passthrough);
        let out = t
            .run_page(vec![json!({}), long.clone(), json!({})])
            .unwrap();
        assert_eq!(out, vec![json!({}), long, json!({})]);
    }

    #[test]
    fn a_module_with_two_memories_is_refused() {
        let f = write_wasm(
            r#"(module
            (memory (export "memory") 1)
            (memory $second 1)
            (func (export "alloc") (param i32) (result i32) (i32.const 0))
            (func (export "transform") (param i32) (param i32) (result i64) (i64.const 0)))"#,
        );
        let err = WasmTransform::compile(&cfg(f.path())).unwrap_err();
        assert!(format!("{err}").contains("failed to compile"), "{err}");
    }

    #[test]
    fn table_growth_is_capped() {
        let wat = format!(
            r#"(module {PREAMBLE}
            (table $t 1 funcref)
            (func (export "transform") (param i32) (param i32) (result i64)
                (drop (table.grow $t (ref.null func) (i32.const 1000000)))
                (i64.const 0)))"#
        );
        let (t, _f) = build(&wat, |_| {});
        let err = t.run_page(vec![json!({"a": 1})]).unwrap_err();
        assert!(format!("{err}").contains("transform"), "{err}");
    }

    #[test]
    fn log_calls_are_capped_per_instance() {
        let (t, _f) = build(&host_wat(), |_| {});
        let eng = t.engine.lock().unwrap();
        let mut inst = eng.new_page_instance().unwrap();
        let extra = 25u64;
        for _ in 0..(crate::engine::MAX_LOGS_PER_INSTANCE as u64 + extra) {
            let r = inst.run(br#"{"a":1}"#).unwrap();
            assert!(matches!(r.outcome, Outcome::Emit(_)));
        }
        assert_eq!(inst.dropped_logs(), extra);
        assert!(!inst.trapped());
        drop(eng);
        let page: Vec<Value> = (0..150).map(|i| json!({"i": i})).collect();
        assert_eq!(t.run_page(page).unwrap().len(), 150);
    }
}

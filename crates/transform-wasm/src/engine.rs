//! The wasmtime engine + compiled module, and per-page instance creation.
//!
//! The [`Engine`] and [`Module`] are compiled once (expensive) and reused
//! across the row's pages. Each page gets a fresh [`Store`] + `Instance`
//! (cheap), which bounds linear-memory growth to a single page and keeps the
//! module stateless across pages.

use crate::config::WasmTransformConfig;
use crate::instance::WasmInstance;
use crate::metrics;
use faucet_core::FaucetError;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Instant, SystemTime};
use wasmtime::{
    Caller, Config, Engine, Extern, Linker, Memory, Module, Store, StoreLimits, StoreLimitsBuilder,
};

/// Per-store host state: the memory limiter and a monotonic epoch for the
/// `now_ns` host import.
pub(crate) struct HostState {
    pub(crate) limits: StoreLimits,
    pub(crate) epoch_base: Instant,
    pub(crate) logs_emitted: u32,
    pub(crate) logs_dropped: u64,
}

/// Longest `faucet_v1::log` message forwarded; longer ones are truncated.
pub(crate) const MAX_LOG_BYTES: usize = 4096;
/// Log lines one instance may emit; the rest are counted and reported once.
pub(crate) const MAX_LOGS_PER_INSTANCE: u32 = 100;
/// Table-element ceiling, so a module cannot grow a table past the memory cap.
pub(crate) const MAX_TABLE_ELEMENTS: usize = 100_000;

/// A compiled WASM transform: owns the wasmtime engine, the compiled module,
/// and the import linker. Cheap to build an instance from, per page.
pub(crate) struct WasmEngine {
    pub(crate) engine: Engine,
    module: Module,
    linker: Linker<HostState>,
    pub(crate) function: String,
    pub(crate) memory_bytes: usize,
    pub(crate) fuel_limit: u64,
    pub(crate) module_label: String,
    path: PathBuf,
    mtime: Option<SystemTime>,
    reload_on_change: bool,
}

impl WasmEngine {
    /// Compile the module and validate the ABI exports. Fails fast (in `new()`)
    /// on a missing file, a malformed module, or a missing/mis-typed export.
    pub(crate) fn compile(cfg: &WasmTransformConfig) -> Result<Self, FaucetError> {
        let path = PathBuf::from(&cfg.module);
        let module_label = cfg.module_label();
        let bytes = std::fs::read(&path).map_err(|e| {
            FaucetError::Config(format!(
                "wasm transform: cannot read module '{}': {e}",
                path.display()
            ))
        })?;
        let mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
        let (engine, module) = off_worker(|| cached_module(&path, &bytes, &module_label))?;
        let linker = build_linker(&engine)?;

        let memory_bytes = (cfg.memory_limit_mb as usize) * 1024 * 1024;
        let engine_wrap = Self {
            engine,
            module,
            linker,
            function: cfg.function.clone(),
            memory_bytes,
            fuel_limit: cfg.fuel_limit,
            module_label,
            path,
            mtime,
            reload_on_change: cfg.reload_on_change,
        };

        // Validate the ABI by instantiating once and resolving the required
        // exports (`WasmInstance::new` fails on a missing memory / `alloc` /
        // transform function). A bad module surfaces here, at config-load time.
        let _probe = engine_wrap.new_page_instance()?;
        Ok(engine_wrap)
    }

    /// Re-stat the module file; if its mtime changed, recompile and atomically
    /// swap the module in. A failed recompile keeps the last-known-good module
    /// and logs a warning (so a bad hot edit never takes down a running
    /// pipeline). No-op unless `reload_on_change`.
    pub(crate) fn reload_if_changed(&mut self) {
        if !self.reload_on_change {
            return;
        }
        let cur = std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok();
        if cur == self.mtime {
            return;
        }
        // mtime moved (or became unreadable) — attempt a recompile.
        match std::fs::read(&self.path) {
            Ok(bytes) => {
                let start = Instant::now();
                match off_worker(|| Module::new(&self.engine, &bytes)) {
                    Ok(module) => {
                        metrics::compile_duration(
                            &self.module_label,
                            start.elapsed().as_secs_f64(),
                        );
                        if let Err(e) = self.instance_for(&module) {
                            tracing::warn!(
                                target: "faucet::transform::wasm",
                                module = %self.module_label,
                                error = %e,
                                "wasm module changed but fails the ABI check; keeping previous module"
                            );
                            return;
                        }
                        self.module = module;
                        self.mtime = cur;
                        tracing::info!(
                            target: "faucet::transform::wasm",
                            module = %self.module_label,
                            "reloaded changed wasm module"
                        );
                    }
                    Err(e) => {
                        // Keep last-known-good; don't advance mtime so we retry
                        // once the file is fixed.
                        tracing::warn!(
                            target: "faucet::transform::wasm",
                            module = %self.module_label,
                            error = %e,
                            "wasm module changed but failed to recompile; keeping previous module"
                        );
                    }
                }
            }
            Err(e) => {
                tracing::warn!(
                    target: "faucet::transform::wasm",
                    module = %self.module_label,
                    error = %e,
                    "wasm module changed but became unreadable; keeping previous module"
                );
            }
        }
    }

    /// Create a fresh store + instance for one page.
    pub(crate) fn new_page_instance(&self) -> Result<WasmInstance, FaucetError> {
        self.instance_for(&self.module)
    }

    fn instance_for(&self, module: &Module) -> Result<WasmInstance, FaucetError> {
        let limits = StoreLimitsBuilder::new()
            .memory_size(self.memory_bytes)
            .memories(1)
            .tables(1)
            .table_elements(MAX_TABLE_ELEMENTS)
            .instances(1)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &self.engine,
            HostState {
                limits,
                epoch_base: Instant::now(),
                logs_emitted: 0,
                logs_dropped: 0,
            },
        );
        store.limiter(|state| &mut state.limits);
        // Instantiation itself burns fuel (active data-segment copies), so seed
        // the budget before instantiating; `WasmInstance::run` resets it per
        // record afterwards.
        store.set_fuel(self.fuel_limit).map_err(|e| {
            FaucetError::Config(format!("wasm transform: could not enable fuel: {e}"))
        })?;
        let instance = self.linker.instantiate(&mut store, module).map_err(|e| {
            FaucetError::Transform(format!(
                "wasm transform: instantiation failed for '{}': {e}",
                self.module_label
            ))
        })?;
        WasmInstance::new(store, instance, &self.function, self.fuel_limit)
    }
}

/// Compiled modules by content, shared across invocations: a fan-out of N
/// parents running one module compiles it once.
type ModuleCache = Mutex<HashMap<(u64, usize), (Engine, Module)>>;

/// Entries kept before the cache is cleared.
const MODULE_CACHE_CAP: usize = 64;

static MODULE_CACHE: OnceLock<ModuleCache> = OnceLock::new();

/// Modules compiled by this process (cache misses).
pub(crate) static MODULES_COMPILED: AtomicU64 = AtomicU64::new(0);

fn cached_module(
    path: &std::path::Path,
    bytes: &[u8],
    label: &str,
) -> Result<(Engine, Module), FaucetError> {
    let mut hasher = std::hash::DefaultHasher::new();
    bytes.hash(&mut hasher);
    let key = (hasher.finish(), bytes.len());
    let cache = MODULE_CACHE.get_or_init(Default::default);
    if let Some(hit) = cache.lock().unwrap_or_else(|e| e.into_inner()).get(&key) {
        return Ok(hit.clone());
    }
    let mut config = Config::new();
    config.consume_fuel(true);
    config.wasm_multi_memory(false);
    let engine = Engine::new(&config)
        .map_err(|e| FaucetError::Config(format!("wasm transform: engine init failed: {e}")))?;
    let compile_start = Instant::now();
    let module = Module::new(&engine, bytes).map_err(|e| {
        FaucetError::Config(format!(
            "wasm transform: failed to compile '{}': {e}",
            path.display()
        ))
    })?;
    MODULES_COMPILED.fetch_add(1, Ordering::Relaxed);
    metrics::compile_duration(label, compile_start.elapsed().as_secs_f64());
    let mut map = cache.lock().unwrap_or_else(|e| e.into_inner());
    if map.len() >= MODULE_CACHE_CAP {
        map.clear();
    }
    map.insert(key, (engine.clone(), module.clone()));
    Ok((engine, module))
}

/// Run blocking work without pinning a tokio worker: on a multi-thread
/// runtime the worker hands its other tasks off first (`block_in_place`).
pub(crate) fn off_worker<T>(f: impl FnOnce() -> T) -> T {
    let multi_thread = tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread);
    if multi_thread {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

/// Build the import linker with the `faucet_v1` host functions.
fn build_linker(engine: &Engine) -> Result<Linker<HostState>, FaucetError> {
    let mut linker = Linker::new(engine);
    linker
        .func_wrap(
            "faucet_v1",
            "log",
            |mut caller: Caller<'_, HostState>, level: i32, ptr: i32, len: i32| {
                let state = caller.data_mut();
                if state.logs_emitted >= MAX_LOGS_PER_INSTANCE {
                    state.logs_dropped += 1;
                    return;
                }
                state.logs_emitted += 1;
                let capped = len.clamp(0, MAX_LOG_BYTES as i32);
                let msg = read_host_string(&mut caller, ptr, capped);
                emit_log(level, &msg);
            },
        )
        .map_err(|e| FaucetError::Config(format!("wasm transform: linker log: {e}")))?;
    linker
        .func_wrap(
            "faucet_v1",
            "now_ns",
            |caller: Caller<'_, HostState>| -> i64 {
                caller.data().epoch_base.elapsed().as_nanos() as i64
            },
        )
        .map_err(|e| FaucetError::Config(format!("wasm transform: linker now_ns: {e}")))?;
    Ok(linker)
}

fn emit_log(level: i32, msg: &str) {
    match level {
        0 => tracing::trace!(target: "faucet::transform::wasm::module", "{msg}"),
        1 => tracing::debug!(target: "faucet::transform::wasm::module", "{msg}"),
        2 => tracing::info!(target: "faucet::transform::wasm::module", "{msg}"),
        3 => tracing::warn!(target: "faucet::transform::wasm::module", "{msg}"),
        _ => tracing::error!(target: "faucet::transform::wasm::module", "{msg}"),
    }
}

/// Best-effort read of a UTF-8 string from module memory for a host call.
/// Never traps the caller — a bad pointer just yields an empty string.
fn read_host_string(caller: &mut Caller<'_, HostState>, ptr: i32, len: i32) -> String {
    let Some(Extern::Memory(mem)) = caller.get_export("memory") else {
        return String::new();
    };
    let (ptr, len) = (ptr as usize, len as usize);
    let data = mem.data(&caller);
    match data.get(ptr..ptr.saturating_add(len)) {
        Some(slice) => String::from_utf8_lossy(slice).into_owned(),
        None => String::new(),
    }
}

/// Look up the exported linear memory named `memory`.
pub(crate) fn memory_export(
    store: &mut Store<HostState>,
    instance: &wasmtime::Instance,
) -> Result<Memory, FaucetError> {
    instance.get_memory(store, "memory").ok_or_else(|| {
        FaucetError::Config(
            "wasm transform: module does not export a linear memory named 'memory'".to_owned(),
        )
    })
}

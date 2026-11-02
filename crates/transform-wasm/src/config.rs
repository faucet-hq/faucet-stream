//! Config types for the WASM transform. No I/O or wasmtime here.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the `wasm` transform.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct WasmTransformConfig {
    /// Filesystem path to the module (absolute, or relative to the working
    /// directory): a compiled `.wasm` binary or WebAssembly text (`.wat`).
    /// URLs are not supported in v1.
    #[schemars(extend("x-faucet-resource" = "path"))]
    pub module: String,
    /// Name of the exported transform function. Default `"transform"`.
    #[serde(default = "default_function")]
    pub function: String,
    /// Linear-memory cap for the module, in mebibytes. A record that grows
    /// memory past this limit fails that record (routed by `on_error`).
    /// Default `16`, at most `4096`. The practical lower bound for ~1 KB JSON
    /// records is ~8 MB.
    #[serde(default = "default_memory_limit_mb")]
    pub memory_limit_mb: u32,
    /// wasmtime fuel budget per record — a deterministic CPU bound. A record
    /// that exhausts its fuel fails (routed by `on_error`). Default
    /// `10_000_000`, at most `10_000_000_000`. Fuel is the effective CPU limit
    /// in v1 (there are no blocking host calls, so a wall-clock timeout would be
    /// redundant).
    #[serde(default = "default_fuel_limit")]
    pub fuel_limit: u64,
    /// What to do when a record fails inside the module (trap, fuel/memory
    /// exhaustion, ABI violation, or non-JSON output). Default `fail`.
    #[serde(default)]
    pub on_error: WasmOnError,
    /// Re-stat the module file's mtime before each page; recompile and swap in
    /// the new module atomically at the page boundary if it changed. In-flight
    /// records within a page always use one module. Default `false`.
    #[serde(default)]
    pub reload_on_change: bool,
}

/// Policy for a per-record module failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WasmOnError {
    /// Abort the whole run with a [`faucet_core::FaucetError::Transform`]
    /// (the default; matches every other transform's fail-fast behaviour).
    #[default]
    Fail,
    /// Drop the failing record and continue (a warning + metric is emitted).
    Skip,
    /// Emit the record unchanged and continue (a warning + metric is emitted).
    Passthrough,
}

fn default_function() -> String {
    "transform".to_owned()
}

fn default_memory_limit_mb() -> u32 {
    16
}

fn default_fuel_limit() -> u64 {
    10_000_000
}

/// The largest `memory_limit_mb`: the whole 32-bit linear address space.
pub const MAX_MEMORY_LIMIT_MB: u32 = 4096;

/// The largest per-record `fuel_limit`.
pub const MAX_FUEL_LIMIT: u64 = 10_000_000_000;

impl WasmTransformConfig {
    /// Refuse a memory or fuel limit of zero or above its ceiling.
    pub(crate) fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        if !(1..=MAX_MEMORY_LIMIT_MB).contains(&self.memory_limit_mb) {
            return Err(faucet_core::FaucetError::Config(format!(
                "wasm transform: memory_limit_mb must be between 1 and {MAX_MEMORY_LIMIT_MB}, got {}",
                self.memory_limit_mb
            )));
        }
        if !(1..=MAX_FUEL_LIMIT).contains(&self.fuel_limit) {
            return Err(faucet_core::FaucetError::Config(format!(
                "wasm transform: fuel_limit must be between 1 and {MAX_FUEL_LIMIT}, got {}",
                self.fuel_limit
            )));
        }
        Ok(())
    }

    /// The low-cardinality metric label for this module — the file basename.
    pub(crate) fn module_label(&self) -> String {
        std::path::Path::new(&self.module)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| self.module.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn limits_are_bounded() {
        let cfg = |mem: u32, fuel: u64| WasmTransformConfig {
            module: "m.wasm".into(),
            function: default_function(),
            memory_limit_mb: mem,
            fuel_limit: fuel,
            on_error: WasmOnError::Fail,
            reload_on_change: false,
        };
        assert!(cfg(16, 10_000_000).validate().is_ok());
        assert!(cfg(MAX_MEMORY_LIMIT_MB, MAX_FUEL_LIMIT).validate().is_ok());
        let err = cfg(MAX_MEMORY_LIMIT_MB + 1, 1)
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("memory_limit_mb"), "{err}");
        assert!(cfg(0, 1).validate().is_err());
        let err = cfg(16, MAX_FUEL_LIMIT + 1)
            .validate()
            .unwrap_err()
            .to_string();
        assert!(err.contains("fuel_limit"), "{err}");
        assert!(cfg(16, 0).validate().is_err());
    }

    #[test]
    fn decodes_minimal_config_with_defaults() {
        let cfg: WasmTransformConfig =
            serde_json::from_value(json!({"module": "./t.wasm"})).unwrap();
        assert_eq!(cfg.module, "./t.wasm");
        assert_eq!(cfg.function, "transform");
        assert_eq!(cfg.memory_limit_mb, 16);
        assert_eq!(cfg.fuel_limit, 10_000_000);
        assert_eq!(cfg.on_error, WasmOnError::Fail);
        assert!(!cfg.reload_on_change);
    }

    #[test]
    fn decodes_full_config() {
        let cfg: WasmTransformConfig = serde_json::from_value(json!({
            "module": "/abs/redact.wasm",
            "function": "run",
            "memory_limit_mb": 32,
            "fuel_limit": 5000,
            "on_error": "skip",
            "reload_on_change": true
        }))
        .unwrap();
        assert_eq!(cfg.function, "run");
        assert_eq!(cfg.memory_limit_mb, 32);
        assert_eq!(cfg.fuel_limit, 5000);
        assert_eq!(cfg.on_error, WasmOnError::Skip);
        assert!(cfg.reload_on_change);
    }

    #[test]
    fn rejects_missing_module() {
        let err = serde_json::from_value::<WasmTransformConfig>(json!({})).unwrap_err();
        assert!(err.to_string().contains("module"), "{err}");
    }

    #[test]
    fn rejects_unknown_on_error() {
        let err = serde_json::from_value::<WasmTransformConfig>(
            json!({"module": "t.wasm", "on_error": "explode"}),
        )
        .unwrap_err();
        assert!(
            err.to_string().to_lowercase().contains("on_error")
                || err.to_string().contains("explode"),
            "{err}"
        );
    }

    #[test]
    fn module_label_is_basename() {
        let cfg: WasmTransformConfig =
            serde_json::from_value(json!({"module": "/a/b/redact_email.wasm"})).unwrap();
        assert_eq!(cfg.module_label(), "redact_email.wasm");
        let bare: WasmTransformConfig =
            serde_json::from_value(json!({"module": "x.wasm"})).unwrap();
        assert_eq!(bare.module_label(), "x.wasm");
    }

    #[test]
    fn schema_builds_with_module_property() {
        let schema = schemars::schema_for!(WasmTransformConfig);
        let json = serde_json::to_value(&schema).unwrap();
        assert!(
            json.get("properties")
                .and_then(|p| p.get("module"))
                .is_some()
        );
    }
}

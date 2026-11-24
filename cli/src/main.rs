//! `faucet` — binary entry point.
//!
//! The stock binary registers only the built-in connectors. To bundle
//! third-party `faucet-source-*` / `faucet-sink-*` connectors into your own
//! `faucet` binary, depend on `faucet-cli` as a library and call
//! [`faucet_cli::run_main`] with a [`PluginRegistry`](faucet_cli::registry::PluginRegistry)
//! that has your connectors registered — see `cli/examples/custom-cli/`.

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> std::process::ExitCode {
    faucet_cli::run_main(faucet_cli::registry::PluginRegistry::with_builtins())
}

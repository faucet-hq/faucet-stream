//! Builds the test-only `echo.proto` into both:
//!
//! 1. Generated tonic server + prost client code (used by integration tests
//!    in `tests/`).
//! 2. A `FileDescriptorSet` binary blob that `GrpcStream` loads at runtime,
//!    so the dynamic-proto path is exercised against the same schema.
//!
//! The descriptor set is written to `$OUT_DIR/echo_descriptor.bin`; the
//! generated client/server module is written to `$OUT_DIR/faucet.test.echo.rs`.
//! Integration tests resolve both paths via `env!("OUT_DIR")`.

#[allow(unsafe_code)] // sets PROTOC for prost-build
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    let descriptor_path = out_dir.join("echo_descriptor.bin");

    // Use the vendored protoc so the build isn't affected by an outdated or
    // broken system protoc (e.g. abseil ABI mismatches on macOS Homebrew).
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // SAFETY: build.rs is single-threaded; setting PROTOC pre-invocation is
    // the documented way to override prost-build's protoc binary.
    unsafe {
        std::env::set_var("PROTOC", &protoc);
    }

    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .file_descriptor_set_path(&descriptor_path)
        .compile_protos(&["tests/proto/echo.proto"], &["tests/proto"])?;

    // Tests need the server + client codegen too; re-run with them enabled
    // (tonic-build can emit both in a single call but doing it twice keeps
    // the descriptor-only invocation clearly scoped).
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["tests/proto/echo.proto"], &["tests/proto"])?;

    println!("cargo:rerun-if-changed=tests/proto/echo.proto");
    println!(
        "cargo:rustc-env=ECHO_DESCRIPTOR_PATH={}",
        descriptor_path.display()
    );
    Ok(())
}

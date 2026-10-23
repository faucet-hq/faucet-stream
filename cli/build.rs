fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Windows gives the main thread 1 MiB; the command futures need more.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("windows-msvc") {
        println!("cargo:rustc-link-arg-bins=/STACK:16777216");
    } else if target.contains("windows-gnu") {
        println!("cargo:rustc-link-arg-bins=-Wl,--stack,16777216");
    } else if target == "aarch64-apple-darwin" {
        // arm64 calls reach ±128 MiB; ld only adds branch islands within __text.
        println!(
            "cargo:rustc-link-arg-bins=-Wl,-rename_section,__TEXT,__text_startup,__TEXT,__text"
        );
    }
}

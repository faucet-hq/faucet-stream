fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    // Windows gives the main thread 1 MiB; the command futures need more.
    let target = std::env::var("TARGET").unwrap_or_default();
    if target.contains("windows-msvc") {
        println!("cargo:rustc-link-arg-bins=/STACK:16777216");
    } else if target.contains("windows-gnu") {
        println!("cargo:rustc-link-arg-bins=-Wl,--stack,16777216");
    }
}

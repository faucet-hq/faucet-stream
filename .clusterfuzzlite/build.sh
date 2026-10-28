#!/bin/bash -eu
# Builds every cargo-fuzz target in fuzz/ and copies it to $OUT.

# The workspace needs a newer compiler (rust-version 1.99) than the nightly the
# base image ships; use the dated nightly the targets were verified with.
NIGHTLY=nightly-2026-10-27
rustup toolchain install "$NIGHTLY" --profile minimal
export RUSTUP_TOOLCHAIN="$NIGHTLY"

cd "$SRC/faucet-stream"
cargo fuzz build -O
for target in $(cargo fuzz list); do
  cp "fuzz/target/x86_64-unknown-linux-gnu/release/$target" "$OUT/"
done

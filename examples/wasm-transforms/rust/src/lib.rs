//! Reference faucet-stream WASM transform, written in Rust.
//!
//! It implements the v1 faucet WASM transform ABI (see the crate docs for
//! `faucet-transform-wasm`). Per record it:
//!   * parses the input JSON object,
//!   * uppercases a top-level string field `name` if present,
//!   * stamps `wasm_processed: true`,
//!   * returns the re-serialized JSON.
//!
//! A record whose top-level value is not a JSON object is reported as an error
//! (the host routes it per the transform's `on_error` policy). Returning `0`
//! would instead drop (filter) the record — see the commented example below.
//!
//! Build: `cargo build --release --target wasm32-unknown-unknown`
//! Output: `target/wasm32-unknown-unknown/release/faucet_wasm_example_add_field.wasm`

use serde_json::Value;
use std::alloc::{alloc as sys_alloc, dealloc, Layout};
use std::slice;

/// The last error message, read by the host through `error_ptr` / `error_len`.
/// One buffer is reused for every error, so failing records never grow the
/// heap. Single-threaded wasm, so the `static mut` access is sound.
static mut LAST_ERROR: Vec<u8> = Vec::new();

/// Allocate `len` bytes for the host to write the input into.
///
/// # Safety
/// The host must pass the same `len` to `free` when releasing this buffer.
#[no_mangle]
pub extern "C" fn alloc(len: usize) -> *mut u8 {
    if len == 0 {
        return 1 as *mut u8; // non-null dangling; nothing will be read/written
    }
    // SAFETY: len > 0, alignment 1 is always valid.
    unsafe { sys_alloc(Layout::from_size_align_unchecked(len, 1)) }
}

/// Free a buffer previously returned by `alloc` (or by `transform` for output).
///
/// # Safety
/// `ptr`/`len` must come from a prior `alloc`/`transform` of exactly `len`.
#[no_mangle]
pub unsafe extern "C" fn free(ptr: *mut u8, len: usize) {
    if len == 0 || ptr.is_null() {
        return;
    }
    dealloc(ptr, Layout::from_size_align_unchecked(len, 1));
}

/// Offset of the last error message in linear memory.
#[no_mangle]
pub extern "C" fn error_ptr() -> *const u8 {
    unsafe { (*std::ptr::addr_of!(LAST_ERROR)).as_ptr() }
}

/// Length of the last error message.
#[no_mangle]
pub extern "C" fn error_len() -> usize {
    unsafe { (*std::ptr::addr_of!(LAST_ERROR)).len() }
}

/// Transform one record. Returns the packed `(out_ptr << 32) | out_len`, `0`
/// to drop the record, or `u64::MAX` to signal an error.
///
/// # Safety
/// `input_ptr`/`input_len` must describe a buffer previously handed to `alloc`.
#[no_mangle]
pub unsafe extern "C" fn transform(input_ptr: *const u8, input_len: usize) -> u64 {
    let input = slice::from_raw_parts(input_ptr, input_len);
    let mut value: Value = match serde_json::from_slice(input) {
        Ok(v) => v,
        Err(e) => return set_error(&format!("invalid input JSON: {e}")),
    };

    let Some(obj) = value.as_object_mut() else {
        return set_error("expected a JSON object");
    };

    if let Some(Value::String(name)) = obj.get("name") {
        let upper = name.to_uppercase();
        obj.insert("name".to_owned(), Value::String(upper));
    }
    obj.insert("wasm_processed".to_owned(), Value::Bool(true));

    // To filter a record instead, return 0 here, e.g.:
    //   if obj.get("drop_me").is_some() { return 0; }

    let out = match serde_json::to_vec(&value) {
        Ok(b) => b,
        Err(e) => return set_error(&format!("serialize output: {e}")),
    };
    emit(out)
}

/// Copy `bytes` into a fresh module-owned buffer and pack the pointer + length.
fn emit(bytes: Vec<u8>) -> u64 {
    let len = bytes.len();
    if len == 0 {
        // Empty but non-drop: point at a dangling non-null with len 0.
        return 1u64 << 32;
    }
    let ptr = alloc(len);
    // SAFETY: `ptr` has room for `len` bytes just allocated above.
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, len);
    }
    ((ptr as u64) << 32) | (len as u64)
}

/// Store `msg` for the host to read and return the error sentinel.
fn set_error(msg: &str) -> u64 {
    // SAFETY: single-threaded; no other reference to LAST_ERROR is live.
    let buf = unsafe { &mut *std::ptr::addr_of_mut!(LAST_ERROR) };
    buf.clear();
    buf.extend_from_slice(msg.as_bytes());
    u64::MAX
}

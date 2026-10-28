;; Reference faucet-stream WASM transform, written by hand in the WebAssembly
;; text format, so the repository carries no compiled binary. faucet loads a
;; `.wat` module as readily as a `.wasm` one.
;;
;; Per record it uppercases the ASCII letters of a top-level string field
;; `name` and stamps `wasm_processed: true`; a record that is not a JSON object
;; is an error. The Rust and AssemblyScript references next to this file do
;; the same with a JSON library (and full Unicode case mapping).
;;
;; The host hands each record over as compact JSON (`serde_json::to_vec`), so
;; the module edits the bytes instead of parsing them: it copies the record,
;; tracks string and nesting state to find the top-level `name` value, and
;; appends `"wasm_processed":true` before the closing brace. A key that is
;; already present is written again; the host keeps the last value.
(module
  (memory (export "memory") 1)

  (data (i32.const 16) "expected a JSON object")
  ;; The tail written in place of the closing brace; the comma is skipped
  ;; for an empty object.
  (data (i32.const 64) ",\"wasm_processed\":true}")

  (global $heap (mut i32) (i32.const 1024))

  ;; Bump-allocate `len` bytes, growing memory when the record needs it.
  (func $bump (param $len i32) (result i32)
    (local $ptr i32)
    (local $end i32)
    (local $have i32)
    (local.set $ptr (global.get $heap))
    (local.set $end (i32.add (local.get $ptr) (local.get $len)))
    (local.set $have (i32.mul (memory.size) (i32.const 65536)))
    (if (i32.gt_u (local.get $end) (local.get $have))
      (then
        (if (i32.eq
              (memory.grow
                (i32.shr_u
                  (i32.add (i32.sub (local.get $end) (local.get $have)) (i32.const 65535))
                  (i32.const 16)))
              (i32.const -1))
          (then unreachable))))
    (global.set $heap (local.get $end))
    (local.get $ptr))

  ;; The host runs one record at a time and has read the previous record's
  ;; output before it allocates the next input, so every input starts the
  ;; heap over and memory stays bounded by the largest record.
  (func (export "alloc") (param $len i32) (result i32)
    (global.set $heap (i32.const 1024))
    (call $bump (local.get $len)))

  (func (export "free") (param i32) (param i32))

  (func (export "error_ptr") (result i32) (i32.const 16))
  (func (export "error_len") (result i32) (i32.const 22))

  (func $is_space (param $c i32) (result i32)
    (i32.or
      (i32.or (i32.eq (local.get $c) (i32.const 32)) (i32.eq (local.get $c) (i32.const 9)))
      (i32.or (i32.eq (local.get $c) (i32.const 10)) (i32.eq (local.get $c) (i32.const 13)))))

  (func (export "transform") (param $in i32) (param $len i32) (result i64)
    (local $out i32)
    (local $last i32)
    (local $i i32)
    (local $at i32)
    (local $c i32)
    (local $depth i32)
    (local $in_str i32)
    (local $expect_key i32)
    (local $is_key i32)
    (local $key_start i32)
    (local $after_name i32)
    (local $upper i32)
    (local $members i32)
    (local $total i32)

    ;; A JSON object: at least `{}`, opening and closing with braces.
    (if (i32.lt_u (local.get $len) (i32.const 2))
      (then (return (i64.const -1))))
    (local.set $last (i32.sub (local.get $len) (i32.const 1)))
    (if (i32.or
          (i32.ne (i32.load8_u (local.get $in)) (i32.const 123))
          (i32.ne (i32.load8_u (i32.add (local.get $in) (local.get $last))) (i32.const 125)))
      (then (return (i64.const -1))))

    ;; Everything but the closing brace, then the tail.
    (local.set $out (call $bump (i32.add (local.get $len) (i32.const 23))))
    (memory.copy (local.get $out) (local.get $in) (local.get $last))

    (block $done
      (loop $scan
        (br_if $done (i32.ge_u (local.get $i) (local.get $last)))
        (local.set $at (i32.add (local.get $out) (local.get $i)))
        (local.set $c (i32.load8_u (local.get $at)))
        (if (local.get $in_str)
          (then
            (if (i32.eq (local.get $c) (i32.const 92))
              ;; A backslash: the escaped byte is kept as written.
              (then (local.set $i (i32.add (local.get $i) (i32.const 1))))
              (else
                (if (i32.eq (local.get $c) (i32.const 34))
                  (then
                    (local.set $in_str (i32.const 0))
                    (local.set $upper (i32.const 0))
                    ;; A key closed: is it exactly `name`?
                    (if (local.get $is_key)
                      (then
                        (local.set $after_name
                          (i32.and
                            (i32.eq (i32.sub (local.get $i) (local.get $key_start)) (i32.const 4))
                            (i32.eq
                              (i32.load (i32.add (local.get $out) (local.get $key_start)))
                              (i32.const 0x656d616e)))))))
                  (else
                    (if (i32.and
                          (local.get $upper)
                          (i32.and
                            (i32.ge_u (local.get $c) (i32.const 97))
                            (i32.le_u (local.get $c) (i32.const 122))))
                      (then
                        (i32.store8 (local.get $at) (i32.sub (local.get $c) (i32.const 32))))))))))
          (else
            (if (i32.eq (local.get $c) (i32.const 34))
              (then
                ;; A string opens: a key when one is expected, the `name`
                ;; value when it follows that key.
                (local.set $in_str (i32.const 1))
                (local.set $is_key (local.get $expect_key))
                (local.set $members (i32.or (local.get $members) (local.get $expect_key)))
                (local.set $upper (local.get $after_name))
                (local.set $expect_key (i32.const 0))
                (local.set $after_name (i32.const 0))
                (local.set $key_start (i32.add (local.get $i) (i32.const 1))))
              (else
                (if (i32.or (i32.eq (local.get $c) (i32.const 123)) (i32.eq (local.get $c) (i32.const 91)))
                  (then
                    (local.set $depth (i32.add (local.get $depth) (i32.const 1)))
                    (local.set $expect_key
                      (i32.and (i32.eq (local.get $c) (i32.const 123)) (i32.eq (local.get $depth) (i32.const 1)))))
                  (else
                    (if (i32.or (i32.eq (local.get $c) (i32.const 125)) (i32.eq (local.get $c) (i32.const 93)))
                      (then (local.set $depth (i32.sub (local.get $depth) (i32.const 1))))
                      (else
                        (if (i32.and (i32.eq (local.get $c) (i32.const 44)) (i32.eq (local.get $depth) (i32.const 1)))
                          (then (local.set $expect_key (i32.const 1))))))))
                ;; Any value other than a string ends the wait for `name`'s.
                (if (i32.eqz
                      (i32.or (i32.eq (local.get $c) (i32.const 58)) (call $is_space (local.get $c))))
                  (then (local.set $after_name (i32.const 0))))))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $scan)))

    (if (local.get $members)
      (then
        (memory.copy (i32.add (local.get $out) (local.get $last)) (i32.const 64) (i32.const 23))
        (local.set $total (i32.add (local.get $last) (i32.const 23))))
      (else
        (memory.copy (i32.add (local.get $out) (local.get $last)) (i32.const 65) (i32.const 22))
        (local.set $total (i32.add (local.get $last) (i32.const 22)))))

    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
      (i64.extend_i32_u (local.get $total)))))

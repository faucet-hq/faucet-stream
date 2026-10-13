#!/usr/bin/env python3
"""Fail when a connector converts a decimal through a float (RFC 0013, #885).

An exact decimal (`DECIMAL`, `NUMERIC`, `MONEY`, `NUMBER`, Arrow `Decimal*`,
BSON `Decimal128`, `BigDecimal`, `rust_decimal`) rendered through `f64` loses
digits silently. `faucet_core::fidelity` is the one place that may touch a float
on a decimal path (it checks exactness); everything else renders through it.

The check reads every non-test Rust source file under `crates/` and `cli/src/`
(skipping `tests/`, `benches/`, `examples/` and trailing `#[cfg(test)]`
modules) and flags a line holding a float conversion (`as f64`, `.to_f64()`,
`f64::from(`, `parse::<f64>`, `from_f64(`, and the `f32` forms) when a decimal
marker appears within WINDOW lines of it.

A line that converts a float that is genuinely not a decimal (a metric, a
duration) but sits near one is listed in `scripts/decimal-f64-allow.txt` as
`<path>: <text the line contains> -- <reason>`. An entry that matches nothing
fails the run, so the list cannot go stale.

Usage: scripts/decimal-f64.py   (from the repo root)
"""

import os
import re
import sys

ALLOW = "scripts/decimal-f64-allow.txt"
ROOTS = ("crates", "cli/src")
SKIP_DIRS = {"tests", "benches", "examples", "target", "fixtures"}
OWNERS = {"crates/core/src/fidelity.rs", "crates/core/src/json_numbers.rs"}
WINDOW = 3

FLOAT = re.compile(
    r"\bas\s+f(?:32|64)\b|\.to_f(?:32|64)\(|\bf(?:32|64)::(?:from|try_from)\(|"
    r"parse::<f(?:32|64)>|\bfrom_f(?:32|64)\("
)
DECIMAL = re.compile(
    r"[Dd]ecimal|NUMERIC|[Nn]umeric\b|MONEY|[Mm]oney\b|BigDecimal|rust_decimal|NUMBER\b"
)


def strip_test_module(text):
    m = re.search(r"#\[cfg\(test\)\]\s*(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{", text)
    return text[: m.start()] if m else text


def code_of(line):
    return re.sub(r"//.*", "", line)


def scan_file(path, text):
    """[(line_no, line)] of float conversions near a decimal marker."""
    lines = [code_of(l) for l in strip_test_module(text).split("\n")]
    hits = []
    for i, line in enumerate(lines):
        if not FLOAT.search(line):
            continue
        window = "\n".join(lines[max(0, i - WINDOW) : i + WINDOW + 1])
        if DECIMAL.search(window):
            hits.append((i + 1, line.strip()))
    return hits


def rust_files(root):
    for base in ROOTS:
        for dirpath, dirs, files in os.walk(os.path.join(root, base)):
            dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
            for f in sorted(files):
                if f.endswith(".rs"):
                    yield os.path.join(dirpath, f)


def parse_allow(path):
    out, errors = [], []
    if not os.path.exists(path):
        return out, errors
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            head, sep, reason = line.partition(" -- ")
            file, colon, needle = head.partition(": ")
            if not sep or not colon or not reason.strip() or not needle.strip():
                errors.append(f"{path}:{n}: expected `<path>: <line text> -- <reason>`")
                continue
            out.append((file.strip(), needle.strip()))
    return out, errors


def check(root="."):
    allow, errors = parse_allow(os.path.join(root, ALLOW))
    used = set()
    for path in rust_files(root):
        rel = os.path.relpath(path, root)
        if rel in OWNERS:
            continue
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
        for line_no, line in scan_file(rel, text):
            matched = [a for a in allow if a[0] == rel and a[1] in line]
            if matched:
                used.update(matched)
                continue
            errors.append(f"{rel}:{line_no}: float conversion beside a decimal: {line}")
    for entry in allow:
        if entry not in used:
            errors.append(f"{ALLOW}: `{entry[0]}: {entry[1]}` matches no flagged line — remove it")
    return errors


def main():
    errors = check()
    if errors:
        print("decimal-f64: a decimal may be converted through a float:")
        for e in errors:
            print(f"  - {e}")
        print(
            "\nRender decimals through faucet_core::fidelity (decimal_text / decimal_from_i128 / "
            f"number_text), or list a line that is not a decimal path in {ALLOW} with the reason."
        )
        return 1
    print("decimal-f64: no decimal is converted through a float")
    return 0


if __name__ == "__main__":
    sys.exit(main())

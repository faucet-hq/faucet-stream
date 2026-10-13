#!/usr/bin/env python3
"""Fail when a connector does not run the value-fidelity corpus (RFC 0013, #885).

Every source and sink crate under `crates/source/` and `crates/sink/` must run
the shared corpus of `faucet_conformance::fidelity` — the values that have
broken a pipeline somewhere — in one of three forms:

- a source/sink pair round trip in `crates/interop-tests/tests/`;
- a one-sided run against the backend (seed the corpus, read it with the
  source; or write it with the sink and read the backend back) in the crate's
  own `tests/`;
- the crate's own value converter, in a `#[cfg(test)]` module of its `src/`.

A test file counts for a connector when it calls a corpus assertion
(`assert_round_trip` / `diff_round_trip`, or `fidelity::` assertions named in
ASSERTIONS) and either belongs to the connector's crate or — for
`crates/interop-tests/tests/` and `crates/conformance/tests/` — names the
connector's crate (`faucet_sink_postgres`).

A connector that cannot run it lists the reason in
`crates/conformance/fidelity-exempt.txt` as `<crate>: <reason>`. An exemption
for a crate that is covered, or that does not exist, fails the run, so the list
cannot go stale.

Usage: scripts/fidelity-coverage.py   (from the repo root)
"""

import os
import re
import sys

EXEMPT = "crates/conformance/fidelity-exempt.txt"
SHARED = ("crates/interop-tests/tests", "crates/conformance/tests")
ROLES = ("crates/source", "crates/sink")
ASSERTIONS = ("assert_round_trip", "diff_round_trip", "assert_corpus_read", "assert_corpus_written")


def strip_comments(text):
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def runs_corpus(text):
    text = strip_comments(text)
    return any(re.search(r"\b" + a + r"\s*\(", text) for a in ASSERTIONS)


def crate_name(crate_dir):
    with open(os.path.join(crate_dir, "Cargo.toml"), encoding="utf-8") as fh:
        m = re.search(r'^name\s*=\s*"([^"]+)"', fh.read(), re.M)
    return m.group(1) if m else os.path.basename(crate_dir)


def rust_files(path):
    for dirpath, dirs, files in os.walk(path):
        dirs.sort()
        for f in sorted(files):
            if f.endswith(".rs"):
                yield os.path.join(dirpath, f)


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def connectors(root):
    out = {}
    for base in ROLES:
        full = os.path.join(root, base)
        if not os.path.isdir(full):
            continue
        for kind in sorted(os.listdir(full)):
            crate_dir = os.path.join(full, kind)
            if os.path.isfile(os.path.join(crate_dir, "Cargo.toml")):
                out[crate_name(crate_dir)] = crate_dir
    return out


def own_coverage(crate_dir):
    for sub in ("tests", "src"):
        for path in rust_files(os.path.join(crate_dir, sub)):
            if runs_corpus(read(path)):
                return os.path.relpath(path, os.path.dirname(os.path.dirname(os.path.dirname(crate_dir))))
    return None


def shared_coverage(root):
    """{crate ident: [test file]} for the shared test directories."""
    out = {}
    for base in SHARED:
        for path in rust_files(os.path.join(root, base)):
            text = read(path)
            if not runs_corpus(text):
                continue
            for ident in set(re.findall(r"\bfaucet_(?:source|sink)_[a-z0-9_]+", strip_comments(text))):
                out.setdefault(ident, []).append(os.path.relpath(path, root))
    return out


def parse_exemptions(path):
    out, errors = {}, []
    if not os.path.exists(path):
        return out, errors
    with open(path, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            crate, sep, reason = line.partition(":")
            if not sep or not reason.strip() or len(crate.split()) != 1:
                errors.append(f"{path}:{n}: expected `<crate>: <reason>`")
                continue
            out[crate.strip()] = reason.strip()
    return out, errors


def check(root="."):
    exempt, errors = parse_exemptions(os.path.join(root, EXEMPT))
    crates = connectors(root)
    shared = shared_coverage(root)
    for crate, crate_dir in sorted(crates.items()):
        covered = own_coverage(crate_dir) or shared.get(crate.replace("-", "_"))
        if crate in exempt:
            if covered:
                errors.append(f"{crate}: exempt, but it runs the fidelity corpus — remove the exemption")
            continue
        if not covered:
            errors.append(f"{crate}: does not run the fidelity corpus")
    for crate in sorted(exempt):
        if crate not in crates:
            errors.append(f"{EXEMPT}: `{crate}` is not a connector crate")
    return errors


def main():
    errors = check()
    if errors:
        print("fidelity coverage: connectors without the value-fidelity corpus:")
        for e in errors:
            print(f"  - {e}")
        print(
            "\nRun faucet_conformance::fidelity's corpus for the connector (a pair in "
            "crates/interop-tests/tests/, a one-sided test in the crate's tests/, or its "
            f"converter in a #[cfg(test)] module), or list the reason it cannot in {EXEMPT}. "
            "See docs/book/src/reference/value-fidelity.md."
        )
        return 1
    print("fidelity coverage: every connector runs the value-fidelity corpus")
    return 0


if __name__ == "__main__":
    sys.exit(main())

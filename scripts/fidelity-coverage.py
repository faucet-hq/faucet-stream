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
ASSERTIONS) outside an `#[ignore]`d test, and either belongs to the connector's
crate or — for `crates/interop-tests/tests/` and `crates/conformance/tests/` —
uses the connector's crate (`faucet_sink_postgres`, or a name imported from it)
inside a function that runs the assertion, directly or through same-file
helpers.

A connector that cannot run it lists the reason in
`crates/conformance/fidelity-exempt.txt` as `<crate>: <reason>`. An exemption
for a crate that is covered, or that does not exist, fails the run, so the list
cannot go stale.

Usage: scripts/fidelity-coverage.py   (from the repo root)
"""

import importlib.util
import os
import re
import sys

EXEMPT = "crates/conformance/fidelity-exempt.txt"
SHARED = ("crates/interop-tests/tests", "crates/conformance/tests")
ROLES = ("crates/source", "crates/sink")
ASSERTIONS = ("assert_round_trip", "diff_round_trip", "assert_corpus_read", "assert_corpus_written")


_cc_spec = importlib.util.spec_from_file_location(
    "conformance_coverage", os.path.join(os.path.dirname(os.path.abspath(__file__)), "conformance-coverage.py")
)
_cc = importlib.util.module_from_spec(_cc_spec)
_cc_spec.loader.exec_module(_cc)
FN = re.compile(r"\bfn\s+(\w+)[^{;]*\{")
CRATE = r"\bfaucet_(?:source|sink)_[a-z0-9_]+"


def clean(text):
    """Comments, string contents and `#[ignore]`d tests removed."""
    return _cc.drop_ignored_tests(_cc.strip_comments(text))


def asserts(text):
    return any(re.search(r"\b" + a + r"\s*\(", text) for a in ASSERTIONS)


def runs_corpus(text):
    return asserts(clean(text))


def functions(text):
    """{name: body} for every `fn` in cleaned source (nested bodies included)."""
    out = {}
    for m in FN.finditer(text):
        depth, i = 1, m.end()
        while i < len(text) and depth:
            depth += {"{": 1, "}": -1}.get(text[i], 0)
            i += 1
        out[m.group(1)] = out.get(m.group(1), "") + text[m.end() : i - 1]
    return out


def impl_blocks(text):
    """{type: body of every `impl` block for it} — a value of that type reaches
    its trait methods without naming them."""
    out = {}
    for m in re.finditer(r"\bimpl\b(?:<[^>]*>)?\s*(?:[\w:<>, ]+\s+for\s+)?(\w+)[^{;]*\{", text):
        depth, i = 1, m.end()
        while i < len(text) and depth:
            depth += {"{": 1, "}": -1}.get(text[i], 0)
            i += 1
        out[m.group(1)] = out.get(m.group(1), "") + text[m.end() : i - 1]
    return out


def imported_names(text):
    """{name: crate ident} for the items a file imports from connector crates."""
    out = {}
    for m in re.finditer(r"\buse\s+(" + CRATE + r")\s*::([^;]*);", text):
        crate, tail = m.group(1), m.group(2)
        for name in re.findall(r"\b(?:as\s+)?([A-Za-z_]\w*)\s*(?=[,}]|$)", tail):
            out[name] = crate
    return out


def credited_crates(text):
    """Connector crates used by a function that (directly or through helpers in
    the same file) runs a corpus assertion."""
    text = clean(text)
    fns = functions(text)
    names = imported_names(text)
    impls = impl_blocks(text)

    def direct(body):
        found = set(re.findall(CRATE, body))
        found |= {names[n] for n in re.findall(r"\b[A-Za-z_]\w*\b", body) if n in names}
        return found

    def mentions(body):
        found = direct(body)
        for ty in set(re.findall(r"\b[A-Z]\w*\b", body)) & impls.keys():
            found |= direct(impls[ty])
        return found

    callees = {f: {g for g in fns if g != f and re.search(r"\b" + g + r"\s*(::<[^>]*>)?\s*\(", body)} for f, body in fns.items()}
    reach = {}
    for f in fns:
        seen, stack = set(), [f]
        while stack:
            g = stack.pop()
            if g not in seen:
                seen.add(g)
                stack.extend(callees[g])
        reach[f] = seen
    out = set()
    for f in fns:
        if any(asserts(fns[g]) for g in reach[f]):
            for g in reach[f]:
                out |= mentions(fns[g])
    return out


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
            for ident in credited_crates(read(path)):
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

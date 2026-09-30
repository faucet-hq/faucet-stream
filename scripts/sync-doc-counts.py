#!/usr/bin/env python3
"""Single source of truth for connector/crate counts across the docs.

The counts (sources, sinks, connectors, common libraries, total crates) are
*derived from the crate directories on disk* — there is no number to hand-edit.
Every doc that states a count wraps it in a sentinel span:

    <!--COUNT:sources-->37<!--/COUNT-->

The HTML comments are invisible in rendered Markdown (GitHub, docs.rs, mdBook),
so the visible text is just "37". This script re-renders the value inside every
span from the live crate count.

Usage:
    python3 scripts/sync-doc-counts.py            # render (rewrite files in place)
    python3 scripts/sync-doc-counts.py --check     # verify, non-zero exit on drift (CI)

Valid keys: sources, sinks, connectors, common, crates, libraries.
Add a new count site by wrapping its number in a `<!--COUNT:<key>-->…<!--/COUNT-->`
span — this script (and the CI `doc-counts` job) then keep it in sync forever.
"""

from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SENTINEL = re.compile(r"(<!--COUNT:(?P<key>[a-z]+)-->)(?P<val>.*?)(<!--/COUNT-->)", re.DOTALL)
# Pruned by directory name anywhere in the tree.
SKIP_DIRS = {".git", "target", "node_modules"}
# Pruned by exact repo-relative path (the mdBook *rendered output* — its `src/`
# sibling holds the sources we DO want to process).
SKIP_PATHS = {os.path.join("docs", "book", "book")}


def counts() -> dict[str, int]:
    def dirs(rel: str) -> int:
        p = os.path.join(ROOT, rel)
        return sum(
            1
            for e in os.listdir(p)
            if os.path.isdir(os.path.join(p, e))
        ) if os.path.isdir(p) else 0

    sources = dirs("crates/source")
    sinks = dirs("crates/sink")
    common = dirs("crates/common")
    # Every published crate in the workspace = one Cargo.toml under crates/**
    # (minus `publish = false` test crates) plus the umbrella (faucet-stream)
    # and the CLI (cli).
    crate_manifests = 0
    for dirpath, dirnames, filenames in os.walk(os.path.join(ROOT, "crates")):
        if "Cargo.toml" in filenames:
            with open(os.path.join(dirpath, "Cargo.toml")) as f:
                if re.search(r"^publish\s*=\s*false", f.read(), re.M):
                    continue
            crate_manifests += 1
    crates = crate_manifests + 2  # + faucet-stream + cli
    return {
        "sources": sources,
        "sinks": sinks,
        "connectors": sources + sinks,
        "common": common,
        "crates": crates,
        "libraries": crates - 1,  # all crates except the `faucet` CLI binary
    }


def iter_doc_files():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [
            d
            for d in dirnames
            if d not in SKIP_DIRS
            and os.path.relpath(os.path.join(dirpath, d), ROOT) not in SKIP_PATHS
        ]
        for fn in filenames:
            if fn.endswith(".md"):
                yield os.path.join(dirpath, fn)


def main() -> int:
    check = "--check" in sys.argv[1:]
    vals = counts()
    drift: list[str] = []
    unknown: list[str] = []
    changed = 0

    for path in iter_doc_files():
        with open(path, encoding="utf-8") as f:
            text = f.read()
        if "<!--COUNT:" not in text:
            continue

        def repl(m: re.Match) -> str:
            key = m.group("key")
            if key not in vals:
                unknown.append(f"{os.path.relpath(path, ROOT)}: unknown count key '{key}'")
                return m.group(0)
            want = str(vals[key])
            if m.group("val") != want:
                drift.append(
                    f"{os.path.relpath(path, ROOT)}: {key} = '{m.group('val')}' → '{want}'"
                )
            return f"{m.group(1)}{want}{m.group(4)}"

        new = SENTINEL.sub(repl, text)
        if new != text:
            changed += 1
            if not check:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(new)

    if unknown:
        print("Unknown count keys (valid: %s):" % ", ".join(sorted(vals)))
        for u in unknown:
            print("  ✗ " + u)
        return 1

    label = "  ".join(f"{k}={v}" for k, v in vals.items())
    print(f"Counts: {label}")

    if check:
        if drift:
            print("\nConnector-count drift — run `python3 scripts/sync-doc-counts.py` to fix:")
            for d in drift:
                print("  ✗ " + d)
            return 1
        print("OK: every <!--COUNT:*--> span is in sync with the crate directories.")
        return 0

    print(f"Rendered {changed} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

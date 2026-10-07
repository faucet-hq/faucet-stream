#!/usr/bin/env python3
"""Fail when a published crate would package a private key (#789 SUPPLY-20).

Test fixtures with key material (`*.pem` holding a PRIVATE KEY, `*.key`,
`*.p12`, `*.pfx`) must be kept out of the `.crate` with the manifest's
`exclude` (or `include`) list: a secret scanner flags the published crate
otherwise. Checks every publishable workspace member without running cargo.

Usage: scripts/packaged-keys.py   (run from the repo root)
"""

import fnmatch
import glob
import os
import sys
import tomllib

KEY_SUFFIXES = (".key", ".p12", ".pfx")


def is_key(path):
    if path.endswith(KEY_SUFFIXES):
        return True
    if path.endswith(".pem"):
        with open(path, "rb") as f:
            return b"PRIVATE KEY" in f.read()
    return False


def matches(rel, patterns):
    for p in patterns:
        p = p.lstrip("/")
        if fnmatch.fnmatch(rel, p) or rel.startswith(p.rstrip("*").rstrip("/") + "/"):
            return True
    return False


def packaged(rel, package):
    include = package.get("include")
    if include is not None:
        return matches(rel, include)
    return not matches(rel, package.get("exclude", []))


def violations(manifest_path):
    with open(manifest_path, "rb") as f:
        package = tomllib.load(f).get("package", {})
    if not package or package.get("publish") is False:
        return []
    root = os.path.dirname(manifest_path)
    found = []
    for dirpath, dirnames, files in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in ("target", ".git")]
        for name in files:
            path = os.path.join(dirpath, name)
            rel = os.path.relpath(path, root).replace(os.sep, "/")
            if is_key(path) and packaged(rel, package):
                found.append(f"{package.get('name')}: {rel}")
    return found


def main():
    manifests = ["faucet-stream/Cargo.toml", "cli/Cargo.toml"]
    manifests += glob.glob("crates/**/Cargo.toml", recursive=True)
    bad = [v for m in sorted(set(manifests)) for v in violations(m)]
    for v in bad:
        print(f"private key would be published: {v} (add it to the crate's `exclude`)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())

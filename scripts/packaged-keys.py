#!/usr/bin/env python3
"""Fail when a published crate would package a private key (#789 SUPPLY-20).

Test fixtures with key material (a key container — `*.key`, `*.p12`, `*.pfx`,
`*.der`, `*.jks`, `*.p8`, `id_rsa`… — or any text file, source included,
holding a PEM `PRIVATE KEY` block) must be kept out of the `.crate` with the
manifest's `exclude` (or `include`) list, or generated at test time: a secret
scanner flags the published crate otherwise. Checks every publishable workspace member without running cargo.

Usage: scripts/packaged-keys.py   (run from the repo root)
"""

import fnmatch
import glob
import os
import sys
import tomllib

import re

KEY_SUFFIXES = (".key", ".p12", ".pfx", ".der", ".jks", ".p8", ".keystore")
KEY_NAMES = ("id_rsa", "id_dsa", "id_ecdsa", "id_ed25519")
# A PEM private-key block with real key material: the header, then at least
# 64 base64 characters (newlines raw or written as `\\n` in source).
PEM_KEY = re.compile(rb"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----(?:\s|\\n)*(?:[A-Za-z0-9+/=]|\s|\\n){64,}")


def is_key(path):
    if path.endswith(KEY_SUFFIXES) or os.path.basename(path) in KEY_NAMES:
        return True
    with open(path, "rb") as f:
        head = f.read(4 << 20)
    if b"\0" in head[:8192]:
        return False
    return PEM_KEY.search(head) is not None


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

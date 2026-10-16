#!/usr/bin/env python3
"""Fail when a version-blind advisory ignore in deny.toml covers more than its reason says.

cargo-deny cannot scope an `ignore` to one version, so each such ignore is
listed here with the versions it was triaged for: a package version that the
advisory affects must be one of them. A new affected copy (a dependency
falling back to an old release) then fails CI instead of passing silently.

Usage: scripts/advisory-scope.py   (run from the repo root)
"""

import sys
import tomllib

# advisory ids -> (package, versions triaged, first fixed version)
SCOPED = {
    ("RUSTSEC-2026-0194", "RUSTSEC-2026-0195"): ("quick-xml", {"0.39.4"}, (0, 41, 0)),
}


def version_tuple(v):
    core = v.split("-", 1)[0].split("+", 1)[0]
    return tuple(int(p) for p in core.split("."))


def violations(lock, deny):
    ignored = {e["id"] if isinstance(e, dict) else e for e in deny.get("advisories", {}).get("ignore", [])}
    found = []
    for ids, (pkg, triaged, fixed) in SCOPED.items():
        if not ignored.intersection(ids):
            continue
        for p in lock.get("package", []):
            if p["name"] == pkg and version_tuple(p["version"]) < fixed and p["version"] not in triaged:
                found.append(f"{pkg} {p['version']} is affected by {', '.join(ids)} but was not triaged")
    return found


def main():
    with open("Cargo.lock", "rb") as f:
        lock = tomllib.load(f)
    with open("deny.toml", "rb") as f:
        deny = tomllib.load(f)
    found = violations(lock, deny)
    if found:
        print("A version-blind advisory ignore in deny.toml now covers an untriaged copy:")
        for line in found:
            print(f"  {line}")
        print("Upgrade the dependency, or triage the version and add it to scripts/advisory-scope.py.")
        return 1
    print("advisory-scope: every ignored advisory covers only triaged versions")
    return 0


if __name__ == "__main__":
    sys.exit(main())

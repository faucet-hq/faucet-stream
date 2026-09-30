#!/usr/bin/env python3
"""Fail when a published connector crate depends on another connector (#784).

A `faucet-source-*`, `faucet-sink-*` or `faucet-common-*` crate may depend on
`faucet-core`, its own `faucet-common-*` crate and third-party crates, in every
dependency section including `[dev-dependencies]` and target-specific tables.
A versioned edge to another connector makes `cargo publish` need that connector
on crates.io first, which the release order does not guarantee. Tests that need
two connectors live in `crates/interop-tests` (`publish = false`).

Usage: scripts/connector-deps.py   (run from the repo root)
"""

import glob
import sys
import tomllib

CONNECTOR_PREFIXES = ("faucet-source-", "faucet-sink-")
CHECKED_PREFIXES = CONNECTOR_PREFIXES + ("faucet-common-",)
SECTIONS = ("dependencies", "dev-dependencies", "build-dependencies")


def dependency_tables(manifest):
    for section in SECTIONS:
        yield section, manifest.get(section, {})
    for target, table in manifest.get("target", {}).items():
        for section in SECTIONS:
            yield f"target.{target}.{section}", table.get(section, {})


def violations(manifest):
    package = manifest.get("package", {})
    name = package.get("name", "")
    if not name.startswith(CHECKED_PREFIXES) or package.get("publish") is False:
        return []
    found = []
    for section, table in dependency_tables(manifest):
        for key, spec in table.items():
            dep = spec.get("package", key) if isinstance(spec, dict) else key
            if dep != name and dep.startswith(CONNECTOR_PREFIXES):
                found.append(f"{name}: [{section}] {dep}")
    return found


def workspace_manifests(root="Cargo.toml"):
    with open(root, "rb") as f:
        members = tomllib.load(f)["workspace"]["members"]
    for pattern in members:
        yield from sorted(glob.glob(f"{pattern}/Cargo.toml"))


def main():
    found = []
    for path in workspace_manifests():
        with open(path, "rb") as f:
            found += violations(tomllib.load(f))
    if found:
        print("Connector crates must not depend on other connector crates (#784).")
        print("Move the test that needs both into crates/interop-tests:")
        for line in found:
            print(f"  {line}")
        return 1
    print("connector-deps: no connector crate depends on another connector")
    return 0


if __name__ == "__main__":
    sys.exit(main())

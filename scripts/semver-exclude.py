#!/usr/bin/env python3
"""Print the comma-separated `exclude` list for the API-stability gate (#789
SUPPLY-18): the always-excluded packages plus every publishable workspace
crate that has no release on crates.io yet (nothing to compare against).
A crate drops out of the list by itself on its first release, so a published
crate can never stay unguarded because someone forgot to prune the list.

    scripts/semver-exclude.py                  # uses `cargo metadata`
    scripts/semver-exclude.py --default-features   # the ZIP_BLOCKED_EXCEL set
"""
import json
import subprocess
import sys
import urllib.error
import urllib.request

# faucet-cli / faucet-stream: binary + umbrella, checked through their parts.
ALWAYS = [
    "faucet-cli",
    "faucet-stream",
]

# #918: on 2026-10-16 every `zip` release but 9.x was yanked. Until calamine /
# rust_xlsxwriter and duckdb release on zip 9, these crates cannot resolve
# with every feature on. Remove each list when its upstream releases.
#
# Excel (calamine, rust_xlsxwriter) is optional, so these are still checked,
# with default features, by a second step of the gate.
ZIP_BLOCKED_EXCEL = [
    "faucet-core",
    "faucet-common-file",
    "faucet-sink-azure-blob",
    "faucet-sink-file",
    "faucet-sink-gcs",
    "faucet-sink-s3",
    "faucet-sink-sftp",
    "faucet-source-azure-blob",
    "faucet-source-file",
    "faucet-source-gcs",
    "faucet-source-s3",
    "faucet-source-sftp",
]
# Unguarded meanwhile: duckdb is not optional in these, and the published
# rest / xml baselines need zip 2 directly (their next release uses zip 9).
ZIP_BLOCKED_DUCKDB = [
    "faucet-sink-duckdb",
    "faucet-source-duckdb",
    "faucet-transform-sql",
    "faucet-source-rest",
    "faucet-source-xml",
]


def index_path(name: str) -> str:
    n = name.lower()
    if len(n) <= 2:
        return f"{len(n)}/{n}"
    if len(n) == 3:
        return f"3/{n[0]}/{n}"
    return f"{n[:2]}/{n[2:4]}/{n}"


def published(name: str) -> bool:
    url = f"https://index.crates.io/{index_path(name)}"
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            return any(line.strip() for line in r.read().decode().splitlines())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


def main() -> int:
    if sys.argv[1:] == ["--default-features"]:
        print(",".join(ZIP_BLOCKED_EXCEL))
        return 0
    meta = json.loads(
        subprocess.check_output(["cargo", "metadata", "--no-deps", "--format-version", "1"])
    )
    exclude = list(ALWAYS) + ZIP_BLOCKED_EXCEL + ZIP_BLOCKED_DUCKDB
    for pkg in meta["packages"]:
        name = pkg["name"]
        if name in exclude or pkg.get("publish") == []:
            continue
        if not published(name):
            exclude.append(name)
    print(",".join(exclude))
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Print the comma-separated `exclude` list for the API-stability gate (#789
SUPPLY-18): the always-excluded packages plus every publishable workspace
crate that has no release on crates.io yet (nothing to compare against).
A crate drops out of the list by itself on its first release, so a published
crate can never stay unguarded because someone forgot to prune the list.

    scripts/semver-exclude.py            # uses `cargo metadata`
"""
import json
import subprocess
import sys
import urllib.error
import urllib.request

# faucet-cli / faucet-stream: binary + umbrella, checked through their parts.
# Delta: the published versions do not build under a fresh resolve, so there
# is no baseline (#794); drop them once a deltalake-1.x release is out.
ALWAYS = [
    "faucet-cli",
    "faucet-stream",
    "faucet-common-delta",
    "faucet-source-delta",
    "faucet-sink-delta",
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
    meta = json.loads(
        subprocess.check_output(["cargo", "metadata", "--no-deps", "--format-version", "1"])
    )
    exclude = list(ALWAYS)
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

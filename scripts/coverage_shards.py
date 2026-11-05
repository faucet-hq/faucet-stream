#!/usr/bin/env python3
"""Plan the Coverage job's shards: which packages or test targets each one runs.

  scripts/coverage_shards.py libs 2 4   # cargo args for library shard 2 of 4
  scripts/coverage_shards.py cli 1 3    # cargo args for faucet-cli shard 1 of 3

Library shards split every workspace package except faucet-cli that has
integration-test targets, balanced by target count (largest first, each to the
lightest shard), so a new crate lands in a shard without editing the workflow.
CLI shards split faucet-cli's test targets in name order. Every package or
target is in exactly one shard.
"""

import json
import subprocess
import sys

CLI = "faucet-cli"


def metadata():
    out = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return json.loads(out)


def test_targets(meta):
    """{package name: sorted integration-test target names} for workspace members."""
    members = set(meta["workspace_members"])
    found = {}
    for pkg in meta["packages"]:
        if pkg["id"] not in members:
            continue
        names = sorted(t["name"] for t in pkg["targets"] if "test" in t["kind"])
        if names:
            found[pkg["name"]] = names
    return found


def plan_libs(targets, count):
    """Split packages into `count` shards balanced by test-target count."""
    shards = [[] for _ in range(count)]
    load = [0] * count
    pkgs = sorted((p for p in targets if p != CLI), key=lambda p: (-len(targets[p]), p))
    for pkg in pkgs:
        i = min(range(count), key=lambda k: (load[k], k))
        shards[i].append(pkg)
        load[i] += len(targets[pkg])
    return [sorted(s) for s in shards]


def plan_cli(targets, count):
    """Split faucet-cli's test targets into `count` contiguous parts by name."""
    names = targets.get(CLI, [])
    per = -(-len(names) // count) if names else 0
    return [names[i * per:(i + 1) * per] for i in range(count)]


def cargo_args(kind, index, count, targets):
    if not 1 <= index <= count:
        raise ValueError(f"shard {index} is outside 1..{count}")
    if kind == "libs":
        pkgs = plan_libs(targets, count)[index - 1]
        return [arg for p in pkgs for arg in ("-p", p)]
    if kind == "cli":
        names = plan_cli(targets, count)[index - 1]
        return ["-p", CLI] + [arg for n in names for arg in ("--test", n)]
    raise ValueError(f"unknown shard kind {kind!r} (expected libs or cli)")


def main(argv):
    if len(argv) != 4:
        print(__doc__, file=sys.stderr)
        return 2
    kind, index, count = argv[1], int(argv[2]), int(argv[3])
    args = cargo_args(kind, index, count, test_targets(metadata()))
    if kind == "libs" and not args or kind == "cli" and len(args) == 2:
        print(f"coverage shard {kind} {index}/{count} has nothing to run", file=sys.stderr)
        return 1
    print(" ".join(args))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

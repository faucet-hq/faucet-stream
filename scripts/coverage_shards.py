#!/usr/bin/env python3
"""Plan the Coverage job's shards: which packages or test targets each one runs.

  scripts/coverage_shards.py libs 2 4   # cargo args for library shard 2 of 4
  scripts/coverage_shards.py cli 1 3    # cargo args for faucet-cli shard 1 of 3

Library shards split every workspace package except faucet-cli that has
integration-test targets, balanced by each package's measured test time in
scripts/coverage-shard-weights.json (largest first, each to the lightest shard).
A package missing from that file weighs its target count times the median time
per target, so a new crate lands in a shard without editing anything; with no
file at all, packages weigh their target count. Regenerate the file with
scripts/coverage_shard_weights.py. A stale file only makes the shards less
even: every package or target is still in exactly one shard. CLI shards split
faucet-cli's test targets in name order.
"""

import json
import pathlib
import statistics
import subprocess
import sys

CLI = "faucet-cli"
WEIGHTS = pathlib.Path(__file__).with_name("coverage-shard-weights.json")


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


def load_weights(path=WEIGHTS):
    """{package: seconds} from the committed weights file, or None without one."""
    try:
        data = json.loads(pathlib.Path(path).read_text())
    except FileNotFoundError:
        return None
    return {str(k): float(v) for k, v in data["packages"].items()}


def package_weights(targets, weights):
    """{package: weight} for every library package. Without measurements a
    package weighs its target count; with them, a package missing from the
    measurements weighs its target count times the median seconds per target."""
    pkgs = [p for p in targets if p != CLI]
    if not weights:
        return {p: float(len(targets[p])) for p in pkgs}
    per_target = [weights[p] / len(targets[p]) for p in pkgs if p in weights and targets[p]]
    fallback = statistics.median(per_target) if per_target else 1.0
    return {p: weights[p] if p in weights else len(targets[p]) * fallback for p in pkgs}


def plan_libs(targets, count, weights=None):
    """Split packages into `count` shards balanced by weight (see package_weights)."""
    shards = [[] for _ in range(count)]
    load = [0.0] * count
    weight = package_weights(targets, weights)
    pkgs = sorted(weight, key=lambda p: (-weight[p], p))
    for pkg in pkgs:
        i = min(range(count), key=lambda k: (load[k], k))
        shards[i].append(pkg)
        load[i] += weight[pkg]
    return [sorted(s) for s in shards]


def plan_cli(targets, count):
    """Split faucet-cli's test targets into `count` contiguous parts by name."""
    names = targets.get(CLI, [])
    per = -(-len(names) // count) if names else 0
    return [names[i * per:(i + 1) * per] for i in range(count)]


def cargo_args(kind, index, count, targets, weights=None):
    if not 1 <= index <= count:
        raise ValueError(f"shard {index} is outside 1..{count}")
    if kind == "libs":
        pkgs = plan_libs(targets, count, weights)[index - 1]
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
    args = cargo_args(kind, index, count, test_targets(metadata()), load_weights())
    if kind == "libs" and not args or kind == "cli" and len(args) == 2:
        print(f"coverage shard {kind} {index}/{count} has nothing to run", file=sys.stderr)
        return 1
    print(" ".join(args))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

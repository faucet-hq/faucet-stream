#!/usr/bin/env python3
"""Measure each library package's test time from CI logs, for coverage_shards.py.

  scripts/coverage_shard_weights.py --run 43455159522 [--run …] [--write]

Reads the `Coverage shard (libs-N)` job logs of each run (through `gh`), times
every integration-test binary from its `Running` line to the next one, and
sums the times per package. Without `--write` it prints the result; with it,
it rewrites scripts/coverage-shard-weights.json. Several runs are averaged.

A binary is matched to its package by replaying the shard's plan: cargo runs a
shard's test binaries package by package in name order, each package's targets
in name order. The plan is the one the run used (the weights file at the run's
commit, else target counts) over the current workspace. If any binary does not
line up, the script stops and names it rather than guess; regenerate from a run
of a commit with the same packages and test targets.
"""

import argparse
import json
import re
import statistics
import subprocess
import sys
from datetime import datetime

import coverage_shards as cs

REPO = "faucet-hq/faucet-stream"
ANSI = re.compile(r"\x1b\[[0-9;]*m|\^\[\[[0-9;]*m")
STAMP = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?)Z")
RUNNING = re.compile(r"\bRunning\s+(tests/\S+)\s+\(([^)]+)\)")
SHARD = re.compile(r"Coverage shard \(libs-(\d+)\)")


def stamp(line):
    m = STAMP.search(line)
    if not m:
        return None
    text = m.group(1)
    if "." in text:
        head, frac = text.split(".")
        text = f"{head}.{frac[:6]}"
    return datetime.fromisoformat(text)


def binary_times(log):
    """[(binary name, seconds)] in run order, from one shard's job log."""
    runs, last_result = [], None
    for raw in log.splitlines():
        line = ANSI.sub("", raw)
        when = stamp(line)
        m = RUNNING.search(line)
        if m and when:
            name = m.group(2).rsplit("/", 1)[-1].rsplit("-", 1)[0]
            runs.append((name, when))
        elif "test result:" in line and when:
            last_result = when
    times = []
    for i, (name, start) in enumerate(runs):
        end = runs[i + 1][1] if i + 1 < len(runs) else last_result
        if end is None or end < start:
            raise ValueError(f"no end time for test binary {name}")
        times.append((name, (end - start).total_seconds()))
    return times


def norm(name):
    return name.replace("-", "_")


def attribute(times, packages, targets):
    """{package: seconds}, walking `times` against the packages' targets in
    the order cargo runs them. Raises ValueError on any mismatch."""
    expected = [(p, norm(t)) for p in sorted(packages) for t in sorted(targets[p], key=norm)]
    got = [norm(n) for n, _ in times]
    if [t for _, t in expected] != got:
        for i, (want, have) in enumerate(zip([t for _, t in expected], got)):
            if want != have:
                raise ValueError(
                    f"binary {i} is {have!r} but the plan expects {want!r} "
                    f"(package {expected[i][0]}); the workspace changed since this run"
                )
        raise ValueError(f"the plan expects {len(expected)} test binaries, the log has {len(got)}")
    out = {p: 0.0 for p in packages}
    for (pkg, _), (_, secs) in zip(expected, times):
        out[pkg] += secs
    return out


def weights_at(sha):
    """The weights file at `sha`, or None when that commit had none."""
    res = subprocess.run(
        ["git", "show", f"{sha}:scripts/coverage-shard-weights.json"],
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        return None
    return {str(k): float(v) for k, v in json.loads(res.stdout)["packages"].items()}


def gh_json(*args):
    return json.loads(subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout)


def run_logs(run_id):
    """(head sha, {shard index: job log}, shard count) for one run."""
    view = gh_json("run", "view", str(run_id), "-R", REPO, "--json", "jobs,headSha")
    jobs = {}
    for job in view["jobs"]:
        m = SHARD.search(job["name"])
        if m:
            jobs[int(m.group(1))] = job["databaseId"]
    if not jobs:
        raise ValueError(f"run {run_id} has no library coverage shards")
    logs = {}
    for index, job_id in jobs.items():
        logs[index] = subprocess.run(
            ["gh", "run", "view", str(run_id), "-R", REPO, "--log", "--job", str(job_id)],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    return view["headSha"], logs, max(jobs)


def measure(run_id, targets):
    sha, logs, count = run_logs(run_id)
    plan = cs.plan_libs(targets, count, weights_at(sha))
    totals = {}
    for index, log in sorted(logs.items()):
        totals.update(attribute(binary_times(log), plan[index - 1], targets))
    return totals


def combine(measurements):
    """Average each package over the runs that measured it, in whole seconds
    (at least 1, so a fast package still counts)."""
    pkgs = sorted({p for m in measurements for p in m})
    return {p: max(1, round(statistics.mean(m[p] for m in measurements if p in m))) for p in pkgs}


def main(argv):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run", action="append", required=True, help="a CI run id (repeatable)")
    ap.add_argument("--write", action="store_true", help=f"rewrite {cs.WEIGHTS.name}")
    args = ap.parse_args(argv)
    targets = cs.test_targets(cs.metadata())
    try:
        packages = combine([measure(r, targets) for r in args.run])
    except ValueError as e:
        print(f"coverage_shard_weights: {e}", file=sys.stderr)
        return 1
    doc = {"generated_from": [int(r) for r in args.run], "packages": packages}
    text = json.dumps(doc, indent=2) + "\n"
    if args.write:
        cs.WEIGHTS.write_text(text)
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

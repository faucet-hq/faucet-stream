#!/usr/bin/env python3
"""Compute patch (changed-line) coverage from an lcov report + a git diff.

Why this exists: `codecov/patch` reports the same number, but it posts
unreliably — of the eight PRs merged before this script landed, only one carried
a `codecov/patch` status on its head commit, including a large code PR that
carried none. A required check that frequently never posts blocks every PR on
"Expected — waiting for status", so codecov cannot be the gate. This runs inside
the already-required `Coverage` job instead, where the result is deterministic
and ours.

Usage:
    patch-coverage.py --lcov lcov.info --base origin/main [--min 95.0]

Exits non-zero when coverage of added/modified lines is below `--min`, printing
the uncovered lines so the failure is actionable rather than just a number.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict

# Kept in sync with the `--ignore-filename-regex` in the Coverage job and with
# codecov.yml. Paths matching these are excluded from the denominator.
IGNORE = re.compile(
    r"(/tests?/|/examples?/|/benches?/|build\.rs$"
    r"|crates/source/gcs/src/stream\.rs$|crates/sink/gcs/src/sink\.rs$"
    r"|crates/sink/clickhouse/src/staged_exec\.rs$|crates/sink/mssql/src/staged_exec\.rs$)"
)


def parse_lcov(path: str) -> dict[str, dict[int, int]]:
    """file -> {line: hit_count} for every instrumented line."""
    files: dict[str, dict[int, int]] = defaultdict(dict)
    current: str | None = None
    with open(path, encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.strip()
            if line.startswith("SF:"):
                current = line[3:]
            elif line.startswith("DA:") and current:
                num, _, count = line[3:].partition(",")
                try:
                    files[current][int(num)] = int(count.split(",")[0])
                except ValueError:
                    continue
            elif line == "end_of_record":
                current = None
    return files


def changed_lines(base: str) -> dict[str, set[int]]:
    """file -> {line numbers added or modified relative to `base`}."""
    try:
        diff = subprocess.run(
            ["git", "diff", "--unified=0", f"{base}...HEAD", "--", "*.rs"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError as e:
        print(f"patch-coverage: git diff failed: {e.stderr}", file=sys.stderr)
        sys.exit(2)

    out: dict[str, set[int]] = defaultdict(set)
    path: str | None = None
    hunk = re.compile(r"^@@ -\S+ \+(\d+)(?:,(\d+))? @@")
    for line in diff.splitlines():
        if line.startswith("+++ b/"):
            path = line[6:]
        elif line.startswith("@@") and path:
            m = hunk.match(line)
            if m:
                start = int(m.group(1))
                count = int(m.group(2) or 1)
                out[path].update(range(start, start + count))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lcov", default="lcov.info")
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--min", type=float, default=95.0)
    ap.add_argument(
        "--report-only",
        action="store_true",
        help="print the number but always exit 0",
    )
    args = ap.parse_args()

    cov = parse_lcov(args.lcov)
    # lcov paths may be absolute; index by suffix so a repo-relative diff path
    # matches regardless of the workspace root.
    by_suffix = {p: lines for p, lines in cov.items()}

    def lookup(rel: str) -> dict[int, int] | None:
        if rel in by_suffix:
            return by_suffix[rel]
        for p, lines in by_suffix.items():
            if p.endswith("/" + rel) or p.endswith(rel):
                return lines
        return None

    total = 0
    hit = 0
    misses: list[str] = []
    for rel, lines in sorted(changed_lines(args.base).items()):
        if IGNORE.search(rel):
            continue
        table = lookup(rel)
        if table is None:
            # Not instrumented at all (e.g. a file excluded from the build).
            continue
        for ln in sorted(lines):
            if ln not in table:
                continue  # not an executable line (comment, brace, decl)
            total += 1
            if table[ln] > 0:
                hit += 1
            elif len(misses) < 40:
                misses.append(f"{rel}:{ln}")

    if total == 0:
        print("patch-coverage: no instrumented changed lines; nothing to check")
        return 0

    pct = 100.0 * hit / total
    print(f"patch-coverage: {hit}/{total} changed lines covered = {pct:.2f}%")

    if pct + 1e-9 < args.min and not args.report_only:
        print(f"\npatch-coverage: FAILED — below the {args.min:.2f}% floor.")
        print("Uncovered changed lines (first 40):")
        for m in misses:
            print(f"  {m}")
        print(
            "\nThe project standard is >=95% patch coverage. If a line is genuinely "
            "untestable (a signal handler, a main() dispatch arm, an infinite "
            "supervisory loop), say so explicitly in the PR rather than lowering "
            "this floor."
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

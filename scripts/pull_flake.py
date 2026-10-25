#!/usr/bin/env python3
"""Decide whether a failed test run failed only because an image pull did.

Exit 0 (retry) when every failing libtest test's captured output names an
image-pull error, or, when no test failed and no test binary crashed, the log
itself does (a pull outside any test). Exit 1 otherwise: a failure the pull did not cause is never retried.

Usage: scripts/pull_flake.py <log file>
"""

import re
import sys

PULL_FLAKE = re.compile(
    r"bytes remaining on stream|toomanyrequests|pull access denied|failed to pull|"
    r"error pulling image|PullImage|manifest unknown|TLS handshake timeout|"
    r"Client\.Timeout exceeded while awaiting headers"
)
BLOCK = re.compile(r"^---- (\S+) stdout ----$", re.M)
FAILED = re.compile(r"^test (\S+) \.\.\. FAILED$", re.M)


def failing_tests(log):
    return set(FAILED.findall(log))


def blocks(log):
    """{test name: its captured output} from libtest's failure report."""
    out = {}
    marks = list(BLOCK.finditer(log))
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(log)
        body = log[m.end() : end]
        stop = re.search(r"^failures:$", body, re.M)
        out[m.group(1)] = out.get(m.group(1), "") + (body[: stop.start()] if stop else body)
    return out


def retryable(log):
    failed = failing_tests(log)
    if not failed:
        return PULL_FLAKE.search(log) is not None and "error: test failed" not in log
    captured = blocks(log)
    return all(PULL_FLAKE.search(captured.get(name, "")) for name in failed)


def main(argv):
    with open(argv[1], encoding="utf-8", errors="replace") as f:
        return 0 if retryable(f.read()) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))

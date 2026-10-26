#!/usr/bin/env python3
"""List every failing test, test binary and build error in cargo test logs.

Reads the logs `scripts/retry-on-image-pull.sh` keeps (RETRY_LOG_KEEP) and
prints one `::error` annotation per failure plus a Markdown table, appended to
`--summary` (the job's $GITHUB_STEP_SUMMARY) when given. Always exits 0: it
reports, the step that ran the tests decides pass or fail.

Usage: scripts/failing_tests.py [--summary FILE] <log> [<log> ...]
"""

import re
import sys
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# A downloaded GitHub job log prefixes each line with its timestamp.
STAMP = re.compile(r"^\d{4}-\d\d-\d\dT[\d:.]+Z ")
RUNNING = re.compile(r"^\s*Running (\S+) \((?:[^)]*/)?([^/)]+)\)\s*$")
FAILED = re.compile(r"^test (\S+) \.\.\. FAILED$")
BLOCK = re.compile(r"^---- (\S+) stdout ----$")
PANIC = re.compile(r"panicked at ([^:\s]+:\d+:\d+):\s*$")
RERUN = re.compile(r"to rerun pass `([^`]+)`")
TARGETS_FAILED = re.compile(r"^error: \d+ targets? failed:$")
TARGET_LINE = re.compile(r"^\s+`([^`]+)`$")
CRASH = re.compile(r"process didn't exit successfully: `?(\S+?)[` ].*\((signal: [^)]*|exit status: \d+)\)")
BUILD_ERROR = re.compile(r"^error(\[E\d+\])?: (?!test failed|\d+ targets? failed)(.+)$")
BUILD_LOC = re.compile(r"^\s*--> (\S+:\d+:\d+)$")


def binary_of(running_path, exe):
    """`tests/integration.rs` + `integration-ab12` -> `integration (tests/integration.rs)`."""
    name = re.sub(r"-[0-9a-f]{8,}$", "", exe)
    return f"{name} ({running_path})"


def parse(text):
    """Failures in one log: {"tests": [...], "targets": [...], "crashes": [...], "build": [...]}."""
    lines = [STAMP.sub("", ANSI.sub("", l)).rstrip("\n") for l in text.splitlines()]
    tests, targets, crashes, build = [], [], [], []
    current = None
    reasons = {}
    for i, line in enumerate(lines):
        m = RUNNING.match(line)
        if m:
            current = binary_of(m.group(1), m.group(2))
            continue
        m = FAILED.match(line)
        if m:
            tests.append({"test": m.group(1), "binary": current, "reason": ""})
            continue
        m = BLOCK.match(line)
        if m:
            for j in range(i + 1, min(i + 40, len(lines))):
                p = PANIC.search(lines[j])
                if p:
                    detail = lines[j + 1].strip() if j + 1 < len(lines) else ""
                    reasons[m.group(1)] = f"{p.group(1)}: {detail}"[:300]
                    break
                if BLOCK.match(lines[j]) or lines[j] == "failures:":
                    break
            continue
        m = RERUN.search(line)
        if m and m.group(1) not in targets:
            targets.append(m.group(1))
            continue
        if TARGETS_FAILED.match(line):
            for nxt in lines[i + 1 :]:
                t = TARGET_LINE.match(nxt)
                if not t:
                    break
                if t.group(1) not in targets:
                    targets.append(t.group(1))
            continue
        m = CRASH.search(line)
        if m and "signal" in m.group(2):
            crashes.append(f"{m.group(1)} ({m.group(2)})")
            continue
        m = BUILD_ERROR.match(line)
        if m and not line.startswith("error: test failed"):
            loc = ""
            for nxt in lines[i + 1 : i + 4]:
                b = BUILD_LOC.match(nxt)
                if b:
                    loc = b.group(1)
                    break
            entry = f"{loc}: {m.group(2)}" if loc else m.group(2)
            if entry not in build:
                build.append(entry)
    for t in tests:
        t["reason"] = reasons.get(t["test"], "")
    return {"tests": tests, "targets": targets, "crashes": crashes, "build": build}


def merge(results):
    out = {"tests": [], "targets": [], "crashes": [], "build": []}
    for r in results:
        for key, items in r.items():
            for item in items:
                if item not in out[key]:
                    out[key].append(item)
    return out


def cell(text):
    return (text or "").replace("|", "\\|").replace("\n", " ")


def render(failures):
    """The Markdown summary; empty string when nothing failed."""
    if not any(failures.values()):
        return ""
    out = ["## Failing tests", ""]
    if failures["tests"]:
        out += ["| Test | Binary | Panic |", "|---|---|---|"]
        out += [
            f"| `{cell(t['test'])}` | {cell(t['binary'] or '?')} | {cell(t['reason'])} |"
            for t in failures["tests"]
        ]
        out.append("")
    if failures["targets"]:
        out += ["Failing targets (rerun with `cargo test <target>`):", ""]
        out += [f"- `{t}`" for t in failures["targets"]]
        out.append("")
    if failures["crashes"]:
        out += ["Test binaries that crashed:", ""] + [f"- `{c}`" for c in failures["crashes"]] + [""]
    if failures["build"]:
        out += ["Build errors:", ""] + [f"- {cell(b)}" for b in failures["build"]] + [""]
    return "\n".join(out)


def annotations(failures):
    out = []
    for t in failures["tests"]:
        where = f" in {t['binary']}" if t["binary"] else ""
        why = f": {t['reason']}" if t["reason"] else ""
        out.append(f"::error title=test failed::{t['test']}{where}{why}")
    out += [f"::error title=test binary crashed::{c}" for c in failures["crashes"]]
    out += [f"::error title=build error::{b}" for b in failures["build"]]
    return out


def main(argv):
    summary = None
    if len(argv) >= 2 and argv[0] == "--summary":
        summary, argv = argv[1], argv[2:]
    results = []
    for path in argv:
        p = Path(path)
        if p.is_file():
            results.append(parse(p.read_text(errors="replace")))
    failures = merge(results)
    for line in annotations(failures):
        print(line)
    text = render(failures)
    if text:
        print(text)
        if summary:
            with open(summary, "a") as f:
                f.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

#!/usr/bin/env python3
"""Require changed I/O code to be exercised by an integration test (#695).

`patch-coverage.py` measures changed lines against unit + integration tests
together, so a new sink method that only a unit test touches reads as fully
covered even though nothing ever ran it against the real backend. This gate
reads an lcov report produced by the integration test binaries alone (the
crates' `tests/` targets) and checks the changed lines in I/O code:

* every in-scope changed file with instrumented changed lines must have at
  least one of them hit by an integration test, and
* the in-scope changed lines together must reach `--min` percent.

Pure logic (config parsing, planners, validation) is left to unit tests and to
`patch-coverage.py`; only the paths where a unit test cannot prove correctness
are in scope.

A PR can downgrade a failure to a warning with a line in its description:

    no-integration-test: <why this change cannot be tested against a backend>

The reason is printed in the job log and the job summary, so the exception is
visible rather than silent.

Usage:
    integration-coverage.py --lcov integration.lcov --base origin/main \\
        [--min 60] [--pr-body-env PR_BODY]
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import re
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "patch_coverage", Path(__file__).with_name("patch-coverage.py")
)
patch_coverage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(patch_coverage)

# Where a unit test cannot show the code works: connector and state-store I/O,
# the executor, and the server's handlers and registries.
SCOPE = re.compile(
    r"^(crates/(source|sink|common|state)/[^/]+/src/"
    r"|cli/src/serve/.*\.rs$"
    r"|cli/src/executor\.rs$"
    r"|cli/src/templates/"
    r"|cli/src/hub/)"
)
# Config structs and their validation are pure: unit tests cover them.
EXEMPT = re.compile(r"(/config\.rs$|/tests?/|/examples?/|/benches?/|build\.rs$)")
ESCAPE = re.compile(r"^\s*no-integration-test:\s*(\S.*?)\s*$", re.MULTILINE | re.IGNORECASE)


def in_scope(path: str) -> bool:
    return bool(SCOPE.search(path)) and not EXEMPT.search(path)


TEST_ATTR = re.compile(r"^\s*#\[cfg\(test\)\]\s*$")


def test_module_lines(source: str) -> set[int]:
    """Line numbers inside `#[cfg(test)] mod … { … }` blocks.

    An integration binary compiles the library without `cfg(test)`, so these
    lines can never run there; counting them would fail every change that adds
    a unit test. Braces are matched on the text, skipping string and char
    literals and line comments, which is enough for rustfmt-formatted code.
    """
    lines = source.splitlines()
    out: set[int] = set()
    i = 0
    while i < len(lines):
        if TEST_ATTR.match(lines[i]):
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines) and re.match(r"^\s*(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{", lines[j]):
                end = _block_end(lines, j)
                out.update(range(i + 1, end + 2))
                i = end + 1
                continue
        i += 1
    return out


def _block_end(lines: list[str], start: int) -> int:
    depth = 0
    for k in range(start, len(lines)):
        text = re.sub(r'"(\\.|[^"\\])*"', '""', lines[k])
        text = re.sub(r"'(\\.|[^'\\])'", "''", text)
        text = text.split("//", 1)[0]
        depth += text.count("{") - text.count("}")
        if depth <= 0 and k > start:
            return k
        if depth <= 0 and "{" in text and text.count("}") >= text.count("{"):
            return k
    return len(lines) - 1


def _parent_module_files(path: Path) -> list[Path]:
    """Files that can declare the module stored at `path` (`mod <name>;`)."""
    if path.name in ("lib.rs", "main.rs"):
        return []
    if path.name == "mod.rs":
        name, folder = path.parent.name, path.parent.parent
    else:
        name, folder = path.stem, path.parent
    if folder.name == "src":
        return [folder / "lib.rs", folder / "main.rs"]
    return [folder.parent / f"{folder.name}.rs", folder / "mod.rs"]


def _declares_test_module(source: str, name: str) -> bool:
    lines = source.splitlines()
    decl = re.compile(rf"^\s*(pub(\([^)]*\))?\s+)?mod\s+{re.escape(name)}\s*;")
    for i, line in enumerate(lines):
        if not TEST_ATTR.match(line):
            continue
        j = i + 1
        while j < len(lines) and (not lines[j].strip() or lines[j].strip().startswith("#[")):
            j += 1
        if j < len(lines) and decl.match(lines[j]):
            return True
    return False


def is_test_only_file(rel: str, _depth: int = 0) -> bool:
    """A file compiled only under `cfg(test)`: its parent module declares it
    with `#[cfg(test)] mod <name>;`, or the parent itself is test-only.
    Integration binaries never compile such a file, like an inline test module."""
    path = Path(rel)
    name = path.parent.name if path.name == "mod.rs" else path.stem
    for parent in _parent_module_files(path):
        try:
            source = parent.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if _declares_test_module(source, name):
            return True
        if _depth < 8 and is_test_only_file(str(parent), _depth + 1):
            return True
    return False


def _source_test_lines(rel: str) -> set[int]:
    try:
        source = Path(rel).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return set()
    if is_test_only_file(rel):
        return set(range(1, len(source.splitlines()) + 1))
    return test_module_lines(source)


def escape_reason(body: str | None) -> str | None:
    if not body:
        return None
    m = ESCAPE.search(body)
    return m.group(1) if m else None


def evaluate(
    changed: dict[str, set[int]],
    integration: dict[str, dict[int, int]],
    unit: dict[str, dict[int, int]] | None,
    test_lines=_source_test_lines,
) -> list[dict]:
    """One row per in-scope changed file that has instrumented changed lines."""
    rows = []
    for rel, lines in sorted(changed.items()):
        if not in_scope(rel):
            continue
        lines = set(lines) - test_lines(rel)
        itable = _lookup(integration, rel) or {}
        utable = _lookup(unit, rel) if unit is not None else None
        known = itable if itable else (utable or {})
        instrumented = sorted(ln for ln in lines if ln in known)
        if not instrumented:
            continue
        hit = [ln for ln in instrumented if itable.get(ln, 0) > 0]
        unit_hit = (
            [ln for ln in instrumented if (utable or {}).get(ln, 0) > 0]
            if utable is not None
            else None
        )
        rows.append(
            {
                "file": rel,
                "changed": len(instrumented),
                "integration": len(hit),
                "unit": None if unit_hit is None else len(unit_hit),
                "missed": [ln for ln in instrumented if ln not in set(hit)],
            }
        )
    return rows


def _lookup(table: dict[str, dict[int, int]] | None, rel: str) -> dict[int, int] | None:
    if table is None:
        return None
    if rel in table:
        return table[rel]
    for path, lines in table.items():
        if path.endswith("/" + rel):
            return lines
    return None


def verdict(rows: list[dict], minimum: float) -> tuple[bool, float, list[str]]:
    """(passed, percent, problems)."""
    total = sum(r["changed"] for r in rows)
    hit = sum(r["integration"] for r in rows)
    pct = 100.0 if total == 0 else 100.0 * hit / total
    problems = [
        f"{r['file']}: none of its {r['changed']} changed lines ran in an integration test"
        for r in rows
        if r["integration"] == 0
    ]
    if total and pct + 1e-9 < minimum:
        problems.append(f"integration coverage of changed I/O lines is {pct:.1f}%, below {minimum:.0f}%")
    return (not problems, pct, problems)


def summary_markdown(rows: list[dict], pct: float, problems: list[str], reason: str | None) -> str:
    out = ["### Integration coverage of changed I/O code", ""]
    if not rows:
        out.append("No changed lines in I/O code — nothing to check.")
        return "\n".join(out) + "\n"
    out += [
        f"**{pct:.1f}%** of changed I/O lines ran in an integration test.",
        "",
        "| File | Changed lines | Integration hit | Unit hit |",
        "|---|---:|---:|---:|",
    ]
    for r in rows:
        unit = "—" if r["unit"] is None else str(r["unit"])
        out.append(f"| `{r['file']}` | {r['changed']} | {r['integration']} | {unit} |")
    if problems:
        out.append("")
        out += [f"- {p}" for p in problems]
    if reason and problems:
        out += ["", f"> Waived by the PR: `no-integration-test: {reason}`"]
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lcov", default="integration.lcov")
    ap.add_argument("--unit-lcov", help="optional unit-only report, shown alongside")
    ap.add_argument("--base", default="origin/main")
    ap.add_argument("--min", type=float, default=60.0)
    ap.add_argument(
        "--pr-body-env",
        default="PR_BODY",
        help="environment variable holding the PR description",
    )
    args = ap.parse_args(argv)

    integration = patch_coverage.parse_lcov(args.lcov)
    unit = patch_coverage.parse_lcov(args.unit_lcov) if args.unit_lcov else None
    rows = evaluate(patch_coverage.changed_lines(args.base), integration, unit)
    passed, pct, problems = verdict(rows, args.min)
    reason = escape_reason(os.environ.get(args.pr_body_env))

    summary = summary_markdown(rows, pct, problems, reason)
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as fh:
            fh.write(summary)

    if not rows:
        print("integration-coverage: no changed lines in I/O code; nothing to check")
        return 0
    print(f"integration-coverage: {pct:.1f}% of changed I/O lines ran in an integration test")
    for r in rows:
        missed = patch_coverage.compress(r["missed"])
        print(f"  {r['integration']:4}/{r['changed']:<4} {r['file']}" + (f"  (not run: {missed})" if missed else ""))
    if passed:
        return 0
    for p in problems:
        print(f"integration-coverage: {p}")
    if reason:
        print(f"::warning::integration coverage waived by the PR: {reason}")
        return 0
    print(
        "\nAdd an integration test (the crate's tests/ directory, against a testcontainer "
        "or wiremock) that runs this code. If the change genuinely cannot be tested against "
        "a backend, add `no-integration-test: <reason>` to the PR description."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())

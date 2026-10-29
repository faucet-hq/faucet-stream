#!/usr/bin/env python3
"""Write the faucet-cli GitHub release body for a release commit (#1005).

One release PR produces one GitHub release, faucet-cli's. Its body is
faucet-cli's changelog section followed by a table of every crate the release
published: each `<crate>-v<X.Y.Z>` tag pointing at the release commit, with
the previous version, a link to the crate's CHANGELOG.md at the tag and a
link to crates.io. The output is the whole body, so editing the release with
it is safe to repeat.

Usage (from the repo root, at the release commit):
  scripts/release-notes.py --sha <commit> [--metadata meta.json] [--out body.md]
Prints nothing and exits 0 when no faucet-cli tag points at the commit.
Tests: python3 scripts/test_release_notes.py
"""

import argparse
import json
import os
import re
import subprocess
import sys

ROOT = "faucet-cli"
TAG_RE = re.compile(r"^(.+)-v(\d+)\.(\d+)\.(\d+)$")
SECTION_RE = re.compile(r"^## \[(\d+\.\d+\.\d+)\]")
DEPS_HEADING = "### Dependencies"


def split_tag(tag):
    m = TAG_RE.match(tag)
    if not m:
        return None
    return m.group(1), tuple(int(x) for x in m.group(2, 3, 4))


def fmt(version):
    return ".".join(str(x) for x in version)


def released(tags_at_commit):
    """{crate: version tuple} for the release tags pointing at the commit."""
    out = {}
    for tag in tags_at_commit:
        parsed = split_tag(tag)
        if parsed:
            out[parsed[0]] = parsed[1]
    return out


def previous_versions(all_tags, current):
    """{crate: highest earlier version or None} for each crate in `current`."""
    earlier = {}
    for tag in all_tags:
        parsed = split_tag(tag)
        if not parsed or parsed[0] not in current:
            continue
        crate, version = parsed
        if version < current[crate] and (earlier.get(crate) is None or version > earlier[crate]):
            earlier[crate] = version
    return {c: earlier.get(c) for c in current}


def changelog_section(text, version):
    """Body of the `## [version]` section, without the generated Dependencies block."""
    lines, out, inside, skipping = text.split("\n"), [], False, False
    for line in lines:
        m = SECTION_RE.match(line)
        if m or line.startswith("## "):
            if inside:
                break
            inside = bool(m) and m.group(1) == version
            continue
        if not inside:
            continue
        if line.strip() == DEPS_HEADING:
            skipping = True
            continue
        if skipping and line.startswith("#"):
            skipping = False
        if not skipping:
            out.append(line)
    return "\n".join(out).strip()


def render(repo, cli_version, section, current, previous, dirs):
    crates = sorted(current, key=lambda c: (c != ROOT, c))
    lines = [section or "No changes in faucet-cli itself; it is released with the crates below.", ""]
    lines += [
        "## Crates in this release",
        "",
        f"This release published {len(crates)} crate{'s' if len(crates) != 1 else ''}.",
        "",
        "| Crate | Version | Changelog | crates.io |",
        "|---|---|---|---|",
    ]
    for crate in crates:
        new = fmt(current[crate])
        old = previous.get(crate)
        version = f"{fmt(old)} → {new}" if old else f"{new} (new)"
        tag = f"{crate}-v{new}"
        path = dirs.get(crate)
        log = f"https://github.com/{repo}/blob/{tag}/{path + '/' if path else ''}CHANGELOG.md"
        lines.append(f"| `{crate}` | {version} | [CHANGELOG.md]({log}) | "
                     f"[{new}](https://crates.io/crates/{crate}/{new}) |")
    return "\n".join(lines) + "\n"


def crate_dirs(metadata):
    root = metadata["workspace_root"]
    return {p["name"]: os.path.relpath(os.path.dirname(p["manifest_path"]), root).replace(os.sep, "/")
            for p in metadata["packages"]}


def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True, check=True).stdout


def main(argv, run_git=git):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sha", required=True)
    ap.add_argument("--metadata", help="`cargo metadata --no-deps --format-version 1` output (default: run cargo)")
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "faucet-hq/faucet-stream"))
    ap.add_argument("--out")
    args = ap.parse_args(argv)
    current = released(run_git("tag", "--points-at", args.sha).split())
    if ROOT not in current:
        print(f"release-notes: no {ROOT} tag points at {args.sha}; nothing to write", file=sys.stderr)
        return 0
    if args.metadata:
        with open(args.metadata, encoding="utf-8") as f:
            meta = json.load(f)
    else:
        meta = json.loads(subprocess.run(["cargo", "metadata", "--no-deps", "--format-version", "1"],
                                         capture_output=True, text=True, check=True).stdout)
    dirs = crate_dirs(meta)
    previous = previous_versions(run_git("tag", "-l", "*-v*").split(), current)
    cli_version = fmt(current[ROOT])
    log = run_git("show", f"{args.sha}:{dirs.get(ROOT, 'cli')}/CHANGELOG.md")
    body = render(args.repo, cli_version, changelog_section(log, cli_version), current, previous, dirs)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(body)
    else:
        sys.stdout.write(body)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

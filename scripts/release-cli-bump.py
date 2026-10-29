#!/usr/bin/env python3
"""Bump faucet-cli on a release PR whenever a crate it builds in is bumped (#1005).

faucet-cli is the product version: its GitHub release carries the binaries,
the container images and the Homebrew formula. release-plz bumps a crate only
for commits in its own directory, and internal requirements are major-only
(`version = "1"`), so a release PR can bump connectors while faucet-cli stays
put and the binaries lag crates.io. This script raises faucet-cli to at least
the highest bump (patch or minor) among the crates in its build closure (the
workspace crates reachable from its `full` feature through normal and build
dependencies), and records them in its CHANGELOG.md section.

A major bump of a crate in the closure stops the script: a faucet-cli major
needs the maintainer's approval.

Re-running is safe: versions are compared with the base revision, so a
faucet-cli version that already satisfies the bump is left alone and the
changelog block is regenerated in place.

Usage (from the repo root, on the release PR branch):
  scripts/release-cli-bump.py --base <rev>           report what would change
  scripts/release-cli-bump.py --base <rev> --write   apply it
Tests: python3 scripts/test_release_cli_bump.py
"""

import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import tomllib

ROOT = "faucet-cli"
ROOT_FEATURES = ("full", "default")
CLI_MANIFEST = "cli/Cargo.toml"
CLI_CHANGELOG = "cli/CHANGELOG.md"
LOCKFILE = "Cargo.lock"
LEVELS = {"patch": 1, "minor": 2, "major": 3}
DEPS_HEADING = "### Dependencies"
VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")
SECTION_RE = re.compile(r"^## \[(\d+\.\d+\.\d+)\](\([^)]*\))?(.*)$")


def parse_version(version):
    m = VERSION_RE.match(version)
    if not m:
        raise ValueError(f"unsupported version {version!r} (expected X.Y.Z)")
    return tuple(int(x) for x in m.groups())


def bump_level(old, new):
    if old == new:
        return None
    if old is None:
        return "minor"
    o, n = parse_version(old), parse_version(new)
    if n < o:
        raise ValueError(f"version went backwards: {old} -> {new}")
    if n[0] != o[0]:
        return "major"
    if n[1] != o[1]:
        return "minor"
    return "patch"


def next_version(version, level):
    major, minor, patch = parse_version(version)
    if level == "major":
        return f"{major + 1}.0.0"
    if level == "minor":
        return f"{major}.{minor + 1}.0"
    return f"{major}.{minor}.{patch + 1}"


def build_closure(packages, root=ROOT, features=ROOT_FEATURES):
    """Workspace crates `root` builds in with `features`, from `cargo metadata --no-deps`."""
    by_name = {p["name"]: p for p in packages}
    if root not in by_name:
        raise ValueError(f"{root} is not a workspace package")
    deps = {
        p["name"]: {(d.get("rename") or d["name"]): d for d in p["dependencies"]
                    if d.get("kind") in (None, "build") and d["name"] in by_name}
        for p in packages
    }
    active, feats, weak = set(), {}, []
    queue = [("pkg", root, None)] + [("feat", root, f) for f in features]

    def activate(d, extra=()):
        out = [("pkg", d["name"], None)] + [("feat", d["name"], f) for f in list(d["features"]) + list(extra)]
        if d["uses_default_features"]:
            out.append(("feat", d["name"], "default"))
        return out

    while queue:
        kind, name, feat = queue.pop()
        if kind == "pkg":
            if name in active:
                continue
            active.add(name)
            for d in deps[name].values():
                if not d["optional"]:
                    queue += activate(d)
            queue += [("feat", dn, sub) for dn, sub in weak if dn == name]
            continue
        seen = feats.setdefault(name, set())
        if feat in seen:
            continue
        seen.add(feat)
        table = by_name[name]["features"]
        if feat not in table:
            d = deps[name].get(feat)
            if d is not None and d["optional"]:
                queue += activate(d)
            continue
        for item in table[feat]:
            if item.startswith("dep:"):
                d = deps[name].get(item[4:])
                if d is not None:
                    queue += activate(d)
            elif "/" in item:
                key, sub = item.split("/", 1)
                d = deps[name].get(key.rstrip("?"))
                if d is None:
                    continue
                if key.endswith("?") and d["optional"] and d["name"] not in active:
                    weak.append((d["name"], sub))
                else:
                    queue += activate(d, [sub])
            else:
                queue.append(("feat", name, item))
    active.discard(root)
    return active


def compute_bumps(base, head):
    """{name: (old, new)} for every package whose version differs; `old` is None for a new crate."""
    return {name: (base.get(name), new) for name, new in head.items() if base.get(name) != new}


def plan(closure, bumps, cli_base, cli_head):
    """Return (new faucet-cli version, [(crate, old, new)] that drove it) or raise on a major."""
    crates = sorted((n, o, v) for n, (o, v) in bumps.items() if n in closure and n != ROOT)
    majors = [c for c in crates if bump_level(c[1], c[2]) == "major"]
    if majors:
        names = ", ".join(f"{n} {o} -> {v}" for n, o, v in majors)
        raise SystemExit(
            f"::error::a crate faucet-cli builds in has a major bump ({names}); "
            "a faucet-cli major needs the maintainer's approval. Bump faucet-cli by hand on the release PR."
        )
    if not crates:
        return cli_head, []
    level = max((bump_level(o, v) for _, o, v in crates), key=LEVELS.get)
    target = next_version(cli_base, level)
    new = cli_head if parse_version(cli_head) >= parse_version(target) else target
    return new, crates


def dependency_line(crates):
    parts = [f"{n} {v} (new)" if o is None else f"{n} {o} → {v}" for n, o, v in crates]
    return "- Bumped with: " + ", ".join(parts)


def update_changelog(text, cli_base, cli_new, crates, today, repo):
    """Put faucet-cli's `cli_new` section first, with a regenerated Dependencies block."""
    link = f"https://github.com/{repo}/compare/{ROOT}-v{cli_base}...{ROOT}-v{cli_new}"
    block = [DEPS_HEADING, "", dependency_line(crates), ""]
    lines = text.split("\n")
    first = next((i for i, l in enumerate(lines) if SECTION_RE.match(l)), None)
    top = SECTION_RE.match(lines[first]) if first is not None else None
    if top is None or parse_version(top.group(1)) <= parse_version(cli_base):
        at = first if first is not None else len(lines)
        section = [f"## [{cli_new}]({link}) - {today}", ""] + block
        return "\n".join(lines[:at] + section + lines[at:])
    end = next((i for i in range(first + 1, len(lines)) if lines[i].startswith("## ")), len(lines))
    body = lines[first + 1 : end]
    kept, skipping = [], False
    for line in body:
        if line.strip() == DEPS_HEADING:
            skipping = True
            continue
        if skipping and line.startswith("#"):
            skipping = False
        if not skipping:
            kept.append(line)
    while kept and kept[-1] == "":
        kept.pop()
    heading = f"## [{cli_new}]({link}){top.group(3)}"
    return "\n".join(lines[:first] + [heading] + kept + [""] + block + lines[end:])


def set_manifest_version(text, version):
    out, in_package, done = [], False, False
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped.startswith("["):
            in_package = stripped == "[package]"
        if in_package and not done and re.match(r'^version\s*=\s*"[^"]*"', stripped):
            line = re.sub(r'"[^"]*"', f'"{version}"', line, count=1)
            done = True
        out.append(line)
    if not done:
        raise ValueError("no [package] version in the manifest")
    return "\n".join(out)


def set_lock_version(text, name, version):
    pattern = re.compile(r'(\[\[package\]\]\nname = "' + re.escape(name) + r'"\nversion = ")[^"]*(")')
    new, n = pattern.subn(lambda m: m.group(1) + version + m.group(2), text)
    if n != 1:
        raise ValueError(f"{name} appears {n} times in the lockfile")
    return new


def base_versions(packages, base, root_dir, show=None):
    show = show or (lambda rev, path: subprocess.run(
        ["git", "show", f"{rev}:{path}"], capture_output=True, text=True, check=False))
    versions = {}
    for p in packages:
        rel = os.path.relpath(p["manifest_path"], root_dir)
        res = show(base, rel)
        if res.returncode != 0:
            versions[p["name"]] = None
            continue
        versions[p["name"]] = tomllib.loads(res.stdout).get("package", {}).get("version")
    return versions


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="revision holding the last released versions")
    ap.add_argument("--metadata", help="`cargo metadata --no-deps --format-version 1` output (default: run cargo)")
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--today", default=datetime.date.today().isoformat())
    ap.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", "faucet-hq/faucet-stream"))
    args = ap.parse_args(argv)
    if args.metadata:
        with open(args.metadata, encoding="utf-8") as f:
            meta = json.load(f)
    else:
        meta = json.loads(subprocess.run(
            ["cargo", "metadata", "--no-deps", "--format-version", "1"],
            capture_output=True, text=True, check=True).stdout)
    packages = meta["packages"]
    head = {p["name"]: p["version"] for p in packages}
    base = base_versions(packages, args.base, meta["workspace_root"])
    closure = build_closure(packages)
    cli_base = base[ROOT] or head[ROOT]
    new, crates = plan(closure, compute_bumps(base, head), cli_base, head[ROOT])
    if not crates:
        print("release-cli-bump: no crate faucet-cli builds in is bumped; nothing to do")
        return 0
    print(f"release-cli-bump: faucet-cli {cli_base} -> {new} (on the PR: {head[ROOT]}), bumped with {len(crates)} crate(s):")
    for n, o, v in crates:
        print(f"  {n} {o or '(new)'} -> {v} ({bump_level(o, v)})")
    if not args.write:
        return 0
    if new != head[ROOT]:
        for path, edit in ((CLI_MANIFEST, lambda t: set_manifest_version(t, new)),
                           (LOCKFILE, lambda t: set_lock_version(t, ROOT, new))):
            with open(path, encoding="utf-8") as f:
                text = f.read()
            with open(path, "w", encoding="utf-8") as f:
                f.write(edit(text))
    with open(CLI_CHANGELOG, encoding="utf-8") as f:
        text = f.read()
    with open(CLI_CHANGELOG, "w", encoding="utf-8") as f:
        f.write(update_changelog(text, cli_base, new, crates, args.today, args.repo))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

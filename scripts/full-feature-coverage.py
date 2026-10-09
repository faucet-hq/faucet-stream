#!/usr/bin/env python3
"""Fail when a cargo feature is left out of `full`, or the image drifts from it (#845).

The published `full` container image is built from the Dockerfile's
`DEFAULT_FEATURES`, which must be exactly the CLI's `full` feature. `full` in
turn must reach every feature the CLI manifest declares, so "full" never quietly
means "most". The umbrella `faucet-stream` crate's `full` gets the same check,
minus `default` and the per-transform `transform-*` features, which are subsets
of `transforms`.

Usage: scripts/full-feature-coverage.py   (run from the repo root)
"""

import re
import sys
import tomllib

CHECKS = (
    ("cli/Cargo.toml", ()),
    ("faucet-stream/Cargo.toml", ("default", "transform-*")),
)
DOCKERFILE = "Dockerfile"


def reachable(features, root="full"):
    seen = set()
    stack = [root]
    while stack:
        name = stack.pop()
        if name in seen or name not in features:
            continue
        seen.add(name)
        for entry in features[name]:
            if not entry.startswith("dep:") and "/" not in entry:
                stack.append(entry)
    return seen


def exempt(name, patterns):
    return any(
        name.startswith(p[:-1]) if p.endswith("*") else name == p for p in patterns
    )


def missing(features, exemptions=()):
    if "full" not in features:
        return ["(no `full` feature)"]
    covered = reachable(features)
    return sorted(
        name for name in features if name not in covered and not exempt(name, exemptions)
    )


def dockerfile_default_features(text):
    found = re.search(r'^ARG DEFAULT_FEATURES="([^"]*)"', text, re.MULTILINE)
    return found.group(1) if found else None


def main():
    failed = False
    for path, exemptions in CHECKS:
        with open(path, "rb") as f:
            features = tomllib.load(f).get("features", {})
        gaps = missing(features, exemptions)
        if gaps:
            failed = True
            print(f"{path}: `full` does not enable: {', '.join(gaps)}")
            print("  Add them to `full` (or to an aggregate `full` enables).")
    with open(DOCKERFILE) as f:
        image = dockerfile_default_features(f.read())
    if image != "full":
        failed = True
        print(f'{DOCKERFILE}: ARG DEFAULT_FEATURES must be "full", found {image!r}.')
        print("  The complete image is the CLI's `full` feature; extend `full` instead.")
    if failed:
        return 1
    print("full-feature-coverage: `full` enables every feature and the image builds it")
    return 0


if __name__ == "__main__":
    sys.exit(main())

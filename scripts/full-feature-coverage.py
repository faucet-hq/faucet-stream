#!/usr/bin/env python3
"""Fail when a cargo feature is left out of `full`, or the image drifts from it (#845).

The published `full` container image is built from the Dockerfile's
`DEFAULT_FEATURES`, which must be exactly the CLI's `full` feature. `full` in
turn must reach every feature the CLI manifest declares, so "full" never quietly
means "most". The umbrella `faucet-stream` crate's `full` gets the same check,
minus `default` and the per-transform `transform-*` features, which are subsets
of `transforms`. Each manifest's docs.rs `features` list must reach every
feature except the native-heavy `DOCS_RS_EXCLUDED` ones (and aggregates that
pull them in), so the docs.rs build stays inside its memory limit (#794).

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
# Features left out of the docs.rs build: their native builds (bundled DuckDB,
# librdkafka, Oracle) overran the docs.rs sandbox (#794).
DOCS_RS_EXCLUDED = {
    "source-duckdb",
    "sink-duckdb",
    "transform-sql",
    "source-kafka",
    "sink-kafka",
    "kafka-schema-registry",
    "lineage-kafka",
    "triggers-kafka",
    "source-oracle",
    "source-oracle-cdc",
    "sink-oracle",
}


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


def docs_rs_gaps(features, docs):
    """Features the docs.rs list should reach but does not, and excluded
    features it reaches anyway."""
    if docs.get("all-features"):
        return [], ["all-features (the docs.rs build runs out of memory, #794)"]
    reached = set()
    for name in docs.get("features", []):
        reached |= reachable(features, name)
    tainted = {
        name for name in features if reachable(features, name) & DOCS_RS_EXCLUDED
    }
    missing = sorted(
        name
        for name in features
        if name not in tainted and name not in ("default", "full") and name not in reached
    )
    return missing, sorted(reached & tainted)


def dockerfile_default_features(text):
    found = re.search(r'^ARG DEFAULT_FEATURES="([^"]*)"', text, re.MULTILINE)
    return found.group(1) if found else None


def main():
    failed = False
    for path, exemptions in CHECKS:
        with open(path, "rb") as f:
            manifest = tomllib.load(f)
        features = manifest.get("features", {})
        docs = manifest.get("package", {}).get("metadata", {}).get("docs", {}).get("rs", {})
        docs_missing, docs_heavy = docs_rs_gaps(features, docs)
        if docs_missing:
            failed = True
            print(f"{path}: [package.metadata.docs.rs] features miss: {', '.join(docs_missing)}")
        if docs_heavy:
            failed = True
            print(f"{path}: [package.metadata.docs.rs] enables: {', '.join(docs_heavy)}")
            print("  These overran the docs.rs sandbox (#794); list features individually.")
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

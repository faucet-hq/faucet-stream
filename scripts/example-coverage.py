#!/usr/bin/env python3
"""Fail when a connector, mode or top-level config block has no example (#853).

Every registered source and sink kind, every sink write mode, every mirror mode
and source replication method, and every top-level key of `PipelineConfig` must
appear in at least one config under `cli/examples/`. Those files are validated
by `cli/tests/cli_end_to_end.rs::shipped_example_yamls_pass_validate`, and the
agent skills point at them instead of carrying copies, so a feature with no
example has no checked documentation.

The registry is read from the source: kinds from `builtin_source_descriptions`
/ `builtin_sink_descriptions` in `cli/src/registry.rs`, top-level keys from the
`PipelineConfig` struct in `cli/src/config.rs`, write modes from `WriteMode` in
`crates/core/src/write_mode.rs`, mirror modes from `ReplicationMode` in
`cli/src/replication/spec.rs`, replication methods from `ReplicationMethod` in
`crates/core/src/replication.rs`.

Exemptions live in `scripts/example-coverage-allow.txt`, one `<category>:<name>`
per line with a `# reason` comment.

Usage: scripts/example-coverage.py   (from the repo root; needs PyYAML)
"""

import glob
import re
import sys

import yaml

REGISTRY = "cli/src/registry.rs"
CONFIG = "cli/src/config.rs"
WRITE_MODE = "crates/core/src/write_mode.rs"
MIRROR_SPEC = "cli/src/replication/spec.rs"
REPLICATION = "crates/core/src/replication.rs"
EXAMPLES = "cli/examples/**/*.yaml"
ALLOWLIST = "scripts/example-coverage-allow.txt"

CONNECTOR_KEYS = {"source": "source", "sources": "source", "sink": "sink", "sinks": "sink"}
SPEC_KEYS = {"type", "config", "ref", "transforms", "inherit_transforms", "status", "tags", "complete_for", "attributes"}


def fn_body(text, name):
    start = text.index(f"fn {name}(")
    depth, i = 0, text.index("{", start)
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[i : j + 1]
    raise ValueError(f"unterminated fn {name}")


def registered_kinds(text, name):
    return set(re.findall(r'\(\s*"([a-z0-9][a-z0-9_-]*)",', fn_body(text, name)))


def struct_body(text, name):
    start = text.index(f"pub struct {name} {{")
    return text[start : text.index("\n}\n", start)]


def snake(name):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def enum_variants(text, name, case=snake):
    start = text.index(f"pub enum {name} {{")
    body = text[start : text.index("\n}\n", start)]
    return {case(v) for v in re.findall(r"^\s{4}([A-Z][A-Za-z]*)\s*[,({]", body, re.M)}


def top_level_keys(text):
    keys = set()
    pending = None
    for line in struct_body(text, "PipelineConfig").splitlines():
        rename = re.search(r'rename\s*=\s*"([a-z_]+)"', line)
        if rename:
            pending = rename.group(1)
        field = re.match(r"\s*pub ([a-z_]+):", line)
        if field:
            keys.add(pending or field.group(1))
            pending = None
    return keys


def connector_specs(node, role=None):
    if isinstance(node, dict):
        if role and isinstance(node.get("type"), str) and set(node) <= SPEC_KEYS:
            yield role, node["type"]
        for key, value in node.items():
            if key in ("sources", "sinks") and isinstance(value, dict):
                for spec in value.values():
                    yield from connector_specs(spec, CONNECTOR_KEYS[key])
            else:
                yield from connector_specs(value, CONNECTOR_KEYS.get(key))
    elif isinstance(node, list):
        for item in node:
            yield from connector_specs(item)


def walk(node):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from walk(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk(item)


def observed(paths):
    seen = {k: set() for k in ("source", "sink", "write_mode", "mirror_mode", "replication_method", "config_key")}
    for path in paths:
        with open(path, encoding="utf-8") as f:
            try:
                doc = yaml.safe_load(f)
            except yaml.YAMLError:
                continue
        if not isinstance(doc, dict) or "pipeline" not in doc:
            continue
        seen["config_key"].update(doc)
        for role, kind in connector_specs(doc.get("pipeline", {})):
            seen[role].add(kind)
        for key in ("mirror", "replication"):
            block = doc.get(key)
            if isinstance(block, dict):
                seen["mirror_mode"].add(block.get("mode", "snapshot_then_cdc"))
                for role, kind in connector_specs(block):
                    seen[role].add(kind)
        for role, kind in connector_specs(doc.get("matrix", [])):
            seen[role].add(kind)
        for node in walk(doc):
            if isinstance(node.get("write_mode"), str):
                seen["write_mode"].add(node["write_mode"])
            method = node.get("replication_method")
            if isinstance(method, dict) and isinstance(method.get("type"), str):
                seen["replication_method"].add(method["type"])
            elif isinstance(method, str):
                seen["replication_method"].add(method)
    return seen


def allowlist(path=ALLOWLIST):
    allowed = set()
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.split("#", 1)[0].strip()
                if line:
                    allowed.add(line)
    except FileNotFoundError:
        pass
    return allowed


def required():
    with open(REGISTRY, encoding="utf-8") as f:
        registry = f.read()
    with open(CONFIG, encoding="utf-8") as f:
        config = f.read()
    with open(WRITE_MODE, encoding="utf-8") as f:
        write_mode = f.read()
    with open(MIRROR_SPEC, encoding="utf-8") as f:
        mirror = f.read()
    with open(REPLICATION, encoding="utf-8") as f:
        replication = f.read()
    return {
        "source": registered_kinds(registry, "builtin_source_descriptions"),
        "sink": registered_kinds(registry, "builtin_sink_descriptions"),
        "write_mode": enum_variants(write_mode, "WriteMode"),
        "mirror_mode": enum_variants(mirror, "ReplicationMode"),
        "replication_method": enum_variants(replication, "ReplicationMethod", case=str),
        "config_key": top_level_keys(config),
    }


def gaps(want, seen, allowed):
    missing, stale = [], []
    for category, names in sorted(want.items()):
        for name in sorted(names):
            tag = f"{category}:{name}"
            if name not in seen[category] and tag not in allowed:
                missing.append(tag)
            elif name in seen[category] and tag in allowed:
                stale.append(tag)
    return missing, stale


def main():
    want = required()
    seen = observed(sorted(glob.glob(EXAMPLES, recursive=True)))
    missing, stale = gaps(want, seen, allowlist())
    total = sum(len(v) for v in want.values())
    if missing or stale:
        if missing:
            print("No example under cli/examples/ uses:")
            for tag in missing:
                print(f"  {tag}")
            print("Add a validated example (modelled on an existing one) or, if it truly")
            print(f"cannot be exemplified, an entry with a reason in {ALLOWLIST}.")
        if stale:
            print(f"These {ALLOWLIST} entries now have an example; remove them:")
            for tag in stale:
                print(f"  {tag}")
        return 1
    print(f"example-coverage: all {total} connectors, modes and config blocks have an example")
    return 0


if __name__ == "__main__":
    sys.exit(main())

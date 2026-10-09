#!/usr/bin/env python3
"""Keep the agent-skills plugin version equal to the faucet-cli version (#853).

The skills ship with the engine: plugin `version` names the faucet release
whose behaviour they describe, and Claude Code / Codex update an installed
plugin only when that string changes.

Usage (from the repo root):
  scripts/plugin-version.py           check; exit 1 when a manifest differs or is malformed
  scripts/plugin-version.py --write   set every manifest's version to the faucet-cli version
Tests: python3 scripts/test_plugin_version.py
"""

import json
import sys
import tomllib

CLI_MANIFEST = "cli/Cargo.toml"
PLUGIN_MANIFESTS = (
    "skills/.claude-plugin/plugin.json",
    "skills/.codex-plugin/plugin.json",
)
MARKETPLACES = (
    ".claude-plugin/marketplace.json",
    ".agents/plugins/marketplace.json",
)


def cli_version(manifest_text):
    return tomllib.loads(manifest_text)["package"]["version"]


def plugin_version(text):
    version = json.loads(text).get("version")
    if not isinstance(version, str):
        raise ValueError("has no string `version`")
    return version


def set_plugin_version(text, version):
    doc = json.loads(text)
    doc["version"] = version
    return json.dumps(doc, indent=2, ensure_ascii=False) + "\n"


def main(argv, cli_manifest=CLI_MANIFEST, plugins=PLUGIN_MANIFESTS, marketplaces=MARKETPLACES):
    with open(cli_manifest, encoding="utf-8") as f:
        want = cli_version(f.read())
    write = "--write" in argv
    problems = []
    for path in marketplaces:
        with open(path, encoding="utf-8") as f:
            try:
                json.load(f)
            except json.JSONDecodeError as e:
                problems.append(f"{path}: invalid JSON: {e}")
    for path in plugins:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        try:
            have = plugin_version(text)
        except (ValueError, json.JSONDecodeError) as e:
            problems.append(f"{path}: {e}")
            continue
        if have == want:
            continue
        if write:
            with open(path, "w", encoding="utf-8") as f:
                f.write(set_plugin_version(text, want))
            print(f"plugin-version: {path} {have} -> {want}")
        else:
            problems.append(f"{path} version is {have}, but faucet-cli is {want} ({cli_manifest})")
    if problems:
        for p in problems:
            print(p)
        if not write:
            print("Run scripts/plugin-version.py --write to sync the plugin versions.")
        return 1
    if not write:
        print(f"plugin-version: every plugin manifest is at faucet-cli {want}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

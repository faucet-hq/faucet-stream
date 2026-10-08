#!/usr/bin/env python3
"""Keep the Helm chart's appVersion equal to the faucet-cli version (#830).

The chart's image tag defaults to `appVersion`, so a stale value makes an
install without `image.tag` pull an old image.

Usage (from the repo root):
  scripts/helm-app-version.py           check; exit 1 when they differ
  scripts/helm-app-version.py --write   set appVersion to the faucet-cli version
"""

import re
import sys
import tomllib

CHART = "deploy/helm/faucet-stream/Chart.yaml"
CLI_MANIFEST = "cli/Cargo.toml"

APP_VERSION = re.compile(r'^appVersion:[ \t]*(["\']?)([^"\'\s#]*)\1[ \t]*(#.*)?$', re.MULTILINE)


def cli_version(manifest_text):
    return tomllib.loads(manifest_text)["package"]["version"]


def app_version(chart_text):
    matches = APP_VERSION.findall(chart_text)
    if len(matches) != 1:
        raise ValueError(f"expected exactly one top-level appVersion, found {len(matches)}")
    return matches[0][1]


def set_app_version(chart_text, version):
    app_version(chart_text)
    return APP_VERSION.sub(f'appVersion: "{version}"', chart_text, count=1)


def main(argv, chart=CHART, manifest=CLI_MANIFEST):
    with open(manifest, encoding="utf-8") as f:
        want = cli_version(f.read())
    with open(chart, encoding="utf-8") as f:
        text = f.read()
    try:
        have = app_version(text)
    except ValueError as e:
        print(f"{chart}: {e}")
        return 1
    if have == want:
        print(f"helm-app-version: {chart} appVersion {have} matches faucet-cli")
        return 0
    if "--write" in argv:
        with open(chart, "w", encoding="utf-8") as f:
            f.write(set_app_version(text, want))
        print(f"helm-app-version: {chart} appVersion {have} -> {want}")
        return 0
    print(f"{chart} appVersion is {have}, but faucet-cli is {want} ({manifest}).")
    print("Run scripts/helm-app-version.py --write to sync it.")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

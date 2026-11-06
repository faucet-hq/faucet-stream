#!/usr/bin/env python3
"""Fail when a published crate would ship without its license texts.

faucet-stream is dual-licensed `MIT OR Apache-2.0`. The repository root holds
`LICENSE-APACHE` (the Apache License 2.0 exactly as published at
https://www.apache.org/licenses/LICENSE-2.0.txt) and `LICENSE-MIT`. Every
published workspace member must carry both files (a symlink to the root copy),
so `cargo package` includes the license texts in every crate.

Usage: scripts/license-files.py   (run from the repo root)
"""

import glob
import hashlib
import os
import sys
import tomllib

LICENSE_FILES = ("LICENSE-APACHE", "LICENSE-MIT")
APACHE_SHA256 = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"


def published_members(root):
    workspace = tomllib.load(open(os.path.join(root, "Cargo.toml"), "rb"))["workspace"]
    for pattern in workspace.get("members", []):
        for member in sorted(glob.glob(os.path.join(root, pattern))):
            manifest = os.path.join(member, "Cargo.toml")
            if not os.path.isfile(manifest):
                continue
            package = tomllib.load(open(manifest, "rb")).get("package", {})
            if package.get("publish", True) is False:
                continue
            yield os.path.relpath(member, root)


def problems(root):
    found = []
    apache = os.path.join(root, "LICENSE-APACHE")
    if not os.path.isfile(apache):
        found.append("LICENSE-APACHE is missing at the repository root")
    elif hashlib.sha256(open(apache, "rb").read()).hexdigest() != APACHE_SHA256:
        found.append(
            "LICENSE-APACHE is not the Apache License 2.0 text published at "
            "https://www.apache.org/licenses/LICENSE-2.0.txt"
        )
    if not os.path.isfile(os.path.join(root, "LICENSE-MIT")):
        found.append("LICENSE-MIT is missing at the repository root")
    for member in published_members(root):
        for name in LICENSE_FILES:
            path = os.path.join(root, member, name)
            if not os.path.isfile(path):
                found.append(f"{member}/{name} is missing (link it to the root copy)")
            elif os.path.realpath(path) != os.path.realpath(os.path.join(root, name)):
                found.append(f"{member}/{name} is not the root copy (make it a symlink to it)")
    return found


def main():
    found = problems(os.getcwd())
    for p in found:
        print(f"license-files: {p}", file=sys.stderr)
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main())

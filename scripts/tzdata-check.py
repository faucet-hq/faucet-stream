#!/usr/bin/env python3
"""The vendored chrono-tz is the one the workspace builds (#1118).

Checks that Cargo.lock resolves chrono-tz from `vendor/chrono-tz` (no
registry `source`, so the `[patch.crates-io]` is in effect) and that the
tzdata in `vendor/chrono-tz/tz/NEWS` and its generated tables are the release
pinned in `supply-chain/tzdata-version`. Regenerating the tables is the CI
job's other step (`cargo test --test codegen` in vendor/chrono-tz).
"""

import os
import re
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def lock_problems(lock_text):
    problems = []
    blocks = [b for b in lock_text.split("[[package]]") if re.search(r'^name = "chrono-tz"$', b, re.M)]
    if not blocks:
        return ["Cargo.lock has no chrono-tz package"]
    for b in blocks:
        source = re.search(r'^source = "([^"]+)"$', b, re.M)
        version = re.search(r'^version = "([^"]+)"$', b, re.M)
        if source:
            problems.append(
                f"Cargo.lock resolves chrono-tz {version.group(1) if version else '?'} from {source.group(1)}: "
                "the [patch.crates-io] entry for vendor/chrono-tz is not in effect"
            )
    return problems


def news_release(news_text):
    m = re.search(r"^Release (\S+) - ", news_text, re.M)
    return m.group(1) if m else None


def generated_release(directory_rs):
    m = re.search(r'IANA_TZDB_VERSION: &str = "([^"]+)"', directory_rs)
    return m.group(1) if m else None


def check(root):
    read = lambda rel: open(os.path.join(root, rel), encoding="utf-8").read()
    problems = lock_problems(read("Cargo.lock"))
    pin = read("supply-chain/tzdata-version").strip()
    news = news_release(read("vendor/chrono-tz/tz/NEWS"))
    generated = generated_release(read("vendor/chrono-tz/src/prebuilt/directory.rs"))
    if news != pin:
        problems.append(f"vendor/chrono-tz/tz is tzdata {news}, the pin is {pin}")
    if generated != pin:
        problems.append(f"vendor/chrono-tz/src/prebuilt was generated from tzdata {generated}, the pin is {pin}")
    return problems


def main():
    problems = check(ROOT)
    for p in problems:
        print(f"tzdata-check: {p}", file=sys.stderr)
    if not problems:
        print("tzdata-check: Cargo.lock uses vendor/chrono-tz; tz/ and src/prebuilt match the pin")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

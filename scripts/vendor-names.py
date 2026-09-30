#!/usr/bin/env python3
"""Fail when a third-party SaaS vendor name appears in the engine (#780).

Connectors and features describe mechanisms; vendor-specific material lives
in the template catalog (faucet-hq/template-hub). Protocols and standards
(OData, OpenLineage, Singer, OAuth2, GraphQL) and platforms that are a
connector's own identity (BigQuery, Snowflake, Kafka, SQL Server, Azure, ...)
are not listed.

Every tracked file is scanned (CHANGELOGs excepted). A name matches in any
spelling a codebase uses for an identifier: `HubSpot`, `hubspot`, `hub_spot`,
`HUB-SPOT`, `quick_books_token`, `QuickBooksClient`.

Usage: scripts/vendor-names.py [--list]   (run from the repo root)
Tests: python3 scripts/test_vendor_names.py
"""

import re
import subprocess
import sys

SKIPPED_FILES = re.compile(r"(^|/)CHANGELOG\.md$")

# Names matched in any case. A name with several parts also matches with a
# space, `_` or `-` between them (or nothing).
INSENSITIVE = [
    "shopify", "my shopify", "salesforce", "sforce", "soql", "hub spot", "air table",
    "net suite", "suite ql", "suite talk", "zen desk", "quick books", "intuit",
    "xero", "zoho", "bamboo hr", "rippling", "bullhorn", "acumatica", "sky slope",
    "rillet", "intacct", "jira", "atlassian", "google ads", "gaql",
    "google analytics", "ga4", "analytics data", "search console", "facebook",
    "meta marketing", "x business use case", "plaid", "share point", "one drive",
    "microsoft graph", "sap business one", "sap b1", "sobject", "sobjects", "sbqq",
    "oracle fusion", "marketo", "mail chimp", "klaviyo", "pipe drive", "workday",
    "fresh desk", "charge bee", "zuora", "docu sign", "adp",
]
# Names that are also ordinary words (an ORC stripe, a ramp), matched only
# capitalised or upper-case.
SENSITIVE = ["Stripe", "STRIPE", "SAP", "Sage", "Ramp", "Meta", "Greenhouse"]
# Literal fragments: domains, key prefixes, config spellings.
RAW = [
    r"stripe\.com", r"(?<![A-Za-z0-9])sk_(?:test|live)_", r"(?<![A-Za-z0-9])stripe(?:_|:[a-z])",
    r"adp\.com", r"graph\.microsoft",
]

# `(path, regex)` pairs a line may match and still pass: notes documenting a
# deprecated vendor-named spelling, kept so old configs keep working.
ALLOWED = [
    ("scripts/vendor-names.py", r".*"),
    ("scripts/test_vendor_names.py", r".*"),
]


def insensitive(name):
    body = "[ _-]?".join(re.escape(p) for p in name.split())
    # The lookarounds are case-sensitive, so a camelCase neighbour
    # (`newHubSpot`, `ShopifyBulk`) still counts as a word boundary.
    return rf"(?:(?<![A-Za-z0-9])|(?<=[a-z])(?=[A-Z]))(?i:{body})(?![a-z])"


def sensitive(name):
    return rf"(?<![A-Za-z0-9]){re.escape(name)}(?![a-z])"


DENY = re.compile(
    "|".join([*map(insensitive, INSENSITIVE), *map(sensitive, SENSITIVE), *RAW])
)


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files"], capture_output=True, text=True, check=True
    ).stdout
    return [f for f in out.splitlines() if not SKIPPED_FILES.search(f)]


def allowed(path, line):
    return any(path == p and re.search(rx, line) for p, rx in ALLOWED)


def scan(path, text):
    """Every vendor name in `text` (the contents of `path`) and in the path."""
    hits = []
    for n, line in enumerate(text.splitlines(), 1):
        m = DENY.search(line)
        if m and not allowed(path, line):
            hits.append(f"{path}:{n}: '{m.group(0)}': {line.strip()[:160]}")
    m = DENY.search(path)
    if m and not allowed(path, path):
        hits.append(f"{path}: file name contains '{m.group(0)}'")
    return hits


def main():
    hits = []
    for path in tracked_files():
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        hits += scan(path, text)
    if hits:
        print("\n".join(hits))
        if "--list" not in sys.argv:
            print(
                f"\n{len(hits)} vendor name(s) found. Describe the mechanism instead "
                "(vendor-specific material belongs in faucet-hq/template-hub).",
                file=sys.stderr,
            )
        return 1
    print("vendor-names: clean")
    return 0


if __name__ == "__main__":
    sys.exit(main())

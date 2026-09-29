#!/usr/bin/env python3
"""Fail when a third-party SaaS vendor name appears in the engine (#780).

Connectors and features describe mechanisms; vendor-specific material lives
in the template catalog (faucet-hq/template-hub). Protocols and standards
(OData, OpenLineage, Singer, OAuth2, GraphQL) and platforms that are a
connector's own identity (BigQuery, Snowflake, Kafka, SQL Server, Azure, ...)
are not listed.

Usage: scripts/vendor-names.py [--list]   (run from the repo root)
"""

import re
import subprocess
import sys

SCANNED = [
    "crates", "cli/src", "cli/tests", "cli/examples", "docs/book/src", "examples",
    "hub", "schemas", "README.md",
]
SKIPPED_FILES = re.compile(r"(^|/)CHANGELOG\.md$")

INSENSITIVE = [
    r"shopify", r"salesforce", r"sforce", r"\bsoql\b", r"hubspot", r"airtable",
    r"netsuite", r"suiteql", r"suitetalk", r"zendesk", r"quickbooks", r"\bintuit\b",
    r"\bxero\b", r"zoho", r"bamboohr", r"rippling", r"bullhorn", r"acumatica",
    r"skyslope", r"rillet", r"intacct", r"\bjira\b", r"atlassian", r"google[ _-]?ads",
    r"\bgaql\b", r"google analytics", r"\bga4\b", r"analyticsdata", r"search console",
    r"facebook", r"meta marketing", r"x-business-use-case", r"\bplaid\b",
    r"sharepoint", r"onedrive", r"microsoft graph", r"graph\.microsoft",
    r"sap business one", r"\bsap b1\b", r"\bsobjects?\b", r"sbqq", r"oracle fusion", r"stripe\.com",
    r"sk_(test|live)_", r"\bstripe_", r"\bstripe:[a-z]", r"adp\.com", r"marketo", r"mailchimp", r"klaviyo",
    r"pipedrive", r"workday", r"freshdesk", r"chargebee", r"zuora", r"docusign",
]
SENSITIVE = [
    r"\bStripe", r"\bSTRIPE_", r"\bADP\b", r"\bSAP\b", r"\bSage\b", r"\bRamp\b",
    r"\bMeta\b", r"\bGreenhouse\b",
]

# `path:regex` pairs a line may match and still pass: notes documenting a
# deprecated vendor-named spelling, kept so old configs keep working.
ALLOWED = [
    ("scripts/vendor-names.py", r".*"),
]

DENY = re.compile("|".join(f"(?i:{p})" for p in INSENSITIVE) + "|" + "|".join(SENSITIVE))


def tracked_files():
    out = subprocess.run(
        ["git", "ls-files", "--", *SCANNED], capture_output=True, text=True, check=True
    ).stdout
    return [f for f in out.splitlines() if not SKIPPED_FILES.search(f)]


def allowed(path, line):
    return any(path == p and re.search(rx, line) for p, rx in ALLOWED)


def main():
    hits = []
    for path in tracked_files():
        try:
            with open(path, encoding="utf-8") as fh:
                lines = fh.read().splitlines()
        except (UnicodeDecodeError, FileNotFoundError, IsADirectoryError):
            continue
        for n, line in enumerate(lines, 1):
            m = DENY.search(line)
            if m and not allowed(path, line):
                hits.append(f"{path}:{n}: '{m.group(0)}': {line.strip()[:160]}")
        m = DENY.search(path)
        if m:
            hits.append(f"{path}: file name contains '{m.group(0)}'")
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

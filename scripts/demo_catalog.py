#!/usr/bin/env python3
"""Seed a running `faucet serve` with a demo template catalog.

Used by `scripts/try-local.sh` so the web console's Templates page opens on a
catalog big enough to show what the list, the filters, and the compatibility
grid look like with real volume: 10 source templates and 10 sink templates
across several owners, in every lifecycle state (launched / draft /
deprecated), with a few partial pairings (a source stream a file sink cannot
write). It also registers a second, unlaunched version of
`faucet-hq/example-csv` with one extra parameter, so the trigger form's
per-version parameters have something to show.

Idempotent: a template id that is already registered is left alone, so the
script can run on every start without piling up versions.

Usage: demo_catalog.py [BASE_URL]   (default http://127.0.0.1:8899)
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8899").rstrip("/")

# (owner, name, description, [(stream, write modes)], state)
SOURCES = [
    ("acme", "billing-api", "Acme billing — invoices, payments and refunds from the internal REST API",
     [("invoices", "[upsert, append]"), ("payments", "[upsert, append]"), ("refunds", "[append]")], "launched"),
    ("acme", "crm-contacts", "Acme CRM — contacts and companies, incremental by updated_at",
     [("contacts", "[upsert, overwrite]"), ("companies", "[upsert, overwrite]")], "launched"),
    ("octo", "issue-tracker", "Issue tracker export — issues, comments and labels",
     [("issues", "[upsert, append]"), ("comments", "[append]"), ("labels", "[overwrite]")], "launched"),
    ("octo", "status-pages", "Public status page incidents and components",
     [("incidents", "[append]")], "draft"),
    ("northwind", "orders", "Northwind orders and order lines, nightly CSV drop",
     [("orders", "[overwrite, upsert]"), ("order_lines", "[overwrite, upsert]")], "launched"),
    ("northwind", "inventory", "Warehouse inventory snapshots",
     [("stock_levels", "[overwrite]")], "launched"),
    # `users` asks only for upsert, which a file sink cannot do: a partial pairing.
    ("lumen", "product-analytics", "Product analytics events, cursor-paginated",
     [("events", "[append]"), ("sessions", "[append]"), ("users", "[upsert]")], "launched"),
    ("lumen", "feature-flags", "Feature flag definitions and evaluations",
     [("flags", "[overwrite]"), ("evaluations", "[append]")], "draft"),
    ("faucet-hq", "example-hr", "Example — employees and departments from an HR CSV export",
     [("employees", "[overwrite, upsert]"), ("departments", "[overwrite]")], "launched"),
    ("faucet-hq", "legacy-erp", "Legacy ERP CSV extracts (superseded by the ERP API template)",
     [("gl_entries", "[append]")], "deprecated"),
]

# (owner, name, description, sink kind, state, param default)
SINKS = [
    ("acme", "warehouse-sqlite", "Acme analytics warehouse — local SQLite mirror", "sqlite", "launched", "./out/acme.db"),
    ("acme", "lake-jsonl", "Acme raw lake — one JSON Lines file per stream", "jsonl", "launched", "./out/lake"),
    ("octo", "exports-csv", "CSV exports for spreadsheets, one file per stream", "csv", "launched", "./out/csv"),
    ("octo", "scratch-sqlite", "Throwaway SQLite database for trying a pairing", "sqlite", "launched", "./out/scratch.db"),
    ("northwind", "reporting-sqlite", "Northwind reporting database (SQLite)", "sqlite", "launched", "./out/northwind.db"),
    ("northwind", "archive-jsonl", "Cold archive, appended JSON Lines", "jsonl", "draft", "./out/archive"),
    ("lumen", "events-jsonl", "Event stream landing zone (JSON Lines)", "jsonl", "launched", "./out/events"),
    ("lumen", "metrics-sqlite", "Metrics store (SQLite)", "sqlite", "launched", "./out/metrics.db"),
    ("faucet-hq", "csv", "Local CSV files, one per stream", "csv", "launched", "./out"),
    ("faucet-hq", "legacy-csv", "Legacy flat CSV layout (superseded by faucet-hq/csv)", "csv", "deprecated", "./out/legacy"),
]


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def source_doc(owner: str, name: str, desc: str, streams: list[tuple[str, str]]) -> str:
    lines = [
        "kind: source-template",
        f"name: {name}",
        f"owner: {owner}",
        f"description: {desc}",
        "params:",
        '  data_dir: { type: string, default: ./data, description: "Directory holding the CSV extracts" }',
        "source:",
        "  type: csv",
        "  config:",
        '    path: "${param.data_dir}/orders.csv"',
        "    has_headers: true",
        "streams:",
    ]
    for i, (stream, write) in enumerate(streams):
        lines.append(f"  - name: {stream}")
        if i:
            lines.append('    source: { config: { path: "${param.data_dir}/customers.csv" } }')
        lines += ["    primary_keys: [id]", f"    write: {write}"]
    return "\n".join(lines) + "\n"


def sink_doc(owner: str, name: str, desc: str, kind: str, default: str) -> str:
    head = [f"kind: sink-template", f"name: {name}", f"owner: {owner}", f"description: {desc}", "params:"]
    if kind == "sqlite":
        return "\n".join(head + [
            f'  db: {{ type: string, default: {default}, description: "Database file (created if missing)" }}',
            "sink:",
            "  type: sqlite",
            "  config:",
            '    database_url: "sqlite:${param.db}"',
            "    column_mapping: auto_map",
            "per_stream:",
            '  table_name: "${stream}"',
        ]) + "\n"
    ext = "jsonl" if kind == "jsonl" else "csv"
    append = "true" if name == "archive-jsonl" else "false"
    return "\n".join(head + [
        f'  dir: {{ type: string, default: {default}, description: "Output directory" }}',
        "sink:",
        f"  type: {kind}",
        "  config:",
        f"    append: {append}",
        "per_stream:",
        f'  path: "${{param.dir}}/${{stream}}.{ext}"',
        "write_mode_aliases:",
        "  overwrite: append",
    ]) + "\n"


def quoted(tid: str) -> str:
    return urllib.parse.quote(tid, safe="")


def register(tid: str, doc: str, state: str, existing: set[str]) -> str:
    if tid in existing:
        return "kept"
    code, resp = request("POST", "/v1/templates", {"id": tid, "config": doc, "config_format": "yaml"})
    if code >= 300:
        return f"failed ({code}: {resp.get('error', {}).get('message', resp)})"
    if state in ("launched", "deprecated"):
        request("POST", f"/v1/templates/{quoted(tid)}/launch", {"version": resp.get("version", 1)})
    if state == "deprecated":
        request("POST", f"/v1/templates/{quoted(tid)}/deprecate", {"reason": "superseded"})
    return state


def example_csv_v2() -> str:
    """A second example-csv version with one extra parameter, left unlaunched."""
    code, rec = request("GET", f"/v1/templates/{quoted('faucet-hq/example-csv')}?version=newest")
    if code != 200 or len(rec.get("versions") or []) > 1 or "batch_size" in (rec.get("params") or {}):
        return "kept"
    body = rec["body"]
    anchor = '  data_dir: { type: string, default: ./hub/examples/data, description: "Directory holding orders.csv and customers.csv" }'
    if anchor not in body or "    has_headers: true" not in body:
        return "skipped (unexpected body)"
    body = body.replace(anchor, anchor + '\n  batch_size: { type: int, default: 500, description: "Rows read per page from each CSV file" }', 1)
    body = body.replace("    has_headers: true", '    has_headers: true\n    batch_size: "${param.batch_size}"', 1)
    code, resp = request("POST", "/v1/templates", {"id": "faucet-hq/example-csv", "config": body, "config_format": "yaml"})
    return f"v{resp.get('version')} registered (not launched)" if code < 300 else f"failed ({code})"


def main() -> int:
    code, listing = request("GET", "/v1/templates")
    if code != 200:
        print(f"demo catalog: {BASE} is not answering /v1/templates ({code})", file=sys.stderr)
        return 1
    existing = {t["id"] for t in listing.get("templates", [])}
    failures = 0
    for owner, name, desc, streams, state in SOURCES:
        outcome = register(f"{owner}/{name}", source_doc(owner, name, desc, streams), state, existing)
        failures += outcome.startswith("failed")
        print(f"  source {owner}/{name}: {outcome}")
    for owner, name, desc, kind, state, default in SINKS:
        outcome = register(f"{owner}/{name}", sink_doc(owner, name, desc, kind, default), state, existing)
        failures += outcome.startswith("failed")
        print(f"  sink   {owner}/{name}: {outcome}")
    print(f"  faucet-hq/example-csv: {example_csv_v2()}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

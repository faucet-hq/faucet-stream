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

Every template it launches carries a `tests:` bundle: launching a version, or
putting it on a channel, needs passing tests (#859). Each bundle names its
companion (a source template names a sink, a sink template a source) and
holds one inline fixture. Drafts carry one too, so they can be launched from
the console.

With a demo workspace directory as the second argument it first seeds the
templates `scripts/try-local.sh` generates there (the per-country export and
its lifecycle, a failing version, a draft, a deprecated template, the hub
pair and the deployment overlay), when the registry is still empty — the
`--serve-only` path, which skips the CLI battery that normally registers them.

Idempotent: a template id that is already registered is left alone, so the
script can run on every start without piling up versions.

Usage: demo_catalog.py [BASE_URL [DEMO_DIR]]   (default http://127.0.0.1:8899)
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8899").rstrip("/")
DEMO_DIR = sys.argv[2] if len(sys.argv) > 2 else None

# The companions the catalog's bundles compose with: every source is tested
# against a SQLite sink (it can write every stream, upsert included) and every
# sink against a source whose streams any sink can take.
SOURCE_COMPANION = "octo/scratch-sqlite"
SINK_COMPANION = "northwind/orders"
# For the append-only sink: a source whose streams all accept append.
APPEND_COMPANION = "acme/billing-api"

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
        "  type: file",
        "  config:",
        '    path: "${param.data_dir}/orders.csv"',
        "    csv: { has_headers: true }",
        "streams:",
    ]
    for i, (stream, write) in enumerate(streams):
        lines.append(f"  - name: {stream}")
        if i:
            lines.append('    source: { config: { path: "${param.data_dir}/customers.csv" } }')
        lines += ["    primary_keys: [id]", f"    write: {write}"]
    return "\n".join(lines) + "\n" + bundle("sink", SOURCE_COMPANION, streams[0][0])


def bundle(companion_kind: str, companion: str, row: str) -> str:
    """A `tests:` block: the companion to compose with and one fixture."""
    return "\n".join([
        "tests:",
        f"  {companion_kind}: {companion}",
        "  fixtures:",
        f"    - name: {row}-pass-through",
        f"      row: {row}",
        "      input: [{ id: 1 }]",
        "      expect: { records: [{ id: 1 }] }",
    ]) + "\n"


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
        ]) + "\n" + bundle("source", SINK_COMPANION, "orders")
    ext = "jsonl" if kind == "jsonl" else "csv"
    lines = head + [
        f'  dir: {{ type: string, default: {default}, description: "Output directory" }}',
        "sink:",
        f"  type: {kind}",
        "  config:",
    ]
    if name == "archive-jsonl":
        # Keeps what earlier runs wrote, so it cannot stand in for overwrite:
        # append-only, and tested against a source with an append stream.
        lines += ["    append: true", "per_stream:", f'  path: "${{param.dir}}/${{stream}}.{ext}"']
        return "\n".join(lines) + "\n" + bundle("source", APPEND_COMPANION, "refunds")
    lines += [
        "    append: false",
        "per_stream:",
        f'  path: "${{param.dir}}/${{stream}}.{ext}"',
        "write_mode_aliases:",
        "  overwrite: append",
    ]
    return "\n".join(lines) + "\n" + bundle("source", SINK_COMPANION, "orders")


def quoted(tid: str) -> str:
    return urllib.parse.quote(tid, safe="")


def error_of(resp: dict) -> str:
    return str(resp.get("error", {}).get("message", resp)) if isinstance(resp, dict) else str(resp)


def post(path: str, body: dict) -> str | None:
    """POST; None on success, else the failure text."""
    code, resp = request("POST", path, body)
    return None if code < 300 else f"{code}: {error_of(resp)}"


def register(tid: str, doc: str, existing: set[str]) -> tuple[str, int | None]:
    """Register one version; returns (outcome, version). Red tests are fine
    here — a companion registered later re-tests it — so only a refused
    registration counts as a failure."""
    if tid in existing:
        return "kept", None
    code, resp = request("POST", "/v1/templates", {"id": tid, "config": doc, "config_format": "yaml"})
    if code >= 300:
        return f"failed ({code}: {error_of(resp)})", None
    return "registered", resp.get("version", 1)


def settle(tid: str, version: int, state: str) -> str:
    """Move a newly registered version to its demo state."""
    steps = []
    if state in ("launched", "deprecated"):
        steps.append(("launch", {"version": version}))
    if state == "deprecated":
        steps.append(("deprecate", {"reason": "superseded"}))
    for action, body in steps:
        err = post(f"/v1/templates/{quoted(tid)}/{action}", body)
        if err:
            return f"failed ({action} {err})"
    return state


def example_csv_v2() -> str:
    """A second example-csv version with one extra parameter, left unlaunched."""
    code, rec = request("GET", f"/v1/templates/{quoted('faucet-hq/example-csv')}?version=newest")
    if code != 200 or len(rec.get("versions") or []) > 1 or "batch_size" in (rec.get("params") or {}):
        return "kept"
    lines = rec["body"].split("\n")
    anchor = next((i for i, l in enumerate(lines) if l.startswith("  data_dir: {")), None)
    headers = next((i for i, l in enumerate(lines) if l == "      has_headers: true"), None)
    if anchor is None or headers is None:
        return "skipped (unexpected body)"
    lines.insert(headers + 1, '    batch_size: "${param.batch_size}"')
    lines.insert(anchor + 1, '  batch_size: { type: int, default: 500, description: "Rows read per page from each CSV file" }')
    code, resp = request("POST", "/v1/templates", {"id": "faucet-hq/example-csv", "config": "\n".join(lines), "config_format": "yaml"})
    return f"v{resp.get('version')} registered (not launched)" if code < 300 else f"failed ({code}: {error_of(resp)})"


def seed_core(demo_dir: str) -> int:
    """The templates the CLI battery registers, over HTTP, into an empty
    registry: the same lifecycle walk-through, so `--serve-only` opens on the
    same Templates page."""
    def doc(name: str) -> str:
        with open(os.path.join(demo_dir, name)) as f:
            return f.read()

    oid = "orders-by-country"
    steps = [
        ("register v1 + launch", "/v1/templates",
         {"config": doc("23_templated.yaml"), "description": "Per-country orders export", "launch": True}),
        ("register v2 on dev", "/v1/templates", {"config": doc("23_templated.yaml"), "tags": ["dev"]}),
        ("promote staging <- dev", f"/v1/templates/{oid}/tags", {"tag": "staging", "version": "dev"}),
        ("launch staging", f"/v1/templates/{oid}/launch", {"version": "staging"}),
        ("promote prod <- stable", f"/v1/templates/{oid}/tags", {"tag": "prod", "version": "stable"}),
        ("rollback", f"/v1/templates/{oid}/rollback", {}),
        ("launch newest", f"/v1/templates/{oid}/launch", {}),
        ("register a draft", "/v1/templates",
         {"id": "orders-by-country-next", "config": doc("23_templated.yaml"),
          "description": "Work in progress — not launched yet"}),
        ("register + launch orders-legacy-dump", "/v1/templates",
         {"id": "orders-legacy-dump", "config": doc("23_templated.yaml"),
          "description": "Superseded by orders-by-country", "launch": True}),
        ("deprecate orders-legacy-dump", "/v1/templates/orders-legacy-dump/deprecate",
         {"reason": "replaced by orders-by-country"}),
        # Red until faucet-hq/jsonl exists; registering that re-tests it.
        ("register example-csv", "/v1/templates", {"config": doc("templates/example-csv.yaml")}),
        ("register jsonl", "/v1/templates", {"config": doc("templates/jsonl.yaml")}),
        ("launch example-csv", f"/v1/templates/{quoted('faucet-hq/example-csv')}/launch", {}),
        ("launch jsonl", f"/v1/templates/{quoted('faucet-hq/jsonl')}/launch", {}),
        ("register sqlite (draft)", "/v1/templates", {"config": doc("templates/sqlite.yaml")}),
        ("register + launch demo-ops", "/v1/templates", {"config": doc("24_demo_ops.yaml"), "launch": True}),
    ]
    failures = 0
    for label, path, body in steps:
        err = post(path, body)
        failures += err is not None
        print(f"  core {label}: {err and 'failed (' + err + ')' or 'ok'}")
    # A version whose fixture fails: it registers, its tests are red, and its
    # launch is refused.
    code, resp = request("POST", "/v1/templates", {"config": doc("23_templated_broken.yaml")})
    passed = (resp.get("tests") or {}).get("passed") if code < 300 else None
    ok = code < 300 and passed is False
    if ok:
        refused = post(f"/v1/templates/{oid}/launch", {"version": resp.get("version")})
        ok = refused is not None
    failures += not ok
    print(f"  core failing version: {'ok (tests red, launch refused)' if ok else f'failed ({code}, passed={passed})'}")
    return failures


def main() -> int:
    code, listing = request("GET", "/v1/templates")
    if code != 200:
        print(f"demo catalog: {BASE} is not answering /v1/templates ({code})", file=sys.stderr)
        return 1
    existing = {t["id"] for t in listing.get("templates", [])}
    failures = 0
    if DEMO_DIR and not existing:
        failures += seed_core(DEMO_DIR)
    # Register everything first: a source's tests are red until its sink
    # companion exists, and registering the companion re-tests it. Then move
    # each new template to its state.
    registered = []
    for owner, name, desc, streams, state in SOURCES:
        tid = f"{owner}/{name}"
        outcome, version = register(tid, source_doc(owner, name, desc, streams), existing)
        registered.append(("source", tid, version, state, outcome))
    for owner, name, desc, kind, state, default in SINKS:
        tid = f"{owner}/{name}"
        outcome, version = register(tid, sink_doc(owner, name, desc, kind, default), existing)
        registered.append(("sink  ", tid, version, state, outcome))
    for side, tid, version, state, outcome in registered:
        if version is not None:
            outcome = settle(tid, version, state)
        failures += outcome.startswith("failed")
        print(f"  {side} {tid}: {outcome}")
    outcome = example_csv_v2()
    failures += outcome.startswith("failed")
    print(f"  faucet-hq/example-csv: {outcome}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

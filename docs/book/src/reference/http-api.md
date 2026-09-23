# HTTP API reference (`faucet serve`)

`faucet serve` exposes a JSON REST control plane for submitting, polling,
listing, cancelling, and streaming the logs of pipeline runs, plus
unauthenticated health and Prometheus endpoints. A machine-readable
[`docs/openapi.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/docs/openapi.yaml)
spec ships alongside this page and is kept in sync with the router by a CI test.

See the [serve cookbook](../cookbook/serve.md) for a guided quickstart, the
security model, and operational guidance. This page is the endpoint reference.

## Authentication

All `/v1/*` endpoints require `Authorization: Bearer <token>` unless the server
was started with `--no-auth`. The token is compared in constant time; the
`Authorization` header is the only accepted credential (no query-string auth).
`/healthz`, `/readyz`, and `/metrics` are always unauthenticated (probes /
scrapers). `OPTIONS` preflight bypasses auth so browsers behind a CORS policy
work.

### RBAC & the audit log (`--auth-config`)

A single `--auth-token` is one implicit **admin** principal. For a team
deployment, `--auth-config <file>` promotes the server to **role-based access
control**: a YAML/JSON file of principals, each a `{ name, token, role }`. Three
built-in roles form a ladder:

| Role | Permitted |
|------|-----------|
| `viewer` | read-only: `GET /v1/runs*`, `GET /v1/schemas*`, `GET /v1/catalog/*`, `GET /v1/templates*`, `GET /v1/local-outputs` |
| `operator` | everything a viewer can do **plus** submit / cancel / delete runs, `POST /v1/doctor`, firing triggers, registering / deleting / triggering pipeline templates, and deleting local sink outputs |
| `admin` | everything, including `GET /v1/audit` |

```yaml
# auth.yaml
principals:
  - { name: alice, token: "${env:ALICE_TOKEN}", role: admin }
  - { name: ci,    token: "${env:CI_TOKEN}",    role: operator }
  - { name: dash,  token: "${env:DASH_TOKEN}",  role: viewer }
```

```bash
faucet serve --auth-config auth.yaml
```

### The read / write / admin token trio

For the common split — dashboards read, operators write, one admin — there is
no file to author. Pass any subset of three flags (or their env vars) and the
server synthesizes the equivalent RBAC config:

```bash
faucet serve \
  --read-token  "$READ_TOKEN" \   # FAUCET_SERVE_READ_TOKEN  → viewer
  --write-token "$WRITE_TOKEN" \  # FAUCET_SERVE_WRITE_TOKEN → operator
  --admin-token "$ADMIN_TOKEN"     # FAUCET_SERVE_ADMIN_TOKEN → admin
```

Prefer the env vars: a flag value is visible in `ps`. The trio is mutually
exclusive with `--auth-token` / `--auth-config` / `--no-auth`, an empty token
is rejected at startup, and reusing one token for two roles is refused (the
role would otherwise depend on scan order).

### Role × route matrix

The contract, enforced by a table-driven test over **every** registered route
(`cli/tests/serve_rbac.rs`): a read token cannot reach anything that changes
state, and a route nobody classified stays admin-only and fails the test until
someone does.

| Route | viewer | operator | admin |
|---|:--:|:--:|:--:|
| `GET /v1/runs`, `/v1/runs/{id}`, `/v1/runs/{id}/logs` | ✓ | ✓ | ✓ |
| `POST /v1/runs`, `DELETE /v1/runs/{id}`, `POST /v1/runs/{id}/cancel` | — | ✓ | ✓ |
| `POST /v1/backfill` | — | ✓ | ✓ |
| `GET /v1/schemas`, `/v1/schemas/{kind}/{name}` | ✓ | ✓ | ✓ |
| `POST /v1/doctor` | — | ✓ | ✓ |
| `POST /v1/dlq/inspect` | ✓ | ✓ | ✓ |
| `POST /v1/dlq/replay`, `/v1/dlq/discard` | — | ✓ | ✓ |
| `POST`/`PUT /v1/triggers/{name}` | — | ✓ | ✓ |
| `GET /v1/catalog/*` | ✓ | ✓ | ✓ |
| `GET /v1/local-outputs`, `/v1/local-outputs/{id}/preview` | ✓ | ✓ | ✓ |
| `DELETE /v1/local-outputs/{id}`, `POST /v1/local-outputs/cleanup` | — | ✓ | ✓ |
| `GET /v1/templates`, `/v1/templates/{id}` | ✓ | ✓ | ✓ |
| `POST /v1/templates`, `DELETE /v1/templates/{id}` | — | ✓ | ✓ |
| `POST /v1/templates/{id}/{runs,tags,launch,rollback,deprecate}` | — | ✓ | ✓ |
| `POST /v1/templates/sync`, `POST /v1/templates/{id}/publish` | — | ✓ | ✓ |
| `POST /mcp` | ✓ | ✓ | ✓ |
| `GET /v1/audit` | — | — | ✓ |
| `POST /v1/reload` | — | — | ✓ |
| *any unclassified `/v1` route* | — | — | ✓ |

Two entries are POSTs a **read** token can reach, because they change nothing:

- `POST /mcp` — the MCP transport's baseline is a read scope; its one mutating
  tool (`run_pipeline`) re-checks `RunWrite` inside the handler.
- `POST /v1/dlq/inspect` — summarises a DLQ location. The location is
  caller-supplied, so a read token can ask the server to read a path on its
  filesystem. That is the same trust boundary as run logs (which carry record
  data), and the reason the control plane is not meant to face the public
  internet.

A request whose role lacks the route's required permission gets `403 forbidden`
(and a `denied` audit record). `--auth-config` is mutually exclusive with
`--auth-token` / `--no-auth`. Every token is registered for log redaction at
startup.

**Audit log.** Every mutating action (`run.submit` / `run.cancel` / `run.delete` /
`template.register` / `template.delete` / `template.run` / `template.promote` /
`local_output.delete` / `local_output.cleanup`)
and every denied attempt is recorded with principal, role, action, run id,
config fingerprint (submit), source IP, timestamp, and result. Admins read it via
`GET /v1/audit`. Records persist in the run-history backend (`faucet_serve_audit`
for the SQL backends; an in-memory ring otherwise) and expire with the
`--retain-terminal-runs-secs` window.

## Endpoints

| Method | Path | Success | Notes |
|--------|------|---------|-------|
| `POST` | `/v1/runs` | `202` | Submit a run; config validated synchronously |
| `GET` | `/v1/runs` | `200` | List runs (filters below) |
| `GET` | `/v1/runs/{id}` | `200` | Get one run record |
| `DELETE` | `/v1/runs/{id}` | `204` | Remove a terminal run from history |
| `POST` | `/v1/runs/{id}/cancel` | `202` / `200` | Request cancel (202) or no-op if terminal (200) |
| `GET` | `/v1/runs/{id}/logs` | `200` | Stream the run's logs (`text/event-stream`), or read persisted logs with `?format=jsonl\|text` |
| `POST` | `/v1/backfill` | `202` | Submit a windowed backfill: one tracked run per window unit (operator) |
| `GET` | `/v1/audit` | `200` | Read the audit log — **admin only** (RBAC). Filters: `principal`, `action`, `since`, `until`, `limit` |
| `POST` | `/v1/reload` | `200` / `422` | Hot-reload the `--default-config` merge base — **admin only** (RBAC). No-op (`reloaded:false`) if no default-config; `422` (old config kept) if the new one is invalid |
| `GET` | `/v1/catalog/datasets` | `200` | List catalogued datasets (`kind`, `q`, `limit`, `cursor`) — requires the `catalog` build feature |
| `GET` | `/v1/catalog/datasets/{id}` | `200` | One dataset's detail: schema timeline, volume, edges |
| `GET` | `/v1/catalog/lineage` | `200` | The lineage edge graph (`root`, `depth`) |
| `GET` | `/v1/local-outputs` | `200` | List tracked local sink output files with age + state (`dataset_id`, `pipeline`, `include_expired`, `limit`) — viewer / `LocalOutputRead` |
| `DELETE` | `/v1/local-outputs/{id}` | `200` | Delete one recorded output file now (operator / `LocalOutputManage`); `404` for an unknown id |
| `POST` | `/v1/local-outputs/cleanup` | `200` | Bulk clean: `older_than_days` \| `expired` \| `dataset_id` \| `run_id` \| `all`, plus `dry_run` (operator / `LocalOutputManage`) |
| `POST` | `/v1/templates` | `201` | Register a pipeline template (operator / `TemplateWrite`) — requires the `templates` build feature |
| `GET` | `/v1/templates` | `200` | List templates — newest version each, plus release state (viewer / `TemplateRead`) |
| `GET` | `/v1/templates/{id}` | `200` | One template version + its whole release state. `?version=stable` (default), another channel, or `?version=N` |
| `DELETE` | `/v1/templates/{id}` | `204` | Delete one version (`?version=<channel\|N>`) or all (operator / `TemplateWrite`) |
| `POST` | `/v1/templates/{id}/runs` | `202` | Trigger a run from a template with `params` / `env` (operator / `RunWrite`) |
| `POST` | `/v1/templates/{id}/tags` | `200` | Point an assignable channel (`prod`, `dev`, …) at a version (operator / `TemplateWrite`) |
| `POST` | `/v1/templates/{id}/launch` | `200` | Make a version live — moves `stable` and so unpinned callers (operator / `TemplateWrite`) |
| `POST` | `/v1/templates/{id}/rollback` | `200` | Re-launch `previous` (operator / `TemplateWrite`) |
| `POST` | `/v1/templates/{id}/deprecate` | `200` | Retire a template, or revive it with `{"undo":true}` (operator / `TemplateWrite`) |
| `POST` | `/v1/templates/sync` | `200` | Pull the `--templates-sync` origins into the registry — `{origin?, dry_run?}`; one report per origin, appends only (operator / `TemplateWrite`; requires the `templates-sync` feature; `422` when the server has no origins) |
| `POST` | `/v1/templates/{id}/publish` | `200` | Write one version back to an origin — `{origin, version?}` (operator / `TemplateWrite`; `templates-sync`) |
| `GET` | `/healthz` | `200` | Liveness (unauthenticated) |
| `GET` | `/readyz` | `200`/`503` | Readiness (unauthenticated) |
| `GET` | `/metrics` | `200` | Prometheus exposition (unauthenticated) |

### `POST /v1/runs`

Request body:

```json
{
  "config": "version: 1\npipeline:\n  source: {...}\n  sink: {...}\n",
  "config_format": "yaml",
  "name": "nightly-rollup",
  "labels": {"requester": "airflow"},
  "timeout_secs": 3600,
  "doctor_first": true,
  "idempotency_key": "airflow-task-123-attempt-2",
  "clock": "2026-05-29T00:00:00Z",
  "callback": {
    "url": "https://caller.example/jobs/abc/complete",
    "extra_fields": { "job_id": "abc" }
  }
}
```

- **`config`** (required) — the YAML or JSON pipeline body.
- **`config_format`** — `yaml` (default) or `json`.
- **`name`** — metadata; also drives the **state-key and metric identity** (see
  the cookbook's cardinality note). Two submissions sharing a `name` share
  replication bookmarks.
- **`labels`** — arbitrary string metadata, stored on the run record only.
- **`timeout_secs`** — wall-clock cap; on expiry the run is marked failed.
- **`doctor_first`** — run preflight probes before executing; on any failure the
  submit returns `422` with the doctor report in `error.details`.
- **`idempotency_key`** — replay protection (see cookbook).
- **`clock`** — overrides the `${now.*}` clock for backfills (default: submit time).
- **`concurrency`** — overrides this run's **connector** concurrency: how many
  concurrent connections/fetches the source and sink may use, whatever the
  config says. This is the multi-tenant knob — one template driving a customer
  with beefy read replicas and one with a small instance, without per-customer
  config copies or the template author pre-declaring a `${param.*}`. It maps
  onto whichever knob the connector declares (`max_connections` /
  `partition_concurrency` / `shard_concurrency` / `concurrency`), so a
  connector with none ignores it. It does **not** change matrix parallelism
  (`execution.max_concurrent`) or the server's `--max-concurrent` slots, and it
  caps only the *client* side — it cannot raise what the upstream will accept.
  Per-shard for a sharded run. `0` is rejected. It is part of the idempotency
  fingerprint, so replaying a key with a different value is a 409, not a
  replay.
- **`callback`** — a per-run completion callback; see below.

Response (`202`):

```json
{ "run_id": "0192…", "status": "queued", "submitted_at": "2026-05-29T12:00:00Z" }
```

A `--default-config` (if the server was started with one) is merged **under** the
submitted config (submitted values win).

### `GET /v1/runs`

Query parameters: `status`, `name`, `since`, `until` (RFC3339), `limit` (default
50, max 500), `cursor`. Ordering is `(submitted_at DESC, run_id DESC)`; `cursor`
is the last `run_id` from the previous page.

```json
{ "runs": [ { "run_id": "…", "status": "completed", … } ], "next_cursor": "0192…" }
```

### `GET /v1/runs/{id}` → `RunRecord`

```json
{
  "run_id": "0192…",
  "name": "nightly-rollup",
  "labels": {"requester": "airflow"},
  "status": "completed",
  "submitted_at": "…", "started_at": "…", "finished_at": "…",
  "elapsed_secs": 12.4,
  "records_written": 4096,
  "invocations": [
    {"row_id": "default", "parent_record_key": null, "records_written": 4096, "error": null}
  ],
  "error": null,
  "idempotency_key": "airflow-task-123-attempt-2",
  "doctor_report": null
}
```

`status` is one of `queued`, `running`, `completed`, `failed`, `cancelled`.
`elapsed_secs` is filled live for running runs.

> **Bookmarks:** run records carry record counts + per-row outcomes, not
> replication bookmarks. Bookmark state is per-row/per-state-key and lives in the
> configured [state backend](../cookbook/state.md), not in the run record.

### `GET /v1/runs/{id}/logs` (SSE)

`text/event-stream`. The server replays the run's bounded ring buffer, then
streams the live tail. Event types:

- `event: log` — one captured log line (subject to the server's `FAUCET_LOG`
  level; secrets are redacted).
- `event: truncated` — the reader fell behind and lines were dropped; rely on
  the centralized log sink for the full history.
- `event: end` — the run reached a terminal state; the stream closes.

The SSE buffer is **ephemeral**: it survives a short drain window after the run
finishes (independent of run-record retention), then is dropped. A known run
whose buffer has expired yields a single `end`.

```bash
curl -N -H "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/v1/runs/0192…/logs
```

#### Persisted logs — `?format=jsonl` / `?format=text` (#529)

With a persistent `--history` backend and `--log-retention-secs > 0`, captured
(redacted) log lines are also stored durably, so they can be fetched **any time
after the run ends** — past the SSE drain window, and from any instance in a
cluster. Add a `format` query parameter to switch the same endpoint from the SSE
stream to a paginated read:

- `?format=jsonl` → `application/x-ndjson`, one `{seq, ts, level, line}` object
  per line, oldest-first. Paginate with `?after=<seq>&limit=<n>` (`limit`
  defaults to 1000, max 10000). A trailing `{"truncated":true}` record means
  earlier lines were dropped by the per-run cap.
- `?format=text` → `text/plain`, the lines concatenated.

```bash
# First page of durable logs, as NDJSON:
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/runs/0192…/logs?format=jsonl&limit=500"
# Next page: pass the last seq you saw.
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/runs/0192…/logs?format=jsonl&after=500"
```

Retention is governed by **`--log-retention-secs`** (default `604800` = 7 days),
independent of run-record retention; `0` disables durable log persistence
(ephemeral SSE only). **`--log-max-lines-per-run`** (default `100000`) caps how
many lines are stored per run. The in-memory `--history` backend stays ephemeral
(no durable persistence).

### `GET /v1/catalog/*` (Data Movement Catalog)

Read-only browsing of the [Data Movement Catalog](../cookbook/catalog.md)
accumulated in the server's `--history` backend (every serve run records into
it automatically). Viewer-readable under RBAC; requires a build with the
`catalog` feature.

- `GET /v1/catalog/datasets?kind=&q=&limit=&cursor=` — paginated dataset list,
  ordered `(last_seen DESC, id DESC)`; `q` is a case-insensitive URI substring.
- `GET /v1/catalog/datasets/{id}` — the dataset plus its deduplicated schema
  timeline (each version with a `diff` vs the previous), recent per-run volume
  points, and upstream/downstream lineage edges. `404` for an unknown id.
- `GET /v1/catalog/lineage?root=&depth=` — the source→sink edge graph; with
  `root` (a dataset id), a BFS slice bounded by `depth` hops.

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/catalog/datasets?kind=postgres&limit=20"
```

### Local sink outputs

Lists and reclaims the **local files** the server's sinks wrote (jsonl / csv /
parquet). The control surface behind the console's Datasets-page cleanup
controls, and the same engine as the background sweeper described under
[Local output retention](cli.md#local-output-retention). Requires a build with
the `catalog` feature.

- `GET /v1/local-outputs?dataset_id=&pipeline=&include_expired=&limit=` — the
  tracked outputs, newest write first, each with `state`, `age_secs`, and the
  retention window in force. The response also carries the server's default
  `retention_days`, whether the sweeper is running (`gc_enabled`), and whether
  the **caller** may delete (`can_manage`), so a client can hide destructive
  controls rather than offer buttons that only 403.
- `DELETE /v1/local-outputs/{id}` — delete one file now.
- `GET /v1/local-outputs/{id}/preview?row_count_to_load=N` — the **first N rows
  of the file**, with their column names. Opt-in: inert (`403`, naming the flag)
  unless the server was started with `--preview-local-outputs`. See
  [Preview](#preview) below.
- `POST /v1/local-outputs/cleanup` — bulk clean. Exactly one scope:
  `{"older_than_days": N}`, `{"expired": true}` (each output's own window),
  `{"dataset_id": "…"}`, `{"run_id": "…"}` ("clean up after that run" — its
  history record is untouched), or `{"all": true}`. Sending none or several is a
  `400` rather than a guess. Add `"dry_run": true` to see what would go.

  A scope that ignores retention windows — `all`, and `older_than_days: 0`, which
  matches every output — also needs `"confirm": true`, or it is refused with a
  `400`. That is the same gate as the CLI's `--yes`, decided by the same
  predicate, so a scripted caller cannot inherit the console's confirm dialog by
  accident.

`state` is `present` (on disk), `expired` (collected — the record is kept), or
`external` (faucet wrote the file but did not create it).

**A refusal is a `200`, not an error.** The report carries `deleted: 0` and a
`skipped` reason: `pre_existing` (faucet did not create the file — never
deleted, by any scope), `in_flight` (the file may still be being written; retried
later), `not_on_disk` (already gone — a no-op, and the record is marked expired),
`already_deleted`, or `delete_failed`.

`in_flight` covers two cases, because one is not enough: the output's ledger row
names a run that is currently executing, **or** the file itself was touched
within `--local-output-in-flight-grace-secs` (default 60). The second is what
protects a *new* run rewriting a path the ledger still attributes to the previous
run — a run id the ledger has not recorded yet. Only recorded paths are ever touched:
never a glob, never a directory. Run history, catalog entries, and lineage are
untouched.

```bash
# What would "clean everything" remove?
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"all": true, "dry_run": true}' \
  http://127.0.0.1:8080/v1/local-outputs/cleanup

# Reclaim anything older than 3 days.
curl -X POST -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"older_than_days": 3}' \
  http://127.0.0.1:8080/v1/local-outputs/cleanup
```

#### Preview

`GET /v1/local-outputs/{id}/preview` reads a tracked output back and returns its
first rows — the other half of "N records written". It is a **source-backed capped
read**: the server builds the matching *source* connector for the output's kind
(`csv` → `source-csv`, `parquet` → `source-parquet`, `jsonl` → its JSON Lines
reader), pulls one page, and stops. A 100-row preview of a 4 GiB file reads its
first few kilobytes; nothing past the cap is decoded.

**It is off by default.** Without `--preview-local-outputs`
(`FAUCET_SERVE_PREVIEW_LOCAL_OUTPUTS`) every request is a `403` naming the flag,
for every role — it is a server capability, not a permission. Reading needs
`LocalOutputRead` (`viewer` and up), the same scope that lists these files, and a
served preview writes a `local_output.preview` **audit** entry naming the
principal, the output, and the row count: it is the one read on this control plane
that returns pipeline *data* rather than metadata about a pipeline, and "who read
this file" cannot be reconstructed after the fact.

An output in state `external` is **never** previewed (`403`). faucet wrote to that
file but did not create it, so its contents are not faucet's to hand out — the
read-side twin of the retention GC's refusal to delete it.

The request names a **ledger id**, never a path: the path comes from the row the
sink wrote, so a preview cannot be aimed at another file.

There is **no offset and no cursor** — these sources are sequential streams with
no row index, so `OFFSET N` could only mean "read N records and discard them",
which costs exactly what a larger limit costs. "Show me more" is spelled "raise
the limit", and the engine makes that cheap by stopping rather than truncating.

| Parameter | Behaviour |
|---|---|
| `row_count_to_load` omitted | The soft cap — `--preview-default-rows` / `FAUCET_SERVE_PREVIEW_DEFAULT_ROWS` (default 500). |
| `row_count_to_load=N` | `N`, clamped to the hard cap — `--preview-max-rows` / `FAUCET_SERVE_PREVIEW_MAX_ROWS` (default 5000). Never honoured above it. |
| `row_count_to_load=all` (or `0`) | The whole dataset — served in full only where the operator lifted the ceiling with `--preview-max-rows 0` (`preview_max_rows: null`); otherwise it resolves to the ceiling. |
| anything else | `400` naming the parameter — never a silent fall back to the default, which would let a capped read pass for a whole file. |

The response carries the rows, the `columns` across them (the table header; empty
when the records are not JSON objects), `row_count` (rows returned), the
`row_limit` the request resolved to (`null` = unlimited), the server's `max_rows`
(`null` = no ceiling), and `truncated` — which is *observed* (one row past the cap
is read) rather than inferred, so "exactly 500 rows" is distinguishable from
"capped at 500".

When `truncated` is true, **`capped_by` says which bound stopped the read**:
`rows` (the row limit), `bytes` (a 64 MiB response-size budget), or `time` (a 30s
deadline). The last two are what make an uncapped read safe to offer: a dataset
larger than the server can hold comes back as as much of it as fits, plus the
reason — never an out-of-memory, and never a clipped table that looks complete.
`capped_by` is absent when the response *is* the whole dataset.

Failure modes are all typed, and none of them is a 500:

| Status | Meaning |
|---|---|
| `403` | Previews disabled on this server; the role lacks `LocalOutputRead`; or the output is `external` — a file faucet wrote to but did not create, whose contents are not faucet's to serve (the same reason the retention GC will not delete it). |
| `404` | No such tracked output. |
| `409` | The file is gone — collected by retention, or removed out of band. The ledger row and the run record are kept; the message says so. |
| `422` | The file is there but unparseable (e.g. a half-written last line from a run that died mid-flush). The message carries the connector's own line/offset diagnostic. |
| `400` | The output's kind has no reader, or this build lacks the source connector for it. |
| `503` | The read was abandoned after the 60-second hard timeout — a single page that never returned, not a verdict on the file's contents. (The 30-second deadline is different: it yields a partial `200` with `capped_by: "time"`.) |

```bash
# The first 20 rows of a tracked output.
ID=$(curl -sH "Authorization: Bearer $TOKEN" \
  http://127.0.0.1:8080/v1/local-outputs | jq -r '.outputs[0].id')
curl -sH "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/local-outputs/$ID/preview?row_count_to_load=20" \
  | jq '{columns, row_count, truncated, capped_by}'

# Every row (needs a server started with --preview-max-rows 0; otherwise this
# comes back clamped to the ceiling, with capped_by: "rows").
curl -sH "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:8080/v1/local-outputs/$ID/preview?row_count_to_load=all" \
  | jq '{row_count, row_limit, truncated, capped_by}'
```

### `/v1/templates*` (template registry)

Register a template once, then trigger runs by `{id, params}` instead of
re-sending a config. The registry holds three **kinds** of document, told apart
by their `kind:` line: a `source-template` (one system — its connector, shared
transforms, and streams), a `sink-template` (one destination), and a complete
`pipeline`. A source template runs **composed** with a sink template named in the
trigger body; a pipeline runs alone; a sink template is never run on its own.
Storage rides the server's `--history` backend, so `faucet template …` and the
MCP template tools see the same registry. Requires a build with the `templates`
feature; see the [cookbook page](../cookbook/templates.md) and the
[Template Hub](../cookbook/template-hub.md).

```bash
# Register (the body is stored verbatim — ${env:…} / ${vault:…} stay unresolved).
curl -sX POST http://127.0.0.1:8080/v1/templates \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"id":"tenant-sync","config":"version: 1\nname: tenant-sync\n…","config_format":"yaml"}'
# → 201 {"id":"tenant-sync","version":1,"params":{…},"created_at":"…","created_by":"…"}

# Trigger a pipeline template.
curl -sX POST http://127.0.0.1:8080/v1/templates/tenant-sync/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"params":{"tenant_id":"acme"},"env":{"API_HOST":"eu.example.com"},"version":2}'
# → 202 {"run_id":"…","status":"queued","submitted_at":"…",
#        "template_id":"tenant-sync","template_version":2,
#        "params":{"tenant_id":"acme","api_token":"***"},"streams":[]}

# Register a source template and a sink template (their ids are their `name:`) …
curl -sX POST http://127.0.0.1:8080/v1/templates -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"config":"kind: source-template\nname: acme-billing\n…","launch":true}'
curl -sX POST http://127.0.0.1:8080/v1/templates -H "Authorization: Bearer $TOKEN" \
  -H 'content-type: application/json' -d '{"config":"kind: sink-template\nname: bigquery\n…","launch":true}'
curl -s "http://127.0.0.1:8080/v1/templates?kind=sink-template" -H "Authorization: Bearer $TOKEN"

# … and run the pairing: the trigger names the sink, and binds both halves' params.
curl -sX POST http://127.0.0.1:8080/v1/templates/acme-billing/runs \
  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
  -d '{"sink":"bigquery","sink_version":"stable","params":{"api_token":"…","bq_project":"my-project"}}'
# → 202 {"run_id":"…","template_id":"acme-billing","template_version":1,
#        "sink_template":"bigquery","sink_template_version":1,
#        "streams":[{"stream":"bills","requested":["overwrite","upsert"],"chosen":"overwrite","key":["id"]}, …],
#        "params":{"api_token":"***","bq_project":"my-project"}}
```

**Kinds.** `GET /v1/templates` rows carry `kind` (`?kind=` filters); rows written
before kinds existed read as `pipeline`. A source template is registered under
its `name` (an explicit `id` must match), its document is validated as a hub
template and run through the publishability lint (a literal credential or a
private hostname is a `422`), and a re-register can never change a template's
kind under the same id. A trigger on a source template without `sink` is a `422`
naming the field; `sink` on a pipeline template is a `422`; a trigger on a sink
template is a `422` pointing at the source side. The composed run's `name` is
the source template's, so its state keys (`{source}::{stream}`) survive a sink
swap, and the run is labelled `sink_template` / `sink_template_version` beside
`template` / `template_version`. Registering a document with no `kind:` still
works as a pipeline but is deprecated: add `kind: pipeline`.

**Registering never moves callers.** `POST /v1/templates` appends a version and
stops there; `POST /v1/templates/{id}/launch` is the one call that moves `stable`
and therefore every unpinned caller. So a template is `draft` until something is
launched (an unpinned trigger is a `422`), then `launched`, and `deprecated` once
retired — a deprecated template still serves pinned and `stable` callers, but the
trigger response carries a `deprecated` field. Pass `launch: true` on register to
do both in one call.

**Version selection.** Versions are numeric and auto-incrementing. On top of them
sits a closed channel set: three **derived** — `stable` (the launched version, and
what an omitted selector resolves to), `previous` (the rollback target), `newest`
(the build tip) — and six **assignable**: `dev`, `test`, `staging`, `pre-prod`,
`canary`, `prod`. There is deliberately no `latest`: it means both "newest build"
and "current release", so it is rejected with a message naming `stable` and
`newest`. `version` accepts a channel name (`"prod"`), a numeric string (`"2"`), or
a bare number (`2`), so a query string and a JSON body agree. `0` and unknown
channel names are rejected rather than silently falling back, and asking for an
*unset* channel is a `422` phrased for that channel (`stable` needs a launch,
`previous` needs a second launch, an environment channel needs a promote).

`POST /v1/templates/{id}/tags` moves an assignable channel:
`{"tag":"prod","version":"stable"}` copies whatever `stable` names today;
`{"tag":"prod","version":3}` pins one. A derived channel cannot be assigned
(`422`) — `stable` moves only via `launch`. `POST /v1/templates/{id}/launch`
defaults to `newest` and returns `{version, replaced, already_launched, status}`;
re-launching the live version is a no-op, which keeps `previous` a real rollback
target. `GET /v1/templates/{id}` returns `status`, `versions` (newest first),
`stable` / `previous` / `newest`, `is_stable`, the `tags` pointer map, and the
`launches` log — so a client can pin, promote, launch, or roll back without a
second request. Use `?version=newest` to read a `draft` template.

The trigger body's `params` / `env` / `version` / `sink` / `sink_version` are
template-specific; every other field (`name`, `labels`, `timeout_secs`,
`doctor_first`, `idempotency_key`, `clock`, `concurrency`) behaves exactly as in
`POST /v1/runs`, because the run is submitted through the same path. The run is
labelled `template` and `template_version` (plus `sink_template` /
`sink_template_version` for a composed run).

Status codes: `404` for an unknown id or pinned version; `422` for a missing
`required` param or a type mismatch, naming the param; `429` when the queue is
full. On a **clustered** server a template declaring `secret: true` params is
refused with `422` — the materialized config is persisted for peer execution, and
the shared history database is not a secret store. Reference the secret from the
template body (`${env:…}` / `${vault:…}`, resolved on the executing instance)
instead.

### `POST /v1/backfill`

Plans a `[from, to)` range into window units (chunked by `window`) and submits
**one tracked run per unit** — see the [backfill
cookbook](../cookbook/backfill.md) for the model.

```json
{
  "config": "version: 1\nname: orders\npipeline: {...}\n",
  "config_format": "yaml",
  "from": "2026-06-01",
  "to": "2026-07-01",
  "window": "1d",
  "timezone": "UTC",
  "name": "orders",
  "labels": {"requester": "airflow"},
  "timeout_secs": 3600
}
```

- **`config`** (required) — every root source must reference a `${backfill.*}`
  or `${now.*}` scoping token (400 otherwise). Bookmark-range backfills are
  CLI-only.
- **`from`** / **`to`** (required) — RFC3339 or `YYYY-MM-DD` (midnight in
  `timezone`), half-open.
- **`window`** / **`timezone`** — default to the config's `backfill:` block.
- **`name`** — base run name; unit runs are `{name}-backfill-{unit}` (the
  pipeline `name` is rewritten per unit so state keys never touch the live
  bookmark). `delivery` is forced to `at_least_once`; `timeout_secs` applies
  per unit.

`202` response: `{backfill, descriptor, planned, submitted, units: [{unit,
start, end, status, run_id?, error?}]}` where `backfill` is the stable range
hash carried as the `backfill` label on every unit run (plus a `backfill_unit`
label). Each unit is submitted with the deterministic idempotency key
`backfill:{hash}:{unit}`, so **re-POSTing the same body is replay-safe** —
already-submitted units replay their existing run, the rest submit (a full
queue marks the remainder `not_submitted`; re-POST to continue). A config
carrying `shard: {count}` makes each unit a sharded run tracked via shard
progress. Requires `RunWrite` (operator); audited as `backfill.submit`.

## Completion callbacks

Instead of polling, a submission can name an endpoint to be POSTed when the run
reaches a terminal state. The destination rides the *submission*, not the config,
so one registered pipeline (or template) can serve many callers each reporting to
their own endpoint.

```json
"callback": {
  "url": "https://caller.example/jobs/abc/complete",
  "method": "POST",
  "headers": { "X-Caller": "orchestrator" },
  "extra_fields": { "job_id": "abc" },
  "on": ["completed", "failed", "cancelled"]
}
```

Accepted on `POST /v1/runs` and `POST /v1/templates/{id}/runs`. The body:

```json
{
  "event": "run.completed",
  "run_id": "0192…",
  "status": "completed",
  "name": "nightly-rollup",
  "labels": { "requester": "airflow" },
  "submitted_at": "2026-05-29T12:00:00Z",
  "started_at": "2026-05-29T12:00:01Z",
  "finished_at": "2026-05-29T12:04:11Z",
  "elapsed_secs": 250.4,
  "records_written": 14203,
  "error": null,
  "attempt": 0,
  "job_id": "abc"
}
```

`run_id` is the id returned by the submission. `error` is redacted. `extra_fields`
are merged at the top level; a key colliding with any field above is refused with
`422` at submit time rather than silently dropped.

`on` defaults to **every** terminal status. Narrowing it is a footgun: a callback
subscribed only to `completed` never fires for a failed or cancelled run, and a
caller waiting on it will hang.

> **Delivery is at-most-once, and best-effort.** The callback fires from the
> in-process terminal transitions. It is **not** fired when a run is failed by
> lease-expiry orphan recovery, by cluster reclaim-poison, or by the
> sharded-parent completion sweep — those happen inside the history backend.
> So treat a missing callback as **unknown**, never as "still running", and
> reconcile against `GET /v1/runs/{id}`, which is always authoritative. A
> non-2xx response is retried a few times with backoff, then dropped with a
> warning; the run's recorded outcome is never affected.

Refusals (all `422` at submit time, so a bad destination never becomes a silent
no-op an hour later):

| Condition | Why |
|---|---|
| Scheme is not `http`/`https` | |
| Host is link-local / cloud-metadata (`169.254.0.0/16`, `fe80::/10`, `metadata.google.internal`, …) | Closes the instance-metadata SSRF hole. Override by naming the host in `--callback-allow-host`. |
| `--callback-allow-host` is set and the host is not in it | Explicit allowlist mode. |
| `headers` supplied on a **clustered** server | A clustered submit persists the run record — including these values — into the shared run-history database for a peer to execute, which would store them in clear text. Authenticate without a request header (e.g. a capability token in a single-use URL path), or submit to a non-clustered server. |
| `extra_fields` key collides with a faucet-emitted field | Would let a submission spoof the `status`/`event` a receiver keys off. |
| `on` contains a non-terminal status | |
| Supplied on `POST /v1/backfill` | One backfill POST fans out into N unit runs, so a single callback has no single run to describe. Poll the unit runs by their `backfill` label instead. |

**Egress posture.** This guard closes the metadata hole; it is not a general
egress control. A caller who can submit a run can already point a `rest` source
at an arbitrary address, so the deployment-level mitigations in the
[serve cookbook](../cookbook/serve.md) still apply. Use `--callback-allow-host`
(repeatable) when you want callbacks restricted to known receivers.

## Error envelope

Every error is a JSON `ApiError`:

```json
{ "error": { "code": "unprocessable", "message": "…", "details": { } } }
```

| Status | When |
|--------|------|
| `400` | Malformed body / parse / interpolation failure; a `schedule:` block in the config |
| `401` | Missing/invalid bearer token |
| `403` | Authenticated, but the principal's role lacks the required permission (RBAC) |
| `404` | Unknown `run_id` |
| `409` | `DELETE` on a running run; idempotency key reused with a different payload |
| `413` | Body exceeds `--body-limit-bytes` |
| `422` | Expand/validation failure; `doctor_first` failed (report in `details`) |
| `429` | Run queue full (carries `Retry-After`) |
| `500` | Internal error |

## Metrics

`/metrics` serves the standard `faucet_*` pipeline metrics plus serve-specific
series: `faucet_serve_requests_total{method,path,status}`,
`faucet_serve_request_duration_seconds{method,path}`, `faucet_serve_runs_queued`,
`faucet_serve_runs_in_flight`, `faucet_serve_runs_total{status,reason}`,
`faucet_serve_idempotency_hits_total`, and `faucet_serve_history_degraded`. See
[Observability](../operations/observability.md).

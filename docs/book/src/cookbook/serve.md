# Running faucet as a service (`faucet serve`)

`faucet serve` turns faucet from a one-shot CLI into a long-running HTTP control
plane: an orchestrator (Airflow, Temporal, Dagster, Argo) submits pipeline
configs over HTTP, polls status, cancels runs, and streams logs — while faucet
amortizes startup (TLS handshakes, connection pools, schema introspection)
across many runs in one process. It is the second supported runtime mode
alongside one-shot [`faucet run`](../reference/cli.md) and the cron
[`faucet schedule`](./scheduling.md).

The full endpoint/schema reference is in [HTTP API reference](../reference/http-api.md);
this page is the guided tour. `serve` requires the `serve` Cargo feature
(`cargo install faucet-cli --features serve`, or `--features full`).

## Quickstart

```bash
# Start the server (loopback by default). Auth is mandatory — see below.
FAUCET_SERVE_AUTH_TOKEN=s3cret faucet serve --listen 127.0.0.1:8080
```

```bash
# Submit a run.
curl -XPOST http://127.0.0.1:8080/v1/runs \
  -H "Authorization: Bearer s3cret" -H 'content-type: application/json' \
  -d '{"config":"version: 1\npipeline:\n  source: {type: file, config: { path: in.csv }}\n  sink: {type: file, config: { path: out.jsonl }}\n","name":"adhoc"}'
# → {"run_id":"0192…","status":"queued","submitted_at":"…"}

# Poll it to completion.
curl -H "Authorization: Bearer s3cret" http://127.0.0.1:8080/v1/runs/0192…

# Tail its logs (SSE).
curl -N -H "Authorization: Bearer s3cret" http://127.0.0.1:8080/v1/runs/0192…/logs
```

## ⚠️ Security model — read before exposing

`serve` executes **arbitrary client-supplied pipeline configs with the server's
identity**. That is a real privilege surface:

- **Full interpolation:** submitted configs resolve `${env:…}`, `${file:…}`,
  `${secret:…}`, and `${vault:…}`/`${aws-sm:…}`/… **against the server's**
  environment, filesystem, and credentials — exactly like `faucet run`. An
  authenticated caller can read any secret the server can reach.
- **SSRF / egress:** a submitted REST/HTTP source can be pointed at
  `169.254.169.254` or internal services and will be fetched with the server's
  network identity.

Mitigations are deployment-level and **mandatory**:

- Never run with `--no-auth` on a non-loopback bind. The no-auth gate is
  explicit: without `--auth-token`/`FAUCET_SERVE_AUTH_TOKEN` **and** without
  `--no-auth`, startup fails.
- Run single-tenant, behind authentication, behind egress controls / network
  policy. The default loopback bind (`127.0.0.1`) is deliberate — exposing
  externally is an explicit choice.
- Terminate TLS at a proxy/ingress (serve speaks plain HTTP).
- Prefer `FAUCET_SERVE_AUTH_TOKEN` over `--auth-token` (the latter leaks through
  `ps`/`/proc`).
- Never run a serve process at `FAUCET_LOG=debug` when submitted configs hold
  resolved secrets — only faucet's own log output is redacted, not third-party
  connector debug logging.

## RBAC & audit log

A single `--auth-token` is one implicit **admin** principal — fine for a personal
deployment, but a team needs scoped access and attribution. `--auth-config
<file>` enables **role-based access control**: a YAML/JSON list of principals,
each a `{ name, token, role }` where role is `viewer` (read-only), `operator`
(submit/cancel/delete runs, doctor, triggers), or `admin` (everything, including
the audit log).

```yaml
# auth.yaml — tokens resolve ${env:…} / ${file:…} / ${secret:…} and
# secret-manager references (${vault:…}, …) when the server starts
principals:
  - { name: alice, token: "${env:ALICE_TOKEN}", role: admin }
  - { name: ci,    token: "${env:CI_TOKEN}",    role: operator }
  - { name: dash,  token: "${env:DASH_TOKEN}",  role: viewer }
```

```bash
faucet serve --auth-config auth.yaml --history postgres://…/faucet
```

A viewer's `POST /v1/runs` returns `403`; its `GET /v1/runs` returns `200`.
`--auth-config` is mutually exclusive with `--auth-token` / `--no-auth`. A
reference that does not resolve refuses the start, and a token still holding
`${` is refused — never accepted literally. No config the server loads can read
those variables or files, nor `FAUCET_VAULT_KEY` and the server's own token
variables.

A config submitted over HTTP or MCP may not use the `singer` source or sink —
they run a program on the host — unless the server is started with
`--allow-subprocess-connectors`. Registered templates may.

**Every mutating action** (`run.submit` / `run.cancel` / `run.delete`) **and every
denied attempt** is written to a tamper-evident audit log — principal, role,
action, run id, config fingerprint, source IP, timestamp, result. Admins read it:

```bash
curl -H "Authorization: Bearer $ADMIN_TOKEN" \
  'http://127.0.0.1:8080/v1/audit?action=run.submit&limit=50'
```

Audit records persist in the run-history backend (`faucet_serve_audit` on the SQL
backends; an in-memory ring for the default backend, lost on restart) and expire
with `--retain-terminal-runs-secs`. For a durable trail, use a
`--history postgres://…`/`sqlite:…` backend.

## Bounded concurrency & backpressure

`--max-concurrent-runs` (default `min(16, cpu_count())`) bounds how many runs
execute at once; `--max-queued-runs` (default 8×) bounds the queue. A submit
past the queue cap returns `429` with `Retry-After`. Note that total concurrent
pipeline work ≈ `max-concurrent-runs × each config's execution.max_concurrent`.

## Idempotency

Supply `idempotency_key` to make retries safe (the common `Idempotency-Key` pattern):

- First submit with a key → runs normally.
- Re-submit the **same key + same request** within `--idempotency-retention-secs`
  (default 24h) → returns the **original** `run_id` (replayed, no new run).
- Same key + a **different** request → `409 Conflict`.
- After the retention window, the key is re-usable for a fresh run.
- Deleting a run also frees its idempotency key immediately — a later submit
  with that key starts a fresh run rather than 404-ing on the deleted record.

The "request" identity covers the merged config **and** the run-affecting
request fields — `clock`, `timeout_secs`, and `labels`. In particular, a retry
that reuses the key but changes the backfill `clock` is a `409`, not a replay of
the original window (so you can't silently get the original clock's results).

The claim is atomic, so concurrent retries can't both start a run.

> **Degraded mode:** while the persistent history backend is degraded (see
> [Run history](#run-history--persistence)), the in-memory fallback can't see
> claims the database made before the outage. Rather than risk a duplicate run,
> submissions **carrying an idempotency key are rejected with `503`** until the
> backend recovers — retry then, or resubmit without a key if at-least-once is
> acceptable. Submissions without a key are unaffected.

## Cancellation

`POST /v1/runs/{id}/cancel` cooperatively cancels an in-flight run (202); on an
already-terminal run it's a 200 no-op. The same cooperative path handles a run
that hits its `timeout_secs` and the server-shutdown drain.

Cancellation is **flush-completing**: the pipeline stops at its next page
boundary and flushes the sink, so a buffered sink (e.g. Parquet, whose footer is
only written on flush) commits the rows written so far rather than orphaning the
whole file (#146 H16). The run is then marked `cancelled`, with the records and
invocations it wrote before stopping — there is no
cross-process resume, so re-submit to continue. A run still stuck *mid-write*
after a bounded flush grace is hard-dropped (its buffered output may be lost),
so a hung run can't wedge shutdown.

## `--default-config` (workspace defaults)

Pass `--default-config <file>` to merge shared settings **under** every submitted
run (submitted values win; objects merge, scalars/arrays replace). Pin `state:`,
`execution:`, and the `auth:` catalog once instead of repeating them per request.
See [`cli/examples/serve_minimal.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/serve_minimal.yaml).

> **Cardinality:** a config's `name:` field drives the metric `pipeline` label and
> the state-key prefix. Use a **stable** `name:` per logical pipeline — never an
> ad-hoc per-run string — or Prometheus cardinality blows up. The request-level
> `name`/`labels` are run-record metadata only, never metric labels.

### Hot-reloading the default-config

After editing the `--default-config` file, reload it in place — no restart, no
interruption to in-flight runs (they already captured their config):

```bash
curl -fsS -X POST -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/reload
# → {"reloaded": true, "path": "…"}
```

`POST /v1/reload` is **admin-only** (RBAC `Reload` permission). It re-reads and
re-validates the file and atomically swaps the merge base; subsequent
submissions merge onto the new base. An **invalid** new config returns `422` and
the previous base is kept. When the server was started without
`--default-config`, it is a no-op (`{"reloaded": false}`).

## Run history & persistence

By default run records live in memory and are lost on restart. For durable
history across restarts, point `--history` at a database (requires the matching
build feature):

```bash
# Postgres (feature: serve-history-postgres)
faucet serve --history 'postgres://user:pw@db/faucet'
# SQLite (feature: serve-history-sqlite)
faucet serve --history 'sqlite:/var/lib/faucet/runs.db'
```

Both create their schema on first connect. If the backend is unreachable at
startup, or becomes unreachable at runtime (a refused or dropped connection, a
pool timeout, an I/O error, a server shutting down or out of connections — each
retried a few times first), serve **degrades** so it stays up: it logs once,
sets the `faucet_serve_history_degraded` gauge, and `/readyz` returns `503`.
Any other database error (a constraint, a decode failure, lock contention) is
returned to the request that hit it and never degrades the server.

While degraded, run records, logs, audit, catalog and usage go to an in-memory
store so runs keep going. Templates, tenants, connections, change requests and
`queue_depth` trigger edges are **not** served from that empty store: those
requests fail with `503` until the backend is back. Every lease tick probes the
backend and leaves degraded mode as soon as it answers; records written to
memory meanwhile are not copied back. Terminal records are retained for
`--retain-terminal-runs-secs` (default 7 days).

### Multi-instance orphan recovery (run-ownership leases)

A persistent backend can be **shared by several `faucet serve` instances** (an
HA pair, a rolling/blue-green deploy). Each instance gets a fresh id at startup
and *owns* the runs it executes; while a run is in flight its owner heartbeats a
**lease** on the run record (at ~⅓ of `--lease-ttl-secs`, default 30s). A run is
only recovered — marked `failed` with `owning serve instance's lease expired` —
once its lease has expired, i.e. its owner stopped heartbeating (crashed or was
shut down). Recovery runs both at startup and periodically, so a surviving
instance reclaims a dead peer's orphans without waiting for a restart.

This means a starting or running instance **never** fails another *live*
instance's in-flight runs — the gap that an unscoped "fail every non-terminal
run at startup" sweep would open on a shared database. Tune `--lease-ttl-secs`
above your worst-case GC/IO stall so a healthy-but-slow instance is never
falsely reclaimed (a longer TTL is safer but slows how quickly a crashed
instance's runs are cleaned up). The in-memory backend is single-process and
unshared, so leases don't apply to it. There is still no cross-process *resume*:
a recovered run is marked failed, not continued — re-submit to retry.

## Graceful shutdown

On `SIGTERM`/`SIGINT` the server starts **draining**: `/readyz` turns `503`
(`"draining": true`) so a load balancer stops routing to it, and every new
submission — runs, template triggers, webhook fires — is refused with `503`,
code `draining`, and `Retry-After: 5`. In-flight runs get
`--shutdown-grace-secs` (default 60) to finish; the remainder are cancelled
cooperatively. On a single instance they are marked failed (`server_shutdown`);
in cluster mode they are handed back as `pending` for a peer to claim, which
resumes them from their bookmark (at-least-once, like any failover). A
cancelled, timed-out or shutdown-interrupted run keeps the record counts and
invocations it reached, so its record shows the rows that did land.

## Health & observability

- `/healthz` — liveness (always 200 while serving).
- `/readyz` — 503 when history is degraded, the queue is full, or the server
  is draining.
- `/metrics` — Prometheus, including `faucet_serve_*` series. `/metrics` is
  unauthenticated; restrict it at the network layer if its labels are sensitive.

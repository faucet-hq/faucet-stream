---
name: faucet-operate
description: >-
  Use when operating a running `faucet serve` (single node or `--cluster`):
  `/readyz` returning 503 or a member reported unhealthy, the run-history
  database degraded or unreachable, runs stuck in pending or running, queued
  runs piling up or a full queue, runs waiting on an execution pool no member
  serves, a cluster member gone or crashed, a run reclaimed or executed twice
  after a failover, cancelling, retrying or deleting runs, finding which
  member owns a run, draining a member, a rolling upgrade of a cluster, and
  querying or pruning run history.
license: MIT OR Apache-2.0
---

# Operating `faucet serve`

This skill keeps a running server healthy: reading its health, unsticking
runs, losing or adding a member, upgrading, and managing run history.
Standing the server up is `faucet-deploy`; one pipeline's failure is
`faucet-debug`; building a client is `faucet-api`.

## Step 0: use the project's faucet version

The project decides which faucet to use, not whatever is on the `PATH`: a
config written for a newer faucet can fail or be misread on an older one.

1. Find the pin: the `"github:faucet-hq/faucet-stream"` entry under `[tools]`
   in the project's `mise.toml` (`faucet init` writes it). A config may also
   carry `requires_faucet: ">=X.Y"`.
2. Run `faucet --version`. Use that binary if it equals the pin (with no pin:
   if it satisfies every `requires_faucet` in the project).
3. Otherwise, if [mise](https://mise.jdx.dev) is installed, run `mise install`
   in the project and prefix every command with `mise exec --`.
4. Otherwise use the pinned container image; its entrypoint is `faucet`:
   `docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/faucet-hq/faucet-stream:<version> --version`.
5. Otherwise install exactly that version:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v<version>/faucet-cli-installer.sh | sh`.
6. With no pin at all, install the latest release (the same installer from
   `releases/latest/download/`) and pin it as described in
   [Pinning the faucet version](https://faucet-hq.github.io/faucet-stream/operations/pinning.html).

Every version-specific fact (which connectors and blocks exist, config keys,
types, defaults, commands and flags) comes from that binary: `faucet list`,
`faucet schema --help`, `faucet schema source|sink|transform <name>`,
`faucet <command> --help`. Never from memory or from this skill. When a config
passes `faucet validate`, set its `requires_faucet:` to `">=<major>.<minor>"`
of that binary; if validate rejects `requires_faucet` as an unknown field, the
binary predates it, so leave it out.

The server's own version is the `version` label of `faucet_build_info` on its
`/metrics` endpoint; read flags with `faucet serve --help` from that same
release.

## Golden rules

1. **Read before acting.** `/readyz`, `GET /v1/runs`, `GET /v1/runs/{id}`,
   `/metrics` and the logs change nothing. Read them first.
2. **Cancel, never kill.** A cancel stops at a page boundary and flushes the
   sink; killing a process mid-write can lose buffered output
   ([Cancellation](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html#cancellation)).
3. **A rerun can duplicate on append sinks.** Failover, reclaim and shutdown
   hand-back are at-least-once; recommend upsert or overwrite for pipelines on
   a cluster
   ([Delivery guarantees](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#delivery-guarantees-and-double-run-boundary)).
4. **Never edit the history database by hand.** Use the API.

## Health: `/healthz` vs `/readyz`

`/healthz` says the process serves; `/readyz` says it should get traffic.
Read the `/readyz` JSON body
([`/readyz` body](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#readyz-body)):
history, queue, draining, and on a cluster every live member with its pools
and load.

| `/readyz` says | Meaning | Do |
|---|---|---|
| draining | The member got a stop signal | Expected during a deploy; it hands unfinished runs back on a cluster |
| queue not ok | Submissions get `429` | Add capacity or members, or slow the callers |
| history not ok, single node | Degraded: history fell back to memory | Below |
| history not ok, cluster | The member cannot reach the shared database; it never falls back | Fix the database or the network path to it |

### When the history backend is degraded (single node)

What still works and what does not is in
[Run history & persistence](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html#run-history--persistence):
runs keep going with records in memory, while template, tenant, connection
and change-request calls, and idempotency-keyed submissions, fail with `503`.

1. Confirm it: `/readyz`, the history-degraded gauge in `/metrics`, and the
   one log line the server writes when it degrades.
2. Fix the backend (disk, credentials, network, the database itself). The
   server reconnects on its own; it does not need a restart.
3. Afterwards: records written to memory during the outage are **not**
   copied back. List what callers submitted in that window (their own logs
   or labels) and treat those runs as unrecorded. Any template, tenant or
   connection change a caller retried during the outage got a `503`;
   re-apply it now and confirm with a `GET`.

## Stuck runs

1. `GET /v1/runs/{id}`: status, attempt and timestamps. The record does not
   name its owner; on a cluster, `/readyz` lists the live members and their
   in-flight counts, and the owner's logs carry the run id.
2. **Pending for long:** on a cluster, check that some live member serves
   the run's pool
   ([Execution pools](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#execution-pools));
   a pool with no member never runs. Start a member of that pool, or cancel.
3. **Running, owner alive:** it is working or blocked. Read its logs
   (`GET /v1/runs/{id}/logs`) and use `faucet-debug`; a row held by another
   run fails with the holder named.
4. **Running, owner gone:** its lease expires and a survivor reclaims it
   (requeued up to the attempt cap, then failed), or on a single node the
   restarted server marks it failed
   ([Run lifecycle](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#run-lifecycle-in-cluster-mode),
   [orphan recovery](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html#multi-instance-orphan-recovery-run-ownership-leases)).
   Wait one lease period before acting.
5. **To stop it:** `POST /v1/runs/{id}/cancel` to any member; it reaches the
   owner within one poll interval
   ([Cross-instance cancel](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#cross-instance-cancel)).
6. **To retry:** re-submit with a new idempotency key (or the same key after
   deleting the failed run), since there is no cross-process resume; the
   pipeline resumes from its bookmark.

## Losing, adding or draining a member

- Losing one: survivors reclaim its runs once leases expire. Watch the
  reclaim metrics ([Prometheus metrics](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#prometheus-metrics)).
  Frequent reclaims of live members mean the lease is shorter than real
  stalls; raise it (`faucet serve --help`).
- Adding one: same image, env, secrets, vault key and default config as the
  rest ([Homogeneous deployment](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#2-homogeneous-deployment-shared-env--secrets)).
  A member missing a credential fails the runs it claims.
- Draining one: send it a stop signal and let its grace period run; it
  refuses new work and hands unfinished runs back
  ([Graceful shutdown](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html#graceful-shutdown)).

## Rolling upgrades

1. Read the release notes and
   [Upgrading faucet safely](https://faucet-hq.github.io/faucet-stream/operations/upgrading.html),
   including its
   [cluster section](https://faucet-hq.github.io/faucet-stream/operations/upgrading.html#clusters-faucet-serve---cluster)
   and [rolling upgrades and stored state](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#rolling-upgrades-and-stored-state).
2. Back up the history database and export state for critical pipelines.
3. Replace one member at a time: drain, upgrade, wait for `/readyz`, then
   the next. Mixed versions are expected meanwhile; features gated on every
   member (execution pools, new state formats) switch on only once the last
   old member is gone.
4. Expect replays of handed-back runs (golden rule 3).
5. After the last member: run `faucet template revalidate` (or let the server
   do it on start) and check that label-holding templates still pass
   ([Faucet upgrades and revalidation](https://faucet-hq.github.io/faucet-stream/cookbook/templates.html#faucet-upgrades-and-revalidation)).

## Run history

- Query it with `GET /v1/runs`: filter by status, name, time and labels
  (`?label=key:value`), page with the cursor
  ([`GET /v1/runs`](https://faucet-hq.github.io/faucet-stream/reference/http-api.html#get-v1runs)).
- Who did what: `GET /v1/audit` (admin).
- Retention of terminal runs and logs is set by server flags
  (`faucet serve --help`); templates, catalog and usage are never purged by
  run retention.
- `DELETE /v1/runs/{id}` removes a terminal run and frees its idempotency
  key.

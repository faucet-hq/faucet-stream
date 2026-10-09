# Deploying faucet

A pipeline config runs three ways, and all three read the same config and the
same state store, so a pipeline moves between them without losing its
bookmark:

| Runtime | Process | Who decides when it runs | Use it when |
|---|---|---|---|
| `faucet run` | Runs every row once, then exits | cron, a systemd timer, an orchestrator (Airflow, Dagster), a Kubernetes CronJob or Job | The pipeline is one step in a larger DAG, or the platform should own retries and history |
| [`faucet schedule`](../cookbook/scheduling.md) | Long-running, fires on the config's `schedule:` cron | faucet | One host or pod runs a pipeline on a timer with no orchestrator; auth tokens and pools stay warm between ticks |
| [`faucet serve`](../cookbook/serve.md) | Long-running HTTP control plane | Callers over HTTP, registered templates, the web console, [event triggers](../cookbook/triggers.md) | Many pipelines and callers, ad-hoc or API-driven runs, RBAC and an audit trail |

A continuous [`faucet mirror`](../cookbook/replication.md) is also
long-running; supervise it like `faucet schedule`. For history, use
[`faucet backfill`](../cookbook/backfill.md) rather than looping `faucet run`.

## One run per row at a time

Two runs of the same row would read the same bookmark and race on the next
one. Every real run takes a lease on its row in the `state:` store, and a
second `faucet run` / `faucet schedule` of a row whose lease is live fails
(see `--force` in the [CLI reference](../reference/cli.md)). The lease needs a
durable store that both processes share, so let the scheduler serialize runs
too:

| Scheduler | Setting |
|---|---|
| `faucet schedule` | an `overlap_policy` (see [Overlap policy](../cookbook/scheduling.md#overlap-policy)); one scheduler process per config |
| Kubernetes CronJob | `concurrencyPolicy: Forbid` |
| Airflow | `max_active_runs=1` on the DAG |
| cron | `flock -n /var/lock/<name>.lock faucet run …` |

## Calling `faucet run` from an orchestrator

faucet is one binary, so an orchestrator task is a shell command. Read the
exit code, and the [machine-readable summary](../reference/cli.md#run---output)
on stdout; logs stay on stderr:

```bash
faucet run /etc/faucet/orders.yaml --no-env-file --log-format json --output json > summary.json
```

- `--no-env-file` stops faucet from loading a stray `.env` from the working
  directory; inject variables from the platform instead.
- `--log-format json` makes each stderr line one JSON object with `pipeline`,
  `row`, `run_id` and `connector` as fields
  ([structured logs](./observability.md#structured-json-logs)).
- A [run budget](../cookbook/usage.md) (`--max-duration-secs` and friends) caps
  one invocation; keep it under the schedule period.

A one-shot run exits before Prometheus can reliably scrape it, so alert on the
task or Job failure and on [`faucet status`](../cookbook/state-and-status.md),
whose exit code reports healthy / degraded / failed.

### Cron

```bash
# crontab: every 15 minutes, never two at once
*/15 * * * * flock -n /var/lock/events.lock faucet run /etc/faucet/events.yaml --no-env-file >> /var/log/faucet.log 2>&1
```

## Choosing a state backend

The state store holds each row's bookmark, its exactly-once watermark and its
run markers ([State stores](../cookbook/state.md#state-stores)). If it forgets,
the next run starts from scratch.

| Where it runs | `memory` | `file` | `redis` | `postgres` |
|---|---|---|---|---|
| Laptop, tests | yes | yes | yes | yes |
| One VM or systemd unit with a local disk | no | yes | yes | yes |
| Container without a persistent volume | no | no (lost with the container) | yes | yes |
| Kubernetes CronJob / Job | no | only with a ReadWriteOnce volume and `concurrencyPolicy: Forbid` | yes | yes |
| `faucet schedule` as a Deployment | no | only with a volume, one replica, `Recreate` strategy | yes | yes |
| `faucet serve` with several replicas | no | no | yes | yes |

- `memory` never persists; a production config must not use it.
- `redis` needs persistence (AOF or RDB) turned on, or a Redis restart loses
  bookmarks. Give each environment its own `namespace`.
- `postgres` is the transactional default, and a natural choice when the sink
  is already Postgres.
- Environments that must not share progress need a different `name:` or a
  different state location.

`faucet doctor` probes the state store with a sentinel write that leaves
nothing behind; run it at deploy time. Back state up with `faucet state
export` before every upgrade or backend move
([Backup, restore, and moving between backends](../cookbook/state-and-status.md#backup-restore-and-moving-between-backends)).

## Containers

Connectors are compile-time features, so the image decides what can run.
Build or pull one as described in
[`deploy/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/deploy/README.md)
and the [installation page](../getting-started/installation.md), then confirm
what it holds before you deploy:

```bash
docker run --rm ghcr.io/faucet-hq/faucet-stream:<version>-<profile> list
```

Pin `<version>-<profile>`, never a moving tag. The image's default command is
`serve`, which refuses to start without auth; pass an explicit command
(`run …`, `schedule …`, or `serve` with auth). With `faucet run --from-env`
the whole pipeline can come from `FAUCET_*` environment variables, with no
config file in the image
([environment-only mode](../reference/cli.md#environment-only-mode)).

## Kubernetes

| Runtime | Object | Must-haves |
|---|---|---|
| `faucet run` on a timer | CronJob | `concurrencyPolicy: Forbid`, `restartPolicy: Never`, `activeDeadlineSeconds` under the period, a small `backoffLimit`, redis or postgres state |
| `faucet run` once (migration, backfill) | Job | the same, plus `ttlSecondsAfterFinished` |
| `faucet schedule` | Deployment, `replicas: 1`, `strategy: Recreate` | `terminationGracePeriodSeconds` above the schedule's shutdown grace |
| `faucet serve` | Deployment + Service | auth, a persistent history backend, readiness on `/readyz`, liveness on `/healthz`, `--cluster` for more than one replica |

The [Helm chart](https://github.com/faucet-hq/faucet-stream/tree/main/deploy/helm/faucet-stream)
renders all of these, verifies at startup that the image holds the connectors
you declare, and ships hardened pod defaults (non-root, read-only root
filesystem, capabilities dropped). Its Job and CronJob mount an `emptyDir`, so
`file` state is lost after every run there: use redis or postgres state with
the chart. Values and auth modes are in the chart's README.

### Kubernetes (Helm)

The Helm chart deploys the `faucet serve` control plane (Deployment), one-shot
pipelines (Job) and scheduled pipelines (CronJob):

```bash
helm install faucet oci://ghcr.io/faucet-hq/charts/faucet-stream -f values.yaml
```

Every `faucet serve` feature has a values block, so nothing needs raw
`serve.extraArgs`:

| Values block | What it turns on |
|---|---|
| `serve.triggers` | event-driven triggers (`--triggers`) |
| `serve.templatesSync` | template hosting + sync (`--templates-sync`) |
| `serve.policy` | data-flow policy on every submission (`--policy`) |
| `serve.connectProviders` | hosted OAuth connect for tenants (`--connect-providers`) |
| `serve.otel` | OTLP traces / metrics / logs for the server (`--otel-config`) |
| `serve.tenants` | tenants; the vault key comes from a Secret as `FAUCET_VAULT_KEY` |
| `serve.approvals` | change requests (`--require-approval`, `--approval-expiry-secs`) |
| `serve.mcp` | the `/mcp` endpoint (`--mcp`, `--mcp-allow-mutations`) |
| `extraVolumes` / `extraVolumeMounts` | extra volumes on the serve, Job and CronJob pods |

The file-backed blocks take the file inline (`content`, rendered to a
ConfigMap), from `existingConfigMap`, or from `existingSecret`. The render
fails on combinations the server would refuse at start (tenants without a vault
key or with in-memory history, template approvals together with template
sync, and similar). Behind an Ingress, the run-log stream
(`/v1/runs/{id}/logs`, Server-Sent Events) needs proxy buffering off and a long
read timeout (the chart sets both for ingress-nginx), and
`/v1/connect/callback` must be publicly reachable for hosted OAuth connect.

The chart's `examples/everything.yaml` turns every feature on; its
[README](https://github.com/faucet-hq/faucet-stream/tree/main/deploy/helm/faucet-stream)
is the full values reference.

## Secrets

Never commit secrets. Use `${env:VAR}` / `${file:PATH}` in the config and inject
real values through your platform's secret mechanism (Kubernetes Secrets loaded
with `envFrom`, file-shaped credentials mounted read-only), or a
[secrets manager](../cookbook/secrets.md) reference. Pass `--no-env-file` so a
stray `.env` is never read.

## Hardening

- **TLS.** `faucet serve` speaks plain HTTP: terminate TLS at an ingress or a
  proxy. Set TLS explicitly on every connector connection, the state store
  and the history database; some connectors default to plaintext.
- **Unauthenticated endpoints.** `/metrics`, `/healthz` and `/readyz` carry no
  auth; keep them on loopback or behind a network policy.
- **Serve is code execution.** A submitted config runs with the server's
  identity and network; read the
  [serve security model](../cookbook/serve.md) before exposing it. Never use
  `--no-auth` beyond loopback, prefer RBAC with `viewer` as the default role,
  restrict egress, and keep `--preview-local-outputs` off outside development.
- **Logs.** Use `--log-format json` and `info` or quieter. Never run at debug
  level with resolved secrets: third-party connector output is outside faucet's
  redaction.
- **Data at rest.** The DLQ holds raw failed records and bookmarks can hold
  key values: give both the access controls of the source data, and configure
  [masking](../cookbook/masking.md) where the source has PII.
- **Upgrades.** Pin the binary and image versions; export state and check it
  with `faucet migrate --state --check` before upgrading
  ([Upgrading faucet safely](./upgrading.md)).

## Exit codes & retries

`faucet run` exits non-zero when a pipeline fails (subject to the
`execution.on_error` policy and any DLQ). Let your scheduler's retry/alert
mechanism react to a non-zero exit; because bookmarks only advance after the sink
confirms, a retried run resumes safely. Under the default at-least-once delivery
the page in flight when a run died can be written twice; a keyed upsert or
[effectively-once delivery](../cookbook/state.md#effectively-once-delivery)
makes the destination immune.

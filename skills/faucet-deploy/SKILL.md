---
name: faucet-deploy
description: >-
  Use when deploying faucet to production or running it unattended: running
  pipelines on cron, systemd, Airflow, Dagster or Kubernetes (Job, CronJob,
  Deployment), using `faucet schedule`, standing up `faucet serve` with auth,
  RBAC, audit and run history, installing the Helm chart or choosing and
  building a container image, choosing a durable state backend, wiring
  Prometheus, OpenTelemetry, Grafana dashboards and alerts, setting up event
  triggers, checking that a build has the features a deployment needs, and
  hardening a deployment (TLS, secrets, log levels, network binds, least
  privilege).
license: MIT OR Apache-2.0
---

# Deploying faucet

`faucet` runs a pipeline config three ways: once (`faucet run`), on its own
cron (`faucet schedule`), or as an HTTP control plane (`faucet serve`). All
three read the same config and the same state store. This skill is how to pick
one, run it so it survives restarts and upgrades, and know when it breaks.

Writing the pipeline config itself is the `faucet-pipelines` skill. Fixing a
failed or stuck pipeline is the `faucet-debug` skill.

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

## Step 1: pick the runtime

Use the runtime table in
[Deploying faucet](https://faucet-hq.github.io/faucet-stream/operations/deploying.html):
`faucet run` when something else owns the timer (cron, systemd, an
orchestrator, a Kubernetes CronJob), `faucet schedule` for one pipeline on a
timer with no orchestrator, `faucet serve` for many pipelines, API callers,
RBAC and audit, and `faucet serve` with triggers for runs fired by a webhook,
an object landing or a queue filling.

Whatever you pick, **one row must never run twice at the same time**. Make the
scheduler enforce it as well as the run lease
([one run per row at a time](https://faucet-hq.github.io/faucet-stream/operations/deploying.html#one-run-per-row-at-a-time)).

## Step 2: confirm the build has what the deployment needs

Connectors, state backends and runtime features (schedule, serve, its history
backends, triggers, OTLP export, secret managers, notifications, catalog) are
compile-time features, and builds differ: the prebuilt binary, each published
image profile, and a custom build. Never assume; ask the exact binary or image
you will deploy:

```bash
faucet --version
faucet list
faucet --help
faucet schema --help
faucet serve --help
```

- `faucet list` names the connectors and state stores compiled in.
- `faucet --help` lists the subcommands; a missing `schedule` or `serve` means
  the build lacks it.
- `faucet schema --help` lists the config blocks this build documents; a block
  it lacks is rejected as an unknown field.
- A feature behind a flag (a history backend, triggers) may fail only at
  start: start the server once against a scratch config before you rely on it.

For an image, run the same commands through it
(`docker run --rm <image> list`). If something is missing, pick another image
profile or build one: [installation](https://faucet-hq.github.io/faucet-stream/getting-started/installation.html)
and [`deploy/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/deploy/README.md).

## Step 3: validate the config for unattended use

```bash
faucet validate --no-secrets --no-env-file pipeline.yaml
faucet doctor pipeline.yaml
```

Run the first in CI with placeholder values for every `${env:…}`; it checks
structure without credentials. A secrets-manager reference passes
`--no-secrets` even on a build that cannot resolve it, so also run
`faucet validate pipeline.yaml` (no flag) where the credentials exist. Run
`faucet doctor` at deploy time: it probes every connector and the state store.

Choose a durable state backend for where the process runs
([choosing a state backend](https://faucet-hq.github.io/faucet-stream/operations/deploying.html#choosing-a-state-backend));
read its keys from `faucet schema config` and the state section of
[State stores](https://faucet-hq.github.io/faucet-stream/cookbook/state.html#state-stores).
Never `memory` in production.

## Checklist: `faucet run` from an orchestrator, cron or CronJob

1. Durable state, shared by every process that runs the pipeline.
2. Invoke with `--no-env-file --log-format json --output json`; read the exit
   code and the stdout summary
   ([calling `faucet run` from an orchestrator](https://faucet-hq.github.io/faucet-stream/operations/deploying.html#calling-faucet-run-from-an-orchestrator)).
3. Serialize runs of one config in the scheduler; keep a timeout below the
   period (a Job deadline, or a run budget: `faucet run --help`).
4. Credentials from platform secrets as env vars or mounted files.
5. Alert on task or Job failure, and on `faucet status` (its exit code reports
   healthy / degraded / failed). A one-shot run is usually gone before a
   Prometheus scrape.

```bash
faucet run /etc/faucet/orders.yaml --no-env-file --log-format json --output json
```

A production-shaped config that works for both `faucet run` and
`faucet schedule` is `cli/examples/scheduled_production.yaml` in the
faucet-stream repository ([`cli/examples`](https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples);
read it at the tag `faucet-cli-v<version>` matching your binary).

## Checklist: `faucet schedule`

1. A `schedule:` block. Read its keys with `faucet schema schedule`; decide the
   overlap policy, a failure limit that exits non-zero so the supervisor
   notices, a run timeout under the period, and a shutdown grace longer than
   the slowest flush ([Scheduling pipelines](https://faucet-hq.github.io/faucet-stream/cookbook/scheduling.html)).
   Examples: `cli/examples/scheduled_production.yaml`,
   `cli/examples/scheduled_nightly.yaml`.
2. A supervisor that restarts on a non-zero exit (systemd `Restart=on-failure`,
   or a one-replica Deployment with the `Recreate` strategy; a rolling update
   briefly runs two schedulers).
3. The supervisor's stop timeout (`terminationGracePeriodSeconds`,
   `TimeoutStopSec`) above the schedule's shutdown grace.
4. A Prometheus endpoint in the config and alerts on the scheduler heartbeat
   and consecutive failures.
5. SIGHUP reloads the config in place; an invalid file is rejected and the
   old one keeps running.

```bash
faucet validate /etc/faucet/orders.yaml
faucet schedule /etc/faucet/orders.yaml --no-env-file --log-format json
```

`faucet validate` reports whether the `schedule:` block parses.

## Checklist: `faucet serve`

Read [Running faucet as a service](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html)
first: a submitted config runs with the server's identity, so serve is code
execution on that host. Every flag below is in `faucet serve --help`.

1. Auth: never `--no-auth` beyond loopback. Prefer RBAC (an `--auth-config`
   file of named principals, or the per-role tokens) over one admin token;
   give `viewer` by default and `operator` only to orchestrators. Pass tokens
   through env vars or Secret-mounted files, never as command-line values. The
   file format and roles: [RBAC & audit log](https://faucet-hq.github.io/faucet-stream/cookbook/serve.html#rbac--audit-log);
   an approvals policy: [Approvals](https://faucet-hq.github.io/faucet-stream/cookbook/approvals.html).
2. A persistent run history (`--history` with a database URL) so runs, audit
   and templates survive a restart; confirm the build accepts it (Step 2).
3. `--cluster` on a shared Postgres history when more than one replica runs.
   Every replica needs the same env, secrets and default config.
4. Probes: liveness `/healthz`, readiness `/readyz`; a stop timeout above the
   server's shutdown grace.
5. TLS at the ingress; `/metrics` reachable only by Prometheus; egress
   restricted.
6. A stable `name:` in every submitted config (it is the metric label and the
   state-key prefix), and an idempotency key from orchestrators.
7. Shared defaults for every submitted run (state, execution) go in a
   `--default-config` file; see `cli/examples/serve_minimal.yaml`.

```bash
faucet serve --listen 0.0.0.0:8080 --auth-config /etc/faucet-auth/auth.yaml --history "$FAUCET_HISTORY_URL" --cluster --log-format json
```

Event triggers: [Event-driven triggers](https://faucet-hq.github.io/faucet-stream/cookbook/triggers.html),
the [triggers reference](https://faucet-hq.github.io/faucet-stream/reference/triggers.html)
(`faucet schema triggers` prints the file schema) and
`cli/examples/triggers/triggers.yaml`. Treat trigger bodies and headers as
untrusted input.

## Kubernetes and Helm

- Shapes per runtime (CronJob, Job, Deployment) and their must-haves:
  [Kubernetes](https://faucet-hq.github.io/faucet-stream/operations/deploying.html#kubernetes).
- The Helm chart is
  [`deploy/helm/faucet-stream`](https://github.com/faucet-hq/faucet-stream/tree/main/deploy/helm/faucet-stream)
  in the repository at the tag matching your version; read its `README.md`
  and `values.yaml` (or `helm show values` on the published chart) rather
  than guessing value names.
- Declare the connectors your configs use in the chart values so a wrong image
  fails at start, and pin the image tag explicitly.
- The chart's Job and CronJob storage does not persist: use redis or postgres
  state with them.

```bash
helm template faucet ./deploy/helm/faucet-stream -f my-values.yaml
```

Render the chart and read the manifests before installing.

## Observability

- Prometheus endpoint, structured logs, OTLP export and log shipping:
  [Observability](https://faucet-hq.github.io/faucet-stream/operations/observability.html).
  The config keys come from `faucet schema config`.
- Grafana dashboards and Prometheus alert rules ship in the repository under
  `observability/` ([Dashboards & alerts](https://faucet-hq.github.io/faucet-stream/cookbook/dashboards.html)).
  Load those instead of writing alerts by hand.
- Alert at least on: failed runs, scheduler heartbeat age and consecutive
  failures, bookmark staleness or source lag, rows going to the DLQ, and a
  degraded serve history.

## Production-readiness checklist

- [ ] The deployed binary or image is the pinned version and has every
      connector and feature the configs use (Step 2)
- [ ] `faucet validate --no-secrets --no-env-file` passes in CI for every config
- [ ] `faucet doctor` passes at deploy time
- [ ] Durable state backend shared by every process; never `memory`
- [ ] Only one run of a given row at a time, enforced by the scheduler too
- [ ] Prometheus endpoints bound to loopback or reachable only by Prometheus;
      serve `/v1` behind auth and TLS
- [ ] Alerts on run failure, staleness or lag, and DLQ growth
- [ ] State exported and migration checked before every upgrade
- [ ] `--no-auth` never exposed; tokens generated and stored as secrets
- [ ] Secrets only through `${env:…}` / `${file:…}` or a secrets manager;
      `--no-env-file` set
- [ ] JSON logs at `info` or quieter; never debug with resolved secrets
- [ ] The [hardening list](https://faucet-hq.github.io/faucet-stream/operations/deploying.html#hardening) reviewed

```bash
faucet doctor /etc/faucet/orders.yaml
faucet state export /etc/faucet/orders.yaml -o orders-state-backup.json
faucet migrate --state /etc/faucet/orders.yaml --check
```

# Chaos testing `faucet serve`

The [reliability suites](./reliability-testing.md) prove the engine's ordering
guarantees in-process. This page covers the layer above: a real deployment of
`faucet serve` from the Helm chart, put through the failures a production
cluster sees: pods and nodes dying, the history database going away,
upgrades, and load. `scripts/chaos/run.sh` runs every scenario on a throwaway
[kind](https://kind.sigs.k8s.io/) cluster and writes a pass/fail report.

## Running it

```bash
scripts/chaos/run.sh                                   # every scenario, ~30 minutes
scripts/chaos/run.sh --only kill-owner,history-outage  # a subset
scripts/chaos/run.sh --image ghcr.io/faucet-hq/faucet-stream:<version>-core
```

It needs Docker, `kind`, `kubectl`, `helm`, `jq` and `curl`, about 8 GB of free
disk and 6 GB of memory for the Docker VM. It writes its own kubeconfig, so
your current `kubectl` context is never changed, and deletes the cluster when
it finishes (`--keep-cluster` leaves it running for inspection). The exit
status is the number of failed scenarios.

| Option | Default | Meaning |
|---|---|---|
| `--image` | the chart's `appVersion` with `-core` | the image under test |
| `--chart` | `deploy/helm/faucet-stream` | a chart directory or an `oci://` reference |
| `--replicas` | `2` | clustered replicas |
| `--only` | every scenario | comma-separated scenario names |
| `--out` | `./chaos-report` | where `chaos-report.md`, `chaos-report.json` and one log per scenario go |
| `--keep-cluster` | off | leave the kind cluster running |

On an arm64 machine an image published for amd64 only (releases up to 1.14.0)
runs under the Docker VM's emulation: slower, but every timing the scenarios
assert is derived from the lease TTL, not wall-clock speed.

## The setup

A three-node cluster: Postgres on the control-plane node (so stopping a worker
never takes the databases down), and the faucet pods spread one per worker.
Every scenario's data lands in Postgres tables through an upsert sink, so row
counts prove whether a re-run duplicated anything. A "slow" pipeline reads 600
rows at 0.15 s each (about 90 s), long enough to fail something mid-run.

The clustered deployment is `--replicas` pods with `cluster.enabled`, Postgres
run history, token auth, a 30 s lease TTL and a trigger that fires every
minute. The single-pod deployment is one replica with SQLite history on a
volume and a one-shot Job.

## What each scenario proves

| Scenario | Failure | Passes when |
|---|---|---|
| `membership` | pods start together on a fresh history database | every pod reports `ready`, `history_ok`, the full member count, and none started degraded |
| `kill-owner` | the pod running a job is force-deleted | another pod reclaims the run (`attempt` ≥ 1) and completes it with exactly 600 rows |
| `cross-cancel` | a run is cancelled through a pod that does not own it | it ends `cancelled`, no rows arrive after that status, and `records_written` matches the table |
| `idempotency` | one idempotency key is sent to both pods | both get the same `run_id`; the key with a different body is `409` |
| `node-stop` | the node under the owning pod is stopped | the survivor completes the run with 600 rows, long before Kubernetes evicts the dead pod |
| `rolling-restart` | every pod is rolled mid-run | the run completes with 600 rows |
| `all-pods` | every pod is killed with runs queued | the interrupted run and every queued run complete |
| `history-outage` | the history database refuses connections | every pod's `/readyz` turns `history_ok: false` within the lease window, the owner cancels its own run before its lease expires (no concurrent execution), and after recovery the run completes with 600 rows |
| `burst` | 40 runs submitted at once across pods | 40 are accepted and each completes exactly once |
| `poison` | the pod running the same job is killed again and again | the run ends `failed` as a poison run instead of looping |
| `schedule` | everything above, while a trigger fires every minute | each minute produced one run; a tick missed during the outage fired once as catch-up |
| `single-kill` | the only pod dies mid-run | the run ends `failed` with the expired-lease reason |
| `single-upgrade` | the single pod with a volume is restarted | the Deployment uses `Recreate`, and two servers never run at once on the SQLite file |
| `job-upgrade` | an upgrade changes the one-shot Job | `helm upgrade` replaces the Job instead of failing on its immutable template |

## In CI

The [`Chaos (faucet serve)`](https://github.com/faucet-hq/faucet-stream/actions/workflows/chaos.yml)
workflow builds a Postgres-only image from the commit, runs every scenario
nightly, and can be started by hand with a scenario subset. The report is in
the run summary and the `chaos-report` artifact. It is not a required check:
a run takes about 30 minutes.

## Adding a scenario

A scenario is a `s_<name>` shell function in `scripts/chaos/run.sh` that
returns non-zero on failure and prints its evidence as its last line; register
it with `run_scenario <name> s_<name>` in the clustered or single-pod block,
and add a row to the table above. Assert outcomes a user would notice (row
counts, run status, `/readyz`), and derive waits from `LEASE_TTL` rather than
fixed sleeps.

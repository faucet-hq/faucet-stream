# Troubleshooting with `faucet doctor`

`faucet doctor` answers "why won't my pipeline run?" *before* you run it. It
probes every connector in a config — auth, network, permissions, reachability —
and prints a green/red checklist, exiting non-zero if anything fails. It is
**non-mutating**: no data is written, no rows inserted, no objects uploaded.

```bash
faucet doctor pipeline.yaml
```

```text
✓ Config parses and interpolates                                 8 ms
✓ Matrix expands to 2 invocations                    0 skipped (children)

▸ Invocation default::us-east  (source=postgres, sink=bigquery)
  ✓ source [postgres] read                                      42 ms
  ✓ sink   [bigquery] auth                                     280 ms
  ✓ state  [redis] sentinel                                     14 ms

▸ Invocation default::eu-west  (source=postgres, sink=bigquery)
  ✓ source [postgres] read                                      39 ms
  ✗ sink   [bigquery] auth (dataset eu_west not found)         410 ms
        hint: check bigquery credentials and that the dataset exists

Summary: 5 passed, 1 failed, 0 skipped       total elapsed 0.5s
```

The **exit code is the number of failed probes** (clamped to 255), so `doctor`
drops straight into a CI gate or a deploy script:

```bash
faucet doctor pipeline.yaml || { echo "preflight failed"; exit 1; }
```

## What gets probed

| Role | Probe |
|------|-------|
| **Source** (most) | Pulls a *single page* via the real read path (DNS + TLS + auth + first request) and stops — never the full dataset. |
| `webhook` source | The configured port is bindable. |
| `websocket` source | TCP connect to the host (no WebSocket handshake). |
| `postgres-cdc` source | The replication slot is reachable (missing slot → `skip`, since `run` can create it). |
| `kafka` source / sink | A cluster `metadata` request (validates brokers + auth without consuming/producing). |
| SQL sinks (`postgres`/`mysql`/`sqlite`) | `SELECT 1` on the pool. |
| `s3` / `gcs` sinks | Bucket head / metadata list. |
| `bigquery` / `snowflake` sinks | Token mint + a read-only metadata call / `SELECT 1`. |
| `redis` / `mongodb` / `elasticsearch` / `http` sinks | `PING` / `ping` / cluster health / a `HEAD` request. |
| File sinks (`jsonl`/`csv`/`parquet`/`stdout`) | Target directory is writable (`stdout` always passes). |
| State stores (`redis`/`postgres`/`file`/`memory`) | A sentinel `put`/`get`/`delete` that leaves no residue. |
| SLA (`sla:` block) | Read-only staleness / volume-baseline probes against the persisted run history — see [SLA monitoring](sla.md). |
| Sources with a head (CDC, streams) | A `lag` probe measured from the stored bookmark; fails past a `max_lag_*` threshold — see [Source lag](sla.md#source-lag). |

## Reading the result

- **`✓ pass`** — the probe succeeded.
- **`✗ fail`** — unreachable / unauthenticated / misconfigured. The parenthesized
  reason and the `hint:` line tell you what to fix.
- **`• skip`** — not applicable: an optional target is absent (e.g. a CDC slot not
  yet created), a connector ships no probe, or an object-store path can't be
  cheaply checked.

## Flags

| Flag | Purpose |
|------|---------|
| `--timeout-secs <N>` | Per-probe timeout in seconds (default 10). Lower it to fail fast against dead hosts. |
| `--json` | Emit a `{ config, invocations, summary }` JSON document for tooling. |
| `--env-file <path>` / `--no-env-file` | Same `.env` handling as `run`. |
| `--offline` | Run only the static config lints — no connectors built, no network, no credentials, secret-manager references left unresolved. Exits non-zero only on lint errors (an `auth: { ref }` naming no provider); warnings (an unused `auth:` provider or `vars:` entry, a no-op `batch_size`) are printed but do not fail it. |
| `--profile <name>` | Probe the config with a `profiles:` overlay applied, as `run --profile` would. |
| `--param NAME=VALUE` / `--param-env NAME[=VALUE]` | Bind run parameters, as `run` does. |

The `--json` shape:

```json
{
  "config": "pipeline.yaml",
  "invocations": [
    {
      "id": "default::eu-west",
      "probes": [
        { "role": "source", "connector": "postgres", "name": "read", "status": "pass", "elapsed_ms": 39 },
        { "role": "sink", "connector": "bigquery", "name": "auth", "status": "fail",
          "reason": "dataset eu_west not found", "elapsed_ms": 410,
          "hint": "check bigquery credentials and that the dataset exists" }
      ]
    }
  ],
  "summary": { "passed": 5, "failed": 1, "skipped": 0, "elapsed_ms": 500 }
}
```

## Limitations

- **Child invocations** in a parent/child matrix are listed but not probed: their
  configs depend on parent records that only exist at run time (same limitation
  as `faucet preview`).
- `doctor` needs real credentials — it resolves secrets like `run` does. Use
  `faucet validate --no-secrets` for an offline grammar-only check.
- Probe `reason`/`hint` text is scrubbed for resolved secrets, but don't run with
  `FAUCET_LOG=debug` against a config holding live secrets (third-party connector
  logging is outside faucet's redaction boundary).

## See also

- [Troubleshooting & FAQ](../operations/troubleshooting.md) — common runtime
  errors and their fixes (this page covers the `faucet doctor` tool itself).
- [Observability](../operations/observability.md) — the metrics to watch when a
  run misbehaves.

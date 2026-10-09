# faucet-stream Helm chart

Deploy [faucet-stream](https://github.com/faucet-hq/faucet-stream) on Kubernetes:

- **`faucet serve`** — the long-running HTTP control plane (Deployment + Service + probes, optional Ingress / HPA / PDB / ServiceMonitor).
- **`faucet run`** — one-shot pipelines (Job) and scheduled pipelines (CronJob).

All three are toggleable; enable what you need.

## Install

The chart is published as an **OCI artifact** on GHCR, so no `helm repo add` is
needed — install straight from the registry:

```bash
# Latest published version (Helm resolves the highest SemVer tag):
helm install faucet oci://ghcr.io/faucet-hq/charts/faucet-stream

# …or pin a version (recommended for CI / production — reproducible):
helm install faucet oci://ghcr.io/faucet-hq/charts/faucet-stream --version 1.8.1
```

Preview the manifests without a cluster, or list the available versions:

```bash
helm template faucet oci://ghcr.io/faucet-hq/charts/faucet-stream --version 1.8.1
helm show chart oci://ghcr.io/faucet-hq/charts/faucet-stream          # metadata for the latest
```

Point the chart at your image and layer in config as usual (see
[Values reference](#values-reference)):

```bash
helm install faucet oci://ghcr.io/faucet-hq/charts/faucet-stream \
  -n faucet --create-namespace \
  --set image.repository=ghcr.io/you/faucet-stream \
  --set image.tag=full \
  -f my-values.yaml
```

To install from a local checkout instead of the registry (e.g. while editing the
chart):

```bash
helm install faucet ./deploy/helm/faucet-stream \
  --set image.repository=ghcr.io/you/faucet-stream \
  --set image.tag=full
```

See [Uninstall](#uninstall) to remove a release.

---

## ⚠️ Connectors are compile-time — read this first

A faucet **source/sink is a Rust feature compiled into the image.** A running
container cannot gain a connector it wasn't built with, and **no `values.yaml`
setting can add one** — Helm deploys an already-built image, it doesn't compile.

This chart handles that with **declare-and-verify**:

```yaml
connectors:
  sources: [rest, postgres, s3]
  sinks:   [bigquery, jsonl, stdout]
  verify:
    enabled: true      # initContainer runs `faucet schema source|sink <name>`
```

When `verify.enabled`, an initContainer checks every declared connector against
the image and **refuses to start the pod** if one is missing — turning a silent
runtime *"unknown connector type"* into a clear boot-time failure that names the
image and the missing feature.

To actually *change* which connectors exist, build a different image. The
Dockerfile takes name-based build args:

```bash
# Lean, named "analytics" profile — only these connectors (skips DuckDB/Kafka natives)
scripts/build-image.sh -t ghcr.io/you/faucet-stream:analytics \
  -s rest,postgres,s3 -k bigquery,snowflake,jsonl

# Complete image — every connector and every feature (the CLI's `full`)
scripts/build-image.sh -t ghcr.io/you/faucet-stream:full

# …or use the published `:full`, which also ships the Oracle Instant Client
# the Oracle connectors load.
```

The recommended workflow is **B: named per-profile images** — publish a few
tagged images (`:core`, `:analytics`, `:cdc`, …) from CI, then point
`image.tag` at the one your deployment needs and let `connectors:` enforce it.
See `.github/workflows/docker-images.yml` for the matrix.

---

## Deployment shapes

### 1. Control plane (`serve`)

Long-running HTTP API. Submit pipelines with `POST /v1/runs`; probe with
`/healthz`, `/readyz`, `/metrics`.

```yaml
serve:
  enabled: true
  replicaCount: 2
  auth:
    mode: token           # token | none | rbac
  cluster:
    enabled: true
  history:
    backend: postgres     # memory | sqlite | postgres
    existingSecret: faucet-history   # key FAUCET_SERVE_HISTORY = postgres://…
```

- **Auth**: `token` (bearer; the chart mints a stable random token into a Secret,
  or use `auth.token` / `auth.existingSecret`), `none` (`--no-auth`, never expose
  externally), or `rbac` (inline `auth.rbacConfig` principals → mounted file).
- **History**: `memory` (ephemeral), `sqlite` (needs `persistence.enabled` for
  durability), or `postgres` (required for `cluster.enabled` multi-instance
  failover). The URL reaches the pod as the `FAUCET_SERVE_HISTORY` env var,
  never on the command line: a postgres `url` is stored in a chart-managed
  Secret, or point `history.existingSecret` / `history.existingSecretKey` at
  your own Secret so the password never sits in Helm values.
- **Replicas**: `replicaCount > 1` or `autoscaling.enabled` is refused at
  render time unless `history.backend: postgres` and `cluster.enabled: true` —
  with per-pod history each replica has its own runs and idempotency keys, so
  a `GET`/cancel lands on the wrong pod and a retried keyed submission can run
  twice.
- **Reusable pipeline definition**: set `pipelineConfig` and it's passed as the
  serve `--default-config` — a workspace default merged under every submitted
  run, so clients only POST overrides. (faucet has no HTTP "register template"
  endpoint; this is the closest equivalent.)

### 2. One-shot pipeline (`job`)

```yaml
job:
  enabled: true
pipelineConfig:
  create: true
  content: |
    version: 1
    name: nightly-load
    pipeline:
      source: { type: postgres, config: { ... } }
      sink:   { type: bigquery, config: { ... } }
```

### 3. Scheduled pipeline (`cronjob`)

```yaml
cronjob:
  enabled: true
  schedule: "0 * * * *"
  timeZone: Etc/UTC
pipelineConfig:
  create: true
  content: |
    version: 1
    # ...
```

`job`/`cronjob` require a `pipelineConfig` (inline `content` or
`existingConfigMap`); the chart fails the render otherwise.

---

## Credentials

Connector secrets (DB passwords, cloud keys) are passed as env, never baked into
the image or config:

```yaml
# Chart-managed Secret (referenced automatically in every pod's envFrom):
secret:
  create: true
  data:
    PGPASSWORD: "s3cr3t"

# …or reference your own:
envFrom:
  - secretRef:
      name: my-existing-credentials
```

Reference them in the pipeline config with faucet's `${env:VAR}` interpolation.

---

## Security defaults

Pods run **non-root (uid 65532), read-only root filesystem, all capabilities
dropped, seccomp RuntimeDefault**. A writable `emptyDir` (or PVC when
`serve.persistence.enabled`) is mounted at `/var/lib/faucet`, plus `/tmp`.
Override via `podSecurityContext` / `securityContext`.

---

## Observability

- `/metrics` (Prometheus, unauthenticated) is always served. Enable scraping
  with `serviceMonitor.enabled=true` (Prometheus Operator) or annotate the
  Service yourself.
- `/healthz` (liveness) and `/readyz` (readiness) back the probes.
- **Log shipping (#806):** `otel.logs.enabled=true` + `otel.logs.endpoint`
  ships every run's log lines over OTLP. For `serve` the chart renders the
  `--otel-config` file and the buffer bounds / link template as env; the
  run-history backend is the buffer, so pair it with `serve.history.backend:
  sqlite` + `serve.persistence.enabled` (or postgres) to survive restarts.
  `job` / `cronjob` pods get a spool volume at `otel.logs.spoolDir`
  (`otel.logs.buffer.persistence.enabled` for a PVC); their pipeline config
  must set `observability.otel.export: [logs]` and
  `observability.logs.spool_dir` to that path. Collector recipes:
  [`deploy/otel/`](../../otel/).

---

## Values reference

The chart's `appVersion` is the faucet-cli version of the release it ships
with (the release PR updates it and CI fails when it drifts), so leaving
`image.tag` empty runs that release's image.

See [`values.yaml`](./values.yaml) — every key is commented. Common ones:

| Key | Default | Purpose |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/faucet-hq/faucet-stream` / appVersion | image to run |
| `connectors.sources` / `.sinks` | `[]` | declared connectors (verified at boot) |
| `connectors.verify.enabled` | `true` | fail pod start if a declared connector is absent |
| `serve.enabled` | `true` | deploy the control plane |
| `serve.auth.mode` | `token` | `token` \| `none` \| `rbac` |
| `serve.history.backend` | `memory` | `memory` \| `sqlite` \| `postgres` |
| `serve.localOutputs.retentionDays` | `7` | days before local sink output files (jsonl/csv/parquet) are reclaimed; `0` disables the sweep |
| `serve.localOutputs.inFlightGraceSeconds` | `60` | never delete an output touched within this window (guards against unlinking a file a run is writing); `0` disables |
| `serve.preview.enabled` | `false` | serve dataset previews of local sink outputs — the console reads a tracked jsonl/csv/parquet file's first rows back over HTTP. Off by default: it exposes file *contents* to anyone with the `LocalOutputRead` scope (viewer and up) |
| `serve.preview.rows` | `500` | rows a preview loads when the request omits `row_count_to_load` (soft cap); `0` = the whole dataset |
| `serve.preview.maxRows` | `5000` | ceiling on one preview's rows (hard cap); a larger request — including `row_count_to_load=all` — is clamped to it. `0` lifts the ceiling, letting one request read an entire output file (still bounded by a 64 MiB response budget and a 30s deadline) |
| `serve.autoscaling.enabled` | `false` | HPA on the Deployment |
| `serve.persistence.enabled` | `false` | PVC for sqlite history / bookmarks |
| `job.enabled` | `false` | one-shot `faucet run` |
| `cronjob.enabled` | `false` | scheduled `faucet run` |
| `cronjob.schedule` | `0 * * * *` | cron expression |
| `pipelineConfig.create` | `false` | render pipeline config into a ConfigMap |
| `serviceMonitor.enabled` | `false` | Prometheus Operator scrape |
| `otel.logs.enabled` | `false` | ship run logs over OTLP (needs an `otel` image) |
| `otel.logs.endpoint` / `.protocol` | `""` / `grpc` | the OTLP collector |
| `otel.logs.retentionSeconds` / `.maxAgeSeconds` / `.maxBytes` | `86400` / `604800` / `1073741824` | buffer retention after delivery and bounds before it |
| `otel.logs.linkTemplate` | `""` | console **View logs** link (`{run_id}` …) |
| `otel.logs.buffer.persistence.enabled` | `false` | PVC for the job / cronjob spool (`emptyDir` otherwise) |
| `ingress.enabled` | `false` | expose serve via Ingress |

---

## Uninstall

```bash
helm uninstall faucet
```

The history PVC (`serve.persistence`) and a generated auth-token Secret carry
`helm.sh/resource-policy: keep` and survive uninstall — delete them manually if
you want a clean slate.

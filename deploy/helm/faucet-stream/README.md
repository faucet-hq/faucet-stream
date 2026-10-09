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
  run, so clients only POST overrides. For named, versioned pipelines use the
  template registry (`/v1/templates`, or `serve.templatesSync` below).

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

## Serve features

Every file- or flag-backed `faucet serve` feature has its own values block, so
none of them needs `serve.extraArgs`. Each needs an image built with that
feature; the default image (the chart's `appVersion` tag, the published full
image) has all of them.

### File-backed: triggers, template sync, policy, connect providers, OTLP

`serve.triggers`, `serve.templatesSync`, `serve.policy`,
`serve.connectProviders` and `serve.otel` share one shape. Give the file in
exactly one way:

| Key | Result |
|---|---|
| `content` | inline YAML (a string, or a mapping rendered as YAML) → a chart-managed ConfigMap; a change rolls the pods |
| `existingConfigMap` | your ConfigMap, which must hold `fileName` |
| `existingSecret` | your Secret, which must hold `fileName` (for a file with credentials in it) |

The file is mounted read-only at `/etc/faucet-<feature>/<fileName>` and the
flag points at it:

| Block | Flag | Mount | Default `fileName` |
|---|---|---|---|
| `serve.triggers` | `--triggers` | `/etc/faucet-triggers/` | `triggers.yaml` |
| `serve.templatesSync` | `--templates-sync` | `/etc/faucet-templates-sync/` | `sync.yaml` |
| `serve.policy` | `--policy` | `/etc/faucet-policy/` | `policy.yaml` |
| `serve.connectProviders` | `--connect-providers` | `/etc/faucet-connect-providers/` | `providers.yaml` |
| `serve.otel` | `--otel-config` | `/etc/faucet-serve-otel/` | `otel.yaml` |

```yaml
serve:
  triggers:
    enabled: true
    content:
      version: 1
      triggers:
        - name: sync-hook
          type: webhook
          template: { id: platform-nightly }
          methods: [POST]
  policy:
    enabled: true
    existingSecret: faucet-policy     # key: policy.yaml
```

Template-sync, connect-provider and OTLP files resolve `${env:VAR}` when the
server loads them, so keep tokens and client secrets in a Secret exposed through
`envFrom` (or `secret.data`) and write `token: "${env:GITHUB_TOKEN}"` in the
file. A trigger's relative `config:` path resolves against
`/etc/faucet-triggers/`; use an absolute path (the `pipelineConfig` mount), an
inline config, or a registered `template:`.

`serve.otel` replaces the server file `otel.logs` renders. With
`otel.logs.enabled`, list `logs` in its `export`; the chart refuses inline
content that would silently stop log shipping. The `otel.logs` buffer bounds
and link template still apply.

### Tenants

```yaml
serve:
  history: { backend: postgres, existingSecret: faucet-history }
  tenants:
    enabled: true
    vaultKey:
      existingSecret: faucet-vault        # kubectl create secret generic faucet-vault \
      existingSecretKey: FAUCET_VAULT_KEY #   --from-literal=FAUCET_VAULT_KEY="$(openssl rand -base64 48)"
    previousKeys:                         # after a rotation: keys to open older credentials
      - existingSecret: faucet-vault-2025
```

The vault key reaches the pod as `FAUCET_VAULT_KEY` from your Secret and never
sits in Helm values. Each previous key is read from its Secret into
`FAUCET_VAULT_PREVIOUS_KEY_<n>` and joined into `FAUCET_VAULT_PREVIOUS_KEYS`, so
it stays out of both the pod spec and the process arguments. The render
fails when tenants are enabled without a vault key or with `memory` history
(tenants and their sealed connections live in the run history).
`serve.connectProviders` requires `serve.tenants`.

### Approvals and MCP

```yaml
serve:
  approvals:
    require: [run, template_launch]   # → --require-approval=run,template_launch
    expirySecs: 43200                 # → --approval-expiry-secs (default 86400)
  mcp:
    enabled: true                     # → --mcp (/mcp, same auth + RBAC as /v1)
    allowMutations: true              # → --mcp-allow-mutations
```

Who may approve is the `approvals:` block of the RBAC config
(`serve.auth.rbacConfig`). `template_register` / `template_launch` cannot be
combined with `serve.templatesSync`: the server refuses to start, so the chart
refuses to render.

### Extra volumes

`extraVolumes` / `extraVolumeMounts` (verbatim k8s specs) are added to the
serve, Job and CronJob pods: a CSI secrets-store volume, a CA bundle, a shared
volume an `extraInitContainers` fetcher fills.

### Ingress

- `GET /v1/runs/{id}/logs` is a Server-Sent Events stream and `/mcp` is
  long-lived. `ingress.streamingAnnotations` (merged under
  `ingress.annotations`, where a key you set wins) turns proxy buffering off
  and raises the read/send timeouts to an hour for ingress-nginx. Set the
  equivalent for another controller (on an AWS ALB, raise the idle timeout),
  or `streamingAnnotations: {}` to drop them.
- `GET /v1/connect/callback` is **public** by design: the OAuth provider
  redirects the user's browser there and the single-use `state` is the
  credential. It must be reachable from browsers at each provider's
  `redirect_base`.
- `POST|PUT /v1/triggers/{name}` (webhook triggers) is bearer-authenticated
  like every `/v1` route, so the calling system sends a token.

### Render-time checks

The render fails on: a feature block enabled with zero or several file sources;
`tenants` without a vault key or with `memory` history; `connectProviders`
without `tenants`; an unknown approval kind, a zero expiry, or template
approvals with template sync; `mcp.allowMutations` without `mcp.enabled`;
`serve.otel` content that drops `logs` while `otel.logs` is on. Template sync
or triggers with `memory` history render, with a warning in the install notes.

---

## Deploy everything

[`examples/everything.yaml`](./examples/everything.yaml) turns every serve
feature on: RBAC with approval rules, Postgres history + cluster mode with two
replicas, triggers, template sync, a policy, tenants with hosted OAuth connect,
OTLP traces/metrics/logs, approvals, MCP, extra volumes, Ingress with TLS and a
ServiceMonitor. Its header lists the Secrets to create first.

```bash
helm install faucet ./deploy/helm/faucet-stream -f deploy/helm/faucet-stream/examples/everything.yaml
```

Chart tests: `deploy/helm/test-chart.sh` (Helm 4) renders every
`ci/*-values.yaml` and checks the `# expect:` lines in it, checks that every
`ci/fail/*-values.yaml` is refused with its `# expect-error:` message, and
renders the examples. CI runs it on every chart change.

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
| `serve.triggers` / `.templatesSync` / `.policy` / `.connectProviders` / `.otel` | disabled | file-backed serve features: `enabled` + one of `content` / `existingConfigMap` / `existingSecret`, and `fileName` (see [Serve features](#serve-features)) |
| `serve.tenants.enabled` | `false` | tenants; needs `vaultKey.existingSecret` (+ `existingSecretKey`, default `FAUCET_VAULT_KEY`) and persistent history |
| `serve.tenants.previousKeys` | `[]` | `{existingSecret, existingSecretKey}` entries → `FAUCET_VAULT_PREVIOUS_KEYS` |
| `serve.approvals.require` | `[]` | change kinds needing approval: `run`, `template_register`, `template_launch` |
| `serve.approvals.expirySecs` | `null` | `--approval-expiry-secs` (server default 86400) |
| `serve.mcp.enabled` / `.allowMutations` | `false` / `false` | `/mcp` endpoint / its mutating tools |
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
| `ingress.streamingAnnotations` | ingress-nginx buffering off + 3600s timeouts | merged under `ingress.annotations` for the SSE log stream and `/mcp` |
| `extraVolumes` / `extraVolumeMounts` | `[]` | added to the serve, Job and CronJob pods |

---

## Uninstall

```bash
helm uninstall faucet
```

The history PVC (`serve.persistence`) and a generated auth-token Secret carry
`helm.sh/resource-policy: keep` and survive uninstall — delete them manually if
you want a clean slate.

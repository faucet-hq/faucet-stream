# Deploying faucet

faucet runs pipelines **to completion** — it's not a long-running daemon. That
makes deployment simple: schedule the binary, point it at a config, and let it
exit. Durable state (bookmarks) lets the next run pick up where the last left off.

## Patterns

### Cron / scheduled jobs

The most common deployment. Run on an interval; incremental replication + a
durable state store mean each run only fetches what's new.

```bash
# crontab: every 15 minutes
*/15 * * * * faucet run /etc/faucet/events.yaml >> /var/log/faucet.log 2>&1
```

### Containers

Build a slim image with only the connectors you need, and supply config via a
mounted file or entirely from the environment:

```dockerfile
FROM rust:slim AS build
RUN cargo install faucet-cli --no-default-features \
    --features "source-rest,sink-bigquery,state-postgres,observability"

FROM debian:stable-slim
COPY --from=build /usr/local/cargo/bin/faucet /usr/local/bin/faucet
ENTRYPOINT ["faucet", "run"]
```

With `faucet run --from-env` you can drive the whole pipeline from `FAUCET_*`
environment variables — no config file in the image. See the
[CLI reference](../reference/cli.md#environment-only-mode).

### Kubernetes CronJob

Wrap the container above in a `CronJob`. Use the `postgres` or `redis` state
backend so bookmarks survive pod restarts, and scrape the metrics endpoint (see
[Observability](./observability.md)).

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
real values through your platform's secret mechanism (Kubernetes secrets, Docker
secrets, a mounted `.env`, etc.).

## Exit codes & retries

`faucet run` exits non-zero when a pipeline fails (subject to the
`execution.on_error` policy and any DLQ). Let your scheduler's retry/alert
mechanism react to a non-zero exit; because bookmarks only advance after the sink
confirms, a retried run resumes safely.

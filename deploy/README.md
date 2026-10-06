# Deploying faucet-stream

Container image + Kubernetes/Helm assets for running faucet-stream anywhere.

| Path | What it is |
|---|---|
| [`../Dockerfile`](../Dockerfile) | Multi-stage image build with **name-based connector selection** (build args). |
| [`../scripts/build-image.sh`](../scripts/build-image.sh) | Helper to build lean or full images by connector name. |
| [`helm/faucet-stream/`](./helm/faucet-stream/) | Helm chart — `serve` Deployment and/or `run` Job/CronJob. |
| [`../.github/workflows/docker-images.yml`](../.github/workflows/docker-images.yml) | CI matrix that publishes named per-profile images to GHCR. |

## The one thing to understand

Connectors are **compile-time Rust features**. The image you build fixes which
sources/sinks exist; nothing at deploy time can add one. So you choose
connectors when you **build the image**, and the Helm chart **declares +
verifies** that choice at deploy time (an initContainer fails startup if the
running image is missing a connector you declared).

## Quick start (Docker)

```bash
# Complete image — all connectors + the serve control plane
docker build -t faucet:full .
# The image's default command is `serve`, which refuses to start without auth.
docker run --rm -p 8080:8080 -e FAUCET_SERVE_AUTH_TOKEN="$(openssl rand -hex 32)" faucet:full
curl -s localhost:8080/healthz && echo OK

# Run a pipeline one-shot
docker run --rm -v "$PWD":/w -w /w faucet:full run pipeline.yaml
```

`serve --no-auth` turns authentication off: anyone who can reach the port can
submit and run pipelines with the container's credentials and network access.
Use it only for a local demo bound to loopback
(`docker run -p 127.0.0.1:8080:8080 faucet:full serve --no-auth`), never on a
published port.

### Lean, named images (recommended for k8s — "profile B")

```bash
scripts/build-image.sh -t ghcr.io/you/faucet:analytics \
  -s rest,postgres,s3 -k bigquery,snowflake,jsonl
docker run --rm ghcr.io/you/faucet:analytics list   # confirm what's compiled in
```

## Quick start (Kubernetes / Helm)

```bash
helm install faucet ./deploy/helm/faucet-stream \
  --set image.repository=ghcr.io/you/faucet \
  --set image.tag=analytics \
  --set 'connectors.sources={rest,postgres,s3}' \
  --set 'connectors.sinks={bigquery,snowflake,jsonl}'
```

See the [chart README](./helm/faucet-stream/README.md) for serve/job/cronjob
config, auth modes, history backends, credentials, and the full values
reference.

## Image size / build-time note

The image is **multi-stage**: a `builder` stage compiles the binary and a
separate `runtime` stage (debian-slim, ~74 MB base) ships *only* the stripped
`faucet` binary — the Rust toolchain, source, and `target/` never reach the
final image. The binary is `strip`-ped in the builder, which removes 50–150 MB
of debug symbols from a full build.

`docker build .` with no `SOURCES`/`SINKS` still builds **every** connector,
including bundled DuckDB (C++) and librdkafka — a large, slow build (~20–40 min).
Naming a subset with `SOURCES`/`SINKS` skips those native deps and yields a much
smaller, faster image. Prefer named profiles for production.

For an even smaller runtime you can swap debian-slim for a distroless base — but
distroless has **no shell**, which disables the Helm `connectors.verify`
initContainer (it runs `/bin/sh`). If you go distroless, set
`connectors.verify.enabled=false`. Ask if you want a `runtime-distroless` build
target wired up.

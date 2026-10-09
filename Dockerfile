# syntax=docker/dockerfile:1.7
#
# faucet-stream container image.
#
# Builds the `faucet` CLI/control-plane binary and ships it on a slim Debian
# runtime as a non-root user. Connectors are Rust *compile-time* features, so
# what a running image can do is fixed at build time — pick it here with the
# name-based build args below (mirrored by the Helm chart's `connectors:` block).
#
# Quick starts
# ------------
#   # Complete image — the CLI's `full` feature: every connector and every
#   # feature (serve, tenants, templates + sync, triggers, SQL/WASM transforms,
#   # OTLP, secret managers, …), plus the Oracle Instant Client the Oracle
#   # connectors load at run time.
#   # Heavy: bundled DuckDB (C++), wasmtime, librdkafka, Delta (~30-60 min).
#   docker build -t faucet:full .
#
#   # Lean image — only the connectors you name (skips DuckDB/Kafka/… natives).
#   docker build \
#     --build-arg SOURCES="rest,postgres,s3" \
#     --build-arg SINKS="bigquery,jsonl,stdout" \
#     -t faucet:rest-pg-s3 .
#
#   # Escape hatch — pass a raw cargo feature list verbatim.
#   docker build --build-arg FEATURES="observability,serve,source-rest,sink-file" -t faucet:min .
#
# Feature selection precedence (first match wins):
#   1. FEATURES set            -> used verbatim (with --no-default-features).
#   2. SOURCES/SINKS both empty -> DEFAULT_FEATURES (`full`: every feature).
#   3. otherwise               -> EXTRAS + source-<each SOURCES> + sink-<each SINKS>.
#
# ORACLE_CLIENT=auto (default) installs Oracle Instant Client exactly when the
# binary has an Oracle connector compiled in; `true` / `false` force it.

ARG RUST_VERSION=1.96.0
ARG DEBIAN_RELEASE=bookworm
ARG ORACLE_CLIENT=auto

########################  builder  ########################
FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder

# --- feature selection knobs (see header) ---
# Comma-separated *short* connector names, e.g. SOURCES="rest,postgres,s3".
ARG SOURCES=""
ARG SINKS=""
# Non-connector features always compiled in for a selective (SOURCES/SINKS) build.
# Deliberately excludes the `source`/`sink`/`default` aggregates so a lean build
# stays lean. `state` = all state backends (memory+file are free; redis/postgres
# link a driver — drop it from EXTRAS if you don't need them).
#
# `triggers` here is the framework + the webhook trigger type only. The
# `object_arrival` and `queue_depth` watchers need `triggers-object-store` /
# `triggers-redis` / `triggers-kafka`, which pull heavy (and for Kafka, native)
# dependency trees — add them explicitly when a lean image needs them. The
# complete image below includes all three.
ARG EXTRAS="observability,state,transforms,compression,quality,contract,masking,cli-progress,serve,serve-ui,schedule,catalog,serve-history-postgres,serve-history-sqlite,notify,lineage,templates,mcp,triggers"
# The complete set used when neither SOURCES nor SINKS is given: the CLI's
# `full` feature, which enables every feature in its manifest. Keep it exactly
# `full` — scripts/full-feature-coverage.py fails CI otherwise (#845).
ARG DEFAULT_FEATURES="full"
# Raw override — when set, wins over everything above.
ARG FEATURES=""

# Native build prerequisites: cmake + a C/C++ toolchain (bundled DuckDB, some
# -sys crates), OpenSSL/SASL/curl headers (librdkafka for the kafka feature).
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake build-essential pkg-config \
        libssl-dev libsasl2-dev libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

# Resolve the cargo feature list, then build. BuildKit cache mounts keep the
# cargo registry + target dir warm across builds; the finished binary is copied
# out of the (ephemeral) target cache so it survives into the runtime stage.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target,sharing=locked \
    set -eu; \
    if [ -n "${FEATURES}" ]; then \
        feats="${FEATURES}"; \
    elif [ -z "${SOURCES}" ] && [ -z "${SINKS}" ]; then \
        feats="${DEFAULT_FEATURES}"; \
    else \
        feats="${EXTRAS}"; \
        for s in $(echo "${SOURCES}" | tr ',' ' '); do [ -n "$s" ] && feats="${feats},source-${s}"; done; \
        for s in $(echo "${SINKS}"  | tr ',' ' '); do [ -n "$s" ] && feats="${feats},sink-${s}"; done; \
    fi; \
    echo "==> building faucet with features: ${feats}"; \
    cargo build --release --locked -p faucet-cli \
        --no-default-features --features "${feats}"; \
    cp /build/target/release/faucet /usr/local/bin/faucet; \
    # Strip debug symbols — on a full build (DuckDB + Kafka + every connector
    # statically linked) this removes 50-150MB from the shipped binary.
    strip --strip-all /usr/local/bin/faucet; \
    /usr/local/bin/faucet --version; \
    mkdir -p /usr/local/share/faucet; \
    if /usr/local/bin/faucet list | grep -qw oracle; then \
        touch /usr/local/share/faucet/has-oracle-connector; \
    fi

####################  oracle-client  ####################
# Oracle Instant Client (Basic Light), which the Oracle connectors load at run
# time through ODPI-C. Downloaded once per architecture, checksum-pinned, and
# pruned to the four libraries ODPI-C loads (~40 MiB compressed). Skipped
# only with ORACLE_CLIENT=false; under `auto` the runtime stage installs it when
# the binary has an Oracle connector. Instant Client is redistributable under
# the Oracle Free Use Terms and Conditions license.
FROM debian:${DEBIAN_RELEASE}-slim AS oracle-client
ARG TARGETARCH
ARG ORACLE_CLIENT
ARG ORACLE_CLIENT_VERSION=23.26.2.0.0
ARG ORACLE_CLIENT_DIR=2326200
ARG ORACLE_CLIENT_SHA256_AMD64=c5e97765e633ad02597b8274c35efe5c10bd249a3285e34ee58fa7b318243225
ARG ORACLE_CLIENT_SHA256_ARM64=1de02a6d7a56cbbd5f6a4f6ddfc5b66340dc0cbf42b1e2ea80975f35c4c2ccd0
RUN set -eu; \
    mkdir -p /opt/oracle/instantclient; \
    if [ "${ORACLE_CLIENT}" = "false" ]; then exit 0; fi; \
    case "${TARGETARCH}" in \
        amd64) arch=x64;   sum="${ORACLE_CLIENT_SHA256_AMD64}" ;; \
        arm64) arch=arm64; sum="${ORACLE_CLIENT_SHA256_ARM64}" ;; \
        *) echo "no Oracle Instant Client for ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl unzip; \
    curl -fsSL -o /tmp/ic.zip \
        "https://download.oracle.com/otn_software/linux/instantclient/${ORACLE_CLIENT_DIR}/instantclient-basiclite-linux.${arch}-${ORACLE_CLIENT_VERSION}.zip"; \
    echo "${sum}  /tmp/ic.zip" | sha256sum -c -; \
    unzip -q /tmp/ic.zip -d /tmp/ic; \
    for lib in libclntsh.so.23.1 libclntshcore.so.23.1 libnnz.so libociicus.so; do \
        cp -a "/tmp/ic/instantclient_"*"/${lib}" /opt/oracle/instantclient/; \
    done; \
    ln -s libclntsh.so.23.1 /opt/oracle/instantclient/libclntsh.so; \
    rm -rf /tmp/ic /tmp/ic.zip /var/lib/apt/lists/*

########################  runtime  ########################
FROM debian:${DEBIAN_RELEASE}-slim AS runtime

LABEL org.opencontainers.image.title="faucet-stream" \
      org.opencontainers.image.description="Config-driven data-movement platform (faucet CLI + serve control plane)" \
      org.opencontainers.image.source="https://github.com/faucet-hq/faucet-stream" \
      org.opencontainers.image.url="https://faucet-hq.github.io/faucet-stream/" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

# Runtime shared libs the connectors dlopen/link against (TLS, SASL for Kafka).
# librdkafka/DuckDB are statically linked by their -sys crates, so no extra pkg.
# The Oracle client (plus the libaio it needs) goes in when the binary has an
# Oracle connector (ORACLE_CLIENT=auto) or when ORACLE_CLIENT=true.
ARG ORACLE_CLIENT
RUN --mount=type=bind,from=oracle-client,source=/opt/oracle/instantclient,target=/mnt/instantclient \
    --mount=type=bind,from=builder,source=/usr/local/share/faucet,target=/mnt/faucet-meta \
    set -eu; \
    pkgs="ca-certificates libssl3 libsasl2-2 zlib1g"; \
    oracle=false; \
    case "${ORACLE_CLIENT}" in \
        true) oracle=true ;; \
        false) ;; \
        auto|"") [ -f /mnt/faucet-meta/has-oracle-connector ] && oracle=true ;; \
        *) echo "ORACLE_CLIENT must be auto, true or false, got '${ORACLE_CLIENT}'" >&2; exit 1 ;; \
    esac; \
    if [ "${oracle}" = true ]; then pkgs="${pkgs} libaio1"; fi; \
    apt-get update && apt-get install -y --no-install-recommends ${pkgs} \
    && rm -rf /var/lib/apt/lists/*; \
    if [ "${oracle}" = true ]; then \
        mkdir -p /opt/oracle/instantclient; \
        cp -a /mnt/instantclient/. /opt/oracle/instantclient/; \
        echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf; \
        ldconfig; \
    fi; \
    groupadd --system --gid 65532 faucet \
    && useradd --system --uid 65532 --gid faucet \
        --home-dir /var/lib/faucet --create-home --shell /usr/sbin/nologin faucet

COPY --from=builder /usr/local/bin/faucet /usr/local/bin/faucet

USER 65532:65532
WORKDIR /var/lib/faucet

# Bind on all interfaces inside the container (k8s Service/probes reach it).
ENV FAUCET_SERVE_LISTEN=0.0.0.0:8080 \
    FAUCET_LOG=info
EXPOSE 8080

ENTRYPOINT ["faucet"]
# Default: the HTTP control plane, which refuses to start without auth — pass
#   -e FAUCET_SERVE_AUTH_TOKEN=... (or --auth-config / the role tokens).
# Override for one-shot runs, e.g.
#   docker run --rm -v $PWD:/w -w /w faucet:full run pipeline.yaml
CMD ["serve"]

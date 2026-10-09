#!/usr/bin/env bash
#
# build-image.sh — build a faucet-stream container image with a name-based
# connector/feature selection, mirroring the Dockerfile's build args and the
# Helm chart's `connectors:` block.
#
# Usage:
#   scripts/build-image.sh [options]
#
# Options (all optional):
#   -t, --tag <ref>        Image tag (default: faucet:local)
#   -s, --sources <list>   Comma-separated source short names (e.g. rest,postgres,s3)
#   -k, --sinks <list>     Comma-separated sink short names   (e.g. bigquery,jsonl)
#   -f, --features <list>  Raw cargo feature list (overrides -s/-k entirely)
#   -e, --extras <list>    Non-connector features for a selective build
#       --push             docker push after a successful build
#       --platform <p>     Buildx platform(s), e.g. linux/amd64,linux/arm64
#   -h, --help             Show this help
#
# Examples:
#   # Complete image (every connector and every feature, plus the Oracle
#   # Instant Client the Oracle connectors load):
#   scripts/build-image.sh -t ghcr.io/you/faucet:full
#
#   # Lean "analytics" profile:
#   scripts/build-image.sh -t ghcr.io/you/faucet:analytics \
#       -s rest,postgres,s3 -k bigquery,snowflake,jsonl
#
set -euo pipefail

TAG="faucet:local"
SOURCES=""
SINKS=""
FEATURES=""
EXTRAS=""
PUSH=0
PLATFORM=""

here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() { sed -n '2,28p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -t|--tag)      TAG="$2"; shift 2 ;;
    -s|--sources)  SOURCES="$2"; shift 2 ;;
    -k|--sinks)    SINKS="$2"; shift 2 ;;
    -f|--features) FEATURES="$2"; shift 2 ;;
    -e|--extras)   EXTRAS="$2"; shift 2 ;;
    --push)        PUSH=1; shift ;;
    --platform)    PLATFORM="$2"; shift 2 ;;
    -h|--help)     usage 0 ;;
    *) echo "unknown option: $1" >&2; usage 1 ;;
  esac
done

# A lean build that names no Oracle connector skips the client download.
oracle=auto
if [ -z "${FEATURES}" ] && { [ -n "${SOURCES}" ] || [ -n "${SINKS}" ]; }; then
  case ",${SOURCES},${SINKS}," in *oracle*) ;; *) oracle=false ;; esac
fi
args=(--build-arg "SOURCES=${SOURCES}" --build-arg "SINKS=${SINKS}" --build-arg "ORACLE_CLIENT=${oracle}")
[ -n "${FEATURES}" ] && args+=(--build-arg "FEATURES=${FEATURES}")
[ -n "${EXTRAS}" ]   && args+=(--build-arg "EXTRAS=${EXTRAS}")

echo "==> building ${TAG}"
echo "    sources=${SOURCES:-<all>} sinks=${SINKS:-<all>} features=${FEATURES:-<computed>}"

if [ -n "${PLATFORM}" ] || [ "${PUSH}" = "1" ]; then
  # buildx path (multi-arch and/or push)
  out="--load"
  [ "${PUSH}" = "1" ] && out="--push"
  [ -n "${PLATFORM}" ] && args+=(--platform "${PLATFORM}")
  docker buildx build "${args[@]}" ${out} -t "${TAG}" -f "${here}/Dockerfile" "${here}"
else
  DOCKER_BUILDKIT=1 docker build "${args[@]}" -t "${TAG}" -f "${here}/Dockerfile" "${here}"
fi

echo "==> done: ${TAG}"
echo "    inspect connectors with:  docker run --rm ${TAG} list"

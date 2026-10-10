#!/usr/bin/env bash
# Pull Docker Hub images through a registry mirror so the container-backed
# tests do not hit Docker Hub's anonymous pull rate limit on shared runners
# (`toomanyrequests`). An image the mirror lacks falls back to Docker Hub.
#   scripts/docker-hub-mirror.sh [mirror-url]
set -euo pipefail

mirror="${1:-${DOCKER_HUB_MIRROR:-https://mirror.gcr.io}}"
conf=/etc/docker/daemon.json
current='{}'
if [ -s "$conf" ]; then
  current="$(sudo cat "$conf")"
fi
updated="$(jq --arg m "$mirror" '."registry-mirrors" = ((."registry-mirrors" // []) + [$m] | unique)' <<<"$current")"
echo "$updated" | sudo tee "$conf" >/dev/null
sudo systemctl restart docker
for _ in $(seq 1 30); do
  if docker info >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
if ! docker info --format '{{json .RegistryConfig.Mirrors}}' | grep -qF "$mirror"; then
  echo "::error::Docker did not pick up the registry mirror $mirror"
  exit 1
fi
echo "Docker Hub pulls go through $mirror"

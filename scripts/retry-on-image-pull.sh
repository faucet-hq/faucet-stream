#!/usr/bin/env bash
# Run a test command; retry it ONLY when it failed because a container image
# could not be pulled (#789 SUPPLY-13). A test that fails for any other reason
# fails the job on its first run, so a flaky test cannot pass by being re-run.
#   scripts/retry-on-image-pull.sh cargo test --workspace --all-features
set -uo pipefail

PULL_FLAKE='bytes remaining on stream|toomanyrequests|pull access denied|failed to pull|error pulling image|PullImage|manifest unknown|TLS handshake timeout|Client\.Timeout exceeded while awaiting headers'
attempts="${RETRY_ATTEMPTS:-3}"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

for attempt in $(seq 1 "$attempts"); do
  echo "::group::$* (attempt $attempt)"
  "$@" 2>&1 | tee "$log"
  status=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ "$status" -eq 0 ]; then
    exit 0
  fi
  if ! grep -qE "$PULL_FLAKE" "$log"; then
    echo "::error::'$*' failed (not an image-pull failure, so it is not retried)"
    exit "$status"
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    echo "::warning::'$*' failed pulling a container image on attempt $attempt; retrying in 20s"
    sleep 20
  fi
done
echo "::error::'$*' failed after $attempts attempts (image pulls kept failing)"
exit 1

#!/usr/bin/env bash
# Run a test command; retry it ONLY when it failed because a container image
# could not be pulled (#789 SUPPLY-13): every failing test must have logged a
# pull error (scripts/pull_flake.py). A test that fails for any other reason
# fails the job on its first run, so a flaky test cannot pass by being re-run.
#   scripts/retry-on-image-pull.sh cargo test --workspace --all-features
# With RETRY_LOG_KEEP set, the last attempt's output is appended to that file
# (scripts/failing_tests.py summarises it).
set -uo pipefail

FLAKE_CHECK="$(dirname "$0")/pull_flake.py"
attempts="${RETRY_ATTEMPTS:-3}"
log="$(mktemp)"
trap 'rm -f "$log"' EXIT

for attempt in $(seq 1 "$attempts"); do
  echo "::group::$* (attempt $attempt)"
  "$@" 2>&1 | tee "$log"
  status=${PIPESTATUS[0]}
  echo "::endgroup::"
  if [ -n "${RETRY_LOG_KEEP:-}" ] && { [ "$status" -eq 0 ] || ! python3 "$FLAKE_CHECK" "$log" || [ "$attempt" -eq "$attempts" ]; }; then
    cat "$log" >> "$RETRY_LOG_KEEP"
  fi
  if [ "$status" -eq 0 ]; then
    exit 0
  fi
  if ! python3 "$FLAKE_CHECK" "$log"; then
    echo "::error::'$*' failed (not only an image-pull failure, so it is not retried)"
    exit "$status"
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    echo "::warning::'$*' failed pulling a container image on attempt $attempt; retrying in 20s"
    sleep 20
  fi
done
echo "::error::'$*' failed after $attempts attempts (image pulls kept failing)"
exit 1

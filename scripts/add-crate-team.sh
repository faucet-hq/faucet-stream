#!/usr/bin/env bash
# Add the owners team to each crate named on the command line, so a crate first
# published by one account is also owned by github:faucet-hq:owners.
#
# Usage: scripts/add-crate-team.sh <crate>...
# Env:   CARGO_REGISTRY_TOKEN (change-owners scope, owned by a team member)
#        TEAM (default github:faucet-hq:owners), UA (crates.io User-Agent)
set -uo pipefail

TEAM="${TEAM:-github:faucet-hq:owners}"
UA="${UA:-faucet-stream release (https://github.com/faucet-hq/faucet-stream)}"

if [ "$#" -eq 0 ]; then
  echo "No crates to add ${TEAM} to."
  exit 0
fi

failed=0
for crate in "$@"; do
  owners=$(curl -fsS -A "$UA" "https://crates.io/api/v1/crates/${crate}/owners" | jq -r '.users[].login') || {
    echo "::error::${crate}: could not read its crates.io owners"
    failed=$((failed + 1))
    continue
  }
  if grep -qxF "$TEAM" <<<"$owners"; then
    echo "${crate}: already owned by ${TEAM}"
  elif cargo owner --add "$TEAM" "$crate"; then
    echo "${crate}: added ${TEAM}"
  else
    echo "::error::${crate}: cargo owner --add ${TEAM} failed"
    failed=$((failed + 1))
  fi
  sleep 1
done
if [ "$failed" -gt 0 ]; then
  echo "::error::${failed} crate(s) still lack ${TEAM}. CARGO_REGISTRY_TOKEN needs the change-owners scope and must belong to a member of that team."
  exit 1
fi

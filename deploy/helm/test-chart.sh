#!/usr/bin/env bash
# Render the Helm chart against every values file under its ci/ directory and
# check the declared expectations (#846). Each file carries them as comments:
#
#   # expect: <text>            the rendered output (manifests + NOTES) contains <text>
#   # expect-not: <text>        it does not
#   # expect-count: <n> <text>  <text> appears on exactly <n> lines
#   # expect-error: <text>      (ci/fail/ only) rendering fails with <text>
#
# examples/*.yaml must render cleanly too. Usage: deploy/helm/test-chart.sh [chart-dir]
set -euo pipefail

chart="${1:-deploy/helm/faucet-stream}"
failures=0

case "$(helm version --short)" in
  v3.*) echo "needs Helm 4: Helm 3 cannot render NOTES (install --dry-run=client) without a cluster" >&2; exit 2 ;;
esac

render() {
  KUBECONFIG=/dev/null helm install t "$chart" --dry-run=client --namespace faucet "$@" 2>&1
}

directives() {
  sed -n "s/^# $2: //p" "$1"
}

fail() {
  echo "::error::$1"
  failures=$((failures + 1))
}

report() {
  if [ "$failures" -eq "$2" ]; then echo "ok   $1${3:-}"; else echo "FAIL $1"; fi
}

for values in "$chart"/ci/*-values.yaml; do
  name="${values#"$chart"/}"
  before=$failures
  if ! out=$(render -f "$values"); then
    fail "$name: render failed: $out"
    continue
  fi
  while IFS= read -r want; do
    [ -n "$want" ] || continue
    grep -qF -- "$want" <<<"$out" || fail "$name: missing: $want"
  done < <(directives "$values" expect)
  while IFS= read -r unwanted; do
    [ -n "$unwanted" ] || continue
    if grep -qF -- "$unwanted" <<<"$out"; then fail "$name: unexpected: $unwanted"; fi
  done < <(directives "$values" expect-not)
  while IFS= read -r spec; do
    [ -n "$spec" ] || continue
    want_count="${spec%% *}"
    text="${spec#* }"
    got=$(grep -cF -- "$text" <<<"$out" || true)
    [ "$got" = "$want_count" ] || fail "$name: expected $want_count lines with '$text', got $got"
  done < <(directives "$values" expect-count)
  report "$name" "$before"
done

for values in "$chart"/ci/fail/*-values.yaml; do
  name="${values#"$chart"/}"
  before=$failures
  want=$(directives "$values" expect-error)
  [ -n "$want" ] || { fail "$name: no '# expect-error:' line"; continue; }
  if out=$(render -f "$values"); then
    fail "$name: rendered, but it must be refused"
    continue
  fi
  grep -qF -- "$want" <<<"$out" || fail "$name: refused with the wrong error (want '$want'): $out"
  report "$name" "$before" " (refused)"
done

for values in "$chart"/examples/*.yaml; do
  name="${values#"$chart"/}"
  if out=$(render -f "$values"); then
    helm lint "$chart" -f "$values" >/dev/null || fail "$name: helm lint failed"
    echo "ok   $name"
  else
    fail "$name: render failed: $out"
  fi
done

if [ "$failures" -ne 0 ]; then
  echo "$failures chart expectation(s) failed" >&2
  exit 1
fi

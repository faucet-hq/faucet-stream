#!/usr/bin/env bash
# Chaos-test `faucet serve` on a throwaway kind cluster (#908) and write a
# pass/fail report. Clustered (N replicas, Postgres history) and single-pod
# (SQLite on a volume) deployments are installed from the Helm chart and put
# through pod kills, a node stop, a full outage, a history-database outage,
# rolling restarts, a burst of runs and a run that keeps losing its pod.
#
#   scripts/chaos/run.sh                       # every scenario
#   scripts/chaos/run.sh --only kill-owner,history-outage
#   scripts/chaos/run.sh --image ghcr.io/faucet-hq/faucet-stream:1.15.0-core
#
# Options:
#   --image REF        image to test (default: the chart's appVersion, `-core`)
#   --chart PATH|OCI   chart to install (default: deploy/helm/faucet-stream)
#   --replicas N       clustered replicas (default 2)
#   --only a,b         run only these scenarios (names in the report)
#   --out DIR          report directory (default ./chaos-report)
#   --keep-cluster     leave the kind cluster running afterwards
#
# Needs docker, kind, kubectl, helm, jq and curl. It uses its own kubeconfig, so
# your current kubectl context is never changed. Exit status: the number of
# failed scenarios.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="${ROOT}/deploy/helm/faucet-stream"
IMAGE=""
REPLICAS=2
ONLY=""
OUT="${PWD}/chaos-report"
KEEP=0
CLUSTER="faucet-chaos"
NS="faucet"
LEASE_TTL=30

while [ $# -gt 0 ]; do
  case "$1" in
    --image) IMAGE="$2"; shift 2 ;;
    --chart) CHART="$2"; shift 2 ;;
    --replicas) REPLICAS="$2"; shift 2 ;;
    --only) ONLY="$2"; shift 2 ;;
    --out) OUT="$2"; shift 2 ;;
    --keep-cluster) KEEP=1; shift ;;
    -h|--help) sed -n '2,24p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

for tool in docker kind kubectl helm jq curl; do
  command -v "$tool" >/dev/null || { echo "missing tool: $tool" >&2; exit 2; }
done

WORK="$(mktemp -d)"
export KUBECONFIG="${WORK}/kubeconfig"
mkdir -p "$OUT"
PF_PIDS=()
RESULTS=()
START_TS=$(date +%s)

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

cleanup() {
  for p in "${PF_PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  if [ "$KEEP" -eq 0 ]; then
    kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1
  else
    log "cluster kept: KUBECONFIG=${KUBECONFIG} kubectl -n ${NS} get pods"
  fi
}
trap cleanup EXIT

k() { kubectl -n "$NS" "$@"; }

# ---------------------------------------------------------------- results ---

wanted() { [ -z "$ONLY" ] || [[ ",${ONLY}," == *",$1,"* ]]; }

record() { # name PASS|FAIL secs evidence
  RESULTS+=("$1|$2|$3|$4")
  log "$2  $1  ($3s)  $4"
}

run_scenario() { # name function
  local name="$1" fn="$2"
  wanted "$name" || return 0
  log "── ${name}"
  local t0 rc evidence
  t0=$(date +%s)
  # Not a subshell: port-forwards a scenario opens must reach the cleanup trap.
  "$fn" > "${OUT}/${name}.log" 2>&1
  rc=$?
  evidence="$(tail -1 "${OUT}/${name}.log")"
  if [ "$rc" -eq 0 ]; then record "$name" PASS "$(( $(date +%s) - t0 ))" "$evidence"
  else record "$name" FAIL "$(( $(date +%s) - t0 ))" "$evidence"; fi
}

# ------------------------------------------------------------------ image ---

chart_app_version() {
  if [ -d "$CHART" ]; then sed -n 's/^appVersion: *"\{0,1\}\([^"]*\)"\{0,1\}/\1/p' "$CHART/Chart.yaml"
  else helm show chart "$CHART" | sed -n 's/^appVersion: *"\{0,1\}\([^"]*\)"\{0,1\}/\1/p'; fi
}

prepare_image() {
  [ -n "$IMAGE" ] || IMAGE="ghcr.io/faucet-hq/faucet-stream:$(chart_app_version)-core"
  log "image: ${IMAGE}"
  if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
    docker pull -q "$IMAGE" >/dev/null 2>&1 || docker pull -q --platform linux/amd64 "$IMAGE" >/dev/null \
      || { echo "cannot pull ${IMAGE}" >&2; exit 2; }
  fi
  local host_arch img_arch
  host_arch="$(docker version -f '{{.Server.Arch}}')"
  img_arch="$(docker image inspect -f '{{.Architecture}}' "$IMAGE")"
  if [ "$host_arch" != "$img_arch" ]; then
    # An image without this node's architecture (releases up to 1.14.0 were
    # amd64-only) runs under the VM's emulation once it is labelled for it.
    log "image is ${img_arch}, nodes are ${host_arch}: wrapping it for emulation"
    printf 'FROM --platform=linux/%s %s\n' "$img_arch" "$IMAGE" > "${WORK}/Dockerfile"
    docker build -q --platform "linux/${host_arch}" -t faucet-chaos:local "$WORK" >/dev/null
    IMAGE="faucet-chaos:local"
  fi
}

# ---------------------------------------------------------------- cluster ---

create_cluster() {
  kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1
  cat > "${WORK}/kind.yaml" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
EOF
  kind create cluster --name "$CLUSTER" --config "${WORK}/kind.yaml" --kubeconfig "$KUBECONFIG" --wait 180s >/dev/null
  kind load docker-image "$IMAGE" --name "$CLUSTER" >/dev/null
  kubectl create namespace "$NS" >/dev/null
}

# Postgres runs on the control-plane node, so stopping a worker never takes
# the databases down with it.
deploy_postgres() {
  cat > "${WORK}/pg.yaml" <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata: { name: pg }
spec:
  selector: { matchLabels: { app: pg } }
  template:
    metadata: { labels: { app: pg } }
    spec:
      nodeSelector: { node-role.kubernetes.io/control-plane: "" }
      tolerations: [{ key: node-role.kubernetes.io/control-plane, operator: Exists, effect: NoSchedule }]
      containers:
        - name: pg
          image: postgres:16
          env: [{ name: POSTGRES_PASSWORD, value: pw }]
          readinessProbe: { exec: { command: [pg_isready, -U, postgres] }, periodSeconds: 2 }
---
apiVersion: v1
kind: Service
metadata: { name: pg }
spec: { selector: { app: pg }, ports: [{ port: 5432 }] }
EOF
  k apply -f "${WORK}/pg.yaml" >/dev/null
  k rollout status deploy/pg --timeout=300s >/dev/null
  psql postgres "CREATE DATABASE data"
  psql data "CREATE TABLE slow_out(id int PRIMARY KEY, v text); CREATE TABLE fast_out(id int PRIMARY KEY, v text); CREATE TABLE tick_log(ts text, fired text)"
  # slow_out is an upsert target, so its row count cannot show a run executing
  # twice. Every write is also logged with the writing pod's address, and
  # single_writer checks no two pods wrote it at overlapping times.
  psql data "CREATE TABLE slow_log(id int, client inet, at timestamptz);
    CREATE FUNCTION log_slow() RETURNS trigger LANGUAGE plpgsql AS \$\$
      BEGIN INSERT INTO slow_log VALUES (NEW.id, inet_client_addr(), clock_timestamp()); RETURN NEW; END \$\$;
    CREATE TRIGGER slow_log AFTER INSERT OR UPDATE ON slow_out FOR EACH ROW EXECUTE FUNCTION log_slow()"
  k create secret generic faucet-creds --from-literal=PGPASSWORD=pw >/dev/null
}

psql() { k exec deploy/pg -- psql -U postgres -d "$1" -v ON_ERROR_STOP=1 -tAq -c "$2"; }
sql() { psql data "$1"; }

reset_slow() { sql "TRUNCATE slow_out; TRUNCATE slow_log"; }

# Fails when two pods wrote slow_out over overlapping intervals: a reclaimed
# run may rewrite rows the dead owner wrote, but only after it stopped.
single_writer() {
  local overlap
  overlap="$(sql "WITH w AS (SELECT client, min(at) AS first, max(at) AS last FROM slow_log GROUP BY client)
    SELECT coalesce(string_agg(a.client || ' [' || a.first || ', ' || a.last || '] / ' || b.client || ' [' || b.first || ', ' || b.last || ']', '; '), '')
    FROM w a JOIN w b ON a.client < b.client AND a.first <= b.last AND b.first <= a.last")"
  [ -z "$overlap" ] || fail "two pods wrote the run at the same time: ${overlap}" || return 1
  [ "$(sql 'SELECT count(DISTINCT id) FROM slow_log')" = 600 ] || fail "$(sql 'SELECT count(DISTINCT id) FROM slow_log') distinct ids written" || return 1
}

writers() { sql "SELECT count(DISTINCT client) FROM slow_log"; }

fresh_history_db() {
  psql postgres "DROP DATABASE IF EXISTS faucet_history WITH (FORCE)" >/dev/null
  psql postgres "CREATE DATABASE faucet_history" >/dev/null
  k delete secret faucet-history --ignore-not-found >/dev/null
  k create secret generic faucet-history \
    --from-literal=FAUCET_SERVE_HISTORY='postgres://postgres:pw@pg:5432/faucet_history' >/dev/null
}

PG='postgres://postgres:${env:PGPASSWORD}@pg:5432/data'

pipeline_yaml() { # slow|fast
  local rows=100 table=fast_out sleep=""
  if [ "$1" = slow ]; then rows=600; table=slow_out; sleep=", LATERAL (SELECT pg_sleep(0.15) WHERE g > 0) s"; fi
  cat <<EOF
version: 1
name: $1
pipeline:
  source:
    type: postgres
    config:
      connection_url: "${PG}"
      query: "SELECT g AS id, md5(g::text) AS v FROM generate_series(1,${rows}) g${sleep}"
      batch_size: 50
  sink:
    type: postgres
    config:
      connection_url: "${PG}"
      table_name: ${table}
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
EOF
}

values_cluster() {
  cat <<EOF
image: { repository: ${IMAGE%:*}, tag: "${IMAGE##*:}", pullPolicy: IfNotPresent }
connectors: { sources: [postgres], sinks: [postgres] }
envFrom: [{ secretRef: { name: faucet-creds } }]
serve:
  replicaCount: ${REPLICAS}
  cluster: { enabled: true }
  history: { backend: postgres, existingSecret: faucet-history }
  auth: { mode: token }
  extraArgs: [--allow-env=PGPASSWORD, "--lease-ttl-secs=${LEASE_TTL}"]
  podDisruptionBudget: { enabled: true }
  # Soft: a hard spread rule leaves a rolling update's surge pod with no
  # allowed node, and it stays Pending.
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: kubernetes.io/hostname
      whenUnsatisfiable: ScheduleAnyway
      nodeTaintsPolicy: Honor
      labelSelector: { matchLabels: { app.kubernetes.io/component: serve } }
  triggers:
    enabled: true
    content:
      version: 1
      triggers:
        - name: every-minute
          type: schedule
          cron: "* * * * *"
          config:
            version: 1
            name: tick
            pipeline:
              source:
                type: postgres
                config:
                  connection_url: "${PG}"
                  query: "SELECT to_char(date_trunc('minute', now()), 'HH24:MI') AS ts, 'x' AS fired"
              sink:
                type: postgres
                config: { connection_url: "${PG}", table_name: tick_log, column_mapping: auto_map }
EOF
}

values_single() {
  cat <<EOF
image: { repository: ${IMAGE%:*}, tag: "${IMAGE##*:}", pullPolicy: IfNotPresent }
connectors: { sources: [postgres], sinks: [postgres] }
envFrom: [{ secretRef: { name: faucet-creds } }]
serve:
  history: { backend: sqlite }
  persistence: { enabled: true, size: 1Gi }
  auth: { mode: token }
  extraArgs: [--allow-env=PGPASSWORD, "--lease-ttl-secs=${LEASE_TTL}"]
job: { enabled: true }
pipelineConfig:
  create: true
  content: |
$(pipeline_yaml fast | sed 's/^/    /')
EOF
}

install() { # cluster|single
  "values_$1" > "${WORK}/values-$1.yaml"
  helm uninstall faucet -n "$NS" --kube-context "kind-${CLUSTER}" --wait >/dev/null 2>&1
  k delete pvc --all --ignore-not-found >/dev/null 2>&1
  helm upgrade --install faucet "$CHART" -n "$NS" --kube-context "kind-${CLUSTER}" \
    -f "${WORK}/values-$1.yaml" --wait --timeout 10m >/dev/null || return 1
  TOKEN="$(k get secret faucet-faucet-stream-serve-auth -o jsonpath='{.data.FAUCET_SERVE_AUTH_TOKEN}' | base64 -d)"
  INSTALLED_AT=$(date +%s)
}

# ------------------------------------------------------------------- API ---

# Ready serve pods that are not being deleted: a terminating pod still reads
# as Running, and a port-forward to it dies mid-scenario.
pods() {
  k get pod -l app.kubernetes.io/component=serve -o json \
    | jq -r '.items[] | select(.metadata.deletionTimestamp == null)
        | select(any(.status.conditions[]?; .type == "Ready" and .status == "True")) | .metadata.name' | sort
}

NEXT_PORT=19100
port_for() { # pod -> local port with a live port-forward
  local pod="$1" port
  port=$NEXT_PORT; NEXT_PORT=$((NEXT_PORT + 1))
  k port-forward "pod/${pod}" "${port}:8080" >/dev/null 2>&1 &
  PF_PIDS+=("$!")
  for _ in $(seq 1 30); do curl -s -o /dev/null "localhost:${port}/healthz" && break; sleep 0.3; done
  echo "$port"
}

api() { local port="$1" path="$2"; shift 2; curl -s -m 20 "localhost:${port}${path}" -H "Authorization: Bearer ${TOKEN}" "$@"; }

submit() { # port slow|fast [idempotency-key]
  local body
  body="$(pipeline_yaml "$2" | jq -Rs --arg k "${3:-}" '{config: ., config_format: "yaml"} + (if $k == "" then {} else {idempotency_key: $k} end)')"
  api "$1" /v1/runs -X POST -H 'Content-Type: application/json' --data-binary "$body"
}

run_id() { jq -r '.run_id // empty'; }
field() { api "$1" "/v1/runs/$2" | jq -r ".${3}"; }

owner_of() { # run id -> pod that last started it
  local id="$1" p t best="" best_t=""
  for p in $(pods); do
    t="$(k logs "$p" -c serve 2>/dev/null | sed 's/\x1b\[[0-9;]*m//g' | grep "serve_run_id=${id}" | grep 'pipeline run starting' | tail -1 | cut -c1-27)"
    if [ -n "$t" ] && [[ "$t" > "$best_t" ]]; then best="$p"; best_t="$t"; fi
  done
  echo "$best"
}

wait_owner() { local id="$1" o; for _ in $(seq 1 60); do o="$(owner_of "$id")"; [ -n "$o" ] && { echo "$o"; return 0; }; sleep 1; done; return 1; }

wait_terminal() { # port id timeout_secs; re-picks a pod if the port-forward dies
  local port="$1" s
  for _ in $(seq 1 "$3"); do
    s="$(field "$port" "$2" status 2>/dev/null)"
    case "$s" in
      completed|failed|cancelled) echo "$s"; return 0 ;;
      ""|null) port="$(any_port)" ;;
    esac
    sleep 1
  done
  echo "timeout (last status: ${s:-none})"; return 1
}

any_port() { port_for "$(pods | head -1)"; }

fail() { echo "FAIL: $*"; return 1; }

# -------------------------------------------------------------- scenarios ---

s_membership() { # #914 too: pods started together must not start degraded
  local p port body
  for p in $(pods); do
    port="$(port_for "$p")"
    body="$(curl -s "localhost:${port}/readyz")"
    [ "$(jq -r .cluster.instances <<<"$body")" = "$REPLICAS" ] || fail "${p}: ${body}" || return 1
    [ "$(jq -r .history_ok <<<"$body")" = true ] || fail "${p}: ${body}" || return 1
    if k logs "$p" -c serve | sed 's/\x1b\[[0-9;]*m//g' | grep -q 'starting DEGRADED'; then fail "${p} started degraded (#914)"; return 1; fi
  done
  echo "every pod ready, ${REPLICAS} members, none started degraded"
}

s_kill_owner() {
  reset_slow
  local port id owner t0 st
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  owner="$(wait_owner "$id")" || fail "no owner for ${id}" || return 1
  sleep 10
  k delete pod "$owner" --grace-period=0 --force >/dev/null 2>&1; t0=$(date +%s)
  sleep 5; port="$(any_port)"
  st="$(wait_terminal "$port" "$id" 300)"
  [ "$st" = completed ] || fail "status ${st}" || return 1
  [ "$(field "$port" "$id" attempt)" -ge 1 ] || fail "not reclaimed" || return 1
  [ "$(sql 'SELECT count(*) FROM slow_out')" = 600 ] || fail "rows $(sql 'SELECT count(*) FROM slow_out')" || return 1
  single_writer || return 1
  echo "owner killed; reclaimed and completed $(( $(date +%s) - t0 ))s later, 600 rows, $(writers) writers never concurrent"
}

s_cross_cancel() {
  reset_slow
  local id owner other port st before after
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  owner="$(wait_owner "$id")" || return 1
  other="$(pods | grep -v "$owner" | head -1)"; port="$(port_for "$other")"
  sleep 8
  api "$port" "/v1/runs/${id}/cancel" -X POST -o /dev/null
  st="$(wait_terminal "$port" "$id" 60)"; before="$(sql 'SELECT count(*) FROM slow_out')"
  [ "$st" = cancelled ] || fail "status ${st}" || return 1
  sleep 10; after="$(sql 'SELECT count(*) FROM slow_out')"
  [ "$before" = "$after" ] || fail "rows grew after cancelled: ${before} -> ${after}" || return 1
  [ "$(field "$port" "$id" records_written)" = "$after" ] || fail "records_written disagrees with ${after} rows" || return 1
  echo "cancelled through the non-owner; ${after} rows, none after the status"
}

s_idempotency() {
  local a b key r1 r2 code
  a="$(port_for "$(pods | sed -n 1p)")"; b="$(port_for "$(pods | sed -n 2p)")"; key="chaos-$RANDOM$RANDOM"
  r1="$(submit "$a" fast "$key" | run_id)"; r2="$(submit "$b" fast "$key" | run_id)"
  [ -n "$r1" ] && [ "$r1" = "$r2" ] || fail "different runs: ${r1} ${r2}" || return 1
  code="$(pipeline_yaml slow | jq -Rs --arg k "$key" '{config: ., config_format: "yaml", idempotency_key: $k}' \
    | curl -s -o /dev/null -w '%{http_code}' -X POST "localhost:${b}/v1/runs" -H "Authorization: Bearer ${TOKEN}" -H 'Content-Type: application/json' --data-binary @-)"
  [ "$code" = 409 ] || fail "reused key with another body: HTTP ${code}" || return 1
  echo "same key on both pods -> one run; different body -> 409"
}

s_node_stop() {
  reset_slow
  local port id owner node t0 st
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  owner="$(wait_owner "$id")" || return 1
  node="$(k get pod "$owner" -o jsonpath='{.spec.nodeName}')"
  for p in $(pods); do
    [ "$p" = "$owner" ] && continue
    [ "$(k get pod "$p" -o jsonpath='{.spec.nodeName}')" != "$node" ] && break
    fail "every replica is on ${node}; stopping it leaves no survivor"; return 1
  done
  sleep 8
  docker stop "$node" >/dev/null; t0=$(date +%s)
  local survivor; survivor="$(pods | grep -v "$owner" | head -1)"; port="$(port_for "$survivor")"
  st="$(wait_terminal "$port" "$id" 360)"
  docker start "$node" >/dev/null
  kubectl wait --for=condition=Ready node --all --timeout=300s >/dev/null
  k rollout status deploy/faucet-faucet-stream --timeout=600s >/dev/null
  [ "$st" = completed ] || fail "status ${st}" || return 1
  [ "$(sql 'SELECT count(*) FROM slow_out')" = 600 ] || fail "rows $(sql 'SELECT count(*) FROM slow_out')" || return 1
  single_writer || return 1
  echo "node ${node} stopped; run completed on the survivor $(( $(date +%s) - t0 ))s later, 600 rows, $(writers) writers never concurrent"
}

s_rolling_restart() {
  reset_slow
  local port id st
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  wait_owner "$id" >/dev/null || return 1
  sleep 8
  k rollout restart deploy/faucet-faucet-stream >/dev/null
  k rollout status deploy/faucet-faucet-stream --timeout=600s >/dev/null
  port="$(any_port)"; st="$(wait_terminal "$port" "$id" 300)"
  [ "$st" = completed ] || fail "status ${st}" || return 1
  [ "$(sql 'SELECT count(*) FROM slow_out')" = 600 ] || fail "rows" || return 1
  single_writer || return 1
  echo "rolled every pod mid-run; run completed with 600 rows, $(writers) writers never concurrent"
}

s_all_pods() {
  reset_slow; sql "TRUNCATE fast_out"
  local port id q1 q2 st
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"; q1="$(submit "$port" fast | run_id)"; q2="$(submit "$port" fast | run_id)"
  wait_owner "$id" >/dev/null || return 1
  k delete pod -l app.kubernetes.io/component=serve --grace-period=0 --force >/dev/null 2>&1
  k rollout status deploy/faucet-faucet-stream --timeout=600s >/dev/null
  sleep 5; port="$(any_port)"
  for r in "$id" "$q1" "$q2"; do
    st="$(wait_terminal "$port" "$r" 360)"; [ "$st" = completed ] || fail "${r}: ${st}" || return 1
  done
  echo "every pod killed; the interrupted run and both queued runs completed"
}

s_history_outage() { # #905: readiness must see it; no run executes twice
  reset_slow
  local port id owner p st body down=0
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  owner="$(wait_owner "$id")" || return 1
  local ports=(); for p in $(pods); do ports+=("$(port_for "$p")"); done
  psql postgres "ALTER DATABASE faucet_history ALLOW_CONNECTIONS false; SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'faucet_history'" >/dev/null
  for _ in $(seq 1 $((LEASE_TTL + 15))); do
    down=0
    for p in "${ports[@]}"; do
      body="$(curl -s -m 3 "localhost:${p}/readyz")"
      [ "$(jq -r .history_ok <<<"$body" 2>/dev/null)" = false ] && down=$((down + 1))
    done
    [ "$down" -eq "${#ports[@]}" ] && break
    sleep 1
  done
  [ "$down" -eq "${#ports[@]}" ] || { psql postgres "ALTER DATABASE faucet_history ALLOW_CONNECTIONS true" >/dev/null; fail "/readyz still history_ok on $(( ${#ports[@]} - down )) pod(s) (#905)"; return 1; }
  sleep 20
  psql postgres "ALTER DATABASE faucet_history ALLOW_CONNECTIONS true" >/dev/null
  for p in "${ports[@]}"; do
    for _ in $(seq 1 60); do [ "$(curl -s -m 3 "localhost:${p}/readyz" | jq -r .history_ok 2>/dev/null)" = true ] && break; sleep 1; done
  done
  st="$(wait_terminal "${ports[0]}" "$id" 360)"
  [ "$st" = completed ] || fail "status ${st}" || return 1
  [ "$(sql 'SELECT count(*) FROM slow_out')" = 600 ] || fail "rows" || return 1
  single_writer || return 1
  k logs "$owner" -c serve | sed 's/\x1b\[[0-9;]*m//g' | grep -q 'could not renew run leases' || fail "the owner never fenced itself" || return 1
  echo "every pod reported history_ok=false within the lease window; owner self-fenced; run completed, 600 rows, $(writers) writers never concurrent"
}

s_burst() {
  local ports=() p i ids=() ok=0 a=0 b=0 n start
  for p in $(pods); do ports+=("$(port_for "$p")"); done
  # Earlier scenarios ran fast runs on these pods too; count only this burst's.
  start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  for i in $(seq 1 40); do ids+=("$(submit "${ports[$((i % ${#ports[@]}))]}" fast | run_id)"); done
  [ "$(printf '%s\n' "${ids[@]}" | grep -c .)" = 40 ] || fail "only $(printf '%s\n' "${ids[@]}" | grep -c .) of 40 accepted" || return 1
  for id in "${ids[@]}"; do
    [ "$(wait_terminal "${ports[0]}" "$id" 300)" = completed ] && ok=$((ok + 1))
  done
  [ "$ok" = 40 ] || fail "${ok} of 40 completed" || return 1
  for p in $(pods); do
    n="$(k logs "$p" -c serve --since-time="$start" | sed 's/\x1b\[[0-9;]*m//g' | grep -c 'invocation completed row=row-0 records_written=100' || true)"
    if [ "$a" -eq 0 ]; then a=$n; else b=$((b + n)); fi
  done
  [ $((a + b)) = 40 ] || fail "$((a + b)) invocations completed for 40 runs (${a}/${b} across pods)" || return 1
  echo "40 accepted, 40 completed once each; split ${a}/${b} across pods"
}

s_poison() {
  local port id owner st
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  for _ in 1 2 3 4 5 6; do
    owner="$(wait_owner "$id")" || break
    k delete pod "$owner" --grace-period=0 --force >/dev/null 2>&1
    sleep $((LEASE_TTL + 10))
    port="$(any_port)"; st="$(field "$port" "$id" status)"
    [ "$st" = failed ] && break
  done
  st="$(wait_terminal "$port" "$id" 120)"
  [ "$st" = failed ] || fail "status ${st}" || return 1
  field "$port" "$id" error | grep -qi poison || fail "error: $(field "$port" "$id" error)" || return 1
  echo "failed as a poison run after $(field "$port" "$id" attempt) reclaims"
}

s_schedule() { # once per tick across every replica, through everything above
  # A tick missed while the history was unreachable fires once as catch-up,
  # in a later minute, so a minute may hold two rows and the total matches.
  local window expected actual dupes
  window="ts > to_char(date_trunc('minute', to_timestamp(${INSTALLED_AT})), 'HH24:MI') AND ts < to_char(date_trunc('minute', now()), 'HH24:MI')"
  expected="$(sql "SELECT count(*) FROM generate_series(date_trunc('minute', to_timestamp(${INSTALLED_AT})) + interval '1 minute', date_trunc('minute', now()) - interval '1 minute', interval '1 minute')")"
  actual="$(sql "SELECT count(*) FROM tick_log WHERE ${window}")"
  dupes="$(sql "SELECT coalesce(string_agg(ts || ' x' || n, ', '), '') FROM (SELECT ts, count(*) n FROM tick_log WHERE ${window} GROUP BY ts HAVING count(*) > 2) d")"
  [ -z "$dupes" ] || fail "a minute fired more than a tick plus one catch-up: ${dupes}" || return 1
  [ "$actual" -ge "$expected" ] || fail "${actual} runs for ${expected} minutes" || return 1
  [ "$actual" -le $((expected + 2)) ] || fail "${actual} runs for ${expected} minutes: duplicates" || return 1
  echo "${actual} scheduled runs over ${expected} minutes across ${REPLICAS} replicas"
}

s_single_kill() {
  reset_slow
  local port id st
  port="$(any_port)"; id="$(submit "$port" slow | run_id)"
  wait_owner "$id" >/dev/null || return 1
  sleep 8
  k delete pod -l app.kubernetes.io/component=serve --grace-period=0 --force >/dev/null 2>&1
  k rollout status deploy/faucet-faucet-stream --timeout=600s >/dev/null
  port="$(any_port)"; st="$(wait_terminal "$port" "$id" $((LEASE_TTL * 4)))"
  [ "$st" = failed ] || fail "status ${st}" || return 1
  echo "the interrupted run ended failed: $(field "$port" "$id" error)"
}

s_single_upgrade() { # #906: never two servers on one SQLite file
  local strategy max=0 n
  strategy="$(k get deploy faucet-faucet-stream -o jsonpath='{.spec.strategy.type}')"
  [ "$strategy" = Recreate ] || fail "strategy ${strategy}" || return 1
  k rollout restart deploy/faucet-faucet-stream >/dev/null
  sleep 2
  for _ in $(seq 1 120); do
    n="$(k get pod -l app.kubernetes.io/component=serve --no-headers 2>/dev/null | grep -c Running)"
    [ "$n" -gt "$max" ] && max=$n
    k rollout status deploy/faucet-faucet-stream --timeout=1s >/dev/null 2>&1 && break
    sleep 1
  done
  [ "$max" -le 1 ] || fail "${max} servers ran at once" || return 1
  echo "Recreate: at most one server at a time during the upgrade"
}

s_job_upgrade() { # #903
  local before after
  before="$(k get job -l app.kubernetes.io/component=run -o name)"
  helm upgrade faucet "$CHART" -n "$NS" --kube-context "kind-${CLUSTER}" -f "${WORK}/values-single.yaml" \
    --set-string 'job.podAnnotations.chaos=changed' --wait --timeout 10m >/dev/null || fail "helm upgrade failed" || return 1
  after="$(k get job -l app.kubernetes.io/component=run -o name)"
  [ "$before" != "$after" ] || fail "the Job was not replaced" || return 1
  echo "upgrade with a changed Job replaced ${before#job.batch/} with ${after#job.batch/}"
}

# ----------------------------------------------------------------- report ---

write_report() {
  local pass=0 failed=0 r
  for r in "${RESULTS[@]:-}"; do [ -z "$r" ] && continue; [[ "$r" == *"|PASS|"* ]] && pass=$((pass + 1)) || failed=$((failed + 1)); done
  {
    echo "# faucet serve chaos report"
    echo
    echo "- image: \`${IMAGE}\`  ·  chart: \`${CHART#"$ROOT"/}\`  ·  replicas: ${REPLICAS}  ·  lease TTL: ${LEASE_TTL}s"
    echo "- started $(date -r "$START_TS" '+%Y-%m-%d %H:%M' 2>/dev/null || date -d "@$START_TS" '+%Y-%m-%d %H:%M'), took $(( ($(date +%s) - START_TS) / 60 )) min"
    echo "- **${pass} passed, ${failed} failed**"
    echo
    echo "| Scenario | Result | Time | Evidence |"
    echo "|---|---|---|---|"
    for r in "${RESULTS[@]:-}"; do
      [ -z "$r" ] && continue
      IFS='|' read -r n s t e <<<"$r"
      echo "| ${n} | ${s} | ${t}s | ${e//|//} |"
    done
  } > "${OUT}/chaos-report.md"
  printf '%s\n' "${RESULTS[@]:-}" | jq -R 'select(length > 0) | split("|") | {scenario: .[0], result: .[1], seconds: (.[2] | tonumber), evidence: .[3]}' \
    | jq -s --arg image "$IMAGE" --argjson replicas "$REPLICAS" '{image: $image, replicas: $replicas, scenarios: .}' > "${OUT}/chaos-report.json"
  cat "${OUT}/chaos-report.md"
  return "$failed"
}

# ------------------------------------------------------------------- main ---

prepare_image
log "creating kind cluster ${CLUSTER}"
create_cluster
deploy_postgres

log "installing ${REPLICAS} clustered replicas"
fresh_history_db
if install cluster; then
  run_scenario membership s_membership
  run_scenario kill-owner s_kill_owner
  run_scenario cross-cancel s_cross_cancel
  run_scenario idempotency s_idempotency
  run_scenario node-stop s_node_stop
  run_scenario rolling-restart s_rolling_restart
  run_scenario all-pods s_all_pods
  run_scenario history-outage s_history_outage
  run_scenario burst s_burst
  run_scenario poison s_poison
  run_scenario schedule s_schedule
else
  record install-cluster FAIL 0 "helm install of the clustered deployment failed"
fi

if wanted single-kill || wanted single-upgrade || wanted job-upgrade; then
  log "installing a single pod with SQLite on a volume"
  if install single; then
    run_scenario single-kill s_single_kill
    run_scenario single-upgrade s_single_upgrade
    run_scenario job-upgrade s_job_upgrade
  else
    record install-single FAIL 0 "helm install of the single-pod deployment failed"
  fi
fi

write_report
exit $?

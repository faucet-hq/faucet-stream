#!/usr/bin/env bash
#
# try-local.sh — build faucet-stream with EVERY feature and exercise it end-to-end
# against a self-contained, file-only dummy setup (no Docker / external services).
#
# What it does:
#   1. Resolves the pinned rustup toolchain (works around a Homebrew rustc on PATH).
#   2. Builds the `faucet` CLI with `--features full` (downloads every crate).
#   3. Generates a throwaway demo workspace (data + configs) under ./faucet-local-demo.
#   4. Runs a battery of `faucet` commands covering the file-based connectors and
#      every config-level feature that works without external infra:
#      transforms · quality + DLQ · data contracts · PII masking · SQL transform ·
#      SQLite round-trip · Parquet round-trip · SLA · file lineage · catalog ·
#      offline pipeline tests · dlq inspect/replay · doctor · preview · serve smoke.
#
# By default it builds a LIGHT feature set (file connectors + governance + the
# web console + persistent history + lineage + catalog) — pure-Rust, builds in a
# few minutes, no cmake/DuckDB/Kafka toolchain needed. Pass --full for the
# everything build (Kafka, gRPC, cloud, DuckDB SQL; needs cmake, ~15-30 min).
#
# Usage (run from anywhere — it resolves the repo root itself):
#   ./scripts/try-local.sh              # light build + battery, then leave the web UI up
#   ./scripts/try-local.sh --full       # build every feature instead of the light set
#   ./scripts/try-local.sh --release    # optimised build (slower compile, faster run)
#   ./scripts/try-local.sh --clean      # wipe the demo workspace first
#   ./scripts/try-local.sh --no-build   # skip the build (reuse an existing binary)
#   ./scripts/try-local.sh --no-serve   # run the battery and exit (no UI) — for CI
#   ./scripts/try-local.sh --serve-only # skip build+battery, just (re)launch the populated UI
#   ./scripts/try-local.sh --port 9000  # web console / serve port (default 8899)
#
# By default the script ends by starting the web console and LEAVING IT RUNNING
# so you can browse Runs / Datasets / Lineage. It submits a handful of demo runs
# through the HTTP API first, so those views arrive already populated. Press
# Ctrl+C to stop the server and exit.
#
# The demo workspace (./faucet-local-demo) is safe to delete at any time.

set -uo pipefail

# ----------------------------------------------------------------------------
# Config / args
# ----------------------------------------------------------------------------
# This script lives in scripts/; the repo root is one level up.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEMO_DIR="${REPO_ROOT}/faucet-local-demo"
BUILD_PROFILE="debug"
DO_BUILD=1
DO_CLEAN=0
DO_SERVE=1
SERVE_ONLY=0
SERVE_PORT=8899

# Light default feature set — file-only connectors + governance + the web
# console + persistent history + lineage + catalog. Pure-Rust deps only, so it
# builds in a few minutes with no cmake / DuckDB / librdkafka. `--full` swaps in
# the everything build (Kafka, gRPC, cloud, DuckDB SQL — needs cmake, ~15-30min).
LIGHT_FEATURES="source-csv,source-sqlite,source-parquet,sink-jsonl,sink-csv,sink-stdout,sink-sqlite,sink-parquet,transforms,quality,contract,masking,serve,serve-ui,serve-history-sqlite,lineage,catalog,schedule,triggers,templates"
BUILD_FEATURES="$LIGHT_FEATURES"

while [ $# -gt 0 ]; do
  case "$1" in
    --release)    BUILD_PROFILE="release" ;;
    --full)       BUILD_FEATURES="full" ;;
    --no-build)   DO_BUILD=0 ;;
    --clean)      DO_CLEAN=1 ;;
    --no-serve)   DO_SERVE=0 ;;
    --serve-only) SERVE_ONLY=1; DO_BUILD=0 ;;
    --port)       shift; SERVE_PORT="$1" ;;
    -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown arg: $1 (use --help)"; exit 2 ;;
  esac
  shift
done

# ----------------------------------------------------------------------------
# Pretty output + pass/fail bookkeeping
# ----------------------------------------------------------------------------
if [ -t 1 ]; then
  BOLD=$'\033[1m'; GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; CYAN=$'\033[36m'; RESET=$'\033[0m'
else
  BOLD=''; GREEN=''; RED=''; YELLOW=''; CYAN=''; RESET=''
fi

PASS=0; FAIL=0; FAILED_STEPS=()

hdr()  { echo; echo "${BOLD}${CYAN}==> $*${RESET}"; }
info() { echo "${YELLOW}··· $*${RESET}"; }

# step "name" cmd args...   → runs a command, records pass/fail, keeps going.
step() {
  local name="$1"; shift
  echo; echo "${BOLD}--- ${name}${RESET}"
  echo "${YELLOW}\$ $*${RESET}"
  if "$@"; then
    echo "${GREEN}✔ PASS: ${name}${RESET}"
    PASS=$((PASS+1))
  else
    echo "${RED}✗ FAIL: ${name} (exit $?)${RESET}"
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$name")
  fi
}

# step_expect_fail: some commands are *supposed* to exit non-zero (e.g. a
# contract `fail` policy). Treat a non-zero exit as the pass condition.
step_expect_fail() {
  local name="$1"; shift
  echo; echo "${BOLD}--- ${name}  (expecting non-zero exit)${RESET}"
  echo "${YELLOW}\$ $*${RESET}"
  if "$@"; then
    echo "${RED}✗ FAIL: ${name} — expected non-zero exit but it succeeded${RESET}"
    FAIL=$((FAIL+1)); FAILED_STEPS+=("$name")
  else
    echo "${GREEN}✔ PASS: ${name} (exited non-zero as expected)${RESET}"
    PASS=$((PASS+1))
  fi
}

# launch_ui: start the web console against a shared SQLite history DB, submit a
# few demo runs through the HTTP API so Runs / Datasets / Lineage arrive
# populated, then BLOCK (server stays up) until Ctrl+C.
SERVE_PID=""
stop_serve() { [ -n "$SERVE_PID" ] && kill "$SERVE_PID" 2>/dev/null; }

launch_ui() {
  local base="http://127.0.0.1:${SERVE_PORT}"

  # Ensure the DLQ demo files exist so the run-detail DLQ panel is usable even
  # in --serve-only mode (the battery normally produces these). Cheap to redo.
  if [ ! -f "${DEMO_DIR}/dlq/contract.jsonl" ] || [ ! -f "${DEMO_DIR}/dlq/quality.jsonl" ]; then
    info "Generating DLQ demo files (quality + contract quarantine)…"
    ( cd "$DEMO_DIR" && "$FAUCET" run 03_quality.yaml >/dev/null 2>&1
                        "$FAUCET" run 04_contract.yaml >/dev/null 2>&1 )
  fi

  # NOTE: the history DB is intentionally NOT wiped — run history accumulates
  # across invocations so the Runs view shows "what has happened" over time.
  # Use --clean to reset the whole workspace (including this DB).
  hdr "Starting the web console on ${base}"
  # Run from DEMO_DIR so submitted configs' ./data and ./out paths resolve.
  ( cd "$DEMO_DIR" && exec "$FAUCET" serve --listen "127.0.0.1:${SERVE_PORT}" \
      --no-auth --history "sqlite:./faucet-meta.db" ) >"${DEMO_DIR}/serve.log" 2>&1 &
  SERVE_PID=$!
  trap 'echo; info "Stopping web console (pid '"$SERVE_PID"')"; stop_serve; exit 0' INT TERM

  for _ in $(seq 1 40); do curl -sf "${base}/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
  if ! curl -sf "${base}/healthz" >/dev/null 2>&1; then
    echo "${RED}Web console failed to start — see ${DEMO_DIR}/serve.log${RESET}"
    stop_serve; return 1
  fi

  # Populate the console: submit representative configs over the HTTP API. Each
  # run executes inside serve, which records the run + its catalog datasets and
  # source→sink lineage edges into the shared history DB.
  if command -v python3 >/dev/null 2>&1 && command -v curl >/dev/null 2>&1; then
    info "Submitting demo runs so Runs / Datasets / Lineage are pre-populated…"
    local spec cfg nm body
    local submit_specs="01_basic.yaml:ui-basic-csv-to-jsonl 02_transforms.yaml:ui-transforms 05_masking.yaml:ui-masking 08a_to_parquet.yaml:ui-csv-to-parquet 14_matrix.yaml:ui-matrix-fanout 03_quality.yaml:ui-quality-with-dlq 04_contract.yaml:ui-contract-with-dlq"
    [ "$HAVE_SQL" -eq 1 ] && submit_specs="$submit_specs 06_sql.yaml:ui-sql-aggregate"
    for spec in $submit_specs; do
      cfg="${DEMO_DIR}/${spec%%:*}"; nm="${spec##*:}"
      [ -f "$cfg" ] || continue
      body="$(python3 -c 'import json,sys;print(json.dumps({"config":open(sys.argv[1]).read(),"name":sys.argv[2]}))' "$cfg" "$nm")"
      if curl -s -o /dev/null -w '%{http_code}' -X POST "${base}/v1/runs" \
           -H 'content-type: application/json' -d "$body" | grep -q '^20'; then
        info "  submitted ${nm}"
      else
        info "  (submit failed for ${nm} — continuing)"
      fi
    done
    sleep 2

    # Seed the template registry over HTTP if it is empty — so `--serve-only`
    # (which skips the CLI battery) still opens on a populated Templates view.
    if curl -s "${base}/v1/templates" | grep -q '"templates":\[\]'; then
      local tcfg="${DEMO_DIR}/23_templated.yaml"
      if [ -f "$tcfg" ]; then
        info "Seeding the template registry (3 versions, one launched)…"
        for tbody in \
          '{"description":"Per-country orders export","launch":true}' \
          '{"tags":["dev"]}' \
          '{"tags":["staging"]}'
        do
          body="$(python3 -c 'import json,sys;b=json.loads(sys.argv[2]);b["config"]=open(sys.argv[1]).read();print(json.dumps(b))' "$tcfg" "$tbody")"
          curl -s -o /dev/null -X POST "${base}/v1/templates" \
            -H 'content-type: application/json' -d "$body"
        done
        curl -s -o /dev/null -X POST "${base}/v1/templates/orders-by-country/tags" \
          -H 'content-type: application/json' -d '{"tag":"prod","version":1}'
        # A source template + a sink template, so the console shows every kind.
        for hub in "hub/source-templates/faucet-hq/example-csv.yaml" "hub/sink-templates/faucet-hq/jsonl.yaml"; do
          [ -f "${REPO_ROOT}/${hub}" ] || continue
          body="$(python3 -c 'import json,sys;print(json.dumps({"config":open(sys.argv[1]).read(),"launch":True}))' "${REPO_ROOT}/${hub}")"
          curl -s -o /dev/null -X POST "${base}/v1/templates" \
            -H 'content-type: application/json' -d "$body"
        done
      fi
    fi
  else
    info "python3/curl unavailable — the console will start empty; use the Submit tab."
  fi

  echo
  echo "${BOLD}${GREEN}Web console is UP →  ${base}${RESET}"
  echo "  Open it in your browser. Suggested tour:"
  echo "    • Runs      — history of every run (persists across restarts). Click one for"
  echo "                  live/streamed logs, per-stage record counts, and its DLQ panel."
  echo "    • A run's detail page → ${BOLD}Dead-letter queue${RESET} panel — this is DLQ replay in the UI:"
  echo "        1. In 'DLQ location' enter:  ${BOLD}./dlq/contract.jsonl${RESET}  (or ./dlq/quality.jsonl)"
  echo "        2. Click ${BOLD}Inspect${RESET} to see the quarantined rows grouped by reason."
  echo "        3. Expand ${BOLD}Replay through a config${RESET}, paste a pipeline config, and Replay"
  echo "           (keep 'dry-run' on first). Discard (with/without archive) is there too."
  echo "    • Datasets  — every dataset touched, with schema timelines + volume/freshness"
  echo "    • Lineage   — the source→sink graph across all runs (with column lineage)"
  echo "    • Templates — the pipeline template registry: each template's status"
  echo "                  (draft / launched / deprecated), which version is live, and a"
  echo "                  ${BOLD}versions page${RESET} where you assign channels (prod, staging, …),"
  echo "                  Launch / Roll back / Deprecate, and trigger a run from a typed"
  echo "                  form generated from the template's params."
  echo "    • Submit    — build/paste a new config and run it live"
  echo "    • Schemas   — browse every connector's config schema"
  echo
  echo "  Run history DB: ${DEMO_DIR}/faucet-meta.db   (accumulates; reset with --clean)"
  echo "  DLQ files:      ${DEMO_DIR}/dlq/{contract,quality}.jsonl"
  echo "  Server log:     ${DEMO_DIR}/serve.log"
  echo "  ${BOLD}Press Ctrl+C to stop the server and exit.${RESET}"
  echo
  wait "$SERVE_PID"
}

# ----------------------------------------------------------------------------
# Toolchain resolution — Homebrew rustc can shadow rustup on PATH.
# ----------------------------------------------------------------------------
hdr "Resolving Rust toolchain"
CHANNEL="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "${REPO_ROOT}/rust-toolchain.toml" 2>/dev/null)"
CHANNEL="${CHANNEL:-stable}"
ARCH="$(uname -m)"
case "$(uname -s)" in
  Darwin) HOST="${ARCH}-apple-darwin" ;;
  Linux)  HOST="${ARCH}-unknown-linux-gnu" ;;
  *)      HOST="" ;;
esac
TC_BIN="${HOME}/.rustup/toolchains/${CHANNEL}-${HOST}/bin"
if [ -x "${TC_BIN}/cargo" ]; then
  export PATH="${TC_BIN}:${PATH}"
  export RUSTC="${TC_BIN}/rustc"
  info "Using pinned toolchain: ${TC_BIN}"
elif command -v rustup >/dev/null 2>&1; then
  info "Pinned toolchain dir not found; deferring to rustup (channel ${CHANNEL})"
else
  info "rustup not found; using whatever cargo is on PATH"
fi
info "cargo: $(command -v cargo)  ($(cargo --version 2>/dev/null))"

# ----------------------------------------------------------------------------
# Build the CLI with every feature (this downloads all crates)
# ----------------------------------------------------------------------------
if [ "$DO_BUILD" -eq 1 ]; then
  hdr "Building faucet CLI with --features ${BUILD_FEATURES}"
  [ "$BUILD_FEATURES" = "full" ] && info "Full build compiles DuckDB / librdkafka / OpenSSL from source (needs cmake; ~15-30 min)."
  if [ "$BUILD_PROFILE" = "release" ]; then
    cargo build --release -p faucet-cli --features "$BUILD_FEATURES" || { echo "${RED}Build failed.${RESET}"; exit 1; }
    FAUCET="${REPO_ROOT}/target/release/faucet"
  else
    cargo build -p faucet-cli --features "$BUILD_FEATURES" || { echo "${RED}Build failed.${RESET}"; exit 1; }
    FAUCET="${REPO_ROOT}/target/debug/faucet"
  fi
else
  FAUCET="${REPO_ROOT}/target/${BUILD_PROFILE}/faucet"
fi

if [ ! -x "$FAUCET" ]; then
  echo "${RED}faucet binary not found at ${FAUCET}. Run without --no-build.${RESET}"; exit 1
fi
info "Binary: ${FAUCET}"

# Feature-probe the binary so steps that need an optional feature are gated
# regardless of how it was built (light default vs --full vs --no-build reuse).
HAVE_SQL=0;      "$FAUCET" schema transform sql >/dev/null 2>&1 && HAVE_SQL=1
HAVE_TRIGGERS=0; "$FAUCET" schema triggers      >/dev/null 2>&1 && HAVE_TRIGGERS=1
HAVE_SCHEDULE=0; "$FAUCET" schedule --help       >/dev/null 2>&1 && HAVE_SCHEDULE=1
HAVE_TEMPLATES=0; "$FAUCET" template --help      >/dev/null 2>&1 && HAVE_TEMPLATES=1
if [ "$HAVE_SQL" -eq 1 ]; then
  info "SQL transform (DuckDB) available."
else
  info "SQL transform not compiled in — the sql-transform steps will be skipped. Use --full to include it."
fi

# ----------------------------------------------------------------------------
# Demo workspace
# ----------------------------------------------------------------------------
if [ "$DO_CLEAN" -eq 1 ]; then
  hdr "Cleaning demo workspace"
  rm -rf "$DEMO_DIR"
fi
mkdir -p "$DEMO_DIR"/{data,out,dlq,state,catalog,tests}
cd "$DEMO_DIR"

hdr "Generating dummy data + configs under ${DEMO_DIR}"

# --- Sample data -----------------------------------------------------------
# Orders: mix of good rows plus deliberately-bad rows to exercise quality/contract DLQ.
cat > data/orders.csv <<'CSV'
order_id,status,customer_email,amount,country_code
1,open,alice@example.com,10.50,US
2,shipped,bob@example.com,25.00,GB
3,cancelled,carol@example.com,5.25,US
4,shipped,not-an-email,99.99,GB
5,frobnicated,dave@example.com,3.00,DE
6,open,,42.00,US
CSV

# Customers: PII to exercise the masking policy.
cat > data/customers.csv <<'CSV'
user_id,full_name,email,ssn,card,phone,ip
u1,Alice Smith,alice@example.com,123-45-6789,4111 1111 1111 1111,555-123-4567,10.0.0.1
u2,Bob Jones,bob@example.com,987-65-4320,5500 0000 0000 0004,555-987-6543,10.0.0.2
CSV

# Countries: reference table for the SQL-transform join.
cat > data/countries.csv <<'CSV'
code,country
US,United States
GB,United Kingdom
DE,Germany
CSV

# --- 1. Basic CSV → JSONL --------------------------------------------------
cat > 01_basic.yaml <<'YAML'
version: 1
name: basic_csv_to_jsonl
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:   { type: jsonl, config: { path: ./out/basic.jsonl } }
YAML

# --- 2. Transforms (set / keys_case / cast / redact) → stdout --------------
cat > 02_transforms.yaml <<'YAML'
version: 1
name: transforms_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  transforms:
    - type: set
      config: { values: { _source: demo } }
    - type: cast
      config: { fields: { amount: float }, on_error: skip }
    - type: redact
      config: { fields: [customer_email], mask: "[redacted]" }
  sink:
    type: stdout
    config: { destination: stdout, format: json_lines, max_records: 10 }
YAML

# --- 3. Quality checks + DLQ (bad email / bad status get quarantined) ------
cat > 03_quality.yaml <<'YAML'
version: 1
name: quality_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  quality:
    record:
      - type: not_null
        field: order_id
        on_failure: abort
      - type: regex_match
        field: customer_email
        pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$'
        on_failure: quarantine
      - type: value_in_set
        field: status
        values: [open, shipped, cancelled]
        on_failure: quarantine
  dlq:
    sink: { type: jsonl, config: { path: ./dlq/quality.jsonl } }
  sink:
    type: jsonl
    config: { path: ./out/quality_clean.jsonl }
YAML

# --- 4. Data contract, quarantine policy → DLQ -----------------------------
cat > 04_contract.yaml <<'YAML'
version: 1
name: contract_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  contract:
    version: "1.0.0"
    on_breach: quarantine
    allow_extra_fields: true
    fields:
      - { name: order_id, type: string, min_length: 1 }
      - { name: status, type: string, enum: [open, shipped, cancelled] }
      - { name: customer_email, type: string, pattern: '^[^@\s]+@[^@\s]+\.[^@\s]+$', required: false, nullable: true }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq/contract.jsonl } }
  sink:
    type: jsonl
    config: { path: ./out/contract_out.jsonl }
YAML

# --- 5. PII masking --------------------------------------------------------
cat > 05_masking.yaml <<'YAML'
version: 1
name: masking_demo
pipeline:
  source: { type: csv, config: { path: ./data/customers.csv } }
  masking:
    key: demo-masking-key
    rules:
      - name: emails
        match: { value_detector: email }
        action: { type: redact }
      - name: ssn
        match: { field_pattern: '(?i)^ssn$' }
        action: { type: hash }
      - name: cards
        match: { value_detector: credit_card }
        action: { type: partial, keep_last: 4 }
      - name: user-id
        match: { fields: [user_id] }
        action: { type: tokenize, prefix: usr_ }
  sink:
    type: jsonl
    config: { path: ./out/masked.jsonl }
YAML

# --- 6. Embedded DuckDB SQL transform (global aggregation + join) ----------
cat > 06_sql.yaml <<'YAML'
version: 1
name: sql_demo
pipeline:
  source:
    type: csv
    config: { path: ./data/orders.csv, has_header: true, batch_size: 0 }
  transforms:
    - type: sql
      config:
        query: |
          SELECT c.country,
                 COUNT(*)                       AS order_count,
                 SUM(CAST(o.amount AS DOUBLE))  AS total_amount
          FROM   batch o
          LEFT JOIN countries c ON o.country_code = c.code
          GROUP BY c.country
          ORDER BY c.country
        relations:
          - name: countries
            source: { type: csv, path: ./data/countries.csv, has_header: true }
  sink:
    type: jsonl
    config: { path: ./out/sql_agg.jsonl }
YAML

# --- 7. SQLite round-trip: CSV → SQLite, then SQLite → CSV -----------------
# The sqlite sink inserts into an EXISTING table (auto_map maps JSON keys to
# columns); it does not create the data table. We pre-create it with the
# `sqlite3` CLI below (this step is skipped if sqlite3 is unavailable).
cat > 07a_to_sqlite.yaml <<'YAML'
version: 1
name: csv_to_sqlite
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:
    type: sqlite
    config:
      database_url: sqlite:./out/orders.db
      table_name: orders
      column_mapping: auto_map
YAML

cat > 07b_from_sqlite.yaml <<'YAML'
version: 1
name: sqlite_to_csv
pipeline:
  source:
    type: sqlite
    config:
      database_url: sqlite:./out/orders.db
      query: SELECT * FROM orders ORDER BY order_id
  sink:
    type: csv
    config: { path: ./out/orders_roundtrip.csv, write_headers: true }
YAML

# --- 8. Parquet round-trip: CSV → Parquet, then Parquet → JSONL ------------
cat > 08a_to_parquet.yaml <<'YAML'
version: 1
name: csv_to_parquet
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:
    type: parquet
    config:
      destination: { type: local_path, path: ./out/orders.parquet }
YAML

cat > 08b_from_parquet.yaml <<'YAML'
version: 1
name: parquet_to_jsonl
pipeline:
  source:
    type: parquet
    config:
      source: { type: local_path, path: ./out/orders.parquet }
  sink:
    type: jsonl
    config: { path: ./out/orders_from_parquet.jsonl }
YAML

# --- 9. SLA monitoring (needs a state block) -------------------------------
cat > 09_sla.yaml <<'YAML'
version: 1
name: sla_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:   { type: jsonl, config: { path: ./out/sla_out.jsonl } }
  state:  { type: file, config: { path: ./state } }
sla:
  max_staleness_secs: 86400
  min_rows_per_run: 1
  volume_anomaly: { method: zscore, sensitivity: 3.0, min_history: 5, window: 20 }
YAML

# --- 10. File lineage (OpenLineage events appended to a local JSONL) -------
cat > 10_lineage.yaml <<'YAML'
version: 1
name: lineage_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  transforms:
    - type: redact
      config: { fields: [customer_email], mask: "***" }
  sink: { type: jsonl, config: { path: ./out/lineage_out.jsonl } }
lineage:
  namespace: local.demo
  job_name: ${name}::${row_id}
  include_schema_facet: true
  include_column_lineage: true
  transport:
    type: file
    config: { path: ./out/lineage_events.jsonl }
YAML

# --- 11. Data Movement Catalog (SQLite-backed) -----------------------------
cat > 11_catalog.yaml <<'YAML'
version: 1
name: catalog_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:   { type: jsonl, config: { path: ./out/catalog_out.jsonl } }
catalog:
  url: sqlite:./catalog/catalog.db
YAML

# --- 12. Offline pipeline test spec (faucet test) --------------------------
cat > tests/pipeline_tests.yaml <<'YAML'
version: 1
tests:
  - name: keys_case + set applied
    pipeline:
      transforms:
        - type: keys_case
          config: { mode: snake }
        - type: set
          config: { values: { tag: t } }
    input:
      - { OrderId: 1, Amount: 9.5 }
    expect:
      records:
        - { order_id: 1, amount: 9.5, tag: t }

  - name: masking redacts emails
    pipeline:
      masking:
        rules:
          - match: { value_detector: email }
            action: { type: redact }
    input:
      - { contact: "x@y.com", city: NYC }
    expect:
      records:
        - { contact: "***", city: NYC }
YAML

# --- 13. Minimal serve config (submitted over HTTP in the serve smoke test) -
cat > 13_serve_run.yaml <<'YAML'
version: 1
name: serve_submitted
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:   { type: jsonl, config: { path: ./out/serve_out.jsonl } }
YAML

# --- 14. Matrix fan-out: one source template, N rows → N outputs -----------
cat > 14_matrix.yaml <<'YAML'
version: 1
name: matrix_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:   { type: jsonl, config: { path: ./out/matrix_default.jsonl } }
matrix:
  - id: us
    sink: { config: { path: ./out/matrix_us.jsonl } }
  - id: gb
    sink: { config: { path: ./out/matrix_gb.jsonl } }
YAML

# --- 15. depends_on completion ordering (stage → report) -------------------
cat > 15_depends.yaml <<'YAML'
version: 1
name: depends_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:   { type: csv, config: { path: ./out/staged.csv } }
matrix:
  - id: stage
    sink: { config: { path: ./out/staged_orders.csv } }
  - id: report
    depends_on: [stage]
    source: { config: { path: ./out/staged_orders.csv } }
    sink: { type: jsonl, config: { path: ./out/report.jsonl } }
YAML

# --- 16. Transforms showcase: set(nested/array) → filter → explode →
#         flatten → value_case ------------------------------------------------
cat > 16_transforms2.yaml <<'YAML'
version: 1
name: transforms2_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  transforms:
    - type: set
      config:
        values:
          meta: { region: us, tier: gold }
          reviews: [{ stars: 5 }, { stars: 3 }]
    - type: filter
      config: { path: status, op: ne, value: cancelled }
    - type: explode
      config: { path: reviews, prefix: review }
    - type: flatten
      config: { separator: "__" }
    - type: value_case
      config: { fields: [status], mode: upper }
  sink:
    type: stdout
    config: { destination: stdout, format: json_lines, max_records: 20 }
YAML

# --- 17. Schema-drift EVOLVE on SQLite (table missing a column → ADD COLUMN)-
cat > 17_drift.yaml <<'YAML'
version: 1
name: drift_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink:
    type: sqlite
    config:
      database_url: sqlite:./out/drift.db
      table_name: orders_drift
      column_mapping: auto_map
  schema:
    on_drift: evolve
YAML

# --- 18. Contract `fail` policy → the run aborts (expected non-zero exit) ---
cat > 18_contract_fail.yaml <<'YAML'
version: 1
name: contract_fail_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  contract:
    version: "1.0.0"
    on_breach: fail
    fields:
      - { name: status, type: string, enum: [open, shipped, cancelled] }
  sink: { type: jsonl, config: { path: ./out/cfail.jsonl } }
YAML

# --- 19. Quality `abort` policy → the run aborts (expected non-zero exit) ---
cat > 19_quality_abort.yaml <<'YAML'
version: 1
name: quality_abort_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  quality:
    record:
      - type: value_in_set
        field: status
        values: [open, shipped, cancelled]
        on_failure: abort
  sink: { type: jsonl, config: { path: ./out/qabort.jsonl } }
YAML

# --- 20. JSON (not YAML) config format -------------------------------------
cat > 20_basic.json <<'JSON'
{ "version": 1, "name": "json_config_demo",
  "pipeline": {
    "source": { "type": "csv", "config": { "path": "./data/orders.csv" } },
    "sink":   { "type": "jsonl", "config": { "path": "./out/from_json.jsonl" } } } }
JSON

# --- 21. Config composition: base + child `extends:` + `profiles:` ----------
cat > 21_base.yaml <<'YAML'
version: 1
name: compose_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink: { type: jsonl, config: { path: ./out/compose_dev.jsonl } }
YAML
cat > 21_child.yaml <<'YAML'
extends: ./21_base.yaml
profiles:
  prod:
    pipeline:
      sink: { config: { path: ./out/compose_prod.jsonl } }
YAML

# --- 23. Parameterized config for the pipeline template registry -----------
cat > 23_templated.yaml <<'YAML'
kind: pipeline
version: 1
name: orders-by-country

params:
  country:
    type: string
    required: true
    description: ISO country code to export
  label:
    type: string
    default: nightly
    description: Free-text tag written into every exported record
  batch_size:
    type: int
    default: 500
    description: Records per page (arrives as a real JSON number, not a string)

pipeline:
  source:
    type: csv
    config: { path: ./data/orders.csv, batch_size: "${param.batch_size}" }
  transforms:
    - type: filter
      config: { path: country_code, op: eq, value: "${param.country}" }
    - type: set
      config: { values: { export_label: "${param.label}" } }
  sink:
    type: jsonl
    config: { path: "./out/orders-${param.country}.jsonl" }
YAML

# --- 22. Scheduler config (validated at compile time; not run here) ---------
cat > 22_scheduled.yaml <<'YAML'
version: 1
name: scheduled_demo
pipeline:
  source: { type: csv, config: { path: ./data/orders.csv } }
  sink: { type: jsonl, config: { path: ./out/sched.jsonl } }
  state: { type: file, config: { path: ./state } }
schedule:
  cron: "0 2 * * *"
  timezone: UTC
YAML

info "Configs written."

# Pre-create the SQLite data tables (the sqlite sink inserts into an existing
# table). Guarded on the sqlite3 CLI being available.
HAVE_SQLITE3=0
if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 out/orders.db 'CREATE TABLE IF NOT EXISTS orders (order_id TEXT, status TEXT, customer_email TEXT, amount TEXT, country_code TEXT);'
  # Drift target deliberately OMITS country_code so `on_drift: evolve` adds it.
  sqlite3 out/drift.db 'DROP TABLE IF EXISTS orders_drift; CREATE TABLE orders_drift (order_id TEXT, status TEXT, customer_email TEXT, amount TEXT);'
  HAVE_SQLITE3=1 && info "SQLite tables created (orders, orders_drift)."
else
  info "sqlite3 CLI not found — the SQLite round-trip and drift-evolve steps will be skipped."
fi

# ============================================================================
# EXERCISE THE CLI  (skipped entirely in --serve-only mode)
# ============================================================================
if [ "$SERVE_ONLY" -eq 0 ]; then

hdr "Introspection commands"
step "version"                "$FAUCET" --version
step "list connectors"        "$FAUCET" list
step "schema source csv"      "$FAUCET" schema source csv
step "schema sink jsonl"      "$FAUCET" schema sink jsonl
step "schema sla"             "$FAUCET" schema sla
step "schema contract"        "$FAUCET" schema contract
step "schema masking"         "$FAUCET" schema masking
[ "$HAVE_TRIGGERS" -eq 1 ] && step "schema triggers"  "$FAUCET" schema triggers
step "schema catalog"         "$FAUCET" schema catalog
step "schema test"            "$FAUCET" schema test
step "init scaffold"          "$FAUCET" init --source csv --sink jsonl --output scaffold_demo.yaml --force scaffold_demo

hdr "Validate every generated config"
VALIDATE_CFGS="01_basic 02_transforms 03_quality 04_contract 05_masking \
           07a_to_sqlite 07b_from_sqlite 08a_to_parquet 08b_from_parquet \
           09_sla 10_lineage 11_catalog 13_serve_run \
           14_matrix 15_depends 16_transforms2 17_drift 18_contract_fail \
           19_quality_abort"
[ "$HAVE_SQL" -eq 1 ]      && VALIDATE_CFGS="$VALIDATE_CFGS 06_sql"
[ "$HAVE_SCHEDULE" -eq 1 ] && VALIDATE_CFGS="$VALIDATE_CFGS 22_scheduled"
for cfg in $VALIDATE_CFGS; do
  step "validate ${cfg}" "$FAUCET" validate "${cfg}.yaml"
done
step "validate 23_templated (params bind to placeholders)" "$FAUCET" validate 23_templated.yaml
step "validate 20_basic.json"  "$FAUCET" validate 20_basic.json
step "validate 21 compose (--show-composed)" "$FAUCET" validate 21_child.yaml --show-composed

hdr "Doctor preflight (probes source/sink/state connectivity)"
step "doctor basic"   "$FAUCET" doctor 01_basic.yaml
step "doctor sla"     "$FAUCET" doctor 09_sla.yaml

hdr "Preview (first page, no writes)"
step "preview basic"  "$FAUCET" preview 01_basic.yaml

hdr "Run the file-based pipelines"
step "run 01 basic"          "$FAUCET" run 01_basic.yaml
step "run 02 transforms"     "$FAUCET" run 02_transforms.yaml
step "run 03 quality (+DLQ)" "$FAUCET" run 03_quality.yaml
step "run 04 contract (+DLQ)" "$FAUCET" run 04_contract.yaml
step "run 05 masking"        "$FAUCET" run 05_masking.yaml
if [ "$HAVE_SQL" -eq 1 ]; then
  step "run 06 sql transform"  "$FAUCET" run 06_sql.yaml
else
  info "Skipping SQL transform run (not compiled in this build)."
fi
if [ "$HAVE_SQLITE3" -eq 1 ]; then
  step "run 07a csv→sqlite"    "$FAUCET" run 07a_to_sqlite.yaml
  step "run 07b sqlite→csv"    "$FAUCET" run 07b_from_sqlite.yaml
else
  info "Skipping SQLite round-trip (no sqlite3 CLI to pre-create the table)."
fi
step "run 08a csv→parquet"   "$FAUCET" run 08a_to_parquet.yaml
step "run 08b parquet→jsonl" "$FAUCET" run 08b_from_parquet.yaml
step "run 09 sla"            "$FAUCET" run 09_sla.yaml
step "run 10 lineage (file)" "$FAUCET" run 10_lineage.yaml
step "run 11 catalog"        "$FAUCET" run 11_catalog.yaml

hdr "Feature-specific inspection commands"
step "contract inspect"          "$FAUCET" contract 04_contract.yaml
step "contract export json-schema" "$FAUCET" contract 04_contract.yaml --export json-schema
step "masking rule breakdown"    "$FAUCET" masking 05_masking.yaml
step "catalog datasets"          "$FAUCET" catalog datasets --config 11_catalog.yaml

hdr "Offline pipeline tests"
step "faucet test"  "$FAUCET" test tests/pipeline_tests.yaml

hdr "DLQ inspect / replay (uses the quarantined contract breaches)"
step "dlq inspect (contract)"     "$FAUCET" dlq inspect ./dlq/contract.jsonl
step "dlq replay (dry-run)"        "$FAUCET" dlq replay 04_contract.yaml --from ./dlq/contract.jsonl --dry-run
step "dlq replay (real, quality)"  "$FAUCET" dlq replay 03_quality.yaml --from ./dlq/quality.jsonl

hdr "Advanced features"
step "run 14 matrix fan-out"        "$FAUCET" run 14_matrix.yaml
step "run 15 depends_on ordering"   "$FAUCET" run 15_depends.yaml
step "run 16 filter+explode+flatten+value_case" "$FAUCET" run 16_transforms2.yaml
if [ "$HAVE_SQLITE3" -eq 1 ]; then
  step "run 17 schema-drift evolve (sqlite ADD COLUMN)" "$FAUCET" run 17_drift.yaml
else
  info "Skipping schema-drift evolve (needs sqlite3 to pre-create the table)."
fi
step_expect_fail "run 18 contract fail aborts"  "$FAUCET" run 18_contract_fail.yaml
step_expect_fail "run 19 quality abort aborts"  "$FAUCET" run 19_quality_abort.yaml
step "run 20 JSON-format config"    "$FAUCET" run 20_basic.json
step "run 21 compose --profile prod" "$FAUCET" run 21_child.yaml --profile prod
step "run --from-env (pure env-var pipeline)" \
  env FAUCET_SOURCE=csv FAUCET_SOURCE_CSV_PATH=./data/orders.csv \
      FAUCET_SINK=jsonl FAUCET_SINK_JSONL_PATH=./out/from_env.jsonl \
      "$FAUCET" run --from-env

hdr "Params & the pipeline template registry"
step "run 23 with --param"  "$FAUCET" run 23_templated.yaml --param country=US --param batch_size=2
if [ "$HAVE_TEMPLATES" -eq 1 ]; then
  # The registry lives in the same SQLite file the web console uses as its
  # --history backend, so everything registered here is triggerable in the UI.
  TPL_STORE="sqlite:./faucet-meta.db"
  step "template register v1 (+ launch → live)" \
    "$FAUCET" template register 23_templated.yaml --store "$TPL_STORE" \
      --description "Per-country orders export" --launch
  step "template register v2 (a nightly — moves nobody)" \
    "$FAUCET" template register 23_templated.yaml --store "$TPL_STORE" --tag dev
  step "template promote staging ← dev" \
    "$FAUCET" template promote orders-by-country --store "$TPL_STORE" --tag staging --version dev
  step "template launch (bless what soaked in staging)" \
    "$FAUCET" template launch orders-by-country --store "$TPL_STORE" --version staging
  step "template promote prod ← stable" \
    "$FAUCET" template promote orders-by-country --store "$TPL_STORE" --tag prod --version stable
  step "template list"  "$FAUCET" template list --store "$TPL_STORE"
  step "template show"  "$FAUCET" template show orders-by-country --store "$TPL_STORE"
  step "template run (stable, by id + params)" \
    "$FAUCET" template run orders-by-country --store "$TPL_STORE" \
      --param country=GB --param label=demo
  step "template rollback"  "$FAUCET" template rollback orders-by-country --store "$TPL_STORE"
  step "template launch (back to newest)" \
    "$FAUCET" template launch orders-by-country --store "$TPL_STORE"
  # Two more so every lifecycle status is represented in `list` / the console.
  step "template register a draft (never launched)" \
    "$FAUCET" template register 23_templated.yaml --store "$TPL_STORE" \
      --id orders-by-country-next --description "Work in progress — not launched yet"
  step "template register + launch, then deprecate" \
    "$FAUCET" template register 23_templated.yaml --store "$TPL_STORE" \
      --id orders-legacy-dump --description "Superseded by orders-by-country" --launch
  step "template deprecate" \
    "$FAUCET" template deprecate orders-legacy-dump --store "$TPL_STORE" \
      --reason "replaced by orders-by-country"
  step "template list (all three statuses)" "$FAUCET" template list --store "$TPL_STORE"
  # The hub kinds (RFC 0008): a source template and a sink template register
  # under their own names and compose at run time — no pre-composed pipeline.
  if [ -f "${REPO_ROOT}/hub/source-templates/faucet-hq/example-csv.yaml" ]; then
    step "template register a source-template (example-csv)" \
      "$FAUCET" template register "${REPO_ROOT}/hub/source-templates/faucet-hq/example-csv.yaml" --store "$TPL_STORE" --launch
    step "template register a sink-template (jsonl)" \
      "$FAUCET" template register "${REPO_ROOT}/hub/sink-templates/faucet-hq/jsonl.yaml" --store "$TPL_STORE" --launch
    step "template register a sink-template (sqlite, left as a draft)" \
      "$FAUCET" template register "${REPO_ROOT}/hub/sink-templates/faucet-hq/sqlite.yaml" --store "$TPL_STORE"
    step "template list --kind sink-template" "$FAUCET" template list --store "$TPL_STORE" --kind sink-template
    step "template run source × sink (composed at run time)" \
      "$FAUCET" template run faucet-hq/example-csv --store "$TPL_STORE" --sink faucet-hq/jsonl \
        --param data_dir="${REPO_ROOT}/hub/examples/data" --param out_dir=./out/hub
  fi
else
  info "Skipping the template registry (built without the \`templates\` feature)."
fi

# ----------------------------------------------------------------------------
# Output inspection
# ----------------------------------------------------------------------------
hdr "Generated outputs"
find "${DEMO_DIR}/out" "${DEMO_DIR}/dlq" "${DEMO_DIR}/catalog" -type f 2>/dev/null | sort | while read -r f; do
  printf "  %-45s %8s bytes\n" "${f#$DEMO_DIR/}" "$(wc -c < "$f" | tr -d ' ')"
done

echo
info "Sample — masked customers (out/masked.jsonl):"
head -n 2 "${DEMO_DIR}/out/masked.jsonl" 2>/dev/null | sed 's/^/    /'
if [ "$HAVE_SQL" -eq 1 ]; then
  info "Sample — SQL aggregation (out/sql_agg.jsonl):"
  sed 's/^/    /' "${DEMO_DIR}/out/sql_agg.jsonl" 2>/dev/null
fi
info "Sample — quality DLQ (dlq/quality.jsonl):"
head -n 2 "${DEMO_DIR}/dlq/quality.jsonl" 2>/dev/null | sed 's/^/    /'

# ----------------------------------------------------------------------------
# Summary
# ----------------------------------------------------------------------------
echo
echo "${BOLD}================ SUMMARY ================${RESET}"
echo "${GREEN}PASS: ${PASS}${RESET}   ${RED}FAIL: ${FAIL}${RESET}"
if [ "$FAIL" -gt 0 ]; then
  echo "${RED}Failed steps:${RESET}"
  for s in "${FAILED_STEPS[@]}"; do echo "  - $s"; done
fi
echo "Demo workspace: ${DEMO_DIR}  (delete any time: rm -rf '${DEMO_DIR}')"
echo "${BOLD}========================================${RESET}"

fi  # end of --serve-only guard (the CLI battery)

# ----------------------------------------------------------------------------
# Web console — default: keep it running so you can browse Runs / Datasets /
# Lineage. --no-serve exits after a quick headless smoke test instead.
# ----------------------------------------------------------------------------
if [ "$DO_SERVE" -eq 1 ]; then
  launch_ui                       # submits demo runs, then BLOCKS until Ctrl+C
else
  hdr "Serve headless smoke test (--no-serve; port ${SERVE_PORT})"
  if command -v curl >/dev/null 2>&1; then
    ( cd "$DEMO_DIR" && exec "$FAUCET" serve --listen "127.0.0.1:${SERVE_PORT}" --no-auth ) \
      >/tmp/faucet-serve.log 2>&1 &
    SERVE_PID=$!
    for _ in $(seq 1 30); do curl -sf "http://127.0.0.1:${SERVE_PORT}/healthz" >/dev/null 2>&1 && break; sleep 0.5; done
    step "serve /healthz"    curl -sf "http://127.0.0.1:${SERVE_PORT}/healthz"
    step "serve /readyz"     curl -sf "http://127.0.0.1:${SERVE_PORT}/readyz"
    step "serve /metrics"    curl -sf "http://127.0.0.1:${SERVE_PORT}/metrics" -o /dev/null
    step "serve /v1/schemas" curl -sf "http://127.0.0.1:${SERVE_PORT}/v1/schemas" -o /dev/null
    stop_serve; wait "$SERVE_PID" 2>/dev/null
  else
    info "curl not found — skipping serve smoke test"
  fi
  echo
  echo "${BOLD}Battery finished (--no-serve). PASS: ${PASS}  FAIL: ${FAIL}${RESET}"
  echo "To browse the UI later:  ./scripts/try-local.sh --serve-only"
  [ "$FAIL" -eq 0 ]
fi

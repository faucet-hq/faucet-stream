#!/usr/bin/env bash
# Reproducible faucet-stream vs Meltano (Singer) benchmark harness.
#
# Scenario A (always): CSV -> JSONL   (faucet source-csv/sink-jsonl vs tap-csv/target-jsonl)
# Scenario B (--postgres, needs Docker): Postgres -> JSONL
#
# Measures wall-clock (hyperfine, median + stddev over N runs, 1 warmup discarded),
# peak RSS (/usr/bin/time), throughput (rows/median), and output parity (row counts).
# Writes machine-readable raw output + a results markdown fragment under
# benchmarks/results/. NEVER fabricates numbers — a tool that fails to install or
# run is recorded as such and left out of the table.
#
# Usage:
#   scripts/run-bench.sh                 # 1M rows, 5 runs, CSV scenario
#   scripts/run-bench.sh --smoke         # 100k rows (fast validation)
#   scripts/run-bench.sh --rows 500000 --runs 7
#   scripts/run-bench.sh --postgres      # also run Postgres scenario (needs Docker)
set -uo pipefail

# ---- config / args --------------------------------------------------------
ROWS=1000000
SEED=42
RUNS=5
DO_PG=0
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Newest interpreter Meltano supports, unless BENCH_PYTHON picks one.
SUPPORTED_PYTHONS="3.14 3.13 3.12 3.11 3.10"
PYTHON="${BENCH_PYTHON:-}"
if [ -z "$PYTHON" ]; then
  for v in $SUPPORTED_PYTHONS; do
    if command -v "python$v" >/dev/null 2>&1; then PYTHON="python$v"; break; fi
  done
  PYTHON="${PYTHON:-python3}"
fi
MELTANO_VERSION="${BENCH_MELTANO_VERSION:-4.4.1}"
FAUCET_BIN="${FAUCET_BIN:-$REPO_ROOT/target/release-lto/faucet}"

while [ $# -gt 0 ]; do
  case "$1" in
    --smoke) ROWS=100000; shift ;;
    --rows) ROWS="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --runs) RUNS="$2"; shift 2 ;;
    --postgres) DO_PG=1; shift ;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

cd "$REPO_ROOT"
DATA_DIR="benchmarks/data"
OUT_DIR="benchmarks/out"
RES_DIR="benchmarks/results"
RAW_DIR="$RES_DIR/raw"
mkdir -p "$DATA_DIR" "$OUT_DIR" "$RAW_DIR"
DATA_CSV="$DATA_DIR/bench.csv"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
have() { command -v "$1" >/dev/null 2>&1; }

# Portable peak-RSS-in-MiB for a command. Uses GNU `time -v` on Linux,
# BSD `time -l` on macOS. Echoes the MiB value (or "NA").
peak_rss_mib() {
  local logf; logf="$(mktemp)"
  if /usr/bin/time -v true >/dev/null 2>&1; then          # GNU time
    /usr/bin/time -v "$@" >/dev/null 2>"$logf"
    awk '/Maximum resident set size/ {print $6/1024}' "$logf"
  else                                                     # BSD time (macOS): bytes
    /usr/bin/time -l "$@" >/dev/null 2>"$logf"
    awk '/maximum resident set size/ {print $1/1048576}' "$logf"
  fi
  rm -f "$logf"
}

count_lines() { [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0; }

# ---- environment capture --------------------------------------------------
log "Capturing environment -> $RES_DIR/versions.txt"
{
  echo "# Benchmark environment (generated $(date -u +%Y-%m-%dT%H:%M:%SZ))"
  echo "rows=$ROWS seed=$SEED runs=$RUNS"
  echo
  if [ "$(uname)" = "Darwin" ]; then
    echo "os: macOS $(sw_vers -productVersion 2>/dev/null) ($(uname -m))"
    echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null)"
    echo "cores: $(sysctl -n hw.ncpu 2>/dev/null)"
    echo "ram_bytes: $(sysctl -n hw.memsize 2>/dev/null)"
  else
    echo "os: $(uname -sr)"
    echo "cpu: $(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2 | xargs)"
    echo "cores: $(nproc 2>/dev/null)"
    echo "ram_kb: $(grep MemTotal /proc/meminfo 2>/dev/null | awk '{print $2}')"
  fi
  echo
  echo "rustc: $(rustc --version 2>/dev/null || echo NA)"
  echo "faucet: $("$FAUCET_BIN" --version 2>/dev/null || echo 'NOT BUILT')"
  echo "hyperfine: $(hyperfine --version 2>/dev/null || echo NA)"
  echo "python(meltano): $($PYTHON --version 2>&1 || echo NA)"
} > "$RES_DIR/versions.txt"
cat "$RES_DIR/versions.txt"

# ---- dataset --------------------------------------------------------------
log "Generating dataset ($ROWS rows, seed $SEED)"
"$PYTHON" scripts/gen_bench_data.py --rows "$ROWS" --seed "$SEED" --out "$DATA_CSV" \
  || { echo "data gen failed" >&2; exit 1; }
CSV_BYTES=$(wc -c < "$DATA_CSV" | tr -d ' ')
echo "dataset_bytes=$CSV_BYTES" >> "$RES_DIR/versions.txt"

# ---- Meltano venv ---------------------------------------------------------
VENV="$REPO_ROOT/.bench-venv"
MELTANO_OK=0
log "Setting up Meltano venv ($PYTHON) at .bench-venv"
if have "$PYTHON"; then
  py_version() { "$1" -c 'import sys; print("%d.%d" % sys.version_info[:2])' 2>/dev/null; }
  if [ ! -x "$VENV/bin/meltano" ] \
      || ! "$VENV/bin/pip" show meltano 2>/dev/null | grep -qx "Version: $MELTANO_VERSION" \
      || [ "$(py_version "$VENV/bin/python")" != "$(py_version "$PYTHON")" ]; then
    rm -rf benchmarks/meltano/.meltano/extractors benchmarks/meltano/.meltano/loaders
    "$PYTHON" -m venv --clear "$VENV" \
      && "$VENV/bin/pip" -q install --upgrade pip \
      && "$VENV/bin/pip" -q install "meltano==$MELTANO_VERSION" \
      || echo "meltano pip install failed (will skip Meltano side)" >&2
  fi
  if [ -x "$VENV/bin/meltano" ]; then
    MELTANO_OK=1
    echo "meltano: $("$VENV/bin/meltano" --version 2>&1 | head -1)" >> "$RES_DIR/versions.txt"
    ( cd benchmarks/meltano && "$VENV/bin/meltano" install extractor tap-csv >/dev/null 2>&1 \
        && "$VENV/bin/meltano" install loader target-jsonl >/dev/null 2>&1 ) \
      || { echo "meltano plugin install failed (will skip Meltano side)" >&2; MELTANO_OK=0; }
    [ "$MELTANO_OK" = 1 ] && "$VENV/bin/pip" freeze >> "$RES_DIR/versions.txt"
  fi
else
  echo "$PYTHON not found; skipping Meltano side" >&2
fi

# ---- Scenario A: CSV -> JSONL --------------------------------------------
log "Scenario A: CSV -> JSONL"
FAUCET_OUT="$OUT_DIR/faucet_out.jsonl"
MELTANO_OUT_DIR="benchmarks/meltano/output"

# faucet timed run
hyperfine --warmup 1 --runs "$RUNS" --export-json "$RAW_DIR/faucet_csv.json" \
  --command-name "faucet csv->jsonl" \
  "'$FAUCET_BIN' run benchmarks/faucet/csv_to_jsonl.yaml" \
  || echo "faucet hyperfine run failed" >&2
FAUCET_ROWS=$(count_lines "$FAUCET_OUT")
log "faucet peak RSS sample"
FAUCET_RSS=$(peak_rss_mib "$FAUCET_BIN" run benchmarks/faucet/csv_to_jsonl.yaml)

# meltano timed run
MELTANO_ROWS=0; MELTANO_RSS="NA"
if [ "$MELTANO_OK" = 1 ]; then
  hyperfine --warmup 1 --runs "$RUNS" --export-json "$RAW_DIR/meltano_csv.json" \
    --command-name "meltano tap-csv->target-jsonl" \
    --prepare "rm -rf '$MELTANO_OUT_DIR'" \
    "cd benchmarks/meltano && '$VENV/bin/meltano' run tap-csv target-jsonl" \
    || echo "meltano hyperfine run failed" >&2
  MELTANO_ROWS=$(cat "$MELTANO_OUT_DIR"/*.jsonl 2>/dev/null | wc -l | tr -d ' ')
  log "meltano peak RSS sample"
  rm -rf "$MELTANO_OUT_DIR"
  MELTANO_RSS=$(cd benchmarks/meltano && peak_rss_mib "$VENV/bin/meltano" run tap-csv target-jsonl)
fi

# ---- results --------------------------------------------------------------
# Pull median + stddev out of the hyperfine JSON with python.
read_stat() { # $1 json, $2 key
  [ -f "$1" ] && "$PYTHON" -c "import json,sys; r=json.load(open('$1'))['results'][0]; print(round(r.get('$2') or 0,4))" 2>/dev/null || echo NA
}
F_MED=$(read_stat "$RAW_DIR/faucet_csv.json" median)
F_STD=$(read_stat "$RAW_DIR/faucet_csv.json" stddev)
M_MED=$(read_stat "$RAW_DIR/meltano_csv.json" median)
M_STD=$(read_stat "$RAW_DIR/meltano_csv.json" stddev)
thr() { "$PYTHON" -c "print(f'{$1/$2:,.0f}') if '$2' not in ('NA','0','0.0') else print('NA')" 2>/dev/null || echo NA; }
F_THR=$(thr "$ROWS" "$F_MED")
M_THR=$(thr "$ROWS" "$M_MED")

log "Writing results fragment -> $RES_DIR/results.md"
{
  echo "<!-- generated by scripts/run-bench.sh — do not hand-edit -->"
  echo "### Scenario A — CSV → JSONL ($ROWS rows, $RUNS runs, seed $SEED)"
  echo
  echo "| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |"
  echo "|---|---|---|---|---|---|"
  echo "| faucet-stream | $F_MED | $F_STD | $F_THR | $FAUCET_RSS | $FAUCET_ROWS |"
  if [ "$MELTANO_OK" = 1 ]; then
    echo "| Meltano (Singer) | $M_MED | $M_STD | $M_THR | $MELTANO_RSS | $MELTANO_ROWS |"
  else
    echo "| Meltano (Singer) | TODO: run locally | | | | (install failed / skipped) |"
  fi
  echo
  echo "- Input CSV: $CSV_BYTES bytes ($(( CSV_BYTES / 1048576 )) MiB)."
  echo "- Parity: faucet emitted **$FAUCET_ROWS** rows; Meltano emitted **$MELTANO_ROWS** rows."
  [ "$MELTANO_OK" = 1 ] && [ "$FAUCET_ROWS" != "$MELTANO_ROWS" ] && \
    echo "  - NOTE: row counts differ — inspect record shapes (Singer may add \`_sdc_*\` metadata columns)."
} > "$RES_DIR/results.md"
cat "$RES_DIR/results.md"

# ---- Scenarios B & C (optional, Docker) -----------------------------------
# B: Postgres -> JSONL      (read/decode-bound: destination is a fast local file)
# C: Postgres -> Postgres   (sink-bound: destination is a real DB write)
#
# Scenario C is the honest "move data between two databases on one machine"
# shape — both tools become bounded by the same Postgres write path rather than
# by per-row interpreter overhead, so the gap narrows vs the CSV->JSONL best case.
PG_CONTAINER="faucet-bench-pg"
PG_IMAGE="postgres:16"
PG_DSN_PSQL="postgresql://postgres:postgres@127.0.0.1:55432/postgres"

pg_psql() { docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d postgres "$@"; }

run_pg_scenarios() {
  if ! have docker || ! docker info >/dev/null 2>&1; then
    echo "Docker not available — Scenarios B & C skipped. See BENCHMARKS.md." >&2
    return 0
  fi
  log "Starting Postgres ($PG_IMAGE) for Scenarios B & C"
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
  docker run -d --name "$PG_CONTAINER" -e POSTGRES_PASSWORD=postgres \
    -p 55432:5432 "$PG_IMAGE" >/dev/null || { echo "postgres container failed to start" >&2; return 1; }
  # Wait for readiness.
  for _ in $(seq 1 30); do
    docker exec "$PG_CONTAINER" pg_isready -U postgres >/dev/null 2>&1 && break
    sleep 1
  done

  log "Loading $DATA_CSV into Postgres (bench) + creating bench_dest"
  pg_psql <<'SQL'
DROP TABLE IF EXISTS bench, bench_dest;
CREATE TABLE bench (id bigint, first_name text, country text, amount double precision,
                    created_at timestamptz, active boolean, attributes jsonb);
-- bench_dest mirrors bench but keeps created_at as text so faucet AutoMap can
-- write the source's RFC3339 string without a per-value cast.
CREATE TABLE bench_dest (id bigint, first_name text, country text, amount double precision,
                         created_at text, active boolean, attributes jsonb);
SQL
  docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d postgres \
    -c "\copy bench FROM STDIN WITH (FORMAT csv, HEADER true)" < "$DATA_CSV" \
    || { echo "COPY into bench failed" >&2; docker rm -f "$PG_CONTAINER" >/dev/null 2>&1; return 1; }

  export BENCH_PG_URL="postgresql://postgres:postgres@127.0.0.1:55432/postgres"
  export TAP_POSTGRES_SQLALCHEMY_URL="$BENCH_PG_URL"
  export TARGET_POSTGRES_SQLALCHEMY_URL="$BENCH_PG_URL"
  local pg_meltano_ok=0
  if [ "$MELTANO_OK" = 1 ]; then
    ( cd benchmarks/meltano \
        && "$VENV/bin/meltano" install extractor tap-postgres >/dev/null 2>&1 \
        && "$VENV/bin/meltano" install loader target-jsonl >/dev/null 2>&1 \
        && "$VENV/bin/meltano" install loader target-postgres >/dev/null 2>&1 ) \
      && pg_meltano_ok=1 || echo "meltano postgres plugins failed to install" >&2
  fi

  # Row count for a Postgres table (0 on error).
  pg_count() {
    docker exec "$PG_CONTAINER" psql -tA -U postgres -d postgres \
      -c "SELECT count(*) FROM $1" 2>/dev/null | tr -d ' ' || echo 0
  }

  # Scenario B: Postgres -> JSONL. Timed runs, then one sampled run for peak RSS
  # (same methodology as Scenario A) + a row-count parity check.
  log "Scenario B: Postgres -> JSONL"
  hyperfine --warmup 1 --runs "$RUNS" --export-json "$RAW_DIR/faucet_pg_jsonl.json" \
    --command-name "faucet pg->jsonl" \
    "'$FAUCET_BIN' run benchmarks/faucet/postgres_to_jsonl.yaml" \
    || echo "faucet pg->jsonl run failed" >&2
  FB_RSS=$(peak_rss_mib "$FAUCET_BIN" run benchmarks/faucet/postgres_to_jsonl.yaml)
  FB_ROWS=$(count_lines "$OUT_DIR/faucet_pg_out.jsonl")
  MB_RSS="NA"; MB_ROWS=0
  if [ "$pg_meltano_ok" = 1 ]; then
    hyperfine --warmup 1 --runs "$RUNS" --export-json "$RAW_DIR/meltano_pg_jsonl.json" \
      --command-name "meltano tap-postgres->target-jsonl" \
      --prepare "rm -rf benchmarks/meltano/output" \
      "cd benchmarks/meltano && '$VENV/bin/meltano' run tap-postgres target-jsonl" \
      || echo "meltano pg->jsonl run failed" >&2
    rm -rf benchmarks/meltano/output
    MB_RSS=$(cd benchmarks/meltano && peak_rss_mib "$VENV/bin/meltano" run tap-postgres target-jsonl)
    MB_ROWS=$(cat benchmarks/meltano/output/*.jsonl 2>/dev/null | wc -l | tr -d ' ')
  fi

  # Scenario C: Postgres -> Postgres (sink-bound). Same shape as B.
  log "Scenario C: Postgres -> Postgres (sink-bound)"
  hyperfine --warmup 1 --runs "$RUNS" --export-json "$RAW_DIR/faucet_pg_pg.json" \
    --command-name "faucet pg->pg" \
    --prepare "docker exec $PG_CONTAINER psql -U postgres -d postgres -c 'TRUNCATE bench_dest'" \
    "'$FAUCET_BIN' run benchmarks/faucet/postgres_to_postgres.yaml" \
    || echo "faucet pg->pg run failed" >&2
  docker exec "$PG_CONTAINER" psql -U postgres -d postgres -c 'TRUNCATE bench_dest' >/dev/null 2>&1
  FC_RSS=$(peak_rss_mib "$FAUCET_BIN" run benchmarks/faucet/postgres_to_postgres.yaml)
  FC_ROWS=$(pg_count bench_dest)
  MC_RSS="NA"; MC_ROWS=0
  if [ "$pg_meltano_ok" = 1 ]; then
    hyperfine --warmup 1 --runs "$RUNS" --export-json "$RAW_DIR/meltano_pg_pg.json" \
      --command-name "meltano tap-postgres->target-postgres" \
      --prepare "docker exec $PG_CONTAINER psql -U postgres -d postgres -c 'DROP SCHEMA IF EXISTS bench_meltano CASCADE'" \
      "cd benchmarks/meltano && '$VENV/bin/meltano' run tap-postgres target-postgres" \
      || echo "meltano pg->pg run failed" >&2
    docker exec "$PG_CONTAINER" psql -U postgres -d postgres -c 'DROP SCHEMA IF EXISTS bench_meltano CASCADE' >/dev/null 2>&1
    MC_RSS=$(cd benchmarks/meltano && peak_rss_mib "$VENV/bin/meltano" run tap-postgres target-postgres)
    MC_ROWS=$(pg_count bench_meltano.bench)
  fi

  # Append a results fragment for the Postgres scenarios (same columns as A:
  # median, stddev, throughput, peak RSS, rows out).
  local fb_med fb_std fc_med fc_std mb_med mb_std mc_med mc_std
  fb_med=$(read_stat "$RAW_DIR/faucet_pg_jsonl.json" median); fb_std=$(read_stat "$RAW_DIR/faucet_pg_jsonl.json" stddev)
  mb_med=$(read_stat "$RAW_DIR/meltano_pg_jsonl.json" median); mb_std=$(read_stat "$RAW_DIR/meltano_pg_jsonl.json" stddev)
  fc_med=$(read_stat "$RAW_DIR/faucet_pg_pg.json" median);     fc_std=$(read_stat "$RAW_DIR/faucet_pg_pg.json" stddev)
  mc_med=$(read_stat "$RAW_DIR/meltano_pg_pg.json" median);    mc_std=$(read_stat "$RAW_DIR/meltano_pg_pg.json" stddev)
  {
    echo
    echo "### Scenario B — Postgres → JSONL ($ROWS rows, $RUNS runs, seed $SEED)"
    echo
    echo "| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |"
    echo "|---|---|---|---|---|---|"
    echo "| faucet-stream | $fb_med | $fb_std | $(thr "$ROWS" "$fb_med") | $FB_RSS | $FB_ROWS |"
    echo "| Meltano (Singer) | $mb_med | $mb_std | $(thr "$ROWS" "$mb_med") | $MB_RSS | $MB_ROWS |"
    echo
    echo "### Scenario C — Postgres → Postgres (sink-bound; $ROWS rows, $RUNS runs, seed $SEED)"
    echo
    echo "| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |"
    echo "|---|---|---|---|---|---|"
    echo "| faucet-stream | $fc_med | $fc_std | $(thr "$ROWS" "$fc_med") | $FC_RSS | $FC_ROWS |"
    echo "| Meltano (Singer) | $mc_med | $mc_std | $(thr "$ROWS" "$mc_med") | $MC_RSS | $MC_ROWS |"
  } >> "$RES_DIR/results.md"

  log "Tearing down Postgres container"
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
}

if [ "$DO_PG" = 1 ]; then
  run_pg_scenarios
fi

if [ "$MELTANO_OK" = 1 ]; then
  for plugin_venv in benchmarks/meltano/.meltano/*/*/venv; do
    [ -x "$plugin_venv/bin/python" ] || continue
    { echo; echo "# plugin $(basename "$(dirname "$plugin_venv")")"
      "$plugin_venv/bin/python" -c 'import importlib.metadata as m; print("\n".join(sorted(d.metadata["Name"] + "==" + d.version for d in m.distributions())))'
    } >> "$RES_DIR/versions.txt"
  done
fi

log "Done. Raw hyperfine JSON in $RAW_DIR/, env in $RES_DIR/versions.txt"

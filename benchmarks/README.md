# faucet-stream benchmarks

Reproducible, honest performance comparison of `faucet-stream` against
[Meltano](https://meltano.com/) (the most common [Singer](https://www.singer.io/)
runtime) on an identical workload. The results and full caveats live in
[`../BENCHMARKS.md`](../BENCHMARKS.md); this directory is the harness.

> **These numbers measure single-machine batch throughput of a CSV→JSONL (and
> optionally Postgres→JSONL) move.** They do not measure distributed throughput,
> connector breadth, or correctness. See the caveats in `BENCHMARKS.md`.

## Layout

```
benchmarks/
  faucet/        faucet pipeline YAMLs (csv_to_jsonl, postgres_to_jsonl, postgres_to_postgres)
  meltano/       pinned Meltano project (meltano.yml: tap-csv/tap-postgres -> target-jsonl/target-postgres)
  data/          generated dataset (gitignored)
  out/           faucet output (gitignored)
  results/       generated results: versions.txt, results.md, raw/*.json (gitignored)
../scripts/
  gen_bench_data.py   seeded deterministic dataset generator
  run-bench.sh        end-to-end orchestrator
```

## Prerequisites

- A release `faucet` binary with the CSV/JSONL and Postgres source+sink features:
  ```bash
  cargo build -p faucet-cli --release \
    --no-default-features --features "source-file,sink-file,file-format-csv,source-postgres,sink-postgres"
  ```
- [`hyperfine`](https://github.com/sharkdp/hyperfine) for wall-clock timing.
- A Meltano-compatible Python (3.9–3.12; the harness defaults to `python3.12`,
  override with `BENCH_PYTHON`). Meltano does **not** yet support Python 3.13/3.14,
  so a system whose only `python3` is 3.13+ must install 3.12.
- Docker (only for the optional Postgres scenario).

## Run it

```bash
# Fast validation (100k rows):
scripts/run-bench.sh --smoke

# The headline run (1,000,000 rows, 5 timed runs + 1 discarded warmup):
scripts/run-bench.sh

# Tweak:
scripts/run-bench.sh --rows 500000 --runs 7 --seed 42

# Scenarios B (Postgres -> JSONL) and C (Postgres -> Postgres, sink-bound) — needs Docker:
scripts/run-bench.sh --postgres
```

Scenario C spins Postgres 16 in Docker, `COPY`-loads the seeded `bench` table,
then times faucet `postgres_to_postgres.yaml` against Meltano
`tap-postgres → target-postgres`. It's the sink-bound "move a table between two
databases" shape, where the destination write — not per-row parsing — dominates,
so it is the fairest single number for a DB→DB move.

Or via make:

```bash
make bench          # 1M rows
make bench-smoke    # 100k rows
```

Outputs land in `benchmarks/results/`:
- `versions.txt` — hardware, OS, Rust/faucet/Meltano versions, `pip freeze`, dataset size.
- `results.md` — the results table fragment (copied into `BENCHMARKS.md`).
- `raw/*.json` — the raw hyperfine exports (median/mean/stddev/min/max per run set).

The harness **degrades gracefully**: if Meltano fails to install (e.g. no
compatible Python), the faucet numbers are still produced and Meltano's row is
marked `TODO: run locally` rather than faked.

## Scenario B — Postgres → JSONL (manual, needs Docker)

Not wired into `run-bench.sh`'s automated path (this dev box has no Docker). To
run it by hand:

```bash
# 1. Start Postgres
docker run -d --name faucet-bench-pg -e POSTGRES_PASSWORD=bench \
  -p 55432:5432 postgres:16

export BENCH_PG_URL="postgres://postgres:bench@localhost:55432/postgres"

# 2. Generate + load the dataset
python3.12 scripts/gen_bench_data.py --rows 1000000 --seed 42 --out benchmarks/data/bench.csv
psql "$BENCH_PG_URL" -c "CREATE TABLE bench (
  id bigint, first_name text, country text, amount double precision,
  created_at timestamptz, active boolean, attributes jsonb);"
psql "$BENCH_PG_URL" -c "\copy bench FROM 'benchmarks/data/bench.csv' WITH (FORMAT csv, HEADER true);"

# 3. faucet
hyperfine --warmup 1 --runs 5 \
  "target/release/faucet run benchmarks/faucet/postgres_to_jsonl.yaml"

# 4. Meltano
export TAP_POSTGRES_SQLALCHEMY_URL="postgresql://postgres:bench@localhost:55432/postgres"
cd benchmarks/meltano
hyperfine --warmup 1 --runs 5 --prepare 'rm -rf output' \
  "../../.bench-venv/bin/meltano run tap-postgres target-jsonl"

# 5. Tear down
docker rm -f faucet-bench-pg
```

Record the medians into `BENCHMARKS.md`'s Scenario B table.

## Determinism

The dataset is fully determined by `--seed` and `--rows` (a seeded `random.Random`),
so both tools consume byte-identical input and re-runs are comparable. The generator
prints the on-disk size; the harness records it in `versions.txt`.

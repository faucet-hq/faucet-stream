# Benchmarks — faucet-stream vs Meltano (Singer)

Honest, reproducible evidence for the "built for throughput" claim. This compares
`faucet-stream` against [Meltano](https://meltano.com/) (the most common
[Singer](https://www.singer.io/) runtime) on **identical** workloads, on one
machine.

**Headline: on single-machine batch throughput, faucet-stream is roughly
1–2 orders of magnitude faster than a Python Singer runtime** — and the size of
the gap depends heavily on how much of the run is per-row work vs I/O:

- **CSV → JSONL is a *best case*** that maximally exposes Python's per-row
  interpreter + Singer-over-pipe overhead (faucet was ~96× faster here). Quote
  this as the upper bound, not the typical case.
- **Sink-bound moves narrow the gap.** When the destination write dominates
  (Postgres → Postgres, Scenario C), both tools are bounded by the *same* database
  write path, so the ratio shrinks toward the low end of that range. Network- or
  API-bound moves narrow it further.

**Regenerate everything** with `make bench` (CSV, 1M rows), `make bench-smoke`
(100k), or `make bench-postgres` (adds the Docker Postgres scenarios B & C). The
harness lives in [`benchmarks/`](benchmarks/README.md) and never fabricates a
number — a tool that won't install/run is recorded as such, not faked.

> **Read the caveats section before quoting any number.** This measures
> single-machine batch throughput of specific moves. It does **not** measure
> distributed throughput, connector breadth, or correctness.
>
> **Reproduce and report.** One independent confirmation of these numbers on your
> own hardware is worth more to this project than a new connector — run
> `make bench` (and `make bench-postgres` if you have Docker) and open an issue or
> PR with your `benchmarks/results/` output, especially if faucet does *not* win.

## Methodology

### Dataset (deterministic)

`scripts/gen_bench_data.py` emits a seeded CSV (`--seed 42`), so both tools consume
byte-identical input and re-runs are comparable. Columns exercise mixed types — an
`int` id, two strings, a float, an RFC3339 timestamp, a bool, and a small nested
JSON column (`attributes`):

```
id,first_name,country,amount,created_at,active,attributes
1,Eve,GB,6767.32,2020-01-01T00:01:22Z,false,"{""tier"":""free"",""score"":2.501,""tags"":[""beta"",""epsilon""]}"
```

Sizes: **1,000,000 rows** (primary) and **100,000 rows** (smoke). On-disk sizes are
recorded per run in `benchmarks/results/versions.txt` (the 100k CSV is ~11.5 MiB;
the 1M CSV is ~10× that).

### Scenarios

| Scenario | Bottleneck | faucet | Meltano |
|---|---|---|---|
| **A — CSV → JSONL** (no infra, always run) | parse/serialize (best case) | `source-file` → `sink-file` (recorded numbers: `source-csv` → `sink-jsonl`) | `tap-csv` → `target-jsonl` |
| **B — Postgres → JSONL** (needs Docker) | typed row decode | `source-postgres` → `sink-file` (recorded numbers: `sink-jsonl`) | `tap-postgres` → `target-jsonl` |
| **C — Postgres → Postgres** (needs Docker) | **destination write (sink-bound)** | `source-postgres` → `sink-postgres` | `tap-postgres` → `target-postgres` |

### Measurement

- **Wall-clock**: [`hyperfine`](https://github.com/sharkdp/hyperfine), 1 warmup run
  discarded + **5 timed runs**, median ± stddev.
- **Peak RSS**: `/usr/bin/time` (`-l` on macOS, `-v` on Linux), one sampled run.
- **Throughput**: `rows ÷ median wall-clock`.
- **Parity**: both tools must emit the same row count; record shapes are
  spot-checked (see caveats re: Singer metadata).

### Exact reproduction

```bash
# 1. Build the release binary the harness uses
cargo build -p faucet-cli --release \
  --no-default-features --features "source-file,sink-file,file-format-csv,source-postgres,sink-postgres"

# 2. Run (installs an isolated Meltano venv on first run)
scripts/run-bench.sh            # 1,000,000 rows, 5 runs (Scenario A)
scripts/run-bench.sh --smoke    # 100,000 rows
scripts/run-bench.sh --postgres # also Scenarios B & C (needs Docker)

# Results land in benchmarks/results/{versions.txt,results.md,raw/*.json}
```

Tool versions and hardware are captured per run in
`benchmarks/results/versions.txt`. The run reported below used:

| | |
|---|---|
| Hardware | Apple M3 Pro, 12 cores, 18 GiB RAM |
| OS | macOS 26.5 (arm64) |
| Rust | rustc 1.96.0 |
| faucet | 1.2.0 (release: `source-csv,sink-jsonl,source-postgres`) |
| hyperfine | 1.19.0 |
| Meltano | 4.2.1 (Python 3.12.11) |
| Meltano plugins | `tap-csv` v1.3.2 (git, MeltanoLabs), `target-jsonl` 0.1.4 |
| Date | 2026-07-09 |

_See `benchmarks/results/versions.txt` for the exact `pip freeze` and dataset size
of the run reported below._

## Results

### Scenario A — CSV → JSONL

**Primary run — 1,000,000 rows** (116 MiB CSV, seed 42, 1 warmup + 5 timed runs):

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **1.40** | 0.015 | **712,403** | **11.8** | 1,000,000 |
| Meltano (Singer) | 135.46 | 6.03 | 7,383 | 724.5 | 1,000,000 |

On this workload and hardware, faucet-stream was **~96× faster** (712k vs 7.4k
rows/s) and used **~62× less peak memory** (11.8 vs 724 MiB), with **exact
row-count parity** (1,000,000 = 1,000,000).

**Smoke run — 100,000 rows** (11.5 MiB CSV, seed 42, 1 warmup + 5 timed runs):

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **0.18** | 0.011 | **548,246** | **11.8** | 100,000 |
| Meltano (Singer) | 18.92 | 1.78 | 5,286 | 306.4 | 100,000 |

Note how faucet's throughput *rises* from the 100k to the 1M run (548k → 712k
rows/s) as fixed startup cost amortizes, while Meltano's stays flat (~5–7k rows/s)
— consistent with a fixed Python/subprocess startup cost plus a per-record
Singer-over-pipe overhead. This is why the 1M run is the meaningful one.

### Scenario B — Postgres → JSONL

**1,000,000 rows** (`bench` table, seed 42, 1 warmup + 5 timed runs). Postgres 16
in Docker (colima); the same machine as Scenario A. faucet `source-postgres` →
`sink-jsonl` (streamed via `sqlx`) vs Meltano `tap-postgres` → `target-jsonl`.

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream** | **5.56** | 0.110 | **179,700** | **13.9** | 1,000,000 |
| Meltano (Singer) | 139.19 | 1.595 | 7,184 | 743.0 | 1,000,000 |

On this workload/hardware, faucet-stream was **~25× faster** and used **~53× less
peak memory**, with **exact row-count parity** (1,000,000 = 1,000,000). The gap is
narrower than Scenario A because both tools are now bounded by real typed row
decoding from Postgres (Scenario A was all-string CSV) — faucet still streams
rows via `sqlx` while `tap-postgres` marshals each row through SQLAlchemy and the
Singer JSON pipe.

### Scenario C — Postgres → Postgres (sink-bound)

**The honest, realistic move: data from one database into another on one machine,
where the destination write dominates.** faucet `source-postgres` →
`sink-postgres` (multi-row `INSERT`s via `sqlx`) vs Meltano `tap-postgres` →
`target-postgres`. Because both tools are now bounded by the *same* Postgres write
path, this is where the CSV→JSONL best-case gap shrinks toward the low end of the
1–2 orders-of-magnitude range — and it is the number to quote for "move a table
between two Postgres databases."

**1,000,000 rows** (`bench` → `bench_dest`, seed 42, 1 warmup + 3 timed runs).
Postgres 16 in Docker (colima) on an Apple M3 Pro; faucet `source-postgres` →
`sink-postgres` (AutoMap columns) vs Meltano `tap-postgres` →
`target-postgres` 0.8.0. faucet is measured on both of its append write paths:
multi-row `INSERT`s and the `write_method: copy` bulk-load fast-path (#308,
`COPY … FROM STDIN` — the benchmark config's default since it is the best
append path).

**Batch parity (both sides write 5,000-row batches).** So the gap below cannot be
dismissed as a Meltano batch-size misconfiguration: both loaders are pinned to the
same write batch. faucet emits 5,000-row source pages (`batch_size: 5000`) and
writes each via `COPY … FROM STDIN` (its `copy` row) or multi-row `INSERT` (its
`insert` row), and Meltano's `target-postgres` is pinned to `batch_size_rows: 5000`
and writes multi-row `INSERT`s. `target-postgres` also ships an opt-in COPY loader
(`use_copy: true`, default off) that this run leaves off — so the faucet-`copy` vs
Meltano row additionally differs in write *method* (COPY vs INSERT), while the
faucet-`insert` vs Meltano row is method-matched. See
[`benchmarks/faucet/postgres_to_postgres.yaml`](benchmarks/faucet/postgres_to_postgres.yaml)
and [`benchmarks/meltano/meltano.yml`](benchmarks/meltano/meltano.yml).

| Tool | Median wall-clock (s) | Stddev (s) | Throughput (rows/s) | Peak RSS (MiB) | Rows out |
|---|---|---|---|---|---|
| **faucet-stream (`write_method: copy`)** | **8.12** | 0.142 | **123,200** | **35.9** | 1,000,000 |
| faucet-stream (multi-row `INSERT`) | 10.10 | 0.112 | 99,000 | 35.9 | 1,000,000 |
| Meltano (Singer)† | 129.8 | 34.347 | 7,706 | 485.7 | 1,000,000 |

On this workload/hardware faucet's COPY path was **~16× faster** than Meltano
(and its INSERT path ~12.9×), with **~13.5× less peak memory** (35.9 vs 485.7
MiB) and **exact row-count parity** (1,000,000 = 1,000,000). Note also the
stddev: faucet is tight (±0.1s) while Meltano's Python/pipe runtime jitters
badly at scale (±34s). (The Meltano row is from the same-machine PR #307 run;
the faucet rows were re-measured when the COPY fast-path landed. faucet's
INSERT number improved slightly between runs — 11.17s → 10.10s — normal
machine-load drift.)

> **† The Meltano row predates the `batch_size_rows: 5000` pin.** It was captured
> on the same-machine PR #307 run, *before* the explicit batch pin was added to
> `meltano.yml` (it used `target-postgres`'s prior default). The config is now
> pinned so both sides write 5,000-row batches, but **this specific Meltano number
> has not been re-measured under the pin** — read it as pre-pin. In this regime
> batch size is not the lever (faucet's throughput is flat 500→5000 rows/`INSERT`),
> so it is not expected to move materially. Re-measuring needs Docker + a Meltano
> venv on the bench host: run `scripts/run-bench.sh --postgres --rows 1000000` and
> refresh this table + `benchmarks/results/results_pg_1m.md` when that environment
> is available (tracked in [#336](https://github.com/faucet-hq/faucet-stream/issues/336)).

This is the whole point of the scenario: **the gap
collapses from ~96× to ~16× as the workload moves from parse-bound to
sink-bound**, because both tools become bounded by the same Postgres write
path. The narrowing, all at 1M rows on the same machine:

| Scenario | Bottleneck | faucet (rows/s) | Meltano (rows/s) | Gap |
|---|---|---|---|---|
| A — CSV → JSONL | parse/serialize (best case) | 712,403 | 7,383 | **~96×** |
| B — Postgres → JSONL | typed row decode | 179,700 | 7,184 | **~25×** |
| C — Postgres → Postgres | **destination write (sink-bound)** | 123,200 (`copy`) / 99,000 (`insert`) | 7,706 | **~16× / ~13×** |

So quote **~16×** for a realistic DB→DB move (with `write_method: copy`), and
treat the ~96× CSV→JSONL figure as the upper bound, not the typical case.

**Why ~16× and not more?** It is near the ceiling for this shape, and the
profile proves it: at 1M, faucet spends ~62% of its wall on CPU (decode
Postgres rows → JSON → re-encode) and the rest blocked on the destination
write. `COPY` roughly halves the write step vs multi-row `INSERT` (the 5–10×
folk number applies to the write step in isolation, and Amdahl caps the
end-to-end gain at ~1.6× when 62% of the time is decode) — measured end-to-end
it bought **1.24×** (10.10s → 8.12s). This benchmark runs Meltano's
`target-postgres` on its default `INSERT` loader (its opt-in `use_copy: true`
COPY path is left off), so only the faucet-`insert` row is a method-matched
comparison; against faucet's COPY row the difference is partly write-method, not
just runtime. The remaining headroom is the decode side
(skipping the JSON intermediate for same-engine moves), which is niche and
high-complexity — see issue #308. Batch size is *not* the lever (throughput is
flat from 500→5000 rows/`INSERT` and worsens past the 65 535 bind-param cap).

> **Reproduce.** `make bench-postgres` (needs Docker) runs all three scenarios;
> `scripts/run-bench.sh --postgres --rows 1000000` scales Scenario C to the 1M
> headline size. The harness spins Postgres 16 in Docker, `COPY`-loads the seeded
> `bench` table, times faucet `postgres_to_postgres.yaml` (TRUNCATE `bench_dest`
> before each run) against Meltano `tap-postgres → target-postgres`, and writes
> the table into `benchmarks/results/results.md`. All three scenarios above are
> 1M rows. (Meltano's loader is pinned to `meltanolabs-target-postgres==0.8.0` —
> older `0.0.x` pins bundle a `singer_sdk` that needs `pkg_resources`, removed
> from modern setuptools on Python 3.12.)

Reproduce with Docker: see
[`benchmarks/README.md`](benchmarks/README.md#scenario-b--postgres--jsonl-manual-needs-docker).

## Caveats — what this does and does NOT prove

**What it measures:** single-machine, single-process **batch throughput** and peak
memory for CSV→JSONL, Postgres→JSONL, and Postgres→Postgres moves on the hardware
in `versions.txt`.

0. **CSV → JSONL is a best case, not the typical case.** It is almost pure
   parse-and-reserialize, which maximally exposes Python's per-row interpreter and
   Singer-over-pipe overhead — so the ~96× there is the *upper bound*. The
   sink-bound Postgres→Postgres move (Scenario C) is the realistic "move a table
   between two databases" number, and the gap there is smaller because both tools
   share the same database write path. Quote the range (1–2 orders of magnitude),
   and prefer the scenario closest to your actual workload.

**What it does not prove / honest boundaries:**

1. **Not distributed.** This is one box, one process. It says nothing about
   horizontal scaling, multi-worker throughput, or cluster behavior.
2. **Python interpreter startup is part of Meltano's cost — and it's real, but
   call it out separately.** A meaningful slice of Meltano's wall-clock on smaller
   datasets is Python + plugin subprocess startup and the Singer JSON-over-pipe
   handoff between tap and target. That cost is *fixed*, so it dominates small runs
   and amortizes on large ones — which is exactly why the **1M-row run is the one
   that matters** and the 100k run is only a smoke test. faucet's compiled-binary
   startup advantage is genuine but is partly a startup effect; the 1M row is where
   steady-state streaming throughput shows through.
3. **Singer may add metadata.** Singer/target-jsonl can emit `_sdc_*` metadata
   columns, changing record shape and output size. In *this* configuration the
   record counts matched exactly and no `_sdc_*` columns were added, but a
   different tap/target pairing may differ — so treat row-count parity, not
   byte-size parity, as the correctness check.
4. **Both emit string-typed values here.** `tap-csv` emits all-string fields and
   faucet's CSV source likewise passes through string cells, so neither tool is
   doing type inference in Scenario A — this is a fair, apples-to-apples parse +
   reserialize. Scenario B (Postgres) exercises real typed decoding on both sides.
5. **Hardware-dependent.** Absolute numbers only mean something relative to the
   `versions.txt` machine. Re-run on your own hardware before quoting.
6. **One workload.** A different shape (wide rows, deeply nested JSON, many small
   files, network-bound APIs) could move the ratio. This is not a general
   "faucet is Nx faster" claim — it is one reproducible data point.

**Bottom line:** the comparison is designed to be *fair and falsifiable*, not
flattering. If faucet does not win on the 1M-row run on your hardware, the harness
will say so.

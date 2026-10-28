---
name: faucet-tune
description: >-
  Use when a faucet pipeline is too slow or must fit a window: sizing a
  migration or full-refresh window, estimating how long a table takes,
  choosing a batch size, connector concurrency, matrix parallelism or
  sharding for one large table, finding whether the source or the sink is
  the bottleneck, weighing write-mode cost (append, upsert, overwrite,
  staged loads), benchmarking a change, or staying inside a source's
  connection limit or a destination's load quota while going faster.
license: MIT OR Apache-2.0
---

# Tuning faucet throughput

Tuning is measurement first: find which side is slow, change one setting,
measure again. This skill is that loop. A run that is slow because it
failed, waited on a rate limit or lagged is a `faucet-debug` problem; start
there if the run is not healthy.

## Step 0: use the project's faucet version

The project decides which faucet to use, not whatever is on the `PATH`: a
config written for a newer faucet can fail or be misread on an older one.

1. Find the pin: the `"github:faucet-hq/faucet-stream"` entry under `[tools]`
   in the project's `mise.toml` (`faucet init` writes it). A config may also
   carry `requires_faucet: ">=X.Y"`.
2. Run `faucet --version`. Use that binary if it equals the pin (with no pin:
   if it satisfies every `requires_faucet` in the project).
3. Otherwise, if [mise](https://mise.jdx.dev) is installed, run `mise install`
   in the project and prefix every command with `mise exec --`.
4. Otherwise use the pinned container image; its entrypoint is `faucet`:
   `docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work ghcr.io/faucet-hq/faucet-stream:<version> --version`.
5. Otherwise install exactly that version:
   `curl --proto '=https' --tlsv1.2 -LsSf https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v<version>/faucet-cli-installer.sh | sh`.
6. With no pin at all, install the latest release (the same installer from
   `releases/latest/download/`) and pin it as described in
   [Pinning the faucet version](https://faucet-hq.github.io/faucet-stream/operations/pinning.html).

Every version-specific fact (which connectors and blocks exist, config keys,
types, defaults, commands and flags) comes from that binary: `faucet list`,
`faucet schema --help`, `faucet schema source|sink|transform <name>`,
`faucet <command> --help`. Never from memory or from this skill. When a config
passes `faucet validate`, set its `requires_faucet:` to `">=<major>.<minor>"`
of that binary; if validate rejects `requires_faucet` as an unknown field, the
binary predates it, so leave it out.

## Step 1: measure a baseline

1. Pick a fixed, representative slice: one table (or a matrix row with
   `--select`), and either a full refresh or a bounded window. Note its row
   count.
2. Run it and keep the summary:

   ```bash
   faucet run pipeline.yaml --select orders --output json --log-format json
   ```

   Rows per second = rows written / duration from the summary.
3. Find the side that dominates. For a long-running `faucet schedule` or
   `faucet serve`, compare the source page time with the sink write and
   flush time in the metrics
   ([Finding the problem from metrics](https://faucet-hq.github.io/faucet-stream/operations/observability.html#finding-the-problem-from-metrics));
   for a one-shot run, compare the page and write timings in the debug-level
   logs of a short run, or run the source into a throwaway local file sink
   and see whether it alone takes the time.
4. Rule out waiting: time slept on rate limits, retries and lock waits is
   not a speed problem (`faucet-debug`, rate-limit recipe).

Do not change anything until you know the bottleneck side.

## Step 2: change one setting at a time

Read every setting's name, default and range from the binary:
`faucet schema source <kind>`, `faucet schema sink <kind>`,
`faucet schema execution`, `faucet run --help`. What each kind of knob does
is in [Throughput tuning](https://faucet-hq.github.io/faucet-stream/cookbook/tuning.html)
and [Performance tuning](https://faucet-hq.github.io/faucet-stream/operations/tuning.html).
In rough order of payoff:

| Bottleneck | Try |
|---|---|
| Either side, many small round trips | The source page size and the sink batch size; or let faucet size sink batches ([Adaptive batching](https://faucet-hq.github.io/faucet-stream/cookbook/adaptive-batching.html)) |
| Sink writes | The sink's bulk or staged load path if it has one, its connection pool or concurrency, then destination-side settings (indexes, logging) |
| Source reads, one large table | Shard the table across workers ([Mode B](https://faucet-hq.github.io/faucet-stream/cookbook/cluster.html#distributing-one-big-source-across-workers-mode-b)); `cli/examples/postgres_to_postgres_sharded.yaml` |
| Many independent tables | More matrix rows in parallel (the `execution` block) |
| Write mode | Upsert costs a keyed merge per page; overwrite stages and swaps; append is cheapest. Change it only when the data's semantics allow ([Upsert](https://faucet-hq.github.io/faucet-stream/cookbook/upsert.html)) |

After each change, rerun the baseline (Step 1) and keep the change only if it
helps. Also check memory: a larger batch holds more rows at once.

## Step 3: respect the guards

Faster must not break the systems at either end:

- A source's connection limit or replica capacity: total connections =
  parallel rows × shards × each connector's pool. Keep it below what the
  source allows, with headroom for its other clients.
- An API's rate limit: more concurrency only adds throttling waits.
- A destination's load-job, statement or ingestion quotas: bigger batches
  usually beat more parallel loads.
- On a server, the run's concurrency can be set per request, so one template
  can serve both a large and a small customer (the `faucet-api` skill).

## Step 4: benchmark a change properly

1. Use a fixed-size synthetic table, or a frozen copy, so every run reads
   the same rows.
2. Run each configuration three times; report the median rows per second and
   the spread.
3. Change one setting between configurations, and record the exact config
   and faucet version with each result.
4. Size a window from the median, plus margin for the slowest table.

The engine's own benchmark harness and its method are described under
[Measure, don't guess](https://faucet-hq.github.io/faucet-stream/cookbook/tuning.html#measure-dont-guess).

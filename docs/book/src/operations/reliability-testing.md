# Reliability testing

faucet's claim is not that rows arrive — it is that **the data can be trusted**:
nothing is lost, nothing is duplicated, nothing is silently corrupted, a full
refresh is atomic, an incremental run resumes exactly where it stopped. Those
are promises about behaviour *under failure*, so they are verified by a
dedicated test program rather than by the connector suites.

This page describes what that program covers, how to run each tier locally, and
how to add to it.

## Why these tests exist separately

A guarantee like *"the bookmark is persisted only after the sink confirms the
data it covers"* is not a property of any single function — it is a property of
the **order** in which the engine calls two collaborators. A unit test over pure
logic cannot observe an order. A connector test cannot either: it sees its own
side of the boundary.

So the reliability suites run the real `Pipeline::run` against doubles that fail
at one *named boundary* and record everything that happened, in order. The
assertions are then written against the sequence — which is what the guarantee
actually says — rather than against a final row count, which is what a weaker
test checks and which stays green through a real regression.

## The tiers

| Tier | What it covers | Docker? | Runs |
|---|---|---|---|
| **Engine guarantees** | Bookmark ordering, exactly-once (both mechanisms), overwrite atomicity, cleanup safety, cancel-still-flushes | No | Every PR, **required** |
| **State compatibility** | Bookmarks / exactly-once envelopes / commit tokens written by a release still load | No | Every PR, **required** |
| **Connector conformance** | Per-connector battery (capabilities truthful, bounded memory, idempotent replay, …) | Per connector | Every PR |
| **Containerized integration** | Real databases, Kafka, object stores; CDC replication | Yes | Every PR, reported |
| **Fidelity round-trip** | Type-exact landing per source↔sink pair | Per pair | Every PR, reported |

The first two tiers are deliberately Docker-free and run in seconds. They are
the required gate because a regression in them corrupts data *silently* — the
run still reports success — so they must never be skippable or flaky.

## Running them locally

```bash
# The required gate: engine guarantees + state-format compatibility.
cargo test -p faucet-conformance --all-features

# The engine's own unit suites, which cover the DLQ routing/budget matrix
# and the masking-before-DLQ ordering.
cargo test -p faucet-core --all-features

# One guarantee at a time.
cargo test -p faucet-conformance --test reliability_bookmark_ordering
cargo test -p faucet-conformance --test reliability_exactly_once
cargo test -p faucet-conformance --test reliability_lifecycle
cargo test -p faucet-conformance --test reliability_cleanup_safety
cargo test -p faucet-conformance --test compat_state_format
```

Container-backed suites need a Docker socket. On macOS with
[colima](https://github.com/abiosoft/colima):

```bash
colima start
export DOCKER_HOST="unix://$HOME/.colima/default/docker.sock"
cargo test --workspace --all-features
```

> The SQL Server image is x86-only, so the MSSQL suites cannot run on an
> ARM Mac — CI covers them.

## What each guarantee suite asserts

### Bookmark ordering (`reliability_bookmark_ordering`)

Every persisted bookmark is backed by writes the sink already confirmed. A
failed write, a failed flush, and a failed state-store `put` each leave the
durable bookmark *behind* the failure, so a resumed run re-reads rather than
skips. Resume starts at the page after the last durable bookmark — no gap, and
at most one page of duplicates.

### Exactly-once (`reliability_exactly_once`)

Both mechanisms, separately:

- **Atomic watermark** — writes route through the idempotent path with
  monotonic, distinct commit tokens; a crash in the window between the sink's
  commit and the state-store `put` replays **nothing**; re-running a completed
  pipeline writes nothing; a failed write leaves no token behind to skip on.
- **Keyed upsert** — convergence with no watermark at all.

The suite also runs a **control arm**: the identical injected failure under the
default at-least-once mode, asserting that it *does* duplicate. Without that,
the exactly-once assertions would not be distinguishing the two modes and could
pass for the wrong reason.

### Overwrite atomicity and cancellation (`reliability_lifecycle`)

`begin_overwrite` precedes the first write and `commit_overwrite` follows the
last. A failed write, a failed flush, and a cancel each abort **without ever
swapping** — a mid-run truncate-then-fail is the most destructive thing a data
tool can do. A failing commit surfaces rather than reporting success.

Separately, a cancelled run *flushes*: that is the difference between a
cooperative cancel and a dropped future, and it is what makes a buffered
Parquet footer or an S3 multipart commit instead of orphaning.

### Cleanup safety (`reliability_cleanup_safety`)

Scoped cleanup is the only feature that **deletes destination rows**, so its
tests are about when it must *not* fire: a failed run, a failed flush, a
cancelled run (which returns `Ok` — the trap), and an overflowed key set all
delete nothing. A complete run that legitimately read zero rows still cleans the
scope, because that is a complete answer.

### State-format compatibility (`compat_state_format`)

Frozen files under `crates/conformance/tests/fixtures/state/` hold state written
by a released version. The tests read them and assert the current code still
understands them.

**A round-trip test cannot catch this class of bug**, because it serializes and
deserializes with the same code — both halves move together and the test stays
green through a breaking change. If one of these fails, the fixture is not what
is wrong: either the reader regressed, or the change needs an explicit
migration.

## Adding to the program

### A new guarantee

1. Add a `Boundary` variant to `faucet_conformance::scripted` if the failure
   point is new, and log a matching `Event`.
2. Write the test against `Pipeline::run` and assert on the **event sequence**,
   not a final count.
3. Prove the assertion can fail. Where the check is reusable, put it in
   `scripted` and add a `#[should_panic]` test that feeds it a deliberately
   wrong event log — the existing durability assertion does exactly this. A
   check that cannot fail is worthless.

### A new source↔sink fidelity pair

Use the shared corpus so the pair cannot quietly pick easier data:

```rust
use faucet_conformance::fidelity::{self, Tolerance};

let landed = /* read the destination back */;
fidelity::assert_round_trip(&fidelity::corpus(), &landed, Tolerance::exact());
```

Start at `Tolerance::exact()`. Widen only with a comment naming the destination
limitation that forces it — `Tolerance` exists so "this column cannot survive
here" is an explicit statement, not a loose comparison hiding a bug.

### A new state shape

Commit a fixture named `<shape>-v<version>.json`, byte-exact as the writer
emitted it, and read it in `compat_state_format.rs`. Never edit an existing
fixture to make a test pass.

## Coverage expectations

Changed lines land at **≥95%** patch coverage. Most low coverage on a failure
path is a design smell rather than an inherent limit: if a branch is only
reachable through I/O, extract the decision into a pure function and test that —
the engine suites above exist precisely because the decisions were extracted
from the I/O paths that used to hide them.

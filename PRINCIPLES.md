# faucet-stream Engineering Principles

This repo exists to be an **engineering-excellence codebase**: generic, extensible,
easy to read, and safe to change. Every PR is reviewed against these principles.
When a change and a principle conflict, either the change adapts or the principle
is amended here — never silently ignored.

---

## 1. Generic over specific — the vendor test

**No concept, module, type, field, or identifier may be named after, or shaped
around, a single vendor, API, or customer.** Vendors are *configurations* of
generic mechanisms, never code paths.

- The test: *"If a second provider needed this tomorrow, would the code change,
  or only a YAML file?"* If the code would change, the design is too specific.
- Vendor names may appear only in: examples, docs, tests-as-fixtures, and
  crates.io keywords. Never in `src/` identifiers, config keys, or error text.
- When a vendor needs something new (an auth dance, a pagination style, a
  describe call), extract the **shape** of the need (a recipe, a style enum, a
  templated request) and let config select it.

## 2. Correctness lives once — in the engine

**Every correctness guarantee (bookmark ordering, retries, delivery semantics,
overwrite atomicity, upsert planning, drift, cleanup) is implemented once, in
`faucet-core`, and applied identically to all connectors.** Connectors only read
and write.

- Before writing a helper in a connector crate, check whether core already has
  the primitive (`retry.rs`, `resilience/`, `shard.rs`, `write_mode.rs`,
  `discover.rs`, `util.rs`). **Reimplementing a core primitive in a connector is
  a review-blocking defect** — the copy will drift and miss the original's edge
  cases (overflow guards, jitter, cancellation).
- If the primitive doesn't exist yet but is generic, add it to core and consume
  it from the connector — in the same PR.
- A fix to shared logic must fix every user of it. If a bug can be fixed in one
  connector without fixing the others, the logic is in the wrong place.

## 3. Extension points, not modifications

Evolution must be additive. Design so tomorrow's feature is a new leaf, not an
edit to an exhaustive surface.

- New capabilities on `Source`/`Sink` are **defaulted trait methods** — existing
  connectors never break.
- Public enums that will grow are `#[non_exhaustive]`; data/config structs are
  evolved under the **declared semver contract**
  (`[package.metadata.cargo-semver-checks.lints]`): struct-literal construction
  and exhaustive matching are not part of the stability surface. Additive
  evolution is a **minor** version, always.
- A genuine signature change keeps a delegating compatibility wrapper for the
  published name. Removals require an explicit, human-approved major.
- **Stability tiers.** Not everything a 1.x release ships is equally frozen.
  A young surface (a config block or feature still finding its shape) is marked
  **Experimental** in its doc comment — the marker flows into `faucet schema`
  output and docs.rs — meaning: *its shape may change in a minor release, with
  the change called out in the changelog.* Everything unmarked is stable under
  the full contract above. Graduation (removing the marker) is a deliberate,
  reviewed act once the shape has survived real use; an experimental marker is
  never a license for sloppiness — the block still meets every other principle.

## 4. Config is an API — design the YAML like one

The YAML surface is the product's primary interface. It gets the same design
rigor as a Rust API.

- **Group by concept, not by arrival order.** A feature with more than one knob
  gets a **block** (a nested object), never siblings sprayed at the parent
  level. `partition: { key, workers, count }` — not `partition_key`,
  `partition_workers`, `partition_count` beside twenty unrelated keys.
- **One concept, one shape, everywhere.** If two features share a concept
  (fan-out, naming, auth, retry), they share one struct and one key shape —
  `#[serde(flatten)]` or a common sub-struct — never two field sets that drift.
  (`WriteSpec` and `AuthSpec` are the house pattern; follow them.)
- **Hierarchy mirrors lifecycle.** Keys that configure the *request* live under
  the request block; keys that configure *what happens per dataset* live under
  the fan-out/emit block; keys that shape the *destination* belong to the sink
  (or an explicit `sink_patch`), not the source.
- **Naming is templated, not hardcoded.** Anything that derives a name
  (`table_id`, state keys, file paths) goes through the shared template
  vocabulary (`${name}`, `${name_snake}`, `${name_lower}`, `${now.*}`) — never a
  casing convention baked into Rust.
- **Sentinels are conventions, documented per field.** `0` = "no limit /
  unbatched" is the house sentinel (`batch_size: 0`, `max_concurrent: 0`); any
  field using it says so in its doc comment. No other magic values.
- Every config struct: `deny_unknown_fields`, doc comments on every field
  (they become `faucet schema` output), defaults that are safe, and validation
  at load time — a bad config must fail before the first byte moves.

## 5. No hidden state, no magic strings

- **No process-global mutable state** in library crates. Caches are owned by the
  instance (or injected), bounded, and invalidatable. A `static` cache in a
  connector is a bug: it leaks across unrelated pipelines in `serve`, defeats
  tests, and can never be flushed.
- **No stringly-typed side channels.** Values threaded through generic maps
  (partition contexts, labels) use named, documented, reserved keys defined in
  exactly one place — better, a typed field. Double-underscore key smuggling is
  a smell to eliminate, not a pattern to extend.
- Constants get names and rationale comments (`MAX_PARTITION_WORKERS`, with
  *why* that number), never inline literals at the use site.

## 6. Errors are typed; transience is classified, never grepped

- Every failure maps to a `FaucetError` variant. `.unwrap()`/`.expect()` only
  for construction-time invariants.
- Retry decisions use the typed classifier (`resilience::classify`,
  status codes, error kinds). **Matching on error message substrings is
  forbidden** — messages are not API and change under a dependency bump,
  silently turning retried errors into fatal ones (or worse, the reverse).
- If a third-party client's error type hides the status, wrap it once at the
  boundary into a typed shape; classify there.

## 7. Pure core, thin I/O shim — and both tested

- Logic that can be pure **is** pure: parsing, planning, tiling, diffing,
  templating live in functions of data → data, in their own module, unit-tested
  exhaustively (including the ugly edges: overflow, empty input, off-by-one).
- I/O wraps the pure core thinly (`stream.rs`/`sink.rs` are the only modules
  that talk to the network). Integration tests (wiremock/testcontainers) cover
  the shim; unit tests cover the logic.
- **≥95% patch coverage is the floor.** A new pure function without unit tests
  does not merge. "It's exercised by the e2e path" is not coverage of its edges.

## 8. Vocabulary: generic verbs, one meaning each

- The repo's verbs — `discover`, `fan_out`, `partition`, `shard`, `replicate`,
  `backfill`, `emit`, `describe` — each mean exactly one thing, defined in
  `.claude/rules/architecture.md`. New features reuse the existing verb when the
  concept matches, and coin a new one (documented) only when it doesn't.
- Two names for one concept (or one name for two concepts) is a defect: pick
  one, alias nothing.

## 9. Found in review = fixed before merge

**An issue identified while a PR is still open is fixed in that PR — never
shipped and "tracked for later."** Deferring a known defect past the merge gate
converts a cheap fix into public API, released behavior, and migration cost.

- This applies to everything the review surfaces: design (a mis-grouped config
  key becomes a breaking change the day it merges), duplication, missing tests,
  hidden state, naming.
- The only sanctioned deferrals are items **outside the PR's blast radius**
  (pre-existing issues the diff merely touches) — and those are filed as
  fully-described GitHub issues *before* the PR merges, not after.
- Corollary: review early. The cost of this rule is small when review happens
  before the PR balloons; that is a feature, not a bug.

## 10. Docs, schema, and examples move with the code

A config/API change in `src/` is incomplete until, in the same PR:
`schemas/faucet.schema.json` regenerated · crate README · docs-site page ·
`cli/examples/` updated · the capability matrix if support changed. The
docs-sync table in `.claude/rules/maintenance.md` is the checklist.

---

*Review checklist: for each PR ask — Would a second vendor need code changes?
Does any logic here already exist in core? Is every new YAML key inside a
concept block? Any global state, magic string, or message-grep? Is every pure
function unit-tested? Is every issue this review found fixed in this PR? Do
the docs/schema/examples move in this PR?*

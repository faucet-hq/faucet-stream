# Testing Standard

*Untested public API is a liability; every non-trivial behavior ships with a test, and changed lines must clear a hard 90% patch-coverage gate.*

This is the authoritative statement of the repository's testing rules. The [contributor testing guide](../contributing/testing.md) shows *how* to satisfy them with the project's tooling.

## What must be tested

- **New code MUST have tests.** Every new function or behavior gets a test — non-negotiable. Untested public surface is treated as a defect in review.
- **Tests MUST assert the specific outcome**, not merely "no panic". Check the extracted records, the pagination-state transition, the exact error variant, the emitted bookmark — the thing that would silently regress.
- **Correctness gates get failure-path tests.** Config validation, `match` arms, and error branches are all unit-testable and are exactly where silent corruption hides; cover them.

## Where tests live

- **Unit tests MUST live in `#[cfg(test)]` modules** at the bottom of the source file, for logic that needs no network I/O — JSONPath extraction, pagination state, auth-header generation, config validation, pure planning functions (`plan_writes`, `plan_pk_shards`, `diff_schema`).
- **Integration tests MUST live in the crate's `tests/` directory**, using `wiremock` for HTTP connectors and `testcontainers` for database/queue connectors. Containers MUST be started through `faucet_conformance::containers` (the `containers` feature), never with a bare `.start().await`: it owns the start-up budget, the retry of start-up failures, the readiness probe and the `FAUCET_REQUIRE_BACKENDS` rule.
- **SHOULD refactor an untestable line rather than exempt it.** Extract the pure logic; make the I/O a thin, separately-covered shim. Most low coverage is a design smell, not an inherent limit.

## Modifying existing tests

- **MUST NOT blindly update an existing test to make a change pass.** If a code change breaks a test, investigate first — silently rewriting the assertion to match new behavior hides regressions. Change the test only once you have confirmed the new behavior is correct and intended.

## The coverage gate

- **Every PR MUST land at ≥90% patch (changed-line) coverage.** `codecov/patch` is a **required** status check; a PR below 90% cannot merge. 90% is the floor, not a stretch goal.
- **MUST verify patch coverage locally before pushing** (`cargo llvm-cov` intersected with the diff) so the gate never surprises you. Docker-gated integration tests do not count toward the instrumented patch number — cover changed lines with unit tests.
- **MAY take an exception only for genuinely untestable surface** (a SIGTERM handler, a `main()` dispatch arm, an infinite supervisory loop), kept to the few unreachable lines and called out explicitly in the PR.

## Project-specific techniques

These recur; use them instead of reaching for Docker or skipping coverage:

- **Offline pool tests:** `sqlx` `connect_lazy` with a short `acquire_timeout` exercises pool-backed source logic without a live database (does not work for the eager MSSQL pool).
- **TUI / terminal paths:** render through a `TestBackend` and split the drive loop from the crossterm setup so the interactive path is covered in CI, which has no TTY.
- **Shipped examples:** every new `cli/examples/*.yaml` must validate under the env-placeholder allowlist in `cli/tests/cli_end_to_end.rs` — use literal hosts/brokers unless the var is allow-listed.
- **Feature-unification hazards:** a test asserting `serde_json::Map` key *order* passes under `-p <crate>` but fails under CI `--all-features` (the `preserve_order` feature flips `BTreeMap`↔`IndexMap`); assert the *set*, not the sequence.

## Related

- [Contributor testing guide](../contributing/testing.md)
- [Performance Standard](./performance.md)
- [Error Handling Standard](./error-handling.md)
- [Common Mistakes](../contributing/common-mistakes.md)

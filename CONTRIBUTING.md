# Contributing to faucet-stream

Thanks for your interest in contributing! faucet-stream is a Rust workspace of
connector crates plus the `faucet` CLI, and it's designed as an ecosystem — both
core changes and third-party connectors are welcome.

By participating you agree to abide by our [Code of Conduct](./CODE_OF_CONDUCT.md).

## Getting set up

```bash
git clone https://github.com/faucet-hq/faucet-stream
cd faucet-stream
cargo build --workspace
```

The toolchain is pinned in `rust-toolchain.toml`. Some connectors link native
libraries — the **Kafka** connectors build `librdkafka`, which needs `cmake` and
a C toolchain (`libsasl2-dev libssl-dev libcurl4-openssl-dev` on Debian/Ubuntu).

## Before you open a PR

Run the same checks CI runs:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features        # Kafka integration tests need Docker
cargo doc --workspace --all-features --no-deps   # must be warning-free
```

For pipelines that touch real services, the [`examples/`](./examples/) directory
has a `docker compose` stack and `make demo` for a no-infrastructure smoke test.

## Tests

- New functions and behaviors **must** have tests — untested public API is a
  liability. Unit tests live in `#[cfg(test)]` modules; HTTP-based connectors use
  `wiremock` integration tests under the crate's `tests/`.
- Don't blindly update an existing test to make it pass — if a change breaks a
  test, investigate first; silently rewriting tests hides regressions.
- Assert the specific outcome, not just "no panic".

## Code quality

- Every failure path maps to a typed `FaucetError` variant. No `.unwrap()` /
  `.expect()` on anything that can fail at runtime.
- No hardcoded credentials, tokens, or service URLs — ever.
- Reuse clients/connections; pool database connections; prefer bulk/multi-row
  APIs and streaming. Performance and reliability are the project's first
  priority.

## Adding a connector

faucet-stream connectors follow a fixed shape. To add `faucet-source-foo` /
`faucet-sink-foo`:

1. **Crate layout** — `lib.rs` (re-exports), `config.rs` (config struct + enums,
   no I/O), `stream.rs` / `sink.rs` (the only place that does I/O; create
   clients/pools in `new()`).
2. **Config** — derive `Serialize + Deserialize + JsonSchema`; implement
   `config_schema()` via `schema_for!`.
3. **docs.rs** — add `[package.metadata.docs.rs]` (`all-features = true`,
   `rustdoc-args = ["--cfg", "docsrs"]`) and make the first line of `lib.rs`
   `#![cfg_attr(docsrs, feature(doc_cfg))]`.
4. **Tests** — unit + integration.
5. **Wire it up** — add the feature to the umbrella crate, the CLI registry, and
   the `feature-check` matrix in `.github/workflows/ci.yml`.
6. **Docs** — a crate `README.md`, an entry in the root README + the docs-site
   [connector catalog](./docs/book/src/reference/connectors.md), and a runnable
   example under `cli/examples/`.

To reach **Tier-1 / conformant**, ship a `tests/conformance.rs` that invokes the
reusable `faucet-conformance` battery against the real connector (valid config
schema, bounded-memory streaming, honest capabilities) and passes it in CI —
that battery is the single source of truth for the tier. Where a connector
legitimately can't satisfy a check, assert the honest branch instead. See the
docs-site [authoring guide](./docs/book/src/extending/authoring-connectors.md)
for a worked example and the
[Faucet Connector Protocol (FCP v0)](./docs/spec/faucet-connector-spec-v0.md)
for the full contract.

### API stability (the `faucet-core` trait surface)

Third-party connectors depend on **`faucet-core`** — and only on `faucet-core`
(FCP §8). To protect them, the CI **`API stability (cargo-semver-checks)`** job
compares this revision's public API against the version last published to
crates.io and **fails on any breaking change**. It is a required check, so a
break cannot merge silently.

The rule is simple: **bump the major version, or don't break it.** The only
sanctioned way past the gate is a deliberate major-version bump (which moves the
baseline) — there is no bypass flag, matching FCP §8 ("breaking changes bump the
version"). Additive changes are always fine and pass the gate: new items, and —
crucially for the object-safe `Source`/`Sink` traits — **new trait methods must
carry a default implementation** so existing connectors keep compiling (a minor
bump, not a breaking one). Run it locally before pushing:

```bash
cargo install cargo-semver-checks --locked   # once
cargo semver-checks check-release --package faucet-core
```

Shared types for a source/sink pair (auth, formats) go in a
`faucet-common-<name>` crate that both depend on. See `faucet-source-rest` for a
reference implementation.

## Filing issues

Use the issue templates. We label every issue with:

- **Type** — `feature` (new capability), `enhancement` (improve existing), or
  `bug` (incorrect behavior).
- **Tier** — `tier-1` (critical / blocks a core use case), `tier-2` (important),
  or `tier-3` (nice-to-have).

Search open issues before filing to avoid duplicates. Feature/enhancement issues
are tracked in the roadmap epic (search the `epic` label).

## Pull requests

- Keep PRs focused; one logical change per PR.
- Put `Closes #N` in the **PR body** (not just commit messages) to link the issue.
- Update the relevant crate `README.md`, the root README, and the docs site when
  you change config fields, defaults, or behavior.
- Don't skip hooks (`--no-verify`) or CI; if a check fails, fix the root cause.

## Versioning & MSRV

- **Semantic Versioning.** The project follows [SemVer](https://semver.org/).
  While pre-1.0, breaking changes may land in minor (`0.x`) releases, but we call
  them out in the changelog. For a connector, a **breaking change** includes
  renaming/removing a config field, changing a field's type, or changing a
  default in a way that alters behavior — not just Rust API changes.
- **Independent crate versions.** Connector crates version independently on
  crates.io, so `faucet-source-rest` and `faucet-sink-bigquery` may sit at
  different versions. Per-crate tags use the form `<crate>-v<X.Y.Z>` (e.g.
  `faucet-source-rest-v0.2.0`); the repo-level `vX.Y.Z` tags track the overall
  release line.
- **Changelog.** Notable changes are recorded in [CHANGELOG.md](./CHANGELOG.md).
  release-plz appends a per-crate section every time it opens a release PR,
  reading the commit log via git-cliff internals; `cliff.toml` is retained for
  manual `git cliff` invocations on demand but is no longer the auto-generation
  path. Write commit subjects as `type(scope): summary` (`feat`, `fix`, `perf`,
  `refactor`, `docs`, `test`, `ci`, `chore`) so they're grouped correctly.
- **MSRV.** The minimum supported Rust version is pinned in
  `rust-toolchain.toml` and enforced by CI (fmt/clippy/test/docs all run on it).
  Bumping the MSRV is itself a notable change — raise it only when needed and
  note it in the changelog.

## Release process

The default release path is automated by [release-plz](https://release-plz.dev/)
(`release-plz.toml` + `.github/workflows/release-plz.yml`).

1. **Push to `main`** (typically via merging a feature PR). release-plz scans
   for `feat` / `fix` / `perf` commits since each crate's last `<crate>-v<X.Y.Z>`
   tag and opens (or updates) a `chore: release` PR that:
   - bumps the version of every crate with qualifying commits,
   - prepends a per-crate section to `CHANGELOG.md`,
   - leaves the other 44 crates untouched.
2. **Review the release PR** like any other change. Edit the changelog text if
   you want different wording; the version bumps follow SemVer from the commit
   prefixes (`feat` → minor, `fix` / `perf` → patch, anything tagged
   `BREAKING CHANGE` → major).
3. **Merge the release PR.** release-plz then publishes the bumped crates to
   crates.io in dependency order (`faucet-core` and the `faucet-common-*` crates
   before connectors before `faucet-stream` and `faucet-cli`), waits for the
   sparse index to propagate between dependents, creates per-crate GitHub
   releases, and pushes the `<crate>-v<X.Y.Z>` tags.

**Commits that don't bump versions.** `docs`, `chore`, `refactor`, `test`, `ci`,
and `build` commits are included in the changelog body for completeness but do
not trigger a version bump (`release_commits` filter in `release-plz.toml`). A
README-only edit never causes a spurious publish.

**Crate ownership.** crates.io makes the publishing account the only owner of
a brand-new crate. After publishing, the release job adds the
`github:faucet-hq:owners` team to every crate it released, so each crate is
owned by both the publisher and the team.

**Manual fallback.** `.github/workflows/release.yml` (`Release (manual fallback)`)
is kept as a `workflow_dispatch` workflow for ad-hoc / bulk re-publishes (e.g.
after a registry incident, or to re-publish all 46 crates from a known-good
revision). Day-to-day releases should go through release-plz.

**Dry-run locally.** Before relying on a release PR, you can preview what
release-plz would do:

```bash
cargo install release-plz --locked
release-plz release-pr --dry-run     # prints the diff it would commit
release-plz release    --dry-run     # prints what it would publish, in order
```

**Tokens required (configured in repo Settings → Secrets).**
- `CARGO_REGISTRY_TOKEN` — crates.io API token with the `publish-new`,
  `publish-update` and `change-owners` scopes for every `faucet-*` crate,
  owned by a member of the `faucet-hq/owners` GitHub team. `change-owners` is
  what lets the release job add the team to new crates.
- `RELEASE_PLZ_TOKEN` (optional but recommended) — a PAT or GitHub App token
  with `contents: write` + `pull-requests: write`. Without it the release PR
  is opened by the default `GITHUB_TOKEN`, which by GitHub policy does **not**
  trigger downstream workflow runs — CI won't run on the release PR.

## License

By contributing, you agree that your contributions are licensed under both the
MIT and Apache-2.0 licenses, matching the project's dual license.

# faucet-stream security self-assessment

This document follows the shape of the
[CNCF TAG Security self-assessment](https://github.com/cncf/tag-security/blob/main/community/assessments/guide/self-assessment.md).
It is written by the project for operators, security reviewers and
contributors. It complements the security model in
[`docs/architecture/security.md`](../architecture/security.md) (the
architecture and hardening checklist) and the disclosure policy in
[`SECURITY.md`](../../SECURITY.md).

| | |
|---|---|
| **Software** | <https://github.com/faucet-hq/faucet-stream> |
| **Security provider** | No. faucet-stream moves data; it is not a security tool, but it holds credentials and handles sensitive records, so its security features are part of its core. |
| **Languages** | Rust (engine, connectors, CLI, server); JavaScript, HTML and CSS (the embedded web console) |
| **Software bill of materials** | Each `faucet-cli` release attaches a CycloneDX SBOM (`faucet-cli.cdx.json`); prebuilt binaries embed their dependency list (`cargo-auditable`); every container image ships an SBOM attestation; `Cargo.lock` pins the full dependency tree. |
| **Security links** | [`SECURITY.md`](../../SECURITY.md) · [security model](../architecture/security.md) · [reference policy](../book/src/reference/http-api.md#reference-policy) · [secrets](../book/src/cookbook/secrets.md) · [OpenSSF Scorecard](https://scorecard.dev/viewer/?uri=github.com/faucet-hq/faucet-stream) |

## Overview

faucet-stream is a config-driven data-movement toolkit: source and sink
connectors (databases, CDC streams, object stores, queues, warehouses, HTTP
APIs) and a `faucet` CLI that runs pipelines declared in YAML or JSON. It runs
as a one-shot CLI, a cron scheduler, an embedded Rust library, or a
long-running HTTP control plane (`faucet serve`).

### Background

To do its job faucet holds credentials for many external systems and streams
records that can carry personal or confidential data. The security questions
are therefore about credentials (who can read them, where they can leak),
data (where records can land, masked or not) and, for `faucet serve`, what a
caller who can submit a config can make the server do.

### Actors

| Actor | Trust | Notes |
|---|---|---|
| **Operator** | Trusted. | Installs faucet, writes configs on disk, starts the CLI or server, owns its flags, the triggers file, templates and the environment. |
| **`faucet serve` caller** | Authenticated, partly trusted. | Submits configs and triggers runs over HTTP or MCP. Has a role (`viewer`, `operator`, `admin`) and optionally a tenant. A caller's config is confined by the server's reference policy. |
| **Tenant principal** | Authenticated, least trusted caller. | Scoped to one tenant: runs registered templates only, sees only its own runs, state, usage and connections. |
| **Webhook / trigger sender** | Unauthenticated or token-authenticated. | Can start a configured trigger; the values it sends are substituted into the operator's config under strict rules. |
| **External systems** | Untrusted data, trusted endpoints. | Sources and sinks. Their responses are parsed defensively (bounded reads, typed decoding). |
| **Connector author** | Trusted code. | A connector runs with the full privileges of the faucet process. |
| **Secrets managers** | Trusted. | Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault, read at config load. |

### Actions

1. **Load a config.** The CLI composes `extends:` / `!include` / profiles,
   resolves `${env:}`, `${file:}`, `${secret:}` and secret-manager references,
   binds typed `params:`, and validates the result against the connectors'
   schemas. Resolved secret values are registered for redaction.
2. **Run a pipeline.** The source reads pages; governance passes run in a fixed
   order (masking first, then quality, contract, schema drift, policy, budget);
   the sink writes; the bookmark advances only after the sink confirms.
3. **Serve.** `faucet serve` authenticates the bearer token, maps it to a
   principal and role, checks the route's permission (an unmapped route is
   admin-only), loads the submitted config under the caller's reference policy,
   admits it against policy, budget and tenant limits, then runs it. Every
   decision of consequence is written to the audit log.

### Goals

- Credentials never appear in faucet's own logs, errors, run history, DLQ
  envelopes, lineage events or API responses.
- A caller of `faucet serve` reaches only the files, URLs, environment
  variables, secret managers and programs the operator allowed.
- Tenants are isolated from each other: runs, state, usage, connections,
  audit and change requests are scoped by tenant.
- PII masked by a `masking:` policy never reaches a sink, the DLQ or a lineage
  sample unmasked; a data-flow `policy:` refuses a config that would send a
  labelled column to a disallowed sink.
- Generated SQL never splices identifiers or values unsafely.
- Release artifacts are verifiable: build provenance for binaries, signed
  container images with provenance and an SBOM.

### Non-goals

- Sandboxing connector code. A connector, including a third-party plugin, runs
  with the full trust of the process.
- Protecting against an operator: whoever controls the host, the flags or the
  configs on disk controls faucet.
- Securing the external systems faucet connects to, or the network between
  them beyond each connector's TLS options.
- Redacting secrets from third-party library output (driver debug logging),
  metric labels or span attributes; see the
  [redaction boundary](../architecture/security.md#the-redaction-boundary--and-its-sharp-edges).
- Multi-party authorization of changes inside the project (a single
  maintainer today; see [GOVERNANCE.md](../../GOVERNANCE.md)).

## Self-assessment use

This self-assessment was created by the faucet-stream maintainers to describe
the project's security posture. It is not a security audit and carries no
certification. It is a starting point for a review by a user or an
independent assessor, and it is updated when a trust boundary changes.

## Security functions and features

### Critical

| Component | What it does | Where |
|---|---|---|
| **Trust boundary: CLI vs `faucet serve`** | The CLI and scheduler trust their configs completely (the operator wrote them). `faucet serve` distinguishes a config's *origin*: local or operator-authored (templates, the triggers file, `--default-config`) versus caller-submitted. Only caller-submitted configs are confined. | `cli/src/runtime/principal.rs` (`Principal`, `Origin`), `cli/src/runtime/server.rs` |
| **Reference policy** | One allow-list for every reference a caller's config makes: path roots (symlinks resolved, `..` refused), URL schemes and hosts (canonical host comparison; link-local and cloud-metadata hosts refused unless listed, on the first request and every redirect), environment variables, secret-manager schemes, and subprocess connectors. Connector config fields that name a path, URL or program are found by a schema annotation (`x-faucet-resource`), so new connectors are covered by declaring the field; a lint test fails on an unannotated one. Default-deny for caller bodies; tenants are confined to their own path root and never run subprocesses. | `faucet_core::resolve` (`ReferencePolicy`), `cli/src/serve/references.rs`, `cli/src/serve/load.rs`, `cli/tests/resource_annotations.rs` |
| **Sandboxed SQL engines** | Connectors whose own input can reach host files (embedded SQLite, DuckDB) expose a confining switch marked `x-faucet-sandbox`; the server forces it on for every config it did not load from its triggers file. | `faucet_core::resolve::sandbox_fields`, `cli/src/serve/load.rs` |
| **Authentication and RBAC** | Bearer tokens compared in constant time; the server refuses to start without auth unless `--no-auth` is explicit. `--auth-config` maps tokens to principals with roles; every `/v1` route has a required permission and an unmapped route is admin-only (fail closed). Tokens are registered for redaction. | `cli/src/serve/auth.rs`, `cli/src/serve/rbac.rs` |
| **Tenant isolation** | The effective tenant of an action comes from the authenticated principal, never from the request body. Tenant routes outside the principal's scope answer 404; state keys are namespaced by tenant; per-tenant concurrency and run budgets are enforced at admission. | `cli/src/serve/tenants/`, `cli/src/serve/history/tenants.rs`, `cli/src/executor.rs` (`StateScope`) |
| **Sealed credentials** | Tenant connections, OAuth refresh tokens, PKCE verifiers and tenant notification secrets are sealed with AES-256-GCM under the server's vault key (`--vault-key`, with key rotation through `--vault-previous-key`), bound to their tenant and name so a sealed value copied to another record does not open. In cluster mode, caller-supplied `secret: true` template params are sealed before the run is persisted to the shared database, or refused when no vault key is configured. | `cli/src/serve/tenants/vault.rs`, `cli/src/templates/store.rs` |
| **Secret handling and redaction** | Secret references resolve at load time; every resolved value is registered with a process-wide redactor that scrubs faucet's tracing output, error messages, DLQ envelope error text and persisted run errors. URL credentials are stripped from dataset URIs and HTTP error text. Error text is redacted where it is created, before it is stored. | `cli/src/secrets/registry.rs`, `faucet_core::redact`, `faucet_core::util::redact_uri_credentials`, `faucet_core::io` |
| **SQL safety** | Identifiers are quoted per dialect with validation (`SqlDialect::check_ident`, `quote_ident`); values are bound as parameters, never spliced; exact decimals never pass through `f64`. A trigger may not substitute a whole sender-chosen value into a field the schema marks as a SQL statement (`x-faucet-sql`). | `faucet-common-sql`, `faucet_core::util`, `cli/src/serve/triggers/context.rs` |
| **HTTP client** | The one sanctioned HTTP client follows same-origin redirects only by default, strips credentials on a cross-origin hop and never replays a body to another origin, bounds response size (1 GiB default) and enforces the reference policy on every hop. A lint forbids building a raw client. | `faucet_core::http`, `clippy.toml` |

### Security relevant

- **Audit log**: run submit/cancel/delete, template lifecycle, change
  requests, policy denials and every authorization denial, through one choke
  point (`cli/src/serve/audit.rs`).
- **Change requests**: optional plan → approve → run flow with configurable
  approver quorum and no self-approval by default (`--require-approval`).
- **Data-flow policy and masking**: classifications, sink attributes and rules
  checked statically before a run and enforced again per record at the sink
  (`faucet_core::policy`, `faucet_core::masking`).
- **Run budgets**: hard ceilings on records, bytes, duration and allowed sinks
  per invocation (`faucet_core::budget`).
- **Bounded parsing**: `${file:}` reads are capped at 1 MiB; HTTP bodies are
  bounded; CDC decoders buffer at most `max_staged_records` per transaction.
- **State at rest**: optional encryption of the file state store
  (`crates/core/src/encryption.rs`).

## Project compliance

faucet-stream does not claim compliance with a regulatory standard (it is
software, not a hosted service). It tracks the
[OpenSSF Best Practices](https://www.bestpractices.dev/) criteria and the
[OpenSSF Scorecard](https://github.com/ossf/scorecard) checks
([#978](https://github.com/faucet-hq/faucet-stream/issues/978)).

## Secure development practices

### Development pipeline

- All changes land through pull requests on a protected `main` branch (no
  force pushes, no deletion) with required checks: formatting, `clippy -D warnings`, the test suite with a 95 %
  patch-coverage standard and an integration-test gate, `cargo-semver-checks`,
  documentation build, reliability suites and the CLI feature builds.
- Static analysis: Clippy on every change, CodeQL for Rust and GitHub Actions
  workflows (`.github/workflows/codeql.yml`).
- Dynamic analysis: `cargo-fuzz` targets for configuration interpolation,
  identifier quoting, the PostgreSQL logical-replication (pgoutput) decoder and
  the exact-number JSON parser (`fuzz/`), run on pull requests by
  ClusterFuzzLite (`.github/workflows/cflite_pr.yml`). Tests run with debug
  assertions; a chaos suite injects failures at named pipeline boundaries.
- Supply chain: `cargo-deny` (licenses, advisories, sources) as a required
  check, a daily advisories scan, Dependabot for Cargo, GitHub Actions and the
  container base images, dependency review on pull requests, every GitHub
  Action pinned by commit SHA, zizmor analysis of the workflows,
  StepSecurity harden-runner egress auditing on CI jobs, base images
  pinned by digest, OSV-Scanner triage in `osv-scanner.toml`.
- Repository security: secret scanning with push protection, private
  vulnerability reporting, `CODEOWNERS` on workflows and release configuration,
  least-privilege workflow tokens (`permissions: read-all` by default).
- Releases: automated by release-plz; publishing jobs wait for approval in a
  protected environment that alone holds the publishing secrets. Binaries are built
  with `cargo-auditable` and carry build-provenance attestations, also
  attached to each GitHub release as `<asset>.sigstore.json`, plus a
  CycloneDX SBOM (`faucet-cli.cdx.json`); container images are keyless-signed with SLSA
  provenance and an SBOM.

### Communication channels

- **Internal**: GitHub issues, pull requests and the roadmap board.
- **Inbound**: GitHub issues and discussions; security reports through GitHub
  private vulnerability reporting ([SECURITY.md](../../SECURITY.md)).
- **Outbound**: release notes and changelogs, GitHub Security Advisories, the
  documentation site.

### Ecosystem

faucet-stream is published on crates.io as a set of crates (`faucet-core` is
the connector SDK; third-party `faucet-source-*` / `faucet-sink-*` crates build
on it), as prebuilt binaries, as a Homebrew formula and as container images
with a Helm chart.

## Security issue resolution

### Responsible disclosure process

Reports go through GitHub private vulnerability reporting. The maintainers
acknowledge within 7 days, assess within 14 days, aim to fix critical and high
issues within 30 days and others within 90 days, and publish a GitHub Security
Advisory (with a CVE when one applies) when the fix ships. Details:
[SECURITY.md](../../SECURITY.md).

### Incident response

A confirmed vulnerability is fixed on a private advisory fork, released as a
patch of every affected crate and as new binaries and images, and announced in
the advisory with the fixed versions and any workaround. A compromised release
credential is revoked and rotated, the affected versions are yanked from
crates.io, and the incident is described in an advisory.

## Appendix

### Known issues and accepted gaps

- **One maintainer (accepted).** Changes are not reviewed by a second person;
  the required checks are the review bar. Branch protection on `main` requires
  the status checks and forbids force pushes and branch deletion, but requires
  no approving review. Scorecard's Branch-Protection, Code-Review and
  Contributors checks score low as a result; these are accepted gaps, not
  planned work. See [GOVERNANCE.md](../../GOVERNANCE.md#accepted-gaps-of-a-single-maintainer-project).
- **Connector code is not sandboxed.** A plugin or a driver can read anything
  the process can.
- **Redaction covers faucet's own output only.** Third-party debug logging can
  print credentials; never run with debug logging while configs hold secrets.
- **Some connectors default to plaintext transport** (for example the
  PostgreSQL CDC source's `tls: disable`), explicitly and visibly in the
  schema. Production deployments must set TLS.
- **The DLQ holds the failed records as they were**; without a masking policy
  it holds raw data.

### OpenSSF Best Practices

In progress ([#978](https://github.com/faucet-hq/faucet-stream/issues/978)).

### Related projects and vendors

Comparable data-movement tools (Singer taps and targets, Meltano, Airbyte) run
connectors as separate processes or containers; faucet compiles connectors into
one Rust binary and confines what a remotely submitted config may reference
rather than isolating the connector process.

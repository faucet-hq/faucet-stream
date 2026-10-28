# Security Policy

## Supported versions

faucet-stream has reached **1.0**. Security fixes are applied to the latest
published `1.x` release on crates.io and to `main`. Older releases — including
all pre-1.0 (`0.x`) versions — are no longer maintained; please upgrade to the
latest release before reporting.

## Reporting a vulnerability

**Please do not report security vulnerabilities through public GitHub issues,
discussions, or pull requests.**

Report privately through either channel:

1. **GitHub private vulnerability reporting** (preferred) — open the repository's
   [**Security** tab](https://github.com/faucet-hq/faucet-stream/security) and
   click **"Report a vulnerability"**
   ([direct link](https://github.com/faucet-hq/faucet-stream/security/advisories/new)).
   This creates a private advisory visible only to maintainers.
2. **Email** — **pawanksikawat@gmail.com**.

Please include:

- a description of the vulnerability and its impact,
- the affected crate(s) and version(s),
- steps to reproduce or a proof of concept,
- any suggested remediation.

## What to expect

| Step | Timeline |
|---|---|
| Acknowledge your report | within **7 days** |
| Confirm or rule out the issue, and give a severity and affected versions | within **14 days** |
| Release a fix for a critical or high-severity issue | target **30 days** from confirmation |
| Release a fix for a medium or low-severity issue | target **90 days** from confirmation |
| Publish the advisory (GitHub Security Advisory, with a CVE when one applies) | when the fix is released, or at **90 days** from the report if no fix is ready, unless we agree a different date with you |

- We'll keep you informed at each step, and tell you if a timeline slips and why.
- Fixes ship as a patch release of every affected crate on crates.io, the
  prebuilt binaries and the container images; the advisory names the fixed
  versions.
- With your consent, we'll credit you in the advisory and release notes.

## What counts as a vulnerability

A flaw in faucet-stream's own code or release artifacts that lets someone break
a guarantee it documents: read or change data, credentials or state they should
not reach; escape a `faucet serve` trust boundary (authentication, RBAC, tenant
isolation, the reference policy, sandboxed connectors); run code or reach the
network or filesystem from a submitted config beyond what the server allows; or
inject into a query, path or command. The trust boundaries are described in
[`docs/security/self-assessment.md`](docs/security/self-assessment.md).

Not vulnerabilities (open a normal issue instead): a credential leaked in your
own config, a misconfiguration the docs warn against, denial of service from a
config you control, and vulnerabilities in the external systems faucet connects
to. Dependency advisories are tracked automatically (`cargo-deny`, Dependabot);
report one privately only if faucet-stream is actually exploitable through it.

## Scope notes

faucet-stream moves data between external systems, so keep in mind:

- **Credentials** belong in environment variables / secret files referenced via
  `${env:VAR}` / `${file:PATH}` / `${secret:VAR}` — never commit them to config
  files. A leaked credential in your own config is not a vulnerability in
  faucet-stream.
- SQL identifier handling (`quote_ident`), bind-parameter substitution, and
  JSON-safe interpolation are security-relevant — bug reports about injection
  vectors in these paths are especially welcome.

# Governance

faucet-stream is an open-source project hosted by the
[`faucet-hq`](https://github.com/faucet-hq) GitHub organization. This page says
who decides what, how changes and releases happen, and how the project keeps
going if a maintainer becomes unavailable.

## Roles

| Role | Who | Can |
|---|---|---|
| **Maintainer** | Today one person: the GitHub account [`@pecsorabs`](https://github.com/pecsorabs), owner of the `faucet-hq` organization and of the code in [`CODEOWNERS`](.github/CODEOWNERS). | Merge pull requests, approve releases, triage security reports, manage the roadmap, grant access. |
| **Contributor** | Anyone who opens an issue or pull request. | Propose changes, review, discuss. No special access is needed. |

A contributor with a record of sustained, high-quality contributions can be
invited to become a maintainer. The existing maintainers decide by consensus;
the invitation and acceptance are recorded in an issue.

## How decisions are made

- **Day-to-day changes** go through pull requests. Every pull request must pass
  the required CI checks (format, lints, tests, coverage, API stability, docs,
  reliability suites; see [CONTRIBUTING.md](CONTRIBUTING.md)) before it merges.
  With one maintainer there is no second human reviewer, so the required checks
  and the written standards are the review bar.
- **Larger changes** (a new subsystem, a public API change, a new trust
  boundary) start as an issue or an RFC under [`rfcs/`](rfcs/), so the design
  is discussed before code lands.
- **Priorities** come from the public roadmap: the
  [roadmap board](https://github.com/orgs/faucet-hq/projects/2) and
  [`docs/roadmap.md`](docs/roadmap.md).
- **Disagreements** are resolved in the issue or pull request. While there is
  one maintainer, the maintainer decides; once there are several, by consensus
  of the maintainers, falling back to a majority.

## How releases happen

Releases are automated and need a maintainer's approval at each publishing
step (details: [CONTRIBUTING.md § Release process](CONTRIBUTING.md#release-process)).

1. Merging to `main` lets [release-plz](https://release-plz.dev/) open a release
   pull request that bumps the affected crates by Semantic Versioning, derived
   from Conventional Commit messages, and updates their changelogs.
2. Merging that pull request publishes the crates to crates.io. The publishing
   jobs run in the protected `release` environment, which waits for a
   maintainer's approval; the publishing secrets live only in that environment.
3. The `faucet-cli` release tag builds the prebuilt binaries (with build
   provenance attestations), the container images on `ghcr.io` (keyless-signed,
   with SLSA provenance and an SBOM) and the Homebrew formula.

Only repository administrators can create release tags.

## Access continuity

The project must survive the loss of any one account. Publishing rights today:

| Asset | Who can publish | Recovery path |
|---|---|---|
| crates.io crates | The maintainer's crates.io account and the `github:faucet-hq:owners` team, added as an owner of every crate. | Any member of the `owners` team of the `faucet-hq` organization can publish and manage crate owners. |
| Container images and Helm chart on `ghcr.io/faucet-hq` | The release workflows, with the repository's own token; packages belong to the organization. | Any organization owner controls the packages and the workflows. |
| GitHub releases, binaries | The release workflows. | Any organization owner. |
| Homebrew tap | The release workflow, with a token scoped to the tap repository. | The tap owner reissues the token; the formula can be regenerated from any release. |
| Documentation site | GitHub Pages, deployed by CI from `main`. | Any organization owner. |

Continuity plan:

- The `faucet-hq` organization has one owner today. If more maintainers join,
  each becomes an organization owner, so settings, packages and teams stop
  depending on one account.
- Members of the `owners` team can publish to crates.io; adding a trusted
  member there is the step that hands over publishing.
- If the maintainer becomes unavailable, a remaining organization owner can
  appoint a new maintainer, rotate the release secrets (crates.io token,
  Homebrew tap token) and continue releases with the documented, automated
  process. Everything a release needs is in this repository.
- Security reports go through GitHub private vulnerability reporting, which
  every organization owner and repository administrator can read, so they do
  not depend on one person's inbox ([SECURITY.md](SECURITY.md)).

## Accepted gaps of a single-maintainer project

These are deliberate, not planned work, while the project has one maintainer:

- **Reviews are required, and the maintainer bypasses them.** `main` is
  governed by two public repository rulesets: *main protection* (no force
  pushes, no deletion, with no bypass for anyone) and *main reviews and
  checks*. The second requires two approving reviews including a code owner,
  dismisses stale approvals, requires approval of the last push, and requires
  the CI gates listed in [CONTRIBUTING.md](CONTRIBUTING.md) on an up-to-date
  branch. Repository admins may bypass it. Every outside contribution goes
  through those rules. With no second reviewer, the maintainer merges their own
  pull requests with the admin bypass, and only after every required check has
  passed.
- **OpenSSF Scorecard** therefore scores *Code-Review* at 0 (the maintainer's
  own changes are not approved by anyone else), *Branch-Protection* short of 10
  (the admin bypass), and *Contributors* low (contributors from one
  organization). These gaps are accepted and revisited when a second maintainer
  joins: the bypass then goes away and reviews apply to everyone.

## Code of conduct

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md);
it says how to report a concern.

## Changing this document

Changes to governance are made by pull request and need the agreement of the
maintainers.

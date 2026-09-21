# RFC 0006 — Template hosting and sync

*Let a running `faucet serve` mirror its pipeline-template registry from a remote store — GitHub, S3, GCS, or Azure Blob — with a single source of truth and an explicit, one-directional publish.*

| | |
|---|---|
| **RFC** | 0006 |
| **Title** | Template hosting and sync |
| **Status** | Accepted |
| **Authors** | faucet-stream maintainers |
| **Related issues** | #589 (this RFC is its first acceptance criterion) · #571 (Template Hub) · #444 (template registry) · epic #38 |
| **Related ADRs** | — |

## Summary

`faucet serve` keeps pipeline templates in a registry backed by the run-history
store (#444). Today the only way to get a template into that registry is to
`POST /v1/templates` or run `faucet template register` against it, once per
template, per server. This RFC proposes a **sync layer**: a declarative list of
remote **origins** (a GitHub repo, an S3/GCS/Azure prefix) that the server
**pulls** templates from into its own registry, plus an explicit
`publish` action that pushes one local template *to* an origin. It is
deliberately **not** bidirectional: an origin is the source of truth for the
templates it owns, pull is the only automatic direction, and publish is a
operator-initiated write that never runs on a timer.

## Motivation

The registry's value proposition is register-once / trigger-by-id. That holds
for one server. It does not survive the second one:

- **Every server is an island.** `cli/src/templates/store.rs::register` writes to
  whatever store `--history` names. Two servers (staging and prod, or two
  regions) have two disjoint registries, and keeping them equal is a manual
  chore whose failure mode is silent — `stable` on one means a different config
  than `stable` on the other, and nothing reports the divergence.
- **A fresh server starts empty.** A `faucet serve` restored from scratch (a new
  cluster, a rebuilt PVC, a disaster-recovery drill) has no templates until
  somebody replays every `register` in the right order, including the `launch`
  that decides what unpinned callers get. The launch log
  (`cli/src/serve/history/templates.rs::LaunchRecord`) is the part most likely to
  be replayed wrong, and it is the part that decides what production runs.
- **The versioned bodies already live in git, for most teams.** Teams keep
  `tenant-sync.yaml` in a repo, review changes there, and then hand-copy the
  merged file into a server. The repo is already the source of truth; the
  registry is a cache of it that drifts.
- **#571 (Template Hub) is the public half of the same idea.** It describes a
  git-backed catalog people publish templates *to*. Without a defined way for a
  server to *consume* one, the hub is a website. These must be one model.

Doing nothing leaves the registry usable but unoperable at more than one server,
and leaves #571 without a consumer.

## Guide-level explanation

A server is pointed at one or more **origins** in a sync file:

```yaml
# templates-sync.yaml
version: 1
origins:
  # The team's own templates, reviewed in a repo.
  - name: platform
    source:
      type: github
      config:
        repo: acme/data-templates
        ref: main                      # branch, tag, or commit sha
        path: templates/               # directory of *.yaml
        auth: { type: token, config: { token: "${env:GITHUB_TOKEN}" } }
    prefix: platform-                  # id namespace owned by this origin

  # A bucket another team publishes to.
  - name: partner
    source:
      type: s3
      config: { bucket: acme-templates, prefix: shared/, region: us-east-1 }
    prefix: partner-
    launch: follow                     # honour the origin's `launch:` marker
```

```bash
faucet serve --history sqlite:./faucet.db --templates-sync templates-sync.yaml
```

The server pulls each origin on start and every `interval` thereafter (default:
never — pull is explicitly triggered unless an interval is configured). A pull is
idempotent: a template whose body already matches the registry's newest version
registers nothing.

Manual verbs, available from the CLI, the HTTP API, and a button in the console's
Templates view:

```bash
faucet template sync --store sqlite:./faucet.db --config templates-sync.yaml
faucet template sync --origin platform --dry-run     # show the plan, change nothing
faucet template publish tenant-sync --origin platform --version stable
```

```
POST /v1/templates/sync            {"origin": "platform", "dry_run": true}
POST /v1/templates/{id}/publish    {"origin": "platform", "version": "stable"}
```

A dry-run prints exactly what a real pull would do, per template:

```
origin platform (github acme/data-templates@main)
  platform-tenant-sync   register v4 (body changed)  launch: follow → launch v4
  platform-billing-sync  unchanged (v2 is current)
  platform-legacy-sync   MISSING at origin — left in place (see `prune`)
2 template(s) to change, 1 unchanged, 1 orphaned
```

### What an origin looks like on the remote

An origin is a directory of ordinary faucet configs. The **file stem is the
template id** (after the origin's `prefix`), so `templates/tenant-sync.yaml`
becomes `platform-tenant-sync`. Release intent travels in an optional sidecar,
because a config body must stay a config body — anything else and the file stops
being runnable with `faucet run`:

```
templates/
  tenant-sync.yaml
  tenant-sync.faucet.yaml     # optional sidecar
```

```yaml
# tenant-sync.faucet.yaml
description: Per-tenant event sync
launch: true                   # this version should become `stable` on pull
tags: [dev]                    # assignable channels to point at it
```

## Reference-level explanation

### No `faucet-core` changes

Sync is CLI/serve orchestration over the existing registry primitives, exactly
like the catalog (#279) and triggers (#196). It adds no `Source`/`Sink` trait
surface and touches no pipeline code.

### New module: `cli/src/templates/sync/`

| File | Contents |
|---|---|
| `spec.rs` | `SyncFile { version, origins }`, `Origin { name, source, prefix, launch, prune, interval_secs }`, `OriginSource` (`Github`/`S3`/`Gcs`/`AzureBlob`, adjacently tagged `{type, config}` like every other connector block), `LaunchPolicy` (`Ignore` \| `Follow` \| `Always`), `PrunePolicy` (`Keep` \| `Deprecate`). Derives `JsonSchema`; `faucet schema templates-sync`. |
| `fetch.rs` | `RemoteTemplate { id, body, format, sidecar }` and the `Fetcher` trait (`async fn list(&self) -> CliResult<Vec<RemoteTemplate>>`), with one impl per origin type. S3/GCS/Azure go through `object_store` — the same dependency and credential plumbing `triggers::object_arrival` already uses. GitHub goes through `reqwest` against the contents API, so a private repo needs only a token and no git binary. |
| `plan.rs` | The **pure** core: `plan(origin, remote: &[RemoteTemplate], local: &TemplateState…) -> SyncPlan`. `SyncPlan { actions: Vec<SyncAction> }`, `SyncAction::{Register{id, body, launch, tags}, Unchanged{id, version}, Orphaned{id}, Deprecate{id}}`. No I/O, so every interesting decision (body unchanged, launch-on-change, orphan handling, id derivation, prefix collision) is unit-testable without a network or a store. |
| `apply.rs` | Executes a plan against the store by calling the **existing** `templates::register` / `launch` / `promote` / `set_deprecated`. Nothing about version assignment, validation, or the launch log is reimplemented. |
| `mod.rs` | `load_sync_file`, `sync_origin`, `sync_all`, `publish`. |

### The single-source-of-truth rule, stated precisely

1. An origin **owns an id namespace**, declared by its `prefix`. Two origins with
   overlapping prefixes are a load-time error, so no template can have two
   owners and "who wins" never needs deciding at runtime.
2. **Pull only ever appends.** It calls `register`, which appends a version; it
   never rewrites or deletes one. A locally-registered version of an
   origin-owned id is therefore preserved, and the origin's next pull simply
   appends after it — divergence is visible in the version list rather than
   silently overwritten.
3. **Pull moves `stable` only when the origin says to.** `launch: ignore`
   (default) registers and moves nothing, preserving the #444 rule that a
   register moves nobody. `launch: follow` honours the sidecar's `launch: true`.
   `launch: always` launches every pulled version — the GitOps setting, opted
   into explicitly because it hands a remote repo the production release lever.
4. **Deletion at the origin is not deletion locally.** A template that vanishes
   upstream is reported as `Orphaned` and left alone (`prune: keep`, the
   default), or deprecated (`prune: deprecate`). It is never deleted: a delete
   cascades to the launch log (#444) and would silently repoint `stable` — far
   too much authority for "someone rebased a branch".
5. **Publish is the only local → remote direction, and it is manual.** It writes
   one version's body (and a sidecar) to the origin; it has no timer and no
   watcher. There is no conflict resolution because there is no concurrent
   automatic writer.

### Idempotence and atomicity

`plan` compares a **normalized body hash** (the same canonicalization
`faucet template show --clean` uses) against the newest local version, so a
re-pull of unchanged content is a no-op and a whitespace-only edit upstream does
not burn a version number. A pull applies per template and reports per template:
an origin that fails halfway leaves the templates it already registered in place
(each is independently valid) and reports the rest as failed. This is
*resumable* rather than transactional, which is the right trade — a partial pull
of independent templates is not a half-updated registry, and rolling back
already-appended versions would itself need deletes.

### Validation happens before anything is stored

Every remote body goes through the existing `templates::register` path, which
parses, extracts and validates `params:`, expands a placeholder-bound copy, and
compiles each row's transform chain. A malformed upstream file fails that
template's action with the ordinary error and does not reach the registry.

### Security

- Origin credentials resolve through the ordinary secrets path (`${env:}`,
  `${vault:}`, …) and are registered for redaction.
- `POST /v1/templates/sync` and `/publish` require `TemplateWrite`
  (operator+); every sync and publish writes an audit record
  (`template.sync` / `template.publish`) naming the origin, the principal, and
  the per-template actions.
- `launch: always` is called out in the docs as handing a remote repository the
  ability to change what production triggers resolve to.
- Pull fetches **configs**, which are data, not code: a config can name a
  connector but cannot introduce one. The plugin surface (#60) is compiled in,
  not loaded from a template.

### Relationship to #571

#571 is the public, git-backed catalog. This RFC is how a server consumes one:
the hub is simply a `github` origin whose repo is the hub's repo, and
`publish` is how a template gets into it. One model, two ends — no second
protocol.

## Drawbacks

- **A second way in.** The registry currently has exactly one write path
  (`register`). Sync adds a second actor writing to it, so "where did v7 come
  from?" needs the audit log to answer. Mitigated by routing every sync write
  through `register` and auditing the origin.
- **Feature and dependency weight.** GitHub fetching adds a `reqwest` call path;
  the object-store origins pull `object_store` with its cloud features. Gated
  behind `templates-sync` (+ `templates-sync-object-store`) so a default build is
  unaffected.
- **A remote can become a release lever.** With `launch: always`, merging to a
  branch changes production. That is the point of GitOps, and it is also a real
  hazard; hence it is opt-in and audited rather than the default.
- **Two places to look.** A template can now originate locally or upstream, and
  an operator must know which. The `prefix`-owns-a-namespace rule is what keeps
  that answerable at a glance.

## Rationale and alternatives

**Why pull-only with an explicit publish?** Bidirectional mirroring needs a
conflict-resolution policy, and every policy is wrong some of the time: last-write-wins
silently discards a reviewed change, and a merge of two YAML configs is not
well-defined. Worse, the registry's state is not just bodies — it is bodies *plus
an append-only launch log*, and merging two divergent launch histories has no
meaning at all. A single owner per namespace removes the question rather than
answering it.

**Alternative: a git working copy on the server.** Clone the repo and read from
disk. Rejected: it requires a git binary or a git library, persistent disk on a
server that may have none, and credential handling for a second protocol — while
the contents API gives the same result over the HTTP client already linked.

**Alternative: push-based (the remote calls the server's webhook).** Rejected as
the *primary* mechanism: it requires the server to be reachable from CI, which is
exactly what a deployed control plane usually is not, and it makes the registry's
contents depend on the delivery of an event rather than on a state that can be
re-derived. It remains an easy addition later — a webhook that triggers the same
pull — which is why the pull path is written as a callable `sync_origin` rather
than only a timer.

**Alternative: do nothing; script it.** A shell loop over `faucet template
register` is what teams do today. It re-implements body-unchanged detection
(badly, so every CI run burns a version), has no dry-run, and carries the launch
decision in whoever wrote the script.

## Prior art

- **Argo CD / Flux** — the pull-based GitOps model this follows: a repo is the
  declared state, the controller reconciles toward it, and nothing writes back.
  We deliberately take their *pull* direction but not their *prune*: they delete
  resources absent from git; we refuse to, because a deleted template silently
  repoints the release channel.
- **Terraform registries / Helm repositories** — a published, versioned artifact
  consumed by many installations, with publication an explicit release step.
  Their version immutability is the same property `register`-appends gives us.
- **Airflow DAG sync (git-sync sidecar)** — the closest analogue in the data
  world, and a cautionary one: syncing *code* means a repo compromise is remote
  execution. Syncing configs, which name connectors rather than introduce them,
  is a materially smaller blast radius.
- **dbt Hub** — a public package index with an explicit publish; #571's model.

## Unresolved questions

Must resolve before implementation:

- Whether `interval_secs` ships in v1 at all, or whether v1 is pull-on-demand
  plus pull-on-start only. (Leaning: ship the interval, default off.)

Can resolve during implementation:

- Whether the sidecar should also carry assignable-channel *moves* (`tags:`
  pointing an existing channel at the new version) or only initial tags.
- Whether `publish` should refuse to overwrite a body at the origin that differs
  from what was last pulled (a lost-update guard), or always overwrite.

## Future possibilities

- A webhook origin (`POST /v1/templates/sync/{origin}`) so CI can trigger a pull
  without waiting for the interval.
- Signature verification on pulled bodies, making `launch: always` safe against a
  compromised repo.
- Consuming #571's public hub as a first-class origin type with a curated index
  rather than a directory listing.

## Related

- [RFC process](./README.md)
- [Parameters & pipeline templates](../docs/book/src/cookbook/templates.md)
- [Documentation hub](../docs/README.md)

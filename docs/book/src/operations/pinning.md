# Pinning the faucet version

The `faucet` binary on a laptop is often not the one that runs the pipeline in
production: that may be a container image, a Kubernetes job or a long-running
`faucet serve`. Two things keep them in step:

- **`requires_faucet`** inside a config or template: the oldest faucet that
  understands it. A binary that is too old refuses the config instead of
  misreading it.
- **A project pin**: the exact faucet version the project uses, so "which
  faucet does this repo need" is answered by the repo, not by whatever is on
  the `PATH`.

This is the split Terraform uses (`required_version` in the config, an exact
version in a pin file).

## `requires_faucet`

An optional top-level key holding a semver requirement, in the syntax Cargo
uses (`>=1.13`, `^1.13.2`, `>=1.13, <2`):

```yaml
version: 1
requires_faucet: ">=1.13"
name: orders
pipeline:
  source: { type: file, config: { path: ./data/orders.csv } }
  sink: { type: file, config: { path: ./out/orders.jsonl } }
```

`faucet validate`, `faucet run`, `faucet schedule`, a `faucet serve` submit and
`faucet template register` check it against the running binary and stop with a
typed error when it is not met:

```text
this pipeline requires faucet >=1.13; this binary is 1.12.0
```

A malformed requirement is a config error at validate time. Hub source and
sink templates and deployment overlays accept the same key: `faucet run
--source X --sink Y` refuses a template whose requirement the binary does not
meet, and so does a server triggering a registered template another (newer)
instance registered.

Matching follows Cargo's rules, with one addition for pre-release binaries: a
pre-release counts as the newest release before it. `1.15.0-rc.1` satisfies
`>=1.14` but not `>=1.15`, because it may lack what 1.15.0 ships; a
requirement that names the pre-release itself (`=1.15.0-rc.1`) matches it.

Binaries released before `requires_faucet` existed do not know the key. Configs
reject unknown top-level keys, so such a binary stops with an unknown-field
error that names `requires_faucet`: also a loud failure, never a silent one.

`faucet init` writes `requires_faucet: ">=<major>.<minor>"` for the binary that
scaffolded the config. Raise it when you start using something a newer release
added; `faucet validate` on the older release tells you whether you have to.

## The project pin

`faucet init` also pins the exact version in the project's
[`mise.toml`](https://mise.jdx.dev/configuration.html), next to the config it
writes. It creates the file or adds the entry to an existing one (other tools
and comments are kept), and never replaces a faucet pin that is already there;
`faucet init --no-pin` skips it. mise installs faucet straight from the GitHub
release archives, verifying their build attestations:

```toml
[tools]
"github:faucet-hq/faucet-stream" = { version = "1.13.3", version_prefix = "faucet-cli-v" }
```

`version_prefix` is needed because faucet's release tags are named
`faucet-cli-v<version>` (one repository releases many crates). Then:

```bash
mise install                     # installs the pinned faucet
mise exec -- faucet --version    # or activate mise in your shell and run `faucet`
```

Without mise, install the pinned version with the release's installer script:

```bash
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/faucet-hq/faucet-stream/releases/download/faucet-cli-v1.13.3/faucet-cli-installer.sh | sh
```

### Containers

For container deployments the image tag is the pin. Each release publishes
`ghcr.io/faucet-hq/faucet-stream:<version>` (the `full` build) and
`:<version>-<profile>` (see [container images](../getting-started/installation.md#container-images)).
Use the same version as `mise.toml`, never `:latest`, in Kubernetes manifests,
Compose files and the Helm chart's `image.tag`. The image's entrypoint is
`faucet`, so it also runs one-off commands against a project:

```bash
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/work" -w /work \
  ghcr.io/faucet-hq/faucet-stream:1.13.3 validate pipeline.yaml
```

## Upgrading a pinned project

1. Change the version in `mise.toml` (and the image tags) and run `mise install`.
2. Run `faucet validate` on every config and `faucet test` on every spec.
3. Read [Upgrading faucet safely](upgrading.md) for what happens to stored
   state on the first run.
4. Raise `requires_faucet` only in configs that now use something the new
   release added.

The [agent skills](../getting-started/agent-skills.md) follow the same rule:
their first step is to find the pin and use that faucet.

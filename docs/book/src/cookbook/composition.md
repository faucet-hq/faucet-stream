# Config composition

Real deployments rarely run a single pipeline file. The same connection,
sink target, and transform chain are reused across dev / staging / prod, and
across many similar pipelines. **Config composition** lets you factor those
shared pieces out of each file and recombine them at load time — without
copy-pasting or templating engines.

Three mechanisms, all resolved when the file is read (before any `${...}`
interpolation runs):

| Mechanism | What it does |
|-----------|--------------|
| `extends:` | Inherit one or more **base** config files; the child deep-merges on top. |
| `profiles:` | Declare named **overlays** in the file; select one at run time with `--profile NAME` / `FAUCET_PROFILE`. |
| `!include path` | Substitute a **YAML fragment** at any node (YAML only). |

This walkthrough uses the files shipped under
[`cli/examples/compose/`](https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples/compose).

## A worked dev / staging / prod setup

### The shared base

`base.yaml` holds everything common to every environment — the source
connection and a neutral default sink — plus a `profiles:` block of
per-environment overlays that each visibly override it:

```yaml
# cli/examples/compose/base.yaml
version: 1
name: composed-pipeline
pipeline:
  source:
    type: file
    config:
      path: ./data/input.csv
  sink:
    type: file
    config:
      path: ./out/output.jsonl   # neutral default — overridden per-env by the profiles below

# Named overlays selected at run time via --profile / FAUCET_PROFILE.
# Each profile points the sink at an environment-specific file.
profiles:
  dev:
    pipeline:
      sink:
        config:
          path: ./out/dev.jsonl
  prod:
    pipeline:
      sink:
        config:
          path: ./out/prod.jsonl
```

### A reusable fragment

`transforms.yaml` is a bare YAML **sequence** — a transform chain you can pull
into any pipeline:

```yaml
# cli/examples/compose/transforms.yaml
- type: flatten
  config: { separator: "__" }
- type: keys_case
  config: { mode: snake }
```

### The pipeline that ties it together

`app.yaml` inherits the base and pulls in the transform chain with `!include`:

```yaml
# cli/examples/compose/app.yaml
extends: ./base.yaml
pipeline:
  transforms: !include ./transforms.yaml
```

Run it against an environment by selecting a profile:

```bash
faucet run cli/examples/compose/app.yaml --profile prod
```

The composed pipeline reads `./data/input.csv` (from the base), applies the
`flatten` → `keys_case` chain (from the include), and writes
`./out/prod.jsonl` (from the `prod` profile overlay). Without `--profile`,
the sink falls back to the neutral base default (`./out/output.jsonl`);
`--profile dev` redirects it to `./out/dev.jsonl`.

## `extends` — base inheritance

`extends:` names one or more base files. Relative paths resolve against the
**directory of the file that declares them**. The child document deep-merges on
top of the base (child keys win on collision).

```yaml
# Single base
extends: ./base.yaml

# A list of bases — merged left-to-right, so later bases override earlier ones,
# and the child document overrides them all.
extends:
  - ./connection.yaml
  - ./sink-defaults.yaml
```

Bases may themselves `extends:` other files; the chain is followed to its root
(a depth cap and cycle detection guard against runaway or circular includes).

## `profiles` + `--profile` / `FAUCET_PROFILE`

A top-level `profiles:` block maps a name to a partial config that is
deep-merged **over** the composed document when that profile is selected.
Nothing is applied unless a profile is chosen:

```bash
faucet run app.yaml --profile prod          # explicit flag
FAUCET_PROFILE=prod faucet run app.yaml      # via environment
```

**The flag overrides the environment variable.** `--profile prod` with
`FAUCET_PROFILE=dev` set selects `prod`. Selecting a name that isn't declared
is a clear load-time error (`unknown profile '<name>'`).

`profiles:` and `extends:` compose freely: a base can declare the profiles and
the child can select one at run time, as in the worked example above.

## `!include` — YAML fragment substitution

`!include path` (a YAML tag) replaces the node it tags with the parsed contents
of another YAML file. The fragment can be any YAML value — a sequence (as in
`transforms.yaml`), a mapping, or a scalar — and is substituted **structurally**
before the document is interpreted:

```yaml
pipeline:
  transforms: !include ./transforms.yaml   # a sequence fragment
  source: !include ./source.yaml           # a mapping fragment
```

`!include` is **YAML-only** — it is a YAML tag, so it has no equivalent in JSON
configs. Paths resolve against the including file's directory, like `extends:`.

## Precedence

Everything is merged with the same deep-merge rule used by `matrix` rows:
**objects merge recursively, arrays replace wholesale, scalars replace.** The
layers, from lowest to highest priority (last wins):

```
extended base(s)  →  child document  →  selected profile  →  matrix row
```

- `extends:` bases are the foundation (a list merges left-to-right).
- The child document (the file you ran) overrides its bases.
- The selected `profile` overlays the composed document.
- At expand time, each `matrix` row deep-merges on top — so a row can still
  override a profile-supplied value.

**Composition resolves before all `${...}` interpolation.** The full load order
is:

1. **Composition** — `extends` / `!include` are stitched, then the selected
   `profile` is overlaid; `extends:` / `profiles:` metadata keys are stripped.
2. **Interpolation** — `${env:…}` / `${file:…}` / `${secret:…}`, then
   `${vars.X}` and `${sources.X}` / `${sinks.X}`.
3. **Secrets-manager directives** — `${vault:…}` etc. (the final load-time
   stage).
4. **Expand** — `matrix` rows are deep-merged per invocation.

This ordering means a `profile` can supply a value that a later
`${env:…}`/`${vars.X}` reference is then resolved within, and that a base file
can carry `${...}` tokens resolved only after the merge.

## Inspecting the result: `validate --show-composed`

`faucet validate --show-composed` prints the fully composed config — bases
merged, the selected profile applied, fragments substituted, and the
`extends:` / `profiles:` metadata stripped — *before* `${...}` interpolation.
It's the fastest way to confirm a multi-file setup resolves to what you expect:

```bash
faucet validate cli/examples/compose/app.yaml --show-composed --profile prod
```

```yaml
version: 1
name: composed-pipeline
pipeline:
  source:
    type: file
    config:
      path: ./data/input.csv
  sink:
    type: file
    config:
      path: ./out/prod.jsonl     # ← from the prod profile
  transforms:                    # ← from the !include
  - type: flatten
    config:
      separator: __
  - type: keys_case
    config:
      mode: snake
```

## Security

**Composition is file-loads-only.** `extends`, `profiles`, and `!include` are
resolved only when `faucet` reads a config from disk (`run`, `validate`,
`preview`, `doctor`, `schedule`). They are **not** honored for configs submitted
to `faucet serve` over HTTP — a submitted body is parsed as a single,
self-contained document with no filesystem access. This keeps a multi-tenant or
internet-exposed `serve` process from being coerced into reading arbitrary local
files via a crafted `extends:` / `!include` path. Compose your config locally and
submit the result (`validate --show-composed` gives you exactly that document).

## See also

- [Config reference — composition](../reference/config.md#config-composition) — concise field grammar for `extends:`, `profiles:`, and `!include`.
- [Transforms](./transforms.md) — the full set of built-in record transforms you can include or layer per profile.
- [Secrets](./secrets.md) — interpolate secrets-manager references that survive composition unchanged until the final load stage.
- [State & resumability](./state.md) — bookmark-based incremental runs that compose cleanly with profile overlays.

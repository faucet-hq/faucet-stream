# Editor setup (autocomplete & validation)

faucet ships a JSON Schema for the whole config document, so a YAML-aware editor
can give you **autocomplete, inline documentation, and validation as you type**
while authoring a `faucet.yaml`.

## Get the schema

Generate it from your own binary (so it reflects exactly the connectors and
blocks you compiled in):

```bash
faucet schema config > faucet.schema.json
```

A prebuilt, comprehensive copy (generated under `--all-features`) is also
committed to the repository at
[`schemas/faucet.schema.json`](https://github.com/faucet-hq/faucet-stream/blob/main/schemas/faucet.schema.json).

## VS Code (YAML extension)

Install the [Red Hat YAML extension](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml),
then either add a modeline to the top of each config:

<!-- faucet:no-validate -->
```yaml
# yaml-language-server: $schema=./faucet.schema.json
version: 1
name: my-pipeline
pipeline:
  source:
    type: rest      # ← autocompletes; picking a type narrows `config:`
    config: { ... }
```

or map it globally in `.vscode/settings.json`:

```json
{
  "yaml.schemas": {
    "./faucet.schema.json": ["faucet.yaml", "faucet.yml", "**/pipelines/*.yaml"]
  }
}
```

## JetBrains IDEs

Settings → **Languages & Frameworks → Schemas and DTDs → JSON Schema Mappings**.
Add a mapping from `faucet.schema.json` to your config file(s) or a glob.

## What you get

- **Top-level grammar** — every block (`pipeline`, `matrix`, `execution`,
  `schedule`, `lineage`, `quality`, `dlq`, `resilience`, …) with its fields and
  descriptions; unknown top-level keys are flagged.
- **Connector discrimination** — the `source:` / `sink:` `type:` field
  autocompletes to the connector kinds your binary knows, and picking one
  narrows the `config:` block to that connector's fields.
- **Interpolation-tolerant** — a `${env:…}` / `${vars:…}` / `${now.*}`
  placeholder is accepted anywhere a typed value is expected, so an interpolated
  config never shows spurious type errors.

> The schema is regenerated and diff-checked in CI, so it never drifts from the
> connectors and config blocks the code actually accepts.

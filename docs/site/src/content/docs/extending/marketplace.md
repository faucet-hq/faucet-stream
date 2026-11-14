---
title: "Connector marketplace"
description: "faucet-stream is a connector marketplace: alongside the built-in connectors, anyone can publish a faucet-source- / faucet-sink- crate and have it discovered and consumed by…"
---

faucet-stream is a **connector marketplace**: alongside the built-in connectors,
anyone can publish a `faucet-source-*` / `faucet-sink-*` crate and have it
discovered and consumed by others. Three commands power this:

| Command | Purpose |
|---|---|
| `faucet search <term>` | Find connectors in the registry index by name / description / keyword / crate. |
| `faucet list --available` | List the whole registry, marking which connectors are compiled into your binary. |
| `faucet install <name>` | Print exactly how to enable/obtain a connector (never executes). |

## The registry index

The index is a committed JSON file,
[`cli/connectors/registry.json`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/connectors/registry.json),
embedded into the binary so `search` / `install` work offline and independently
of which connectors you compiled in. Each entry:

```json
{
  "name": "kafka",
  "kind": "source",
  "verified": true,
  "description": "Apache Kafka consumer with idle/max-messages termination"
}
```

- `crate` defaults to `faucet-<kind>-<name>`; `feature` defaults to `<kind>-<name>`.
- `verified: true` marks a first-party built-in; community connectors set `false`
  and give an explicit `crate`.
- Point at a custom or mirror index with `--index <path>` on any of the three
  commands.

## Installing a connector

`faucet install` inspects the entry and your binary and prints the right recipe:

- **Built-in, already compiled in** → use it directly (`type: <name>`).
- **Built-in, not compiled in** → `cargo install faucet-cli --features <kind>-<name>`.
- **Community** → a custom-binary snippet that `cargo add`s the crate and
  registers it via `PluginRegistry` (see
  [Custom binaries with third-party connectors](https://github.com/faucet-hq/faucet-stream/blob/main/cli/README.md#custom-binaries-with-third-party-connectors)).

Trust is explicit: community connectors are marked, and `install` only ever
*prints* instructions — it never downloads or runs code.

## Publishing your connector

1. Scaffold it: `faucet new connector <name> --kind source|sink` (see
   [Authoring a connector](/faucet-stream/extending/authoring-connectors/)).
2. Publish to crates.io with system-name-first keywords.
3. Open a PR adding an entry to `cli/connectors/registry.json` with
   `verified: false` so `faucet search` surfaces it.

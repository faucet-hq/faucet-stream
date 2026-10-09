# Agent skills

faucet ships [Agent Skills](https://agentskills.io/) that teach coding agents
(Claude Code, Codex and any tool that reads the format) to work with faucet:

| Skill | Use it to |
|---|---|
| `faucet-pipelines` | Write or change a pipeline config and get it through `faucet validate`. |
| `faucet-debug` | Find out why a run failed, is slow or lagging, or wrote the wrong rows. |
| `faucet-templates` | Run a Template Hub template, or write, test and publish one. |
| `faucet-connector` | Build a `faucet-source-*` or `faucet-sink-*` crate on `faucet-core`. |
| `faucet-migrate` | Move a Singer-protocol or other ELT setup to faucet and cut over safely. |
| `faucet-deploy` | Run faucet in production with `run`, `schedule` or `serve`. |

The skills live in the faucet-stream repository under
[`skills/`](https://github.com/faucet-hq/faucet-stream/tree/main/skills) and
are released with the engine: the plugin's version is the `faucet-cli` version.

## Install

**Claude Code**

```text
/plugin marketplace add faucet-hq/faucet-stream
/plugin install faucet@faucet
```

From a shell, `--sparse` keeps the checkout to the two directories the plugin
needs:

```bash
claude plugin marketplace add faucet-hq/faucet-stream --sparse .claude-plugin skills
claude plugin install faucet@faucet
```

**Codex**

```bash
codex plugin marketplace add faucet-hq/faucet-stream
codex plugin add faucet@faucet
```

**Any agent, through the skills registry**

```bash
npx -y skills add faucet-hq/faucet-stream
```

Installed plugins update when the plugin version changes, which is once per
faucet release. To follow one release exactly, add the marketplace at its tag:
`faucet-hq/faucet-stream@faucet-cli-v<version>`.

The skills used to live in `faucet-hq/faucet-skills`, under the same
marketplace name. Remove that marketplace first
(`/plugin marketplace remove faucet`), then add this one.

The agent also needs a `faucet` binary; see [Installation](installation.md),
and [Pinning the faucet version](../operations/pinning.md) for how a project
declares which one.

## Why the skills stay correct

Skills that copy facts out of the docs go stale with the next release. These
don't carry version-specific facts at all:

- **The project's faucet is the authority.** Each skill's first step finds the
  project's pin (`mise.toml`, `requires_faucet`) and uses that binary, the
  pinned container image, or installs that version.
- **Facts come from that binary.** Which connectors and blocks exist, config
  keys, types, defaults and flags come from `faucet list`, `faucet schema` and
  `--help`; a starting config from `faucet init`; the verdict from
  `faucet validate` and `faucet doctor`. A slim build only offers what it
  compiled in, and the skills never assume a connector exists.
- **Behaviour is documented here.** Skills link to this site for how the dead
  letter queue, state, delivery guarantees and the rest work.
- **Examples are tested.** Skills point at the configs in
  [`cli/examples/`](https://github.com/faucet-hq/faucet-stream/tree/main/cli/examples),
  which CI validates on every change. A CI check also fails when a connector,
  write mode, mirror mode or top-level config block has no example there.
- **Configs record what they were written for.** When a config passes
  validation, the skill sets `requires_faucet` to that faucet's minor version,
  so an older binary refuses it instead of misreading it.

## Contributing

Edit the skills in `skills/` of the faucet-stream repository; the rules for
their content are in
[`skills/README.md`](https://github.com/faucet-hq/faucet-stream/blob/main/skills/README.md).
Before opening a pull request:

```bash
python3 scripts/check_skills.py --faucet faucet
python3 scripts/example-coverage.py
```

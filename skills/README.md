# faucet agent skills

Agent Skills that teach coding agents (Claude Code, Codex and any tool that
reads the [Agent Skills](https://agentskills.io/) format) to work with faucet.
They ship with the engine: each faucet release carries the skills that match
it, and the plugin version equals the `faucet-cli` version.

Install: see [Agent skills](https://faucet-hq.github.io/faucet-stream/getting-started/agent-skills.html).

| Skill | Use it to |
|---|---|
| [`faucet-pipelines`](faucet-pipelines/SKILL.md) | Write or change a pipeline config and get it through `faucet validate`. |
| [`faucet-debug`](faucet-debug/SKILL.md) | Find out why a run failed, is slow or lagging, or wrote the wrong rows. |
| [`faucet-templates`](faucet-templates/SKILL.md) | Run a Template Hub template, or write, test and publish one. |
| [`faucet-connector`](faucet-connector/SKILL.md) | Build a `faucet-source-*` or `faucet-sink-*` crate on `faucet-core`. |
| [`faucet-migrate`](faucet-migrate/SKILL.md) | Move a Singer-protocol or other ELT setup to faucet and cut over safely. |
| [`faucet-deploy`](faucet-deploy/SKILL.md) | Run faucet in production with `run`, `schedule` or `serve`. |

## Rules for skill content

Skills give **steps, not facts**. Anything that changes between faucet
releases comes from the binary the project uses, at run time:

| Fact | Where the skill gets it |
|---|---|
| Connectors, transforms and blocks in this build | `faucet list`, `faucet schema --help` |
| Config keys, types, enum values, defaults | `faucet schema source\|sink\|transform <name>`, `faucet schema <block>` |
| A correct starting config | `faucet init --source <x> --sink <y>` |
| Whether a config is right | `faucet validate`, then `faucet doctor` |
| Commands and flags | `faucet <command> --help` |

A skill must not list config keys, defaults, connector names or flags as
prose. Engine behaviour (how the DLQ, state or delivery guarantees work) lives
in the docs site under `docs/book/src/`; a skill links to the published page
(`https://faucet-hq.github.io/faucet-stream/<path>.html`). Example configs
live in `cli/examples/` (validated by CI); a skill names them by path rather
than copying them. A fenced `yaml` block in a skill must be a complete faucet
document (it starts with `version:` or `kind:`); CI extracts and validates it.

Every skill starts with the same version step, so the agent uses the faucet
the project pins rather than whatever is on the `PATH`.

## Checks

```bash
python3 scripts/check_skills.py                       # structure, links, docs pages, example paths
python3 scripts/check_skills.py --faucet faucet       # also every quoted command and flag, and validates inline configs
python3 scripts/check_skills.py --emit-yaml out/      # write each inline faucet document to out/ for `faucet validate`
python3 scripts/plugin-version.py                     # plugin.json version equals faucet-cli
```

The connector examples under `faucet-connector/examples/` build against the
workspace `faucet-core` in CI (`Skills` job); they are not workspace members
and are never published.

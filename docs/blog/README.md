# Blog / evergreen posts

Long-form, evergreen content for cross-posting (dev.to, Hashnode, Medium,
lobste.rs) and for feeding the per-release publicity checklist in
[`../launch/RELEASE_PUBLICITY.md`](../launch/RELEASE_PUBLICITY.md). Every post is
grounded in real, shipped configs from [`cli/examples/`](../../cli/examples) —
edit voice/framing for the target platform before publishing, but keep the
technical claims accurate to the code.

| Post | Type | Grounded in |
|------|------|-------------|
| [Exactly-once delivery without a broker](exactly-once-without-a-broker.md) | Engineering deep-dive | `cli/examples/kafka_to_postgres_exactly_once.yaml`, the `delivery: exactly_once` machinery |
| [Migrating from Meltano/Singer to faucet-stream](migrating-from-meltano.md) | Migration guide | `cli/examples/rest_to_postgres.yaml`, the vs-Meltano comparison |

When you publish one, link it from the relevant `docs/site/src/content/docs/comparison/*.md`
page and add the URL to the release-publicity checklist so it gets syndicated.

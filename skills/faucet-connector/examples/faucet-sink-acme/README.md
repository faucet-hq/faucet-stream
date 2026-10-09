# faucet-sink-acme

Example faucet sink: writes records to an acme collection through a bulk endpoint, as appends or as keyed upserts.

```text
sink:
  type: acme
  config:
    base_url: https://acme.example.com/api
    token: ${env:ACME_TOKEN}
    collection: orders
    write_mode: upsert
    key: [id]
```

The block above is this connector's entry in a pipeline config; it runs from a custom `faucet` binary that registers the crate (see [Authoring a connector](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html)).

`cargo test` runs the unit tests and the `faucet-conformance` battery against a `wiremock` backend.

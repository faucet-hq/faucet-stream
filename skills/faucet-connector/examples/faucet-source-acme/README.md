# faucet-source-acme

Example faucet source: reads an acme collection over HTTP with keyset pagination (`?after=<id>&limit=<n>`) and resumes from the last committed id.

```text
source:
  type: acme
  config:
    base_url: https://acme.example.com/api
    token: ${env:ACME_TOKEN}
    collection: orders
```

The block above is this connector's entry in a pipeline config; it runs from a custom `faucet` binary that registers the crate (see [Authoring a connector](https://faucet-hq.github.io/faucet-stream/extending/authoring-connectors.html)).

`cargo test` runs the unit tests and the `faucet-conformance` battery against a `wiremock` backend.

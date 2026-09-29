# Bearer-token REST API (via the REST source)

A plain bearer-authenticated, offset-token-paginated JSON API needs no
dedicated connector crate: faucet's generic [`rest` source](../reference/connectors.md)
covers it end-to-end (issue
[#414](https://github.com/faucet-hq/faucet-stream/issues/414)). The recipe
below targets a placeholder API; swap in your own `base_url` and `path`.

## How it maps onto the `rest` source

| API concept | `rest` config |
|---|---|
| Personal access token | `auth: { type: bearer, config: { token: ${env:API_TOKEN} } }` |
| Workspace + collection URL | `base_url: https://api.example.com`, `path: /v0/<base_id>/<table>` |
| Records array | `records_path: "$.records"` |
| Offset-token pagination | `pagination: { type: Cursor, next_token_path: offset, param_name: offset }` |
| Rate limit (HTTP 429 + `Retry-After`) | built-in retry: `max_retries` / `retry_backoff` |
| Page size, server-side filters | `query_params` |

Pagination stops automatically when the response omits `offset`. Each record is
`{ id, createdTime, fields: {…} }`; the `flatten` transform collapses `fields`
into dotted top-level keys (`fields.Name`, …).

## Runnable recipe

A complete, runnable config lives at
[`cli/examples/rest_to_jsonl_bearer.yaml`](https://github.com/faucet-hq/faucet-stream/blob/main/cli/examples/rest_to_jsonl_bearer.yaml):

```yaml
version: 1
name: rest_to_jsonl_bearer

vars:
  base_id: ${env:API_BASE_ID}
  table: Contacts

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v0/${vars.base_id}/${vars.table}
      method: GET
      auth:
        type: bearer
        config:
          token: ${env:API_TOKEN}
      query_params:
        pageSize: "100"
        # Optional: push a filter server-side.
        # filter: "status != 'archived'"
      records_path: "$.records"
      pagination:
        type: Cursor
        next_token_path: offset
        param_name: offset
      # A 429 + Retry-After is retried with backoff.
      max_retries: 5
      retry_backoff: 1
      tolerated_http_errors: []
      replication_method:
        type: FullTable
      primary_keys: ["id"]
      requests: []
      schema_sample_size: 100

  # Each record is { id, createdTime, fields: {...} }. Flatten collapses
  # the nested `fields` object into dotted top-level keys (fields.Name, …) for a
  # flatter downstream shape.
  transforms:
    - type: flatten

  sink:
    type: file
    config:
      path: ./out/contacts.jsonl
```

```bash
export API_TOKEN=...       # read scope
export API_BASE_ID=...
faucet run cli/examples/rest_to_jsonl_bearer.yaml
```

## Notes

- **Nested values** — attachments and linked records come back as JSON arrays;
  they pass through as-is.
- **Incremental** — an API with no server-side change cursor can still narrow
  with a filter on a last-modified field in `query_params`, or run full-table.
- **Writing back** — this recipe is read-only.

## See also

- [Authentication](./auth.md) — the bearer-token setup this recipe relies on.
- [Pagination styles](./pagination.md) — how the REST source walks an
  `offset` cursor.

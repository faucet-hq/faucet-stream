# REST API → BigQuery (incremental)

This tutorial pulls records from a paginated REST API and streams them into a
BigQuery table, then converts it to an **incremental** pipeline that only fetches
new rows on each run.

## Full-table version

```yaml
version: 1
name: rest_to_bigquery

pipeline:
  source:
    type: rest
    config:
      base_url: https://api.example.com
      path: /v1/events
      method: GET
      name: events
      auth:
        type: basic
        config:
          username: ${env:API_USER}
          password: ${env:API_PASS}
      records_path: $.events[*]
      pagination:
        type: PageNumber
        param_name: page
        start_page: 1
        page_size: 500
        page_size_param: per_page
      max_pages: 200
      timeout: 45
      max_retries: 5
      retry_backoff: 2
      tolerated_http_errors: [404]
      replication_method:
        type: FullTable
      primary_keys: [event_id]
      schema_sample_size: 100

  sink:
    type: bigquery
    config:
      project_id: my-gcp-project
      dataset_id: analytics
      table_id: events
      auth:
        type: service_account_key_path
        config:
          path: service-account.json
      batch_size: 1000
```

Secrets come from the environment via `${env:VAR}` — keep credentials out of the
config file. Put them in a sibling `.env` or export them before running.

```bash
export API_USER=… API_PASS=…
faucet run rest_to_bigquery.yaml
```

The `records_path` is a JSONPath that selects the array of records inside each
response body; `pagination` walks pages until an empty page or `max_pages`. See
the [pagination cookbook](../cookbook/pagination.md) for the other styles.

## Make it incremental

Switch `replication_method` from `FullTable` to a key-based incremental method
and attach a state store so progress survives between runs:

```yaml
pipeline:
  source:
    type: rest
    config:
      # … as above, without max_pages …
      replication_method:
        type: Incremental
        cursor_field: updated_at
      primary_keys: [event_id]
  sink:
    # … as above …
  state:
    type: file
    config:
      path: ./state
```

Now each run records the maximum `updated_at` it saw; the next run resumes from
that bookmark. Drop `max_pages` here: a pass the cap cuts short has not seen the
rest of the feed, so it cannot record a bookmark and the run fails rather than
re-reading the same pages forever. Keep it only with `assume_ordered: true` and a
`replication_bind` that sends the bookmark to the API, for a feed sorted by
`updated_at`, where the largest value read is a safe place to resume. Swap the `file` state store for `redis` or `postgres` for shared,
durable state across machines — see [state](../cookbook/state.md).

> **Tip:** run `faucet schema source rest` and `faucet schema sink bigquery` to
> see every available config field with its type and default.

# Pagination styles (REST source)

The REST source walks multi-page responses automatically. Set `pagination.type`
to one of the styles below. `max_pages` is a hard cap across all of them, and
every style has a loop/termination guard so a misbehaving API can't loop forever.

| Style | Stops when |
|-------|-----------|
| `None` | after the first page |
| `Cursor` | the next-token JSONPath is null/absent (or repeats) |
| `CursorInBody` | the next-token JSONPath is null/absent (or repeats) — for POST-search APIs that take the cursor in the request body |
| `PageNumber` | a page returns zero records (or an identical body repeats) |
| `Offset` | the offset reaches `total` (via `total_path`) or a short page arrives |
| `OffsetInBody` | a short page arrives — offset/limit are written into the JSON **request body** (POST-query APIs) |
| `RecordFieldCursor` | a short page arrives — keyset paging by the running max/min of a **record field** |
| `LinkHeader` | there's no `rel="next"` in the `Link` response header |
| `NextLinkInBody` | the next-page URL in the body is absent, null, or empty |

> An HTTP **`204 No Content`** (or any 2xx with an empty body) is treated as an
> empty page under every style, so a feed that ends with a `204` after its last
> data page (e.g. `$top`/`$skip` paging) stops cleanly instead of erroring.

## Cursor

```yaml
pagination:
  type: Cursor
  next_token_path: $.meta.next_cursor  # JSONPath to the next-page token
  param_name: starting_after           # query param to send it back as
```

## Cursor in body (POST search)

For endpoints that page a **POST** search body — the next cursor comes back in
the response and must be written back into the request JSON body (e.g. a
CRM `POST …/objects/{obj}/search`):

```yaml
source:
  type: rest
  config:
    method: POST
    body: { limit: 100, sorts: ["hs_lastmodifieddate"] }   # base search body
    records_path: $.results[*]
    pagination:
      type: CursorInBody
      next_token_path: $.paging.next.after   # JSONPath to the next cursor
      body_cursor_field: after               # body field to inject it into
```

The first request sends `body` unchanged; each later request adds
`body[body_cursor_field] = <cursor>`. Pagination stops when the cursor is
null/absent or repeats.

`body_cursor_field` may be a JSON Pointer for a nested location, so a GraphQL
API reached through the REST source can page by `variables.after`:

```yaml
    method: POST
    body:
      query: "query($first: Int, $after: String) { orders(first: $first, after: $after) { nodes { id } pageInfo { endCursor } } }"
      variables: { first: 250 }
    records_path: $.data.orders.nodes[*]
    pagination:
      type: CursorInBody
      next_token_path: $.data.orders.pageInfo.endCursor
      body_cursor_field: /variables/after
```

A pointer must resolve to an existing value or to a new key of an existing
object (here `variables`); intermediate objects and array elements are never
created, and a pointer that does not resolve fails the request. The same
applies to `OffsetInBody`'s `offset_field` / `limit_field`, and to
`replication_bind` / `window` binds with `into: body` + `path` (see the
[state cookbook](state.md)).

## Page number

```yaml
pagination:
  type: PageNumber
  param_name: page
  start_page: 1
  page_size: 500
  page_size_param: per_page
```

## Offset / limit

```yaml
pagination:
  type: Offset
  limit: 1000
  limit_param: limit
  offset_param: offset
  total_path: $.meta.total             # optional; enables an exact stop
```

## Offset / limit in the body (POST-query APIs)

For POST endpoints that take `offset`/`limit` in the JSON **request body** (not the query string). The offset advances by each page's record count and paging stops on a short page.

```yaml
pagination:
  type: OffsetInBody
  offset_field: offset
  limit_field: limit
  limit: 500
  stop_when_short: true                # default
```

Report APIs return the whole response as **one** record (headers plus a `rows`
array, e.g. a `runReport` endpoint with `records_path: "$"`). Counting records would
read every page as short and stop after page 1, so count the rows instead, and
stop at the reported total:

```yaml
pagination:
  type: OffsetInBody
  offset_field: offset
  limit_field: limit
  limit: 10000
  rows_path: "$.rows"                  # advance by the rows on each page
  total_path: "$.rowCount"             # stop once offset reaches the total
```

`rows_path` counts an array's elements, 1 for an object, 0 for null or no match
(which ends paging). A missing or non-numeric `total_path` falls back to the
short-page rule.

## Keyset (record-field cursor)

Page by the running **max** (or **min**) of a record field — the pattern accounting journal APIs use (`offset = max(JournalNumber)` of the last page). Stops on a short page.

```yaml
pagination:
  type: RecordFieldCursor
  field: JournalNumber
  into: query                          # or `body`
  param: offset
  agg: max                             # or `min`
  page_size: 100
  stop_when_short: true
```

## Resumable cursor & multi-array responses

- **`persist_cursor: true`** on a `Cursor` / `CursorInBody` stream saves the terminal cursor as the run's bookmark (via a `state:` store) and seeds it into the next run's first request — so an envelope-cursor feed (e.g. a `/transactions/sync` endpoint) resumes incrementally instead of re-pulling from the start.
- **`records_multi`** emits several response arrays in one pass (one pagination advance), each stamped with a configurable `op_field` — pair with a sink `write_mode: upsert` + `delete_marker` to route added/modified→upsert and removed→delete from a single sync response.
- **`record_ancestors`** lifts fields from an enclosing array-element ancestor onto records unwrapped from a nested `records_path` (e.g. keep an event's envelope `id` on each unwrapped object).

See the [`faucet-source-rest` README](https://github.com/faucet-hq/faucet-stream/tree/main/crates/source/rest) for the full field reference.

## Link header

```yaml
pagination:
  type: LinkHeader      # follows the RFC 5988 `Link: <…>; rel="next"` header
```

## Next link in body

```yaml
pagination:
  type: NextLinkInBody
  next_link_path: $.links.next         # JSONPath to the absolute next-page URL
```

> Use `faucet schema source rest` to see the exact fields and defaults for each
> style in your installed version.

## See also

- [Authentication](./auth.md) — pair pagination with the right auth for your API.
- [REST API → BigQuery (incremental)](../tutorials/rest-to-bigquery.md) — a full
  paginated REST pipeline, end to end.

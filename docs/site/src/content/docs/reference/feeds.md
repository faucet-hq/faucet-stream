---
title: "Feeds"
description: "Reference for feeds in faucet serve: the feed and delivery definition, the feeds file, the ingest HTTP contract and its status codes, the management routes, status words, the faucet feed command, metrics, alerts and where the data lives."
---

Reference for [feeds](/faucet-stream/cookbook/feeds/): durable event logs in
`faucet serve` that senders push into over HTTP and deliveries read with their
own positions. The cookbook page explains how to use them; this page lists
every field, route, status and metric.

Feeds need the `feeds` Cargo feature (in `full`) and a run history that
survives a restart.

## Feed definition

One feed. It is the body of `POST /v1/feeds`, the file `faucet feed create -f`
reads, and one entry of the feeds file. `faucet schema feed` prints its JSON
Schema. Unknown keys are refused.

Durations are whole seconds as a number, or text with a unit: `90s`, `15m`,
`24h`, `7d`. Sizes are bytes as a number, or text with a unit: `512KiB`,
`64MiB`, `50GiB` (`KB`, `MB`, `GB` are powers of 1000).

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | Unique per tenant. Letters, digits, `_` and `-`, at most 64 characters. Names the feed in URLs, metrics and the runs it starts. |
| `description` | string | | What the feed is for. |
| `tenant` | string | | The tenant the feed belongs to. Its deliveries run on behalf of that tenant. Through the API the tenant comes from `?tenant=` or the caller's own tenant. |
| `store` | string | `default` | The server-defined store that holds the events. `default` follows the run history; other names come from `--feed-store`. A feed stays in the physical store that first held it: see [Moving a feed's store](#moving-a-feeds-store). |
| `ingest` | object | see below | How senders push events in. |
| `retention` | object | see below | How long events are kept. |
| `seal` | bool | on for a tenant's feed, else off | Seal event payloads with the server's vault key. |
| `deliveries` | list | `[]` | Up to 16 deliveries. |

### `ingest`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `auth.tokens` | `managed` | | Write-only feed tokens issued through the API, stored hashed. |
| `auth.signature` | object | | An HMAC signature over the request body (below). |
| `auth.allowed_addresses` | list of strings | `[]` (any) | Sender addresses or CIDR ranges that may push. |
| `accept` | list of strings | `application/json`, `application/x-ndjson`, `application/x-www-form-urlencoded`, `text/plain` | Accepted request content types. A form's repeated key is delivered as an array of its values in the order sent. An event (JSON object, array element, NDJSON line or form) with a top-level `_feed` field is refused with `400`: the key is reserved for the delivery's metadata. |
| `max_event_bytes` | size | `1MiB` | Largest single event. |
| `max_request_bytes` | size | `10MiB` | Largest request body, after any gzip expansion. Capped by the server's `--feeds-max-request-bytes`. Must be at least `max_event_bytes`. |
| `dedupe` | object | | Store a retried event once (below). |
| `rate_limit.events_per_sec` | integer | | Events a second this feed accepts, sustained, per server. |
| `rate_limit.burst` | integer | `events_per_sec` | Events accepted in a burst. |
| `url_token` | bool | `false` | Also accept the feed token in a `?token=` query parameter. |

A feed needs at least one of `auth.tokens` and `auth.signature`. With both, a
request must satisfy both.

**`auth.signature`**

| Field | Type | Default | Meaning |
|---|---|---|---|
| `header` | string | required | The request header that carries the signature. |
| `secret` | string | required | The shared signing secret. In the feeds file, use a `${env:…}` or secret reference. Through the API it is sealed with the vault key (a server without one refuses it) and read back as `$sealed`; sending `$sealed` back keeps the stored secret. A server whose vault key cannot open the stored secret (another key, a key rotated without `--vault-previous-key`) still lists, edits and deletes the feed, shows it as `secret_unreadable`, and answers every ingest request to it with `503` (reason `seal`, with `Retry-After`) so senders retry against a server that can; for a feed with no token that answer is given before any check, so it tells a caller the feed exists. Sending `$sealed` back for such a feed is refused (`422`); send the secret itself to repair it. |
| `algorithm` | `sha256` \| `sha512` | `sha256` | The HMAC digest. |
| `encoding` | `hex` \| `base64` | `hex` | How the digest is written in the header. Hex is matched without regard to case. |
| `prefix` | string | | Text before the digest in the header value, such as `sha256=`. |
| `timestamp_header` | string | | A header carrying the Unix-seconds time the sender signed. When set, the signed content is `<timestamp>.<body>` and a timestamp more than `tolerance_secs` from now is rejected. |
| `tolerance_secs` | integer | `300` | Accepted clock difference for `timestamp_header`. |

**Replays.** With `timestamp_header`, a signed request that arrives again
while its timestamp is still accepted is stored once: the second copy is
answered `202` with `"duplicate": true` and the first copy's ids. This holds
on every server of a cluster, because the feed store keeps the record. An
event that carries its own dedupe key (`dedupe:`) is matched by that key
instead. Without `timestamp_header` a captured signed request can be sent
again at any time and is stored again unless the feed has a `dedupe:` key;
creating such a feed answers with a `warnings` entry (and `faucet feed
create` prints it), and the feed is still created.

**`dedupe`**

| Field | Type | Default | Meaning |
|---|---|---|---|
| `header` | string | | The request header that carries the event's key. Set exactly one of `header` and `body_path`. |
| `body_path` | string | | A dot path into a JSON event that holds its key (`id`, `event.id`). The value must be a string, a number or a boolean. |
| `window` | duration | `24h` | How long a key is remembered. |
| `on_duplicate` | `accept` \| `conflict` | `accept` | What the sender of a duplicate is told: `202` with `duplicate: true`, or `409`. |

With `header`, a request of several events uses `<value>:<index>` as each
event's key. A key longer than 256 bytes, or holding a control character
(a NUL, a tab, a newline), is stored as its SHA-256 (`sha256:<hex>`, also what
the event browser shows); equal keys still match. An event without a key is
stored without dedupe.

Requests that arrive together are stored in one transaction. When the store
refuses that transaction before committing it, each request is tried alone,
so a request the store cannot take fails by itself (`503 store`) and the
others are stored once. When the store fails at or after the commit (the
outcome is unknown), every request of the transaction gets `503` and is
retried by its sender.

### `retention`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `max_age` | duration | `7d` | How long a delivered event stays replayable. |
| `undelivered.max_age` | duration | `30d` | Oldest undelivered event. Past it the feed refuses new events. |
| `undelivered.max_bytes` | size | `50GiB` | Most undelivered bytes. Past it the feed refuses new events. |
| `warn_at` | list of fractions | `[0.5, 0.8]` | Ascending fractions (between 0 and 1) of the undelivered limits at which `feed_near_full` is sent. |

Retention runs on every server beside the delivery scheduler (never inside its
pass), for one feed at a time per feed (a claim in the feed store): once a
minute it removes every event older than `max_age` below every delivery's
position and every expired dedupe key, in calls of at most 1,000 rows. A pass
works for at most 15 seconds over all feeds, starting where the last one
stopped; a feed it could not finish is due again a second later, so
retention keeps up with the feed's rate. `faucet_feed_maintenance_behind`
(and the `FaucetFeedMaintenanceBehind` alert) report a feed whose retention
keeps running out of time.

### Delivery

| Field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | Unique within the feed. Letters, digits, `_` and `-`. |
| `template` | object | | The registered template each run uses. Its source must be `type: feed`. Set `template` or `pipeline`, not both. |
| `pipeline` | string | | A pipeline config file on the server, relative to the feeds file. **Feeds file only**; the API refuses it. Its source must be `type: feed`. |
| `flush.max_events` | integer | `5000` | Start a run at this many undelivered events. |
| `flush.max_bytes` | size | `64MiB` | Or at this many bytes. |
| `flush.max_age` | duration | `60s` | Or when the oldest undelivered event is this old. |
| `flush.min_interval` | duration | `10s` | Shortest gap between two runs. A tenant's feed is also held to `--tenant-delivery-min-interval-secs`. |
| `max_batch.events` | integer | `10000` | Most events one run delivers. |
| `max_batch.bytes` | size | `64MiB` | Most payload bytes one run delivers. |
| `start` | string | `earliest` | Where a new delivery starts: `earliest` (the oldest retained event), `latest` (after the last stored event), `seq:<n>`, or `time:<RFC 3339>`. |
| `retries` | integer | `5` | Failed runs in a row of a single event, with other events waiting behind it, before the delivery stalls; the failures must also span at least 10 minutes. |
| `pool` | string | | The [execution pool](/faucet-stream/cookbook/cluster/) the runs go to. |
| `timeout_secs` | integer | `3600` | Per-run timeout. |
| `labels` | map | `{}` | Static labels added to every run. `feed`, `feed_delivery` and `feed_claim` are set by the server and cannot be used. |
| `paused` | bool | `false` | Keeps its position but starts no runs. |

**`template`**

| Field | Type | Default | Meaning |
|---|---|---|---|
| `id` | string | required | Template id. |
| `version` | number or channel | `stable` | A version number pins it; a channel is resolved at each run. |
| `params` | map | `{}` | Values for the template's `params:`. In an API feed, secret params are sealed with the vault key and read back as a marker. In a `--feeds` file they stay in the file (not sealed): a param the template declares secret is shown as the same marker (every param, when the template cannot be read) and its value is scrubbed from the logs, and any string of the file's feed that held a `${…}` reference is shown as the reference (`${env:API_KEY}`), never its value. |
| `sink`, `sink_version` | string, number or channel | | For a source template: the sink template to compose in. |
| `overlay`, `overlay_version` | string, number or channel | | A registered deployment overlay. |

A delivery's pipeline must be exactly one root row that reads the feed. A
matrix, `partition`, `shard`, `fan_out`, `parent`, `complete_for` and a node
graph are refused, and so is setting the source's `feed`, `delivery` or
`store`: the server supplies them. Each delivery run's pipeline name is the
config's name with the delivery's short id appended, so two deliveries that
share a template never share state, a bookmark or an exactly-once watermark.

The `feed` source's own settings (`page_events`, `include_metadata`) are in
the [connector README](https://github.com/faucet-hq/faucet-stream/blob/main/crates/source/feed/README.md).

## Feeds file

`faucet serve --feeds <path>` (YAML or JSON). `faucet schema feeds` prints its
JSON Schema.

| Field | Type | Meaning |
|---|---|---|
| `version` | integer | Must be `1`. |
| `feeds` | list | Feed definitions. Names are unique per tenant. |

`${env:…}`, `${file:…}` and secret-manager references are resolved when the
file is loaded. The file, every `pipeline` file it names and its signature
secrets are protected from configs submitted to the server. A missing
`pipeline` file, or one that is not a single root row reading the feed, fails
startup. Feeds from the file are read-only at runtime (edit, delete: `409`);
their deliveries can still be paused, resumed, replayed, skipped and
recovered, and their tokens are issued through the API.

## Ingest

| Method | Path | Feed |
|---|---|---|
| `POST` | `/v1/feeds/{name}/events` | A server feed |
| `POST` | `/v1/tenants/{tenant}/feeds/{name}/events` | A tenant's feed |

These routes sit outside the server's bearer authentication and its role
checks. They accept a feed credential only:

| Credential | How it is sent |
|---|---|
| Feed token | `Authorization: Bearer <token>` (the bare token is accepted too), or `?token=<token>` when the feed sets `url_token: true` |
| Signature | The header named by `auth.signature.header` |

With `--feeds-listen` the routes (and `/healthz`) are served on that listener
only.

### Request body

| `Content-Type` | Events |
|---|---|
| `application/json` or any `…+json`: a JSON array | One event per element |
| `application/json` or any `…+json`: any other value | One event |
| `application/x-ndjson`, `application/ndjson`, `application/jsonl`, `application/jsonlines`, `application/x-jsonlines` | One event per non-empty line |
| Any other type the feed accepts | One event, stored as sent |

The content type must be one the feed's `accept` lists. JSON and NDJSON must
parse and nest no deeper than 64 levels; `text/*` must be UTF-8. A body may
carry `Content-Encoding: gzip`; any other encoding is refused. The signature
covers the bytes as sent, and every size limit applies to the expanded bytes.

Each event is stored with its content type, the time it arrived, and these
request headers when present: `User-Agent`, `X-Request-Id` and the feed's
dedupe header.

### Limits

| Limit | Value |
|---|---|
| Request body | The feed's `ingest.max_request_bytes` (default 10 MiB), capped by `--feeds-max-request-bytes` (default 64 MiB) |
| One event | The feed's `ingest.max_event_bytes` (default 1 MiB) |
| Events per request | 10,000 |
| JSON nesting | 64 levels |
| Time to send the body | `--feeds-request-timeout-secs` (default 30) |
| Requests handled at once, per server | `--feeds-max-concurrent-requests` (default 256) |
| Requests of a signature-only feed being read before they are verified | 32 per feed, per server |
| Rate | The feed's `ingest.rate_limit` and the tenant's `max_feed_events_per_sec`, per server |

### Answer

```json
{"ids": ["0193…", "0193…"], "first_seq": 41, "last_seq": 42}
```

`ids` has one id per event, in request order. When some events were
duplicates, the body also has `duplicates` (how many) and `duplicate` (`true`
when every event was one). The answer is sent after the events are committed
to the feed store.

### Status codes

Errors have the body `{"error": {"code": "…", "message": "…"}}`.

| Status | `code` | When | `Retry-After` |
|---|---|---|---|
| `202` | | The events are stored (or were duplicates, under `on_duplicate: accept`). | |
| `400` | `bad_request` | An empty body, a body that could not be read, invalid JSON, JSON nested too deep, text that is not UTF-8, invalid gzip, or no event in the body. | |
| `401` | `unauthorized` | An unknown feed, a missing, wrong, expired or revoked token, a bad signature, or a sender address the feed does not allow. All four answer the same. | |
| `408` | `timeout` | The body did not arrive in time. | |
| `409` | | Every event was a duplicate and the feed sets `on_duplicate: conflict`. The body is the normal answer with `duplicate: true`. | |
| `413` | `too_large` | The body (as sent or expanded) is over the request limit, one event is over the event limit, or the request has more than 10,000 events. | |
| `415` | `unsupported_media_type` | A content type the feed does not accept, or a `Content-Encoding` other than gzip. | |
| `429` | `rate_limited` | The feed's or the tenant's rate limit. | seconds until it would be accepted |
| `503` | `unavailable` | The feed is full (an undelivered limit or the tenant's byte quota); the feed store is unavailable; the server is at its concurrent-request limit, cannot read its feed definitions, cannot seal a sealed feed's events, or is shutting down. | `60` when full or unsealable, `5` for the store, stale definitions and shutdown, `1` when busy |

A request is all or nothing: on any status but `202` (and `409`) nothing from
it was stored. On shutdown the routes answer `503` while requests already
being stored are finished and answered.

A sender should retry `408`, `429` and `503`, and fix the request on `400`,
`413` and `415`.

## Management routes

All under the server's bearer authentication. `?tenant=<id>` names the tenant
whose feed is meant (a tenant-scoped principal's own tenant is implied, and it
reaches no other). On the routes that change a definition, `?reason=` is kept
on the change request when the server gates the change.

| Method | Path | Permission | Audit action |
|---|---|---|---|
| `GET` | `/v1/feeds` | `FeedRead` | `feed.list` |
| `GET` | `/v1/feeds/{name}` | `FeedRead` | `feed.get` |
| `POST` | `/v1/feeds` | `FeedWrite` | `feed.create` |
| `PUT` | `/v1/feeds/{name}` | `FeedWrite` | `feed.update` |
| `DELETE` | `/v1/feeds/{name}` | `FeedAdmin` | `feed.delete` |
| `POST` | `/v1/feeds/{name}/deliveries` | `FeedWrite` | `feed.delivery_set` |
| `PUT` | `/v1/feeds/{name}/deliveries/{delivery}` | `FeedWrite` | `feed.delivery_set` |
| `DELETE` | `/v1/feeds/{name}/deliveries/{delivery}` | `FeedWrite` | `feed.delivery_delete` |
| `POST` | `/v1/feeds/{name}/deliveries/{delivery}/pause` (`{"reason"}`, optional) | `FeedWrite` | `feed.pause` |
| `POST` | `/v1/feeds/{name}/deliveries/{delivery}/resume` | `FeedWrite` | `feed.resume` |
| `POST` | `/v1/feeds/{name}/deliveries/{delivery}/deliver-now` | `FeedWrite` | `feed.deliver_now` |
| `POST` | `/v1/feeds/{name}/deliveries/{delivery}/replay` (`{"seq"}` or `{"time"}`) | `FeedAdmin` | `feed.replay` |
| `POST` | `/v1/feeds/{name}/deliveries/{delivery}/skip` (`{"reason", "to"?}`) | `FeedAdmin` | `feed.skip` |
| `POST` | `/v1/feeds/{name}/deliveries/{delivery}/recover` | `FeedAdmin` | `feed.recover` |
| `GET` | `/v1/feeds/{name}/tokens` | `FeedRead` | `feed.token_list` |
| `POST` | `/v1/feeds/{name}/tokens` (`{"name", "expires_at"?}`) | `FeedWrite` | `feed.token_create` |
| `DELETE` | `/v1/feeds/{name}/tokens/{id}` | `FeedWrite` | `feed.token_revoke` |
| `GET` | `/v1/feeds/{name}/events[?before=&from=&limit=]` | `FeedRead` | `feed.events_list` |
| `GET` | `/v1/feeds/{name}/events/{seq}` | `FeedAdmin` | `feed.events_read` |
| `GET` | `/v1/feeds/{name}/refusals[?limit=]` | `FeedRead` | `feed.refusals` |

`FeedRead` is granted to viewers and up, `FeedWrite` to operators and up, and
`FeedAdmin` to admins.

- **Create** answers `201` with the feed's view; **update** `200`;
  **delete** `204`. Under `--require-approval feed` (and, for a change that
  adds or edits a delivery, under `--require-approval run`) each answers `202`
  with the pending change request.
- Deleting a feed or a delivery while one of its runs is in flight answers
  `409` naming the run. Deleting a feed removes its events, positions and
  tokens, each delivery's status and the state keys its runs kept; deleting
  a delivery removes its position, its status and its state keys. Rows a
  sink keeps for a delivery (a commit-token row, for example) stay.
- A delete is recorded in the feed's definition before anything is removed.
  From that moment the feed is not listed, answers `404` to the API and
  `401` to senders, and starts no runs; a deleted feed's name can be used
  again once the removal has finished (`409` until then). If the server
  stops or a store fails partway, the scheduler of any server finishes the
  removal on its next pass, so a `204` means "deleted", and the clean-up may
  still be running for a few seconds.
- A feed from the `--feeds` file answers `409` to update and delete.
- `GET /v1/feeds` answers `{feeds, stores, approval, quotas?}`: `stores` are
  the names a new feed may use, `approval` is `always`, `deliveries` or
  `never`, and `quotas` is the tenant's when the listing is for one tenant.
- `GET /v1/feeds/{name}` adds the feed's `gaps`, `tokens` and its change log.
- **Replay** takes exactly one of `seq` (the event to deliver next) and `time`
  (the first event stored at or after it), and must go back. **Skip** must go
  forward, at most to the feed's end; `to` is the next event to read and
  defaults to one past the position. Replay, skip and recover answer
  `{from, to, epoch}`, and `409` while the delivery has a run in flight.
- **Token create** answers `201` with the token's record and its text in
  `token` (`fdt_` and 64 hex characters). The text is never shown again; lists
  show the first 10 characters as `prefix`.
- **Events** lists retained events without payloads, newest first: 50 by
  default, at most 200; `next_before` is the cursor of the next (older) page.
  Each has `seq`, `id`, `received_at`, `stored_at`, `content_type`, `size`,
  `sealed`, `headers`, `dedupe_key` and `delivered_by` (the deliveries past
  it). Reading one event returns its `payload` with `payload_encoding`
  (`utf8` or `base64`); a sealed event's payload is withheld.
- **Refusals** answers `{scope, refusals}`: the requests this server refused
  lately for the feed (at most 200, kept in memory, lost on restart) with
  time, reason, status, sender address and message, never a payload or a
  credential. `scope` is always `this_server`: behind several replicas each
  one lists what it refused itself, and `faucet_feed_ingest_refused_total`
  counts refusals across all of them.

The [MCP server](/faucet-stream/cookbook/mcp/) has two read tools,
`list_feeds` and `get_feed`, for callers that hold `FeedRead`.

## Status words

**A delivery**

| Status | Meaning | Runs start |
|---|---|---|
| `ok` | Delivering. | yes |
| `failing` | Three or more runs in a row failed. It keeps retrying with a smaller batch. | yes |
| `stalled` | Runs of a single event that held other events back failed `retries` times in a row, over at least 10 minutes. A lone pending event that keeps failing shows `failing` and keeps being retried instead. | no, until resume, skip or replay |
| `held` | Its bookmark and the feed store disagree (a restored store). | no, until recover |
| `paused` | Paused by an operator, by its definition, or because its run was cancelled or rolled back. | no, until resume |
| `needs_approval` | The server requires approval for runs and this delivery has none (or follows a channel). | no |
| `setting_up` | The delivery is defined but the feed store has no position for it yet (the store call failed when it was made). The scheduler creates it, at the delivery's `start`, on its next pass. | no, until it is set up |

A delivery's last run has one of these outcomes: `delivered`, `failed`,
`refused`, `cancelled`, `claim_lost`, `held`.

**A feed** shows the worst of its deliveries: `held`, then
`secret_unreadable` (this server cannot open the feed's signature secret and
refuses its senders with `503`), then `stalled`, then
`full` (an undelivered limit is reached and new events are refused), then
`over_quota` (its tenant's feeds in this feed store hold the tenant's whole
`max_feed_bytes`, delivered events kept for replay included, so new events
are refused), then `paused` (every delivery is paused), else `receiving`.

A feed reports two sizes. `retained_bytes` is every event it still holds,
including delivered events kept for replay. `undelivered_bytes` is what its
slowest delivery has not delivered yet; the undelivered limit, the
`undelivered_ratio`, `feed_near_full` and the `full` status count only that.
A delivery's byte flush (`flush.max_bytes`) likewise counts the bytes from
its own position on.

On a feed's own page (`GET /v1/feeds/{name}`) each delivery also carries
`undelivered_bytes` (the payload bytes from its own position on; absent
when the store cannot count them), `last_delivered_at` (when a run last
moved its position) and, while it waits out a back-off after a failed or
refused run, `next_run_at` (no run starts before it).

### Moving a feed's store

Each feed store records an identity when its schema is first created (a
restored backup keeps it; a database recreated empty gets a new one), and the
server records which store holds each feed's rows. When a feed's definition
(a `--feeds` file's `store:`), or the `--feed-store` mapping of its store
name, comes to name a different physical store, the feed is not re-created
there empty: it shows the status `store_moved` (worst after `held`), its
senders get `503` (reason `store_moved`, with `Retry-After`), nothing is
delivered or created for it, `/readyz` lists it under `store_moved` and
`faucet_feed_store_moved` is `1` (alert `FaucetFeedStoreMoved`). A server
whose `--feeds` file has such a feed refuses to start, naming it.

Point the definition back at the old store, or adopt the new one:
`faucet feed adopt-store <feed>` or `POST /v1/feeds/{name}/adopt-store`
(admin, audited as `feed.adopt_store`) starts the feed empty in the store it
names: its deliveries' run state is removed and each starts at its `start`.
Its events in the old store are not delivered. `faucet feed adopt-store
--all [--in <store>]` adopts every such feed at once, for a store that was
lost and recreated. A store that keeps no identity (a third-party backend)
cannot be checked; the server logs that once.

**Self-repair.** On every pass the scheduler compares each feed's store rows
with its definition: a delivery position no definition lists (a change that
stopped halfway) is deleted unless a run holds it, a defined delivery with no
position is created, and a changed tenant byte quota is written to the
tenant's existing feeds. A feed marked deleted, or a delivery dropped from
its feed, whose removal did not finish is finished here too, on whichever
server gets to it first.

## `faucet serve` flags

| Flag | Meaning |
|---|---|
| `--feeds <path>` | The feeds file. Refused with `--history memory`. |
| `--feed-store <name>=<url>` | Define a named feed store: `sqlite:<path>` or `postgres://…` (repeatable). `default` follows the run history and cannot be redefined. |
| `--feeds-listen <addr>` | Serve the ingest routes and `/healthz` on their own listener; the main listener then serves no ingest route. Env `FAUCET_SERVE_FEEDS_LISTEN`. |
| `--feeds-allowed-host <host>` | A host name the feeds listener answers for (repeatable; `*` = any). Without one it answers for any host. Needs `--feeds-listen`. |
| `--feeds-max-request-bytes <n>` | The largest ingest body the server reads, whatever a feed says. Default 64 MiB. |
| `--feeds-request-timeout-secs <n>` | How long a request may take to send its body. Default 30. |
| `--feeds-max-concurrent-requests <n>` | Ingest requests handled at once. Default 256. |
| `--tenant-delivery-min-interval-secs <n>` | Shortest gap between two runs of a tenant feed's delivery. Default 10. |
| `--require-approval feed` | Gate feed changes behind change requests. |
| `--vault-key` / `FAUCET_VAULT_KEY` | Needed for sealed payloads, signature secrets of API-made feeds and secret delivery params. |

## `faucet feed`

Manages feeds straight in their stores, the way `faucet template` manages
templates. It talks to no server (except `send`), and it starts no delivery
runs: a server does. It sees the feeds stored in the run history, and the
feeds of a server's `--feeds` file when the same file is passed with
`--feeds`. A file feed can then be listed, shown, pushed to, tailed, given
tokens, and have its deliveries paused, resumed, replayed, skipped or
recovered. `create`, `apply` and `delete` refuse a file feed: it changes in
the file.

| Global flag | Meaning |
|---|---|
| `--store <url>` | The run history the server keeps its feeds in: `sqlite:<path>` or `postgres://…`. Env `FAUCET_FEED_STORE`. The feed store follows it. |
| `--feed-store <name>=<url>` | A named feed store, as given to `faucet serve` (repeatable). |
| `--feeds <path>` | The feeds file the server runs with. It is loaded as the server loads it, so the environment variables and secrets it references must resolve. |
| `--vault-key <key>` | The server's vault key. Env `FAUCET_VAULT_KEY`. |
| `--tenant <id>` | The tenant whose feeds are meant. |
| `--json` | Print JSON. |

| Verb | Does |
|---|---|
| `list` | Feeds with their status, size and pending events. |
| `show <feed>` | One feed: deliveries and their positions, gaps, tokens. |
| `create -f <file>` | Create a feed from a definition (`-` = stdin). |
| `apply -f <file>` | Create the feed the file names, or replace it when it exists. |
| `delete <feed>` | Delete a feed with its events, positions and tokens. |
| `pause <feed> <delivery> [--reason <text>]` | Pause a delivery. |
| `resume <feed> <delivery>` | Resume a delivery and clear a stall. |
| `deliver-now <feed> <delivery>` | Have a server start the next run without waiting for the flush policy. |
| `replay <feed> <delivery> --to-seq <n>` \| `--to-time <RFC 3339>` | Move a delivery back. |
| `skip <feed> <delivery> --reason <text> [--to <n>]` | Move a delivery forward, leaving a gap. |
| `recover <feed> <delivery>` | Put a held delivery back on a valid position. |
| `adopt-store <feed>` \| `adopt-store --all [--in <store>]` | Start a feed whose events are in another feed store empty in the store it names (one feed, or every such feed of the server or of one store). Its events in the old store are not delivered. |
| `events <feed> [--before <n>] [--from <n>] [--limit <n>]` | List retained events, never their payloads. |
| `tail <feed> [-n <N>] [--follow] [--interval-secs <n>] [--payload]` | Print the newest retained events, oldest first (10 by default, at most 200). `--follow` keeps printing new ones, looking every `--interval-secs` (default 2); use it on a PostgreSQL feed store, because nothing else can write to a SQLite store while `tail` holds it. `--payload` prints payloads too, never a sealed event's. |
| `stats <feed>` | The feed's size, what the last 24 hours brought, and each delivery's backlog. |
| `cursor <feed> [<delivery>]` | Where each delivery reads next: position, epoch, pending events, live claim and gaps. Read-only. |
| `push <feed> [<file>] [--content-type <type>] [--dedupe-key <key>]` | Append events from a file or stdin straight to the feed's store, with the checks, dedupe and sealing the ingest route applies. No sender credential or rate limit applies; the tenant's byte quota and the feed's undelivered limits do (the store checks them). |
| `send --url <ingest URL> …` | Post events to a running server over HTTP, as a sender does. Needs no `--store`. |
| `token create <feed> <token name> [--expires-at <RFC 3339>]` | Issue a token; its text is printed once. |
| `token list <feed>` | The feed's tokens, never their text. |
| `token revoke <feed> <id>` | Revoke a token. |

`send` goes through the ingest route, so the feed's credential, limits, rate
limit and tenant quota all apply; `push` writes to the store directly and
skips the credential and the rate limits (the tenant's byte quota and the
feed's undelivered limits are checked by the store, so they still apply). `send` flags: `-f <file>` (default stdin), `--content-type` (default
`application/json`), `--token-env <VAR>` (or `--token`, which other processes
on the host can see), `--signature-secret-env <VAR>` with
`--signature-header <name>` and, to match the feed's signature settings,
`--signature-algorithm` (`sha256`), `--signature-encoding` (`hex`),
`--signature-prefix`, `--signature-timestamp-header`; `--header NAME=VALUE`
(repeatable, for a dedupe header), `--gzip`, `--timeout-secs` (30).

A SQLite feed store takes one process at a time. While a server is running on
it, use the API instead of the verbs that read or write events.
A PostgreSQL store has no such limit.

`faucet schema feed` prints the JSON Schema of one feed definition, and
`faucet schema feeds` that of the feeds file.

## Notifications

| Event | Sent when |
|---|---|
| `feed_near_full` | Undelivered data crosses one of the feed's `retention.warn_at` fractions. Once per crossing across every server; it can send again after the feed drained below. |
| `feed_stalled` | A delivery stalls. Once per stall. |

Both go through the [`notifications:`](/faucet-stream/cookbook/notifications/)
block of each delivery's pipeline and, for a tenant's feed, the tenant's.

## Metrics

The `feed` label is the feed's name, `<tenant>/<name>` for a tenant's feed.

| Metric | Type | Meaning |
|---|---|---|
| `faucet_feed_ingest_requests_total{feed,status}` | counter | Ingest requests by HTTP status. A request for an unknown feed has an empty `feed`. |
| `faucet_feed_ingest_refused_total{feed,reason}` | counter | Refused requests by reason: `unauthorized`, `malformed`, `too_large`, `timeout`, `content_type`, `content_encoding`, `rate_limited`, `tenant_rate_limited`, `full`, `store`, `store_moved`, `seal`, `busy`, `definitions_stale`, `draining`. |
| `faucet_feed_events_total{feed}` | counter | Events stored. |
| `faucet_feed_bytes_total{feed}` | counter | Payload bytes stored. |
| `faucet_feed_dedupe_hits_total{feed}` | counter | Events not stored because their key was already seen. |
| `faucet_feed_ingest_replays_total{feed}` | counter | Signed requests received again inside their timestamp tolerance and not stored again. |
| `faucet_feed_setup_total{feed,delivery}` | counter | Feeds (`delivery` empty) and deliveries found defined but missing from the store, which the scheduler then creates. |
| `faucet_feed_orphan_deliveries_total{feed}` | counter | Delivery positions deleted from the store because no definition listed them. |
| `faucet_feed_append_duration_seconds{feed}` | histogram | Time of one store transaction. |
| `faucet_feed_append_group_requests{feed}` | histogram | Requests stored together in one transaction. |
| `faucet_feed_store_errors_total{feed}` | counter | Ingest appends the feed store failed (senders got `503 store`). |
| `faucet_feed_depth{feed}` | gauge | Events the feed's slowest delivery has not delivered yet. |
| `faucet_feed_undelivered_ratio{feed}` | gauge | Undelivered bytes or age as a fraction of the feed's limits (the larger). At 1 the feed refuses new events. |
| `faucet_feed_delivery_lag{feed,delivery}` | gauge | Events the delivery has not read. |
| `faucet_feed_oldest_undelivered_seconds{feed,delivery}` | gauge | Age of the oldest undelivered event. |
| `faucet_feed_runs_total{feed,delivery,outcome}` | counter | Delivery runs by outcome: `delivered`, `failed`, `refused`, `cancelled`, `claim_lost`, `held`, `needs_approval`. |
| `faucet_feed_delivery_stalled{feed,delivery}` | gauge | `1` while the delivery is stalled. |
| `faucet_feed_held{feed}` | gauge | Held deliveries of the feed. |
| `faucet_feed_store_moved{feed}` | gauge | `1` while the feed's events are in another feed store than the one it names. |
| `faucet_feed_gaps_total{feed}` | counter | Gaps recorded by a skip or a recover. |
| `faucet_feed_trimmed_events_total{feed}` | counter | Events removed by retention. |
| `faucet_feed_maintenance_behind{feed}` | gauge | `1` while the feed's last retention pass ran out of time with expired events or dedupe keys left. |
| `faucet_feed_scheduler_restarts_total` | counter | Times the delivery scheduler was restarted after it failed. |

A deleted feed's or delivery's gauges go back to zero, so its alerts clear.

### Alerts

The shipped rules (`observability/prometheus/alerts.yml`, group
`faucet-feeds`):

| Alert | Fires when | Severity |
|---|---|---|
| `FaucetFeedDeliveryStalled` | `faucet_feed_delivery_stalled == 1` for 1 minute | critical |
| `FaucetFeedDeliveryHeld` | `faucet_feed_held > 0` for 1 minute | critical |
| `FaucetFeedStoreMoved` | `faucet_feed_store_moved == 1` for 1 minute | critical |
| `FaucetFeedMaintenanceBehind` | `faucet_feed_maintenance_behind == 1` for 15 minutes | warning |
| `FaucetFeedDeliveryLagging` | The oldest undelivered event has waited more than 10 minutes, for 10 minutes | warning |
| `FaucetFeedNearFull` | `faucet_feed_undelivered_ratio >= 0.8` for 5 minutes | warning |
| `FaucetFeedRunsFailing` | Three or more runs failed or were refused in 30 minutes | warning |
| `FaucetFeedStoreErrors` | `faucet_feed_store_errors_total` kept growing for 5 minutes: appends to the feed's store fail and senders get `503` | critical |
| `FaucetFeedIngestErrors` | More than 5% of a feed's ingest requests were answered with a `5xx` over 5 minutes | warning |
| `FaucetFeedIngestRefusals` | More than 20% of a feed's ingest requests were refused for one sender-side reason (`unauthorized`, `malformed`, `too_large`, `content_type`, `content_encoding`, `rate_limited`, `tenant_rate_limited`, `timeout`) over 15 minutes | warning |

The two ingest alerts carry an empty `feed` label for requests no feed
admitted (an unknown feed or a credential no feed accepts).

### `/readyz`

`/readyz` has a `feeds` object: `started` (a feed exists, so the scheduler
runs here), `store_ok`, `feeds` (how many), the `stalled` and `held`
deliveries by name, and `secret_unreadable` (feeds whose signature secret this
server cannot open) when there are any. An unreachable feed store sets `store_ok: false`; it does
not fail the readiness check.

## Where the data lives

| Data | Where |
|---|---|
| Feed definitions made through the API, with their deliveries and change log | Run history, table `faucet_feeds` |
| Token hashes | Run history, table `faucet_feed_tokens` |
| Each delivery's status (batch size, failures, pause, live claim), the ids of feeds-file feeds, the warning counter | Run history settings |
| Events, positions, claims, dedupe keys, gaps | The feed store: tables `faucet_feed_events`, `faucet_feed_heads`, `faucet_feed_deliveries`, `faucet_feed_dedupe`, `faucet_feed_gaps`, `faucet_feed_meta` |

The `default` feed store follows the run history:

- **SQLite history:** a `feeds.db` file beside the history file (so
  `$FAUCET_DATA_DIR/feeds.db` with the default history), with its `-wal` and
  `-shm` files and a `feeds.db.lock` the server holds while it runs. One
  server per file.
- **PostgreSQL history:** the same database, through the feed store's own
  connection pool.

A store is opened, and `feeds.db` created, when the first feed is. The server
protects a SQLite store's files from configs submitted to it.

**Back up the run history and the feed store together.** Back `feeds.db` up
with SQLite's online backup, as for the history file (see
[Backup and restore](/faucet-stream/operations/backup/)). After restoring a
feed store that is older than what destinations already hold, affected
deliveries are **held** instead of skipping or re-reading; run **recover** on
each. Events accepted after the backup was taken are gone, as with any
restore.

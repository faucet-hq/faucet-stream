# API proposals for the console redesign (#998)

Nothing here is implemented. Rules from the owner: only data already stored or already in hand at write time; server-set fields never accepted from a body; memory + SQLite + Postgres + fallback; tenant scoping, RBAC permission and audit action per route; OpenAPI + tests + http-api.md. Schema version: the unmerged branch takes v8 for `submitted_by`; anything here that needs a column proposes **v9**.

Assumed from other work (not duplicated, UI falls back gracefully): `GET /v1/overview` (#1006), `RunRecord.submitted_by`, `GET /v1/runs` `counts`, `GET /v1/changes/counts`, template `registrations`.

| # | Endpoint / field | Params / shape | Backed by | Permission · audit | UI feature |
|---|---|---|---|---|---|
| 1 | `GET /v1/runs/{id}/config` | → `{format, body, redacted}`; 404 when no body stored | `RunRecord.config_body` (stored for clustered/caller runs) | RunRead · `run.config`; sealed values stay `${sealed:N}` | Run Config tab, Re-run, Open in Submit |
| 2 | `RunRecord.dlq_location` (field) | `Option<String>`, serde default | parsed config's `dlq:` sink path at submit | as run read | DLQ tab prefilled with the run's own DLQ |
| 3 | `GET /v1/runs?dataset_id=` (filter) | additive query param | stored invocation `usage.dataset_id` | RunRead | Dataset "recent runs" panel without client paging |
| 4 | `GET /v1/audit` `result=` filter + `before=` cursor | `before` = entry id (UUIDv7) | `faucet_serve_audit` / memory ring | AuditRead | Audit "denials only", infinite scroll |
| 5 | `GET /v1/audit/{id}` | → one `AuditEntry`, 404 when purged | same store | AuditRead · `audit.get` | Audit deep link to an old entry |
| 6 | `GET /v1/triggers` | → `[{name, type, enabled, healthy, methods?, cron?, timezone?, last_fire?, fired_total, errors_total, template?, tenants?}]` | compiled `--triggers` file + `TriggersHandle` + values behind `faucet_serve_trigger_*` | StatusRead · `trigger.list` | Triggers list with type, schedule, last fire; Fire only for webhooks |
| 7 | template version `registered_by` (field) | string, server-set from the principal at `POST /v1/templates` | principal at register time | as template read | "vN registered by X" (reuse `registrations` if it carries it) |
| 8 | `GET /v1/templates/{id}/runs` (optional) | `?limit=` | run history `template` label | TemplateRead · `template.runs` | convenience only; `/v1/runs?label=template:<id>` works today |

// Where everything lives (#998): the one place that builds a page's URL, so
// any id or name shown anywhere can link to its page.
import { hashFor } from "./page.js";

const enc = encodeURIComponent;

export const href = {
  runs: (query = {}) => hashFor("#/runs", query),
  run: (id, tab = "") => hashFor(`#/runs/${enc(id)}`, { tab }),
  changes: (query = {}) => hashFor("#/changes", query),
  change: (id) => `#/changes/${enc(id)}`,
  templates: (query = {}) => hashFor("#/templates", query),
  template: (id, query = {}) => hashFor(`#/templates/${enc(id)}`, typeof query === "object" ? query : { version: query }),
  datasets: (query = {}) => hashFor("#/catalog", query),
  dataset: (id, tab = "") => hashFor(`#/catalog/${enc(id)}`, { tab }),
  lineage: (id = "") => (id ? `#/lineage/${enc(id)}` : "#/lineage"),
  schemas: () => "#/schemas",
  schema: (kind, name) => `#/schemas/${enc(kind)}/${enc(name)}`,
  submit: (query = {}) => hashFor("#/submit", query),
  usage: (query = {}) => hashFor("#/usage", query),
  tenants: () => "#/tenants",
  tenant: (id, tab = "") => hashFor(`#/tenants/${enc(id)}`, { tab }),
  audit: (query = {}) => hashFor("#/audit", query),
  auditEntry: (id) => `#/audit/${enc(id)}`,
  triggers: () => "#/triggers",
  trigger: (name) => `#/triggers/${enc(name)}`,
  overview: () => "#/overview",
  schedules: () => "#/schedules",
  policy: () => "#/policy",
  settings: () => "#/settings",
};

/** A short, human name for a dataset URI: the last path segments without
 *  the scheme, for list titles. The full URI stays in tooltips and detail. */
export function datasetName(uri) {
  const s = String(uri || "");
  const m = s.match(/^([a-z0-9+.-]+):\/\/(.*)$/i);
  const rest = m ? m[2] : s;
  if (!rest) return m ? `${m[1]}://` : s;
  const parts = rest.split("/").filter(Boolean);
  if (m && /^(file|s3|gs|gcs|az|abfss?|sftp|hdfs)$/i.test(m[1])) return parts.slice(-2).join("/") || rest;
  return rest.length > 60 ? `…${rest.slice(-58)}` : rest;
}

/** The scheme of a dataset URI ("file", "postgres", …). */
export function datasetScheme(uri) {
  const m = String(uri || "").match(/^([a-z0-9+.-]+):/i);
  return m ? m[1] : "";
}

export { shortId } from "./components.js";

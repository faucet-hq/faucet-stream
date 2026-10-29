import { api, toast } from "../api.js";
import { escapeHtml, mdInline } from "../utils.js";

export async function renderSchemas(container) {
  let catalog = { sources: [], sinks: [], transforms: [], state: [] };
  try { catalog = await api("/v1/schemas"); } catch (e) { toast(e.message, "error"); }

  // State stores have no per-connector JSON schema (they're simple kinds), so we
  // show a short description instead of a clickable schema link. Unknown kinds
  // fall back to just the name.
  const STATE_DESC = {
    memory: "In-process only — checkpoints last for the run; nothing survives a restart.",
    file: "Local file — durable checkpoints via fsync + atomic rename.",
    redis: "Redis — shared checkpoints; durability follows your Redis persistence config.",
    postgres: "Postgres table — durable checkpoints, upserted per commit.",
    sqlite: "Local SQLite — durable checkpoints in a single file.",
  };

  const group = (title, kind, items) =>
    `<div class="schema-group"><h3>${title}</h3>${items
      .map((i) => `<button class="schema-item" data-kind="${kind}" data-name="${escapeHtml(i.name)}" title="${escapeHtml(i.description)}">${escapeHtml(i.name)}</button>`)
      .join("")}</div>`;

  container.innerHTML = `
    <div class="page p3"><div class="page-head"><h1>Schemas</h1></div>
      <p class="p3-sub">Every connector, transform and state store compiled into this server, with its config schema.</p>
      <div class="schemas-page">
      <aside class="schema-list">
        <p class="schema-note">Showing the <b>${catalog.sources.length} source${catalog.sources.length === 1 ? "" : "s"}</b> and <b>${catalog.sinks.length} sink${catalog.sinks.length === 1 ? "" : "s"}</b> built into this server. Rebuild with more Cargo features (e.g. <code>--features full</code>) to enable the rest.</p>
        ${group("Sources", "source", catalog.sources)}
        ${group("Sinks", "sink", catalog.sinks)}
        ${group("Transforms", "transform", catalog.transforms)}
        <div class="schema-group"><h3>State stores</h3>
          <div class="schema-state-list">
            ${catalog.state.map((s) => `<div class="schema-state"><b class="mono">${escapeHtml(s)}</b>${STATE_DESC[s] ? `<span>${escapeHtml(STATE_DESC[s])}</span>` : ""}</div>`).join("")}
          </div>
        </div>
      </aside>
      <section id="schema-view" class="schema-view"><div class="sp-none"><b>Select a connector</b><span>Its fields, types, defaults and the raw JSON Schema appear here.</span></div></section>
    </div></div>`;

  const view = container.querySelector("#schema-view");
  container.querySelectorAll(".schema-item").forEach((btn) => {
    btn.onclick = async () => {
      container.querySelectorAll(".schema-item").forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      view.innerHTML = `<div class="empty">loading…</div>`;
      try {
        const schema = await api(`/v1/schemas/${btn.dataset.kind}/${encodeURIComponent(btn.dataset.name)}`);
        view.innerHTML = `<h2 class="schema-h"><span class="c-muted">${escapeHtml(btn.dataset.kind)}</span> ${escapeHtml(btn.dataset.name)}</h2>` + fieldTable(schema) +
          `<details class="raw"><summary>raw JSON Schema</summary><pre>${escapeHtml(JSON.stringify(schema, null, 2))}</pre></details>`;
      } catch (e) {
        view.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
      }
    };
  });
}

function fieldTable(schema) {
  const props = schema.properties || {};
  const req = new Set(schema.required || []);
  const rows = Object.entries(props)
    .map(([name, p]) => {
      const type = p.type || (p.$ref ? p.$ref.split("/").pop() : p.enum ? "enum" : p.oneOf || p.anyOf ? "union" : "object");
      const def = p.default !== undefined ? `<code>${escapeHtml(JSON.stringify(p.default))}</code>` : "";
      return `<tr><td>${escapeHtml(name)}${req.has(name) ? " *" : ""}</td><td>${escapeHtml(String(type))}</td><td>${def}</td><td>${mdInline(firstSentence(p.description || ""))}</td></tr>`;
    })
    .join("");
  return `<table class="tbl"><thead><tr><th>field</th><th>type</th><th>default</th><th>description</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function firstSentence(s) { return s ? s.split(/\.\s/)[0].slice(0, 160) : ""; }

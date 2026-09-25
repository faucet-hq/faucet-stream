// Data Movement Catalog (#279): the Datasets browser — a filterable list of
// every dataset the server's pipelines have touched, plus a per-dataset
// detail view (schema timeline with diffs, recent volume, lineage edges).
//
// Also the control surface for local-output retention (#587/#588) and the
// dataset **preview** of those outputs (#586) — the rows a run actually wrote,
// read back through the matching source connector. Cleanup of
// *data artifacts* belongs next to the data artifacts, not on the Runs tab,
// which is about execution history. The model the UI has to convey:
// **data artifacts are disposable; run history is durable** — cleaning an output
// removes the file and leaves the run record alone, so a cleaned output renders
// as `expired`, never as a broken row.
import { api, toast } from "../api.js";
import { navigate } from "../router.js";
import { escapeHtml, fmtCompact, fmtInt } from "../utils.js";
import { attachDatePicker } from "./date-picker.js";
import { fmtTime } from "./runs.js";

const CATALOG_MISSING =
  "The Data Movement Catalog endpoints are not available on this server " +
  "(faucet was built without the `catalog` feature).";

export function catalogUnavailable(e) {
  // A route that is not wired returns a bare 404 (no ApiError envelope code).
  return e && e.status === 404 && !e.code;
}

export async function renderDatasets(container) {
  container.innerHTML = `
    <div class="page page-wide">
      <div class="page-head">
        <h1>Datasets</h1>
        <button class="btn-ghost" id="d-lineage">Lineage graph →</button>
      </div>
      <div class="filters filters-1line">
        <details class="dd" id="f-kind-dd">
          <summary id="f-kind-sum">kind ▾</summary>
          <div class="dd-menu" id="f-kind-menu"><span class="dd-empty">run a pipeline first</span></div>
        </details>
        <input id="f-q" placeholder="search URI" />
        <button class="btn-ghost" id="f-refresh" title="refresh">↻</button>
        <span class="filter-chips" id="f-roles">
          <button class="chip chip-on" data-role="">all</button>
          <button class="chip" data-role="source">source</button>
          <button class="chip" data-role="sink">sink</button>
        </span>
        <input id="f-from" class="date-input" type="text" readonly placeholder="from…" />
        <input id="f-to" class="date-input" type="text" readonly placeholder="to…" />
      </div>
      <table class="ds-table">
        <thead><tr>
          <th>kind</th><th>dataset</th><th>roles</th><th>runs</th><th>rows</th><th>last seen</th>
        </tr></thead>
        <tbody id="ds-list"></tbody>
      </table>
      <nav id="ds-pager" class="pager" aria-label="Dataset pages"></nav>
      <div id="lo-section"></div>
    </div>`;

  const list = container.querySelector("#ds-list");
  const pager = container.querySelector("#ds-pager");
  container.querySelector("#d-lineage").onclick = () => navigate("#/lineage");
  const PAGE_SIZE = 50;
  let page = 1; // 1-based current page over the *filtered* set
  let all = []; // the whole catalog, fetched once; filtered + paginated client-side
  let role = ""; // "", "source", or "sink"
  const kinds = new Set(); // selected kinds (empty = all)

  // Datasets matching every filter EXCEPT kind — the base the kind facet and
  // the final table are both computed from, so the kind options only ever list
  // kinds still reachable given the active role/date filters (faceted).
  function baseFiltered() {
    const from = container.querySelector("#f-from").dataset.value || ""; // ISO or ""
    const to = container.querySelector("#f-to").dataset.value || "";
    return all.filter((d) => {
      if (role && !(d.roles || []).includes(role)) return false;
      const ts = d.last_success || "";
      if (from && ts && ts < from) return false;
      if (to && ts && ts > to) return false;
      return true;
    });
  }

  // The fully-filtered set (role/date + kind + URI search), sorted as the server
  // returned it. Pagination slices this — so the page count always reflects the
  // active filters.
  function filtered() {
    const q = container.querySelector("#f-q").value.trim().toLowerCase();
    return baseFiltered()
      .filter((d) => !kinds.size || kinds.has(d.kind))
      .filter((d) => !q || (d.uri || "").toLowerCase().includes(q));
  }

  function render() {
    populateKinds(); // keep the kind facet in sync with role/date
    const rows = filtered();
    const pageCount = Math.max(1, Math.ceil(rows.length / PAGE_SIZE));
    if (page > pageCount) page = pageCount;
    if (page < 1) page = 1;
    list.innerHTML = "";
    if (!rows.length) {
      list.innerHTML = `<tr><td colspan="6" class="empty">${
        all.length ? "No datasets match the filters." : "No datasets catalogued yet — run a pipeline first."
      }</td></tr>`;
      renderPager(0, 0);
      return;
    }
    const start = (page - 1) * PAGE_SIZE;
    for (const d of rows.slice(start, start + PAGE_SIZE)) list.appendChild(row(d));
    renderPager(pageCount, rows.length);
  }

  // « ‹ 1 … 4 [5] 6 … 20 › » — numbered pages with ellipses; a single page hides
  // the pager entirely. A count summary ("51–58 of 58") sits alongside.
  function renderPager(pageCount, total) {
    pager.innerHTML = "";
    if (pageCount <= 1) return;
    const go = (p) => { page = Math.min(Math.max(1, p), pageCount); render(); };
    const btn = (label, target, opts = {}) => {
      const b = document.createElement("button");
      b.className = "pager-btn" + (opts.current ? " is-current" : "");
      b.textContent = label;
      if (opts.disabled) b.disabled = true;
      else b.onclick = () => go(target);
      return b;
    };
    const gap = () => {
      const s = document.createElement("span");
      s.className = "pager-gap";
      s.textContent = "…";
      return s;
    };
    // window of page numbers around the current page
    const nums = new Set([1, pageCount, page, page - 1, page + 1]);
    const shown = [...nums].filter((n) => n >= 1 && n <= pageCount).sort((a, b) => a - b);
    pager.appendChild(btn("‹", page - 1, { disabled: page === 1 }));
    let prev = 0;
    for (const n of shown) {
      if (n - prev > 1) pager.appendChild(gap());
      pager.appendChild(btn(String(n), n, { current: n === page }));
      prev = n;
    }
    pager.appendChild(btn("›", page + 1, { disabled: page === pageCount }));
    const info = document.createElement("span");
    info.className = "pager-info";
    const from = (page - 1) * PAGE_SIZE + 1;
    const to = Math.min(page * PAGE_SIZE, total);
    info.textContent = `${from}–${to} of ${total}`;
    pager.appendChild(info);
  }

  // Populate the kind multi-select from the kinds reachable under the *other*
  // active filters (role/date), and drop any selected kind that no longer fits.
  function populateKinds() {
    const menu = container.querySelector("#f-kind-menu");
    const distinct = [...new Set(baseFiltered().map((d) => d.kind))].sort();
    for (const k of [...kinds]) if (!distinct.includes(k)) kinds.delete(k);
    if (!distinct.length) return;
    menu.innerHTML = distinct
      .map(
        (k) =>
          `<label class="dd-opt"><input type="checkbox" value="${escapeHtml(k)}"${
            kinds.has(k) ? " checked" : ""
          } /> ${escapeHtml(k)}</label>`,
      )
      .join("");
    menu.querySelectorAll("input").forEach((cb) => {
      cb.onchange = () => {
        if (cb.checked) kinds.add(cb.value);
        else kinds.delete(cb.value);
        container.querySelector("#f-kind-sum").textContent = kinds.size ? `kind (${kinds.size}) ▾` : "kind ▾";
        page = 1;
        render();
      };
    });
  }

  // The catalog is a bounded set (the datasets a pipeline touches), so we fetch
  // it all once by walking the server's cursor, then filter + paginate in the
  // client — which is what makes true numbered pages (jump to any page, a page
  // count that tracks the filters) possible. Deduped by id in case a concurrent
  // write shifts the server sort between cursor pages.
  const MAX_FETCH_PAGES = 100; // safety backstop (~5000 datasets)
  async function loadAll() {
    try {
      const acc = [];
      const seen = new Set();
      let cursor = null;
      for (let i = 0; i < MAX_FETCH_PAGES; i++) {
        const p = new URLSearchParams();
        p.set("limit", "200");
        if (cursor) p.set("cursor", cursor);
        const data = await api(`/v1/catalog/datasets?${p}`);
        for (const d of data.datasets) if (!seen.has(d.id)) { seen.add(d.id); acc.push(d); }
        cursor = data.next_cursor || null;
        if (!cursor) break;
      }
      all = acc;
      page = 1;
      populateKinds();
      render();
    } catch (e) {
      if (catalogUnavailable(e)) list.innerHTML = `<tr><td colspan="6" class="empty">${CATALOG_MISSING}</td></tr>`;
      else toast(e.message, "error");
    }
  }

  // Any filter change re-filters the already-fetched set and jumps back to page 1.
  const refilter = () => { page = 1; render(); };
  container.querySelector("#f-refresh").onclick = () => loadAll();
  container.querySelector("#f-q").oninput = refilter;
  attachDatePicker(container.querySelector("#f-from"));
  attachDatePicker(container.querySelector("#f-to"));
  container.querySelector("#f-from").addEventListener("change", refilter);
  container.querySelector("#f-to").addEventListener("change", refilter);
  container.querySelectorAll("#f-roles .chip").forEach((c) => {
    c.onclick = () => {
      role = c.dataset.role;
      container.querySelectorAll("#f-roles .chip").forEach((x) => x.classList.toggle("chip-on", x === c));
      refilter();
    };
  });
  // Collapse the kind dropdown when clicking anywhere outside it. Named so the
  // teardown can remove it — a per-render anonymous listener would accumulate
  // on `document` across navigations, each closure pinning its detached DOM.
  const kindDd = container.querySelector("#f-kind-dd");
  const closeKindDd = (e) => {
    if (kindDd.open && !kindDd.contains(e.target)) kindDd.open = false;
  };
  document.addEventListener("click", closeKindDd);
  await loadAll();
  const loCleanup = await renderLocalOutputs(container.querySelector("#lo-section"), {});
  return () => {
    document.removeEventListener("click", closeKindDd);
    if (loCleanup) loCleanup();
  };
}

// ── Local outputs (#587/#588) ───────────────────────────────────────────────

const LOCAL_OUTPUTS_MISSING =
  "Local-output retention is not available on this server " +
  "(faucet was built without the `catalog` feature).";

/** The confirm text for a scope that ignores retention windows. */
const CLEAN_ALL_PROMPT = (what) =>
  `Delete ${what}, including files still inside their retention window?\n\n` +
  "Only files faucet created are deleted. Run history, catalog entries, " +
  "and lineage are not touched.";

/** State labels + one-line meanings, so the UI never shows a bare word. */
const STATE_HINT = {
  present: "on disk",
  expired: "cleaned — the run record is kept",
  external:
    "faucet wrote this file but did not create it, so it is never cleaned — and not previewed either",
  replaced:
    "faucet overwrote a file it did not create: previewable, since it holds only faucet's output, but never cleaned",
};

/**
 * Render the "Local outputs" panel into `host`.
 *
 * `scope.datasetId` narrows it to one dataset (the detail view); omitted lists
 * everything (the browser). Destructive controls are rendered only when the
 * server says the caller holds the manage scope — a viewer sees the list and no
 * buttons, rather than buttons that can only 403.
 */
export async function renderLocalOutputs(host, scope = {}) {
  if (!host) return null;
  const datasetId = scope.datasetId || null;
  // Collapse the Manage disclosure when clicking anywhere outside it. One
  // delegated document listener per panel render — installed here (not inside
  // `load()`, which re-runs on every refresh/toggle and would accumulate one
  // listener per click) and removed by the returned teardown. (Named
  // `removeListeners` — `cleanup` is already the local-outputs sweep helper.)
  const collapseManage = (e) => {
    const manage = host.querySelector(".lo-manage");
    if (manage && manage.open && !manage.contains(e.target)) manage.open = false;
  };
  document.addEventListener("click", collapseManage);
  const removeListeners = () => document.removeEventListener("click", collapseManage);
  let showExpired = false;

  host.innerHTML = `
    <h2 class="lo-head">Local outputs</h2>
    <div id="lo-body"><div class="empty">loading…</div></div>`;
  const body = host.querySelector("#lo-body");

  async function load() {
    const p = new URLSearchParams();
    if (datasetId) p.set("dataset_id", datasetId);
    if (showExpired) p.set("include_expired", "true");
    let data;
    try {
      data = await api(`/v1/local-outputs?${p}`);
    } catch (e) {
      body.innerHTML = `<div class="empty">${
        catalogUnavailable(e) ? LOCAL_OUTPUTS_MISSING : escapeHtml(e.message)
      }</div>`;
      return;
    }
    paint(data);
  }

  function paint(data) {
    const {
      outputs,
      retention_days: retention,
      gc_enabled: gcOn,
      can_manage: canManage,
      preview_enabled: canPreview,
      preview_default_rows: previewDefault,
      preview_max_rows: previewMax,
    } = data;
    // Either cap may be null — the server's way of saying "no limit".
    const caps = { defaultRows: previewDefault, maxRows: previewMax, total: scope.totalRecords };
    body.innerHTML = `
      <div class="lo-bar">
        <span class="run-meta">${outputs.length} tracked${
          gcOn
            ? ` · auto-cleaned after ${retention} day${retention === 1 ? "" : "s"}`
            : " · automatic cleanup disabled"
        }</span>
        <details class="lo-manage">
          <summary>Manage ▾</summary>
          <div class="lo-manage-menu">
            <label class="lo-toggle"><input type="checkbox" id="lo-expired" ${
              showExpired ? "checked" : ""
            } /> show cleaned</label>
            ${
              canManage
                ? `<span class="lo-purge">
                     <input type="number" id="lo-days" min="0" value="${retention}" />
                     <button class="btn-warn" id="lo-purge-btn">Purge older than ${retention} day${retention === 1 ? "" : "s"}</button>
                   </span>
                   <button class="btn-danger" id="lo-all">${
                     datasetId ? "Clean this dataset's outputs" : "Clean all local outputs"
                   }</button>`
                : ""
            }
            <button class="btn-ghost" id="lo-refresh">↻ refresh</button>
          </div>
        </details>
      </div>
      <div class="lo-list">${
        outputs.length
          ? outputs.map((o) => outputRow(o, canManage, canPreview)).join("")
          : `<div class="empty">No local output files tracked${
              datasetId ? " for this dataset" : ""
            } yet — run a pipeline with a jsonl, csv, or parquet sink.</div>`
      }</div>`;

    body.querySelector("#lo-expired").onchange = (e) => {
      showExpired = e.target.checked;
      load();
    };
    body.querySelector("#lo-refresh").onclick = () => load();



    body.querySelectorAll(".lo-preview-btn").forEach((btn) => {
      btn.onclick = () => togglePreview(btn, caps);
    });

    if (!canManage) return;
    const daysInput = body.querySelector("#lo-days");
    const purgeBtn = body.querySelector("#lo-purge-btn");
    // Keep the button label in sync with the typed window, so N is never a
    // placeholder — it always names the exact days that will be purged.
    daysInput.oninput = () => {
      const n = daysInput.value.trim();
      purgeBtn.textContent =
        n === "" ? "Purge older than N days" : `Purge older than ${n} day${n === "1" ? "" : "s"}`;
    };
    purgeBtn.onclick = () => {
      const days = Number(daysInput.value);
      if (!Number.isFinite(days) || days < 0) {
        toast("Enter a number of days (0 or more).", "error");
        return;
      }
      // The typed window *is* the intent, so no dialog — except for 0, which
      // means "everything" and clears the same gate as "clean all".
      if (days === 0 && !confirm(CLEAN_ALL_PROMPT("every tracked local output"))) return;
      cleanup(
        { older_than_days: days, confirm: days === 0 },
        `purged outputs older than ${days} day(s)`,
      );
    };
    body.querySelector("#lo-all").onclick = () => {
      // "Clean all" removes files that are still inside their retention window,
      // so it is never a bare one-click.
      const what = datasetId ? "this dataset's tracked outputs" : "every tracked local output";
      if (!confirm(CLEAN_ALL_PROMPT(what))) return;
      // `confirm: true` is what the server's own gate requires for a scope that
      // can delete files still inside their retention window — the dialog above
      // is what earns it. A scripted caller has to opt in deliberately.
      cleanup(
        datasetId ? { dataset_id: datasetId } : { all: true, confirm: true },
        "cleaned local outputs",
      );
    };

    body.querySelectorAll(".lo-del").forEach((btn) => {
      btn.onclick = async () => {
        btn.disabled = true;
        try {
          report(await api(`/v1/local-outputs/${encodeURIComponent(btn.dataset.id)}`, {
            method: "DELETE",
          }));
          await load();
        } catch (e) {
          toast(e.message, "error");
          btn.disabled = false;
        }
      };
    });
  }

  async function cleanup(payload, ok) {
    try {
      report(await api("/v1/local-outputs/cleanup", { method: "POST", body: payload }), ok);
      await load();
    } catch (e) {
      toast(e.message, "error");
    }
  }

  /**
   * Turn a sweep report into one toast. A refusal is a successful request with
   * `deleted: 0`, so "nothing happened" must always come with the reason —
   * otherwise the button looks broken when it was in fact protecting a file.
   */
  function report(rep, fallback) {
    if (rep.deleted > 0) {
      toast(`${fallback || "cleaned"}: ${rep.deleted} file(s), ${fmtBytes(rep.bytes)} reclaimed`);
    }
    const skipped = rep.outputs.filter((o) => o.skipped);
    if (!rep.deleted && !skipped.length) {
      toast("Nothing to clean.");
    }
    for (const o of skipped.slice(0, 3)) {
      toast(`${o.path}: ${skipReason(o)}`, o.skipped === "delete_failed" ? "error" : "info");
    }
    if (skipped.length > 3) toast(`…and ${skipped.length - 3} more skipped.`);
  }

  await load();
  return removeListeners;
}

function outputRow(o, canManage, canPreview) {
  // The row and its preview panel are wrapped together so the panel can expand
  // *below* the row instead of becoming another grid cell inside it.
  return `
    <div class="lo-item" data-id="${escapeHtml(o.id)}">
      <div class="run-row lo-row lo-${escapeHtml(o.state)}">
        <span class="pill">${escapeHtml(o.kind)}</span>
        <span class="run-name mono" title="${escapeHtml(o.path)}">${escapeHtml(o.path)}</span>
        <span class="pill lo-state" title="${escapeHtml(STATE_HINT[o.state] || "")}">${escapeHtml(
          o.state,
        )}</span>
        <span class="run-meta">${escapeHtml(fmtAge(o.age_secs))}</span>
        <span class="run-meta run-time" title="last written">${fmtTime(o.last_written_at)}</span>
        ${
          canPreview && PREVIEWABLE.has(o.kind) && (o.state === "present" || o.state === "replaced")
            ? `<button class="btn-ghost lo-preview-btn">Preview</button>`
            : `<span class="run-meta"></span>`
        }
        ${
          canManage && o.state === "present"
            ? `<button class="btn-danger lo-del" data-id="${escapeHtml(o.id)}">Delete now</button>`
            : `<span class="run-meta"></span>`
        }
      </div>
      <div class="lo-preview" hidden></div>
    </div>`;
}

// ── Dataset preview (#586) ──────────────────────────────────────────────────
//
// "N records written" is not the same information as "here are the records". A
// preview is a **source-backed capped read**: the server reads the output back
// through the matching source connector (csv → source-csv, parquet →
// source-parquet, jsonl → its reader) and returns the first N rows. The client
// never names a path — only the ledger id of an output the server already
// tracks — so there is nothing here that could point the read somewhere else.
//
// The server owns the caps; this panel only *reflects* them (`max` on the input,
// the pre-filled default), so a typed number is never silently clamped without
// the user having been told the ceiling.
//
// Only a `present` or `replaced` output gets a Preview button (a `replaced` file
// already existed, but faucet truncated it, so it holds only faucet's output).
// An `expired` one has no file left, and an `external` one is a file faucet wrote to but did not create — the server refuses
// to serve its contents for the same reason the GC refuses to delete it — so
// offering either would be offering a button that can only fail.

/** Sink kinds that have a reader on the server. Anything else gets no button. */
const PREVIEWABLE = new Set(["jsonl", "csv", "parquet"]);

/** Truncation point for one cell's rendered text. */
const CELL_MAX = 240;

/**
 * Why a read stopped short, in the user's words. A partial answer that does not
 * say it is partial is the one genuinely dangerous thing a preview can do —
 * every one of these must read as "there is more", not as an error.
 */
const CAPPED_HINT = {
  rows: "stopped at the row limit — the dataset has more",
  bytes: "stopped at this server's response-size budget — the dataset has more",
  time: "stopped at this server's read deadline — the dataset has more",
};

/**
 * Expand / collapse an output's preview panel. The first expansion builds the
 * panel and loads; later ones just re-show what was already fetched, so
 * collapsing is not a reason to re-read the file.
 */
function togglePreview(btn, caps) {
  const item = btn.closest(".lo-item");
  const panel = item.querySelector(".lo-preview");
  if (!panel.hidden) {
    panel.hidden = true;
    btn.textContent = "Preview";
    return;
  }
  panel.hidden = false;
  btn.textContent = "Hide";
  if (panel.dataset.ready) return;
  panel.dataset.ready = "1";
  // The ceiling is the server's, so the input advertises it rather than letting
  // someone type a number that comes back quietly clamped.
  const ceiling = caps.maxRows
    ? ` max="${caps.maxRows}" title="this server caps a preview at ${caps.maxRows} rows"`
    : ` title="this server sets no row ceiling"`;
  panel.innerHTML = `
    <div class="dp-bar">
      <label class="dp-rows-label">rows
        <input type="number" class="dp-rows" min="1"${ceiling} value="${
          caps.defaultRows ?? ""
        }" placeholder="all" />
      </label>
      <button class="btn-ghost dp-load">Load</button>
      <button class="btn-ghost dp-load-all" title="${
        caps.maxRows
          ? `load up to this server's preview ceiling of ${caps.maxRows} rows`
          : "every row in the dataset"
      }">${caps.maxRows ? `Max (${caps.maxRows})` : "All rows"}</button>
      <span class="run-meta dp-status"></span>
    </div>
    <div class="dp-body"></div>`;
  const load = (all) => loadPreview(item.dataset.id, panel, caps, all);
  panel.querySelector(".dp-load").onclick = () => load(false);
  panel.querySelector(".dp-load-all").onclick = () => load(true);
  panel.querySelector(".dp-rows").onkeydown = (e) => {
    if (e.key === "Enter") load(false);
  };
  load(false);
}

/**
 * Fetch one page and render it. Errors land in the panel, not a toast.
 *
 * `all` asks for the whole dataset (`row_count_to_load=all`). The server decides
 * what that means: with a ceiling configured it comes back clamped and the status
 * line says so, which is why this asks rather than computing a big number.
 */
async function loadPreview(id, panel, caps, all) {
  const input = panel.querySelector(".dp-rows");
  const want = all ? "all" : rowsParam(input.value, caps);
  const status = panel.querySelector(".dp-status");
  const out = panel.querySelector(".dp-body");
  status.textContent = "loading…";
  try {
    const data = await api(
      `/v1/local-outputs/${encodeURIComponent(id)}/preview?row_count_to_load=${want}`,
    );
    // Reflect what the server actually resolved to — an out-of-range entry (or
    // "all" against a ceiling) visibly becomes the number that was really used.
    input.value = data.row_limit === null ? "" : String(data.row_limit);
    status.textContent = previewStatus(data, caps);
    out.innerHTML = previewTable(data);
  } catch (e) {
    // Every documented failure here is expected and explainable — the file was
    // cleaned (#587), previews are disabled, the last line is half-written — so
    // the server's message is the useful thing to show.
    status.textContent = "";
    out.innerHTML = `<div class="empty">${escapeHtml(e.message)}</div>`;
  }
}

/**
 * The `row_count_to_load` value for a typed row count. An empty or nonsensical
 * entry falls back to the server's default (or `all` where that *is* the
 * default) rather than being sent as-is for the server to reject.
 */
function rowsParam(raw, caps) {
  const n = Math.floor(Number(raw));
  if (!Number.isFinite(n) || n < 1) return caps.defaultRows ?? "all";
  return caps.maxRows ? Math.min(n, caps.maxRows) : n;
}

function previewStatus(d, caps) {
  const n = d.row_count;
  const total = caps && caps.total;
  const ms = `${d.elapsed_ms} ms`;
  // When the dataset's total row count is known, say "N of M" outright — far
  // clearer than "N rows … the dataset has more". Otherwise fall back to the
  // capped/whole wording.
  if (d.truncated) {
    if (total != null) {
      return `showing ${fmtInt(n)} of ${fmtInt(total)} rows · ${ms}`;
    }
    const cap = caps && caps.maxRows ? ` (UI cap ${fmtInt(caps.maxRows)})` : "";
    return `${fmtInt(n)} row${n === 1 ? "" : "s"}${cap} · ${ms} · dataset has more`;
  }
  return total != null && Number(total) > n
    ? `showing all ${fmtInt(n)} of ${fmtInt(total)} rows · ${ms}`
    : `${fmtInt(n)} row${n === 1 ? "" : "s"} · ${ms} · whole dataset`;
}

function previewTable(d) {
  if (!d.row_count) return `<div class="empty">This output has no rows.</div>`;
  const cols = d.columns.length ? d.columns : null;
  const head = cols
    ? cols.map((c) => `<th>${escapeHtml(c)}</th>`).join("")
    : `<th>value</th>`;
  const body = d.rows.map((r) => bodyRow(r, cols)).join("");
  return `<div class="dp-scroll"><table class="dp-table">
      <thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function bodyRow(r, cols) {
  // A record that is not an object has no columns to slot into. Rendering it
  // across the row keeps its value visible instead of showing a line of blanks.
  if (!cols || !isPlainObject(r)) {
    return `<tr><td class="dp-raw"${cols ? ` colspan="${cols.length}"` : ""}>${cell(r)}</td></tr>`;
  }
  return `<tr>${cols.map((c) => `<td>${cell(r[c])}</td>`).join("")}</tr>`;
}

function isPlainObject(v) {
  return v !== null && typeof v === "object" && !Array.isArray(v);
}

/**
 * Render one cell. A *missing* field and a `null` one are different facts about
 * the record, so they must not render identically.
 */
function cell(v) {
  if (v === undefined) {
    return `<span class="dp-absent" title="this record has no such field">—</span>`;
  }
  if (v === null) return `<span class="dp-absent">null</span>`;
  const text = typeof v === "string" ? v : JSON.stringify(v);
  if (text.length <= CELL_MAX) return escapeHtml(text);
  // One enormous value (a blob, an embedded document) must not blow up the
  // table; the full text stays reachable in the tooltip.
  return `<span title="${escapeHtml(text.slice(0, 2000))}">${escapeHtml(
    text.slice(0, CELL_MAX),
  )}…</span>`;
}

/** Why a file was left alone, in the user's words rather than the enum's. */
function skipReason(o) {
  switch (o.skipped) {
    case "pre_existing":
      return "not deleted — faucet wrote this file but did not create it";
    case "already_deleted":
      return "already cleaned";
    case "not_on_disk":
      return "already gone from disk — marked cleaned";
    case "in_flight":
      return "a run is still writing it — will be retried";
    case "delete_failed":
      return `could not delete — ${o.error || "unknown error"}`;
    default:
      return "skipped";
  }
}

function fmtAge(secs) {
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m old`;
  const hours = Math.floor(mins / 60);
  if (hours < 48) return `${hours}h old`;
  return `${Math.floor(hours / 24)}d old`;
}

function fmtBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MiB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(1)} GiB`;
}

function row(d) {
  const el = document.createElement("tr");
  el.className = "ds-tr";
  el.onclick = () => navigate(`#/catalog/${d.id}`);
  el.innerHTML = `
    <td><span class="pill">${escapeHtml(d.kind)}</span></td>
    <td class="ds-uri"><div class="ds-uri-in mono" title="${escapeHtml(d.uri)}">${escapeHtml(d.uri)}</div></td>
    <td class="ds-meta">${escapeHtml(d.roles.join("+"))}</td>
    <td class="ds-meta ds-num">${fmtInt(d.runs)}</td>
    <td class="ds-meta ds-num" title="${fmtInt(d.last_records)}">${fmtCompact(d.last_records)}</td>
    <td class="ds-meta ds-time" title="last success">${fmtTime(d.last_success)}</td>`;
  // Drop the right-edge fade once the URI is scrolled to its end (or doesn't
  // scroll at all), so the last characters render crisp instead of faded.
  const uri = el.querySelector(".ds-uri-in");
  if (uri) {
    const syncFade = () =>
      uri.classList.toggle("at-end", uri.scrollLeft + uri.clientWidth >= uri.scrollWidth - 1);
    uri.addEventListener("scroll", syncFade, { passive: true });
    requestAnimationFrame(syncFade); // initial state, after layout
  }
  return el;
}

export async function renderDatasetDetail(container, params) {
  container.innerHTML = `<div class="empty">loading…</div>`;
  let d;
  try {
    d = await api(`/v1/catalog/datasets/${encodeURIComponent(params.id)}`);
  } catch (e) {
    container.innerHTML = `<div class="empty">${
      catalogUnavailable(e) ? CATALOG_MISSING : escapeHtml(e.message)
    }</div>`;
    return;
  }

  const maxRows = Math.max(1, ...d.stats.map((s) => s.records));
  container.innerHTML = `
    <div class="page">
      <div class="page-head">
        <h1 class="mono dataset-title">${escapeHtml(d.uri)}</h1>
      </div>
      <div class="detail-grid">
        <div><label>Kind</label><span class="pill">${escapeHtml(d.kind)}</span></div>
        <div><label>Roles</label>${escapeHtml(d.roles.join(", "))}</div>
        <div><label>Pipeline</label>${escapeHtml(d.pipeline)}</div>
        <div><label>Runs</label>${d.runs}</div>
        <div><label>Rows (last / total)</label>${fmtInt(d.last_records)} / ${fmtInt(d.total_records)}</div>
        <div><label>First seen</label>${fmtTime(d.first_seen)}</div>
        <div><label>Last success</label>${fmtTime(d.last_success)}</div>
        <div><label>Id</label><span class="mono">${escapeHtml(d.id)}</span></div>
      </div>

      <h2>Volume (recent runs)</h2>
      ${
        d.stats.length >= 3
          ? `<div class="volume-bars">${d.stats
              .slice()
              .reverse()
              .map(
                (s) =>
                  `<div class="volume-bar" title="${escapeHtml(`${fmtInt(s.records)} rows — ${fmtTime(s.recorded_at)} (run ${s.run_id})`)}"
                    style="height:${Math.max(4, Math.round((s.records / maxRows) * 64))}px"></div>`,
              )
              .join("")}</div>`
          : d.stats.length
            ? `<div class="volume-summary">${d.stats.length} run${d.stats.length === 1 ? "" : "s"} recorded · latest <b>${fmtInt(
                d.stats[d.stats.length - 1].records,
              )}</b> rows on ${fmtTime(d.stats[d.stats.length - 1].recorded_at)} <span class="volume-hint">— the trend chart appears once there are 3+ runs</span></div>`
            : `<div class="empty">no volume points yet</div>`
      }

      <h2>Schema timeline</h2>
      <div id="timeline"></div>

      <div id="lo-section"></div>

      <h2>Lineage</h2>
      <div class="edge-lists">
        <div>
          <h3>Upstream</h3>
          ${edgeList(d.upstream, (e) => e.src_id, (e) => e.src_uri)}
        </div>
        <div>
          <h3>Downstream</h3>
          ${edgeList(d.downstream, (e) => e.dst_id, (e) => e.dst_uri)}
        </div>
      </div>
      <button class="btn-ghost" id="d-graph">View in lineage graph →</button>
    </div>`;

  container.querySelector("#d-graph").onclick = () => navigate(`#/lineage/${d.id}`);
  // This dataset's own local files, with the same controls scoped to it.
  const loCleanup = await renderLocalOutputs(container.querySelector("#lo-section"), {
    datasetId: d.id,
    totalRecords: d.total_records,
  });
  container.querySelectorAll(".edge-link").forEach((a) => {
    a.onclick = () => navigate(`#/catalog/${a.dataset.id}`);
  });

  const timeline = container.querySelector("#timeline");
  if (!d.schema_timeline.length) {
    timeline.innerHTML = `<div class="empty">no schema observed yet</div>`;
  }
  for (const v of d.schema_timeline.slice().reverse()) {
    timeline.appendChild(versionCard(v));
  }
  return () => {
    if (loCleanup) loCleanup();
  };
}

function edgeList(edges, idOf, uriOf) {
  if (!edges.length) return `<div class="empty">none</div>`;
  return edges
    .map(
      (e) =>
        `<div class="run-row edge-link" data-id="${escapeHtml(idOf(e))}">
          <span class="run-name mono">${escapeHtml(uriOf(e))}</span>
          <span class="run-meta">${fmtInt(e.runs)} run${e.runs === 1 ? "" : "s"}</span>
          <span class="run-meta">${fmtInt(e.last_records)} rows</span>
        </div>`,
    )
    .join("");
}

function versionCard(v) {
  const el = document.createElement("details");
  el.className = "schema-version";
  const cols = Object.keys((v.schema && v.schema.properties) || {});
  el.innerHTML = `
    <summary>
      <b>v${v.version}</b>
      <span class="run-meta">${fmtTime(v.recorded_at)}</span>
      <span class="run-meta">${cols.length} column${cols.length === 1 ? "" : "s"}</span>
      ${diffBadges(v.diff)}
    </summary>
    <pre class="mono schema-json">${escapeHtml(JSON.stringify(v.schema, null, 2))}</pre>`;
  return el;
}

function diffBadges(diff) {
  if (!diff) return "";
  const name = (c) => (typeof c === "string" ? c : c.column);
  const badge = (cls, sym, items) =>
    (items || [])
      .map((c) => `<span class="pill diff-${cls}">${sym}${escapeHtml(name(c))}</span>`)
      .join("");
  return (
    badge("added", "+", diff.added) +
    badge("widened", "~", diff.widened) +
    badge("changed", "!", diff.changed) +
    badge("removed", "−", diff.removed)
  );
}

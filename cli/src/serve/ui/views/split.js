// The list / detail split pane (#998) shared by Runs and Changes: a ~370px list
// (tabs, stat strip, collapsible sections) beside a detail pane. Below 1100px
// the detail replaces the list and a back link returns to it.
import { sectionHeader } from "../components.js";
import { escapeHtml } from "../utils.js";

/** Lay the pane into `container`; returns its parts. */
export function splitPane(container, { name, listHead = "" }) {
  container.innerHTML = `
    <div class="sp" data-split="${escapeHtml(name)}">
      <section class="sp-list" aria-label="${escapeHtml(name)} list">
        <div class="sp-list-head">${listHead}</div>
        <div class="sp-list-body" tabindex="-1"></div>
      </section>
      <section class="sp-detail" aria-label="${escapeHtml(name)} detail"></section>
    </div>`;
  const root = container.querySelector(".sp");
  return {
    root,
    head: root.querySelector(".sp-list-head"),
    body: root.querySelector(".sp-list-body"),
    detail: root.querySelector(".sp-detail"),
    /** Whether an item is selected: on a narrow window that shows the detail. */
    setSelected(on) {
      root.classList.toggle("has-sel", !!on);
    },
  };
}

function readSet(key) {
  try {
    return new Set(JSON.parse(localStorage.getItem(key)) || []);
  } catch {
    return new Set();
  }
}

/** Collapsible list sections that survive re-rendering. `sections` is
 *  `[{ id, label, count, rows: [html] }]`; empty sections are left out.
 *  Markup only changes when the content does, and the scroll position stays. */
export function sectionList(body, storageKey) {
  const collapsed = readSet(storageKey);
  let last = "";
  body.addEventListener("click", (e) => {
    const btn = e.target.closest(".c-sect");
    if (!btn || !body.contains(btn)) return;
    const id = btn.parentElement.dataset.sect;
    const now = !collapsed.has(id);
    if (now) collapsed.add(id);
    else collapsed.delete(id);
    btn.setAttribute("aria-expanded", String(!now));
    btn.parentElement.classList.toggle("collapsed", now);
    try {
      localStorage.setItem(storageKey, JSON.stringify([...collapsed]));
    } catch {
      /* storage unavailable: the choice lasts for this page only */
    }
  });
  return {
    render(sections, { empty = "", footer = "" } = {}) {
      const shown = sections.filter((s) => s.rows.length);
      const html = shown.length
        ? shown
            .map(
              (s) =>
                `<div class="sp-sect${collapsed.has(s.id) ? " collapsed" : ""}" data-sect="${escapeHtml(s.id)}">${sectionHeader(
                  s.label,
                  s.count ?? s.rows.length,
                  { collapsed: collapsed.has(s.id) },
                )}<div class="sp-rows">${s.rows.join("")}</div></div>`,
            )
            .join("") + footer
        : empty;
      if (html === last) return false;
      const top = body.scrollTop;
      body.innerHTML = html;
      body.scrollTop = top;
      last = html;
      return true;
    },
    /** Mark the row whose `data-id` is `id` selected without re-rendering. */
    select(id) {
      body.querySelectorAll(".c-row").forEach((r) => {
        const on = r.dataset.id === id;
        r.classList.toggle("on", on);
        if (on) r.setAttribute("aria-current", "true");
        else r.removeAttribute("aria-current");
      });
    },
    invalidate() {
      last = "";
    },
  };
}

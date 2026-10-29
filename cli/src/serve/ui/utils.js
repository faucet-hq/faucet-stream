// Small shared helpers for the console UI.

/** Format an integer with thousands separators (e.g. 1500000 → "1,500,000").
 * Non-numbers pass through as their string form. Shared so every list/detail
 * renders counts identically. */
export function fmtInt(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n.toLocaleString("en-US") : String(v ?? "");
}

/** Format a duration in seconds as compact h/m/s, no spaces, showing only the
 * two highest units (e.g. 3723 → "1h2m", 184 → "3m4s", 45.2 → "45s",
 * <1s → "0.8s"). Seconds are dropped once the run is an hour or more — the exact
 * value belongs in a tooltip. Nullish → "—". */
export function fmtDuration(secs) {
  const s = Number(secs);
  if (!Number.isFinite(s)) return "—";
  if (s < 1) return `${s.toFixed(1)}s`;
  const t = Math.round(s);
  const h = Math.floor(t / 3600);
  const m = Math.floor((t % 3600) / 60);
  const sec = t % 60;
  if (h) return `${h}h${m}m`;
  if (m) return `${m}m${sec}s`;
  return `${sec}s`;
}

/** How long ago an RFC 3339 instant was, compactly: `12s`, `14m`, `3h`, `2d`, `5w`. */
export function fmtAge(iso, now = Date.now()) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return "";
  const s = Math.max(0, Math.round((now - t) / 1000));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h`;
  if (s < 86400 * 14) return `${Math.floor(s / 86400)}d`;
  return `${Math.floor(s / (86400 * 7))}w`;
}

/** Format a count compactly with a K/M/B suffix (e.g. 4801657 → "4.8M",
 * 3_883_982 → "3.88M", 1234 → "1.23K", 999 → "999"). Trims trailing zeros;
 * under 1000 shows the exact integer. Non-numbers pass through. */
export function fmtCompact(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return String(v ?? "");
  const abs = Math.abs(n);
  if (abs < 1000) return String(n);
  const units = [
    [1e9, "B"],
    [1e6, "M"],
    [1e3, "K"],
  ];
  for (const [base, suf] of units) {
    if (abs >= base) {
      const val = n / base;
      // Fewer decimals as the value grows; trim trailing zeros ONLY after a
      // decimal point (4.80→4.8, 4.00→4) — never from an integer (200 must stay
      // 200, not become 2).
      let str = val.toFixed(val >= 100 ? 0 : val >= 10 ? 1 : 2);
      if (str.includes(".")) str = str.replace(/0+$/, "").replace(/\.$/, "");
      return `${str}${suf}`;
    }
  }
  return String(n);
}

/** Escape a string for safe interpolation into innerHTML. */
export function escapeHtml(s) {
  return String(s ?? "").replace(
    /[&<>"]/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c],
  );
}

/**
 * Render the minimal inline markdown used in connector descriptions — `**bold**`
 * and `` `code` `` — as safe HTML. The text is HTML-escaped first, then our own
 * tags are injected, so it never introduces markup from the source string.
 */
export function mdInline(s) {
  return escapeHtml(s)
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/`([^`]+)`/g, "<code>$1</code>");
}

const COPY_ICON = `<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><rect x="5.5" y="5.5" width="8" height="8" rx="1.5"/><path d="M10.5 3.5v-.5a1.5 1.5 0 0 0-1.5-1.5H3A1.5 1.5 0 0 0 1.5 3v6A1.5 1.5 0 0 0 3 10.5h.5"/></svg>`;
const CHECK_ICON = `<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="M3 8.5l3 3 7-7"/></svg>`;

/** Write `text` to the clipboard; falls back to a hidden textarea where the async API is unavailable (plain-http hosts). */
export async function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.setAttribute("readonly", "");
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  ta.select();
  const ok = document.execCommand("copy");
  ta.remove();
  if (!ok) throw new Error("copy is not available in this browser");
}

/** An icon button that copies `getText()` and briefly shows a check mark. */
export function copyButton(getText, label = "Copy") {
  const btn = document.createElement("button");
  btn.type = "button";
  btn.className = "copy-btn";
  btn.title = label;
  btn.setAttribute("aria-label", label);
  btn.innerHTML = COPY_ICON;
  let timer;
  btn.onclick = async (ev) => {
    ev.stopPropagation();
    try {
      await copyText(getText());
      btn.classList.add("copied");
      btn.innerHTML = `${CHECK_ICON}<span>Copied</span>`;
      clearTimeout(timer);
      timer = setTimeout(() => {
        btn.classList.remove("copied");
        btn.innerHTML = COPY_ICON;
      }, 1500);
    } catch (e) {
      btn.dispatchEvent(new CustomEvent("copy-failed", { bubbles: true, detail: e }));
    }
  };
  return btn;
}

/** Run statuses that never change again (mirrors `RunStatus::is_terminal`). */
export const TERMINAL_RUN_STATUSES = ["completed", "failed", "cancelled"];

// Labels the server stamps on a run (provenance): shown after the caller's
// own labels and muted. Mirrors RESERVED_LABELS plus the other server labels.
const PROVENANCE_LABELS = new Set([
  "template", "template_version", "allow_untested", "sink_template",
  "sink_template_version", "overlay", "overlay_version", "selection",
  "pipeline", "backfill", "backfill_unit", "dlq_replay",
]);

export function isProvenanceLabel(key) {
  return PROVENANCE_LABELS.has(key) || key.startsWith("faucet.");
}

/** A run's labels as `[key, value, provenance]`, caller labels first. */
export function sortedLabels(labels) {
  return Object.entries(labels || {})
    .map(([k, v]) => [k, String(v), isProvenanceLabel(k)])
    .sort((a, b) => (a[2] - b[2]) || a[0].localeCompare(b[0]));
}

/** One clickable `key: value` chip; long values are cut with a tooltip. */
export function labelChip(key, value, provenance) {
  const shown = value.length > 40 ? `${value.slice(0, 39)}…` : value;
  const filter = `${key}:${value}`;
  return `<button type="button" class="label-chip${provenance ? " label-chip-prov" : ""}" data-label="${escapeHtml(filter)}" title="Filter runs by ${escapeHtml(filter)}"><span class="label-chip-k">${escapeHtml(key)}</span><span class="label-chip-v">${escapeHtml(shown)}</span></button>`;
}

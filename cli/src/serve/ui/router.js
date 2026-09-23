// Hash router. Routes call render(container, params) and return an optional
// teardown() (used to stop log streams / polling on navigation).
const routes = [];
let teardown = null;
let rootContainer = null;

export function route(pattern, render) {
  // pattern like "#/runs/:id" → regex with named groups
  const names = [];
  const re = new RegExp(
    "^" +
      pattern.replace(/:[^/]+/g, (m) => {
        names.push(m.slice(1));
        return "([^/]+)";
      }) +
      "$",
  );
  routes.push({ re, names, render });
}

export function navigate(hash) {
  window.location.hash = hash;
}

async function dispatch(container) {
  if (teardown) {
    try { teardown(); } catch { /* ignore */ }
    teardown = null;
  }
  const hash = window.location.hash || "#/runs";
  // `#/path?k=v` — the query never takes part in matching; views read it as
  // `params.query` (a URLSearchParams).
  const qIdx = hash.indexOf("?");
  const path = qIdx === -1 ? hash : hash.slice(0, qIdx);
  const query = new URLSearchParams(qIdx === -1 ? "" : hash.slice(qIdx + 1));
  for (const r of routes) {
    const m = path.match(r.re);
    if (m) {
      const params = { query };
      r.names.forEach((n, i) => (params[n] = decodeURIComponent(m[i + 1])));
      teardown = (await r.render(container, params)) || null;
      return;
    }
  }
  container.innerHTML = `<div class="empty">Not found</div>`;
}

export function startRouter(container) {
  rootContainer = container;
  window.addEventListener("hashchange", () => dispatch(container));
  dispatch(container);
}

/** Re-render the current route in place (e.g. after a timezone-preference change). */
export function refresh() {
  if (rootContainer) dispatch(rootContainer);
}

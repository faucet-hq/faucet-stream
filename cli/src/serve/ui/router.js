// Hash router. Routes call render(container, params) and return an optional
// teardown() (used to stop log streams / polling on navigation).
const routes = [];
let teardown = null;
let rootContainer = null;
let generation = 0;

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
  const gen = ++generation;
  if (teardown) {
    try { teardown(); } catch { /* ignore */ }
    teardown = null;
  }
  // Each dispatch renders into its own host. A newer dispatch swaps its host
  // in, so a superseded render that resolves late paints a detached element
  // instead of the current page. The host is attached before rendering so
  // views can still measure layout.
  const host = document.createElement("div");
  host.className = "view-host";
  container.replaceChildren(host);
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
      let td = null;
      try {
        td = (await r.render(host, params)) || null;
      } catch (e) {
        if (gen === generation) throw e;
        return;
      }
      if (gen !== generation) {
        if (td) { try { td(); } catch { /* ignore */ } }
        host.remove();
        return;
      }
      teardown = td;
      return;
    }
  }
  host.innerHTML = `<div class="page"><div class="empty">Not found</div></div>`;
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

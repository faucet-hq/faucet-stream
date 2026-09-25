// Centralized API client: base path, bearer token (localStorage), error envelope.
const TOKEN_KEY = "faucet.token";

export const token = {
  get: () => localStorage.getItem(TOKEN_KEY) || "",
  set: (t) => localStorage.setItem(TOKEN_KEY, t),
  clear: () => localStorage.removeItem(TOKEN_KEY),
};

// Listeners notified when a request gets a 401 (so the shell can open the modal).
const unauthorizedHandlers = new Set();
export const onUnauthorized = (fn) => unauthorizedHandlers.add(fn);

// Listeners notified when a request gets a 403 (the caller's role may have changed).
const forbiddenHandlers = new Set();
export const onForbidden = (fn) => forbiddenHandlers.add(fn);

export class ApiError extends Error {
  constructor(status, code, message, details, retryAfter) {
    super(message || code || `HTTP ${status}`);
    this.status = status;
    this.code = code;
    this.details = details;
    this.retryAfter = retryAfter;
  }
}

export function authHeaders(extra = {}) {
  const t = token.get();
  return t ? { Authorization: `Bearer ${t}`, ...extra } : { ...extra };
}

export async function api(path, { method = "GET", body, headers } = {}) {
  const opts = { method, headers: authHeaders(headers || {}) };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const resp = await fetch(path, opts);
  if (resp.status === 401) {
    unauthorizedHandlers.forEach((fn) => fn());
    throw new ApiError(401, "unauthorized", "authentication required");
  }
  if (resp.status === 204) return null;
  const text = await resp.text();
  const json = text ? safeJson(text) : null;
  if (resp.status === 403 && path !== "/v1/whoami") forbiddenHandlers.forEach((fn) => fn());
  if (!resp.ok) {
    const e = json && json.error ? json.error : {};
    const retryAfter = Number(resp.headers.get("retry-after")) || undefined;
    throw new ApiError(resp.status, e.code, e.message, e.details, retryAfter);
  }
  return json;
}

function safeJson(text) {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

// Probe whether the server requires auth (used at boot). Returns true if a
// tokenless GET /v1/runs is accepted (no-auth mode or token already valid).
export async function authOk() {
  const resp = await fetch("/v1/runs?limit=1", { headers: authHeaders() });
  return resp.status !== 401;
}

export function toast(message, kind = "info") {
  const host = document.getElementById("toasts");
  if (!host) return;
  const el = document.createElement("div");
  el.className = `toast toast-${kind}`;
  el.textContent = message;
  host.appendChild(el);
  setTimeout(() => el.remove(), 5000);
}

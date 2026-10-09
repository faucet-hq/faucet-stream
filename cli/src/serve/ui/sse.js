import { authHeaders } from "./api.js";

// Stream a run's logs. Calls handlers.onLog(line) / onTruncated(msg) / onEnd()
// / onLink(url) (the local copy aged out; the log service has it) / onError(err). Returns an AbortController — call .abort() to stop.
export function streamLogs(runId, handlers) {
  const ctrl = new AbortController();
  (async () => {
    let resp;
    try {
      resp = await fetch(`/v1/runs/${encodeURIComponent(runId)}/logs`, {
        headers: authHeaders({ Accept: "text/event-stream" }),
        signal: ctrl.signal,
      });
    } catch (err) {
      if (!ctrl.signal.aborted) handlers.onError?.(err);
      return;
    }
    if (resp.status === 404) {
      handlers.onExpired?.();
      return;
    }
    if (!resp.ok || !resp.body) {
      handlers.onError?.(new Error(`logs HTTP ${resp.status}`));
      return;
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        let idx;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          dispatch(buf.slice(0, idx), handlers);
          buf = buf.slice(idx + 2);
        }
      }
    } catch (err) {
      if (!ctrl.signal.aborted) handlers.onError?.(err);
    }
  })();
  return ctrl;
}

// Parse one SSE frame ("event: X\ndata: Y" lines; data may span multiple lines).
function dispatch(frame, handlers) {
  let event = "message";
  const data = [];
  for (const line of frame.split("\n")) {
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).replace(/^ /, ""));
    // lines beginning with ":" are comments (keep-alive) — ignore.
  }
  const payload = data.join("\n");
  if (event === "log") handlers.onLog?.(payload);
  else if (event === "truncated") handlers.onTruncated?.(payload);
  else if (event === "end") handlers.onEnd?.();
  else if (event === "link") handlers.onLink?.(payload);
}

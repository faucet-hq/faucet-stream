import { tmpdir } from "node:os";
// Render every view × state × theme of the mockups to shots/.
// usage: node render.mjs [--views a,b] [--port 9350]
import { spawn } from "node:child_process";
import { writeFileSync, mkdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const only = arg("--views", "") ? arg("--views", "").split(",") : null;
const port = Number(arg("--port", "9350"));
const themes = (arg("--themes", "dark,light")).split(",");
mkdirSync(path.join(here, "shots"), { recursive: true });
const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const chrome = spawn(CHROME, ["--headless=new", `--remote-debugging-port=${port}`, "--disable-gpu", "--no-first-run", "--allow-file-access-from-files", `--user-data-dir=${path.join(tmpdir(), `mock-prof-${port}-${Date.now()}`)}`, "about:blank"], { stdio: "ignore" });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
let ver;
for (let i = 0; i < 60; i++) { try { ver = await (await fetch(`http://127.0.0.1:${port}/json/version`)).json(); break; } catch { await sleep(200); } }
const ws = new WebSocket(ver.webSocketDebuggerUrl);
await new Promise((r) => ws.addEventListener("open", r));
let id = 0; const pending = new Map(); const events = [];
ws.addEventListener("message", (m) => { const d = JSON.parse(m.data); if (d.id && pending.has(d.id)) { pending.get(d.id)(d); pending.delete(d.id); } else if (d.method) events.push(d); });
const send = (method, params = {}, sessionId) => new Promise((res) => { const i = ++id; pending.set(i, res); ws.send(JSON.stringify({ id: i, method, params, sessionId })); });
const { result: { targetId } } = await send("Target.createTarget", { url: "about:blank" });
const { result: { sessionId } } = await send("Target.attachToTarget", { targetId, flatten: true });
const s = (m, p) => send(m, p, sessionId);
await s("Page.enable"); await s("Runtime.enable");
const evalJs = async (e) => (await s("Runtime.evaluate", { expression: e, returnByValue: true })).result?.result?.value;
const base = "file://" + here;
await s("Emulation.setDeviceMetricsOverride", { width: 1440, height: 900, deviceScaleFactor: 2, mobile: false });
await s("Page.navigate", { url: `${base}/gallery.html` }); await sleep(800);
const all = await evalJs(`JSON.stringify(Object.values(M.VIEWS).map(v => ({ name: v.name, shape: v.shape || 'split', states: Object.entries(v.states).map(([k, st]) => ({ key: k, shape: st.shape || v.shape || 'split', params: st.params || {} })) })))`);
const views = JSON.parse(all).filter((v) => !only || only.includes(v.name));
let n = 0;
for (const v of views) for (const st of v.states) for (const theme of themes) {
  const widths = st.shape === "split" || st.shape === "canvas-panel" ? [1440, 1000] : [1440];
  for (const w of widths) {
    await s("Emulation.setDeviceMetricsOverride", { width: w, height: 900, deviceScaleFactor: 2, mobile: false });
    const q = new URLSearchParams({ view: v.name, state: st.key, theme, ...st.params });
    await s("Page.navigate", { url: `${base}/index.html?${q}` });
    await sleep(350);
    const { result } = await s("Page.captureScreenshot", { format: "png" });
    const file = `shots/${v.name}-${st.key}-${theme}${w === 1440 ? "" : "-" + w}.png`;
    writeFileSync(path.join(here, file), Buffer.from(result.data, "base64"));
    n++;
  }
}
const exc = events.filter((e) => e.method === "Runtime.exceptionThrown").map((e) => e.params.exceptionDetails?.exception?.description || e.params.exceptionDetails?.text);
console.log(`rendered ${n} screenshots`);
console.log("exceptions:", JSON.stringify(exc.slice(0, 10)));
chrome.kill();
process.exit(0);

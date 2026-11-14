// Render every view × state × theme × width to shots/, plus shots/manifest.js for the gallery.
// usage: node render.mjs [--views a,b] [--themes light,dark] [--port 9351]
import { tmpdir } from "node:os";
import { spawn } from "node:child_process";
import { writeFileSync, mkdirSync, rmSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const here = path.dirname(fileURLToPath(import.meta.url));
const arg = (k, d) => { const i = process.argv.indexOf(k); return i > 0 ? process.argv[i + 1] : d; };
const only = arg("--views", "") ? arg("--views", "").split(",") : null;
const port = Number(arg("--port", "9351"));
const themes = arg("--themes", "light,dark").split(",");
mkdirSync(path.join(here, "shots"), { recursive: true });
const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const profile = path.join(tmpdir(), `docs-mock-${port}-${Date.now()}`);
const chrome = spawn(CHROME, ["--headless=new", `--remote-debugging-port=${port}`, "--disable-gpu", "--no-first-run", "--hide-scrollbars", "--allow-file-access-from-files", `--user-data-dir=${profile}`, "about:blank"], { stdio: "ignore" });
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
const H = { 1440: 900, 1000: 800, 390: 844 };

await s("Emulation.setDeviceMetricsOverride", { width: 1440, height: 900, deviceScaleFactor: 2, mobile: false });
await s("Page.navigate", { url: `${base}/index.html` }); await sleep(900);
const all = JSON.parse(await evalJs(`JSON.stringify(Object.values(M.VIEWS).map(v => ({ name: v.name, title: v.title, group: v.group, states: Object.entries(v.states).map(([k, st]) => ({ key: k, label: st.label, widths: st.widths || [1440, 390], viewport: !!st.after || ['Overlays'].includes(v.group) })) })))`));
const views = all.filter((v) => !only || only.includes(v.name));
const manifest = [];
let n = 0;
for (const v of views) for (const st of v.states) for (const w of st.widths) for (const theme of themes) {
  const h = H[w] || 900;
  await s("Emulation.setDeviceMetricsOverride", { width: w, height: h, deviceScaleFactor: 2, mobile: w < 600 });
  await s("Page.navigate", { url: `${base}/index.html?${new URLSearchParams({ view: v.name, state: st.key, theme })}` });
  await sleep(w === 390 ? 700 : 550);
  let opts = { format: "png" };
  if (!st.viewport) {
    const full = Math.min(await evalJs("document.documentElement.scrollHeight"), 7000);
    opts = { format: "png", captureBeyondViewport: true, clip: { x: 0, y: 0, width: w, height: full, scale: 1 } };
  }
  const { result } = await s("Page.captureScreenshot", opts);
  const file = `shots/${v.name}-${st.key}-${theme}-${w}.png`;
  writeFileSync(path.join(here, file), Buffer.from(result.data, "base64"));
  manifest.push({ view: v.name, title: v.title, group: v.group, state: st.key, label: st.label, theme, width: w, file });
  n++;
}
if (!only) writeFileSync(path.join(here, "shots/manifest.js"), "window.SHOTS = " + JSON.stringify(manifest, null, 1) + ";\n");
const exc = events.filter((e) => e.method === "Runtime.exceptionThrown").map((e) => e.params.exceptionDetails?.exception?.description || e.params.exceptionDetails?.text);
console.log(`rendered ${n} screenshots`);
console.log("exceptions:", JSON.stringify(exc.slice(0, 10)));
chrome.kill();
await sleep(500);
rmSync(profile, { recursive: true, force: true });
process.exit(0);

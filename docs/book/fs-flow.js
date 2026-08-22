/* fs-flow — "live" pipeline diagrams for the docs, matching the marketing site.
   Replaces left-to-right mermaid flowcharts with animated node-chips connected by
   flowing packets (the packets ARE the records moving through the pipeline).

   Usage in markdown (raw HTML passes through mdBook):
     <div class="fs-flow" data-flow="Source|faucet pipeline|Sink">Source → faucet pipeline → Sink</div>
   Node syntax: "Name" or "Name::sublabel", pipe-separated. The element's text
   content is a plain-text fallback shown if this script never runs (JS off).

   Dependency-free; a no-op when no .fs-flow[data-flow] elements exist. Animation
   is pure CSS and honors prefers-reduced-motion (see custom.css). */
(() => {
  const flows = document.querySelectorAll(".fs-flow[data-flow]");
  if (!flows.length) return;

  flows.forEach((el) => {
    if (el.dataset.fsFlowDone) return;

    const nodes = el.dataset.flow
      .split("|")
      .map((s) => s.trim())
      .filter(Boolean);
    if (!nodes.length) return;

    // Clear the plain-text fallback and rebuild as chips + connectors.
    el.textContent = "";

    nodes.forEach((raw, i) => {
      const [name, sub] = raw.split("::").map((s) => (s || "").trim());

      const node = document.createElement("div");
      node.className = "fs-flow-node";

      const nm = document.createElement("span");
      nm.className = "fs-flow-name";
      nm.textContent = name;
      node.appendChild(nm);

      if (sub) {
        const s = document.createElement("span");
        s.className = "fs-flow-sub";
        s.textContent = sub;
        node.appendChild(s);
      }
      el.appendChild(node);

      if (i < nodes.length - 1) {
        const edge = document.createElement("div");
        edge.className = "fs-flow-edge";
        edge.setAttribute("aria-hidden", "true");
        edge.innerHTML = "<i></i><i></i><i></i>";
        el.appendChild(edge);
      }
    });

    // Accessible label = the flow as a sentence.
    el.setAttribute("role", "img");
    el.setAttribute(
      "aria-label",
      nodes.map((n) => n.split("::")[0].trim()).join(" then "),
    );
    el.dataset.fsFlowDone = "1";
  });
})();

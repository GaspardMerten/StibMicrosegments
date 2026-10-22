// ---------------------------------------------------------------- direction (Sens), global
// The analysis page in the frame tells its directions ({ms: "dirs"}); this control drives it with
// {ms: "dir"} (no reload: both directions are already loaded) and keeps &dir= in the URL.
"use strict";
let DIR = null, FRAME = null;
function drawSens(dirs, cur) {
  if (!dirs || dirs.length < 2) { $("sensfld").hidden = true; return; }
  $("sensfld").hidden = false;
  $("sens").innerHTML = dirs.map(d => `<button type="button" data-dir="${d.dir}" aria-pressed="${d.dir === cur}">vers ${esc(d.to)}</button>`).join("");
}
function setDir(d, send) {
  DIR = d;
  $("sens").querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(+b.dataset.dir === d)));
  const u = new URLSearchParams(location.search); u.set("dir", d);
  history.replaceState(null, "", location.pathname + "?" + u);
  if (send && FRAME && FRAME.contentWindow) FRAME.contentWindow.postMessage({ms: "dir", dir: d}, "*");
}
// the frame's directions ({ms: "dirs"}): keep the chosen one when the line has it
function onDirs(m) {
  const ds = m.dirs || [], want = ds.some(d => d.dir === DIR) ? DIR : m.dir;
  drawSens(ds, want);
  if (want != null) setDir(want, want !== m.dir);
}
$("sens").addEventListener("click", e => { const b = e.target.closest("button"); if (b) setDir(+b.dataset.dir, true); });

// ---------------------------------------------------------------- one line / two periods
let RUN = 0, OFF_FRAME = null;    // OFF_FRAME: lets go of the current frame (its message listener, its run)
// the form's periods: [[label, first, last]], one ("") or two ("A", "B")
const formPeriods = () => TAB === "cmp" ? [["A", $("a1").value, $("a2").value], ["B", $("b1").value, $("b2").value]]
  : [["", $("from").value, $("to").value]];
async function runLine() {
  const line = $("linebtn").dataset.line;
  if (!line) return;
  const p = lineParams();
  const pu = new URLSearchParams(p); if (DIR != null) pu.set("dir", DIR);
  history.replaceState(null, "", location.pathname + "?" + pu);
  const me = ++RUN;
  $("go").disabled = true; $("notes").innerHTML = ""; $("routes").innerHTML = ""; QUAL = null; ROUTES = null;
  busy($("status"), TAB === "cmp" ? `Comparaison de la ligne ${esc(line)}` : `Calcul de la ligne ${esc(line)}`,
    TAB === "cmp" ? "une comparaison prend jusqu'à une minute la première fois" : "la première fois, quelques mois prennent 10 à 40 s");
  try {
    if (await loadQuality(line, me)) await showFrame(p, me);
  } catch (e) {
    if (me === RUN) { done($("status"), esc(e.message), true); $("view").innerHTML = ""; }
  }
  if (me === RUN) $("go").disabled = false;
}
// route versions (optional, never blocks the analysis) and the coverage of the periods; true when the
// analysis can run (some day kept, run still current)
async function loadQuality(line, me) {
  const dows = dowsSel(), reason = XL.reasonFn(), xtxt = XL.text();     // as when the run starts, whatever the form says later
  const periods = formPeriods();
  for (const [, a, b] of periods) if (!a || !b || a > b) throw new Error("Choisissez des dates de début et de fin dans l'ordre.");
  const L = encodeURIComponent(line);
  const span = [periods.map(x => x[1]).sort()[0], periods.map(x => x[2]).sort().pop()];
  getJSON(`/api/versions?line=${L}&from=${span[0]}&to=${span[1]}`).then(r => { if (me === RUN) { ROUTES = r.versions || []; drawRoutes(); } }).catch(() => {});
  const covs = await Promise.all(periods.map(([, a, b]) => getJSON(`/api/coverage?line=${L}&from=${a}&to=${b}`)));
  if (me !== RUN) return false;
  QUAL = {periods: covs.map((c, i) => [periods[i][0], c.days]), dows, reason, xtxt, sum: null};
  if (drawQuality().every(c => c.ok > 0)) return true;
  done($("status"), "Aucun jour retenu : choisissez une autre période ou d'autres jours.", true);
  $("view").innerHTML = "";
  return false;
}
// the text of a /view answer that is not the analysis page (template missing, server error)
function frameError(doc) {
  const t = ((doc.body && doc.body.textContent) || "").trim();
  try { return detailText(JSON.parse(t).detail) || t; } catch (e) { return t || "HTTP error"; }
}
// the analysis page in a frame; settles on its "ready" or "error" message, or when it loads without the page
function showFrame(p, me) {
  if (OFF_FRAME) OFF_FRAME();
  return new Promise(resolve => {
    $("view").innerHTML = `<iframe src="/view?${p}&rev=${REV}" scrolling="no" allowfullscreen title="Analyse de la ligne ${esc(p.get("line"))}"></iframe>`;
    const fr = $("view").querySelector("iframe");
    FRAME = fr;
    let settled = false;
    const settle = err => {
      if (settled) return;
      settled = true;
      if (me === RUN && err != null) { done($("status"), esc(err), true); $("view").innerHTML = ""; }
      else if (me === RUN) done($("status"), "");
      resolve();
    };
    const onMsg = ev => {
      if (ev.source !== fr.contentWindow || !ev.data || !ev.data.ms) return;
      const m = ev.data;
      if (m.ms === "height") fr.style.height = m.h + "px";   // the frame takes its page's height
      else if (m.ms === "summary") { if (me === RUN && QUAL) { QUAL.sum = m; drawQuality(); } }
      else if (m.ms === "dirs") onDirs(m);
      else settle(m.ms === "error" ? m.message : null);
    };
    fr.addEventListener("load", () => {     // no loader in the frame: no message will come
      const doc = fr.contentDocument;
      if (doc && doc.location.href !== "about:blank" && !doc.getElementById("ms-data")) settle(frameError(doc));
    });
    window.addEventListener("message", onMsg);
    OFF_FRAME = () => { window.removeEventListener("message", onMsg); settle(null); };
  });
}
$("f").addEventListener("submit", e => { e.preventDefault(); runLine(); });
$("notes").addEventListener("click", e => {
  if (!e.target.closest("[data-xopen]")) return;
  $("fmore").open = true; $("fmore").scrollIntoView({behavior: "smooth", block: "center"});
});

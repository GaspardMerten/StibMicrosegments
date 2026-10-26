// ---------------------------------------------------------------- direction (Sens), global
// The analysis page in the frame tells its directions ({ms: "dirs"}); this control drives it with
// {ms: "dir"} (no reload: both directions are already loaded) and keeps &dir= in the URL.
import {$, REV, S, busy, detailText, done, dowGet, esc, getJSON, selChanged} from "./util.js?rev=__REV__";
import {XL} from "./excl.js?rev=__REV__";
import {dowsSel, setQuality, setSummary} from "./quality.js?rev=__REV__";
import {excludeRoutes, showRoutes} from "./routes.js?rev=__REV__";

let FRAME = null;     // the direction asked for is S.dir (also set from the URL by main.js)
function drawSens(dirs, cur) {
  if (!dirs || dirs.length < 2) { $("sensfld").hidden = true; return; }
  $("sensfld").hidden = false;
  $("sens").innerHTML = dirs.map(d => `<button type="button" data-dir="${d.dir}" aria-pressed="${d.dir === cur}">vers ${esc(d.to)}</button>`).join("");
  selChanged();
}
function setDir(d, send) {
  S.dir = d;
  $("sens").querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(+b.dataset.dir === d)));
  const u = new URLSearchParams(location.search); u.set("dir", d);
  history.replaceState(null, "", location.pathname + "?" + u);
  selChanged();
  if (send && FRAME && FRAME.contentWindow) FRAME.contentWindow.postMessage({ms: "dir", dir: d}, "*");
}
// the frame's directions ({ms: "dirs"}): keep the chosen one when the line has it
function onDirs(m) {
  const ds = m.dirs || [], want = ds.some(d => d.dir === S.dir) ? S.dir : m.dir;
  drawSens(ds, want);
  if (want != null) setDir(want, want !== m.dir);
}
$("sens").addEventListener("click", e => { const b = e.target.closest("button"); if (b) setDir(+b.dataset.dir, true); });

// ---------------------------------------------------------------- one line / two periods
// the form's API parameters (line, period(s), days, exclusions)
export function lineParams() {
  const p = new URLSearchParams();
  p.set("line", $("linebtn").dataset.line);
  if (S.tab === "cmp") { p.set("a", `${$("a1").value}..${$("a2").value}`); p.set("b", `${$("b1").value}..${$("b2").value}`); }
  else { p.set("from", $("from").value); p.set("to", $("to").value); }
  p.set("dow", dowGet($("dow")));
  // segments are always 30 m and the reference 20-24 h (20:00-23:59) on the site (the API still takes seg= and ref=)
  return XL.params(p);
}
let RUN = 0, OFF_FRAME = null;    // OFF_FRAME: lets go of the current frame (its message listener, its run)
// the form's periods: [[label, first, last]], one ("") or two ("A", "B")
const formPeriods = () => S.tab === "cmp" ? [["A", $("a1").value, $("a2").value], ["B", $("b1").value, $("b2").value]]
  : [["", $("from").value, $("to").value]];
export async function runLine() {
  const line = $("linebtn").dataset.line;
  if (!line) return;
  const p = lineParams();
  const pu = new URLSearchParams(p); if (S.dir != null) pu.set("dir", S.dir);
  history.replaceState(null, "", location.pathname + "?" + pu);
  const me = ++RUN;
  selChanged(true);
  $("go").disabled = true; $("notes").innerHTML = ""; setQuality(null); showRoutes(null);
  busy($("status"), S.tab === "cmp" ? `Comparaison de la ligne ${esc(line)}` : `Calcul de la ligne ${esc(line)}`,
    S.tab === "cmp" ? "une comparaison prend jusqu'à une minute la première fois" : "la première fois, quelques mois prennent 10 à 40 s");
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
  getJSON(`/api/versions?line=${L}&from=${span[0]}&to=${span[1]}`).then(r => { if (me === RUN) showRoutes(r.versions || []); }).catch(() => {});
  const covs = await Promise.all(periods.map(([, a, b]) => getJSON(`/api/coverage?line=${L}&from=${a}&to=${b}`)));
  if (me !== RUN) return false;
  if (setQuality({periods: covs.map((c, i) => [periods[i][0], c.days]), dows, reason, xtxt, sum: null}).every(c => c.ok > 0)) return true;
  done($("status"), "Aucun jour retenu : choisissez une autre période ou d'autres jours.", true);
  $("view").innerHTML = "";
  return false;
}
// the text of a /view answer that is not the analysis page (template missing, server error)
function frameError(doc) {
  const t = ((doc.body && doc.body.textContent) || "").trim();
  try { return detailText(JSON.parse(t).detail) || t; } catch (e) { return t || "HTTP error"; }
}
// the line's GTFS colours for the frame's title badge (lc, ltc: hex without "#"; the loader takes them
// off the API query and hands them to the page as route_color / route_text_color)
function colorParams(line) {
  const l = S.lines.find(x => x.line === line), hex = v => /^[0-9a-f]{6}$/i.test(v || "") ? v : "";
  return l && hex(l.color) ? `&lc=${l.color}&ltc=${hex(l.text_color) || "FFFFFF"}` : "";
}
// the analysis page in a frame; settles on its "ready" or "error" message, or when it loads without the page
function showFrame(p, me) {
  if (OFF_FRAME) OFF_FRAME();
  return new Promise(resolve => {
    $("view").innerHTML = `<iframe src="/view?${p}${colorParams(p.get("line"))}&rev=${REV}" scrolling="no" allowfullscreen title="Analyse de la ligne ${esc(p.get("line"))}"></iframe>`;
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
      else if (m.ms === "summary") { if (me === RUN) setSummary(m); }
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
$("routes").addEventListener("click", e => { if (excludeRoutes(e.target)) runLine(); });
$("notes").addEventListener("click", e => {
  if (!e.target.closest("[data-xopen]")) return;
  $("fmore").open = true; $("fmore").scrollIntoView({behavior: "smooth", block: "center"});
});

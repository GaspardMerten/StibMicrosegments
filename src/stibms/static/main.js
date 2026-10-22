// The page's entry module (index.html): tabs, URL state, start.
import {$, S, done, dowSet, esc, fmonth, getJSON} from "./util.js?rev=__REV__";
import {XL, XR} from "./excl.js?rev=__REV__";
import {lineOf, presetState, presets, setLine} from "./lines.js?rev=__REV__";
import {runLine} from "./lineview.js?rev=__REV__";
import {runRank} from "./rank.js?rev=__REV__";

// ---------------------------------------------------------------- tabs + URL state
function setTab(t) {
  S.tab = t;
  document.querySelectorAll("nav a").forEach(a => { if (a.dataset.tab === t) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
  $("lineview").hidden = t === "rank"; $("rankview").hidden = t !== "rank";
  $("single").hidden = t !== "one"; $("double").hidden = t !== "cmp";
  $("go").textContent = t === "cmp" ? "Comparer" : "Afficher";
  document.title = {one: "Une ligne", cmp: "Comparer deux périodes", rank: "Classement du réseau"}[t] + " · STIB microsegments";
  presets();
}
// the line asked for when it exists, else 55, else the first line with data
function defaultLine(asked) {
  if (asked && lineOf(asked)) return asked;
  const withData = S.lines.filter(l => l.available);
  return (withData.find(l => l.line === "55") || withData[0] || S.lines[0] || {}).line;
}
document.querySelectorAll("nav a").forEach(a => a.addEventListener("click", e => {
  e.preventDefault();
  const t = a.dataset.tab;
  if (t === S.tab) return;
  let line = $("linebtn").dataset.line;
  if (t !== "rank" && !line && (line = defaultLine())) setLine(line, false);   // the page opened on the ranking
  history.pushState(null, "", a.getAttribute("href") + (t !== "rank" && line ? "?line=" + encodeURIComponent(line) : ""));
  setTab(t);
  window.scrollTo(0, 0);
  if (t === "rank") { $("rfrom").value = ""; $("rto").value = ""; runRank(); } else if (line && lineOf(line) && lineOf(line).available) runLine();
}));
window.addEventListener("popstate", () => boot(false));

// ---------------------------------------------------------------- start
function pendingBanner() {
  const pm = (S.info && S.info.pending_months) || [];
  if (!pm.length) { $("pending").hidden = true; return; }
  $("pending").hidden = false;
  $("pending").innerHTML = `<b>Préparation des données en cours.</b> ${pm.length === 1 ? "Le mois de " + fmonth(pm[0]) + " n'est" : pm.length + " mois (" + fmonth(pm[0]) + " → " + fmonth(pm[pm.length - 1]) + ") ne sont"} pas encore calculé${pm.length > 1 ? "s" : ""} : ${pm.length > 1 ? "ils apparaîtront" : "il apparaîtra"} au fil du calcul.`;
}
// the ranking form from the URL, then the ranking
function bootRank(u) {
  for (const [k, id] of [["from", "rfrom"], ["to", "rto"]]) $(id).value = u.get(k) || "";
  dowSet($("rdow"), u.get("dow"));
  if (XR.load(u)) $("rmore").open = true;
  XR.setRange(S.info.ingested.from, S.info.ingested.to);
  const m = u.get("mode") || "all";
  $("rmode").querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", String(x.dataset.mode === m)));
  // a value the menu does not offer would empty it (and the API refuses top=)
  if ([...$("rtop").options].some(o => o.value === u.get("top"))) $("rtop").value = u.get("top");
  $("rterm").checked = u.get("terminus") === "true";
  for (const id of ["rfrom", "rto"]) { $(id).min = S.info.ingested.from || ""; $(id).max = S.info.ingested.to || ""; }
  return runRank();
}
// the line form from the URL, then the analysis
function bootLine(u) {
  const want = defaultLine(u.get("line"));
  if (!want) { done($("status"), "Aucune ligne disponible pour l'instant : données en cours de préparation.", true); return; }
  setLine(want, false);
  if (u.get("from")) $("from").value = u.get("from");
  if (u.get("to")) $("to").value = u.get("to");
  presetState();
  S.dir = u.get("dir") != null && u.get("dir") !== "" ? +u.get("dir") : null;
  if (u.get("a")) { const [x, y] = u.get("a").split(".."); $("a1").value = x; $("a2").value = y; }
  if (u.get("b")) { const [x, y] = u.get("b").split(".."); $("b1").value = x; $("b2").value = y; }
  dowSet($("dow"), u.get("dow"));
  if (XL.load(u)) $("fmore").open = true;
  const l = lineOf(want);
  if (l && l.available) runLine();
  else done($("status"), `La ligne ${esc(want)} n'a pas encore de données calculées : choisissez une autre ligne.`, true);
}
async function boot(first) {
  const path = location.pathname.replace(/\/+$/, "");
  if (path === "/methode") history.replaceState(null, "", "/");      // the method page is gone
  const tab = path === "/comparer" ? "cmp" : path === "/classement" ? "rank" : "one";
  setTab(tab);
  const u = new URLSearchParams(location.search);
  if (first) {
    try {
      const r = await getJSON("/api/lines");
      S.lines = r.lines; S.info = {...(r.months || {}), ingested: r.ingested};
    } catch (e) {     // in the status line of the tab shown
      done($(tab === "rank" ? "rstatus" : "status"), "Impossible de charger les lignes : " + esc(e.message), true);
      return;
    }
    pendingBanner();
  }
  return tab === "rank" ? bootRank(u) : bootLine(u);
}
boot(true);

// ---------------------------------------------------------------- ranking
import {$, DOW, busy, done, dowGet, esc, fdate, getJSON, nf} from "./util.js?rev=__REV__";
import {XR} from "./excl.js?rev=__REV__";
import {badge, lineOf} from "./lines.js?rev=__REV__";
import {REASONS} from "./quality.js?rev=__REV__";

const L = window.L;     // Leaflet: a classic script in the page's head
const RMODES = [["all", "Tous"], ["tram", "Tram"], ["bus", "Bus"], ["metro", "Métro"]];
$("rmode").innerHTML = RMODES.map(([k, t]) => `<button type="button" data-mode="${k}" aria-pressed="${k === "all"}">${t}</button>`).join("");
$("rmode").addEventListener("click", e => {
  const b = e.target.closest("button"); if (!b) return;
  $("rmode").querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
  runRank();
});
const rmode = () => ($("rmode").querySelector("[aria-pressed=true]") || {}).dataset?.mode || "all";
function rankParams() {
  const p = new URLSearchParams();
  if ($("rfrom").value) p.set("from", $("rfrom").value);
  if ($("rto").value) p.set("to", $("rto").value);
  p.set("dow", dowGet($("rdow")));
  p.set("mode", rmode()); p.set("top", $("rtop").value);
  if ($("rterm").checked) p.set("terminus", "true");
  return XR.params(p);
}
let RMAP = null, RLAYERS = [], RK = 0;
export async function runRank() {
  const p = rankParams();
  history.replaceState(null, "", "/classement?" + p);
  const me = ++RK;
  $("rgo").disabled = true;
  busy($("rstatus"), "Classement en cours", "");
  try {
    const R = await getJSON("/api/ranking?" + p);
    if (me !== RK) return;
    drawRank(R);
    done($("rstatus"), "");
  } catch (e) {
    if (me === RK) { done($("rstatus"), esc(e.message), true); $("rout").innerHTML = ""; }
  }
  if (me === RK) $("rgo").disabled = false;
}
$("rf").addEventListener("submit", e => { e.preventDefault(); runRank(); });
$("rterm").addEventListener("change", runRank);
$("rtop").addEventListener("change", runRank);

// ranking intro: time lost compared with the evening, per vehicle, between stops
function rlede(termIn) {
  return "Les tronçons du réseau où les véhicules STIB perdent le plus de temps par rapport au soir (20 h–minuit), entre les arrêts, de 6 h à 21 h. Les lignes qui partagent un tronçon s'additionnent"
    + (termIn ? "." : " ; terminus et attentes pour l'horaire exclus.");
}
$("rlede").textContent = rlede(false);
function heat(t) {
  const s = [[255, 230, 0], [253, 141, 60], [227, 26, 28], [189, 0, 90]];
  t = Math.max(0, Math.min(1, t)) * (s.length - 1);
  const i = Math.min(Math.floor(t), s.length - 2), u = t - i;
  return `rgb(${s[i].map((a, k) => Math.round(a + (s[i + 1][k] - a) * u)).join(",")})`;
}
const RREASON = {terminus: "terminus", short_terminus: "terminus partiel", holding: "attente pour l'horaire"};
const RREASON_LONG = {terminus: "terminus de la ligne (pause entre deux courses)", short_terminus: "terminus partiel : des véhicules y stationnent plus de 3 min",
  holding: "arrêt où des véhicules attendent plus de 3 min pour respecter l'horaire"};
const PHONE = matchMedia("(max-width:600px)");
const PEAK = {am: "surtout le matin", pm: "surtout l'après-midi", day: "toute la journée"};
// lines of the window the line list (latest GTFS) no longer has: why "sur 89" when the list shows 87
const goneTxt = g => g && g.length ? `, dont ${g.length} hors du réseau actuel (${g.map(esc).join(", ")})` : "";
// the sentence above the tiles: period, days, exclusions, mode, and what the top stretches cost
function rankSummary(R, S, top, modeTxt) {
  const q = R.query;
  const daysTxt = q.dow.length === 5 && q.dow.join() === "0,1,2,3,4" ? "jours ouvrables" : "jours " + q.dow.map(d => DOW[d]).join(", ");
  // days left out on purpose, worded as on the line page
  const xd = Object.entries(R.days_excluded || {}).map(([k, n]) => `${n} ${(REASONS[k] || [k, k])[n > 1 ? 1 : 0]}`);
  const exTxt = xd.length ? ` hors ${xd.join(", ")}` : "";
  return `
    <p class="rk-sum">Du <b>${fdate(q.first)}</b> au <b>${fdate(q.last)}</b>, ${daysTxt}${exTxt}, ${modeTxt} (${R.lines_used} lignes) :
      les ${S.length} tronçons les plus coûteux font perdre <b>${nf(top, 0)} heures-véhicule par jour</b> entre les arrêts par rapport au soir (20 h–minuit),
      soit environ <b>${nf(Math.round(top * R.days_per_year / 100) * 100, 0)} heures par an</b>.</p>
    <div class="tiles">
      <div class="tile"><b>${nf(S[0].veh_h_day, 1)} h</b><span>par jour sur le tronçon n° 1, ${esc(S[0].from)} → ${esc(S[0].to)}</span></div>
      <div class="tile"><b>${nf(top, 0)} h</b><span>par jour sur les ${S.length} premiers tronçons</span></div>
      <div class="tile"><b>${nf(R.total_veh_h_day, 0)} h</b><span>par jour sur tout le réseau (tronçons en perte)</span></div>
      <div class="tile"><b>${R.lines_used}</b><span>lignes analysées${R.lines_total > R.lines_used ? ` sur ${R.lines_total}` : ""}${goneTxt(R.lines_gone)}</span></div>
    </div>`;
}
// one row per stretch; its line badges open the line page with the same days and exclusions
function rankRows(R, S, max) {
  const q = R.query;
  const lq = XR.params(new URLSearchParams({from: q.first, to: q.last, dow: q.dow.join(",")}));
  const lb = x => badge(lineOf(x.line) || (R.lines || []).find(y => y.line === x.line) || {line: x.line});
  return S.map((s, i) => `<tr data-i="${i}">
        <td class="rk">${s.rank}</td>
        <td class="str"><b>${esc(s.from)} → ${esc(s.to)}</b>
          <small>${nf(s.len_m)} m · ${nf(s.passages_day)} passages / jour</small>
          <small class="ph-x">${nf(s.veh_h_year)} h-véh. par an · ${nf(s.s_per_passage, 0)} s perdues par véhicule</small>
          <div class="lines">${s.lines.map(x => `<a href="/?line=${encodeURIComponent(x.line)}&${lq}" title="Ligne ${esc(x.line)} : ${nf(Math.max(0, x.veh_h_day), 1)} h / jour, ${PEAK[x.peak] || ""}${x.reason ? " · " + RREASON_LONG[x.reason] : ""}">${lb(x)}</a>`).join("")}</div>
          ${(s.reasons || []).map(r => `<span class="tag" title="${esc(RREASON_LONG[r] || r)}">${esc(RREASON[r] || r)}</span>`).join("")}
          <div class="bar-in" style="width:${Math.max(2, 100 * s.veh_h_day / max)}%"></div></td>
        <td class="n hd">${nf(s.veh_h_day, 1)}<span class="u"> h/j</span></td><td class="n">${nf(s.veh_h_year)}</td><td class="n">${nf(s.s_per_passage, 0)}</td></tr>`).join("");
}
// the stretches left out (termini, holding points), folded on phone
function rankExcluded(ex) {
  if (!ex.n) return "";
  return `<details class="rk-ex"${PHONE.matches ? "" : " open"}><summary><b>${nf(ex.n)} tronçon${ex.n > 1 ? "s" : ""} laissé${ex.n > 1 ? "s" : ""} de côté</b> (${nf(ex.veh_h_day, 0)} h-véh. / jour)<span class="ex-why"> : terminus ou attente volontaire pour l'horaire, pas un ralentissement. Les principaux :</span></summary>
      <ul>${ex.top.map(x => `<li>${esc(x.from)} → ${esc(x.to)} (${x.lines.map(y => esc(y.line)).join(", ")}) : ${nf(x.veh_h_day, 1)} h / jour, ${nf(x.s_per_passage, 0)} s perdues par véhicule · ${x.reasons.map(r => esc(RREASON_LONG[r] || r)).join(" ; ")}</li>`).join("")}</ul></details>`;
}
function drawRank(R) {
  const q = R.query, S = R.stretches || [];
  if (!$("rfrom").value) $("rfrom").value = q.first;
  if (!$("rto").value) $("rto").value = q.last;
  $("rlede").textContent = rlede(!!q.terminus);
  const modeTxt = {all: "toutes les lignes", tram: "les trams", bus: "les bus", metro: "le métro"}[q.mode];
  if (!S.length) {
    $("rout").innerHTML = `<div class="notice">Aucun tronçon à classer pour ${modeTxt} sur cette période (${R.lines_used} ligne${R.lines_used > 1 ? "s" : ""} avec des données).</div>`;
    return;
  }
  const top = S.reduce((a, s) => a + Math.max(0, s.veh_h_day), 0);
  const max = Math.max(...S.map(s => s.veh_h_day));
  $("rout").innerHTML = `${rankSummary(R, S, top, modeTxt)}
    <div class="rkgrid">
      <div class="mapcol"><div id="rkmap"></div></div>
      <div><table class="tbl"><thead><tr><th>#</th><th>Tronçon et lignes</th><th style="text-align:right">h-véh. / jour</th><th style="text-align:right">par an</th><th style="text-align:right" title="secondes perdues par véhicule, par rapport au soir (20 h–minuit)">s / véhicule</th></tr></thead>
      <tbody>${rankRows(R, S, max)}</tbody></table></div>
    </div>
    ${rankExcluded(R.excluded || {n: 0, top: []})}
    <p class="method">Toutes les 20 secondes, chaque véhicule STIB signale sa position ; sur chaque tronçon de 30 m, 1 signalement ≈ 20 s passées là. Temps perdu par rapport au soir (20 h–minuit, mêmes jours) par véhicule, × passages de 6 h à 21 h = temps perdu par jour. Zones d'arrêt (là où la STIB rattache le véhicule à l'arrêt, de son approche à son départ) et jours au flux incomplet exclus, comme les jours exclus dans les options (jours fériés et grèves par défaut). Par an : × ${R.days_per_year} jours (ordre de grandeur).</p>`;
  drawRankMap(S, max);
  bindRankRows();
}
// the stretches on the map, coloured and sized by their cost; the 15 first numbered
function drawRankMap(S, max) {
  if (RMAP) { RMAP.remove(); RMAP = null; }
  RMAP = L.map("rkmap", {scrollWheelZoom: false});
  L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
    {attribution: "Tiles © Esri — Esri, HERE, Garmin, © OpenStreetMap contributors", maxZoom: 16}).addTo(RMAP);
  RLAYERS = [];
  const all = [];
  [...S].reverse().forEach(s => {
    const geos = [s.c, ...(s.cs || [])].filter(c => c && c.length >= 2);
    if (!geos.length) return;
    const lls = geos.map(c => c.map(([x, y]) => [y, x]));
    const pl = L.polyline(lls, {color: heat(s.veh_h_day / max), weight: 4 + 6 * s.veh_h_day / max, opacity: .95})
      .bindTooltip(`<b>${s.rank}. ${esc(s.from)} → ${esc(s.to)}</b><br>${nf(s.veh_h_day, 1)} h-véh. / jour · lignes ${s.lines.map(x => esc(x.line)).join(", ")}`, {sticky: true})
      .addTo(RMAP);
    pl.on("click", () => selectRow(s.rank - 1, false));
    RLAYERS[s.rank - 1] = pl; all.push(pl);
    if (s.rank <= 15) {
      const ll = lls[0], mid = ll[Math.floor(ll.length / 2)];
      L.marker(mid, {icon: L.divIcon({className: "rkicon", html: String(s.rank)})}).addTo(RMAP).on("click", () => selectRow(s.rank - 1, false));
    }
  });
  if (all.length) RMAP.fitBounds(L.featureGroup(all).getBounds(), {padding: [20, 20]});
}
function bindRankRows() {
  document.querySelectorAll("#rout tbody tr").forEach(tr => tr.addEventListener("click", e => {
    if (e.target.closest("a")) return;
    if (PHONE.matches) tr.classList.toggle("open");   // phone: the row's detail opens on tap
    selectRow(+tr.dataset.i, true);
  }));
  const exd = document.querySelector("#rout .rk-ex");   // the left-out list folds on phone only
  if (exd) {
    exd.querySelector("summary").addEventListener("click", e => { if (!PHONE.matches && exd.open) e.preventDefault(); });
    PHONE.onchange = () => { const d = document.querySelector("#rout .rk-ex"); if (d) d.open = !PHONE.matches; };
  }
}
function selectRow(i, zoom) {
  document.querySelectorAll("#rout tbody tr").forEach(tr => tr.classList.toggle("sel", +tr.dataset.i === i));
  const l = RLAYERS[i];
  if (l) { if (zoom) RMAP.fitBounds(l.getBounds(), {maxZoom: 16, padding: [60, 60]}); l.openTooltip(); }
  if (!zoom) { const tr = document.querySelector(`#rout tbody tr[data-i="${i}"]`); if (tr) tr.scrollIntoView({block: "nearest", behavior: "smooth"}); }
}

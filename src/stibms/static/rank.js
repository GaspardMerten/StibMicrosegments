// ---------------------------------------------------------------- network page: selection, indicators, map, ranking
import {$, DOW, MODE_FR, S, addDays, busy, done, dowGet, esc, fdate, fmonth, getJSON, monthEnd, nf, selChanged} from "./util.js?rev=__REV__";
import {XR} from "./excl.js?rev=__REV__";
import {badge, lineOf, modeKey, niceName} from "./lines.js?rev=__REV__";
import {REASONS} from "./quality.js?rev=__REV__";
import {ran, watch} from "./stale.js?rev=__REV__";
import {drawNet, setFocus} from "./net.js?rev=__REV__";

const L = window.L;     // Leaflet: a classic script in the page's head
const RMODES = [["all", "Tout"], ["bus", "Bus"], ["tram", "Tram"], ["metro", "Métro"]];
const DEF_HOURS = [6, 21];
const HPRESETS = [["Journée", 6, 21], ["Pointe du matin", 7, 10], ["Pointe du soir", 16, 19], ["Soirée", 20, 24], ["Tout le service", 4, 28]];
const hourLab = h => h < 24 ? `${h} h` : h === 24 ? "minuit" : `${h - 24} h (nuit)`;
const v = {lines: new Set(), focusAsked: null, metric: "v", R: null, RK: null, MP: null};

// ---------------------------------------------------------------- form
$("rmode").innerHTML = RMODES.map(([k, t]) => `<button type="button" data-mode="${k}" aria-pressed="${k === "all"}">${t}</button>`).join("");
const fillHours = (el, a, b, d) => { el.innerHTML = Array.from({length: b - a + 1}, (_, i) => a + i).map(h => `<option value="${h}"${h === d ? " selected" : ""}>${hourLab(h)}</option>`).join(""); };
fillHours($("rh0"), 4, 27, DEF_HOURS[0]); fillHours($("rh1"), 5, 28, DEF_HOURS[1]);
$("rpresets").innerHTML = `<span class="lbl">Heures</span>` + HPRESETS.map(([t, a, b]) => `<button type="button" data-h0="${a}" data-h1="${b}">${t}</button>`).join("")
  + `<span class="lbl gap">Période</span><button type="button" data-p="12">12 derniers mois</button><button type="button" data-p="all">Tout</button>`;
function hourState() {
  $("rpresets").querySelectorAll("[data-h0]").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.h0 === $("rh0").value && b.dataset.h1 === $("rh1").value)));
}
function rangeHours(changed) {
  if (+$("rh1").value <= +$("rh0").value) {
    if (changed === "rh0") $("rh1").value = Math.min(28, +$("rh0").value + 1);
    else $("rh0").value = Math.max(4, +$("rh1").value - 1);
  }
  hourState();
}
$("rh0").addEventListener("change", () => rangeHours("rh0"));
$("rh1").addEventListener("change", () => rangeHours("rh1"));
$("rpresets").addEventListener("click", e => {
  const b = e.target.closest("button"); if (!b) return;
  if (b.dataset.h0) { $("rh0").value = b.dataset.h0; $("rh1").value = b.dataset.h1; hourState(); }
  if (b.dataset.p === "12") { $("rfrom").value = ""; $("rto").value = ""; }
  if (b.dataset.p === "all") { $("rfrom").value = S.info.ingested.from; $("rto").value = S.info.ingested.to; }
  v.focusAsked = null;
  runRank();
});
// ---------------------------------------------------------------- compare with a reference period
const cmpOn = () => $("rcmp").checked && !!$("rcfrom").value && !!$("rcto").value;
function cmpState() { for (const id of ["rcfrom", "rcto"]) $(id).disabled = !$("rcmp").checked; }
const shiftYear = (s, n) => { const [y, m, d] = s.split("-").map(Number); const x = new Date(Date.UTC(y + n, m - 1, Math.min(d, new Date(Date.UTC(y + n, m, 0)).getUTCDate()), 12)); return x.toISOString().slice(0, 10); };
function cmpPreset(kind) {
  const a = $("rfrom").value, b = $("rto").value;
  if (!a || !b) return;
  if (kind === "year") { $("rcfrom").value = shiftYear(a, -1); $("rcto").value = shiftYear(b, -1); }
  else { const n = Math.round((new Date(b) - new Date(a)) / 864e5); $("rcto").value = addDays(a, -1); $("rcfrom").value = addDays(a, -1 - n); }
}
$("rcmp").addEventListener("change", () => {
  if ($("rcmp").checked && !($("rcfrom").value && $("rcto").value)) cmpPreset("year");
  cmpState(); runRank();
});
$("rcpresets").addEventListener("click", e => {
  const b = e.target.closest("button"); if (!b) return;
  cmpPreset(b.dataset.c); $("rcmp").checked = true; cmpState(); runRank();
});
cmpState();
const rmode = () => ($("rmode").querySelector("[aria-pressed=true]") || {}).dataset?.mode || null;
function setMode(m) { $("rmode").querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", String(x.dataset.mode === m))); }
$("rmode").addEventListener("click", e => {
  const b = e.target.closest("button"); if (!b) return;
  setMode(b.dataset.mode); v.lines.clear(); drawBadges(); runRank();
});
// ---------------------------------------------------------------- lines: any set of lines
function drawBadges() {
  const ids = [...v.lines];
  if (!ids.length) { const m = rmode(); $("rbadges").textContent = !m || m === "all" ? "Toutes les lignes" : `Toutes les lignes (${MODE_FR[m].toLowerCase()})`; return; }
  const shown = ids.slice(0, 7);
  $("rbadges").innerHTML = shown.map(id => badge(lineOf(id) || {line: id})).join("") + (ids.length > shown.length ? `<span class="more-n">+${ids.length - shown.length}</span>` : "");
}
function drawLinePicker(filter = "") {
  const f = filter.trim().toLowerCase(), groups = {};
  for (const l of S.lines) {
    if (f && !(l.line.toLowerCase().startsWith(f) || (l.name || "").toLowerCase().includes(f))) continue;
    (groups[modeKey(l)] ||= []).push(l);
  }
  $("rlgroups").innerHTML = ["metro", "tram", "bus", "night", "autre"].filter(m => groups[m]).map(m =>
    `<div class="rpg"><h4>${MODE_FR[m]}</h4><button type="button" class="mini" data-group="${m}">Toutes</button></div><div class="chips">` + groups[m].map(l =>
      `<button type="button" class="chip" role="option" data-line="${esc(l.line)}" aria-selected="${v.lines.has(l.line)}" aria-disabled="${!l.available}" title="${esc(l.line + " · " + niceName(l.name))}">${badge(l)}</button>`).join("") + "</div>").join("")
    || `<p class="lbl" style="margin:8px 0">Aucune ligne ne correspond.</p>`;
}
function toggleLines(ids, on) {
  for (const id of ids) on ? v.lines.add(id) : v.lines.delete(id);
  setMode(v.lines.size ? null : "all");
  drawBadges(); drawLinePicker($("rlsearch").value);
  $("rf").dispatchEvent(new Event("change"));
}
$("rlinebtn").addEventListener("click", () => {
  const open = $("rpanel").hidden;
  $("rpanel").hidden = !open; $("rlinebtn").setAttribute("aria-expanded", String(open));
  if (open) { drawLinePicker($("rlsearch").value); setTimeout(() => $("rlsearch").focus(), 0); }
});
$("rlsearch").addEventListener("input", e => drawLinePicker(e.target.value));
$("rlsearch").addEventListener("keydown", e => { if (e.key === "Enter") e.preventDefault(); if (e.key === "Escape") closePicker(); });
$("rlclear").addEventListener("click", () => toggleLines([...v.lines], false));
$("rlgroups").addEventListener("click", e => {
  const c = e.target.closest(".chip");
  if (c) { if (c.getAttribute("aria-disabled") !== "true") toggleLines([c.dataset.line], !v.lines.has(c.dataset.line)); return; }
  const g = e.target.closest("[data-group]");
  if (g) { const ids = S.lines.filter(l => modeKey(l) === g.dataset.group && l.available).map(l => l.line); toggleLines(ids, !ids.every(i => v.lines.has(i))); }
});
function closePicker() {
  if ($("rpanel").hidden) return;
  $("rpanel").hidden = true; $("rlinebtn").setAttribute("aria-expanded", "false");
}
document.addEventListener("click", e => { if (!e.target.closest("#rf .picker") && e.target.isConnected) closePicker(); });

// the form from the URL (main.js)
export function setNetState(u) {
  v.lines = new Set((u.get("lines") || "").split(",").filter(Boolean));
  setMode(v.lines.size ? null : u.get("mode") || "all");
  const [a, b] = (u.get("hours") || "").split("-").map(Number);
  $("rh0").value = a >= 4 && a <= 27 ? a : DEF_HOURS[0]; $("rh1").value = b >= 5 && b <= 28 ? b : DEF_HOURS[1];
  rangeHours("rh1");
  v.focusAsked = u.get("focus");
  v.metric = u.get("metric") === "vr" ? "vr" : "v";
  $("rcmp").checked = !!(u.get("cfrom") && u.get("cto")); $("rcfrom").value = u.get("cfrom") || ""; $("rcto").value = u.get("cto") || "";
  cmpState();
  $("rmetric").querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", String(x.dataset.m === v.metric)));
  drawBadges();
}
// what the form asks for (the sticky bar and the "settings changed" hint read this)
function rankParams() {
  const p = new URLSearchParams();
  if ($("rfrom").value) p.set("from", $("rfrom").value);
  if ($("rto").value) p.set("to", $("rto").value);
  p.set("dow", dowGet($("rdow")));
  p.set("hours", `${$("rh0").value}-${$("rh1").value}`);
  if (v.lines.size) p.set("lines", [...v.lines].join(",")); else p.set("mode", rmode() || "all");
  if ($("rterm").checked) p.set("terminus", "true");
  if (cmpOn()) { p.set("cfrom", $("rcfrom").value); p.set("cto", $("rcto").value); }
  return XR.params(p);
}
// the API's parameters (the comparison is two calls), for the reference period when ref
function apiParams(ref) {
  const p = rankParams(), a = p.get("cfrom"), b = p.get("cto");
  p.delete("cfrom"); p.delete("cto");
  if (ref) { p.set("from", a); p.set("to", b); }
  return p;
}
// the displayed window of a focus: a month within the period, or the period
function focusWindow(f, q) {
  if (f === "all" || !f) return [q.first, q.last];
  const a = f + "-01", b = monthEnd(f);
  return [a < q.first ? q.first : a, b > q.last ? q.last : b];
}
function pushUrl() {
  const p = rankParams();
  if (v.focus) p.set("focus", v.focus);
  if (v.metric !== "v") p.set("metric", v.metric);
  if ($("rtop").value !== "50") p.set("top", $("rtop").value);
  history.replaceState(null, "", "/classement?" + p);
}

// ---------------------------------------------------------------- run: indicators, then map + ranking
let NK = 0, FK = 0;
export async function runRank() {
  const p = rankParams();
  const me = ++NK;
  ran("rf", p);
  selChanged(true);
  $("rgo").disabled = true;
  $("rall").classList.add("loading");
  busy($("rstatus"), "Calcul des indicateurs", "");
  try {
    const cmp = cmpOn();
    const [R, R2] = await Promise.all([getJSON("/api/network?" + apiParams(false)), cmp ? getJSON("/api/network?" + apiParams(true)) : null]);
    if (me !== NK) return;
    v.R = R; v.R2 = R2 && R2.months.length ? R2 : null;
    if (!$("rfrom").value) $("rfrom").value = R.query.first;
    if (!$("rto").value) $("rto").value = R.query.last;
    ran("rf", rankParams());
    if (!R.months.length) {
      $("rnet").innerHTML = `<div class="notice">Aucune donnée calculée sur cette période.</div>`;
      $("rout").innerHTML = ""; $("rmaphead").hidden = true;
      done($("rstatus"), "");
      return;
    }
    pickFocus(R);
    drawNet(R, v.focus, chooseFocus, v.R2);
    if (cmp && !v.R2) $("rnet").insertAdjacentHTML("afterbegin", `<div class="notice">Aucune donnée calculée sur la période de référence (${esc(fdate($("rcfrom").value))} → ${esc(fdate($("rcto").value))}) : la comparaison n'est pas affichée.</div>`);
    selChanged();
    done($("rstatus"), "");
    await runFocus();
  } catch (e) {
    if (me === NK) { done($("rstatus"), esc(e.message), true); $("rnet").innerHTML = ""; $("rout").innerHTML = ""; }
  } finally {
    if (me === NK) { $("rgo").disabled = false; $("rall").classList.remove("loading"); }
  }
}
function pickFocus(R) {
  const ms = R.months, spanDays = (new Date(R.query.last) - new Date(R.query.first)) / 864e5;
  const complete = [...ms].reverse().find(m => m.complete) || ms[ms.length - 1];
  const asked = v.focusAsked;
  v.focus = asked === "all" && spanDays <= 400 ? "all" : ms.some(m => m.month === asked) ? asked : complete.month;
  v.focusAsked = v.focus;
  $("rfocus").innerHTML = [...ms].reverse().map(m => `<option value="${m.month}">${esc(fmonth(m.month))}</option>`).join("")
    + (spanDays <= 400 ? `<option value="all">Toute la période</option>` : "");
  $("rfocus").value = v.focus;
  $("rmaphead").hidden = false;
  $("rfocus").closest("label").hidden = !!v.R2;   // the comparison maps the two whole periods
}
function chooseFocus(f) {
  v.focus = v.focusAsked = f;
  $("rfocus").value = f;
  setFocus(f);
  runFocus();
}
$("rfocus").addEventListener("change", e => chooseFocus(e.target.value));
$("rtop").addEventListener("change", () => runFocus());
$("rterm").addEventListener("change", () => runRank());
$("rmetric").addEventListener("click", e => {
  const b = e.target.closest("button"); if (!b) return;
  v.metric = b.dataset.m;
  $("rmetric").querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", String(x === b)));
  if (v.R2 && v.MA) drawCompare(v.MA, v.MB); else paintBase();
  pushUrl();
});
async function runFocus() {
  if (v.R2) return runCompare();
  const q = v.R.query, [a, b] = focusWindow(v.focus, q), base = apiParams(false);
  base.set("from", a); base.set("to", b);
  const rp = new URLSearchParams(base); rp.set("top", $("rtop").value);
  rp.delete("mode"); if (!v.lines.size) rp.set("mode", rmode() || "all");
  const me = ++FK;
  pushUrl();
  $("rout").classList.add("loading");
  busy($("rstatus"), "Carte et classement", "");
  try {
    const [RK, MP] = await Promise.all([getJSON("/api/ranking?" + rp), getJSON("/api/network/map?" + base)]);
    if (me !== FK) return;
    v.RK = RK; v.MP = MP;
    drawRank(RK, MP);
    done($("rstatus"), "");
  } catch (e) {
    if (me === FK) { done($("rstatus"), esc(e.message), true); $("rout").innerHTML = ""; }
  }
  if (me === FK) $("rout").classList.remove("loading");
}
// the comparison's map: both whole periods (a map covers at most 400 days)
async function runCompare() {
  const me = ++FK, q = v.R.query, r = v.R2.query, days = x => (new Date(x.last) - new Date(x.first)) / 864e5;
  pushUrl();
  if (days(q) > 400 || days(r) > 400) {
    v.MA = v.MB = null;
    $("rout").innerHTML = `<div class="notice">La carte compare deux périodes d'au plus 400 jours chacune. Raccourcissez la période ou la référence pour voir les tronçons qui ont changé.</div>`;
    return;
  }
  $("rout").classList.add("loading");
  busy($("rstatus"), "Carte des deux périodes", "");
  try {
    const [MA, MB] = await Promise.all([getJSON("/api/network/map?" + apiParams(false)), getJSON("/api/network/map?" + apiParams(true))]);
    if (me !== FK) return;
    v.MA = MA; v.MB = MB;
    drawCompare(MA, MB);
    done($("rstatus"), "");
  } catch (e) {
    if (me === FK) { done($("rstatus"), esc(e.message), true); $("rout").innerHTML = ""; }
  }
  if (me === FK) $("rout").classList.remove("loading");
}
$("rf").addEventListener("submit", e => { e.preventDefault(); runRank(); });
watch("rf", rankParams, "rall");
$("rlede").textContent = "Vitesses et kilomètres de la STIB, mois par mois, pour tout le réseau, un mode ou les lignes de votre choix. La carte situe les ralentissements, le classement liste les tronçons où l'on perd le plus de temps.";

// ---------------------------------------------------------------- ranking
const RREASON = {terminus: "terminus", short_terminus: "terminus partiel", holding: "attente pour l'horaire"};
const RREASON_LONG = {terminus: "terminus de la ligne (pause entre deux courses)", short_terminus: "terminus partiel : des véhicules y stationnent plus de 3 min",
  holding: "arrêt où des véhicules attendent plus de 3 min pour respecter l'horaire"};
const PHONE = matchMedia("(max-width:600px)");
const PEAK = {am: "surtout le matin", pm: "surtout le soir", day: "toute la journée"};
const hoursOf = q => (q.hours || DEF_HOURS);
const hrange = q => { const [a, b] = hoursOf(q); return `entre ${hourLab(a)} et ${hourLab(b)}`; };
function rankSummary(R, S_, top, modeTxt) {
  const q = R.query;
  const daysTxt = q.dow.length === 5 && q.dow.join() === "0,1,2,3,4" ? "jours ouvrables" : "jours " + q.dow.map(d => DOW[d]).join(", ");
  const xd = Object.entries(R.days_excluded || {}).map(([k, n]) => `${n} ${(REASONS[k] || [k, k])[n > 1 ? 1 : 0]}`);
  const exTxt = xd.length ? ` hors ${xd.join(", ")}` : "";
  return `<p class="rk-sum">Du <b>${fdate(q.first)}</b> au <b>${fdate(q.last)}</b>, ${daysTxt}${exTxt}, ${modeTxt} (${R.lines_used} ligne${R.lines_used > 1 ? "s" : ""}) :
      les ${S_.length} premiers tronçons font perdre <b>${nf(top, 0)} heures-véhicule par jour</b> ${hrange(q)}, entre les arrêts, par rapport à la soirée (20 h–minuit). C'est ${nf(Math.min(100, 100 * top / Math.max(R.total_veh_h_day, 1e-9)), 0)} % du temps perdu sur l'ensemble de la sélection (${nf(R.total_veh_h_day, 0)} heures).</p>`;
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
const KMH = [[4.5, "#7a1020"], [7.5, "#e5484d"], [10.5, "#f0a13a"], [13.5, "#b8e27f"], [16.5, "#3fb950"], [21, "#1f8f45"], [27, "#11683e"]];
const NULLC = "#3a4560", METROC = "#8f9ab2";
// the site's speed scale (same stops as the line page), in sRGB: enough for a map
function kmhColor(k) {
  if (k == null) return NULLC;
  if (k <= KMH[0][0]) return KMH[0][1];
  for (let i = 0; i < KMH.length - 1; i++) {
    if (k <= KMH[i + 1][0]) {
      const u = (k - KMH[i][0]) / (KMH[i + 1][0] - KMH[i][0]), hx = h => [1, 3, 5].map(j => parseInt(h.slice(j, j + 2), 16));
      const A = hx(KMH[i][1]), B = hx(KMH[i + 1][1]);
      return `rgb(${A.map((a, j) => Math.round(a + (B[j] - a) * u)).join(",")})`;
    }
  }
  return KMH[KMH.length - 1][1];
}
function drawRank(R, MP) {
  const q = R.query, S_ = R.stretches || [];
  const modeTxt = q.lines ? (q.lines.length > 1 ? `les lignes ${q.lines.map(esc).join(", ")}` : `la ligne ${esc(q.lines[0])}`) : {all: "toutes les lignes", tram: "les trams", bus: "les bus", metro: "le métro"}[q.mode];
  const top = S_.reduce((a, s) => a + Math.max(0, s.veh_h_day), 0);
  const max = Math.max(1e-9, ...S_.map(s => s.veh_h_day));
  const metro = (q.mode === "metro" || (q.lines && q.lines.every(l => (lineOf(l) || {}).mode === "metro"))) && R.lines_used;
  const none = `<div class="notice">Aucun tronçon en perte de temps pour ${modeTxt} sur cette période.${metro ? " La STIB ne situe les rames de métro qu'aux stations : on ne peut pas mesurer leur vitesse entre deux stations." : ""}</div>`;
  $("rout").innerHTML = `<div class="rkgrid">
      <div class="mapcol"><div id="rkmap"></div><div class="maplg" id="rklg"></div></div>
      <div class="rklist"><h2 class="fh">Les tronçons les plus coûteux</h2>${S_.length ? `${rankSummary(R, S_, top, modeTxt)}
      <table class="tbl"><thead><tr><th>#</th><th>Tronçon et lignes</th><th style="text-align:right">h-véh. / jour</th><th style="text-align:right">par an</th><th style="text-align:right" title="secondes perdues par véhicule, par rapport à la soirée (20 h–minuit)">s / véhicule</th></tr></thead>
      <tbody>${rankRows(R, S_, max)}</tbody></table>` : none}</div>
    </div>
    ${rankExcluded(R.excluded || {n: 0, top: []})}
    <p class="method">Le temps moyen d'un véhicule sur chaque tronçon est comparé à celui de la soirée (20 h–minuit, mêmes jours), puis multiplié par le nombre de passages ${hrange(q)}. Les zones d'arrêt et les heures où le flux est incomplet ne comptent pas, pas plus que les jours exclus dans les options. Le chiffre par an multiplie par ${R.days_per_year} jours : c'est un ordre de grandeur.</p>`;
  drawRankMap(S_, MP);
  bindRankRows();
}
// a stretch's tooltip on the map: rank and stops, line badges, time lost per day and per vehicle
function rankTip(s) {
  const lb = x => badge(lineOf(x.line) || {line: x.line});
  return `<b>${s.rank}. ${esc(s.from)} → ${esc(s.to)}</b><span class="lines">${s.lines.map(lb).join("")}</span>`
    + `${nf(s.veh_h_day, 1)} h-véh. perdues par jour · ${nf(s.s_per_passage, 0)} s par véhicule`;
}
function linkTip(l) {
  const lb = x => badge(lineOf(x) || {line: x});
  const sp = l.metro ? `<span>Pas de vitesse : rames situées aux stations seulement</span>`
    : `<b class="tv">${nf(l.vr, 1)} km/h</b> entre arrêts · <b class="tv">${nf(l.v, 1)} km/h</b> avec arrêts`;
  return `<b>${esc(l.f)} → ${esc(l.t)}</b><span class="lines">${l.l.slice(0, 10).map(lb).join("")}</span>${sp}<br>${nf(l.pd, 0)} passages par jour · ${nf(l.len)} m${l.term ? " · terminus (attente comprise)" : ""}${l.reg ? "<br>Arrêt de régulation : les véhicules y attendent, hors des vitesses" : ""}`;
}
// to the right of the direction of travel, so both directions show when zoomed in
function offsetPath(c, m = 9) {
  const k = Math.cos(c[0][1] * Math.PI / 180), dy = m / 111320, dx = dy / k, out = [];
  for (let i = 0; i < c.length; i++) {
    const a = c[Math.max(0, i - 1)], b = c[Math.min(c.length - 1, i + 1)];
    const ex = (b[0] - a[0]) * k, ey = b[1] - a[1], n = Math.hypot(ex, ey) || 1;
    out.push([c[i][1] - ex / n * dy, c[i][0] + ey / n * dx]);
  }
  return out;
}
let RMAP = null, RLAYERS = [], BASE = [];
function paintBase() {
  for (const {l, pl} of BASE) {
    const val = l[v.metric];
    pl.setStyle({color: l.metro ? METROC : l.term ? NULLC : kmhColor(val), weight: 3, opacity: l.term && !l.metro ? .7 : .95});
  }
  $("rklg") && ($("rklg").innerHTML = legendHtml());
}
function legendHtml() {
  const stops = KMH.map(([k, c]) => `${c} ${100 * (k - 4.5) / 22.5}%`).join(",");
  const ticks = [6, 12, 18, 24].map(k => `<i style="left:${100 * (k - 4.5) / 22.5}%">${k}</i>`).join("");
  return `<div class="lgh"><b>${v.metric === "vr" ? "Vitesse entre arrêts" : "Vitesse avec arrêts"}</b> <span>km/h</span></div><div class="lgb" style="background:linear-gradient(90deg,${stops})"></div><div class="lgt">${ticks}</div>
    <div class="lgk"><span><b class="rkicon lgn">1</b> tronçon du classement</span><span><i class="lgg"></i> terminus, régulation (attente)</span><span><i class="lgg lgm"></i> métro : pas de vitesse</span></div>`;
}
function drawRankMap(S_, MP) {
  if (RMAP) { RMAP.remove(); RMAP = null; }
  RMAP = L.map("rkmap", {scrollWheelZoom: false, preferCanvas: true, zoomSnap: .25});
  L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
    {attribution: "Tiles © Esri — Esri, HERE, Garmin, © OpenStreetMap contributors", maxZoom: 16}).addTo(RMAP);
  const cv = L.canvas({tolerance: 6});
  BASE = (MP.links || []).map(l => {
    const pl = L.polyline(offsetPath(l.c), {renderer: cv, lineCap: "butt"}).bindTooltip(() => linkTip(l), {sticky: true, className: "rktip"}).addTo(RMAP);
    return {l, pl};
  });
  paintBase();
  RLAYERS = [];
  const top = L.svg();
  [...S_].reverse().forEach(s => {
    const geos = [s.c, ...(s.cs || [])].filter(c => c && c.length >= 2);
    if (!geos.length) return;
    const lls = geos.map(c => c.map(([x, y]) => [y, x]));
    const tip = rankTip(s), pick = () => selectRow(s.rank - 1, false);
    const pl = L.polyline(lls, {renderer: top, color: "#fff", weight: 2 + 3 * (s.veh_h_day / S_[0].veh_h_day), opacity: .9}).bindTooltip(tip, {sticky: true, className: "rktip"}).addTo(RMAP).on("click", pick);
    RLAYERS[s.rank - 1] = pl;
    if (s.rank <= 15) {
      const ll = lls[0], mid = ll[Math.floor(ll.length / 2)];
      L.marker(mid, {icon: L.divIcon({className: "rkicon", html: String(s.rank)})}).bindTooltip(tip, {className: "rktip", direction: "top", offset: [0, -10]}).addTo(RMAP).on("click", pick);
    }
  });
  const all = [...BASE.map(b => b.pl), ...RLAYERS.filter(Boolean)];
  if (all.length) RMAP.fitBounds(L.featureGroup(all).getBounds(), {padding: [8, 8]});
  else RMAP.setView([50.85, 4.35], 12);
}
// ---------------------------------------------------------------- comparison map and lists
const DV = 4;   // km/h at the ends of the scale
const DVC = {slow: [229, 72, 77], mid: [107, 116, 136], fast: [57, 135, 229]};
function dvColor(x) {
  if (x == null) return NULLC;
  const u = Math.max(-1, Math.min(1, x / DV)), to = u < 0 ? DVC.slow : DVC.fast, w = Math.abs(u);
  return `rgb(${DVC.mid.map((c, i) => Math.round(c + (to[i] - c) * w)).join(",")})`;
}
const MIN_PD = 5;    // passages a day in both periods for a stretch to be listed
function cmpRows(MA, MB) {
  const ref = new Map((MB.links || []).map(l => [l.k, l])), m = v.metric;
  return (MA.links || []).map(l => {
    const r = ref.get(l.k), a = l[m], b = r ? r[m] : null;
    const ok = a != null && b != null && a > 0 && b > 0;
    const ds = ok ? l.len / (a / 3.6) - l.len / (b / 3.6) : null;      // seconds per vehicle, + = slower now
    return {l, r, a, b, dv: ok ? a - b : null, ds, dh: ok ? ds * l.pd / 3600 : null, listed: ok && l.pd >= MIN_PD && r.pd >= MIN_PD};
  });
}
function cmpTable(xs, kind) {
  if (!xs.length) return `<p class="lbl">Aucun tronçon.</p>`;
  const lb = x => badge(lineOf(x) || {line: x});
  return `<table class="tbl"><thead><tr><th>Tronçon et lignes</th><th style="text-align:right">km/h</th><th style="text-align:right" title="secondes par véhicule">s / véh.</th><th style="text-align:right" title="heures-véhicule par jour, au nombre de passages de la période">h-véh. / jour</th></tr></thead><tbody>${xs.map(x =>
    `<tr data-k="${esc(x.l.k)}"><td><b>${esc(x.l.f)} → ${esc(x.l.t)}</b><small>${nf(x.l.len)} m · ${nf(x.l.pd, 0)} passages / jour</small><div class="lines">${x.l.l.slice(0, 8).map(lb).join("")}</div></td>
      <td class="n">${nf(x.b, 1)} → ${nf(x.a, 1)}</td><td class="n">${x.ds > 0 ? "+" : "−"}${nf(Math.abs(x.ds), 0)}</td><td class="n">${x.dh > 0 ? "+" : "−"}${nf(Math.abs(x.dh), 1)}</td></tr>`).join("")}</tbody></table>`;
}
function drawCompare(MA, MB) {
  const rows = cmpRows(MA, MB), n = PHONE.matches ? 10 : +$("rtop").value, metric = v.metric === "vr" ? "entre arrêts" : "avec arrêts";
  const listed = rows.filter(x => x.listed);
  const worse = listed.filter(x => x.dh > 0).sort((a, b) => b.dh - a.dh).slice(0, n), better = listed.filter(x => x.dh < 0).sort((a, b) => a.dh - b.dh).slice(0, n);
  const tot = listed.reduce((s, x) => s + x.dh, 0), q = v.R.query, r = v.R2.query;
  $("rout").innerHTML = `<div class="rkgrid">
      <div class="mapcol"><div id="rkmap"></div><div class="maplg" id="rklg"></div></div>
      <div class="rklist cmplists"><h2 class="fh">Ce qui a changé</h2>
        <p class="rk-sum">Vitesse ${metric}, du <b>${fdate(q.first)}</b> au <b>${fdate(q.last)}</b> contre du <b>${fdate(r.first)}</b> au <b>${fdate(r.last)}</b>. Sur les ${nf(listed.length)} tronçons mesurés dans les deux périodes, les véhicules passent <b>${nf(Math.abs(tot), 0)} heures-véhicule ${tot >= 0 ? "de plus" : "de moins"} par jour</b>, au nombre de passages de la période.</p>
        <h3>Les plus ralentis</h3>${cmpTable(worse, "worse")}
        <h3>Les plus accélérés</h3>${cmpTable(better, "better")}</div>
    </div>
    <p class="method">Pour chaque tronçon, le temps d'un véhicule (longueur divisée par la vitesse) dans la période est comparé à celui de la référence, puis multiplié par les passages par jour de la période. Seuls les tronçons avec au moins ${MIN_PD} passages par jour dans les deux périodes sont listés ; terminus, arrêts de régulation, jours exclus et heures au flux incomplet ne comptent pas, comme ailleurs sur la page.</p>`;
  if (RMAP) { RMAP.remove(); RMAP = null; }
  RMAP = L.map("rkmap", {scrollWheelZoom: false, preferCanvas: true, zoomSnap: .25});
  L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
    {attribution: "Tiles © Esri — Esri, HERE, Garmin, © OpenStreetMap contributors", maxZoom: 16}).addTo(RMAP);
  const cv = L.canvas({tolerance: 6}), byK = new Map();
  const tip = x => `<b>${esc(x.l.f)} → ${esc(x.l.t)}</b><span class="lines">${x.l.l.slice(0, 10).map(y => badge(lineOf(y) || {line: y})).join("")}</span>`
    + (x.dv == null ? "<span>Pas de vitesse dans l'une des deux périodes</span>" : `<b class="tv">${nf(x.b, 1)} → ${nf(x.a, 1)} km/h</b> (${x.dv > 0 ? "+" : "−"}${nf(Math.abs(x.dv), 1)}) · ${x.ds > 0 ? "+" : "−"}${nf(Math.abs(x.ds), 0)} s par véhicule`)
    + `<br>${nf(x.l.pd, 0)} passages par jour · ${nf(x.l.len)} m`;
  BASE = [];
  for (const x of rows) {
    const pl = L.polyline(offsetPath(x.l.c), {renderer: cv, lineCap: "butt", color: dvColor(x.dv), weight: x.listed ? 3 : 2, opacity: x.dv == null ? .6 : .95})
      .bindTooltip(() => tip(x), {sticky: true, className: "rktip"}).addTo(RMAP);
    byK.set(x.l.k, pl); BASE.push({l: x.l, pl});
  }
  if (BASE.length) RMAP.fitBounds(L.featureGroup(BASE.map(b => b.pl)).getBounds(), {padding: [8, 8]}); else RMAP.setView([50.85, 4.35], 12);
  const stops = [-DV, 0, DV].map(x => `${dvColor(x)} ${100 * (x + DV) / (2 * DV)}%`).join(",");
  $("rklg").innerHTML = `<div class="lgh"><b>Écart de vitesse ${metric}</b> <span>km/h, période moins référence</span></div><div class="lgb" style="background:linear-gradient(90deg,${stops})"></div>
    <div class="lgt">${[-4, -2, 0, 2, 4].map(k => `<i style="left:${100 * (k + DV) / (2 * DV)}%">${k > 0 ? "+" : ""}${k}</i>`).join("")}</div><div class="lgd"><span>plus lent</span><span>plus rapide</span></div>
    <div class="lgk"><span><i class="lgg"></i> pas de vitesse dans l'une des périodes</span></div>`;
  let sel = null;
  $("rout").querySelectorAll(".cmplists tbody tr").forEach(tr => tr.addEventListener("click", e => {
    if (e.target.closest("a")) return;
    const pl = byK.get(tr.dataset.k); if (!pl) return;
    $("rout").querySelectorAll(".cmplists tr.sel").forEach(x => x.classList.remove("sel")); tr.classList.add("sel");
    if (sel) sel.pl.setStyle({color: sel.c, weight: 3});
    sel = {pl, c: pl.options.color}; pl.setStyle({color: "#F3C300", weight: 6}); pl.bringToFront();
    RMAP.fitBounds(pl.getBounds(), {maxZoom: 16, padding: [60, 60]}); pl.openTooltip();
  }));
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
  RLAYERS.forEach((x, j) => x && x.setStyle({color: j === i ? "#F3C300" : "#fff"}));
  const l = RLAYERS[i];
  if (l) { if (zoom) RMAP.fitBounds(l.getBounds(), {maxZoom: 16, padding: [60, 60]}); l.openTooltip(); }
  if (!zoom) { const tr = document.querySelector(`#rout tbody tr[data-i="${i}"]`); if (tr) tr.scrollIntoView({block: "nearest", behavior: "smooth"}); }
}

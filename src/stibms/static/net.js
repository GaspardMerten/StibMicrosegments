// ---------------------------------------------------------------- network indicators: tiles, evolution, table
import {$, esc, fmonth, nf} from "./util.js?rev=__REV__";
import {lineChart, monthAxis} from "./charts.js?rev=__REV__";

// fixed per entity (never per rank); checked with the dataviz palette validator on the dark surface
export const COL = {all: "#c9d0de", bus: "#3987e5", tram: "#d95926", metro: "#199e70", sel: "#F3C300", plan: "#7f8aa3"};
const NAME = {all: "Réseau", bus: "Bus", tram: "Tram", metro: "Métro", sel: "Sélection"};
// STIB's activity report 2025 (statistical part), figures for 2025 and 2024
const STIB_URL = "https://stib-activityreports.brussels/img/STIB_statistic_report_en.pdf";
const OFFICIAL = {speed: {tram: [16.6, 16.2], bus: [15.4, 15.8], metro: [27.9, 27.9]}, km: {all: [51.35, 52.81], metro: [5.87, 5.93], tram: [14.84, 15.35], bus: [29.23, 30.08], taxibus: [1.41, 1.44]}};
const st = {R: null, scope: "all", focus: null, onFocus: null};

const millions = v => v == null ? "–" : v >= 1e6 ? `${nf(v / 1e6, 2)} million${v >= 2e6 ? "s" : ""}` : nf(Math.round(v / 100) * 100);
const thousand = v => v == null ? "–" : nf(Math.round(v / 1000));
const kmOf = (g, s) => g && (s === "all" ? {plan: g.plan_all, real: g.real_all, pct: g.pct_all} : {plan: g.plan, real: g.real, pct: g.pct});
const daysOf = (m, s) => s === "all" ? m.days_all : m.days;
const groupsShown = R => (R.selection === "sel" ? ["sel"] : []).concat(["all", "bus", "tram", "metro"]);

// a month's values, or the whole period's
function focusValues(R, key) {
  if (st.focus === "all" || !st.focus) return {g: R.total[key], days: st.scope === "all" ? R.days_all : R.days, prev: null};
  const i = R.months.findIndex(m => m.month === st.focus);
  if (i < 0) return {g: R.total[key], days: R.days_all, prev: null};
  const m = R.months[i], before = R.months[i - 1];
  const [y, n] = st.focus.split("-").map(Number);
  const pm = `${n === 1 ? y - 1 : y}-${String(n === 1 ? 12 : n - 1).padStart(2, "0")}`;
  return {g: m.groups[key], days: daysOf(m, st.scope), prev: before && before.month === pm ? before : null, m};
}
const delta = (a, b, unit, d = 1) => {
  if (a == null || b == null) return "";
  const x = a - b;
  if (Math.abs(x) < .5 * 10 ** -d) return `<em class="dl">stable</em>`;
  return `<em class="dl"><span aria-hidden="true">${x > 0 ? "▲" : "▼"}</span> ${x > 0 ? "+" : "−"}${nf(Math.abs(x), d)} ${unit}</em>`;
};
function tilesHtml(R) {
  const key = R.selection, {g, days, prev} = focusValues(R, key);
  if (!g) return "";
  const when = st.focus && st.focus !== "all" ? fmonth(st.focus) : "la période";
  const pg = prev && prev.groups[key], pm = prev ? fmonth(prev.month).split(" ")[0] : "";
  const vs = html => html ? `${html} <small>par rapport à ${esc(pm)}</small>` : "";
  const noSp = key === "metro";
  const km = kmOf(g, st.scope), kp = pg && kmOf(pg, st.scope);
  const perDay = v => days ? `${nf(Math.round(v / days / 100) * 100)} km par jour` : "";
  const nosp = `<span>La STIB ne situe les rames qu'aux stations : pas de vitesse mesurée. Objectif de la STIB : ${nf(OFFICIAL.speed.metro[0], 1)} km/h.</span>`;
  const scope = `<div class="seg sm" id="rkmscope" role="group" aria-label="Kilomètres comptés"><button type="button" data-s="all" aria-pressed="${st.scope === "all"}">Km : mois entiers</button><button type="button" data-s="sel" aria-pressed="${st.scope === "sel"}">Km : jours et heures choisis</button></div>`;
  return `<div class="fhr"><h2 class="fh">${esc(NAME[key])} · ${esc(when)}</h2>${scope}</div><div class="kpis">
    <div class="kpi"><label>Vitesse entre arrêts</label>${noSp ? `<b>n/d</b>${nosp}` : `<b>${nf(g.vr, 1)}<u> km/h</u></b><span>${vs(delta(g.vr, pg && pg.vr, "km/h")) || "hors zones d'arrêt"}</span>`}</div>
    <div class="kpi"><label>Vitesse avec arrêts</label>${noSp ? `<b>n/d</b>` : `<b>${nf(g.v, 1)}<u> km/h</u></b><span>${vs(delta(g.v, pg && pg.v, "km/h")) || "temps aux arrêts compris"}</span>`}</div>
    <div class="kpi"><label>Km planifiés</label><b>${millions(km.plan)}</b><span>${perDay(km.plan)}</span></div>
    <div class="kpi"><label>Km réalisés</label><b>${millions(km.real)}</b><span><strong>${nf(km.pct, 1)} %</strong> du planifié${kp && kp.pct != null && km.pct != null ? ` · ${delta(km.pct, kp.pct, "pts")}` : ""}</span></div>
  </div>`;
}
function speedSeries(R, field) {
  const key = R.selection, keys = key === "all" ? ["all", "bus", "tram"] : key === "metro" ? [] : [key, "all"];
  return keys.map(k => ({name: k === "all" ? "Réseau (bus + tram)" : NAME[k], color: COL[k], width: k === "all" && key !== "all" ? 1.5 : 2,
    values: Object.fromEntries(R.months.map(m => [m.month, m.groups[k] ? m.groups[k][field] : null]))}));
}
function kmSeries(R) {
  const key = R.selection, s = st.scope;
  const perDay = (m, f) => { const g = m.groups[key], d = daysOf(m, s), k = kmOf(g, s); return g && d ? Math.round(k[f] / d) : null; };
  return [{name: "Planifié", color: COL.plan, values: Object.fromEntries(R.months.map(m => [m.month, perDay(m, "plan")]))},
    {name: "Réalisé", color: key === "all" ? COL.sel : COL[key], values: Object.fromEntries(R.months.map(m => [m.month, perDay(m, "real")]))}];
}
const legend = series => `<div class="lg">${series.map(s => `<span><i style="background:${s.color}"></i>${esc(s.name)}</span>`).join("")}</div>`;
function hoursTxt(q) { const [a, b] = q.hours; return `${a} h–${b} h`; }
function daysTxt(q) {
  const d = (q.dow || []).join();
  return d === "0,1,2,3,4" ? "jours ouvrables" : d === "0,1,2,3,4,5,6" ? "tous les jours" : "jours choisis";
}
function chartsHtml(R) {
  const q = R.query, sc = speedSeries(R, "v"), sr = speedSeries(R, "vr"), km = kmSeries(R);
  const speed = R.selection === "metro" ? "" : `
    <figure class="card"><figcaption><b>Vitesse entre arrêts</b><small>km/h, ${esc(hoursTxt(q))}, ${esc(daysTxt(q))}</small></figcaption><div id="cr"></div>${legend(sr)}</figure>
    <figure class="card"><figcaption><b>Vitesse avec arrêts</b><small>km/h, ${esc(hoursTxt(q))}, ${esc(daysTxt(q))}</small></figcaption><div id="cc"></div>${legend(sc)}</figure>`;
  return `<div class="cards${speed ? "" : " one"}">${speed}
    <figure class="card"><figcaption><b>Production kilométrique</b><small>km par jour (${st.scope === "all" ? "tous les jours et toutes les heures" : esc(daysTxt(q) + ", " + hoursTxt(q))})</small></figcaption><div id="ck"></div>${legend(km)}</figure>
  </div>`;
}
function drawCharts(R) {
  const axis = monthAxis(R.months.map(m => m.month));
  const common = {axis, monthTitle: m => { const x = R.months.find(y => y.month === m); return fmonth(m) + (x ? ` · ${st.scope === "all" ? x.days_all : x.days} j` : " · pas de données"); },
    focus: st.focus, onPick: m => { if (R.months.some(x => x.month === m) && st.onFocus) st.onFocus(m); }};
  if ($("cr")) lineChart($("cr"), {...common, series: speedSeries(R, "vr"), digits: 1, label: "Vitesse entre arrêts par mois, km/h"});
  if ($("cc")) lineChart($("cc"), {...common, series: speedSeries(R, "v"), digits: 1, label: "Vitesse avec arrêts par mois, km/h"});
  lineChart($("ck"), {...common, series: kmSeries(R), digits: 0, zero: true, label: "Kilomètres par jour planifiés et réalisés, par mois"});
}
// ---------------------------------------------------------------- table: months x groups
function tableHtml(R) {
  const keys = groupsShown(R), noSp = k => k === "metro", s = st.scope;
  const head1 = keys.map(k => `<th colspan="${noSp(k) ? 3 : 5}" class="gh"><i style="background:${COL[k]}"></i>${esc(k === "all" ? "Réseau" : NAME[k])}</th>`).join("");
  const head2 = keys.map(k => `${noSp(k) ? "" : `<th title="Vitesse entre arrêts, km/h">Entre arrêts</th><th title="Vitesse avec arrêts, km/h">Avec arrêts</th>`}<th title="Km planifiés, en milliers">Plan.</th><th title="Km réalisés, en milliers">Réal.</th><th title="Km réalisés / planifiés, sur les heures où le flux fonctionne">%</th>`).join("");
  const cells = (g, k) => {
    const km = kmOf(g, s);
    return `${noSp(k) ? "" : `<td class="s0">${nf(g && g.vr, 1)}</td><td>${nf(g && g.v, 1)}</td>`}<td class="k0">${thousand(km && km.plan)}</td><td>${thousand(km && km.real)}</td><td>${nf(km && km.pct, 1)}</td>`;
  };
  const rows = [...R.months].reverse().map(m => {
    const part = m.days_all < m.days_in_month ? `<small title="${m.days_all} jours collectés sur ${m.days_in_month}"> ${m.days_all} j</small>` : "";
    return `<tr data-m="${m.month}" class="${st.focus === m.month ? "on" : ""}"><th scope="row">${esc(fmonth(m.month))}${part}</th>${keys.map(k => cells(m.groups[k], k)).join("")}</tr>`;
  }).join("");
  const tot = `<tr class="tot"><th scope="row">Toute la période</th>${keys.map(k => cells(R.total[k], k)).join("")}</tr>`;
  return `<div class="tblhead"><h2>Synthèse par mois</h2><span>Vitesses en km/h (${esc(hoursTxt(R.query))}, ${esc(daysTxt(R.query))}, bus + tram pour le réseau). Km en milliers (${s === "all" ? "tous les jours et toutes les heures" : "jours et heures choisis"}).</span><button type="button" class="mini" id="rcsv">Télécharger le CSV</button></div>
    <div class="tscroll"><table class="mt"><thead><tr><th rowspan="2" class="mh">Mois</th>${head1}</tr><tr>${head2}</tr></thead><tbody>${rows}${tot}</tbody></table></div>`;
}
function csv(R) {
  const keys = groupsShown(R), s = st.scope, rows = [["mois", "groupe", "jours", "vitesse_entre_arrets_kmh", "vitesse_avec_arrets_kmh", "km_planifies", "km_realises", "pct_realise"].join(",")];
  for (const m of R.months) for (const k of keys) {
    const g = m.groups[k], km = kmOf(g, s);
    if (g) rows.push([m.month, NAME[k], daysOf(m, s), g.vr ?? "", g.v ?? "", km.plan, km.real, km.pct ?? ""].join(","));
  }
  const a = document.createElement("a");
  a.href = URL.createObjectURL(new Blob([rows.join("\n") + "\n"], {type: "text/csv;charset=utf-8"}));
  a.download = `stib-reseau-${R.query.first}-${R.query.last}.csv`;
  a.click();
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}
function methodHtml(R) {
  const o = OFFICIAL;
  return `<details class="nm"><summary>Comment lire ces chiffres, et les repères de la STIB</summary>
    <p><b>Vitesses.</b> Distance parcourue divisée par le temps passé, mesuré par les positions envoyées toutes les 20 s. « Entre arrêts » retire les zones d'arrêt (de l'approche jusqu'au départ). Le premier et le dernier tronçon de chaque parcours ne comptent pas : on y attend au terminus. Les tronçons de régulation non plus : ceux où, ce mois-là, au moins la moitié du temps perdu vient de véhicules à l'arrêt 3 minutes ou plus. Partout ailleurs, un véhicule bloqué compte. Les heures où le flux est incomplet ne comptent pas non plus.</p>
    <p><b>Métro.</b> La STIB ne situe les rames qu'aux stations : ses kilomètres sont comptés, pas sa vitesse. La vitesse du réseau est donc celle des bus et des trams.</p>
    <p><b>Km planifiés.</b> Les trajets du GTFS en vigueur ce jour-là, chacun réparti sur les heures où il roule. Un jour de grève, le GTFS contient parfois déjà le service réduit : le planifié est alors le plan revu. <b>Km réalisés.</b> Les passages observés multipliés par la longueur de chaque tronçon. Le % ne compte que les heures où le flux fonctionne : une ligne absente d'un flux qui fonctionne n'a pas roulé. Ce % situe bien l'évolution d'un mois à l'autre ; son niveau reste approximatif (environ 1 % d'écart de longueur entre sources, passages manqués possibles).</p>
    <p><b>Repères officiels</b> (<a href="${STIB_URL}" target="_blank" rel="noopener">rapport statistique STIB 2025</a>). Vitesse cible, moyenne de la semaine, service d'hiver : tram ${nf(o.speed.tram[0], 1)} km/h en 2025 (${nf(o.speed.tram[1], 1)} en 2024), bus ${nf(o.speed.bus[0], 1)} (${nf(o.speed.bus[1], 1)}), métro ${nf(o.speed.metro[0], 1)} (${nf(o.speed.metro[1], 1)}). Pour comparer, choisissez tous les jours et toutes les heures. Kilomètres en service commercial : ${nf(o.km.all[0], 2)} millions en 2025 (${nf(o.km.all[1], 2)} en 2024), dont bus ${nf(o.km.bus[0], 2)}, tram ${nf(o.km.tram[0], 2)}, métro ${nf(o.km.metro[0], 2)} et taxibus ${nf(o.km.taxibus[0], 2)}. Les taxibus ne sont pas dans nos données.</p>
  </details>`;
}
function draw() {
  const R = st.R;
  if (!R) return;
  $("rnet").innerHTML = tilesHtml(R) + chartsHtml(R) + tableHtml(R) + methodHtml(R);
  drawCharts(R);
  $("rkmscope").addEventListener("click", e => { const b = e.target.closest("button"); if (b) { st.scope = b.dataset.s; draw(); } });
  $("rcsv").addEventListener("click", () => csv(R));
  $("rnet").querySelectorAll("tbody tr[data-m]").forEach(tr => tr.addEventListener("click", () => st.onFocus && st.onFocus(tr.dataset.m)));
}
export function drawNet(R, focus, onFocus) { st.R = R; st.focus = focus; st.onFocus = onFocus; draw(); }
export function setFocus(f) { st.focus = f; draw(); }
new ResizeObserver(() => { if (st.R && $("ck")) { clearTimeout(st.t); st.t = setTimeout(() => drawCharts(st.R), 120); } }).observe($("rnet"));

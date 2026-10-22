// ---------------------------------------------------------------- data quality
// One coloured square per day. Before the analysis: /api/coverage and the chosen exclusions; once the
// frame has loaded, its contract ({ms: "summary"}) says which days the analysis kept and why the others went.
import {$, DOW, S, esc, fdate, fmonth, nf} from "./util.js?rev=__REV__";

export const REASONS = {low_coverage: ["flux incomplet", "flux incomplet"], frozen: ["flux figé", "flux figé"], no_data: ["sans données", "sans données"],
  line_absent: ["ligne absente du flux", "ligne absente du flux"], deviation: ["hors itinéraire", "hors itinéraire"],
  vehicle_ratio: ["nombre de véhicules anormal", "nombre de véhicules anormal"], holiday: ["férié", "fériés"],
  school_holiday: ["en vacances scolaires", "en vacances scolaires"], reduced_service: ["de grève", "de grève"],
  user_period: ["en période exclue", "en période exclue"], excluded: ["exclu", "exclus"],
  not_ingested: ["non collecté", "non collectés"], not_derived: ["en préparation", "en préparation"]};
const COL = {ok: "#5fc48a", holiday: "#8e5bb5", school_holiday: "#3aa6b9", reduced_service: "#d07a3a", user_period: "#8a93a8",
  low_coverage: "#e7a33e", frozen: "#e7a33e", line_absent: "#c0392b", no_data: "#c0392b", deviation: "#c0392b", vehicle_ratio: "#c0392b",
  not_ingested: "#3a4560", not_derived: "#4f8fd8", off: "#26324d"};
// legend: one entry per colour shown
const LEGEND = [["ok", "retenu"], ["holiday", "férié"], ["school_holiday", "vacances scolaires"], ["reduced_service", "grève"],
  ["user_period", "période exclue"], ["low_coverage", "flux incomplet"], ["line_absent", "ligne absente, sans données ou hors itinéraire"],
  ["not_derived", "en préparation"], ["not_ingested", "non collecté"], ["off", "jour non sélectionné"]];
const colOf = k => COL[k] || COL.line_absent;
// counted as one reason, "flux incomplet", as the package's period line does (missing, frozen, empty feed hours)
const FEED = ["low_coverage", "frozen", "no_data"];
const sumKey = k => FEED.includes(k) ? "low_coverage" : k;
const big = v => v >= 1e6 ? `${nf(v / 1e6, 1)} million${v >= 2e6 ? "s" : ""}` : nf(v);
let QUAL = null;      // {periods: [[label, days]], dows, reason: the run's days and exclusions, sum: the frame's summary or null}
// the run's coverage (null: none); returns the periods' summaries ([] when none)
export function setQuality(q) { QUAL = q; return q ? drawQuality() : []; }
// the frame's summary of the analysis, for the current run only
export function setSummary(m) { if (QUAL) { QUAL.sum = m; drawQuality(); } }
export function dowsSel() {
  const checked = [...$("dow").querySelectorAll("input:checked")].map(x => +x.value);
  return new Set(checked.length ? checked : [0, 1, 2, 3, 4]);
}
function covSummary(days, dows, reason, label, ps) {
  const ex = ps ? Object.fromEntries((ps.excluded || []).map(e => [e.date, e.reason])) : null;
  const cls = d => {
    if (!dows.has(d.dow)) return "off";
    if (!ex) return reason(d) || d.status;
    if (ex[d.date]) return ex[d.date] === "excluded" ? (reason(d) || "user_period") : ex[d.date];
    return d.status === "not_ingested" || d.status === "not_derived" ? d.status : "ok";
  };
  const sel = days.filter(d => dows.has(d.dow));
  const ok = ps ? ps.days : sel.filter(d => cls(d) === "ok").length;
  const by = {}, seen = new Set();
  for (const d of days) seen.add(colOf(cls(d)));
  for (const d of sel) if (cls(d) !== "ok") (by[sumKey(cls(d))] ||= []).push(d);
  const why = d => cls(d) === "off" ? "non sélectionné" : cls(d) === "ok" ? "retenu"
    : (d.holiday && cls(d) === "holiday" ? d.holiday : (REASONS[cls(d)] || [cls(d)])[0]);
  const strip = days.map(d => `<i style="background:${colOf(cls(d))}" title="${d.date} · ${DOW[d.dow]} · ${esc(why(d))}"></i>`).join("");
  const parts = Object.entries(by).map(([k, v]) => `${v.length} ${(REASONS[k] || [k, k])[v.length > 1 ? 1 : 0]}`);
  return {ok, sel: sel.length, by, seen, html: `<div class="cov">${label ? `<b class="${label === "A" ? "pa" : "pb"}">${label}</b>` : ""}<span><b>${nf(ok)}</b> jour${ok > 1 ? "s" : ""} retenu${ok > 1 ? "s" : ""} sur ${sel.length}${parts.length ? " (" + parts.join(", ") + ")" : ""}</span><span class="strip">${strip}</span></div>`};
}
function drawQuality() {
  if (!QUAL) return [];
  const dows = QUAL.dows, sum = QUAL.sum, seen = new Set();
  const covs = QUAL.periods.map(([label, days], i) => {
    const ps = sum ? (sum.compare ? sum.compare[i ? "b" : "a"] : sum.period) : null;
    const c = covSummary(days, dows, QUAL.reason, label, ps);
    c.seen.forEach(x => seen.add(x));
    return c;
  });
  const nums = sum ? `<div class="nums"><span><b>${nf(sum.segs)}</b> microsegments de ${nf(sum.seg_m || 30)} m (deux sens)</span>${sum.compare ? "" : `<span><b>${big(sum.obs)}</b> signalements sur la période</span>`}</div>` : "";
  const legend = LEGEND.filter(([k]) => seen.has(colOf(k))).map(([k, t]) => `<span><i style="background:${colOf(k)}"></i>${t}</span>`).join("");
  const merged = {ok: 0, sel: 0, by: {}};
  for (const c of covs) { merged.ok += c.ok; merged.sel += c.sel; for (const [k, v] of Object.entries(c.by)) (merged.by[k] ||= []).push(...v); }
  const xnote = `<div class="xnote"><span>Analyse <b>${esc(QUAL.xtxt)}</b> (jours exclus dans les options)</span><button type="button" class="mini" data-xopen>Modifier</button></div>`;
  $("notes").innerHTML = `<div class="dq">${xnote}${covs.map(c => c.html).join("")}${nums}<div class="legend">${legend}</div></div>` + covNotes(merged);
  return covs;
}
function covNotes(sum) {
  const notes = [];
  const nd = sum.by.not_derived || [], ni = sum.by.not_ingested || [];
  if (nd.length) {
    const ms = [...new Set(nd.map(d => d.date.slice(0, 7)))].map(fmonth);
    notes.push(`<div class="notice info"><b>Données en cours de préparation.</b> Les signalements de ${ms.join(", ")} sont collectés mais pas encore calculés${sum.ok ? " : ces jours-là sont laissés de côté pour l'instant" : ""}. Le calcul tourne en arrière-plan ; réessayez dans quelques minutes.</div>`);
  }
  const to = QUAL.periods.map(p => (p[1][p[1].length - 1] || {}).date || "").sort().pop();
  if (ni.length && ni.length === sum.sel) notes.push(S.info.ingested.from && to < S.info.ingested.from
    ? `<div class="notice"><b>Pas de données</b> pour cette période : la collecte commence le ${fdate(S.info.ingested.from)}.</div>`
    : `<div class="notice"><b>Pas encore de données</b> pour cette période : la collecte va jusqu'au ${fdate(S.info.ingested.to)}.</div>`);
  return notes.join("");
}

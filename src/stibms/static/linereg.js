// ---------------------------------------------------------------- one line's regularity: per stop, per hour
// Timetable against real headways at each stop (stibms.regularity), for the direction chosen above the frame.
// Two periods (tab "Comparaison"): A and B side by side.
import {$, S, esc, fspan, getJSON, nf} from "./util.js?rev=__REV__";
import {lineChart} from "./charts.js?rev=__REV__";

const HOURS = [["Journée", 6, 21], ["Pointe du matin", 7, 10], ["Pointe du soir", 16, 19], ["Soirée", 20, 24]];
const C = {plan: "#7f8aa3", real: "#F3C300", a: "#8fb3e8", b: "#F3C300"};
const st = {base: null, periods: [], hours: [6, 21], R: [], me: 0};

const mins = x => x == null ? "–" : `${nf(x, 1)}<u> min</u>`;
const sec = x => x == null ? "–" : `${x > 0.005 ? "+" : x < -0.005 ? "−" : ""}${nf(Math.round(Math.abs(x) * 60))} s`;
const hourLab = h => h < 24 ? `${h} h` : `${h - 24} h`;

// the form's parameters (lineview.lineParams) and its periods [[label, first, last]]
export function loadRegularity(p, periods) {
  st.base = new URLSearchParams(p); st.periods = periods;
  for (const k of ["a", "b", "from", "to", "line"]) st.base.delete(k);
  st.base.set("lines", p.get("line"));
  $("regview").innerHTML = "";
  fetchAll();
}
export function clearRegularity() { st.me++; st.R = []; st.args = null; $("regview").innerHTML = ""; }
export function regDir() { if (st.R.length) draw(); }

async function fetchAll() {
  const me = ++st.me;
  $("regview").innerHTML = `<section class="regl"><h2 class="fh">Régularité</h2><p class="lbl">Calcul des intervalles entre véhicules…</p></section>`;
  try {
    const R = await Promise.all(st.periods.map(([, a, b]) => {
      const q = new URLSearchParams(st.base); q.set("from", a); q.set("to", b); q.set("hours", `${st.hours[0]}-${st.hours[1]}`);
      return getJSON("/api/network/regularity?" + q);
    }));
    if (me !== st.me) return;
    st.R = R;
    st.args = null;
    draw();
  } catch (e) {
    if (me === st.me) $("regview").innerHTML = `<section class="regl"><h2 class="fh">Régularité</h2><div class="notice">${esc(e.message)}</div></section>`;
  }
}
// the direction shown: the one chosen for the frame when this line has it
function dirOf(R) {
  const ds = R.dirs || [];
  return ds.find(d => d.dir === S.dir) || ds[0] || null;
}
function tiles(R, ref) {
  const t = R.total || {}, r = ref ? ref.total || {} : null;
  const vs = (a, b, f) => r && a != null && b != null ? `<span class="ref">${ref === st.R[0] ? "A" : "réf."} ${f(b)}</span>` : "";
  return `<div class="kpis">
    <div class="kpi"><label>Attente prévue</label><b>${mins(t.ws)}</b><span>par l'horaire, un véhicule toutes les ${nf(t.hs, 1)} min en moyenne</span>${vs(t.ws, r && r.ws, x => nf(x, 1) + " min")}</div>
    <div class="kpi"><label>Attente réelle</label><b>${mins(t.wr)}</b><span>voyageur arrivant au hasard à l'arrêt</span>${vs(t.wr, r && r.wr, x => nf(x, 1) + " min")}</div>
    <div class="kpi"><label>Attente en plus</label><b>${sec(t.ew)}</b><span>due aux écarts entre véhicules</span>${vs(t.ew, r && r.ew, sec)}</div>
    <div class="kpi"><label>Intervalles réguliers</label><b>${nf(t.ok, 1)}<u> %</u></b><span>${nf(t.bunch, 1)} % de véhicules groupés, ${nf(t.gap, 1)} % de trous</span>${vs(t.ok, r && r.ok, x => nf(x, 1) + " %")}</div>
  </div>`;
}
function stopTable(d, d0) {
  const ref = d0 ? new Map(d0.stops.map(s => [s.id, s])) : null;
  const max = Math.max(30, ...d.stops.map(s => Math.abs((s.ew || 0) * 60)));
  const rows = d.stops.map(s => {
    const r = ref && ref.get(s.id), w = Math.min(100, 100 * Math.abs((s.ew || 0) * 60) / max);
    return `<tr><th scope="row">${esc(s.name)}</th><td class="n">${nf(s.ws, 1)}</td><td class="n">${nf(s.wr, 1)}</td>
      <td class="n bar"><span class="rb${(s.ew || 0) < 0 ? " neg" : ""}" style="width:${w}%"></span><b>${sec(s.ew)}</b></td>${ref ? `<td class="n">${r ? sec(r.ew) : "–"}</td>` : ""}
      <td class="n">${nf(s.bunch, 0)} %</td><td class="n">${nf(s.gap, 0)} %</td></tr>`;
  }).join("");
  return `<div class="tscroll"><table class="tbl regt"><thead><tr><th>Arrêt</th><th class="n" title="minutes">Attente prévue</th><th class="n" title="minutes">Attente réelle</th><th class="n">En plus${ref ? " (B)" : ""}</th>${ref ? `<th class="n">En plus (A)</th>` : ""}<th class="n" title="véhicule à moins de la moitié de l'intervalle prévu derrière le précédent">Groupés</th><th class="n" title="intervalle de plus d'une fois et demie le prévu">Trous</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}
function draw() {
  const two = st.R.length === 2, R = st.R[st.R.length - 1], A = two ? st.R[0] : null;
  const d = dirOf(R), dA = A ? (A.dirs || []).find(x => d && x.dir === d.dir) : null;
  const hs = `<div class="seg sm" id="reghours" role="group" aria-label="Heures">${HOURS.map(([t, a, b]) => `<button type="button" data-h0="${a}" data-h1="${b}" aria-pressed="${a === st.hours[0] && b === st.hours[1]}">${t}</button>`).join("")}</div>`;
  const head = `<div class="fhr"><h2 class="fh">Régularité${d ? ` · vers ${esc(d.to)}` : ""}</h2>${hs}</div>`;
  if (!R.total || !d) {
    $("regview").innerHTML = `<section class="regl">${head}<div class="notice">Pas de mesure de régularité pour cette ligne sur ces jours et ces heures.</div></section>`;
    bind(); return;
  }
  const when = two ? `<p class="lbl"><b class="pa">A</b>${esc(fspan(st.periods[0][1], st.periods[0][2]))} (${nf(A.days)} j) · <b class="pb">B</b>${esc(fspan(st.periods[1][1], st.periods[1][2]))} (${nf(R.days)} j) · ${hourLab(st.hours[0])}–${hourLab(st.hours[1])}</p>`
    : `<p class="lbl">${nf(R.days)} jours · ${hourLab(st.hours[0])}–${hourLab(st.hours[1])} · tous sens confondus pour les chiffres ci-dessous</p>`;
  $("regview").innerHTML = `<section class="regl">${head}${when}${tiles(R, A)}
    <div class="cards c2">
      <figure class="card"><figcaption><b>Le long de la ligne</b><small>${two ? "attente en plus à chaque arrêt, secondes, A et B" : "attente moyenne à chaque arrêt, minutes, par l'horaire et en réalité"}</small></figcaption><div id="rgs"></div><div class="lg" id="rgsl"></div></figure>
      <figure class="card"><figcaption><b>Au fil de la journée</b><small>${two ? "attente en plus par heure, secondes, A et B" : "attente moyenne par heure, minutes, tous les arrêts"}</small></figcaption><div id="rgh"></div><div class="lg" id="rghl"></div></figure>
    </div>
    ${stopTable(d, dA)}
    <p class="method">À chaque arrêt, le temps entre deux véhicules de la ligne dans ce sens, d'après l'horaire et d'après les heures de passage réelles (flux de ponctualité de la STIB, heures observées). L'attente est celle d'un voyageur qui arrive au hasard : plus les intervalles sont inégaux, plus elle s'allonge. Un véhicule est groupé quand il suit le précédent à moins de la moitié de l'intervalle prévu, un trou quand l'intervalle dépasse une fois et demie le prévu. Mêmes jours que l'analyse, hors grèves et jours sans données ; le terminus d'arrivée et les arrêts mal couverts par le flux ne sont pas mesurés.</p>
  </section>`;
  st.args = [d, dA, R, A];
  charts(...st.args);
  bind();
}
function charts(d, dA, R, A) {
  const keys = d.stops.map((s, i) => String(i)), names = d.stops.map(s => s.name), last = keys.length - 1;
  const short = n => n.length > 16 ? n.slice(0, 15) + "…" : n;
  const xs = (k, i) => i === 0 || i === last ? short(names[i]) : "";
  const byStop = f => Object.fromEntries(d.stops.map((s, i) => [String(i), f(s)]));
  let ss, hs;
  const hk = [...new Set([...(R.hours || []), ...((A && A.hours) || [])].map(h => h.hour))].sort((a, b) => a - b).map(String);
  const byHour = (X, f) => Object.fromEntries((X.hours || []).map(h => [String(h.hour), f(h)]));
  const s60 = x => x == null ? null : Math.round(x * 60);
  if (A) {
    const ref = new Map(((dA && dA.stops) || []).map(s => [s.id, s]));
    ss = [{name: "A", color: C.a, values: byStop(s => ref.get(s.id) ? s60(ref.get(s.id).ew) : null)}, {name: "B", color: C.b, values: byStop(s => s60(s.ew))}];
    hs = [{name: "A", color: C.a, values: byHour(A, h => s60(h.ew))}, {name: "B", color: C.b, values: byHour(R, h => s60(h.ew))}];
  } else {
    ss = [{name: "Prévue (horaire)", color: C.plan, values: byStop(s => s.ws)}, {name: "Réelle", color: C.real, values: byStop(s => s.wr)}];
    hs = [{name: "Prévue (horaire)", color: C.plan, values: byHour(R, h => h.ws)}, {name: "Réelle", color: C.real, values: byHour(R, h => h.wr)}];
  }
  const dg = A ? 0 : 1, lg = s => s.map(x => `<span><i style="background:${x.color}"></i>${esc(x.name)}</span>`).join("");
  lineChart($("rgs"), {axis: keys, series: ss, digits: dg, xLabel: xs, monthTitle: k => names[+k], label: "Attente à chaque arrêt de la ligne"});
  lineChart($("rgh"), {axis: hk, series: hs, digits: dg, xLabel: k => +k % 3 === 0 ? hourLab(+k) : "", monthTitle: k => `${hourLab(+k)}–${hourLab(+k + 1)}`, label: "Attente par heure de la journée"});
  $("rgsl").innerHTML = lg(ss); $("rghl").innerHTML = lg(hs);
}
function bind() {
  $("reghours")?.addEventListener("click", e => {
    const b = e.target.closest("button"); if (!b) return;
    st.hours = [+b.dataset.h0, +b.dataset.h1];
    fetchAll();
  });
}
new ResizeObserver(() => {
  const w = $("regview").clientWidth;
  if (w === st.w) return;
  st.w = w;
  if (st.args && $("rgs")) { clearTimeout(st.t); st.t = setTimeout(() => charts(...st.args), 150); }
}).observe($("regview"));

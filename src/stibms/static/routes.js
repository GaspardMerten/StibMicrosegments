// ---------------------------------------------------------------- route changes in the period
// /api/versions: per direction, the routes run as main pattern (one per stop sequence, with the periods it
// ran); the reference is the one with the most stops. A change lists its periods and, per direction, the
// stops it drops or adds, or, with the reference's stops, its detours and moved stops (``detours``: a link
// longer or shorter by hundreds of metres); its button adds its periods to the excluded periods.
import {$, andList, cap, esc, fday, nf} from "./util.js?rev=__REV__";
import {XL} from "./excl.js?rev=__REV__";

let ROUTES = null, RGROUPS = [];
// "le 6 juin 2026", "du 6 au 28 juin 2026 (15 jours)", "du 30 mai au 2 juin 2026 (4 jours)"
function fper(p) {
  if (p.first === p.last) return `le ${fday(p.first, true)}`;
  const [a, b] = [p.first, p.last], dn = s => { const d = +s.slice(8, 10); return d === 1 ? "1er" : String(d); };
  const from = a.slice(0, 7) === b.slice(0, 7) ? dn(a) : fday(a, a.slice(0, 4) !== b.slice(0, 4));
  return `du ${from} au ${fday(b, true)} (${p.n_days} jour${p.n_days > 1 ? "s" : ""})`;
}
// one sentence for both directions: "Da Vinci, Bordet Station et Paix non desservis ; Helmet desservi en plus ;
// détour entre X et Y (+415 m)" (stop names merged over the directions, at most 5 then "et N autres")
function changeTxt(xs) {
  const uniq = a => [...new Set(a)];
  const list = a => a.length > 5 ? `${a.slice(0, 5).map(esc).join(", ")} et ${a.length - 5} autre${a.length > 6 ? "s" : ""}` : andList(a.map(esc));
  const rm = uniq(xs.flatMap(x => x.removed || [])), ad = uniq(xs.flatMap(x => x.added || []));
  const out = [];
  if (rm.length) out.push(`${list(rm)} non desservi${rm.length > 1 ? "s" : ""}`);
  if (ad.length) out.push(`${list(ad)} desservi${ad.length > 1 ? "s" : ""} en plus`);
  const m = v => `${v > 0 ? "+" : "−"}${nf(Math.abs(v))} m`;
  out.push(...uniq(xs.flatMap(x => (x.detours || []).map(t => t.kind === "moved_stop"
    ? `arrêt ${esc(t.stop)} déplacé (${nf(t.delta_m)} m)` : `détour entre ${esc(t.from)} et ${esc(t.to)} (${m(t.delta_m)})`))));
  return out.join(" ; ");
}
const ROUTE_MIN_DAYS = 5;
// the versions of /api/versions (null: none yet)
export function showRoutes(v) { ROUTES = v; drawRoutes(); }
function drawRoutes() {
  const v = ROUTES || [];
  // versions with the reference's stops are a change only with a detour or a moved stop (else: shape edits)
  // only periods of ROUTE_MIN_DAYS days or more are listed (a one-day short-working is no route change)
  const changed = v.filter(x => x.removed.length || x.added.length || (x.detours || []).length)
    .map(x => ({...x, periods: x.periods.filter(p => p.n_days >= ROUTE_MIN_DAYS)})).filter(x => x.periods.length);
  // a reference route that already ran at the start may come from a lasting change made before the period
  const lasting = v.filter(x => x.ref && x.lasting);
  if (!changed.length && !lasting.length) { $("routes").innerHTML = ""; return; }
  // directions changed on the same days share a line
  const g = new Map();
  for (const x of changed) { const k = x.periods.map(p => p.first + ".." + p.last).join(","); if (!g.has(k)) g.set(k, []); g.get(k).push(x); }
  RGROUPS = [...g.entries()].sort((a, b) => a[0] < b[0] ? -1 : 1).map(([, xs]) => ({periods: xs[0].periods, xs}));
  const lg = new Map();
  for (const x of lasting) { const k = x.lasting.since; if (!lg.has(k)) lg.set(k, []); lg.get(k).push(x); }
  const before = [...lg.entries()].map(([since, xs]) =>
    `<li><span><b>Depuis le ${fday(since, true)}</b> : ${changeTxt(xs.map(x => x.lasting))}</span></li>`).join("");
  $("routes").innerHTML = `<div class="routes"><b>Changements de tracé</b><ul>${before}${RGROUPS.map((gr, i) => {
    const ex = gr.periods.every(p => XL.covers(p.first, p.last));
    const np = gr.periods.length;
    return `<li><span><b>${cap(andList(gr.periods.map(fper)))}</b> : ${changeTxt(gr.xs)}</span><span class="acts"><button type="button" class="mini" data-excl="${i}"${ex ? " disabled" : ""}>${ex ? (np > 1 ? "Périodes exclues" : "Période exclue") : (np > 1 ? "Exclure ces périodes" : "Exclure cette période")}</button></span></li>`;
  }).join("")}</ul></div>`;
}
// "Exclure cette période": adds the group's periods to the excluded periods; true when the line must rerun
// (the listener is in lineview.js, which runs the line: no import cycle)
export function excludeRoutes(target) {
  const b = target.closest("button");
  if (!b || b.dataset.excl == null) return false;
  for (const p of RGROUPS[+b.dataset.excl].periods) XL.add(p.first, p.last);
  $("fmore").open = true;
  return true;
}

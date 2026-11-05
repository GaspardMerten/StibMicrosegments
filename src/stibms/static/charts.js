// ---------------------------------------------------------------- line chart per month (SVG)
// One value axis, thin lines, the last value labelled, hairline grid, one crosshair with every series in
// its tooltip. A month without data breaks the line. A click on a month picks it (onPick).
import {MON3, esc, nf} from "./util.js?rev=__REV__";

const svgns = "http://www.w3.org/2000/svg";
function niceTicks(lo, hi, n = 4) {
  const span = Math.max(hi - lo, 1e-9), raw = span / n, mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 5, 10].map(k => k * mag).find(s => s >= raw) || raw;
  const a = Math.floor(lo / step) * step, out = [];
  for (let v = a; v <= hi + step * .999; v += step) out.push(+v.toFixed(6));
  return out;
}
// consecutive calendar months from the first to the last with data
export function monthAxis(months) {
  if (!months.length) return [];
  const idx = m => { const [y, n] = m.split("-").map(Number); return y * 12 + n - 1; };
  const out = [];
  for (let k = idx(months[0]); k <= idx(months[months.length - 1]); k++) out.push(`${Math.floor(k / 12)}-${String(k % 12 + 1).padStart(2, "0")}`);
  return out;
}
const monLab = (m, yr) => { const [y, n] = m.split("-"); return MON3[+n - 1] + (yr ? " " + y.slice(2) : ""); };

// spec: {axis: [month] (or any keys, with xLabel(key, i) -> text or ""), series: [{name, color, width?, values: {month: y}}], unit, digits, focus, onPick, zero}
export function lineChart(el, spec) {
  el.innerHTML = "";
  el.classList.add("lc");
  const tip = document.createElement("div");
  tip.className = "lctip"; tip.hidden = true;
  const W = Math.max(280, el.clientWidth), H = 220, M = {l: 58, r: 62, t: 14, b: 26};
  const {axis, series} = spec;
  const all = series.flatMap(s => axis.map(m => s.values[m]).filter(v => v != null));
  if (!all.length) { el.innerHTML = `<p class="lcnone">Pas de valeur pour cette sélection.</p>`; return; }
  let lo = Math.min(...all), hi = Math.max(...all);
  if (spec.zero) lo = 0;
  const pad = (hi - lo || Math.abs(hi) * .1 || 1) * .12;
  if (!spec.zero) lo -= pad;
  hi += pad;
  const ticks = niceTicks(lo, hi);
  // as many decimals as the step needs: 19,5 / 20 / 20,5, never 20 / 20 / 21
  const tickDigits = Math.max(0, ...ticks.slice(1).map((t, i) => Math.ceil(-Math.log10(Math.min(1, t - ticks[i])) - 1e-9)));
  lo = Math.min(lo, ticks[0]); hi = Math.max(hi, ticks[ticks.length - 1]);
  const x = i => axis.length < 2 ? (M.l + W - M.r) / 2 : M.l + i * (W - M.l - M.r) / (axis.length - 1);
  const y = v => H - M.b - (v - lo) / (hi - lo) * (H - M.t - M.b);
  const svg = document.createElementNS(svgns, "svg");
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`); svg.setAttribute("width", W); svg.setAttribute("height", H);
  svg.setAttribute("role", "img"); svg.setAttribute("tabindex", "0");
  svg.setAttribute("aria-label", spec.label || series.map(s => s.name).join(", "));
  const parts = [];
  for (const t of ticks) parts.push(`<line class="gr" x1="${M.l}" x2="${W - M.r}" y1="${y(t)}" y2="${y(t)}"/><text class="ax" x="${M.l - 8}" y="${y(t) + 4}" text-anchor="end">${nf(t, tickDigits)}</text>`);
  // month labels: never colliding, the year at each January and at the start
  const every = Math.max(1, Math.ceil(axis.length / Math.max(2, Math.floor((W - M.l - M.r) / 62))));
  axis.forEach((m, i) => {
    if (!spec.xLabel && i % every && i !== axis.length - 1) return;      // xLabel picks its own ticks
    if (!spec.xLabel && i === axis.length - 1 && i % every && (i % every) * (W - M.l - M.r) / Math.max(1, axis.length - 1) < 50) return;
    const lab = spec.xLabel ? spec.xLabel(m, i) : monLab(m, i === 0 || m.endsWith("-01"));
    if (lab) parts.push(`<text class="ax" x="${x(i)}" y="${H - 7}" text-anchor="middle">${esc(lab)}</text>`);
  });
  const fi = axis.indexOf(spec.focus);
  if (fi >= 0) parts.push(`<line class="fc" x1="${x(fi)}" x2="${x(fi)}" y1="${M.t}" y2="${H - M.b}"/>`);
  for (const s of series) {
    let d = "", on = false;
    axis.forEach((m, i) => { const v = s.values[m]; if (v == null) { on = false; return; } d += `${on ? "L" : "M"}${x(i).toFixed(1)} ${y(v).toFixed(1)}`; on = true; });
    parts.push(`<path d="${d}" fill="none" stroke="${s.color}" stroke-width="${s.width || 2}" stroke-linejoin="round" stroke-linecap="round"/>`);
    // a month alone between gaps would draw nothing: a dot
    axis.forEach((m, i) => { if (s.values[m] != null && s.values[axis[i - 1]] == null && s.values[axis[i + 1]] == null) parts.push(`<circle cx="${x(i)}" cy="${y(s.values[m])}" r="3" fill="${s.color}"/>`); });
  }
  // the last value of each series, labels pushed apart
  const ends = series.map(s => { for (let i = axis.length - 1; i >= 0; i--) if (s.values[axis[i]] != null) return {s, i, v: s.values[axis[i]]}; return null; }).filter(Boolean).sort((a, b) => y(a.v) - y(b.v));
  let last = -99;
  for (const e of ends) {
    const yy = Math.max(y(e.v), last + 15); last = yy;
    parts.push(`<circle class="end" cx="${x(e.i)}" cy="${y(e.v)}" r="4" fill="${e.s.color}"/><text class="vl" x="${x(e.i) + 9}" y="${yy + 4}">${nf(e.v, spec.digits)}</text>`);
  }
  parts.push(`<line class="ch" id="ch" x1="0" x2="0" y1="${M.t}" y2="${H - M.b}" visibility="hidden"/>`);
  svg.innerHTML = parts.join("");
  const ch = svg.querySelector(".ch");
  el.append(svg, tip);
  const show = i => {
    const m = axis[i];
    ch.setAttribute("x1", x(i)); ch.setAttribute("x2", x(i)); ch.setAttribute("visibility", "visible");
    const rows = series.map(s => `<div class="r"><i style="background:${s.color}"></i><b>${s.values[m] == null ? "–" : nf(s.values[m], spec.digits)}</b><span>${esc(s.name)}</span></div>`).join("");
    tip.innerHTML = `<div class="h">${esc(spec.monthTitle ? spec.monthTitle(m) : m)}</div>${rows}`;
    tip.hidden = false;
    const tw = tip.offsetWidth, left = x(i) + 12 + tw > W ? x(i) - tw - 12 : x(i) + 12;
    tip.style.left = Math.max(0, left) + "px"; tip.style.top = M.t + "px";
    el.dataset.i = i;
  };
  const hide = () => { ch.setAttribute("visibility", "hidden"); tip.hidden = true; delete el.dataset.i; };
  const nearest = e => {
    const r = svg.getBoundingClientRect(), px = (e.clientX - r.left) * W / r.width;
    let b = 0, bd = 1e9; axis.forEach((_, i) => { const d = Math.abs(x(i) - px); if (d < bd) { bd = d; b = i; } });
    return b;
  };
  svg.addEventListener("pointermove", e => show(nearest(e)));
  svg.addEventListener("pointerleave", hide);
  svg.addEventListener("blur", hide);
  svg.addEventListener("keydown", e => {
    const cur = el.dataset.i != null ? +el.dataset.i : (fi >= 0 ? fi : axis.length - 1);
    if (e.key === "ArrowLeft") { show(Math.max(0, cur - 1)); e.preventDefault(); }
    if (e.key === "ArrowRight") { show(Math.min(axis.length - 1, cur + 1)); e.preventDefault(); }
    if (e.key === "Enter" && spec.onPick) spec.onPick(axis[cur]);
  });
  if (spec.onPick) { svg.style.cursor = "pointer"; svg.addEventListener("click", e => spec.onPick(axis[nearest(e)])); }
}

// Shared helpers of the page: DOM, formatting, API calls, weekday pickers, status line, page state.
// The page's scripts are ES modules (index.html loads main.js); each imports what it uses, always with
// "?rev=__REV__" so every module is one instance per release.
export const $ = id => document.getElementById(id);
export const DOW = ["Lu", "Ma", "Me", "Je", "Ve", "Sa", "Di"];
export const MODE_FR = {metro: "Métro", tram: "Tram", bus: "Bus", night: "Bus de nuit", autre: "Autres"};
export const MONTHS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"];
export const MON3 = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."];
// state shared by the modules: the lines of /api/lines, their months and ingested span, the tab shown,
// the direction asked for (line page)
export const S = {lines: [], info: null, tab: "one", dir: null};
export const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
export const iso = d => d.toISOString().slice(0, 10);
export const addDays = (s, n) => { const d = new Date(s + "T12:00:00Z"); d.setUTCDate(d.getUTCDate() + n); return iso(d); };
export const fdate = s => { const d = new Date(s + "T12:00:00Z"); return `${d.getUTCDate() === 1 ? "1er" : d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`; };
export const fday = (s, y) => { const d = new Date(s + "T12:00:00Z"); return `${d.getUTCDate() === 1 ? "1er" : d.getUTCDate()} ${MON3[d.getUTCMonth()]}${y ? " " + d.getUTCFullYear() : ""}`; };
export const fspan = (a, b) => a === b ? fday(a, true) : `${fday(a, a.slice(0, 4) !== b.slice(0, 4))} → ${fday(b, true)}`;
export const fmonth = m => { const [y, mm] = m.split("-"); return `${MONTHS[+mm - 1]} ${y}`; };
export const nf = (v, d = 0) => v == null ? "–" : (+v).toLocaleString("fr-BE", {minimumFractionDigits: d, maximumFractionDigits: d});
export const monthEnd = m => { const [y, mm] = m.split("-").map(Number); return iso(new Date(Date.UTC(y, mm, 0, 12))); };
export const andList = a => a.length > 1 ? a.slice(0, -1).join(", ") + " et " + a[a.length - 1] : a.join("");
export const cap = s => s.charAt(0).toUpperCase() + s.slice(1);

// REV: the release (set by the server); in every API URL so a new release never reads an older cached response
export const REV = "__REV__";
// error bodies: FastAPI JSON (detail: a string, or a list of {msg} for invalid parameters)
export const detailText = d => Array.isArray(d) ? d.map(x => (x && x.msg) || x).join(" ; ") : d;
export async function getJSON(url) {
  const r = await fetch(url + (url.includes("?") ? "&" : "?") + "rev=" + REV);
  let body = null;
  try { body = await r.json(); } catch (e) { /* not JSON */ }
  if (!r.ok) throw new Error((body && detailText(body.detail)) || r.statusText || ("HTTP " + r.status));
  return body;
}

// ---------------------------------------------------------------- weekday pickers
export function dowBoxes(el, name) {
  for (let i = 0; i < 7; i++) {
    const l = document.createElement("label");
    l.innerHTML = `<input type="checkbox" name="${name}" value="${i}" ${i < 5 ? "checked" : ""}>${DOW[i]}`;
    el.appendChild(l);
  }
}
dowBoxes($("dow"), "dow"); dowBoxes($("rdow"), "rdow");
export function dowGet(el) {
  const v = [...el.querySelectorAll("input:checked")].map(x => +x.value);
  if (!v.length) return "0-4";
  const contiguous = v.every((x, i) => i === 0 || x === v[i - 1] + 1);
  return contiguous && v.length > 1 ? `${v[0]}-${v[v.length - 1]}` : v.join(",");
}
export function dowSet(el, s) {
  if (!s) return;
  const set = new Set();
  for (const part of s.split(",")) {
    if (part.includes("-")) { const [a, b] = part.split("-").map(Number); for (let i = a; i <= b; i++) set.add(i); }
    else if (part !== "") set.add(+part);
  }
  el.querySelectorAll("input").forEach(i => i.checked = set.has(+i.value));
}

// ---------------------------------------------------------------- status line (one timer per element)
export function busy(el, msg, hint) {
  clearInterval(el._timer);
  const t0 = performance.now();
  const draw = () => {
    const s = Math.round((performance.now() - t0) / 1000);
    el.className = ""; el.innerHTML = `<span class="spin"></span><span>${msg}${s >= 2 ? ` · ${s} s` : ""}${s >= 6 && hint ? ` · ${hint}` : ""}</span>`;
  };
  draw(); el._timer = setInterval(draw, 1000);
}
export function done(el, msg, err) { clearInterval(el._timer); el.className = err ? "err" : ""; el.innerHTML = msg ? `<span>${msg}</span>` : ""; }

// ---------------------------------------------------------------- selection changed
// the sticky selection bar (sel.js) redraws on this event; run: a new analysis or ranking started
// (the bar's open panel closes)
export const selChanged = run => document.dispatchEvent(new CustomEvent("ms:sel", {detail: {run: !!run}}));
// the weekdays ticked, in words: "Lu–Ve", "Lu, Me, Ve", "tous les jours"
export function dowText(el) {
  const v = [...el.querySelectorAll("input:checked")].map(x => +x.value);
  if (!v.length || v.length === 7) return v.length ? "tous les jours" : "Lu–Ve";
  const run = v.length > 2 && v.every((x, i) => i === 0 || x === v[i - 1] + 1);
  return run ? `${DOW[v[0]]}–${DOW[v[v.length - 1]]}` : v.map(i => DOW[i]).join(", ");
}

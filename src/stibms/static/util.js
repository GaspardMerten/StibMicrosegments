// Shared helpers of the page: DOM, formatting, API calls, weekday pickers, status line.
// The page's scripts are classic scripts sharing one scope, loaded in order (index.html); main.js boots.
"use strict";
const $ = id => document.getElementById(id);
const DOW = ["Lu", "Ma", "Me", "Je", "Ve", "Sa", "Di"];
const MODE_FR = {metro: "Métro", tram: "Tram", bus: "Bus", night: "Bus de nuit", autre: "Autres"};
const MONTHS = ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"];
const MON3 = ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."];
let LINES = [], INFO = null, TAB = "one";
const esc = s => String(s == null ? "" : s).replace(/[&<>"]/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"}[c]));
const iso = d => d.toISOString().slice(0, 10);
const addDays = (s, n) => { const d = new Date(s + "T12:00:00Z"); d.setUTCDate(d.getUTCDate() + n); return iso(d); };
const fdate = s => { const d = new Date(s + "T12:00:00Z"); return `${d.getUTCDate() === 1 ? "1er" : d.getUTCDate()} ${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`; };
const fday = (s, y) => { const d = new Date(s + "T12:00:00Z"); return `${d.getUTCDate() === 1 ? "1er" : d.getUTCDate()} ${MON3[d.getUTCMonth()]}${y ? " " + d.getUTCFullYear() : ""}`; };
const fspan = (a, b) => a === b ? fday(a, true) : `${fday(a, a.slice(0, 4) !== b.slice(0, 4))} → ${fday(b, true)}`;
const fmonth = m => { const [y, mm] = m.split("-"); return `${MONTHS[+mm - 1]} ${y}`; };
const nf = (v, d = 0) => v == null ? "–" : (+v).toLocaleString("fr-BE", {minimumFractionDigits: d, maximumFractionDigits: d});
const monthEnd = m => { const [y, mm] = m.split("-").map(Number); return iso(new Date(Date.UTC(y, mm, 0, 12))); };
const andList = a => a.length > 1 ? a.slice(0, -1).join(", ") + " et " + a[a.length - 1] : a.join("");
const cap = s => s.charAt(0).toUpperCase() + s.slice(1);

// REV: the release (set by the server); in every API URL so a new release never reads an older cached response
const REV = "__REV__";
// error bodies: FastAPI JSON (detail: a string, or a list of {msg} for invalid parameters)
const detailText = d => Array.isArray(d) ? d.map(x => (x && x.msg) || x).join(" ; ") : d;
async function getJSON(url) {
  const r = await fetch(url + (url.includes("?") ? "&" : "?") + "rev=" + REV);
  let body = null;
  try { body = await r.json(); } catch (e) { /* not JSON */ }
  if (!r.ok) throw new Error((body && detailText(body.detail)) || r.statusText || ("HTTP " + r.status));
  return body;
}

// ---------------------------------------------------------------- weekday pickers
function dowBoxes(el, name) {
  for (let i = 0; i < 7; i++) {
    const l = document.createElement("label");
    l.innerHTML = `<input type="checkbox" name="${name}" value="${i}" ${i < 5 ? "checked" : ""}>${DOW[i]}`;
    el.appendChild(l);
  }
}
dowBoxes($("dow"), "dow"); dowBoxes($("rdow"), "rdow");
function dowGet(el) {
  const v = [...el.querySelectorAll("input:checked")].map(x => +x.value);
  if (!v.length) return "0-4";
  const contiguous = v.every((x, i) => i === 0 || x === v[i - 1] + 1);
  return contiguous && v.length > 1 ? `${v[0]}-${v[v.length - 1]}` : v.join(",");
}
function dowSet(el, s) {
  if (!s) return;
  const set = new Set();
  for (const part of s.split(",")) {
    if (part.includes("-")) { const [a, b] = part.split("-").map(Number); for (let i = a; i <= b; i++) set.add(i); }
    else if (part !== "") set.add(+part);
  }
  el.querySelectorAll("input").forEach(i => i.checked = set.has(+i.value));
}

// ---------------------------------------------------------------- status line (one timer per element)
function busy(el, msg, hint) {
  clearInterval(el._timer);
  const t0 = performance.now();
  const draw = () => {
    const s = Math.round((performance.now() - t0) / 1000);
    el.className = ""; el.innerHTML = `<span class="spin"></span><span>${msg}${s >= 2 ? ` · ${s} s` : ""}${s >= 6 && hint ? ` — ${hint}` : ""}</span>`;
  };
  draw(); el._timer = setInterval(draw, 1000);
}
function done(el, msg, err) { clearInterval(el._timer); el.className = err ? "err" : ""; el.innerHTML = msg ? `<span>${msg}</span>` : ""; }

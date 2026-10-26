// ---------------------------------------------------------------- sticky selection bar
// Once the form has scrolled out of view, a bar fixed at the top says what is shown (line, direction,
// period(s), days, exclusions; mode and options on the ranking). A click opens the form right under
// it, over the page (the form's place keeps its height: nothing moves); a run, Escape, a click
// outside or scrolling back to the form closes it.
import {$, S, dowText, esc, fspan} from "./util.js?rev=__REV__";
import {XL, XR} from "./excl.js?rev=__REV__";
import {badge, lineOf} from "./lines.js?rev=__REV__";

const BAR = $("selbar"), BTN = $("selbtn");
const isRank = () => S.tab === "rank";
const form = () => $(isRank() ? "rf" : "f");
const place = () => form().parentElement;      // the .bar: keeps the form's height while it floats
const item = (k, v) => `<span class="si">${k ? `<b class="sk">${k}</b>` : ""}${v}</span>`;
const span = (a, b) => a && b ? esc(fspan(a, b)) : "…";

function lineItems() {
  const id = $("linebtn").dataset.line, l = lineOf(id);
  const out = [`<span class="si sl">${badge(l || {line: id})}<span class="nm">${esc($("linename").textContent)}</span></span>`];
  const dir = $("sens").querySelector("[aria-pressed=true]");
  if (!$("sensfld").hidden && dir) out.push(item("", esc(dir.textContent)));
  if (S.tab === "cmp") out.push(item('<i class="pa">A</i>', span($("a1").value, $("a2").value)), item('<i class="pb">B</i>', span($("b1").value, $("b2").value)));
  else out.push(item("Période", span($("from").value, $("to").value)));
  out.push(item("Jours", esc(dowText($("dow")))), item("", esc(XL.text())));
  return out;
}
function rankItems() {
  const mode = $("rmode").querySelector("[aria-pressed=true]");
  return [item("Mode", esc(mode ? mode.textContent : "Tous")), item("Période", span($("rfrom").value, $("rto").value)),
    item("Jours", esc(dowText($("rdow")))), item("", esc(XR.text())), item("", `${esc($("rtop").value)} tronçons`),
    ...($("rterm").checked ? [item("", "terminus inclus")] : [])];
}
function draw() {
  if (!isRank() && !$("linebtn").dataset.line) return;
  BTN.innerHTML = `<span class="sitems">${(isRank() ? rankItems() : lineItems()).join("")}</span><span class="sedit">Modifier</span>`;
}

function setOpen(open) {
  const f = form();
  if (open === f.classList.contains("float")) return;
  if (open) { place().style.minHeight = place().offsetHeight + "px"; f.classList.add("float"); }
  else { f.classList.remove("float"); place().style.minHeight = ""; }
  BTN.setAttribute("aria-expanded", String(open));
  BAR.classList.toggle("open", open);
}
const close = () => setOpen(false);
function show(on) {
  BAR.classList.toggle("on", on);
  BAR.inert = !on;
  if (!on) setOpen(false);
}

// the bar shows while the current form's place is above the viewport
const io = new IntersectionObserver(es => {
  for (const e of es) if (e.target === place()) show(!e.isIntersecting && e.boundingClientRect.bottom < 0);
});
for (const id of ["f", "rf"]) io.observe($(id).parentElement);
BTN.addEventListener("click", () => { draw(); setOpen(!form().classList.contains("float")); });
document.addEventListener("ms:sel", e => { draw(); if (e.detail.run) close(); });
document.addEventListener("keydown", e => { if (e.key === "Escape" && BAR.classList.contains("open")) { close(); BTN.focus(); } });
document.addEventListener("click", e => {
  if (BAR.classList.contains("open") && e.target.isConnected && !e.target.closest("#selbar, form.float")) close();
});
// a tab change: the other form's place is observed afresh
export function selTab() { for (const id of ["f", "rf"]) { $(id).classList.remove("float"); $(id).parentElement.style.minHeight = ""; } show(false); draw(); }

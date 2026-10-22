// ---------------------------------------------------------------- lines
"use strict";
const lineOf = id => LINES.find(l => l.line === id);
const modeKey = l => /^N/i.test(l.line) ? "night" : (l.mode === "metro" || l.mode === "tram" || l.mode === "bus" ? l.mode : "autre");
function badge(l, cls = "lb") {
  const bg = l && l.color ? "#" + l.color : "#555", fg = l && l.text_color ? "#" + l.text_color : "#fff";
  return `<span class="${cls}" style="background:${bg};color:${fg}">${esc(l ? l.line : "?")}</span>`;
}
function niceName(n) {
  if (!n) return "";
  return n.toLowerCase().replace(/(^|[\s\-'(])([a-zà-ÿ])/g, (m, a, b) => a + b.toUpperCase()).replace(/ - /g, " – ")
    .replace(/(?<=\S )(De|Du|Des|La|Le|Les|Et|Aux?|En|Sur)(?= )/g, w => w.toLowerCase())   // particles lowercase, not at the start
    .replace(/(?<=\S )(L'|D')/g, w => w.toLowerCase())
    .replace(/\b(Ulb|Vub|Wtc|Uz|Coov|Ceria|Cbr|Rtbf|Vrt)\b/g, w => w.toUpperCase());
}
function drawPicker(filter = "") {
  const f = filter.trim().toLowerCase();
  const groups = {};
  for (const l of LINES) {
    if (f && !(l.line.toLowerCase().startsWith(f) || (l.name || "").toLowerCase().includes(f))) continue;
    (groups[modeKey(l)] ||= []).push(l);
  }
  const cur = $("linebtn").dataset.line;
  $("lgroups").innerHTML = ["metro", "tram", "bus", "night", "autre"].filter(m => groups[m]).map(m =>
    `<h4>${MODE_FR[m]}</h4><div class="chips">` + groups[m].map(l =>
      `<button type="button" class="chip" role="option" data-line="${esc(l.line)}" aria-selected="${l.line === cur}"
        aria-disabled="${!l.available}" title="${esc(l.line + " · " + niceName(l.name) + (l.available ? "" : " (pas encore de données)"))}">${badge(l)}</button>`).join("") + "</div>").join("")
    || `<p class="lbl" style="margin:8px 0">Aucune ligne ne correspond.</p>`;
}
function setLine(id, keepDates) {
  const l = lineOf(id);
  $("linebtn").dataset.line = id;
  $("linebadge").outerHTML = badge(l, "lb").replace('class="lb"', 'class="lb" id="linebadge"');
  $("linename").textContent = l ? niceName(l.name) || "Ligne " + id : "Ligne " + id;
  // the whole collection range (not the line's): a period kept across lines stays valid in the form
  for (const k of ["from", "to", "a1", "a2", "b1", "b2"]) {
    $(k).min = (INFO && INFO.ingested.from) || "";
    $(k).max = (INFO && INFO.ingested.to) || "";
  }
  XL.setRange(INFO && INFO.ingested.from, INFO && INFO.ingested.to);
  if (!keepDates || !$("from").value) defaultDates(l);
  presets();
}
function defaultDates(l) {
  const av = l && l.available;
  const d = av && av.default ? av.default : (INFO ? {from: addDays(INFO.ingested.to, -89), to: INFO.ingested.to} : null);
  if (!d) return;
  $("from").value = d.from; $("to").value = d.to;
  // compare: the default period split in two halves
  const mid = addDays(d.from, Math.floor((new Date(d.to) - new Date(d.from)) / 864e5 / 2));
  $("a1").value = d.from; $("a2").value = mid; $("b1").value = addDays(mid, 1); $("b2").value = d.to;
}
function presets() {
  const l = lineOf($("linebtn").dataset.line), av = l && l.available;
  if (!av || TAB !== "one") { $("presets").innerHTML = ""; return; }
  const ms = av.months.filter(m => monthEnd(m) <= av.to);
  const opts = [];
  if (ms.length) opts.push(["Dernier mois", ms[ms.length - 1] + "-01", monthEnd(ms[ms.length - 1])]);
  if (ms.length >= 3) opts.push(["3 derniers mois", ms[ms.length - 3] + "-01", monthEnd(ms[ms.length - 1])]);
  if (ms.length >= 6) opts.push(["6 derniers mois", ms[ms.length - 6] + "-01", monthEnd(ms[ms.length - 1])]);
  opts.push(["30 derniers jours", addDays(av.to, -29), av.to]);
  // two shortcuts with the same dates: keep the first
  for (let i = opts.length - 1; i > 0; i--) if (opts.slice(0, i).some(o => o[1] === opts[i][1] && o[2] === opts[i][2])) opts.splice(i, 1);
  $("presets").innerHTML = `<span class="lbl" style="align-self:center">Raccourcis</span>` +
    opts.map(([t, a, b]) => `<button type="button" data-a="${a < av.from ? av.from : a}" data-b="${b}" aria-pressed="false">${t}</button>`).join("");
  presetState();
}
// a shortcut is shown as active only while the dates are exactly its dates
function presetState() {
  $("presets").querySelectorAll("button").forEach(b => b.setAttribute("aria-pressed", String(b.dataset.a === $("from").value && b.dataset.b === $("to").value)));
}
["from", "to"].forEach(id => $(id).addEventListener("input", presetState));
$("presets").addEventListener("click", e => {
  const b = e.target.closest("button"); if (!b) return;
  $("from").value = b.dataset.a; $("to").value = b.dataset.b; presetState(); b.blur(); runLine();
});
$("linebtn").addEventListener("click", () => {
  const open = $("panel").hidden;
  $("panel").hidden = !open; $("linebtn").setAttribute("aria-expanded", String(open));
  if (open) { drawPicker($("lsearch").value); setTimeout(() => $("lsearch").focus(), 0); }
});
$("lsearch").addEventListener("input", e => drawPicker(e.target.value));
$("lsearch").addEventListener("keydown", e => {
  if (e.key === "Enter") { e.preventDefault(); const c = $("lgroups").querySelector(".chip"); if (c) c.click(); }
  if (e.key === "Escape") closePanel();
});
$("lgroups").addEventListener("click", e => {
  const c = e.target.closest(".chip"); if (!c) return;
  setLine(c.dataset.line, true); closePanel(); runLine();     // period, days and exclusions are kept
});
function closePanel() { $("panel").hidden = true; $("linebtn").setAttribute("aria-expanded", "false"); }
document.addEventListener("click", e => { if (!e.target.closest(".picker")) closePanel(); });

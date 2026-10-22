// ---------------------------------------------------------------- excluded days: one block, one row per category
// API defaults: public holidays and strikes (reduced service) excluded, school holidays included, no period.
// The same block on the line page and the ranking; "Exclure cette période" (route changes) adds to its periods.
import {$, addDays, andList, fspan, iso} from "./util.js?rev=__REV__";

const XCATS = [["holidays", "Jours fériés", "Jours fériés belges"],
  ["reduced", "Grèves / service réduit", "Jours où la STIB prévoit un service réduit"],
  ["school", "Vacances scolaires", "Jours où la STIB roule en horaire de vacances"],
  ["periods", "Périodes", "Vos propres périodes, ajoutées ci-dessous"]];
const XDEF = {holidays: true, reduced: true, school: false, periods: false};
const XWORD = {holidays: "fériés", reduced: "grèves", school: "vacances scolaires"};
const exclHtml = () => XCATS.map(([k, t, h]) => `<div class="xrow"><label class="check"><input type="checkbox" value="${k}"${XDEF[k] ? " checked" : ""}> ${t}</label><small>${h}</small>${k === "periods"
  ? `<div class="xper"><div class="xchips"></div><div class="dates"><input type="date" aria-label="Exclure du"><em>→</em><input type="date" aria-label="au"><button type="button" class="mini">Ajouter</button></div></div>` : ""}</div>`).join("");
// sorted, overlapping or adjacent periods merged
function normPeriods(P) {
  const out = [];
  for (const [a, b] of P.slice().sort((x, y) => x[0] < y[0] ? -1 : 1)) {
    const last = out[out.length - 1];
    if (last && a <= addDays(last[1], 1)) { if (b > last[1]) last[1] = b; } else out.push([a, b]);
  }
  return out;
}
// "D1..D2,D3" -> [[D1, D2], [D3, D3]]: real dates only (2025-02-30 is dropped), each period in order
function parsePeriods(s) {
  const real = d => /^\d{4}-\d\d-\d\d$/.test(d) && !isNaN(new Date(d + "T12:00:00Z")) && iso(new Date(d + "T12:00:00Z")) === d;
  return (s || "").split(",").map(x => x.split("..")).filter(p => p.length <= 2 && p.every(real))
    .map(([a, b = a]) => [a, b]).filter(([a, b]) => a <= b);
}
function makeExcl(el, sumEl) {
  let P = [];
  el.innerHTML = exclHtml();
  const per = el.querySelector(".xper");
  const chips = per.querySelector(".xchips"), [ia, ib] = per.querySelectorAll("input"), add = per.querySelector("button");
  const box = k => el.querySelector(`input[value=${k}]`);
  const usedP = () => box("periods").checked ? P : [];
  const draw = () => {
    chips.hidden = !P.length;
    chips.innerHTML = P.map(([a, b], i) => `<span class="xchip">${fspan(a, b)}<button type="button" data-i="${i}" aria-label="Ne plus exclure ${fspan(a, b)}">×</button></span>`).join("");
    box("periods").disabled = !P.length;
    if (!P.length) box("periods").checked = false;
    sumEl.textContent = X.text();
  };
  chips.addEventListener("click", e => { const b = e.target.closest("button"); if (!b) return; P.splice(+b.dataset.i, 1); draw(); });
  ia.addEventListener("change", () => { if (ia.value && (!ib.value || ib.value < ia.value)) ib.value = ia.value; });
  add.addEventListener("click", () => { if (!ia.value) return; X.add(ia.value, ib.value || ia.value); ia.value = ib.value = ""; });
  el.addEventListener("change", e => { if (e.target.type === "checkbox") draw(); });
  const X = {
    add(a, b) { if (b < a) [a, b] = [b, a]; P = normPeriods([...P, [a, b]]); box("periods").checked = true; draw(); },
    covers: (a, b) => usedP().some(([x, y]) => x <= a && b <= y),
    on: k => box(k).checked,
    setRange(min, max) { for (const i of [ia, ib]) { i.min = min || ""; i.max = max || ""; } },
    // what is left out, in words ("hors fériés et grèves"): under the form and in the quality block
    text() {
      const w = ["holidays", "reduced", "school"].filter(k => X.on(k)).map(k => XWORD[k]);
      const n = usedP().length;
      if (n) w.push(`${n} période${n > 1 ? "s" : ""}`);
      return w.length ? "hors " + andList(w) : "aucun jour exclu";
    },
    // the API parameters that differ from its defaults
    params(p) {
      if (!X.on("holidays")) p.set("holidays", "include");
      if (!X.on("reduced")) p.set("reduced", "include");
      if (X.on("school")) p.set("school", "exclude");
      if (usedP().length) p.set("exclude", P.map(([a, b]) => a === b ? a : `${a}..${b}`).join(","));
      return p;
    },
    // from URL parameters; true when something differs from the defaults
    load(u) {
      box("holidays").checked = u.get("holidays") !== "include";
      box("reduced").checked = u.get("reduced") !== "include";
      box("school").checked = u.get("school") === "exclude";
      P = normPeriods(parsePeriods(u.get("exclude")));
      box("periods").checked = P.length > 0; draw();
      return XCATS.some(([k]) => X.on(k) !== XDEF[k]);
    },
    // why a day of /api/coverage is left out on purpose (the API's order), or null; frozen at the
    // time of the call (a run keeps the choices it was started with)
    reasonFn() {
      const on = Object.fromEntries(XCATS.map(([k]) => [k, X.on(k)])), per = usedP().map(p => p.slice());
      return d => {
        if (d.holiday && on.holidays) return "holiday";
        if (d.service === "reduced_service" && on.reduced) return "reduced_service";
        if (d.service === "school_holiday" && on.school) return "school_holiday";
        return per.some(([a, b]) => a <= d.date && d.date <= b) ? "user_period" : null;
      };
    },
  };
  draw();
  return X;
}
export const XL = makeExcl($("xbox"), $("xsum")), XR = makeExcl($("rxbox"), $("rxsum"));

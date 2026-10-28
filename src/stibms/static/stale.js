// ---------------------------------------------------------------- settings changed since the last run
// Dates, days and excluded days apply on "Afficher" / "Classer" (a run takes seconds to a minute; one per
// keystroke in a date field would queue several). Until then the form says so next to its button, the
// result shown is dimmed and the sticky bar reads "Réglages modifiés".
import {$} from "./util.js?rev=__REV__";

const LAST = {};      // per form id: its parameters at the last run
const isStale = id => LAST[id] != null && LAST[id] !== LAST[id + ":now"];
function mark(id, view) {
  const on = isStale(id), f = $(id);
  f.classList.toggle("stale", on);
  f.querySelector(".stalemsg").hidden = !on;
  $(view).classList.toggle("stale", on);
  $("selbar").classList.toggle("stale", on);
}
// a run of form ``id`` with parameters p starts: its result is current again
export function ran(id, p) { LAST[id] = LAST[id + ":now"] = String(p); }
// form id: params() its parameters now, view the element of its result
export function watch(id, params, view) {
  const f = $(id), go = f.querySelector(".go"), msg = document.createElement("span");
  msg.className = "stalemsg"; msg.hidden = true; msg.setAttribute("role", "status");
  go.after(msg);
  // after the form's own handlers (an excluded period is added on a click); the hint names the button
  // as it reads now ("Afficher", or "Comparer" on the comparison)
  const check = () => setTimeout(() => {
    LAST[id + ":now"] = String(params());
    msg.textContent = `Réglages modifiés : cliquez sur ${go.textContent} pour mettre à jour`;
    mark(id, view);
  });
  for (const ev of ["input", "change", "click"]) f.addEventListener(ev, check);
  document.addEventListener("ms:sel", check);
}

// /view: the package page with a fetch shim (stibms.api.pages). Runs inside the page's iframe: fetches the
// analysis or comparison with the page's own query string, runs the package's parked script on it and
// tells the platform page what it needs (summary, ready, height, error).
(function () {
  var main = document.getElementById("ms-main");
  var q = new URLSearchParams(location.search);
  var api = (q.get("a") && q.get("b")) ? "/api/compare" : "/api/analysis";
  // "rev" (code version) is not a parameter: it only keeps browsers from reusing a response of an older
  // release; set, not added (the frame's own URL carries it already)
  q.set("rev", "__REV__");
  fetch(api + "?" + q).then(function (r) {
    // error bodies: FastAPI JSON (detail: a string, or a list of {msg}) or a proxy's HTML / text
    if (!r.ok) return r.text().then(function (t) {
      var m = r.status + " " + r.statusText;
      try { var e = JSON.parse(t).detail; m = Array.isArray(e) ? e.map(function (x) { return x.msg || x; }).join(" ; ") : (e || m); } catch (x) {}
      throw new Error(m);
    });
    return r.text();
  }).then(function (txt) {
    // the site's page options win over whatever an older cached response says (HTTP or bucket cache)
    var d = JSON.parse(txt);
    d.show_hotspots = false; d.dir_control = false; d.method = false; d.host_controls = true;
    document.getElementById("ms-data").textContent = JSON.stringify(d);
    var s = document.createElement("script");
    s.textContent = main.textContent;
    document.body.appendChild(s);
    // the platform page shows the numbers of the analysis in its data-quality block (the page's own tiles are hidden)
    try {
      var segs = 0, obs = 0, pr = d.period || {}, c = d.compare;
      (d.dirs || []).forEach(function (x) {
        segs += ((x.seg || {}).key || []).length;
        ((x.wk || {}).obs || []).forEach(function (w) { w.forEach(function (row) { row.forEach(function (v) { obs += v; }); }); });
      });
      var per = function (p) { return {first: p.first, last: p.last, days: p.days, excluded: p.excluded || []}; };
      parent.postMessage({ms: "summary", seg_m: (d.params || {}).segment_m, segs: segs, obs: obs, period: per(pr),
                          compare: c ? {a: per(c.a), b: per(c.b)} : null}, "*");
    } catch (x) {}
    try { parent.postMessage({ms: "ready"}, "*"); } catch (x) {}
    // the platform page sizes this frame to its content (no scroll inside a scroll)
    var last = 0;
    function size() {
      var h = Math.ceil(document.documentElement.scrollHeight);
      if (h !== last) { last = h; try { parent.postMessage({ms: "height", h: h}, "*"); } catch (x) {} }
    }
    size();
    try { new ResizeObserver(size).observe(document.body); } catch (x) {}
    window.addEventListener("resize", size);
    document.addEventListener("fullscreenchange", function () { setTimeout(size, 50); });
    window.addEventListener("load", size);
  }).catch(function (e) {
    document.body.innerHTML = '<p style="color:#c0392b;font:15px system-ui;padding:16px">' + e.message.replace(/</g, "&lt;") + "</p>";
    try { parent.postMessage({ms: "error", message: e.message}, "*"); } catch (x) {}
  });
})();

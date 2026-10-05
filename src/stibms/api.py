"""HTTP API and frontend of the STIB microsegments platform.

    uvicorn stibms.api:app --port 8080          # MS_BUCKET=gs://... or a local directory

Endpoints (dates YYYY-MM-DD, ``dow`` as ``0-4`` or ``0,2,4`` with 0 = Monday):

    GET /api/health
    GET /api/lines?date=                      lines of the GTFS in force on date (default: latest)
    GET /api/coverage?line&from&to            per day feed coverage, line observations, holiday, status
    GET /api/versions?line&from&to            GTFS versions (main pattern runs) per direction
    GET /api/analysis[.csv|.parquet]?line&from&to&dow&holidays=exclude&seg=30&phase=0&grid=equal
                                       &stopzone=30,60&ref=20-23
                                              JSON contract (microsegments.contract) / result table
    GET /api/hotspots[.csv|.parquet]?...      hotspot stretches (same parameters)
    GET /api/tune?...&lengths=15,20,30,40,50  segment-length criteria (slow, cached)
    GET /api/compare?line&a=D1..D2&b=D3..D4&dow&holidays&seg&...
                                              period B's contract + the ``compare`` section
    GET /api/ranking?from&to&dow&holidays&mode=all|tram|bus|metro&top=50
                                              costliest stretches of the network (vehicle-hours lost
                                              per day); default window when from/to are omitted.
                                              Custom windows are computed over several polls:
                                              {"status": "running", "done", "total"} until done.
    GET /api/status                           ingested range, derived months, months pending
    GET /, /comparer, /classement             the page (one line / two periods / network ranking)
    GET /view?<analysis or compare params>    the package page fed by /api/analysis or /api/compare

Results are cached in process (LRU) and in the bucket (``results/v{ALGO}/<hash>.json.gz``), keyed
by the parameters, ALGO_VERSION, the package version and the data stamp (last ingested date and
derive time of the months involved), so a nightly ingest invalidates only what it touches.
Periods that are fully past and ingested are sent with ``Cache-Control: max-age=86400``.
Heavy imports (polars, microsegments) happen on the first data request, not at start-up.
"""
from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import logging
import os
import threading
import time
from collections import OrderedDict
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query as Q, Request
from fastapi.responses import FileResponse, JSONResponse, Response

log = logging.getLogger("stibms.api")
STATIC = Path(__file__).parent / "static"
LONG_CACHE = "public, max-age=86400"
SHORT_CACHE = "public, max-age=300"

app = FastAPI(title="STIB microsegments", docs_url="/api/docs", openapi_url="/api/openapi.json")


# ---------------------------------------------------------------------------------- state
class _LRU:
    def __init__(self, size: int):
        self.size, self.d, self.lock = size, OrderedDict(), threading.Lock()

    def get(self, k):
        with self.lock:
            if k in self.d:
                self.d.move_to_end(k)
                return self.d[k]
        return None

    def put(self, k, v):
        with self.lock:
            self.d[k] = v
            self.d.move_to_end(k)
            while len(self.d) > self.size:
                self.d.popitem(last=False)

    def clear(self):
        with self.lock:
            self.d.clear()


class State:
    def __init__(self):
        self._data = None
        self.runs = _LRU(int(os.environ.get("MS_RUN_CACHE", "6")))      # Run objects (arrays, heavy)
        self.blobs = _LRU(int(os.environ.get("MS_BLOB_CACHE", "128")))  # gzipped JSON
        self._locks: dict[str, threading.Lock] = {}
        self._lk = threading.Lock()

    @property
    def data(self):
        if self._data is None:
            from .data import Data
            root = os.environ.get("MS_BUCKET")
            if not root:
                raise HTTPException(503, "MS_BUCKET is not set")
            self._data = Data(root)
        return self._data

    def lock(self, key: str) -> threading.Lock:
        with self._lk:
            return self._locks.setdefault(key, threading.Lock())

    def reset(self):
        self._data = None
        self.runs.clear()
        self.blobs.clear()


state = State()


# ---------------------------------------------------------------------------------- params
def _date(s: str | None, name: str) -> dt.date | None:
    if s is None or s == "":
        return None
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        raise HTTPException(422, f"{name}: expected YYYY-MM-DD")


def _ints(s: str, name: str) -> list[int]:
    out: list[int] = []
    try:
        for part in s.replace(" ", "").split(","):
            if not part:
                continue
            if "-" in part:
                a, b = part.split("-")
                out += list(range(int(a), int(b) + 1))
            else:
                out.append(int(part))
    except ValueError:
        raise HTTPException(422, f"{name}: expected numbers like 0-4 or 0,2,4")
    return out


def _pair(s: str, name: str, cast=float, sep=None) -> tuple:
    seps = [sep] if sep else [",", "-", ":"]
    for c in seps:
        if c in s:
            a, b = s.split(c, 1)
            try:
                return cast(a), cast(b)
            except ValueError:
                break
    raise HTTPException(422, f"{name}: expected two numbers, e.g. 30,60")


def period(line: str = Q(..., description="line number (GTFS route_short_name)"),
           from_: str = Q(..., alias="from"), to: str = Q(...)):
    a, b = _date(from_, "from"), _date(to, "to")
    if a > b:
        raise HTTPException(422, "from must be <= to")
    if (b - a).days > 400:
        raise HTTPException(422, "period longer than 400 days")
    return line.strip(), a, b


def query(line: str = Q(...), from_: str = Q(..., alias="from"), to: str = Q(...),
          dow: str = Q("0-4"), holidays: str = Q("exclude"), seg: float = Q(30.0),
          phase: float = Q(0.0), grid: str = Q("equal"), stopzone: str = Q("30,60"), ref: str = Q("20-23")):
    from .analysis import Query
    ln, a, b = period(line, from_, to)
    try:
        return Query(line=ln, first=a, last=b, dow=tuple(sorted(set(_ints(dow, "dow")))), holidays=holidays,
                     seg=float(seg), phase=float(phase), grid=grid,
                     stopzone=tuple(float(x) for x in _pair(stopzone, "stopzone", sep=",")),
                     ref=tuple(int(x) for x in _pair(ref, "ref", int)))
    except ValueError as e:
        raise HTTPException(422, str(e))


# ---------------------------------------------------------------------------------- caching
def _stamp(data, a: dt.date, b: dt.date, a2: dt.date | None = None, b2: dt.date | None = None) -> str:
    """Changes whenever data of [a, b] (and [a2, b2]) changes: last ingested date + derive times."""
    from .data import date_range
    dates = date_range(a, b) + (date_range(a2, b2) if a2 is not None else [])
    return data._cached(f"stamp:{a}:{b}:{a2}:{b2}", lambda: data.stamp(dates), ttl=120)


def _cache_control(data, b: dt.date) -> str:
    from .servicedays import yesterday
    ing = data.ingested_dates()
    return LONG_CACHE if ing and b < yesterday() and b <= ing[-1] else SHORT_CACHE


def _gz_response(request: Request, gz: bytes, cc: str, media="application/json", extra: dict | None = None) -> Response:
    headers = {"Cache-Control": cc, "Vary": "Accept-Encoding", **(extra or {})}
    if "gzip" in request.headers.get("accept-encoding", ""):
        headers["Content-Encoding"] = "gzip"
        return Response(gz, media_type=media, headers=headers)
    return Response(gzip.decompress(gz), media_type=media, headers=headers)


def _cached_json(kind: str, q, compute, request: Request) -> Response:
    """kind: result family; compute() -> JSON-able. In-process LRU, then bucket, then compute."""
    data = state.data
    key = f"{kind}-{q.key(_stamp_of(data, q))}"
    cc = _cache_control(data, q.last)
    gz = state.blobs.get(key)
    src = "memory"
    if gz is None:
        with state.lock(key):
            gz = state.blobs.get(key)
            if gz is None:
                from .data import ALGO_VERSION
                rel = f"results/v{ALGO_VERSION}/{key}.json.gz"
                src = "bucket"
                try:
                    gz = data.store.read_bytes(rel) if data.store.exists(rel) else None
                except Exception:  # noqa: BLE001 - a cache miss, not an error
                    gz = None
                if gz is None:
                    src = "computed"
                    t = time.time()
                    obj = compute()
                    gz = gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode(), 6)
                    log.info("%s computed in %.2f s (%d kB gz)", key, time.time() - t, len(gz) // 1024)
                    try:
                        data.store.write_bytes(rel, gz)
                    except Exception:  # noqa: BLE001
                        log.exception("result cache write failed")
                state.blobs.put(key, gz)
    return _gz_response(request, gz, cc, extra={"X-Cache": src})


def _stamp_of(data, q) -> str:
    if hasattr(q, "a") and hasattr(q, "b"):
        return _stamp(data, q.a[0], q.a[1], q.b[0], q.b[1])
    return _stamp(data, q.first, q.last)


def _run(q):
    from . import analysis as A
    data = state.data
    key = q.key(_stamp(data, q.first, q.last))
    r = state.runs.get(key)
    if r is None:
        with state.lock("run-" + key):
            r = state.runs.get(key)
            if r is None:
                try:
                    r = A.run(data, q)
                except A.NotFound as e:
                    raise HTTPException(404, str(e))
                state.runs.put(key, r)
    return r


def _table_response(df, fmt: str, name: str, cc: str) -> Response:
    if fmt == "csv":
        buf = io.BytesIO()
        flat = df.select([c for c in df.columns if not str(df.schema[c]).startswith(("List", "Struct"))])
        flat.write_csv(buf)
        return Response(buf.getvalue(), media_type="text/csv; charset=utf-8",
                        headers={"Cache-Control": cc, "Content-Disposition": f'attachment; filename="{name}.csv"'})
    buf = io.BytesIO()
    df.write_parquet(buf, compression="zstd")
    return Response(buf.getvalue(), media_type="application/vnd.apache.parquet",
                    headers={"Cache-Control": cc, "Content-Disposition": f'attachment; filename="{name}.parquet"'})


def _fname(q, what: str) -> str:
    return f"stib_{q.line}_{q.first}_{q.last}_{what}_{q.seg:g}m"


# ---------------------------------------------------------------------------------- routes
@app.get("/api/health")
def health():
    from . import __version__
    return {"ok": True, "version": __version__, "bucket": os.environ.get("MS_BUCKET", "")[:5] + "…"}


def _default_period(months: list[str], ing: list[dt.date], n: int = 3) -> dict | None:
    """Last ``n`` complete months (ingested up to their last day) among ``months``; else the last
    ``n`` months, cut at the last ingested date."""
    if not months or not ing:
        return None
    full = [m for m in months if _month_last(m) <= ing[-1]]
    pick = (full or months)[-n:]
    a = max(dt.date.fromisoformat(pick[0] + "-01"), ing[0])
    b = min(_month_last(pick[-1]), ing[-1])
    return {"from": a.isoformat(), "to": b.isoformat()}


@app.get("/api/lines")
def lines(date: str | None = None):
    import polars as pl
    from microsegments.report import ROUTE_TYPES
    data = state.data

    def f():
        idx = data.gtfs_index()
        if not idx.height:
            return {"date": None, "lines": []}
        d = _date(date, "date")
        x = idx if d is None else idx.filter(pl.col("service_date") <= d)
        if not x.height:
            x = idx.head(1)
        row = x.tail(1).row(0, named=True)
        r = data.routes(row["sha"])
        months = data.line_months()
        done = data.derived_months()      # a month being derived has some line files already
        ing = data.ingested_dates()
        out = []
        for rr in r.iter_rows(named=True):
            ln = str(rr["route_short_name"])
            ms_ = [m for m in months.get(ln, []) if m in done]
            avail = None
            if ms_:
                a = next((x for x in ing if x.isoformat()[:7] >= ms_[0]), None)
                b = next((x for x in reversed(ing) if x.isoformat()[:7] <= ms_[-1]), None)
                avail = {"from": a.isoformat() if a else None, "to": b.isoformat() if b else None, "months": ms_,
                         "default": _default_period(ms_, ing)}
            rt = rr.get("route_type")
            out.append({"line": ln, "name": rr.get("route_long_name"), "route_type": rt,
                        "mode": ROUTE_TYPES.get(int(rt)) if rt is not None else None,
                        "color": rr.get("route_color"), "text_color": rr.get("route_text_color"),
                        "available": avail})
        out.sort(key=lambda o: (o["mode"] not in ("metro",), o["mode"] != "tram",
                                (len(o["line"]), o["line"]) if o["line"].isdigit() else (99, o["line"])))
        return {"date": row["service_date"].isoformat(), "gtfs": row["sha"][:12],
                "ingested": {"from": ing[0].isoformat() if ing else None, "to": ing[-1].isoformat() if ing else None},
                "months": _months_info(data), "lines": out}
    res = data._cached(f"lines:{date}", f, ttl=300)
    return JSONResponse(res, headers={"Cache-Control": SHORT_CACHE})


@app.get("/api/coverage")
def coverage(p=Depends(period)):
    from . import analysis as A
    line, a, b = p
    data = state.data
    days = A.coverage_days(data, line, a, b)
    return JSONResponse({"line": line, "from": a.isoformat(), "to": b.isoformat(), "days": days},
                        headers={"Cache-Control": _cache_control(data, b)})


@app.get("/api/versions")
def versions(p=Depends(period)):
    from . import analysis as A
    line, a, b = p
    data = state.data
    v = A.versions(data, line, a, b)
    return JSONResponse({"line": line, "from": a.isoformat(), "to": b.isoformat(), "versions": v},
                        headers={"Cache-Control": _cache_control(data, b)})


@app.get("/api/analysis")
def analysis(request: Request, q=Depends(query)):
    from . import analysis as A

    def compute():
        r = _run(q)
        c = A.contract(r)
        c["timings"] = r.timings
        return c
    return _cached_json("analysis", q, compute, request)


@app.get("/api/analysis.{fmt}")
def analysis_table(fmt: str, q=Depends(query)):
    from . import analysis as A
    if fmt not in ("csv", "parquet"):
        raise HTTPException(404)
    r = _run(q)
    return _table_response(A.result_table(r), fmt, _fname(q, "segments"), _cache_control(state.data, q.last))


@app.get("/api/hotspots")
def hotspots(request: Request, q=Depends(query)):
    def compute():
        from microsegments.report import frame_records
        r = _run(q)
        hs = r.hotspots()
        return {"line": q.line, "query": q.canonical(), "hotspots": frame_records(hs)}
    return _cached_json("hotspots", q, compute, request)


@app.get("/api/hotspots.{fmt}")
def hotspots_table(fmt: str, q=Depends(query)):
    if fmt not in ("csv", "parquet"):
        raise HTTPException(404)
    r = _run(q)
    return _table_response(r.hotspots(), fmt, _fname(q, "hotspots"), _cache_control(state.data, q.last))


@app.get("/api/tune")
def tune(request: Request, q=Depends(query), lengths: str = Q("15,20,30,40,50,75"), B: int = Q(50)):
    Ls = tuple(sorted({float(x) for x in lengths.split(",") if x.strip()}))
    if not Ls or len(Ls) > 10 or any(L < 5 or L > 500 for L in Ls):
        raise HTTPException(422, "lengths: 1 to 10 values within 5..500")
    B = max(10, min(int(B), 200))

    class _K:  # cache key carries the tune parameters too
        first, last = q.first, q.last

        @staticmethod
        def key(stamp):
            return q.key(stamp, "tune", Ls, B)

    def compute():
        from microsegments.config import Params, Quality, Select
        from microsegments.report import frame_records
        from microsegments.segments import segment
        from microsegments.tune import segment_length
        from . import analysis as A
        r = _run(q)
        t = time.time()
        net, uids = r.net, sorted(r.segments["pattern_uid"].unique().to_list())

        def seg_fn(L, phase=0.0, stop_zone=q.stopzone):
            return segment(net, L, phase, q.grid, stop_zone, patterns=uids, geometry=False)
        an = r.analysis
        res = segment_length(r.placed, seg_fn, Ls, coverage=r.coverage if r.coverage.height else None,
                             passages=r.passages, pattern_days=net.pattern_days, select=an.select,
                             params=an.params, quality=an.quality, B=B)
        return {"line": q.line, "query": q.canonical(), "lengths": list(Ls), "B": B,
                "recommended": res.recommended, "rule": res.rule, "table": frame_records(res.table),
                "seconds": round(time.time() - t, 1)}
    return _cached_json("tune", _K, compute, request)


def _range(s: str, name: str) -> tuple[dt.date, dt.date]:
    try:
        a, b = s.split("..")
        a, b = dt.date.fromisoformat(a.strip()), dt.date.fromisoformat(b.strip())
    except ValueError:
        raise HTTPException(422, f"{name}: expected YYYY-MM-DD..YYYY-MM-DD")
    return a, b


def compare_query(line: str = Q(...), a: str = Q(..., description="period A (before), YYYY-MM-DD..YYYY-MM-DD"),
                  b: str = Q(..., description="period B (after)"), dow: str = Q("0-4"), holidays: str = Q("exclude"),
                  seg: float = Q(30.0), phase: float = Q(0.0), grid: str = Q("equal"), stopzone: str = Q("30,60"),
                  ref: str = Q("20-23")):
    from .analysis import CompareQuery
    try:
        return CompareQuery(line=line.strip(), a=_range(a, "a"), b=_range(b, "b"),
                            dow=tuple(sorted(set(_ints(dow, "dow")))), holidays=holidays, seg=float(seg),
                            phase=float(phase), grid=grid,
                            stopzone=tuple(float(x) for x in _pair(stopzone, "stopzone", sep=",")),
                            ref=tuple(int(x) for x in _pair(ref, "ref", int)))
    except ValueError as e:
        raise HTTPException(422, str(e))


@app.get("/api/compare")
def compare(request: Request, cq=Depends(compare_query)):
    from . import analysis as A

    def compute():
        try:
            return A.run_compare(state.data, cq)
        except A.NotFound as e:
            raise HTTPException(404, str(e))
    return _cached_json("compare", cq, compute, request)


def _months_info(data) -> dict:
    ing = data.ingested_dates()
    have = data.derived_months()
    from .data import month_key
    ing_months = sorted({month_key(d) for d in ing})
    complete = [m for m in have if ing and dt.date.fromisoformat(m + "-01") <= ing[-1]
                and _month_last(m) <= ing[-1]]
    return {"ingested": {"from": ing[0].isoformat() if ing else None, "to": ing[-1].isoformat() if ing else None,
                         "days": len(ing)},
            "derived_months": list(have), "pending_months": [m for m in ing_months if m not in have],
            "complete_months": complete}


def _month_last(m: str) -> dt.date:
    from .data import month_dates
    return month_dates(m)[-1]


@app.get("/api/status")
def status():
    data = state.data
    res = data._cached("status", lambda: _months_info(data), ttl=120)
    try:
        res = {**res, "ranking_default": data.store.read_json(f"results/v{_algo()}/ranking-default.json")}
    except FileNotFoundError:
        res = {**res, "ranking_default": None}
    return JSONResponse(res, headers={"Cache-Control": "public, max-age=60"})


def _algo() -> int:
    from .data import ALGO_VERSION
    return ALGO_VERSION


_ranking_lock = threading.Lock()


@app.get("/api/ranking")
def ranking(request: Request, from_: str | None = Q(None, alias="from"), to: str | None = None,
            dow: str = Q("0-4"), holidays: str = Q("exclude"), mode: str = Q("all"), top: int = Q(50),
            terminus: bool = Q(False)):
    from . import ranking as R
    data = state.data
    top = max(5, min(int(top), 200))
    a, b = _date(from_, "from"), _date(to, "to")
    if a is None or b is None:
        w = R.default_window(data)
        if w is None:
            raise HTTPException(404, "aucun mois complet calculé pour l'instant : données en cours de préparation")
        a, b = a or w[0], b or w[1]
    try:
        rq = R.RankQuery(a, b, tuple(sorted(set(_ints(dow, "dow")))), holidays, mode, terminus)
    except ValueError as e:
        raise HTTPException(422, str(e))
    key = "ranking-" + rq.key(_stamp(data, a, b)) + f"-{mode}-{int(terminus)}-{top}"
    gz = state.blobs.get(key)
    if gz is None:
        # One computing request per instance; others report progress from the bucket. Each call
        # computes for at most ~25 s (Cloud Run gives CPU only during requests), the page re-polls.
        if not _ranking_lock.acquire(timeout=1):
            return JSONResponse({"status": "running", "busy": True, "query": rq.canonical()}, status_code=202,
                                headers={"Cache-Control": "no-store"})
        try:
            res = R.compute(data, rq, top=top, budget_s=float(os.environ.get("MS_RANK_BUDGET", "25")))
        finally:
            _ranking_lock.release()
        if res.get("status") != "done":
            return JSONResponse(res, status_code=202, headers={"Cache-Control": "no-store"})
        gz = gzip.compress(json.dumps(res, separators=(",", ":"), default=str).encode(), 6)
        state.blobs.put(key, gz)
    return _gz_response(request, gz, _cache_control(data, b))


@app.get("/", include_in_schema=False)
@app.get("/comparer", include_in_schema=False)
@app.get("/classement", include_in_schema=False)
def index():
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": SHORT_CACHE})


_LOADER = """<script>
(function () {
  var main = document.getElementById("ms-main");
  var q = new URLSearchParams(location.search);
  var api = (q.get("a") && q.get("b")) ? "/api/compare" : "/api/analysis";
  fetch(api + location.search).then(function (r) {
    if (!r.ok) return r.json().then(function (e) { throw new Error(e.detail || r.statusText); });
    return r.text();
  }).then(function (txt) {
    document.getElementById("ms-data").textContent = txt;
    var s = document.createElement("script");
    s.textContent = main.textContent;
    document.body.appendChild(s);
    try { parent.postMessage({ms: "ready"}, "*"); } catch (x) {}
  }).catch(function (e) {
    document.body.innerHTML = '<p style="color:#c0392b;font:15px system-ui;padding:16px">' + e.message.replace(/</g, "&lt;") + "</p>";
    try { parent.postMessage({ms: "error", message: e.message}, "*"); } catch (x) {}
  });
})();
</script>"""


def _template() -> str | None:
    """The package's page (``microsegments.html.export.render``) with a fetch shim instead of embedded
    data: the page is rendered with an empty contract, its main script is parked as inert text, and
    a loader fetches ``/api/analysis`` with the page's own query string, fills the ``ms-data`` JSON
    element and runs the parked script. The page is otherwise unchanged."""
    import re
    try:
        from microsegments.html.export import render
        html = render({"tiles": True}, title="STIB · micro-segments", lang="fr",
                      description="Observations des véhicules STIB par micro-segment.")
    except Exception:  # noqa: BLE001 - template not shipped / incompatible: the page falls back
        log.exception("page template unavailable")
        return None
    m = re.search(r'<script id="ms-data" type="application/json">.*?</script>\s*<script>(.*?)</script>', html, flags=re.S)
    if not m:
        return None
    a, b = m.span(1)
    html = (html[:m.start()] + '<script id="ms-data" type="application/json"></script>\n'
            + '<script type="text/plain" id="ms-main">' + html[a:b] + "</script>" + html[m.end():])
    i = html.rfind("</body>")
    return html[:i] + _LOADER + html[i:] if i >= 0 else html + _LOADER


@app.api_route("/view", methods=["GET", "HEAD"], include_in_schema=False)
def view():
    html = state.blobs.get("template")
    if html is None:
        html = _template()
        if html is None:
            raise HTTPException(404, "the microsegments page template is not available")
        state.blobs.put("template", html)
    return Response(html, media_type="text/html; charset=utf-8", headers={"Cache-Control": SHORT_CACHE})


@app.get("/static/{name}", include_in_schema=False)
def static(name: str):
    p = (STATIC / name).resolve()
    if p.parent != STATIC.resolve() or not p.is_file():
        raise HTTPException(404)
    return FileResponse(p, headers={"Cache-Control": SHORT_CACHE})


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception):
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse({"detail": f"{type(exc).__name__}: {exc}"[:500]}, status_code=500)

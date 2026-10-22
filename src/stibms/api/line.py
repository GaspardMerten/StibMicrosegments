"""One line: coverage, route versions, the analysis (JSON, tables), hotspots, tuning, two periods."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query as Q, Request
from fastapi.responses import JSONResponse

from .cache import cache_control, cached_json, cached_run, table_response
from .context import state
from .params import compare_query, period, query

router = APIRouter()
TABLES = ("csv", "parquet")


def _period_json(p, key: str, rows) -> JSONResponse:
    line, a, b = p
    return JSONResponse({"line": line, "from": a.isoformat(), "to": b.isoformat(), key: rows},
                        headers={"Cache-Control": cache_control(state.data, b)})


def _fname(q, what: str) -> str:
    return f"stib_{q.line}_{q.first}_{q.last}_{what}_{q.seg:g}m"


def _table(fmt: str, q, what: str, frame):
    if fmt not in TABLES:
        raise HTTPException(404)
    return table_response(frame(cached_run(q)), fmt, _fname(q, what), cache_control(state.data, q.last, q))


@router.get("/api/coverage")
def coverage(p=Depends(period)):
    from ..coverage import coverage_days
    return _period_json(p, "days", coverage_days(state.data, *p))


@router.get("/api/versions")
def versions(p=Depends(period)):
    from ..versions import versions as route_versions
    return _period_json(p, "versions", route_versions(state.data, *p))


@router.get("/api/analysis")
def analysis(request: Request, q=Depends(query)):
    from .. import analysis as A

    def compute():
        r = cached_run(q)
        c = A.contract(r)
        c["timings"] = r.timings
        return c
    return cached_json("analysis", q, compute, request)


@router.get("/api/analysis.{fmt}")
def analysis_table(fmt: str, q=Depends(query)):
    from ..analysis import result_table
    return _table(fmt, q, "segments", result_table)


@router.get("/api/hotspots")
def hotspots(request: Request, q=Depends(query)):
    def compute():
        from microsegments.report import frame_records
        return {"line": q.line, "query": q.canonical(), "hotspots": frame_records(cached_run(q).hotspots())}
    return cached_json("hotspots", q, compute, request)


@router.get("/api/hotspots.{fmt}")
def hotspots_table(fmt: str, q=Depends(query)):
    return _table(fmt, q, "hotspots", lambda r: r.hotspots())


@router.get("/api/tune")
def tune(request: Request, q=Depends(query), lengths: str = Q("15,20,30,40,50,75"), B: int = Q(50)):
    from ..analysis import tune as tune_lengths
    try:
        Ls = tuple(sorted({float(x) for x in lengths.split(",") if x.strip()}))
    except ValueError:
        Ls = ()
    if not Ls or len(Ls) > 10 or any(L < 5 or L > 500 for L in Ls):
        raise HTTPException(422, "lengths: 1 to 10 values within 5..500")
    B = max(10, min(int(B), 200))
    # the cache key carries the tune parameters too
    return cached_json("tune", q, lambda: tune_lengths(cached_run(q), Ls, B), request, "tune", Ls, B)


@router.get("/api/compare")
def compare(request: Request, cq=Depends(compare_query)):
    from .. import analysis as A

    def compute():
        try:
            return A.run_compare(state.data, cq)
        except A.NotFound as e:
            raise HTTPException(404, str(e))
    return cached_json("compare", cq, compute, request)

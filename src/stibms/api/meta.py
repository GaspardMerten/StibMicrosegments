"""/api/health, /api/lines, /api/status: what the pages need before any analysis."""
from __future__ import annotations

import datetime as dt
import os

from fastapi import APIRouter, Request

from .cache import SHORT_CACHE, json_response
from .context import state
from .params import parse_date

router = APIRouter()


def _month_last(m: str) -> dt.date:
    from ..data import month_dates
    return month_dates(m)[-1]


def _span(dates: list[dt.date]) -> dict:
    return {"from": dates[0].isoformat() if dates else None, "to": dates[-1].isoformat() if dates else None}


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


def _months_info(data) -> dict:
    from ..data import month_key
    ing = data.ingested_dates()
    have = data.derived_months()
    ing_months = sorted({month_key(d) for d in ing})
    complete = [m for m in have if ing and _month_last(m) <= ing[-1]]
    return {"ingested": {**_span(ing), "days": len(ing)},
            "derived_months": list(have), "pending_months": [m for m in ing_months if m not in have],
            "complete_months": complete}


def _available(line_months: list[str], ing: list[dt.date]) -> dict | None:
    """The period a line has derived data for, its months and its default period."""
    if not line_months:
        return None
    a = next((x for x in ing if x.isoformat()[:7] >= line_months[0]), None)
    b = next((x for x in reversed(ing) if x.isoformat()[:7] <= line_months[-1]), None)
    return {"from": a.isoformat() if a else None, "to": b.isoformat() if b else None, "months": line_months,
            "default": _default_period(line_months, ing)}


def _line_order(o: dict) -> tuple:
    """Metro, tram, then bus; numbers in numeric order, then the others."""
    ln = o["line"]
    return o["mode"] not in ("metro",), o["mode"] != "tram", (len(ln), ln) if ln.isdigit() else (99, ln)


def _lines(data, date: str | None) -> dict:
    import polars as pl
    from microsegments.report import ROUTE_TYPES
    idx = data.gtfs_index()
    if not idx.height:
        return {"date": None, "lines": []}
    d = parse_date(date, "date")
    x = idx if d is None else idx.filter(pl.col("service_date") <= d)
    row = (x if x.height else idx.head(1)).tail(1).row(0, named=True)
    months, ing = data.line_months(), data.ingested_dates()
    done = data.derived_months()      # a month being derived has some line files already
    out = []
    for rr in data.routes(row["sha"]).iter_rows(named=True):
        ln, rt = str(rr["route_short_name"]), rr.get("route_type")
        out.append({"line": ln, "name": rr.get("route_long_name"), "route_type": rt,
                    "mode": ROUTE_TYPES.get(int(rt)) if rt is not None else None,
                    "color": rr.get("route_color"), "text_color": rr.get("route_text_color"),
                    "available": _available([m for m in months.get(ln, []) if m in done], ing)})
    out.sort(key=_line_order)
    return {"date": row["service_date"].isoformat(), "gtfs": row["sha"][:12], "ingested": _span(ing),
            "months": _months_info(data), "lines": out}


@router.get("/api/health")
def health():
    from .. import __version__
    return {"ok": True, "version": __version__, "bucket": os.environ.get("MS_BUCKET", "")[:5] + "…"}


@router.get("/api/lines")
def lines(request: Request, date: str | None = None):
    data = state.data
    res = data._cached(f"lines:{date}", lambda: _lines(data, date), ttl=300)
    return json_response(request, res, SHORT_CACHE)


@router.get("/api/status")
def status(request: Request):
    from ..ranking import DEFAULT_PTR
    data = state.data
    res = data._cached("status", lambda: _months_info(data), ttl=120)
    try:
        res = {**res, "ranking_default": data.store.read_json(DEFAULT_PTR)}
    except FileNotFoundError:
        res = {**res, "ranking_default": None}
    return json_response(request, res, "public, max-age=60")

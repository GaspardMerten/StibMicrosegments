"""/api/network and /api/network/map: the network page's indicators (``stibms.network``)."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query as Q, Request

from .cache import Result, respond
from .context import state
from .params import build, dows, hour_span, line_list, parse_date, periods

router = APIRouter()


def _kw(from_, to, dow, hours, holidays, mode, lines, school, reduced, exclude) -> dict:
    from .. import network as N
    a, b = parse_date(from_, "from"), parse_date(to, "to")
    if a is None or b is None:
        w = N.default_window(state.data)
        if w is None:
            raise HTTPException(404, "aucun mois complet calculé pour l'instant : données en cours de préparation")
        a, b = a or w[0], b or w[1]
    return dict(first=a, last=b, dow=dows(dow), hours=hour_span(hours), holidays=holidays, mode=mode, lines=line_list(lines),
                school=school, reduced=reduced, exclude=periods(exclude))


def net_query(from_: str | None = Q(None, alias="from"), to: str | None = None, dow: str = Q("0-4"),
              hours: str = Q("6-21", description="hours a-b, b excluded; 24 and more: after midnight"),
              holidays: str = Q("exclude"), mode: str = Q("all"), lines: str = Q("", description="line numbers, 55,81"),
              school: str = Q("include"), reduced: str = Q("exclude"), exclude: str = Q("")):
    from .. import network as N
    return build(N.NetQuery, **_kw(from_, to, dow, hours, holidays, mode, lines, school, reduced, exclude))


def map_query(from_: str | None = Q(None, alias="from"), to: str | None = None, dow: str = Q("0-4"),
              hours: str = Q("6-21"), holidays: str = Q("exclude"), mode: str = Q("all"), lines: str = Q(""),
              school: str = Q("include"), reduced: str = Q("exclude"), exclude: str = Q("")):
    from .. import network as N
    return build(N.MapQuery, **_kw(from_, to, dow, hours, holidays, mode, lines, school, reduced, exclude))


def net_result(q) -> Result:
    from .. import network as N
    return Result("network", q, lambda: N.compute(state.data, q), heavy=False, keep=lambda r: bool(r.get("months")))


def map_result(q) -> Result:
    from .. import network as N
    return Result("netmap", q, lambda: N.compute_map(state.data, q), heavy=False, keep=lambda r: bool(r.get("links")))


@router.get("/api/network")
def network(request: Request, q=Depends(net_query)):
    return respond(request, net_result(q))


@router.get("/api/network/map")
def network_map(request: Request, q=Depends(map_query)):
    return respond(request, map_result(q))

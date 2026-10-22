"""/api/ranking: the costliest stretches of the network, summed from the monthly link cubes."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query as Q, Request

from .cache import Result, respond
from .context import state
from .params import build, dows, parse_date, periods

router = APIRouter()


def rank_query(from_: str | None = Q(None, alias="from"), to: str | None = None,
               dow: str = Q("0-4"), holidays: str = Q("exclude"), mode: str = Q("all"), top: int = Q(50),
               terminus: bool = Q(False), stops: bool = Q(False), school: str = Q("include"),
               reduced: str = Q("exclude"), exclude: str = Q("")):
    """(RankQuery, top); the default window when from / to are omitted."""
    from .. import ranking as R
    top = max(5, min(int(top), 200))
    a, b = parse_date(from_, "from"), parse_date(to, "to")
    if a is None or b is None:
        w = R.default_window(state.data)
        if w is None:
            raise HTTPException(404, "aucun mois complet calculé pour l'instant : données en cours de préparation")
        a, b = a or w[0], b or w[1]
    rq = build(R.RankQuery, first=a, last=b, dow=dows(dow), holidays=holidays, mode=mode, terminus=terminus,
               stops=stops, school=school, reduced=reduced, exclude=periods(exclude))
    return rq, top


def rank_result(rq, top: int) -> Result:
    """Cheap (no HEAVY); an empty window may just not have its cubes yet: not cached."""
    from .. import ranking as R
    return Result("ranking", rq, lambda: R.compute(state.data, rq, top=top), (top,), heavy=False,
                  keep=lambda res: bool(res.get("stretches")))


@router.get("/api/ranking")
def ranking(request: Request, rt=Depends(rank_query)):
    return respond(request, rank_result(*rt))

"""/api/ranking: the costliest stretches of the network, summed from the monthly link cubes."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query as Q, Request

from .cache import cache_control, data_stamp, gz_response, gzip_json
from .context import state
from .params import build, dows, parse_date, periods

router = APIRouter()


@router.get("/api/ranking")
def ranking(request: Request, from_: str | None = Q(None, alias="from"), to: str | None = None,
            dow: str = Q("0-4"), holidays: str = Q("exclude"), mode: str = Q("all"), top: int = Q(50),
            terminus: bool = Q(False), stops: bool = Q(False), school: str = Q("include"),
            reduced: str = Q("exclude"), exclude: str = Q("")):
    from .. import ranking as R
    data = state.data
    top = max(5, min(int(top), 200))
    a, b = parse_date(from_, "from"), parse_date(to, "to")
    if a is None or b is None:
        w = R.default_window(data)
        if w is None:
            raise HTTPException(404, "aucun mois complet calculé pour l'instant : données en cours de préparation")
        a, b = a or w[0], b or w[1]
    rq = build(R.RankQuery, first=a, last=b, dow=dows(dow), holidays=holidays, mode=mode, terminus=terminus,
               stops=stops, school=school, reduced=reduced, exclude=periods(exclude))
    key = "ranking-" + rq.key(data_stamp(data, rq), top)

    def compute():
        res = R.compute(data, rq, top=top)
        return gzip_json(res), bool(res.get("stretches"))
    # an empty window may just not have its cubes yet: not remembered
    gz, _ = state.once(state.blobs, key, compute, keep=lambda v: v[1])
    return gz_response(request, gz, cache_control(data, b, rq))

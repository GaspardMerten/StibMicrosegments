"""Query-string parsing: dates, day lists, periods, and the query dependencies of the endpoints."""
from __future__ import annotations

import datetime as dt

from fastapi import HTTPException, Query as Q


def parse_date(s: str | None, name: str) -> dt.date | None:
    if s is None or s == "":
        return None
    try:
        return dt.date.fromisoformat(s)
    except ValueError:
        raise HTTPException(422, f"{name} : date invalide (AAAA-MM-JJ).")


def ints(s: str, name: str) -> list[int]:
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


def dows(s: str) -> tuple[int, ...]:
    return tuple(sorted(set(ints(s, "dow"))))


def pair(s: str, name: str, cast=float, sep=None) -> tuple:
    for c in [sep] if sep else [",", "-", ":"]:
        if c in s:
            a, b = s.split(c, 1)
            try:
                return cast(a), cast(b)
            except ValueError:
                break
    raise HTTPException(422, f"{name}: expected two numbers, e.g. 30,60")


def periods(s: str | None, name: str = "exclude") -> tuple:
    """``2025-12-20..2026-01-04,2026-02-16`` -> ((first, last), ...); a single date is a one-day period."""
    out = []
    for part in (s or "").replace(" ", "").split(","):
        if not part:
            continue
        a, dots, b = part.partition("..")
        if dots and not (a and b):
            raise HTTPException(422, f"{name} : période incomplète (AAAA-MM-JJ..AAAA-MM-JJ).")
        x, y = parse_date(a, name), parse_date(b or a, name)
        if x is None or y is None:
            raise HTTPException(422, f"{name} : période invalide (AAAA-MM-JJ..AAAA-MM-JJ).")
        if x > y:
            raise HTTPException(422, f"{name} : la date de début doit précéder la date de fin.")
        out.append((x, y))
    if len(out) > 50:
        raise HTTPException(422, f"{name} : 50 périodes au plus.")
    return tuple(out)


def date_range_param(s: str, name: str) -> tuple[dt.date, dt.date]:
    """``D1..D2`` (both required)."""
    try:
        a, b = s.split("..")
        a, b = dt.date.fromisoformat(a.strip()), dt.date.fromisoformat(b.strip())
    except ValueError:
        raise HTTPException(422, f"{name} : période invalide (AAAA-MM-JJ..AAAA-MM-JJ).")
    return a, b


def build(cls, **kw):
    """A query dataclass; its ValueError (a bad combination) is a 422."""
    try:
        return cls(**kw)
    except ValueError as e:
        raise HTTPException(422, str(e))


def call(dep, **kw):
    """A query dependency called outside a request (``stibms.warm``): its declared defaults, overridden
    by ``kw`` (python names: ``from_``), so a precomputed query is the one the API parses."""
    import inspect
    d = {n: getattr(p.default, "default", p.default) for n, p in inspect.signature(dep).parameters.items()}
    return dep(**{**d, **kw})


# ---------------------------------------------------------------------------------- dependencies
def period(line: str = Q(..., description="line number (GTFS route_short_name)"),
           from_: str = Q(..., alias="from"), to: str = Q(...)):
    a, b = parse_date(from_, "from"), parse_date(to, "to")
    if a > b:
        raise HTTPException(422, "La date de début doit précéder la date de fin.")
    if (b - a).days > 400:
        raise HTTPException(422, "Période trop longue : 400 jours au plus.")
    return line.strip(), a, b


def _shared(dow, holidays, seg, phase, grid, stopzone, ref, school, reduced, exclude) -> dict:
    """The analysis parameters common to one period and two."""
    return dict(dow=dows(dow), holidays=holidays, seg=float(seg), phase=float(phase), grid=grid,
                stopzone=tuple(float(x) for x in pair(stopzone, "stopzone", sep=",")),
                ref=tuple(int(x) for x in pair(ref, "ref", int)), school=school, reduced=reduced,
                exclude=periods(exclude))


def query(line: str = Q(...), from_: str = Q(..., alias="from"), to: str = Q(...),
          dow: str = Q("0-4"), holidays: str = Q("exclude"), seg: float = Q(30.0),
          phase: float = Q(0.0), grid: str = Q("stops"), stopzone: str = Q("36,38"),
          ref: str = Q("20-24", description="evening reference hours a-b, b excluded (20-24 = 20:00-23:59)"),
          school: str = Q("include", description="STIB school-holiday timetable days: exclude | include"),
          reduced: str = Q("exclude", description="reduced-service days (strikes): exclude | include"),
          exclude: str = Q("", description="periods left out, D1..D2,D3..D4 (a single date is one day)")):
    from ..queries import Query
    ln, a, b = period(line, from_, to)
    return build(Query, line=ln, first=a, last=b,
                 **_shared(dow, holidays, seg, phase, grid, stopzone, ref, school, reduced, exclude))


def compare_query(line: str = Q(...), a: str = Q(..., description="period A (before), YYYY-MM-DD..YYYY-MM-DD"),
                  b: str = Q(..., description="period B (after)"), dow: str = Q("0-4"), holidays: str = Q("exclude"),
                  seg: float = Q(30.0), phase: float = Q(0.0), grid: str = Q("stops"), stopzone: str = Q("36,38"),
                  ref: str = Q("20-24"), school: str = Q("include"), reduced: str = Q("exclude"),
                  exclude: str = Q("", description="periods left out of both A and B, D1..D2,D3..D4")):
    from ..queries import CompareQuery
    pa, pb = date_range_param(a, "a"), date_range_param(b, "b")
    return build(CompareQuery, line=line.strip(), a=pa, b=pb,
                 **_shared(dow, holidays, seg, phase, grid, stopzone, ref, school, reduced, exclude))

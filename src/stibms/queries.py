"""The line page's queries (one period, two periods), their checks and cache keys.

The day-exclusion helpers are shared with ``stibms.ranking.RankQuery``.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import asdict, dataclass

from . import ms, schedule
from .data import ALGO_VERSION

# defaults of the day exclusions, the same on every endpoint (Query, CompareQuery, RankQuery, the API)
DAY_DEFAULTS = {"school": "include", "reduced": "exclude"}


def digest(blob: list) -> str:
    """Cache key of a JSON-able list."""
    return hashlib.sha256(json.dumps(blob, sort_keys=True, default=str).encode()).hexdigest()[:32]


def check_span(first: dt.date, last: dt.date) -> None:
    if first > last:
        raise ValueError("from must be <= to")
    if (last - first).days > 400:
        raise ValueError("period longer than 400 days")


def check_dow(dow) -> None:
    if not dow or any(d not in range(7) for d in dow):
        raise ValueError("dow must be a subset of 0..6 (0 = Monday)")


def check_holidays(holidays: str) -> None:
    if holidays not in ("exclude", "include"):
        raise ValueError("holidays must be exclude or include")


def check_days(q) -> None:
    for k in ("school", "reduced"):
        if getattr(q, k) not in schedule.CHOICES:
            raise ValueError(f"{k} must be exclude or include")
    for a, b in q.exclude:
        if a > b:
            raise ValueError("exclude: each period must start before it ends")
    if len(q.exclude) > 50:
        raise ValueError("exclude: 50 periods at most")


def days_canonical(q, first: dt.date, last: dt.date) -> dict:
    """``asdict(q)`` with the day-exclusion fields only when they differ from their defaults
    (``DAY_DEFAULTS``: one key per choice, whether the default is spelt out or not), user periods
    clipped to the queried days and merged."""
    d = asdict(q)
    for k, v in DAY_DEFAULTS.items():
        if d[k] == v:
            del d[k]
    ex = schedule.norm_periods(q.exclude, first, last)
    if ex:
        d["exclude"] = [[a.isoformat(), b.isoformat()] for a, b in ex]
    else:
        del d["exclude"]
    d["dow"] = sorted(set(q.dow))
    return d


def uses_schedule(q) -> bool:
    """True when ``q`` leaves out or shows GTFS service categories (they move with new feeds)."""
    return getattr(q, "schedule", False) or "exclude" in (getattr(q, "school", "include"), getattr(q, "reduced", "include"))


@dataclass(frozen=True)
class Query:
    line: str
    first: dt.date
    last: dt.date
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"            # "exclude" | "include"
    seg: float = 30.0
    phase: float = 0.0
    grid: str = "stops"                  # "stops" (zones from the feed, see stibms.stopzones) | "equal" | "fixed"
    stopzone: tuple[float, float] = (36.0, 38.0)   # before / after a stop; "stops": after = default exit mark
    ref: tuple[int, int] = (20, 24)              # evening reference, 20:00-23:59
    hours: tuple[int, int] = (5, 24)
    B: int = 200                         # hotspot bootstrap draws
    school: str = "include"              # STIB school-holiday timetable days (stibms.schedule)
    reduced: str = "exclude"             # reduced-service days (strikes), left out by default
    exclude: tuple = ()                  # user periods ((first, last), ...), see schedule.norm_periods

    def __post_init__(self):
        check_span(self.first, self.last)
        if not 5 <= self.seg <= 500:
            raise ValueError("seg must be within 5..500 m")
        if self.grid not in ("stops", "equal", "fixed"):
            raise ValueError("grid must be stops, equal or fixed")
        check_holidays(self.holidays)
        check_days(self)
        check_dow(self.dow)
        if not (0 <= self.ref[0] < self.ref[1] <= 28):
            raise ValueError("ref must be a:b hours with a < b")

    @property
    def ranges(self) -> list[tuple[dt.date, dt.date]]:
        return [(self.first, self.last)]

    def canonical(self) -> dict:
        d = days_canonical(self, self.first, self.last)
        d["first"], d["last"] = self.first.isoformat(), self.last.isoformat()
        return d

    def key(self, *extra) -> str:
        return digest([self.canonical(), ALGO_VERSION, ms.version(), *extra])


@dataclass(frozen=True)
class CompareQuery:
    line: str
    a: tuple[dt.date, dt.date]           # period A (before)
    b: tuple[dt.date, dt.date]           # period B (after)
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"
    seg: float = 30.0
    phase: float = 0.0
    grid: str = "stops"
    stopzone: tuple[float, float] = (36.0, 38.0)
    ref: tuple[int, int] = (20, 24)              # evening reference, 20:00-23:59
    hours: tuple[int, int] = (5, 24)
    B: int = 100                         # bootstrap draws (difference CI and period-B hotspots)
    school: str = "include"
    reduced: str = "exclude"
    exclude: tuple = ()                  # user periods, applied to both A and B

    def __post_init__(self):
        for name, (x, y) in (("a", self.a), ("b", self.b)):
            if x > y:
                raise ValueError(f"{name}: start must be <= end")
            if (y - x).days > 400:
                raise ValueError(f"{name}: period longer than 400 days")
        # the single-period checks (seg, grid, dow, ...)
        Query(self.line, self.b[0], self.b[1], self.dow, self.holidays, self.seg, self.phase, self.grid,
              self.stopzone, self.ref, school=self.school, reduced=self.reduced, exclude=self.exclude)

    @property
    def first(self) -> dt.date:
        return min(self.a[0], self.b[0])

    @property
    def last(self) -> dt.date:
        return max(self.a[1], self.b[1])

    @property
    def ranges(self) -> list[tuple[dt.date, dt.date]]:
        return [self.a, self.b]

    def canonical(self) -> dict:
        d = days_canonical(self, self.first, self.last)
        d["a"] = [self.a[0].isoformat(), self.a[1].isoformat()]
        d["b"] = [self.b[0].isoformat(), self.b[1].isoformat()]
        return d

    def key(self, *extra) -> str:
        return digest([self.canonical(), ALGO_VERSION, ms.version(), *extra])


@dataclass(frozen=True)
class PeriodQuery:
    """A line over a period without analysis parameters (coverage, route versions), for the result
    cache. ``back``: days before ``first`` the result also reads (the stamp covers them);
    ``schedule``: the result shows the GTFS service categories."""
    line: str
    first: dt.date
    last: dt.date
    back: int = 0
    schedule: bool = False

    @property
    def ranges(self) -> list[tuple[dt.date, dt.date]]:
        return [(self.first - dt.timedelta(days=self.back), self.last)]

    def canonical(self) -> dict:
        return {"line": self.line, "first": self.first.isoformat(), "last": self.last.isoformat()}

    def key(self, *extra) -> str:
        return digest([self.canonical(), ALGO_VERSION, ms.version(), *extra])

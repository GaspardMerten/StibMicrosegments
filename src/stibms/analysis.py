"""One API request -> one analysis, from the derived files (no raw data, no GTFS parsing).

Segmentation, counting and the indicators run on request from the placed month files, so any segment
length, phase, grid or stop zone is available without re-deriving.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import time
from dataclasses import asdict, dataclass, field

import polars as pl

from . import holidays as hol
from . import ms
from .data import ALGO_VERSION, Data, date_range

SOURCE = "STIB vehicle-distance et punctuality via MobilityTwin.Brussels"


class NotFound(LookupError):
    pass


@dataclass(frozen=True)
class Query:
    line: str
    first: dt.date
    last: dt.date
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"            # "exclude" | "include"
    seg: float = 30.0
    phase: float = 0.0
    grid: str = "equal"                  # "equal" | "fixed"
    stopzone: tuple[float, float] = (30.0, 60.0)
    ref: tuple[int, int] = (20, 23)
    hours: tuple[int, int] = (5, 24)
    B: int = 200                         # hotspot bootstrap draws

    def __post_init__(self):
        if self.first > self.last:
            raise ValueError("from must be <= to")
        if (self.last - self.first).days > 400:
            raise ValueError("period longer than 400 days")
        if not 5 <= self.seg <= 500:
            raise ValueError("seg must be within 5..500 m")
        if self.grid not in ("equal", "fixed"):
            raise ValueError("grid must be equal or fixed")
        if self.holidays not in ("exclude", "include"):
            raise ValueError("holidays must be exclude or include")
        if not self.dow or any(d not in range(7) for d in self.dow):
            raise ValueError("dow must be a subset of 0..6 (0 = Monday)")
        if not (0 <= self.ref[0] < self.ref[1] <= 28):
            raise ValueError("ref must be a:b hours with a < b")

    def canonical(self) -> dict:
        d = asdict(self)
        d["first"], d["last"] = self.first.isoformat(), self.last.isoformat()
        d["dow"] = sorted(set(self.dow))
        return d

    def key(self, *extra) -> str:
        blob = json.dumps([self.canonical(), ALGO_VERSION, ms.version(), *extra], sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:32]


@dataclass
class Run:
    query: Query
    dates: list[dt.date]
    holidays: dict[dt.date, str]
    net: object
    segments: pl.DataFrame
    analysis: object
    coverage: pl.DataFrame
    placed: pl.DataFrame
    passages: pl.DataFrame | None
    timings: dict = field(default_factory=dict)
    _hotspots: pl.DataFrame | None = None
    mode: str | None = None

    def hotspots(self) -> pl.DataFrame:
        if self._hotspots is None:
            from microsegments.hotspots import hotspots
            t = time.time()
            self._hotspots = hotspots(self.analysis, B=self.query.B, links=self.net.links)
            self.timings["hotspots"] = round(time.time() - t, 3)
        return self._hotspots


def clamp_dates(data: Data, q: Query) -> list[dt.date]:
    ing = data.ingested_dates()
    if not ing:
        return []
    last = min(q.last, ing[-1])
    return date_range(q.first, last) if q.first <= last else []


def route_mode(data: Data, line: str, dates) -> str | None:
    from microsegments.report import ROUTE_TYPES
    shas = data.shas_for(dates)
    if not shas:
        return None
    r = data.routes(shas[max(shas)]).filter(pl.col("route_short_name").cast(pl.Utf8) == str(line))
    if not r.height or r["route_type"][0] is None:
        return None
    return ROUTE_TYPES.get(int(r["route_type"][0]))


LINE_ABSENT = 4          # microsegments.schema.Flag.LINE_ABSENT
TYPICAL_MIN_OBS = 20     # an hour "normally has" the line when its median count is at least this


def line_absent(placed: pl.DataFrame, dates) -> pl.DataFrame:
    """(service_date, hour) where the feed was polled but carried no row of the line while that
    hour normally has it (median over the period >= TYPICAL_MIN_OBS). Partial source days exist
    (2025-03-31: 15 lines of ~80 in the feed); feed-level coverage cannot see them."""
    grid = pl.DataFrame({"service_date": list(dates)}, schema={"service_date": pl.Date}).join(
        pl.DataFrame({"hour": pl.Series(range(4, 28), dtype=pl.Int8)}), how="cross")
    n = placed.group_by("service_date", "hour").len().with_columns(pl.col("hour").cast(pl.Int8))
    g = grid.join(n, on=["service_date", "hour"], how="left").with_columns(pl.col("len").fill_null(0))
    typ = g.group_by("hour").agg(pl.col("len").median().alias("typ"))
    return (g.join(typ, on="hour").filter((pl.col("len") == 0) & (pl.col("typ") >= TYPICAL_MIN_OBS))
            .select("service_date", "hour"))


def with_line_absence(cov: pl.DataFrame, placed: pl.DataFrame, dates) -> pl.DataFrame:
    if not cov.height:
        return cov
    ab = line_absent(placed, dates).with_columns(pl.lit(True).alias("_ab"))
    return (cov.join(ab, on=["service_date", "hour"], how="left")
            .with_columns(pl.when(pl.col("_ab")).then(pl.col("flags") | LINE_ABSENT).otherwise(pl.col("flags"))
                          .cast(cov.schema["flags"]).alias("flags"))
            .drop("_ab"))


def run(data: Data, q: Query) -> Run:
    from microsegments.aggregate import count
    from microsegments.config import Params, Quality, Select
    from microsegments.metrics import analyse
    from microsegments.segments import segment

    tm: dict[str, float] = {}
    t = time.time()
    dates = clamp_dates(data, q)
    if not dates:
        raise NotFound("no ingested data in this period")
    placed = data.placed(q.line, dates)
    if placed is None or not placed.height:
        raise NotFound(f"no derived data for line {q.line} in this period")
    passages = data.passages(q.line, dates)
    cov = with_line_absence(data.coverage(dates), placed, dates)
    net = data.network(q.line, dates)
    tm["load"] = round(time.time() - t, 3)

    t = time.time()
    uids = sorted(set(placed["pattern_uid"].drop_nulls().unique().to_list())
                  | set(net.pattern_days.filter(pl.col("is_main"))["pattern_uid"].unique().to_list()))
    segs = segment(net, q.seg, q.phase, q.grid, q.stopzone, patterns=uids)
    tm["segment"] = round(time.time() - t, 3)

    params = Params(segment_m=q.seg, phase_m=q.phase, grid=q.grid, stop_zone=q.stopzone, reference_hours=q.ref)
    t = time.time()
    cube = count(placed, segs, params.tick_s)
    tm["count"] = round(time.time() - t, 3)

    hd = hol.between(dates[0], dates[-1]) if q.holidays == "exclude" else {}
    select = Select(route=q.line, dates=f"{dates[0]}..{dates[-1]}", weekdays=sorted(set(q.dow)),
                    exclude_dates=[d.isoformat() for d in hd if d.weekday() in q.dow], hours=q.hours)
    t = time.time()
    an = analyse(cube, cov if cov.height else None, passages, segs, net.pattern_days, select, params, Quality())
    tm["analyse"] = round(time.time() - t, 3)
    return Run(query=q, dates=dates, holidays=hd, net=net, segments=segs, analysis=an, coverage=cov,
               placed=placed, passages=passages, timings=tm, mode=route_mode(data, q.line, dates))


def contract(r: Run) -> dict:
    t = time.time()
    hs = r.hotspots()
    q = r.query
    out = ms.to_contract(r.analysis, r.net, hotspots=hs, line=q.line, mode=r.mode, source=SOURCE,
                         title=f"Ligne {q.line}")
    # holidays are excluded on purpose: say so instead of the generic "excluded"
    for e in out.get("period", {}).get("excluded", []):
        d = dt.date.fromisoformat(e["date"])
        if d in r.holidays:
            e["reason"] = "holiday"
            e["label"] = r.holidays[d]
    cv = out.get("coverage", {})
    if "inc" in cv:
        cv["inc"] = ["holiday" if v == "excluded" and dt.date.fromisoformat(d) in r.holidays else v
                     for d, v in zip(cv.get("dates", []), cv["inc"])]
    out["tiles"] = True
    out["query"] = q.canonical()
    out["algo"] = ALGO_VERSION
    out["microsegments"] = ms.version()
    r.timings["contract"] = round(time.time() - t, 3)
    return out


def result_table(r: Run) -> pl.DataFrame:
    """RESULT rows of the analysis with readable stop names, for .csv / .parquet downloads."""
    res = r.analysis.result
    names = r.net.links.select("link_key", "from_name", "to_name").unique("link_key", keep="first")
    out = res.join(names, on="link_key", how="left")
    return out.with_columns(pl.lit(r.query.line).alias("line"))


def coverage_days(data: Data, line: str, first: dt.date, last: dt.date) -> list[dict]:
    """Per date: feed coverage over 6-21 h, hours below 80 %, the line's counted observations,
    holiday name. ``status``: ok | low_coverage | no_data | not_ingested."""
    from microsegments.config import Quality
    qual = Quality()
    ing = set(data.ingested_dates())
    dates = date_range(first, last)
    pl_ = data.placed(line, dates)
    cov = data.coverage(dates)
    if pl_ is not None:
        cov = with_line_absence(cov, pl_, [d for d in dates if d in ing])
    per: dict[dt.date, tuple[float, int]] = {}
    if cov.height:
        g = (cov.filter(pl.col("hour").is_between(6, 20))
             .group_by("service_date")
             .agg(pl.col("coverage").mean().alias("c"),
                  ((pl.col("coverage") < qual.min_hour_coverage) | ((pl.col("flags") & 3) != 0)).sum().alias("bad"),
                  ((pl.col("flags") & LINE_ABSENT) != 0).sum().alias("absent")))
        per = {r[0]: (float(r[1]), int(r[2]), int(r[3])) for r in g.iter_rows()}
    obs: dict[dt.date, int] = {}
    if pl_ is not None:
        obs = dict(pl_.group_by("service_date").len().iter_rows())
    hd = hol.between(first, last)
    out = []
    for d in dates:
        c, bad, absent = per.get(d, (None, None, None))
        if d not in ing:
            status = "not_ingested"
        elif c is None:
            status = "no_data"
        elif bad / 15 > 1 - qual.min_day_coverage + 1e-9:
            status = "low_coverage"
        elif not obs.get(d) or (bad + absent) / 15 > 1 - qual.min_day_coverage + 1e-9:
            status = "line_absent"
        else:
            status = "ok"
        out.append({"date": d.isoformat(), "dow": d.weekday(), "coverage": None if c is None else round(c, 3),
                    "hours_bad": bad, "hours_line_absent": absent, "obs": int(obs.get(d, 0)), "holiday": hd.get(d), "status": status})
    return out


def versions(data: Data, line: str, first: dt.date, last: dt.date) -> list[dict]:
    dates = [d for d in date_range(first, last) if d in set(data.ingested_dates())]
    if not dates:
        return []
    net = data.network(line, dates)
    if not net.pattern_days.height:
        return []
    v = net.versions()
    pats = net.patterns.select("pattern_uid", "stop_names")
    v = v.join(pats, on="pattern_uid", how="left")
    out = []
    for r in v.iter_rows(named=True):
        names = r.pop("stop_names") or []
        r["first"], r["last"] = r["first"].isoformat(), r["last"].isoformat()
        r["from"], r["to"], r["n_stops"] = (names[0] if names else None), (names[-1] if names else None), len(names)
        out.append(r)
    return out

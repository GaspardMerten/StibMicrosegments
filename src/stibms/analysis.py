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


@dataclass
class Loaded:
    """Derived inputs of one line on a set of dates (one read, shared by the periods of a comparison)."""
    dates: list[dt.date]
    placed: pl.DataFrame
    passages: pl.DataFrame | None
    coverage: pl.DataFrame
    net: object
    timings: dict = field(default_factory=dict)


def not_derived_months(data: Data, dates) -> list[str]:
    from .data import months_of
    have = data.derived_months()
    return [m for m in months_of(dates) if m not in have]


def load(data: Data, line: str, dates: list[dt.date], label: str = "cette période") -> Loaded:
    t = time.time()
    if not dates:
        raise NotFound(f"aucune donnée ingérée pour {label}")
    placed = data.placed(line, dates)
    if placed is None or not placed.height:
        pending = not_derived_months(data, dates)
        if pending:
            raise NotFound(f"données en cours de préparation pour {label} (mois {', '.join(pending)} pas encore calculés)")
        raise NotFound(f"pas d'observation de la ligne {line} pour {label}")
    passages = data.passages(line, dates)
    cov = with_line_absence(data.coverage(dates), placed, dates)
    net = data.network(line, dates)
    return Loaded(dates, placed, passages, cov, net, {"load": round(time.time() - t, 3)})


def _segments(ld: Loaded, q) -> pl.DataFrame:
    from microsegments.segments import segment
    uids = sorted(set(ld.placed["pattern_uid"].drop_nulls().unique().to_list())
                  | set(ld.net.pattern_days.filter(pl.col("is_main"))["pattern_uid"].unique().to_list()))
    return segment(ld.net, q.seg, q.phase, q.grid, q.stopzone, patterns=uids)


def _params(q):
    from microsegments.config import Params
    return Params(segment_m=q.seg, phase_m=q.phase, grid=q.grid, stop_zone=q.stopzone, reference_hours=q.ref)


def _analyse(cube, ld: Loaded, segs, q, first: dt.date, last: dt.date, dates: list[dt.date]):
    from microsegments.config import Quality, Select
    from microsegments.metrics import analyse
    hd = hol.between(first, last) if q.holidays == "exclude" else {}
    present = set(dates)
    select = Select(route=q.line, dates=f"{first}..{last}", weekdays=sorted(set(q.dow)),
                    exclude_dates=[d.isoformat() for d in hd if d.weekday() in q.dow and d in present],
                    hours=q.hours)
    cov = ld.coverage
    return analyse(cube, cov if cov.height else None, ld.passages, segs, ld.net.pattern_days, select,
                   _params(q), Quality()), hd


def run(data: Data, q: Query, with_mode: bool = True) -> Run:
    from microsegments.aggregate import count

    dates = clamp_dates(data, q)
    ld = load(data, q.line, dates)
    tm = dict(ld.timings)
    t = time.time()
    segs = _segments(ld, q)
    tm["segment"] = round(time.time() - t, 3)
    t = time.time()
    cube = count(ld.placed, segs, _params(q).tick_s)
    tm["count"] = round(time.time() - t, 3)
    t = time.time()
    an, hd = _analyse(cube, ld, segs, q, dates[0], dates[-1], dates)
    tm["analyse"] = round(time.time() - t, 3)
    return Run(query=q, dates=dates, holidays=hd, net=ld.net, segments=segs, analysis=an, coverage=ld.coverage,
               placed=ld.placed, passages=ld.passages, timings=tm,
               mode=route_mode(data, q.line, dates) if with_mode else None)


def _relabel_holidays(excluded: list[dict], holidays: dict) -> None:
    for e in excluded:
        d = dt.date.fromisoformat(e["date"])
        if d in holidays:
            e["reason"] = "holiday"
            e["label"] = holidays[d]


def contract(r: Run) -> dict:
    t = time.time()
    hs = r.hotspots()
    q = r.query
    out = ms.to_contract(r.analysis, r.net, hotspots=hs, line=q.line, mode=r.mode, source=SOURCE,
                         title=f"Ligne {q.line}", names="title")
    # holidays are excluded on purpose: say so instead of the generic "excluded"
    _relabel_holidays(out.get("period", {}).get("excluded", []), r.holidays)
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


# ---------------------------------------------------------------------------------- two periods
@dataclass(frozen=True)
class CompareQuery:
    line: str
    a: tuple[dt.date, dt.date]           # period A (before)
    b: tuple[dt.date, dt.date]           # period B (after)
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"
    seg: float = 30.0
    phase: float = 0.0
    grid: str = "equal"
    stopzone: tuple[float, float] = (30.0, 60.0)
    ref: tuple[int, int] = (20, 23)
    hours: tuple[int, int] = (5, 24)
    B: int = 100                         # bootstrap draws (difference CI and period-B hotspots)

    def __post_init__(self):
        for name, (x, y) in (("a", self.a), ("b", self.b)):
            if x > y:
                raise ValueError(f"{name}: start must be <= end")
            if (y - x).days > 400:
                raise ValueError(f"{name}: period longer than 400 days")
        # the single-period checks (seg, grid, dow, ...)
        Query(self.line, self.b[0], self.b[1], self.dow, self.holidays, self.seg, self.phase, self.grid,
              self.stopzone, self.ref)

    @property
    def first(self) -> dt.date:
        return min(self.a[0], self.b[0])

    @property
    def last(self) -> dt.date:
        return max(self.a[1], self.b[1])

    def canonical(self) -> dict:
        d = asdict(self)
        d["a"] = [self.a[0].isoformat(), self.a[1].isoformat()]
        d["b"] = [self.b[0].isoformat(), self.b[1].isoformat()]
        d["dow"] = sorted(set(self.dow))
        return d

    def key(self, *extra) -> str:
        blob = json.dumps([self.canonical(), ALGO_VERSION, ms.version(), *extra], sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()[:32]


def _clamp(data: Data, a: dt.date, b: dt.date) -> list[dt.date]:
    ing = data.ingested_dates()
    if not ing:
        return []
    last = min(b, ing[-1])
    return date_range(a, last) if a <= last else []


def run_compare(data: Data, cq: CompareQuery) -> dict:
    """The platform twin of ``microsegments.compare.run_compare``: one load over the union of both
    periods (one network, one segmentation, so segment keys agree), one count, one analysis per
    period on its own days, then ``compare``. Returns period B's contract plus the ``compare``
    section (the package page opens in its comparison view)."""
    from microsegments.aggregate import count
    from microsegments.compare import compare
    from microsegments.hotspots import hotspot_status, hotspots
    from microsegments.report import name_map

    tm: dict[str, float] = {}
    da, db = _clamp(data, *cq.a), _clamp(data, *cq.b)
    if not da:
        raise NotFound("période A : aucune donnée ingérée")
    if not db:
        raise NotFound("période B : aucune donnée ingérée")
    union = sorted(set(da) | set(db))
    ld = load(data, cq.line, union, "ces périodes")
    tm.update(ld.timings)
    seen = set(ld.placed["service_date"].unique().to_list())
    for lab, ds in (("période A", da), ("période B", db)):
        if not seen & set(ds):
            pending = not_derived_months(data, ds)
            raise NotFound(f"{lab} : " + (f"données en cours de préparation (mois {', '.join(pending)} pas encore calculés)"
                                          if pending else f"pas d'observation de la ligne {cq.line}"))
    t = time.time()
    segs = _segments(ld, cq)
    cube = count(ld.placed, segs, _params(cq).tick_s)
    tm["segment_count"] = round(time.time() - t, 3)
    t = time.time()
    an_a, hd_a = _analyse(cube, ld, segs, cq, da[0], da[-1], da)
    an_b, hd_b = _analyse(cube, ld, segs, cq, db[0], db[-1], db)
    tm["analyse"] = round(time.time() - t, 3)
    if not an_a.days or not an_b.days:
        which = "A" if not an_a.days else "B"
        raise NotFound(f"période {which} : aucun jour utilisable (couverture insuffisante ou ligne absente)")
    t = time.time()
    cmp = compare(an_a, an_b, B=cq.B, links=ld.net.links)
    tm["compare"] = round(time.time() - t, 3)
    t = time.time()
    hs = hotspots(an_b, B=cq.B, links=ld.net.links) if hotspot_status(an_b)[0] else None
    tm["hotspots"] = round(time.time() - t, 3)
    mode = route_mode(data, cq.line, db)
    names = name_map("title", ld.net)
    out = ms.to_contract(an_b, ld.net, hotspots=hs, line=cq.line, mode=mode, source=SOURCE,
                         title=f"Ligne {cq.line}", names="title")
    out["compare"] = cmp.to_contract(names)
    _relabel_holidays(out.get("period", {}).get("excluded", []), hd_b)
    _relabel_holidays(out["compare"]["a"]["excluded"], hd_a)
    _relabel_holidays(out["compare"]["b"]["excluded"], hd_b)
    out["tiles"] = True
    out["query"] = cq.canonical()
    out["algo"] = ALGO_VERSION
    out["microsegments"] = ms.version()
    out["timings"] = tm
    return out


# ---------------------------------------------------------------------------------- network ranking
LINK_COST_SCHEMA = {
    "direction_id": pl.Int8, "link_key": pl.Utf8, "from_name": pl.Utf8, "to_name": pl.Utf8, "len_m": pl.Float64,
    "veh_s_day": pl.Float64, "veh_s_day_line": pl.Float64, "passages_day": pl.Float64, "n_days": pl.Int32,
    "peak_hour": pl.Int8, "terminal": pl.Boolean,
}


def link_costs(r: Run) -> pl.DataFrame:
    """Vehicle time lost per day on each link (stop pair) of the line, 6-21 h on the included days.

    Per segment, the day band (``hour`` = -3) gives excess E = O - r (obs per passage over the
    evening reference) and passages P (summed over included days); E x P / n_days x tick_s = seconds
    of vehicle time per day above the evening (= the sum over day hours of excess x passages, since
    the band's O is a ratio of sums). Summed over the segments of the link (signed: a faster bit
    offsets a slower one). ``veh_s_day_line`` is the same against the line-level reference.
    ``terminal``: first or last link of one of the line's patterns (dwell there is mostly regulation
    time at the terminus, which the ranking leaves out by default)."""
    tick = r.analysis.params.tick_s
    res = r.analysis.result
    if not res.height:
        return pl.DataFrame(schema=LINK_COST_SCHEMA)
    res = res.unique(["direction_id", "seg_key", "hour"], keep="first")
    day = res.filter(pl.col("hour") == -3)
    per_day = pl.col("passages") / pl.col("n_days").cast(pl.Float64)
    seg = day.filter(pl.col("n_days") > 0).with_columns(
        (pl.col("excess_per_passage").fill_null(0) * per_day * tick).alias("s"),
        (pl.col("excess_line_per_passage").fill_null(0) * per_day * tick).alias("sl"),
        per_day.alias("pd"))
    # strongest hour of each link (most vehicle time lost)
    hrs = (res.filter((pl.col("hour") >= 6) & (pl.col("hour") < 21) & (pl.col("n_days") > 0))
           .with_columns((pl.col("excess_per_passage").fill_null(0) * pl.col("passages") / pl.col("n_days").cast(pl.Float64)).alias("s"))
           .group_by("direction_id", "link_key", "hour").agg(pl.col("s").sum())
           .sort("s", descending=True).unique(["direction_id", "link_key"], keep="first")
           .select("direction_id", "link_key", pl.col("hour").alias("peak_hour")))
    g = (seg.group_by("direction_id", "link_key")
         .agg(pl.col("s").sum().alias("veh_s_day"), pl.col("sl").sum().alias("veh_s_day_line"),
              pl.col("pd").median().alias("passages_day"), pl.col("n_days").max().cast(pl.Int32),
              pl.col("len_m").sum().alias("len_m")))
    names = r.net.links.select("link_key", "from_name", "to_name").unique("link_key", keep="first")
    # links into or out of a terminus of the line: regulation time, not traffic
    lk = r.net.links
    ends = (lk.with_columns(pl.col("link_idx").max().over("pattern_uid").alias("_m"))
            .filter((pl.col("link_idx") == 0) | (pl.col("link_idx") == pl.col("_m")))["link_key"].unique())
    out = (g.join(names, on="link_key", how="left").join(hrs, on=["direction_id", "link_key"], how="left")
           .with_columns(pl.col("link_key").is_in(ends.implode()).alias("terminal")))
    return out.select([pl.col(c).cast(t) for c, t in LINK_COST_SCHEMA.items()])


def result_table(r: Run) -> pl.DataFrame:
    """RESULT rows of the analysis with readable stop names, for .csv / .parquet downloads."""
    res = r.analysis.result
    names = r.net.links.select("link_key", "from_name", "to_name").unique("link_key", keep="first")
    out = res.join(names, on="link_key", how="left")
    return out.with_columns(pl.lit(r.query.line).alias("line"))


def coverage_days(data: Data, line: str, first: dt.date, last: dt.date) -> list[dict]:
    """Per date: feed coverage over 6-21 h, hours below 80 %, the line's counted observations,
    holiday name. ``status``: ok | low_coverage | no_data | line_absent | not_derived (ingested, month
    not derived yet) | not_ingested."""
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
    have = data.derived_months()
    out = []
    for d in dates:
        c, bad, absent = per.get(d, (None, None, None))
        if d not in ing:
            status = "not_ingested"
        elif f"{d.year:04d}-{d.month:02d}" not in have:
            status = "not_derived"
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

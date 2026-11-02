"""The network page's indicators: speed and km per month, per mode and for any set of lines; speed per link.

    GET /api/network?from&to&dow&hours=6-21&holidays&school&reduced&exclude&mode=all|bus|tram|metro&lines=55,81
    GET /api/network/map?... the same on a window of at most 400 days

Read from the monthly tables of ``stibms.linkcube``: ``linehours`` and ``plan`` for the evolution (a few 10 k
rows per month, so months add up in well under a second), ``hourly`` for the map.

Per group of lines (network, each mode, the lines asked for) and month:

    speed with stops     v  = sum d_s / sum t_s    metres over seconds of the usable hours where the position feed
                                                    holds the line (``linkcube.MIN_FEED_SHARE``), first and last
                                                    links of each pattern and the month's regulation links
                                                    left out (waiting on purpose, see ``stibms.linkcube``)
    speed without stops  vr = sum d_r / sum t_r    the same between the stop zones (``stibms.stopzones``)
    km planned           the GTFS timetable, each trip's length shared over the hours it runs in
    km run               passages x link length, every hour with passages
    pct                  km run / km planned on the hours the feed was up. A line missing from a working feed did
                         not run, so a strike counts as lost km; an hour without feed is not known either way.

``plan`` / ``real`` / ``pct`` follow the days and hours asked for; ``plan_all`` / ``real_all`` / ``pct_all`` are
the production of the whole month (every day and hour within the period, whatever the filters), the figure to
compare with STIB's. The metro has km but no speed: the feed only places its trains at stations (a train
between two stations reports nothing), so its speeds would read 27-48 km/h against STIB's 27.9.
The network's speed is therefore bus + tram.

Days left out (``left_out`` in the answer, per month and for the period):

- no feed: the feed was down for most of the day's planned km (``NOFEED_SHARE``). Nothing is known about it,
  so it never counts in the km, whatever the options (summer 2024: the source published nothing for weeks);
- strike: the timetable announced a reduced service (``schedule``), or the feed was up and under
  ``STRIKE_RATIO`` of the planned km ran (STIB does not always publish the strike timetable). Left out of the
  speeds, the km and the map with ``reduced=exclude`` (the default), counted with ``reduced=include``.

Planned km on a strike day may already be the reduced service: STIB publishes it in the GTFS (``stibms.schedule``).
"""
from __future__ import annotations

import datetime as dt
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import polars as pl

from . import ms, schedule
from .data import ALGO_VERSION, Data, by_line, date_range, month_dates, months_between
from .linkcube import TICK_S
from .linkcube import rel as cube_rel
from .queries import check_days, check_dow, check_holidays, check_span, days_canonical, digest
from .stopzones import APPROACH_M, default_head, heads

NET_VERSION = 4
NOFEED_SHARE = 0.5          # under this share of the planned km in hours with feed: a day without data
STRIKE_RATIO = 0.75         # under this share of the planned km run, feed up: a strike day
MODES = ("all", "bus", "tram", "metro")
NO_SPEED = {"metro"}             # stations only in the feed: no speed (module docstring)
DEFAULT_HOURS = (6, 21)
DEFAULT_MONTHS = 12
MAP_MIN_PASSAGES_DAY = 1.0       # fewer passages a day: a detour, no speed shown
MAP_MAX_KMH = 70.0               # above: a link too short for the 20 s between reports, not a speed
_GEO_DECIMALS = 5
_GEO_STEP = 2e-4                 # about 15-20 m: enough for a network map, a fifth of the points


def check_hours(hours: tuple[int, int]) -> None:
    if not (0 <= hours[0] < hours[1] <= 28):
        raise ValueError("hours must be a-b with a < b, within 0..28 (24 and more: after midnight)")


@dataclass(frozen=True)
class NetQuery:
    first: dt.date
    last: dt.date
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    hours: tuple[int, int] = DEFAULT_HOURS
    holidays: str = "exclude"
    mode: str = "all"
    lines: tuple[str, ...] = ()      # when given, the selection is these lines whatever the mode
    school: str = "include"
    reduced: str = "exclude"
    exclude: tuple = ()

    def __post_init__(self):
        if self.first > self.last:
            raise ValueError("from must be <= to")
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {', '.join(MODES)}")
        check_dow(self.dow)
        check_hours(self.hours)
        check_holidays(self.holidays)
        check_days(self)

    @property
    def ranges(self) -> list[tuple[dt.date, dt.date]]:
        return [(self.first, self.last)]

    def canonical(self) -> dict:
        d = days_canonical(self, self.first, self.last)
        d["first"], d["last"] = self.first.isoformat(), self.last.isoformat()
        d["lines"] = sorted(self.lines, key=by_line)
        return d

    def key(self, stamp: str, *extra) -> str:
        return digest([self.canonical(), ALGO_VERSION, NET_VERSION, ms.version(), stamp, *extra])

    def exclusions(self, data: Data) -> dict:
        return schedule.exclusions(data, self.first, self.last, self.holidays, self.school, self.reduced, self.exclude)


@dataclass(frozen=True)
class MapQuery(NetQuery):
    """The map reads hourly tables: a bounded window."""
    def __post_init__(self):
        super().__post_init__()
        check_span(self.first, self.last)


def default_window(data: Data, n: int = DEFAULT_MONTHS) -> tuple[dt.date, dt.date] | None:
    """The last ``n`` calendar months up to the last complete derived month."""
    ing = data.ingested_dates()
    months = [m for m in data.derived_months() if ing and month_dates(m)[-1] <= ing[-1]]
    if not months:
        return None
    y, m = map(int, months[-1].split("-"))
    k = y * 12 + m - 1 - (n - 1)
    first = dt.date(k // 12, k % 12 + 1, 1)
    return max(first, dt.date(*map(int, (months[0] + "-01").split("-")))), month_dates(months[-1])[-1]


# ---------------------------------------------------------------------------------- inputs
_cache: dict[tuple, pl.DataFrame | None] = {}
_lock = threading.Lock()
_MAX_TABLES = 80


def _table(data: Data, month: str, name: str, columns: list[str] | None = None) -> pl.DataFrame | None:
    """A month's small table, cached in process per derive stamp (the hourly ones are read each time)."""
    k = (month, name, data.month_stamp(month))
    with _lock:
        if k in _cache:
            return _cache[k]
    df = data.read_optional(cube_rel(month, name), columns)
    if df is None:           # not built yet: do not remember the absence
        return None
    with _lock:
        for old in [x for x in _cache if x[:2] == k[:2]]:
            del _cache[old]
        _cache[k] = df
        while len(_cache) > _MAX_TABLES:
            _cache.pop(next(iter(_cache)))
    return df


def modes_of(data: Data) -> dict[str, str | None]:
    from .ranking import line_modes
    return {ln: (v or {}).get("mode") for ln, v in line_modes(data).items()}


def _month_of(col: str = "service_date") -> pl.Expr:
    return pl.col(col).dt.strftime("%Y-%m").alias("month")


def _keep_days(q: NetQuery, excluded) -> pl.Expr:
    return (pl.col("service_date").dt.weekday().sub(1).is_in(sorted(set(q.dow))) & ~pl.col("service_date").is_in(sorted(excluded)))


def _in_hours(q: NetQuery) -> pl.Expr:
    return pl.col("hour").is_between(q.hours[0], q.hours[1] - 1)


# ---------------------------------------------------------------------------------- evolution
def _per_line_month(lh: pl.DataFrame, plan: pl.DataFrame, chosen: pl.Expr) -> pl.DataFrame:
    """Per (line, month), on the rows ``chosen``: speed totals, km run and planned (all, and feed up)."""
    r = lh.filter(chosen).group_by("line").agg(
        pl.col("km").sum().cast(pl.Float64).alias("real"), pl.col("km").filter("feed_ok").sum().cast(pl.Float64).alias("real_ok"),
        *[pl.col(c).sum().cast(pl.Float64) for c in ("d_s", "t_s", "d_r", "t_r")])
    if lh.height:     # a line with no cube that month (failed, never derived) is not 100 % lost
        plan = plan.filter(pl.col("line").is_in(lh["line"].unique().implode()))
    p = plan.filter(chosen).group_by("line").agg(
        pl.col("km").sum().cast(pl.Float64).alias("plan"), pl.col("km").filter("feed_ok").sum().cast(pl.Float64).alias("plan_ok"))
    return r.join(p, on="line", how="full", coalesce=True).fill_null(0.0)


def _group_values(rows: list[dict], speed: bool) -> dict:
    """The totals of a group of (line, month) rows."""
    t = {k: sum(r[k] for r in rows) for k in ("real", "real_ok", "plan", "plan_ok", "d_s", "t_s", "d_r", "t_r",
                                              "real_all", "real_ok_all", "plan_all", "plan_ok_all")}
    kmh = lambda d, s: round(d / s * 3.6, 2) if speed and s > 0 and d > 0 else None  # noqa: E731
    pct = lambda a, b: round(100 * a / b, 1) if b > 0 else None  # noqa: E731
    return {"v": kmh(t["d_s"], t["t_s"]), "vr": kmh(t["d_r"], t["t_r"]),
            "plan": round(t["plan"]), "real": round(t["real"]), "pct": pct(t["real_ok"], t["plan_ok"]),
            "plan_all": round(t["plan_all"]), "real_all": round(t["real_all"]), "pct_all": pct(t["real_ok_all"], t["plan_ok_all"]),
            "n_lines": sum(1 for r in rows if r["plan_all"] > 0 or r["real_all"] > 0)}


def _groups(q: NetQuery, modes: dict) -> dict[str, tuple]:
    """group -> (keep a line?, its speed counts?). ``sel``: the lines asked for, when they are not a mode."""
    def mode(m):
        return lambda ln: modes.get(ln) == m
    out = {"all": (lambda ln: True), "bus": mode("bus"), "tram": mode("tram"), "metro": mode("metro")}
    if q.lines:
        want = set(q.lines)
        out["sel"] = lambda ln: ln in want
    return out


def _values(rows: list[dict], groups: dict, modes: dict) -> dict:
    out = {}
    for g, keep in groups.items():
        sub = [r for r in rows if keep(r["line"])]
        # a group's speed leaves out the lines without one (the network: bus + tram)
        spd = [{**r, **({"d_s": 0.0, "t_s": 0.0, "d_r": 0.0, "t_r": 0.0} if modes.get(r["line"]) in NO_SPEED else {})} for r in sub]
        out[g] = _group_values(spd, speed=True)
    return out


def _anomalies(lh: pl.DataFrame, plan: pl.DataFrame, reduced: set) -> dict[dt.date, tuple[str, int | None]]:
    """date -> ("nofeed", None) | ("strike", % of the planned km run) for the network (module docstring). The plan
    counts the lines of the month's tables only (a line never derived is not a strike)."""
    if not plan.height:
        return {}
    lines = lh["line"].unique().implode() if lh.height else pl.Series([], dtype=pl.Utf8).implode()
    p = plan.filter(pl.col("line").is_in(lines)).group_by("service_date").agg(
        pl.col("km").sum().cast(pl.Float64).alias("p"), pl.col("km").filter("feed_ok").sum().cast(pl.Float64).alias("p_ok"))
    r = lh.group_by("service_date").agg(pl.col("km").filter("feed_ok").sum().cast(pl.Float64).alias("r_ok"))
    out = {}
    for d, pp, pok, rok in p.join(r, on="service_date", how="left").fill_null(0.0).select("service_date", "p", "p_ok", "r_ok").iter_rows():
        if pp <= 0:
            continue
        if pok < NOFEED_SHARE * pp:
            out[d] = ("nofeed", None)
        elif d in reduced or rok < STRIKE_RATIO * pok:
            out[d] = ("strike", round(100 * rok / pok))
    return out


def _month_tables(data: Data, q: NetQuery, month: str):
    lh, plan = _table(data, month, "linehours"), _table(data, month, "plan")
    if lh is None or plan is None:
        return None, None
    span = pl.col("service_date").is_between(q.first, q.last)
    return lh.filter(span), plan.filter(span)


def _left_out(q: NetQuery, an: dict) -> tuple[set, set]:
    """(days out of the query's selection, days out of the whole-month km) from a month's anomalies."""
    nofeed = {d for d, (w, _) in an.items() if w == "nofeed"}
    strikes = {d for d, (w, _) in an.items() if w == "strike"} if q.reduced == "exclude" else set()
    return nofeed | strikes, nofeed | strikes


def left_out_days(data: Data, first: dt.date, last: dt.date, reduced: str = "exclude") -> set:
    """The days of [first, last] the network page leaves out with these options (no feed; strikes with
    ``reduced=exclude``), for the ranking to leave out the same."""
    q = NetQuery(first=first, last=last, reduced=reduced)
    red, out = _reduced_days(data, q), set()
    for m in months_between(first, last):
        if m in data.derived_months():
            lh, plan = _month_tables(data, q, m)
            if lh is not None:
                out |= _left_out(q, _anomalies(lh, plan, red))[0]
    return out


def _reduced_days(data: Data, q: NetQuery) -> set:
    return {d for d, c in schedule.categories(data, q.first, q.last).items() if c == "reduced_service"}


def _month_rows(data: Data, q: NetQuery, month: str, excluded: set, modes: dict, reduced: set) -> dict | None:
    """One month's rows per line, on the query's days and hours and on the whole month, and its days."""
    lh, plan = _month_tables(data, q, month)
    if lh is None or not (lh.height or plan.height):
        return None
    an = _anomalies(lh, plan, reduced)
    out_sel, out_all = _left_out(q, an)
    excluded = set(excluded) | out_sel
    sel = _keep_days(q, excluded) & _in_hours(q)
    a, b = _per_line_month(lh, plan, sel), _per_line_month(lh, plan, ~pl.col("service_date").is_in(sorted(out_all)))
    b = b.select("line", pl.col("real").alias("real_all"), pl.col("real_ok").alias("real_ok_all"),
                 pl.col("plan").alias("plan_all"), pl.col("plan_ok").alias("plan_ok_all"))
    rows = (b.join(a, on="line", how="left").fill_null(0.0)).to_dicts()
    days = set(lh["service_date"].unique().to_list()) | set(plan["service_date"].unique().to_list())
    kept = [d for d in days if d.weekday() in q.dow and d not in excluded]
    left = [{"date": d.isoformat(), "why": w, "pct": p, "out": d in out_all} for d, (w, p) in sorted(an.items())]
    return {"rows": rows, "days_all": len(days - out_all), "days": len(kept), "left_out": left}


def compute(data: Data, q: NetQuery) -> dict:
    """Series per month and totals of the period, per group."""
    t0 = time.time()
    modes, excluded = modes_of(data), set(q.exclusions(data))
    groups, reduced = _groups(q, modes), _reduced_days(data, q)
    months = [m for m in months_between(q.first, q.last) if m in data.derived_months()]
    with ThreadPoolExecutor(max_workers=min(12, max(1, len(months)))) as ex:
        got = list(ex.map(lambda m: _month_rows(data, q, m, excluded, modes, reduced), months))
    series, per_line = [], {}
    ingested = set(data.ingested_dates())
    for m, g in zip(months, got):
        if g is None:
            continue
        cal = month_dates(m)
        inside = [d for d in cal if q.first <= d <= q.last]
        series.append({"month": m, "days": g["days"], "days_all": g["days_all"], "days_in_month": len(cal),
                       "complete": len(inside) == len(cal) and all(d in ingested for d in cal),
                       "groups": _values(g["rows"], groups, modes), "left_out": g["left_out"]})
        for r in g["rows"]:
            acc = per_line.setdefault(r["line"], dict.fromkeys(r, 0.0))
            for k, v in r.items():
                if k != "line":
                    acc[k] += v
            acc["line"] = r["line"]
    sel = "sel" if q.lines else q.mode
    return {"query": q.canonical(), "selection": sel, "months": series,
            "total": _values(list(per_line.values()), groups, modes) if per_line else {},
            "days": sum(s["days"] for s in series), "days_all": sum(s["days_all"] for s in series),
            "no_speed": sorted(NO_SPEED), "algo": ALGO_VERSION, "net_version": NET_VERSION, "microsegments": ms.version(),
            "status": "done", "stamp": data.stamp(date_range(q.first, q.last)), "seconds": round(time.time() - t0, 2)}


# ---------------------------------------------------------------------------------- map
def _keep_lines(q: NetQuery, modes: dict) -> pl.Expr | None:
    if q.lines:
        return pl.col("line").is_in(list(q.lines))
    if q.mode != "all":
        return pl.col("line").is_in([ln for ln, m in modes.items() if m == q.mode])
    return None


def _month_links(data: Data, q: MapQuery, month: str, excluded: set, keep: pl.Expr | None) -> pl.DataFrame | None:
    """Per (line, link): passages and observations of the usable hours asked for in this month. Aggregated
    month by month, so a long window never holds its hourly rows in memory."""
    h = data.read_optional(cube_rel(month, "hourly"), ["line", "service_date", "hour", "link_key", "n", "spd_ok", "o_r", "o_s"])
    if h is None or not h.height:
        return None
    f = h.filter(pl.col("spd_ok") & pl.col("service_date").is_between(q.first, q.last) & _keep_days(q, excluded) & _in_hours(q))
    if keep is not None:
        f = f.filter(keep)
    g = f.group_by("line", "link_key").agg(pl.col("n").sum().cast(pl.Float64), pl.col("o_r").sum().cast(pl.Float64), pl.col("o_s").sum().cast(pl.Float64),
                                           pl.col("service_date").n_unique().alias("days"))
    roles = _table(data, month, "links")
    if roles is None or "reg" not in roles.columns:
        g = g.with_columns(pl.lit(False).alias("reg"), pl.lit(False).alias("reg_r"))
    else:
        g = (g.join(roles.select("line", "link_key", "reg", "reg_r").unique(["line", "link_key"]), on=["line", "link_key"], how="left")
             .with_columns(pl.col("reg").fill_null(False), pl.col("reg_r").fill_null(False)))
    # what the speeds count: a regulation link is out in the month where it is one, its passages still shown
    whole, between = (pl.when(pl.col(c)).then(0.0).otherwise(1.0) for c in ("reg", "reg_r"))
    return g.select("line", "link_key", "n", "days", (pl.col("n") * whole).alias("n_v"), (pl.col("n") * between).alias("n_r"),
                    ((pl.col("o_r") + pl.col("o_s")) * whole).alias("o_v"), (pl.col("o_r") * between).alias("o_r"))


def _thin(geo: list | None) -> list:
    """Coordinates rounded (about 1 m), points closer than ``_GEO_STEP`` dropped, the ends kept: the map's
    payload is a whole network."""
    pts = [[round(x, _GEO_DECIMALS), round(y, _GEO_DECIMALS)] for x, y in geo or []]
    out = pts[:1]
    for p in pts[1:-1]:
        if abs(p[0] - out[-1][0]) + abs(p[1] - out[-1][1]) >= _GEO_STEP:
            out.append(p)
    return out + pts[-1:] if len(pts) > 1 else out


def compute_map(data: Data, q: MapQuery) -> dict:
    """Speed per link (every line on it), with and without stop zones."""
    from .ranking import _registry
    from microsegments.report import title_name
    t0 = time.time()
    modes, excluded = modes_of(data), set(q.exclusions(data))
    keep = _keep_lines(q, modes)
    months = [m for m in months_between(q.first, q.last) if m in data.derived_months()]
    reduced = _reduced_days(data, q)
    for m in months:      # strike days and days without feed, as on the evolution
        lh, plan = _month_tables(data, q, m)
        if lh is not None:
            excluded |= _left_out(q, _anomalies(lh, plan, reduced))[0]
    with ThreadPoolExecutor(max_workers=min(4, max(1, len(months)))) as ex:   # hourly tables: a few at a time
        got = [x for x in ex.map(lambda m: _month_links(data, q, m, excluded, keep), months) if x is not None and x.height]
    empty = {"query": q.canonical(), "links": [], "status": "done", "n_days": 0, "seconds": round(time.time() - t0, 2)}
    if not got:
        return empty
    roles = [x for x in (_table(data, m, "links") for m in months) if x is not None and x.height]
    marks = [x for x in (_table(data, m, "marks") for m in months) if x is not None and x.height]
    if not roles:
        return empty
    links = (pl.concat(roles, how="diagonal_relaxed").sort("term", descending=True).unique(["line", "link_key"], keep="first"))
    mk = pl.concat(marks, how="diagonal_relaxed") if marks else None
    head = (mk.group_by("line", "link_key").agg((pl.col("mark_m").max().ceil() + 1.0).alias("head_m")) if mk is not None and mk.height
            else pl.DataFrame(schema={"line": pl.Utf8, "link_key": pl.Utf8, "head_m": pl.Float64}))   # stopzones.heads, per line
    x = (pl.concat(got).group_by("line", "link_key").agg(*[pl.col(c).sum() for c in ("n", "n_v", "n_r", "o_v", "o_r", "days")])
         .join(links.select("line", "link_key", "from_name", "to_name", "len_m", "term"), on=["line", "link_key"], how="inner")
         .join(head, on=["line", "link_key"], how="left"))
    run = (pl.col("len_m") - pl.col("head_m").fill_null(default_head()) - APPROACH_M).clip(0)
    no_speed = [ln for ln, m in modes.items() if m in NO_SPEED]
    x = x.with_columns(pl.col("line").is_in(no_speed).alias("nosp"), (pl.col("n_v") * pl.col("len_m")).alias("_d"), (pl.col("n_r") * run).alias("_dr"))
    n_days = max(int(r) for r in x["days"].to_list()) if x.height else 0
    g = (x.group_by("link_key").agg(
        pl.col("_d").filter(~pl.col("nosp")).sum().alias("d"), pl.col("_dr").filter(~pl.col("nosp")).sum().alias("dr"),
        (pl.col("o_v") * TICK_S).filter(~pl.col("nosp")).sum().alias("t"),
        (pl.col("o_r") * TICK_S).filter(~pl.col("nosp")).sum().alias("tr"),
        pl.col("n").sum(), pl.col("days").max(), pl.col("nosp").all().alias("metro"), (pl.col("term") == 1).any().alias("term"),
        pl.col("from_name").first(), pl.col("to_name").first(), pl.col("line").unique().alias("lines"), pl.col("len_m").median()))
    reg = _registry(data)
    geo = {}
    if reg is not None:
        geo = {k: _thin(c) for k, c in reg.filter(pl.col("link_key").is_in(g["link_key"].implode())).select("link_key", "geometry").iter_rows()}
    def kmh(d, t, ok):
        v = d / t * 3.6 if ok and t > 0 and d > 0 else None
        return round(v, 1) if v is not None and v <= MAP_MAX_KMH else None
    out = []
    for r in g.iter_rows(named=True):
        c = geo.get(r["link_key"])
        if not c or len(c) < 2:
            continue
        per_day = r["n"] / r["days"] if r["days"] else 0
        ok = not r["metro"] and per_day >= MAP_MIN_PASSAGES_DAY
        out.append({"k": r["link_key"], "c": c, "f": title_name(r["from_name"]), "t": title_name(r["to_name"]),
                    "l": sorted(r["lines"], key=by_line), "pd": round(per_day, 1), "len": round(r["len_m"] or 0),
                    "v": kmh(r["d"], r["t"], ok), "vr": kmh(r["dr"], r["tr"], ok),
                    "term": bool(r["term"]), "metro": bool(r["metro"]), "reg": bool(ok and (r["d"] <= 0 or r["dr"] <= 0))})
    return {"query": q.canonical(), "links": out, "status": "done", "n_days": n_days,
            "stamp": data.stamp(date_range(q.first, q.last)), "seconds": round(time.time() - t0, 2)}

"""One API request -> one analysis, from the derived files (no raw data, no GTFS parsing).

Segmentation, counting and the indicators run on request from the placed month files, so any segment
length, phase, grid or stop zone is available without re-deriving.
"""
from __future__ import annotations

import contextlib
import datetime as dt
import io
import time
from dataclasses import dataclass, field

import polars as pl

from . import ms, schedule, stopzones
from .coverage import hour_counts, with_absence
from .data import ALGO_VERSION, Data, date_range, months_of
from .queries import CompareQuery, Query  # noqa: F401 - the analysis' queries, re-exported

SOURCE = "STIB vehicle-distance et punctuality via MobilityTwin.Brussels"
SHOW_HOTSPOTS = False     # the site does not show the package's hotspots (page option show_hotspots)
# page options: the platform page has its own direction control (Sens) and no method section
EMBED = {"dir_control": False, "method": False, "host_controls": True}


class NotFound(LookupError):
    pass


@contextlib.contextmanager
def _timed(tm: dict, name: str):
    t = time.time()
    yield
    tm[name] = round(time.time() - t, 3)


@dataclass
class Run:
    """One analysis, without its placed rows (``tune`` reads them again)."""
    query: Query
    dates: list[dt.date]
    holidays: dict[dt.date, tuple[str, str | None]]   # days left out on purpose: date -> (reason, label)
    net: object
    segments: pl.DataFrame
    analysis: object
    coverage: pl.DataFrame
    passages: pl.DataFrame | None
    timings: dict = field(default_factory=dict)
    zones: pl.DataFrame | None = None
    _hotspots: pl.DataFrame | None = None
    mode: str | None = None

    def hotspots(self) -> pl.DataFrame:
        if self._hotspots is None:
            from microsegments.hotspots import hotspots
            with _timed(self.timings, "hotspots"):
                self._hotspots = hotspots(self.analysis, B=self.query.B, links=self.net.links)
        return self._hotspots


def clamp(data: Data, first: dt.date, last: dt.date) -> list[dt.date]:
    """The dates of [first, last] up to the last ingested date."""
    ing = data.ingested_dates()
    if not ing:
        return []
    last = min(last, ing[-1])
    return date_range(first, last) if first <= last else []


def route_mode(data: Data, line: str, dates) -> str | None:
    from microsegments.report import ROUTE_TYPES
    shas = data.shas_for(dates)
    if not shas:
        return None
    r = data.routes(shas[max(shas)]).filter(pl.col("route_short_name").cast(pl.Utf8) == str(line))
    if not r.height or r["route_type"][0] is None:
        return None
    return ROUTE_TYPES.get(int(r["route_type"][0]))


@dataclass
class Loaded:
    """Derived inputs of one line on a set of dates (one read, shared by the periods of a comparison).
    The placed rows stay as their month files (``blobs``), decoded and prepared one month at a time
    (:func:`placed_months`): a year of placed frames is gigabytes once prepared."""
    dates: list[dt.date]
    blobs: dict[str, bytes]              # month -> placed Parquet file
    uids: set[str]                       # pattern_uids with a placed row
    seen: set[dt.date]                   # dates with a placed row
    passages: pl.DataFrame | None
    coverage: pl.DataFrame
    net: object
    marks: pl.DataFrame                  # exit marks (stibms.stopzones)
    timings: dict = field(default_factory=dict)
    zones: pl.DataFrame | None = None    # (link_key, head_m): stop zone end after each stop (stibms.stopzones)


def not_derived_months(data: Data, dates) -> list[str]:
    have = data.derived_months()
    return [m for m in months_of(dates) if m not in have]


def _decode(blob: bytes, dates, columns: list[str] | None = None) -> pl.DataFrame:
    return pl.read_parquet(io.BytesIO(blob), columns=columns).filter(pl.col("service_date").is_in(dates))


def _scan(blobs: dict[str, bytes], dates) -> tuple[pl.DataFrame, set, set]:
    """Rows per (date, hour), pattern_uids and dates of the placed files, a few columns at a time."""
    counts, uids, seen = [], set(), set()
    for b in blobs.values():
        f = _decode(b, dates, ["service_date", "hour", "pattern_uid"])
        counts.append(hour_counts(f))
        uids |= set(f["pattern_uid"].drop_nulls().unique().to_list())
        seen |= set(f["service_date"].unique().to_list())
    return (pl.concat(counts) if counts else pl.DataFrame(schema={"service_date": pl.Date, "hour": pl.Int8, "len": pl.UInt32}),
            uids, seen)


def load(data: Data, line: str, dates: list[dt.date], label: str = "cette période") -> Loaded:
    t = time.time()
    if not dates:
        raise NotFound(f"aucune donnée ingérée pour {label}")
    blobs = data.placed_blobs(line, dates)
    counts, uids, seen = _scan(blobs, dates)
    if not seen:
        pending = not_derived_months(data, dates)
        if pending:
            raise NotFound(f"données en cours de préparation pour {label} (mois {', '.join(pending)} pas encore calculés)")
        raise NotFound(f"aucun signalement de la ligne {line} pour {label}")
    passages = data.passages(line, dates)
    ing = set(data.ingested_dates())     # gaps in the collection are no hours without the line
    cov = with_absence(data.coverage(dates), counts, [d for d in dates if d in ing])
    net = data.network(line, dates)
    mk = exit_marks(data, line, dates, blobs)
    return Loaded(dates, blobs, uids, seen, passages, cov, net, mk, {"load": round(time.time() - t, 3)},
                  zones=stopzones.heads(mk))


def placed_months(ld: Loaded):
    """The prepared placed rows (``stopzones.prepare``), one month at a time."""
    for b in ld.blobs.values():
        yield stopzones.prepare(_decode(b, ld.dates), ld.net, ld.marks)


def placed(ld: Loaded) -> pl.DataFrame:
    """All prepared placed rows (segment-length tuning)."""
    return pl.concat(list(placed_months(ld)))


MARK_COLS = ["ts", "service_date", "pattern_uid", "link_idx", "link_key", "raw_dist_m", "track_id", "flags"]


def exit_marks(data: Data, line: str, dates, blobs: dict[str, bytes]) -> pl.DataFrame:
    """The line's exit marks over the months of ``dates`` (written with the link cubes), learned from
    the placed files ``blobs`` for the months without a marks file (not yet derived)."""
    from .linkcube import rel
    months = months_of(dates)
    got = data.read_many([rel(m, "marks") for m in months])
    parts = [f.filter(pl.col("line") == line) for f in got if f is not None and f.height]
    missing = [m for m, f in zip(months, got) if f is None and m in blobs]
    if missing:
        sub = pl.concat([_decode(blobs[m], dates, MARK_COLS) for m in missing])
        parts.append(stopzones.marks(sub, line))
    return pl.concat(parts, how="vertical_relaxed").unique() if parts else pl.DataFrame(schema=stopzones.MARKS_SCHEMA)


def _stop_zone(q) -> tuple[float, float]:
    """Grid "stops": the zones follow the feed, APPROACH_M before, the learned exit mark after."""
    return (stopzones.APPROACH_M, stopzones.default_head()) if q.grid == "stops" else q.stopzone


def _segments(ld: Loaded, q) -> pl.DataFrame:
    from microsegments.segments import segment
    uids = sorted(ld.uids | set(ld.net.pattern_days.filter(pl.col("is_main"))["pattern_uid"].unique().to_list()))
    return segment(ld.net, q.seg, q.phase, q.grid, _stop_zone(q), patterns=uids,
                   zones=ld.zones if q.grid == "stops" else None)


def _params(q):
    from microsegments.config import Params
    return Params(segment_m=q.seg, phase_m=q.phase, grid=q.grid, stop_zone=_stop_zone(q), reference_hours=q.ref,
                  tick_from_coverage=True)


def _analyse(data: Data, cube, ld: Loaded, segs, q, dates: list[dt.date]):
    """Analysis of ``dates`` (a run of days); returns it with the days left out on purpose (date -> (reason, label))."""
    from microsegments.config import Quality, Select
    from microsegments.metrics import analyse
    first, last = dates[0], dates[-1]
    hd = schedule.exclusions(data, first, last, q.holidays, q.school, q.reduced, q.exclude)
    present = set(dates)
    select = Select(route=q.line, dates=f"{first}..{last}", weekdays=sorted(set(q.dow)),
                    exclude_dates=[d.isoformat() for d in sorted(hd) if d.weekday() in q.dow and d in present],
                    hours=q.hours)
    cov = ld.coverage
    return analyse(cube, cov if cov.height else None, ld.passages, segs, ld.net.pattern_days, select,
                   _params(q), Quality()), hd


def _count(ld: Loaded, segs, q) -> pl.DataFrame:
    """The cube of ``ld``, counted month by month: counts of different days add up, and the months'
    cubes (each sorted, service_date first) in month order are the sorted cube."""
    from microsegments.aggregate import count
    tick = _params(q).tick_s
    return pl.concat([count(p, segs, tick) for p in placed_months(ld)])


def run(data: Data, q: Query) -> Run:
    dates = clamp(data, q.first, q.last)
    ld = load(data, q.line, dates)
    tm = dict(ld.timings)
    with _timed(tm, "segment"):
        segs = _segments(ld, q)
    with _timed(tm, "count"):
        cube = _count(ld, segs, q)
    with _timed(tm, "analyse"):
        an, hd = _analyse(data, cube, ld, segs, q, dates)
    return Run(query=q, dates=dates, holidays=hd, net=ld.net, segments=segs, analysis=an, coverage=ld.coverage,
               passages=ld.passages, timings=tm, zones=ld.zones, mode=route_mode(data, q.line, dates))


def _relabel(excluded: list[dict], days: dict) -> None:
    """The package says "excluded" for every day the platform left out: give the reason (holiday,
    school_holiday, reduced_service, user_period) and its label instead."""
    for e in excluded:
        d = dt.date.fromisoformat(e["date"])
        if d in days:
            e["reason"], label = days[d]
            if label:
                e["label"] = label


def _relabel_inc(cv: dict, days: dict) -> None:
    if "inc" in cv:
        cv["inc"] = [days[dt.date.fromisoformat(d)][0] if v == "excluded" and dt.date.fromisoformat(d) in days else v
                     for d, v in zip(cv.get("dates", []), cv["inc"])]


def _page(an, net, q, mode: str | None, hs, days: dict, compare: dict | None = None) -> dict:
    """The contract of ``an`` with the site's page options; ``days``: the days left out on purpose."""
    out = ms.to_contract(an, net, hotspots=hs, line=q.line, mode=mode, source=SOURCE,
                         title=f"Ligne {q.line}", names="title")
    if compare is not None:
        out["compare"] = compare
    out["show_hotspots"] = SHOW_HOTSPOTS
    out.update(EMBED)
    # holidays, school holidays, ... are excluded on purpose: say so instead of the generic "excluded"
    _relabel(out.get("period", {}).get("excluded", []), days)
    _relabel_inc(out.get("coverage", {}), days)
    out["tiles"] = True
    out["query"] = q.canonical()
    out["algo"] = ALGO_VERSION
    out["microsegments"] = ms.version()
    return out


def contract(r: Run) -> dict:
    with _timed(r.timings, "contract"):
        hs = r.hotspots() if SHOW_HOTSPOTS else None
        out = _page(r.analysis, r.net, r.query, r.mode, hs, r.holidays)
    return out


# ---------------------------------------------------------------------------------- two periods
def _compare_dates(data: Data, cq: CompareQuery) -> tuple[list[dt.date], list[dt.date]]:
    da, db = clamp(data, *cq.a), clamp(data, *cq.b)
    if not da:
        raise NotFound("période A : aucune donnée ingérée")
    if not db:
        raise NotFound("période B : aucune donnée ingérée")
    return da, db


def _check_seen(data: Data, cq: CompareQuery, ld: Loaded, da: list[dt.date], db: list[dt.date]) -> None:
    seen = ld.seen
    for lab, ds in (("période A", da), ("période B", db)):
        if not seen & set(ds):
            pending = not_derived_months(data, ds)
            raise NotFound(f"{lab} : " + (f"données en cours de préparation (mois {', '.join(pending)} pas encore calculés)"
                                          if pending else f"aucun signalement de la ligne {cq.line}"))


def run_compare(data: Data, cq: CompareQuery) -> dict:
    """The platform twin of ``microsegments.compare.run_compare``: one load over the union of both
    periods (one network, one segmentation, so segment keys agree), one count, one analysis per
    period on its own days, then ``compare``. Returns period B's contract plus the ``compare``
    section (the package page opens in its comparison view)."""
    from microsegments.compare import compare
    from microsegments.hotspots import hotspot_status, hotspots
    from microsegments.report import name_map

    da, db = _compare_dates(data, cq)
    ld = load(data, cq.line, sorted(set(da) | set(db)), "ces périodes")
    tm = dict(ld.timings)
    _check_seen(data, cq, ld, da, db)
    with _timed(tm, "segment_count"):
        segs = _segments(ld, cq)
        cube = _count(ld, segs, cq)
    with _timed(tm, "analyse"):
        an_a, hd_a = _analyse(data, cube, ld, segs, cq, da)
        an_b, hd_b = _analyse(data, cube, ld, segs, cq, db)
    if not an_a.days or not an_b.days:
        which = "A" if not an_a.days else "B"
        raise NotFound(f"période {which} : aucun jour utilisable (couverture insuffisante ou ligne absente)")
    with _timed(tm, "compare"):
        cmp = compare(an_a, an_b, B=cq.B, links=ld.net.links)
    with _timed(tm, "hotspots"):
        hs = hotspots(an_b, B=cq.B, links=ld.net.links) if SHOW_HOTSPOTS and hotspot_status(an_b)[0] else None
    out = _page(an_b, ld.net, cq, route_mode(data, cq.line, db), hs, hd_b,
                compare=cmp.to_contract(name_map("title", ld.net)))
    _relabel(out["compare"]["a"]["excluded"], hd_a)
    _relabel(out["compare"]["b"]["excluded"], hd_b)
    out["timings"] = tm
    return out


# ---------------------------------------------------------------------------------- downloads, tuning
def result_table(r: Run) -> pl.DataFrame:
    """RESULT rows of the analysis with readable stop names, for .csv / .parquet downloads."""
    res = r.analysis.result
    names = r.net.links.select("link_key", "from_name", "to_name").unique("link_key", keep="first")
    out = res.join(names, on="link_key", how="left")
    return out.with_columns(pl.lit(r.query.line).alias("line"))


def tune(data: Data, r: Run, lengths: tuple[float, ...], B: int) -> dict:
    """Segment-length criteria of the run's analysis (``microsegments.tune.segment_length``), on its
    placed rows read again."""
    from microsegments.report import frame_records
    from microsegments.segments import segment
    from microsegments.tune import segment_length
    q, an, net = r.query, r.analysis, r.net
    t = time.time()
    uids = sorted(r.segments["pattern_uid"].unique().to_list())

    def seg_fn(L, phase=0.0, stop_zone=an.params.stop_zone):
        return segment(net, L, phase, q.grid, stop_zone, patterns=uids, geometry=False,
                       zones=r.zones if q.grid == "stops" else None)
    res = segment_length(placed(load(data, q.line, r.dates)), seg_fn, lengths, coverage=r.coverage if r.coverage.height else None,
                         passages=r.passages, pattern_days=net.pattern_days, select=an.select,
                         params=an.params, quality=an.quality, B=B)
    return {"line": q.line, "query": q.canonical(), "lengths": list(lengths), "B": B,
            "recommended": res.recommended, "rule": res.rule, "table": frame_records(res.table),
            "seconds": round(time.time() - t, 1)}

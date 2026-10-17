"""STIB service level per day, read from the GTFS: school-holiday timetable and reduced service.

STIB runs a lighter "vacances" timetable on school-holiday weekdays; the GTFS shows it as fewer
trips (2024-2026 feeds: ~16.5-17.8 k trips on a short-holiday weekday, ~15 k in July-August, against
~18.5-20 k on a school day, ~11.5 k on a public holiday, 8.6-12.3 k on national strike days). Per
date D, the trips scheduled on D by the feed in force on D (the index's last feed on or before D)
are compared with a normal day of the same type (Monday-Friday, Saturday, Sunday; public holidays
left out): the ``REF_Q`` quantile of the trips of that type in the ``REF_DAYS`` days before D, and
the same in the ``REF_DAYS`` days after D (both sides need ``MIN_SIDE`` days, so the first and last
days of the feed history are not classified).
The lower of the two is the reference, so a lasting change of service level (a network change, the
return to school in September) never makes the days on its lighter side look like holidays.
``REF_DAYS`` is long enough for each side of a July-August day to reach school days (June,
September).

    ratio = trips / reference
    weekday, ratio < HOLIDAY_RATIO                      -> "school_holiday" (STIB holiday timetable, bridge days)
    ratio < REDUCED_RATIO, isolated                     -> "reduced_service" (strike, any day type)

A weekday inside a run of ``RUN_DAYS`` or more low weekdays (the summer timetable is ~0.77 of a
school day) is a holiday unless it is also well below the run itself (a strike during a holiday
week). Guard: no weekday is flagged when more than ``MAX_SHARE`` of the weekdays around it would be
(the reference itself is then doubtful).

Public holidays (``stibms.holidays``) are left to their own category. The STIB holiday timetable
follows the French-speaking (FWB) school calendar (the Flemish Easter weeks 2025-04-07..18 run the
normal timetable; the summer timetable starts on the Monday before the FWB holidays), so it is the
one school calendar that changes the service, and it updates itself with each feed.

Trips per date of a feed are cached in the bucket (``gtfs/daytrips/sha=<sha>.parquet``, content
addressed like the feeds, never rewritten; the GTFS ingest writes it for each new feed) and in process.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import logging
import threading
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor

import polars as pl

from . import holidays as hol
from .data import Data, date_range

log = logging.getLogger("stibms.schedule")
REF_DAYS = 90
REF_Q = 0.9
MIN_SIDE = 5
HOLIDAY_RATIO = 0.94
REDUCED_RATIO = 0.75
RUN_DAYS = 5
RUN_DROP = 0.85          # a strike in a holiday run: below 0.85 x the run's median ratio
MAX_SHARE = 0.6
CATEGORIES = ("school_holiday", "reduced_service")
LABELS = {"school_holiday": "horaire de vacances STIB", "reduced_service": "service réduit (grève)"}
_DOW = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
_SCHEMA = {"date": pl.Date, "trips": pl.Int64}
_locks: dict[str, threading.Lock] = {}
_locks_lk = threading.Lock()


def _memo(data: Data, key: str, ttl: float):
    """A value another thread computed while this one waited for the lock (Data._cached memo)."""
    import time
    hit = data._memo.get(key)
    return hit[1] if hit is not None and time.time() - hit[0] < ttl else None


def _lock(key: str) -> threading.Lock:
    """One computation per key at a time (concurrent cold requests share it instead of repeating it)."""
    with _locks_lk:
        return _locks.setdefault(key, threading.Lock())


def feed_trips(cal: pl.DataFrame, cal_dates: pl.DataFrame, trips: pl.DataFrame) -> pl.DataFrame:
    """(date, trips): trips scheduled per date by one feed (calendar + calendar_dates)."""
    rows = []
    for r in cal.iter_rows(named=True):
        if r["start_date"] is None or r["end_date"] is None:
            continue
        for d in date_range(r["start_date"], r["end_date"]):
            if r[_DOW[d.weekday()]]:
                rows.append((r["service_id"], d))
    on = pl.DataFrame(rows, schema={"service_id": pl.Utf8, "date": pl.Date}, orient="row")
    if cal_dates.height:
        cd = cal_dates.select(pl.col("service_id").cast(pl.Utf8), pl.col("date").cast(pl.Date), "exception_type")
        on = pl.concat([on, cd.filter(pl.col("exception_type") == 1).select("service_id", "date")]).unique()
        on = on.join(cd.filter(pl.col("exception_type") == 2).select("service_id", "date"), on=["service_id", "date"], how="anti")
    n = trips.select(pl.col("service_id").cast(pl.Utf8)).group_by("service_id").len()
    return (on.join(n, on="service_id").group_by("date").agg(pl.col("len").sum().cast(pl.Int64).alias("trips"))
            .sort("date"))


def _rel(sha: str) -> str:
    return f"gtfs/daytrips/sha={sha}.parquet"


def write_feed_trips(store, sha: str) -> pl.DataFrame:
    """Trips per date of feed ``sha`` from its tables, written to ``gtfs/daytrips`` (the GTFS ingest
    calls it for each feed it stores; ``sha_trips`` for older feeds)."""
    base = f"gtfs/feeds/{sha}"
    try:
        cd = store.read_parquet(f"{base}/calendar_dates.parquet")
    except FileNotFoundError:
        cd = pl.DataFrame(schema={"service_id": pl.Utf8, "date": pl.Date, "exception_type": pl.Int8})
    try:
        out = feed_trips(store.read_parquet(f"{base}/calendar.parquet"), cd,
                         store.read_parquet(f"{base}/trips.parquet", columns=["service_id"]))
    except FileNotFoundError:
        return pl.DataFrame(schema=_SCHEMA)
    try:
        store.write_parquet(_rel(sha), out)
    except Exception:  # noqa: BLE001 - a cache, recomputed next time
        log.exception("daytrips cache write failed for %s", sha[:12])
    return out


def sha_trips(data: Data, sha: str) -> pl.DataFrame:
    def f():
        with _lock("daytrips:" + sha):
            hit = _memo(data, f"daytrips:{sha}", 1e9)
            if hit is not None:
                return hit
            got = data.read_optional(_rel(sha))
            return got if got is not None else write_feed_trips(data.store, sha)
    return data._cached(f"daytrips:{sha}", f, ttl=1e9)


def day_trips(data: Data, first: dt.date, last: dt.date) -> dict[dt.date, int]:
    """date -> trips scheduled that day by the feed in force (dates no feed covers are left out)."""
    idx = data.gtfs_index()
    if not idx.height:
        return {}
    days, shas = idx["service_date"].to_list(), idx["sha"].to_list()
    order = list(dict.fromkeys(shas))
    pos = {s: i for i, s in enumerate(order)}
    want = date_range(first, last)
    own = {d: shas[max(0, bisect_right(days, d) - 1)] for d in want}
    # the feed in force, then the next and previous feeds (a feed's calendar may not reach a date)
    cands = {d: [order[j] for j in (pos[s], pos[s] + 1, pos[s] + 2, pos[s] - 1) if 0 <= j < len(order)]
             for d, s in own.items()}
    need = sorted({s for c in cands.values() for s in c})
    with ThreadPoolExecutor(max_workers=min(12, max(1, len(need)))) as ex:
        tables = dict(zip(need, ex.map(lambda s: sha_trips(data, s), need)))
    per = {s: dict(zip(t["date"].to_list(), t["trips"].to_list())) for s, t in tables.items()}
    out = {}
    for d, c in cands.items():
        v = next((per[s][d] for s in c if d in per[s]), None)
        if v is not None:
            out[d] = int(v)
    return out


def _q(vals: list[int]) -> int:
    s = sorted(vals)
    return s[min(len(s) - 1, int(REF_Q * (len(s) - 1) + 0.5))]


def ratios(trips: dict[dt.date, int]) -> dict[dt.date, float]:
    """date -> trips / reference (module doc); public holidays and dates without a reference left out."""
    if not trips:
        return {}
    ph = set(hol.between(min(trips), max(trips)))
    kind = lambda d: 0 if d.weekday() < 5 else d.weekday()          # noqa: E731 - weekdays, Saturday, Sunday
    by: dict[int, tuple[list, list]] = {}
    for d in sorted(trips):
        if d not in ph:
            ds, vs = by.setdefault(kind(d), ([], []))
            ds.append(d)
            vs.append(trips[d])
    span = dt.timedelta(days=REF_DAYS)
    out = {}
    for d, t in trips.items():
        if d in ph:
            continue
        ds, vs = by[kind(d)]
        i0, i, i1 = bisect_left(ds, d - span), bisect_left(ds, d), bisect_right(ds, d + span)
        sides = (vs[i0:i + 1], vs[i:i1])
        if min(len(x) for x in sides) < MIN_SIDE:       # both sides needed (the edges of the feed history)
            continue
        refs = [_q(x) for x in sides]
        if min(refs):
            out[d] = t / min(refs)
    # guard: a reference under which most weekdays around would be low is not a normal day
    wk = [d for d in sorted(out) if d.weekday() < 5]
    keep = {}
    for d in wk:
        ref = trips[d] / out[d]
        a, b = bisect_left(wk, d - span), bisect_right(wk, d + span)
        keep[d] = sum(trips[x] < HOLIDAY_RATIO * ref for x in wk[a:b]) <= MAX_SHARE * (b - a)
    return {d: r for d, r in out.items() if d.weekday() >= 5 or keep[d]}


def _runs(r: dict[dt.date, float]) -> dict[dt.date, list[float]]:
    """weekday -> ratios of the run of consecutive low weekdays it belongs to (public holidays continue a run)."""
    out: dict[dt.date, list[float]] = {}
    cur: list[dt.date] = []
    if not r:
        return out
    ph = set(hol.between(min(r), max(r)))

    def close():
        vals = [r[x] for x in cur if x in r]
        for x in cur:
            if x in r:
                out[x] = vals
        cur.clear()
    for d in date_range(min(r), max(r)):
        if d.weekday() >= 5:
            continue
        if d in ph or (d in r and r[d] < HOLIDAY_RATIO):
            cur.append(d)
        else:
            close()
    close()
    return out


def classify(trips: dict[dt.date, int], first: dt.date, last: dt.date) -> dict[dt.date, str]:
    """date in [first, last] -> "school_holiday" | "reduced_service" (normal days are left out)."""
    r = ratios(trips)
    runs = _runs({d: v for d, v in r.items() if d.weekday() < 5})
    out = {}
    for d in date_range(first, last):
        v = r.get(d)
        if v is None:
            continue
        if d.weekday() >= 5:
            if v < REDUCED_RATIO:
                out[d] = "reduced_service"
            continue
        run = runs.get(d, [v])
        if v < REDUCED_RATIO and (len(run) < RUN_DAYS or v < RUN_DROP * sorted(run)[len(run) // 2]):
            out[d] = "reduced_service"
        elif v < HOLIDAY_RATIO:
            out[d] = "school_holiday"
    return out


def categories(data: Data, first: dt.date, last: dt.date) -> dict[dt.date, str]:
    """The service categories of [first, last] (see the module doc), cached in process."""
    key = f"schedule:{first}:{last}"

    def f():
        with _lock(key):
            hit = _memo(data, key, 600)
            if hit is not None:
                return hit
            pad = dt.timedelta(days=REF_DAYS + 14)
            return classify(day_trips(data, first - pad, last + pad), first, last)
    return data._cached(key, f, ttl=600)


def stamp(data: Data, *ranges: tuple[dt.date, dt.date]) -> str:
    """Changes when the categorised dates of the ranges change (result cache keys)."""
    items = sorted(x for a, b in ranges for x in categories(data, a, b).items())
    return hashlib.sha256(repr(items).encode()).hexdigest()[:12]


def newest_feed_date(data: Data) -> dt.date | None:
    idx = data.gtfs_index()
    return idx["service_date"].max() if idx.height else None


# ---------------------------------------------------------------------------------- day exclusions
CHOICES = ("exclude", "include")


def norm_periods(periods, first: dt.date, last: dt.date) -> tuple[tuple[dt.date, dt.date], ...]:
    """User periods clipped to [first, last], sorted, overlapping or adjacent ones merged (the same
    days give the same cache key)."""
    out: list[list[dt.date]] = []
    for a, b in sorted((max(a, first), min(b, last)) for a, b in periods):
        if a > b:
            continue
        if out and a <= out[-1][1] + dt.timedelta(days=1):
            out[-1][1] = max(out[-1][1], b)
        else:
            out.append([a, b])
    return tuple((a, b) for a, b in out)


def exclusions(data: Data | None, first: dt.date, last: dt.date, holidays: str = "exclude",
               school: str = "include", reduced: str = "include", periods=()) -> dict[dt.date, tuple[str, str | None]]:
    """date -> (reason, label) of the days of [first, last] left out on purpose. One reason per day,
    the most specific first: public holiday, reduced service, school holiday, then the user's periods."""
    out: dict[dt.date, tuple[str, str | None]] = {}
    for a, b in periods:
        for d in date_range(max(a, first), min(b, last)):
            out[d] = ("user_period", None)
    if data is not None and "exclude" in (school, reduced):
        for d, c in categories(data, first, last).items():
            if (c == "school_holiday" and school == "exclude") or (c == "reduced_service" and reduced == "exclude"):
                out[d] = (c, LABELS[c])
    if holidays == "exclude":
        out.update({d: ("holiday", n) for d, n in hol.between(first, last).items()})
    return out

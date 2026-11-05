"""Regularity: the time between two vehicles of a line at a stop (headway), by the timetable and for real.

    python -m stibms.derive --from 2024-04 --to 2026-10 --regularity-only

Read from the punctuality feed (``raw/punctuality``): each stop time of a trip of the GTFS timetable, with
its real time and its delay, so the timetable time is the real time minus the delay. Per line, direction
and stop, on each service day:

- timetable headways: the trips in the order of their timetable times, the gap between two in a row;
- real headways: the same trips in the order they really came, the gap between two in a row.

Both sides take the same trips, those the feed holds. A trip cancelled, or run but not tracked, is missing
from both, so it opens a gap in both and does not read as irregularity: regularity measures how evenly the
trips that ran were spaced; whether they ran is the km run against the km planned. Only trips of the
timetable (``trip_schedule_relationship`` 0) count, at the times the feed observed (not inferred), and not at a
trip's last stop (nobody boards; a terminus arrival waits for its departure). A stop that saw under
``MIN_STOP_SHARE`` of the trips of its line and direction that day (the median stop) is left out that day: its
headways are those of a subset of the trips (some termini). A timetable gap over ``BREAK_S`` (night, a break in the
service) ends a block: no headway spans it, on either side.

The headline figure is the average wait of a passenger arriving at random, ``sum h^2 / (2 sum h)``, by the
real headways against the timetable ones; the difference is the **excess wait** (minutes), what the
irregularity costs. Sums add up over stops, hours, days and lines, so any group's wait comes from the same
sums (each stop and hour counts as much as it lasts). Each real headway is also compared with the trip's
timetable headway: ``bunch`` under ``BUNCH`` times it (two vehicles together), ``gap`` over ``GAP`` times it.

A headway falls in the hour (of its service day, 24 and more after midnight) where it ends: real headways
by the real time, timetable ones by the timetable time. The feed's file of a day stops at midnight, so the
hours after midnight are partial.

``derived/v{ALGO}/linkcube/month=M/``:

- ``headways.parquet``: (line, direction_id, stop_id, service_date, hour) ``n``, ``r1``, ``r2`` (real
  headways ending in that hour: count, sum of seconds, sum of squares), ``m``, ``s1``, ``s2`` (the same,
  timetable), ``bunch``, ``gap``, ``nc`` (real headways with a timetable headway to compare with);
- ``reglines.parquet``: the same summed per (line, service_date, hour), for the network page's evolution;
- ``regstops.parquet``: (line, direction_id, stop_id) ``off_s``, the stop's usual time after the trip's first stop
  by the timetable (mean over the days of the daily median), ``days``: the line page orders its stops by it.
"""
from __future__ import annotations

import datetime as dt
import logging
import time

import polars as pl

from .data import Data, derived

log = logging.getLogger("stibms.regularity")

BREAK_S = 3600       # a timetable gap over an hour: a break in the service, no headway across it
BUNCH = 0.5          # a real headway under half its timetable headway: bunched
GAP = 1.5            # over one and a half times: a gap
MIN_STOP_SHARE = 0.8 # a stop's trips that day against the median stop of its line and direction
TZ = "Europe/Brussels"
SUMS = ("n", "r1", "r2", "m", "s1", "s2", "nc", "bunch", "gap")
TYPES = {"r1": pl.UInt32, "s1": pl.UInt32, "r2": pl.UInt64, "s2": pl.UInt64,       # seconds within a stop-hour
         **{c: pl.UInt16 for c in ("n", "m", "nc", "bunch", "gap")}}
KEY = ["line", "direction_id", "stop_id", "service_date", "hour"]
COLUMNS = ["trip_id", "start_date", "route_id", "direction_id", "trip_schedule_relationship", "stop_sequence", "stop_id",
           "arrival_time", "departure_time", "arrival_delay", "departure_delay", "observed"]


def rel(month: str, name: str) -> str:
    return derived(f"linkcube/month={month}/{name}.parquet")


def stop_times(pu: pl.DataFrame) -> pl.DataFrame:
    """(line, direction_id, stop_id, service_date, trip_id, t, s): real and timetable times (epoch seconds) of
    the timetable's trips, arrival or else departure."""
    arr = pl.col("arrival_time").is_not_null() & pl.col("arrival_delay").is_not_null()
    t = pl.when(arr).then(pl.col("arrival_time")).otherwise(pl.col("departure_time")).dt.epoch("s")
    delay = pl.when(arr).then(pl.col("arrival_delay")).otherwise(pl.col("departure_delay")).cast(pl.Int64)
    keep = pl.col("trip_schedule_relationship").fill_null(0) == 0 if "trip_schedule_relationship" in pu.columns else pl.lit(True)
    if "observed" in pu.columns:
        keep = keep & pl.col("observed").fill_null(False)
    # a trip's last stop: nobody boards there, and arrivals at a terminus wait for their departure. Found among all
    # its stop times: when the terminus is not observed, the stop before it is not the last
    last = pl.col("stop_sequence") == pl.col("stop_sequence").max().over("route_id", "trip_id", "start_date")
    return (pu.filter(keep.fill_null(False) | last).filter(~last)
            .select(pl.col("route_id").cast(pl.Utf8).alias("line"), pl.col("direction_id").cast(pl.Int8),
                    pl.col("stop_id").cast(pl.Utf8), pl.col("start_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d").alias("service_date"),
                    pl.col("trip_id").cast(pl.Utf8), pl.col("stop_sequence"), t.alias("t"), (t - delay).alias("s"))
            .drop_nulls(["line", "stop_id", "service_date", "t", "s"])
            .unique(["line", "trip_id", "stop_sequence", "service_date"])
            .drop("stop_sequence"))


def covered(st: pl.DataFrame) -> pl.DataFrame:
    """The stop times of the stops that saw at least ``MIN_STOP_SHARE`` of their line and direction's trips that day
    (the median over its stops): a stop where the feed holds a subset of the trips (some termini) has headways
    of that subset only, not comparable."""
    g = ["line", "direction_id", "service_date"]
    c = st.group_by(*g, "stop_id").agg(pl.col("trip_id").n_unique().alias("_k"))
    c = c.with_columns(pl.col("_k").median().over(g).alias("_med")).filter(pl.col("_k") >= MIN_STOP_SHARE * pl.col("_med"))
    return st.join(c.select(*g, "stop_id"), on=[*g, "stop_id"], how="semi")


def headways(st: pl.DataFrame) -> pl.DataFrame:
    """The per (line, direction, stop, service day, hour) sums of the module docstring from ``stop_times``."""
    g = ["line", "direction_id", "stop_id", "service_date"]
    midnight = pl.col("service_date").cast(pl.Datetime("us")).dt.replace_time_zone(TZ, ambiguous="earliest").dt.epoch("s")
    x = (st.sort(*g, "s")
         .with_columns(pl.col("s").diff().over(g).alias("hs"))
         .with_columns(((pl.col("hs").is_null()) | (pl.col("hs") > BREAK_S)).cum_sum().over(g).alias("blk"))
         .with_columns(pl.when(pl.col("hs") > BREAK_S).then(None).otherwise(pl.col("hs")).alias("hs")))
    b = g + ["blk"]
    x = (x.sort(*b, "t").with_columns(pl.col("t").diff().over(b).alias("h"))
         .with_columns(((pl.col("t") - midnight) // 3600).cast(pl.Int8).alias("hour_r"),
                       ((pl.col("s") - midnight) // 3600).cast(pl.Int8).alias("hour_s")))
    f = lambda c: pl.col(c).cast(pl.Float64)  # noqa: E731
    cmp = pl.col("h").is_not_null() & (pl.col("hs") > 0)
    real = (x.filter(pl.col("h").is_not_null()).group_by(*g, pl.col("hour_r").alias("hour")).agg(
        pl.len().alias("n"), f("h").sum().alias("r1"), (f("h") ** 2).sum().alias("r2"),
        cmp.sum().alias("nc"), (cmp & (pl.col("h") < BUNCH * pl.col("hs"))).sum().alias("bunch"),
        (cmp & (pl.col("h") > GAP * pl.col("hs"))).sum().alias("gap")))
    plan = (x.filter(pl.col("hs").is_not_null()).group_by(*g, pl.col("hour_s").alias("hour")).agg(
        pl.len().alias("m"), f("hs").sum().alias("s1"), (f("hs") ** 2).sum().alias("s2")))
    out = real.join(plan, on=KEY, how="full", coalesce=True)
    return out.select(*KEY, *[pl.col(c).fill_null(0).cast(TYPES[c]) for c in SUMS])


def sums() -> list[pl.Expr]:
    """The ``SUMS`` summed, as floats (a month of seconds overflows the stored 32-bit integers)."""
    return [pl.col(c).cast(pl.Float64).sum().alias(c) for c in SUMS]


def offsets(st: pl.DataFrame) -> pl.DataFrame:
    """(line, direction_id, stop_id, off_s): the day's median timetable time after the trip's first stop."""
    off = pl.col("s") - pl.col("s").min().over("line", "trip_id", "service_date")
    return st.with_columns(off.alias("off_s")).group_by("line", "direction_id", "stop_id").agg(pl.col("off_s").median())


def per_line(hw: pl.DataFrame) -> pl.DataFrame:
    return hw.group_by("line", "service_date", "hour").agg(sums()).sort("line", "service_date", "hour")


def waits(t: dict) -> dict:
    """Minutes and shares from summed ``SUMS`` (None where nothing to compute on)."""
    w = lambda a, b: round(b / (2 * a) / 60, 2) if a > 0 else None  # noqa: E731
    wr, ws = w(t["r1"], t["r2"]), w(t["s1"], t["s2"])
    share = lambda k: round(100 * t[k] / t["nc"], 1) if t["nc"] > 0 else None  # noqa: E731
    return {"wr": wr, "ws": ws, "ew": round(wr - ws, 2) if wr is not None and ws is not None else None,
            "hr": round(t["r1"] / t["n"] / 60, 1) if t["n"] else None, "hs": round(t["s1"] / t["m"] / 60, 1) if t["m"] else None,
            "bunch": share("bunch"), "gap": share("gap"),
            "ok": round(100 - share("bunch") - share("gap"), 1) if t["nc"] > 0 else None, "nh": int(t["n"])}


def write_month(data: Data, month: str, dates: list[dt.date] | None = None) -> dict:
    """Compute the month's tables from its punctuality files, one day at a time."""
    t0 = time.time()
    dates = data.month_ingested(month) if dates is None else dates
    parts, offs = [], []
    for d in dates:
        pu = data.read_optional(f"raw/punctuality/date={d}/p.parquet")
        if pu is None or not pu.height:
            continue
        st = covered(stop_times(pu.select([c for c in COLUMNS if c in pu.columns])))
        parts.append(headways(st))
        offs.append(offsets(st))
    hw = pl.concat(parts) if parts else pl.DataFrame(schema={**dict(zip(KEY, (pl.Utf8, pl.Int8, pl.Utf8, pl.Date, pl.Int8))), **TYPES})
    hw = hw.sort("line", "direction_id", "stop_id", "service_date", "hour")
    data.store.write_parquet(rel(month, "headways"), hw)
    data.store.write_parquet(rel(month, "reglines"), per_line(hw))
    so = (pl.concat(offs).group_by("line", "direction_id", "stop_id").agg(pl.col("off_s").mean().round(0), pl.len().alias("days"))
          if offs else pl.DataFrame(schema={"line": pl.Utf8, "direction_id": pl.Int8, "stop_id": pl.Utf8, "off_s": pl.Float64, "days": pl.UInt32}))
    data.store.write_parquet(rel(month, "regstops"), so.sort("line", "direction_id", "off_s"))
    log.info("%s regularity: %d days, %d rows in %.1f s", month, len(parts), hw.height, time.time() - t0)
    return {"month": month, "days": len(parts), "rows": hw.height, "seconds": round(time.time() - t0, 1)}

"""Link-level cube for the network ranking: one small file per month, all lines.

    python -m stibms.derive --from 2024-04 --to 2026-09 --linkcube-only     # (re)build from placed files

``derived/v{ALGO}/linkcube/month=M/``:

- ``cube.parquet``: per (line, service_date, link_key) the observations and passages of the line's
  vehicles in four hour bands (day 6-21 h, evening 20-23 h = the reference, am 7-10 h, pm 16-19 h),
  observations split by zone (``_r`` running = between stops, ``_s`` stop zone = 30 m before to 60 m
  after a stop, the page's default stop zone) and, for day and evening, the observations that belong
  to a long stand (``l_``: the vehicle did not move for >= ``LONG_STAND_S``). Only usable hours are
  summed (feed coverage >= 80 %, not frozen, the line present in the feed) and only usable days are
  written (at most a quarter of the 6-21 h hours dropped), as in the line analysis.
- ``days.parquet``: (line, service_date) usable days, the denominator of "per average day".
- ``links.parquet``: (line, link_key, from_name, to_name, len_m, term) with ``term`` 1 when the link is
  the first or last link of one of the line's patterns that month, 2 when one of its stops ends one of
  them (a short-working or partial terminus), else 0.

The ranking of any window then only sums these rows (``stibms.ranking``): no analysis runs in the API.

Long stands. A STIB vehicle that stands still reports exactly the same distance poll after poll. A
stand is a run of a track's fixes that move less than ``STAND_MOVE_M`` with no gap above
``STAND_GAP_S``. Signal cycles in Brussels are at most ~120 s, so a stand of 3 minutes or more is a
vehicle held on purpose (timing point, layover of a short-working trip parked past its terminus stop),
not traffic. In September 2026 such stands made 70-95 % of the excess of the stretches that topped the
ranking without being traffic hotspots (UZ Brussel -> UZ-Pediatrie on 9, Madou -> Presse on 29,
Duchesse de Brabant -> Triangle on 89, Gazelle -> Homborch on 43), and under 30 % elsewhere.
"""
from __future__ import annotations

import datetime as dt
import logging
import time

import polars as pl

from .data import Data, derived

log = logging.getLogger("stibms.linkcube")

STAND_MOVE_M = 8.0
STAND_GAP_S = 90
LONG_STAND_S = 180
STOP_ZONE = (30.0, 60.0)       # metres before / after a stop
BANDS = {"day": (6, 21), "eve": (20, 23), "am": (7, 10), "pm": (16, 19)}
LONG_BANDS = ("day", "eve")
DAY_HOURS = (6, 21)


def rel(month: str, name: str) -> str:
    return derived(f"linkcube/month={month}/{name}.parquet")


def good_hours(cov: pl.DataFrame, placed: pl.DataFrame, dates) -> tuple[pl.DataFrame, list[dt.date]]:
    """(service_date, hour) usable for the line, and the usable days (same rules as the analysis)."""
    from microsegments.config import Quality
    from .analysis import LINE_ABSENT, with_line_absence
    q = Quality()
    if not cov.height:
        return pl.DataFrame(schema={"service_date": pl.Date, "hour": pl.Int8}), []
    c = with_line_absence(cov, placed, dates).with_columns(pl.col("hour").cast(pl.Int8))
    c = c.with_columns(((pl.col("coverage") >= q.min_hour_coverage) & ((pl.col("flags") & (3 | LINE_ABSENT)) == 0))
                       .alias("ok"))
    a, b = DAY_HOURS
    bad = (c.filter(pl.col("hour").is_between(a, b - 1)).group_by("service_date")
           .agg((~pl.col("ok")).sum().alias("bad")))
    days = bad.filter(pl.col("bad") / (b - a) <= 1 - q.min_day_coverage + 1e-9)["service_date"].sort().to_list()
    ok = c.filter(pl.col("ok") & pl.col("service_date").is_in(days)).select("service_date", "hour")
    return ok, days


def stands(placed: pl.DataFrame) -> pl.DataFrame:
    """placed + ``long`` (the fix belongs to a stand of >= LONG_STAND_S)."""
    p = placed.sort("track_id", "ts")
    new = ((pl.col("track_id") != pl.col("track_id").shift())
           | ((pl.col("s_m") - pl.col("s_m").shift()).abs() > STAND_MOVE_M)
           | ((pl.col("ts") - pl.col("ts").shift()).dt.total_seconds() > STAND_GAP_S)).fill_null(True)
    p = p.with_columns(new.cum_sum().alias("_sid"))
    dur = (pl.col("ts").max() - pl.col("ts").min()).dt.total_seconds().over("_sid")
    return p.with_columns((dur >= LONG_STAND_S).alias("long")).drop("_sid")


def line_cube(line: str, placed: pl.DataFrame, passages: pl.DataFrame | None, net, cov: pl.DataFrame,
              dates) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """(cube, days, links) of one line for one month (see the module docstring)."""
    ok, days = good_hours(cov, placed, dates)
    lit = pl.lit(line, pl.Utf8).alias("line")
    days_df = pl.DataFrame({"service_date": days}, schema={"service_date": pl.Date}).with_columns(lit)
    lk = net.links
    if not placed.height or not lk.height or not days:
        return pl.DataFrame(), days_df, pl.DataFrame()
    p = placed.filter(pl.col("count").fill_null(True)) if "count" in placed.columns else placed
    p = stands(p)
    p = p.join(lk.select("pattern_uid", "link_idx", pl.col("len_m").alias("_len")), on=["pattern_uid", "link_idx"],
               how="left")
    before, after = STOP_ZONE
    p = p.with_columns(((pl.col("pos_m") <= after) | (pl.col("pos_m") > pl.col("_len") - before))
                       .fill_null(False).alias("stop"), pl.col("hour").cast(pl.Int8))
    p = p.join(ok, on=["service_date", "hour"], how="inner")
    aggs = []
    for b, (h0, h1) in BANDS.items():
        inb = pl.col("hour").is_between(h0, h1 - 1)
        aggs += [(inb & ~pl.col("stop")).sum().cast(pl.UInt32).alias(f"o_{b}_r"),
                 (inb & pl.col("stop")).sum().cast(pl.UInt32).alias(f"o_{b}_s")]
        if b in LONG_BANDS:
            aggs += [(inb & ~pl.col("stop") & pl.col("long")).sum().cast(pl.UInt32).alias(f"l_{b}_r"),
                     (inb & pl.col("stop") & pl.col("long")).sum().cast(pl.UInt32).alias(f"l_{b}_s")]
    o = p.group_by("service_date", "link_key").agg(aggs)
    if passages is not None and passages.height:
        ps = passages.with_columns(pl.col("hour").cast(pl.Int8)).join(ok, on=["service_date", "hour"], how="inner")
        pas = ps.group_by("service_date", "link_key").agg(
            [pl.col("n").filter(pl.col("hour").is_between(h0, h1 - 1)).sum().cast(pl.Float32).alias(f"p_{b}")
             for b, (h0, h1) in BANDS.items()])
        o = o.join(pas, on=["service_date", "link_key"], how="full", coalesce=True)
    else:
        o = o.with_columns([pl.lit(0.0, pl.Float32).alias(f"p_{b}") for b in BANDS])
    num = [c for c in o.columns if c[:2] in ("o_", "l_", "p_")]
    o = o.with_columns([pl.col(c).fill_null(0) for c in num]).with_columns(lit)
    # links: names, length, terminus role (first/last link of a pattern; a stop that ends a pattern)
    pats = net.patterns.select("pattern_uid", "stop_ids")
    ends = set(pats["stop_ids"].list.first().to_list()) | set(pats["stop_ids"].list.last().to_list())
    first_last = (lk.with_columns(pl.col("link_idx").max().over("pattern_uid").alias("_m"))
                  .filter((pl.col("link_idx") == 0) | (pl.col("link_idx") == pl.col("_m")))["link_key"].unique())
    links = (lk.group_by("link_key").agg(pl.col("from_stop").first(), pl.col("to_stop").first(),
                                         pl.col("from_name").first(), pl.col("to_name").first(),
                                         pl.col("len_m").median())
             .with_columns(pl.when(pl.col("link_key").is_in(first_last.implode())).then(1)
                           .when(pl.col("from_stop").is_in(list(ends)) | pl.col("to_stop").is_in(list(ends))).then(2)
                           .otherwise(0).cast(pl.Int8).alias("term"), lit)
             .drop("from_stop", "to_stop"))
    return o, days_df, links


def write_month(data: Data, month: str, lines: list[str] | None = None) -> dict:
    """Build ``linkcube/month=M`` from the month's placed + passages files (all lines)."""
    from .data import month_dates
    t0 = time.time()
    ing = set(data.ingested_dates())
    dates = [d for d in month_dates(month) if d in ing]
    have = data.line_months()
    todo = [ln for ln, ms_ in have.items() if month in ms_ and (lines is None or ln in lines)]
    cov = data.coverage(dates)
    cubes, dayss, linkss, failed = [], [], [], {}
    for ln in sorted(todo, key=lambda x: (len(x), x)):
        try:
            placed = data.placed(ln, dates)
            if placed is None or not placed.height:
                continue
            c, d, lk = line_cube(ln, placed, data.passages(ln, dates), data.network(ln, dates), cov, dates)
            if c.height:
                cubes.append(c)
                linkss.append(lk)
            dayss.append(d)
        except Exception as e:  # noqa: BLE001 - one broken line must not block the month
            log.exception("linkcube %s line %s failed", month, ln)
            failed[ln] = repr(e)[:200]
    if lines is not None:   # partial rebuild: keep the other lines' rows
        for name, acc in (("cube", cubes), ("days", dayss), ("links", linkss)):
            old = data.read_optional(rel(month, name))
            if old is not None and old.height:
                acc.append(old.filter(~pl.col("line").is_in(lines)))
    for name, acc in (("cube", cubes), ("days", dayss), ("links", linkss)):
        df = pl.concat(acc, how="diagonal_relaxed") if acc else pl.DataFrame()
        data.store.write_parquet(rel(month, name), df)
    n = sum(c.height for c in cubes)
    log.info("linkcube %s: %d lines, %d rows, %.1f s", month, len(todo), n, time.time() - t0)
    return {"month": month, "lines": len(todo), "rows": n, "failed": failed, "seconds": round(time.time() - t0, 1)}

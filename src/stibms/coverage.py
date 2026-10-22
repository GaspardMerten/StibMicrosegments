"""Usable hours and days of a line: feed coverage plus the hours the line is missing from the feed.

Shared by the line analysis (``stibms.analysis``), the coverage endpoint and the link cube.
"""
from __future__ import annotations

import datetime as dt

import polars as pl

from . import holidays as hol
from . import schedule
from .data import Data, date_range, month_key

LINE_ABSENT = 4          # microsegments.schema.Flag.LINE_ABSENT
FEED_BAD = 3             # coverage flags of the feed itself (no poll, frozen)
TYPICAL_MIN_OBS = 20     # an hour "normally has" the line when its median count is at least this
DQ_HOURS = (6, 20)       # hours a day is judged on (inclusive), as the package's Quality


def line_absent(placed: pl.DataFrame, dates) -> pl.DataFrame:
    """(service_date, hour) where the feed was polled but carried no row of the line while that
    hour normally has it (median over the period >= TYPICAL_MIN_OBS). Partial source days exist
    (2025-03-31: 15 lines of ~80 in the feed); feed-level coverage cannot see them. ``dates``: the
    ingested dates only (a date without data would pull the median down)."""
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


def _per_day(cov: pl.DataFrame, min_hour: float) -> dict[dt.date, tuple[float, int, int, int]]:
    """date -> (mean coverage, hours bad in the feed, hours without the line, hours unusable) over DQ_HOURS."""
    if not cov.height:
        return {}
    low = (pl.col("coverage") < min_hour) | ((pl.col("flags") & FEED_BAD) != 0)
    absent = (pl.col("flags") & LINE_ABSENT) != 0
    g = (cov.filter(pl.col("hour").is_between(*DQ_HOURS)).group_by("service_date")
         .agg(pl.col("coverage").mean().alias("c"), low.sum().alias("bad"), absent.sum().alias("absent"),
              (low | absent).sum().alias("unusable")))
    return {r[0]: (float(r[1]), int(r[2]), int(r[3]), int(r[4])) for r in g.iter_rows()}


def _status(d: dt.date, ingested: set, derived: dict, per: tuple | None, obs: int, max_bad: float) -> str:
    if d not in ingested:
        return "not_ingested"
    if month_key(d) not in derived:
        return "not_derived"
    if per is None:
        return "no_data"
    n = DQ_HOURS[1] - DQ_HOURS[0] + 1
    _, bad, _, unusable = per
    if bad / n > max_bad:
        return "low_coverage"
    if not obs or unusable / n > max_bad:
        return "line_absent"
    return "ok"


def coverage_days(data: Data, line: str, first: dt.date, last: dt.date) -> list[dict]:
    """Per date: feed coverage over 6-21 h, hours below 80 %, the line's counted observations,
    holiday name, STIB service category (``schedule``: school_holiday | reduced_service | null).
    ``status``: ok | low_coverage | no_data | line_absent | not_derived (ingested, month not derived
    yet) | not_ingested."""
    from microsegments.config import Quality
    qual = Quality()
    ing = set(data.ingested_dates())
    dates = date_range(first, last)
    placed = data.placed(line, dates)
    cov = data.coverage(dates)
    if placed is not None:
        cov = with_line_absence(cov, placed, [d for d in dates if d in ing])
    per = _per_day(cov, qual.min_hour_coverage)
    obs = dict(placed.group_by("service_date").len().iter_rows()) if placed is not None else {}
    hd = hol.between(first, last)
    try:
        cats = schedule.categories(data, first, last)
    except Exception:  # noqa: BLE001 - the coverage answers without the categories
        cats = {}
    have = data.derived_months()
    max_bad = 1 - qual.min_day_coverage + 1e-9
    out = []
    for d in dates:
        c, bad, absent, _ = per.get(d, (None, None, None, None))
        out.append({"date": d.isoformat(), "dow": d.weekday(), "coverage": None if c is None else round(c, 3),
                    "hours_bad": bad, "hours_line_absent": absent, "obs": int(obs.get(d, 0)), "holiday": hd.get(d),
                    "service": cats.get(d), "status": _status(d, ing, have, per.get(d), obs.get(d), max_bad)})
    return out

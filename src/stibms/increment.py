"""Incremental derive of a month: which service days to place, and how their rows join the month.

The nightly derive places only the month's new service days (not in the month's derive report, or
re-ingested since it was written) and merges their rows into the month's placed / passages files.
Coverage (2 s) and the link cube (its exit marks are learned over the whole month) stay full.

Placement learns two things over all the rows it is given: the feed length of each link (p99.5 of
the feed metres, ``microsegments.locate.linear``) and the terminus aliases. A day placed alone
learns them on one day: on 2026-10-21, against the whole month, 0.2-3.6 % of the observations of
lines 55, 7, 92, 63 land on another link or zone. So a new day is placed with what the month's last
full run learned (``feedlen/line=X/month=M.parquet`` and the report's ``aliases``; links that run
did not see use the day's own lengths), and the month is placed in full again:

- whenever it has twice the days of its last full run (nights 2, 4, 8, 16 of a month): every day is
  then placed with lengths learned on at least half the month (lengths from days 1-11 vs 1-21:
  0.1-1.2 % of observations move, 63 being the worst);
- on the night that brings its last calendar day, so a closed month is exactly the full derive;
- after a package or ``ALGO_VERSION`` change, a failed line, or with ``--full``.
"""
from __future__ import annotations

import copy
import datetime as dt

import polars as pl

from . import ms
from .data import ALGO_VERSION, Data, by_line, derived, month_dates

FULL_GROWTH = 2.0      # full run when the month has this many times the days of its last full run
MIN_ROWS = 20          # feed length learned on fewer rows: the GTFS length (as microsegments)


def flen_rel(line: str, month: str) -> str:
    return derived(f"feedlen/line={line}/month={month}.parquet")


def _full_reason(month: str, dates: list[dt.date], prev: dict | None) -> str | None:
    if prev is None:
        return "not derived"
    if prev.get("algo") != ALGO_VERSION or prev.get("microsegments") != ms.version():
        return "package or algo changed"
    if prev.get("failed"):
        return "a line failed"
    if "full_days" not in prev:
        return "derived before incremental runs"
    if set(prev.get("dates", [])) - {d.isoformat() for d in dates}:
        return "days removed"
    return None


def changed_days(data: Data, dates: list[dt.date], prev: dict) -> list[dt.date]:
    """Dates not derived yet, or re-ingested after the month's last derive."""
    done, at = set(prev.get("dates", [])), prev.get("written_at", "")

    def stamp(d: dt.date) -> str:
        try:
            return data.store.read_json(f"raw/_SUCCESS/date={d}.json").get("written_at", "")
        except FileNotFoundError:
            return ""
    stamps = data._map(stamp, dates)
    return [d for d, s in zip(dates, stamps) if d.isoformat() not in done or s > at]


def plan(data: Data, month: str, dates: list[dt.date], prev: dict | None, full: bool = False) -> dict:
    """{mode: full | incremental | current, days: the days to place, why}."""
    why = "asked" if full else _full_reason(month, dates, prev)
    if why:
        return {"mode": "full", "days": dates, "why": why}
    new = changed_days(data, dates, prev)
    if not new:
        return {"mode": "current", "days": [], "why": "up to date"}
    if month_dates(month)[-1] in new:
        return {"mode": "full", "days": dates, "why": "month closes"}
    if len(dates) >= FULL_GROWTH * prev["full_days"]:
        return {"mode": "full", "days": dates, "why": f"{len(dates)} days, last full run on {prev['full_days']}"}
    return {"mode": "incremental", "days": new, "why": f"{len(new)} new day(s)"}


def with_aliases(cfg, aliases: dict | None):
    """``cfg`` with the month's learned terminus aliases given (codes not in them are still learned)."""
    if not aliases:
        return cfg
    c = copy.deepcopy(cfg)
    c.input.linear.terminus_aliases = {**aliases, **(c.input.linear.terminus_aliases or {})}
    return c


def with_feed_lengths(cfg, month_flen: pl.DataFrame, day_flen: pl.DataFrame):
    """``cfg`` placing with the month's feed lengths, the day's own where the month had too few rows."""
    m = month_flen.filter(pl.col("n") >= MIN_ROWS).select("pattern_uid", pl.col("link_idx").cast(pl.Int16),
                                                         pl.col("flen").alias("_m"))
    f = (day_flen.select("pattern_uid", pl.col("link_idx").cast(pl.Int16), "flen")
         .join(m, on=["pattern_uid", "link_idx"], how="left")
         .select("pattern_uid", "link_idx", pl.coalesce("_m", "flen").alias("flen")))
    c = copy.deepcopy(cfg)
    c.input.linear.feed_length = f
    return c


def merge(old: pl.DataFrame | None, new: pl.DataFrame, days: list[dt.date], sort: list[str]) -> pl.DataFrame:
    """The month's rows: ``old`` without ``days``, plus ``new`` (rows of ``days``)."""
    if old is None or not old.height:
        return new
    keep = old.filter(~pl.col("service_date").is_in(days))
    return pl.concat([keep, new], how="diagonal_relaxed").sort(sort, maintain_order=True)


def now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def report(month: str, dates: list[dt.date], plan: dict, prev: dict | None, per_line: list, failed: dict,
           partial: bool) -> dict:
    """The month's derive report. An incremental (or ``partial``: some lines) run keeps the other
    lines' entries and aliases, and the day count of the month's last full run."""
    prev = prev or {}
    keep = plan["mode"] == "incremental" or partial
    old = {r["line"]: r for r in prev.get("per_line", [])} if keep else {}
    merged = {**old, **{r["line"]: r for r in per_line}}
    aliases = dict(prev.get("aliases", {})) if keep else {}
    if plan["mode"] == "full":
        aliases.update({r["line"]: r.get("aliases", {}) for r in per_line})
    fresh = plan["mode"] == "full" and not (partial and "full_days" in prev)
    return {"month": month, "algo": ALGO_VERSION, "microsegments": ms.version(),
            "dates": [d.isoformat() for d in dates], "lines": len(merged), "failed": failed,
            "mode": plan["mode"], "why": plan["why"], "days_placed": [d.isoformat() for d in plan["days"]],
            "full_days": len(dates) if fresh else prev["full_days"], "full_at": now() if fresh else prev.get("full_at"),
            "aliases": aliases, "per_line": sorted(merged.values(), key=lambda r: by_line(r["line"]))}

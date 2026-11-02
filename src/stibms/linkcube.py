"""Link-level cube for the network ranking: one small file per month, all lines.

    python -m stibms.derive --from 2024-04 --to 2026-09 --linkcube-only     # (re)build from placed files

``derived/v{ALGO}/linkcube/month=M/``:

- ``cube.parquet``: per (line, service_date, link_key) the observations (each weighted by its hour's
  real seconds per poll / ``TICK_S``, see :func:`report_weight`) and passages of the line's
  vehicles in four hour bands (day 6-21 h, evening 20-24 h, i.e. 20:00-23:59 = the reference, am 7-10 h, pm 16-19 h),
  observations split by zone (``_r`` running = between stops, ``_s`` stop zone = from where the feed
  switches to the stop to its exit mark, as on the line page, see ``stibms.stopzones``) and, for day and evening, the observations that belong
  to a long stand (``l_``: the vehicle did not move for >= ``LONG_STAND_S``). Only usable hours are
  summed (feed coverage >= 80 %, not frozen, the line present in the feed) and only usable days are
  written (at most a quarter of the 6-21 h hours dropped), as in the line analysis.
- ``days.parquet``: (line, service_date) usable days, the denominator of "per average day".
- ``marks.parquet``: (line, link_key, mark_m) the month's exit marks (``stibms.stopzones.marks``), also
  read by the line page so both cut the same stop zones.
- ``links.parquet``: (line, link_key, from_name, to_name, len_m, term, reg, reg_r) with ``term`` 1 when the link is
  the first or last link of one of the line's patterns that month, 2 when one of its stops ends one of
  them (a short-working or partial terminus), else 0; ``reg`` / ``reg_r``: regulation link of the month (below),
  whole link / between stop zones.
- ``hourly.parquet``: the same per hour, for any hours asked on the network page: (line, service_date, hour,
  link_key, n, ok, o_r, o_s, l_r, l_s). ``n`` counts the passages of every hour; ``ok`` marks the usable hours
  and only those have observations (``o_``, ``l_`` as above, all zones of a hour band summed by the reader);
  ``spd_ok``: a usable hour where the position feed also holds the line's vehicles (below).
- ``linehours.parquet``: per (line, service_date, hour) the production and speed totals of the network page,
  so months of evolution stay a few 10 k rows: ``km`` (passages x link length, every link and hour),
  ``feed_ok`` (the feed itself was up: a line missing then really did not run), ``d_s``, ``t_s`` metres and
  seconds of the usable hours, stop zones included, ``d_r``, ``t_r`` the same between stop zones. Speeds leave
  out the first and last link of each pattern (layover at the terminus, not running), the month's regulation
  links and the hours not ``spd_ok``.

Regulation links. The ranking's test (``stibms.ranking``), run once per month and line on the ranking's default
days (Monday-Friday, no public holiday or reduced service) and hours (6-21 h against the evening): at least
``REG_SHARE`` of the link's excess comes from long stands. Fixed per month so that the evolution of a speed does
not depend on the hours asked for. ``reg`` tests the whole link (speed with stops), ``reg_r`` the part between stop
zones (speed without stops), as the ranking does with and without ``stops``. A link with no evening service or no
excess cannot be tested and stays in.

Speeds and a partial feed. Passages also come from the punctuality feed (``n = max(n_feed, n_events)``),
observations only from the position feed. When the position feed holds a fraction of a line's vehicles (tram
7 in February 2025: 1 600 passages a day seen against 9 700 in punctuality, while the coverage of the feed
looks fine), full passages over a fraction of the time read as 160 km/h. An hour counts for speeds only when
the position feed saw at least ``MIN_FEED_SHARE`` of the line's passages; km still count it.
- ``plan.parquet``: (line, service_date, hour, km, trips, feed_ok) the GTFS timetable of the month
  (``schedule.plan_month``).

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

from . import schedule, stopzones
from .coverage import FEED_BAD, LINE_ABSENT, with_line_absence
from .data import Data, derived

log = logging.getLogger("stibms.linkcube")

STAND_MOVE_M = 8.0
STAND_GAP_S = 90
LONG_STAND_S = 180
TICK_S = 20.0         # the feed's poll period: one report stands for TICK_S seconds
BANDS = {"day": (6, 21), "eve": (20, 24), "am": (7, 10), "pm": (16, 19)}
LONG_BANDS = ("day", "eve")
DAY_HOURS = (6, 21)
REG_SHARE = 0.5        # share of the excess in long stands that makes a link a regulation point
MIN_FEED_SHARE = 0.6   # normal hours are above 0.9 (n is at most a few % above n_feed)
TABLES = ("cube", "days", "links", "marks", "hourly", "linehours")   # per line; plan.parquet is the timetable


def rel(month: str, name: str) -> str:
    return derived(f"linkcube/month={month}/{name}.parquet")


def good_hours(cov: pl.DataFrame, placed: pl.DataFrame, dates) -> tuple[pl.DataFrame, list[dt.date]]:
    """(service_date, hour) usable for the line, and the usable days (same rules as the analysis)."""
    from microsegments.config import Quality
    q = Quality()
    if not cov.height:
        return pl.DataFrame(schema={"service_date": pl.Date, "hour": pl.Int8}), []
    c = with_line_absence(cov, placed, dates).with_columns(pl.col("hour").cast(pl.Int8))
    c = c.with_columns(((pl.col("coverage") >= q.min_hour_coverage) & ((pl.col("flags") & (FEED_BAD | LINE_ABSENT)) == 0))
                       .alias("ok"))
    a, b = DAY_HOURS
    bad = (c.filter(pl.col("hour").is_between(a, b - 1)).group_by("service_date")
           .agg((~pl.col("ok")).sum().alias("bad")))
    days = bad.filter(pl.col("bad") / (b - a) <= 1 - q.min_day_coverage + 1e-9)["service_date"].sort().to_list()
    ok = c.filter(pl.col("ok") & pl.col("service_date").is_in(days)).select("service_date", "hour")
    return ok, days


def report_weight(cov: pl.DataFrame) -> pl.DataFrame:
    """(service_date, hour, w): what one report is worth in ``TICK_S`` units, the real seconds a distinct
    poll stands for that hour (covered_s / n_snapshots), as the line page (``tick_from_coverage``)."""
    if not cov.height or "n_snapshots" not in cov.columns:
        return pl.DataFrame(schema={"service_date": pl.Date, "hour": pl.Int8, "w": pl.Float64})
    sec = pl.col("covered_s").cast(pl.Float64) / pl.col("n_snapshots").cast(pl.Float64)
    return cov.select("service_date", pl.col("hour").cast(pl.Int8),
                      pl.when(pl.col("n_snapshots") > 0).then(sec / TICK_S).otherwise(1.0).alias("w"))


def stands(placed: pl.DataFrame) -> pl.DataFrame:
    """placed + ``long`` (the fix belongs to a stand of >= LONG_STAND_S)."""
    p = placed.sort("track_id", "ts")
    new = ((pl.col("track_id") != pl.col("track_id").shift())
           | ((pl.col("s_m") - pl.col("s_m").shift()).abs() > STAND_MOVE_M)
           | ((pl.col("ts") - pl.col("ts").shift()).dt.total_seconds() > STAND_GAP_S)).fill_null(True)
    p = p.with_columns(new.cum_sum().alias("_sid"))
    dur = (pl.col("ts").max() - pl.col("ts").min()).dt.total_seconds().over("_sid")
    return p.with_columns((dur >= LONG_STAND_S).alias("long")).drop("_sid")


def _band_aggs() -> list[pl.Expr]:
    """Weighted observations per hour band and zone (running / stop), and those of long stands."""
    w, stop, aggs = pl.col("w"), pl.col("stop"), []
    for b, (h0, h1) in BANDS.items():
        inb = pl.col("hour").is_between(h0, h1 - 1)
        aggs += [w.filter(inb & ~stop).sum().cast(pl.Float32).alias(f"o_{b}_r"),
                 w.filter(inb & stop).sum().cast(pl.Float32).alias(f"o_{b}_s")]
        if b in LONG_BANDS:
            aggs += [w.filter(inb & ~stop & pl.col("long")).sum().cast(pl.Float32).alias(f"l_{b}_r"),
                     w.filter(inb & stop & pl.col("long")).sum().cast(pl.Float32).alias(f"l_{b}_s")]
    return aggs


def _fixes(p: pl.DataFrame, net, mk: pl.DataFrame, ok: pl.DataFrame, cov: pl.DataFrame) -> pl.DataFrame:
    """The usable hours' fixes with their zone (``stop``), long stand flag and weight ``w``."""
    p = stands(stopzones.prepare(p, net, mk))
    p = (p.join(net.links.select("pattern_uid", "link_idx", pl.col("len_m").alias("_len")),
                on=["pattern_uid", "link_idx"], how="left")
         .join(stopzones.heads(mk), on="link_key", how="left"))
    head = pl.col("head_m").fill_null(stopzones.default_head())
    p = p.with_columns(((pl.col("pos_m") <= head) | (pl.col("pos_m") > pl.col("_len") - stopzones.APPROACH_M))
                       .fill_null(False).alias("stop"), pl.col("hour").cast(pl.Int8))
    p = p.join(ok, on=["service_date", "hour"], how="inner").join(report_weight(cov), on=["service_date", "hour"],
                                                                 how="left")
    return p.with_columns(pl.col("w").fill_null(1.0))


def _hourly(fx: pl.DataFrame, passages: pl.DataFrame | None, ok: pl.DataFrame) -> pl.DataFrame:
    """Per (service_date, hour, link_key): passages of any hour, observations of the usable ones."""
    w, stop, long_ = pl.col("w"), pl.col("stop"), pl.col("long")
    o = fx.group_by("service_date", "hour", "link_key").agg(
        w.filter(~stop).sum().cast(pl.Float32).alias("o_r"), w.filter(stop).sum().cast(pl.Float32).alias("o_s"),
        w.filter(~stop & long_).sum().cast(pl.Float32).alias("l_r"), w.filter(stop & long_).sum().cast(pl.Float32).alias("l_s"))
    if passages is None or not passages.height:
        h = o.with_columns(pl.lit(0.0, pl.Float32).alias("n"), pl.lit(0.0, pl.Float32).alias("_nf"))
    else:
        n = (passages.with_columns(pl.col("hour").cast(pl.Int8)).group_by("service_date", "hour", "link_key")
             .agg(pl.col("n").sum().cast(pl.Float32), pl.col("n_feed").sum().cast(pl.Float32).alias("_nf")))
        h = o.join(n, on=["service_date", "hour", "link_key"], how="full", coalesce=True)
    good = ok.with_columns(pl.lit(True).alias("ok"))
    h = (h.join(good, on=["service_date", "hour"], how="left")
         .with_columns(pl.col("ok").fill_null(False), *[pl.col(c).fill_null(0) for c in ("n", "_nf", "o_r", "o_s", "l_r", "l_s")]))
    seen = pl.col("_nf").sum().over("service_date", "hour") >= MIN_FEED_SHARE * pl.col("n").sum().over("service_date", "hour")
    return h.with_columns((pl.col("ok") & seen).alias("spd_ok")).drop("_nf")


def _line_hours(h: pl.DataFrame, roles: pl.DataFrame, mk: pl.DataFrame) -> pl.DataFrame:
    """Per (service_date, hour): km of every link; metres and seconds of the usable hours without the
    first and last links, whole and between stop zones."""
    x = h.join(roles.select("link_key", "len_m", "term", "reg", "reg_r"), on="link_key", how="left").join(stopzones.heads(mk), on="link_key", how="left")
    run = (pl.col("len_m") - pl.col("head_m").fill_null(stopzones.default_head()) - stopzones.APPROACH_M).clip(0)
    use = pl.col("spd_ok") & (pl.col("term") != 1)
    whole, between = use & ~pl.col("reg").fill_null(False), use & ~pl.col("reg_r").fill_null(False)
    return x.group_by("service_date", "hour").agg(
        ((pl.col("n") * pl.col("len_m")).sum() / 1000).cast(pl.Float32).alias("km"),
        (pl.col("n") * pl.col("len_m")).filter(whole).sum().cast(pl.Float32).alias("d_s"),
        ((pl.col("o_r") + pl.col("o_s")) * TICK_S).filter(whole).sum().cast(pl.Float32).alias("t_s"),
        (pl.col("n") * run).filter(between).sum().cast(pl.Float32).alias("d_r"),
        (pl.col("o_r") * TICK_S).filter(between).sum().cast(pl.Float32).alias("t_r"))


def _with_passages(o: pl.DataFrame, passages: pl.DataFrame | None, ok: pl.DataFrame) -> pl.DataFrame:
    if passages is None or not passages.height:
        return o.with_columns([pl.lit(0.0, pl.Float32).alias(f"p_{b}") for b in BANDS])
    ps = passages.with_columns(pl.col("hour").cast(pl.Int8)).join(ok, on=["service_date", "hour"], how="inner")
    pas = ps.group_by("service_date", "link_key").agg(
        [pl.col("n").filter(pl.col("hour").is_between(h0, h1 - 1)).sum().cast(pl.Float32).alias(f"p_{b}")
         for b, (h0, h1) in BANDS.items()])
    return o.join(pas, on=["service_date", "link_key"], how="full", coalesce=True)


def _link_roles(net) -> pl.DataFrame:
    """(link_key, from_name, to_name, len_m, term): names, length, terminus role (first/last link of a
    pattern; a stop that ends a pattern)."""
    lk = net.links
    pats = net.patterns.select("pattern_uid", "stop_ids")
    ends = list(set(pats["stop_ids"].list.first().to_list()) | set(pats["stop_ids"].list.last().to_list()))
    first_last = (lk.with_columns(pl.col("link_idx").max().over("pattern_uid").alias("_m"))
                  .filter((pl.col("link_idx") == 0) | (pl.col("link_idx") == pl.col("_m")))["link_key"].unique())
    return (lk.group_by("link_key").agg(pl.col("from_stop").first(), pl.col("to_stop").first(),
                                        pl.col("from_name").first(), pl.col("to_name").first(),
                                        pl.col("len_m").median())
            .with_columns(pl.when(pl.col("link_key").is_in(first_last.implode())).then(1)
                          .when(pl.col("from_stop").is_in(ends) | pl.col("to_stop").is_in(ends)).then(2)
                          .otherwise(0).cast(pl.Int8).alias("term"))
            .drop("from_stop", "to_stop"))


def _regulation(o: pl.DataFrame, skip=()) -> pl.DataFrame:
    """(link_key, reg, reg_r) from the line's cube rows: the regulation links of the month (module docstring),
    ``skip`` the days the ranking leaves out by default."""
    d = (o.filter((pl.col("service_date").dt.weekday() <= 5) & ~pl.col("service_date").is_in(list(skip)))
         .group_by("link_key").agg(pl.col(c).sum().cast(pl.Float64) for c in
                                   ("o_day_r", "o_day_s", "l_day_r", "l_day_s", "o_eve_r", "o_eve_s", "l_eve_r", "l_eve_s", "p_day", "p_eve"))
         .filter((pl.col("p_day") > 0) & (pl.col("p_eve") > 0)))

    def test(zones):
        def excess(p):
            day = pl.sum_horizontal(pl.col(f"{p}_day_{z}") for z in zones)
            eve = pl.sum_horizontal(pl.col(f"{p}_eve_{z}") for z in zones)
            return day / pl.col("p_day") - eve / pl.col("p_eve")
        return (excess("o") > 0) & (excess("l") >= REG_SHARE * excess("o"))
    return d.select("link_key", test("rs").alias("reg"), test("r").alias("reg_r"))


def line_cube(line: str, placed: pl.DataFrame, passages: pl.DataFrame | None, net, cov: pl.DataFrame,
              dates, skip=()) -> dict[str, pl.DataFrame]:
    """The tables of one line for one month, by file name (see the module docstring). ``skip``: the days the
    ranking leaves out by default, for the regulation links."""
    ok, days = good_hours(cov, placed, dates)
    lit = pl.lit(line, pl.Utf8).alias("line")
    days_df = pl.DataFrame({"service_date": days}, schema={"service_date": pl.Date}).with_columns(lit)
    p = placed.filter(pl.col("count").fill_null(True)) if "count" in placed.columns else placed
    mk = stopzones.marks(p, line)
    if not p.height or not net.links.height or not days:
        return {"days": days_df, "marks": mk}
    fx = _fixes(p, net, mk, ok, cov)
    o = _with_passages(fx.group_by("service_date", "link_key").agg(_band_aggs()), passages, ok)
    num = [c for c in o.columns if c[:2] in ("o_", "l_", "p_")]
    o = o.with_columns([pl.col(c).fill_null(0) for c in num]).with_columns(lit)
    roles = (_link_roles(net).join(_regulation(o, skip), on="link_key", how="left")
             .with_columns(pl.col("reg").fill_null(False), pl.col("reg_r").fill_null(False)))
    h = _hourly(fx, passages, ok)
    return {"cube": o, "days": days_df, "links": roles.with_columns(lit), "marks": mk,
            "hourly": h.with_columns(lit), "linehours": _line_hours(h, roles, mk).with_columns(lit)}


def _with_feed(df: pl.DataFrame, cov: pl.DataFrame) -> pl.DataFrame:
    """``df`` (service_date, hour, ...) + ``feed_ok``: the feed itself was up that hour (a line missing
    from a working feed really did not run: its km count as lost, not as unknown)."""
    from microsegments.config import Quality
    if not df.height:
        return df.with_columns(pl.lit(False).alias("feed_ok"))
    f = cov.select("service_date", pl.col("hour").cast(pl.Int8),
                   ((pl.col("coverage") >= Quality().min_hour_coverage) & ((pl.col("flags") & FEED_BAD) == 0)).alias("feed_ok"))
    return df.with_columns(pl.col("hour").cast(pl.Int8)).join(f, on=["service_date", "hour"], how="left").with_columns(pl.col("feed_ok").fill_null(False))


def write_month(data: Data, month: str, lines: list[str] | None = None) -> dict:
    """Build ``linkcube/month=M`` from the month's placed + passages files (all lines)."""
    t0 = time.time()
    dates = data.month_ingested(month)
    have = data.line_months()
    todo = [ln for ln, ms_ in have.items() if month in ms_ and (lines is None or ln in lines)]
    cov = data.coverage(dates)
    skip = sorted(schedule.exclusions(data, dates[0], dates[-1])) if dates else []
    acc = {n: [] for n in TABLES}
    failed = {}
    for ln in sorted(todo, key=lambda x: (len(x), x)):
        try:
            placed = data.placed(ln, dates)
            if placed is None or not placed.height:
                continue
            for name, df in line_cube(ln, placed, data.passages(ln, dates), data.network(ln, dates), cov, dates, skip).items():
                acc[name].append(df)
        except Exception as e:  # noqa: BLE001 - one broken line must not block the month
            log.exception("linkcube %s line %s failed", month, ln)
            failed[ln] = repr(e)[:200]
    for name, frames in acc.items():
        old = data.read_optional(rel(month, name)) if lines is not None else None
        if old is not None and old.height:   # partial rebuild: keep the other lines' rows
            frames.append(old.filter(~pl.col("line").is_in(lines)))
        df = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
        if name == "linehours":
            df = _with_feed(df, cov)
        if name == "hourly" and df.height:    # read for one or a few lines: row groups by line
            df = df.sort("line", "service_date", "hour")
        data.store.write_parquet(rel(month, name), df)
    # the timetable, every line: the km planned do not depend on the lines rebuilt
    data.store.write_parquet(rel(month, "plan"), _with_feed(schedule.plan_month(data, dates), cov))
    n = sum(c.height for c in acc["cube"])
    log.info("linkcube %s: %d lines, %d rows, %.1f s", month, len(todo), n, time.time() - t0)
    return {"month": month, "lines": len(todo), "rows": n, "failed": failed, "seconds": round(time.time() - t0, 1)}

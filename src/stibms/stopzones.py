"""Stop zones from the STIB feed itself (vehicle-distance), not a fixed radius.

Around a stop the feed has no usable position:

- **before**: the vehicle switches to the stop ~36 m before it (``APPROACH_M``, the same everywhere,
  35.8-36.3 m over 10 months) and then reads ``dist = 0``;
- **after**: it reads ``dist = 0`` until it leaves, then sends an exit message whose distance is a
  fixed even value set per (line, direction, stop), mostly 34-50 m, sometimes two values for one stop,
  shown until the next poll.

So a zone runs from ``APPROACH_M`` before the stop to just past the stop's highest exit mark, and the
running part in between is cut in equal ~30 m micro-segments (``microsegments.segments`` grid "stops").

- :func:`marks`: the exit marks of each (line, link leaving the stop): the even raw distances in
  ``MARK_RANGE`` that a large share of the vehicles report first when they leave the stop.
  Learned per month by the derive (``linkcube/month=M/marks.parquet``) and shared by the line page
  (union over the period's months) and the ranking cube, so both cut the same zones.
- :func:`heads`: zone end after the stop = highest mark + 1 m (whole metres, so every pattern of a link
  gets the same micro-segment keys); ``EXIT_DEFAULT_M`` + 1 where no mark was learned.
- :func:`prepare`: placed files keep the package's stretch of feed metres over the whole link; positions
  are put back on ``[0, len - APPROACH_M]`` (``microsegments.locate.linear.approach``), and exit-message
  rows are kept inside the zone even where the link's feed length is off (detour, wrong GTFS length).
"""
from __future__ import annotations

import polars as pl

APPROACH_M = 36.0
EXIT_DEFAULT_M = 38.0
MARK_RANGE = (10.0, 60.0)
MARK_MIN_N = 20
MARK_SHARE = 0.1
MARKS_SCHEMA = {"line": pl.Utf8, "link_key": pl.Utf8, "mark_m": pl.Float64}


def _not_at_stop() -> pl.Expr:
    from microsegments.schema import Flag
    return (pl.col("flags") & int(Flag.AT_STOP)) == 0


def marks(placed: pl.DataFrame, line: str) -> pl.DataFrame:
    """(line, link_key, mark_m): the exit marks found in ``placed`` (raw feed metres).

    Only departures count: a vehicle's first report on the link after a report on the link before
    (same track; standing at the stop or not: the exit message is also sent when it does not stop), or
    a track's first report (leaving a terminus: its layover rows are not stored). A value is a mark when it is even, in ``MARK_RANGE``, seen ``MARK_MIN_N`` times
    and makes ``MARK_SHARE`` of the link's departures (periodic first reports spread over many values;
    a vehicle queuing at a signal repeats a value but is no departure)."""
    if not placed.height or not {"raw_dist_m", "track_id"} <= set(placed.columns):
        return pl.DataFrame(schema=MARKS_SCHEMA)
    lo, hi = MARK_RANGE
    tr = ["track_id"]
    x = (placed.filter(pl.col("track_id").is_not_null()).sort("track_id", "ts")
         .with_columns(pl.col("link_idx").shift(1).over(tr).alias("_prev_li"),
                       pl.col("pattern_uid").shift(1).over(tr).alias("_prev_u")))
    dep = x.filter(_not_at_stop() & (pl.col("_prev_li").is_null()
                                     | ((pl.col("pattern_uid") == pl.col("_prev_u")) & (pl.col("link_idx") == pl.col("_prev_li") + 1))))
    c = (dep.filter(pl.col("raw_dist_m").is_between(lo, hi)).group_by("link_key", "raw_dist_m").agg(pl.len().alias("n"))
         .with_columns((pl.col("n") / pl.col("n").sum().over("link_key")).alias("share")))
    m = c.filter((pl.col("raw_dist_m") % 2 == 0) & (pl.col("n") >= MARK_MIN_N) & (pl.col("share") >= MARK_SHARE))
    return m.select(pl.lit(line, pl.Utf8).alias("line"), "link_key", pl.col("raw_dist_m").cast(pl.Float64).alias("mark_m"))


def heads(mk: pl.DataFrame) -> pl.DataFrame:
    """(link_key, head_m) from marks (any period, months merged)."""
    return mk.group_by("link_key").agg((pl.col("mark_m").max().ceil() + 1.0).alias("head_m"))


def default_head() -> float:
    return EXIT_DEFAULT_M + 1.0


def prepare(placed: pl.DataFrame, net, mk: pl.DataFrame | None) -> pl.DataFrame:
    """Placed rows with positions in GTFS metres, exit-message rows inside their zone."""
    from microsegments.locate.linear import approach
    p = approach(placed, net.links, APPROACH_M)
    if mk is None or not mk.height or not p.height:
        return p
    hd = heads(mk)
    ex = mk.select("link_key", pl.col("mark_m").cast(placed.schema["raw_dist_m"]).alias("_mark"),
                   pl.lit(True).alias("_exit")).unique()
    p = (p.join(ex, left_on=["link_key", "raw_dist_m"], right_on=["link_key", "_mark"], how="left",
                maintain_order="left")
         .join(hd.rename({"head_m": "_head"}), on="link_key", how="left", maintain_order="left"))
    clamp = pl.col("_exit").fill_null(False) & _not_at_stop() & (pl.col("pos_m") > pl.col("_head") - 0.5)
    pos = pl.when(clamp).then(pl.col("_head") - 0.5).otherwise(pl.col("pos_m")).cast(placed.schema["pos_m"])
    p = p.with_columns(pos.alias("_pos"))
    if "s_m" in p.columns:
        p = p.with_columns((pl.col("s_m") - pl.col("pos_m") + pl.col("_pos")).cast(placed.schema["s_m"]).alias("s_m"))
    return p.with_columns(pl.col("_pos").alias("pos_m")).drop("_pos", "_exit", "_head")

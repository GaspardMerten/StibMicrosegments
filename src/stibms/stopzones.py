"""Stop zones from the STIB feed itself (vehicle-distance), not a fixed radius.

Around a stop the feed has no usable position:

- **before**: the vehicle switches to the stop ~36 m before it (``APPROACH_M``, the same everywhere,
  35.8-36.3 m over 10 months) and then reads ``dist = 0``;
- **after**: it reads ``dist = 0`` until it leaves, then sends one exit message whose distance is a fixed
  even value set per (line, direction, stop), 34-50 m (mode 38 m), shown until the next poll.

So a zone runs from ``APPROACH_M`` before the stop to that stop's exit mark, and the running part in
between is cut in equal ~30 m micro-segments (``microsegments.segments`` grid ``"stops"``).

Placed files keep the package's stretch of feed metres over the whole link; :func:`prepare` puts them
back on ``[0, len - APPROACH_M]`` (``microsegments.locate.linear.approach``) so positions are GTFS metres.
:func:`exit_marks` learns each stop's exit mark from the period's rows: the most frequent even raw
distance in ``EXIT_RANGE`` on the link leaving the stop, kept when it is clearly a spike (at least
``EXIT_MIN_N`` rows and ``EXIT_RATIO`` x the next value, and it lands within 6 m of ``EXIT_RANGE`` once
placed), else the stop gets ``EXIT_DEFAULT_M``.
"""
from __future__ import annotations

import polars as pl

APPROACH_M = 36.0
EXIT_DEFAULT_M = 38.0
EXIT_RANGE = (30.0, 52.0)
EXIT_MIN_N = 20
EXIT_RATIO = 2.5
EXIT_PAD_M = 0.5        # the exit row itself lands inside the zone (bins are (lo, hi])
STOP_ZONE = (APPROACH_M, EXIT_DEFAULT_M)


def prepare(placed: pl.DataFrame, net) -> pl.DataFrame:
    """Placed rows with positions in GTFS metres (see the module docstring)."""
    from microsegments.locate.linear import approach
    return approach(placed, net.links, APPROACH_M)


def exit_marks(placed: pl.DataFrame) -> pl.DataFrame:
    """(pattern_uid, link_key, head_m): the exit mark of the stop each link leaves, in ``pos_m`` metres
    (after :func:`prepare`). Links without a clear spike are left out (the default applies)."""
    from microsegments.schema import Flag
    out_schema = {"pattern_uid": pl.Utf8, "link_key": pl.Utf8, "head_m": pl.Float64}
    if not placed.height or "raw_dist_m" not in placed.columns:
        return pl.DataFrame(schema=out_schema)
    lo, hi = EXIT_RANGE
    x = placed.filter(((pl.col("flags") & int(Flag.AT_STOP)) == 0) & pl.col("raw_dist_m").is_between(lo, hi)
                      & (pl.col("raw_dist_m") % 2 == 0))
    if not x.height:
        return pl.DataFrame(schema=out_schema)
    c = (x.group_by("pattern_uid", "link_key", "raw_dist_m")
         .agg(pl.len().alias("n"), pl.col("pos_m").max().cast(pl.Float64).alias("pos"))
         .sort(["pattern_uid", "link_key", "n"], descending=[False, False, True])
         .group_by("pattern_uid", "link_key", maintain_order=True)
         .agg(pl.col("n").first().alias("n1"), pl.col("n").slice(1, 1).first().fill_null(0).alias("n2"),
              pl.col("pos").first()))
    # a link whose feed metres are far from GTFS metres (detour, wrong GTFS length) maps the mark far off
    ok = ((pl.col("n1") >= EXIT_MIN_N) & (pl.col("n1") >= EXIT_RATIO * pl.col("n2"))
          & pl.col("pos").is_between(lo - 6, hi + 6))
    return c.filter(ok).select("pattern_uid", "link_key", (pl.col("pos") + EXIT_PAD_M).alias("head_m"))

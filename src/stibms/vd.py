"""stib/vehicle-distance -> raw/vd/date=D/{vd,snaps}.parquet for one service day, all lines.

Sources. The bulk Parquet files of the feed (``/parquetized``: one per UTC day, hourly for the
current day) carry the same rows as the ~20 s JSON snapshots at a fraction of the transfer
(~5 MB a UTC day instead of ~250 MB of JSON). They omit empty snapshots, so the JSON *index* is
also read to list every poll. Any part of the window not covered by a Parquet file is fetched as
raw JSON snapshots (16 threads), so a missing bulk file only makes the day slower.

Frozen polls. ``frozen`` = the poll's content (its multiset of rows) is identical to the previous
poll's in the window, the first poll is never frozen; consecutive empty polls are frozen too.
This is the ``microsegments.io.stib`` definition. A frozen poll keeps its entry in snaps.parquet
(``n_rows`` = rows of its payload) but its rows are NOT repeated in vd.parquet: they equal the
rows of the last non-frozen poll, so nothing is lost and counting code never double-counts a
stalled feed. Coverage therefore sees every poll with its flag; vehicle rows exist exactly for
the non-frozen polls with n_rows > 0.

Note: the prototype (fetch_vd55_2025.py) only skipped polls whose blob URL equalled the previous
one, which never happens; it kept content-identical polls. Its line-55 rows are ours plus the
rows of the frozen polls.
"""
from __future__ import annotations

import io
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date

import polars as pl
import pyarrow as pa

from .mobilitytwin import MobilityTwin, ParquetPart, Snapshot
from .servicedays import window

ENDPOINT = "stib/vehicle-distance"
COMPONENT = "stib_vehicle_distance"
WORKERS = 16
ROW_GROUP_TARGET = 128_000

ROWS_SCHEMA = {"ts": pl.Int64, "line": pl.Utf8, "dir": pl.Utf8, "point": pl.Utf8, "dist": pl.Int32}
SNAPS_SCHEMA = {"ts": pl.Int64, "n_rows": pl.UInt32, "frozen": pl.Boolean}


@dataclass
class VdDay:
    rows: pl.DataFrame
    snaps: pl.DataFrame
    stats: dict = field(default_factory=dict)


def _empty_rows() -> pl.DataFrame:
    return pl.DataFrame(schema=ROWS_SCHEMA)


def rows_from_bulk(content: bytes) -> pl.DataFrame:
    df = pl.read_parquet(io.BytesIO(content),
                         columns=["lineId", "directionId", "pointId", "distanceFromPoint", "date"])
    return df.select(
        pl.col("date").dt.epoch("s").alias("ts"),
        pl.col("lineId").cast(pl.Utf8).alias("line"),
        pl.col("directionId").cast(pl.Utf8).alias("dir"),
        pl.col("pointId").cast(pl.Utf8).alias("point"),
        pl.col("distanceFromPoint").cast(pl.Int32).alias("dist"),
    )


def rows_from_json(ts: int, content: bytes | str | list | dict) -> pl.DataFrame:
    data = json.loads(content) if isinstance(content, (bytes, str)) else content
    if isinstance(data, dict):
        data = data.get("data") or data.get("results") or []
    if not data:
        return _empty_rows()

    def s(v):
        return None if v is None else str(v)

    return pl.DataFrame({
        "ts": [ts] * len(data),
        "line": [s(r.get("lineId")) for r in data],
        "dir": [s(r.get("directionId")) for r in data],
        "point": [s(r.get("pointId")) for r in data],
        "dist": [None if r.get("distanceFromPoint") is None else int(r["distanceFromPoint"]) for r in data],
    }, schema=ROWS_SCHEMA)


def build(rows: pl.DataFrame, poll_ts: list[int] | None = None) -> VdDay:
    """Rows of every poll (ts, line, dir, point, dist) + the list of polls (may include empty ones)
    -> deduplicated vd rows and snaps with the frozen flag."""
    rows = rows.cast(ROWS_SCHEMA)
    digests = (
        rows.with_columns(h=pl.struct("line", "dir", "point", "dist").hash(seed=7))
        .group_by("ts")
        .agg(pl.len().cast(pl.UInt32).alias("n_rows"), pl.col("h").sort().alias("hs"))
        .with_columns(pl.col("hs").hash(seed=11).alias("digest"))
        .drop("hs")
    )
    polls = pl.DataFrame({"ts": sorted(set(poll_ts or []))}, schema={"ts": pl.Int64})
    snaps = (
        pl.concat([polls, digests.select("ts")]).unique().sort("ts")
        .join(digests, on="ts", how="left")
        .with_columns(pl.col("n_rows").fill_null(0), pl.col("digest").fill_null(0))
    )
    snaps = snaps.with_columns(
        (pl.col("digest") == pl.col("digest").shift(1)).fill_null(False).alias("frozen")
    ).select(list(SNAPS_SCHEMA)).cast(SNAPS_SCHEMA)
    keep = snaps.filter(~pl.col("frozen") & (pl.col("n_rows") > 0)).select("ts")
    vd = rows.join(keep, on="ts", how="semi", maintain_order="left").sort(["line", "ts"], maintain_order=True,
                                                                          nulls_last=True)
    stats = {
        "polls": snaps.height,
        "frozen": int(snaps["frozen"].sum()),
        "empty": int((snaps["n_rows"] == 0).sum()),
        "rows_all_polls": rows.height,
        "rows": vd.height,
        "lines": vd["line"].n_unique(),
    }
    return VdDay(vd, snaps, stats)


def row_group_bounds(vd: pl.DataFrame, target: int = ROW_GROUP_TARGET) -> list[int]:
    """Row offsets starting a row group: lines packed greedily up to ~target rows, a line never
    split unless it alone exceeds 2x target. vd must be sorted by line."""
    if vd.height == 0:
        return [0]
    sizes = vd.group_by("line", maintain_order=True).len()["len"].to_list()
    bounds, start, acc = [0], 0, 0
    for n in sizes:
        if acc and acc + n > target:
            start += acc
            bounds.append(start)
            acc = 0
        acc += n
    return bounds


def to_arrow(vd: pl.DataFrame) -> pa.Table:
    return vd.cast(ROWS_SCHEMA).to_arrow()


def fetch_day(mt: MobilityTwin, d: date, workers: int = WORKERS) -> VdDay:
    t0, t1 = window(d)
    index: list[Snapshot] = mt.index(ENDPOINT, t0, t1)
    parts: list[ParquetPart] = mt.parquet_parts(COMPONENT, t0, t1)
    # A daily file supersedes the hourly ones it overlaps.
    covered: list[tuple[int, int]] = []
    chosen: list[ParquetPart] = []
    for p in sorted(parts, key=lambda p: -(p.end - p.start)):
        if not any(a <= p.start and p.end <= b for a, b in covered):
            chosen.append(p)
            covered.append((p.start, p.end))
    chosen.sort(key=lambda p: p.start)

    def in_bulk(ts: int) -> bool:
        return any(p.start <= ts < p.end for p in chosen)

    json_snaps = [s for s in index if not in_bulk(s.timestamp)]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        bulk = list(pool.map(lambda p: rows_from_bulk(mt.fetch(p.url)), chosen))
        js = list(pool.map(lambda s: rows_from_json(s.timestamp, mt.fetch(s.url)), json_snaps))
    frames = [f.filter(pl.col("ts").is_between(t0, t1)) for f in bulk] + js
    rows = pl.concat(frames) if frames else _empty_rows()
    day = build(rows, [s.timestamp for s in index])
    idx_ts = {s.timestamp for s in index}
    day.stats.update({
        "window_utc": [t0, t1],
        "index_polls": len(index),
        "bulk_files": [p.url.rsplit("/", 2)[-2] + "/" + p.url.rsplit("/", 1)[-1] for p in chosen],
        "json_polls": len(json_snaps),
        "polls_not_in_index": int(day.snaps.filter(~pl.col("ts").is_in(pl.Series(sorted(idx_ts), dtype=pl.Int64).implode())).height),
    })
    return day

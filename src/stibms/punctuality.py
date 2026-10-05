"""stib/punctuality -> raw/punctuality/date=D/p.parquet.

The archive holds one file per Brussels *calendar* day (times 00:00-24:00 local, trips with
start_date = D), so trips running after midnight are cut. Its snapshot timestamp changed
convention over time: older files are stamped at local midnight starting D, recent ones at local
midnight ending D. The file for D is therefore picked by content: the footer statistics of
``start_date`` (read with a range request) must equal D.

Times are naive UTC in the source; they are stored as Datetime(us, UTC).
"""
from __future__ import annotations

import io
import struct
from datetime import date, timedelta

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from .mobilitytwin import MobilityTwin, Snapshot
from .servicedays import local_midnight

ENDPOINT = "stib/punctuality"
COLUMNS = [
    "trip_id", "start_date", "start_time", "route_id", "route_gtfs_id", "direction_id", "journey_id",
    "trip_schedule_relationship", "stop_sequence", "stop_id",
    "arrival_time", "departure_time", "arrival_delay", "departure_delay", "observed", "inferred",
]


def _start_dates(mt: MobilityTwin, url: str) -> tuple[str | None, str | None]:
    """min/max of start_date from the Parquet footer, two small range requests."""
    size = mt.size(url)
    tail = mt.fetch_range(url, size - 8, 8)
    footer_len = struct.unpack("<I", tail[:4])[0]
    n = min(size, footer_len + 8)
    meta = pq.read_metadata(pa.BufferReader(mt.fetch_range(url, size - n, n)))
    i = meta.schema.to_arrow_schema().get_field_index("start_date")
    lo, hi = None, None
    for g in range(meta.num_row_groups):
        st = meta.row_group(g).column(i).statistics
        if st is None or not st.has_min_max:
            return None, None
        lo = st.min if lo is None else min(lo, st.min)
        hi = st.max if hi is None else max(hi, st.max)
    return lo, hi


def find_snapshot(mt: MobilityTwin, d: date) -> Snapshot | None:
    a, b = local_midnight(d), local_midnight(d + timedelta(days=1))
    cands = mt.index(ENDPOINT, a - 3 * 3600, b + 3 * 3600)
    # Newer convention first (stamped at the end of D), then the older one.
    cands.sort(key=lambda s: (abs(s.timestamp - b), abs(s.timestamp - a)))
    want = d.strftime("%Y%m%d")
    for s in cands:
        lo, hi = _start_dates(mt, s.url)
        if lo == want and hi == want:
            return s
    return None


def fetch_day(mt: MobilityTwin, d: date) -> tuple[pl.DataFrame | None, dict]:
    s = find_snapshot(mt, d)
    if s is None:
        return None, {"found": False}
    df = pl.read_parquet(io.BytesIO(mt.fetch(s.url)))
    cols = [c for c in COLUMNS if c in df.columns]
    df = df.select(cols).with_columns(
        pl.col(c).cast(pl.Datetime("us")).dt.replace_time_zone("UTC")
        for c in ("arrival_time", "departure_time") if c in cols
    )
    return df, {"found": True, "snapshot": s.date, "rows": df.height, "trips": df["trip_id"].n_unique()}

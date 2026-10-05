"""stib/gtfs-parquet -> gtfs/feeds/<sha>/<table>.parquet + gtfs/dates/date=D.json + gtfs/index.parquet.

The snapshot for service date D is the latest one taken on or before D (local date): daily from
2024-08-24, roughly fortnightly before (first 2024-04-05). Snapshots are zips of Parquet tables
(gtfs-parquet). The zip bytes change every day, the table files do not, so the content sha is the
sha256 of the sorted ``name:sha256(table bytes)`` lines. Each distinct feed is stored once, its
tables copied as they are.

The index is rebuilt from the per-date JSON entries (``rebuild_index``), so concurrent backfill
tasks never write the same object except the index itself, which any later run rewrites in full.
"""
from __future__ import annotations

import hashlib
import io
import zipfile
from datetime import date, datetime, timedelta, timezone

import polars as pl

from .mobilitytwin import MobilityTwin, Snapshot
from .servicedays import TZ, local_midnight
from .storage import Store

ENDPOINT = "stib/gtfs-parquet"
LOOKBACK_DAYS = 31


def content_sha(zf: zipfile.ZipFile) -> str:
    lines = sorted(f"{i.filename}:{hashlib.sha256(zf.read(i)).hexdigest()}"
                   for i in zf.infolist() if not i.is_dir())
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


def find_snapshot(mt: MobilityTwin, d: date) -> Snapshot | None:
    end = local_midnight(d + timedelta(days=1)) - 1
    snaps = mt.index(ENDPOINT, local_midnight(d - timedelta(days=LOOKBACK_DAYS)), end)
    return snaps[-1] if snaps else None


def ingest_day(mt: MobilityTwin, store: Store, d: date) -> dict:
    s = find_snapshot(mt, d)
    if s is None:
        return {"found": False}
    zf = zipfile.ZipFile(io.BytesIO(mt.fetch(s.url)))
    sha = content_sha(zf)
    tables = []
    # Content-addressed: a feed with its _SUCCESS is never rewritten, even with --force.
    if not store.exists(f"gtfs/feeds/{sha}/_SUCCESS"):
        for i in zf.infolist():
            if i.filename.endswith(".parquet"):
                store.write_bytes(f"gtfs/feeds/{sha}/{i.filename.rsplit('/', 1)[-1]}", zf.read(i))
                tables.append(i.filename)
        store.write_json(f"gtfs/feeds/{sha}/_SUCCESS", {"sha": sha, "first_snapshot": s.date, "tables": tables})
    snap_local = datetime.fromtimestamp(s.timestamp, timezone.utc).astimezone(TZ).date()
    entry = {"service_date": d.isoformat(), "sha": sha, "snapshot": s.date,
             "snapshot_date": snap_local.isoformat(), "age_days": (d - snap_local).days}
    store.write_json(f"gtfs/dates/date={d.isoformat()}.json", entry)
    return {"found": True, **entry, "new_feed": bool(tables)}


def rebuild_index(store: Store) -> pl.DataFrame:
    entries = [store.read_json(p) for p in store.glob("gtfs/dates/date=*.json")]
    df = pl.DataFrame(entries, schema={"service_date": pl.Utf8, "sha": pl.Utf8, "snapshot": pl.Utf8,
                                       "snapshot_date": pl.Utf8, "age_days": pl.Int32})
    df = df.with_columns(pl.col("service_date").str.to_date(), pl.col("snapshot_date").str.to_date()).sort("service_date")
    store.write_parquet("gtfs/index.parquet", df)
    return df

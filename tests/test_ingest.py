"""Offline tests of the vd writers, frozen handling and idempotency on a recorded sample
(3 consecutive stib/vehicle-distance snapshots of 2025-03-18 ~11:00, lines 55 and 92)."""
from __future__ import annotations

import io
import json
import zipfile
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from stibms import gtfs, ingest, vd
from stibms.mobilitytwin import ParquetPart, Snapshot
from stibms.storage import Store

SAMPLE = json.loads((Path(__file__).parent / "data" / "vd_sample.json").read_text())
DAY = date(2025, 3, 18)


def sample_rows() -> pl.DataFrame:
    return pl.concat([vd.rows_from_json(s["timestamp"], s["data"]) for s in SAMPLE])


def bulk_bytes(snaps) -> bytes:
    """The sample in the /parquetized layout (lineId, directionId, pointId, distanceFromPoint, date)."""
    recs = [{**r, "date": datetime.fromisoformat(s["date"])} for s in snaps for r in s["data"]]
    df = pl.DataFrame(recs).select(
        pl.col("directionId").cast(pl.Utf8), pl.col("distanceFromPoint").cast(pl.Int64),
        pl.col("pointId").cast(pl.Utf8), pl.col("lineId").cast(pl.Utf8), pl.col("date"))
    buf = io.BytesIO()
    df.write_parquet(buf)
    return buf.getvalue()


def gtfs_zip() -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name in ("routes", "stops"):
            b = io.BytesIO()
            pl.DataFrame({f"{name[:-1]}_id": ["1", "2"]}).write_parquet(b)
            z.writestr(f"{name}.parquet", b.getvalue())
    return buf.getvalue()


class FakeMT:
    """Stands in for MobilityTwin: the first two polls come from a bulk Parquet file, the third
    only as JSON (not covered by the bulk file), plus one empty poll that only the index knows."""

    def __init__(self):
        self.bytes_downloaded = 0
        self.calls = 0
        s = SAMPLE
        self.empty_ts = s[-1]["timestamp"] + 20
        self.blobs = {f"json/{x['timestamp']}": json.dumps(x["data"]).encode() for x in s}
        self.blobs[f"json/{self.empty_ts}"] = b"[]"
        self.blobs["bulk"] = bulk_bytes(s[:2])
        self.blobs["gtfs"] = gtfs_zip()

    def index(self, endpoint, start, end):
        self.calls += 1
        if endpoint == "stib/vehicle-distance":
            ts = [x["timestamp"] for x in SAMPLE] + [self.empty_ts]
            return [Snapshot(t, "", f"json/{t}") for t in ts if start <= t <= end]
        if endpoint == "stib/gtfs-parquet":
            return [Snapshot(1742271600, "2025-03-18T04:20:00", "gtfs")]
        return []  # no punctuality

    def parquet_parts(self, component, start, end):
        return [ParquetPart("x/stib_vehicle_distance/bulk", SAMPLE[0]["timestamp"], SAMPLE[1]["timestamp"] + 1)]

    def fetch(self, url):
        key = "bulk" if url.endswith("/bulk") else url
        b = self.blobs[key]
        self.bytes_downloaded += len(b)
        return b


def test_build_frozen_and_rows():
    rows = sample_rows()
    t = [s["timestamp"] for s in SAMPLE]
    # A fourth poll repeating the third one is frozen; an empty poll and a repeat of it too.
    dup = rows.filter(pl.col("ts") == t[2]).with_columns(pl.lit(t[2] + 20, pl.Int64).alias("ts"))
    day = vd.build(pl.concat([rows, dup]), t + [t[2] + 20, t[2] + 40, t[2] + 60])
    sn = day.snaps
    assert sn["ts"].to_list() == t + [t[2] + 20, t[2] + 40, t[2] + 60]
    assert sn["frozen"].to_list() == [False, False, False, True, False, True]
    assert sn["n_rows"].to_list() == [31, 31, 31, 31, 0, 0]
    # Rows of the frozen poll are not repeated.
    assert day.rows.height == rows.height
    assert day.rows.schema == pl.Schema(vd.ROWS_SCHEMA)
    # Sorted by (line, ts).
    assert day.rows.select("line", "ts").equals(day.rows.select("line", "ts").sort("line", "ts"))


def test_frozen_ignores_row_order():
    rows = sample_rows()
    t = [s["timestamp"] for s in SAMPLE]
    shuffled = rows.filter(pl.col("ts") == t[0]).reverse().with_columns(pl.lit(t[0] + 5, pl.Int64).alias("ts"))
    day = vd.build(pl.concat([rows.filter(pl.col("ts") == t[0]), shuffled]))
    assert day.snaps["frozen"].to_list() == [False, True]


def test_row_groups_by_line(tmp_path):
    rows = vd.build(sample_rows()).rows
    bounds = vd.row_group_bounds(rows, target=10)
    store = Store(str(tmp_path))
    store.write_table("vd.parquet", vd.to_arrow(rows), row_group_bounds=bounds)
    f = pq.ParquetFile(tmp_path / "vd.parquet")
    assert f.metadata.num_row_groups == 2  # one per line here
    for g in range(f.metadata.num_row_groups):
        st = f.metadata.row_group(g).column(1).statistics
        assert st.min == st.max
    assert f.metadata.row_group(0).column(0).compression == "ZSTD"
    assert pl.read_parquet(tmp_path / "vd.parquet").equals(rows)


def test_ingest_idempotent(tmp_path):
    store, mt = Store(str(tmp_path)), FakeMT()
    rep = ingest.ingest_date(mt, store, DAY)
    assert rep["vd"]["polls"] == 4 and rep["vd"]["json_polls"] == 2 and rep["vd"]["empty"] == 1
    vdf = pl.read_parquet(tmp_path / f"raw/vd/date={DAY}/vd.parquet")
    assert vdf.height == sum(len(s["data"]) for s in SAMPLE)
    assert set(vdf["line"]) == {"55", "92"}
    assert (tmp_path / f"raw/_SUCCESS/date={DAY}.json").exists()
    assert rep["punctuality"] == {"found": False}
    sha = rep["gtfs"]["sha"]
    assert (tmp_path / f"gtfs/feeds/{sha}/routes.parquet").exists()
    idx = gtfs.rebuild_index(store)
    assert idx["sha"].to_list() == [sha] and idx["service_date"].to_list() == [DAY]

    before = {p: p.stat().st_mtime_ns for p in tmp_path.rglob("*") if p.is_file()}
    calls = mt.calls
    assert ingest.ingest_date(mt, store, DAY) is None  # skipped, nothing fetched or written
    assert mt.calls == calls
    assert {p: p.stat().st_mtime_ns for p in tmp_path.rglob("*") if p.is_file()} == before

    again = ingest.ingest_date(mt, store, DAY, force=True)
    assert pl.read_parquet(tmp_path / f"raw/vd/date={DAY}/vd.parquet").equals(vdf)
    assert again["gtfs"]["new_feed"] is False  # same content sha, feed not rewritten


def test_gtfs_sha_ignores_zip_bytes():
    a = zipfile.ZipFile(io.BytesIO(gtfs_zip()))
    b = zipfile.ZipFile(io.BytesIO(gtfs_zip()))
    assert gtfs.content_sha(a) == gtfs.content_sha(b)


def test_shard():
    days = list(range(10))
    parts = [ingest.shard(days, i, 3) for i in range(3)]
    assert sorted(sum(parts, [])) == days and parts[0] == [0, 3, 6, 9]


def test_not_closed_day_refused(tmp_path):
    with pytest.raises(RuntimeError, match="not closed"):
        ingest.ingest_date(FakeMT(), Store(str(tmp_path)), date.today())


def test_incomplete_bulk_falls_back_to_json(tmp_path):
    """A bulk file missing a line (vs the JSON snapshots of the same polls) is replaced by JSON."""
    mt = FakeMT()
    part = [{**s, "data": [r for r in s["data"] if str(r["lineId"]) == "55"]} for s in SAMPLE[:2]]
    mt.blobs["bulk"] = bulk_bytes(part)
    day = vd.fetch_day(mt, DAY)
    assert day.stats["bulk_dropped"] == ["bulk"]
    assert day.stats["bulk_checks"][0]["incomplete"]
    full = vd.build(sample_rows(), [s["timestamp"] for s in SAMPLE] + [mt.empty_ts])
    assert day.rows.height == full.rows.height and set(day.rows["line"].unique()) == {"55", "92"}
    ok = vd.fetch_day(FakeMT(), DAY)
    assert ok.stats["bulk_dropped"] == [] and not ok.stats["bulk_checks"][0]["incomplete"]


def test_partial_source_flag(tmp_path):
    from datetime import timedelta
    store = Store(str(tmp_path))
    d = date(2025, 3, 31)                              # a Monday
    for i in range(1, 15):
        x = d - timedelta(days=i)
        store.write_json(ingest.success_path(x), {"vd": {"lines": 78 if x.weekday() < 5 else 72}})
    assert ingest.source_check(store, d, 15) == {"partial_source": True, "lines_ref": 78}
    assert not ingest.source_check(store, d, 76)["partial_source"]
    assert not ingest.source_check(store, date(2024, 1, 1), 5)["partial_source"]   # no history

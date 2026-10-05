"""A small bucket built from the microsegments simulator: line S1, 8 service days from Monday
2025-05-26 (Ascension on Thursday 29), a long outage on day 2 (Wednesday 28), the line cut short from
day 5 (Saturday 31) on, two months (May / June) and two GTFS feeds."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

START = dt.date(2025, 5, 26)
DAYS = 8
OUTAGE_DAY = dt.date(2025, 5, 28)
CHANGE_DAY = 5


def _gtfs_parquet(src: Path, dst: Path) -> None:
    from gtfs_parquet.parse import parse_gtfs_file
    dst.mkdir(parents=True, exist_ok=True)
    for f in src.glob("*.txt"):
        parse_gtfs_file(f, f.name).write_parquet(dst / f"{f.stem}.parquet")
    (dst / "_SUCCESS").write_text("{}")


def _punctuality(ev: pl.DataFrame) -> pl.DataFrame:
    """STOP_EVENT -> the raw stib/punctuality columns stored by the ingest."""
    return ev.select(
        pl.col("trip_key").str.split(":").list.get(0).alias("journey_id"),
        pl.col("trip_key").str.split(":").list.get(1).alias("trip_id"),
        pl.col("service_date").dt.strftime("%Y%m%d").alias("start_date"),
        pl.col("stop_sequence"), pl.col("stop_id"), pl.col("route_id"),
        pl.col("arrival").dt.replace_time_zone(None).alias("arrival_time"),
        pl.col("departure").dt.replace_time_zone(None).alias("departure_time"),
        pl.col("observed"),
    )


@pytest.fixture(scope="session")
def sim_root(tmp_path_factory) -> Path:
    pytest.importorskip("microsegments")
    from microsegments.io.timeutil import add_local_time
    from microsegments.simulate import simulate

    root = tmp_path_factory.mktemp("bucket")
    r = simulate(seed=3, days=DAYS, start=START, change_day=CHANGE_DAY,
                 outages=[((OUTAGE_DAY - START).days, "05:00", 15 * 60)], frozen=[])
    # Two feeds with the same content but different bytes (STIB republishes daily): sha A before the
    # change, sha B from it, so the network spans two feeds.
    shas = {}
    for name in ("a", "b"):
        sha = hashlib.sha256(f"sim-{name}".encode()).hexdigest()
        _gtfs_parquet(Path(r.gtfs_dir), root / "gtfs" / "feeds" / sha)
        shas[name] = sha
    raw = add_local_time(r.raw.with_columns(
        (pl.col("ts") * 1_000_000).cast(pl.Datetime("us")).dt.replace_time_zone("UTC").alias("_t")),
        r.tz, ts_col="_t")
    sn = add_local_time(r.snapshots, r.tz)
    pu = _punctuality(r.stop_events)
    idx = []
    for i, d in enumerate(r.dates):
        day = root / "raw" / "vd" / f"date={d}"
        day.mkdir(parents=True)
        raw.filter(pl.col("service_date") == d).select(
            "ts", "line", "dir", "point", pl.col("dist").cast(pl.Int32)).sort("line", "ts").write_parquet(day / "vd.parquet")
        sn.filter(pl.col("service_date") == d).select(
            pl.col("ts").dt.epoch("s"), "n_rows", "frozen").write_parquet(day / "snaps.parquet")
        p = root / "raw" / "punctuality" / f"date={d}"
        p.mkdir(parents=True)
        pu.filter(pl.col("start_date") == f"{d:%Y%m%d}").write_parquet(p / "p.parquet")
        s = (root / "raw" / "_SUCCESS")
        s.mkdir(parents=True, exist_ok=True)
        (s / f"date={d}.json").write_text(json.dumps({"date": str(d)}))
        idx.append({"service_date": d, "sha": shas["a" if i < CHANGE_DAY else "b"], "snapshot": str(d),
                    "snapshot_date": d, "age_days": 0})
    pl.DataFrame(idx).write_parquet(root / "gtfs" / "index.parquet")
    return root


@pytest.fixture(scope="session")
def derived_root(sim_root) -> Path:
    from stibms import ms
    if not ms.have_locate():
        pytest.skip("microsegments.locate / tracks not available")
    from stibms.data import Data
    from stibms.derive import derive_month
    data = Data(str(sim_root))
    for m in ("2025-05", "2025-06"):
        rep = derive_month(data, m)
        assert not rep["failed"], rep["failed"]
    return sim_root


@pytest.fixture()
def client(derived_root, monkeypatch):
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from stibms import api
    monkeypatch.setenv("MS_BUCKET", str(derived_root))
    api.state.reset()
    return TestClient(api.app)

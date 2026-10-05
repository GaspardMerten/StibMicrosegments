"""Live check against MobilityTwin (needs MOBILITYTWIN_TOKEN): pytest -m network.

Line 55 of 2025-03-18 must equal the prototype's data2025/stib/vd55/20250318.parquet on every
non-frozen poll; the prototype's rows at frozen polls equal the last non-frozen poll's rows."""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from stibms import ingest
from stibms.mobilitytwin import MobilityTwin
from stibms.storage import Store

PROTO = Path(os.environ.get("VD55_DIR", Path.home() / "Documents/Dev/CoDE/BrusselsTransitStuck/data2025/stib/vd55"))


@pytest.mark.network
def test_line55_matches_prototype(tmp_path):
    if not (PROTO / "20250318.parquet").exists():
        pytest.skip("prototype vd55 not available")
    store = Store(str(tmp_path))
    ingest.ingest_date(MobilityTwin(), store, date(2025, 3, 18))
    vd = pl.read_parquet(tmp_path / "raw/vd/date=2025-03-18/vd.parquet")
    sn = pl.read_parquet(tmp_path / "raw/vd/date=2025-03-18/snaps.parquet")
    proto = pl.read_parquet(PROTO / "20250318.parquet").with_columns(pl.col("dist").cast(pl.Int32))
    psn = pl.read_parquet(PROTO / "20250318.snaps.parquet")
    assert sn["ts"].to_list() == psn["ts"].sort().to_list()
    k = ["ts", "dir", "point", "dist"]
    frozen = sn.filter("frozen")["ts"].implode()
    ours = vd.filter(pl.col("line") == "55").select(k).sort(k)
    assert ours.equals(proto.filter(~pl.col("ts").is_in(frozen)).select(k).sort(k))

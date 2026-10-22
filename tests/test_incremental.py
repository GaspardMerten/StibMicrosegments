"""Incremental derive (``stibms.increment``) and the per-line network copies (``stibms.linenets``) on
a copy of the simulated bucket (conftest), so the shared derived bucket is left alone."""
from __future__ import annotations

import datetime as dt
import json
import shutil

import polars as pl
import pytest

from stibms.data import Data, derived

from .conftest import START

MAY = "2025-05"     # 2025-05-26 .. 31: the month closes on its 6th day


@pytest.fixture()
def raw_root(sim_root, tmp_path):
    """The simulated raw bucket, nothing derived."""
    pytest.importorskip("microsegments")
    root = tmp_path / "bucket"
    shutil.copytree(sim_root, root, ignore=shutil.ignore_patterns("derived", "results", "daytrips"))
    return root


def _hide(root, days):
    """Hide ingested days (their raw _SUCCESS); returns a function putting them back."""
    kept = {d: (root / f"raw/_SUCCESS/date={d}.json").read_bytes() for d in days}
    for d in days:
        (root / f"raw/_SUCCESS/date={d}.json").unlink()
    return lambda: [(root / f"raw/_SUCCESS/date={d}.json").write_bytes(b) for d, b in kept.items()]


def _derive(root, **kw):
    from stibms.derive import derive_month
    return derive_month(Data(str(root)), MAY, **kw)


def _read(root, kind):
    return pl.read_parquet(root / derived(f"{kind}/line=S1/month={MAY}.parquet"))


def _cube(root):
    c = pl.read_parquet(root / derived(f"linkcube/month={MAY}/cube.parquet"))
    return c.sort(c.columns)


def test_incremental_nights_then_full_at_month_close(raw_root, tmp_path):
    days = [START + dt.timedelta(days=i) for i in range(6)]
    back = _hide(raw_root, days[3:])
    assert _derive(raw_root)["mode"] == "full"                       # never derived
    placed0 = _read(raw_root, "placed")
    back()
    _hide(raw_root, days[5:])                                       # two new days, 5 < 2 x 3
    rep = _derive(raw_root)
    assert rep["mode"] == "incremental" and rep["days_placed"] == [str(days[3]), str(days[4])]
    assert rep["full_days"] == 3 and len(rep["dates"]) == 5 and rep["lines"] == 1
    placed1 = _read(raw_root, "placed")
    old = pl.col("service_date") < days[3]
    assert placed1.filter(old).equals(placed0.filter(old))            # earlier days untouched
    assert placed1.filter(~old)["service_date"].unique().sort().to_list() == days[3:5]
    assert _derive(raw_root)["mode"] == "current"                    # nothing new: no-op
    # against a full derive of the same days: the same rows, positions within a few metres (the new
    # days use the feed lengths of 3 days, the full run those of 5), the same cube
    ref = tmp_path / "ref"
    shutil.copytree(raw_root, ref, ignore=shutil.ignore_patterns("derived"))
    assert _derive(ref)["mode"] == "full"
    a, b = (_read(r, "placed").sort("ts", "raw_dist_m", "link_key") for r in (raw_root, ref))
    assert a.drop("pos_m", "s_m", "track_id").equals(b.drop("pos_m", "s_m", "track_id"))
    assert (a["pos_m"] - b["pos_m"]).abs().quantile(0.99) < 5.0
    pa, pb = (_read(r, "passages") for r in (raw_root, ref))
    assert pa.sort(pa.columns).equals(pb.sort(pb.columns))
    ca, cb = _cube(raw_root), _cube(ref)
    assert ca.height == cb.height and abs(ca["o_day_r"].sum() / cb["o_day_r"].sum() - 1) < 0.01
    back()                                                          # the last calendar day: full again
    rep = _derive(raw_root)
    assert rep["mode"] == "full" and rep["why"] == "month closes" and rep["full_days"] == 6
    full = tmp_path / "full"                                        # a closed month is the full derive
    shutil.copytree(raw_root, full, ignore=shutil.ignore_patterns("derived"))
    _derive(full)
    for kind in ("placed", "passages"):
        assert _read(raw_root, kind).equals(_read(full, kind)), kind
    assert _cube(raw_root).equals(_cube(full))


def test_reingested_day_and_full_reasons(raw_root, monkeypatch):
    from stibms import increment, ms
    first = _derive(raw_root)
    assert first["mode"] == "full" and first["full_days"] == 6
    data = Data(str(raw_root))
    dates = data.month_ingested(MAY)
    prev = data.store.read_json(derived(f"_SUCCESS/month={MAY}.json"))
    assert increment.plan(data, MAY, dates, prev)["mode"] == "current"
    p = raw_root / f"raw/_SUCCESS/date={START + dt.timedelta(days=1)}.json"
    p.write_text(json.dumps({"written_at": "2999-01-01T00:00:00+00:00"}))   # re-ingested after the derive
    assert increment.plan(data, MAY, dates, prev) == {"mode": "incremental", "days": [START + dt.timedelta(days=1)],
                                                      "why": "1 new day(s)"}
    assert increment.plan(data, MAY, dates, prev, full=True)["why"] == "asked"
    assert increment.plan(data, MAY, dates, {k: v for k, v in prev.items() if k != "full_days"})["mode"] == "full"
    monkeypatch.setattr(ms, "version", lambda: "0.0.0-other")
    assert increment.plan(data, MAY, dates, prev)["why"] == "package or algo changed"


def test_flen_and_aliases_given_to_new_days():
    from stibms import increment, ms
    pytest.importorskip("microsegments")
    month = pl.DataFrame({"pattern_uid": ["p", "p", "p"], "link_idx": [0, 1, 2], "flen": [110.0, 220.0, 330.0],
                          "n": [500, 5, 300]})
    day = pl.DataFrame({"pattern_uid": ["p", "p", "p", "q"], "link_idx": [0, 1, 2, 0], "flen": [100.0, 200.0, 300.0, 50.0]})
    f = increment.with_feed_lengths(ms.config(), month, day).input.linear.feed_length.sort("pattern_uid", "link_idx")
    assert f["flen"].to_list() == [110.0, 200.0, 330.0, 50.0]   # the month's when learned on enough rows
    cfg = increment.with_aliases(ms.config(), {"9943": "5501"})
    assert cfg.input.linear.terminus_aliases == {"9943": "5501"} and not ms.config().input.linear.terminus_aliases


def test_linenets_same_versions_and_fallback(derived_root, monkeypatch):
    from stibms import linenets, versions
    data = Data(str(derived_root))
    man = json.loads((derived_root / linenets.MANIFEST).read_text())
    assert set(man["shas"]) == set(data.gtfs_index()["sha"].unique()) and (derived_root / linenets.rel("S1", "links")).exists()
    a, b = START, START + dt.timedelta(days=7)
    dates = data.month_ingested(MAY) + data.month_ingested("2025-06")
    net, ref = linenets.network(data, "S1", dates), data.network("S1", dates, geometry=False)
    for k in ("patterns", "pattern_days", "links"):
        assert getattr(net, k).equals(getattr(ref, k)), k
    new = versions.versions(data, "S1", a, b)
    monkeypatch.setattr(linenets, "network", lambda *x, **kw: None)
    assert json.dumps(versions.versions(Data(str(derived_root)), "S1", a, b)) == json.dumps(new)
    monkeypatch.undo()
    assert linenets.network(data, "nope", dates).pattern_days.height == 0


def test_linenets_forget_falls_back(raw_root):
    from stibms import linenets
    _derive(raw_root)
    data = Data(str(raw_root))
    dates = data.month_ingested(MAY)
    assert linenets.network(data, "S1", dates) is not None
    linenets.forget(data, data.gtfs_index()["sha"][0])        # a feed being rebuilt: read the per-feed files
    assert linenets.network(Data(str(raw_root)), "S1", dates) is None
    from stibms.derive import main
    assert main(["--local", str(raw_root), "--linenets-only"]) == 0     # the backfill job's command
    assert linenets.network(Data(str(raw_root)), "S1", dates) is not None

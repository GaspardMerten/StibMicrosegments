"""Derive + API on the simulated bucket (conftest). Skips when microsegments (or its locate /
tracks modules) is not installed."""
from __future__ import annotations

import datetime as dt
import io

import polars as pl
import pytest

from stibms import holidays
from stibms.data import derived

from .conftest import CHANGE_DAY, OUTAGE_DAY, START

Q = "line=S1&from=2025-05-26&to=2025-06-02"


def test_holidays():
    assert holidays.easter(2024) == dt.date(2024, 3, 31)
    assert holidays.easter(2025) == dt.date(2025, 4, 20)
    assert holidays.easter(2027) == dt.date(2027, 3, 28)
    h = holidays.holidays(2025)
    assert h[dt.date(2025, 5, 29)] == "Ascension"
    assert h[dt.date(2025, 6, 9)] == "Lundi de Pentecôte"
    assert len(h) == 10
    assert list(holidays.between(dt.date(2025, 5, 1), dt.date(2025, 5, 31))) == [dt.date(2025, 5, 1), dt.date(2025, 5, 29)]


def test_derive_outputs(derived_root):
    d = derived_root / derived()
    assert (d / "linkkeys.parquet").exists()
    assert len(list((d / "patterns").glob("sha=*/_SUCCESS"))) == 2
    for m in ("2025-05", "2025-06"):
        assert (d / "coverage" / f"month={m}.parquet").exists()
        placed = pl.read_parquet(d / "placed" / "line=S1" / f"month={m}.parquet")
        assert placed.height > 1000 and placed["count"].all()
        assert placed["service_date"].dt.strftime("%Y-%m").unique().to_list() == [m]
        assert pl.read_parquet(d / "passages" / "line=S1" / f"month={m}.parquet").height > 0


def test_derive_is_idempotent(derived_root):
    from stibms.data import Data
    from stibms.derive import derive_month
    p = derived_root / derived("placed/line=S1/month=2025-06.parquet")
    before = pl.read_parquet(p)
    keys = pl.read_parquet(derived_root / derived("linkkeys.parquet")).height
    derive_month(Data(str(derived_root)), "2025-06")
    assert pl.read_parquet(p).equals(before)
    assert pl.read_parquet(derived_root / derived("linkkeys.parquet")).height == keys


def test_health_and_lines(client):
    assert client.get("/api/health").json()["ok"]
    r = client.get("/api/lines").json()
    s1 = next(x for x in r["lines"] if x["line"] == "S1")
    assert s1["available"]["from"] == "2025-05-26" and s1["available"]["to"] == "2025-06-02"
    assert r["ingested"] == {"from": "2025-05-26", "to": "2025-06-02"}


def test_coverage_flags_outage_and_holiday(client):
    days = {d["date"]: d for d in client.get(f"/api/coverage?{Q}").json()["days"]}
    assert len(days) == 8
    assert days[str(OUTAGE_DAY)]["status"] in ("low_coverage", "no_data")
    assert days["2025-05-26"]["status"] == "ok"
    assert days["2025-05-29"]["holiday"] == "Ascension"


def test_versions(client):
    v = client.get(f"/api/versions?{Q}").json()["versions"]
    d0 = [x for x in v if x["direction_id"] == 0]
    assert len(d0) == 2
    cut = START + dt.timedelta(days=CHANGE_DAY)
    assert {x["first"] for x in d0} == {str(START), str(cut)}
    assert sum(x["n_stops"] for x in d0 if x["first"] == str(cut)) < sum(x["n_stops"] for x in d0 if x["first"] == str(START))


def test_analysis_contract(client):
    r = client.get(f"/api/analysis?{Q}&dow=0-6")
    assert r.status_code == 200, r.text
    assert r.headers["x-cache"] == "computed"
    assert "max-age=86400" in r.headers["cache-control"]
    c = r.json()
    assert c["v"] == 1 and c["line"] == "S1" and c["dirs"]
    ex = {e["date"]: e["reason"] for e in c["period"]["excluded"]}
    assert ex[str(OUTAGE_DAY)] != "holiday"
    assert ex["2025-05-29"] == "holiday"
    assert c["period"]["days"] == 6
    d0 = next(d for d in c["dirs"] if d["dir"] == 0)
    assert len(d0["wk"]["obs"]) == 7 and len(d0["wk"]["obs"][0]) == len(c["hours"])
    assert len(d0["wk"]["obs"][0][0]) == len(d0["seg"]["key"])
    # cut links are present on part of the days only
    assert any(f & 32 for f in d0["seg"]["flags"])
    assert len([v for v in c["versions"] if v["dir"] == 0]) == 2
    r2 = client.get(f"/api/analysis?{Q}&dow=0-6")
    assert r2.headers["x-cache"] == "memory" and r2.content == r.content


def test_analysis_bucket_cache(client, derived_root):
    from stibms import api
    client.get(f"/api/analysis?{Q}&seg=50")
    assert list((derived_root / "results").rglob("analysis-*.json.gz"))
    api.state.blobs.clear()
    assert client.get(f"/api/analysis?{Q}&seg=50").headers["x-cache"] == "bucket"


def test_segment_length_changes_segments(client):
    a = client.get(f"/api/analysis?{Q}&seg=20").json()
    b = client.get(f"/api/analysis?{Q}&seg=60").json()
    na = sum(len(d["seg"]["key"]) for d in a["dirs"])
    nb = sum(len(d["seg"]["key"]) for d in b["dirs"])
    assert na > 2 * nb


def test_downloads_and_hotspots(client):
    r = client.get(f"/api/analysis.csv?{Q}")
    assert r.status_code == 200 and r.text.startswith("pattern_uid,")
    df = pl.read_parquet(io.BytesIO(client.get(f"/api/analysis.parquet?{Q}").content))
    assert {"seg_key", "obs_per_h", "from_name", "line"} <= set(df.columns)
    h = client.get(f"/api/hotspots?{Q}").json()
    assert "hotspots" in h
    assert client.get(f"/api/hotspots.csv?{Q}").status_code == 200


def test_tune(client):
    r = client.get(f"/api/tune?{Q}&lengths=20,40&B=10")
    assert r.status_code == 200, r.text
    t = r.json()
    assert t["recommended"] in (20.0, 40.0) and len(t["table"]) == 2


def test_errors(client):
    assert client.get("/api/analysis?line=S1&from=2025-06-02&to=2025-05-26").status_code == 422
    assert client.get(f"/api/analysis?{Q}&seg=1").status_code == 422
    assert client.get(f"/api/analysis?{Q}&dow=9").status_code == 422
    assert client.get("/api/analysis?line=NOPE&from=2025-05-26&to=2025-05-27").status_code == 404
    assert client.get("/api/analysis?line=S1&from=2024-01-01&to=2024-01-05").status_code == 404


def test_pages(client):
    assert client.get("/").status_code == 200
    v = client.get("/view")
    if v.status_code == 200:   # package template present: data comes from the fetch shim
        assert 'id="ms-main"' in v.text and "/api/analysis" in v.text and "__DATA__" not in v.text
    else:
        assert v.status_code == 404


def test_query_key_is_canonical():
    pytest.importorskip("microsegments")
    from stibms.analysis import Query
    a = Query("55", dt.date(2025, 3, 1), dt.date(2025, 3, 31), dow=(4, 0, 1, 2, 3))
    b = Query("55", dt.date(2025, 3, 1), dt.date(2025, 3, 31), dow=(0, 1, 2, 3, 4))
    assert a.key() == b.key() != Query("55", dt.date(2025, 3, 1), dt.date(2025, 3, 31), seg=20).key()


def test_line_absent_hours():
    pytest.importorskip("microsegments")
    from stibms.analysis import line_absent
    days = [dt.date(2025, 3, 3) + dt.timedelta(days=i) for i in range(4)]
    rows = [(d, h) for d in days for h in range(6, 22) for _ in range(30) if not (d == days[2] and h < 12)]
    placed = pl.DataFrame(rows, schema={"service_date": pl.Date, "hour": pl.Int8}, orient="row")
    ab = line_absent(placed, days)
    assert ab["service_date"].unique().to_list() == [days[2]]
    assert sorted(ab["hour"].to_list()) == list(range(6, 12))


def test_compare(client):
    r = client.get("/api/compare?line=S1&a=2025-05-26..2025-05-29&b=2025-05-30..2025-06-02&dow=0-6&seg=50")
    assert r.status_code == 200, r.text
    c = r.json()
    assert c["compare"]["a"]["first"] >= "2025-05-26" and c["compare"]["b"]["last"] <= "2025-06-02"
    d0 = c["compare"]["dirs"][0]
    assert len(d0["d"]) == len(c["compare"]["cols"]) and len(d0["d"][0]) == len(d0["seg_key"])
    assert c["query"]["a"] == ["2025-05-26", "2025-05-29"]
    assert client.get("/api/compare?line=S1&a=2025-05-26..2025-05-29&b=2025-05-30..2025-06-02&dow=0-6&seg=50").headers["x-cache"] == "memory"
    assert client.get("/api/compare?line=S1&a=2025-05-29..2025-05-26&b=2025-05-30..2025-06-02").status_code == 422
    assert client.get("/api/compare?line=S1&a=2024-01-01..2024-01-05&b=2025-05-30..2025-06-02").status_code == 404


def test_ranking(client, monkeypatch):
    monkeypatch.setenv("MS_RANK_BUDGET", "0")      # one line per poll: exercise the progress path
    q = "/api/ranking?from=2025-05-26&to=2025-06-02&dow=0-6&top=10"
    r = client.get(q)
    for _ in range(5):
        if r.status_code == 200:
            break
        assert r.status_code == 202 and r.json()["status"] == "running"
        r = client.get(q)
    assert r.status_code == 200, r.text
    res = r.json()
    assert res["status"] == "done" and res["lines_used"] == 1
    s = res["stretches"]
    assert 0 < len(s) <= 10 and s[0]["rank"] == 1
    assert s[0]["veh_h_day"] >= s[-1]["veh_h_day"]
    assert s[0]["lines"][0]["line"] == "S1" and s[0]["c"]
    assert client.get(q.replace("top=10", "top=10&mode=metro")).json()["lines_used"] == 0


def test_status_lines_default_and_not_derived(client, derived_root):
    st = client.get("/api/status").json()
    assert st["derived_months"] == ["2025-05", "2025-06"] and st["pending_months"] == []
    s1 = next(x for x in client.get("/api/lines").json()["lines"] if x["line"] == "S1")
    assert s1["available"]["default"] == {"from": "2025-05-26", "to": "2025-05-31"}   # complete months only
    from stibms import api
    p = derived_root / derived("_SUCCESS/month=2025-06.json")
    keep = p.read_bytes()
    p.unlink()
    try:
        api.state.reset()
        days = {d["date"]: d["status"] for d in client.get(f"/api/coverage?{Q}").json()["days"]}
        assert days["2025-06-01"] == "not_derived" and days["2025-05-26"] == "ok"
    finally:
        p.write_bytes(keep)
        api.state.reset()


def test_page_paths(client):
    for path in ("/", "/comparer", "/classement"):
        assert client.get(path).status_code == 200


def test_coverage_only_rewrite(derived_root):
    from stibms.data import Data
    from stibms.derive import rewrite_coverage
    import json
    rel = derived_root / derived("_SUCCESS/month=2025-06.json")
    before = json.loads(rel.read_text())
    cov = pl.read_parquet(derived_root / derived("coverage/month=2025-06.parquet"))
    rewrite_coverage(Data(str(derived_root)), "2025-06")
    after = json.loads(rel.read_text())
    assert after["lines"] == before["lines"] and after["coverage_rewritten_at"] == after["written_at"]
    assert pl.read_parquet(derived_root / derived("coverage/month=2025-06.parquet")).equals(cov)

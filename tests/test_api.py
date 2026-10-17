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
    assert "max-age=3600" in r.headers["cache-control"]
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
    # microsegments >= 0.2.7: when no candidate passes the gates, the default 30 m is kept
    assert len(t["table"]) == 2
    assert t["recommended"] in (20.0, 40.0) or (t["recommended"] == 30.0 and "default" in t["rule"])


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


def test_ranking(client):
    q = "/api/ranking?from=2025-05-26&to=2025-06-02&dow=0-6&top=10"
    r = client.get(q)
    assert r.status_code == 200, r.text
    res = r.json()
    assert res["status"] == "done" and res["lines_used"] == 1 and res["zones"] == "running"
    s = res["stretches"]
    assert 0 < len(s) <= 10 and s[0]["rank"] == 1
    assert s[0]["veh_h_day"] >= s[-1]["veh_h_day"]
    assert s[0]["lines"][0]["line"] == "S1" and s[0]["c"]
    assert {"veh_h_day_run", "veh_h_day_stop", "reasons", "link_keys"} <= set(s[0])
    assert "excluded" in res
    both = client.get(q + "&stops=true&terminus=true").json()
    assert both["zones"] == "all" and both["excluded"]["n"] == 0
    assert client.get(q.replace("top=10", "top=10&mode=metro")).json()["lines_used"] == 0


def test_linkcube_written(derived_root):
    c = pl.read_parquet(derived_root / derived("linkcube/month=2025-05/cube.parquet"))
    assert {"line", "service_date", "link_key", "o_day_r", "o_day_s", "l_day_r", "p_day", "p_eve"} <= set(c.columns)
    assert c["line"].unique().to_list() == ["S1"] and c["p_day"].sum() > 0
    lk = pl.read_parquet(derived_root / derived("linkcube/month=2025-05/links.parquet"))
    assert set(lk["term"].unique().to_list()) <= {0, 1, 2} and (lk["term"] == 1).any()


def test_regulation_overlap():
    from stibms.ranking import overlap
    a = [[4.35, 50.85], [4.352, 50.85]]                 # ~140 m east-west
    b = [[4.3505, 50.85005], [4.352, 50.85005]]         # same street, 5 m north, shorter
    c = [[4.35, 50.852], [4.352, 50.852]]               # 220 m north
    assert overlap(a, b) == 1.0 and overlap(a, c) == 0.0


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
    for path in ("/", "/comparer", "/classement", "/methode"):
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


def test_schedule_classify():
    from stibms import schedule
    days = [dt.date(2025, 2, 3) + dt.timedelta(days=i) for i in range(70)]
    hol_weeks = {d for d in days if dt.date(2025, 2, 24) <= d <= dt.date(2025, 3, 7)}
    strike = dt.date(2025, 3, 31)
    trips = {}
    for d in days:
        t = 19500 if d.weekday() < 5 else (12000 if d.weekday() == 5 else 9000)
        if d in hol_weeks and d.weekday() < 5:
            t = 17500
        if d == strike:
            t = 12000
        trips[d] = t
    c = schedule.classify(trips, days[0], days[-1])
    assert {d for d, k in c.items() if k == "school_holiday"} == {d for d in hol_weeks if d.weekday() < 5}
    assert c[strike] == "reduced_service"
    assert not any(d.weekday() >= 5 for d in c)          # Saturdays and Sundays keep their own level
    # a public holiday (Easter Monday 2025-04-21) is left to the holiday category
    trips[dt.date(2025, 4, 21)] = 11500
    assert dt.date(2025, 4, 21) not in schedule.classify(trips, days[0], dt.date(2025, 4, 21))


def test_feed_trips():
    from stibms import schedule
    cal = pl.DataFrame({"service_id": ["wk", "sa"], "monday": [1, 0], "tuesday": [1, 0], "wednesday": [1, 0],
                        "thursday": [1, 0], "friday": [1, 0], "saturday": [0, 1], "sunday": [0, 0],
                        "start_date": [dt.date(2025, 3, 3)] * 2, "end_date": [dt.date(2025, 3, 9)] * 2})
    cd = pl.DataFrame({"service_id": ["wk", "sa"], "date": [dt.date(2025, 3, 5), dt.date(2025, 3, 9)],
                       "exception_type": [2, 1]}, schema_overrides={"exception_type": pl.Int8})
    trips = pl.DataFrame({"service_id": ["wk"] * 3 + ["sa"]})
    t = dict(schedule.feed_trips(cal, cd, trips).iter_rows())
    assert t[dt.date(2025, 3, 3)] == 3 and dt.date(2025, 3, 5) not in t
    assert t[dt.date(2025, 3, 8)] == 1 and t[dt.date(2025, 3, 9)] == 1


def test_exclusion_periods_canonical():
    pytest.importorskip("microsegments")
    from stibms import schedule
    from stibms.analysis import Query
    d = dt.date
    assert schedule.norm_periods([(d(2025, 3, 10), d(2025, 3, 12)), (d(2025, 3, 1), d(2025, 3, 5)), (d(2025, 3, 6), d(2025, 3, 7)),
                                  (d(2024, 1, 1), d(2024, 1, 2))], d(2025, 3, 3), d(2025, 3, 31)) == (
        (d(2025, 3, 3), d(2025, 3, 7)), (d(2025, 3, 10), d(2025, 3, 12)))
    base = Query("55", d(2025, 3, 1), d(2025, 3, 31))
    # defaults leave the key as it was; periods outside the queried days change nothing
    assert "school" not in base.canonical() and "exclude" not in base.canonical()
    assert Query("55", d(2025, 3, 1), d(2025, 3, 31), school="include", exclude=((d(2024, 1, 1), d(2024, 1, 3)),)).key() == base.key()
    a = Query("55", d(2025, 3, 1), d(2025, 3, 31), exclude=((d(2025, 3, 3), d(2025, 3, 4)), (d(2025, 3, 5), d(2025, 3, 5))))
    b = Query("55", d(2025, 3, 1), d(2025, 3, 31), exclude=((d(2025, 3, 3), d(2025, 3, 5)),))
    assert a.key() == b.key() != base.key()
    assert a.canonical()["exclude"] == [["2025-03-03", "2025-03-05"]]
    assert Query("55", d(2025, 3, 1), d(2025, 3, 31), school="exclude").key() != base.key()
    with pytest.raises(ValueError):
        Query("55", d(2025, 3, 1), d(2025, 3, 31), school="maybe")


def test_analysis_excluded_periods(client):
    plain = client.get(f"/api/analysis?{Q}&dow=0-6").json()
    r = client.get(f"/api/analysis?{Q}&dow=0-6&exclude=2025-05-26..2025-05-27,2025-06-02&school=exclude&reduced=exclude")
    assert r.status_code == 200, r.text
    c = r.json()
    ex = {e["date"]: e["reason"] for e in c["period"]["excluded"]}
    assert ex["2025-05-26"] == ex["2025-05-27"] == ex["2025-06-02"] == "user_period"
    assert ex["2025-05-29"] == "holiday"
    assert c["period"]["days"] < plain["period"]["days"]
    assert c["query"]["exclude"] == [["2025-05-26", "2025-05-27"], ["2025-06-02", "2025-06-02"]]
    assert c["query"]["school"] == "exclude" and "school" not in plain["query"]
    # a period outside the queried days is the plain query (same cache entry)
    assert client.get(f"/api/analysis?{Q}&dow=0-6&exclude=2024-01-01..2024-01-31").headers["x-cache"] == "memory"
    for bad in ("exclude=2025-05-27..2025-05-26", "exclude=hier", "school=oui", "reduced=1",
                "exclude=2025-05-27..", "exclude=..2025-05-27"):
        assert client.get(f"/api/analysis?{Q}&{bad}").status_code == 422, bad


def test_compare_excluded_periods(client):
    r = client.get("/api/compare?line=S1&a=2025-05-26..2025-05-29&b=2025-05-30..2025-06-02&dow=0-6&seg=50"
                   "&exclude=2025-05-27,2025-06-01")
    assert r.status_code == 200, r.text
    c = r.json()["compare"]
    assert {"date": "2025-05-27", "reason": "user_period"} in c["a"]["excluded"]
    assert {"date": "2025-06-01", "reason": "user_period"} in c["b"]["excluded"]


def test_ranking_exclusions(client):
    q = "/api/ranking?from=2025-05-26&to=2025-06-02&dow=0-6&top=10"
    plain = client.get(q).json()
    assert plain["days_excluded"] == {"holiday": 1}
    r = client.get(q + "&exclude=2025-05-26&school=exclude").json()     # links need 5 usable days (MIN_DAYS)
    assert r["days_excluded"]["user_period"] == 1 and r["query"]["exclude"] == [["2025-05-26", "2025-05-26"]]
    assert r["stretches"] and r["stretches"][0]["lines"][0]["n_days"] < plain["stretches"][0]["lines"][0]["n_days"]
    assert client.get(q + "&exclude=2025-06-02..2025-05-26").status_code == 422
    # only days with data count: the period before the first ingested day adds nothing
    early = client.get("/api/ranking?from=2025-05-20&to=2025-06-02&dow=0-6&top=10&exclude=2025-05-20..2025-05-25").json()
    assert "user_period" not in early["days_excluded"]


def test_versions_reference_and_changes(client):
    v = client.get(f"/api/versions?{Q}").json()["versions"]
    d0 = [x for x in v if x["direction_id"] == 0]
    ref = [x for x in d0 if x["ref"]]
    assert len(ref) == 1 and ref[0]["n_stops"] == max(x["n_stops"] for x in d0)
    short = next(x for x in d0 if not x["ref"])
    assert short["removed"] and not set(short["removed"]) & set(short["stops"])
    assert all(x["stops"] for x in v)
    assert "service" in client.get(f"/api/coverage?{Q}").json()["days"][0]


def _weekdays(a, n):
    return [a + dt.timedelta(days=i) for i in range(n)]


def test_schedule_robustness():
    from collections import Counter
    from stibms import schedule
    d = dt.date
    # a lasting service increase (network change, return to school): the lighter side is not a holiday
    days = _weekdays(d(2025, 1, 6), 200)
    step = {x: (19000 if x < d(2025, 3, 3) else 20600) if x.weekday() < 5 else 12000 for x in days}
    assert schedule.classify(step, days[0], days[-1]) == {}
    # a summer timetable well below 0.75 of a school day is a holiday run, not a strike...
    days = _weekdays(d(2025, 4, 1), 240)
    summer = {x: (14000 if d(2025, 7, 1) <= x <= d(2025, 8, 29) else 19500) if x.weekday() < 5 else 12000 for x in days}
    c = schedule.classify(summer, d(2025, 6, 1), d(2025, 9, 30))
    assert Counter(c.values()) == {"school_holiday": 42} and min(c) == d(2025, 7, 1) and max(c) == d(2025, 8, 29)
    # ... while a strike inside it, or alone, is reduced service
    summer[d(2025, 7, 16)] = 9000
    summer[d(2025, 9, 16)] = 12000
    c = schedule.classify(summer, d(2025, 6, 1), d(2025, 9, 30))
    assert c[d(2025, 7, 16)] == "reduced_service" and c[d(2025, 9, 16)] == "reduced_service"
    # guard: when most weekdays around a date would be low, nothing is flagged
    days = _weekdays(d(2025, 1, 6), 120)
    odd = {x: (19500 if x < d(2025, 1, 20) else 17000) if x.weekday() < 5 else 12000 for x in days}
    odd.update({x: 19500 for x in days if d(2025, 4, 21) <= x and x.weekday() < 5})
    assert not schedule.classify(odd, d(2025, 2, 10), d(2025, 3, 21))


def test_daytrips_cache(derived_root):
    from stibms import schedule
    from stibms.data import Data
    data = Data(str(derived_root))
    sha = data.gtfs_index()["sha"][0]
    t = schedule.sha_trips(data, sha)
    assert t.height and (derived_root / f"gtfs/daytrips/sha={sha}.parquet").exists()
    assert schedule.write_feed_trips(data.store, "nope").height == 0


def test_schedule_cache_control(client):
    # school holidays depend on feeds up to 90 days after the period: not cacheable for long yet
    assert "max-age=3600" in client.get(f"/api/analysis?{Q}&dow=0-6").headers["cache-control"]
    assert "max-age=300" in client.get(f"/api/analysis?{Q}&dow=0-6&school=exclude").headers["cache-control"]

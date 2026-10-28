"""Derive + API on the simulated bucket (conftest). Skips when microsegments (or its locate /
tracks modules) is not installed."""
from __future__ import annotations

import datetime as dt
import io
import json

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
    # strikes are excluded by default: they depend on later feeds, so the period is not cached long yet
    assert "max-age=300" in r.headers["cache-control"]
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
        assert 'q.set("rev"' in v.text      # the frame's URL has rev already: set, not appended
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
    from stibms.coverage import line_absent
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
    assert res["lines_total"] == 1 and res["lines_gone"] == []     # S1 is in the latest feed
    s = res["stretches"]
    assert 0 < len(s) <= 10 and s[0]["rank"] == 1
    assert s[0]["veh_h_day"] >= s[-1]["veh_h_day"]
    assert s[0]["lines"][0]["line"] == "S1" and s[0]["c"]
    assert {"veh_h_day_run", "veh_h_day_stop", "reasons", "link_keys"} <= set(s[0])
    assert "excluded" in res
    assert all(x["veh_h_day"] > 0 for x in s)      # only stretches in loss
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
    from stibms.stretches import overlap
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
    assert "max-age=3600" in client.get(f"/api/analysis?{Q}&dow=0-6&reduced=include").headers["cache-control"]
    assert "max-age=300" in client.get(f"/api/analysis?{Q}&dow=0-6&reduced=include&school=exclude").headers["cache-control"]
    assert "max-age=300" in client.get(f"/api/analysis?{Q}&dow=0-6").headers["cache-control"]   # strikes excluded by default


def test_versions_grouped_by_stops():
    from stibms.versions import _group_versions
    full, short = ["A", "B", "C", "D"], ["B", "C", "D"]

    def run(a, b, n, stops, uid, lens=None):
        return {"direction_id": 0, "pattern_uid": uid, "first": a, "last": b, "n_days": n, "from": stops[0],
                "to": stops[-1], "n_stops": len(stops), "stops": stops, "_len": lens or [100.0] * (len(stops) - 1)}
    # the short route under two pattern_uids (feed change), next to each other, then back after the full one
    g = _group_versions([run("2026-06-01", "2026-06-05", 5, full, "f1"), run("2026-06-06", "2026-06-10", 4, short, "s1"),
                         run("2026-06-11", "2026-06-12", 2, short, "s2"), run("2026-06-13", "2026-06-20", 8, full, "f2"),
                         run("2026-06-21", "2026-06-21", 1, short, "s1"), run("2026-06-22", "2026-06-30", 9, full, "f1", [100.0, 101.0, 99.0])])
    assert len(g) == 2
    ref, sh = (next(x for x in g if x["ref"] is r) for r in (True, False))
    assert ref["stops"] == full and ref["n_days"] == 22 and len(ref["periods"]) == 3      # a shape edit stays the reference
    assert sh["removed"] == ["A"] and sh["pattern_uid"] == "s1" and sh["n_days"] == 7
    assert sh["periods"] == [{"first": "2026-06-06", "last": "2026-06-12", "n_days": 6},
                             {"first": "2026-06-21", "last": "2026-06-21", "n_days": 1}]
    # a works route with more stops for a few days (71 extended to Herrmann-Debroux) is the change, not the route
    ext = full + ["E", "F"]
    g = _group_versions([run("2026-01-01", "2026-05-07", 127, full, "f1"), run("2026-05-08", "2026-05-17", 10, ext, "x1"),
                         run("2026-05-18", "2026-06-30", 44, full, "f1")])
    ref, x = (next(v for v in g if v["ref"] is r) for r in (True, False))
    assert ref["stops"] == full and ref["n_days"] == 171 and x["added"] == ["E", "F"] and not x["removed"]


def test_lasting_change_before_period():
    from stibms.versions import _lasting_change
    full, cut, works = ["A", "B", "C", "D"], ["B", "C", "D"], ["C", "D"]

    def run(a, b, n, stops):
        return {"direction_id": 0, "first": a, "last": b, "n_days": n, "from": stops[0], "to": stops[-1],
                "n_stops": len(stops), "stops": stops}
    ref = {"stops": cut, "first": "2025-09-01"}
    # cut back on 05-21 after a 4-day works route, a one-day interruption since: the cut dates from 05-21
    back = [run("2025-01-01", "2025-05-16", 136, full), run("2025-05-17", "2025-05-20", 4, works),
            run("2025-05-21", "2025-06-06", 17, cut), run("2025-06-07", "2025-06-07", 1, works),
            run("2025-06-08", "2025-08-31", 85, cut)]
    c = _lasting_change(back, ref)
    assert c["since"] == "2025-05-21" and c["removed"] == ["A"] and not c["added"] and c["before"]["last"] == "2025-05-16"
    assert _lasting_change(back[2:], ref) is None                 # the same route all the lookback
    assert _lasting_change([], ref) is None


def test_query_canonical_is_stable():
    """The canonical forms are the result cache keys (with the package version): any change here
    orphans every cached result."""
    pytest.importorskip("microsegments")
    from stibms.queries import CompareQuery, Query
    from stibms.ranking import RankQuery
    D = dt.date

    def canon(x):
        return json.loads(json.dumps(x.canonical()))
    base = {"holidays": "exclude", "seg": 30.0, "phase": 0.0, "grid": "stops", "stopzone": [36.0, 38.0],
            "ref": [20, 24], "hours": [5, 24]}
    q = Query("55", D(2025, 3, 1), D(2025, 3, 31), dow=(4, 0), school="exclude", reduced="include",
              exclude=((D(2025, 3, 10), D(2025, 3, 12)), (D(2025, 3, 13), D(2025, 3, 13)), (D(2025, 2, 1), D(2025, 2, 5))))
    assert canon(q) == {"line": "55", "first": "2025-03-01", "last": "2025-03-31", "dow": [0, 4], **base, "B": 200,
                             "school": "exclude", "reduced": "include", "exclude": [["2025-03-10", "2025-03-13"]]}
    assert list(Query("55", D(2025, 3, 1), D(2025, 3, 31)).canonical()) == ["line", "first", "last", "dow", *base, "B"]
    cq = CompareQuery("55", (D(2025, 3, 1), D(2025, 3, 31)), (D(2025, 5, 1), D(2025, 5, 31)),
                      exclude=((D(2025, 4, 1), D(2025, 4, 2)),))
    assert canon(cq) == {"line": "55", "a": ["2025-03-01", "2025-03-31"], "b": ["2025-05-01", "2025-05-31"],
                              "dow": [0, 1, 2, 3, 4], **base, "B": 100, "exclude": [["2025-04-01", "2025-04-02"]]}
    rq = RankQuery(D(2025, 3, 1), D(2025, 3, 31), mode="tram", stops=True, exclude=((D(2025, 3, 3), D(2025, 3, 3)),))
    assert canon(rq) == {"first": "2025-03-01", "last": "2025-03-31", "dow": [0, 1, 2, 3, 4], "holidays": "exclude",
                              "mode": "tram", "terminus": False, "stops": True, "exclude": [["2025-03-03", "2025-03-03"]]}


def test_static_files_carry_the_release(client):
    from stibms.api import rev
    page = client.get("/").text
    assert "__REV__" not in page and f"/static/app.css?rev={rev()}" in page
    for name in ("util", "excl", "lines", "quality", "lineview", "routes", "rank", "main"):
        assert f"/static/{name}.js?rev={rev()}" in page
        r = client.get(f"/static/{name}.js")
        assert r.status_code == 200 and "javascript" in r.headers["content-type"] and "__REV__" not in r.text
    assert f'const REV = "{rev()}"' in client.get("/static/util.js").text
    assert client.get("/static/app.css").headers["content-type"].startswith("text/css")
    assert client.get("/static/nope.js").status_code == 404


def test_tune_bad_lengths(client):
    assert client.get(f"/api/tune?{Q}&lengths=abc").status_code == 422
    assert client.get(f"/api/tune?{Q}&lengths=1,2").status_code == 422


def test_day_status_counts_an_hour_once():
    from stibms.coverage import LINE_ABSENT, _per_day, _status
    d = dt.date(2025, 3, 3)
    # 3 of the 15 hours are both low and without the line: 3 unusable hours (< 25 %), not 6
    cov = pl.DataFrame({"service_date": [d] * 24, "hour": list(range(4, 28)),
                        "coverage": [0.5 if 6 <= h < 9 else 1.0 for h in range(4, 28)],
                        "flags": [LINE_ABSENT if 6 <= h < 9 else 0 for h in range(4, 28)]})
    per = _per_day(cov, 0.8)
    assert per[d][1:] == (3, 3, 3)
    assert _status(d, {d}, {"2025-03": ""}, per[d], 100, 0.25 + 1e-9) == "ok"


def test_coverage_and_versions_cached(client, derived_root):
    from stibms import api
    for kind in ("coverage", "versions"):
        r = client.get(f"/api/{kind}?{Q}")
        assert r.status_code == 200 and r.headers["x-cache"] in ("computed", "bucket")   # earlier tests share the bucket
        assert client.get(f"/api/{kind}?{Q}").headers["x-cache"] == "memory"
        assert list((derived_root / "results").rglob(f"{kind}-r{api.RESULT_REV}-*.json.gz"))
    api.state.blobs.clear()
    assert client.get(f"/api/versions?{Q}").headers["x-cache"] == "bucket"


def test_gzip_once(client):
    page = client.get("/", headers={"Accept-Encoding": "gzip"})
    assert page.headers["content-encoding"] == "gzip" and "<html" in page.text.lower()
    r = client.get(f"/api/coverage?{Q}", headers={"Accept-Encoding": "gzip"})
    assert r.headers["content-encoding"] == "gzip" and len(r.json()["days"]) == 8   # decoded once
    plain = client.get(f"/api/coverage?{Q}", headers={"Accept-Encoding": "identity"})
    assert "content-encoding" not in plain.headers and plain.json() == r.json()


def test_rev_from_metadata():
    from importlib.metadata import version
    from stibms.api import RESULT_REV, rev
    assert rev() == f"{version('microsegments')}-{RESULT_REV}"


def test_json_analysis_keeps_no_run(client):
    from stibms import api
    assert client.get(f"/api/analysis?{Q}&seg=40").status_code == 200
    assert not api.state.runs.d
    assert client.get(f"/api/analysis.csv?{Q}&seg=40").status_code == 200
    (r,) = api.state.runs.d.values()
    assert not hasattr(r, "placed")


def test_line_tables_sliced_and_read_once(derived_root, monkeypatch):
    import threading
    from stibms.data import Data
    data = Data(str(derived_root))
    sha = data.gtfs_index()["sha"][0]
    reads, real = [], data.store.read_parquet

    def counting(rel, **kw):
        reads.append(rel)
        return real(rel, **kw)
    monkeypatch.setattr(data.store, "read_parquet", counting)
    ts = [threading.Thread(target=data.line_tables, args=(sha, "S1", False)) for _ in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert len(reads) == 4          # one read of each table, not one per thread
    t = data.line_tables(sha, "S1", False)
    assert len(reads) == 4 and t["patterns"]["line"].unique().to_list() == ["S1"]
    assert t["links"]["geometry"].null_count() == t["links"].height
    g = data.line_tables(sha, "S1")
    assert g["links"]["geometry"].null_count() == 0
    net = data.network("S1", data.ingested_dates(), geometry=False)
    assert net.links.height == data.network("S1", data.ingested_dates()).links.height


def test_sized_lru():
    from stibms.data import SizedLRU
    c = SizedLRU(10)
    c.put("a", 1, 4)
    c.put("b", 2, 4)
    c.get("a")
    c.put("c", 3, 4)            # over 10 bytes: the least recently used goes
    assert c.get("b") is None and c.get("a") == 1 and c.get("c") == 3 and c.size == 8


def test_monthly_count_is_whole_count(derived_root):
    from microsegments.aggregate import count
    from stibms import analysis as A
    from stibms.data import Data
    data = Data(str(derived_root))
    q = A.Query(line="S1", first=dt.date(2025, 5, 26), last=dt.date(2025, 6, 2), dow=tuple(range(7)))
    ld = A.load(data, "S1", A.clamp(data, q.first, q.last))
    assert len(ld.blobs) == 2      # May and June
    segs = A._segments(ld, q)
    whole = count(A.placed(ld), segs, A._params(q).tick_s)
    assert A._count(ld, segs, q).equals(whole)


def test_etag_and_304(client):
    url = f"/api/analysis?{Q}"
    gz = client.get(url, headers={"Accept-Encoding": "gzip"})
    tag = gz.headers["etag"]
    assert gz.status_code == 200 and tag.startswith('"') and tag.endswith('-gz"')
    plain = client.get(url, headers={"Accept-Encoding": "identity"})
    assert plain.headers["etag"] == tag.replace("-gz", "") and "content-encoding" not in plain.headers
    nm = client.get(url, headers={"Accept-Encoding": "gzip", "If-None-Match": tag})
    assert nm.status_code == 304 and not nm.content
    assert nm.headers["etag"] == tag and nm.headers["cache-control"] == gz.headers["cache-control"]
    assert client.get(url, headers={"If-None-Match": f'W/{plain.headers["etag"]}, "x"'}).status_code == 304
    assert client.get(url + "&dow=0-6", headers={"If-None-Match": tag}).status_code == 200   # other params, other tag
    rk = "/api/ranking?from=2025-05-26&to=2025-06-02&dow=0-6&top=10"
    t = client.get(rk).headers["etag"]
    assert client.get(rk, headers={"If-None-Match": t}).status_code == 304
    ln = client.get("/api/lines")
    assert ln.headers["etag"].startswith('W/"')
    assert client.get("/api/lines", headers={"If-None-Match": ln.headers["etag"]}).status_code == 304


def test_304_skips_the_result(client, monkeypatch):
    from stibms.api import cache
    tag = client.get(f"/api/coverage?{Q}").headers["etag"]
    monkeypatch.setattr(cache, "load", lambda res: pytest.fail("a 304 must not load the result"))
    assert client.get(f"/api/coverage?{Q}", headers={"If-None-Match": tag}).status_code == 304


def test_ranking_bucket_cache_and_excluded_stretches(client, derived_root):
    from stibms import api
    q = "/api/ranking?from=2025-05-26&to=2025-06-02&dow=0-6&top=10"
    res = client.get(q).json()
    assert list((derived_root / "results").rglob(f"ranking-r{api.RESULT_REV}-*.json.gz"))
    api.state.blobs.clear()
    assert client.get(q).headers["x-cache"] == "bucket"
    # an empty window (no metro) is not cached: its cubes may come later
    assert client.get(q + "&mode=metro").headers["x-cache"] == "computed"
    assert client.get(q + "&mode=metro").headers["x-cache"] == "computed"
    # excluded.n counts stretches, as the page words it: the listed ones are among them
    ex = res["excluded"]
    assert ex["n"] >= len(ex["top"]) and (ex["n"] > 10 or ex["n"] == len(ex["top"]))


def test_warm_fills_the_cache(client, derived_root, monkeypatch):
    from stibms import api, warm
    monkeypatch.setenv("MS_BUCKET", str(derived_root))
    monkeypatch.setenv("MS_BLOB_CACHE", "128")
    for p in (derived_root / "results").rglob("*-r*.json.gz"):
        p.unlink()
    api.state.reset()
    assert warm.main(["--local", str(derived_root), "--workers", "2"]) == 0
    d = next(x for x in client.get("/api/lines").json()["lines"] if x["line"] == "S1")["available"]["default"]
    api.state.blobs.clear()
    p = f"line=S1&from={d['from']}&to={d['to']}"
    for url in (f"/api/coverage?{p}", f"/api/versions?{p}", f"/api/analysis?{p}&dow=0-4"):
        r = client.get(url + "&rev=x")
        assert r.status_code == 200 and r.headers["x-cache"] == "bucket", url
    # the page's default ranking; an empty one (here: May has 4 weekdays, < MIN_DAYS) is never cached
    r = client.get("/api/ranking?dow=0-4&mode=all&top=50&rev=x")
    assert r.headers["x-cache"] == ("bucket" if r.json()["stretches"] else "computed")


def test_overlap_cache_bounded():
    from stibms import stretches
    from stibms.data import SizedLRU
    assert isinstance(stretches._ov_cache, SizedLRU) and stretches._ov_cache.max_bytes == stretches.OV_CACHE_N


def test_page_scripts_are_modules(client):
    import re
    from stibms.api import rev
    page = client.get("/").text
    assert f'<script type="module" src="/static/main.js?rev={rev()}">' in page
    for name in ("util", "excl", "lines", "quality", "lineview", "routes", "rank", "main"):
        js = client.get(f"/static/{name}.js").text
        specs = re.findall(r'from "([^"]+)"', js)
        assert all(s.endswith(f".js?rev={rev()}") for s in specs), (name, specs)   # one instance per module


def test_warm_skips_a_refused_request(client, monkeypatch):
    from fastapi import HTTPException
    from stibms import warm

    def refuse(code):
        def f():
            raise HTTPException(code, "no")
        return f
    assert warm._warm("x", refuse(404)) == "missing" and warm._warm("x", refuse(422)) == "failed"
    monkeypatch.setattr(warm, "ranking_result", refuse(422))
    monkeypatch.setattr(warm, "default_lines", lambda data, only: [("S1", {"from": "2025-05-26", "to": "2025-05-27"})])
    monkeypatch.setattr(warm, "line_results", lambda ln, d, a: [(f"coverage {ln}", refuse(404))])
    assert warm.run() == {"failed": 1, "missing": 1}


def test_ranking_lists_only_stretches_in_loss():
    """No time lost anywhere (the metro between stations) gives no stretch, not a list of zeros (the
    page's colour scale divided by 0)."""
    import polars as pl
    from stibms.ranking import _stretches
    row = {"line": "1", "veh_s_day_run": 0.0, "veh_s_day_stop": -5.0, "passages_day": 100.0, "peak": "day",
           "n_days": 20, "reason": None, "reg_share": None, "len_m": 600.0}
    df = pl.DataFrame([{**row, "link_key": k, "from_t": a, "to_t": b, "veh_s_day": v}
                       for k, a, b, v in [("a>b", "A", "B", 0.0), ("b>c", "B", "C", -3.0), ("c>d", "C", "D", 7200.0)]])
    rows, tot, n = _stretches(df, 10, {}, 261)
    assert [r["link_key"] for r in rows] == ["c>d"] and n == 1 and tot == 7200.0
    assert _stretches(df.filter(pl.col("veh_s_day") <= 0), 10, {}, 261) == ([], 0.0, 0)

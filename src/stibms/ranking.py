"""Network ranking: the stretches (stop pairs) where STIB vehicles lose the most time, all lines.

    python -m stibms.ranking --default [--bucket gs://b | --local DIR]      # nightly precompute
    python -m stibms.ranking --from 2026-07-01 --to 2026-09-30 [--dow 0-4] [--mode tram] [--stops]

Any window is a sum over the monthly link cubes (``stibms.linkcube``, written by the derive): per line
and link, observations and passages of the usable hours, by hour band and zone. Per (line, link):

    excess per passage  E = (obs_day / pas_day - obs_eve / pas_eve) x tick_s     (evening 20-23 h = reference)
    vehicle time lost   E x pas_day / D                                          (D = usable days of the line)

which equals the line page's sum over the link's 30 m segments of excess x passages (same ratios of
sums). By default only the observations between stops count (``stops=False``): dwell growth at
stops is passenger load, not traffic; ``stops=True`` adds the stop zones (30 m before to 60 m after a
stop). Lines on the same stop pair and track share a ``link_key`` and add up; platforms of the same
stops (same names, tracks overlapping >= 80 % within 30 m) are merged into one stretch.

Regulation and termini (left out unless ``terminus=True``, listed with their reason):

- ``terminus``: first or last link of one of the line's patterns (layover at the terminus);
- ``short_terminus`` / ``holding``: at least half of the excess comes from vehicles standing still for
  3 minutes or more (``linkcube.LONG_STAND_S``), next to a stop that ends one of the line's patterns
  (short workings parked past their last stop) or elsewhere (timing points). Traffic stops are short.

A window costs well under a second (a few 100 k rows per month), so the API answers synchronously.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import math
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass

import polars as pl

from . import ms
from .data import ALGO_VERSION, Data, date_range, month_dates, months_between
from .linkcube import rel as cube_rel

log = logging.getLogger("stibms.ranking")
MODES = ("all", "tram", "bus", "metro")
RANK_VERSION = 3      # bump when the ranking changes meaning (new cache keys)
DEFAULT_PTR = f"results/v{ALGO_VERSION}/ranking-default.json"
TICK_S = 20.0
MIN_DAYS = 5          # links seen on fewer usable days are left out (a short detour otherwise tops the list)
REG_SHARE = 0.5       # share of the excess in long stands that makes a link a regulation point
MERGE_OVERLAP = 0.8   # two links of the same stop names are one stretch when >= 80 % of the shorter ...
MERGE_DIST_M = 30.0   # ... lies within 30 m of the longer
REASONS = {"terminus": "terminus de la ligne", "short_terminus": "terminus partiel (arrêts de régulation)",
           "holding": "point de régulation (arrêts de plus de 3 min)"}


@dataclass(frozen=True)
class RankQuery:
    first: dt.date
    last: dt.date
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"
    mode: str = "all"
    terminus: bool = False               # include termini and regulation points
    stops: bool = False                  # include the stop zones

    def __post_init__(self):
        if self.first > self.last:
            raise ValueError("from must be <= to")
        if (self.last - self.first).days > 400:
            raise ValueError("period longer than 400 days")
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {', '.join(MODES)}")
        if not self.dow or any(d not in range(7) for d in self.dow):
            raise ValueError("dow must be a subset of 0..6 (0 = Monday)")
        if self.holidays not in ("exclude", "include"):
            raise ValueError("holidays must be exclude or include")

    def canonical(self) -> dict:
        d = asdict(self)
        d["first"], d["last"], d["dow"] = self.first.isoformat(), self.last.isoformat(), sorted(set(self.dow))
        return d

    def key(self, stamp: str, top: int = 50) -> str:
        blob = json.dumps([self.canonical(), top, ALGO_VERSION, RANK_VERSION, ms.version(), stamp], sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:32]


DEFAULT_MONTHS = 1    # the default window: the last complete month (weekdays)


def default_window(data: Data, today: dt.date | None = None, n: int = DEFAULT_MONTHS) -> tuple[dt.date, dt.date] | None:
    """Last ``n`` complete months that are ingested up to their last day and derived."""
    ing = data.ingested_dates()
    if not ing:
        return None
    have = data.derived_months()
    months = [m for m in have if month_dates(m)[-1] <= ing[-1]]
    if not months:
        return None
    pick = months[-n:]
    return month_dates(pick[0])[0], month_dates(pick[-1])[-1]


def line_modes(data: Data) -> dict[str, dict]:
    """line -> {mode, color, text_color, name} from the latest GTFS feeds."""
    def f():
        from microsegments.report import ROUTE_TYPES
        idx = data.gtfs_index()
        if not idx.height:
            return {}
        out = {}
        for sha in reversed(idx["sha"].unique(maintain_order=True).to_list()[-6:]):
            for r in data.routes(sha).iter_rows(named=True):
                ln = str(r["route_short_name"])
                if ln in out:
                    continue
                rt = r.get("route_type")
                out[ln] = {"mode": ROUTE_TYPES.get(int(rt)) if rt is not None else None, "color": r.get("route_color"),
                           "text_color": r.get("route_text_color"), "name": r.get("route_long_name")}
        return out
    return data._cached("line_modes", f)


# ---------------------------------------------------------------------------------- inputs
_month_cache: dict[tuple[str, str], tuple] = {}
_mc_lock = threading.Lock()


def month_tables(data: Data, month: str) -> tuple[pl.DataFrame | None, pl.DataFrame | None, pl.DataFrame | None]:
    """(cube, days, links) of a month, cached in process per derive stamp."""
    k = (month, data.month_stamp(month))
    with _mc_lock:
        hit = _month_cache.get(k)
    if hit is not None:
        return hit
    out = tuple(data.read_many([cube_rel(month, n) for n in ("cube", "days", "links")]))
    if out[0] is None:      # not built yet (backfill running): do not remember the absence
        return out
    with _mc_lock:
        for kk in [kk for kk in _month_cache if kk[0] == month]:
            del _month_cache[kk]
        _month_cache[k] = out
        while len(_month_cache) > 40:
            _month_cache.pop(next(iter(_month_cache)))
    return out


def _registry(data: Data) -> pl.DataFrame | None:
    return data._cached("rank_registry", lambda: data.read_optional(f"derived/v{ALGO_VERSION}/linkkeys.parquet"))


# ---------------------------------------------------------------------------------- geometry
def _xy(coords, lat0: float) -> list[tuple[float, float]]:
    k = math.cos(math.radians(lat0))
    return [(x * 111320.0 * k, y * 110540.0) for x, y in coords]


def _dist_pt_poly(p, poly) -> float:
    best = float("inf")
    for (x1, y1), (x2, y2) in zip(poly, poly[1:]):
        dx, dy = x2 - x1, y2 - y1
        L = dx * dx + dy * dy
        t = 0.0 if L == 0 else max(0.0, min(1.0, ((p[0] - x1) * dx + (p[1] - y1) * dy) / L))
        best = min(best, math.hypot(p[0] - x1 - t * dx, p[1] - y1 - t * dy))
    return best


def _densify(poly, step: float = 5.0):
    out = [poly[0]]
    for (x1, y1), (x2, y2) in zip(poly, poly[1:]):
        n = max(1, int(math.hypot(x2 - x1, y2 - y1) // step))
        out += [(x1 + (x2 - x1) * i / n, y1 + (y2 - y1) * i / n) for i in range(1, n + 1)]
    return out


def overlap(a, b) -> float:
    """Share of the shorter polyline (lon/lat) within MERGE_DIST_M of the other one."""
    if not a or not b or len(a) < 2 or len(b) < 2:
        return 0.0
    lat0 = a[0][1]
    A, B = _xy(a, lat0), _xy(b, lat0)
    la = sum(math.hypot(x2 - x1, y2 - y1) for (x1, y1), (x2, y2) in zip(A, A[1:]))
    lb = sum(math.hypot(x2 - x1, y2 - y1) for (x1, y1), (x2, y2) in zip(B, B[1:]))
    short, long_ = (A, B) if la <= lb else (B, A)
    pts = _densify(short)
    return sum(_dist_pt_poly(p, long_) <= MERGE_DIST_M for p in pts) / len(pts)


def merge_groups(g: pl.DataFrame, geo: dict[str, list]) -> dict[str, str]:
    """link_key -> stretch key: links with the same (title-cased) stop names whose tracks overlap
    (different platforms of the same stops) become one stretch, keyed by its costliest link."""
    out = {k: k for k in g["link_key"].to_list()}
    dup = g.group_by("from_t", "to_t").agg(pl.col("link_key"), pl.col("veh_s_day")).filter(pl.col("link_key").list.len() > 1)
    for keys, vals in zip(dup["link_key"].to_list(), dup["veh_s_day"].to_list()):
        order = [k for _, k in sorted(zip(vals, keys), key=lambda t: -t[0])]
        parent = {k: k for k in order}

        def find(k):
            while parent[k] != k:
                k = parent[k]
            return k
        for i, a in enumerate(order):
            for b in order[i + 1:]:
                if find(a) != find(b) and overlap(geo.get(a), geo.get(b)) >= MERGE_OVERLAP:
                    ra, rb = find(a), find(b)
                    # the root is the costlier link (earlier in ``order``)
                    if order.index(ra) < order.index(rb):
                        parent[rb] = ra
                    else:
                        parent[ra] = rb
        for k in order:
            out[k] = find(k)
    return out


# ---------------------------------------------------------------------------------- ranking
def per_line_links(data: Data, rq: RankQuery, info: dict) -> tuple[pl.DataFrame, int, int]:
    """Per (line, link_key): vehicle time lost per day (selected zones, running and stop parts),
    passages per day, reason. Returns (frame, lines with data, lines used after the mode filter)."""
    from . import holidays as hol
    frames, days, links = [], [], []
    for m in months_between(rq.first, rq.last):
        c, d, lk = month_tables(data, m)
        if c is not None and c.height:
            frames.append(c)
        if d is not None and d.height:
            days.append(d)
        if lk is not None and lk.height:
            links.append(lk)
    if not frames:
        return pl.DataFrame(), 0, 0
    hd = hol.between(rq.first, rq.last) if rq.holidays == "exclude" else {}
    keep_date = (pl.col("service_date").is_between(rq.first, rq.last)
                 & pl.col("service_date").dt.weekday().sub(1).is_in(sorted(set(rq.dow)))
                 & ~pl.col("service_date").is_in(list(hd)))
    cube = pl.concat(frames, how="diagonal_relaxed").filter(keep_date)
    dd = pl.concat(days, how="diagonal_relaxed").filter(keep_date).unique()
    n_lines = dd["line"].n_unique() if dd.height else 0
    if rq.mode != "all":
        want = [ln for ln, v in info.items() if (v or {}).get("mode") == rq.mode]
        cube, dd = cube.filter(pl.col("line").is_in(want)), dd.filter(pl.col("line").is_in(want))
    if not cube.height:
        return pl.DataFrame(), n_lines, 0
    D = dd.group_by("line").agg(pl.len().alias("D"))
    num = [c for c in cube.columns if c[:2] in ("o_", "l_", "p_")]
    s = (cube.group_by("line", "link_key")
         .agg([pl.col(c).sum().cast(pl.Float64) for c in num] + [(pl.col("p_day") > 0).sum().alias("n_days")])
         .join(D, on="line", how="inner").filter(pl.col("n_days") >= MIN_DAYS))

    def sel(prefix, band):
        r = pl.col(f"{prefix}_{band}_r")
        return r + pl.col(f"{prefix}_{band}_s") if rq.stops else r

    def ex(o_day, o_eve):   # seconds per passage over the evening
        return (o_day / pl.col("p_day") - o_eve / pl.col("p_eve")) * TICK_S

    pd_ = pl.col("p_day") / pl.col("D")
    s = s.filter((pl.col("p_day") > 0) & (pl.col("p_eve") > 0)).with_columns(
        ex(sel("o", "day"), sel("o", "eve")).alias("ex"),
        ex(pl.col("o_day_r"), pl.col("o_eve_r")).alias("ex_r"),
        ex(pl.col("o_day_s"), pl.col("o_eve_s")).alias("ex_s"),
        ex(sel("l", "day"), sel("l", "eve")).alias("ex_long"),
        ((sel("o", "am") / pl.col("p_am") - sel("o", "eve") / pl.col("p_eve")) * TICK_S).alias("ex_am"),
        ((sel("o", "pm") / pl.col("p_pm") - sel("o", "eve") / pl.col("p_eve")) * TICK_S).alias("ex_pm"),
        pd_.alias("passages_day"))
    s = s.with_columns((pl.col("ex") * pl.col("passages_day")).alias("veh_s_day"),
                       (pl.col("ex_r") * pl.col("passages_day")).alias("veh_s_day_run"),
                       (pl.col("ex_s") * pl.col("passages_day")).alias("veh_s_day_stop"),
                       pl.when(pl.col("ex") > 0).then(pl.col("ex_long") / pl.col("ex")).alias("reg_share"))
    lk = (pl.concat(links, how="diagonal_relaxed").sort("term", descending=True)
          .unique(["line", "link_key"], keep="first"))
    s = s.join(lk.select("line", "link_key", "from_name", "to_name", "len_m", "term"), on=["line", "link_key"], how="left")
    reg = pl.col("reg_share").fill_null(0) >= REG_SHARE
    s = s.with_columns(
        pl.when(pl.col("term") == 1).then(pl.lit("terminus"))
        .when(reg & (pl.col("term") == 2)).then(pl.lit("short_terminus"))
        .when(reg).then(pl.lit("holding")).otherwise(None).alias("reason"),
        pl.when(pl.col("ex_am").fill_nan(None).fill_null(0) > 1.25 * pl.col("ex_pm").fill_nan(None).fill_null(0)).then(pl.lit("am"))
        .when(pl.col("ex_pm").fill_nan(None).fill_null(0) > 1.25 * pl.col("ex_am").fill_nan(None).fill_null(0)).then(pl.lit("pm"))
        .otherwise(pl.lit("day")).alias("peak"))
    return s, n_lines, s["line"].n_unique()


def assemble(data: Data, rq: RankQuery, s: pl.DataFrame, top: int, info: dict, n_lines: int, n_used: int) -> dict:
    from microsegments.report import title_name
    days_per_year = round(365.25 / 7 * len(set(rq.dow)))
    base = {"query": rq.canonical(), "lines_total": n_lines, "lines_used": n_used, "days_per_year": days_per_year,
            "algo": ALGO_VERSION, "rank_version": RANK_VERSION, "microsegments": ms.version(),
            "zones": "all" if rq.stops else "running", "reasons": REASONS}
    if not s.height:
        return {**base, "stretches": [], "lines": [], "excluded": {"n": 0, "veh_h_day": 0, "top": []}}
    s = s.with_columns(pl.col("from_name").map_elements(title_name, return_dtype=pl.Utf8).alias("from_t"),
                       pl.col("to_name").map_elements(title_name, return_dtype=pl.Utf8).alias("to_t"))
    reg = _registry(data)
    geo_all = {}
    if reg is not None:
        sub = reg.filter(pl.col("link_key").is_in(s["link_key"].unique().implode())).select("link_key", "geometry")
        geo_all = {k: [[round(x, 6), round(y, 6)] for x, y in (g or [])] for k, g in sub.iter_rows()}
    flagged = s.filter(pl.col("reason").is_not_null())
    kept = s if rq.terminus else s.filter(pl.col("reason").is_null())

    def stretches(df: pl.DataFrame, n: int) -> tuple[list[dict], float]:
        if not df.height:
            return [], 0.0
        per_key = (df.group_by("link_key").agg(pl.col("veh_s_day").sum(), pl.col("from_t").first(), pl.col("to_t").first()))
        sk = merge_groups(per_key, geo_all)
        df = df.with_columns(pl.col("link_key").replace_strict(sk, default=None).alias("stretch"))
        g = (df.group_by("stretch")
             .agg(pl.col("veh_s_day").sum(), pl.col("veh_s_day_run").sum(), pl.col("veh_s_day_stop").sum(),
                  pl.col("passages_day").sum(), pl.col("link_key").unique().alias("keys"),
                  pl.struct("line", "veh_s_day", "veh_s_day_run", "veh_s_day_stop", "passages_day", "peak", "n_days",
                            "reason", "reg_share").sort_by("veh_s_day", descending=True).alias("by_line"))
             .sort("veh_s_day", descending=True))
        tot = float(g.filter(pl.col("veh_s_day") > 0)["veh_s_day"].sum())
        names = df.select("link_key", "from_t", "to_t", "len_m").unique("link_key", keep="first")
        g = g.head(n).join(names, left_on="stretch", right_on="link_key", how="left").sort("veh_s_day", descending=True)
        out = []
        for i, r in enumerate(g.iter_rows(named=True)):
            vh = r["veh_s_day"] / 3600
            reasons = sorted({x["reason"] for x in r["by_line"] if x["reason"]})
            out.append({
                "rank": i + 1, "link_key": r["stretch"], "link_keys": sorted(r["keys"]),
                "from": r["from_t"], "to": r["to_t"], "len_m": round(r["len_m"] or 0, 1),
                "veh_h_day": round(vh, 2), "veh_h_year": round(vh * days_per_year),
                "veh_h_day_run": round(r["veh_s_day_run"] / 3600, 2), "veh_h_day_stop": round(r["veh_s_day_stop"] / 3600, 2),
                "passages_day": round(r["passages_day"], 1),
                "s_per_passage": round(r["veh_s_day"] / r["passages_day"], 1) if r["passages_day"] else None,
                "reasons": reasons,
                "lines": [{"line": x["line"], "veh_h_day": round(x["veh_s_day"] / 3600, 2),
                           "veh_h_day_run": round(x["veh_s_day_run"] / 3600, 2),
                           "veh_h_day_stop": round(x["veh_s_day_stop"] / 3600, 2),
                           "passages_day": round(x["passages_day"], 1), "peak": x["peak"], "n_days": x["n_days"],
                           "reason": x["reason"],
                           "stand_share": None if x["reg_share"] is None else round(x["reg_share"], 2)}
                          for x in r["by_line"]],
                "c": geo_all.get(r["stretch"]),
                "cs": [geo_all[k] for k in sorted(r["keys"]) if k != r["stretch"] and k in geo_all],
            })
        return out, tot

    main, total = stretches(kept, top)
    ex_rows = flagged if not rq.terminus else flagged.head(0)
    ex_top, ex_tot = stretches(ex_rows.filter(pl.col("veh_s_day") > 0), 10)
    line_tot = (kept.group_by("line").agg(pl.col("veh_s_day").filter(pl.col("veh_s_day") > 0).sum())
                .sort("veh_s_day", descending=True))
    return {**base, "stretches": main, "total_veh_h_day": round(total / 3600, 1),
            "excluded": {"n": int(ex_rows.filter(pl.col("veh_s_day") > 0).height), "veh_h_day": round(ex_tot / 3600, 1),
                         "top": [{k: x[k] for k in ("from", "to", "veh_h_day", "s_per_passage", "reasons", "lines")} for x in ex_top]},
            "lines": [{"line": ln, "veh_h_day": round(v / 3600, 2), **(info.get(ln) or {})} for ln, v in line_tot.iter_rows()]}


def compute(data: Data, rq: RankQuery, top: int = 50) -> dict:
    """The ranking of ``rq`` from the link cubes (well under a second for a few months)."""
    t0 = time.time()
    info = line_modes(data)
    s, n_lines, n_used = per_line_links(data, rq, info)
    res = assemble(data, rq, s, top, info, n_lines, n_used)
    res["status"] = "done"
    res["stamp"] = data.stamp(date_range(rq.first, rq.last))
    res["computed_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    res["seconds"] = round(time.time() - t0, 2)
    return res


def precompute_default(data: Data, top: int = 50) -> dict | None:
    """Nightly: check the default window computes and write its pointer (the page opens on it)."""
    w = default_window(data)
    if w is None:
        log.warning("ranking: no complete derived month, nothing to precompute")
        return None
    t = time.time()
    rq = RankQuery(w[0], w[1])
    res = compute(data, rq, top=top)
    data.store.write_json(DEFAULT_PTR, {"from": w[0].isoformat(), "to": w[1].isoformat(), "dow": [0, 1, 2, 3, 4],
                                        "top": top, "written_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
    log.info("ranking default %s..%s: %d stretches, %.1f s", w[0], w[1], len(res["stretches"]), time.time() - t)
    return res


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m stibms.ranking", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--default", action="store_true", help="the nightly default window (last complete month)")
    ap.add_argument("--from", dest="first")
    ap.add_argument("--to")
    ap.add_argument("--dow", default="0-4")
    ap.add_argument("--mode", default="all", choices=MODES)
    ap.add_argument("--top", type=int, default=50)
    ap.add_argument("--stops", action="store_true", help="include the stop zones")
    ap.add_argument("--terminus", action="store_true", help="include termini and regulation points")
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--bucket")
    where.add_argument("--local")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    root = args.local or args.bucket or os.environ.get("MS_BUCKET")
    if not root:
        ap.error("give --bucket, --local or set MS_BUCKET")
    data = Data(root)
    if args.default:
        precompute_default(data, args.top)
        return 0
    if not (args.first and args.to):
        ap.error("give --default or --from and --to")
    a, b = args.dow.split("-") if "-" in args.dow else (None, None)
    dow = tuple(range(int(a), int(b) + 1)) if a is not None else tuple(int(x) for x in args.dow.split(","))
    res = compute(data, RankQuery(dt.date.fromisoformat(args.first), dt.date.fromisoformat(args.to), dow, mode=args.mode,
                                  terminus=args.terminus, stops=args.stops), top=args.top)
    print(json.dumps({k: res.get(k) for k in ("lines_used", "total_veh_h_day", "seconds")}))
    for s in res["stretches"][:25]:
        print(f"{s['rank']:>3} {s['veh_h_day']:>6.2f} h/j {s['s_per_passage'] or 0:>6.1f} s/p  {s['from']} -> {s['to']}  "
              f"[{', '.join(x['line'] for x in s['lines'])}]{' ' + ','.join(s['reasons']) if s['reasons'] else ''}")
    ex = res.get("excluded") or {}
    print(f"excluded: {ex.get('n')} links, {ex.get('veh_h_day')} h/day")
    for x in ex.get("top", []):
        print(f"    {x['veh_h_day']:>6.2f} h/j {x['s_per_passage'] or 0:>6.1f} s/p  {x['from']} -> {x['to']}  "
              f"[{', '.join(y['line'] for y in x['lines'])}] {','.join(x['reasons'])}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

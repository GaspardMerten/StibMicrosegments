"""Network ranking: the stretches (stop pairs) where STIB vehicles lose the most time, all lines.

    python -m stibms.ranking --default [--bucket gs://b | --local DIR]      # nightly precompute
    python -m stibms.ranking --from 2026-07-01 --to 2026-09-30 [--dow 0-4] [--mode tram] [--stops]

Any window is a sum over the monthly link cubes (``stibms.linkcube``, written by the derive): per line
and link, observations and passages of the usable hours, by hour band and zone. Per (line, link):

    excess per passage  E = (obs_day / pas_day - obs_eve / pas_eve) x tick_s     (evening 20-24 h = reference)
    vehicle time lost   E x pas_day / D                                          (D = usable days of the line)

which equals the line page's sum over the link's 30 m segments of excess x passages (same ratios of
sums). By default only the observations between stops count (``stops=False``): dwell growth at
stops is passenger load, not traffic; ``stops=True`` adds the stop zones (from ~36 m before a stop
to its exit mark, see ``stibms.stopzones``). Lines on the same stop pair and track share a ``link_key`` and add up; platforms of the same
stops (same names, tracks overlapping >= 80 % within 30 m, see ``stibms.stretches``) are merged into one stretch.

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
import json
import logging
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import polars as pl

from . import ms, schedule
from .data import ALGO_VERSION, Data, by_line, date_range, month_dates, months_between
from .linkcube import TICK_S
from .linkcube import rel as cube_rel
from .queries import check_days, check_dow, check_holidays, check_span, days_canonical, digest
from .stretches import merge_groups

log = logging.getLogger("stibms.ranking")
MODES = ("all", "tram", "bus", "metro")
RANK_VERSION = 5      # bump when the ranking changes meaning (new cache keys); 5: excluded.n counts stretches
DEFAULT_PTR = f"results/v{ALGO_VERSION}/ranking-default.json"
DEFAULT_MONTHS = 1    # the default window: the last complete month (weekdays)
MIN_DAYS = 5          # links seen on fewer usable days are left out (a short detour otherwise tops the list)
REG_SHARE = 0.5       # share of the excess in long stands that makes a link a regulation point
REASONS = {"terminus": "terminus de la ligne", "short_terminus": "terminus partiel (des véhicules y stationnent plus de 3 min)",
           "holding": "arrêt où des véhicules attendent plus de 3 min pour respecter l'horaire"}


@dataclass(frozen=True)
class RankQuery:
    first: dt.date
    last: dt.date
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"
    mode: str = "all"
    terminus: bool = False               # include termini and regulation points
    stops: bool = False                  # include the stop zones
    school: str = "include"              # days left out on purpose, as the line page (stibms.schedule)
    reduced: str = "exclude"             # strikes left out by default
    exclude: tuple = ()

    def __post_init__(self):
        check_span(self.first, self.last)
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {', '.join(MODES)}")
        check_dow(self.dow)
        check_holidays(self.holidays)
        check_days(self)

    @property
    def ranges(self) -> list[tuple[dt.date, dt.date]]:
        return [(self.first, self.last)]

    def canonical(self) -> dict:
        d = days_canonical(self, self.first, self.last)
        d["first"], d["last"] = self.first.isoformat(), self.last.isoformat()
        return d

    def key(self, stamp: str, top: int = 50) -> str:
        return digest([self.canonical(), top, ALGO_VERSION, RANK_VERSION, ms.version(), stamp])

    def exclusions(self, data: Data) -> dict:
        return schedule.exclusions(data, self.first, self.last, self.holidays, self.school, self.reduced, self.exclude)


def default_window(data: Data, n: int = DEFAULT_MONTHS) -> tuple[dt.date, dt.date] | None:
    """Last ``n`` complete months that are ingested up to their last day and derived."""
    ing = data.ingested_dates()
    if not ing:
        return None
    months = [m for m in data.derived_months() if month_dates(m)[-1] <= ing[-1]]
    if not months:
        return None
    pick = months[-n:]
    return month_dates(pick[0])[0], month_dates(pick[-1])[-1]


def line_modes(data: Data, ttl: float = 3600) -> dict[str, dict]:
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
    return data._cached("line_modes", f, ttl=ttl)


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


def window_tables(data: Data, rq: RankQuery) -> list[tuple]:
    """month_tables of every month of the window (in parallel threads)."""
    months = months_between(rq.first, rq.last)
    if len(months) <= 1:
        return [month_tables(data, m) for m in months]
    with ThreadPoolExecutor(max_workers=min(12, len(months))) as ex:
        return list(ex.map(lambda m: month_tables(data, m), months))


def _registry(data: Data, ttl: float = 3600) -> pl.DataFrame | None:
    return data._cached("rank_registry", lambda: data.read_optional(f"derived/v{ALGO_VERSION}/linkkeys.parquet"), ttl=ttl)


# ---------------------------------------------------------------------------------- ranking
def _excess(s: pl.DataFrame, stops: bool) -> pl.DataFrame:
    """Per (line, link): seconds lost per passage over the evening (selected zones, running and stop
    parts, long stands, am / pm peaks), passages and vehicle time lost per day."""
    def sel(prefix, band):
        r = pl.col(f"{prefix}_{band}_r")
        return r + pl.col(f"{prefix}_{band}_s") if stops else r

    def ex(o, o_eve, p="p_day"):   # seconds per passage over the evening
        return (o / pl.col(p) - o_eve / pl.col("p_eve")) * TICK_S

    s = s.filter((pl.col("p_day") > 0) & (pl.col("p_eve") > 0)).with_columns(
        ex(sel("o", "day"), sel("o", "eve")).alias("ex"),
        ex(pl.col("o_day_r"), pl.col("o_eve_r")).alias("ex_r"),
        ex(pl.col("o_day_s"), pl.col("o_eve_s")).alias("ex_s"),
        ex(sel("l", "day"), sel("l", "eve")).alias("ex_long"),
        ex(sel("o", "am"), sel("o", "eve"), "p_am").alias("ex_am"),
        ex(sel("o", "pm"), sel("o", "eve"), "p_pm").alias("ex_pm"),
        (pl.col("p_day") / pl.col("D")).alias("passages_day"))
    return s.with_columns((pl.col("ex") * pl.col("passages_day")).alias("veh_s_day"),
                          (pl.col("ex_r") * pl.col("passages_day")).alias("veh_s_day_run"),
                          (pl.col("ex_s") * pl.col("passages_day")).alias("veh_s_day_stop"),
                          pl.when(pl.col("ex") > 0).then(pl.col("ex_long") / pl.col("ex")).alias("reg_share"))


def _reasons(s: pl.DataFrame, links: list[pl.DataFrame]) -> pl.DataFrame:
    """Stop names, length and the reason a link is left out (terminus, short_terminus, holding), and
    when its excess peaks (am, pm, day)."""
    lk = (pl.concat(links, how="diagonal_relaxed").sort("term", descending=True)
          .unique(["line", "link_key"], keep="first"))
    s = s.join(lk.select("line", "link_key", "from_name", "to_name", "len_m", "term"), on=["line", "link_key"], how="left")
    reg = pl.col("reg_share").fill_null(0) >= REG_SHARE
    am, pm = (pl.col(c).fill_nan(None).fill_null(0) for c in ("ex_am", "ex_pm"))
    return s.with_columns(
        pl.when(pl.col("term") == 1).then(pl.lit("terminus"))
        .when(reg & (pl.col("term") == 2)).then(pl.lit("short_terminus"))
        .when(reg).then(pl.lit("holding")).otherwise(None).alias("reason"),
        pl.when(am > 1.25 * pm).then(pl.lit("am")).when(pm > 1.25 * am).then(pl.lit("pm"))
        .otherwise(pl.lit("day")).alias("peak"))


def per_line_links(data: Data, rq: RankQuery, info: dict) -> tuple[pl.DataFrame, list[str], int]:
    """Per (line, link_key): vehicle time lost per day (selected zones, running and stop parts),
    passages per day, reason. Returns (frame, lines with data, lines used after the mode filter)."""
    tables = window_tables(data, rq)
    frames, days, links = ([t[i] for t in tables if t[i] is not None and t[i].height] for i in range(3))
    if not frames:
        return pl.DataFrame(), [], 0
    keep_date = (pl.col("service_date").is_between(rq.first, rq.last)
                 & pl.col("service_date").dt.weekday().sub(1).is_in(sorted(set(rq.dow)))
                 & ~pl.col("service_date").is_in(sorted(rq.exclusions(data))))
    cube = pl.concat(frames, how="diagonal_relaxed").filter(keep_date)
    dd = pl.concat(days, how="diagonal_relaxed").filter(keep_date).unique()
    lines = dd["line"].cast(pl.Utf8).unique().to_list() if dd.height else []
    if rq.mode != "all":
        want = [ln for ln, v in info.items() if (v or {}).get("mode") == rq.mode]
        cube, dd = cube.filter(pl.col("line").is_in(want)), dd.filter(pl.col("line").is_in(want))
    if not cube.height:
        return pl.DataFrame(), lines, 0
    D = dd.group_by("line").agg(pl.len().alias("D"))
    num = [c for c in cube.columns if c[:2] in ("o_", "l_", "p_")]
    s = (cube.group_by("line", "link_key")
         .agg([pl.col(c).sum().cast(pl.Float64) for c in num] + [(pl.col("p_day") > 0).sum().alias("n_days")])
         .join(D, on="line", how="inner").filter(pl.col("n_days") >= MIN_DAYS))
    s = _reasons(_excess(s, rq.stops), links)
    return s, lines, s["line"].n_unique()


def _geometry(data: Data, keys: pl.Series) -> dict[str, list]:
    """link_key -> its track (lon / lat, 6 decimals) from the link-key registry."""
    reg = _registry(data)
    if reg is None:
        return {}
    sub = reg.filter(pl.col("link_key").is_in(keys.unique().implode())).select("link_key", "geometry")
    return {k: [[round(x, 6), round(y, 6)] for x, y in (g or [])] for k, g in sub.iter_rows()}


def _line_row(x: dict) -> dict:
    return {"line": x["line"], "veh_h_day": round(x["veh_s_day"] / 3600, 2),
            "veh_h_day_run": round(x["veh_s_day_run"] / 3600, 2),
            "veh_h_day_stop": round(x["veh_s_day_stop"] / 3600, 2),
            "passages_day": round(x["passages_day"], 1), "peak": x["peak"], "n_days": x["n_days"],
            "reason": x["reason"], "stand_share": None if x["reg_share"] is None else round(x["reg_share"], 2)}


def _stretch_row(rank: int, r: dict, geo: dict, days_per_year: int) -> dict:
    vh = r["veh_s_day"] / 3600
    return {
        "rank": rank, "link_key": r["stretch"], "link_keys": sorted(r["keys"]),
        "from": r["from_t"], "to": r["to_t"], "len_m": round(r["len_m"] or 0, 1),
        "veh_h_day": round(vh, 2), "veh_h_year": round(vh * days_per_year),
        "veh_h_day_run": round(r["veh_s_day_run"] / 3600, 2), "veh_h_day_stop": round(r["veh_s_day_stop"] / 3600, 2),
        "passages_day": round(r["passages_day"], 1),
        "s_per_passage": round(r["veh_s_day"] / r["passages_day"], 1) if r["passages_day"] else None,
        "reasons": sorted({x["reason"] for x in r["by_line"] if x["reason"]}),
        "lines": [_line_row(x) for x in r["by_line"]],
        "c": geo.get(r["stretch"]),
        "cs": [geo[k] for k in sorted(r["keys"]) if k != r["stretch"] and k in geo],
    }


def _stretches(df: pl.DataFrame, n: int, geo: dict, days_per_year: int) -> tuple[list[dict], float, int]:
    """The ``n`` costliest stretches of ``df`` (per line and link), the time lost on all stretches in
    loss and their number."""
    if not df.height:
        return [], 0.0, 0
    per_key = df.group_by("link_key").agg(pl.col("veh_s_day").sum(), pl.col("from_t").first(), pl.col("to_t").first())
    df = df.with_columns(pl.col("link_key").replace_strict(merge_groups(per_key, geo), default=None).alias("stretch"))
    g = (df.group_by("stretch")
         .agg(pl.col("veh_s_day").sum(), pl.col("veh_s_day_run").sum(), pl.col("veh_s_day_stop").sum(),
              pl.col("passages_day").sum(), pl.col("link_key").unique().alias("keys"),
              pl.struct("line", "veh_s_day", "veh_s_day_run", "veh_s_day_stop", "passages_day", "peak", "n_days",
                        "reason", "reg_share").sort_by("veh_s_day", descending=True).alias("by_line"))
         .sort("veh_s_day", descending=True))
    loss = g.filter(pl.col("veh_s_day") > 0)
    tot, n_loss = float(loss["veh_s_day"].sum()), loss.height
    names = df.select("link_key", "from_t", "to_t", "len_m").unique("link_key", keep="first")
    g = g.head(n).join(names, left_on="stretch", right_on="link_key", how="left").sort("veh_s_day", descending=True)
    return [_stretch_row(i + 1, r, geo, days_per_year) for i, r in enumerate(g.iter_rows(named=True))], tot, n_loss


def current_lines(data: Data) -> set[str]:
    """The lines of the latest GTFS feed (what ``/api/lines`` lists)."""
    idx = data.gtfs_index()
    return set(data.routes(idx["sha"][-1])["route_short_name"].cast(pl.Utf8).to_list()) if idx.height else set()


def assemble(data: Data, rq: RankQuery, s: pl.DataFrame, top: int, info: dict, lines: list[str], n_used: int) -> dict:
    """``lines``: the lines with data in the window; ``lines_gone``: those the latest feed (the
    page's line list) no longer has (replacement services, withdrawn lines)."""
    from microsegments.report import title_name
    days_per_year = round(365.25 / 7 * len(set(rq.dow)))
    gone = sorted(set(lines) - current_lines(data), key=by_line)
    base = {"query": rq.canonical(), "lines_total": len(lines), "lines_gone": gone, "lines_used": n_used,
            "days_per_year": days_per_year,
            "algo": ALGO_VERSION, "rank_version": RANK_VERSION, "microsegments": ms.version(),
            "zones": "all" if rq.stops else "running", "reasons": REASONS}
    if not s.height:
        return {**base, "stretches": [], "lines": [], "excluded": {"n": 0, "veh_h_day": 0, "top": []}}
    s = s.with_columns(pl.col("from_name").map_elements(title_name, return_dtype=pl.Utf8).alias("from_t"),
                       pl.col("to_name").map_elements(title_name, return_dtype=pl.Utf8).alias("to_t"))
    geo = _geometry(data, s["link_key"])
    kept = s if rq.terminus else s.filter(pl.col("reason").is_null())
    flagged = s.head(0) if rq.terminus else s.filter(pl.col("reason").is_not_null() & (pl.col("veh_s_day") > 0))
    main, total, _ = _stretches(kept, top, geo, days_per_year)
    # the page says "N tronçons laissés de côté": stretches (lines and platforms merged), not (line, link) rows
    ex_top, ex_tot, ex_n = _stretches(flagged, 10, geo, days_per_year)
    line_tot = (kept.group_by("line").agg(pl.col("veh_s_day").filter(pl.col("veh_s_day") > 0).sum())
                .sort("veh_s_day", descending=True))
    return {**base, "stretches": main, "total_veh_h_day": round(total / 3600, 1),
            "excluded": {"n": ex_n, "veh_h_day": round(ex_tot / 3600, 1),
                         "top": [{k: x[k] for k in ("from", "to", "veh_h_day", "s_per_passage", "reasons", "lines")} for x in ex_top]},
            "lines": [{"line": ln, "veh_h_day": round(v / 3600, 2), **(info.get(ln) or {})} for ln, v in line_tot.iter_rows()]}


def days_excluded(data: Data, rq: RankQuery) -> dict[str, int]:
    """Selected days with data (any line) left out on purpose, per reason."""
    present: set[dt.date] = set()
    for _, dd, _ in window_tables(data, rq):
        if dd is not None and dd.height:
            present |= set(dd["service_date"].unique().to_list())
    dow, left = set(rq.dow), {}
    for d, (why, _) in rq.exclusions(data).items():
        if d.weekday() in dow and d in present:
            left[why] = left.get(why, 0) + 1
    return left


def compute(data: Data, rq: RankQuery, top: int = 50) -> dict:
    """The ranking of ``rq`` from the link cubes (well under a second for a few months)."""
    t0 = time.time()
    # read again: the result is stored (90 days) under the cubes' stamp, and the derive that wrote
    # those cubes may have added link keys (geometry) and lines after this process cached them
    _registry(data, ttl=0)
    info = line_modes(data, ttl=0)
    s, lines, n_used = per_line_links(data, rq, info)
    res = assemble(data, rq, s, top, info, lines, n_used)
    res["days_excluded"] = days_excluded(data, rq)
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
    res = compute(data, RankQuery(w[0], w[1]), top=top)
    data.store.write_json(DEFAULT_PTR, {"from": w[0].isoformat(), "to": w[1].isoformat(), "dow": [0, 1, 2, 3, 4],
                                        "top": top, "written_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
    log.info("ranking default %s..%s: %d stretches, %.1f s", w[0], w[1], len(res["stretches"]), time.time() - t)
    return res


def _print(res: dict) -> None:
    print(json.dumps({k: res.get(k) for k in ("lines_used", "total_veh_h_day", "seconds")}))
    for s in res["stretches"][:25]:
        print(f"{s['rank']:>3} {s['veh_h_day']:>6.2f} h/j {s['s_per_passage'] or 0:>6.1f} s/p  {s['from']} -> {s['to']}  "
              f"[{', '.join(x['line'] for x in s['lines'])}]{' ' + ','.join(s['reasons']) if s['reasons'] else ''}")
    ex = res.get("excluded") or {}
    print(f"excluded: {ex.get('n')} stretches, {ex.get('veh_h_day')} h/day")
    for x in ex.get("top", []):
        print(f"    {x['veh_h_day']:>6.2f} h/j {x['s_per_passage'] or 0:>6.1f} s/p  {x['from']} -> {x['to']}  "
              f"[{', '.join(y['line'] for y in x['lines'])}] {','.join(x['reasons'])}")


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
    _print(compute(data, RankQuery(dt.date.fromisoformat(args.first), dt.date.fromisoformat(args.to), dow,
                                   mode=args.mode, terminus=args.terminus, stops=args.stops), top=args.top))
    return 0


if __name__ == "__main__":
    sys.exit(main())

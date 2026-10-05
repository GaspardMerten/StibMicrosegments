"""Network ranking: the stretches (stop pairs) where STIB vehicles lose the most time, all lines.

    python -m stibms.ranking --default [--bucket gs://b | --local DIR]      # nightly precompute
    python -m stibms.ranking --from 2026-07-01 --to 2026-09-30 [--dow 0-4] [--mode tram]

For each line with derived data in the window, the line's analysis (30 m segments, evening reference,
holidays excluded) gives the vehicle time lost per day on each of its links above the evening
(``analysis.link_costs``). Lines sharing a stretch share its ``link_key`` (the key registry is
global: same stop pair and same geometry within 20 m -> same key), so the costs of the lines on a
stretch add up. Vehicle-hours per day x selected weekdays per year (holidays not removed) gives an
order of magnitude per year.

Per-line parts are cached in the bucket (``results/v{ALGO}/rankline-<key>.parquet``), so a window is
computed incrementally: ``compute(..., budget_s=)`` does as many lines as fit in the budget and
reports progress; the API calls it on each poll of a custom window (Cloud Run only gives CPU during
requests). The assembled ranking is ``results/v{ALGO}/ranking-<key>.json.gz``;
``results/v{ALGO}/ranking-default.json`` points at the nightly default window (the last complete
month of weekdays).
"""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import io
import json
import logging
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass

import polars as pl

from . import ms
from .data import ALGO_VERSION, Data, date_range, month_dates

log = logging.getLogger("stibms.ranking")
MODES = ("all", "tram", "bus", "metro")
RANK_VERSION = 2      # bump when link_costs / assemble change meaning (new cache keys)
DEFAULT_PTR = f"results/v{ALGO_VERSION}/ranking-default.json"
_line_locks: dict[str, threading.Lock] = {}
_lk = threading.Lock()


@dataclass(frozen=True)
class RankQuery:
    first: dt.date
    last: dt.date
    dow: tuple[int, ...] = (0, 1, 2, 3, 4)
    holidays: str = "exclude"
    mode: str = "all"
    terminus: bool = False               # count the links into / out of a line's terminus

    def __post_init__(self):
        if self.first > self.last:
            raise ValueError("from must be <= to")
        if (self.last - self.first).days > 400:
            raise ValueError("period longer than 400 days")
        if self.mode not in MODES:
            raise ValueError(f"mode must be one of {', '.join(MODES)}")
        if not self.dow or any(d not in range(7) for d in self.dow):
            raise ValueError("dow must be a subset of 0..6 (0 = Monday)")

    def canonical(self) -> dict:
        d = asdict(self)
        d["first"], d["last"], d["dow"] = self.first.isoformat(), self.last.isoformat(), sorted(set(self.dow))
        return d

    def line_query(self, line: str):
        from .analysis import Query
        return Query(line=line, first=self.first, last=self.last, dow=tuple(sorted(set(self.dow))),
                     holidays=self.holidays, B=0)

    def key(self, stamp: str) -> str:
        c = self.canonical()
        c.pop("mode")      # mode and terminus only filter the parts; parts are shared
        c.pop("terminus")
        blob = json.dumps([c, ALGO_VERSION, RANK_VERSION, ms.version(), stamp], sort_keys=True)
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
    """line -> {mode, color, text_color, name} from the latest GTFS feed."""
    from microsegments.report import ROUTE_TYPES
    idx = data.gtfs_index()
    if not idx.height:
        return {}
    out = {}
    # latest feed first, earlier feeds fill lines that disappeared
    for sha in reversed(idx["sha"].unique(maintain_order=True).to_list()[-6:]):
        for r in data.routes(sha).iter_rows(named=True):
            ln = str(r["route_short_name"])
            if ln in out:
                continue
            rt = r.get("route_type")
            out[ln] = {"mode": ROUTE_TYPES.get(int(rt)) if rt is not None else None, "color": r.get("route_color"),
                       "text_color": r.get("route_text_color"), "name": r.get("route_long_name")}
    return out


def lines_in(data: Data, rq: RankQuery) -> list[str]:
    """Lines with a derived month overlapping the window, any mode (sorted)."""
    from .data import months_between
    want = set(months_between(rq.first, rq.last)) & set(data.derived_months())
    lines = [ln for ln, ms_ in data.line_months().items() if want & set(ms_)]
    return sorted(lines, key=lambda x: (not x.isdigit(), len(x), x))


def _stamp(data: Data, rq: RankQuery) -> str:
    return data.stamp(date_range(rq.first, rq.last))


def _part_rel(key: str, line: str) -> str:
    return f"results/v{ALGO_VERSION}/rankline-{key}-{line}.parquet"


def line_part(data: Data, rq: RankQuery, key: str, line: str) -> pl.DataFrame | None:
    """Link costs of one line (bucket-cached). None when the line has no usable data."""
    from . import analysis as A
    rel = _part_rel(key, line)
    hit = data.read_optional(rel)
    if hit is not None:
        return hit if hit.height else None
    with _lk:
        lock = _line_locks.setdefault(rel, threading.Lock())
    with lock:
        hit = data.read_optional(rel)
        if hit is not None:
            return hit if hit.height else None
        t = time.time()
        try:
            r = A.run(data, rq.line_query(line), with_mode=False)
            df = A.link_costs(r)
            del r
        except A.NotFound:
            df = pl.DataFrame(schema=A.LINK_COST_SCHEMA)
        except Exception:  # noqa: BLE001 - one broken line must not block the ranking
            log.exception("ranking: line %s failed", line)
            df = pl.DataFrame(schema=A.LINK_COST_SCHEMA)
        buf = io.BytesIO()
        df.write_parquet(buf, compression="zstd")
        data.store.write_bytes(rel, buf.getvalue())
        log.info("ranking %s line %s: %d links in %.1f s", key[:8], line, df.height, time.time() - t)
        return df if df.height else None


def assemble(data: Data, rq: RankQuery, parts: dict[str, pl.DataFrame], top: int, info: dict) -> dict:
    """Sum the per-line link costs per link_key, rank, attach geometry and the lines involved."""
    modes = info
    keep = {ln: df for ln, df in parts.items() if df is not None and df.height
            and (rq.mode == "all" or (modes.get(ln) or {}).get("mode") == rq.mode)}
    days_per_year = round(365.25 / 7 * len(set(rq.dow)))
    base = {"query": rq.canonical(), "lines_total": len(parts), "lines_used": len(keep),
            "days_per_year": days_per_year, "algo": ALGO_VERSION, "microsegments": ms.version()}
    if not keep:
        return {**base, "stretches": [], "lines": []}
    allp = pl.concat([df.with_columns(pl.lit(ln).alias("line")) for ln, df in keep.items()], how="diagonal_relaxed")
    if not rq.terminus and "terminal" in allp.columns:
        allp = allp.filter(~pl.col("terminal").fill_null(False))
    per_line = (allp.group_by("link_key", "line")
                .agg(pl.col("veh_s_day").sum(), pl.col("veh_s_day_line").sum(), pl.col("passages_day").sum(),
                     pl.col("peak_hour").first(), pl.col("n_days").max()))
    g = (per_line.group_by("link_key")
         .agg(pl.col("veh_s_day").sum(), pl.col("veh_s_day_line").sum(), pl.col("passages_day").sum(),
              pl.struct("line", "veh_s_day", "veh_s_day_line", "passages_day", "peak_hour", "n_days")
              .sort_by("veh_s_day", descending=True).alias("by_line"))
         .sort("veh_s_day", descending=True))
    names = allp.select("link_key", "from_name", "to_name", "len_m").unique("link_key", keep="first")
    g = g.join(names, on="link_key", how="left").sort("veh_s_day", descending=True)
    total_pos = float(g.filter(pl.col("veh_s_day") > 0)["veh_s_day"].sum())
    head = g.head(top)
    geo = _geometry(data, head["link_key"].to_list())
    from microsegments.report import title_name
    out = []
    for i, r in enumerate(head.iter_rows(named=True)):
        vh = r["veh_s_day"] / 3600
        out.append({
            "rank": i + 1, "link_key": r["link_key"],
            "from": title_name(r["from_name"]), "to": title_name(r["to_name"]),
            "len_m": round(r["len_m"] or 0, 1),
            "veh_h_day": round(vh, 2), "veh_h_year": round(vh * days_per_year),
            "veh_h_day_line": round(r["veh_s_day_line"] / 3600, 2),
            "passages_day": round(r["passages_day"], 1),
            "s_per_passage": round(r["veh_s_day"] / r["passages_day"], 1) if r["passages_day"] else None,
            "lines": [{"line": x["line"], "veh_h_day": round(x["veh_s_day"] / 3600, 2),
                       "veh_h_day_line": round(x["veh_s_day_line"] / 3600, 2),
                       "passages_day": round(x["passages_day"], 1),
                       "peak_hour": x["peak_hour"], "n_days": x["n_days"]} for x in r["by_line"]],
            "c": geo.get(r["link_key"]),
        })
    line_tot = (per_line.group_by("line").agg(pl.col("veh_s_day").filter(pl.col("veh_s_day") > 0).sum())
                .sort("veh_s_day", descending=True))
    return {**base, "stretches": out,
            "total_veh_h_day": round(total_pos / 3600, 1),
            "lines": [{"line": ln, "veh_h_day": round(v / 3600, 2), **(modes.get(ln) or {})}
                      for ln, v in line_tot.iter_rows()]}


def _geometry(data: Data, keys: list[str]) -> dict[str, list]:
    reg = data.read_optional(f"derived/v{ALGO_VERSION}/linkkeys.parquet")
    if reg is None or not keys:
        return {}
    sub = reg.filter(pl.col("link_key").is_in(keys)).select("link_key", "geometry")
    return {k: [[round(x, 6), round(y, 6)] for x, y in (g or [])] for k, g in sub.iter_rows()}


def _rel(key: str, rq: RankQuery, top: int) -> str:
    return f"results/v{ALGO_VERSION}/ranking-{key}-{rq.mode}{'-term' if rq.terminus else ''}-{top}.json.gz"


def compute(data: Data, rq: RankQuery, top: int = 50, budget_s: float | None = None,
            progress=None) -> dict:
    """The ranking of ``rq`` (bucket-cached), or, if the budget runs out first, a progress report
    ``{"status": "running", "done": n, "total": m}``."""
    stamp = _stamp(data, rq)
    key = rq.key(stamp)
    rel = _rel(key, rq, top)
    try:
        return json.loads(gzip.decompress(data.store.read_bytes(rel)))
    except FileNotFoundError:
        pass
    info = line_modes(data)
    lines = [ln for ln in lines_in(data, rq) if rq.mode == "all" or (info.get(ln) or {}).get("mode") == rq.mode]
    t0 = time.time()
    parts: dict[str, pl.DataFrame | None] = {}
    todo = []
    existing = set(data.store.glob(f"results/v{ALGO_VERSION}/rankline-{key}-*.parquet"))
    for ln in lines:
        if _part_rel(key, ln) in existing:
            parts[ln] = data.read_optional(_part_rel(key, ln))
        else:
            todo.append(ln)
    for i, ln in enumerate(todo):
        if budget_s is not None and i > 0 and time.time() - t0 > budget_s:   # at least one line per call
            return {"status": "running", "done": len(lines) - len(todo) + i, "total": len(lines),
                    "query": rq.canonical(), "key": key}
        parts[ln] = line_part(data, rq, key, ln)
        if progress:
            progress(len(lines) - len(todo) + i + 1, len(lines), ln)
    res = assemble(data, rq, parts, top, info)
    res["status"] = "done"
    res["stamp"] = stamp
    res["computed_at"] = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    res["seconds"] = round(time.time() - t0, 1)
    data.store.write_bytes(rel, gzip.compress(json.dumps(res, separators=(",", ":"), default=str).encode(), 6))
    return res


def precompute_default(data: Data, top: int = 50) -> dict | None:
    """Nightly: the default window (all modes, then each mode from the same cached parts)."""
    w = default_window(data)
    if w is None:
        log.warning("ranking: no complete derived month, nothing to precompute")
        return None
    t = time.time()
    out = None
    for mode in MODES:
        rq = RankQuery(w[0], w[1], mode=mode)
        res = compute(data, rq, top=top,
                      progress=lambda i, n, ln: log.info("ranking default: %d/%d (line %s)", i, n, ln) if i % 10 == 0 or i == n else None)
        if mode == "all":
            out = res
    data.store.write_json(DEFAULT_PTR, {"from": w[0].isoformat(), "to": w[1].isoformat(), "dow": [0, 1, 2, 3, 4],
                                        "top": top, "written_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")})
    log.info("ranking default %s..%s: %d stretches, %.0f s", w[0], w[1], len(out["stretches"]) if out else 0, time.time() - t)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m stibms.ranking", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--default", action="store_true", help="the nightly default window (last complete month)")
    ap.add_argument("--from", dest="first")
    ap.add_argument("--to")
    ap.add_argument("--dow", default="0-4")
    ap.add_argument("--mode", default="all", choices=MODES)
    ap.add_argument("--top", type=int, default=50)
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
    res = compute(data, RankQuery(dt.date.fromisoformat(args.first), dt.date.fromisoformat(args.to), dow, mode=args.mode),
                  top=args.top, progress=lambda i, n, ln: log.info("%d/%d line %s", i, n, ln))
    print(json.dumps({k: res.get(k) for k in ("lines_used", "total_veh_h_day", "seconds")}))
    for s in res["stretches"][:15]:
        print(f"{s['rank']:>3} {s['veh_h_day']:>7.2f} h/j  {s['from']} -> {s['to']}  "
              f"[{', '.join(x['line'] for x in s['lines'])}]")
    return 0


if __name__ == "__main__":
    sys.exit(main())

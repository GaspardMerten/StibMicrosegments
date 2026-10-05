"""Raw -> derived, per month: networks per GTFS feed, feed coverage, placed observations and passages
per line (layout in ``stibms.data``).

    python -m stibms.derive --month 2025-03 [--lines all|55,71] [--bucket gs://b | --local DIR]

Steps for a month M (dates = ingested service dates of M):

1. **Patterns** per GTFS feed (sha) used in M, for all lines, on every ingested date that uses the
   feed (``derived/v{ALGO}/patterns/sha=<sha>/``). Link keys come from the persistent registry
   ``linkkeys.parquet``, so they stay stable across feeds and months. A feed is rebuilt only when
   its date set grew (the nightly feed), and feeds are processed in date order.
2. **Coverage** of the whole feed per date x hour (``coverage/month=M.parquet``).
3. **Placed + passages** per line: the month's vehicle-distance rows of the line are located on the
   network of their date (``microsegments.locate.place``), then passages are counted from the tracks
   and the punctuality stop events (``microsegments.tracks.passages``). Feed lengths and terminus
   aliases are learned over the whole month.

Segment length, phase and stop zone are not fixed here: the API segments on request.
Re-running a month rewrites its files (idempotent). Run months one at a time: the link-key registry
is read at the start and written at the end, so two concurrent runs would race on it.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path

import polars as pl

from . import ms
from .data import ALGO_VERSION, PLACED_COLS, Data, derived, month_dates, month_key
from .servicedays import parse_date

__all__ = ["ALGO_VERSION", "derive_month", "main"]
log = logging.getLogger("stibms.derive")


# ---------------------------------------------------------------------------------- patterns
def ensure_patterns(data: Data, shas: list[str], registry, force: bool = False) -> dict:
    """(Re)build the per-feed network tables of ``shas`` (all lines). Returns timing / counts."""
    from microsegments.network import GtfsSource, build_network

    idx = data.gtfs_index()
    g = idx.group_by("sha").agg(pl.col("service_date").min())
    first = dict(zip(g["sha"].to_list(), g["service_date"].to_list()))
    report = {}
    for sha in sorted(set(shas), key=lambda s: first.get(s, dt.date.max)):
        dates = sorted(idx.filter(pl.col("sha") == sha)["service_date"].to_list())
        base = derived(f"patterns/sha={sha}")
        if not force and data.store.exists(f"{base}/_SUCCESS"):
            have = set(data.store.read_json(f"{base}/_SUCCESS").get("dates", []))
            if {d.isoformat() for d in dates} <= have:
                continue
        t0 = time.time()
        src = GtfsSource(data.feed_dir(sha))
        routes = data.routes(sha)
        lines = sorted(set(routes["route_short_name"].drop_nulls().cast(pl.Utf8).to_list()))
        out = {n: [] for n in ("patterns", "links", "stops", "pattern_days")}
        failed = {}
        for line in lines:
            try:
                net = build_network(src, line, dates, registry=registry)
            except Exception as e:  # one broken route must not block the feed
                failed[line] = repr(e)[:200]
                continue
            if not net.patterns.height:
                continue
            lit = pl.lit(line, pl.Utf8).alias("line")
            out["patterns"].append(net.patterns.with_columns(lit))
            out["links"].append(net.links)
            out["stops"].append(net.stops.with_columns(lit))
            out["pattern_days"].append(net.pattern_days.with_columns(lit))
        for n, frames in out.items():
            df = pl.concat(frames, how="vertical_relaxed") if frames else pl.DataFrame()
            if n == "links":
                df = df.unique(["pattern_uid", "link_idx"], keep="first", maintain_order=True)
            data.store.write_parquet(f"{base}/{n}.parquet", df)
        data.store.write_json(f"{base}/_SUCCESS", {
            "sha": sha, "dates": [d.isoformat() for d in dates], "lines": len(lines), "failed": failed,
            "algo": ALGO_VERSION, "microsegments": ms.version(),
            "written_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 1)})
        report[sha[:12]] = {"dates": len(dates), "lines": len(lines), "failed": len(failed),
                            "seconds": round(time.time() - t0, 1)}
        log.info("patterns %s: %d dates, %d lines (%d failed), %.1f s", sha[:12], len(dates), len(lines),
                 len(failed), time.time() - t0)
    return report


# ---------------------------------------------------------------------------------- coverage
def write_coverage(data: Data, month: str, dates: list[dt.date]) -> pl.DataFrame:
    from microsegments.io import coverage_from_config
    cfg = ms.config()
    snaps = data.snapshots(dates)
    cov = coverage_from_config(snaps, cfg, dates=(min(dates), max(dates)))
    data.store.write_parquet(derived(f"coverage/month={month}.parquet"), cov)
    return cov


# ---------------------------------------------------------------------------------- per line
def split_by_line(data: Data, dates: list[dt.date], tmp: Path, lines: set[str] | None) -> set[str]:
    """Write tmp/vd/line=X/D.parquet and tmp/p/line=X/D.parquet from the day files (one read per day)."""
    seen: set[str] = set()
    for d in dates:
        vd = data.store.read_parquet(f"raw/vd/date={d}/vd.parquet")
        if lines is not None:
            vd = vd.filter(pl.col("line").is_in(list(lines)))
        for (line,), g in vd.group_by("line"):
            if line is None:
                continue
            p = tmp / "vd" / f"line={line}"
            p.mkdir(parents=True, exist_ok=True)
            g.write_parquet(p / f"{d}.parquet", compression="lz4")
            seen.add(str(line))
        rel = f"raw/punctuality/date={d}/p.parquet"
        if data.store.exists(rel):
            pu = data.store.read_parquet(rel)
            pu = pu.with_columns(pl.col("route_id").cast(pl.Utf8))
            if lines is not None:
                pu = pu.filter(pl.col("route_id").is_in(list(lines)))
            for (line,), g in pu.group_by("route_id"):
                p = tmp / "p" / f"line={line}"
                p.mkdir(parents=True, exist_ok=True)
                g.write_parquet(p / f"{d}.parquet", compression="lz4")
    return seen


def derive_line(data: Data, month: str, line: str, dates: list[dt.date], tmp: Path, cfg) -> dict:
    from microsegments.io.events import read_stib_punctuality
    from microsegments.io.stib import compact_to_observations

    t0 = time.time()
    files = sorted((tmp / "vd" / f"line={line}").glob("*.parquet"))
    obs = compact_to_observations(pl.scan_parquet(files), route_id=line,
                                  id_normaliser=cfg.input.linear.id_normaliser).collect()
    net = data.network(line, dates)
    if not net.patterns.height:
        return {"line": line, "skipped": "no GTFS pattern"}
    placed = ms.place(obs, net, cfg)
    frame = ms.placed_frame(placed)
    t_place = time.time() - t0
    pfiles = sorted((tmp / "p" / f"line={line}").glob("*.parquet"))
    events = read_stib_punctuality(pfiles, route=line).collect() if pfiles else None
    pas = ms.passages(placed, net, events)
    # Only counted rows are stored: terminus pseudo rows and layover only matter for tracking, and
    # passages are computed here from the full frame.
    keep = [c for c in PLACED_COLS if c in frame.columns]
    out = frame.filter(pl.col("count").fill_null(True)).select(keep).sort("ts")
    data.store.write_parquet(derived(f"placed/line={line}/month={month}.parquet"), out)
    data.store.write_parquet(derived(f"passages/line={line}/month={month}.parquet"), pas)
    rep = getattr(placed, "report", {}) or {}
    return {"line": line, "rows": obs.height, "placed": out.height, "passages": pas.height,
            "report": {str(k): int(v) for k, v in rep.items()},
            "aliases": dict(getattr(placed, "aliases", {}) or {}),
            "seconds": round(time.time() - t0, 2), "seconds_place": round(t_place, 2)}


# ---------------------------------------------------------------------------------- month
def derive_month(data: Data, month: str, lines: list[str] | None = None, force_patterns: bool = False) -> dict:
    t0 = time.time()
    ingested = set(data.ingested_dates())
    dates = [d for d in month_dates(month) if d in ingested]
    if not dates:
        log.warning("%s: no ingested date", month)
        return {"month": month, "dates": 0}
    cfg = ms.config()
    registry = data.link_registry()
    n_keys = len(registry)
    shas = sorted(set(data.shas_for(dates).values()))
    pat = ensure_patterns(data, shas, registry, force=force_patterns)
    if len(registry) != n_keys:
        data.store.write_parquet(derived("linkkeys.parquet"), registry.table)
    t_pat = time.time() - t0

    t1 = time.time()
    write_coverage(data, month, dates)
    t_cov = time.time() - t1

    per_line, failed = [], {}
    tmp = Path(tempfile.mkdtemp(prefix=f"derive-{month}-"))
    try:
        t2 = time.time()
        seen = split_by_line(data, dates, tmp, set(lines) if lines else None)
        t_split = time.time() - t2
        todo = sorted(seen, key=lambda x: (len(x), x)) if not lines else [x for x in lines if x in seen]
        for line in todo:
            try:
                r = derive_line(data, month, line, dates, tmp, cfg)
                per_line.append(r)
                log.info("%s line %s: %s", month, line, {k: r[k] for k in r if k not in ("report", "aliases")})
            except ms.PackageMissing:
                raise
            except Exception as e:
                log.exception("%s line %s failed", month, line)
                failed[line] = repr(e)[:300]
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    report = {
        "month": month, "algo": ALGO_VERSION, "microsegments": ms.version(),
        "dates": [d.isoformat() for d in dates], "lines": len(per_line), "failed": failed,
        "seconds": {"patterns": round(t_pat, 1), "coverage": round(t_cov, 1), "split": round(t_split, 1),
                    "lines": round(sum(r.get("seconds", 0) for r in per_line), 1),
                    "total": round(time.time() - t0, 1)},
        "patterns": pat, "per_line": per_line,
        "written_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }
    data.store.write_json(derived(f"_SUCCESS/month={month}.json"), report)
    data.invalidate()
    log.info("%s: %d dates, %d lines, %d failed, %s", month, len(dates), len(per_line), len(failed),
             report["seconds"])
    return report


def months_for(first: dt.date, last: dt.date) -> list[str]:
    from .data import months_between
    return months_between(first, last)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m stibms.derive", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--month", help="YYYY-MM, or 'current' (month of yesterday's service day)")
    ap.add_argument("--from", dest="first", help="first month YYYY-MM (with --to: a range of months)")
    ap.add_argument("--to", help="last month YYYY-MM")
    ap.add_argument("--lines", default="all", help="'all' or comma-separated line numbers")
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--bucket", help="gs://bucket[/prefix] (default $MS_BUCKET)")
    where.add_argument("--local", help="local directory instead of GCS")
    ap.add_argument("--force-patterns", action="store_true", help="rebuild the per-feed networks")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    root = args.local or args.bucket or os.environ.get("MS_BUCKET")
    if not root:
        ap.error("give --bucket, --local or set MS_BUCKET")
    if args.first:
        a = dt.date.fromisoformat(args.first + "-01")
        b = dt.date.fromisoformat((args.to or args.first) + "-01")
        months = months_for(a, b)
    elif args.month in (None, "current"):
        months = [month_key(parse_date("yesterday"))]
    else:
        months = [args.month]
    lines = None if args.lines == "all" else [x.strip() for x in args.lines.split(",") if x.strip()]
    data = Data(root)
    for m in months:
        rep = derive_month(data, m, lines, force_patterns=args.force_patterns)
        print(json.dumps({k: rep.get(k) for k in ("month", "lines", "failed", "seconds")}, default=str))
        if rep.get("failed"):
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

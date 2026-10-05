"""Ingest STIB feeds from MobilityTwin into compact Parquet, one service day at a time.

    python -m stibms.ingest --date 2025-03-18 [--to 2025-03-31] [--bucket gs://b | --local DIR]
                            [--shard-from-env] [--force] [--workers 16]

Per service date D (04:00 -> 03:00 Europe/Brussels), all lines:

    raw/vd/date=D/vd.parquet           ts:int64 (epoch s), line, dir, point:str, dist:int32;
                                       sorted by (line, ts), zstd, row groups packed by line
    raw/vd/date=D/snaps.parquet        ts:int64, n_rows:uint32, frozen:bool (every poll, see vd.py)
    raw/punctuality/date=D/p.parquet   stib/punctuality, needed columns (calendar day D)
    gtfs/feeds/<sha>/<table>.parquet   feed in force on D, stored once per content sha
    gtfs/dates/date=D.json, gtfs/index.parquet (service_date -> sha)
    raw/_SUCCESS/date=D.json           counts, sources, bytes; written last

A date with its _SUCCESS is skipped unless --force, so reruns and job retries are no-ops.
--date accepts ``yesterday`` (the nightly job). With --shard-from-env the date list is split
round-robin by CLOUD_RUN_TASK_INDEX / CLOUD_RUN_TASK_COUNT (Cloud Run job tasks).
The storage root defaults to $MS_BUCKET.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
import traceback
from datetime import date, datetime, timezone

from . import __version__, gtfs, punctuality, vd
from .mobilitytwin import MobilityTwin
from .servicedays import date_range, parse_date, window
from .storage import Store

log = logging.getLogger("stibms.ingest")
READY_MARGIN_S = 20 * 60  # the last hourly bulk file lands ~30 min after the hour; JSON fills the rest


def success_path(d: date) -> str:
    return f"raw/_SUCCESS/date={d.isoformat()}.json"


def shard(dates: list[date], index: int, count: int) -> list[date]:
    return dates[index::count] if count > 1 else list(dates)


def ingest_date(mt: MobilityTwin, store: Store, d: date, force: bool = False, workers: int = vd.WORKERS) -> dict | None:
    if not force and store.exists(success_path(d)):
        log.info("%s: already ingested, skipped", d)
        return None
    _, t1 = window(d)
    if time.time() < t1 + READY_MARGIN_S:
        raise RuntimeError(f"{d}: service day not closed yet (ends {datetime.fromtimestamp(t1, timezone.utc)})")
    started, b0, w0 = time.time(), mt.bytes_downloaded, store.bytes_written

    day = vd.fetch_day(mt, d, workers=workers)
    t_vd = time.time() - started
    size_vd = store.write_table(f"raw/vd/date={d}/vd.parquet", vd.to_arrow(day.rows),
                                row_group_bounds=vd.row_group_bounds(day.rows))
    size_snaps = store.write_parquet(f"raw/vd/date={d}/snaps.parquet", day.snaps)

    p_df, p_stats = punctuality.fetch_day(mt, d)
    if p_df is not None:
        p_stats["bytes"] = store.write_parquet(f"raw/punctuality/date={d}/p.parquet", p_df)

    g_stats = gtfs.ingest_day(mt, store, d)

    report = {
        "date": d.isoformat(),
        "version": __version__,
        "written_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "vd": {**day.stats, "bytes_vd": size_vd, "bytes_snaps": size_snaps, "seconds": round(t_vd, 1)},
        "punctuality": p_stats,
        "gtfs": g_stats,
        "bytes_downloaded": mt.bytes_downloaded - b0,
        "bytes_written": store.bytes_written - w0,
        "seconds": round(time.time() - started, 1),
    }
    store.write_json(success_path(d), report)
    log.info("%s: %d polls (%d frozen, %d empty), %d rows, %d lines; punctuality %s; gtfs %s; "
             "%.1f MB down, %.1f MB written, %.0f s", d, day.stats["polls"], day.stats["frozen"],
             day.stats["empty"], day.stats["rows"], day.stats["lines"], p_stats.get("rows", "missing"),
             g_stats.get("sha", "missing")[:12], report["bytes_downloaded"] / 1e6,
             report["bytes_written"] / 1e6, report["seconds"])
    return report


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m stibms.ingest", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", required=True, help="first service date (YYYY-MM-DD or 'yesterday')")
    ap.add_argument("--to", help="last service date, inclusive (default: --date)")
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--bucket", help="gs://bucket[/prefix] (default $MS_BUCKET)")
    where.add_argument("--local", help="local directory instead of GCS")
    ap.add_argument("--shard-from-env", action="store_true",
                    help="split dates by CLOUD_RUN_TASK_INDEX / CLOUD_RUN_TASK_COUNT")
    ap.add_argument("--force", action="store_true", help="rewrite dates that have a _SUCCESS")
    ap.add_argument("--workers", type=int, default=vd.WORKERS)
    ap.add_argument("--no-index", action="store_true", help="do not rebuild gtfs/index.parquet")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

    root = args.local or args.bucket or os.environ.get("MS_BUCKET")
    if not root:
        ap.error("give --bucket, --local or set MS_BUCKET")
    first = parse_date(args.date)
    last = parse_date(args.to) if args.to else first
    dates = date_range(first, last)
    if args.shard_from_env:
        idx = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
        cnt = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
        dates = shard(dates, idx, cnt)
        log.info("task %d/%d: %d dates", idx, cnt, len(dates))

    store, mt = Store(root), MobilityTwin()
    failed = []
    for d in dates:
        try:
            ingest_date(mt, store, d, force=args.force, workers=args.workers)
        except Exception:  # keep going; the job retries and done dates are skipped
            log.error("%s failed:\n%s", d, traceback.format_exc())
            failed.append(d)
    if not args.no_index:
        gtfs.rebuild_index(store)
    if failed:
        log.error("%d dates failed: %s", len(failed), ", ".join(map(str, failed)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

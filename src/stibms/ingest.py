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

Nightly (``--date yesterday``): the touched month is re-derived (on the 1st, yesterday is the last day
of the previous month, so that month is completed; a month whose derive is missing is retried, and
the previous month once more if it ended without its full run), then the network ranking of the
default window is refreshed (``stibms.ranking``) and the pages' default requests are precomputed
into the result cache (``stibms.warm``, a subprocess given the time left in the job).

A date with its _SUCCESS is skipped unless --force, so reruns and job retries are no-ops.
--date accepts ``yesterday`` (the nightly job). With --shard-from-env the date list is split
round-robin by CLOUD_RUN_TASK_INDEX / CLOUD_RUN_TASK_COUNT (Cloud Run job tasks).
The storage root defaults to $MS_BUCKET.
"""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone

from . import __version__, gtfs, punctuality, vd
from .data import derived, month_key
from .mobilitytwin import MobilityTwin
from .servicedays import date_range, parse_date, window
from .storage import Store

log = logging.getLogger("stibms.ingest")
STARTED = time.time()     # ~ process start: the warm-up gets what is left of the job's timeout
READY_MARGIN_S = 20 * 60  # the last hourly bulk file lands ~30 min after the hour; JSON fills the rest


def success_path(d: date) -> str:
    return f"raw/_SUCCESS/date={d.isoformat()}.json"


def shard(dates: list[date], index: int, count: int) -> list[date]:
    return dates[index::count] if count > 1 else list(dates)


PARTIAL_SHARE = 0.6   # a day with fewer lines than this share of the recent median is flagged


def source_check(store: Store, d: date, lines: int, back: int = 10) -> dict:
    """Lines of the day vs the median of the previous ingested days of the same kind (weekday /
    Saturday / Sunday). ``partial_source``: the feed itself carried only part of the network that day
    (see vd.py); the analysis then excludes the missing lines per line."""
    kind = lambda x: max(x.weekday(), 4)  # noqa: E731 - Monday-Friday (4), Saturday (5), Sunday (6)
    prev = []
    for i in range(1, 29):
        x = d - timedelta(days=i)
        if kind(x) != kind(d):
            continue
        try:
            prev.append(int(store.read_json(success_path(x))["vd"]["lines"]))
        except (FileNotFoundError, KeyError, ValueError):
            continue
        if len(prev) >= back:
            break
    prev = [n for n in prev if n > 0]
    if len(prev) < 3:
        return {"partial_source": False, "lines_ref": None}
    ref = sorted(prev)[len(prev) // 2]
    return {"partial_source": bool(lines < PARTIAL_SHARE * ref), "lines_ref": ref}


def ingest_date(mt: MobilityTwin, store: Store, d: date, force: bool = False, workers: int = vd.WORKERS) -> dict | None:
    if not force and store.exists(success_path(d)):
        log.info("%s: already ingested, skipped", d)
        return None
    _, t1 = window(d)
    if time.time() < t1 + READY_MARGIN_S:
        raise RuntimeError(f"{d}: service day not closed yet (ends {datetime.fromtimestamp(t1, timezone.utc)})")
    started, b0, w0 = time.time(), mt.bytes_downloaded, store.bytes_written

    day = vd.fetch_day(mt, d, workers=workers)
    day.stats.update(source_check(store, d, day.stats.get("lines", 0)))
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
    if day.stats.get("partial_source"):
        log.warning("%s: only %d lines in the feed (usually %s): partial at the source", d,
                    day.stats["lines"], day.stats.get("lines_ref"))
    if day.stats.get("bulk_dropped"):
        log.warning("%s: incomplete bulk file(s) replaced by JSON snapshots: %s", d, day.stats["bulk_dropped"])
    log.info("%s: %d polls (%d frozen, %d empty), %d rows, %d lines; punctuality %s; gtfs %s; "
             "%.1f MB down, %.1f MB written, %.0f s", d, day.stats["polls"], day.stats["frozen"],
             day.stats["empty"], day.stats["rows"], day.stats["lines"], p_stats.get("rows", "missing"),
             g_stats.get("sha", "missing")[:12], report["bytes_downloaded"] / 1e6,
             report["bytes_written"] / 1e6, report["seconds"])
    return report


def _have_package() -> bool:
    import importlib.util
    return importlib.util.find_spec("microsegments") is not None


def derive_months(store: Store, months: list[str]) -> bool:
    """Run stibms.derive on ``months`` (all lines). Returns True if something failed."""
    if not _have_package():
        log.warning("microsegments is not installed: derive skipped")
        return False
    from . import derive
    from .data import Data
    data, bad = Data(store), False
    for m in months:
        try:
            rep = derive.derive_month(data, m)
            bad |= bool(rep.get("failed"))
        except Exception:
            log.error("derive %s failed:\n%s", m, traceback.format_exc())
            bad = True
    return bad


def refresh_ranking(store: Store) -> bool:
    """Precompute the default network ranking. Returns True if it failed."""
    if not _have_package():
        return False
    try:
        from . import ranking
        from .data import Data
        ranking.precompute_default(Data(store))
        return False
    except Exception:
        log.error("ranking precompute failed:\n%s", traceback.format_exc())
        return True


JOB_S = 3300       # the job's timeout (terraform/run.tf: 3600 s) less a margin
MIN_WARM_S = 120   # less left than this: no warm-up


def warm_budget() -> float:
    return JOB_S - (time.time() - STARTED)


def drop_feed_copies() -> None:
    """Remove the local GTFS feed copies (``Data.feed_dir``; /tmp is memory on Cloud Run). Nothing in
    this process reads them after the derive; the warm-up downloads again the few it needs."""
    shutil.rmtree(os.path.join(os.environ.get("MS_CACHE", "/tmp/stibms-cache"), "gtfs"), ignore_errors=True)


def warm_pages(root: str) -> None:
    """Precompute the default page results in a separate process (its memory is freed on exit), in
    the time left in the job. Only a cache: a failure or the budget running out is logged, not reported."""
    import subprocess
    if not _have_package():
        return
    budget = warm_budget()
    if budget < MIN_WARM_S:
        log.warning("warm-up skipped: %.0f s left in the job", budget)
        return
    remote = "://" in root
    if remote:
        drop_feed_copies()
    try:
        r = subprocess.run([sys.executable, "-m", "stibms.warm", "--bucket" if remote else "--local", root], timeout=budget)
        if r.returncode:
            log.warning("warm-up ended with code %d", r.returncode)
    except subprocess.TimeoutExpired:
        log.warning("warm-up stopped after %.0f s", budget)


def _parser() -> argparse.ArgumentParser:
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
    ap.add_argument("--no-ranking", action="store_true", help="nightly: do not refresh the network ranking")
    ap.add_argument("--no-warm", action="store_true", help="nightly: do not precompute the default pages")
    ap.add_argument("--no-derive", action="store_true",
                    help="do not re-derive the touched months afterwards (always skipped with --shard-from-env)")
    return ap


def _dates(args) -> list[date]:
    first = parse_date(args.date)
    dates = date_range(first, parse_date(args.to) if args.to else first)
    if args.shard_from_env:
        idx = int(os.environ.get("CLOUD_RUN_TASK_INDEX", "0"))
        cnt = int(os.environ.get("CLOUD_RUN_TASK_COUNT", "1"))
        dates = shard(dates, idx, cnt)
        log.info("task %d/%d: %d dates", idx, cnt, len(dates))
    return dates


def _nightly_extra(store: Store, m: str) -> set[str]:
    """Months the nightly derives without a new day: the month of yesterday if its derive is missing
    (a failed night), the month before if it ended without a full run of its final state
    (``increment.needs_close``: its last day's ingest failed)."""
    from . import increment
    from .data import month_dates
    out = set() if store.exists(derived(f"_SUCCESS/month={m}.json")) else {m}
    before = month_key(month_dates(m)[0] - timedelta(days=1))
    try:
        prev = store.read_json(derived(f"_SUCCESS/month={before}.json"))
    except FileNotFoundError:
        prev = None
    return out | ({before} if increment.needs_close(before, prev) else set())


def _after_ingest(store: Store, args, done: list[date]) -> bool:
    """Re-derive the months that got new days (the nightly run: the current month, all lines), then
    on the nightly run refresh the default ranking. Returns True if something failed."""
    nightly = args.date.strip().lower() == "yesterday"
    months = {month_key(d) for d in done}
    if nightly:
        months |= _nightly_extra(store, month_key(parse_date("yesterday")))
    bad = derive_months(store, sorted(months)) if months else False
    # the network ranking of the default window (last complete month): a sum over the link cubes
    if nightly and not args.no_ranking:
        bad |= refresh_ranking(store)
    if nightly and not args.no_warm:
        warm_pages(args.local or args.bucket or os.environ["MS_BUCKET"])
    return bad


def main(argv: list[str] | None = None) -> int:
    ap = _parser()
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    root = args.local or args.bucket or os.environ.get("MS_BUCKET")
    if not root:
        ap.error("give --bucket, --local or set MS_BUCKET")
    dates = _dates(args)
    store, mt = Store(root), MobilityTwin()
    failed, done = [], []
    for d in dates:
        try:
            if ingest_date(mt, store, d, force=args.force, workers=args.workers) is not None:
                done.append(d)
        except Exception:  # keep going; the job retries and done dates are skipped
            log.error("%s failed:\n%s", d, traceback.format_exc())
            failed.append(d)
    if not args.no_index:
        gtfs.rebuild_index(store)
    # Backfill tasks run in parallel and would race on the link-key registry: they skip the derive
    # and the months are derived afterwards with python -m stibms.derive --from A --to B.
    if not args.no_derive and not args.shard_from_env and _after_ingest(store, args, done):
        return 1
    if failed:
        log.error("%d dates failed: %s", len(failed), ", ".join(map(str, failed)))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

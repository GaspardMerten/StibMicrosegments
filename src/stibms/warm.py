"""Precompute the pages' default requests into the result cache (run after the nightly derive).

    python -m stibms.warm [--bucket gs://b | --local DIR] [--lines 55,71] [--workers 2] [--no-analysis]

Per line with derived data (``/api/lines``, latest GTFS), on its default period (``available.default``,
what ``static/lines.js`` puts in the form) with the API's default parameters: ``/api/coverage``,
``/api/versions`` and ``/api/analysis`` (the frame of the line page; both directions in one result);
then the ranking page's default (``/api/ranking?dow=0-4&mode=all&top=50``, default window).

The results go through the API's own path (``api.cache.load``: same keys, same compute, same bucket
objects), so the service then serves them from the bucket. Computes take the HEAVY guard (``MS_HEAVY``,
1 by default): ``--workers`` only overlaps bucket probes and the cheap requests, one analysis is in
memory at a time, as on the service (2 GiB). A result already in the bucket costs one read.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor

log = logging.getLogger("stibms.warm")


def _warm(name: str, make) -> str:
    """'computed' | 'bucket' | 'missing' (404: nothing to show) | 'failed'. ``make`` builds the result
    here, so a request the API would refuse (404, 422) skips this item only."""
    from fastapi import HTTPException
    from .api.cache import load
    try:
        _, _, src = load(make())
        return src
    except HTTPException as e:
        if e.status_code == 404:
            log.info("%s: %s", name, e.detail)
            return "missing"
        log.warning("%s: HTTP %s %s", name, e.status_code, e.detail)
    except Exception:  # noqa: BLE001 - one line must not stop the others
        log.exception("%s failed", name)
    return "failed"


def line_results(line: str, period: dict, analysis: bool = True) -> list[tuple[str, object]]:
    """(name, result builder) of the default page requests of one line (``period``: its ``available.default``)."""
    from .api import line as L
    from .api.params import call, period as period_dep, query
    p = lambda: call(period_dep, line=line, from_=period["from"], to=period["to"])  # noqa: E731
    out = [(f"coverage {line}", lambda: L.coverage_result(p())), (f"versions {line}", lambda: L.versions_result(p()))]
    if analysis:
        out.append((f"analysis {line}", lambda: L.analysis_result(call(query, line=line, from_=period["from"], to=period["to"]))))
    return out


def default_lines(data, only: set[str] | None = None) -> list[tuple[str, dict]]:
    """(line, default period) of the lines the page lists with data."""
    from .api.meta import _lines
    out = []
    for x in _lines(data, None)["lines"]:
        d = (x.get("available") or {}).get("default")
        if d and (only is None or x["line"] in only):
            out.append((x["line"], d))
    return out


def ranking_result():
    from .api.params import call
    from .api.rank import rank_query, rank_result
    return rank_result(*call(rank_query))


def run(lines: set[str] | None = None, workers: int = 2, analysis: bool = True) -> dict[str, int]:
    """Warm every default result; counts per outcome."""
    from .api.context import state
    jobs = [("ranking default", ranking_result)]
    jobs += [j for ln, d in default_lines(state.data, lines) for j in line_results(ln, d, analysis)]
    log.info("warming %d results with %d workers", len(jobs), workers)
    counts: dict[str, int] = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        for src in ex.map(lambda j: _warm(*j), jobs):
            counts[src] = counts.get(src, 0) + 1
    return counts


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m stibms.warm", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    where = ap.add_mutually_exclusive_group()
    where.add_argument("--bucket")
    where.add_argument("--local")
    ap.add_argument("--lines", help="only these lines, e.g. 55,71")
    ap.add_argument("--workers", type=int, default=2, help="requests in flight (computes still one at a time)")
    ap.add_argument("--no-analysis", action="store_true", help="coverage, versions and ranking only")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    root = args.local or args.bucket or os.environ.get("MS_BUCKET")
    if not root:
        ap.error("give --bucket, --local or set MS_BUCKET")
    os.environ["MS_BUCKET"] = root
    os.environ.setdefault("MS_BLOB_CACHE", "2")    # the results go to the bucket; keep little in memory
    t = time.time()
    only = {x.strip() for x in args.lines.split(",") if x.strip()} if args.lines else None
    counts = run(only, args.workers, not args.no_analysis)
    log.info("warm done in %.0f s: %s", time.time() - t, counts)
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())

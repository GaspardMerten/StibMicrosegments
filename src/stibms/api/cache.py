"""Result caching: data stamps, cache keys, Cache-Control, ETags, gzipped JSON in process and in the bucket."""
from __future__ import annotations

import contextlib
import datetime as dt
import functools
import gzip
import hashlib
import io
import json
import time
from dataclasses import dataclass
from typing import Callable

from fastapi import HTTPException, Request
from fastapi.responses import Response

from .context import HEAVY, log, state

LONG_CACHE = "public, max-age=3600"   # results of past periods; short enough for package upgrades
SHORT_CACHE = "public, max-age=300"

# Bump when the JSON a result holds changes without the data changing (page options such as
# show_hotspots, contract fields): older bucket results are then ignored instead of served.
# 8: line absence over ingested days only (a refactor changed it under 7); coverage and versions cached.
RESULT_REV = 8


@functools.cache
def rev() -> str:
    """Release token added to the pages' API calls (``rev=``, ignored by the API): a new release (package
    or RESULT_REV) changes every URL, so browsers do not reuse responses cached under an older one.
    Read from the package metadata: importing the package costs the cold home page seconds."""
    from importlib.metadata import PackageNotFoundError, version
    try:
        return f"{version('microsegments')}-{RESULT_REV}"
    except PackageNotFoundError:
        from .. import ms
        return f"{ms.version()}-{RESULT_REV}"


def stamp(data, ranges: list[tuple[dt.date, dt.date]]) -> str:
    """Changes whenever data of the ranges (one or two) changes: last ingested date + derive times."""
    from ..data import date_range
    (a, b), (a2, b2) = ranges[0], (ranges[1] if len(ranges) > 1 else (None, None))
    dates = [d for x, y in ranges for d in date_range(x, y)]
    return data._cached(f"stamp:{a}:{b}:{a2}:{b2}", lambda: data.stamp(dates), ttl=120)


def schedule_stamp(data, q) -> str:
    """The GTFS service categories move with new feeds (their reference spans the weeks around a
    date): a query that excludes them is keyed on the categorised days too."""
    from ..queries import uses_schedule
    if not uses_schedule(q):
        return ""
    from .. import schedule
    return ":" + schedule.stamp(data, *q.ranges)


def data_stamp(data, q) -> str:
    """The stamp a result of ``q`` (Query, CompareQuery, RankQuery) is keyed on."""
    return stamp(data, q.ranges) + schedule_stamp(data, q)


def cache_control(data, b: dt.date, q=None) -> str:
    """Long for a past, ingested period. Short too when ``q`` excludes GTFS service categories whose
    reference (``schedule.REF_DAYS`` after ``b``) reaches past the newest feed: later feeds may move them."""
    from ..queries import uses_schedule
    from ..servicedays import yesterday
    ing = data.ingested_dates()
    if q is not None and uses_schedule(q):
        from .. import schedule
        nf = schedule.newest_feed_date(data)
        if nf is None or b + dt.timedelta(days=schedule.REF_DAYS) > nf:
            return SHORT_CACHE
    return LONG_CACHE if ing and b < yesterday() and b <= ing[-1] else SHORT_CACHE


def gzip_json(obj) -> bytes:
    return gzip.compress(json.dumps(obj, separators=(",", ":"), default=str).encode(), 6)


# ---------------------------------------------------------------------------------- ETag
def etag(key: str, gz: bool) -> str:
    """Strong validator of the result ``key`` (it holds the parameters, RESULT_REV, the package and the
    data stamp) in one encoding: the gzipped and the plain bodies are different bytes."""
    h = hashlib.sha256(f"{key}:{rev()}".encode()).hexdigest()[:32]
    return f'"{h}-gz"' if gz else f'"{h}"'


def _accepts_gzip(request: Request) -> bool:
    return "gzip" in request.headers.get("accept-encoding", "")


def _matches(request: Request, tags: tuple[str, ...]) -> bool:
    """If-None-Match against ``tags`` (weak comparison, as RFC 9110 asks for this header)."""
    inm = request.headers.get("if-none-match", "")
    if inm.strip() == "*":
        return True
    return any(t.strip().removeprefix("W/") in tags for t in inm.split(","))


def not_modified(request: Request, key: str, cc: str) -> Response | None:
    """A 304 when the client holds result ``key`` already (either encoding), else None."""
    if not _matches(request, (etag(key, True), etag(key, False))):
        return None
    return Response(status_code=304, headers={"Cache-Control": cc, "Vary": "Accept-Encoding",
                                              "ETag": etag(key, _accepts_gzip(request))})


def gz_response(request: Request, gz: bytes, cc: str, media="application/json", extra: dict | None = None,
                key: str | None = None) -> Response:
    headers = {"Cache-Control": cc, "Vary": "Accept-Encoding", **(extra or {})}
    zipped = _accepts_gzip(request)
    if key is not None:
        headers["ETag"] = etag(key, zipped)
    if zipped:
        headers["Content-Encoding"] = "gzip"
        return Response(gz, media_type=media, headers=headers)
    return Response(gzip.decompress(gz), media_type=media, headers=headers)


def json_response(request: Request, obj, cc: str) -> Response:
    """Small JSON (lines, status) with a weak ETag of its content: GZipMiddleware may re-encode it."""
    body = json.dumps(obj, separators=(",", ":"), default=str).encode()
    tag = 'W/"' + hashlib.sha256(body).hexdigest()[:32] + '"'
    headers = {"Cache-Control": cc, "ETag": tag}
    if _matches(request, (tag.removeprefix("W/"),)):
        return Response(status_code=304, headers=headers)
    return Response(body, media_type="application/json", headers=headers)


# ---------------------------------------------------------------------------------- result cache
def _bucket_read(data, rel: str) -> bytes | None:
    try:
        return data.store.read_bytes(rel)      # one request: a missing object is the miss
    except FileNotFoundError:
        return None
    except Exception:  # noqa: BLE001 - a cache miss, not an error
        log.warning("result cache read failed for %s", rel, exc_info=True)
        return None


def _bucket_or_compute(data, key: str, compute, heavy: bool = True, keep=None) -> tuple[bytes, str, bool]:
    """(gzipped JSON, source, kept): the bucket's copy, else computed (under HEAVY unless ``heavy`` is
    false) and written there unless ``keep(result)`` is false."""
    from ..data import ALGO_VERSION
    rel = f"results/v{ALGO_VERSION}/{key}.json.gz"
    gz = _bucket_read(data, rel)
    if gz is not None:
        return gz, "bucket", True
    with HEAVY if heavy else contextlib.nullcontext():
        t = time.time()
        obj = compute()
        gz = gzip_json(obj)
    log.info("%s computed in %.2f s (%d kB gz)", key, time.time() - t, len(gz) // 1024)
    ok = keep is None or bool(keep(obj))
    if ok:
        try:
            data.store.write_bytes(rel, gz)
        except Exception:  # noqa: BLE001
            log.exception("result cache write failed")
    return gz, "computed", ok


@dataclass(frozen=True)
class Result:
    """A cached JSON result: its family, query (``key``, ``last``, ``ranges``), compute() -> JSON-able,
    more key parts; ``heavy``: computed under HEAVY; ``keep``: whether a result may be cached."""
    kind: str
    q: object
    compute: Callable
    extra: tuple = ()
    heavy: bool = True
    keep: Callable | None = None

    def key(self, data) -> str:
        return f"{self.kind}-r{RESULT_REV}-{self.q.key(data_stamp(data, self.q), *self.extra)}"


def load(res: Result) -> tuple[str, bytes, str]:
    """(key, gzipped JSON, source) of ``res``: in-process LRU, then bucket, then compute. Shared by the
    API and ``stibms.warm``."""
    data = state.data
    key, src, kept = res.key(data), "memory", True

    def get():
        nonlocal src, kept
        gz, src, kept = _bucket_or_compute(data, key, res.compute, res.heavy, res.keep)
        return gz
    gz = state.once(state.blobs, key, get, keep=lambda _: kept)
    return key, gz, src


def respond(request: Request, res: Result) -> Response:
    """``res`` as an HTTP response: 304 on a matching If-None-Match (nothing loaded), else the body."""
    data = state.data
    cc = cache_control(data, res.q.last, res.q)
    nm = not_modified(request, res.key(data), cc)
    if nm is not None:
        return nm
    key, gz, src = load(res)
    return gz_response(request, gz, cc, extra={"X-Cache": src}, key=key)


def cached_json(kind: str, q, compute, request: Request, *extra) -> Response:
    """kind: result family; compute() -> JSON-able; ``extra``: more key parts."""
    return respond(request, Result(kind, q, compute, extra))


def compute_run(q):
    """The analysis Run of ``q``, computed under HEAVY (NotFound: 404)."""
    from .. import analysis as A
    try:
        with HEAVY:
            return A.run(state.data, q)
    except A.NotFound as e:
        raise HTTPException(404, str(e))


def cached_run(q):
    """The analysis Run of ``q`` for the tables, hotspots and tuning (in-process LRU, MS_RUN_CACHE)."""
    key = q.key(data_stamp(state.data, q))
    return state.once(state.runs, key, lambda: compute_run(q), lock="run-" + key)


def table_response(df, fmt: str, name: str, cc: str) -> Response:
    buf = io.BytesIO()
    if fmt == "csv":
        df.select([c for c in df.columns if not str(df.schema[c]).startswith(("List", "Struct"))]).write_csv(buf)
        media = "text/csv; charset=utf-8"
    else:
        df.write_parquet(buf, compression="zstd")
        media = "application/vnd.apache.parquet"
    return Response(buf.getvalue(), media_type=media,
                    headers={"Cache-Control": cc, "Content-Disposition": f'attachment; filename="{name}.{fmt}"'})

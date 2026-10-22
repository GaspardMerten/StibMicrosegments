"""HTTP API and frontend of the STIB microsegments platform.

    uvicorn stibms.api:app --port 8080          # MS_BUCKET=gs://... or a local directory

Endpoints (dates YYYY-MM-DD, ``dow`` as ``0-4`` or ``0,2,4`` with 0 = Monday):

    GET /api/health
    GET /api/lines?date=                      lines of the GTFS in force on date (default: latest)
    GET /api/coverage?line&from&to            per day feed coverage, line observations, holiday, status
    GET /api/versions?line&from&to            routes run as main pattern per direction (one per stop
                                              sequence), with their stops, ``periods`` (runs of days,
                                              n_days) and the stops removed / added against the reference
                                              (most stops, latest on a tie: ``ref``); with the
                                              reference's stops, ``detours`` (detours, moved stops);
                                              on a reference running from the start, ``lasting``: the
                                              change made before the period that made it the route
    GET /api/analysis[.csv|.parquet]?line&from&to&dow&holidays=exclude&seg=30&phase=0&grid=stops
                                       &stopzone=36,38&ref=20-24&school=include&reduced=exclude
                                       &exclude=D1..D2,D3
                                              JSON contract (microsegments.contract) / result table
    GET /api/hotspots[.csv|.parquet]?...      hotspot stretches (same parameters)
    GET /api/tune?...&lengths=15,20,30,40,50  segment-length criteria (slow, cached)
    GET /api/compare?line&a=D1..D2&b=D3..D4&dow&holidays&seg&...
                                              period B's contract + the ``compare`` section
    GET /api/ranking?from&to&dow&holidays&school&reduced&exclude&mode=all|tram|bus|metro&top=50&stops=false&terminus=false
                                              costliest stretches of the network (vehicle-hours lost
                                              per day, between stops unless stops=true; termini and
                                              timing stops (vehicles waiting for the schedule) left out unless terminus=true);
                                              default window when from/to are omitted. Sums the
                                              monthly link cubes: < 1 s for any window.
    Days left out on purpose, on every endpoint taking a period (analysis, hotspots, tune, compare,
    ranking): holidays (public holidays, default exclude), school (STIB school-holiday timetable,
    default include), reduced (reduced service, strikes, default exclude), exclude (user periods,
    applied to both periods of a comparison). See stibms.schedule; period.excluded gives their
    reasons holiday, school_holiday, reduced_service, user_period.
    GET /api/status                           ingested range, derived months, months pending
    GET /, /comparer, /classement             the page (one line / two periods / network ranking); /methode -> /
    GET /view?<analysis or compare params>    the package page fed by /api/analysis or /api/compare

Results are cached in process (LRU) and in the bucket (``results/v{ALGO}/<hash>.json.gz``), keyed
by the parameters, ALGO_VERSION, the package version and the data stamp (last ingested date and
derive time of the months involved), so a nightly ingest invalidates only what it touches.
Periods that are fully past and ingested are sent with ``Cache-Control: max-age=86400``.
Heavy imports (polars, microsegments) happen on the first data request, not at start-up.
CPU-heavy work (an analysis or comparison not in cache) runs one at a time per instance (``HEAVY``),
so a burst of new requests cannot starve the cheap ones (health, lines, cached results, ranking);
cache hits (memory or bucket) never take that guard. A start-up thread warms imports and listings.
The line page hides the package's hotspots (``show_hotspots: false``), so they are not computed.

Modules: ``context`` (state, HEAVY, warm-up), ``params`` (query strings), ``cache`` (stamps, keys,
Cache-Control, result cache), routes in ``meta`` (health, lines, status), ``line`` (one line, two
periods), ``rank`` (ranking) and ``pages`` (the pages and static files).
"""
from __future__ import annotations

import contextlib

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import line, meta, pages, rank
from .cache import RESULT_REV, rev  # noqa: F401 - part of the API module's surface
from .context import log, start_warm, state  # noqa: F401


@contextlib.asynccontextmanager
async def _lifespan(app):
    start_warm()
    yield


app = FastAPI(title="STIB microsegments", docs_url="/api/docs", openapi_url="/api/openapi.json", lifespan=_lifespan)
for _m in (meta, line, rank, pages):
    app.include_router(_m.router)


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception):
    log.exception("unhandled error on %s", request.url.path)
    return JSONResponse({"detail": f"{type(exc).__name__}: {exc}"[:500]}, status_code=500)

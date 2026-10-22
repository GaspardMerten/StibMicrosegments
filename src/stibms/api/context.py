"""Per-process state of the API: the bucket, the in-process caches, the HEAVY guard, the warm-up."""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict

from fastapi import HTTPException

log = logging.getLogger("stibms.api")

# One cache-missing analysis / comparison at a time per instance (2 vCPU): the others wait their turn
# instead of all slowing down together, and cheap endpoints keep a free core.
_heavy_sem = threading.BoundedSemaphore(int(os.environ.get("MS_HEAVY", "1")))
_heavy_tls = threading.local()


class _Heavy:
    """Re-entrant per thread (a cached analysis computes its run inside the same guard)."""
    def __enter__(self):
        d = getattr(_heavy_tls, "depth", 0)
        if d == 0:
            _heavy_sem.acquire()
        _heavy_tls.depth = d + 1

    def __exit__(self, *exc):
        _heavy_tls.depth -= 1
        if _heavy_tls.depth == 0:
            _heavy_sem.release()


HEAVY = _Heavy()


class _LRU:
    def __init__(self, size: int):
        self.size, self.d, self.lock = size, OrderedDict(), threading.Lock()

    def get(self, k):
        with self.lock:
            if k in self.d:
                self.d.move_to_end(k)
                return self.d[k]
        return None

    def put(self, k, v):
        with self.lock:
            self.d[k] = v
            self.d.move_to_end(k)
            while len(self.d) > self.size:
                self.d.popitem(last=False)

    def clear(self):
        with self.lock:
            self.d.clear()


class State:
    def __init__(self):
        self._data = None
        self.warming: threading.Thread | None = None
        self.runs = _LRU(int(os.environ.get("MS_RUN_CACHE", "6")))      # Run objects (arrays, heavy)
        self.blobs = _LRU(int(os.environ.get("MS_BLOB_CACHE", "128")))  # gzipped JSON
        self._locks: dict[str, threading.Lock] = {}
        self._lk = threading.Lock()

    @property
    def data(self):
        w = self.warming
        if self._data is None and w is not None and w is not threading.current_thread():
            w.join(30)   # the warm-up is doing exactly what this request needs: share it
        if self._data is None:
            from ..data import Data
            root = os.environ.get("MS_BUCKET")
            if not root:
                raise HTTPException(503, "MS_BUCKET is not set")
            self._data = Data(root)
        return self._data

    def lock(self, key: str) -> threading.Lock:
        with self._lk:
            return self._locks.setdefault(key, threading.Lock())

    def reset(self):
        self._data = None
        self.runs.clear()
        self.blobs.clear()

    def once(self, cache: _LRU, key: str, compute, keep=None, lock: str | None = None):
        """``cache[key]``, computed by one request at a time per key (the others wait and take its
        value); not remembered when ``keep(value)`` is false."""
        v = cache.get(key)
        if v is None:
            with self.lock(lock or key):
                v = cache.get(key)
                if v is None:
                    v = compute()
                    if keep is None or keep(v):
                        cache.put(key, v)
        return v


state = State()


def _warm() -> None:
    """Per-process set-up started with the process (in a thread: health answers at once) instead of
    on the first data request: heavy imports, the bucket client and its token, the listings the
    cache keys depend on. Cached results never take the HEAVY guard; a cached analysis measured at
    ~8 s under load was this set-up on an instance Cloud Run had just added (CPU-driven scale-out)."""
    from .cache import stamp
    t = time.time()
    try:
        from .. import analysis, ranking  # noqa: F401 - imports polars and the package modules
        data = state.data
        ing = data.ingested_dates()
        data.derived_months()
        if ing:
            stamp(data, [(ing[-1].replace(day=1), ing[-1])])
        log.info("warm in %.2f s", time.time() - t)
    except Exception:  # noqa: BLE001 - never block start-up; the first request retries lazily
        log.exception("warm-up failed")


def start_warm() -> None:
    if os.environ.get("MS_BUCKET") and os.environ.get("MS_WARM", "1") != "0":
        state.warming = threading.Thread(target=_warm, name="ms-warm", daemon=True)
        state.warming.start()

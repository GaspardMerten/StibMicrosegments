"""Read side of the bucket layout, shared by the derive job and the API.

    raw/vd/date=D/{vd,snaps}.parquet, raw/punctuality/date=D/p.parquet, raw/_SUCCESS/date=D.json
    gtfs/feeds/<sha>/<table>.parquet, gtfs/index.parquet (service_date -> sha)
    derived/v{ALGO}/linkkeys.parquet                        link_key registry (network.keys)
    derived/v{ALGO}/patterns/sha=<sha>/{patterns,links,stops,pattern_days}.parquet  all lines of a feed
    derived/v{ALGO}/coverage/month=YYYY-MM.parquet          feed coverage (all lines) per date x hour
    derived/v{ALGO}/placed/line=X/month=YYYY-MM.parquet     located observations of a line
    derived/v{ALGO}/passages/line=X/month=YYYY-MM.parquet   passages per date x hour x link
    derived/v{ALGO}/_SUCCESS/month=YYYY-MM.json             derive report
    results/v{ALGO}/<hash>.json.gz                          API result cache

``ALGO_VERSION`` is bumped whenever derived files change meaning; a new version is a new prefix,
so old results stay readable until the new one is fully derived.
"""
from __future__ import annotations

import datetime as dt
import io
import os
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from .servicedays import date_range  # noqa: F401 - re-exported, the one date range of the package
from .storage import Store

ALGO_VERSION = 1
FALLBACK_DAYS = 7     # a feed's network also covers this many days after its last own date
FALLBACK_FEEDS = 2    # earlier feeds tried when a date's own feed has no trip of the line
GTFS_TABLES = ("routes", "trips", "stop_times", "stops", "shapes", "calendar", "calendar_dates")
PLACED_COLS = ("ts", "service_date", "hour", "dow", "pattern_uid", "direction_id", "link_idx", "link_key",
               "pos_m", "s_m", "raw_dist_m", "track_id", "count", "flags")


GEOMETRY = pl.List(pl.List(pl.Float64))


class SizedLRU:
    """LRU bounded by the total size of its values (bytes, given on put)."""

    def __init__(self, max_bytes: int):
        self.max_bytes, self.size, self.d, self.lock = max_bytes, 0, OrderedDict(), threading.Lock()

    def get(self, k):
        with self.lock:
            hit = self.d.get(k)
            if hit is not None:
                self.d.move_to_end(k)
                return hit[0]
        return None

    def put(self, k, v, nbytes: int) -> None:
        with self.lock:
            old = self.d.pop(k, None)
            self.size -= old[1] if old else 0
            self.d[k] = (v, nbytes)
            self.size += nbytes
            while self.size > self.max_bytes and len(self.d) > 1:
                self.size -= self.d.popitem(last=False)[1][1]


def _compact(df: pl.DataFrame) -> pl.DataFrame:
    """A copy of ``df`` in buffers of its own: a filtered frame's strings still point into the buffers
    of the whole file (all lines of a feed), which a cached slice would otherwise keep alive."""
    buf = io.BytesIO()
    df.write_ipc(buf)
    buf.seek(0)
    return pl.read_ipc(buf)


def derived(rel: str = "") -> str:
    return f"derived/v{ALGO_VERSION}/{rel}".rstrip("/")


def month_key(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def month_dates(month: str) -> list[dt.date]:
    y, m = map(int, month.split("-"))
    first = dt.date(y, m, 1)
    nxt = dt.date(y + (m == 12), m % 12 + 1, 1)
    return [first + dt.timedelta(days=i) for i in range((nxt - first).days)]


def months_between(a: dt.date, b: dt.date) -> list[str]:
    out, d = [], dt.date(a.year, a.month, 1)
    while d <= b:
        out.append(month_key(d))
        d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return out


def by_line(line: str) -> tuple[int, str]:
    """Sort key of line numbers: 1, 2, ..., 10, ..., N04."""
    return len(line), line


def months_of(dates) -> list[str]:
    """Distinct months of ``dates`` (two periods far apart read only their own months)."""
    return sorted({month_key(d) for d in dates})


class Data:
    """Bucket access with small in-process caches (index, ingested dates, per-(feed, line) networks)."""

    def __init__(self, root: str | Store, cache_dir: str | None = None, ttl_s: float = 600.0):
        self.store = root if isinstance(root, Store) else Store(root)
        self.cache_dir = Path(cache_dir or os.environ.get("MS_CACHE", "/tmp/stibms-cache"))
        self.ttl_s = ttl_s
        self._lock = threading.Lock()
        self._memo: dict[str, tuple[float, object]] = {}
        self._slices = SizedLRU(int(os.environ.get("MS_NET_CACHE_MB", "128")) << 20)   # (sha, line, geometry)
        self._sha_locks: dict[str, threading.Lock] = {}

    # ------------------------------------------------------------------ memo
    def _cached(self, key: str, fn, ttl: float | None = None):
        ttl = self.ttl_s if ttl is None else ttl
        now = time.time()
        with self._lock:
            hit = self._memo.get(key)
            if hit and now - hit[0] < ttl:
                return hit[1]
        val = fn()
        with self._lock:
            self._memo[key] = (now, val)
        return val

    def invalidate(self) -> None:
        with self._lock:
            self._memo.clear()

    # ------------------------------------------------------------------ raw
    def ingested_dates(self) -> list[dt.date]:
        def f():
            out = []
            for p in self.store.glob("raw/_SUCCESS/date=*.json"):
                out.append(dt.date.fromisoformat(p.rsplit("date=", 1)[1][:10]))
            return sorted(out)
        return self._cached("ingested", f)

    def month_ingested(self, month: str) -> list[dt.date]:
        """The ingested dates of ``month``."""
        ing = set(self.ingested_dates())
        return [d for d in month_dates(month) if d in ing]

    def gtfs_index(self) -> pl.DataFrame:
        def f():
            if not self.store.exists("gtfs/index.parquet"):
                return pl.DataFrame(schema={"service_date": pl.Date, "sha": pl.Utf8})
            return self.store.read_parquet("gtfs/index.parquet").select("service_date", "sha").sort("service_date")
        return self._cached("index", f)

    def shas_for(self, dates) -> dict[dt.date, str]:
        """service_date -> sha for the given dates (dates without a GTFS entry are left out)."""
        ds = set(dates)
        idx = self.gtfs_index().filter(pl.col("service_date").is_in(list(ds)))
        return dict(zip(idx["service_date"].to_list(), idx["sha"].to_list()))

    def sha_dates(self, sha: str) -> list[dt.date]:
        """Dates a feed's network is built for: its own index dates plus the dates of the following
        feeds within FALLBACK_DAYS after its last own date. STIB sometimes publishes, on the morning
        of D, a feed whose calendar starts after D (2025-05-16, 2025-05-20 on line 55): the previous
        feed still describes D, and ``network`` falls back on it."""
        idx = self.gtfs_index()
        own = idx.filter(pl.col("sha") == sha)["service_date"].to_list()
        if not own:
            return []
        last = max(own)
        later = idx.filter((pl.col("service_date") > last)
                           & (pl.col("service_date") <= last + dt.timedelta(days=FALLBACK_DAYS)))["service_date"]
        return sorted(set(own) | set(later.to_list()))

    def candidate_shas(self, dates) -> dict[dt.date, list[str]]:
        """service_date -> [its own sha, then up to FALLBACK_FEEDS earlier distinct shas]."""
        idx = self.gtfs_index()
        shas = idx["sha"].to_list()
        days = idx["service_date"].to_list()
        # distinct feeds in date order, with the position of each date's feed
        order: list[str] = []
        pos: dict[dt.date, int] = {}
        for d, sh in zip(days, shas):
            if not order or order[-1] != sh:
                order.append(sh)
            pos[d] = len(order) - 1
        out = {}
        for d in dates:
            if d in pos:
                i = pos[d]
                cands = []
                for j in range(i, -1, -1):
                    if order[j] not in cands:
                        cands.append(order[j])
                    if len(cands) > FALLBACK_FEEDS:
                        break
                out[d] = cands
        return out

    def feed_dir(self, sha: str) -> str:
        """Local directory of a GTFS feed (downloaded once to the cache dir when on GCS)."""
        rel = f"gtfs/feeds/{sha}"
        if self.store.local:
            return self.store.path(rel)
        out = self.cache_dir / "gtfs" / sha
        done = out / "_done"
        if not done.exists():
            out.mkdir(parents=True, exist_ok=True)
            for t in GTFS_TABLES:
                if self.store.exists(f"{rel}/{t}.parquet"):
                    tmp = out / f"{t}.parquet.part"
                    tmp.write_bytes(self.store.read_bytes(f"{rel}/{t}.parquet"))
                    tmp.replace(out / f"{t}.parquet")
            done.write_text("ok")
        return str(out)

    def routes(self, sha: str) -> pl.DataFrame:
        return self._cached(f"routes:{sha}", lambda: self.store.read_parquet(f"gtfs/feeds/{sha}/routes.parquet"),
                            ttl=1e9)

    def read_optional(self, rel: str, columns: list[str] | None = None) -> pl.DataFrame | None:
        """One request instead of exists + read (matters on GCS)."""
        try:
            return self.store.read_parquet(rel, columns=columns)
        except FileNotFoundError:
            return None

    def _read_bytes_optional(self, rel: str) -> bytes | None:
        try:
            return self.store.read_bytes(rel)
        except FileNotFoundError:
            return None

    @staticmethod
    def _map(fn, items: list) -> list:
        """fn over items, in parallel threads (GCS latency)."""
        if len(items) <= 1:
            return [fn(x) for x in items]
        with ThreadPoolExecutor(max_workers=min(16, len(items))) as ex:
            return list(ex.map(fn, items))

    def read_many(self, rels: list[str], columns: list[str] | None = None) -> list[pl.DataFrame | None]:
        """read_optional over several files, in parallel threads."""
        return self._map(lambda r: self.read_optional(r, columns), rels)

    def snapshots(self, dates) -> pl.DataFrame:
        """SNAPSHOT frame (ts UTC, n_rows, frozen) of the given ingested dates."""
        frames = []
        for d in dates:
            rel = f"raw/vd/date={d}/snaps.parquet"
            if self.store.exists(rel):
                frames.append(self.store.read_parquet(rel))
        if not frames:
            return pl.DataFrame(schema={"ts": pl.Datetime("us", "UTC"), "n_rows": pl.UInt32, "frozen": pl.Boolean})
        df = pl.concat(frames, how="diagonal_relaxed")
        return df.select(
            (pl.col("ts").cast(pl.Int64) * 1_000_000).cast(pl.Datetime("us")).dt.replace_time_zone("UTC").alias("ts"),
            pl.col("n_rows").cast(pl.UInt32),
            pl.col("frozen").cast(pl.Boolean).fill_null(False),
        ).unique("ts", keep="first").sort("ts")

    # ------------------------------------------------------------------ derived
    def link_registry(self):
        from microsegments.network import KeyRegistry
        t = self.read_optional(derived("linkkeys.parquet"))
        return KeyRegistry(t)

    def _sha_stamp(self, sha: str) -> str | None:
        def read_stamp():
            try:
                return self.store.read_json(derived(f"patterns/sha={sha}/_SUCCESS")).get("written_at")
            except FileNotFoundError:
                return None
        return self._cached(f"shastamp:{sha}", read_stamp)

    def line_tables(self, sha: str, line: str, geometry: bool = True) -> dict[str, pl.DataFrame] | None:
        """patterns / links / stops / pattern_days of one line in one feed (cached per (sha, line); a
        feed's tables only change when its date set grows, see derive.ensure_patterns). Without
        ``geometry`` the shapes are not read (null columns)."""
        stamp = self._sha_stamp(sha)
        if stamp is None:
            return None
        key = (sha, line, geometry)
        hit = self._slices.get(key)
        if hit is not None and hit[0] == stamp:
            return hit[1]
        with self._sha_lock(sha):     # concurrent requests read a feed once
            hit = self._slices.get(key)
            if hit is not None and hit[0] == stamp:
                return hit[1]
            out = self._read_line_tables(sha, line, geometry)
            self._slices.put(key, (stamp, out), sum(f.estimated_size() for f in out.values()))
        return out

    def _sha_lock(self, sha: str) -> threading.Lock:
        with self._lock:
            return self._sha_locks.setdefault(sha, threading.Lock())

    def _read_line_tables(self, sha: str, line: str, geometry: bool) -> dict[str, pl.DataFrame]:
        base = derived(f"patterns/sha={sha}")

        def read(name: str, schema: dict, keep) -> pl.DataFrame:
            cols = None if geometry else [c for c in schema if c != "geometry"]
            f = self.store.read_parquet(f"{base}/{name}.parquet", columns=cols).filter(keep)
            return f if geometry else f.with_columns(pl.lit(None, GEOMETRY).alias("geometry"))
        from microsegments import schema
        mine = pl.col("line") == line
        pats = read("patterns", {**schema.PATTERN, "line": pl.Utf8}, mine)
        out = {"patterns": pats,
               "links": read("links", schema.LINK, pl.col("pattern_uid").is_in(pats["pattern_uid"].implode())),
               "stops": self.store.read_parquet(f"{base}/stops.parquet").filter(mine),
               "pattern_days": self.store.read_parquet(f"{base}/pattern_days.parquet").filter(mine)}
        return {k: _compact(f) for k, f in out.items()}

    def _feed_tables(self, shas: list[str], line: str, geometry: bool) -> dict[str, dict | None]:
        def f(sha):
            return self.line_tables(sha, line, geometry)
        if len(shas) <= 1:
            return {s: f(s) for s in shas}
        with ThreadPoolExecutor(max_workers=min(8, len(shas))) as ex:
            return dict(zip(shas, ex.map(f, shas)))

    @staticmethod
    def _pick_feeds(cands: dict, line_days: dict[str, pl.DataFrame]) -> dict[str, list[dt.date]]:
        """sha -> its dates: for each date the first candidate feed that runs the line that day."""
        pick: dict[str, list[dt.date]] = {}
        for d, cs in cands.items():
            for sha in cs:
                ld = line_days.get(sha)
                if ld is not None and ld.height and (ld["service_date"] == d).any():
                    pick.setdefault(sha, []).append(d)
                    break
        return pick

    def network(self, line: str, dates, registry=None, geometry: bool = True):
        """microsegments Network of ``line`` on ``dates`` assembled from the per-sha tables (without
        ``geometry``: null shapes, for what only needs stops and lengths)."""
        from microsegments.network import KeyRegistry, Network
        from microsegments.network.patterns import PATTERN_DAY_EXTRA
        from microsegments import schema

        line = str(line)
        cands = self.candidate_shas(dates)
        tables = self._feed_tables(sorted({x for c in cands.values() for x in c}), line, geometry)
        # this line's pattern days per feed, then for each date the first feed that runs the line
        line_days = {sha: t["pattern_days"] for sha, t in tables.items() if t is not None}
        pats, links, stops, days = [], [], [], []
        for sha, mine in self._pick_feeds(cands, line_days).items():
            t = tables[sha]
            pats.append(t["patterns"])
            links.append(t["links"])
            stops.append(t["stops"])
            days.append(line_days[sha].filter(pl.col("service_date").is_in(mine)))
        pd_schema = {**schema.PATTERN_DAY, **PATTERN_DAY_EXTRA}
        if not pats:
            patterns = pl.DataFrame(schema=schema.PATTERN)
            lk = pl.DataFrame(schema=schema.LINK)
            st = pl.DataFrame(schema={"stop_id": pl.Utf8, "stop_name": pl.Utf8, "stop_lon": pl.Float64, "stop_lat": pl.Float64})
            pdays = pl.DataFrame(schema=pd_schema)
        else:
            patterns = pl.concat(pats).unique("pattern_uid", keep="first", maintain_order=True).select(list(schema.PATTERN))
            lk = pl.concat(links).unique(["pattern_uid", "link_idx"], keep="first", maintain_order=True).select(list(schema.LINK))
            st = pl.concat(stops).unique("stop_id", keep="first", maintain_order=True).drop("line")
            pdays = pl.concat(days).select(list(pd_schema))
        found = set(pdays["service_date"].to_list())
        return Network(route=line, patterns=patterns.sort("direction_id", "pattern_uid"),
                       pattern_days=pdays.sort("service_date", "direction_id", "n_trips", descending=[False, False, True]),
                       links=lk.sort("pattern_uid", "link_idx"), stops=st,
                       registry=registry if registry is not None else KeyRegistry(),
                       missing_dates=sorted(d for d in dates if d not in found))

    def coverage(self, dates) -> pl.DataFrame:
        ds = sorted(set(dates))
        if not ds:
            return pl.DataFrame()
        frames = [f for f in self.read_many([derived(f"coverage/month={m}.parquet") for m in months_of(ds)])
                  if f is not None]
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames).filter(pl.col("service_date").is_in(ds))

    def _line_months(self, kind: str, line: str, dates, columns: list[str] | None = None) -> pl.DataFrame | None:
        ds = sorted(set(dates))
        if not ds:
            return None
        rels = [derived(f"{kind}/line={line}/month={m}.parquet") for m in months_of(ds)]
        frames = [f.filter(pl.col("service_date").is_in(ds)) for f in self.read_many(rels, columns) if f is not None]
        return pl.concat(frames, how="diagonal_relaxed") if frames else None

    def placed(self, line: str, dates, columns: list[str] | None = None) -> pl.DataFrame | None:
        return self._line_months("placed", line, dates, columns)

    def placed_blobs(self, line: str, dates) -> dict[str, bytes]:
        """month -> the line's placed file of that month, as Parquet bytes (several times smaller than
        the frame: an analysis decodes one month at a time)."""
        months = months_of(dates)
        got = self._map(self._read_bytes_optional, [derived(f"placed/line={line}/month={m}.parquet") for m in months])
        return {m: b for m, b in zip(months, got) if b is not None}

    def passages(self, line: str, dates) -> pl.DataFrame | None:
        return self._line_months("passages", line, dates)

    def derived_months(self) -> dict[str, str]:
        """month -> derive time (``written_at`` is read lazily by ``month_stamp``) for every month with
        a derive report."""
        def f():
            out = {}
            for p in self.store.glob(derived("_SUCCESS/month=*.json")):
                out[p.rsplit("month=", 1)[1][:7]] = p
            return dict(sorted(out.items()))
        return self._cached("derived_months", f, ttl=120)

    def month_stamp(self, month: str) -> str:
        """Derive time of a month ('' when not derived) and of its passages / link cube rewrites if any,
        cached briefly."""
        def f():
            rel = derived(f"_SUCCESS/month={month}.json")
            try:
                rep = self.store.read_json(rel)
                return "".join(rep.get(k, "") for k in ("written_at", "passages_rewritten_at", "linkcube_rewritten_at"))
            except FileNotFoundError:
                return ""
        return self._cached(f"mstamp:{month}", f, ttl=120)

    def stamp(self, dates) -> str:
        """Changes whenever the data of ``dates`` changes: last ingested date among them + derive
        times of their months (result cache keys)."""
        ds = sorted(set(dates))
        if not ds:
            return "-"
        ing = [d for d in self.ingested_dates() if ds[0] <= d <= ds[-1]]
        return "|".join([ing[-1].isoformat() if ing else "-"] + self._map(self.month_stamp, months_of(ds)))

    def line_months(self) -> dict[str, list[str]]:
        """line -> months with a placed file."""
        def f():
            out: dict[str, list[str]] = {}
            for p in self.store.glob(derived("placed/line=*/month=*.parquet")):
                parts = p.split("/")
                line = next(x for x in parts if x.startswith("line="))[5:]
                month = next(x for x in parts if x.startswith("month="))[6:13]
                out.setdefault(line, []).append(month)
            return {k: sorted(v) for k, v in out.items()}
        return self._cached("line_months", f)

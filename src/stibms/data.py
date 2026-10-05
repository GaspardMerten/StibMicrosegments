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
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from .storage import Store

ALGO_VERSION = 1
GTFS_TABLES = ("routes", "trips", "stop_times", "stops", "shapes", "calendar", "calendar_dates")
PLACED_COLS = ("ts", "service_date", "hour", "dow", "pattern_uid", "direction_id", "link_idx", "link_key",
               "pos_m", "s_m", "raw_dist_m", "track_id", "count", "flags")


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


def date_range(a: dt.date, b: dt.date) -> list[dt.date]:
    return [a + dt.timedelta(days=i) for i in range((b - a).days + 1)]


class Data:
    """Bucket access with small in-process caches (index, ingested dates, per-sha networks)."""

    def __init__(self, root: str | Store, cache_dir: str | None = None, ttl_s: float = 600.0):
        self.store = root if isinstance(root, Store) else Store(root)
        self.cache_dir = Path(cache_dir or os.environ.get("MS_CACHE", "/tmp/stibms-cache"))
        self.ttl_s = ttl_s
        self._lock = threading.Lock()
        self._memo: dict[str, tuple[float, object]] = {}
        self._sha_tables: dict[str, dict[str, pl.DataFrame]] = {}

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

    def read_optional(self, rel: str) -> pl.DataFrame | None:
        """One request instead of exists + read (matters on GCS)."""
        try:
            return self.store.read_parquet(rel)
        except FileNotFoundError:
            return None

    def read_many(self, rels: list[str]) -> list[pl.DataFrame | None]:
        """read_optional over several files, in parallel threads (GCS latency)."""
        if len(rels) <= 1:
            return [self.read_optional(r) for r in rels]
        with ThreadPoolExecutor(max_workers=min(16, len(rels))) as ex:
            return list(ex.map(self.read_optional, rels))

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

    def sha_tables(self, sha: str) -> dict[str, pl.DataFrame] | None:
        """patterns / links / stops / pattern_days of one feed, all lines (cached; a feed's tables
        only change when its date set grows, see derive.ensure_patterns)."""
        base = derived(f"patterns/sha={sha}")

        def read_stamp():
            try:
                return self.store.read_json(f"{base}/_SUCCESS").get("written_at")
            except FileNotFoundError:
                return None
        stamp = self._cached(f"shastamp:{sha}", read_stamp)
        hit = self._sha_tables.get(sha)
        if hit is not None and hit.get("_stamp") == stamp:
            return hit
        if stamp is None:
            return None
        names = ("patterns", "links", "stops", "pattern_days")
        out = dict(zip(names, self.read_many([f"{base}/{n}.parquet" for n in names])))
        out["_stamp"] = stamp  # type: ignore[assignment]
        with self._lock:
            self._sha_tables[sha] = out
        return out

    def network(self, line: str, dates, registry=None):
        """microsegments Network of ``line`` on ``dates`` assembled from the per-sha tables."""
        from microsegments.network import KeyRegistry, Network
        from microsegments.network.patterns import PATTERN_DAY_EXTRA
        from microsegments import schema

        line = str(line)
        by_date = self.shas_for(dates)
        pats, links, stops, days = [], [], [], []
        order = sorted(set(by_date.values()), key=lambda s: min(d for d, x in by_date.items() if x == s))
        if len(order) > 1:
            with ThreadPoolExecutor(max_workers=min(8, len(order))) as ex:
                tables = dict(zip(order, ex.map(self.sha_tables, order)))
        else:
            tables = {s: self.sha_tables(s) for s in order}
        for sha in order:
            t = tables[sha]
            if t is None:
                continue
            mine = [d for d, x in by_date.items() if x == sha]
            p = t["patterns"].filter(pl.col("line") == line)
            if not p.height:
                continue
            uids = p["pattern_uid"].implode()
            pats.append(p)
            links.append(t["links"].filter(pl.col("pattern_uid").is_in(uids)))
            stops.append(t["stops"].filter(pl.col("line") == line))
            days.append(t["pattern_days"].filter((pl.col("line") == line) & pl.col("service_date").is_in(mine)))
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
        missing = sorted(d for d in dates if d not in found)
        return Network(route=line, patterns=patterns.sort("direction_id", "pattern_uid"),
                       pattern_days=pdays.sort("service_date", "direction_id", "n_trips", descending=[False, False, True]),
                       links=lk.sort("pattern_uid", "link_idx"), stops=st,
                       registry=registry if registry is not None else KeyRegistry(), missing_dates=missing)

    def coverage(self, dates) -> pl.DataFrame:
        ds = sorted(set(dates))
        if not ds:
            return pl.DataFrame()
        frames = [f for f in self.read_many([derived(f"coverage/month={m}.parquet") for m in months_between(ds[0], ds[-1])])
                  if f is not None]
        if not frames:
            return pl.DataFrame()
        return pl.concat(frames).filter(pl.col("service_date").is_in(ds))

    def _line_months(self, kind: str, line: str, dates) -> pl.DataFrame | None:
        ds = sorted(set(dates))
        if not ds:
            return None
        rels = [derived(f"{kind}/line={line}/month={m}.parquet") for m in months_between(ds[0], ds[-1])]
        frames = [f.filter(pl.col("service_date").is_in(ds)) for f in self.read_many(rels) if f is not None]
        return pl.concat(frames, how="diagonal_relaxed") if frames else None

    def placed(self, line: str, dates) -> pl.DataFrame | None:
        return self._line_months("placed", line, dates)

    def passages(self, line: str, dates) -> pl.DataFrame | None:
        return self._line_months("passages", line, dates)

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

"""Per-line copies of the per-feed network tables, without shapes, for the route changes endpoint.

``derived/v{ALGO}/linenets/``:

- ``line=X/{patterns,links,pattern_days}.parquet``: the rows of line X in every built feed's
  ``patterns/sha=<sha>/`` tables (``sha`` column added, ``geometry`` dropped);
- ``_SUCCESS.json``: ``shas`` {sha: patterns written_at} and ``lines`` {sha: [lines]} of the feeds
  copied, written after the line files.

``stibms.versions`` needs the network of a line over a long period (13 months with the lookback):
read from the per-feed files, that is 4 reads of all-lines files per feed (~12 s cold on GCS). Here
it is 3 small reads, and :func:`network` runs ``Data.network`` itself on these rows, so the result is
the same. A feed is dropped from the manifest before its tables are rebuilt (:func:`forget`) and put
back by :func:`update`: a missing feed means "read the per-feed files" (``network`` returns None).

    python -m stibms.derive --linenets-only        # (re)build from all built feeds
"""
from __future__ import annotations

import logging
import time

import polars as pl

from .data import GEOMETRY, Data, by_line, derived

log = logging.getLogger("stibms.linenets")
TABLES = ("patterns", "links", "pattern_days")
MANIFEST = derived("linenets/_SUCCESS.json")


def rel(line: str, name: str) -> str:
    return derived(f"linenets/line={line}/{name}.parquet")


# ---------------------------------------------------------------------------------- write
def _read_manifest(data: Data) -> dict | None:
    try:
        return data.store.read_json(MANIFEST)
    except FileNotFoundError:
        return None


def _write_manifest(data: Data, man: dict) -> None:
    man["written_at"] = time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime())
    data.store.write_json(MANIFEST, man)


def forget(data: Data, sha: str) -> None:
    """Drop ``sha`` from the manifest (its per-feed tables are about to be rewritten)."""
    man = _read_manifest(data)
    if man and sha in man.get("shas", {}):
        man["shas"].pop(sha)
        _write_manifest(data, man)


def _shapeless(cols: dict) -> list[str]:
    return [c for c in cols if c != "geometry"]


def _sha_lines(data: Data, sha: str) -> dict[str, dict[str, pl.DataFrame]]:
    """line -> its rows of the feed's tables (the slices ``Data._read_line_tables`` takes)."""
    from microsegments import schema
    base = derived(f"patterns/sha={sha}")
    pats = data.store.read_parquet(f"{base}/patterns.parquet", columns=[*_shapeless(schema.PATTERN), "line"])
    links = data.store.read_parquet(f"{base}/links.parquet", columns=_shapeless(schema.LINK))
    days = data.store.read_parquet(f"{base}/pattern_days.parquet")
    lit = pl.lit(sha, pl.Utf8).alias("sha")
    out = {}
    for (line,), p in pats.group_by("line", maintain_order=True):
        if line is None:
            continue
        out[str(line)] = {
            "patterns": p.with_columns(lit),
            "links": links.filter(pl.col("pattern_uid").is_in(p["pattern_uid"].implode())).with_columns(lit),
            "pattern_days": days.filter(pl.col("line") == line).with_columns(lit)}
    return out


def _write_line(data: Data, line: str, new: list[dict], drop: set[str], fresh: bool) -> None:
    """Rewrite line X's files: its rows of feeds not in ``drop`` kept (unless ``fresh``), ``new`` added."""
    for n in TABLES:
        acc = [t[n] for t in new]
        old = None if fresh else data.read_optional(rel(line, n))
        if old is not None and old.height:
            acc.insert(0, old.filter(~pl.col("sha").is_in(list(drop))))
        data.store.write_parquet(rel(line, n), pl.concat(acc, how="diagonal_relaxed") if acc else pl.DataFrame())


def update(data: Data, shas: list[str], fresh: bool = False) -> dict:
    """Copy the line rows of the feeds ``shas`` (all built feeds and nothing kept with ``fresh``)."""
    t0 = time.time()
    man = (None if fresh else _read_manifest(data)) or {"shas": {}, "lines": {}}
    per_line: dict[str, list[dict]] = {}
    stamps = {}
    for sha in shas:
        stamps[sha] = data.store.read_json(derived(f"patterns/sha={sha}/_SUCCESS")).get("written_at")
        got = _sha_lines(data, sha)
        for line, t in got.items():
            per_line.setdefault(line, []).append(t)
        for line in man["lines"].get(sha, []):      # lines the feed had before: their old rows go
            per_line.setdefault(line, [])
        man["lines"][sha] = sorted(got, key=by_line)
    drop = set(shas)
    data._map(lambda ln: _write_line(data, ln, per_line[ln], drop, fresh), sorted(per_line, key=by_line))
    man["shas"].update(stamps)
    _write_manifest(data, man)
    log.info("linenets: %d feeds, %d lines, %.1f s", len(shas), len(per_line), time.time() - t0)
    return {"feeds": len(shas), "lines": len(per_line), "seconds": round(time.time() - t0, 1)}


def built_shas(data: Data) -> list[str]:
    return sorted(p.rsplit("sha=", 1)[1].split("/")[0] for p in data.store.glob(derived("patterns/sha=*/_SUCCESS")))


def rebuild(data: Data) -> dict:
    """All line files from all built feeds (backfill)."""
    return update(data, built_shas(data), fresh=True)


# ---------------------------------------------------------------------------------- read
class _View:
    """What ``Data.network`` reads, served from one line's files: ``Data.network`` runs on it as is."""
    _pick_feeds = staticmethod(Data._pick_feeds)

    def __init__(self, data: Data, by_sha: dict[str, dict[str, pl.DataFrame]]):
        self.candidate_shas = data.candidate_shas
        self.by_sha = by_sha

    def _feed_tables(self, shas, line, geometry):
        return {s: self.by_sha.get(s) for s in shas}


def _split(tables: dict[str, pl.DataFrame]) -> dict[str, dict[str, pl.DataFrame]]:
    """sha -> the line's tables in that feed, shaped as ``Data.line_tables(geometry=False)``."""
    geo = pl.lit(None, GEOMETRY).alias("geometry")
    parts = {n: t.partition_by("sha", as_dict=True, include_key=False) if t.height else {} for n, t in tables.items()}
    stops = pl.DataFrame(schema={"stop_id": pl.Utf8, "stop_name": pl.Utf8, "stop_lon": pl.Float64,
                                 "stop_lat": pl.Float64, "line": pl.Utf8})
    empty = {n: t.clear().drop("sha", strict=False) for n, t in tables.items()}
    out = {}
    for (sha,), p in parts["patterns"].items():
        out[sha] = {"patterns": p.with_columns(geo),
                    "links": parts["links"].get((sha,), empty["links"]).with_columns(geo),
                    "stops": stops, "pattern_days": parts["pattern_days"].get((sha,), empty["pattern_days"])}
    return out


def _line(data: Data, line: str, stamp: str) -> dict[str, dict[str, pl.DataFrame]] | None:
    """The line's tables per feed (None: no files)."""
    def f():
        got = data.read_many([rel(line, n) for n in TABLES])
        return None if any(g is None for g in got) else _split(dict(zip(TABLES, got)))
    return data._cached(f"linenets:{line}:{stamp}", f)


def network(data: Data, line: str, dates):
    """Network of ``line`` on ``dates`` without shapes, as ``data.network(line, dates, geometry=False)``,
    or None when a feed those dates may use is not in the manifest (then read the per-feed files)."""
    man = data._cached("linenets:manifest", lambda: _read_manifest(data), ttl=120)
    if not man:
        return None
    shas = {x for c in data.candidate_shas(dates).values() for x in c}
    if not shas <= set(man.get("shas", {})):
        return None
    line = str(line)
    tables = _line(data, line, man["written_at"])
    if tables is None and any(line in man["lines"].get(s, []) for s in shas):
        return None                                  # files missing for a line the feeds have
    return Data.network(_View(data, tables or {}), line, dates, geometry=False)

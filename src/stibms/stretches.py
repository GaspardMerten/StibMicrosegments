"""Stretches of the network ranking: platforms of the same stops whose tracks overlap are one stretch.

Two links with the same (title-cased) stop names are one stretch when at least ``MERGE_OVERLAP`` of the
shorter track lies within ``MERGE_DIST_M`` of the longer one.
"""
from __future__ import annotations

import math

import numpy as np
import polars as pl

MERGE_OVERLAP = 0.8   # two links of the same stop names are one stretch when >= 80 % of the shorter ...
MERGE_DIST_M = 30.0   # ... lies within 30 m of the longer
_ov_cache: dict[tuple[str, str], float] = {}   # (link_key, link_key) -> overlap


def _xy(coords, lat0: float):
    a = np.asarray(coords, dtype=float)
    return np.column_stack([a[:, 0] * 111320.0 * math.cos(math.radians(lat0)), a[:, 1] * 110540.0])


def _densify(P, step: float = 5.0):
    out = [P[:1]]
    for p, q in zip(P[:-1], P[1:]):
        n = max(1, int(np.hypot(*(q - p)) // step))
        t = np.arange(1, n + 1)[:, None] / n
        out.append(p + (q - p) * t)
    return np.vstack(out)


def overlap(a, b) -> float:
    """Share of the shorter polyline (lon/lat) within MERGE_DIST_M of the other one."""
    if not a or not b or len(a) < 2 or len(b) < 2:
        return 0.0
    lat0 = a[0][1]
    A, B = _xy(a, lat0), _xy(b, lat0)
    la, lb = np.hypot(*np.diff(A, axis=0).T).sum(), np.hypot(*np.diff(B, axis=0).T).sum()
    short, long_ = (A, B) if la <= lb else (B, A)
    pts = _densify(short)                                  # n x 2
    p0, d = long_[:-1], np.diff(long_, axis=0)             # m x 2
    L = (d * d).sum(1)
    t = np.clip(((pts[:, None, :] - p0[None]) * d[None]).sum(2) / np.where(L > 0, L, 1)[None], 0, 1)
    dist = np.hypot(*(pts[:, None, :] - p0[None] - t[..., None] * d[None]).transpose(2, 0, 1)).min(1)
    return float((dist <= MERGE_DIST_M).mean())


def _overlap_keys(a: str, b: str, geo: dict) -> float:
    k = (a, b) if a < b else (b, a)
    v = _ov_cache.get(k)
    if v is None:
        v = _ov_cache[k] = overlap(geo.get(a), geo.get(b))
    return v


def merge_groups(g: pl.DataFrame, geo: dict[str, list]) -> dict[str, str]:
    """link_key -> stretch key: links with the same (title-cased) stop names whose tracks overlap
    (different platforms of the same stops) become one stretch, keyed by its costliest link."""
    out = {k: k for k in g["link_key"].to_list()}
    dup = g.group_by("from_t", "to_t").agg(pl.col("link_key"), pl.col("veh_s_day")).filter(pl.col("link_key").list.len() > 1)
    for keys, vals in zip(dup["link_key"].to_list(), dup["veh_s_day"].to_list()):
        order = [k for _, k in sorted(zip(vals, keys), key=lambda t: -t[0])]
        parent = {k: k for k in order}

        def find(k):
            while parent[k] != k:
                k = parent[k]
            return k
        for i, a in enumerate(order):
            for b in order[i + 1:]:
                if find(a) != find(b) and _overlap_keys(a, b, geo) >= MERGE_OVERLAP:
                    ra, rb = find(a), find(b)
                    # the root is the costlier link (earlier in ``order``)
                    if order.index(ra) < order.index(rb):
                        parent[rb] = ra
                    else:
                        parent[ra] = rb
        for k in order:
            out[k] = find(k)
    return out

"""Route changes of a line over a period (``/api/versions``): the main-pattern routes per direction,
the stops they drop or add against the direction's reference, their detours and moved stops, and the
lasting change that made the reference the route when it already ran at the start.
"""
from __future__ import annotations

import datetime as dt

from . import linenets
from .data import Data, date_range

# A version with the reference's stops differs by its track only. GTFS shape edits move a link by a
# few metres to a few tens; a detour moves it by hundreds. Local data (2024-10, 2025-02..05, all lines,
# incl. 92 in 2025-05 and 7 in 2025-02..05, which have none): shape edits and small platform moves stay
# under 45 m per link, or 150 m when the next link takes it back (73 International -> Ceria -123 m),
# and under 1.3 % in total; detours are 254-415 m on one link (54 Ma Campagne -> Trinité +415 m,
# 58 Vilvoorde Station -> Blokken -261 m). A temporary
# stop moved along the route lengthens one link and shortens the next by as much (75 Westland Shopping
# 250 m): runs of changed links are summed, so that is a moved stop, not a detour.
DETOUR_MIN_M = 150.0      # a run of links is a detour when its length changes by more than ...
DETOUR_SHARE = 0.20       # ... max(150 m, 20 % of its reference length)
DETOUR_TOTAL = 0.05       # or the whole route by more than 5 %
DETOUR_RUN_M = 30.0       # links that changed by more than this form a run (below: shape noise)


def _track_changes(ref_len: list[float], len_: list[float], names: list[str]) -> list[dict]:
    """Detours and moved stops of a version with the reference's stops, from the per-link lengths
    (``names``: the stops, one more than the links). Each: kind (detour | moved_stop), from, to (the
    stops around the run), stop (moved_stop), delta_m (length change of the run, or the stop's shift)."""
    d = [b - a for a, b in zip(ref_len, len_)]
    out, i, n = [], 0, len(d)
    big = lambda k, ref: abs(k) > max(DETOUR_MIN_M, DETOUR_SHARE * ref)   # noqa: E731
    while i < n:
        if abs(d[i]) <= DETOUR_RUN_M:
            i += 1
            continue
        j = i
        while j + 1 < n and abs(d[j + 1]) > DETOUR_RUN_M:
            j += 1
        net, ref = sum(d[i:j + 1]), sum(ref_len[i:j + 1])
        if big(net, ref):
            out.append({"kind": "detour", "from": names[i], "to": names[j + 1], "delta_m": round(net)})
        else:
            k = max(range(i, j + 1), key=lambda x: abs(d[x]))
            if big(d[k], ref_len[k]) and k < j:       # one link longer, the next shorter: the stop moved
                out.append({"kind": "moved_stop", "stop": names[k + 1], "from": names[i], "to": names[j + 1],
                            "delta_m": round(abs(d[k]))})
        i = j + 1
    tot = sum(len_) - sum(ref_len)
    if not any(x["kind"] == "detour" for x in out) and abs(tot) > DETOUR_TOTAL * sum(ref_len):
        i = max(range(n), key=lambda x: abs(d[x]))
        out.append({"kind": "detour", "from": names[i], "to": names[i + 1], "delta_m": round(tot)})
    return out


def _group_versions(runs: list[dict]) -> list[dict]:
    """One route per direction and stop sequence (and, with the reference's stops, per set of detours /
    moved stops), from the main-pattern runs of ``Network.versions`` (``runs``: in date order per
    direction, each with ``first``, ``last``, ``n_days``, ``stops`` and the per-link ``_len``). Feed
    changes and shape edits give several pattern_uids, and a route that comes back gives several runs:
    they are one route here. ``periods``: its runs, those next to each other merged (runs follow
    each other over the days with data, so a gap of days without data does not split a period),
    each with first, last, n_days; ``first`` / ``last`` / ``n_days`` over all of them. The reference
    is the route of the run with the most stops, the latest on a tie: ``ref`` true on it, ``removed`` /
    ``added`` stop names elsewhere, and on a route with the reference's stops its ``detours`` (see
    :func:`_track_changes`; a shape edit has none, so it is the reference)."""
    out = []
    for dr in sorted({r["direction_id"] for r in runs}):
        rs = sorted((r for r in runs if r["direction_id"] == dr), key=lambda r: r["first"])
        ref = max(rs, key=lambda r: (r["n_stops"], r["last"], r["first"]))
        groups = _direction_groups(dr, rs, ref)
        ref_key = (tuple(ref["stops"]), ())
        out += [_finish_group(g, key == ref_key, ref) for key, g in groups.items()]
    return out


def _direction_groups(dr, rs: list[dict], ref: dict) -> dict[tuple, dict]:
    """The runs ``rs`` of one direction (date order) grouped by stops and detours, with their periods."""
    groups: dict[tuple, dict] = {}
    prev = None
    for r in rs:
        same = r["stops"] == ref["stops"] and len(r["_len"]) == len(ref["_len"]) == len(r["stops"]) - 1
        det = _track_changes(ref["_len"], r["_len"], r["stops"]) if same and r is not ref else []
        key = (tuple(r["stops"]), tuple((t["kind"], t["from"], t["to"], t.get("stop")) for t in det))
        g = groups.get(key)
        if g is None:
            g = groups[key] = {"direction_id": dr, "pattern_uid": r["pattern_uid"], "from": r["from"], "to": r["to"],
                               "n_stops": r["n_stops"], "stops": r["stops"], "detours": det, "periods": [], "_best": 0}
        if key == prev:
            p = g["periods"][-1]
            p["last"], p["n_days"] = r["last"], p["n_days"] + r["n_days"]
        else:
            g["periods"].append({"first": r["first"], "last": r["last"], "n_days": r["n_days"]})
        if r["n_days"] > g["_best"]:          # the pattern and detour lengths of its longest run
            g.update(pattern_uid=r["pattern_uid"], detours=det, _best=r["n_days"])
        prev = key
    return groups


def _finish_group(g: dict, is_ref: bool, ref: dict) -> dict:
    del g["_best"]
    g["ref"] = is_ref
    have, refs = set(g["stops"]), set(ref["stops"])
    g["removed"] = [] if is_ref else [n for n in ref["stops"] if n not in have]
    g["added"] = [] if is_ref else [n for n in g["stops"] if n not in refs]
    g["first"], g["last"] = g["periods"][0]["first"], g["periods"][-1]["last"]
    g["n_days"] = sum(p["n_days"] for p in g["periods"])
    return g


# A route that already ran when the period starts may itself be a lasting change made before it (55,
# 2025-05-21: Da Vinci -> Rogier cut back to Tilleul -> Rogier); inside the period it is the reference
# and shows nothing. The main-pattern runs of the year before tell when it started and what ran before.
LOOKBACK_DAYS = 365
LASTING_MIN_DAYS = 7      # another route for fewer days (works, one-day diversions) does not end it


def _lasting_change(back: list[dict], ref: dict) -> dict | None:
    """The change that made ``ref`` (a :func:`_group_versions` reference, running when the period
    starts) the route, from ``back`` (main-pattern runs before the period, ``_runs`` format, one
    direction): walking back over the runs of its stops and over shorter interruptions, the first
    run of another route of at least ``LASTING_MIN_DAYS`` days is the route before. None when there
    is none in ``back`` or it serves the same stops. Else: since (first day of ``ref``'s stops after
    it), and the route before (from, to, n_stops, stops, last) with ``removed`` / ``added`` stop names."""
    since, before = None, None
    for r in sorted(back, key=lambda r: r["first"], reverse=True):
        if r["stops"] == ref["stops"]:
            since = r["first"]
        elif r["n_days"] >= LASTING_MIN_DAYS:
            before = r
            break
    if before is None:
        return None
    have, prev = set(ref["stops"]), set(before["stops"])
    removed = [n for n in before["stops"] if n not in have]
    added = [n for n in ref["stops"] if n not in prev]
    if not removed and not added:
        return None
    return {"since": since or ref["first"],
            "before": {"from": before["from"], "to": before["to"], "n_stops": before["n_stops"],
                       "stops": before["stops"], "last": before["last"]},
            "removed": removed, "added": added}


def _runs(net) -> list[dict]:
    from microsegments.report import title_name
    v = net.versions().join(net.patterns.select("pattern_uid", "stop_names"), on="pattern_uid", how="left")
    runs = []
    for r in v.iter_rows(named=True):
        names = r["stop_names"] or []
        runs.append({"direction_id": r["direction_id"], "pattern_uid": r["pattern_uid"],
                     "first": r["first"].isoformat(), "last": r["last"].isoformat(), "n_days": r["n_days"],
                     "from": names[0] if names else None, "to": names[-1] if names else None, "n_stops": len(names),
                     "stops": [title_name(n) for n in names], "_len": net.links_of(r["pattern_uid"])["len_m"].to_list()})
    return runs


def _line_runs(data: Data, line: str, dates: list[dt.date]) -> list[dict]:
    if not dates:
        return []
    # stops and link lengths only: the line's own copy of the feed tables, else the per-feed files
    net = linenets.network(data, line, dates)
    if net is None:
        net = data.network(line, dates, geometry=False)
    return _runs(net) if net.pattern_days.height else []


def versions(data: Data, line: str, first: dt.date, last: dt.date) -> list[dict]:
    """Routes run as main pattern per direction over the period, with their stop names, the periods
    they ran and what changes against the direction's reference (see :func:`_group_versions`). A
    reference already running on the period's first day carries ``lasting`` when it replaced another
    route in the ``LOOKBACK_DAYS`` before (see :func:`_lasting_change`), else ``lasting`` is None."""
    ingested = set(data.ingested_dates())
    runs = _line_runs(data, line, [d for d in date_range(first, last) if d in ingested])
    if not runs:
        return []
    out = _group_versions(runs)
    back = _line_runs(data, line, [d for d in date_range(first - dt.timedelta(days=LOOKBACK_DAYS),
                                                         first - dt.timedelta(days=1)) if d in ingested])
    for g in out:
        g["lasting"] = None
        start = min(r["first"] for r in runs if r["direction_id"] == g["direction_id"])
        if g["ref"] and g["first"] == start:
            g["lasting"] = _lasting_change([r for r in back if r["direction_id"] == g["direction_id"]], g)
    return out

"""The few entry points of the ``microsegments`` package the platform calls, in one place.

Everything is imported lazily (API cold start) and checked here, so a package change shows up as
one clear error instead of scattered AttributeErrors.
"""
from __future__ import annotations

import inspect
from functools import lru_cache


class PackageMissing(RuntimeError):
    """The installed microsegments lacks a module the platform needs."""


@lru_cache(maxsize=None)
def version() -> str:
    import microsegments
    return microsegments.__version__


def config():
    """Config used for every STIB derive / analysis (vehicle-distance: linear input)."""
    from microsegments import Config
    return Config.from_dict({"input": {"kind": "stib", "timezone": "Europe/Brussels", "service_day_start": "04:00"},
                             "gtfs": {"route_key": "route_short_name"}})


def have_locate() -> bool:
    try:
        from microsegments import locate, tracks  # noqa: F401
        return hasattr(locate, "place") and hasattr(tracks, "passages")
    except ImportError:
        return False


def place(obs, net, cfg):
    try:
        from microsegments.locate import place as _place
    except ImportError as e:
        raise PackageMissing("microsegments.locate.place is not available") from e
    return _place(obs, net, cfg)


def passages(placed, net, events):
    try:
        from microsegments.tracks import passages as _passages
    except ImportError as e:
        raise PackageMissing("microsegments.tracks.passages is not available") from e
    return _passages(placed, net, events)


def placed_frame(placed):
    """``Placed`` object or bare frame -> frame."""
    return getattr(placed, "frame", placed)


def _to_contract_fn():
    for mod in ("microsegments.report", "microsegments.pipeline", "microsegments.html.export"):
        try:
            m = __import__(mod, fromlist=["to_contract"])
        except ImportError:
            continue
        if hasattr(m, "to_contract"):
            return m.to_contract
    return None


def have_contract() -> bool:
    return _to_contract_fn() is not None


def to_contract(analysis, net, hotspots=None, **kw) -> dict:
    """The JSON contract (microsegments.contract) of an Analysis; keyword arguments the installed
    ``to_contract`` does not take are dropped."""
    fn = _to_contract_fn()
    if fn is None:
        raise PackageMissing("microsegments to_contract is not available (report / pipeline module)")
    sig = inspect.signature(fn)
    var_kw = any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values())
    kw = {k: v for k, v in kw.items() if var_kw or k in sig.parameters}
    return fn(analysis, net, hotspots=hotspots, **kw)

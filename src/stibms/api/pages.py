"""The site's pages: ``/``, ``/comparer``, ``/classement`` (one page, ``static/index.html``), ``/view`` (the
package's analysis page) and the static files. ``__REV__`` in the page, its scripts and stylesheets is
the release token (``cache.rev``): every URL of a new release is new."""
from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, Response

from .cache import SHORT_CACHE, rev
from .context import log, state

router = APIRouter()
STATIC = Path(__file__).parent.parent / "static"
LOADER = Path(__file__).parent / "loader.js"
REV_TYPES = {".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8"}


def _with_rev(p: Path, media: str) -> Response:
    return Response(p.read_text().replace("__REV__", rev()), media_type=media, headers={"Cache-Control": SHORT_CACHE})


@router.get("/", include_in_schema=False)
@router.get("/comparer", include_in_schema=False)
@router.get("/classement", include_in_schema=False)
def index():
    return _with_rev(STATIC / "index.html", "text/html; charset=utf-8")


@router.get("/methode", include_in_schema=False)
def methode():
    return RedirectResponse("/", status_code=301)


def _template() -> str | None:
    """The package's page (``microsegments.html.export.render``) with a fetch shim instead of embedded
    data: the page is rendered with an empty contract, its main script is parked as inert text, and
    the loader (``loader.js``) fetches ``/api/analysis`` with the page's own query string, fills the
    ``ms-data`` JSON element and runs the parked script. The page is otherwise unchanged."""
    try:
        from microsegments.html.export import render
        html = render({"tiles": True}, title="STIB · microsegments", lang="fr",
                      description="Vitesse moyenne des véhicules STIB sur chaque tronçon de 30 m.")
    except Exception:  # noqa: BLE001 - template not shipped / incompatible: the page falls back
        log.exception("page template unavailable")
        return None
    m = re.search(r'<script id="ms-data" type="application/json">.*?</script>\s*<script>(.*?)</script>', html, flags=re.S)
    if not m:
        return None
    a, b = m.span(1)
    html = (html[:m.start()] + '<script id="ms-data" type="application/json"></script>\n'
            + '<script type="text/plain" id="ms-main">' + html[a:b] + "</script>" + html[m.end():])
    i = html.rfind("</body>")
    loader = "<script>\n" + LOADER.read_text().replace("__REV__", rev()) + "</script>"
    return html[:i] + loader + html[i:] if i >= 0 else html + loader


@router.api_route("/view", methods=["GET", "HEAD"], include_in_schema=False)
def view():
    html = state.once(state.blobs, f"template-{rev()}", _template)   # a new release renders a new page
    if html is None:
        raise HTTPException(404, "the microsegments page template is not available")
    return Response(html, media_type="text/html; charset=utf-8", headers={"Cache-Control": SHORT_CACHE})


@router.get("/static/{name}", include_in_schema=False)
def static(name: str):
    p = (STATIC / name).resolve()
    if p.parent != STATIC.resolve() or not p.is_file():
        raise HTTPException(404)
    if p.suffix in REV_TYPES:
        return _with_rev(p, REV_TYPES[p.suffix])
    return FileResponse(p, headers={"Cache-Control": SHORT_CACHE})

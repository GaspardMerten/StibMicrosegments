"""Minimal MobilityTwin.Brussels client for the ingest (index, blob fetch, parquetized listing).

Same API as the public single-file client (https://mobilitytwin.brussels/client/mobilitytwin.py):

- ``/<group>/<endpoint>/index?start_timestamp&end_timestamp`` lists every snapshot of a range with
  a public blob URL (paged by ``next_start_timestamp``). Blobs need no token.
- ``/parquetized?component=stib_vehicle_distance&start_timestamp&end_timestamp`` lists the bulk
  Parquet files of a feed: one per UTC day (``.../stib_vehicle_distance/<d0>_to_<d1>.parquet``)
  for closed days and one per hour (``.../stib_vehicle_distance_parquetize/...``) for the current
  one. Same rows as the JSON snapshots (columns lineId, directionId, pointId, distanceFromPoint,
  date), minus the empty snapshots. History back to 2024-01, longer than the JSON index
  (2024-08-21).

The token comes from ``MOBILITYTWIN_TOKEN`` (Secret Manager env var on Cloud Run) or the nearest
``.env``. It is never logged.
"""
from __future__ import annotations

import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests

API_URL = os.environ.get("MOBILITYTWIN_API_URL", "https://api.mobilitytwin.brussels")
TOKEN_ENV = "MOBILITYTWIN_TOKEN"
_RANGE = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})")


class MobilityTwinError(RuntimeError):
    pass


@dataclass(frozen=True)
class Snapshot:
    timestamp: int  # epoch s UTC (floor of `date`)
    date: str       # ISO, UTC, naive
    url: str


@dataclass(frozen=True)
class ParquetPart:
    url: str
    start: int  # epoch s, inclusive
    end: int    # epoch s, exclusive


def load_token() -> str:
    token = os.environ.get(TOKEN_ENV)
    if token:
        return token.strip()
    for folder in [Path.cwd(), *Path.cwd().parents]:
        env_file = folder / ".env"
        if env_file.is_file():
            for line in env_file.read_text().splitlines():
                key, _, value = line.partition("=")
                if key.strip().removeprefix("export ").strip() == TOKEN_ENV and value.strip():
                    return value.strip().strip("'\"")
    raise MobilityTwinError(f"No API token: set {TOKEN_ENV} or add it to a .env file")


def _parse_range_ts(text: str) -> int:
    return int(datetime.strptime(text, "%Y-%m-%d_%H-%M-%S").replace(tzinfo=timezone.utc).timestamp())


class MobilityTwin:
    def __init__(self, token: str | None = None, api_url: str = API_URL, timeout: float = 300,
                 retries: int = 5):
        self._token = token or load_token()
        self.api_url = api_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self._local = threading.local()
        self._lock = threading.Lock()
        self.bytes_downloaded = 0
        self.requests = 0

    def _session(self) -> requests.Session:
        s = getattr(self._local, "session", None)
        if s is None:
            s = self._local.session = requests.Session()
        return s

    def _get(self, url: str, params: dict | None = None, auth: bool = True,
             headers: dict | None = None) -> requests.Response:
        h = dict(headers or {})
        if auth:
            h["Authorization"] = f"Bearer {self._token}"
        for attempt in range(self.retries + 1):
            try:
                r = self._session().get(url, params=params, headers=h, timeout=self.timeout)
            except requests.RequestException:
                if attempt == self.retries:
                    raise
            else:
                if r.status_code < 400:
                    with self._lock:
                        self.bytes_downloaded += len(r.content)
                        self.requests += 1
                    return r
                if r.status_code not in (429, 500, 502, 503, 504) or attempt == self.retries:
                    # r.url never carries the token (it is a header).
                    raise MobilityTwinError(f"{r.status_code} for {r.url}: {r.text[:300]}")
            time.sleep(min(2 ** attempt, 30))
        raise AssertionError("unreachable")

    def index(self, endpoint: str, start: int, end: int) -> list[Snapshot]:
        """Every snapshot of ``group/endpoint`` with start <= timestamp <= end, oldest first."""
        out, seen, start_ts = [], set(), int(start)
        while True:
            page = self._get(f"{self.api_url}/{endpoint.strip('/')}/index",
                             {"start_timestamp": start_ts, "end_timestamp": int(end)}).json()
            for row in page["results"]:
                if row["date"] not in seen:
                    seen.add(row["date"])
                    out.append(Snapshot(int(row["timestamp"]), row["date"], row["url"]))
            nxt = page.get("next_start_timestamp")
            if not nxt:
                return out
            start_ts = int(nxt)

    def parquet_parts(self, component: str, start: int, end: int) -> list[ParquetPart]:
        """Bulk Parquet files of ``component`` (e.g. ``stib_vehicle_distance``) overlapping [start, end]."""
        r = self._get(f"{self.api_url}/parquetized",
                      {"component": component, "start_timestamp": int(start), "end_timestamp": int(end)})
        body = r.json()
        parts = []
        for url in body.get("results", []):
            m = _RANGE.search(url)
            if m:
                parts.append(ParquetPart(url, _parse_range_ts(m.group(1)), _parse_range_ts(m.group(2))))
        parts.sort(key=lambda p: (p.start, p.end))
        return [p for p in parts if p.end > start and p.start <= end]

    def fetch(self, url: str) -> bytes:
        """Raw bytes of a public blob (no token sent)."""
        return self._get(url, auth=False).content

    def fetch_range(self, url: str, start: int, length: int) -> bytes:
        return self._get(url, auth=False, headers={"Range": f"bytes={start}-{start + length - 1}"}).content

    def size(self, url: str) -> int:
        r = self._session().head(url, timeout=self.timeout)
        r.raise_for_status()
        return int(r.headers["content-length"])

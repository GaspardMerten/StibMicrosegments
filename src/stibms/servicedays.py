"""Brussels service days. Service day D runs from D 04:00 to D+1 03:00 Europe/Brussels (the same
window as the prototype fetch_vd55_2025.py), so a 01:30 poll belongs to the previous day and the
03:00-04:00 hour belongs to none (the network is closed)."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Europe/Brussels")
DAY_START = time(4, 0)
DAY_END_NEXT = time(3, 0)


def window(d: date) -> tuple[int, int]:
    """(t0, t1) epoch seconds UTC of service day ``d``; both bounds inclusive, as the index API."""
    t0 = datetime.combine(d, DAY_START, TZ)
    t1 = datetime.combine(d + timedelta(days=1), DAY_END_NEXT, TZ)
    return int(t0.timestamp()), int(t1.timestamp())


def local_midnight(d: date) -> int:
    return int(datetime.combine(d, time(0), TZ).timestamp())


def date_range(a: date, b: date) -> list[date]:
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


def yesterday(now: datetime | None = None) -> date:
    now = now or datetime.now(timezone.utc)
    return now.astimezone(TZ).date() - timedelta(days=1)


def parse_date(text: str) -> date:
    text = text.strip().lower()
    if text == "yesterday":
        return yesterday()
    return date.fromisoformat(text)

"""Belgian public holidays (federal list, the ten days off for everyone).

Easter is computed (anonymous Gregorian algorithm), so any year works. Regional days (8 May in
Brussels, 11 July, 27 September) and school holidays are not included.
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache


def easter(year: int) -> date:
    a, b, c = year % 19, year // 100, year % 100
    d, e = b // 4, b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = c // 4, c % 4
    l_ = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_) // 451
    month = (h + l_ - 7 * m + 114) // 31
    day = (h + l_ - 7 * m + 114) % 31 + 1
    return date(year, month, day)


@lru_cache(maxsize=64)
def holidays(year: int) -> dict[date, str]:
    e = easter(year)
    return {
        date(year, 1, 1): "Nouvel An",
        e + timedelta(days=1): "Lundi de Pâques",
        date(year, 5, 1): "Fête du Travail",
        e + timedelta(days=39): "Ascension",
        e + timedelta(days=50): "Lundi de Pentecôte",
        date(year, 7, 21): "Fête nationale",
        date(year, 8, 15): "Assomption",
        date(year, 11, 1): "Toussaint",
        date(year, 11, 11): "Armistice",
        date(year, 12, 25): "Noël",
    }


def between(a: date, b: date) -> dict[date, str]:
    out: dict[date, str] = {}
    for y in range(a.year, b.year + 1):
        out.update({d: n for d, n in holidays(y).items() if a <= d <= b})
    return dict(sorted(out.items()))

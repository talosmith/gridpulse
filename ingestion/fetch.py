"""Fetch raw payloads and wrap them in a self-describing envelope.

The envelope keeps the raw API response untouched (bronze = source of truth)
plus the metadata needed to replay, audit and deduplicate it later.
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone

from .sources import (
    ARCHIVE_LAG_DAYS,
    OPEN_METEO_ARCHIVE_URL,
    OPEN_METEO_URL,
    REE_BASE_URL,
    ReeDataset,
    WeatherDataset,
)

USER_AGENT = "gridpulse/0.1 (+https://github.com/talosmith/gridpulse)"
ENVELOPE_VERSION = 1


class FetchError(RuntimeError):
    pass


@dataclass(frozen=True)
class Envelope:
    source: str
    dataset: str
    window_start: date
    window_end: date
    request_url: str
    fetched_at: datetime
    payload: object

    def to_json(self) -> str:
        return json.dumps(
            {
                "envelope_version": ENVELOPE_VERSION,
                "source": self.source,
                "dataset": self.dataset,
                "window_start": self.window_start.isoformat(),
                "window_end": self.window_end.isoformat(),
                "request_url": self.request_url,
                "fetched_at": self.fetched_at.isoformat(),
                "payload": self.payload,
            },
            ensure_ascii=False,
        )


def http_get_json(url: str, retries: int = 3, timeout: int = 30) -> object:
    """GET a JSON document with simple exponential backoff on transient errors."""
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")[:300]
            last_error = FetchError(f"HTTP {error.code} for {url}: {body}")
            if error.code < 500 and error.code != 429:
                # REE answers 400 when data is not published yet: not worth retrying
                break
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as error:
            last_error = FetchError(f"{type(error).__name__} for {url}: {error}")
        time.sleep(2**attempt)
    raise last_error  # type: ignore[misc]


def ree_url(dataset: ReeDataset, start: date, end: date) -> str:
    query = urllib.parse.urlencode(
        {
            "start_date": f"{start.isoformat()}T00:00",
            "end_date": f"{end.isoformat()}T23:59",
            "time_trunc": dataset.time_trunc,
        }
    )
    return f"{REE_BASE_URL}/{dataset.path}?{query}"


def weather_url(dataset: WeatherDataset, start: date, end: date, today: date | None = None) -> str:
    """Old windows go to the ERA5 archive; recent ones to the forecast API (which only keeps ~3 months)."""
    today = today or datetime.now(timezone.utc).date()
    base = OPEN_METEO_ARCHIVE_URL if (today - end).days > ARCHIVE_LAG_DAYS else OPEN_METEO_URL
    query = urllib.parse.urlencode(
        {
            "latitude": ",".join(str(p.latitude) for p in dataset.points),
            "longitude": ",".join(str(p.longitude) for p in dataset.points),
            "hourly": ",".join(dataset.variables),
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "timezone": dataset.timezone,
            **dataset.extra,
        }
    )
    return f"{base}?{query}"


def fetch_ree(dataset: ReeDataset, start: date, end: date, get=http_get_json) -> Envelope:
    url = ree_url(dataset, start, end)
    payload = get(url)
    if isinstance(payload, dict) and "errors" in payload:
        raise FetchError(f"REE error for {dataset.name}: {payload['errors']}")
    return Envelope("ree", dataset.name, start, end, url, _now(), payload)


def fetch_weather(dataset: WeatherDataset, start: date, end: date, get=http_get_json) -> Envelope:
    url = weather_url(dataset, start, end)
    payload = get(url)
    # Open-Meteo returns one object per location, in request order: tag each one
    locations = payload if isinstance(payload, list) else [payload]
    if len(locations) != len(dataset.points):
        raise FetchError(f"expected {len(dataset.points)} locations, got {len(locations)}")
    tagged = [{"point": p.name, **loc} for p, loc in zip(dataset.points, locations)]
    return Envelope("open_meteo", dataset.name, start, end, url, _now(), tagged)


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)

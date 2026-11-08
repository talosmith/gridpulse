"""Catalog of the public data sources GridPulse collects.

Every dataset is described declaratively so that adding a new one is a
config change, not a code change.
"""
from __future__ import annotations

from dataclasses import dataclass, field

REE_BASE_URL = "https://apidatos.ree.es/es/datos"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"


@dataclass(frozen=True)
class ReeDataset:
    """An endpoint of the Red Eléctrica de España open data API (no key needed)."""

    name: str
    path: str
    time_trunc: str  # "hour" or "day" (what the endpoint accepts)
    description: str


REE_DATASETS: tuple[ReeDataset, ...] = (
    ReeDataset(
        name="prices",
        path="mercados/precios-mercados-tiempo-real",
        time_trunc="hour",
        description="Day-ahead spot price (15 min) and PVPC regulated tariff (hourly), EUR/MWh",
    ),
    ReeDataset(
        name="demand",
        path="demanda/demanda-tiempo-real",
        time_trunc="hour",
        description="Real, forecast and scheduled peninsular demand (5 min), MW",
    ),
    ReeDataset(
        name="generation_mix",
        path="generacion/estructura-generacion",
        time_trunc="day",
        description="Daily generation by technology (nuclear, wind, solar PV, CCGT...), MWh",
    ),
    ReeDataset(
        name="renewable_share",
        path="generacion/evolucion-renovable-no-renovable",
        time_trunc="day",
        description="Daily renewable vs non-renewable generation, MWh",
    ),
)


@dataclass(frozen=True)
class WeatherPoint:
    """A location whose weather drives Spanish demand, wind or solar output."""

    name: str
    latitude: float
    longitude: float
    why: str


WEATHER_POINTS: tuple[WeatherPoint, ...] = (
    WeatherPoint("madrid", 40.42, -3.70, "largest demand centre"),
    WeatherPoint("barcelona", 41.39, 2.17, "demand centre"),
    WeatherPoint("sevilla", 37.39, -5.98, "solar belt (Andalucía)"),
    WeatherPoint("badajoz", 38.88, -6.97, "solar PV cluster (Extremadura)"),
    WeatherPoint("zaragoza", 41.65, -0.88, "wind cluster (Aragón)"),
    WeatherPoint("a_coruna", 43.36, -8.41, "wind cluster (Galicia)"),
)

WEATHER_VARIABLES: tuple[str, ...] = (
    "temperature_2m",
    "wind_speed_100m",  # hub height, what turbines feel
    "shortwave_radiation",  # what PV panels feel
    "cloud_cover",
)


@dataclass(frozen=True)
class WeatherDataset:
    name: str = "hourly_weather"
    points: tuple[WeatherPoint, ...] = WEATHER_POINTS
    variables: tuple[str, ...] = WEATHER_VARIABLES
    # UTC on purpose: in local time Open-Meteo drops the repeated hour of the
    # autumn DST change (25 Oct 2026 came back with 24 hours instead of 25).
    timezone: str = "GMT"
    extra: dict = field(default_factory=dict)


WEATHER_DATASET = WeatherDataset()

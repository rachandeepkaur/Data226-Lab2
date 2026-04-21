from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List


def fetch_daily_rows(
    base_url: str,
    locations: List[Dict[str, Any]],
    history_days: int,
    *,
    utc_now: datetime | None = None,
) -> List[Dict[str, Any]]:
    """
    Call Open-Meteo forecast API for daily aggregates and return rows keyed for WEATHER_RAW_DAILY.
    """
    import requests

    end = (utc_now or datetime.utcnow()).date()
    start = end - timedelta(days=history_days)
    endpoint = f"{base_url.rstrip('/')}/v1/forecast"

    rows: List[Dict[str, Any]] = []

    for loc in locations:
        params = {
            "latitude": loc["lat"],
            "longitude": loc["lon"],
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "daily": [
                "temperature_2m_max",
                "temperature_2m_min",
                "temperature_2m_mean",
                "precipitation_sum",
            ],
            "timezone": "UTC",
        }

        r = requests.get(endpoint, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        daily = data.get("daily", {})
        dates = daily.get("time", [])

        for i, d in enumerate(dates):
            rows.append(
                {
                    "LOCATION_NAME": loc["name"],
                    "LATITUDE": float(loc["lat"]),
                    "LONGITUDE": float(loc["lon"]),
                    "DATE": d,
                    "TEMP_MAX": daily.get("temperature_2m_max", [None])[i],
                    "TEMP_MIN": daily.get("temperature_2m_min", [None])[i],
                    "TEMP_MEAN": daily.get("temperature_2m_mean", [None])[i],
                    "PRECIP_MM": daily.get("precipitation_sum", [None])[i],
                }
            )

    return rows

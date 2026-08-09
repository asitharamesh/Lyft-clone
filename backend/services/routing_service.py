"""
Distance / ETA calculation.

Two strategies:
  1. `haversine_km` - pure math, O(1), used everywhere as a cheap first-pass
     estimate (great-circle distance). This is what the original project
     used for everything, including final fare pricing - which
     under-estimates real driving distance since roads aren't straight lines.
  2. `road_route` - calls OSRM (the same routing engine the frontend already
     uses to *draw* the route polyline) to get the actual road-network
     distance and duration. This is what should be used for fare/ETA once a
     specific pickup/drop pair is known, since it's far more accurate.

`road_route` degrades gracefully to the haversine estimate (scaled by a
typical road-vs-straight-line detour factor) if OSRM is slow, unreachable,
or disabled - so the ride flow never breaks because a third-party routing
service hiccuped.
"""
import logging
import math

import requests

from config import Config

logger = logging.getLogger("lyft.routing")

# Straight-line distance underestimates real road distance. This factor is a
# commonly used rule-of-thumb correction when a live routing engine isn't
# available.
ROAD_DETOUR_FACTOR = 1.3


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return float("inf")
    r = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _fallback_estimate(lat1, lon1, lat2, lon2) -> dict:
    straight = haversine_km(lat1, lon1, lat2, lon2)
    distance_km = straight * ROAD_DETOUR_FACTOR
    eta_minutes = (distance_km / Config.AVG_CITY_SPEED_KMH) * 60
    return {"distance_km": distance_km, "eta_minutes": eta_minutes, "source": "haversine_fallback"}


def road_route(lat1, lon1, lat2, lon2) -> dict:
    """Returns {distance_km, eta_minutes, source}. `source` is either
    'osrm' or 'haversine_fallback' so callers/logs can see which path was
    used (useful for debugging + demonstrating graceful degradation)."""
    if None in (lat1, lon1, lat2, lon2):
        return {"distance_km": float("inf"), "eta_minutes": float("inf"), "source": "invalid"}

    if not Config.USE_LIVE_ROUTING:
        return _fallback_estimate(lat1, lon1, lat2, lon2)

    url = (
        f"{Config.OSRM_BASE_URL}/route/v1/driving/"
        f"{lon1},{lat1};{lon2},{lat2}?overview=false"
    )
    try:
        resp = requests.get(url, timeout=Config.OSRM_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        route = data["routes"][0]
        return {
            "distance_km": route["distance"] / 1000.0,
            "eta_minutes": route["duration"] / 60.0,
            "source": "osrm",
        }
    except (requests.RequestException, KeyError, IndexError, ValueError) as e:
        logger.warning("OSRM routing failed (%s), falling back to haversine estimate", e)
        return _fallback_estimate(lat1, lon1, lat2, lon2)

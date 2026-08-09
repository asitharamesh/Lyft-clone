"""
Driver matching.

The original implementation, on every ride request, pulled *every single
available driver row* out of Postgres and looped over them in Python
computing haversine distance one by one - O(N) per match, repeated on every
rejection, holding a DB connection the whole time. That's fine for a demo
with 5 drivers; it falls over at real scale.

This version keeps driver locations in a Redis GEO index (a sorted set
under the hood, geohash-encoded) so "who is near this point" is answered in
O(log N + M) via Redis's native `GEOSEARCH`, without touching Postgres at
all on the hot path. Postgres remains the source of truth for driver
profile data (rating, name) and is only queried for the small candidate
set actually returned, and only as a fallback if Redis is unavailable.

Driver selection also isn't purely "closest driver wins" - it blends
distance with the driver's rating so a slightly farther but much
better-rated driver can be preferred, which is closer to how real
dispatch systems (Uber/Lyft) score candidates.
"""
import logging

from config import Config
from db import get_db_conn, get_redis
from services.routing_service import haversine_km

logger = logging.getLogger("lyft.matching")

GEO_KEY = Config.REDIS_DRIVER_GEO_KEY

# How much one full rating star is "worth" in kilometers when scoring
# candidates. A driver rated 1.0 star higher than another is treated as if
# they were RATING_WEIGHT_KM closer.
RATING_WEIGHT_KM = 0.6


def upsert_driver_location(driver_id: int, lat: float, lng: float) -> None:
    r = get_redis()
    r.geoadd(GEO_KEY, (lng, lat, str(driver_id)))
    # A short TTL companion key marks the driver "fresh"; if their app dies
    # without a clean disconnect event, they silently age out of matching
    # instead of being offered rides forever.
    r.setex(f"driver:heartbeat:{driver_id}", Config.DRIVER_LOCATION_TTL_SECONDS, "1")


def remove_driver_location(driver_id: int) -> None:
    r = get_redis()
    r.zrem(GEO_KEY, str(driver_id))
    r.delete(f"driver:heartbeat:{driver_id}")


def _is_fresh(driver_id: int) -> bool:
    return get_redis().exists(f"driver:heartbeat:{driver_id}") == 1


def _candidates_from_redis(lat, lng, exclude_ids, count):
    r = get_redis()
    try:
        raw = r.geosearch(
            name=GEO_KEY,
            longitude=lng,
            latitude=lat,
            radius=Config.DRIVER_SEARCH_RADIUS_KM,
            unit="km",
            sort="ASC",
            count=count + len(exclude_ids),
            withdist=True,
        )
    except Exception as e:  # noqa: BLE001 - redis down/unreachable
        logger.warning("Redis geosearch failed (%s); will fall back to SQL", e)
        return None

    out = []
    for member, dist_km in raw:
        driver_id = int(member)
        if driver_id in exclude_ids:
            continue
        if not _is_fresh(driver_id):
            continue
        out.append((driver_id, dist_km))
        if len(out) >= count:
            break
    return out


def _candidates_from_sql(lat, lng, exclude_ids, count):
    """Fallback path used only if Redis is unreachable. Uses an indexed
    bounding-box pre-filter (cheap, uses the btree index on lat/lng) before
    refining with haversine, rather than scanning the whole drivers table."""
    delta = Config.DRIVER_SEARCH_RADIUS_KM / 111.0  # ~km per degree latitude
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, latitude, longitude
                FROM drivers
                WHERE is_available = TRUE
                  AND latitude BETWEEN %s AND %s
                  AND longitude BETWEEN %s AND %s
                """,
                (lat - delta, lat + delta, lng - delta, lng + delta),
            )
            rows = cur.fetchall()

    scored = []
    for driver_id, d_lat, d_lng in rows:
        if driver_id in exclude_ids:
            continue
        dist = haversine_km(lat, lng, d_lat, d_lng)
        if dist <= Config.DRIVER_SEARCH_RADIUS_KM:
            scored.append((driver_id, dist))
    scored.sort(key=lambda x: x[1])
    return scored[:count]


def _fetch_driver_profiles(driver_ids):
    if not driver_ids:
        return {}
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, rating FROM drivers WHERE id = ANY(%s)",
                (list(driver_ids),),
            )
            return {row[0]: {"name": row[1], "rating": float(row[2] or 4.5)} for row in cur.fetchall()}


def find_best_driver(lat: float, lng: float, exclude_ids: set[int]) -> dict | None:
    """Returns {id, name, distance_km, rating} for the best-scoring driver,
    or None if nobody is available."""
    candidates = _candidates_from_redis(lat, lng, exclude_ids, Config.DRIVER_SEARCH_CANDIDATES)
    if candidates is None:
        candidates = _candidates_from_sql(lat, lng, exclude_ids, Config.DRIVER_SEARCH_CANDIDATES)

    if not candidates:
        return None

    profiles = _fetch_driver_profiles([c[0] for c in candidates])

    best = None
    best_score = float("inf")
    for driver_id, dist_km in candidates:
        profile = profiles.get(driver_id)
        if not profile:
            continue
        # Lower score wins: distance penalized/rewarded by rating deviation
        # from a 4.5-star baseline.
        score = dist_km - (profile["rating"] - 4.5) * RATING_WEIGHT_KM
        if score < best_score:
            best_score = score
            best = {
                "id": driver_id,
                "name": profile["name"],
                "distance_km": round(dist_km, 3),
                "rating": profile["rating"],
            }
    return best

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


# Refresh a driver's position and heartbeat only if they are still in the
# pool. Atomic, so a late heartbeat can never re-add a driver who just went
# offline or accepted a ride.
_REFRESH_IF_MEMBER_LUA = """
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
    redis.call('SETEX', KEYS[2], ARGV[4], '1')
    redis.call('GEOADD', KEYS[1], ARGV[2], ARGV[3], ARGV[1])
    return 1
end
return 0
"""

# Remove a pool member only if its heartbeat is (still) missing. Atomic, so a
# driver who refreshes at the same moment is never removed by mistake.
_REMOVE_IF_STALE_LUA = """
if redis.call('EXISTS', KEYS[2]) == 0 then
    return redis.call('ZREM', KEYS[1], ARGV[1])
end
return 0
"""


def _heartbeat_key(driver_id) -> str:
    return f"driver:heartbeat:{driver_id}"


def upsert_driver_location(driver_id: int, lat: float, lng: float) -> None:
    pipe = get_redis().pipeline(transaction=True)
    # A short TTL companion key marks the driver "fresh"; if their app dies
    # without a clean disconnect event, they silently age out of matching
    # instead of being offered rides forever. Written before GEOADD (and in
    # one MULTI) so the stale-member sweep never sees a member without it.
    pipe.setex(_heartbeat_key(driver_id), Config.DRIVER_LOCATION_TTL_SECONDS, "1")
    pipe.geoadd(GEO_KEY, (lng, lat, str(driver_id)))
    pipe.execute()


def refresh_driver_heartbeat(driver_id: int, lat: float, lng: float) -> bool:
    """Returns True if the driver was in the pool and has been refreshed,
    False if they are not in the pool (offline, on a trip, or already swept
    as stale). Never adds a driver to the pool."""
    result = get_redis().eval(
        _REFRESH_IF_MEMBER_LUA, 2, GEO_KEY, _heartbeat_key(driver_id),
        str(driver_id), lng, lat, Config.DRIVER_LOCATION_TTL_SECONDS,
    )
    return result == 1


def remove_driver_location(driver_id: int) -> None:
    pipe = get_redis().pipeline(transaction=True)
    pipe.zrem(GEO_KEY, str(driver_id))
    pipe.delete(_heartbeat_key(driver_id))
    pipe.execute()


def remove_if_stale(driver_id) -> bool:
    return get_redis().eval(_REMOVE_IF_STALE_LUA, 2, GEO_KEY, _heartbeat_key(driver_id), str(driver_id)) == 1


def sweep_stale_drivers() -> int:
    """Redis GEO has no per-member TTL: when a driver's heartbeat key
    expires, their drivers:geo member stays behind. Remove those members so
    they stop occupying the index. Returns the number removed."""
    r = get_redis()
    members = r.zrange(GEO_KEY, 0, -1)
    if not members:
        return 0
    pipe = r.pipeline(transaction=False)
    for member in members:
        pipe.exists(_heartbeat_key(member))
    removed = 0
    for member, fresh in zip(members, pipe.execute()):
        if not fresh and remove_if_stale(member):
            removed += 1
    return removed


def _candidates_from_redis(lat, lng, exclude_ids, count):
    """Nearest fresh, non-excluded drivers, closest first.

    GEOSEARCH knows nothing about heartbeats, so stale (and excluded)
    members come back from it too. If the search were capped at `count`
    before filtering, ten stale drivers next to the pickup would use up
    every slot and hide a live driver further away. So freshness is checked
    for each returned member (one pipelined round trip), stale members are
    removed from the index as they are found, and the search is widened until
    `count` usable drivers are found or the radius has nothing more."""
    r = get_redis()
    search_count = count + len(exclude_ids)
    try:
        while True:
            raw = r.geosearch(
                name=GEO_KEY,
                longitude=lng,
                latitude=lat,
                radius=Config.DRIVER_SEARCH_RADIUS_KM,
                unit="km",
                sort="ASC",
                count=search_count,
                withdist=True,
            )
            members = [(int(member), dist_km) for member, dist_km in raw if int(member) not in exclude_ids]
            pipe = r.pipeline(transaction=False)
            for driver_id, _ in members:
                pipe.exists(_heartbeat_key(driver_id))
            fresh_flags = pipe.execute() if members else []

            out = []
            for (driver_id, dist_km), fresh in zip(members, fresh_flags):
                if fresh:
                    out.append((driver_id, dist_km))
                else:
                    remove_if_stale(driver_id)
            if len(out) >= count or len(raw) < search_count:
                return out[:count]
            search_count *= 2
    except Exception as e:  # noqa: BLE001 - redis down/unreachable
        logger.warning("Redis geosearch failed (%s); will fall back to SQL", e)
        return None


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

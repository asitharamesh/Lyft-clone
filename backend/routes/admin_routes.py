"""
Admin-only operational API (read-only), used by frontend/admin.html.

Authorization is enforced here on every request, never in the browser:
  1. `Authorization: Bearer <JWT>` must be present and valid  -> else 401
  2. the token's role claim must be "admin"                    -> else 403
  3. users.is_admin must still be TRUE in Postgres             -> else 403
Rider and driver tokens therefore get 403 even though they are valid JWTs,
and revoking the flag (set_admin.py --revoke) takes effect immediately.

The response contains aggregates and ids only: no passwords or hashes,
emails, tokens, secrets, environment variables, raw Redis values, or rider
pickup/drop coordinates. Driver positions are included for the operational
map table, rounded to ~11 m.
"""
import datetime
import logging
import time
from functools import wraps

from flask import Blueprint, g, jsonify, request

from config import Config
from db import get_db_conn, get_redis, health_check
from services import ops_stats
from services.auth_service import decode_token
from sockets.handlers import connected_socket_summary

logger = logging.getLogger("lyft.admin")

admin_bp = Blueprint("admin", __name__, url_prefix="/api/admin")

STUCK_MINUTES = 30
MAX_DRIVER_ROWS = 200


def _is_admin_in_db(user_id: int) -> bool:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT is_admin FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
    return bool(row and row[0])


def require_admin(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        header = request.headers.get("Authorization", "")
        token = header[7:].strip() if header.startswith("Bearer ") else ""
        claims = decode_token(token) if token else None
        if not claims:
            return jsonify({"success": False, "message": "Authentication required"}), 401
        if claims.get("role") != "admin":
            return jsonify({"success": False, "message": "Admin access required"}), 403
        try:
            user_id = int(claims["sub"])
            is_admin = _is_admin_in_db(user_id)
        except Exception:  # noqa: BLE001 - fail closed
            logger.exception("Admin authorization check failed")
            return jsonify({"success": False, "message": "Admin access required"}), 403
        if not is_admin:
            return jsonify({"success": False, "message": "Admin access required"}), 403
        g.admin_user_id = user_id
        return view(*args, **kwargs)

    return wrapper


def _iso(value):
    return value.isoformat() if isinstance(value, (datetime.datetime, datetime.date)) else value


def _rows(cur, sql, params=()):
    cur.execute(sql, params)
    columns = [c.name for c in cur.description]
    return [{col: _iso(val) for col, val in zip(columns, row)} for row in cur.fetchall()]


def _one(cur, sql, params=()):
    rows = _rows(cur, sql, params)
    return rows[0] if rows else {}


def _section(builder):
    try:
        return builder()
    except Exception:  # noqa: BLE001 - one failing source must not hide the others
        logger.exception("Admin overview section %s failed", builder.__name__)
        return {"error": "This section could not be loaded (see server logs)."}


def _system():
    deps = health_check()
    return {
        "backend": "up",
        "postgres": deps["postgres"],
        "redis": deps["redis"],
        "started_at": datetime.datetime.fromtimestamp(ops_stats.PROCESS_STARTED_AT, datetime.timezone.utc).isoformat(),
        "uptime_seconds": int(time.time() - ops_stats.PROCESS_STARTED_AT),
        "environment": Config.ENV,
        "ride_simulator": Config.RIDE_SIMULATOR,
        "load_test_mode": Config.LOAD_TEST_MODE,
        "sockets": connected_socket_summary(),
        "log_counts": ops_stats.log_counter.snapshot(),
        "scope_note": "Socket and log counts cover this backend process only.",
    }


def _users():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            users = _one(cur, """
                SELECT COUNT(*) AS total_user_accounts,
                       COUNT(*) FILTER (WHERE NOT is_admin) AS riders,
                       COUNT(*) FILTER (WHERE is_admin) AS admins
                FROM users
            """)
            drivers = _one(cur, """
                SELECT COUNT(*) AS drivers, COUNT(*) FILTER (WHERE is_available) AS drivers_marked_available
                FROM drivers
            """)
    sockets = connected_socket_summary()["unique_users_by_role"]
    return {
        **users,
        **drivers,
        "connected_riders": sockets.get("rider", 0),
        "connected_drivers": sockets.get("driver", 0),
        "connected_admins": sockets.get("admin", 0),
    }


def _matching():
    r = get_redis()
    geo_key = Config.REDIS_DRIVER_GEO_KEY
    members = r.zrange(geo_key, 0, -1)
    positions = r.geopos(geo_key, *members) if members else []
    pipe = r.pipeline(transaction=False)
    for member in members:
        pipe.ttl(f"driver:heartbeat:{member}")
    ttls = pipe.execute() if members else []

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT driver_id FROM ride_offers WHERE status = 'pending' AND expires_at > NOW()")
            offered = {row[0] for row in cur.fetchall()}
            cur.execute(
                "SELECT driver_id FROM rides WHERE status IN ('accepted', 'in_progress') AND request_id IS NOT NULL"
            )
            on_trip = {row[0] for row in cur.fetchall()}
            ids = [int(m) for m in members]
            cur.execute("SELECT id, name FROM drivers WHERE id = ANY(%s)", (ids,))
            names = dict(cur.fetchall())

    rows = []
    counts = {"in_geo_index": len(members), "fresh": 0, "stale": 0, "offered": 0, "on_trip_but_in_index": 0, "eligible": 0}
    for member, pos, ttl in zip(members, positions, ttls):
        driver_id = int(member)
        fresh = ttl is not None and ttl > 0
        is_offered = driver_id in offered
        is_on_trip = driver_id in on_trip
        eligible = fresh and not is_offered and not is_on_trip
        counts["fresh" if fresh else "stale"] += 1
        counts["offered"] += int(is_offered)
        counts["on_trip_but_in_index"] += int(is_on_trip)
        counts["eligible"] += int(eligible)
        if len(rows) < MAX_DRIVER_ROWS:
            rows.append({
                "driver_id": driver_id,
                "name": names.get(driver_id),
                "lat": round(pos[1], 4) if pos else None,
                "lng": round(pos[0], 4) if pos else None,
                "heartbeat_ttl_seconds": ttl if fresh else 0,
                "fresh": fresh,
                "offered": is_offered,
                "on_trip": is_on_trip,
                "eligible": eligible,
            })
    counters = ops_stats.read_counters()
    return {
        **counts,
        "drivers_on_trip": len(on_trip),
        "drivers_holding_offer": len(offered),
        "match_offered_total": counters.get("match_offered", 0),
        "no_driver_available_total": counters.get("match_no_driver", 0),
        "offer_expired_total": counters.get("match_offer_expired", 0),
        "request_expired_total": counters.get("match_request_expired", 0),
        "config": {
            "search_radius_km": Config.DRIVER_SEARCH_RADIUS_KM,
            "candidates": Config.DRIVER_SEARCH_CANDIDATES,
            "heartbeat_ttl_seconds": Config.DRIVER_LOCATION_TTL_SECONDS,
            "heartbeat_interval_seconds": Config.DRIVER_HEARTBEAT_INTERVAL_SECONDS,
            "offer_timeout_seconds": Config.OFFER_TIMEOUT_SECONDS,
        },
        "drivers": rows,
        "recent_matches": ops_stats.recent_matches(),
    }


def _rides():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            by_status = dict(_rows_tuple(cur, """
                SELECT status, COUNT(*) FROM rides WHERE request_id IS NOT NULL GROUP BY status
            """))
            by_progress = dict(_rows_tuple(cur, """
                SELECT COALESCE(progress, 'none'), COUNT(*) FROM rides
                WHERE request_id IS NOT NULL AND status IN ('accepted', 'in_progress') GROUP BY 1
            """))
            legacy = _one(cur, "SELECT COUNT(*) AS legacy_rows_without_lifecycle FROM rides WHERE request_id IS NULL")
            offered_now = _one(cur, """
                SELECT COUNT(DISTINCT ride_id) AS requested_with_pending_offer
                FROM ride_offers WHERE status = 'pending' AND expires_at > NOW()
            """)
            issues = {
                "requested_without_offer_over_60s": _rows(cur, """
                    SELECT LEFT(r.request_id::text, 8) AS request, r.updated_at FROM rides r
                    WHERE r.status = 'requested' AND r.request_id IS NOT NULL
                      AND r.updated_at < NOW() - INTERVAL '60 seconds'
                      AND NOT EXISTS (SELECT 1 FROM ride_offers o WHERE o.ride_id = r.id
                                      AND o.status = 'pending' AND o.expires_at > NOW())
                    ORDER BY r.updated_at LIMIT 20
                """),
                "active_without_update": _rows(cur, """
                    SELECT LEFT(request_id::text, 8) AS request, status, progress, driver_id, updated_at FROM rides
                    WHERE status IN ('accepted', 'in_progress') AND request_id IS NOT NULL
                      AND updated_at < NOW() - make_interval(mins => %s)
                    ORDER BY updated_at LIMIT 20
                """, (STUCK_MINUTES,)),
                "completed_unpaid": _rows(cur, """
                    SELECT LEFT(request_id::text, 8) AS request, driver_id, fare, completed_at FROM rides
                    WHERE status = 'completed' AND paid_at IS NULL AND request_id IS NOT NULL
                      AND completed_at < NOW() - make_interval(mins => %s)
                    ORDER BY completed_at LIMIT 20
                """, (STUCK_MINUTES,)),
                "inconsistent": _rows(cur, """
                    SELECT LEFT(request_id::text, 8) AS request, status, progress, driver_id, paid_at FROM rides
                    WHERE request_id IS NOT NULL AND (
                        (status IN ('accepted', 'in_progress', 'completed') AND driver_id IS NULL)
                        OR (paid_at IS NOT NULL AND status <> 'completed')
                        OR (status = 'in_progress' AND progress <> 'trip_started')
                    )
                    LIMIT 20
                """),
            }
            recent = _rows(cur, """
                SELECT LEFT(request_id::text, 8) AS request, status, progress, driver_id, fare,
                       cancel_reason, paid_at IS NOT NULL AS paid, updated_at
                FROM rides WHERE request_id IS NOT NULL ORDER BY updated_at DESC LIMIT 20
            """)
    return {
        "by_status": {s: by_status.get(s, 0) for s in ("requested", "accepted", "in_progress", "completed", "cancelled")},
        "active_by_progress": by_progress,
        **offered_now,
        **legacy,
        "issues": issues,
        "recent": recent,
    }


def _rows_tuple(cur, sql, params=()):
    cur.execute(sql, params)
    return cur.fetchall()


def _offers():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            by_status = dict(_rows_tuple(cur, "SELECT status, COUNT(*) FROM ride_offers GROUP BY status"))
            overdue = _one(cur, """
                SELECT COUNT(*) AS overdue_pending FROM ride_offers
                WHERE status = 'pending' AND expires_at <= NOW()
            """)
            pending = _rows(cur, """
                SELECT o.id AS offer_id, LEFT(r.request_id::text, 8) AS request, o.driver_id,
                       ROUND(EXTRACT(EPOCH FROM (NOW() - o.created_at)))::int AS age_seconds,
                       ROUND(EXTRACT(EPOCH FROM (o.expires_at - NOW())))::int AS expires_in_seconds
                FROM ride_offers o JOIN rides r ON r.id = o.ride_id
                WHERE o.status = 'pending' ORDER BY o.created_at LIMIT 50
            """)
            recent = _rows(cur, """
                SELECT o.id AS offer_id, LEFT(r.request_id::text, 8) AS request, o.driver_id, o.status,
                       o.created_at, o.responded_at
                FROM ride_offers o JOIN rides r ON r.id = o.ride_id
                ORDER BY o.id DESC LIMIT 20
            """)
    return {
        "by_status": {s: by_status.get(s, 0) for s in ("pending", "accepted", "rejected", "expired")},
        **overdue,
        "pending": pending,
        "recent": recent,
    }


def _redis():
    r = get_redis()
    patterns = {
        "driver_heartbeat_keys": "driver:heartbeat:*",
        "driver_active_ride_cache_keys": "driver:active_ride:*",
        "driver_location_throttle_keys": "driver:loc_persist:*",
        "stats_keys": "stats:*",
        "legacy_ride_request_keys": "ride_request:*",
    }
    counts = {name: sum(1 for _ in r.scan_iter(match=pattern, count=500)) for name, pattern in patterns.items()}
    memory = r.info("memory")
    server = r.info("server")
    return {
        "db_keys": r.dbsize(),
        "drivers_geo_members": r.zcard(Config.REDIS_DRIVER_GEO_KEY),
        **counts,
        "used_memory_human": memory.get("used_memory_human"),
        "redis_version": server.get("redis_version"),
    }


def _postgres():
    tables = ("users", "drivers", "restaurants", "menu_items", "rides", "ride_offers", "food_orders")
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            counts = {}
            for table in tables:  # fixed whitelist, not user input
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                counts[table] = cur.fetchone()[0]
            cur.execute("SHOW server_version")
            version = cur.fetchone()[0]
    return {
        "table_rows": counts,
        "server_version": version,
        "pool": {"min": Config.DB_POOL_MIN_CONN, "max": Config.DB_POOL_MAX_CONN},
    }


def _payments():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            totals = _one(cur, """
                SELECT COUNT(*) FILTER (WHERE paid_at IS NOT NULL) AS confirmations,
                       COALESCE(SUM(paid_amount), 0)::float AS total_confirmed_amount,
                       COUNT(*) FILTER (WHERE status = 'completed' AND paid_at IS NULL
                                        AND request_id IS NOT NULL) AS completed_awaiting_confirmation,
                       COUNT(*) FILTER (WHERE paid_at IS NOT NULL AND paid_amount <> fare) AS paid_amount_differs_from_fare
                FROM rides
            """)
            recent = _rows(cur, """
                SELECT LEFT(request_id::text, 8) AS request, driver_id, paid_amount::float AS amount, paid_at
                FROM rides WHERE paid_at IS NOT NULL ORDER BY paid_at DESC LIMIT 10
            """)
    counters = ops_stats.read_counters()
    return {
        "notice": "Payment confirmation only: drivers confirm they collected the fare outside the system. "
                  "No payment gateway is integrated and no money is processed.",
        **totals,
        "refused_duplicate_confirmations": counters.get("payment_duplicate_attempts", 0),
        "refused_amount_mismatch": counters.get("payment_amount_mismatch", 0),
        "refused_not_eligible": counters.get("payment_not_eligible", 0),
        "recent_confirmations": recent,
    }


@admin_bp.route("/overview", methods=["GET"])
@require_admin
def overview():
    return jsonify({
        "success": True,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "system": _section(_system),
        "users": _section(_users),
        "matching": _section(_matching),
        "rides": _section(_rides),
        "offers": _section(_offers),
        "redis": _section(_redis),
        "postgres": _section(_postgres),
        "payments": _section(_payments),
    })

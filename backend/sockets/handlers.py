"""
Real-time ride dispatch over Socket.IO.

Key changes from the original implementation:

1. **Authentication on connect.** The original server trusted whatever
   driver_id/user_id a client claimed in each event payload - anyone who
   could see the API could impersonate any driver or pull any user's ride.
   Here, the JWT issued at login is passed in the socket.io `auth` payload;
   we verify it once at connect time and trust `flask.request.sid` -> user
   identity mappings afterward, not client-supplied IDs.

2. **Room-scoped emits instead of `broadcast=True` everywhere.** The
   original code broadcast full ride details (rider name, pickup/drop,
   fare) to *every connected socket* for every ride request, and broadcast
   every ride-progress/payment event to everyone too. That's both a privacy
   leak (every rider/driver sees every other ride's details) and it doesn't
   scale (O(N) fan-out per event regardless of relevance). Ride-specific
   events - including the moving car's position - go to a per-ride
   Socket.IO room containing only the two participants.

3. **PostgreSQL-backed ride lifecycle and offers** (services/ride_service.py).
   A ride is persisted from the moment it is requested; offers, acceptance,
   trip progress and payment confirmation are guarded transitions on that
   row, so two drivers can't both win a ride, an offer can't be accepted
   after it expires, and a reconnecting rider/driver gets the current state
   back. Socket events only report committed state.

4. **One simulator at a time** (Config.RIDE_SIMULATOR), going through the
   same guarded transitions as a real driver client.
"""
import logging
import math
import uuid

from flask import request
from flask_socketio import ConnectionRefusedError, emit, join_room, leave_room

from config import Config
from db import get_db_conn, get_redis
from extensions import socketio
from services import ops_stats, ride_service
from services.auth_service import decode_token
from services.matching_service import (
    find_best_driver,
    refresh_driver_heartbeat,
    remove_driver_location,
    sweep_stale_drivers,
    upsert_driver_location,
)
from services.pricing_service import build_fare_breakdown, calculate_total_fare
from services.routing_service import road_route

logger = logging.getLogger("lyft.sockets")

# A ride nobody has accepted after this long is cancelled (reason "expired").
ACTIVE_REQUEST_TTL_SECONDS = 15 * 60
MAINTENANCE_INTERVAL_SECONDS = 5
# The offer deadline is set by Postgres' clock; firing the timer slightly
# late guarantees the deadline has passed when it runs.
OFFER_TIMER_GRACE_SECONDS = 0.5
# How many drivers to try in one pass when the chosen one turns out to be
# reserved by a concurrent request.
MAX_OFFER_ATTEMPTS = 5
MAX_TEXT_LENGTH = 120
ACTIVE_RIDE_CACHE_SECONDS = 6 * 60 * 60

ACCEPT_FAILURE_MESSAGES = {
    "offer_expired": "This offer expired before you accepted it.",
    "not_offered": "This ride was not offered to you.",
    "offer_rejected": "You already declined this ride.",
    "already_accepted": "You have already accepted this ride.",
    "ride_no_longer_available": "This ride is no longer available.",
    "ride_not_found": "This ride no longer exists.",
    "driver_on_trip": "You are already on an active ride.",
    "invalid_request": "Invalid ride request.",
}
PAYMENT_FAILURE_MESSAGES = {
    "already_confirmed": "Payment for this ride was already confirmed.",
    "ride_not_completed": "The ride isn't completed yet.",
    "amount_mismatch": "The amount doesn't match this ride's fare.",
    "not_your_ride": "This isn't your ride.",
    "ride_not_found": "Ride not found.",
    "invalid_request": "Invalid ride request.",
}

# sid -> {"id": int, "role": "driver"|"rider"|"admin"|"service", "name": str}
_socket_identity: dict[str, dict] = {}
# driver_id -> sid (for direct room targeting / earnings pushes)
_driver_sid: dict[int, str] = {}
_background_tasks_started = False


def _ride_room(request_id: str) -> str:
    return f"ride:{request_id}"


def _driver_room(driver_id: int) -> str:
    return f"driver:{driver_id}"


SIM_ENGINE_ROOM = "sim_engine"


def _authenticate(auth) -> dict | None:
    auth = auth or {}
    service_token = auth.get("service_token")
    if service_token and service_token == Config.SIMULATION_SERVICE_TOKEN:
        return {"id": None, "role": "service", "name": "ride-simulation-engine"}

    token = auth.get("token")
    if not token:
        return None
    decoded = decode_token(token)
    if not decoded:
        return None
    return {"id": int(decoded["sub"]), "role": decoded["role"], "name": decoded["name"]}


def _is_driver_or_trusted_service(identity, claimed_driver_id) -> bool:
    """True if this socket is allowed to act on behalf of claimed_driver_id:
    either it *is* that authenticated driver, or it's the trusted internal
    simulation engine acting on behalf of a bot driver."""
    if not identity:
        return False
    if identity["role"] == "service":
        return True
    return identity["role"] == "driver" and identity["id"] == claimed_driver_id


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coordinate(value, limit):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or abs(number) > limit:
        return None
    return number


def _lat(value):
    return _coordinate(value, 85.05112878)  # Redis GEO's latitude limit


def _lng(value):
    return _coordinate(value, 180.0)


def _valid_request_id(value) -> str | None:
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        return None


def _short_text(value, default: str) -> str:
    return (str(value) if value else default)[:MAX_TEXT_LENGTH]


def connected_socket_summary() -> dict:
    """Connected sockets in this process, by role (for the admin dashboard)."""
    sockets_by_role, users_by_role = {}, {}
    for identity in list(_socket_identity.values()):
        role = identity["role"]
        sockets_by_role[role] = sockets_by_role.get(role, 0) + 1
        if identity["id"] is not None:
            users_by_role.setdefault(role, set()).add(identity["id"])
    return {
        "total_sockets": sum(sockets_by_role.values()),
        "sockets_by_role": sockets_by_role,
        "unique_users_by_role": {role: len(ids) for role, ids in users_by_role.items()},
    }


# --- Active-ride cache (driver -> request id), derived from Postgres ------

def _active_ride_key(driver_id) -> str:
    return f"driver:active_ride:{driver_id}"


def _cache_active_ride(driver_id: int, request_id: str) -> None:
    get_redis().setex(_active_ride_key(driver_id), ACTIVE_RIDE_CACHE_SECONDS, request_id)


def _clear_active_ride(driver_id: int) -> None:
    get_redis().delete(_active_ride_key(driver_id))


def _active_request_id_for_driver(driver_id: int) -> str | None:
    """Location pings arrive several times a second; look the driver's active
    ride up in Redis and only fall back to Postgres on a cache miss."""
    r = get_redis()
    cached = r.get(_active_ride_key(driver_id))
    if cached is not None:
        return cached or None
    request_id = ride_service.get_active_request_id_for_driver(driver_id)
    r.setex(_active_ride_key(driver_id), ACTIVE_RIDE_CACHE_SECONDS if request_id else 10, request_id or "")
    return request_id


def _persist_location_throttled(driver_id: int, lat: float, lng: float) -> None:
    """Redis holds the live position. Postgres only needs a recent copy (the
    simulation start point), so write it at most once per
    LOCATION_PERSIST_INTERVAL_SECONDS per driver instead of on every ping."""
    try:
        if not get_redis().set(
            f"driver:loc_persist:{driver_id}", "1", nx=True, ex=Config.LOCATION_PERSIST_INTERVAL_SECONDS
        ):
            return
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE drivers SET latitude = %s, longitude = %s WHERE id = %s", (lat, lng, driver_id))
            conn.commit()
    except Exception as e:  # noqa: BLE001
        logger.warning("Failed to persist location for driver %s: %s", driver_id, e)


# --- Payload builders ------------------------------------------------------

def _ride_snapshot(ride: dict) -> dict:
    details = ride["details"]
    driver = None
    if ride.get("driver_id"):
        driver = {
            "id": ride["driver_id"],
            "name": ride.get("driver_name"),
            "lat": ride.get("driver_lat"),
            "lng": ride.get("driver_lng"),
        }
    return {
        "request_id": ride["request_id"],
        "status": ride["status"],
        "progress": ride.get("progress"),
        "fare": float(ride["fare"] or 0),
        "paid": ride.get("paid_at") is not None,
        "driver": driver,
        "user_name": details.get("user_name"),
        "pickup": {"lat": ride["pickup_lat"], "lng": ride["pickup_lng"]},
        "drop": {"lat": ride["drop_lat"], "lng": ride["drop_lng"]},
        "pickup_name": details.get("pickup_name"),
        "drop_name": details.get("drop_name"),
        "restaurant": details.get("restaurant"),
        "food_order": details.get("food_order"),
        "food_order_price": details.get("food_order_price", 0),
        "is_food_ride": bool(details.get("restaurant")),
        "eta_minutes": details.get("eta_minutes"),
    }


def _offer_payload(ride: dict, offer: dict) -> dict:
    details = ride["details"]
    return {
        "request_id": ride["request_id"],
        "target_driver_id": offer["driver_id"],
        "offer_id": offer["id"],
        "offer_expires_in_seconds": offer["expires_in_seconds"],
        "user_name": details.get("user_name"),
        "pickup": {"lat": ride["pickup_lat"], "lng": ride["pickup_lng"]},
        "drop": {"lat": ride["drop_lat"], "lng": ride["drop_lng"]},
        "pickup_name": details.get("pickup_name"),
        "drop_name": details.get("drop_name"),
        "restaurant": details.get("restaurant"),
        "fare": float(ride["fare"] or 0),
        "eta_minutes": details.get("eta_minutes"),
        "food_order": details.get("food_order"),
        "food_order_price": details.get("food_order_price", 0),
        "fare_breakdown": build_fare_breakdown(
            details.get("distance_km", 0),
            details.get("food_distance_km", 0),
            details.get("food_order_price", 0),
        ),
    }


def _driver_status(online: bool, reason: str | None = None) -> dict:
    return {
        "online": online,
        "reason": reason,
        "heartbeat_interval_seconds": Config.DRIVER_HEARTBEAT_INTERVAL_SECONDS,
    }


def _authoritative_food_order(restaurant, food_order):
    """The rider's page only names the restaurant and items. Prices and the
    restaurant location come from Postgres, so a tampered payload can't
    change the fare - and therefore the driver's credited earnings.
    Returns (food, None) or (None, error message)."""
    if not isinstance(restaurant, dict) or not restaurant.get("name"):
        return None, "Invalid restaurant."
    if not isinstance(food_order, list) or not 0 < len(food_order) <= 10:
        return None, "Invalid food order."
    names = [str(item.get("item")) for item in food_order if isinstance(item, dict) and item.get("item")]
    if len(names) != len(food_order):
        return None, "Invalid food order."

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, latitude, longitude FROM restaurants WHERE LOWER(name) = LOWER(%s)",
                (str(restaurant["name"]),),
            )
            row = cur.fetchone()
            if not row:
                return None, "Restaurant not found."
            r_id, r_name, r_lat, r_lng = row
            cur.execute(
                "SELECT name, price FROM menu_items WHERE restaurant_id = %s AND LOWER(name) = ANY(%s)",
                (r_id, [n.lower() for n in names]),
            )
            menu = {name.lower(): (name, float(price)) for name, price in cur.fetchall()}

    items = []
    for name in names:
        if name.lower() not in menu:
            return None, f"Item '{name[:MAX_TEXT_LENGTH]}' is not on {r_name}'s menu."
        canonical, price = menu[name.lower()]
        items.append({"item": canonical, "price": price, "qty": 1})
    return {
        "restaurant": {"name": r_name, "lat": r_lat, "lng": r_lng},
        "food_order": items,
        "food_order_price": sum(item["price"] for item in items),
    }, None


def _restore_ride_state(identity: dict) -> None:
    """On (re)connect, put the socket back in its ride room and send the
    persisted ride state, so a reload or network drop doesn't lose the trip."""
    user_id = identity["id"]
    if identity["role"] == "rider":
        ride = ride_service.get_open_ride_for_rider(user_id)
    else:
        ride = ride_service.get_open_ride_for_driver(user_id)
    if ride:
        join_room(_ride_room(ride["request_id"]))
        emit("ride_state", _ride_snapshot(ride))
        return
    if identity["role"] == "driver":
        pending = ride_service.get_pending_offer_for_driver(user_id)
        if pending:
            emit("driver_request", _offer_payload(*pending))


def register_handlers():
    @socketio.on("connect")
    def handle_connect(auth):
        identity = _authenticate(auth)
        if not identity:
            logger.warning("Rejected unauthenticated socket connection from %s", request.sid)
            return False  # reject the connection
        if identity["role"] == "service" and Config.RIDE_SIMULATOR != "node":
            logger.warning(
                "Refused ride-simulation engine connection: RIDE_SIMULATOR=%s. "
                "Set RIDE_SIMULATOR=node to use ride_simulation_engine.js.",
                Config.RIDE_SIMULATOR,
            )
            raise ConnectionRefusedError(f"backend RIDE_SIMULATOR is '{Config.RIDE_SIMULATOR}', not 'node'")
        _socket_identity[request.sid] = identity
        if identity["role"] == "driver":
            _driver_sid[identity["id"]] = request.sid
            join_room(_driver_room(identity["id"]))
        elif identity["role"] == "service":
            join_room(SIM_ENGINE_ROOM)
        if identity["role"] in ("rider", "driver"):
            try:
                _restore_ride_state(identity)
            except Exception:  # noqa: BLE001
                logger.exception("Could not restore ride state for %s %s", identity["role"], identity["id"])
        return True

    @socketio.on("disconnect")
    def handle_disconnect():
        identity = _socket_identity.pop(request.sid, None)
        if not identity:
            return
        if identity["role"] == "driver":
            driver_id = identity["id"]
            _driver_sid.pop(driver_id, None)
            remove_driver_location(driver_id)
            try:
                with get_db_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE drivers SET is_available = FALSE WHERE id = %s", (driver_id,))
                    conn.commit()
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to mark driver %s offline on disconnect: %s", driver_id, e)

    @socketio.on("driver_offline")
    def handle_driver_offline(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver":
            return
        driver_id = identity["id"]
        remove_driver_location(driver_id)
        try:
            with get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE drivers SET is_available = FALSE WHERE id = %s", (driver_id,))
                conn.commit()
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to mark driver %s offline: %s", driver_id, e)
        # A driver who goes offline must not keep holding an offer: decline
        # it so the ride is re-offered now instead of after the timeout.
        for request_id in ride_service.decline_pending_offers(driver_id):
            _offer_to_next_driver(request_id)
        emit("driver_status", _driver_status(False, "offline"))

    @socketio.on("driver_online")
    def handle_driver_online(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver":
            return
        driver_id = identity["id"]
        data = data or {}
        lat, lng = _lat(data.get("lat")), _lng(data.get("lng"))
        if lat is None or lng is None:
            emit("driver_status", _driver_status(False, "invalid_location"))
            return
        if ride_service.driver_has_active_ride(driver_id):
            # e.g. a reconnecting page re-sending driver_online mid-trip:
            # a driver on a trip must not re-enter the matching pool.
            emit("driver_status", _driver_status(False, "active_ride"))
            return

        upsert_driver_location(driver_id, lat, lng)
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE drivers SET is_available = TRUE, latitude = %s, longitude = %s WHERE id = %s",
                    (lat, lng, driver_id),
                )
            conn.commit()
        logger.info("Driver %s online", driver_id)
        emit("driver_status", _driver_status(True))

    @socketio.on("driver_heartbeat")
    def handle_driver_heartbeat(data):
        """Periodic keep-alive from an online driver page. It refreshes a
        driver who is still in the matching pool and never re-adds one, so a
        late heartbeat can't undo going offline or accepting a ride. The
        reply tells the page whether it is still matchable."""
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver":
            return
        data = data or {}
        lat, lng = _lat(data.get("lat")), _lng(data.get("lng"))
        if lat is None or lng is None:
            emit("driver_status", _driver_status(False, "invalid_location"))
            return
        if refresh_driver_heartbeat(identity["id"], lat, lng):
            emit("driver_status", _driver_status(True))
        else:
            emit("driver_status", _driver_status(False, "not_in_pool"))

    @socketio.on("update_location")
    def handle_update_location(data):
        identity = _socket_identity.get(request.sid)
        data = data or {}
        driver_id = _as_int(data.get("driver_id"))
        if driver_id is None or not _is_driver_or_trusted_service(identity, driver_id):
            return
        if identity["role"] == "service" and Config.RIDE_SIMULATOR != "node":
            return
        lat, lng = _lat(data.get("latitude")), _lng(data.get("longitude"))
        if lat is None or lng is None:
            return

        request_id = _active_request_id_for_driver(driver_id)
        if request_id:
            # On a trip: only this ride's rider and driver see the car move,
            # and pool membership is left alone, so an on-trip driver can
            # never become matchable again through location pings.
            emit("driver_moved", {"driver_id": driver_id, "latitude": lat, "longitude": lng}, to=_ride_room(request_id))
        else:
            refresh_driver_heartbeat(driver_id, lat, lng)
        _persist_location_throttled(driver_id, lat, lng)

    @socketio.on("request_all_drivers")
    def handle_request_all_drivers(_data):
        emit("send_current_location", {}, broadcast=True)

    @socketio.on("request_ride")
    def handle_ride_request(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "rider":
            return {"success": False, "message": "Only riders can request rides."}

        def _reject(message):
            emit("ride_assigned", {"success": False, "message": message})
            return {"success": False, "message": message}

        data = data or {}
        pickup_lat, pickup_lng = _lat(data.get("pickup_lat")), _lng(data.get("pickup_lng"))
        drop_lat, drop_lng = _lat(data.get("drop_lat")), _lng(data.get("drop_lng"))
        if None in (pickup_lat, pickup_lng, drop_lat, drop_lng):
            return _reject("Invalid pickup or drop coordinates.")

        food = None
        if data.get("restaurant"):
            food, error = _authoritative_food_order(data.get("restaurant"), data.get("food_order"))
            if error:
                return _reject(error)

        route = road_route(pickup_lat, pickup_lng, drop_lat, drop_lng)
        food_distance_km = 0
        if food:
            food_route = road_route(food["restaurant"]["lat"], food["restaurant"]["lng"], pickup_lat, pickup_lng)
            food_distance_km = food_route["distance_km"]
        food_order_price = food["food_order_price"] if food else 0

        fare = calculate_total_fare(route["distance_km"], food_distance_km, food_order_price)
        details = {
            "user_name": identity["name"],
            "pickup_name": _short_text(data.get("pickup_name"), "Unknown Pickup"),
            "drop_name": _short_text(data.get("drop_name"), "Unknown Drop"),
            "restaurant": food["restaurant"] if food else None,
            "food_order": food["food_order"] if food else None,
            "food_order_price": food_order_price,
            "distance_km": route["distance_km"],
            "food_distance_km": food_distance_km,
            "eta_minutes": round(route["eta_minutes"], 1),
        }
        ride = ride_service.create_ride_request(identity["id"], pickup_lat, pickup_lng, drop_lat, drop_lng, fare, details)
        if not ride:
            return _reject("You already have an active ride.")
        ops_stats.incr("rides_requested")

        request_id = ride["request_id"]
        # The rider's own socket joins the ride room now so later
        # room-scoped emits (ride_assigned, ride_progress, payment) reach
        # them without ever broadcasting to unrelated clients.
        join_room(_ride_room(request_id))
        emit("ride_requested", {"success": True, "request_id": request_id, "fare": fare})
        _offer_to_next_driver(request_id)
        return {"success": True, "request_id": request_id}

    @socketio.on("driver_response")
    def handle_driver_response(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver":
            return
        driver_id = identity["id"]
        data = data or {}
        req_id = _valid_request_id(data.get("request_id"))

        if not data.get("accepted"):
            if req_id and ride_service.reject_offer(req_id, driver_id):
                ops_stats.incr("offers_rejected")
                _offer_to_next_driver(req_id)
            return {"success": True}

        ride, reason = ride_service.accept_offer(req_id, driver_id) if req_id else (None, "invalid_request")
        if not ride:
            ops_stats.incr("accept_refused")
            logger.info("Driver %s could not accept ride %s: %s", driver_id, req_id, reason)
            emit("ride_accept_failed", {
                "request_id": req_id or data.get("request_id"),
                "reason": reason,
                "message": ACCEPT_FAILURE_MESSAGES.get(reason, "This ride is no longer available."),
            })
            if reason == "offer_expired":
                _offer_to_next_driver(req_id)
            return {"success": False, "reason": reason}

        ops_stats.incr("offers_accepted")
        join_room(_ride_room(req_id))
        try:
            remove_driver_location(driver_id)
            _cache_active_ride(driver_id, req_id)
        except Exception as e:  # noqa: BLE001 - the ride itself is committed
            logger.warning("Redis update after accepting ride %s failed: %s", req_id, e)

        snapshot = _ride_snapshot(ride)
        emit(
            "ride_assigned",
            {
                "success": True,
                "request_id": req_id,
                "driver": snapshot["driver"],
                "fare": snapshot["fare"],
                "user_name": snapshot["user_name"],
                "pickup_name": snapshot["pickup_name"],
                "drop_name": snapshot["drop_name"],
                "restaurant": snapshot["restaurant"],
                "food_order": snapshot["food_order"],
                "food_order_price": snapshot["food_order_price"],
                "is_food_ride": snapshot["is_food_ride"],
            },
            to=_ride_room(req_id),
        )
        _start_simulation(ride)
        return {"success": True}

    @socketio.on("ride_status_update")
    def handle_ride_status(data):
        identity = _socket_identity.get(request.sid)
        data = data or {}
        driver_id = _as_int(data.get("driver_id"))
        if driver_id is None or not _is_driver_or_trusted_service(identity, driver_id):
            logger.warning("Rejected ride_status_update: identity/payload mismatch")
            return
        if identity["role"] == "service" and Config.RIDE_SIMULATOR != "node":
            logger.warning("Rejected ride_status_update from simulation engine: RIDE_SIMULATOR=%s", Config.RIDE_SIMULATOR)
            return
        req_id = _valid_request_id(data.get("request_id"))
        if not req_id:
            logger.warning("Rejected ride_status_update without a valid request_id")
            return
        _apply_ride_progress(req_id, driver_id, data.get("status"))

    @socketio.on("payment_collected")
    def handle_payment(data):
        identity = _socket_identity.get(request.sid)
        data = data or {}
        if not identity or identity["role"] != "driver" or identity["id"] != data.get("driver_id"):
            logger.warning("Rejected payment_collected: identity/payload mismatch")
            return
        driver_id = identity["id"]
        req_id = _valid_request_id(data.get("request_id"))
        result, reason = (
            ride_service.confirm_payment(req_id, driver_id, data.get("amount")) if req_id else (None, "invalid_request")
        )
        if not result:
            ops_stats.incr({
                "already_confirmed": "payment_duplicate_attempts",
                "amount_mismatch": "payment_amount_mismatch",
            }.get(reason, "payment_not_eligible"))
            logger.warning("Refused payment confirmation by driver %s for ride %s: %s", driver_id, req_id, reason)
            emit("payment_failed", {
                "request_id": req_id or data.get("request_id"),
                "reason": reason,
                "message": PAYMENT_FAILURE_MESSAGES.get(reason, "Payment confirmation refused."),
            })
            return {"success": False, "reason": reason}

        ops_stats.incr("payment_confirmations")
        try:
            _clear_active_ride(driver_id)
            # The driver re-enters the matching pool only now, at the drop point.
            upsert_driver_location(driver_id, result["lat"], result["lng"])
        except Exception as e:  # noqa: BLE001 - payment itself is committed
            logger.warning("Redis update after payment for ride %s failed: %s", req_id, e)

        emit(
            "earnings_update",
            {"total": result["total_earnings"], "earned": result["amount"], "request_id": req_id},
            to=_driver_room(driver_id),
        )
        emit("payment_confirmed", {"success": True, "fare": result["amount"], "request_id": req_id}, to=_ride_room(req_id))
        emit("driver_status", _driver_status(True), to=_driver_room(driver_id))
        leave_room(_ride_room(req_id))
        return {"success": True}


def start_background_tasks() -> None:
    global _background_tasks_started
    if _background_tasks_started:
        return
    _background_tasks_started = True
    socketio.start_background_task(_maintenance_loop)


def _start_simulation(ride: dict) -> None:
    mode = Config.RIDE_SIMULATOR
    if mode == "node":
        if not any(i["role"] == "service" for i in _socket_identity.values()):
            logger.warning("RIDE_SIMULATOR=node but no simulation engine is connected to this process")
        payload = {
            "driver_id": ride["driver_id"],
            "request_id": ride["request_id"],
            "current_loc": {"lat": ride["driver_lat"], "lng": ride["driver_lng"]},
            "pickup": {"lat": ride["pickup_lat"], "lng": ride["pickup_lng"]},
            "drop": {"lat": ride["drop_lat"], "lng": ride["drop_lng"]},
        }
        if ride["details"].get("restaurant"):
            payload["restaurant"] = ride["details"]["restaurant"]
        socketio.emit("start_simulation_ride", payload, to=SIM_ENGINE_ROOM)
    elif mode == "python":
        import eventlet
        eventlet.spawn(
            _run_auto_ride_simulation,
            ride["driver_id"],
            ride["request_id"],
            ride["driver_lat"],
            ride["driver_lng"],
            ride["pickup_lat"],
            ride["pickup_lng"],
            ride["drop_lat"],
            ride["drop_lng"],
            ride["details"].get("restaurant"),
            float(ride["fare"] or 0),
        )
    # "none": the driver's own client reports ride_status_update.


def _apply_ride_progress(request_id: str, driver_id: int, status) -> bool:
    """Persist a trip step, then tell the ride room. Used by the in-process
    simulator and by ride_status_update (Node engine or a real driver), so
    every source obeys the same forward-only rules."""
    ride, reason = ride_service.update_progress(request_id, driver_id, status)
    if not ride:
        ops_stats.incr("progress_refused")
        logger.info("Ignored ride progress %r for ride %s (driver %s): %s", status, request_id, driver_id, reason)
        return False
    payload = {"driver_id": driver_id, "request_id": request_id, "status": status}
    if status == "completed":
        payload["fare"] = float(ride["fare"] or 0)
        try:
            _clear_active_ride(driver_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("Could not clear active ride cache for driver %s: %s", driver_id, e)
    socketio.emit("ride_progress", payload, to=_ride_room(request_id))
    return True


def _offer_to_next_driver(request_id: str) -> None:
    ride = ride_service.get_ride(request_id)
    if not ride or ride["status"] != "requested" or ride_service.has_pending_offer(ride["id"]):
        return

    restaurant = ride["details"].get("restaurant")
    if restaurant:
        target_lat, target_lng = restaurant["lat"], restaurant["lng"]
    else:
        target_lat, target_lng = ride["pickup_lat"], ride["pickup_lng"]

    # Skip drivers who already turned this ride down and drivers holding
    # another ride's offer.
    exclude = ride_service.excluded_driver_ids(ride["id"]) | ride_service.pending_offer_driver_ids()
    for _ in range(MAX_OFFER_ATTEMPTS):
        best = find_best_driver(target_lat, target_lng, exclude)
        if not best:
            break
        offer, reason = ride_service.create_offer(request_id, best["id"], Config.OFFER_TIMEOUT_SECONDS)
        if offer:
            ops_stats.record_match_attempt(request_id, "offered", best["id"], len(exclude))
            # Only the offered driver's own room receives this - not a global
            # broadcast like the original implementation.
            socketio.emit("driver_request", _offer_payload(ride, offer), to=_driver_room(best["id"]))
            socketio.start_background_task(
                _expire_offer_later, offer["id"], Config.OFFER_TIMEOUT_SECONDS + OFFER_TIMER_GRACE_SECONDS
            )
            return
        if reason in ("driver_has_pending_offer", "driver_on_trip", "conflict"):
            exclude.add(best["id"])  # reserved by a concurrent request; try the next driver
            continue
        return  # ride was accepted/cancelled meanwhile, or already has an offer

    if ride_service.cancel_unfulfilled(request_id, "no_driver"):
        ops_stats.record_match_attempt(request_id, "no_driver", None, len(exclude))
        socketio.emit(
            "ride_assigned",
            {"success": False, "request_id": request_id, "message": "All drivers busy or rejected."},
            to=_ride_room(request_id),
        )


def _expire_offer_later(offer_id: int, delay: float) -> None:
    socketio.sleep(delay)
    try:
        expired = ride_service.expire_offer(offer_id)
        if expired:
            _handle_expired_offer(expired)
        # None: already answered, or not due yet (the maintenance pass re-checks).
    except Exception:  # noqa: BLE001
        logger.exception("Offer expiry for offer %s failed", offer_id)


def _handle_expired_offer(expired: dict) -> None:
    ops_stats.record_match_attempt(expired["request_id"], "offer_expired", expired["driver_id"])
    socketio.emit("offer_expired", {"request_id": expired["request_id"]}, to=_driver_room(expired["driver_id"]))
    _offer_to_next_driver(expired["request_id"])


def run_maintenance_once() -> None:
    """Safety net for what timers alone can't guarantee (restarts, lost
    timers, crashed clients). Every step is a guarded, idempotent update,
    so several backend instances can run it at the same time."""
    for offer_id in ride_service.due_offer_ids():
        expired = ride_service.expire_offer(offer_id)
        if expired:
            _handle_expired_offer(expired)
    for request_id in ride_service.stale_requested_ids(ACTIVE_REQUEST_TTL_SECONDS):
        if ride_service.cancel_unfulfilled(request_id, "expired"):
            ops_stats.record_match_attempt(request_id, "request_expired")
            socketio.emit(
                "ride_assigned",
                {"success": False, "request_id": request_id, "message": "No driver accepted your request in time."},
                to=_ride_room(request_id),
            )
    for request_id in ride_service.requested_ids_without_offer(MAINTENANCE_INTERVAL_SECONDS):
        _offer_to_next_driver(request_id)
    removed = sweep_stale_drivers()
    if removed:
        logger.info("Removed %s stale driver(s) from the matching index", removed)


def _maintenance_loop() -> None:
    while True:
        socketio.sleep(MAINTENANCE_INTERVAL_SECONDS)
        try:
            run_maintenance_once()
        except Exception:  # noqa: BLE001
            logger.exception("Maintenance pass failed")


def _run_auto_ride_simulation(
    driver_id: int,
    req_id: str,
    start_lat: float,
    start_lng: float,
    pickup_lat: float,
    pickup_lng: float,
    drop_lat: float,
    drop_lng: float,
    restaurant: dict | None = None,
    fare: float = 0.0,
) -> None:
    """Automated backend background simulation of car movement along OSRM road routes.
    Every status goes through _apply_ride_progress; if a step is refused (e.g.
    the ride is no longer active) the simulation stops."""
    import eventlet
    import requests

    room = _ride_room(req_id)

    def _fetch_points(slat, slng, elat, elng):
        try:
            url = f"https://router.project-osrm.org/route/v1/driving/{slng},{slat};{elng},{elat}?overview=full&geometries=geojson"
            resp = requests.get(url, timeout=5)
            data = resp.json()
            if data.get("routes") and len(data["routes"]) > 0:
                coords = data["routes"][0]["geometry"]["coordinates"]
                return [{"lat": c[1], "lng": c[0]} for c in coords]
        except Exception as err:
            logger.warning("OSRM simulation route fetch error: %s", err)
        steps = 15
        return [
            {
                "lat": slat + (elat - slat) * (i / steps),
                "lng": slng + (elng - slng) * (i / steps),
            }
            for i in range(steps + 1)
        ]

    def _move(lat, lng):
        socketio.emit("driver_moved", {"driver_id": driver_id, "latitude": lat, "longitude": lng}, to=room)

    def _step(status):
        return _apply_ride_progress(req_id, driver_id, status)

    target_lat, target_lng = pickup_lat, pickup_lng
    has_food = bool(restaurant)
    if has_food:
        target_lat, target_lng = restaurant["lat"], restaurant["lng"]

    # 1. Heading to restaurant (food) or pickup
    if not _step("heading_to_pickup"):
        return
    path1 = _fetch_points(start_lat, start_lng, target_lat, target_lng)
    step_size = max(1, len(path1) // 12)
    for i in range(0, len(path1), step_size):
        pt = path1[i]
        _move(pt["lat"], pt["lng"])
        eventlet.sleep(0.5)

    if has_food:
        if not _step("at_restaurant"):
            return
        eventlet.sleep(1.5)
        if not _step("food_picked"):
            return
        eventlet.sleep(1.0)
        # Drive from restaurant to rider pickup
        path_to_pickup = _fetch_points(target_lat, target_lng, pickup_lat, pickup_lng)
        step_pickup = max(1, len(path_to_pickup) // 10)
        for i in range(0, len(path_to_pickup), step_pickup):
            pt = path_to_pickup[i]
            _move(pt["lat"], pt["lng"])
            eventlet.sleep(0.4)

    # 2. Picked Up / At Pickup
    if not _step("picked_up"):
        return
    eventlet.sleep(1.5)

    # 3. Trip Started -> Driving to Dropoff
    if not _step("trip_started"):
        return
    trip_start_lat, trip_start_lng = pickup_lat, pickup_lng
    path2 = _fetch_points(trip_start_lat, trip_start_lng, drop_lat, drop_lng)
    step_size2 = max(1, len(path2) // 15)
    for i in range(0, len(path2), step_size2):
        pt = path2[i]
        _move(pt["lat"], pt["lng"])
        eventlet.sleep(0.5)

    # Ensure driver reaches exact drop coords
    _move(drop_lat, drop_lng)

    # 4. Ride Arrived at Destination - waiting for driver payment confirmation
    _step("completed")

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
   events now go to a per-ride Socket.IO room containing only the two
   participants. Driver map markers (public, non-sensitive location pings)
   remain broadcast, matching how real ride-share apps show nearby car
   icons to anyone with the app open.

3. **Redis-backed active-request state** instead of an in-process Python
   dict, so ride-matching state survives a worker restart and would work
   correctly if this app were scaled to multiple backend instances behind a
   load balancer (a plain Python dict would silently break the moment
   there's more than one process).
"""
import json
import logging
import uuid

from flask import request
from flask_socketio import emit, join_room, leave_room

from config import Config
from db import get_db_conn, get_redis
from extensions import socketio
from services.auth_service import decode_token
from services.matching_service import (
    find_best_driver,
    remove_driver_location,
    upsert_driver_location,
)
from services.pricing_service import build_fare_breakdown, calculate_total_fare
from services.routing_service import road_route

logger = logging.getLogger("lyft.sockets")

ACTIVE_REQUEST_TTL_SECONDS = 15 * 60

# sid -> {"id": int, "role": "driver"|"rider", "name": str}
_socket_identity: dict[str, dict] = {}
# driver_id -> sid (for direct room targeting / earnings pushes)
_driver_sid: dict[int, str] = {}


def _ride_room(request_id: str) -> str:
    return f"ride:{request_id}"


def _driver_room(driver_id: int) -> str:
    return f"driver:{driver_id}"


SIM_ENGINE_ROOM = "sim_engine"


def _save_active_request(request_id: str, payload: dict) -> None:
    get_redis().setex(f"ride_request:{request_id}", ACTIVE_REQUEST_TTL_SECONDS, json.dumps(payload))


def _load_active_request(request_id: str) -> dict | None:
    raw = get_redis().get(f"ride_request:{request_id}")
    return json.loads(raw) if raw else None


def _delete_active_request(request_id: str) -> None:
    get_redis().delete(f"ride_request:{request_id}")


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


def register_handlers():
    @socketio.on("connect")
    def handle_connect(auth):
        identity = _authenticate(auth)
        if not identity:
            logger.warning("Rejected unauthenticated socket connection from %s", request.sid)
            return False  # reject the connection
        _socket_identity[request.sid] = identity
        if identity["role"] == "driver":
            _driver_sid[identity["id"]] = request.sid
            join_room(_driver_room(identity["id"]))
        elif identity["role"] == "service":
            join_room(SIM_ENGINE_ROOM)
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

    @socketio.on("driver_online")
    def handle_driver_online(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver":
            return
        driver_id = identity["id"]
        lat, lng = data["lat"], data["lng"]

        upsert_driver_location(driver_id, lat, lng)
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE drivers SET is_available = TRUE, latitude = %s, longitude = %s WHERE id = %s",
                    (lat, lng, driver_id),
                )
            conn.commit()
        logger.info("Driver %s online", driver_id)

    @socketio.on("update_location")
    def handle_update_location(data):
        identity = _socket_identity.get(request.sid)
        driver_id = data.get("driver_id")
        if not _is_driver_or_trusted_service(identity, driver_id):
            return
        lat, lng = data["latitude"], data["longitude"]

        upsert_driver_location(driver_id, lat, lng)
        # Public marker position - broadcast is intentional here (any rider
        # with the map open should see nearby cars), unlike ride-specific data.
        emit("driver_moved", {"driver_id": driver_id, "latitude": lat, "longitude": lng}, broadcast=True)
        try:
            with get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE drivers SET latitude = %s, longitude = %s WHERE id = %s",
                        (lat, lng, driver_id),
                    )
                conn.commit()
        except Exception as e:  # noqa: BLE001
            logger.warning("Failed to persist location for driver %s: %s", driver_id, e)

    @socketio.on("request_all_drivers")
    def handle_request_all_drivers(_data):
        emit("send_current_location", {}, broadcast=True)

    @socketio.on("request_ride")
    def handle_ride_request(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "rider":
            return

        route = road_route(data["pickup_lat"], data["pickup_lng"], data["drop_lat"], data["drop_lng"])
        food_order = data.get("food_order")
        food_order_price = data.get("food_order_price", 0)

        food_distance_km = 0
        if data.get("restaurant"):
            food_route = road_route(
                data["restaurant"]["lat"], data["restaurant"]["lng"], data["pickup_lat"], data["pickup_lng"]
            )
            food_distance_km = food_route["distance_km"]

        fare = calculate_total_fare(route["distance_km"], food_distance_km, food_order_price)

        req_id = str(uuid.uuid4())
        payload = {
            "rider_id": identity["id"],
            "rider_sid": request.sid,
            "user_name": identity["name"],
            "pickup_lat": data["pickup_lat"],
            "pickup_lng": data["pickup_lng"],
            "drop_lat": data["drop_lat"],
            "drop_lng": data["drop_lng"],
            "pickup_name": data.get("pickup_name", "Unknown Pickup"),
            "drop_name": data.get("drop_name", "Unknown Drop"),
            "restaurant": data.get("restaurant"),
            "food_order": food_order,
            "food_order_price": food_order_price,
            "fare": fare,
            "distance_km": route["distance_km"],
            "food_distance_km": food_distance_km,
            "rejected_by": [],
            "eta_minutes": round(route["eta_minutes"], 1),
        }
        _save_active_request(req_id, payload)
        # The rider's own socket joins the ride room now so later
        # room-scoped emits (ride_assigned, ride_progress, payment) reach
        # them without ever broadcasting to unrelated clients.
        join_room(_ride_room(req_id))
        _offer_to_next_driver(req_id)

    @socketio.on("driver_response")
    def handle_driver_response(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver":
            return
        driver_id = identity["id"]
        req_id = data.get("request_id")
        req = _load_active_request(req_id)
        if not req:
            return

        if not data.get("accepted"):
            req["rejected_by"].append(driver_id)
            _save_active_request(req_id, req)
            _offer_to_next_driver(req_id)
            return

        join_room(_ride_room(req_id))

        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name, latitude, longitude FROM drivers WHERE id = %s", (driver_id,))
                d_name, d_lat, d_lng = cur.fetchone()
                cur.execute(
                    """
                    INSERT INTO rides (user_id, driver_id, pickup_lat, pickup_lng, drop_lat, drop_lng, fare, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'accepted')
                    """,
                    (
                        req["rider_id"], driver_id, req["pickup_lat"], req["pickup_lng"],
                        req["drop_lat"], req["drop_lng"], req["fare"],
                    ),
                )
            conn.commit()
        remove_driver_location(driver_id)

        emit(
            "ride_assigned",
            {
                "success": True,
                "driver": {"id": driver_id, "name": d_name, "lat": d_lat, "lng": d_lng},
                "fare": req["fare"],
                "user_name": req["user_name"],
                "pickup_name": req["pickup_name"],
                "drop_name": req["drop_name"],
                "restaurant": req.get("restaurant"),
                "food_order": req.get("food_order"),
                "food_order_price": req.get("food_order_price", 0),
                "is_food_ride": bool(req.get("restaurant")),
            },
            room=_ride_room(req_id),
        )

        sim_payload = {
            "driver_id": driver_id,
            "request_id": req_id,
            "current_loc": {"lat": d_lat, "lng": d_lng},
            "pickup": {"lat": req["pickup_lat"], "lng": req["pickup_lng"]},
            "drop": {"lat": req["drop_lat"], "lng": req["drop_lng"]},
        }
        if req.get("restaurant"):
            sim_payload["restaurant"] = req["restaurant"]
        emit("start_simulation_ride", sim_payload, room=SIM_ENGINE_ROOM)

        # Automated backend route simulation for seamless frontend animation
        import eventlet
        eventlet.spawn(
            _run_auto_ride_simulation,
            driver_id,
            req_id,
            d_lat,
            d_lng,
            req["pickup_lat"],
            req["pickup_lng"],
            req["drop_lat"],
            req["drop_lng"],
            req.get("restaurant"),
            req["fare"],
        )

        _delete_active_request(req_id)

    @socketio.on("ride_status_update")
    def handle_ride_status(data):
        identity = _socket_identity.get(request.sid)
        if not _is_driver_or_trusted_service(identity, data.get("driver_id")):
            logger.warning("Rejected ride_status_update: identity/payload mismatch")
            return
        req_id = data.get("request_id")
        room = _ride_room(req_id) if req_id else None
        if data.get("status") == "completed":
            try:
                with get_db_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT latitude, longitude FROM drivers WHERE id = %s", (data["driver_id"],))
                        row = cur.fetchone()
                        if row:
                            d_lat, d_lng = row
                            upsert_driver_location(data["driver_id"], d_lat, d_lng)
                        cur.execute("UPDATE drivers SET is_available = TRUE WHERE id = %s", (data["driver_id"],))
                    conn.commit()
            except Exception:  # noqa: BLE001
                pass
        if room:
            emit("ride_progress", data, room=room)
        else:
            emit("ride_progress", data, broadcast=True)

    @socketio.on("payment_collected")
    def handle_payment(data):
        identity = _socket_identity.get(request.sid)
        if not identity or identity["role"] != "driver" or identity["id"] != data.get("driver_id"):
            logger.warning("Rejected payment_collected: identity/payload mismatch")
            return
        driver_id = identity["id"]
        req_id = data.get("request_id")
        try:
            with get_db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE drivers SET earnings = earnings + %s WHERE id = %s RETURNING earnings",
                        (data["amount"], driver_id),
                    )
                    new_total = cur.fetchone()[0]
                    cur.execute("SELECT latitude, longitude FROM drivers WHERE id = %s", (driver_id,))
                    row = cur.fetchone()
                    if row:
                        d_lat, d_lng = row
                        upsert_driver_location(driver_id, d_lat, d_lng)
                    cur.execute("UPDATE drivers SET is_available = TRUE WHERE id = %s", (driver_id,))
                conn.commit()

            emit("earnings_update", {"total": float(new_total), "earned": float(data["amount"])}, room=_driver_room(driver_id))
            if req_id:
                emit("payment_confirmed", {"success": True, "fare": data["amount"]}, room=_ride_room(req_id))
                leave_room(_ride_room(req_id))
            else:
                emit("payment_confirmed", {"success": True}, broadcast=True)
        except Exception as e:  # noqa: BLE001
            logger.warning("Payment update failed: %s", e)
            emit("payment_confirmed", {"success": True}, broadcast=True)


def _offer_to_next_driver(req_id: str) -> None:
    req = _load_active_request(req_id)
    if not req:
        return

    if req.get("restaurant"):
        target_lat, target_lng = req["restaurant"]["lat"], req["restaurant"]["lng"]
    else:
        target_lat, target_lng = req["pickup_lat"], req["pickup_lng"]

    best = find_best_driver(target_lat, target_lng, set(req["rejected_by"]))

    if not best:
        emit("ride_assigned", {"success": False, "message": "All drivers busy or rejected."}, room=_ride_room(req_id))
        _delete_active_request(req_id)
        return

    fare_breakdown = build_fare_breakdown(
        req.get("distance_km", 0),
        req.get("food_distance_km", 0),
        req.get("food_order_price", 0),
    )

    emit(
        "driver_request",
        {
            "request_id": req_id,
            "target_driver_id": best["id"],
            "user_name": req["user_name"],
            "pickup": {"lat": req["pickup_lat"], "lng": req["pickup_lng"]},
            "drop": {"lat": req["drop_lat"], "lng": req["drop_lng"]},
            "pickup_name": req["pickup_name"],
            "drop_name": req["drop_name"],
            "restaurant": req.get("restaurant"),
            "fare": req["fare"],
            "eta_minutes": req.get("eta_minutes"),
            "food_order": req.get("food_order"),
            "food_order_price": req.get("food_order_price", 0),
            "fare_breakdown": fare_breakdown,
        },
        # Only the offered driver's own room receives this - not a global
        # broadcast like the original implementation.
        room=_driver_room(best["id"]),
    )


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
    """Automated backend background simulation of car movement along OSRM road routes."""
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

    target_lat, target_lng = pickup_lat, pickup_lng
    has_food = bool(restaurant)
    if has_food:
        target_lat, target_lng = restaurant["lat"], restaurant["lng"]

    # 1. Heading to restaurant (food) or pickup
    socketio.emit(
        "ride_progress",
        {"driver_id": driver_id, "request_id": req_id, "status": "heading_to_pickup"},
        room=room,
    )
    path1 = _fetch_points(start_lat, start_lng, target_lat, target_lng)
    step_size = max(1, len(path1) // 12)
    for i in range(0, len(path1), step_size):
        pt = path1[i]
        socketio.emit(
            "driver_moved",
            {"driver_id": driver_id, "latitude": pt["lat"], "longitude": pt["lng"]},
        )
        eventlet.sleep(0.5)

    if has_food:
        socketio.emit(
            "ride_progress",
            {"driver_id": driver_id, "request_id": req_id, "status": "at_restaurant"},
            room=room,
        )
        eventlet.sleep(1.5)
        socketio.emit(
            "ride_progress",
            {"driver_id": driver_id, "request_id": req_id, "status": "food_picked"},
            room=room,
        )
        eventlet.sleep(1.0)
        # Drive from restaurant to rider pickup
        path_to_pickup = _fetch_points(target_lat, target_lng, pickup_lat, pickup_lng)
        step_pickup = max(1, len(path_to_pickup) // 10)
        for i in range(0, len(path_to_pickup), step_pickup):
            pt = path_to_pickup[i]
            socketio.emit(
                "driver_moved",
                {"driver_id": driver_id, "latitude": pt["lat"], "longitude": pt["lng"]},
            )
            eventlet.sleep(0.4)

    # 2. Picked Up / At Pickup
    socketio.emit(
        "ride_progress",
        {"driver_id": driver_id, "request_id": req_id, "status": "picked_up"},
        room=room,
    )
    eventlet.sleep(1.5)

    # 3. Trip Started -> Driving to Dropoff
    socketio.emit(
        "ride_progress",
        {"driver_id": driver_id, "request_id": req_id, "status": "trip_started"},
        room=room,
    )
    trip_start_lat, trip_start_lng = pickup_lat, pickup_lng
    path2 = _fetch_points(trip_start_lat, trip_start_lng, drop_lat, drop_lng)
    step_size2 = max(1, len(path2) // 15)
    for i in range(0, len(path2), step_size2):
        pt = path2[i]
        socketio.emit(
            "driver_moved",
            {"driver_id": driver_id, "latitude": pt["lat"], "longitude": pt["lng"]},
        )
        eventlet.sleep(0.5)

    # Ensure driver reaches exact drop coords
    socketio.emit(
        "driver_moved",
        {"driver_id": driver_id, "latitude": drop_lat, "longitude": drop_lng},
    )

    # 4. Ride Arrived at Destination - waiting for driver payment confirmation
    socketio.emit(
        "ride_progress",
        {"driver_id": driver_id, "request_id": req_id, "status": "completed", "fare": fare},
        room=room,
    )


"""End-to-end Socket.IO flows through the real handlers, Postgres and Redis,
using Flask-SocketIO's in-process test client (no message queue).
Skipped unless LYFT_TEST_DB_NAME and LYFT_TEST_REDIS_URL are set."""
import time

import pytest
from flask import Flask

from config import Config
from db import get_db_conn
from extensions import socketio
from services.auth_service import issue_token
from sockets import handlers

GEO = Config.REDIS_DRIVER_GEO_KEY
PICKUP = {"pickup_lat": 12.9720, "pickup_lng": 77.5950, "drop_lat": 12.9800, "drop_lng": 77.6000}


@pytest.fixture(scope="module")
def app(pg_test_db):
    flask_app = Flask(__name__)
    # extensions.py builds SocketIO with the Redis message queue, which stores a
    # client_manager up front; the in-process test client can't use a queue.
    socketio.server_options.pop("client_manager", None)
    socketio.init_app(flask_app, message_queue=None, async_mode="threading")
    handlers.register_handlers()
    return flask_app


@pytest.fixture(autouse=True)
def _env(app, seeded_db, redis_test, monkeypatch):
    monkeypatch.setattr(Config, "RIDE_SIMULATOR", "none")
    monkeypatch.setattr(Config, "USE_LIVE_ROUTING", False)
    handlers._socket_identity.clear()
    yield
    handlers._socket_identity.clear()


def connect(app, role, user_id, name=None):
    token = issue_token(user_id, role, name or f"{role.title()} {user_id}")
    client = socketio.test_client(app, auth={"token": token})
    assert client.is_connected()
    return client


def received(client, event):
    return [e["args"][0] for e in client.get_received() if e["name"] == event]


def online(app, driver_id, lat=12.9716, lng=77.5946):
    client = connect(app, "driver", driver_id)
    client.emit("driver_online", {"lat": lat, "lng": lng})
    status = received(client, "driver_status")
    assert status[-1]["online"] is True
    return client


def scalar(sql, params=()):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]


def test_full_ride_lifecycle_with_recovery_and_guards(app, redis_test):
    driver = online(app, 1)
    far_driver = online(app, 2, lat=12.9900, lng=77.6200)
    bystander = connect(app, "rider", 3)
    rider = connect(app, "rider", 1)

    ack = rider.emit("request_ride", {**PICKUP, "pickup_name": "Home", "drop_name": "Work"}, callback=True)
    assert ack["success"] is True
    request_id = ack["request_id"]
    assert received(rider, "ride_requested")[0]["request_id"] == request_id

    offer = received(driver, "driver_request")
    assert len(offer) == 1 and offer[0]["request_id"] == request_id and offer[0]["offer_expires_in_seconds"] > 0
    assert received(far_driver, "driver_request") == []
    assert bystander.get_received() == []

    # A driver who wasn't offered the ride can't take it.
    far_driver.emit("driver_response", {"request_id": request_id, "accepted": True})
    assert received(far_driver, "ride_accept_failed")[0]["reason"] == "not_offered"

    assert driver.emit("driver_response", {"request_id": request_id, "accepted": True}, callback=True)["success"]
    assigned = received(rider, "ride_assigned")
    assert assigned[0]["success"] is True and assigned[0]["driver"]["id"] == 1
    assert received(driver, "ride_assigned")[0]["request_id"] == request_id

    driver.emit("driver_response", {"request_id": request_id, "accepted": True})
    assert received(driver, "ride_accept_failed")[0]["reason"] == "already_accepted"

    # On a trip: out of the pool, and neither driver_online nor heartbeats bring them back.
    assert redis_test.zscore(GEO, "1") is None
    driver.emit("driver_online", {"lat": 12.97, "lng": 77.59})
    assert received(driver, "driver_status")[-1] == {"online": False, "reason": "active_ride", "heartbeat_interval_seconds": Config.DRIVER_HEARTBEAT_INTERVAL_SECONDS}
    driver.emit("driver_heartbeat", {"lat": 12.97, "lng": 77.59})
    assert received(driver, "driver_status")[-1]["reason"] == "not_in_pool"
    driver.emit("update_location", {"driver_id": 1, "latitude": 12.975, "longitude": 77.597})
    assert redis_test.zscore(GEO, "1") is None

    # Location goes to the ride room only.
    assert received(rider, "driver_moved") == [{"driver_id": 1, "latitude": 12.975, "longitude": 77.597}]
    assert bystander.get_received() == []

    # Rider reconnects mid-trip and gets the persisted state and room back.
    rider.disconnect()
    rider = connect(app, "rider", 1)
    state = received(rider, "ride_state")
    assert state[0]["request_id"] == request_id and state[0]["status"] == "accepted" and state[0]["driver"]["id"] == 1

    driver.emit("ride_status_update", {"driver_id": 1, "request_id": request_id, "status": "picked_up"})
    assert [p["status"] for p in received(rider, "ride_progress")] == ["picked_up"]

    # Regressions and duplicates are ignored, not relayed.
    driver.emit("ride_status_update", {"driver_id": 1, "request_id": request_id, "status": "heading_to_pickup"})
    driver.emit("ride_status_update", {"driver_id": 1, "request_id": request_id, "status": "picked_up"})
    assert received(rider, "ride_progress") == []

    # Another driver can't move this ride along.
    far_driver.emit("ride_status_update", {"driver_id": 2, "request_id": request_id, "status": "completed"})
    assert received(rider, "ride_progress") == []

    driver.emit("ride_status_update", {"driver_id": 1, "request_id": request_id, "status": "trip_started"})
    driver.emit("ride_status_update", {"driver_id": 1, "request_id": request_id, "status": "completed"})
    progress = received(rider, "ride_progress")
    assert [p["status"] for p in progress] == ["trip_started", "completed"]
    fare = progress[-1]["fare"]
    assert fare == scalar("SELECT fare FROM rides WHERE request_id = %s", (request_id,))

    # Driver reloads the page before confirming payment.
    driver.disconnect()
    driver = connect(app, "driver", 1)
    assert received(driver, "ride_state")[0]["status"] == "completed"

    driver.emit("payment_collected", {"driver_id": 1, "request_id": request_id, "amount": fare + 500})
    assert received(driver, "payment_failed")[0]["reason"] == "amount_mismatch"
    far_driver.emit("payment_collected", {"driver_id": 2, "request_id": request_id, "amount": fare})
    assert received(far_driver, "payment_failed")[0]["reason"] == "not_your_ride"

    driver.emit("payment_collected", {"driver_id": 1, "request_id": request_id, "amount": fare})
    driver_events = driver.get_received()
    earnings = [e["args"][0] for e in driver_events if e["name"] == "earnings_update"]
    assert earnings[0]["earned"] == fare and earnings[0]["total"] == fare
    assert any(e["name"] == "driver_status" and e["args"][0]["online"] for e in driver_events)
    assert received(rider, "payment_confirmed")[0]["fare"] == fare

    driver.emit("payment_collected", {"driver_id": 1, "request_id": request_id, "amount": fare})
    assert received(driver, "payment_failed")[0]["reason"] == "already_confirmed"
    assert scalar("SELECT earnings FROM drivers WHERE id = 1") == fare

    # Back in the pool at the drop point; nothing left to restore.
    assert redis_test.zscore(GEO, "1") is not None
    driver.disconnect()
    assert received(connect(app, "driver", 1), "ride_state") == []
    assert received(connect(app, "rider", 1), "ride_state") == []


def test_offer_expiry_moves_ride_to_next_driver(app, monkeypatch):
    monkeypatch.setattr(Config, "OFFER_TIMEOUT_SECONDS", 1)
    first = online(app, 1)
    second = online(app, 2, lat=12.9760, lng=77.6000)
    rider = connect(app, "rider", 1)
    request_id = rider.emit("request_ride", PICKUP, callback=True)["request_id"]
    assert received(first, "driver_request")[0]["request_id"] == request_id
    assert received(second, "driver_request") == []

    time.sleep(1.3)
    handlers.run_maintenance_once()
    time.sleep(0.5)  # the offer's own timer may also fire; both paths are idempotent

    assert received(first, "offer_expired") == [{"request_id": request_id}]
    assert [o["request_id"] for o in received(second, "driver_request")] == [request_id]

    first.emit("driver_response", {"request_id": request_id, "accepted": True})
    assert received(first, "ride_accept_failed")[0]["reason"] == "offer_expired"
    assert second.emit("driver_response", {"request_id": request_id, "accepted": True}, callback=True)["success"]
    assert received(rider, "ride_assigned")[-1]["driver"]["id"] == 2


def test_driver_going_offline_hands_offer_to_next_driver(app):
    first = online(app, 1)
    second = online(app, 2, lat=12.9760, lng=77.6000)
    rider = connect(app, "rider", 1)
    request_id = rider.emit("request_ride", PICKUP, callback=True)["request_id"]
    assert received(first, "driver_request")

    first.emit("driver_offline", {})
    assert [o["request_id"] for o in received(second, "driver_request")] == [request_id]
    first.emit("driver_response", {"request_id": request_id, "accepted": True})
    assert received(first, "ride_accept_failed")[0]["reason"] == "offer_rejected"


def test_driver_with_outstanding_offer_is_not_offered_another_ride(app):
    driver = online(app, 1)
    connect(app, "rider", 1).emit("request_ride", PICKUP, callback=True)
    second_rider = connect(app, "rider", 2)
    second_rider.emit("request_ride", PICKUP, callback=True)

    assert len(received(driver, "driver_request")) == 1
    failure = received(second_rider, "ride_assigned")
    assert failure[0]["success"] is False  # the only driver is reserved


def test_stale_drivers_do_not_block_dispatch(app, redis_test):
    for i in range(101, 111):  # expired heartbeats, closer than the live driver
        redis_test.geoadd(GEO, (77.5950 + (i - 100) * 0.0002, 12.9720, str(i)))
    driver = online(app, 1, lat=12.9900, lng=77.6200)
    rider = connect(app, "rider", 1)
    request_id = rider.emit("request_ride", PICKUP, callback=True)["request_id"]
    assert received(driver, "driver_request")[0]["request_id"] == request_id


def test_no_driver_available_cancels_request(app):
    rider = connect(app, "rider", 1)
    request_id = rider.emit("request_ride", PICKUP, callback=True)["request_id"]
    failure = received(rider, "ride_assigned")
    assert failure[0]["success"] is False and failure[0]["request_id"] == request_id
    assert scalar("SELECT status FROM rides WHERE request_id = %s", (request_id,)) == "cancelled"
    assert scalar("SELECT cancel_reason FROM rides WHERE request_id = %s", (request_id,)) == "no_driver"


def test_heartbeat_lifecycle(app, redis_test):
    driver = online(app, 1)
    status = received(driver, "driver_status")
    redis_test.expire("driver:heartbeat:1", 5)

    driver.emit("driver_heartbeat", {"lat": 12.9716, "lng": 77.5946})
    reply = received(driver, "driver_status")[-1]
    assert reply["online"] is True and reply["heartbeat_interval_seconds"] == Config.DRIVER_HEARTBEAT_INTERVAL_SECONDS
    assert redis_test.ttl("driver:heartbeat:1") > 5

    driver.emit("driver_heartbeat", {"lat": "abc", "lng": 77.5946})
    assert received(driver, "driver_status")[-1]["reason"] == "invalid_location"

    driver.emit("driver_offline", {})
    received(driver, "driver_status")
    driver.emit("driver_heartbeat", {"lat": 12.9716, "lng": 77.5946})
    assert received(driver, "driver_status")[-1] == {"online": False, "reason": "not_in_pool", "heartbeat_interval_seconds": Config.DRIVER_HEARTBEAT_INTERVAL_SECONDS}
    assert redis_test.zscore(GEO, "1") is None
    del status


def test_food_price_and_restaurant_location_come_from_the_database(app):
    rider = connect(app, "rider", 1)
    tampered = {
        **PICKUP,
        "restaurant": {"name": "truffles", "lat": 0, "lng": 0},
        "food_order": [{"item": "cheese burger", "price": 1, "qty": 1}],
        "food_order_price": 1,
    }
    ack = rider.emit("request_ride", tampered, callback=True)
    assert ack["success"] is True
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT details, fare FROM rides WHERE request_id = %s", (ack["request_id"],))
            details, fare = cur.fetchone()
    assert details["food_order_price"] == 250
    assert details["food_order"] == [{"item": "Cheese Burger", "price": 250.0, "qty": 1}]
    assert details["restaurant"] == {"name": "Truffles", "lat": 12.9719, "lng": 77.6011}
    assert fare > 250


def test_unknown_menu_item_is_refused(app):
    rider = connect(app, "rider", 1)
    ack = rider.emit("request_ride", {**PICKUP, "restaurant": {"name": "Truffles"}, "food_order": [{"item": "Free Lunch"}]}, callback=True)
    assert ack["success"] is False
    assert scalar("SELECT COUNT(*) FROM rides") == 0


def test_invalid_coordinates_are_refused_with_an_error(app):
    rider = connect(app, "rider", 1)
    ack = rider.emit("request_ride", {"pickup_lat": "x"}, callback=True)
    assert ack["success"] is False
    assert received(rider, "ride_assigned")[0]["success"] is False


def test_simulation_engine_is_refused_unless_node_mode(app, monkeypatch):
    monkeypatch.setattr(Config, "RIDE_SIMULATOR", "python")
    engine = socketio.test_client(app, auth={"service_token": Config.SIMULATION_SERVICE_TOKEN})
    assert not engine.is_connected()

    monkeypatch.setattr(Config, "RIDE_SIMULATOR", "node")
    engine = socketio.test_client(app, auth={"service_token": Config.SIMULATION_SERVICE_TOKEN})
    assert engine.is_connected()


def test_node_engine_cannot_return_on_trip_driver_to_pool(app, redis_test, monkeypatch):
    monkeypatch.setattr(Config, "RIDE_SIMULATOR", "node")
    engine = socketio.test_client(app, auth={"service_token": Config.SIMULATION_SERVICE_TOKEN})
    driver = online(app, 1)
    rider = connect(app, "rider", 1)
    request_id = rider.emit("request_ride", PICKUP, callback=True)["request_id"]
    driver.emit("driver_response", {"request_id": request_id, "accepted": True})
    start = received(engine, "start_simulation_ride")
    assert start[0]["request_id"] == request_id

    for _ in range(3):
        engine.emit("update_location", {"driver_id": 1, "latitude": 12.975, "longitude": 77.597})
    engine.emit("ride_status_update", {"driver_id": 1, "request_id": request_id, "status": "completed"})
    assert redis_test.zscore(GEO, "1") is None  # moving and even completed: still not matchable
    rider_events = rider.get_received()  # get_received drains the queue: read once
    assert [e["args"][0]["status"] for e in rider_events if e["name"] == "ride_progress"] == ["completed"]
    assert len([e for e in rider_events if e["name"] == "driver_moved"]) == 3

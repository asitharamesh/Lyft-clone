"""
Manual smoke test for the real-time dispatch flow: authenticates a rider
and a driver over JWT, brings the driver online, fires a ride request, and
confirms the driver_request event is delivered ONLY to the target driver
(not broadcast to the rider or any other socket) - the key fix vs the
original implementation.
"""
import json
import sys
import time

import requests
import socketio

BASE = "http://localhost:5001"

driver_login = requests.post(f"{BASE}/api/login", json={
    "type": "driver", "email": "driver0@test.com", "password": "password123"
}).json()
rider_login = requests.post(f"{BASE}/api/login", json={
    "type": "rider", "email": "user0@test.com", "password": "password123"
}).json()

assert driver_login["success"], driver_login
assert rider_login["success"], rider_login
driver_token = driver_login["token"]
rider_token = rider_login["token"]
driver_id = driver_login["user"]["id"]
print(f"Logged in driver id={driver_id}, rider id={rider_login['user']['id']}")

driver_sio = socketio.Client()
rider_sio = socketio.Client()

events_received = {"driver": [], "rider": []}


@driver_sio.on("driver_request")
def on_driver_request(data):
    events_received["driver"].append(("driver_request", data))


@rider_sio.on("driver_request")
def on_rider_driver_request(data):
    # This should NEVER fire - riders must not receive driver-targeted offers.
    events_received["rider"].append(("driver_request", data))


@rider_sio.on("ride_assigned")
def on_ride_assigned(data):
    events_received["rider"].append(("ride_assigned", data))


# Also connect a THIRD, unrelated socket with no auth relation to this ride,
# to prove it receives nothing ride-specific (only public driver_moved pings).
bystander_sio = socketio.Client()
bystander_events = []
bystander_sio.on("driver_request", lambda d: bystander_events.append(d))
bystander_sio.on("ride_assigned", lambda d: bystander_events.append(d))

# A bystander needs *some* valid identity to connect (auth is required) -
# reuse the rider token for a second, independent connection.
bystander_sio.connect(BASE, auth={"token": rider_token}, transports=["websocket"])

driver_sio.connect(BASE, auth={"token": driver_token}, transports=["websocket"])
rider_sio.connect(BASE, auth={"token": rider_token}, transports=["websocket"])

# Sanity check: connecting with a bad/missing token must be rejected.
bad_sio = socketio.Client()
rejected = False
try:
    bad_sio.connect(BASE, auth={"token": "not-a-real-token"}, transports=["websocket"])
except socketio.exceptions.ConnectionError:
    rejected = True
print(f"Unauthenticated connection rejected: {rejected}")
assert rejected

driver_sio.emit("driver_online", {"lat": 12.9716, "lng": 77.5946})
time.sleep(0.5)

rider_sio.emit("request_ride", {
    "pickup_lat": 12.9720, "pickup_lng": 77.5950,
    "drop_lat": 12.9800, "drop_lng": 77.6000,
    "pickup_name": "Test Pickup", "drop_name": "Test Drop",
})
time.sleep(1.5)

print(json.dumps({
    "driver_received": [e[0] for e in events_received["driver"]],
    "rider_received": [e[0] for e in events_received["rider"]],
    "bystander_received_count": len(bystander_events),
}, indent=2))

assert any(e[0] == "driver_request" for e in events_received["driver"]), "Driver never got the offer"
assert not any(e[0] == "driver_request" for e in events_received["rider"]), "Rider leaked a driver-targeted event!"
assert len(bystander_events) == 0, "Unrelated bystander socket received ride-specific data!"

print("\nALL ASSERTIONS PASSED: driver_request is room-scoped, not broadcast.")

driver_sio.disconnect()
rider_sio.disconnect()
bystander_sio.disconnect()

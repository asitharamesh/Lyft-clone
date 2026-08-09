import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.routing_service import haversine_km  # noqa: E402


def test_haversine_zero_distance_same_point():
    assert haversine_km(12.97, 77.59, 12.97, 77.59) == 0


def test_haversine_known_distance_bangalore_points():
    # Roughly 5-7km apart real-world landmarks in Bangalore.
    dist = haversine_km(12.9716, 77.5946, 12.9338, 77.6125)
    assert 4.0 < dist < 5.5


def test_haversine_handles_missing_coordinates():
    assert haversine_km(None, 77.59, 12.97, 77.59) == float("inf")

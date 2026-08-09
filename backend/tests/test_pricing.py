import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.pricing_service import (  # noqa: E402
    calculate_ride_fare,
    calculate_total_fare,
    build_fare_breakdown,
)


def test_base_fare_with_zero_distance():
    assert calculate_ride_fare(0) == 40


def test_fare_scales_with_distance():
    assert calculate_ride_fare(5) == 100  # 40 + 5*12


def test_total_fare_includes_food_surcharge_and_price():
    total = calculate_total_fare(distance_km=5, food_distance_km=2, food_order_price=300)
    # ride: 40 + 60 = 100 ; food: 2*12 + 50 + 300 = 374 ; total = 474
    assert total == 474


def test_total_fare_without_food_matches_ride_fare():
    assert calculate_total_fare(distance_km=3) == calculate_ride_fare(3)


def test_build_fare_breakdown_includes_all_components():
    breakdown = build_fare_breakdown(distance_km=5, food_distance_km=2, food_order_price=300)

    assert breakdown["base_fare"] == 40
    assert breakdown["ride_km_cost"] == 60
    assert breakdown["ride_fare"] == 100
    assert breakdown["food_distance_km"] == 2
    assert breakdown["food_pickup_surcharge"] == 50
    assert breakdown["food_delivery_fee"] == 74
    assert breakdown["food_order_price"] == 300
    assert breakdown["total"] == 474

"""
Fare calculation, pulled out of the socket handler into a pure function so
it's independently unit-testable (see tests/test_pricing.py) and reusable
from both the ride and food-delivery flows.
"""
BASE_FARE = 40
PER_KM_RATE = 12
FOOD_PICKUP_SURCHARGE = 50


def calculate_ride_fare(distance_km: float) -> float:
    return round(BASE_FARE + distance_km * PER_KM_RATE)


def calculate_food_fare(restaurant_to_pickup_km: float, food_order_price: float) -> float:
    return round(restaurant_to_pickup_km * PER_KM_RATE + FOOD_PICKUP_SURCHARGE + food_order_price)


def build_fare_breakdown(distance_km: float, food_distance_km: float = 0, food_order_price: float = 0) -> dict:
    ride_distance_km = max(distance_km, 0)
    ride_fare = calculate_ride_fare(ride_distance_km)
    food_distance = max(food_distance_km, 0)
    food_delivery_fee = calculate_food_fare(food_distance, 0)
    total = ride_fare + (food_delivery_fee if food_distance else 0) + (food_order_price if food_order_price else 0)

    return {
        "base_fare": BASE_FARE,
        "ride_km_cost": round(ride_distance_km * PER_KM_RATE),
        "ride_fare": ride_fare,
        "food_distance_km": round(food_distance, 2) if food_distance else 0,
        "food_pickup_surcharge": FOOD_PICKUP_SURCHARGE if food_distance else 0,
        "food_delivery_fee": food_delivery_fee,
        "food_order_price": round(food_order_price, 2),
        "total": round(total),
    }


def calculate_total_fare(distance_km: float, food_distance_km: float = 0, food_order_price: float = 0) -> float:
    return build_fare_breakdown(distance_km, food_distance_km, food_order_price)["total"]

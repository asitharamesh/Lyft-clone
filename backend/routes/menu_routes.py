import re

from flask import Blueprint, jsonify, request

from db import get_db_conn
from services.pricing_service import build_fare_breakdown
from services.routing_service import road_route

menu_bp = Blueprint("menu", __name__, url_prefix="/api")

LINK_PATTERN = re.compile(
    r"(?:https?://)?(?:www\.)?lyft\.com/([^/\s?#]+)(?:/([^/\s?#]+))?",
    re.IGNORECASE,
)


def _slug_to_name(slug: str) -> str:
    return " ".join(part.capitalize() for part in slug.strip().split("-") if part)


def parse_order_link(link: str) -> tuple[str | None, str | None, str | None]:
    """Parse http://lyft.com/restaurant-slug/item-slug into display names."""
    match = LINK_PATTERN.match((link or "").strip())
    if not match:
        return None, None, (
            "Invalid link. Use format: http://lyft.com/restaurant/item "
            "(e.g. http://lyft.com/truffles/cheese-burger)"
        )
    restaurant_name = _slug_to_name(match.group(1))
    item_slug = match.group(2)
    item_name = _slug_to_name(item_slug) if item_slug else None
    return restaurant_name, item_name, None


def _find_menu_items(cur, restaurant_id: int, item_name: str | None):
    if item_name:
        cur.execute(
            """
            SELECT name, price FROM menu_items
            WHERE restaurant_id = %s
              AND (
                LOWER(name) = LOWER(%s)
                OR LOWER(REPLACE(name, ' ', '-')) = LOWER(REPLACE(%s, ' ', '-'))
              )
            """,
            (restaurant_id, item_name, item_name),
        )
        rows = cur.fetchall()
        if rows:
            return rows
        cur.execute(
            """
            SELECT name, price FROM menu_items
            WHERE restaurant_id = %s AND LOWER(name) LIKE LOWER(%s)
            LIMIT 1
            """,
            (restaurant_id, f"%{item_name.replace(' ', '%')}%"),
        )
        return cur.fetchall()

    cur.execute(
        "SELECT name, price FROM menu_items WHERE restaurant_id = %s ORDER BY RANDOM() LIMIT 2",
        (restaurant_id,),
    )
    return cur.fetchall()


@menu_bp.route("/menu", methods=["POST"])
def get_restaurant_menu():
    data = request.get_json(silent=True) or {}
    link = (data.get("link") or "").strip()
    restaurant_name = (data.get("name") or "").strip()
    item_name_raw = data.get("item")

    if link:
        parsed_restaurant, parsed_item, link_err = parse_order_link(link)
        if link_err:
            return jsonify({"success": False, "message": link_err}), 400
        restaurant_name = parsed_restaurant or restaurant_name
        item_name_raw = parsed_item or item_name_raw

    if not restaurant_name:
        return jsonify({"success": False, "message": "Restaurant name is required"}), 400

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, latitude, longitude FROM restaurants WHERE LOWER(name) = LOWER(%s)",
                (restaurant_name,),
            )
            restaurant = cur.fetchone()

            if not restaurant:
                return jsonify({
                    "success": False,
                    "message": "Restaurant not found. Try 'Truffles' or 'Meghana Foods'",
                }), 404

            r_id, r_name, r_lat, r_lng = restaurant
            menu_items_raw = _find_menu_items(cur, r_id, item_name_raw)
            if item_name_raw and not menu_items_raw:
                return jsonify({
                    "success": False,
                    "message": f"Item '{item_name_raw}' not found at {r_name}. Check the link and try again.",
                }), 404

    if not menu_items_raw:
        return jsonify({"success": False, "message": "No menu items found for this restaurant"}), 404

    food_order = [{"item": name, "price": float(price), "qty": 1} for name, price in menu_items_raw]
    food_order_price = sum(item["price"] for item in food_order)

    return jsonify({
        "success": True,
        "restaurant": {"name": r_name, "lat": r_lat, "lng": r_lng},
        "food_order": food_order,
        "food_order_price": float(food_order_price),
    })


@menu_bp.route("/fare-estimate", methods=["POST"])
def fare_estimate():
    """Authoritative fare breakdown — same math used when a ride is booked."""
    data = request.get_json(silent=True) or {}
    try:
        pickup_lat = float(data["pickup_lat"])
        pickup_lng = float(data["pickup_lng"])
        drop_lat = float(data["drop_lat"])
        drop_lng = float(data["drop_lng"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"success": False, "message": "pickup and drop coordinates are required"}), 400

    food_order_price = float(data.get("food_order_price") or 0)
    restaurant = data.get("restaurant")

    route = road_route(pickup_lat, pickup_lng, drop_lat, drop_lng)
    food_distance_km = 0.0

    if restaurant and restaurant.get("lat") is not None and restaurant.get("lng") is not None:
        food_route = road_route(
            float(restaurant["lat"]),
            float(restaurant["lng"]),
            pickup_lat,
            pickup_lng,
        )
        food_distance_km = food_route["distance_km"]

    breakdown = build_fare_breakdown(route["distance_km"], food_distance_km, food_order_price)

    return jsonify({
        "success": True,
        "distance_km": round(route["distance_km"], 2),
        **breakdown,
        "is_food_ride": bool(food_distance_km),
    })

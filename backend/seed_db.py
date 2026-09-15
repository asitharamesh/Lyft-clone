"""Seed demo data. Usage: python seed_db.py

Passwords are bcrypt-hashed before insert (never stored plaintext), even
for throwaway demo/seed accounts, so the demo DB behaves exactly like a
real one and can't set a bad example.
"""
import psycopg2

from config import Config
from services.auth_service import hash_password

DEMO_PASSWORD = "password123"  # meets the 8-char minimum enforced by /api/signup

RESTAURANTS = [
    ("Meghana Foods", "Biryani", 4.8, 12.9338, 77.6125),
    ("Truffles", "Burgers", 4.6, 12.9719, 77.6011),
    ("CTR", "South Indian", 4.9, 12.9982, 77.5712),
    ("Empire", "North Indian", 4.2, 12.9779, 77.6060),
    ("Corner House", "Desserts", 4.9, 12.9657, 77.5942),
]

MENU_BY_CUISINE = {
    "Biryani": [("Chicken Biryani", 350), ("Paneer Biryani", 320), ("Mutton Biryani", 450)],
    "Burgers": [("Classic Burger", 210), ("Cheese Burger", 250), ("Veggie Burger", 180)],
    "South Indian": [("Masala Dosa", 120), ("Idly Vada", 80), ("Filter Coffee", 50)],
    "North Indian": [("Butter Chicken", 380), ("Garlic Naan", 60), ("Paneer Tikka Masala", 350)],
    "Desserts": [("Death By Chocolate", 300), ("Hot Fudge Sundae", 250), ("Cake Fudge", 280)],
}

DEMO_DRIVERS = [
    ("Asha Rao", "driver0@test.com", 12.9716, 77.5946, 4.9),
    ("Vikram Shet", "driver1@test.com", 12.9600, 77.6100, 4.6),
    ("Priya Nair", "driver2@test.com", 12.9850, 77.5900, 4.8),
]


def seed_data():
    conn = psycopg2.connect(
        host=Config.DB_HOST, port=Config.DB_PORT, dbname=Config.DB_NAME,
        user=Config.DB_USER, password=Config.DB_PASSWORD,
    )
    hashed = hash_password(DEMO_PASSWORD)

    try:
        with conn.cursor() as cur:
            print("Clearing old data...")
            cur.execute("TRUNCATE TABLE users, drivers, rides, restaurants, menu_items RESTART IDENTITY CASCADE;")

            print("Seeding demo riders...")
            for i in range(5):
                cur.execute(
                    "INSERT INTO users (name, email, password, latitude, longitude) VALUES (%s, %s, %s, %s, %s)",
                    (f"User {i}", f"user{i}@test.com", hashed, 12.9716, 77.5946),
                )

            print("Seeding demo admin...")
            cur.execute(
                "INSERT INTO users (name, email, password, latitude, longitude, is_admin) "
                "VALUES (%s, %s, %s, %s, %s, TRUE)",
                ("Admin", "admin@test.com", hashed, 12.9716, 77.5946),
            )

            print("Seeding demo drivers...")
            for name, email, lat, lng, rating in DEMO_DRIVERS:
                cur.execute(
                    """INSERT INTO drivers (name, email, password, license_plate, latitude, longitude,
                                             is_available, earnings, rating)
                       VALUES (%s, %s, %s, 'KA-01-AB-1234', %s, %s, FALSE, 0.00, %s)""",
                    (name, email, hashed, lat, lng, rating),
                )

            print("Seeding restaurants + menus...")
            for r_name, cuisine, rating, lat, lng in RESTAURANTS:
                cur.execute(
                    "INSERT INTO restaurants (name, cuisine, rating, latitude, longitude) "
                    "VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (r_name, cuisine, rating, lat, lng),
                )
                r_id = cur.fetchone()[0]
                items = MENU_BY_CUISINE.get(cuisine, [("Special Item 1", 150), ("Special Item 2", 180)])
                for item_name, price in items:
                    cur.execute(
                        "INSERT INTO menu_items (restaurant_id, name, price) VALUES (%s, %s, %s)",
                        (r_id, item_name, price),
                    )
                print(f"  -> {r_name}: {len(items)} items")

        conn.commit()
        print(f"Done. Demo login password for all seeded accounts: {DEMO_PASSWORD}")
    finally:
        conn.close()


if __name__ == "__main__":
    seed_data()

"""
Shared fixtures.

Unit tests need nothing running. Integration tests are opt-in and use
dedicated, disposable stores named explicitly by environment variables:

  LYFT_TEST_DB_NAME=lyft_clone_test          # must end in _test: tables are dropped
  LYFT_TEST_REDIS_URL=redis://localhost:6379/15  # must not be db 0: it is flushed

Other connection settings (host, user, password) come from backend/.env.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from config import Config  # noqa: E402


@pytest.fixture(scope="session")
def pg_test_db():
    name = os.getenv("LYFT_TEST_DB_NAME")
    if not name:
        pytest.skip("set LYFT_TEST_DB_NAME=<database ending in _test> to run Postgres integration tests")
    if not name.endswith("_test"):
        pytest.fail("LYFT_TEST_DB_NAME must end with '_test' - its tables are dropped and recreated")

    import db
    import init_db

    Config.DB_NAME = name
    db.close_pool()
    conn = init_db.connect()
    try:
        init_db.run_schema(conn)
        init_db.run_migrations(conn)
    finally:
        conn.close()
    db.init_pool()
    yield name
    db.close_pool()


@pytest.fixture
def seeded_db(pg_test_db):
    """Riders 1-3, drivers 1-3 and one restaurant with a menu, fresh per test."""
    from db import get_db_conn

    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE ride_offers, rides, food_orders, menu_items, restaurants, drivers, users "
                "RESTART IDENTITY CASCADE"
            )
            for i in range(1, 4):
                cur.execute(
                    "INSERT INTO users (name, email, password) VALUES (%s, %s, 'not-a-real-hash')",
                    (f"Rider {i}", f"rider{i}@test.com"),
                )
                cur.execute(
                    "INSERT INTO drivers (name, email, password, latitude, longitude, rating) "
                    "VALUES (%s, %s, 'not-a-real-hash', 12.97, 77.59, 4.8)",
                    (f"Driver {i}", f"driver{i}@test.com"),
                )
            cur.execute(
                "INSERT INTO restaurants (name, cuisine, latitude, longitude) "
                "VALUES ('Truffles', 'Burgers', 12.9719, 77.6011) RETURNING id"
            )
            restaurant_id = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO menu_items (restaurant_id, name, price) VALUES (%s, 'Cheese Burger', 250), (%s, 'Veggie Burger', 180)",
                (restaurant_id, restaurant_id),
            )
        conn.commit()


@pytest.fixture
def redis_test():
    url = os.getenv("LYFT_TEST_REDIS_URL")
    if not url:
        pytest.skip("set LYFT_TEST_REDIS_URL=redis://host:port/<non-zero db> to run Redis integration tests")
    db_index = url.rstrip("/").rsplit("/", 1)[-1]
    if not db_index.isdigit() or db_index == "0":
        pytest.fail("LYFT_TEST_REDIS_URL must name a non-zero Redis database - it is flushed")

    import db

    previous_url = Config.REDIS_URL
    Config.REDIS_URL = url
    db._redis_client = None
    client = db.get_redis()
    client.flushdb()
    yield client
    client.flushdb()
    db._redis_client = None
    Config.REDIS_URL = previous_url

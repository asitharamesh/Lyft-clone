-- Lyft-clone schema.
--
-- Notable changes from the original version:
--   * `password` columns now store bcrypt hashes (60 chars), never plaintext.
--   * Added `rating` to drivers, used by the matching algorithm.
--   * Added indexes that the app's real query patterns need:
--       - partial index on available drivers' coordinates (the hot path
--         for the SQL matching fallback)
--       - foreign-key indexes on rides/menu_items/food_orders so joins and
--         lookups don't degrade into sequential scans as tables grow
--   * Optional PostGIS-lite `cube`/`earthdistance` extension for KNN
--     queries directly in SQL, if you want geo queries in Postgres as a
--     secondary option to the Redis GEO index used in matching_service.py.

DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS ride_offers;   -- created by migrations/001_ride_lifecycle.sql
DROP TABLE IF EXISTS rides;
DROP TABLE IF EXISTS food_orders;
DROP TABLE IF EXISTS menu_items;
DROP TABLE IF EXISTS restaurants;
DROP TABLE IF EXISTS drivers;
DROP TABLE IF EXISTS users;

-- Optional: enables ll_to_earth()/earth_distance() for SQL-side geo KNN
-- queries. Safe to leave commented out; the app does not require it
-- (Redis GEO is the primary matching path, with a plain lat/lng bounding
-- box query as fallback).
-- CREATE EXTENSION IF NOT EXISTS cube;
-- CREATE EXTENSION IF NOT EXISTS earthdistance;

CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,     -- bcrypt hash
    phone VARCHAR(20),
    latitude FLOAT DEFAULT 0,
    longitude FLOAT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE drivers (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,     -- bcrypt hash
    phone VARCHAR(20),
    license_plate VARCHAR(20),
    latitude FLOAT DEFAULT 0,
    longitude FLOAT DEFAULT 0,
    is_available BOOLEAN DEFAULT FALSE,
    earnings DECIMAL(10, 2) DEFAULT 0.00,
    rating FLOAT DEFAULT 4.8,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE restaurants (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    cuisine VARCHAR(50),
    rating FLOAT DEFAULT 4.5,
    image_url VARCHAR(255),
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

CREATE TABLE menu_items (
    id SERIAL PRIMARY KEY,
    restaurant_id INTEGER REFERENCES restaurants(id) ON DELETE CASCADE,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    is_veg BOOLEAN DEFAULT TRUE
);

CREATE TABLE food_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    restaurant_id INTEGER REFERENCES restaurants(id),
    driver_id INTEGER REFERENCES drivers(id),
    status VARCHAR(20) DEFAULT 'preparing',
    total_amount DECIMAL(10, 2),
    delivery_address VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE rides (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    driver_id INTEGER REFERENCES drivers(id),
    food_order_id INTEGER REFERENCES food_orders(id),
    pickup_lat FLOAT NOT NULL,
    pickup_lng FLOAT NOT NULL,
    drop_lat FLOAT NOT NULL,
    drop_lng FLOAT NOT NULL,
    status VARCHAR(20) DEFAULT 'requested',
    fare DECIMAL(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --- Indexes ---

-- Speeds up the SQL fallback matcher's bounding-box pre-filter and any
-- "show available drivers" admin/debug queries.
CREATE INDEX idx_drivers_available_location
    ON drivers (is_available, latitude, longitude)
    WHERE is_available = TRUE;

CREATE INDEX idx_menu_items_restaurant ON menu_items (restaurant_id);
CREATE INDEX idx_food_orders_user ON food_orders (user_id);
CREATE INDEX idx_food_orders_driver ON food_orders (driver_id);
CREATE INDEX idx_rides_user ON rides (user_id);
CREATE INDEX idx_rides_driver ON rides (driver_id);
CREATE INDEX idx_rides_created_at ON rides (created_at DESC);

-- email already has a UNIQUE constraint above, which Postgres backs with a
-- unique btree index automatically - login lookups by email are indexed.

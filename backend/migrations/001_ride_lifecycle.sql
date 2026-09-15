-- 001_ride_lifecycle.sql
--
-- Additive, idempotent migration: safe to re-run, keeps existing data.
-- Existing database:  python init_db.py --migrate
-- Fresh database:     python init_db.py   (schema.sql, then this file)
--
-- Adds what the persisted ride lifecycle, driver offers, payment
-- confirmation and the admin dashboard need. Ride rows created before this
-- migration keep request_id = NULL; the application treats them as legacy
-- (never resumed) and they are excluded from the uniqueness rules below.

-- --- Admin role (default: not an admin; only set via set_admin.py) ---
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_admin BOOLEAN NOT NULL DEFAULT FALSE;

-- --- Ride lifecycle ---
ALTER TABLE rides ADD COLUMN IF NOT EXISTS request_id UUID;
ALTER TABLE rides ADD COLUMN IF NOT EXISTS progress VARCHAR(30);
ALTER TABLE rides ADD COLUMN IF NOT EXISTS details JSONB;
ALTER TABLE rides ADD COLUMN IF NOT EXISTS cancel_reason VARCHAR(30);
ALTER TABLE rides ADD COLUMN IF NOT EXISTS accepted_at TIMESTAMPTZ;
ALTER TABLE rides ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
ALTER TABLE rides ADD COLUMN IF NOT EXISTS paid_at TIMESTAMPTZ;
ALTER TABLE rides ADD COLUMN IF NOT EXISTS paid_amount DECIMAL(10, 2);
ALTER TABLE rides ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'rides_status_check') THEN
        -- NOT VALID: enforced for new and updated rows without failing on
        -- any unexpected value already stored in old rows.
        ALTER TABLE rides ADD CONSTRAINT rides_status_check
            CHECK (status IN ('requested', 'accepted', 'in_progress', 'completed', 'cancelled')) NOT VALID;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_rides_request_id ON rides (request_id);

-- A driver can be on at most one active ride.
CREATE UNIQUE INDEX IF NOT EXISTS uq_rides_active_driver ON rides (driver_id)
    WHERE status IN ('accepted', 'in_progress') AND request_id IS NOT NULL;

-- A rider can have at most one open ride.
CREATE UNIQUE INDEX IF NOT EXISTS uq_rides_open_rider ON rides (user_id)
    WHERE status IN ('requested', 'accepted', 'in_progress') AND request_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rides_status ON rides (status);

-- --- Driver offers ---
CREATE TABLE IF NOT EXISTS ride_offers (
    id SERIAL PRIMARY KEY,
    ride_id INTEGER NOT NULL REFERENCES rides(id),
    driver_id INTEGER NOT NULL REFERENCES drivers(id),
    status VARCHAR(20) NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'accepted', 'rejected', 'expired')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ NOT NULL,
    responded_at TIMESTAMPTZ
);

-- A driver holds at most one pending offer, and a ride has at most one
-- pending offer. These make "reserve this driver for this ride" atomic.
CREATE UNIQUE INDEX IF NOT EXISTS uq_ride_offers_pending_driver ON ride_offers (driver_id)
    WHERE status = 'pending';
CREATE UNIQUE INDEX IF NOT EXISTS uq_ride_offers_pending_ride ON ride_offers (ride_id)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_ride_offers_ride ON ride_offers (ride_id);
CREATE INDEX IF NOT EXISTS idx_ride_offers_status_expires ON ride_offers (status, expires_at);

"""
Ride lifecycle and driver offers, persisted in PostgreSQL.

PostgreSQL is the single source of truth for ride state and offer state
(migrations/001_ride_lifecycle.sql). Socket.IO events only report what has
been committed here.

  rides.status        requested -> accepted -> in_progress -> completed
                      requested -> cancelled   (no driver found / request expired)
  rides.progress      trip step shown in the UI, forward-only:
                      accepted, heading_to_pickup, [at_restaurant, food_picked,]
                      picked_up, trip_started, completed
  rides.paid_at       set once when the driver confirms payment was collected
                      (a confirmation only - no payment is processed)
  ride_offers.status  pending -> accepted | rejected | expired

Every transition is a guarded UPDATE inside a transaction that first locks
the ride row (SELECT ... FOR UPDATE), so a repeated or concurrent event (two
accepts, a duplicate "completed", a second payment confirmation) can only
succeed once. Partial unique indexes are the durable backstop: one pending
offer per driver, one pending offer per ride, one active ride per driver,
one open ride per rider. Lock order is always rides row, then ride_offers
rows.
"""
import logging
import uuid

from psycopg2 import errors as pg_errors
from psycopg2.extras import Json

from db import get_db_conn

logger = logging.getLogger("lyft.rides")

ACTIVE_STATUSES = ("accepted", "in_progress")
PROGRESS_STEPS = (
    "accepted",
    "heading_to_pickup",
    "at_restaurant",
    "food_picked",
    "picked_up",
    "trip_started",
    "completed",
)
FOOD_ONLY_STEPS = {"at_restaurant", "food_picked"}
_STATUS_FOR_STEP = {"trip_started": "in_progress", "completed": "completed"}  # other steps: "accepted"

_SELECT_RIDE = """
    SELECT r.id, r.request_id, r.user_id, r.driver_id, r.status, r.progress, r.fare,
           r.pickup_lat, r.pickup_lng, r.drop_lat, r.drop_lng, r.details, r.cancel_reason,
           r.paid_at, r.paid_amount, d.name, d.latitude, d.longitude
    FROM rides r
    LEFT JOIN drivers d ON d.id = r.driver_id
"""
_RIDE_KEYS = (
    "id", "request_id", "user_id", "driver_id", "status", "progress", "fare",
    "pickup_lat", "pickup_lng", "drop_lat", "drop_lng", "details", "cancel_reason",
    "paid_at", "paid_amount", "driver_name", "driver_lat", "driver_lng",
)
_OPEN_RIDE_CONDITION = "(r.status IN ('requested', 'accepted', 'in_progress') OR (r.status = 'completed' AND r.paid_at IS NULL))"


def _ride(row) -> dict | None:
    if not row:
        return None
    ride = dict(zip(_RIDE_KEYS, row))
    ride["details"] = ride["details"] or {}
    return ride


def _lock_ride(cur, request_id: str) -> dict | None:
    cur.execute(_SELECT_RIDE + " WHERE r.request_id = %s FOR UPDATE OF r", (request_id,))
    return _ride(cur.fetchone())


# --- Reads ---------------------------------------------------------------

def get_ride(request_id: str) -> dict | None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_SELECT_RIDE + " WHERE r.request_id = %s", (request_id,))
            return _ride(cur.fetchone())


def get_open_ride_for_rider(rider_id: int) -> dict | None:
    """The rider's current ride, including a completed ride whose payment
    hasn't been confirmed yet (used to restore the page after a reconnect)."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _SELECT_RIDE + f" WHERE r.user_id = %s AND r.request_id IS NOT NULL AND {_OPEN_RIDE_CONDITION}"
                " ORDER BY r.id DESC LIMIT 1",
                (rider_id,),
            )
            return _ride(cur.fetchone())


def get_open_ride_for_driver(driver_id: int) -> dict | None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                _SELECT_RIDE + f" WHERE r.driver_id = %s AND r.request_id IS NOT NULL AND {_OPEN_RIDE_CONDITION}"
                " ORDER BY r.id DESC LIMIT 1",
                (driver_id,),
            )
            return _ride(cur.fetchone())


def get_active_request_id_for_driver(driver_id: int) -> str | None:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT request_id FROM rides WHERE driver_id = %s AND request_id IS NOT NULL"
                " AND status IN ('accepted', 'in_progress') LIMIT 1",
                (driver_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None


def driver_has_active_ride(driver_id: int) -> bool:
    return get_active_request_id_for_driver(driver_id) is not None


def has_pending_offer(ride_id: int) -> bool:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM ride_offers WHERE ride_id = %s AND status = 'pending' AND expires_at > NOW()",
                (ride_id,),
            )
            return cur.fetchone() is not None


def pending_offer_driver_ids() -> set[int]:
    """Drivers currently holding an unexpired offer - not available to be
    offered another ride."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT driver_id FROM ride_offers WHERE status = 'pending' AND expires_at > NOW()")
            return {row[0] for row in cur.fetchall()}


def excluded_driver_ids(ride_id: int) -> set[int]:
    """Drivers who already rejected, or let expire, an offer for this ride."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT driver_id FROM ride_offers WHERE ride_id = %s AND status IN ('rejected', 'expired')",
                (ride_id,),
            )
            return {row[0] for row in cur.fetchall()}


def get_pending_offer_for_driver(driver_id: int):
    """(ride, offer) for the driver's unexpired pending offer, or None."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.id, o.driver_id, EXTRACT(EPOCH FROM (o.expires_at - NOW())), r.request_id
                FROM ride_offers o JOIN rides r ON r.id = o.ride_id
                WHERE o.driver_id = %s AND o.status = 'pending' AND o.expires_at > NOW()
                  AND r.status = 'requested'
                """,
                (driver_id,),
            )
            row = cur.fetchone()
    if not row:
        return None
    ride = get_ride(row[3])
    if not ride:
        return None
    return ride, {"id": row[0], "driver_id": row[1], "expires_in_seconds": max(0, int(row[2]))}


def due_offer_ids() -> list[int]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM ride_offers WHERE status = 'pending' AND expires_at <= NOW()")
            return [row[0] for row in cur.fetchall()]


def stale_requested_ids(max_age_seconds: int) -> list[str]:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT request_id FROM rides WHERE status = 'requested' AND request_id IS NOT NULL"
                " AND updated_at < NOW() - make_interval(secs => %s)",
                (max_age_seconds,),
            )
            return [row[0] for row in cur.fetchall()]


def requested_ids_without_offer(min_age_seconds: int) -> list[str]:
    """Requested rides nobody is currently offering (e.g. the server restarted
    before an offer was made, or an expiry timer was lost)."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.request_id FROM rides r
                WHERE r.status = 'requested' AND r.request_id IS NOT NULL
                  AND r.updated_at < NOW() - make_interval(secs => %s)
                  AND NOT EXISTS (
                      SELECT 1 FROM ride_offers o
                      WHERE o.ride_id = r.id AND o.status = 'pending' AND o.expires_at > NOW()
                  )
                """,
                (min_age_seconds,),
            )
            return [row[0] for row in cur.fetchall()]


# --- Transitions -----------------------------------------------------------

def create_ride_request(rider_id, pickup_lat, pickup_lng, drop_lat, drop_lng, fare, details) -> dict | None:
    """Persists a new ride in 'requested'. Returns None if the rider already
    has an open ride."""
    request_id = str(uuid.uuid4())
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO rides (request_id, user_id, pickup_lat, pickup_lng, drop_lat, drop_lng,
                                       fare, status, details, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'requested', %s, NOW())
                    RETURNING id
                    """,
                    (request_id, rider_id, pickup_lat, pickup_lng, drop_lat, drop_lng, fare, Json(details)),
                )
                ride_id = cur.fetchone()[0]
            conn.commit()
    except pg_errors.UniqueViolation:
        return None
    return {"id": ride_id, "request_id": request_id}


def create_offer(request_id: str, driver_id: int, timeout_seconds: int):
    """Reserves `driver_id` for this ride until the offer expires.
    Returns (offer, None) or (None, reason)."""
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                ride = _lock_ride(cur, request_id)
                if not ride:
                    conn.rollback()
                    return None, "ride_not_found"
                if ride["status"] != "requested":
                    conn.rollback()
                    return None, "ride_not_requested"
                # Mark overdue offers expired first, so the "one pending offer
                # per driver / per ride" indexes reflect reality.
                cur.execute(
                    """
                    UPDATE ride_offers SET status = 'expired', responded_at = NOW()
                    WHERE status = 'pending' AND expires_at <= NOW() AND (ride_id = %s OR driver_id = %s)
                    """,
                    (ride["id"], driver_id),
                )
                cur.execute("SELECT 1 FROM ride_offers WHERE ride_id = %s AND status = 'pending'", (ride["id"],))
                if cur.fetchone():
                    conn.commit()
                    return None, "ride_has_pending_offer"
                cur.execute(
                    "SELECT 1 FROM rides WHERE driver_id = %s AND request_id IS NOT NULL"
                    " AND status IN ('accepted', 'in_progress')",
                    (driver_id,),
                )
                if cur.fetchone():
                    conn.commit()
                    return None, "driver_on_trip"
                try:
                    cur.execute(
                        """
                        INSERT INTO ride_offers (ride_id, driver_id, status, expires_at)
                        VALUES (%s, %s, 'pending', NOW() + make_interval(secs => %s))
                        RETURNING id, EXTRACT(EPOCH FROM (expires_at - NOW()))
                        """,
                        (ride["id"], driver_id, timeout_seconds),
                    )
                except pg_errors.UniqueViolation:
                    conn.rollback()
                    return None, "driver_has_pending_offer"
                offer_id, expires_in = cur.fetchone()
            conn.commit()
    except pg_errors.DeadlockDetected:
        return None, "conflict"
    return {"id": offer_id, "driver_id": driver_id, "expires_in_seconds": max(0, int(round(expires_in)))}, None


def accept_offer(request_id: str, driver_id: int):
    """Atomically moves the ride requested -> accepted for this driver.
    Only succeeds if the driver holds an unexpired pending offer for it.
    Returns (ride, None) or (None, reason)."""
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                ride = _lock_ride(cur, request_id)
                if not ride:
                    conn.rollback()
                    return None, "ride_not_found"
                cur.execute(
                    """
                    SELECT id, status, expires_at <= NOW() FROM ride_offers
                    WHERE ride_id = %s AND driver_id = %s
                    ORDER BY id DESC LIMIT 1
                    FOR UPDATE
                    """,
                    (ride["id"], driver_id),
                )
                offer = cur.fetchone()
                if not offer:
                    conn.rollback()
                    return None, "not_offered"
                offer_id, offer_status, overdue = offer
                if offer_status == "pending" and overdue:
                    cur.execute(
                        "UPDATE ride_offers SET status = 'expired', responded_at = NOW() WHERE id = %s",
                        (offer_id,),
                    )
                    conn.commit()
                    return None, "offer_expired"
                if offer_status != "pending":
                    conn.rollback()
                    return None, {
                        "expired": "offer_expired",
                        "rejected": "offer_rejected",
                        "accepted": "already_accepted",
                    }.get(offer_status, "not_offered")
                if ride["status"] != "requested":
                    conn.rollback()
                    return None, "ride_no_longer_available"

                cur.execute(
                    "UPDATE ride_offers SET status = 'accepted', responded_at = NOW() WHERE id = %s AND status = 'pending'",
                    (offer_id,),
                )
                try:
                    cur.execute(
                        """
                        UPDATE rides
                        SET status = 'accepted', progress = 'accepted', driver_id = %s,
                            accepted_at = NOW(), updated_at = NOW()
                        WHERE id = %s AND status = 'requested'
                        """,
                        (driver_id, ride["id"]),
                    )
                except pg_errors.UniqueViolation:
                    conn.rollback()
                    return None, "driver_on_trip"
                if cur.rowcount != 1:
                    conn.rollback()
                    return None, "ride_no_longer_available"
                # Keeps the SQL fallback matcher from picking a driver on a trip.
                cur.execute(
                    "UPDATE drivers SET is_available = FALSE WHERE id = %s RETURNING name, latitude, longitude",
                    (driver_id,),
                )
                driver_row = cur.fetchone()
                if not driver_row:
                    conn.rollback()
                    return None, "driver_not_found"
            conn.commit()
    except pg_errors.DeadlockDetected:
        return None, "conflict"

    ride.update(
        status="accepted", progress="accepted", driver_id=driver_id,
        driver_name=driver_row[0], driver_lat=driver_row[1], driver_lng=driver_row[2],
    )
    return ride, None


def reject_offer(request_id: str, driver_id: int) -> bool:
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            ride = _lock_ride(cur, request_id)
            if not ride:
                conn.rollback()
                return False
            cur.execute(
                """
                UPDATE ride_offers SET status = 'rejected', responded_at = NOW()
                WHERE ride_id = %s AND driver_id = %s AND status = 'pending'
                RETURNING id
                """,
                (ride["id"], driver_id),
            )
            rejected = cur.fetchone() is not None
        conn.commit()
    return rejected


def decline_pending_offers(driver_id: int) -> list[str]:
    """Rejects every pending offer the driver holds (e.g. they went offline).
    Returns the affected request ids."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT r.request_id FROM ride_offers o JOIN rides r ON r.id = o.ride_id"
                " WHERE o.driver_id = %s AND o.status = 'pending'",
                (driver_id,),
            )
            request_ids = [row[0] for row in cur.fetchall()]
    return [rid for rid in request_ids if reject_offer(rid, driver_id)]


def expire_offer(offer_id: int) -> dict | None:
    """Marks a pending offer expired if its deadline has passed.
    Returns {request_id, driver_id} if this call expired it, else None."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT r.request_id FROM ride_offers o JOIN rides r ON r.id = o.ride_id WHERE o.id = %s",
                (offer_id,),
            )
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return None
            request_id = row[0]
            if not _lock_ride(cur, request_id):
                conn.rollback()
                return None
            cur.execute(
                """
                UPDATE ride_offers SET status = 'expired', responded_at = NOW()
                WHERE id = %s AND status = 'pending' AND expires_at <= NOW()
                RETURNING driver_id
                """,
                (offer_id,),
            )
            expired = cur.fetchone()
        conn.commit()
    return {"request_id": request_id, "driver_id": expired[0]} if expired else None


def cancel_unfulfilled(request_id: str, reason: str) -> bool:
    """requested -> cancelled, only while no unexpired offer is outstanding."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            ride = _lock_ride(cur, request_id)
            if not ride or ride["status"] != "requested":
                conn.rollback()
                return False
            cur.execute(
                """
                UPDATE rides SET status = 'cancelled', cancel_reason = %s, updated_at = NOW()
                WHERE id = %s AND status = 'requested'
                  AND NOT EXISTS (
                      SELECT 1 FROM ride_offers
                      WHERE ride_id = %s AND status = 'pending' AND expires_at > NOW()
                  )
                """,
                (reason, ride["id"], ride["id"]),
            )
            cancelled = cur.rowcount == 1
            if cancelled:
                cur.execute(
                    "UPDATE ride_offers SET status = 'expired', responded_at = NOW()"
                    " WHERE ride_id = %s AND status = 'pending'",
                    (ride["id"],),
                )
        conn.commit()
    return cancelled


def update_progress(request_id: str, driver_id: int, step: str):
    """Records a trip step reported by the ride's driver (or the simulator
    acting for them). Steps only move forward; duplicates, regressions,
    food steps on a non-food ride and steps for inactive rides are refused.
    Returns (ride, None) or (None, reason)."""
    if step not in PROGRESS_STEPS or step == "accepted":
        return None, "invalid_status"
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            ride = _lock_ride(cur, request_id)
            if not ride:
                conn.rollback()
                return None, "ride_not_found"
            if ride["driver_id"] != driver_id:
                conn.rollback()
                return None, "not_your_ride"
            if ride["status"] not in ACTIVE_STATUSES:
                conn.rollback()
                return None, "ride_not_active"
            if step in FOOD_ONLY_STEPS and not ride["details"].get("restaurant"):
                conn.rollback()
                return None, "not_a_food_ride"
            current = ride["progress"] if ride["progress"] in PROGRESS_STEPS else "accepted"
            if PROGRESS_STEPS.index(step) <= PROGRESS_STEPS.index(current):
                conn.rollback()
                return None, "stale_or_duplicate"

            new_status = _STATUS_FOR_STEP.get(step, "accepted")
            cur.execute(
                """
                UPDATE rides
                SET progress = %s, status = %s, updated_at = NOW(),
                    completed_at = CASE WHEN %s = 'completed' THEN NOW() ELSE completed_at END
                WHERE id = %s
                """,
                (step, new_status, new_status, ride["id"]),
            )
            if step == "completed":
                # The trip ends at the drop point; this is where the driver
                # re-enters the pool once payment is confirmed.
                cur.execute(
                    "UPDATE drivers SET latitude = %s, longitude = %s WHERE id = %s",
                    (ride["drop_lat"], ride["drop_lng"], driver_id),
                )
        conn.commit()
    ride.update(progress=step, status=new_status)
    return ride, None


def confirm_payment(request_id: str, driver_id: int, claimed_amount):
    """Driver confirms they collected the fare (cash, outside the system).
    The amount credited is always the stored fare; a client-sent amount that
    differs is refused. Exactly once per ride, only for the ride's driver,
    only after completion. Returns (result, None) or (None, reason)."""
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            ride = _lock_ride(cur, request_id)
            if not ride:
                conn.rollback()
                return None, "ride_not_found"
            if ride["driver_id"] != driver_id:
                conn.rollback()
                return None, "not_your_ride"
            if ride["paid_at"] is not None:
                conn.rollback()
                return None, "already_confirmed"
            if ride["status"] != "completed":
                conn.rollback()
                return None, "ride_not_completed"
            fare = float(ride["fare"] or 0)
            if claimed_amount is not None:
                try:
                    claimed = float(claimed_amount)
                except (TypeError, ValueError):
                    claimed = None
                if claimed is None or abs(claimed - fare) > 0.01:
                    conn.rollback()
                    return None, "amount_mismatch"

            cur.execute(
                """
                UPDATE rides SET paid_at = NOW(), paid_amount = fare, updated_at = NOW()
                WHERE id = %s AND status = 'completed' AND paid_at IS NULL
                """,
                (ride["id"],),
            )
            if cur.rowcount != 1:
                conn.rollback()
                return None, "already_confirmed"
            cur.execute(
                """
                UPDATE drivers SET earnings = earnings + %s, is_available = TRUE
                WHERE id = %s RETURNING earnings, latitude, longitude
                """,
                (fare, driver_id),
            )
            earnings, lat, lng = cur.fetchone()
        conn.commit()
    return {"amount": fare, "total_earnings": float(earnings), "lat": lat, "lng": lng}, None

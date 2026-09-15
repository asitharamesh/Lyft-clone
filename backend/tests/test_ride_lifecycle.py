"""Ride lifecycle, offers and payment confirmation against a real Postgres
(see conftest.py - skipped unless LYFT_TEST_DB_NAME is set)."""
import threading
import time

import pytest

from db import get_db_conn
from services import ride_service

pytestmark = pytest.mark.usefixtures("seeded_db")

PICKUP = (12.9720, 77.5950)
DROP = (12.9800, 77.6000)
FARE = 136


def _request(rider_id=1, restaurant=None):
    details = {"user_name": f"Rider {rider_id}", "pickup_name": "P", "drop_name": "D", "restaurant": restaurant}
    ride = ride_service.create_ride_request(rider_id, *PICKUP, *DROP, FARE, details)
    assert ride is not None
    return ride["request_id"]


def _offer(request_id, driver_id, timeout=20):
    offer, reason = ride_service.create_offer(request_id, driver_id, timeout)
    assert offer is not None, reason
    return offer


def _accepted(request_id, driver_id):
    _offer(request_id, driver_id)
    ride, reason = ride_service.accept_offer(request_id, driver_id)
    assert ride is not None, reason
    return ride


def _row(request_id):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, progress, driver_id, paid_at, paid_amount, cancel_reason FROM rides WHERE request_id = %s",
                (request_id,),
            )
            return dict(zip(("status", "progress", "driver_id", "paid_at", "paid_amount", "cancel_reason"), cur.fetchone()))


def _scalar(sql, params=()):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()[0]


def _run_concurrently(fn, arg_list):
    barrier = threading.Barrier(len(arg_list))
    results = [None] * len(arg_list)

    def worker(index, args):
        barrier.wait()
        results[index] = fn(*args)

    threads = [threading.Thread(target=worker, args=(i, a)) for i, a in enumerate(arg_list)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


# --- requested / offers / acceptance ---------------------------------------

def test_request_is_persisted_as_requested():
    request_id = _request()
    assert _row(request_id)["status"] == "requested"


def test_rider_cannot_open_a_second_ride():
    _request(rider_id=1)
    assert ride_service.create_ride_request(1, *PICKUP, *DROP, FARE, {}) is None


def test_offer_acceptance_assigns_ride_to_offered_driver():
    request_id = _request()
    _offer(request_id, 1)
    ride, reason = ride_service.accept_offer(request_id, 1)
    assert reason is None
    assert ride["driver_id"] == 1 and ride["driver_name"] == "Driver 1"
    row = _row(request_id)
    assert row["status"] == "accepted" and row["driver_id"] == 1 and row["progress"] == "accepted"
    assert _scalar("SELECT is_available FROM drivers WHERE id = 1") is False


def test_driver_who_was_not_offered_cannot_accept():
    request_id = _request()
    _offer(request_id, 1)
    ride, reason = ride_service.accept_offer(request_id, 2)
    assert ride is None and reason == "not_offered"
    assert _row(request_id)["status"] == "requested"


def test_concurrent_accepts_assign_the_ride_exactly_once():
    request_id = _request()
    _offer(request_id, 1)
    # 6 simultaneous accepts from the offered driver (double taps / retries)
    # and 6 from a driver who was never offered the ride.
    attempts = [(request_id, 1)] * 6 + [(request_id, 2)] * 6
    results = _run_concurrently(ride_service.accept_offer, attempts)

    winners = [ride for ride, _ in results if ride is not None]
    assert len(winners) == 1 and winners[0]["driver_id"] == 1
    reasons = sorted(reason for ride, reason in results if ride is None)
    assert reasons.count("already_accepted") == 5 and reasons.count("not_offered") == 6
    assert _scalar("SELECT COUNT(*) FROM rides WHERE request_id = %s AND status = 'accepted'", (request_id,)) == 1
    assert _scalar("SELECT COUNT(*) FROM ride_offers WHERE status = 'accepted'") == 1


def test_late_accept_from_previous_driver_loses_to_current_one():
    request_id = _request()
    _offer(request_id, 1, timeout=1)
    time.sleep(1.3)
    _offer(request_id, 2)  # driver 1's offer has lapsed; the ride moves on

    late, winner = _run_concurrently(ride_service.accept_offer, [(request_id, 1), (request_id, 2)])
    assert late == (None, "offer_expired")
    assert winner[0] is not None and winner[0]["driver_id"] == 2
    assert _row(request_id)["driver_id"] == 2


def test_rejected_offer_cannot_then_be_accepted():
    request_id = _request()
    _offer(request_id, 1)
    assert ride_service.reject_offer(request_id, 1) is True
    assert ride_service.accept_offer(request_id, 1) == (None, "offer_rejected")
    assert ride_service.excluded_driver_ids(_scalar("SELECT id FROM rides WHERE request_id = %s", (request_id,))) == {1}


# --- expiry ------------------------------------------------------------------

def test_expired_offer_cannot_be_accepted_and_is_cleaned_up():
    request_id = _request()
    offer = _offer(request_id, 1, timeout=1)
    assert ride_service.pending_offer_driver_ids() == {1}
    time.sleep(1.3)

    # The deadline is enforced even if no timer has run yet.
    assert ride_service.pending_offer_driver_ids() == set()
    assert ride_service.due_offer_ids() == [offer["id"]]
    assert ride_service.accept_offer(request_id, 1) == (None, "offer_expired")

    # Accepting recorded the expiry; the sweep finds nothing left to do.
    assert ride_service.due_offer_ids() == []
    assert ride_service.expire_offer(offer["id"]) is None
    assert _scalar("SELECT status FROM ride_offers WHERE id = %s", (offer["id"],)) == "expired"
    assert _row(request_id)["status"] == "requested"


def test_expire_offer_is_idempotent_and_only_after_deadline():
    request_id = _request()
    offer = _offer(request_id, 1, timeout=1)
    assert ride_service.expire_offer(offer["id"]) is None  # not due yet
    time.sleep(1.3)
    assert ride_service.expire_offer(offer["id"]) == {"request_id": request_id, "driver_id": 1}
    assert ride_service.expire_offer(offer["id"]) is None


# --- one offer per driver / per ride -----------------------------------------

def test_driver_holding_an_offer_gets_no_second_offer():
    first = _request(rider_id=1)
    second = _request(rider_id=2)
    _offer(first, 1)

    assert ride_service.create_offer(second, 1, 20) == (None, "driver_has_pending_offer")
    assert ride_service.pending_offer_driver_ids() == {1}

    ride_service.reject_offer(first, 1)
    offer, reason = ride_service.create_offer(second, 1, 20)
    assert offer is not None and reason is None


def test_ride_has_only_one_outstanding_offer():
    request_id = _request()
    _offer(request_id, 1)
    assert ride_service.create_offer(request_id, 2, 20) == (None, "ride_has_pending_offer")


def test_driver_on_a_trip_is_not_offered_another_ride():
    first = _request(rider_id=1)
    _accepted(first, 1)
    second = _request(rider_id=2)
    assert ride_service.create_offer(second, 1, 20) == (None, "driver_on_trip")
    assert ride_service.driver_has_active_ride(1) is True


def test_cancel_unfulfilled_waits_for_outstanding_offer():
    request_id = _request()
    _offer(request_id, 1, timeout=1)
    assert ride_service.cancel_unfulfilled(request_id, "no_driver") is False
    time.sleep(1.3)
    assert ride_service.cancel_unfulfilled(request_id, "no_driver") is True
    row = _row(request_id)
    assert row["status"] == "cancelled" and row["cancel_reason"] == "no_driver"
    assert ride_service.create_offer(request_id, 2, 20) == (None, "ride_not_requested")


# --- trip progress -----------------------------------------------------------

def test_trip_progress_is_forward_only_and_persisted():
    request_id = _request()
    _accepted(request_id, 1)

    assert ride_service.update_progress(request_id, 1, "heading_to_pickup")[1] is None
    assert ride_service.update_progress(request_id, 1, "at_restaurant") == (None, "not_a_food_ride")
    assert ride_service.update_progress(request_id, 2, "picked_up") == (None, "not_your_ride")
    assert ride_service.update_progress(request_id, 1, "picked_up")[1] is None
    assert ride_service.update_progress(request_id, 1, "heading_to_pickup") == (None, "stale_or_duplicate")
    assert ride_service.update_progress(request_id, 1, "picked_up") == (None, "stale_or_duplicate")
    assert ride_service.update_progress(request_id, 1, "flying") == (None, "invalid_status")

    ride_service.update_progress(request_id, 1, "trip_started")
    assert _row(request_id)["status"] == "in_progress"

    ride_service.update_progress(request_id, 1, "completed")
    row = _row(request_id)
    assert row["status"] == "completed" and row["progress"] == "completed"
    assert ride_service.update_progress(request_id, 1, "completed") == (None, "ride_not_active")
    # The trip ended at the drop point.
    assert _scalar("SELECT latitude FROM drivers WHERE id = 1") == DROP[0]


def test_food_steps_are_allowed_on_food_rides():
    request_id = _request(restaurant={"name": "Truffles", "lat": 12.9719, "lng": 77.6011})
    _accepted(request_id, 1)
    for step in ("heading_to_pickup", "at_restaurant", "food_picked", "picked_up"):
        assert ride_service.update_progress(request_id, 1, step)[1] is None


# --- recovery ----------------------------------------------------------------

def test_open_ride_is_recoverable_through_every_stage():
    request_id = _request()
    assert ride_service.get_open_ride_for_rider(1)["status"] == "requested"

    _offer(request_id, 1)
    ride, offer = ride_service.get_pending_offer_for_driver(1)
    assert ride["request_id"] == request_id and 0 < offer["expires_in_seconds"] <= 20

    ride_service.accept_offer(request_id, 1)
    assert ride_service.get_pending_offer_for_driver(1) is None
    for_driver = ride_service.get_open_ride_for_driver(1)
    assert for_driver["request_id"] == request_id and for_driver["status"] == "accepted"
    assert ride_service.get_open_ride_for_rider(1)["driver_name"] == "Driver 1"

    ride_service.update_progress(request_id, 1, "trip_started")
    assert ride_service.get_open_ride_for_rider(1)["progress"] == "trip_started"

    ride_service.update_progress(request_id, 1, "completed")
    assert ride_service.get_open_ride_for_rider(1)["status"] == "completed"
    assert ride_service.get_open_ride_for_driver(1)["status"] == "completed"

    ride_service.confirm_payment(request_id, 1, FARE)
    assert ride_service.get_open_ride_for_rider(1) is None
    assert ride_service.get_open_ride_for_driver(1) is None


def test_legacy_rows_without_request_id_are_left_alone():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO rides (user_id, driver_id, pickup_lat, pickup_lng, drop_lat, drop_lng, fare, status)"
                " VALUES (1, 1, 1, 1, 2, 2, 50, 'accepted'), (1, 1, 1, 1, 2, 2, 60, 'accepted')"
            )
        conn.commit()
    assert ride_service.get_open_ride_for_rider(1) is None
    assert ride_service.driver_has_active_ride(1) is False
    request_id = _request(rider_id=1)
    assert _accepted(request_id, 1)["driver_id"] == 1


# --- payment confirmation ------------------------------------------------------

def _completed_ride(driver_id=1):
    request_id = _request()
    _accepted(request_id, driver_id)
    ride_service.update_progress(request_id, driver_id, "completed")
    return request_id


def test_payment_confirmation_rules():
    request_id = _request()
    _accepted(request_id, 1)
    assert ride_service.confirm_payment(request_id, 1, FARE) == (None, "ride_not_completed")

    ride_service.update_progress(request_id, 1, "completed")
    assert ride_service.confirm_payment(request_id, 2, FARE) == (None, "not_your_ride")
    assert ride_service.confirm_payment(request_id, 1, 999999) == (None, "amount_mismatch")
    assert ride_service.confirm_payment(request_id, 1, "lots") == (None, "amount_mismatch")
    assert _scalar("SELECT earnings FROM drivers WHERE id = 1") == 0

    result, reason = ride_service.confirm_payment(request_id, 1, FARE)
    assert reason is None and result["amount"] == FARE and result["total_earnings"] == FARE
    row = _row(request_id)
    assert row["paid_at"] is not None and row["paid_amount"] == FARE

    assert ride_service.confirm_payment(request_id, 1, FARE) == (None, "already_confirmed")
    assert _scalar("SELECT earnings FROM drivers WHERE id = 1") == FARE
    assert _scalar("SELECT is_available FROM drivers WHERE id = 1") is True


def test_payment_amount_is_optional_but_always_the_stored_fare():
    request_id = _completed_ride()
    result, _ = ride_service.confirm_payment(request_id, 1, None)
    assert result["amount"] == FARE


def test_concurrent_payment_confirmations_credit_once():
    request_id = _completed_ride()
    results = _run_concurrently(ride_service.confirm_payment, [(request_id, 1, FARE)] * 8)
    assert sum(1 for result, _ in results if result) == 1
    assert sorted({reason for result, reason in results if not result}) == ["already_confirmed"]
    assert _scalar("SELECT earnings FROM drivers WHERE id = 1") == FARE


def test_signup_style_insert_is_not_admin_by_default():
    assert _scalar("SELECT COUNT(*) FROM users WHERE is_admin") == 0

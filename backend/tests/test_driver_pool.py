"""Driver pool freshness: stale drivers must not use up matching slots, and
heartbeats must never put a driver back into the pool."""
import time
from unittest.mock import patch

import pytest

from config import Config
from services import matching_service


class FakeGeoRedis:
    """Just enough of redis-py for _candidates_from_redis."""

    def __init__(self, members, fresh_ids):
        self.members = dict(members)  # driver_id -> distance_km
        self.fresh = set(fresh_ids)
        self.searches = []

    def geosearch(self, name, longitude, latitude, radius, unit, sort, count, withdist):
        self.searches.append(count)
        ordered = sorted(self.members.items(), key=lambda kv: kv[1])
        return [[str(driver_id), dist] for driver_id, dist in ordered[:count]]

    def pipeline(self, transaction=False):
        redis = self

        class Pipe:
            def __init__(self):
                self.keys = []

            def exists(self, key):
                self.keys.append(key)

            def execute(self):
                return [int(int(k.rsplit(":", 1)[1]) in redis.fresh) for k in self.keys]

        return Pipe()

    def eval(self, script, numkeys, geo_key, heartbeat_key, member):
        driver_id = int(member)
        if driver_id not in self.fresh and driver_id in self.members:
            del self.members[driver_id]
            return 1
        return 0


def test_stale_drivers_do_not_use_up_candidate_slots():
    # Ten stale drivers right next to the pickup, one live driver 7 km away.
    members = {i: 0.1 * i for i in range(1, 11)}
    members[99] = 7.0
    fake = FakeGeoRedis(members, fresh_ids={99})

    with patch.object(matching_service, "get_redis", return_value=fake):
        candidates = matching_service._candidates_from_redis(12.97, 77.59, set(), 10)

    assert candidates == [(99, 7.0)]
    assert list(fake.members) == [99]  # stale members removed as they were found
    assert len(fake.searches) >= 2  # the search widened past the first 10


def test_excluded_drivers_do_not_use_up_candidate_slots():
    members = {i: 0.1 * i for i in range(1, 16)}
    members[99] = 6.0
    fake = FakeGeoRedis(members, fresh_ids=set(members))

    with patch.object(matching_service, "get_redis", return_value=fake):
        candidates = matching_service._candidates_from_redis(12.97, 77.59, set(range(1, 16)), 1)

    assert candidates == [(99, 6.0)]


def test_candidates_stop_when_enough_fresh_drivers_found():
    members = {i: float(i) for i in range(1, 30)}
    fake = FakeGeoRedis(members, fresh_ids=set(members))
    with patch.object(matching_service, "get_redis", return_value=fake):
        candidates = matching_service._candidates_from_redis(12.97, 77.59, set(), 10)
    assert [c[0] for c in candidates] == list(range(1, 11))
    assert fake.searches == [10]


# --- against real Redis (skipped unless LYFT_TEST_REDIS_URL is set) -----------

GEO = Config.REDIS_DRIVER_GEO_KEY


def _profiles(ids):
    return {i: {"name": f"D{i}", "rating": 4.8} for i in ids}


def test_inactive_drivers_cannot_block_an_active_nearby_driver(redis_test):
    for i in range(1, 11):  # members whose heartbeat has expired
        redis_test.geoadd(GEO, (77.5946 + i * 0.0005, 12.9716, str(i)))
    matching_service.upsert_driver_location(42, 12.9716, 77.6400)  # ~4.8 km east, fresh

    with patch.object(matching_service, "_fetch_driver_profiles", side_effect=_profiles):
        best = matching_service.find_best_driver(12.9716, 77.5946, set())

    assert best["id"] == 42
    assert redis_test.zrange(GEO, 0, -1) == ["42"]


def test_driver_whose_heartbeat_expires_is_excluded_and_swept(redis_test, monkeypatch):
    monkeypatch.setattr(Config, "DRIVER_LOCATION_TTL_SECONDS", 1)
    matching_service.upsert_driver_location(7, 12.9716, 77.5946)
    assert matching_service._candidates_from_redis(12.9716, 77.5946, set(), 10)[0][0] == 7

    time.sleep(1.5)
    assert redis_test.zscore(GEO, "7") is not None  # GEO itself keeps the member...
    assert matching_service.sweep_stale_drivers() == 1  # ...until the sweep removes it
    assert redis_test.zscore(GEO, "7") is None
    assert matching_service._candidates_from_redis(12.9716, 77.5946, set(), 10) == []


def test_heartbeat_refreshes_but_never_readds(redis_test):
    matching_service.upsert_driver_location(5, 12.97, 77.59)
    redis_test.expire("driver:heartbeat:5", 3)
    assert matching_service.refresh_driver_heartbeat(5, 12.98, 77.60) is True
    assert redis_test.ttl("driver:heartbeat:5") > 3

    matching_service.remove_driver_location(5)  # offline, or accepted a ride
    assert matching_service.refresh_driver_heartbeat(5, 12.98, 77.60) is False
    assert redis_test.zscore(GEO, "5") is None
    assert redis_test.exists("driver:heartbeat:5") == 0


def test_sweep_keeps_fresh_drivers(redis_test):
    matching_service.upsert_driver_location(1, 12.97, 77.59)
    redis_test.geoadd(GEO, (77.59, 12.97, "2"))
    assert matching_service.sweep_stale_drivers() == 1
    assert redis_test.zrange(GEO, 0, -1) == ["1"]

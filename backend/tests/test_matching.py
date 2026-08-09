import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services import matching_service  # noqa: E402


def test_closer_lower_rated_driver_can_lose_to_farther_higher_rated_driver():
    # Driver A: 1.0km away, 4.0 rating. Driver B: 1.3km away, 5.0 rating.
    # RATING_WEIGHT_KM=0.6 means a 1-star gap is worth 0.6km, so B's extra
    # 0.3km is more than offset by its 1-star rating advantage -> B should win.
    candidates = [(1, 1.0), (2, 1.3)]
    profiles = {1: {"name": "A", "rating": 4.0}, 2: {"name": "B", "rating": 5.0}}

    with patch.object(matching_service, "_candidates_from_redis", return_value=candidates), \
         patch.object(matching_service, "_fetch_driver_profiles", return_value=profiles):
        best = matching_service.find_best_driver(12.97, 77.59, exclude_ids=set())

    assert best["id"] == 2


def test_no_candidates_returns_none():
    with patch.object(matching_service, "_candidates_from_redis", return_value=[]):
        assert matching_service.find_best_driver(12.97, 77.59, exclude_ids=set()) is None


def test_falls_back_to_sql_when_redis_unavailable():
    with patch.object(matching_service, "_candidates_from_redis", return_value=None), \
         patch.object(matching_service, "_candidates_from_sql", return_value=[(5, 2.0)]) as sql_mock, \
         patch.object(matching_service, "_fetch_driver_profiles", return_value={5: {"name": "C", "rating": 4.5}}):
        best = matching_service.find_best_driver(12.97, 77.59, exclude_ids=set())

    sql_mock.assert_called_once()
    assert best["id"] == 5

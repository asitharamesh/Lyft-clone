"""
Operational counters for the admin dashboard.

Counters are plain integers in Redis under `stats:*` (shared by every backend
instance); the recent-matches list holds only short request ids, driver ids
and outcomes. Nothing here stores names, emails, coordinates or tokens.
Failures to record a stat are logged and never break the request.
"""
import json
import logging
import threading
import time

from db import get_redis

logger = logging.getLogger("lyft.stats")

PROCESS_STARTED_AT = time.time()
STATS_PREFIX = "stats:"
RECENT_MATCHES_KEY = "stats:recent_matches"
RECENT_MATCHES_LIMIT = 50


def incr(name: str, amount: int = 1) -> None:
    try:
        get_redis().incr(STATS_PREFIX + name, amount)
    except Exception as e:  # noqa: BLE001
        logger.debug("Could not record stat %s: %s", name, e)


def record_match_attempt(request_id: str, outcome: str, driver_id=None, excluded: int = 0) -> None:
    entry = {
        "at": time.time(),
        "request": str(request_id)[:8],
        "outcome": outcome,
        "driver_id": driver_id,
        "excluded": excluded,
    }
    try:
        pipe = get_redis().pipeline(transaction=False)
        pipe.lpush(RECENT_MATCHES_KEY, json.dumps(entry))
        pipe.ltrim(RECENT_MATCHES_KEY, 0, RECENT_MATCHES_LIMIT - 1)
        pipe.incr(STATS_PREFIX + "match_" + outcome)
        pipe.execute()
    except Exception as e:  # noqa: BLE001
        logger.debug("Could not record match attempt: %s", e)


def read_counters() -> dict:
    r = get_redis()
    keys = [k for k in r.scan_iter(match=STATS_PREFIX + "*", count=200) if k != RECENT_MATCHES_KEY]
    if not keys:
        return {}
    values = r.mget(keys)
    return {k[len(STATS_PREFIX):]: int(v or 0) for k, v in zip(keys, values)}


def recent_matches() -> list[dict]:
    return [json.loads(item) for item in get_redis().lrange(RECENT_MATCHES_KEY, 0, RECENT_MATCHES_LIMIT - 1)]


class LogLevelCounter(logging.Handler):
    """Counts WARNING and ERROR (and above) records in this process."""

    def __init__(self):
        super().__init__(level=logging.WARNING)
        self._lock = threading.Lock()
        self._counts = {"warning": 0, "error": 0}

    def emit(self, record):
        key = "error" if record.levelno >= logging.ERROR else "warning"
        with self._lock:
            self._counts[key] += 1

    def snapshot(self) -> dict:
        with self._lock:
            return dict(self._counts)


log_counter = LogLevelCounter()

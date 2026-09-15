"""
Connection management.

The original project opened and closed a brand-new psycopg2 connection on
*every single* HTTP request and *every single* socket event (TCP handshake +
Postgres auth each time). That's the single biggest latency/scalability
problem in the original code. Here we use a threaded connection pool so
connections are reused, and a Redis client for fast geospatial driver
lookups + caching.
"""
import logging
import threading
from contextlib import contextmanager

import psycopg2
import redis
from psycopg2 import extensions, pool

from config import Config

logger = logging.getLogger("lyft.db")

_pg_pool = None
_pool_slots = None
_redis_client = None


def _make_psycopg_green():
    """psycopg2 does its network I/O inside libpq (C), which eventlet's
    monkey patching cannot see, so without this every SQL statement blocks
    the single eventlet thread and freezes all socket traffic until it
    returns. Registering a wait callback (the same approach as the
    `psycogreen` package) makes psycopg2 yield to the eventlet hub while it
    waits on the database socket. Plain scripts and tests that don't
    monkey-patch keep normal blocking behaviour."""
    try:
        from eventlet import patcher
        from eventlet.hubs import trampoline
    except ImportError:
        return
    if not patcher.is_monkey_patched("socket"):
        return

    def _eventlet_wait(conn, timeout=None):
        while True:
            state = conn.poll()
            if state == extensions.POLL_OK:
                return
            if state == extensions.POLL_READ:
                trampoline(conn.fileno(), read=True)
            elif state == extensions.POLL_WRITE:
                trampoline(conn.fileno(), write=True)
            else:
                raise psycopg2.OperationalError(f"Bad result from poll: {state!r}")

    extensions.set_wait_callback(_eventlet_wait)
    logger.info("psycopg2 wait callback installed (cooperative under eventlet)")


def init_pool():
    """Initialize the Postgres connection pool. Call once at app startup."""
    global _pg_pool, _pool_slots
    if _pg_pool is not None:
        return _pg_pool
    _make_psycopg_green()
    # ThreadedConnectionPool raises PoolError instead of waiting when every
    # connection is borrowed. Now that queries yield, more handlers can hold
    # a connection at once, so callers wait for a free slot instead.
    _pool_slots = threading.BoundedSemaphore(Config.DB_POOL_MAX_CONN)
    _pg_pool = psycopg2.pool.ThreadedConnectionPool(
        Config.DB_POOL_MIN_CONN,
        Config.DB_POOL_MAX_CONN,
        host=Config.DB_HOST,
        port=Config.DB_PORT,
        dbname=Config.DB_NAME,
        user=Config.DB_USER,
        password=Config.DB_PASSWORD,
    )
    logger.info(
        "Postgres pool ready (min=%s, max=%s)",
        Config.DB_POOL_MIN_CONN,
        Config.DB_POOL_MAX_CONN,
    )
    return _pg_pool


def get_redis():
    """Lazily create a shared Redis client (connection-pooled internally)."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis.from_url(Config.REDIS_URL, decode_responses=True)
    return _redis_client


@contextmanager
def get_db_conn():
    """
    Context manager that borrows a connection from the pool and always
    returns it, even on error. Usage:

        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    if _pg_pool is None:
        init_pool()
    pg_pool, slots = _pg_pool, _pool_slots
    slots.acquire()
    try:
        conn = pg_pool.getconn()
    except Exception:
        slots.release()
        raise
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pg_pool.putconn(conn)
        slots.release()


def close_pool():
    global _pg_pool, _pool_slots
    if _pg_pool is not None:
        _pg_pool.closeall()
        _pg_pool = None
        _pool_slots = None


def health_check() -> dict:
    """Used by /api/health for readiness/liveness probes."""
    status = {"postgres": "down", "redis": "down"}
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        status["postgres"] = "up"
    except Exception as e:  # noqa: BLE001
        logger.warning("Postgres health check failed: %s", e)
    try:
        get_redis().ping()
        status["redis"] = "up"
    except Exception as e:  # noqa: BLE001
        logger.warning("Redis health check failed: %s", e)
    return status

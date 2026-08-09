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
from contextlib import contextmanager

import psycopg2
import redis
from psycopg2 import pool

from config import Config

logger = logging.getLogger("lyft.db")

_pg_pool = None
_redis_client = None


def init_pool():
    """Initialize the Postgres connection pool. Call once at app startup."""
    global _pg_pool
    if _pg_pool is not None:
        return _pg_pool
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
    conn = _pg_pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        _pg_pool.putconn(conn)


def close_pool():
    global _pg_pool
    if _pg_pool is not None:
        _pg_pool.closeall()
        _pg_pool = None


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

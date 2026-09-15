
import os
from datetime import timedelta

from dotenv import load_dotenv

load_dotenv()


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in ("1", "true", "yes", "on")


class Config:
    # --- Postgres ---
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "lyft_clone")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    DB_POOL_MIN_CONN = int(os.getenv("DB_POOL_MIN_CONN", "2"))
    DB_POOL_MAX_CONN = int(os.getenv("DB_POOL_MAX_CONN", "20"))

    # --- Redis (driver geo-index, caching, pub/sub for multi-instance sockets) ---
    REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_DRIVER_GEO_KEY = "drivers:geo"
    DRIVER_LOCATION_TTL_SECONDS = int(os.getenv("DRIVER_LOCATION_TTL_SECONDS", "120"))
    # How often an online browser driver refreshes its heartbeat. Must stay
    # well below DRIVER_LOCATION_TTL_SECONDS; sent to the driver page by the
    # server so it only needs to be configured here.
    DRIVER_HEARTBEAT_INTERVAL_SECONDS = int(os.getenv("DRIVER_HEARTBEAT_INTERVAL_SECONDS", "30"))
    # Live positions live in Redis; Postgres keeps a copy written at most this
    # often per driver instead of on every location ping.
    LOCATION_PERSIST_INTERVAL_SECONDS = int(os.getenv("LOCATION_PERSIST_INTERVAL_SECONDS", "15"))

    # --- Auth / JWT ---
    JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me")
    JWT_ALGORITHM = "HS256"
    JWT_EXPIRY = timedelta(hours=int(os.getenv("JWT_EXPIRY_HOURS", "12")))
    BCRYPT_ROUNDS = int(os.getenv("BCRYPT_ROUNDS", "12"))

    # Separate, non-user credential for the internal ride-simulation engine
    # (backend/ride_simulation_engine.js), which drives simulated GPS/status
    # updates for accepted rides in this demo. It authenticates with this
    # shared secret instead of a per-user JWT, and is the only identity
    # allowed to report location/status on behalf of *any* driver_id - real
    # driver/rider clients can only ever act as themselves.
    SIMULATION_SERVICE_TOKEN = os.getenv("SIMULATION_SERVICE_TOKEN", "dev-sim-token-change-me")

    # --- CORS ---
    CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:5500,http://127.0.0.1:5500").split(",")

    # --- Rate limiting ---
    RATELIMIT_STORAGE_URI = os.getenv("RATELIMIT_STORAGE_URI", REDIS_URL)
    # LOAD_TEST_MODE is only for controlled load tests (scripts/load_test.js
    # creates dozens of accounts from one IP). It replaces the login/signup
    # limits with LOAD_TEST_RATE_LIMIT, and app.py refuses to start with it
    # when FLASK_ENV=production. Normal limits are untouched otherwise.
    LOAD_TEST_MODE = _env_bool("LOAD_TEST_MODE", False)
    LOAD_TEST_RATE_LIMIT = os.getenv("LOAD_TEST_RATE_LIMIT", "10000 per minute")
    LOGIN_RATE_LIMIT = LOAD_TEST_RATE_LIMIT if LOAD_TEST_MODE else os.getenv("LOGIN_RATE_LIMIT", "10 per minute")
    SIGNUP_RATE_LIMIT = LOAD_TEST_RATE_LIMIT if LOAD_TEST_MODE else os.getenv("SIGNUP_RATE_LIMIT", "5 per minute")

    # --- Dispatch ---
    # How long a driver has to answer a ride offer before it expires and the
    # ride is offered to the next driver.
    OFFER_TIMEOUT_SECONDS = int(os.getenv("OFFER_TIMEOUT_SECONDS", "20"))
    # Which simulator animates accepted rides - exactly one runs:
    #   "python" (default) - in-process simulator in sockets/handlers.py
    #   "node"             - backend/ride_simulation_engine.js (the backend
    #                        refuses the engine's connection in any other mode)
    #   "none"             - no simulator; driver clients send ride_status_update
    RIDE_SIMULATOR = os.getenv("RIDE_SIMULATOR", "python").strip().lower()

    # --- Matching / routing ---
    DRIVER_SEARCH_RADIUS_KM = float(os.getenv("DRIVER_SEARCH_RADIUS_KM", "8"))
    DRIVER_SEARCH_CANDIDATES = int(os.getenv("DRIVER_SEARCH_CANDIDATES", "10"))
    OSRM_BASE_URL = os.getenv("OSRM_BASE_URL", "https://router.project-osrm.org")
    OSRM_TIMEOUT_SECONDS = float(os.getenv("OSRM_TIMEOUT_SECONDS", "1.5"))
    USE_LIVE_ROUTING = _env_bool("USE_LIVE_ROUTING", True)
    AVG_CITY_SPEED_KMH = float(os.getenv("AVG_CITY_SPEED_KMH", "28"))

    # --- Misc ---
    SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
    ENV = os.getenv("FLASK_ENV", "development")
    PORT = int(os.getenv("PORT", "5001"))

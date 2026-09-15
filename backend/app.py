import eventlet

eventlet.monkey_patch()

import logging  # noqa: E402

from flask import Flask
from flask_cors import CORS

from config import Config
from db import close_pool, get_db_conn, init_pool
from extensions import limiter, socketio
from routes.admin_routes import admin_bp
from routes.auth_routes import auth_bp
from routes.health_routes import health_bp
from routes.menu_routes import menu_bp
from services.ops_stats import log_counter
from sockets.handlers import register_handlers, start_background_tasks

logging.basicConfig(
    level=logging.INFO if Config.ENV != "development" else logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("lyft").addHandler(log_counter)

logger = logging.getLogger("lyft.app")


def _check_config():
    if Config.RIDE_SIMULATOR not in ("python", "node", "none"):
        raise RuntimeError(f"RIDE_SIMULATOR must be 'python', 'node' or 'none', got {Config.RIDE_SIMULATOR!r}")
    if Config.LOAD_TEST_MODE:
        if Config.ENV == "production":
            raise RuntimeError("LOAD_TEST_MODE must not be enabled when FLASK_ENV=production")
        logger.warning("LOAD_TEST_MODE is on: login/signup rate limits raised to %s", Config.LOAD_TEST_RATE_LIMIT)
    if Config.DRIVER_HEARTBEAT_INTERVAL_SECONDS >= Config.DRIVER_LOCATION_TTL_SECONDS:
        logger.warning("DRIVER_HEARTBEAT_INTERVAL_SECONDS should be well below DRIVER_LOCATION_TTL_SECONDS")
    logger.info("Ride simulator: %s", Config.RIDE_SIMULATOR)


def _check_schema():
    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.ride_offers')")
                migrated = cur.fetchone()[0] is not None
        if not migrated:
            logger.error("Database schema is out of date: run `python init_db.py --migrate`")
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not check database schema: %s", e)


def create_app() -> Flask:
    _check_config()
    app = Flask(__name__)
    app.config["SECRET_KEY"] = Config.SECRET_KEY

    CORS(app, origins=Config.CORS_ORIGINS, supports_credentials=True)

    init_pool()
    _check_schema()
    limiter.init_app(app)
    socketio.init_app(app)
    register_handlers()
    start_background_tasks()

    app.register_blueprint(auth_bp)
    app.register_blueprint(menu_bp)
    app.register_blueprint(health_bp)
    app.register_blueprint(admin_bp)

    @app.route("/")
    def home():
        return {"service": "lyft-clone-backend", "status": "running"}

    @app.teardown_appcontext
    def _shutdown(_exception=None):
        pass  # pooled connections are returned per-request already

    return app


app = create_app()


def _reset_drivers_offline():
    """On a fresh server start, no sockets are connected yet, so no driver
    can really be 'online'. Force-clear stale is_available flags left over
    from a previous run/crash."""
    from db import get_db_conn

    try:
        with get_db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE drivers SET is_available = FALSE")
            conn.commit()
        logger.info("Reset all drivers to offline on startup")
    except Exception as e:  # noqa: BLE001
        logger.warning("Could not reset drivers on startup: %s", e)


if __name__ == "__main__":
    _reset_drivers_offline()
    try:
        socketio.run(app, host="0.0.0.0", port=Config.PORT, debug=(Config.ENV == "development"))
    finally:
        close_pool()

import eventlet

eventlet.monkey_patch()

import logging  # noqa: E402

from flask import Flask
from flask_cors import CORS

from config import Config
from db import close_pool, init_pool
from extensions import limiter, socketio
from routes.auth_routes import auth_bp
from routes.health_routes import health_bp
from routes.menu_routes import menu_bp
from sockets.handlers import register_handlers

logging.basicConfig(
    level=logging.INFO if Config.ENV != "development" else logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("werkzeug").setLevel(logging.WARNING)

logger = logging.getLogger("lyft.app")


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = Config.SECRET_KEY

    CORS(app, origins=Config.CORS_ORIGINS, supports_credentials=True)

    init_pool()
    limiter.init_app(app)
    socketio.init_app(app)
    register_handlers()

    app.register_blueprint(auth_bp)
    app.register_blueprint(menu_bp)
    app.register_blueprint(health_bp)

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

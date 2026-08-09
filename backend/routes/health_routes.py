from flask import Blueprint, jsonify

from db import health_check

health_bp = Blueprint("health", __name__, url_prefix="/api")


@health_bp.route("/health", methods=["GET"])
def health():
    status = health_check()
    ok = status["postgres"] == "up" and status["redis"] == "up"
    return jsonify({"success": ok, "dependencies": status}), (200 if ok else 503)
